# main.py — Orquestrador principal do Setup Atirador v9.
# Entry point para cron e execução manual.
#
# Pipeline:
#   1. fetch_perpetuals → universe
#   2. fetch BTC klines 15m → build_btc_context
#   3. Para cada token: gather klines (5 TFs) → dataframes → evaluate_token
#   4. Persistência: state, logger, journal
#   5. Notificações: telegram notify_call/notify_heartbeat
#   6. Watchdog + notify_error no wrapper global

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Optional

import aiohttp
import pandas as pd

from config import (
    BRT,
    BTC_SYMBOL,
    KLINE_LIMIT_15M,
    KLINE_LIMIT_1H,
    KLINE_LIMIT_4H,
    KLINE_LIMIT_5M,
    KLINE_LIMIT_1M,
    LOG_DIR,
    MAX_CONCURRENT_FETCHES,
    SETUPS_ENABLED,
    VERSION,
)
from exchanges import (
    fetch_fear_greed_async,
    fetch_klines_cached_async,
    fetch_perpetuals,
    klines_to_dataframe,
    reset_data_source_attempts,
)
from journal import TradeJournalV9
from logger import RoundLoggerV9
from signals import build_btc_context, evaluate_token
from state import (
    cleanup_state,
    load_state,
    save_state,
    update_oi_history,
    update_setups_history,
)
from telegram import (
    RoundStats,
    notify_call,
    notify_error,
    notify_heartbeat,
)

LOG = logging.getLogger("atirador")

_WATCHDOG_PATH = "/tmp/atirador_last_run.json"


# ===========================================================================
# Logging setup
# ===========================================================================

def setup_logger() -> tuple[logging.Logger, str, str]:
    """Configura logging com saída para arquivo (BRT) e stdout.

    Cria o diretório de logs se não existir.
    Retorna (logger, log_file_path, ts_scan).
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    ts_brt = datetime.now(BRT)
    ts_str = ts_brt.strftime("%Y%m%d_%H%M")
    logfile = f"{LOG_DIR}/atirador_LOG_{ts_str}.log"

    logger = logging.getLogger("atirador")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    def brt_converter(timestamp, *args):
        return datetime.fromtimestamp(timestamp, BRT).timetuple()

    fmt_file = logging.Formatter(
        "%(asctime)s BRT [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fmt_file.converter = brt_converter

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt_file)

    fmt_term = logging.Formatter("%(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt_term)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"[v9] Log iniciado: {logfile}")
    return logger, logfile, ts_str


def log_section(title: str) -> None:
    LOG.info(f"\n{'─'*55}")
    LOG.info(f"  {title}")
    LOG.info(f"{'─'*55}")


# ===========================================================================
# Helpers
# ===========================================================================

def _to_iso(value) -> Optional[str]:
    """pd.Timestamp / datetime / str → ISO; None se vazio."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except Exception:
            return str(value)
    return str(value)


def _detect_near_miss(decision) -> Optional[dict]:
    """Retorna dict para logger.add_near_miss se qualificar como near-miss.

    Regra v9:
        - decision.action == "SKIP"
        - pelo menos 1 SetupResult em all_setup_results com triggered=True
        - retorna o setup com maior confidence entre os triggered

    Retorno: dict com chaves (symbol, closest_setup_name, closest_confidence,
    closest_direction, closest_regime, conditions, ts) ou None.
    """
    if getattr(decision, "action", None) != "SKIP":
        return None

    all_results = list(getattr(decision, "all_setup_results", []) or [])
    triggered = [r for r in all_results if getattr(r, "triggered", False)]
    if not triggered:
        return None

    closest = max(triggered, key=lambda r: getattr(r, "confidence", 0.0) or 0.0)

    # Conditions vivem em SetupResult.evidence["conditions"] (não no top-level
    # do dataclass) e são dicts produzidos por setups.base.evaluate_condition.
    # Bug pré-PR3: lia `closest.conditions` (atributo inexistente) → 100% vazio.
    evidence = getattr(closest, "evidence", {}) or {}
    raw = evidence.get("conditions", []) if isinstance(evidence, dict) else []
    conditions_serialized = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        try:
            conditions_serialized.append({
                "name":      c.get("name"),
                "value":     float(c.get("value", 0.0) or 0.0),
                "threshold": float(c.get("threshold", 0.0) or 0.0),
                "operator":  c.get("operator"),
                "passed":    bool(c.get("passed", False)),
                "weight":    float(c.get("weight", 0.0) or 0.0),
            })
        except Exception:
            pass

    ts = _to_iso(getattr(decision, "timestamp", None)) or datetime.now(BRT).isoformat()

    return {
        "symbol":             getattr(decision, "symbol", None),
        "closest_setup_name": getattr(closest, "setup_name", None),
        "closest_confidence": float(getattr(closest, "confidence", 0.0) or 0.0),
        "closest_direction":  getattr(closest, "direction", None),
        "closest_regime":     getattr(decision, "regime", None),
        "conditions":         conditions_serialized,
        "ts":                 ts,
    }


async def _fetch_token_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    sem: asyncio.Semaphore,
) -> dict:
    """Busca 5 TFs de klines para um token. Semáforo limita concorrência.

    Retorna dict com chaves df_15m, df_1h, df_4h, df_5m, df_1m — todos
    pd.DataFrame (podem ser vazios se fetch falhou).

    Não levanta exceção — falhas viram DataFrames vazios, que o
    evaluate_token vai rejeitar no build_market_context.
    """
    async with sem:
        try:
            k15, k1h, k4h, k5m, k1m = await asyncio.gather(
                fetch_klines_cached_async(session, symbol, "15m", KLINE_LIMIT_15M),
                fetch_klines_cached_async(session, symbol, "1H",  KLINE_LIMIT_1H),
                fetch_klines_cached_async(session, symbol, "4H",  KLINE_LIMIT_4H),
                fetch_klines_cached_async(session, symbol, "5m",  KLINE_LIMIT_5M),
                fetch_klines_cached_async(session, symbol, "1m",  KLINE_LIMIT_1M),
                return_exceptions=True,
            )
        except Exception as e:
            LOG.warning(f"[v9] gather klines {symbol}: {e}")
            return {tf: pd.DataFrame() for tf in
                    ("df_15m", "df_1h", "df_4h", "df_5m", "df_1m")}

    def _to_df(result) -> pd.DataFrame:
        if isinstance(result, Exception):
            return pd.DataFrame()
        if not result:
            return pd.DataFrame()
        try:
            return klines_to_dataframe(result)
        except Exception:
            return pd.DataFrame()

    return {
        "df_15m": _to_df(k15),
        "df_1h":  _to_df(k1h),
        "df_4h":  _to_df(k4h),
        "df_5m":  _to_df(k5m),
        "df_1m":  _to_df(k1m),
    }


def _build_round_stats(
    decisions: list,
    perpetuals: list[dict],
    btc_context: dict,
    exchange: str,
    fgi: int,
    elapsed: float,
) -> RoundStats:
    """Agrega decisions + contexto em RoundStats para notify_heartbeat."""
    n_universe = len(perpetuals)
    n_regime_eligible = sum(
        1 for d in decisions
        if getattr(d, "regime", None) not in (None, "")
    )

    def _any_triggered(decision) -> bool:
        results = getattr(decision, "all_setup_results", []) or []
        return any(getattr(r, "triggered", False) for r in results)

    n_triggered = sum(1 for d in decisions if _any_triggered(d))
    n_calls = sum(1 for d in decisions if getattr(d, "action", None) == "CALL")
    n_near_miss = sum(
        1 for d in decisions
        if getattr(d, "action", None) == "SKIP" and _any_triggered(d)
    )

    counter: dict[str, int] = {}
    for d in decisions:
        for r in getattr(d, "all_setup_results", []) or []:
            if getattr(r, "triggered", False):
                name = getattr(r, "setup_name", None)
                if name:
                    counter[name] = counter.get(name, 0) + 1

    return RoundStats(
        n_universe=n_universe,
        n_regime_eligible=n_regime_eligible,
        n_triggered=n_triggered,
        n_calls=n_calls,
        n_near_miss=n_near_miss,
        setups_counter=counter,
        fgi=fgi,
        btc_regime=btc_context.get("regime", "UNKNOWN"),
        btc_change_pct_15m=float(btc_context.get("change_pct_15m", 0.0) or 0.0),
        elapsed_seconds=elapsed,
        exchange=exchange,
    )


def _write_watchdog() -> None:
    """Grava /tmp/atirador_last_run.json. Falha silenciosa."""
    try:
        payload = {
            "last_run": datetime.now(BRT).isoformat(),
            "version":  f"v{VERSION}",
        }
        with open(_WATCHDOG_PATH, "w") as f:
            json.dump(payload, f)
    except Exception:
        pass


def _get_open_symbols(journal: TradeJournalV9) -> list[str]:
    """Lista símbolos com trade OPEN no journal.

    TradeJournalV9 expõe get_open_trade(symbol, direction) mas não uma
    listagem. Consulta direta via conexão interna (acesso a atributo privado
    aceitável porque ambos os módulos estão sob nossa gestão).
    """
    try:
        conn = journal._connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM trades WHERE status='OPEN'"
            ).fetchall()
            return [r["symbol"] for r in rows] if rows else []
        finally:
            conn.close()
    except Exception:
        return []


# ===========================================================================
# Pipeline principal
# ===========================================================================

async def run_scan_async() -> None:
    """Pipeline v9 completo de uma rodada.

    1. Universe (OKX → Gate → Bitget fallback)
    2. BTC klines 15m → build_btc_context
    3. Para cada token (async gather de klines + evaluate serial)
    4. Persistência: state, logger, journal
    5. Notificações Telegram
    6. Watchdog
    """
    t_start = time.time()
    reset_data_source_attempts()

    round_log: Optional[RoundLoggerV9] = None
    try:
        round_log = RoundLoggerV9(version=str(VERSION))
    except Exception:
        LOG.warning("[v9] RoundLoggerV9 falhou ao inicializar", exc_info=True)

    trade_journal: Optional[TradeJournalV9] = None
    try:
        trade_journal = TradeJournalV9()
    except Exception:
        LOG.warning("[v9] TradeJournalV9 falhou ao inicializar", exc_info=True)

    state = load_state()

    # ── 1. Fear & Greed + Universe ──────────────────────────────────────
    log_section("Universe + FGI")
    async with aiohttp.ClientSession() as session:
        fg = await fetch_fear_greed_async(session)
    fgi = int(fg.get("value") or 50)
    LOG.info(f"[v9] FGI={fgi} ({fg.get('classification', '–')})")

    perpetuals, exchange = await fetch_perpetuals()
    if not perpetuals:
        LOG.error("[v9] Sem perpetuals — abortando")
        return

    symbols = [p["symbol"] for p in perpetuals]
    LOG.info(f"[v9] Universo: {len(symbols)} tokens ({exchange})")

    # ── 2. BTC context ──────────────────────────────────────────────────
    log_section("BTC context")
    btc_context: dict = {"regime": "UNKNOWN", "change_pct_15m": 0.0, "atr_pct": 0.0}
    try:
        async with aiohttp.ClientSession() as session:
            btc_klines = await fetch_klines_cached_async(
                session, BTC_SYMBOL, "15m", KLINE_LIMIT_15M,
            )
        if btc_klines:
            df_btc = klines_to_dataframe(btc_klines)
            if not df_btc.empty:
                btc_context = build_btc_context(df_btc)
                LOG.info(
                    f"[v9] BTC: regime={btc_context['regime']} "
                    f"change_15m={btc_context['change_pct_15m']:+.2f}%"
                )
    except Exception as e:
        LOG.warning(f"[v9] Falha ao montar btc_context: {e}")

    # ── 3. Open trades (para evitar duplicação em evaluate_token) ───────
    open_symbols: list[str] = []
    if trade_journal is not None:
        try:
            open_symbols = _get_open_symbols(trade_journal)
        except Exception as e:
            LOG.warning(f"[v9] Falha ao listar open trades: {e}")

    # ── 4. Pipeline por token ───────────────────────────────────────────
    log_section(f"Pipeline por token (concurrency={MAX_CONCURRENT_FETCHES})")
    decisions: list = []
    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)

    async with aiohttp.ClientSession() as session:
        fetch_tasks = [
            _fetch_token_klines(session, sym, sem) for sym in symbols
        ]
        klines_by_token = await asyncio.gather(*fetch_tasks, return_exceptions=False)

    disabled_setups = {
        name for name, enabled in SETUPS_ENABLED.items() if not enabled
    }

    # Evaluate serial (pandas_ta não é thread-safe + logs ordenados)
    for sym, k in zip(symbols, klines_by_token):
        df_15m = k["df_15m"]
        if df_15m.empty:
            continue
        try:
            decision = evaluate_token(
                symbol=sym,
                df_15m=df_15m,
                df_1h=k["df_1h"],
                df_4h=k["df_4h"],
                df_5m=k["df_5m"] if not k["df_5m"].empty else None,
                df_1m=k["df_1m"] if not k["df_1m"].empty else None,
                open_trades=open_symbols,
                btc_context=btc_context,
                disabled_setups=disabled_setups,
            )
            decisions.append(decision)
        except Exception as e:
            LOG.warning(f"[v9] evaluate_token({sym}) falhou: {e}", exc_info=False)

    LOG.info(f"[v9] Avaliados: {len(decisions)}/{len(symbols)}")

    # ── 5. Notificações Telegram — CALLs ────────────────────────────────
    log_section("Telegram — CALLs")
    for d in decisions:
        if getattr(d, "action", None) == "CALL":
            try:
                notify_call(d)
            except Exception:
                LOG.warning(f"[v9] notify_call falhou para {d.symbol}", exc_info=True)

    # ── 6. Journal — open + tracking ────────────────────────────────────
    if trade_journal is not None:
        log_section("Journal")
        for d in decisions:
            if getattr(d, "action", None) == "CALL":
                try:
                    trade_journal.open_trade(d, fgi=fgi)
                except Exception:
                    LOG.warning(f"[v9] open_trade falhou para {d.symbol}", exc_info=True)
        try:
            closed = trade_journal.check_open_trades()
            if closed > 0:
                LOG.info(f"[v9] Journal tracker: {closed} trade(s) fechado(s)")
        except Exception:
            LOG.warning("[v9] check_open_trades falhou", exc_info=True)

    # ── 7. State update ─────────────────────────────────────────────────
    try:
        ts_now = datetime.now(BRT).isoformat()
        update_setups_history(state, decisions, ts_now)
        update_oi_history(state, perpetuals, ts_now)
        cleanup_state(state)
        save_state(state)
    except Exception:
        LOG.warning("[v9] State update falhou", exc_info=True)

    # ── 8. RoundLogger commit ───────────────────────────────────────────
    elapsed = time.time() - t_start
    if round_log is not None:
        try:
            round_log.set_meta(
                fgi=fgi,
                btc_regime=btc_context.get("regime", "UNKNOWN"),
                btc_change_pct_15m=float(btc_context.get("change_pct_15m", 0.0) or 0.0),
                exchange_primary=exchange,
                setups_enabled=dict(SETUPS_ENABLED),
                btc_filter_blocked=any(
                    getattr(d, "skip_reason", None) == "btc_abrupt_move"
                    for d in decisions
                ),
            )
            setups_triggered_total = sum(
                1
                for d in decisions
                for r in getattr(d, "all_setup_results", []) or []
                if getattr(r, "triggered", False)
            )
            near_misses = [_detect_near_miss(d) for d in decisions]
            near_misses = [nm for nm in near_misses if nm is not None]

            round_log.set_pipeline(
                universe=len(symbols),
                tokens_evaluated=len(decisions),
                calls_emitted=sum(1 for d in decisions if d.action == "CALL"),
                setups_triggered_total=setups_triggered_total,
                near_misses_count=len(near_misses),
            )
            round_log.set_exec_seconds(elapsed)

            for d in decisions:
                round_log.add_token_evaluation(d)

            for nm in near_misses:
                round_log.add_near_miss(**nm)

            for d in decisions:
                if getattr(d, "action", None) == "CALL":
                    tp = getattr(d, "trade_plan", None)
                    round_log.add_event(
                        type="CALL",
                        symbol=d.symbol,
                        direction=d.direction or "",
                        signal_tag=d.signal_tag or "",
                        confidence=d.confidence,
                        leverage=float(getattr(tp, "leverage", 0.0) or 0.0),
                    )
            round_log.commit()
        except Exception:
            LOG.warning("[v9] RoundLogger commit falhou", exc_info=True)

    # ── 9. Telegram heartbeat ───────────────────────────────────────────
    try:
        stats = _build_round_stats(
            decisions=decisions,
            perpetuals=perpetuals,
            btc_context=btc_context,
            exchange=exchange,
            fgi=fgi,
            elapsed=elapsed,
        )
        notify_heartbeat(stats)
    except Exception:
        LOG.warning("[v9] notify_heartbeat falhou", exc_info=True)

    # ── 10. Watchdog ────────────────────────────────────────────────────
    _write_watchdog()

    n_calls = sum(1 for d in decisions if getattr(d, "action", None) == "CALL")
    LOG.info(f"[v9] Rodada concluída em {elapsed:.1f}s — {n_calls} CALL")


# ===========================================================================
# Entry point
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description=f"Setup Atirador v{VERSION}")
    parser.add_argument("--once", action="store_true",
                        help="Executa uma rodada e sai (modo cron)")
    parser.add_argument("--dry-run", action="store_true",
                        help="(reservado) Não envia Telegram")
    args = parser.parse_args()

    global LOG
    LOG, _, _ = setup_logger()

    if args.dry_run:
        os.environ["DRY_RUN"] = "1"

    LOG.info(f"[v9] Setup Atirador v{VERSION} iniciando...")

    try:
        asyncio.run(run_scan_async())
    except KeyboardInterrupt:
        LOG.info("[v9] Interrompido pelo usuário")
    except Exception as e:
        tb = traceback.format_exc()
        LOG.exception(f"[v9] Erro fatal: {e}")
        try:
            notify_error(
                title="Falha na rodada",
                error_message=tb,
                context={"version": VERSION, "ts": datetime.now(BRT).isoformat()},
            )
        except Exception:
            LOG.warning("[v9] notify_error também falhou — sem alerta", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
