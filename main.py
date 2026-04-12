# main.py — Orquestrador principal do Setup Atirador v8.1.0
# Entry point para cron e execução manual.
# Contém apenas lógica de orquestração — toda lógica de negócio vive nos módulos.

import asyncio
import logging
import sys
import time
from datetime import datetime

import aiohttp

from config import VERSION, BRT, LOG_DIR, COLS_4H, COLS_1H, COLS_15M_TECH, ZONA_ORDER
from exchanges import fetch_perpetuals, fetch_fear_greed_async, fetch_klines_cached_async
from gates import fetch_tv_batch_async, recommendation_from_value
from indicators import get_candle_lock_status, apply_candle_lock
from scoring import check_rejeicao_presente, check_estrutura_direcional, check_forca_movimento
from signals import analisar_token_async, calc_trade_params, calc_trade_params_short
from state import load_daily_state, save_daily_state, update_score_history, cleanup_score_history
from telegram import tg_notify_v7

try:
    from logger import RoundLogger
    _OBSERVABILITY = True
except ImportError:
    _OBSERVABILITY = False

try:
    from journal import TradeJournal
    _OBSERVABILITY_JOURNAL = True
except ImportError:
    _OBSERVABILITY_JOURNAL = False

LOG = logging.getLogger("atirador")


# ===========================================================================
# Logging setup
# ===========================================================================

def setup_logger() -> tuple[logging.Logger, str, str]:
    """Configura logging com saída para arquivo (BRT) e stdout.

    Cria o diretório de logs se não existir.
    Retorna (logger, log_file_path, ts_scan).
    """
    import os
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
        datefmt="%Y-%m-%d %H:%M:%S"
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
    logger.info(f"[v8] Log iniciado: {logfile}")
    return logger, logfile, ts_str


def log_section(title: str) -> None:
    LOG.info(f"\n{'─'*55}")
    LOG.info(f"  {title}")
    LOG.info(f"{'─'*55}")


# ===========================================================================
# Pipeline principal
# ===========================================================================

async def run_scan_async() -> None:
    """
    Pipeline principal v8.1.0:
    1. Fetch perpetuals (universo)
    2. TV batch 4H (Gate 4H strict — LONG/SHORT, NEUTRAL → drop)
    3. TV batch 1H (contexto)
    4. Candle lock 15m
    5. TV batch 15m (BB/volume para Check C)
    6. Por token: analisar_token_async → Check A/B/C → decisão
    7. Notificações Telegram
    8. Atualização de estado
    9. Observability (RoundLogger + TradeJournal)
    """
    t_start = time.time()

    # ── Observability: instanciar localmente ──────────────────────────────────
    round_log = None
    if _OBSERVABILITY:
        try:
            round_log = RoundLogger(version=VERSION)
        except Exception:
            LOG.warning("[v8] RoundLogger falhou ao inicializar", exc_info=True)
            round_log = None

    trade_journal = None
    if _OBSERVABILITY_JOURNAL:
        try:
            trade_journal = TradeJournal()
        except Exception:
            LOG.warning("[v8] TradeJournal falhou ao inicializar", exc_info=True)
            trade_journal = None

    state = load_daily_state()

    # ── Fear & Greed ──────────────────────────────────────────────────────────
    log_section("Fear & Greed Index")
    async with aiohttp.ClientSession() as session:
        fg = await fetch_fear_greed_async(session)
    fg_val   = fg.get("value") or 50
    fg_class = fg.get("classification", "–")
    LOG.info(f"[v8] FGI={fg_val} ({fg_class})")

    # ── Perpetuals (universo) ─────────────────────────────────────────────────
    log_section("Universo de perpetuals")
    perps, exchange = await fetch_perpetuals()
    if not perps:
        LOG.error("[v8] Sem perpetuals — abortando")
        return

    symbols = [p["symbol"] for p in perps]
    prices  = {p["symbol"]: p["price"] for p in perps}
    LOG.info(f"[v8] Universo: {len(symbols)} símbolos ({exchange})")

    # ── TV batch 4H ───────────────────────────────────────────────────────────
    log_section("TradingView 4H")
    async with aiohttp.ClientSession() as session:
        tv4h, _ = await fetch_tv_batch_async(session, symbols, COLS_4H)

    # ── Gate 4H strict ────────────────────────────────────────────────────────
    gate_long_syms  = []
    gate_short_syms = []
    for sym in symbols:
        d4  = tv4h.get(sym, {})
        rec = recommendation_from_value(d4.get("Recommend.All|240", 0))
        if rec in ("BUY", "STRONG_BUY"):
            gate_long_syms.append(sym)
        elif rec in ("SELL", "STRONG_SELL"):
            gate_short_syms.append(sym)
        # NEUTRAL → drop

    n_gate_long  = len(gate_long_syms)
    n_gate_short = len(gate_short_syms)
    gate_syms    = gate_long_syms + gate_short_syms
    LOG.info(f"[v8] Gate 4H: {n_gate_long} LONG, {n_gate_short} SHORT")

    # ── TV batch 1H ───────────────────────────────────────────────────────────
    log_section("TradingView 1H")
    async with aiohttp.ClientSession() as session:
        tv1h, _ = await fetch_tv_batch_async(session, gate_syms, COLS_1H)

    # ── Candle lock 15m ───────────────────────────────────────────────────────
    candle_lock = get_candle_lock_status()
    if candle_lock["use_prev"]:
        LOG.info(
            f"[v8] Candle lock ativo — vela em formação "
            f"({candle_lock['seconds_open']:.0f}s), "
            f"próximo fechamento em {candle_lock['next_close']:.0f}s"
        )

    # ── TV batch 15m ──────────────────────────────────────────────────────────
    log_section("TradingView 15m")
    async with aiohttp.ClientSession() as session:
        tv15m, _ = await fetch_tv_batch_async(session, gate_syms, COLS_15M_TECH)

    # ── Análise por token ─────────────────────────────────────────────────────
    log_section("Análise por token")
    results      = []
    n_zona_long  = 0
    n_zona_short = 0

    async with aiohttp.ClientSession() as session:
        for sym in gate_syms:
            d4    = tv4h.get(sym, {})
            d1    = tv1h.get(sym, {})
            d15   = tv15m.get(sym, {})
            price = prices.get(sym, 0.0)
            if not price:
                continue
            try:
                r = await analisar_token_async(session, sym, d4, d1, price, state, exchange, candle_lock)
                if r is None:
                    continue

                # Injetar dados 15m do TV para Check C (BB) se ausente
                if "check_c_det" not in r:
                    if d15:
                        candles_15m_recheck = await fetch_klines_cached_async(
                            session, sym, "15m", 20)
                        if candles_15m_recheck:
                            candles_15m_recheck = apply_candle_lock(candles_15m_recheck, candle_lock)
                        if candles_15m_recheck:
                            c_total, c_det = check_forca_movimento(
                                candles_15m_recheck, d15, state, r["direction"])
                            r["check_c_total"] = c_total
                            r["check_c_det"]   = c_det
                            thr_c = r["check_c_thr"]
                            if r["check_a_ok"] and r["check_b_ok"]:
                                r["status"] = "CALL" if c_total >= thr_c else "QUASE"
                                if r["status"] == "CALL" and not r.get("params"):
                                    candles_4h = await fetch_klines_cached_async(
                                        session, sym, "4H", 50)
                                    candles_1h = await fetch_klines_cached_async(
                                        session, sym, "1H", 50)
                                    if r["direction"] == "LONG":
                                        r["params"] = calc_trade_params(
                                            sym, price, r["zona_qualidade"],
                                            c_total, candles_4h, candles_1h)
                                    else:
                                        r["params"] = calc_trade_params_short(
                                            sym, price, r["zona_qualidade"],
                                            c_total, candles_4h, candles_1h)

                if r["direction"] == "LONG":
                    n_zona_long += 1
                else:
                    n_zona_short += 1
                results.append(r)

            except Exception as e:
                LOG.warning(f"[v8] Erro em {sym}: {e}", exc_info=True)

    # ── BTC 4H trend (busca independente do universo) ────────────────────────
    btc_4h = "–"
    try:
        async with aiohttp.ClientSession() as session:
            btc_tv, _ = await fetch_tv_batch_async(session, ["BTCUSDT"], COLS_4H)
        btc_d4 = btc_tv.get("BTCUSDT", {})
        if btc_d4:
            btc_4h = recommendation_from_value(btc_d4.get("Recommend.All|240", 0))
    except Exception:
        pass

    # ── Notificações Telegram ─────────────────────────────────────────────────
    log_section("Notificações Telegram")
    elapsed = time.time() - t_start
    tg_notify_v7(
        results      = results,
        fg_val       = fg_val,
        n_univ       = len(symbols),
        n_gate_short = n_gate_short,
        n_gate_long  = n_gate_long,
        n_zona_short = n_zona_short,
        n_zona_long  = n_zona_long,
        elapsed      = elapsed,
        exchange     = exchange,
        btc_4h       = btc_4h,
    )

    # ── Atualização de estado ─────────────────────────────────────────────────
    ts_now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    update_score_history(state, results, ts_now)
    cleanup_score_history(state)
    save_daily_state(state)

    # ── Observability: RoundLogger ────────────────────────────────────────────
    if round_log is not None:
        try:
            round_log.set_meta(
                fgi          = fg_val,
                btc_4h       = btc_4h,
                exchange     = exchange,
                candle_locked= candle_lock.get("use_prev", False),
            )
            round_log.set_pipeline(
                universe      = len(symbols),
                after_gate_4h = len(gate_syms),
                after_gate_1h = len(gate_syms),
                scored_15m    = len(results),
            )
            for r in results:
                round_log.add_token(
                    symbol         = r["symbol"],
                    direction      = r["direction"],
                    status         = r["status"],
                    zona_qualidade = r.get("zona_qualidade", ""),
                    zona_descricao = r.get("zona_descricao", ""),
                    check_a_ok     = r.get("check_a_ok", False),
                    check_a_reason = r.get("check_a_reason", ""),
                    check_a_ev     = r.get("check_a_ev", {}),
                    check_b_ok     = r.get("check_b_ok", False),
                    check_b_reason = r.get("check_b_reason", ""),
                    check_b_ev     = r.get("check_b_ev", {}),
                    check_c_total  = r.get("check_c_total", 0),
                    check_c_thr    = r.get("check_c_thr", 0),
                    check_c_det    = r.get("check_c_det", {}),
                    zona_rich      = r.get("zona_rich"),
                )
            for r in results:
                if r["status"] in ("CALL", "QUASE"):
                    round_log.add_event(
                        type           = r["status"],
                        symbol         = r["symbol"],
                        direction      = r["direction"],
                        zona_qualidade = r.get("zona_qualidade", ""),
                        check_c_total  = r.get("check_c_total", 0),
                        check_c_thr    = r.get("check_c_thr", 0),
                    )
            round_log.set_exec_seconds(elapsed)
            round_log.commit()
        except Exception:
            LOG.warning("[v8] RoundLogger.commit() falhou", exc_info=True)

    # ── Observability: TradeJournal ───────────────────────────────────────────
    if trade_journal is not None:
        try:
            for r in results:
                if r["status"] not in ("CALL", "QUASE"):
                    continue
                if r["status"] == "CALL" and not r.get("params"):
                    continue
                p = r.get("params") or {}
                pillars = {
                    # Checks — resultados booleanos
                    "check_a_ok":     r.get("check_a_ok"),
                    "check_a_reason": r.get("check_a_reason"),
                    "check_a_ev":     r.get("check_a_ev", {}),
                    "check_b_ok":     r.get("check_b_ok"),
                    "check_b_reason": r.get("check_b_reason"),
                    "check_b_ev":     r.get("check_b_ev", {}),
                    "check_c_total":  r.get("check_c_total"),
                    "check_c_thr":    r.get("check_c_thr"),
                    "check_c_det":    r.get("check_c_det", {}),
                    # Zona
                    "zona_qualidade": r.get("zona_qualidade"),
                    "zona_descricao": r.get("zona_descricao"),
                    "zona_rich":      r.get("zona_rich", {}),
                    # Contexto de mercado no momento do sinal
                    "gate_4h":        r.get("rec_4h"),
                    "gate_1h":        r.get("rec_1h"),
                    "price":          r.get("price"),
                    # Candle de referência (OHLCV completo)
                    "candle_ref":     r.get("candle_ref", {}),
                }
                trade_journal.open_trade(
                    symbol        = r["symbol"],
                    direction     = r["direction"],
                    type          = r["status"],
                    score         = r.get("check_c_total", 0),
                    entry_price   = p.get("entry", 0),
                    sl_price      = p.get("sl", 0),
                    tp1           = p.get("tp1", 0),
                    tp2           = p.get("tp2", 0),
                    tp3           = p.get("tp3", 0),
                    fgi           = fg_val,
                    btc_4h        = btc_4h,
                    pillars_dict  = pillars,
                    kline_venue   = r.get("exchange"),
                    venue_quality = r.get("zona_qualidade"),
                )
        except Exception:
            LOG.warning("[v8] TradeJournal falhou", exc_info=True)

    # ── Watchdog ──────────────────────────────────────────────────────────────
    try:
        import json as _json
        import os as _os
        _wd = {"last_run": datetime.now(BRT).isoformat(), "version": f"v{VERSION}"}
        with open("/tmp/atirador_last_run.json", "w") as _f:
            _json.dump(_wd, _f)
    except Exception:
        pass

    n_calls = sum(1 for r in results if r["status"] == "CALL")
    n_quase = sum(1 for r in results if r["status"] == "QUASE")
    LOG.info(f"[v8] Rodada concluída em {elapsed:.1f}s — {n_calls} CALL, {n_quase} QUASE")


# ===========================================================================
# Entry point
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=f"Setup Atirador v{VERSION}")
    parser.add_argument("--once", action="store_true",
                        help="Executa uma rodada e sai (modo cron)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Não envia Telegram, apenas loga")
    args = parser.parse_args()

    global LOG
    LOG, _, _ = setup_logger()

    if args.dry_run:
        import os as _os
        _os.environ["DRY_RUN"] = "1"

    LOG.info(f"[v8] Setup Atirador v{VERSION} iniciando...")

    try:
        asyncio.run(run_scan_async())
    except KeyboardInterrupt:
        LOG.info("[v8] Interrompido pelo usuário")
    except Exception as e:
        LOG.exception(f"[v8] Erro fatal: {e}")
        raise


if __name__ == "__main__":
    main()
