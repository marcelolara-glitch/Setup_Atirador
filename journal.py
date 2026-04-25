"""journal.py — Forward Testing v9 (Camada 2).

Substitui o TradeJournal v8 (gate+check+score, is_hypothetical) pela
:class:`TradeJournalV9` alinhada ao modelo multi-setup com saídas parciais
TP1→TP2→TP3 e SL dinâmico (breakeven após TP1, tp1 após TP2).

A lógica de avanço/saída é delegada a :func:`risk.update_trade_state`;
o journal apenas persiste o estado e alimenta o update com candles novos.

Princípios:
    - Falhas silenciosas em I/O (nunca crasha o scan).
    - Paths isolados (``JOURNAL_DB_V9``); v8 fica intocado.
    - Fetcher síncrono interno — não depende de ``exchanges.py``.
    - Duck typing com ``SignalDecision`` (não importa de ``signals.py``).
    - Dedupe de trades OPEN por (symbol, direction) — crítico com 5 setups paralelos.

Ciclo de vida:
    Fase A — Capture: ``open_trade(decision)`` registra CALL com status OPEN.
    Fase B — Track:   ``check_open_trades(fetch_klines_fn)`` avalia via klines.
    Fase C — Close:   automático quando SL ou TP3 atingido, ou timeout 48h.

Este módulo é construído em 4 PRs (12c.A → 12c.D). Este PR (12c.A) entrega
o esqueleto + helpers puros; os métodos da classe estão como stubs.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Optional

import requests

from config import BRT, JOURNAL_DB_V9, JOURNAL_DIR, TRADE_TIMEOUT_HOURS
from risk import TradePlan, TradeState, update_trade_state

LOG = logging.getLogger("atirador")


# ---------------------------------------------------------------------------
# DDL SQLite v9
# ---------------------------------------------------------------------------

_DDL_V9 = """
CREATE TABLE IF NOT EXISTS trades (
    id                    TEXT PRIMARY KEY,
    timestamp             TEXT NOT NULL,
    symbol                TEXT NOT NULL,
    direction             TEXT NOT NULL,
    setup_name            TEXT NOT NULL,
    confluent_setups      TEXT,
    confidence            REAL NOT NULL,
    regime_at_entry       TEXT NOT NULL,
    entry_price           REAL NOT NULL,
    sl_price              REAL NOT NULL,
    current_sl            REAL NOT NULL,
    tp1_price             REAL NOT NULL,
    tp2_price             REAL NOT NULL,
    tp3_price             REAL NOT NULL,
    leverage              REAL NOT NULL,
    atr_value             REAL,
    position_split        TEXT NOT NULL,
    tp1_hit               INTEGER NOT NULL DEFAULT 0,
    tp2_hit               INTEGER NOT NULL DEFAULT 0,
    tp3_hit               INTEGER NOT NULL DEFAULT 0,
    position_remaining    REAL NOT NULL DEFAULT 1.0,
    context_fgi           INTEGER,
    context_btc_regime    TEXT,
    status                TEXT NOT NULL DEFAULT 'OPEN',
    exit_price            REAL,
    exit_time             TEXT,
    pnl_pct               REAL,
    max_runup             REAL DEFAULT 0,
    max_drawdown          REAL DEFAULT 0,
    timeout_hours         INTEGER NOT NULL DEFAULT 48,
    evidence_json         TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_status    ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol    ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_setup     ON trades(setup_name);
"""


# ---------------------------------------------------------------------------
# Helpers matemáticos puros (standalone, sem I/O)
# ---------------------------------------------------------------------------


def _plan_from_row(row: dict) -> TradePlan:
    """Reconstrói TradePlan a partir de linha do DB.

    Campos não armazenados (distance_pct, risk_reward_tp*) ficam 0.0 —
    ``update_trade_state`` só consome direction/entry/sl/tp1/2/3/position_split.
    """
    split_raw = row.get("position_split") or "[0.5, 0.3, 0.2]"
    try:
        split = tuple(json.loads(split_raw))
    except Exception:
        split = (0.5, 0.3, 0.2)

    return TradePlan(
        direction=row["direction"],
        entry_price=float(row["entry_price"]),
        sl_price=float(row["sl_price"]),
        sl_distance_pct=0.0,
        tp1_price=float(row["tp1_price"]),
        tp2_price=float(row["tp2_price"]),
        tp3_price=float(row["tp3_price"]),
        tp1_distance_pct=0.0,
        tp2_distance_pct=0.0,
        tp3_distance_pct=0.0,
        risk_reward_tp1=0.0,
        risk_reward_tp2=0.0,
        risk_reward_tp3=0.0,
        atr_value=float(row.get("atr_value") or 0.0),
        position_split=split,
        leverage=float(row["leverage"]),
    )


def _state_from_row(row: dict, plan: TradePlan) -> TradeState:
    """Reconstrói TradeState a partir de linha do DB + TradePlan."""
    return TradeState(
        trade_plan=plan,
        current_sl=float(row["current_sl"]),
        tp1_hit=bool(row["tp1_hit"]),
        tp2_hit=bool(row["tp2_hit"]),
        tp3_hit=bool(row["tp3_hit"]),
        position_remaining=float(row["position_remaining"]),
        status=row["status"],
    )


def _calc_partial_pnl(
    status: str,
    direction: str,
    entry: float,
    tp1: float,
    tp2: float,
    tp3: float,
    current_sl: float,
    position_split: tuple,
) -> float:
    """Calcula pnl_pct ponderado conforme saídas parciais.

    Esquema v9 de saídas:
        - WIN_TP3: 50% em tp1 + 30% em tp2 + 20% em tp3
        - WIN_TP2: 50% em tp1 + 30% em tp2 + 20% em current_sl (= tp1_price)
        - WIN_TP1: 50% em tp1 + 50% em current_sl (= entry, breakeven)
        - LOSS_SL: 100% em current_sl (= sl_price original)
        - EXPIRED: 0.0

    SHORT inverte sinal: lucro quando preço cai.
    """
    if entry <= 0:
        return 0.0

    def pct_long(price: float) -> float:
        return (price - entry) / entry * 100.0

    def pct_short(price: float) -> float:
        return (entry - price) / entry * 100.0

    pct = pct_long if direction == "LONG" else pct_short

    if not position_split or len(position_split) < 3:
        split_tp1, split_tp2, split_tp3 = 0.5, 0.3, 0.2
    else:
        split_tp1 = float(position_split[0])
        split_tp2 = float(position_split[1])
        split_tp3 = float(position_split[2])

    if status == "WIN_TP3":
        return (
            split_tp1 * pct(tp1)
            + split_tp2 * pct(tp2)
            + split_tp3 * pct(tp3)
        )
    if status == "WIN_TP2":
        return (
            split_tp1 * pct(tp1)
            + split_tp2 * pct(tp2)
            + split_tp3 * pct(current_sl)
        )
    if status == "WIN_TP1":
        return (
            split_tp1 * pct(tp1)
            + (1.0 - split_tp1) * pct(current_sl)
        )
    if status == "LOSS_SL":
        return pct(current_sl)
    # EXPIRED ou desconhecido
    return 0.0


def _calc_metrics_v9(trades: list[dict]) -> dict:
    """Métricas sobre lista de trades fechados (status != 'OPEN').

    Retorna dict com total/wins/losses/win_rate/profit_factor/expectancy/
    breakdown/avg_pnl/avg_runup/avg_drawdown. Lista vazia retorna zeros.
    """
    if not trades:
        return {
            "total": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "profit_factor": 0.0, "expectancy": 0.0,
            "breakdown": {},
            "avg_pnl": 0.0, "avg_runup": 0.0, "avg_drawdown": 0.0,
        }

    wins = [t for t in trades if (t.get("status") or "").startswith("WIN")]
    losses = [t for t in trades if t.get("status") in ("LOSS_SL", "EXPIRED")]
    total = len(trades)
    n_wins = len(wins)

    win_rate = round(n_wins / total * 100, 1)

    sum_wins = sum(max(0.0, t.get("pnl_pct") or 0.0) for t in wins)
    sum_losses_abs = sum(abs(t.get("pnl_pct") or 0.0) for t in losses)
    if sum_losses_abs > 0:
        profit_factor = round(sum_wins / sum_losses_abs, 2)
    elif sum_wins > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    pnls = [t["pnl_pct"] for t in trades if t.get("pnl_pct") is not None]
    expectancy = round(sum(pnls) / len(pnls), 2) if pnls else 0.0

    breakdown = dict(Counter((t.get("status") or "UNKNOWN") for t in trades))

    runups = [t.get("max_runup") or 0.0 for t in trades]
    drawdowns = [t.get("max_drawdown") or 0.0 for t in trades]

    return {
        "total": total,
        "wins": n_wins,
        "losses": total - n_wins,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "breakdown": breakdown,
        "avg_pnl": expectancy,
        "avg_runup": round(sum(runups) / len(runups), 2) if runups else 0.0,
        "avg_drawdown": round(sum(drawdowns) / len(drawdowns), 2) if drawdowns else 0.0,
    }


_BREAKDOWN_STATUSES_V9 = ("WIN_TP1", "WIN_TP2", "WIN_TP3", "LOSS_SL", "EXPIRED")


def _normalize_breakdown_v9(breakdown: dict) -> dict:
    """Preenche o breakdown com as 5 chaves canônicas (0 para ausentes).

    Mantém as chaves já presentes e adiciona as faltantes com valor 0.
    Usado por :meth:`TradeJournalV9.get_performance` e
    :meth:`TradeJournalV9.get_performance_by_setup` para garantir que o
    consumidor sempre possa acessar qualquer status sem KeyError.
    """
    out = {k: 0 for k in _BREAKDOWN_STATUSES_V9}
    out.update(breakdown or {})
    return out


# ---------------------------------------------------------------------------
# Fetcher síncrono interno — isolado de exchanges.py
# ---------------------------------------------------------------------------

_OKX_KLINES_URL = "https://www.okx.com/api/v5/market/candles"
_BITGET_KLINES_URL = "https://api.bitget.com/api/v2/mix/market/candles"

_BITGET_GRAN_MAP = {
    "15m": "15min", "5m": "5min", "1m": "1min",
    "1H": "1H", "4H": "4H",
}


def _fetch_klines_sync_v9(
    symbol: str,
    granularity: str = "15m",
    limit: int = 20,
) -> list[dict]:
    """Fetcher síncrono OKX → Bitget. Retorna klines em ordem crescente.

    Schema:
        [{"ts": int_ms, "open": float, "high": float, "low": float,
          "close": float, "volume": float}, ...]

    Preserva filtro OKX confirm="0" (candles não-fechadas).
    OKX retorna decrescente → reverse(). Bitget já é crescente.
    Falha silenciosa: retorna [] se ambas exchanges falharem.
    """
    # OKX — primário
    try:
        inst_id = symbol.replace("USDT", "-USDT-SWAP")
        resp = requests.get(
            _OKX_KLINES_URL,
            params={"instId": inst_id, "bar": granularity, "limit": str(limit)},
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                result = [
                    {
                        "ts": int(c[0]),
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5]),
                    }
                    for c in data
                    if len(c) <= 8 or c[8] != "0"  # filtra candles não fechadas
                ]
                result.reverse()  # OKX: decrescente → crescente
                return result
    except Exception as e:
        LOG.debug(f"[_fetch_klines_sync_v9] OKX {symbol}: {e}")

    # Bitget — fallback
    try:
        bg_gran = _BITGET_GRAN_MAP.get(granularity, "15min")
        resp = requests.get(
            _BITGET_KLINES_URL,
            params={
                "symbol": symbol,
                "productType": "USDT-FUTURES",
                "granularity": bg_gran,
                "limit": str(limit),
            },
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                return [
                    {
                        "ts": int(c[0]),
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5]),
                    }
                    for c in data
                ]
                # Bitget: já crescente → sem reverse
    except Exception as e:
        LOG.debug(f"[_fetch_klines_sync_v9] Bitget {symbol}: {e}")

    return []


# ---------------------------------------------------------------------------
# Classe principal
# ---------------------------------------------------------------------------


class TradeJournalV9:
    """Forward testing v9 — tracking de trades com saídas parciais.

    Ciclo: open_trade → check_open_trades (via risk.update_trade_state) →
    fechamento automático ao bater SL/TP3/timeout.

    Falhas silenciosas em I/O. Dedupe por (symbol, direction, OPEN).
    """

    def __init__(self, db_path: str = JOURNAL_DB_V9):
        """Inicializa journal v9. Cria DB e tabela se necessário.

        Aceita ``:memory:`` para testes. Falhas de init são silenciosas
        (warning no LOG) para não crashar o scan por problema de disco.
        """
        self.db_path = db_path
        if db_path != ":memory:":
            try:
                parent = os.path.dirname(db_path) or JOURNAL_DIR
                os.makedirs(parent, exist_ok=True)
            except Exception as e:
                LOG.warning(f"[TradeJournalV9] Falha ao criar diretório: {e}")

        try:
            conn = self._connect()
            try:
                conn.executescript(_DDL_V9)
                # Migration v9.1.1: trailing SL history (idempotente)
                for col in ("sl_moved_to_be_at", "sl_moved_to_tp1_at"):
                    try:
                        conn.execute(f"ALTER TABLE trades ADD COLUMN {col} TEXT")
                    except sqlite3.OperationalError:
                        pass  # coluna já existe
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha ao inicializar DB: {e}")

    def _connect(self) -> sqlite3.Connection:
        """Abre conexão SQLite. ``row_factory=sqlite3.Row`` para acesso por nome."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass  # :memory: não suporta WAL, ignora
        conn.row_factory = sqlite3.Row
        return conn

    # ── métodos públicos — stubs (implementados em 12c.B/C/D) ───────────────

    def open_trade(self, decision: Any, fgi: int = 0) -> Optional[str]:
        """Registra novo trade a partir de um ``SignalDecision`` com action=CALL.

        Parâmetros:
            decision: Duck-typed SignalDecision. Precisa ter atributos
                symbol, direction, action, signal_tag, confluent_setups,
                confidence, regime, trade_plan e all_setup_results.
            fgi: Fear & Greed index opcional para contexto (default 0).

        Retorno:
            trade_id no formato ``"{symbol}_{direction}_{YYYYMMDD_HHMM}"``.
            None se:
                - decision.action != "CALL"
                - decision.trade_plan é None
                - erro de I/O ao persistir

        Dedupe: se já existir trade OPEN para (symbol, direction), retorna
        o id existente sem criar nova linha. Log em nível DEBUG.
        """
        # Validações iniciais
        action = getattr(decision, "action", None)
        if action != "CALL":
            return None

        trade_plan = getattr(decision, "trade_plan", None)
        if trade_plan is None:
            return None

        symbol = getattr(decision, "symbol", None)
        direction = getattr(decision, "direction", None)
        if not symbol or direction not in ("LONG", "SHORT"):
            return None

        # Dedupe por (symbol, direction, OPEN)
        try:
            existing = self.get_open_trade(symbol, direction)
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha no dedupe de {symbol} {direction}: {e}")
            existing = None

        if existing:
            LOG.debug(
                f"[TradeJournalV9] Trade já aberto para {symbol} {direction} "
                f"— dedupe (id existente: {existing['id']})"
            )
            return existing["id"]

        # Monta trade_id e timestamp
        now = datetime.now(BRT)
        round_suffix = now.strftime("%Y%m%d_%H%M")
        trade_id = f"{symbol}_{direction}_{round_suffix}"
        ts = now.isoformat()

        # Extrai campos do trade_plan (duck-typed)
        try:
            entry_price = float(getattr(trade_plan, "entry_price"))
            sl_price = float(getattr(trade_plan, "sl_price"))
            tp1_price = float(getattr(trade_plan, "tp1_price"))
            tp2_price = float(getattr(trade_plan, "tp2_price"))
            tp3_price = float(getattr(trade_plan, "tp3_price"))
            leverage = float(getattr(trade_plan, "leverage"))
            atr_value = float(getattr(trade_plan, "atr_value", 0.0) or 0.0)
            position_split = list(getattr(trade_plan, "position_split", (0.5, 0.3, 0.2)))
            tp1_source = str(getattr(trade_plan, "tp1_source", "fallback_1r"))
            sl_source = str(getattr(trade_plan, "sl_source", "structural_swing"))
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] trade_plan inválido para {symbol}: {e}")
            return None

        # Extrai campos do decision
        signal_tag = getattr(decision, "signal_tag", None) or "unknown"
        confluent_setups = list(getattr(decision, "confluent_setups", []) or [])
        confidence = float(getattr(decision, "confidence", 0.0) or 0.0)
        regime = getattr(decision, "regime", "UNKNOWN") or "UNKNOWN"

        # Monta evidence (top-level dict para observabilidade SMC v9.1)
        try:
            all_results = list(getattr(decision, "all_setup_results", []) or [])
            setups_payload = [
                {
                    "setup_name": getattr(r, "setup_name", None),
                    "evidence": getattr(r, "evidence", {}) or {},
                }
                for r in all_results
                if getattr(r, "triggered", False)
            ]
        except Exception:
            setups_payload = []
        evidence = {
            "tp1_source": tp1_source,
            "sl_source": sl_source,
            "setups": setups_payload,
        }

        # Persiste
        try:
            conn = self._connect()
            try:
                conn.execute(
                    """INSERT INTO trades (
                           id, timestamp, symbol, direction, setup_name,
                           confluent_setups, confidence, regime_at_entry,
                           entry_price, sl_price, current_sl,
                           tp1_price, tp2_price, tp3_price,
                           leverage, atr_value, position_split,
                           tp1_hit, tp2_hit, tp3_hit, position_remaining,
                           context_fgi, context_btc_regime,
                           status, timeout_hours, evidence_json,
                           max_runup, max_drawdown
                       ) VALUES (
                           ?, ?, ?, ?, ?,
                           ?, ?, ?,
                           ?, ?, ?,
                           ?, ?, ?,
                           ?, ?, ?,
                           0, 0, 0, 1.0,
                           ?, ?,
                           'OPEN', ?, ?,
                           0, 0
                       )""",
                    (
                        trade_id, ts, symbol, direction, signal_tag,
                        json.dumps(confluent_setups, default=str),
                        confidence,
                        regime,
                        entry_price, sl_price, sl_price,  # current_sl = sl_price no open
                        tp1_price, tp2_price, tp3_price,
                        leverage, atr_value,
                        json.dumps(position_split, default=str),
                        int(fgi),
                        regime,  # context_btc_regime: usa regime do decision como proxy
                        int(TRADE_TIMEOUT_HOURS),
                        json.dumps(evidence, default=str),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha ao abrir trade {trade_id}: {e}")
            return None

        LOG.debug(
            f"[TradeJournalV9] Aberto: {trade_id} | setup={signal_tag} "
            f"| entry={entry_price:.6f} | conf={confidence:.1f}"
        )
        return trade_id

    def get_open_trade(
        self,
        symbol: str,
        direction: Optional[str] = None,
    ) -> Optional[dict]:
        """Retorna trade OPEN para (symbol, direction), ou None.

        Parâmetros:
            symbol: ex. ``"BTC"`` ou ``"BTCUSDT"``; normalizado para
                terminar em ``"USDT"`` se necessário.
            direction: ``"LONG"``/``"SHORT"`` para filtrar; None retorna
                qualquer trade OPEN do símbolo (primeiro encontrado).

        Retorno:
            dict com todos os campos da linha (via ``sqlite3.Row`` → dict),
            ou None se não houver trade OPEN ou em caso de erro de I/O.
        """
        try:
            sym = (symbol or "").upper()
            if not sym:
                return None
            if not sym.endswith("USDT"):
                sym = sym + "USDT"

            conn = self._connect()
            try:
                if direction in ("LONG", "SHORT"):
                    row = conn.execute(
                        "SELECT * FROM trades WHERE symbol=? AND direction=? "
                        "AND status='OPEN' LIMIT 1",
                        (sym, direction),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT * FROM trades WHERE symbol=? AND status='OPEN' "
                        "LIMIT 1",
                        (sym,),
                    ).fetchone()
            finally:
                conn.close()

            return dict(row) if row else None
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha em get_open_trade({symbol}): {e}")
            return None

    def check_open_trades(self, fetch_klines_fn=None) -> int:
        """Itera trades OPEN, avalia avanço/saída via klines, persiste mudanças.

        Parâmetros:
            fetch_klines_fn: callable opcional ``(symbol) -> list[dict]`` que
                retorna klines no formato v9 (``[{"ts": int_ms, "open":...}]``).
                Se None, usa :func:`_fetch_klines_sync_v9` internamente. Injeção
                permite testes sem rede.

        Retorno:
            Número de trades que chegaram a status final nesta verificação
            (``LOSS_SL``/``WIN_TP1``/``WIN_TP2``/``WIN_TP3``/``EXPIRED``).
            Updates intermediários (TP1 hit mas trade ainda OPEN) não contam.
        """
        try:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM trades WHERE status='OPEN'"
                ).fetchall()
            finally:
                conn.close()
            open_trades = [dict(r) for r in rows]
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha ao buscar trades abertos: {e}")
            return 0

        if not open_trades:
            return 0

        closed = 0
        for row in open_trades:
            try:
                closed += self._check_one_trade(row, fetch_klines_fn)
            except Exception as e:
                LOG.warning(
                    f"[TradeJournalV9] Erro ao verificar trade "
                    f"{row.get('id', '?')}: {e}"
                )

        if closed > 0:
            LOG.info(f"[TradeJournalV9] check_open_trades: {closed} trade(s) fechado(s)")
        return closed

    def _check_one_trade(self, row: dict, fetch_klines_fn) -> int:
        """Avalia um único trade contra klines recentes. Persiste mudanças.

        Pipeline:
            1. Timeout check (antes de buscar klines — evita I/O desnecessário).
            2. Fetch de klines 15m (via callable injetado ou fetcher interno).
            3. Filtra só candles novos (ts > timestamp de abertura em ms).
            4. Reconstrói TradePlan + TradeState via helpers.
            5. Loop cronológico: atualiza runup/drawdown, chama update_trade_state,
               interrompe ao primeiro status final (decisão: max_runup/drawdown
               inclui o candle da saída).
            6. Persiste: _close_trade se status final; _update_partial se só
               mudou SL/tp_hit/position_remaining; no-op se nada mudou.

        Retorna 1 se chegou a status final, 0 caso contrário.
        """
        now = datetime.now(BRT)
        trade_id = row["id"]
        symbol = row["symbol"]
        direction = row["direction"]
        entry = float(row["entry_price"])

        # --- 1. Timeout check ------------------------------------------------
        try:
            ts_open = datetime.fromisoformat(row["timestamp"])
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] timestamp inválido em {trade_id}: {e}")
            return 0

        hours_open = (now - ts_open).total_seconds() / 3600.0
        timeout_h = float(row.get("timeout_hours") or TRADE_TIMEOUT_HOURS)
        if hours_open >= timeout_h:
            self._close_trade(
                trade_id=trade_id,
                status="EXPIRED",
                exit_price=entry,
                exit_time=now.isoformat(),
                pnl_pct=0.0,
            )
            LOG.info(
                f"[TradeJournalV9] EXPIRED: {trade_id} "
                f"({hours_open:.1f}h > {timeout_h:.0f}h)"
            )
            return 1

        # --- 2. Busca klines -------------------------------------------------
        try:
            if fetch_klines_fn is not None:
                klines = fetch_klines_fn(symbol)
            else:
                klines = _fetch_klines_sync_v9(symbol, granularity="15m", limit=20)
        except Exception as e:
            LOG.debug(f"[TradeJournalV9] Falha ao buscar klines para {symbol}: {e}")
            return 0

        if not klines:
            return 0

        # --- 3. Filtra só candles novos -------------------------------------
        # timestamp ISO do trade em ms (para comparar com ts de klines)
        try:
            open_ts_ms = int(ts_open.timestamp() * 1000)
        except Exception:
            open_ts_ms = 0

        new_klines = [
            k for k in klines
            if isinstance(k, dict) and int(k.get("ts", 0)) > open_ts_ms
        ]
        if not new_klines:
            return 0

        # Garante ordem crescente de ts (candles chegam crescentes do fetcher,
        # mas não custa garantir)
        new_klines.sort(key=lambda k: int(k["ts"]))

        # --- 4. Reconstrói TradePlan + TradeState ---------------------------
        try:
            plan = _plan_from_row(row)
            state = _state_from_row(row, plan)
        except Exception as e:
            LOG.warning(
                f"[TradeJournalV9] Falha ao reconstruir plan/state "
                f"de {trade_id}: {e}"
            )
            return 0

        # Acumuladores de extremos
        max_runup = float(row.get("max_runup") or 0.0)
        max_drawdown = float(row.get("max_drawdown") or 0.0)
        initial_sl = float(row.get("current_sl") or row.get("sl_price") or 0.0)
        initial_tp1 = bool(row.get("tp1_hit"))
        initial_tp2 = bool(row.get("tp2_hit"))
        initial_tp3 = bool(row.get("tp3_hit"))
        initial_pos = float(row.get("position_remaining") or 1.0)
        initial_runup = float(row.get("max_runup") or 0.0)
        initial_drawdown = float(row.get("max_drawdown") or 0.0)

        # --- 5. Loop cronológico --------------------------------------------
        for candle in new_klines:
            try:
                hi = float(candle["high"])
                lo = float(candle["low"])
            except Exception:
                continue

            if entry > 0:
                if direction == "LONG":
                    runup = (hi - entry) / entry * 100.0
                    drawdown = (entry - lo) / entry * 100.0
                else:  # SHORT
                    runup = (entry - lo) / entry * 100.0
                    drawdown = (hi - entry) / entry * 100.0
                if runup > max_runup:
                    max_runup = runup
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

            state = update_trade_state(state, candle)

            if state.status != "OPEN":
                break  # não processa candles após saída

        # --- 6. Trailing SL — registra timestamps idempotentes ---------------
        # SL→BE acontece quando TP1 hit (current_sl = entry).
        # SL→TP1 acontece quando TP2 hit (current_sl = tp1_price).
        # Só registra transições NESTA rodada (initial_* False → state.* True).
        # Idempotência adicional via guard SQL `WHERE col IS NULL`.
        if state.tp1_hit and not initial_tp1:
            self._record_sl_move(trade_id, "sl_moved_to_be_at", now.isoformat())
        if state.tp2_hit and not initial_tp2:
            self._record_sl_move(trade_id, "sl_moved_to_tp1_at", now.isoformat())

        # --- 7. Persistência -------------------------------------------------
        if state.status != "OPEN":
            # Status final: calcula exit_price + pnl
            exit_price = self._exit_price_for_status(state, plan, row)
            pnl_pct = _calc_partial_pnl(
                status=state.status,
                direction=direction,
                entry=entry,
                tp1=float(plan.tp1_price),
                tp2=float(plan.tp2_price),
                tp3=float(plan.tp3_price),
                current_sl=float(state.current_sl),
                position_split=plan.position_split,
            )
            self._close_trade(
                trade_id=trade_id,
                status=state.status,
                exit_price=exit_price,
                exit_time=now.isoformat(),
                pnl_pct=pnl_pct,
                current_sl=state.current_sl,
                tp1_hit=state.tp1_hit,
                tp2_hit=state.tp2_hit,
                tp3_hit=state.tp3_hit,
                position_remaining=state.position_remaining,
                max_runup=max_runup,
                max_drawdown=max_drawdown,
            )
            LOG.info(
                f"[TradeJournalV9] {state.status}: {trade_id} "
                f"| pnl={pnl_pct:+.2f}% | exit={exit_price:.6f}"
            )
            return 1

        # Trade continua OPEN — atualiza se algo mudou
        changed = (
            state.current_sl != initial_sl
            or state.tp1_hit != initial_tp1
            or state.tp2_hit != initial_tp2
            or state.tp3_hit != initial_tp3
            or state.position_remaining != initial_pos
            or max_runup != initial_runup
            or max_drawdown != initial_drawdown
        )
        if changed:
            self._update_partial(
                trade_id=trade_id,
                current_sl=state.current_sl,
                tp1_hit=state.tp1_hit,
                tp2_hit=state.tp2_hit,
                tp3_hit=state.tp3_hit,
                position_remaining=state.position_remaining,
                max_runup=max_runup,
                max_drawdown=max_drawdown,
            )

        return 0

    @staticmethod
    def _exit_price_for_status(state: TradeState, plan: TradePlan, row: dict) -> float:
        """Retorna o exit_price conforme o status final do trade.

        Convenção v9 (simplificada — a matemática ponderada de pnl está em
        ``_calc_partial_pnl``):
            - WIN_TP3 → tp3_price
            - WIN_TP2 → tp2_price
            - WIN_TP1 → tp1_price
            - LOSS_SL → current_sl no momento (pode ser sl_price original,
                        entry após TP1, ou tp1_price após TP2)
            - EXPIRED → entry_price (0% pnl)
        """
        status = state.status
        if status == "WIN_TP3":
            return float(plan.tp3_price)
        if status == "WIN_TP2":
            return float(plan.tp2_price)
        if status == "WIN_TP1":
            return float(plan.tp1_price)
        if status == "LOSS_SL":
            return float(state.current_sl)
        # EXPIRED ou desconhecido — devolve entry
        return float(row.get("entry_price") or 0.0)

    def get_performance(
        self,
        setup_name: Optional[str] = None,
        direction: Optional[str] = None,
        days: int = 30,
    ) -> dict:
        """Calcula métricas sobre trades fechados nos últimos ``days`` dias.

        Parâmetros:
            setup_name: filtro opcional — match exato no campo setup_name.
                Note: se um trade tem setup_name="cont_pull+rev_zone"
                (confluência), filtrar por "cont_pull" NÃO o captura aqui.
                Para métricas por setup individual use
                :meth:`get_performance_by_setup`.
            direction: filtro opcional ("LONG" ou "SHORT").
            days: janela em dias (default 30). Usa ``timestamp`` do trade,
                não ``exit_time``.

        Retorno:
            dict com chaves documentadas em ``_calc_metrics_v9`` (total,
            wins, losses, win_rate, profit_factor, expectancy, breakdown,
            avg_pnl, avg_runup, avg_drawdown). Dict vazio de métricas
            (total=0, tudo zerado) se não houver trades ou em erro de I/O.
        """
        try:
            # Janela temporal: timestamp >= now - days
            cutoff = (datetime.now(BRT) - timedelta(days=int(days))).isoformat()

            # Monta query dinamicamente com filtros opcionais
            where_clauses = ["status != 'OPEN'", "timestamp >= ?"]
            params: list = [cutoff]

            if setup_name:
                where_clauses.append("setup_name = ?")
                params.append(setup_name)
            if direction in ("LONG", "SHORT"):
                where_clauses.append("direction = ?")
                params.append(direction)

            where_sql = " AND ".join(where_clauses)

            conn = self._connect()
            try:
                rows = conn.execute(
                    f"SELECT * FROM trades WHERE {where_sql}",
                    tuple(params),
                ).fetchall()
            finally:
                conn.close()

            trades = [dict(r) for r in rows]
            metrics = _calc_metrics_v9(trades)
            metrics["breakdown"] = _normalize_breakdown_v9(metrics.get("breakdown") or {})
            return metrics
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha em get_performance: {e}")
            metrics = _calc_metrics_v9([])
            metrics["breakdown"] = _normalize_breakdown_v9(metrics.get("breakdown") or {})
            return metrics

    def get_performance_by_setup(self, days: int = 30) -> dict:
        """Retorna métricas por setup individual, com split de confluências.

        Para confluências (setup_name="cont_pull+rev_zone"), o trade é
        contabilizado nas métricas de "cont_pull" E de "rev_zone"
        individualmente. Essa divisão responde "qual setup traz mais
        sinal?" quando a maioria dos trades vem de confluências.

        Parâmetros:
            days: janela em dias (default 30).

        Retorno:
            dict ``{setup_name: metrics_dict}`` onde ``metrics_dict``
            tem a estrutura de ``_calc_metrics_v9``. Retorna dict vazio
            se não houver trades fechados ou em erro de I/O.
        """
        try:
            cutoff = (datetime.now(BRT) - timedelta(days=int(days))).isoformat()

            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM trades "
                    "WHERE status != 'OPEN' AND timestamp >= ?",
                    (cutoff,),
                ).fetchall()
            finally:
                conn.close()

            all_trades = [dict(r) for r in rows]
            if not all_trades:
                return {}

            # Agrupa por setup individual (split do "+")
            by_setup: dict[str, list] = {}
            for trade in all_trades:
                setup_str = trade.get("setup_name") or ""
                if not setup_str:
                    continue
                individual_setups = [s.strip() for s in setup_str.split("+") if s.strip()]
                for s in individual_setups:
                    by_setup.setdefault(s, []).append(trade)

            # Calcula métricas para cada setup
            result = {}
            for s, trades in by_setup.items():
                m = _calc_metrics_v9(trades)
                m["breakdown"] = _normalize_breakdown_v9(m.get("breakdown") or {})
                result[s] = m
            return result
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha em get_performance_by_setup: {e}")
            return {}

    # ── helpers privados de UPDATE (usados em 12c.C) ────────────────────────

    def _close_trade(
        self,
        trade_id: str,
        status: str,
        exit_price: float,
        exit_time: str,
        pnl_pct: float,
        current_sl: Optional[float] = None,
        tp1_hit: Optional[bool] = None,
        tp2_hit: Optional[bool] = None,
        tp3_hit: Optional[bool] = None,
        position_remaining: Optional[float] = None,
        max_runup: Optional[float] = None,
        max_drawdown: Optional[float] = None,
    ) -> bool:
        """Fecha um trade atualizando status + campos de saída.

        Campos opcionais (current_sl, tp*_hit, position_remaining,
        max_runup, max_drawdown) são atualizados apenas se fornecidos —
        build dinâmico do UPDATE evita sobrescrever com None.

        Retorna True se UPDATE executou, False se falhou.
        """
        fields = [
            ("status", status),
            ("exit_price", float(exit_price)),
            ("exit_time", exit_time),
            ("pnl_pct", round(float(pnl_pct), 4)),
        ]
        if current_sl is not None:
            fields.append(("current_sl", float(current_sl)))
        if tp1_hit is not None:
            fields.append(("tp1_hit", 1 if tp1_hit else 0))
        if tp2_hit is not None:
            fields.append(("tp2_hit", 1 if tp2_hit else 0))
        if tp3_hit is not None:
            fields.append(("tp3_hit", 1 if tp3_hit else 0))
        if position_remaining is not None:
            fields.append(("position_remaining", float(position_remaining)))
        if max_runup is not None:
            fields.append(("max_runup", round(float(max_runup), 4)))
        if max_drawdown is not None:
            fields.append(("max_drawdown", round(float(max_drawdown), 4)))

        set_clause = ", ".join(f"{name}=?" for name, _ in fields)
        params = tuple(value for _, value in fields) + (trade_id,)

        try:
            conn = self._connect()
            try:
                conn.execute(
                    f"UPDATE trades SET {set_clause} WHERE id=?",
                    params,
                )
                conn.commit()
            finally:
                conn.close()
            return True
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha em _close_trade({trade_id}): {e}")
            return False

    def _update_partial(
        self,
        trade_id: str,
        current_sl: float,
        tp1_hit: bool,
        tp2_hit: bool,
        tp3_hit: bool,
        position_remaining: float,
        max_runup: float,
        max_drawdown: float,
    ) -> bool:
        """Atualiza campos intermediários (trade continua OPEN).

        Usado após TP1/TP2 hit quando o trade ainda não fechou. Não muda
        status nem grava exit_price/exit_time/pnl_pct.

        Retorna True se UPDATE executou, False se falhou.
        """
        try:
            conn = self._connect()
            try:
                conn.execute(
                    """UPDATE trades
                       SET current_sl=?, tp1_hit=?, tp2_hit=?, tp3_hit=?,
                           position_remaining=?, max_runup=?, max_drawdown=?
                       WHERE id=?""",
                    (
                        float(current_sl),
                        1 if tp1_hit else 0,
                        1 if tp2_hit else 0,
                        1 if tp3_hit else 0,
                        float(position_remaining),
                        round(float(max_runup), 4),
                        round(float(max_drawdown), 4),
                        trade_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            return True
        except Exception as e:
            LOG.warning(f"[TradeJournalV9] Falha em _update_partial({trade_id}): {e}")
            return False

    def _record_sl_move(self, trade_id: str, column: str, ts_iso: str) -> bool:
        """Persiste o timestamp de uma transição do trailing SL.

        ``column`` é ``"sl_moved_to_be_at"`` (após TP1) ou
        ``"sl_moved_to_tp1_at"`` (após TP2). O guard ``WHERE col IS NULL``
        garante que double-call (race entre rodadas) não sobrescreve o
        timestamp original.

        Falhas silenciosas — warning no LOG.
        """
        if column not in ("sl_moved_to_be_at", "sl_moved_to_tp1_at"):
            return False
        try:
            conn = self._connect()
            try:
                conn.execute(
                    f"UPDATE trades SET {column}=? "
                    f"WHERE id=? AND {column} IS NULL",
                    (ts_iso, trade_id),
                )
                conn.commit()
            finally:
                conn.close()
            return True
        except Exception as e:
            LOG.warning(
                f"[TradeJournalV9] Falha em _record_sl_move({trade_id}, {column}): {e}"
            )
            return False
