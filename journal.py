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
from datetime import datetime
from typing import Any, Optional

import requests

from config import BRT, JOURNAL_DB_V9, JOURNAL_DIR, TRADE_TIMEOUT_HOURS
from risk import TradePlan, TradeState, update_trade_state  # noqa: F401 (usados em 12c.C)

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
        """Registra novo trade a partir de SignalDecision. [12c.B]"""
        raise NotImplementedError("open_trade: será implementado em 12c.B")

    def get_open_trade(
        self,
        symbol: str,
        direction: Optional[str] = None,
    ) -> Optional[dict]:
        """Retorna trade OPEN para (symbol, direction) ou None. [12c.B]"""
        raise NotImplementedError("get_open_trade: será implementado em 12c.B")

    def check_open_trades(self, fetch_klines_fn=None) -> int:
        """Avalia trades OPEN via klines, atualiza estado. [12c.C]"""
        raise NotImplementedError("check_open_trades: será implementado em 12c.C")

    def get_performance(
        self,
        setup_name: Optional[str] = None,
        direction: Optional[str] = None,
        days: int = 30,
    ) -> dict:
        """Métricas sobre trades fechados. [12c.D]"""
        raise NotImplementedError("get_performance: será implementado em 12c.D")

    def get_performance_by_setup(self, days: int = 30) -> dict:
        """Performance por setup individual (splits confluências). [12c.D]"""
        raise NotImplementedError("get_performance_by_setup: será implementado em 12c.D")
