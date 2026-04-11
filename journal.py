#!/usr/bin/env python3
"""
journal.py — Forward Testing Automatizado (Camada 2)
=====================================================
[v6.6.5] Módulo de observabilidade — registra CALLs e QUASEs como entradas
de forward test, monitora automaticamente se SL/TP foram atingidos via klines,
e calcula métricas de performance.

Completamente independente do logger.py.

Ciclo de vida de um trade:
  Fase A — Capture: open_trade() registra CALL ou QUASE com status OPEN
  Fase B — Track:   check_open_trades() avalia SL/TP via klines a cada rodada
  Fase C — Close:   automático quando condição de saída é atingida
"""
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

# ── Configuração ──────────────────────────────────────────────────────────────
BRT         = timezone(timedelta(hours=-3))
JOURNAL_DB  = os.path.expanduser("~/Setup_Atirador/journal/atirador_journal.db")
JOURNAL_DIR = os.path.dirname(JOURNAL_DB)

LOG = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id               TEXT PRIMARY KEY,
    timestamp        TEXT NOT NULL,
    symbol           TEXT NOT NULL,
    direction        TEXT NOT NULL,
    type             TEXT NOT NULL,
    is_hypothetical  INTEGER NOT NULL DEFAULT 0,
    score            INTEGER,
    entry_price      REAL,
    sl_price         REAL,
    tp1_price        REAL,
    tp2_price        REAL,
    tp3_price        REAL,
    context_fgi      INTEGER,
    context_btc      TEXT,
    status           TEXT NOT NULL DEFAULT 'OPEN',
    exit_price       REAL,
    exit_time        TEXT,
    pnl_pct          REAL,
    max_runup        REAL,
    max_drawdown     REAL,
    timeout_hours    INTEGER NOT NULL DEFAULT 48,
    pillars_json     TEXT,
    kline_venue      TEXT,
    tv_venue         TEXT,
    venue_quality    TEXT
);

CREATE INDEX IF NOT EXISTS idx_status    ON trades(status);
CREATE INDEX IF NOT EXISTS idx_symbol    ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_type      ON trades(type);
"""


def _now_brt() -> datetime:
    return datetime.now(BRT)


def _now_iso() -> str:
    return _now_brt().isoformat()


class TradeJournal:
    """
    [v6.6.5] Forward testing automatizado — Camada 2 da arquitetura de observabilidade.

    Ciclo de vida de um trade:
      Fase A — Capture: open_trade() registra CALL ou QUASE com status OPEN
      Fase B — Track:   check_open_trades() avalia SL/TP via klines a cada rodada
      Fase C — Close:   automático pelo Tracker quando condição de saída é atingida

    Separação CALL vs QUASE:
      - CALLs: type="CALL", is_hypothetical=0 — métricas operacionais reais
      - QUASEs: type="QUASE", is_hypothetical=1 — análise de calibração de threshold
        SL e TPs das QUASEs são calculados hipoteticamente com a mesma lógica das CALLs,
        capturados antes de descartar o token. Não foram emitidos como sinais operáveis.

    Falhas de I/O são silenciosas — warning no LOG, nunca interrompem o scan.
    check_open_trades() roda ANTES do scan principal em cada rodada.
    """

    def __init__(self, db_path: str = JOURNAL_DB):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        try:
            conn = self._connect()
            conn.executescript(_DDL)
            # [v6.6.6] Migration: add venue columns to existing tables
            for _sql in [
                "ALTER TABLE trades ADD COLUMN kline_venue TEXT",
                "ALTER TABLE trades ADD COLUMN tv_venue TEXT",
                "ALTER TABLE trades ADD COLUMN venue_quality TEXT",
            ]:
                try:
                    conn.execute(_sql)
                except sqlite3.OperationalError:
                    pass  # column already exists
            conn.commit()
            conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao inicializar DB: {e}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def open_trade(self, symbol: str, direction: str, type: str,
                   score: int, entry_price: float,
                   sl_price: float, tp1: float, tp2: float, tp3: float,
                   fgi: int, btc_4h: str, pillars_dict: dict,
                   kline_venue: str = None, tv_venue: str = None,
                   venue_quality: str = None) -> str:
        """
        Registra novo trade com status OPEN.
        type: "CALL" | "QUASE"
        is_hypothetical definido automaticamente: QUASE → 1, CALL → 0
        kline_venue, tv_venue, venue_quality: observabilidade de venue [v6.6.6]
        Retorna o id gerado.
        """
        now   = _now_brt()
        ts    = now.isoformat()
        rid   = now.strftime("%Y%m%d_%H%M")
        trade_id = f"{symbol}_{direction}_{rid}"
        is_hyp   = 1 if type == "QUASE" else 0

        try:
            conn = self._connect()
            # Se já existe um trade aberto para este símbolo+direção, não abre outro
            existing = conn.execute(
                "SELECT id FROM trades WHERE symbol=? AND direction=? AND status='OPEN'",
                (symbol, direction)
            ).fetchone()
            if existing:
                LOG.debug(f"[TradeJournal] Trade já aberto para {symbol} {direction} — ignorando novo open")
                conn.close()
                return existing["id"]

            conn.execute(
                """INSERT INTO trades
                   (id, timestamp, symbol, direction, type, is_hypothetical,
                    score, entry_price, sl_price, tp1_price, tp2_price, tp3_price,
                    context_fgi, context_btc, status, pillars_json,
                    kline_venue, tv_venue, venue_quality)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?,?)""",
                (
                    trade_id, ts, symbol, direction, type, is_hyp,
                    score, entry_price, sl_price, tp1, tp2, tp3,
                    fgi, btc_4h,
                    json.dumps(pillars_dict, ensure_ascii=False),
                    kline_venue, tv_venue, venue_quality,
                )
            )
            conn.commit()
            conn.close()
            LOG.debug(f"[TradeJournal] Aberto: {trade_id} | type={type} | entry={entry_price:.4f}")
            return trade_id
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao abrir trade {symbol}: {e}")
            return trade_id

    def check_open_trades(self, fetch_klines_fn=None) -> int:
        """
        Avalia todos os trades com status OPEN.
        Chamado no início de cada rodada, antes do scan principal.
        fetch_klines_fn: função sync opcional que recebe (symbol) e retorna lista de klines.
                         Se None, usa fetcher interno via requests.
        Retorna número de trades fechados nesta verificação.

        Lógica de avaliação por candle (ordem cronológica):
          LONG: low <= sl_price → LOSS_SL | high >= tp1/2/3 → WIN_TP1/2/3
          SHORT: high >= sl_price → LOSS_SL | low <= tp1/2/3 → WIN_TP1/2/3

        Atualiza max_runup e max_drawdown acumulados a cada check.
        Se aberto há mais de timeout_hours → EXPIRED no preço atual.
        """
        try:
            conn = self._connect()
            rows = conn.execute(
                "SELECT * FROM trades WHERE status='OPEN'"
            ).fetchall()
            conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao buscar trades abertos: {e}")
            return 0

        if not rows:
            return 0

        closed = 0
        for row in rows:
            try:
                closed += self._check_one_trade(dict(row), fetch_klines_fn)
            except Exception as e:
                LOG.warning(f"[TradeJournal] Erro ao verificar trade {row['id']}: {e}")

        if closed:
            LOG.info(f"[TradeJournal] check_open_trades: {closed} trade(s) fechado(s)")
        return closed

    def _check_one_trade(self, trade: dict, fetch_klines_fn) -> int:
        """Verifica um trade individual. Retorna 1 se fechado, 0 caso contrário."""
        now      = _now_brt()
        ts_open  = datetime.fromisoformat(trade["timestamp"])
        hours_open = (now - ts_open).total_seconds() / 3600

        # Timeout check
        if hours_open >= trade["timeout_hours"]:
            self._close_trade(
                trade_id   = trade["id"],
                status     = "EXPIRED",
                exit_price = trade["entry_price"],
                exit_time  = now.isoformat(),
                pnl_pct    = 0.0,
            )
            LOG.info(f"[TradeJournal] EXPIRED: {trade['id']} ({hours_open:.1f}h aberto)")
            return 1

        # Busca klines para avaliação
        symbol = trade["symbol"]
        try:
            if fetch_klines_fn is not None:
                klines = fetch_klines_fn(symbol)
            else:
                klines = _fetch_klines_sync(symbol)
        except Exception as e:
            LOG.debug(f"[TradeJournal] Falha ao buscar klines para {symbol}: {e}")
            return 0

        if not klines:
            return 0

        direction = trade["direction"]
        sl  = trade["sl_price"]
        tp1 = trade["tp1_price"]
        tp2 = trade["tp2_price"]
        tp3 = trade["tp3_price"]
        entry = trade["entry_price"]

        # Acumula max_runup e max_drawdown
        max_runup    = trade.get("max_runup") or 0.0
        max_drawdown = trade.get("max_drawdown") or 0.0

        final_status = None
        exit_price   = None

        for candle in klines:
            try:
                high = float(candle[2]) if isinstance(candle, (list, tuple)) else float(candle.get("high", 0))
                low  = float(candle[3]) if isinstance(candle, (list, tuple)) else float(candle.get("low", 0))
            except (IndexError, TypeError, ValueError):
                continue

            if entry > 0:
                if direction == "LONG":
                    runup    = (high  - entry) / entry * 100
                    drawdown = (entry - low)   / entry * 100
                else:
                    runup    = (entry - low)   / entry * 100
                    drawdown = (high  - entry) / entry * 100
                max_runup    = max(max_runup,    runup)
                max_drawdown = max(max_drawdown, drawdown)

            if direction == "LONG":
                if sl and low <= sl:
                    final_status = "LOSS_SL"; exit_price = sl; break
                if tp3 and high >= tp3:
                    final_status = "WIN_TP3"; exit_price = tp3; break
                if tp2 and high >= tp2:
                    final_status = "WIN_TP2"; exit_price = tp2; break
                if tp1 and high >= tp1:
                    final_status = "WIN_TP1"; exit_price = tp1; break
            else:  # SHORT
                if sl and high >= sl:
                    final_status = "LOSS_SL"; exit_price = sl; break
                if tp3 and low <= tp3:
                    final_status = "WIN_TP3"; exit_price = tp3; break
                if tp2 and low <= tp2:
                    final_status = "WIN_TP2"; exit_price = tp2; break
                if tp1 and low <= tp1:
                    final_status = "WIN_TP1"; exit_price = tp1; break

        # Atualiza max_runup e max_drawdown mesmo sem fechar
        self._update_extremes(trade["id"], max_runup, max_drawdown)

        if final_status and exit_price and entry > 0:
            if direction == "LONG":
                pnl_pct = (exit_price - entry) / entry * 100
            else:
                pnl_pct = (entry - exit_price) / entry * 100
            self._close_trade(
                trade_id   = trade["id"],
                status     = final_status,
                exit_price = exit_price,
                exit_time  = now.isoformat(),
                pnl_pct    = round(pnl_pct, 4),
            )
            LOG.info(f"[TradeJournal] {final_status}: {trade['id']} | pnl={pnl_pct:+.2f}%")
            return 1

        return 0

    def _close_trade(self, trade_id: str, status: str, exit_price: float,
                     exit_time: str, pnl_pct: float):
        try:
            conn = self._connect()
            conn.execute(
                """UPDATE trades SET status=?, exit_price=?, exit_time=?, pnl_pct=?
                   WHERE id=?""",
                (status, exit_price, exit_time, pnl_pct, trade_id)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao fechar trade {trade_id}: {e}")

    def _update_extremes(self, trade_id: str, max_runup: float, max_drawdown: float):
        try:
            conn = self._connect()
            conn.execute(
                "UPDATE trades SET max_runup=?, max_drawdown=? WHERE id=?",
                (round(max_runup, 4), round(max_drawdown, 4), trade_id)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            LOG.debug(f"[TradeJournal] Falha ao atualizar extremos {trade_id}: {e}")

    def get_performance(self, direction: str = None, days: int = 30) -> dict:
        """
        Calcula métricas de performance apenas sobre CALLs (is_hypothetical=0).
        Retorna: win_rate, profit_factor, expectancy, breakdown por status.
        direction: "LONG" | "SHORT" | None (ambos)
        """
        try:
            conn  = self._connect()
            since = (datetime.now(BRT) - timedelta(days=days)).isoformat()
            q     = "SELECT * FROM trades WHERE is_hypothetical=0 AND timestamp >= ? AND status != 'OPEN'"
            args  = [since]
            if direction:
                q += " AND direction=?"
                args.append(direction)
            rows = conn.execute(q, args).fetchall()
            conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao calcular performance: {e}")
            return {}

        trades = [dict(r) for r in rows]
        return _calc_metrics(trades)

    def get_quase_calibration(self, days: int = 30) -> dict:
        """
        Calcula win rate das QUASEs (is_hypothetical=1) para calibração de threshold.
        Compara com win rate das CALLs do mesmo período.
        """
        try:
            conn  = self._connect()
            since = (datetime.now(BRT) - timedelta(days=days)).isoformat()
            calls  = conn.execute(
                "SELECT * FROM trades WHERE is_hypothetical=0 AND timestamp >= ? AND status != 'OPEN'",
                [since]
            ).fetchall()
            quases = conn.execute(
                "SELECT * FROM trades WHERE is_hypothetical=1 AND timestamp >= ? AND status != 'OPEN'",
                [since]
            ).fetchall()
            # Gap médio
            gaps = conn.execute(
                """SELECT AVG(CAST(threshold - score AS REAL)) as avg_gap
                   FROM trades WHERE is_hypothetical=1 AND timestamp >= ?""",
                [since]
            ).fetchone()
            conn.close()
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao calcular calibração: {e}")
            return {}

        calls_m  = _calc_metrics([dict(r) for r in calls])
        quases_m = _calc_metrics([dict(r) for r in quases])
        avg_gap  = gaps["avg_gap"] if gaps and gaps["avg_gap"] else 0.0

        return {
            "calls"   : calls_m,
            "quases"  : quases_m,
            "avg_gap" : round(avg_gap, 1) if avg_gap else 0.0,
        }

    def get_open_trade(self, symbol: str) -> Optional[dict]:
        """Retorna trade aberto para o símbolo, ou None se não houver."""
        try:
            sym = symbol.upper()
            if not sym.endswith("USDT"):
                sym += "USDT"
            conn = self._connect()
            row  = conn.execute(
                "SELECT * FROM trades WHERE symbol=? AND status='OPEN' LIMIT 1",
                (sym,)
            ).fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            LOG.warning(f"[TradeJournal] Falha ao buscar trade aberto {symbol}: {e}")
            return None


# ── Utilitários ───────────────────────────────────────────────────────────────

def _calc_metrics(trades: list) -> dict:
    """Calcula métricas a partir de uma lista de trades fechados."""
    if not trades:
        return {"total": 0, "closed": 0, "win_rate": 0.0, "profit_factor": 0.0,
                "expectancy": 0.0, "breakdown": {}}

    wins  = [t for t in trades if t["status"] and t["status"].startswith("WIN")]
    losses= [t for t in trades if t["status"] in ("LOSS_SL", "EXPIRED")]
    total = len(trades)
    n_wins = len(wins)

    win_rate = round(n_wins / total * 100, 1) if total else 0.0

    # Profit factor = soma dos ganhos / soma das perdas absolutas
    sum_wins   = sum(abs(t["pnl_pct"] or 0) for t in wins)
    sum_losses = sum(abs(t["pnl_pct"] or 0) for t in losses)
    profit_factor = round(sum_wins / sum_losses, 2) if sum_losses > 0 else (float("inf") if sum_wins > 0 else 0.0)

    # Expectancy = média de pnl_pct
    pnls = [t["pnl_pct"] for t in trades if t["pnl_pct"] is not None]
    expectancy = round(sum(pnls) / len(pnls), 2) if pnls else 0.0

    # Breakdown por status
    breakdown = {}
    for t in trades:
        s = t["status"] or "UNKNOWN"
        breakdown[s] = breakdown.get(s, 0) + 1

    return {
        "total"        : total,
        "closed"       : total,
        "wins"         : n_wins,
        "losses"       : total - n_wins,
        "win_rate"     : win_rate,
        "profit_factor": profit_factor,
        "expectancy"   : expectancy,
        "breakdown"    : breakdown,
    }


def _fetch_klines_sync(symbol: str, granularity: str = "15m", limit: int = 60) -> list:
    """Fetcher interno de klines — primário OKX, fallback Bitget.

    Retorna lista de klines no formato [ts, open, high, low, close, vol].
    OKX retorna decrescente → revertido para crescente.
    Bitget retorna crescente → sem reverse necessário.
    """
    # OKX — primário
    try:
        okx_bar = {"15m": "15m", "1H": "1H", "4H": "4H"}.get(granularity, "15m")
        url  = "https://www.okx.com/api/v5/market/candles"
        resp = _requests.get(url, params={
            "instId": symbol.replace("USDT", "-USDT-SWAP"),
            "bar"   : okx_bar,
            "limit" : str(limit),
        }, timeout=8)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                result = [[c[0], c[1], c[2], c[3], c[4], c[5]] for c in data]
                result.reverse()  # OKX: decrescente → crescente
                return result
    except Exception:
        pass

    # Bitget — fallback
    gran_map = {"15m": "15min", "1H": "1H", "4H": "4H"}
    bg_gran  = gran_map.get(granularity, "15min")
    try:
        url  = f"https://api.bitget.com/api/v2/mix/market/candles"
        resp = _requests.get(url, params={
            "symbol"      : symbol,
            "productType" : "USDT-FUTURES",
            "granularity" : bg_gran,
            "limit"       : str(limit),
        }, timeout=8)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                # Bitget: já crescente → sem reverse
                return data
    except Exception:
        pass

    return []
