# backtest/candle_store.py — Setup Atirador v9
# Armazenamento local de candles OHLCV para backtest offline.
# Grava/le em backtest/candles_v9.db (SQLite). NAO toca produção em runtime.
# stdlib puro (sqlite3) — sem dependencia de produção neste arquivo.

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "candles_v9.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    symbol     TEXT    NOT NULL,
    timeframe  TEXT    NOT NULL,
    ts         INTEGER NOT NULL,   -- epoch-ms, hora de ABERTURA da vela (nativo OKX)
    open       REAL    NOT NULL,
    high       REAL    NOT NULL,
    low        REAL    NOT NULL,
    close      REAL    NOT NULL,
    volume     REAL    NOT NULL,
    PRIMARY KEY (symbol, timeframe, ts)
);
"""


def connect(db_path: "Path | str" = DB_PATH) -> sqlite3.Connection:
    """Abre conexao e garante o schema. Cria o .db se nao existir."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript(_SCHEMA)
    return conn


def upsert_candles(conn: sqlite3.Connection, symbol: str, timeframe: str,
                   klines: "list[dict]") -> int:
    """Grava candles de forma idempotente. `klines` no formato de producao
    ({ts, open, high, low, close, volume}). Re-rodar nao duplica.
    Retorna o numero de candles processados."""
    rows = [
        (symbol, timeframe, int(k["ts"]),
         float(k["open"]), float(k["high"]), float(k["low"]),
         float(k["close"]), float(k["volume"]))
        for k in klines
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO candles "
        "(symbol, timeframe, ts, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def read_candles(conn: sqlite3.Connection, symbol: str, timeframe: str,
                 start_ms: "int | None" = None,
                 end_ms: "int | None" = None) -> "list[dict]":
    """Le candles em ordem ASCENDENTE no formato de producao
    ({ts, open, high, low, close, volume}), pronto pra klines_to_dataframe.
    `end_ms` e INCLUSIVO — passe o cursor do replay aqui pra respeitar no-lookahead."""
    q = ("SELECT ts, open, high, low, close, volume FROM candles "
         "WHERE symbol = ? AND timeframe = ?")
    params: list = [symbol, timeframe]
    if start_ms is not None:
        q += " AND ts >= ?"
        params.append(int(start_ms))
    if end_ms is not None:
        q += " AND ts <= ?"
        params.append(int(end_ms))
    q += " ORDER BY ts ASC"
    cur = conn.execute(q, params)
    return [
        {"ts": r[0], "open": r[1], "high": r[2],
         "low": r[3], "close": r[4], "volume": r[5]}
        for r in cur.fetchall()
    ]


def coverage_report(conn: sqlite3.Connection, timeframe: str,
                    start_ms: int, end_ms: int, bar_ms: int) -> "list[dict]":
    """Checa completude por simbolo na janela [start_ms, end_ms] (ambos inclusivos).
    E o 'acompanhar' — pega buraco de gravacao e listagem recente.
    Retorna por simbolo presente: {symbol, count, expected, missing, pct}.
    Perpetuos crypto sao 24/7, entao a contagem esperada e continua."""
    expected = int((end_ms - start_ms) // bar_ms) + 1
    cur = conn.execute(
        "SELECT symbol, COUNT(*) FROM candles "
        "WHERE timeframe = ? AND ts BETWEEN ? AND ? "
        "GROUP BY symbol ORDER BY symbol",
        (timeframe, int(start_ms), int(end_ms)),
    )
    out = []
    for sym, cnt in cur.fetchall():
        out.append({
            "symbol": sym,
            "count": cnt,
            "expected": expected,
            "missing": expected - cnt,
            "pct": round(100.0 * cnt / expected, 1) if expected else 0.0,
        })
    return out
