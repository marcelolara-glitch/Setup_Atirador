"""v10/schema.py — tabela `trades_v10`. Tabela NOVA, sem ALTER, sem migração.

Mora no mesmo arquivo do v9 (``journal/atirador_journal_v9.db``) para que uma
consulta só cruze histórico e ciclo novo. A tabela `trades` do v9 fica
READ-ONLY: nada aqui a lê, escreve ou altera.

REGRA DO SCHEMA: nenhuma coluna é específica de um modelo de saída. TP1/TP2/
TP3 e SL do bracket vivem em ``exit_params_json``; o alvo vigente da inversão
vive em ``exit_state_json``. Um setup com modelo de saída novo entra sem DDL.

``UNIQUE(setup_id, config_hash, symbol, entry_ts)`` dá idempotência: reprocessar
a mesma barra com a mesma configuração não duplica linha.
"""

from __future__ import annotations

import sqlite3

from config import JOURNAL_DB_V9

TABELA = "trades_v10"

DDL = """
CREATE TABLE IF NOT EXISTS trades_v10 (
    id                TEXT PRIMARY KEY,
    setup_id          TEXT NOT NULL,
    config_hash       TEXT NOT NULL,
    mode              TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    tf                TEXT NOT NULL,
    direction         TEXT NOT NULL,
    entry_ts          TEXT NOT NULL,
    entry_price       REAL NOT NULL,
    status            TEXT NOT NULL DEFAULT 'OPEN',
    exit_ts           TEXT,
    exit_price        REAL,
    pnl_pct           REAL,
    pnl_bps_liq       REAL,
    max_runup         REAL DEFAULT 0,
    max_drawdown      REAL DEFAULT 0,
    evidence_json     TEXT,
    exit_params_json  TEXT,
    exit_state_json   TEXT,
    UNIQUE (setup_id, config_hash, symbol, entry_ts)
);
CREATE INDEX IF NOT EXISTS idx_v10_setup_status ON trades_v10(setup_id, status);
CREATE INDEX IF NOT EXISTS idx_v10_symbol       ON trades_v10(symbol);
CREATE INDEX IF NOT EXISTS idx_v10_entry_ts     ON trades_v10(entry_ts);
"""

# Ordem canônica das colunas — o INSERT do dia que houver escrita usa esta
# lista, não um SELECT *, para que acrescentar coluna não quebre posição.
COLUNAS: tuple[str, ...] = (
    "id", "setup_id", "config_hash", "mode", "symbol", "tf", "direction",
    "entry_ts", "entry_price", "status", "exit_ts", "exit_price", "pnl_pct",
    "pnl_bps_liq", "max_runup", "max_drawdown", "evidence_json",
    "exit_params_json", "exit_state_json",
)


def connect(db_path: str = JOURNAL_DB_V9) -> sqlite3.Connection:
    """Abre conexão e garante `trades_v10`. Não toca em nenhuma outra tabela."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # mesmo modo que o journal v9 usa
    conn.executescript(DDL)
    return conn
