# ===========================================================================
# LOGGER v7 — Observabilidade para Setup Atirador v7.0.0
# ===========================================================================
# DB: ~/Setup_Atirador/logs/scan_log_v7.db
#
# Tabelas:
#   rounds       — uma linha por rodada de scan
#   token_scores — uma linha por (round_id, symbol, direction)
#   round_events — eventos notáveis (CALL, QUASE) dentro de cada rodada
#
# Uso:
#   logger = RoundLoggerV7(version="7.0.0")
#   logger.set_meta(fgi=12, btc_4h="NEUTRAL", exchange="OKX", candle_locked=False)
#   logger.set_pipeline(universe=87, gate_4h_long=8, gate_4h_short=23,
#                       in_zona_long=2, in_zona_short=7)
#   logger.add_token(symbol="POLUSDT", direction="SHORT", ...)
#   logger.add_event("CALL", "POLUSDT", "SHORT", zona="ALTA_OB4H", check_c=3)
#   logger.commit()   # grava tudo no DB
# ===========================================================================

import os
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))

DB_PATH = os.path.join(
    os.path.expanduser("~"), "Setup_Atirador", "logs", "scan_log_v7.db"
)

DDL = """
CREATE TABLE IF NOT EXISTS rounds (
    round_id      TEXT PRIMARY KEY,
    ts            TEXT NOT NULL,
    version       TEXT,
    fgi           INTEGER,
    btc_4h        TEXT,
    exchange      TEXT,
    candle_locked INTEGER,
    exec_secs     REAL,
    univ_count    INTEGER,
    gate_4h_long  INTEGER,
    gate_4h_short INTEGER,
    in_zona_long  INTEGER,
    in_zona_short INTEGER,
    venue_summary TEXT
);

CREATE TABLE IF NOT EXISTS token_scores (
    round_id        TEXT NOT NULL,
    ts              TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    direction       TEXT NOT NULL,
    summary_4h      TEXT,
    summary_1h      TEXT,
    zona_qualidade  TEXT,
    zona_descricao  TEXT,
    check_a         INTEGER,
    check_a_razao   TEXT,
    check_b         INTEGER,
    check_b_razao   TEXT,
    check_c_total   INTEGER,
    check_c1_bb     INTEGER,
    check_c1_razao  TEXT,
    check_c2_vol    INTEGER,
    check_c2_razao  TEXT,
    check_c3_cvd    INTEGER,
    check_c3_razao  TEXT,
    check_c4_oi     INTEGER,
    check_c4_razao  TEXT,
    status          TEXT,
    PRIMARY KEY (round_id, symbol, direction)
);

CREATE TABLE IF NOT EXISTS round_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id  TEXT NOT NULL,
    ts        TEXT NOT NULL,
    type      TEXT NOT NULL,
    symbol    TEXT NOT NULL,
    direction TEXT NOT NULL,
    zona      TEXT,
    check_c   INTEGER
);
"""


def _ensure_db(path: str) -> sqlite3.Connection:
    """Garante que o diretório e as tabelas existem, retorna conexão."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(DDL)
    conn.commit()
    return conn


class RoundLoggerV7:
    """
    Logger de rodada para v7.0.0.
    Coleta dados em memória e persiste no SQLite apenas ao chamar commit().
    Thread-safe apenas para uso single-threaded (padrão asyncio).
    """

    def __init__(self, version: str = "7.0.0", db_path: str = None):
        self.version    = version
        self.db_path    = db_path or DB_PATH
        self.round_id   = datetime.now(BRT).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self.ts         = datetime.now(BRT).isoformat()
        self._committed = False

        # Meta da rodada
        self._fgi           = None
        self._btc_4h        = None
        self._exchange      = None
        self._candle_locked = None
        self._exec_secs     = None
        self._univ_count    = None
        self._gate_4h_long  = None
        self._gate_4h_short = None
        self._in_zona_long  = None
        self._in_zona_short = None
        self._venue_summary = None

        # Acumuladores
        self._tokens: list[dict] = []
        self._events: list[dict] = []

    # ------------------------------------------------------------------
    def set_meta(self, *, fgi: int = None, btc_4h: str = None,
                 exchange: str = None, candle_locked: bool = None):
        """Registra contexto de mercado da rodada."""
        if fgi           is not None: self._fgi           = int(fgi)
        if btc_4h        is not None: self._btc_4h        = str(btc_4h)
        if exchange      is not None: self._exchange      = str(exchange)
        if candle_locked is not None: self._candle_locked = int(bool(candle_locked))

    def set_pipeline(self, *, universe: int = None,
                     gate_4h_long: int = None, gate_4h_short: int = None,
                     in_zona_long: int = None, in_zona_short: int = None,
                     venue_summary: str = None):
        """Registra contagens do pipeline."""
        if universe      is not None: self._univ_count    = int(universe)
        if gate_4h_long  is not None: self._gate_4h_long  = int(gate_4h_long)
        if gate_4h_short is not None: self._gate_4h_short = int(gate_4h_short)
        if in_zona_long  is not None: self._in_zona_long  = int(in_zona_long)
        if in_zona_short is not None: self._in_zona_short = int(in_zona_short)
        if venue_summary is not None: self._venue_summary = str(venue_summary)

    def set_exec_seconds(self, secs: float):
        self._exec_secs = float(secs)

    # ------------------------------------------------------------------
    def add_token(self, *,
                  symbol: str,
                  direction: str,
                  summary_4h: str = None,
                  summary_1h: str = None,
                  zona_qualidade: str = None,
                  zona_descricao: str = None,
                  check_a: bool = None,
                  check_a_razao: str = None,
                  check_b: bool = None,
                  check_b_razao: str = None,
                  check_c_total: int = None,
                  check_c_detalhes: dict = None,
                  status: str = None):
        """
        Registra resultado de análise de um token/direção.
        check_c_detalhes deve ser o dict retornado por check_forca_movimento:
          {"c1_bb": "...", "c2_vol": "...", "c3_cvd": "...", "c4_oi": "..."}
        """
        det = check_c_detalhes or {}

        def _parse_c(key):
            """Extrai bool (1/0) e razão de um detalhe do check C."""
            txt = det.get(key, "")
            passed = 1 if "✅" in txt else (0 if "❌" in txt else None)
            return passed, txt

        c1_v, c1_r = _parse_c("c1_bb")
        c2_v, c2_r = _parse_c("c2_vol")
        c3_v, c3_r = _parse_c("c3_cvd")
        c4_v, c4_r = _parse_c("c4_oi")

        row = {
            "round_id"       : self.round_id,
            "ts"             : datetime.now(BRT).isoformat(),
            "symbol"         : symbol,
            "direction"      : direction,
            "summary_4h"     : summary_4h,
            "summary_1h"     : summary_1h,
            "zona_qualidade" : zona_qualidade,
            "zona_descricao" : zona_descricao,
            "check_a"        : int(bool(check_a)) if check_a is not None else None,
            "check_a_razao"  : check_a_razao,
            "check_b"        : int(bool(check_b)) if check_b is not None else None,
            "check_b_razao"  : check_b_razao,
            "check_c_total"  : check_c_total,
            "check_c1_bb"    : c1_v,
            "check_c1_razao" : c1_r or None,
            "check_c2_vol"   : c2_v,
            "check_c2_razao" : c2_r or None,
            "check_c3_cvd"   : c3_v,
            "check_c3_razao" : c3_r or None,
            "check_c4_oi"    : c4_v,
            "check_c4_razao" : c4_r or None,
            "status"         : status,
        }
        self._tokens.append(row)

    def add_event(self, event_type: str, symbol: str, direction: str,
                  zona: str = None, check_c: int = None):
        """
        Registra evento notável (CALL / QUASE).
        event_type: "CALL" | "QUASE" | "RADAR" | "DROP"
        """
        self._events.append({
            "round_id" : self.round_id,
            "ts"       : datetime.now(BRT).isoformat(),
            "type"     : event_type,
            "symbol"   : symbol,
            "direction": direction,
            "zona"     : zona,
            "check_c"  : check_c,
        })

    # ------------------------------------------------------------------
    def commit(self):
        """Persiste todos os dados acumulados no SQLite. Idempotente."""
        if self._committed:
            return
        try:
            conn = _ensure_db(self.db_path)
            with conn:
                # rounds
                conn.execute(
                    """INSERT OR REPLACE INTO rounds
                       (round_id, ts, version, fgi, btc_4h, exchange, candle_locked,
                        exec_secs, univ_count, gate_4h_long, gate_4h_short,
                        in_zona_long, in_zona_short, venue_summary)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (self.round_id, self.ts, self.version,
                     self._fgi, self._btc_4h, self._exchange, self._candle_locked,
                     self._exec_secs, self._univ_count,
                     self._gate_4h_long, self._gate_4h_short,
                     self._in_zona_long, self._in_zona_short,
                     self._venue_summary)
                )
                # token_scores
                for row in self._tokens:
                    conn.execute(
                        """INSERT OR REPLACE INTO token_scores
                           (round_id, ts, symbol, direction,
                            summary_4h, summary_1h,
                            zona_qualidade, zona_descricao,
                            check_a, check_a_razao,
                            check_b, check_b_razao,
                            check_c_total,
                            check_c1_bb, check_c1_razao,
                            check_c2_vol, check_c2_razao,
                            check_c3_cvd, check_c3_razao,
                            check_c4_oi, check_c4_razao,
                            status)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (row["round_id"], row["ts"], row["symbol"], row["direction"],
                         row["summary_4h"], row["summary_1h"],
                         row["zona_qualidade"], row["zona_descricao"],
                         row["check_a"], row["check_a_razao"],
                         row["check_b"], row["check_b_razao"],
                         row["check_c_total"],
                         row["check_c1_bb"], row["check_c1_razao"],
                         row["check_c2_vol"], row["check_c2_razao"],
                         row["check_c3_cvd"], row["check_c3_razao"],
                         row["check_c4_oi"], row["check_c4_razao"],
                         row["status"])
                    )
                # round_events
                for ev in self._events:
                    conn.execute(
                        """INSERT INTO round_events
                           (round_id, ts, type, symbol, direction, zona, check_c)
                           VALUES (?,?,?,?,?,?,?)""",
                        (ev["round_id"], ev["ts"], ev["type"],
                         ev["symbol"], ev["direction"], ev["zona"], ev["check_c"])
                    )
            self._committed = True
        except Exception as exc:
            # Nunca interrompe o scan por falha de log
            print(f"[logger_v7] WARN: commit falhou: {type(exc).__name__}: {exc}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
