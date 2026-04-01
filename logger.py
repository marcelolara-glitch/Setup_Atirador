#!/usr/bin/env python3
"""
logger.py — Logging Estruturado de Rodada (Camada 1)
=====================================================
[v6.6.5] Módulo de observabilidade — registra tudo que o sistema calcula
em cada rodada para debug de consistência e calibração de indicadores.

Padrão de uso:
    log = RoundLogger(version=VERSION)
    log.set_meta(...)
    log.set_pipeline(...)
    log.add_token(...)      # chamado N vezes, um por token no 15m
    log.add_event(...)      # chamado para cada CALL e QUASE emitido
    log.commit()            # UMA vez, no finally: do bloco principal

CLI:
    python3 logger.py --rebuild    # reconstrói SQLite a partir do JSONL
"""
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

# ── Configuração ──────────────────────────────────────────────────────────────
BRT      = timezone(timedelta(hours=-3))
BASE_DIR = os.path.expanduser("~/Setup_Atirador/logs")
JSONL_PATH = os.path.join(BASE_DIR, "scan_log.jsonl")
DB_PATH    = os.path.join(BASE_DIR, "scan_log.db")

LOG = logging.getLogger(__name__)


# ── DDL SQLite ────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS rounds (
    round_id      TEXT PRIMARY KEY,
    ts            TEXT NOT NULL,
    version       TEXT,
    fgi           INTEGER,
    btc_4h        TEXT,
    thr_long      INTEGER,
    thr_short     INTEGER,
    exchange      TEXT,
    candle_locked INTEGER,
    exec_secs     REAL,
    univ_count    INTEGER,
    gate_4h       INTEGER,
    gate_1h       INTEGER,
    scored_15m    INTEGER
);

CREATE TABLE IF NOT EXISTS token_scores (
    round_id      TEXT NOT NULL,
    ts            TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    direction     TEXT NOT NULL,
    score_total   INTEGER,
    threshold     INTEGER,
    gap           INTEGER,
    status        TEXT,
    p1  INTEGER,  p1_reason  TEXT,
    p2  INTEGER,  p2_reason  TEXT,
    p3  INTEGER,  p3_reason  TEXT,
    p4  INTEGER,  p4_reason  TEXT,
    p5  INTEGER,  p5_reason  TEXT,
    p6  INTEGER,  p6_reason  TEXT,
    p7  INTEGER,  p7_reason  TEXT,
    p8  INTEGER,  p8_reason  TEXT,
    p9  INTEGER,  p9_reason  TEXT,
    p1h INTEGER,  p1h_reason TEXT,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id)
);

CREATE TABLE IF NOT EXISTS round_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id  TEXT NOT NULL,
    ts        TEXT NOT NULL,
    type      TEXT NOT NULL,
    symbol    TEXT NOT NULL,
    direction TEXT NOT NULL,
    score     INTEGER,
    gap       INTEGER
);

CREATE INDEX IF NOT EXISTS idx_ts_scores  ON token_scores(ts);
CREATE INDEX IF NOT EXISTS idx_sym_scores ON token_scores(symbol);
CREATE INDEX IF NOT EXISTS idx_ts_events  ON round_events(ts);
"""


class RoundLogger:
    """
    [v6.6.5] Logging estruturado de rodada — Camada 1 da arquitetura de observabilidade.

    Registra em duas camadas complementares:
      - scan_log.jsonl: verdade bruta, append-only, nunca corrompível
      - scan_log.db (SQLite): camada analítica, reconstituível do JSONL

    Padrão de uso:
      log = RoundLogger(version=VERSION)
      log.set_meta(...)
      log.set_pipeline(...)
      log.add_token(...)      # chamado N vezes, um por token no 15m
      log.add_event(...)      # chamado para cada CALL e QUASE emitido
      log.commit()            # UMA vez, no finally: do bloco principal

    Falhas de I/O são silenciosas — warning no LOG, nunca interrompem o scan.
    Tempo adicional estimado por rodada: 50–200ms (desprezível vs timeout 25min).
    """

    def __init__(self, version: str):
        # Gera round_id no formato "YYYYMMDD_HHMM" em BRT
        now = datetime.now(BRT)
        self.round_id  = now.strftime("%Y%m%d_%H%M")
        self.ts        = now.isoformat()
        self.version   = version
        self._meta     = {}
        self._pipeline = {}
        self._tokens   = []
        self._events   = []
        self._exec_secs = None
        self._committed = False
        os.makedirs(BASE_DIR, exist_ok=True)

    def set_meta(self, fgi: int, btc_4h: str, threshold_long: int,
                 threshold_short: int, exchange: str, candle_locked: bool):
        """Contexto da rodada — chamado após determinar FGI e thresholds."""
        self._meta = {
            "fgi"              : fgi,
            "btc_4h_trend"     : btc_4h,
            "threshold_long"   : threshold_long,
            "threshold_short"  : threshold_short,
            "exchange_primary" : exchange,
            "exec_seconds"     : None,   # preenchido em set_exec_seconds
            "candle_locked"    : candle_locked,
        }

    def set_pipeline(self, universe: int, after_gate_4h: int,
                     after_gate_1h: int, scored_15m: int):
        """Contagens do pipeline — chamado após completar os gates."""
        self._pipeline = {
            "universe"     : universe,
            "after_gate_4h": after_gate_4h,
            "after_gate_1h": after_gate_1h,
            "scored_15m"   : scored_15m,
        }

    def set_exec_seconds(self, seconds: float):
        """Tempo de execução — chamado no finally: com time.time() - t_start."""
        self._exec_secs = seconds
        if self._meta:
            self._meta["exec_seconds"] = round(seconds, 1)

    def add_token(self, symbol: str, direction: str, score_total: int,
                  threshold: int, gap: int, status: str, pillars: dict):
        """
        Chamado para cada token que chega ao scoring 15m.
        status: "CALL" | "QUASE" | "RADAR" | "DROP"
        pillars: dict com chaves P1..P9, P1H
          cada valor: {"score": int, "reason": str}
        """
        self._tokens.append({
            "symbol"     : symbol,
            "direction"  : direction,
            "score_total": score_total,
            "threshold"  : threshold,
            "gap"        : gap,
            "status"     : status,
            "pillars"    : pillars,
        })

    def add_event(self, type: str, symbol: str, direction: str,
                  score: int, gap: int):
        """
        Chamado ao emitir CALL ou QUASE para o Telegram.
        type: "CALL" | "QUASE"
        """
        self._events.append({
            "type"     : type,
            "symbol"   : symbol,
            "direction": direction,
            "score"    : score,
            "gap"      : gap,
        })

    def commit(self) -> bool:
        """
        Escreve JSONL e SQLite atomicamente.
        JSONL primeiro — se falhar, loga warning e tenta SQLite mesmo assim.
        SQLite segundo — se falhar, JSONL já está salvo como fallback.
        Retorna True se ambos OK, False se qualquer um falhou.
        """
        if self._committed:
            return True
        self._committed = True

        record = {
            "ts"      : self.ts,
            "version" : self.version,
            "round_id": self.round_id,
            "meta"    : self._meta,
            "pipeline": self._pipeline,
            "tokens"  : self._tokens,
            "events"  : self._events,
        }

        jsonl_ok = self._write_jsonl(record)
        db_ok    = self._write_db(record)
        return jsonl_ok and db_ok

    # ── I/O privado ──────────────────────────────────────────────────────────

    def _write_jsonl(self, record: dict) -> bool:
        try:
            with open(JSONL_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            return True
        except Exception as e:
            LOG.warning(f"[RoundLogger] Falha ao gravar JSONL: {e}")
            return False

    def _write_db(self, record: dict) -> bool:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_DDL)

            meta = record.get("meta", {})
            pipe = record.get("pipeline", {})
            rid  = record["round_id"]
            ts   = record["ts"]

            conn.execute(
                """INSERT OR REPLACE INTO rounds VALUES
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rid, ts, record.get("version"),
                    meta.get("fgi"), meta.get("btc_4h_trend"),
                    meta.get("threshold_long"), meta.get("threshold_short"),
                    meta.get("exchange_primary"),
                    1 if meta.get("candle_locked") else 0,
                    meta.get("exec_seconds"),
                    pipe.get("universe"), pipe.get("after_gate_4h"),
                    pipe.get("after_gate_1h"), pipe.get("scored_15m"),
                )
            )

            for tok in record.get("tokens", []):
                p = tok.get("pillars", {})
                def _ps(key):
                    return p.get(key, {}).get("score")
                def _pr(key):
                    return p.get(key, {}).get("reason")
                conn.execute(
                    """INSERT INTO token_scores VALUES
                       (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        rid, ts, tok["symbol"], tok["direction"],
                        tok.get("score_total"), tok.get("threshold"), tok.get("gap"),
                        tok.get("status"),
                        _ps("P1"), _pr("P1"),
                        _ps("P2"), _pr("P2"),
                        _ps("P3"), _pr("P3"),
                        _ps("P4"), _pr("P4"),
                        _ps("P5"), _pr("P5"),
                        _ps("P6"), _pr("P6"),
                        _ps("P7"), _pr("P7"),
                        _ps("P8"), _pr("P8"),
                        _ps("P9"), _pr("P9"),
                        _ps("P1H"), _pr("P1H"),
                    )
                )

            for ev in record.get("events", []):
                conn.execute(
                    """INSERT INTO round_events (round_id, ts, type, symbol, direction, score, gap)
                       VALUES (?,?,?,?,?,?,?)""",
                    (rid, ts, ev["type"], ev["symbol"], ev["direction"],
                     ev.get("score"), ev.get("gap"))
                )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            LOG.warning(f"[RoundLogger] Falha ao gravar SQLite: {e}")
            return False


# ── Reconstrução do banco ─────────────────────────────────────────────────────

def rebuild_db(jsonl_path: str = JSONL_PATH, db_path: str = DB_PATH) -> int:
    """
    Reconstrói o SQLite completo a partir do JSONL.
    Chamável via: python3 logger.py --rebuild
    Retorna o número de rodadas processadas.
    """
    if not os.path.exists(jsonl_path):
        print(f"[rebuild_db] JSONL não encontrado: {jsonl_path}")
        return 0

    # Dropa tabelas antigas e recria
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("DROP TABLE IF EXISTS token_scores")
    conn.execute("DROP TABLE IF EXISTS round_events")
    conn.execute("DROP TABLE IF EXISTS rounds")
    conn.executescript(_DDL)
    conn.commit()

    count = 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                _insert_record(conn, record)
                count += 1
            except Exception as e:
                print(f"[rebuild_db] Erro na linha {count+1}: {e}")

    conn.commit()
    conn.close()
    print(f"[rebuild_db] Reconstruído: {count} rodadas → {db_path}")
    return count


def _insert_record(conn: sqlite3.Connection, record: dict):
    """Insere um registro JSONL no banco SQLite (usado pelo rebuild_db)."""
    meta = record.get("meta", {})
    pipe = record.get("pipeline", {})
    rid  = record["round_id"]
    ts   = record["ts"]

    conn.execute(
        "INSERT OR REPLACE INTO rounds VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            rid, ts, record.get("version"),
            meta.get("fgi"), meta.get("btc_4h_trend"),
            meta.get("threshold_long"), meta.get("threshold_short"),
            meta.get("exchange_primary"),
            1 if meta.get("candle_locked") else 0,
            meta.get("exec_seconds"),
            pipe.get("universe"), pipe.get("after_gate_4h"),
            pipe.get("after_gate_1h"), pipe.get("scored_15m"),
        )
    )

    for tok in record.get("tokens", []):
        p = tok.get("pillars", {})
        def _ps(key):
            return p.get(key, {}).get("score")
        def _pr(key):
            return p.get(key, {}).get("reason")
        conn.execute(
            "INSERT INTO token_scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                rid, ts, tok["symbol"], tok["direction"],
                tok.get("score_total"), tok.get("threshold"), tok.get("gap"),
                tok.get("status"),
                _ps("P1"), _pr("P1"), _ps("P2"), _pr("P2"),
                _ps("P3"), _pr("P3"), _ps("P4"), _pr("P4"),
                _ps("P5"), _pr("P5"), _ps("P6"), _pr("P6"),
                _ps("P7"), _pr("P7"), _ps("P8"), _pr("P8"),
                _ps("P9"), _pr("P9"), _ps("P1H"), _pr("P1H"),
            )
        )

    for ev in record.get("events", []):
        conn.execute(
            "INSERT INTO round_events (round_id, ts, type, symbol, direction, score, gap) VALUES (?,?,?,?,?,?,?)",
            (rid, ts, ev["type"], ev["symbol"], ev["direction"],
             ev.get("score"), ev.get("gap"))
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--rebuild":
        jsonl = sys.argv[2] if len(sys.argv) > 2 else JSONL_PATH
        db    = sys.argv[3] if len(sys.argv) > 3 else DB_PATH
        rebuild_db(jsonl, db)
    else:
        print("Uso: python3 logger.py --rebuild [jsonl_path] [db_path]")
