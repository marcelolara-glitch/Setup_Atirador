"""logger.py — Logging Estruturado de Rodada v9 (Camada 1).

Reescrita completa para a arquitetura multi-setup. Substitui o
``RoundLogger`` v8 (gate+check+score) por :class:`RoundLoggerV9`
(action/signal_tag/confidence/regime + near-misses observacionais).

Arquitetura de duas camadas (preservada do v8):
    - JSONL append-only em ``SCAN_LOG_JSONL_V9`` — verdade bruta.
    - SQLite analítico em ``SCAN_LOG_DB_V9`` — reconstruível via
      :func:`rebuild_db` a partir do JSONL.

Padrão de uso::

    log = RoundLoggerV9(version=VERSION)
    log.set_meta(...)
    log.set_pipeline(...)
    log.set_exec_seconds(t)
    for decision in decisions:
        log.add_token_evaluation(decision)
    for nm in near_misses:
        log.add_near_miss(...)
    for call in calls:
        log.add_event(type="CALL", ...)
    log.commit()                   # UMA vez, no finally:

CLI::

    python3 logger.py --rebuild [jsonl_path] [db_path]

Princípios:
    - Falhas silenciosas em I/O (`LOG.warning`/`LOG.error`); nunca crasha.
    - JSONL primeiro: se SQLite falhar, JSONL sobrevive como fallback.
    - Idempotência: double-`commit()` é no-op.
    - Sem migração em runtime; schema v9 nasce limpo (paths isolados).
    - Duck typing com ``SignalDecision``/``SetupResult``/``TradePlan`` —
      logger não importa de ``signals.py``/``setups.base``/``risk``.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime
from typing import Any, Optional

from config import BRT, LOG_DIR, SCAN_LOG_DB_V9, SCAN_LOG_JSONL_V9

LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL SQLite v9
# ---------------------------------------------------------------------------

_DDL_V9 = """
CREATE TABLE IF NOT EXISTS rounds (
    round_id                TEXT PRIMARY KEY,
    ts                      TEXT NOT NULL,
    version                 TEXT,
    fgi                     INTEGER,
    btc_regime              TEXT,
    btc_change_pct_15m      REAL,
    btc_filter_blocked      INTEGER,
    exchange_primary        TEXT,
    setups_enabled          TEXT,
    exec_secs               REAL,
    universe                INTEGER,
    tokens_evaluated        INTEGER,
    calls_emitted           INTEGER,
    setups_triggered_total  INTEGER,
    near_misses_count       INTEGER
);

CREATE TABLE IF NOT EXISTS token_evaluations (
    round_id             TEXT NOT NULL,
    ts                   TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    timeframe            TEXT,
    action               TEXT NOT NULL,
    direction            TEXT,
    signal_tag           TEXT,
    confluent_setups     TEXT,
    confidence           REAL,
    regime               TEXT,
    skip_reason          TEXT,
    all_setup_results    TEXT,
    protection_filters   TEXT,
    trade_plan           TEXT,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id)
);
CREATE INDEX IF NOT EXISTS idx_tev_ts     ON token_evaluations(ts);
CREATE INDEX IF NOT EXISTS idx_tev_symbol ON token_evaluations(symbol);
CREATE INDEX IF NOT EXISTS idx_tev_action ON token_evaluations(action);

CREATE TABLE IF NOT EXISTS near_misses (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id            TEXT NOT NULL,
    ts                  TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    closest_setup_name  TEXT NOT NULL,
    closest_confidence  REAL,
    closest_direction   TEXT,
    closest_regime      TEXT,
    conditions          TEXT,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id)
);
CREATE INDEX IF NOT EXISTS idx_nm_ts     ON near_misses(ts);
CREATE INDEX IF NOT EXISTS idx_nm_symbol ON near_misses(symbol);
CREATE INDEX IF NOT EXISTS idx_nm_setup  ON near_misses(closest_setup_name);

CREATE TABLE IF NOT EXISTS round_events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id             TEXT NOT NULL,
    ts                   TEXT NOT NULL,
    type                 TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    direction            TEXT NOT NULL,
    signal_tag           TEXT NOT NULL,
    confidence           REAL,
    leverage             REAL
);
CREATE INDEX IF NOT EXISTS idx_ev_ts     ON round_events(ts);
CREATE INDEX IF NOT EXISTS idx_ev_symbol ON round_events(symbol);
CREATE INDEX IF NOT EXISTS idx_ev_type   ON round_events(type);
"""


# ---------------------------------------------------------------------------
# Helpers privados — serialização tolerante (duck typing)
# ---------------------------------------------------------------------------


def _to_iso(value: Any) -> Optional[str]:
    """Converte ``pd.Timestamp``/``datetime``/str para ISO; None se vazio."""
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


def _asdict_safe(obj: Any) -> Any:
    """``dataclasses.asdict`` quando possível; fallback para ``__dict__``."""
    if obj is None:
        return None
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        try:
            return dataclasses.asdict(obj)
        except Exception:
            pass
    if isinstance(obj, dict):
        return obj
    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict):
        return dict(d)
    return obj


def _serialize_setup_result(result: Any) -> dict:
    """Serializa um ``SetupResult`` (com ``triggered_at`` ``pd.Timestamp``).

    ``dataclasses.asdict()`` falha em ``pd.Timestamp``; convertemos manualmente.
    """
    if result is None:
        return {}
    return {
        "setup_name":   getattr(result, "setup_name", None),
        "triggered":    bool(getattr(result, "triggered", False)),
        "direction":    getattr(result, "direction", None),
        "confidence":   float(getattr(result, "confidence", 0.0) or 0.0),
        "evidence":     getattr(result, "evidence", {}) or {},
        "triggered_at": _to_iso(getattr(result, "triggered_at", None)),
    }


def _serialize_trade_plan(plan: Any) -> Optional[dict]:
    """Serializa um ``TradePlan`` para dict JSON-amigável; None se vazio."""
    if plan is None:
        return None
    d = _asdict_safe(plan)
    if not isinstance(d, dict):
        return None
    split = d.get("position_split")
    if isinstance(split, tuple):
        d["position_split"] = list(split)
    return d


def _json_or_none(value: Any) -> Optional[str]:
    """``json.dumps(value, ensure_ascii=False)`` se não None; senão None."""
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception as e:
        LOG.warning(f"[logger] Falha ao serializar JSON: {e}")
        return None


# ---------------------------------------------------------------------------
# Classe principal
# ---------------------------------------------------------------------------


class RoundLoggerV9:
    """Logging estruturado de rodada v9 — Camada 1 (multi-setup + near-misses).

    Acumula contexto da rodada em memória e persiste em duas camadas no
    :meth:`commit`: JSONL append-only (verdade bruta) + SQLite analítico.

    Falhas de I/O são silenciosas — warning/error no LOG, nunca interrompem
    o scan. Idempotente: double-commit é no-op.
    """

    def __init__(self, version: str):
        """Cria o logger da rodada. ``round_id`` = ``YYYYMMDD_HHMM`` em BRT."""
        now = datetime.now(BRT)
        self.round_id: str = now.strftime("%Y%m%d_%H%M")
        self.ts: str = now.isoformat()
        self.version: str = version
        self._meta: dict = {}
        self._pipeline: dict = {}
        self._tokens: list[dict] = []
        self._near_misses: list[dict] = []
        self._events: list[dict] = []
        self._exec_secs: Optional[float] = None
        self._committed: bool = False
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
        except Exception as e:
            LOG.warning(f"[RoundLoggerV9] Falha ao criar LOG_DIR: {e}")

    # ── set_meta / set_pipeline / set_exec_seconds ──────────────────────────

    def set_meta(
        self,
        fgi: int,
        btc_regime: str,
        btc_change_pct_15m: float,
        exchange_primary: str,
        setups_enabled: dict[str, bool],
        btc_filter_blocked: bool = False,
    ) -> None:
        """Grava o contexto macro da rodada (FGI, regime BTC, exchange ativa).

        Parâmetros:
            fgi: Fear & Greed index (0-100).
            btc_regime: ``TREND_UP`` | ``TREND_DOWN`` | ``RANGE`` | ``SQUEEZE``.
            btc_change_pct_15m: variação do BTC na última vela 15m.
            exchange_primary: ``"OKX"`` | ``"Gate.io"`` | ``"Bitget"``.
            setups_enabled: dict de kill-switches ativos.
            btc_filter_blocked: True se algum CALL foi bloqueado pelo filtro BTC.
        """
        self._meta = {
            "fgi":                 fgi,
            "btc_regime":          btc_regime,
            "btc_change_pct_15m":  float(btc_change_pct_15m),
            "exchange_primary":    exchange_primary,
            "setups_enabled":      dict(setups_enabled or {}),
            "btc_filter_blocked":  bool(btc_filter_blocked),
            "exec_seconds":        self._exec_secs,
        }

    def set_pipeline(
        self,
        universe: int,
        tokens_evaluated: int,
        calls_emitted: int,
        setups_triggered_total: int,
        near_misses_count: int,
    ) -> None:
        """Grava as contagens do pipeline da rodada.

        Parâmetros:
            universe: total de símbolos qualificados por OKX/Gate/Bitget.
            tokens_evaluated: quantos chegaram a ``signals.evaluate_token()``.
            calls_emitted: decisions com ``action == "CALL"``.
            setups_triggered_total: soma de setups disparados (confluências
                contam múltiplo).
            near_misses_count: tokens com pelo menos 1 setup triggered mas
                que acabaram em SKIP.
        """
        self._pipeline = {
            "universe":               int(universe),
            "tokens_evaluated":       int(tokens_evaluated),
            "calls_emitted":          int(calls_emitted),
            "setups_triggered_total": int(setups_triggered_total),
            "near_misses_count":      int(near_misses_count),
        }

    def set_exec_seconds(self, seconds: float) -> None:
        """Registra tempo total de execução da rodada (em segundos)."""
        self._exec_secs = float(seconds)
        if self._meta:
            self._meta["exec_seconds"] = round(self._exec_secs, 1)

    # ── add_token_evaluation / add_near_miss / add_event ────────────────────

    def add_token_evaluation(self, decision: Any) -> None:
        """Acumula um snapshot de ``SignalDecision`` (CALL ou SKIP).

        Aceita qualquer objeto duck-typed com os atributos do
        ``SignalDecision`` v9; não importa a classe real para evitar
        acoplamento com ``signals.py``. ``timestamp`` (``pd.Timestamp``)
        é convertido para ISO.
        """
        try:
            all_results = list(getattr(decision, "all_setup_results", []) or [])
            snapshot = {
                "symbol":             getattr(decision, "symbol", None),
                "timestamp":          _to_iso(getattr(decision, "timestamp", None)),
                "timeframe":          getattr(decision, "timeframe", None),
                "action":             getattr(decision, "action", None),
                "direction":          getattr(decision, "direction", None),
                "signal_tag":         getattr(decision, "signal_tag", None),
                "confluent_setups":   list(getattr(decision, "confluent_setups", []) or []),
                "confidence":         float(getattr(decision, "confidence", 0.0) or 0.0),
                "regime":             getattr(decision, "regime", None),
                "skip_reason":        getattr(decision, "skip_reason", None),
                "all_setup_results":  [_serialize_setup_result(r) for r in all_results],
                "protection_filters": dict(getattr(decision, "protection_filters", {}) or {}),
                "trade_plan":         _serialize_trade_plan(getattr(decision, "trade_plan", None)),
            }
        except Exception as e:
            LOG.warning(f"[RoundLoggerV9] Falha ao montar snapshot de decision: {e}")
            return
        self._tokens.append(snapshot)

    def add_near_miss(
        self,
        symbol: str,
        closest_setup_name: str,
        closest_confidence: float,
        closest_direction: Optional[str],
        closest_regime: str,
        conditions: list[dict],
        ts: str,
    ) -> None:
        """Registra um token que quase disparou (observabilidade pura).

        A lógica de detecção do near-miss é responsabilidade do chamador
        (``main.py``). O logger apenas armazena. Sem SL/TP hipotéticos —
        difere conceitualmente do ``QUASE`` do v8.
        """
        self._near_misses.append({
            "symbol":             symbol,
            "closest_setup_name": closest_setup_name,
            "closest_confidence": float(closest_confidence or 0.0),
            "closest_direction":  closest_direction,
            "closest_regime":     closest_regime,
            "conditions":         list(conditions or []),
            "ts":                 ts,
        })

    def add_event(
        self,
        type: str,
        symbol: str,
        direction: str,
        signal_tag: str,
        confidence: float,
        leverage: float,
    ) -> None:
        """Registra um evento operacional da rodada (em v9, apenas ``CALL``)."""
        self._events.append({
            "type":       type,
            "symbol":     symbol,
            "direction":  direction,
            "signal_tag": signal_tag,
            "confidence": float(confidence or 0.0),
            "leverage":   float(leverage or 0.0),
        })

    # ── commit ──────────────────────────────────────────────────────────────

    def commit(self) -> bool:
        """Persiste a rodada em JSONL e SQLite. Idempotente.

        JSONL é gravado primeiro (verdade bruta, sempre recuperável via
        :func:`rebuild_db`). SQLite vem em seguida; se falhar, o JSONL
        ainda preserva os dados.

        Retorna:
            True se ambos JSONL e SQLite gravaram com sucesso, False caso
            contrário. Double-commit é no-op (retorna True).
        """
        if self._committed:
            return True
        self._committed = True

        record = {
            "ts":          self.ts,
            "version":     self.version,
            "round_id":    self.round_id,
            "meta":        self._meta,
            "pipeline":    self._pipeline,
            "tokens":      self._tokens,
            "near_misses": self._near_misses,
            "events":      self._events,
        }

        jsonl_ok = self._write_jsonl(record)
        db_ok    = self._write_db(record)
        return jsonl_ok and db_ok

    # ── I/O privado ─────────────────────────────────────────────────────────

    def _write_jsonl(self, record: dict) -> bool:
        try:
            with open(SCAN_LOG_JSONL_V9, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            return True
        except Exception as e:
            LOG.warning(f"[RoundLoggerV9] Falha ao gravar JSONL: {e}")
            return False

    def _write_db(self, record: dict) -> bool:
        try:
            conn = sqlite3.connect(SCAN_LOG_DB_V9, timeout=10)
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.executescript(_DDL_V9)
                _insert_record_v9(conn, record)
                conn.commit()
            finally:
                conn.close()
            return True
        except Exception as e:
            LOG.warning(f"[RoundLoggerV9] Falha ao gravar SQLite: {e}")
            return False


# ---------------------------------------------------------------------------
# INSERT helper (compartilhado entre commit() e rebuild_db)
# ---------------------------------------------------------------------------


def _insert_record_v9(conn: sqlite3.Connection, record: dict) -> None:
    """Insere um registro JSONL v9 nas 4 tabelas SQLite.

    Usado por :meth:`RoundLoggerV9._write_db` e :func:`rebuild_db`. Falhas
    em rounds/tokens/near_misses/events individuais são logadas mas não
    abortam o restante (rebuild de 10k rodadas não pode parar na primeira
    linha corrompida).
    """
    meta = record.get("meta") or {}
    pipe = record.get("pipeline") or {}
    rid  = record.get("round_id")
    ts   = record.get("ts")

    if not rid or not ts:
        raise ValueError("registro sem round_id/ts")

    conn.execute(
        """INSERT OR REPLACE INTO rounds (
               round_id, ts, version,
               fgi, btc_regime, btc_change_pct_15m, btc_filter_blocked,
               exchange_primary, setups_enabled, exec_secs,
               universe, tokens_evaluated, calls_emitted,
               setups_triggered_total, near_misses_count
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            rid,
            ts,
            record.get("version"),
            meta.get("fgi"),
            meta.get("btc_regime"),
            meta.get("btc_change_pct_15m"),
            1 if meta.get("btc_filter_blocked") else 0,
            meta.get("exchange_primary"),
            _json_or_none(meta.get("setups_enabled")),
            meta.get("exec_seconds"),
            pipe.get("universe"),
            pipe.get("tokens_evaluated"),
            pipe.get("calls_emitted"),
            pipe.get("setups_triggered_total"),
            pipe.get("near_misses_count"),
        ),
    )

    for tok in record.get("tokens", []) or []:
        try:
            conn.execute(
                """INSERT INTO token_evaluations (
                       round_id, ts, symbol, timeframe, action, direction,
                       signal_tag, confluent_setups, confidence, regime,
                       skip_reason, all_setup_results, protection_filters,
                       trade_plan
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rid,
                    ts,
                    tok.get("symbol"),
                    tok.get("timeframe"),
                    tok.get("action"),
                    tok.get("direction"),
                    tok.get("signal_tag"),
                    _json_or_none(tok.get("confluent_setups")),
                    tok.get("confidence"),
                    tok.get("regime"),
                    tok.get("skip_reason"),
                    _json_or_none(tok.get("all_setup_results")),
                    _json_or_none(tok.get("protection_filters")),
                    _json_or_none(tok.get("trade_plan")),
                ),
            )
        except Exception as e:
            LOG.warning(f"[logger] Falha ao inserir token {tok.get('symbol')}: {e}")

    for nm in record.get("near_misses", []) or []:
        try:
            conn.execute(
                """INSERT INTO near_misses (
                       round_id, ts, symbol, closest_setup_name,
                       closest_confidence, closest_direction, closest_regime,
                       conditions
                   ) VALUES (?,?,?,?,?,?,?,?)""",
                (
                    rid,
                    nm.get("ts", ts),
                    nm.get("symbol"),
                    nm.get("closest_setup_name"),
                    nm.get("closest_confidence"),
                    nm.get("closest_direction"),
                    nm.get("closest_regime"),
                    _json_or_none(nm.get("conditions")),
                ),
            )
        except Exception as e:
            LOG.warning(f"[logger] Falha ao inserir near_miss {nm.get('symbol')}: {e}")

    for ev in record.get("events", []) or []:
        try:
            conn.execute(
                """INSERT INTO round_events (
                       round_id, ts, type, symbol, direction, signal_tag,
                       confidence, leverage
                   ) VALUES (?,?,?,?,?,?,?,?)""",
                (
                    rid,
                    ts,
                    ev.get("type"),
                    ev.get("symbol"),
                    ev.get("direction"),
                    ev.get("signal_tag"),
                    ev.get("confidence"),
                    ev.get("leverage"),
                ),
            )
        except Exception as e:
            LOG.warning(f"[logger] Falha ao inserir event {ev.get('symbol')}: {e}")


# ---------------------------------------------------------------------------
# rebuild_db — reconstrói SQLite v9 a partir do JSONL
# ---------------------------------------------------------------------------


def rebuild_db(
    jsonl_path: str = SCAN_LOG_JSONL_V9,
    db_path: str = SCAN_LOG_DB_V9,
) -> int:
    """Reconstrói o SQLite v9 completo a partir do JSONL.

    1. Drop das 4 tabelas v9 (``rounds``, ``token_evaluations``,
       ``near_misses``, ``round_events``).
    2. ``CREATE TABLE IF NOT EXISTS`` com o DDL v9.
    3. Para cada linha do JSONL, chama :func:`_insert_record_v9`.

    Linhas malformadas são contadas como erro mas não abortam o processo.
    Retorna o número de rodadas processadas com sucesso.
    """
    if not os.path.exists(jsonl_path):
        LOG.warning(f"[rebuild_db] JSONL não encontrado: {jsonl_path}")
        try:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
        except Exception as e:
            LOG.warning(f"[rebuild_db] Falha ao criar diretório do DB: {e}")
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            try:
                conn.executescript(_DDL_V9)
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            LOG.error(f"[rebuild_db] Falha ao criar DB vazio: {e}")
        return 0

    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    except Exception as e:
        LOG.warning(f"[rebuild_db] Falha ao criar diretório do DB: {e}")

    try:
        conn = sqlite3.connect(db_path, timeout=10)
    except Exception as e:
        LOG.error(f"[rebuild_db] Falha ao abrir DB: {e}")
        return 0

    success = 0
    errors = 0
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("DROP TABLE IF EXISTS round_events")
        conn.execute("DROP TABLE IF EXISTS near_misses")
        conn.execute("DROP TABLE IF EXISTS token_evaluations")
        conn.execute("DROP TABLE IF EXISTS rounds")
        conn.executescript(_DDL_V9)
        conn.commit()

        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    _insert_record_v9(conn, record)
                    success += 1
                except Exception as e:
                    errors += 1
                    LOG.warning(f"[rebuild_db] Erro na linha {line_no}: {e}")
        conn.commit()
    except Exception as e:
        LOG.error(f"[rebuild_db] Falha durante reconstrução: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    LOG.info(
        f"[rebuild_db] Reconstruído: {success} rodadas, {errors} erros → {db_path}"
    )
    return success


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--rebuild":
        jsonl = sys.argv[2] if len(sys.argv) > 2 else SCAN_LOG_JSONL_V9
        db    = sys.argv[3] if len(sys.argv) > 3 else SCAN_LOG_DB_V9
        n = rebuild_db(jsonl, db)
        print(f"[rebuild_db] {n} rodadas processadas → {db}")
    else:
        print("Uso: python3 logger.py --rebuild [jsonl_path] [db_path]")
