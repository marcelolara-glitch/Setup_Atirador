"""Testes do módulo logger v9 — 2 camadas (JSONL + SQLite) e rebuild.

Cobre os 16 casos obrigatórios do briefing 12b: instanciação, set_meta,
set_pipeline, add_token_evaluation (CALL/SKIP/Timestamp), add_near_miss,
add_event, commit (JSONL/SQLite/idempotência/fallback) e rebuild_db
(vazio/single/malformado/drop).
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixture — redireciona SCAN_LOG_JSONL_V9/SCAN_LOG_DB_V9 para tmp e
# (re)importa o logger limpo para que ele leia os paths novos.
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_paths(tmp_path, monkeypatch):
    jsonl = tmp_path / "logs" / "scan_log_v9.jsonl"
    db    = tmp_path / "logs" / "scan_log_v9.db"
    log_dir = str(tmp_path / "logs")

    import config as cfg
    monkeypatch.setattr(cfg, "SCAN_LOG_JSONL_V9", str(jsonl), raising=True)
    monkeypatch.setattr(cfg, "SCAN_LOG_DB_V9", str(db), raising=True)
    monkeypatch.setattr(cfg, "LOG_DIR", log_dir, raising=True)

    import logger as logger_mod
    monkeypatch.setattr(logger_mod, "SCAN_LOG_JSONL_V9", str(jsonl), raising=True)
    monkeypatch.setattr(logger_mod, "SCAN_LOG_DB_V9", str(db), raising=True)
    monkeypatch.setattr(logger_mod, "LOG_DIR", log_dir, raising=True)

    return {
        "jsonl": str(jsonl),
        "db":    str(db),
        "log_dir": log_dir,
        "logger": logger_mod,
    }


# ---------------------------------------------------------------------------
# Fakes — não acoplam ao signals.py / setups.base / risk.py reais
# ---------------------------------------------------------------------------


@dataclass
class FakeSetupResult:
    setup_name: str = "cont_pull"
    triggered: bool = True
    direction: Optional[str] = "LONG"
    confidence: float = 75.0
    evidence: dict = field(default_factory=lambda: {"foo": "bar"})
    triggered_at: Any = field(default_factory=lambda: pd.Timestamp("2026-04-23T14:15:00"))


@dataclass
class FakeTradePlan:
    direction: str = "LONG"
    entry_price: float = 100.0
    sl_price: float = 98.0
    sl_distance_pct: float = 2.0
    tp1_price: float = 102.0
    tp2_price: float = 104.0
    tp3_price: float = 107.0
    tp1_distance_pct: float = 2.0
    tp2_distance_pct: float = 4.0
    tp3_distance_pct: float = 7.0
    risk_reward_tp1: float = 1.0
    risk_reward_tp2: float = 2.0
    risk_reward_tp3: float = 3.5
    atr_value: float = 1.5
    position_split: tuple = (0.5, 0.3, 0.2)
    leverage: float = 5.0


@dataclass
class FakeSignalDecision:
    symbol: str = "BTCUSDT"
    timestamp: Any = field(default_factory=lambda: pd.Timestamp("2026-04-23T14:15:00"))
    timeframe: str = "15m"
    action: str = "CALL"
    direction: Optional[str] = "LONG"
    signal_tag: Optional[str] = "cont_pull"
    confluent_setups: list = field(default_factory=lambda: ["cont_pull"])
    confidence: float = 82.5
    trade_plan: Any = field(default_factory=FakeTradePlan)
    regime: str = "TREND_UP"
    all_setup_results: list = field(default_factory=lambda: [FakeSetupResult()])
    protection_filters: dict = field(default_factory=lambda: {
        "already_open": True, "volatility_ok": True,
        "btc_stable": True, "confidence_threshold": True,
    })
    skip_reason: Optional[str] = None


def _make_skip_decision(symbol: str = "ETHUSDT", reason: str = "no_setup_triggered") -> FakeSignalDecision:
    return FakeSignalDecision(
        symbol=symbol,
        action="SKIP",
        direction=None,
        signal_tag=None,
        confluent_setups=[],
        confidence=0.0,
        trade_plan=None,
        all_setup_results=[FakeSetupResult(triggered=False, confidence=42.0)],
        protection_filters={},
        skip_reason=reason,
    )


# ---------------------------------------------------------------------------
# 1. Instanciação
# ---------------------------------------------------------------------------


def test_round_logger_instantiation(tmp_paths):
    """Cria logger, round_id no formato YYYYMMDD_HHMM, ts ISO, _committed=False."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    assert re.fullmatch(r"\d{8}_\d{4}", R.round_id), R.round_id
    datetime.fromisoformat(R.ts)  # parse válido
    assert R.version == "9.0.0-beta"
    assert R._committed is False
    assert R._tokens == [] and R._near_misses == [] and R._events == []
    assert os.path.isdir(tmp_paths["log_dir"])


# ---------------------------------------------------------------------------
# 2-3. set_meta / set_pipeline
# ---------------------------------------------------------------------------


def test_set_meta_populates_fields(tmp_paths):
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    R.set_meta(
        fgi=55,
        btc_regime="TREND_UP",
        btc_change_pct_15m=0.42,
        exchange_primary="OKX",
        setups_enabled={"cont_pull": True, "rev_zone": False},
        btc_filter_blocked=True,
    )
    assert R._meta["fgi"] == 55
    assert R._meta["btc_regime"] == "TREND_UP"
    assert R._meta["btc_change_pct_15m"] == 0.42
    assert R._meta["exchange_primary"] == "OKX"
    assert R._meta["setups_enabled"] == {"cont_pull": True, "rev_zone": False}
    assert R._meta["btc_filter_blocked"] is True


def test_set_pipeline_populates_fields(tmp_paths):
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    R.set_pipeline(
        universe=120,
        tokens_evaluated=110,
        calls_emitted=3,
        setups_triggered_total=5,
        near_misses_count=8,
    )
    assert R._pipeline == {
        "universe": 120,
        "tokens_evaluated": 110,
        "calls_emitted": 3,
        "setups_triggered_total": 5,
        "near_misses_count": 8,
    }


# ---------------------------------------------------------------------------
# 4-6. add_token_evaluation
# ---------------------------------------------------------------------------


def test_add_token_evaluation_call(tmp_paths):
    """CALL preserva todos os campos esperados, incluindo trade_plan serializado."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    decision = FakeSignalDecision()
    R.add_token_evaluation(decision)

    assert len(R._tokens) == 1
    snap = R._tokens[0]
    assert snap["symbol"] == "BTCUSDT"
    assert snap["action"] == "CALL"
    assert snap["direction"] == "LONG"
    assert snap["signal_tag"] == "cont_pull"
    assert snap["confluent_setups"] == ["cont_pull"]
    assert snap["confidence"] == 82.5
    assert snap["regime"] == "TREND_UP"
    assert snap["skip_reason"] is None

    # Setup results e trade plan serializados
    assert isinstance(snap["all_setup_results"], list)
    assert snap["all_setup_results"][0]["setup_name"] == "cont_pull"
    assert isinstance(snap["all_setup_results"][0]["triggered_at"], str)

    assert isinstance(snap["trade_plan"], dict)
    assert snap["trade_plan"]["entry_price"] == 100.0
    assert snap["trade_plan"]["leverage"] == 5.0


def test_add_token_evaluation_skip(tmp_paths):
    """SKIP grava trade_plan=None e skip_reason populado."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    R.add_token_evaluation(_make_skip_decision())

    snap = R._tokens[0]
    assert snap["action"] == "SKIP"
    assert snap["direction"] is None
    assert snap["trade_plan"] is None
    assert snap["skip_reason"] == "no_setup_triggered"
    assert snap["all_setup_results"][0]["triggered"] is False


def test_add_token_evaluation_handles_timestamp(tmp_paths):
    """``pd.Timestamp`` em decision.timestamp serializa como ISO string."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    ts = pd.Timestamp("2026-04-23T14:15:00")
    decision = FakeSignalDecision(timestamp=ts)
    R.add_token_evaluation(decision)

    snap = R._tokens[0]
    assert isinstance(snap["timestamp"], str)
    # ISO parseável
    datetime.fromisoformat(snap["timestamp"])
    # E o triggered_at do setup result também
    assert isinstance(snap["all_setup_results"][0]["triggered_at"], str)


# ---------------------------------------------------------------------------
# 7. add_near_miss
# ---------------------------------------------------------------------------


def test_add_near_miss(tmp_paths):
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    conditions = [
        {"name": "trend_aligned", "value": 1, "threshold": 1, "operator": ">=",
         "passed": True, "weight": 1.0},
        {"name": "rsi_zone", "value": 55, "threshold": 65, "operator": ">=",
         "passed": False, "weight": 1.0},
    ]
    R.add_near_miss(
        symbol="ETHUSDT",
        closest_setup_name="cont_pull",
        closest_confidence=68.0,
        closest_direction="LONG",
        closest_regime="TREND_UP",
        conditions=conditions,
        ts="2026-04-23T14:15:00-03:00",
    )
    assert len(R._near_misses) == 1
    nm = R._near_misses[0]
    assert nm["symbol"] == "ETHUSDT"
    assert nm["closest_setup_name"] == "cont_pull"
    assert nm["closest_confidence"] == 68.0
    assert nm["closest_direction"] == "LONG"
    assert nm["closest_regime"] == "TREND_UP"
    assert nm["conditions"] == conditions
    assert nm["ts"] == "2026-04-23T14:15:00-03:00"


# ---------------------------------------------------------------------------
# 8. add_event
# ---------------------------------------------------------------------------


def test_add_event_call(tmp_paths):
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    R.add_event(
        type="CALL",
        symbol="BTCUSDT",
        direction="LONG",
        signal_tag="cont_pull+rev_zone",
        confidence=88.5,
        leverage=7.0,
    )
    assert len(R._events) == 1
    ev = R._events[0]
    assert ev["type"] == "CALL"
    assert ev["symbol"] == "BTCUSDT"
    assert ev["direction"] == "LONG"
    assert ev["signal_tag"] == "cont_pull+rev_zone"
    assert ev["confidence"] == 88.5
    assert ev["leverage"] == 7.0


# ---------------------------------------------------------------------------
# 9-10. commit — JSONL + SQLite
# ---------------------------------------------------------------------------


def _populate_full_round(R, *, with_skip: bool = True):
    R.set_meta(
        fgi=55, btc_regime="TREND_UP", btc_change_pct_15m=0.5,
        exchange_primary="OKX",
        setups_enabled={"cont_pull": True}, btc_filter_blocked=False,
    )
    R.set_pipeline(
        universe=100, tokens_evaluated=80,
        calls_emitted=1, setups_triggered_total=2, near_misses_count=1,
    )
    R.set_exec_seconds(12.34)
    R.add_token_evaluation(FakeSignalDecision())
    if with_skip:
        R.add_token_evaluation(_make_skip_decision())
    R.add_near_miss(
        symbol="DOGEUSDT", closest_setup_name="rev_zone",
        closest_confidence=45.0, closest_direction="SHORT",
        closest_regime="RANGE", conditions=[{"name": "x", "passed": False}],
        ts=R.ts,
    )
    R.add_event(
        type="CALL", symbol="BTCUSDT", direction="LONG",
        signal_tag="cont_pull", confidence=82.5, leverage=5.0,
    )


def test_commit_writes_jsonl(tmp_paths):
    """Após commit, arquivo JSONL existe com uma linha JSON válida."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R)
    assert R.commit() is True

    assert os.path.exists(tmp_paths["jsonl"])
    with open(tmp_paths["jsonl"], "r", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["round_id"] == R.round_id
    assert record["version"] == "9.0.0-beta"
    assert record["meta"]["fgi"] == 55
    assert record["pipeline"]["calls_emitted"] == 1
    assert len(record["tokens"]) == 2
    assert len(record["near_misses"]) == 1
    assert len(record["events"]) == 1


def test_commit_writes_sqlite(tmp_paths):
    """Após commit, SQLite existe com 4 tabelas e linhas presentes."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R)
    assert R.commit() is True

    assert os.path.exists(tmp_paths["db"])
    conn = sqlite3.connect(tmp_paths["db"])
    try:
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert {"rounds", "token_evaluations", "near_misses", "round_events"} <= tables

        assert conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM token_evaluations").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM near_misses").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM round_events").fetchone()[0] == 1

        row = conn.execute(
            "SELECT fgi, btc_regime, exchange_primary, calls_emitted, near_misses_count "
            "FROM rounds"
        ).fetchone()
        assert row == (55, "TREND_UP", "OKX", 1, 1)

        # Trade plan persistido como JSON
        tp_json = conn.execute(
            "SELECT trade_plan FROM token_evaluations WHERE action='CALL'"
        ).fetchone()[0]
        assert tp_json is not None
        tp = json.loads(tp_json)
        assert tp["entry_price"] == 100.0

        # SKIP tem trade_plan NULL
        tp_skip = conn.execute(
            "SELECT trade_plan FROM token_evaluations WHERE action='SKIP'"
        ).fetchone()[0]
        assert tp_skip is None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 11. Idempotência
# ---------------------------------------------------------------------------


def test_commit_idempotent(tmp_paths):
    """Chamar commit duas vezes não duplica linhas (JSONL nem SQLite)."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R)
    assert R.commit() is True
    assert R.commit() is True  # no-op

    with open(tmp_paths["jsonl"], "r", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    assert len(lines) == 1

    conn = sqlite3.connect(tmp_paths["db"])
    try:
        assert conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM token_evaluations").fetchone()[0] == 2
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 12. JSONL fallback quando SQLite falha
# ---------------------------------------------------------------------------


def test_commit_jsonl_fallback_on_sqlite_error(tmp_paths, monkeypatch):
    """Se sqlite3.connect falhar, JSONL ainda é gravado e commit retorna False."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R)

    def _boom(*args, **kwargs):
        raise sqlite3.OperationalError("boom — disco simulado cheio")

    monkeypatch.setattr(tmp_paths["logger"].sqlite3, "connect", _boom)
    assert R.commit() is False  # SQLite falhou

    # JSONL ainda foi escrito
    assert os.path.exists(tmp_paths["jsonl"])
    with open(tmp_paths["jsonl"], "r", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["round_id"] == R.round_id


# ---------------------------------------------------------------------------
# 13-16. rebuild_db
# ---------------------------------------------------------------------------


def test_rebuild_db_empty_jsonl(tmp_paths):
    """JSONL inexistente: 0 rodadas, DB criado com tabelas vazias."""
    n = tmp_paths["logger"].rebuild_db(
        jsonl_path=tmp_paths["jsonl"], db_path=tmp_paths["db"]
    )
    assert n == 0
    assert os.path.exists(tmp_paths["db"])
    conn = sqlite3.connect(tmp_paths["db"])
    try:
        assert conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM token_evaluations").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM near_misses").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM round_events").fetchone()[0] == 0
    finally:
        conn.close()


def test_rebuild_db_single_record(tmp_paths):
    """1 linha no JSONL → 1 rodada no DB com tokens/near_misses/events relacionados."""
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R)
    R.commit()

    # Apaga o DB e reconstrói só do JSONL
    os.remove(tmp_paths["db"])
    n = tmp_paths["logger"].rebuild_db(
        jsonl_path=tmp_paths["jsonl"], db_path=tmp_paths["db"]
    )
    assert n == 1

    conn = sqlite3.connect(tmp_paths["db"])
    try:
        assert conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM token_evaluations").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM near_misses").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM round_events").fetchone()[0] == 1
        # FK consistency: round_id presente nas tabelas filhas casa com rounds.round_id
        rid = conn.execute("SELECT round_id FROM rounds").fetchone()[0]
        for table in ("token_evaluations", "near_misses", "round_events"):
            rows = conn.execute(
                f"SELECT DISTINCT round_id FROM {table}"
            ).fetchall()
            assert all(r[0] == rid for r in rows)
    finally:
        conn.close()


def test_rebuild_db_handles_malformed_line(tmp_paths, caplog):
    """Mistura linhas válidas e inválidas: válidas inseridas, inválidas contadas."""
    # Gera JSONL manualmente — duas válidas e uma corrompida no meio.
    os.makedirs(os.path.dirname(tmp_paths["jsonl"]), exist_ok=True)
    R1 = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R1, with_skip=False)
    record1 = {
        "ts": R1.ts, "version": R1.version, "round_id": R1.round_id,
        "meta": R1._meta, "pipeline": R1._pipeline,
        "tokens": R1._tokens, "near_misses": R1._near_misses, "events": R1._events,
    }
    record2 = dict(record1)
    record2["round_id"] = R1.round_id + "_2"
    with open(tmp_paths["jsonl"], "w", encoding="utf-8") as f:
        f.write(json.dumps(record1, ensure_ascii=False, default=str) + "\n")
        f.write("{not valid json[[\n")
        f.write(json.dumps(record2, ensure_ascii=False, default=str) + "\n")

    with caplog.at_level("WARNING"):
        n = tmp_paths["logger"].rebuild_db(
            jsonl_path=tmp_paths["jsonl"], db_path=tmp_paths["db"]
        )
    assert n == 2

    conn = sqlite3.connect(tmp_paths["db"])
    try:
        assert conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0] == 2
    finally:
        conn.close()


def test_rebuild_db_drops_existing_tables(tmp_paths):
    """DB existente com schema diferente é dropado e recriado."""
    # Cria DB com schema "errado" (rounds com 1 coluna)
    os.makedirs(os.path.dirname(tmp_paths["db"]), exist_ok=True)
    conn = sqlite3.connect(tmp_paths["db"])
    try:
        conn.executescript("""
            CREATE TABLE rounds (legacy_only TEXT);
            CREATE TABLE token_evaluations (legacy_only TEXT);
            CREATE TABLE near_misses (legacy_only TEXT);
            CREATE TABLE round_events (legacy_only TEXT);
            INSERT INTO rounds VALUES ('garbage');
        """)
        conn.commit()
    finally:
        conn.close()

    # JSONL com 1 rodada
    R = tmp_paths["logger"].RoundLoggerV9(version="9.0.0-beta")
    _populate_full_round(R)
    record = {
        "ts": R.ts, "version": R.version, "round_id": R.round_id,
        "meta": R._meta, "pipeline": R._pipeline,
        "tokens": R._tokens, "near_misses": R._near_misses, "events": R._events,
    }
    os.makedirs(os.path.dirname(tmp_paths["jsonl"]), exist_ok=True)
    with open(tmp_paths["jsonl"], "w", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    n = tmp_paths["logger"].rebuild_db(
        jsonl_path=tmp_paths["jsonl"], db_path=tmp_paths["db"]
    )
    assert n == 1

    conn = sqlite3.connect(tmp_paths["db"])
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(rounds)").fetchall()}
        assert "fgi" in cols
        assert "btc_regime" in cols
        assert "legacy_only" not in cols

        # Linha "garbage" foi descartada com o DROP
        assert conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0] == 1
        assert conn.execute("SELECT version FROM rounds").fetchone()[0] == "9.0.0-beta"
    finally:
        conn.close()
