"""Testes do módulo state (v9) — persistência de estado multi-setup.

Cobre os 18 casos obrigatórios do briefing 12a: default, corrupção,
rejeição de schema v8, roundtrip, truncamento, cleanup por TTL,
``get_recent_setups`` e ``get_oi_trend``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import pytest

import state as state_mod
from config import BRT, OI_HISTORY_MAX_ROUNDS, SETUPS_HISTORY_MAX_ROUNDS, STATE_TTL_HOURS


# ---------------------------------------------------------------------------
# Fixture — redireciona STATE_FILE_V9 para diretório temporário
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_state_file(tmp_path, monkeypatch):
    """Redireciona ``STATE_FILE_V9`` para dentro de ``tmp_path``."""
    path = tmp_path / "states" / "atirador_state_v9.json"
    monkeypatch.setattr(state_mod, "STATE_FILE_V9", str(path))
    return str(path)


# ---------------------------------------------------------------------------
# FakeSignalDecision — não acopla ao signals.py real
# ---------------------------------------------------------------------------


@dataclass
class FakeSignalDecision:
    symbol: str
    regime: str = "TREND_UP"
    action: str = "CALL"
    direction: Optional[str] = "LONG"
    signal_tag: Optional[str] = "cont_pull"
    confluent_setups: list = field(default_factory=lambda: ["cont_pull"])
    confidence: float = 82.5
    skip_reason: Optional[str] = None


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# ---------------------------------------------------------------------------
# 1-3. load_state
# ---------------------------------------------------------------------------


def test_load_state_default(tmp_state_file):
    """Arquivo inexistente retorna default com todas as chaves esperadas."""
    assert not os.path.exists(tmp_state_file)
    s = state_mod.load_state()
    assert set(s.keys()) == {"version", "date", "last_run", "setups_history", "oi_history"}
    assert s["setups_history"] == {}
    assert s["oi_history"] == {}
    assert s["last_run"] is None
    assert s["version"].startswith("9.")


def test_load_state_corrupted(tmp_state_file, caplog):
    """JSON inválido no arquivo retorna default sem crash e loga warning."""
    os.makedirs(os.path.dirname(tmp_state_file), exist_ok=True)
    with open(tmp_state_file, "w") as f:
        f.write("{not valid json[[")

    with caplog.at_level("WARNING", logger="atirador"):
        s = state_mod.load_state()

    assert set(s.keys()) == {"version", "date", "last_run", "setups_history", "oi_history"}
    assert s["setups_history"] == {}
    assert any("Erro ao carregar" in rec.message for rec in caplog.records)


def test_load_state_v8_schema_rejected(tmp_state_file, caplog):
    """Arquivo com chave ``score_history`` (schema v8) é rejeitado com erro."""
    os.makedirs(os.path.dirname(tmp_state_file), exist_ok=True)
    v8_payload = {
        "date": "2026-04-23",
        "score_history": {
            "BTCUSDT": {"scores": [], "oi_history": []},
        },
    }
    with open(tmp_state_file, "w") as f:
        json.dump(v8_payload, f)

    with caplog.at_level("ERROR", logger="atirador"):
        s = state_mod.load_state()

    assert "score_history" not in s
    assert s["setups_history"] == {}
    assert any("v8 detectado" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# 4-5. save_state
# ---------------------------------------------------------------------------


def test_save_and_load_roundtrip(tmp_state_file):
    """``save_state`` seguido de ``load_state`` preserva dados."""
    s = state_mod.load_state()
    s["setups_history"]["BTCUSDT"] = {
        "rounds": [{"ts": "2026-04-23T14:00:00-03:00", "action": "CALL"}]
    }
    s["oi_history"]["BTCUSDT"] = [{"ts": "2026-04-23T14:00:00-03:00", "oi_usd": 1_000_000.0}]
    state_mod.save_state(s)

    reloaded = state_mod.load_state()
    assert reloaded["setups_history"]["BTCUSDT"]["rounds"][0]["action"] == "CALL"
    assert reloaded["oi_history"]["BTCUSDT"][0]["oi_usd"] == 1_000_000.0


def test_save_state_updates_last_run(tmp_state_file):
    """``save_state`` preenche ``last_run`` com timestamp ISO."""
    s = state_mod.load_state()
    assert s["last_run"] is None
    state_mod.save_state(s)
    assert s["last_run"] is not None
    datetime.fromisoformat(s["last_run"])  # parse válido


# ---------------------------------------------------------------------------
# 6-8. update_setups_history
# ---------------------------------------------------------------------------


def test_update_setups_history_call(tmp_state_file):
    """Snapshot de CALL preserva direction, signal_tag e confluent_setups."""
    s = state_mod.load_state()
    decision = FakeSignalDecision(symbol="BTCUSDT")
    ts = "2026-04-23T14:15:00-03:00"

    state_mod.update_setups_history(s, [decision], ts)

    rounds = s["setups_history"]["BTCUSDT"]["rounds"]
    assert len(rounds) == 1
    snap = rounds[0]
    assert snap["ts"] == ts
    assert snap["action"] == "CALL"
    assert snap["direction"] == "LONG"
    assert snap["signal_tag"] == "cont_pull"
    assert snap["confluent_setups"] == ["cont_pull"]
    assert snap["confidence"] == 82.5
    assert snap["skip_reason"] is None


def test_update_setups_history_skip(tmp_state_file):
    """SKIP grava direction/signal_tag None e preserva skip_reason."""
    s = state_mod.load_state()
    decision = FakeSignalDecision(
        symbol="ETHUSDT",
        action="SKIP",
        direction=None,
        signal_tag=None,
        confluent_setups=[],
        confidence=0.0,
        skip_reason="no_setup_triggered",
    )
    state_mod.update_setups_history(s, [decision], "2026-04-23T14:00:00-03:00")

    snap = s["setups_history"]["ETHUSDT"]["rounds"][0]
    assert snap["action"] == "SKIP"
    assert snap["direction"] is None
    assert snap["signal_tag"] is None
    assert snap["confluent_setups"] == []
    assert snap["skip_reason"] == "no_setup_triggered"


def test_update_setups_history_truncation(tmp_state_file):
    """Exceder MAX_ROUNDS mantém apenas os últimos MAX."""
    s = state_mod.load_state()
    decision = FakeSignalDecision(symbol="BTCUSDT")
    total = SETUPS_HISTORY_MAX_ROUNDS + 10

    base = datetime(2026, 4, 23, tzinfo=BRT)
    for i in range(total):
        ts = _iso(base + timedelta(minutes=15 * i))
        state_mod.update_setups_history(s, [decision], ts)

    rounds = s["setups_history"]["BTCUSDT"]["rounds"]
    assert len(rounds) == SETUPS_HISTORY_MAX_ROUNDS
    first_kept_ts = rounds[0]["ts"]
    expected_first = _iso(base + timedelta(minutes=15 * (total - SETUPS_HISTORY_MAX_ROUNDS)))
    assert first_kept_ts == expected_first


# ---------------------------------------------------------------------------
# 9. update_oi_history
# ---------------------------------------------------------------------------


def test_update_oi_history_basic(tmp_state_file):
    """``update_oi_history`` anexa {ts, oi_usd} e trunca em OI_HISTORY_MAX_ROUNDS."""
    s = state_mod.load_state()
    perps = [
        {"symbol": "BTCUSDT", "oi_usd": 12_000_000.0},
        {"symbol": "ETHUSDT", "oi_usd": 8_000_000.0},
    ]
    state_mod.update_oi_history(s, perps, "2026-04-23T14:00:00-03:00")
    state_mod.update_oi_history(s, perps, "2026-04-23T14:15:00-03:00")

    assert len(s["oi_history"]["BTCUSDT"]) == 2
    assert s["oi_history"]["BTCUSDT"][-1]["oi_usd"] == 12_000_000.0
    assert s["oi_history"]["ETHUSDT"][0]["ts"] == "2026-04-23T14:00:00-03:00"

    # Truncamento
    for i in range(OI_HISTORY_MAX_ROUNDS + 5):
        state_mod.update_oi_history(s, [{"symbol": "BTCUSDT", "oi_usd": float(i)}], f"2026-04-23T{i:02d}")
    assert len(s["oi_history"]["BTCUSDT"]) == OI_HISTORY_MAX_ROUNDS


# ---------------------------------------------------------------------------
# 10-12. cleanup_state
# ---------------------------------------------------------------------------


def test_cleanup_state_removes_stale(tmp_state_file):
    """Símbolos com última entrada > TTL são removidos."""
    s = state_mod.load_state()
    old_ts = _iso(datetime.now(BRT) - timedelta(hours=STATE_TTL_HOURS + 2))
    s["setups_history"]["STALE"] = {
        "rounds": [{"ts": old_ts, "action": "CALL"}]
    }
    s["oi_history"]["STALE"] = [{"ts": old_ts, "oi_usd": 1.0}]

    state_mod.cleanup_state(s)

    assert "STALE" not in s["setups_history"]
    assert "STALE" not in s["oi_history"]


def test_cleanup_state_keeps_fresh(tmp_state_file):
    """Símbolos com timestamp recente são preservados."""
    s = state_mod.load_state()
    fresh_ts = _iso(datetime.now(BRT) - timedelta(minutes=30))
    s["setups_history"]["FRESH"] = {
        "rounds": [{"ts": fresh_ts, "action": "CALL"}]
    }
    s["oi_history"]["FRESH"] = [{"ts": fresh_ts, "oi_usd": 1.0}]

    state_mod.cleanup_state(s)

    assert "FRESH" in s["setups_history"]
    assert "FRESH" in s["oi_history"]


def test_cleanup_state_handles_bad_ts(tmp_state_file):
    """Timestamp não-parseável → símbolo removido das duas estruturas."""
    s = state_mod.load_state()
    s["setups_history"]["BADTS"] = {
        "rounds": [{"ts": "not-a-timestamp", "action": "CALL"}]
    }
    s["oi_history"]["BADTS"] = [{"ts": "nope", "oi_usd": 1.0}]

    state_mod.cleanup_state(s)

    assert "BADTS" not in s["setups_history"]
    assert "BADTS" not in s["oi_history"]


# ---------------------------------------------------------------------------
# 13-14. get_recent_setups
# ---------------------------------------------------------------------------


def test_get_recent_setups(tmp_state_file):
    """Retorna últimos N, ordem mais recente primeiro."""
    s = state_mod.load_state()
    decision = FakeSignalDecision(symbol="BTCUSDT")
    base = datetime(2026, 4, 23, tzinfo=BRT)
    for i in range(10):
        state_mod.update_setups_history(
            s, [decision], _iso(base + timedelta(minutes=15 * i))
        )

    recent = state_mod.get_recent_setups(s, "BTCUSDT", n=3)
    assert len(recent) == 3
    # mais recente primeiro (i=9 > i=8 > i=7)
    assert recent[0]["ts"] == _iso(base + timedelta(minutes=15 * 9))
    assert recent[1]["ts"] == _iso(base + timedelta(minutes=15 * 8))
    assert recent[2]["ts"] == _iso(base + timedelta(minutes=15 * 7))


def test_get_recent_setups_missing_symbol(tmp_state_file):
    """Símbolo inexistente retorna lista vazia."""
    s = state_mod.load_state()
    assert state_mod.get_recent_setups(s, "NOPEUSDT", n=5) == []


# ---------------------------------------------------------------------------
# 15-18. get_oi_trend
# ---------------------------------------------------------------------------


def _build_oi_state(values: list[float]) -> dict:
    s = state_mod._default_state()
    base = datetime(2026, 4, 23, tzinfo=BRT)
    s["oi_history"]["BTCUSDT"] = [
        {"ts": _iso(base + timedelta(minutes=15 * i)), "oi_usd": v}
        for i, v in enumerate(values)
    ]
    return s


def test_get_oi_trend_up():
    """OI crescente (último lookback > anterior em mais de 3%) → UP."""
    s = _build_oi_state([100.0, 100.0, 100.0, 110.0, 110.0, 110.0])
    assert state_mod.get_oi_trend(s, "BTCUSDT", lookback=3) == "UP"


def test_get_oi_trend_down():
    """OI decrescente (último lookback < anterior em mais de 3%) → DOWN."""
    s = _build_oi_state([110.0, 110.0, 110.0, 100.0, 100.0, 100.0])
    assert state_mod.get_oi_trend(s, "BTCUSDT", lookback=3) == "DOWN"


def test_get_oi_trend_stable():
    """Variação dentro da faixa +-3% → STABLE."""
    s = _build_oi_state([100.0, 100.0, 100.0, 101.0, 101.0, 101.0])
    assert state_mod.get_oi_trend(s, "BTCUSDT", lookback=3) == "STABLE"


def test_get_oi_trend_insufficient_data():
    """Menos de 2*lookback entradas → INSUFFICIENT_DATA."""
    s = _build_oi_state([100.0, 101.0, 102.0, 103.0, 104.0])  # 5 < 6
    assert state_mod.get_oi_trend(s, "BTCUSDT", lookback=3) == "INSUFFICIENT_DATA"
    assert state_mod.get_oi_trend(s, "MISSINGUSDT", lookback=3) == "INSUFFICIENT_DATA"
