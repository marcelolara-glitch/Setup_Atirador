"""Testes do orquestrador main v9 (PR 13B).

Cobre os helpers puros (``_detect_near_miss``, ``_build_round_stats``,
``_write_watchdog``, ``_get_open_symbols``) e async (``_fetch_token_klines``),
além do wrapper global de erro que dispara ``notify_error``.

IMPORTANTE: o harness do Claude Code não tem ``pandas_ta_classic``. Instalamos
um stub em ``sys.modules`` antes de importar ``main`` — o stub cobre os três
módulos que importam ``pandas_ta_classic`` no import-time (``indicators``,
``regime``, ``setups.rev_exaust``). Os testes não chamam código que depende de
``pta.*``.
Validação final na VM: ``pytest tests/test_main_v9.py -v``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Stub de pandas_ta_classic — precisa estar em sys.modules antes de importar main.
# ---------------------------------------------------------------------------
if "pandas_ta_classic" not in sys.modules:
    sys.modules["pandas_ta_classic"] = types.ModuleType("pandas_ta_classic")


import main  # noqa: E402
from telegram import RoundStats  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures auxiliares — SignalDecision/SetupResult duck-typed
# ---------------------------------------------------------------------------


def _make_setup_result(
    name: str = "cont_pull",
    triggered: bool = True,
    confidence: float = 70.0,
    direction: str = "LONG",
    conditions: list | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        setup_name=name,
        triggered=triggered,
        confidence=confidence,
        direction=direction,
        conditions=conditions or [],
    )


def _make_decision(
    symbol: str = "BTCUSDT",
    action: str = "CALL",
    triggered_setups: list | None = None,
    confidence: float = 80.0,
    signal_tag: str = "cont_pull",
    direction: str = "LONG",
    regime: str = "TREND_UP",
    skip_reason: str | None = None,
    leverage: float = 5.0,
) -> SimpleNamespace:
    all_results = list(triggered_setups or [])
    trade_plan = SimpleNamespace(leverage=leverage) if action == "CALL" else None
    return SimpleNamespace(
        symbol=symbol,
        timestamp=pd.Timestamp("2026-04-24T12:00:00"),
        timeframe="15m",
        action=action,
        direction=direction,
        signal_tag=signal_tag,
        confluent_setups=[r.setup_name for r in all_results if r.triggered],
        confidence=confidence,
        trade_plan=trade_plan,
        regime=regime,
        all_setup_results=all_results,
        protection_filters={},
        skip_reason=skip_reason,
    )


# ---------------------------------------------------------------------------
# 1-3. _detect_near_miss
# ---------------------------------------------------------------------------


def test_detect_near_miss_skip_with_triggered():
    results = [
        _make_setup_result(name="rev_zone", triggered=True, confidence=55.0),
        _make_setup_result(name="cont_pull", triggered=True, confidence=72.5),
        _make_setup_result(name="breaker", triggered=False, confidence=40.0),
    ]
    d = _make_decision(
        action="SKIP",
        triggered_setups=results,
        confidence=72.5,
        signal_tag="cont_pull+rev_zone",
        skip_reason="low_confidence",
    )
    nm = main._detect_near_miss(d)
    assert nm is not None
    assert nm["symbol"] == "BTCUSDT"
    assert nm["closest_setup_name"] == "cont_pull"
    assert nm["closest_confidence"] == 72.5
    assert nm["closest_direction"] == "LONG"
    assert nm["closest_regime"] == "TREND_UP"
    assert nm["ts"]  # ISO string


def test_detect_near_miss_call_returns_none():
    results = [_make_setup_result(triggered=True, confidence=85.0)]
    d = _make_decision(action="CALL", triggered_setups=results)
    assert main._detect_near_miss(d) is None


def test_detect_near_miss_skip_no_triggered():
    results = [
        _make_setup_result(name="rev_zone", triggered=False, confidence=30.0),
        _make_setup_result(name="breaker", triggered=False, confidence=20.0),
    ]
    d = _make_decision(
        action="SKIP",
        triggered_setups=results,
        skip_reason="no_setup_triggered",
    )
    assert main._detect_near_miss(d) is None


# ---------------------------------------------------------------------------
# 4-6. _build_round_stats
# ---------------------------------------------------------------------------


def test_build_round_stats_empty():
    stats = main._build_round_stats(
        decisions=[],
        perpetuals=[],
        btc_context={"regime": "RANGE", "change_pct_15m": 0.1},
        exchange="OKX",
        fgi=50,
        elapsed=1.2,
    )
    assert isinstance(stats, RoundStats)
    assert stats.n_universe == 0
    assert stats.n_regime_eligible == 0
    assert stats.n_triggered == 0
    assert stats.n_calls == 0
    assert stats.n_near_miss == 0
    assert stats.setups_counter == {}
    assert stats.exchange == "OKX"
    assert stats.btc_regime == "RANGE"
    assert stats.fgi == 50


def test_build_round_stats_mix():
    call = _make_decision(
        symbol="BTCUSDT",
        action="CALL",
        triggered_setups=[_make_setup_result(name="cont_pull", triggered=True)],
    )
    near = _make_decision(
        symbol="ETHUSDT",
        action="SKIP",
        triggered_setups=[_make_setup_result(name="rev_zone", triggered=True, confidence=55.0)],
        skip_reason="low_confidence",
    )
    empty = _make_decision(
        symbol="SOLUSDT",
        action="SKIP",
        triggered_setups=[_make_setup_result(name="breaker", triggered=False)],
        skip_reason="no_setup_triggered",
    )
    perps = [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}, {"symbol": "SOLUSDT"}]

    stats = main._build_round_stats(
        decisions=[call, near, empty],
        perpetuals=perps,
        btc_context={"regime": "TREND_UP", "change_pct_15m": 0.5},
        exchange="OKX",
        fgi=60,
        elapsed=2.0,
    )
    assert stats.n_universe == 3
    assert stats.n_regime_eligible == 3
    assert stats.n_calls == 1
    assert stats.n_triggered == 2
    assert stats.n_near_miss == 1
    assert stats.setups_counter == {"cont_pull": 1, "rev_zone": 1}


def test_build_round_stats_confluence_counts_each_setup():
    call = _make_decision(
        symbol="BTCUSDT",
        action="CALL",
        signal_tag="cont_pull+rev_zone",
        triggered_setups=[
            _make_setup_result(name="cont_pull", triggered=True, confidence=80.0),
            _make_setup_result(name="rev_zone", triggered=True, confidence=70.0),
        ],
    )
    stats = main._build_round_stats(
        decisions=[call],
        perpetuals=[{"symbol": "BTCUSDT"}],
        btc_context={"regime": "TREND_UP"},
        exchange="OKX",
        fgi=55,
        elapsed=1.0,
    )
    assert stats.n_calls == 1
    assert stats.setups_counter == {"cont_pull": 1, "rev_zone": 1}


# ---------------------------------------------------------------------------
# 7-8. _fetch_token_klines
# ---------------------------------------------------------------------------


def test_fetch_token_klines_all_empty_on_failure(monkeypatch):
    async def _boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(main, "fetch_klines_cached_async", _boom)

    async def _run():
        sem = asyncio.Semaphore(5)
        return await main._fetch_token_klines(session=None, symbol="BTCUSDT", sem=sem)

    result = asyncio.run(_run())
    assert set(result.keys()) == {"df_15m", "df_1h", "df_4h", "df_5m", "df_1m"}
    for df in result.values():
        assert isinstance(df, pd.DataFrame)
        assert df.empty


def test_fetch_token_klines_partial_success(monkeypatch):
    fake_klines = [
        {"ts": 1, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 100.0},
        {"ts": 2, "open": 1.05, "high": 1.2, "low": 1.0, "close": 1.15, "volume": 120.0},
    ]

    async def _fake_fetch(session, symbol, gran, limit):
        if gran == "15m":
            return fake_klines
        return []

    fake_df = pd.DataFrame(fake_klines).set_index("ts")

    def _fake_to_df(klines):
        if not klines:
            return pd.DataFrame()
        return fake_df

    monkeypatch.setattr(main, "fetch_klines_cached_async", _fake_fetch)
    monkeypatch.setattr(main, "klines_to_dataframe", _fake_to_df)

    async def _run():
        sem = asyncio.Semaphore(5)
        return await main._fetch_token_klines(session=None, symbol="BTCUSDT", sem=sem)

    result = asyncio.run(_run())
    assert not result["df_15m"].empty
    assert result["df_1h"].empty
    assert result["df_4h"].empty
    assert result["df_5m"].empty
    assert result["df_1m"].empty


# ---------------------------------------------------------------------------
# 9-10. _write_watchdog
# ---------------------------------------------------------------------------


def test_write_watchdog_happy_path(tmp_path, monkeypatch):
    wd_path = str(tmp_path / "watchdog.json")
    monkeypatch.setattr(main, "_WATCHDOG_PATH", wd_path)
    main._write_watchdog()
    assert os.path.exists(wd_path)
    with open(wd_path) as f:
        payload = json.load(f)
    assert "last_run" in payload
    assert "version" in payload
    assert payload["version"].startswith("v")


def test_write_watchdog_silent_failure(monkeypatch):
    # Caminho que não pode ser criado (diretório inexistente + permissão)
    monkeypatch.setattr(main, "_WATCHDOG_PATH", "/nonexistent_dir_xyz/watchdog.json")
    main._write_watchdog()  # não deve levantar


# ---------------------------------------------------------------------------
# 11. _get_open_symbols
# ---------------------------------------------------------------------------


def test_get_open_symbols_empty_journal():
    from journal import TradeJournalV9
    journal = TradeJournalV9(db_path=":memory:")
    assert main._get_open_symbols(journal) == []


# ---------------------------------------------------------------------------
# 12. Entry-point: wrapper global dispara notify_error
# ---------------------------------------------------------------------------


def test_main_error_wrapper_calls_notify_error(monkeypatch, caplog):
    def _boom(_coro):
        raise RuntimeError("explosion in scan")

    # Substitui run_scan_async por uma função síncrona (evita warning de
    # coroutine não aguardada) — o wrapper captura o raise vindo de asyncio.run
    monkeypatch.setattr(main, "run_scan_async", lambda: None)
    monkeypatch.setattr(main.asyncio, "run", _boom)

    captured = {}

    def _fake_notify_error(title, error_message, context=None):
        captured["title"] = title
        captured["msg"] = error_message
        captured["context"] = context
        return True

    monkeypatch.setattr(main, "notify_error", _fake_notify_error)

    # Evita mexer no logging real do harness
    monkeypatch.setattr(main, "setup_logger", lambda: (main.LOG, "", ""))

    # --once só para passar o argparse; o wrapper captura a exception de asyncio.run
    monkeypatch.setattr(sys, "argv", ["main.py", "--once"])

    with pytest.raises(SystemExit) as excinfo:
        main.main()

    assert excinfo.value.code == 1
    assert captured.get("title") == "Falha na rodada"
    assert "explosion in scan" in (captured.get("msg") or "")
    ctx = captured.get("context") or {}
    assert "version" in ctx
    assert "ts" in ctx
