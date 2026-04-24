"""Testes de exchanges.py — foca apenas no que foi adicionado/adaptado em v9.

Não refaz testes para código portado literal do v8 (hierarquia OKX→Gate→Bitget,
retry, cache em disco, filtro confirm=0 etc) — esse código roda em produção
há meses.
"""

from __future__ import annotations

import asyncio
import inspect

import pandas as pd
import pytest

import exchanges


# ---------------------------------------------------------------------------
# klines_to_dataframe
# ---------------------------------------------------------------------------


def _sample_klines(n: int = 10) -> list[dict]:
    base_ts = 1_700_000_000_000  # ms
    step_ms = 15 * 60 * 1000
    return [
        {
            "ts": base_ts + i * step_ms,
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 + i,
            "close": 100.5 + i,
            "volume": 1_000.0 + i,
        }
        for i in range(n)
    ]


def test_klines_to_dataframe_basic():
    klines = _sample_klines(10)
    df = exchanges.klines_to_dataframe(klines)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert isinstance(df.index, pd.DatetimeIndex)


def test_klines_to_dataframe_empty():
    df = exchanges.klines_to_dataframe([])

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_klines_to_dataframe_dtypes():
    df = exchanges.klines_to_dataframe(_sample_klines(5))
    for col in ["open", "high", "low", "close", "volume"]:
        assert df[col].dtype == float


def test_klines_to_dataframe_index_ascending():
    klines = _sample_klines(8)
    df = exchanges.klines_to_dataframe(klines)
    assert df.index.is_monotonic_increasing


# ---------------------------------------------------------------------------
# get/reset data_source_attempts
# ---------------------------------------------------------------------------


def test_get_data_source_attempts_returns_copy():
    exchanges.reset_data_source_attempts()
    exchanges._data_source_attempts.append({"fonte": "test"})

    out = exchanges.get_data_source_attempts()
    out.append({"fonte": "should-not-leak"})

    # Modificar o retorno não deve afetar o estado interno
    assert len(exchanges._data_source_attempts) == 1
    assert exchanges._data_source_attempts[0]["fonte"] == "test"

    exchanges.reset_data_source_attempts()


def test_reset_data_source_attempts():
    exchanges._data_source_attempts.append({"fonte": "x"})
    exchanges._data_source_attempts.append({"fonte": "y"})
    assert len(exchanges._data_source_attempts) >= 2

    exchanges.reset_data_source_attempts()

    assert exchanges.get_data_source_attempts() == []


# ---------------------------------------------------------------------------
# fetch_btc_context
# ---------------------------------------------------------------------------


def test_fetch_btc_context_structure(monkeypatch):
    klines = _sample_klines(20)

    async def _fake_fetch(session, symbol, granularity, limit):
        assert symbol == "BTCUSDT"
        assert granularity == "15m"
        return klines

    monkeypatch.setattr(exchanges, "fetch_klines_cached_async", _fake_fetch)

    result = asyncio.run(exchanges.fetch_btc_context(session=None))

    assert set(result.keys()) == {"symbol", "change_pct_15m", "atr_pct"}
    assert result["symbol"] == "BTCUSDT"
    assert isinstance(result["change_pct_15m"], float)
    assert isinstance(result["atr_pct"], float)
    # Com amostra monotônica crescente, change_pct deve ser positivo
    assert result["change_pct_15m"] > 0
    assert result["atr_pct"] > 0


def test_fetch_btc_context_empty_klines(monkeypatch):
    async def _fake_fetch(session, symbol, granularity, limit):
        return []

    monkeypatch.setattr(exchanges, "fetch_klines_cached_async", _fake_fetch)

    result = asyncio.run(exchanges.fetch_btc_context(session=None))

    assert result == {"symbol": "BTCUSDT", "change_pct_15m": 0.0, "atr_pct": 0.0}


# ---------------------------------------------------------------------------
# Cache TTL para TFs curtos
# ---------------------------------------------------------------------------


def test_kline_cache_ttl_for_short_tfs_is_zero():
    """Verifica que o fonte de fetch_klines_cached_async trata 15m/5m/1m
    como sempre-fresh (TTL=0), e outros TFs usam KLINE_CACHE_TTL_H."""
    src = inspect.getsource(exchanges.fetch_klines_cached_async)
    # A linha de TTL deve testar o set completo {15m, 5m, 1m}
    assert 'granularity in ("15m", "5m", "1m")' in src
    assert "KLINE_CACHE_TTL_H" in src


# ---------------------------------------------------------------------------
# _parse_retry_after
# ---------------------------------------------------------------------------


def test_parse_retry_after_seconds():
    assert exchanges._parse_retry_after("30", fallback=2) == 30.0


def test_parse_retry_after_empty_uses_fallback():
    assert exchanges._parse_retry_after("", fallback=4) == 4.0


def test_parse_retry_after_invalid_uses_fallback():
    assert exchanges._parse_retry_after("abc", fallback=5) == 5.0


def test_parse_retry_after_caps_at_60():
    assert exchanges._parse_retry_after("300", fallback=2) == 60.0


def test_parse_retry_after_caps_at_1_minimum():
    assert exchanges._parse_retry_after("0", fallback=2) == 1.0


# ---------------------------------------------------------------------------
# api_get_async — 429 handling
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status: int, body: bytes = b"{}", headers: dict | None = None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def read(self):
        return self._body


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, timeout=None, headers=None):
        self.calls += 1
        return self._responses.pop(0)


def test_api_get_async_429_respects_retry_after(monkeypatch):
    """429 com Retry-After=1 deve causar sleep >= 1.0 antes de retry; 200 retorna dados."""
    slept: list[float] = []

    async def _fake_sleep(delay):
        slept.append(delay)

    monkeypatch.setattr(exchanges.asyncio, "sleep", _fake_sleep)

    session = _FakeSession([
        _FakeResponse(429, b"{}", headers={"Retry-After": "1"}),
        _FakeResponse(200, b'{"ok": true}'),
    ])

    result = asyncio.run(exchanges.api_get_async(session, "http://x/test"))

    assert result == {"ok": True}
    assert len(slept) == 1
    assert slept[0] >= 1.0


def test_api_get_async_429_without_retry_after_uses_exponential(monkeypatch):
    """429 sem Retry-After deve cair no fallback exponencial (2^i, mínimo 1.0)."""
    slept: list[float] = []

    async def _fake_sleep(delay):
        slept.append(delay)

    monkeypatch.setattr(exchanges.asyncio, "sleep", _fake_sleep)

    session = _FakeSession([
        _FakeResponse(429, b"{}"),
        _FakeResponse(200, b'{"ok": 1}'),
    ])

    result = asyncio.run(exchanges.api_get_async(session, "http://x/y"))

    assert result == {"ok": 1}
    assert len(slept) == 1
    # Primeira tentativa i=0 → fallback 2^0=1, clamp mínimo 1.0
    assert slept[0] == 1.0
