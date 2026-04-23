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
