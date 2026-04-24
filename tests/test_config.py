"""Testes de config.py — validações simples dos ajustes v9."""

from __future__ import annotations

import config


def test_version_is_v9_beta():
    assert config.VERSION == "9.0.0-beta"


def test_setups_enabled_has_all_five():
    assert isinstance(config.SETUPS_ENABLED, dict)
    assert set(config.SETUPS_ENABLED.keys()) == {
        "cont_pull",
        "rev_exaust",
        "break_range",
        "rev_zone",
        "breaker",
    }


def test_min_turnover_lowered():
    assert config.MIN_TURNOVER_24H == 1_000_000


def test_min_oi_lowered():
    assert config.MIN_OI_USD == 3_000_000


def test_kline_top_n_is_none():
    assert config.KLINE_TOP_N is None
    assert config.KLINE_TOP_N_LIGHT is None


def test_no_tradingview_url():
    assert "tradingview" not in config.URLS


def test_bitget_product_type_preserved():
    assert config.BITGET_PRODUCT_TYPE == "USDT-FUTURES"


def test_v9_paths_isolated():
    assert config.STATE_FILE_V9 != config.STATE_FILE
    assert config.SCAN_LOG_DB_V9.endswith("scan_log_v9.db")
    assert config.JOURNAL_DB_V9.endswith("atirador_journal_v9.db")
    assert config.SCAN_LOG_JSONL_V9.endswith("scan_log_v9.jsonl")
    assert "_v9" in config.STATE_FILE_V9


def test_kline_limits_are_150():
    assert config.KLINE_LIMIT_15M == 150
    assert config.KLINE_LIMIT_1H == 150
    assert config.KLINE_LIMIT_4H == 150
    assert config.KLINE_LIMIT_5M == 150
    assert config.KLINE_LIMIT_1M == 150


def test_max_concurrent_fetches_is_5():
    assert config.MAX_CONCURRENT_FETCHES == 5
