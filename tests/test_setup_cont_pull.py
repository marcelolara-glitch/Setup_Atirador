"""Testes do setup cont_pull e dos helpers de :mod:`setups.base`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import pytest

from setups.base import (
    SetupResult,
    calculate_setup_confidence,
    evaluate_condition,
)
from setups.cont_pull import evaluate_cont_pull


# ---------------------------------------------------------------------------
# Fakes locais de MarketContext e RegimeClassification
# ---------------------------------------------------------------------------
# As dataclasses reais vivem em indicators.py e regime.py (foundation v9).
# Os setups só dependem de duck typing sobre os atributos acessados, então
# estes stubs mínimos bastam para exercitar a lógica.


@dataclass
class FakeCtx:
    symbol: str
    timestamp: pd.Timestamp
    close: float
    ema21: float
    ema50: float
    volume_ratio: float
    rsi_14: float


@dataclass
class FakeRegime:
    regime: str
    adx_value: float = 30.0
    ema_slope: float = 0.5
    ema_alignment: str = "bullish"
    bb_width_ratio: float = 1.2
    confidence: float = 70.0


# ---------------------------------------------------------------------------
# Helpers de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_flat_df(
    n: int = 100,
    close_price: float = 100.0,
    last_low: Optional[float] = None,
    last_high: Optional[float] = None,
    last_close: Optional[float] = None,
    volume: float = 1000.0,
) -> pd.DataFrame:
    """DF OHLCV com preço constante em ``close_price`` e override opcional na última vela."""
    rows = []
    for _ in range(n):
        rows.append(
            {
                "open": close_price,
                "high": close_price + 0.5,
                "low": close_price - 0.5,
                "close": close_price,
                "volume": volume,
            }
        )
    # override da última vela (índice -1) para controlar touch e close
    if last_low is not None:
        rows[-1]["low"] = last_low
    if last_high is not None:
        rows[-1]["high"] = last_high
    if last_close is not None:
        rows[-1]["close"] = last_close

    df = pd.DataFrame(rows)
    df.index = pd.date_range("2025-01-01", periods=n, freq="15min")
    return df


def _build_ctx(
    close: float = 101.0,
    ema21: float = 100.0,
    ema50: float = 95.0,
    volume_ratio: float = 1.3,
    rsi_14: float = 50.0,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        close=close,
        ema21=ema21,
        ema50=ema50,
        volume_ratio=volume_ratio,
        rsi_14=rsi_14,
    )


# ---------------------------------------------------------------------------
# 1. Helper evaluate_condition
# ---------------------------------------------------------------------------


def test_evaluate_condition_helper():
    """evaluate_condition deve suportar todos os operadores e retornar dict completo."""
    r = evaluate_condition("gt", 10, 5, operator=">", weight=1.5)
    assert r == {
        "name": "gt",
        "value": 10,
        "threshold": 5,
        "operator": ">",
        "passed": True,
        "weight": 1.5,
    }

    assert evaluate_condition("ge_pass", 5, 5, operator=">=")["passed"] is True
    assert evaluate_condition("lt_pass", 3, 5, operator="<")["passed"] is True
    assert evaluate_condition("le_pass", 5, 5, operator="<=")["passed"] is True
    assert evaluate_condition("eq_pass", 5, 5, operator="==")["passed"] is True
    assert evaluate_condition("gt_fail", 3, 5, operator=">")["passed"] is False

    with pytest.raises(ValueError):
        evaluate_condition("bad", 1, 1, operator="!!")


# ---------------------------------------------------------------------------
# 2. Helper calculate_setup_confidence
# ---------------------------------------------------------------------------


def test_calculate_setup_confidence_weighted():
    """Peso maior em uma condition deve dominar o score."""
    conds = [
        {"passed": True, "weight": 3.0},
        {"passed": False, "weight": 1.0},
    ]
    # 3 / 4 = 75%
    assert calculate_setup_confidence(conds) == 75.0

    # Espelho: falha pesada, passa leve
    conds2 = [
        {"passed": False, "weight": 3.0},
        {"passed": True, "weight": 1.0},
    ]
    assert calculate_setup_confidence(conds2) == 25.0

    # Todos passam
    all_pass = [{"passed": True, "weight": 2.0}, {"passed": True, "weight": 1.0}]
    assert calculate_setup_confidence(all_pass) == 100.0

    # Lista vazia
    assert calculate_setup_confidence([]) == 0.0


# ---------------------------------------------------------------------------
# 3. cont_pull — LONG triggered
# ---------------------------------------------------------------------------


def test_cont_pull_long_triggered():
    """Regime TREND_UP, touch na EMA21, close acima, volume e RSI ok → triggered."""
    df = _make_flat_df(n=100, close_price=100.0, last_low=99.5, last_close=101.0)
    ctx = _build_ctx(
        close=101.0,
        ema21=100.0,
        ema50=95.0,
        volume_ratio=1.3,
        rsi_14=50.0,
    )
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_cont_pull(df, ctx, regime)

    assert isinstance(result, SetupResult)
    assert result.setup_name == "cont_pull"
    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.confidence == 100.0
    assert result.triggered_at == ctx.timestamp
    assert result.evidence["regime"] == "TREND_UP"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 4. cont_pull — SHORT triggered
# ---------------------------------------------------------------------------


def test_cont_pull_short_triggered():
    """Espelho SHORT: regime TREND_DOWN, touch por cima, close abaixo."""
    df = _make_flat_df(n=100, close_price=100.0, last_high=100.5, last_close=99.0)
    ctx = _build_ctx(
        close=99.0,
        ema21=100.0,
        ema50=105.0,
        volume_ratio=1.3,
        rsi_14=50.0,
    )
    regime = FakeRegime(regime="TREND_DOWN", ema_alignment="bearish")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is True
    assert result.direction == "SHORT"
    assert result.confidence == 100.0
    assert result.evidence["regime"] == "TREND_DOWN"


# ---------------------------------------------------------------------------
# 5. cont_pull — regime não-tendência
# ---------------------------------------------------------------------------


def test_cont_pull_skips_in_range():
    """Regime RANGE → não dispara, evidence tem skip_reason."""
    df = _make_flat_df()
    ctx = _build_ctx()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert "skip_reason" in result.evidence
    assert "RANGE" in result.evidence["skip_reason"]


def test_cont_pull_skips_in_squeeze():
    """Regime SQUEEZE → não dispara."""
    df = _make_flat_df()
    ctx = _build_ctx()
    regime = FakeRegime(regime="SQUEEZE")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    assert "SQUEEZE" in result.evidence["skip_reason"]


# ---------------------------------------------------------------------------
# 6. cont_pull — sem touch na EMA21
# ---------------------------------------------------------------------------


def test_cont_pull_no_ema_touch():
    """Últimos 3 candles com low sempre acima da EMA21 → condition falha."""
    # close_price alto e lows acima → EMA21 ~ 100 mas lows últimos 3 = 110
    df = _make_flat_df(n=100, close_price=100.0)
    # override manual dos últimos 3 lows para ficarem bem acima da EMA21
    df.loc[df.index[-3:], "low"] = 110.0
    df.loc[df.index[-3:], "high"] = 112.0
    df.loc[df.index[-3:], "open"] = 111.0
    df.loc[df.index[-3:], "close"] = 111.0

    ctx = _build_ctx(
        close=111.0,
        ema21=100.0,
        ema50=95.0,
        volume_ratio=1.3,
        rsi_14=50.0,
    )
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is False
    touch_cond = next(
        c for c in result.evidence["conditions"] if c["name"] == "ema21_touch_last_3"
    )
    assert touch_cond["passed"] is False


# ---------------------------------------------------------------------------
# 7. cont_pull — tendência quebrada
# ---------------------------------------------------------------------------


def test_cont_pull_broken_trend():
    """ema21 < ema50 → condition trend_intact falha e setup não dispara."""
    df = _make_flat_df(n=100, close_price=100.0, last_low=99.5, last_close=101.0)
    ctx = _build_ctx(
        close=101.0,
        ema21=100.0,
        ema50=102.0,  # ema21 < ema50
        volume_ratio=1.3,
        rsi_14=50.0,
    )
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is False
    trend_cond = next(
        c for c in result.evidence["conditions"] if "trend_intact" in c["name"]
    )
    assert trend_cond["passed"] is False


# ---------------------------------------------------------------------------
# 8. cont_pull — volume baixo
# ---------------------------------------------------------------------------


def test_cont_pull_low_volume():
    """volume_ratio < 1.2 → condition falha, não dispara."""
    df = _make_flat_df(n=100, close_price=100.0, last_low=99.5, last_close=101.0)
    ctx = _build_ctx(
        close=101.0,
        ema21=100.0,
        ema50=95.0,
        volume_ratio=0.9,  # abaixo do mínimo
        rsi_14=50.0,
    )
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is False
    vol_cond = next(
        c for c in result.evidence["conditions"] if c["name"] == "volume_ratio_reactive"
    )
    assert vol_cond["passed"] is False


# ---------------------------------------------------------------------------
# 9. cont_pull — RSI extremo
# ---------------------------------------------------------------------------


def test_cont_pull_extreme_rsi():
    """RSI > 65 → condition rsi14_in_neutral_band falha."""
    df = _make_flat_df(n=100, close_price=100.0, last_low=99.5, last_close=101.0)
    ctx = _build_ctx(
        close=101.0,
        ema21=100.0,
        ema50=95.0,
        volume_ratio=1.3,
        rsi_14=72.0,  # fora da banda neutra
    )
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_cont_pull(df, ctx, regime)

    assert result.triggered is False
    rsi_cond = next(
        c for c in result.evidence["conditions"] if c["name"] == "rsi14_in_neutral_band"
    )
    assert rsi_cond["passed"] is False


# ---------------------------------------------------------------------------
# 10. cont_pull — confidence com 4 de 5
# ---------------------------------------------------------------------------


def test_cont_pull_confidence_score():
    """Todos passam → 100; falha isolada de peso 1 (volume) → confidence < 100."""
    # Todos passam
    df_all = _make_flat_df(n=100, close_price=100.0, last_low=99.5, last_close=101.0)
    ctx_all = _build_ctx()
    result_all = evaluate_cont_pull(df_all, ctx_all, FakeRegime(regime="TREND_UP"))
    assert result_all.confidence == 100.0
    assert result_all.triggered is True

    # 4 de 5 passam (volume falha, peso 1.0). Total weight = 7.5; passed = 6.5.
    # Esperado: round(6.5 / 7.5 * 100, 1) ≈ 86.7
    ctx_low_vol = _build_ctx(volume_ratio=0.8)
    result_part = evaluate_cont_pull(df_all, ctx_low_vol, FakeRegime(regime="TREND_UP"))
    assert result_part.triggered is False
    assert result_part.confidence < 100.0
    assert result_part.confidence == pytest.approx(86.7, abs=0.1)
