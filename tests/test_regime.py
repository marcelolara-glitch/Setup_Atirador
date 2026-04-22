"""Testes do módulo regime — classificador de regime de mercado."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from regime import (
    RegimeClassification,
    classify_regime,
    get_ema_alignment,
    get_ema_slope,
)


# ---------------------------------------------------------------------------
# Helpers de construção de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_df(closes: list[float], noise: float = 0.1, seed: int = 0) -> pd.DataFrame:
    """Constrói DataFrame OHLCV sintético a partir de uma série de closes."""
    rng = np.random.default_rng(seed)
    n = len(closes)
    closes_arr = np.asarray(closes, dtype=float)
    opens = np.concatenate([[closes_arr[0]], closes_arr[:-1]])
    highs = np.maximum(opens, closes_arr) + rng.uniform(0.0, noise, n)
    lows = np.minimum(opens, closes_arr) - rng.uniform(0.0, noise, n)
    volumes = rng.uniform(100.0, 1000.0, n)

    df = pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes_arr,
            "volume": volumes,
        }
    )
    df.index = pd.date_range("2025-01-01", periods=n, freq="15min")
    return df


def _trending_up(n: int = 200, start: float = 100.0, step: float = 0.6) -> pd.DataFrame:
    """Série com subida forte e consistente."""
    closes = [start + i * step for i in range(n)]
    return _make_df(closes, noise=0.05, seed=1)


def _trending_down(n: int = 200, start: float = 200.0, step: float = 0.6) -> pd.DataFrame:
    """Série com queda forte e consistente."""
    closes = [start - i * step for i in range(n)]
    return _make_df(closes, noise=0.05, seed=2)


def _ranging(n: int = 200, center: float = 100.0, amplitude: float = 1.5) -> pd.DataFrame:
    """Série oscilando em faixa estreita, sem tendência."""
    rng = np.random.default_rng(3)
    closes = center + rng.uniform(-amplitude, amplitude, n)
    return _make_df(list(closes), noise=0.1, seed=3)


def _squeeze_series(n: int = 200, squeeze_tail: int = 25) -> pd.DataFrame:
    """Série com volatilidade alta no passado e compressão forte no presente.

    A cauda comprimida precisa ser maior que o período das BB (20) para que a
    janela rolante atual fique inteiramente dentro da compressão, produzindo
    BB width bem abaixo da mediana histórica.
    """
    rng = np.random.default_rng(4)
    high_vol = 100.0 + rng.uniform(-20.0, 20.0, n - squeeze_tail)
    low_vol = 100.0 + rng.uniform(-0.05, 0.05, squeeze_tail)
    closes = np.concatenate([high_vol, low_vol])
    return _make_df(list(closes), noise=0.02, seed=4)


# ---------------------------------------------------------------------------
# Testes de classificação de regime
# ---------------------------------------------------------------------------


def test_trend_up_detection() -> None:
    df = _trending_up()
    result = classify_regime(df)
    assert result.regime == "TREND_UP"
    assert result.ema_alignment == "bullish"
    assert result.ema_slope > 0.0
    assert result.adx_value > 25.0


def test_trend_down_detection() -> None:
    df = _trending_down()
    result = classify_regime(df)
    assert result.regime == "TREND_DOWN"
    assert result.ema_alignment == "bearish"
    assert result.ema_slope < 0.0
    assert result.adx_value > 25.0


def test_range_detection() -> None:
    df = _ranging()
    result = classify_regime(df)
    assert result.regime == "RANGE"
    # Em mercado ranging, ADX costuma ficar baixo (< ~25). Não exigimos um
    # valor específico, apenas que NÃO tenha sido classificado como tendência.


def test_squeeze_detection() -> None:
    df = _squeeze_series()
    result = classify_regime(df)
    assert result.regime == "SQUEEZE"
    assert result.bb_width_ratio < 0.7


def test_squeeze_overrides_trend() -> None:
    """Mesmo com ADX alto, BB comprimida deve dominar a classificação."""
    # Série com tendência de alta longa e compressão curta no final — assim
    # o ADX ainda reflete a tendência e a BB width atual fica bem abaixo da
    # mediana das últimas 50 velas.
    trend_part = [100.0 + i * 0.6 for i in range(190)]
    last_value = trend_part[-1]
    rng = np.random.default_rng(5)
    squeeze_part = list(last_value + rng.uniform(-0.02, 0.02, 10))
    closes = trend_part + squeeze_part
    df = _make_df(closes, noise=0.02, seed=5)

    result = classify_regime(df)
    assert result.regime == "SQUEEZE"
    assert result.bb_width_ratio < 0.7


def test_ema_slope_positive() -> None:
    """EMA subindo consistentemente deve retornar slope > 0."""
    series = pd.Series([100.0 + i * 0.5 for i in range(50)])
    slope = get_ema_slope(series, periods=5)
    assert slope > 0.0


def test_ema_slope_negative() -> None:
    series = pd.Series([200.0 - i * 0.5 for i in range(50)])
    slope = get_ema_slope(series, periods=5)
    assert slope < 0.0


def test_ema_slope_flat() -> None:
    series = pd.Series([100.0] * 50)
    slope = get_ema_slope(series, periods=5)
    assert slope == 0.0


def test_ema_alignment_bullish() -> None:
    short = pd.Series([105.0, 106.0, 107.0, 108.0, 109.0])
    long_ = pd.Series([100.0, 100.5, 101.0, 101.5, 102.0])
    assert get_ema_alignment(short, long_, periods=3) == "bullish"


def test_ema_alignment_bearish() -> None:
    short = pd.Series([95.0, 94.0, 93.0, 92.0, 91.0])
    long_ = pd.Series([100.0, 99.5, 99.0, 98.5, 98.0])
    assert get_ema_alignment(short, long_, periods=3) == "bearish"


def test_ema_alignment_mixed() -> None:
    # Short cruza abaixo do long na última vela — mixed.
    short = pd.Series([105.0, 104.0, 102.0, 99.0])
    long_ = pd.Series([100.0, 100.5, 101.0, 101.5])
    assert get_ema_alignment(short, long_, periods=3) == "mixed"


def test_insufficient_candles_raises() -> None:
    df = _trending_up(n=50)
    with pytest.raises(ValueError):
        classify_regime(df)


def test_missing_columns_raises() -> None:
    df = _trending_up(n=200).drop(columns=["volume"])
    with pytest.raises(ValueError):
        classify_regime(df)


def test_regime_classification_returns_dataclass() -> None:
    df = _trending_up()
    result = classify_regime(df)
    assert isinstance(result, RegimeClassification)
    assert isinstance(result.regime, str)
    assert isinstance(result.adx_value, float)
    assert isinstance(result.ema_slope, float)
    assert isinstance(result.ema_alignment, str)
    assert isinstance(result.bb_width_ratio, float)
    assert isinstance(result.confidence, float)
    assert result.regime in {"TREND_UP", "TREND_DOWN", "RANGE", "SQUEEZE"}
    assert result.ema_alignment in {"bullish", "bearish", "mixed"}


def test_confidence_bounded() -> None:
    """Confidence deve estar sempre em [0, 100] para qualquer regime."""
    for df in (_trending_up(), _trending_down(), _ranging(), _squeeze_series()):
        result = classify_regime(df)
        assert 0.0 <= result.confidence <= 100.0
