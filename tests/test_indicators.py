"""Testes de indicators.py — MarketContext unificado (v9)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from indicators import (
    MIN_CANDLES,
    MarketContext,
    MultiTFContext,
    build_market_context,
    build_multi_timeframe_context,
    get_last_swing_levels,
)


# ---------------------------------------------------------------------------
# Fixtures helpers
# ---------------------------------------------------------------------------


def _make_df(candles: list[dict], freq: str = "15min") -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df.index = pd.date_range("2025-01-01", periods=len(df), freq=freq)
    return df


def _candle(
    o: float,
    h: float,
    l: float,
    c: float,
    v: float = 1000.0,
) -> dict:
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def _uptrend_candles(n: int = 120, start: float = 100.0, step: float = 0.3) -> list[dict]:
    """Tendência de alta limpa — preço sobe consistentemente (sem pullbacks)."""
    candles = []
    for i in range(n):
        base = start + i * step
        candles.append(_candle(base, base + 0.4, base - 0.2, base + 0.3, v=1000.0 + i))
    return candles


def _downtrend_candles(n: int = 120, start: float = 200.0, step: float = 0.3) -> list[dict]:
    """Tendência de baixa limpa (sem pullbacks)."""
    candles = []
    for i in range(n):
        base = start - i * step
        candles.append(_candle(base, base + 0.2, base - 0.4, base - 0.3, v=1000.0 + i))
    return candles


def _hh_hl_uptrend(total: int = 120, leg: int = 10, step: float = 1.0) -> list[dict]:
    """Uptrend estruturado com HH/HL — ondas de alta + pullback + ondas de alta."""
    candles: list[dict] = []
    price = 100.0
    direction_up = True
    while len(candles) < total:
        for _ in range(leg):
            if len(candles) >= total:
                break
            if direction_up:
                price += step
                candles.append(_candle(price - 0.2, price + 0.3, price - 0.4, price + 0.2))
            else:
                price -= step * 0.4  # pullback menor que impulso → HL
                candles.append(_candle(price + 0.2, price + 0.4, price - 0.3, price - 0.2))
        direction_up = not direction_up
    # Garante que a última perna é de alta → última swing é high
    if not direction_up:
        for _ in range(leg):
            if len(candles) >= total:
                break
            price += step
            candles.append(_candle(price - 0.2, price + 0.3, price - 0.4, price + 0.2))
    return candles[:total]


def _lh_ll_downtrend(total: int = 120, leg: int = 10, step: float = 1.0) -> list[dict]:
    """Downtrend estruturado com LH/LL — ondas de baixa + pullback + ondas de baixa."""
    candles: list[dict] = []
    price = 200.0
    direction_down = True
    while len(candles) < total:
        for _ in range(leg):
            if len(candles) >= total:
                break
            if direction_down:
                price -= step
                candles.append(_candle(price + 0.2, price + 0.4, price - 0.3, price - 0.2))
            else:
                price += step * 0.4  # pullback menor → LH
                candles.append(_candle(price - 0.2, price + 0.3, price - 0.4, price + 0.2))
        direction_down = not direction_down
    if not direction_down:
        for _ in range(leg):
            if len(candles) >= total:
                break
            price -= step
            candles.append(_candle(price + 0.2, price + 0.4, price - 0.3, price - 0.2))
    return candles[:total]


def _sideways_candles(n: int = 120, base: float = 100.0) -> list[dict]:
    """Ruído em torno de um valor base."""
    rng = np.random.default_rng(seed=42)
    candles = []
    for _ in range(n):
        close = base + rng.uniform(-0.5, 0.5)
        open_ = base + rng.uniform(-0.5, 0.5)
        high = max(open_, close) + rng.uniform(0.1, 0.4)
        low = min(open_, close) - rng.uniform(0.1, 0.4)
        candles.append(_candle(open_, high, low, close, v=1000.0))
    return candles


# ---------------------------------------------------------------------------
# 1. Shape básico
# ---------------------------------------------------------------------------


def test_build_market_context_basic():
    """DataFrame válido retorna MarketContext com todos os campos preenchidos."""
    df = _make_df(_uptrend_candles(120))
    ctx = build_market_context(df, symbol="TEST", timeframe="15m")

    assert isinstance(ctx, MarketContext)
    assert ctx.symbol == "TEST"
    assert ctx.timeframe == "15m"
    assert ctx.timestamp == df.index[-1]

    # Preços coerentes com última vela
    assert ctx.close == pytest.approx(df["close"].iloc[-1])
    assert ctx.open == pytest.approx(df["open"].iloc[-1])
    assert ctx.high == pytest.approx(df["high"].iloc[-1])
    assert ctx.low == pytest.approx(df["low"].iloc[-1])
    assert ctx.volume == pytest.approx(df["volume"].iloc[-1])

    # Indicadores numéricos não-NaN
    for field in (
        "ema9", "ema21", "ema50", "ema_slope_21",
        "adx", "rsi_3", "rsi_14", "williams_r_14",
        "atr", "bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_position",
        "volume_median_20", "volume_ratio", "obv", "cmf",
        "donchian_upper_20", "donchian_lower_20", "donchian_mid_20",
        "wick_top_pct", "wick_bottom_pct", "body_pct",
    ):
        value = getattr(ctx, field)
        assert value is not None
        assert not np.isnan(value), f"{field} é NaN"

    assert isinstance(ctx.is_bullish, bool)
    assert ctx.structure_bias in {"bullish", "bearish", "neutral"}


# ---------------------------------------------------------------------------
# 2. EMAs coerentes em tendência de alta
# ---------------------------------------------------------------------------


def test_market_context_ema_values_coherent():
    """Em uptrend sustentado, EMA9 > EMA21 > EMA50."""
    df = _make_df(_uptrend_candles(120))
    ctx = build_market_context(df, symbol="UP")

    assert ctx.ema9 > ctx.ema21 > ctx.ema50
    assert ctx.ema_slope_21 > 0  # EMA21 subindo


# ---------------------------------------------------------------------------
# 3. bb_position bounded
# ---------------------------------------------------------------------------


def test_market_context_bb_position_bounded():
    """bb_position sempre entre 0 e 1 (clamp)."""
    for candles in (
        _uptrend_candles(120),
        _downtrend_candles(120),
        _sideways_candles(120),
    ):
        df = _make_df(candles)
        ctx = build_market_context(df, symbol="BB")
        assert 0.0 <= ctx.bb_position <= 1.0


# ---------------------------------------------------------------------------
# 4. volume_ratio coerente
# ---------------------------------------------------------------------------


def test_market_context_volume_ratio():
    """Volume da última vela controlado → volume_ratio coerente."""
    candles = _uptrend_candles(120)
    # Força última vela com volume 5x a mediana implícita
    candles[-1]["volume"] = 5000.0
    df = _make_df(candles)
    ctx = build_market_context(df, symbol="VR")

    # Mediana esperada está em torno de 1000; ratio esperado ~5
    assert ctx.volume_ratio == pytest.approx(
        ctx.volume / ctx.volume_median_20, rel=1e-6
    )
    assert ctx.volume_ratio > 3.0


# ---------------------------------------------------------------------------
# 5. Wicks + body somam ~1
# ---------------------------------------------------------------------------


def test_market_context_wicks_sum_to_100():
    """wick_top + body + wick_bottom ≈ 1.0 para vela com range > 0."""
    candles = _uptrend_candles(120)
    df = _make_df(candles)
    ctx = build_market_context(df, symbol="W")

    total = ctx.wick_top_pct + ctx.body_pct + ctx.wick_bottom_pct
    assert total == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# 6. Structure bias bullish
# ---------------------------------------------------------------------------


def test_structure_bias_bullish():
    """Sequência HH/HL + EMA21 > EMA50 → bias "bullish"."""
    df = _make_df(_hh_hl_uptrend(120))
    ctx = build_market_context(df, symbol="B")
    assert ctx.ema21 > ctx.ema50
    assert ctx.structure_bias == "bullish"


# ---------------------------------------------------------------------------
# 7. Structure bias bearish
# ---------------------------------------------------------------------------


def test_structure_bias_bearish():
    """Sequência LH/LL + EMA21 < EMA50 → bias "bearish"."""
    df = _make_df(_lh_ll_downtrend(120))
    ctx = build_market_context(df, symbol="D")
    assert ctx.ema21 < ctx.ema50
    assert ctx.structure_bias == "bearish"


# ---------------------------------------------------------------------------
# 7b. Matriz de decisão flexível de _derive_structure_bias
# ---------------------------------------------------------------------------


def test_structure_bias_emas_decide_when_swings_neutral():
    """EMAs direcionais + ausência de swings → EMAs decidem."""
    from indicators import _derive_structure_bias
    assert _derive_structure_bias(None, None, ema21=110.0, ema50=100.0) == "bullish"
    assert _derive_structure_bias(None, None, ema21=90.0, ema50=100.0) == "bearish"


def test_structure_bias_neutral_on_contradiction():
    """Swings e EMAs em direções opostas → neutral."""
    from indicators import _derive_structure_bias
    assert _derive_structure_bias(
        last_sh_idx=50, last_sl_idx=20, ema21=90.0, ema50=100.0
    ) == "neutral"
    assert _derive_structure_bias(
        last_sh_idx=20, last_sl_idx=50, ema21=110.0, ema50=100.0
    ) == "neutral"


def test_structure_bias_swings_decide_when_emas_neutral():
    """EMAs iguais + swings direcionais → swings decidem."""
    from indicators import _derive_structure_bias
    assert _derive_structure_bias(
        last_sh_idx=50, last_sl_idx=20, ema21=100.0, ema50=100.0
    ) == "bullish"
    assert _derive_structure_bias(
        last_sh_idx=20, last_sl_idx=50, ema21=100.0, ema50=100.0
    ) == "bearish"


def test_structure_bias_fully_neutral():
    """Swings ausentes + EMAs iguais → neutral."""
    from indicators import _derive_structure_bias
    assert _derive_structure_bias(None, None, ema21=100.0, ema50=100.0) == "neutral"


# ---------------------------------------------------------------------------
# 8. MultiTF retorna 3 TFs
# ---------------------------------------------------------------------------


def test_multi_tf_context_returns_all_tfs():
    df_15m = _make_df(_uptrend_candles(120))
    df_1h = _make_df(_uptrend_candles(120), freq="1h")
    df_4h = _make_df(_uptrend_candles(120), freq="4h")

    mtf = build_multi_timeframe_context(df_15m, df_1h, df_4h, symbol="MTF")

    assert isinstance(mtf, MultiTFContext)
    assert isinstance(mtf.primary, MarketContext)
    assert mtf.primary.timeframe == "15m"

    for htf in (mtf.htf_1h, mtf.htf_4h):
        assert isinstance(htf, dict)
        assert set(htf.keys()) == {"ema21_above_ema50", "rsi_14", "adx"}
        assert isinstance(htf["ema21_above_ema50"], bool)
        assert isinstance(htf["rsi_14"], float)
        assert isinstance(htf["adx"], float)
        # uptrend → ema21 > ema50
        assert htf["ema21_above_ema50"] is True


# ---------------------------------------------------------------------------
# 9. get_last_swing_levels
# ---------------------------------------------------------------------------


def test_get_last_swing_levels():
    """Retorna valores coerentes: preços dentro do range e índices válidos."""
    # Estrutura com swing high e swing low claros
    candles = []
    # Sobe até idx 10, desce até idx 20, sobe até idx 30
    for i in range(10):
        p = 100 + i
        candles.append(_candle(p, p + 0.5, p - 0.5, p + 0.3))
    # Topo em idx 10 (swing high)
    candles.append(_candle(109.5, 115.0, 109.0, 113.0))  # swing high em 10
    for i in range(10):
        p = 112 - i
        candles.append(_candle(p, p + 0.3, p - 0.5, p - 0.3))
    # Fundo em idx 21 (swing low)
    candles.append(_candle(102, 102.5, 95.0, 97.0))  # swing low em 21
    for i in range(10):
        p = 98 + i
        candles.append(_candle(p, p + 0.5, p - 0.3, p + 0.4))
    # mais candles para encher
    for _ in range(10):
        candles.append(_candle(107, 108, 106, 107.5))

    df = _make_df(candles)
    sh_price, sl_price, sh_idx, sl_idx = get_last_swing_levels(df, lookback=len(df))

    assert sh_price is not None
    assert sl_price is not None
    assert sh_idx is not None
    assert sl_idx is not None

    # Preços dentro do range do df
    assert df["low"].min() <= sl_price <= df["high"].max()
    assert df["low"].min() <= sh_price <= df["high"].max()

    # swing low é estruturalmente abaixo do swing high
    assert sl_price < sh_price

    # Índices em range válido
    assert 0 <= sh_idx < len(df)
    assert 0 <= sl_idx < len(df)


# ---------------------------------------------------------------------------
# 10. Insufficient candles → ValueError
# ---------------------------------------------------------------------------


def test_insufficient_candles_raises():
    """< MIN_CANDLES → ValueError."""
    df = _make_df(_uptrend_candles(MIN_CANDLES - 1))
    with pytest.raises(ValueError, match="mínimo exigido"):
        build_market_context(df, symbol="X")


# ---------------------------------------------------------------------------
# Extras — validação de colunas
# ---------------------------------------------------------------------------


def test_missing_column_raises():
    df = pd.DataFrame(
        {
            "open": np.arange(120, dtype=float),
            "high": np.arange(120, dtype=float) + 1,
            "low": np.arange(120, dtype=float) - 1,
            "close": np.arange(120, dtype=float),
            # volume ausente
        },
        index=pd.date_range("2025-01-01", periods=120, freq="15min"),
    )
    with pytest.raises(ValueError, match="colunas obrigatórias"):
        build_market_context(df, symbol="X")


def test_get_last_swing_levels_empty_df():
    """DataFrame vazio → todos campos None."""
    df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    result = get_last_swing_levels(df)
    assert result == (None, None, None, None)
