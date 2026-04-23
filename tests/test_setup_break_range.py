"""Testes do setup break_range — breakout de Donchian com volume."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from setups.base import SetupResult
from setups.break_range import evaluate_break_range


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
    high: float
    low: float
    atr: float
    volume_ratio: float
    donchian_upper_20: float
    donchian_lower_20: float


@dataclass
class FakeRegime:
    regime: str
    adx_value: float = 15.0
    ema_slope: float = 0.0
    ema_alignment: str = "mixed"
    bb_width_ratio: float = 0.8
    confidence: float = 60.0


# ---------------------------------------------------------------------------
# Helpers de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_range_df(
    n: int = 60,
    base: float = 100.0,
    half_range: float = 0.5,
    volume: float = 1000.0,
    last_open: float | None = None,
    last_high: float | None = None,
    last_low: float | None = None,
    last_close: float | None = None,
    last_volume: float | None = None,
) -> pd.DataFrame:
    """DF OHLCV com as primeiras ``n-1`` velas oscilando dentro de uma faixa
    estreita ``[base-half_range, base+half_range]`` e a última vela configurável.

    As 20 velas anteriores à última (índices ``-21:-1``) definem o Donchian
    prévio: ``upper_prev = base + half_range`` e ``lower_prev = base - half_range``.
    """
    rows = []
    for i in range(n - 1):
        direction = 1 if i % 2 == 0 else -1
        price_mid = base + direction * (half_range * 0.6)
        rows.append(
            {
                "open": price_mid,
                "high": base + half_range,
                "low": base - half_range,
                "close": price_mid,
                "volume": volume,
            }
        )
    # Última vela — defaults ficam dentro do range (sem rompimento)
    rows.append(
        {
            "open": last_open if last_open is not None else base,
            "high": last_high if last_high is not None else base + half_range * 0.5,
            "low": last_low if last_low is not None else base - half_range * 0.5,
            "close": last_close if last_close is not None else base,
            "volume": last_volume if last_volume is not None else volume,
        }
    )
    df = pd.DataFrame(rows)
    df.index = pd.date_range("2025-01-01", periods=n, freq="15min")
    return df


def _build_ctx(
    close: float,
    high: float,
    low: float,
    atr: float = 0.5,
    volume_ratio: float = 1.8,
    donchian_upper_20: float = 100.5,
    donchian_lower_20: float = 99.5,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 12:00:00"),
        close=close,
        high=high,
        low=low,
        atr=atr,
        volume_ratio=volume_ratio,
        donchian_upper_20=donchian_upper_20,
        donchian_lower_20=donchian_lower_20,
    )


# ---------------------------------------------------------------------------
# 1. LONG triggered
# ---------------------------------------------------------------------------


def test_break_range_long_triggered():
    """Regime RANGE, close acima do Donchian prévio, volume 1.8×, close no
    terço superior → triggered=True, LONG."""
    df = _make_range_df(
        n=60,
        base=100.0,
        half_range=0.5,
        last_open=100.3,
        last_high=101.2,
        last_low=100.2,
        last_close=101.1,  # close muito perto do high → terço superior
    )
    ctx = _build_ctx(
        close=101.1,
        high=101.2,
        low=100.2,
        atr=1.0,
        volume_ratio=1.8,
        donchian_upper_20=101.2,  # inclui a atual
        donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert isinstance(result, SetupResult)
    assert result.setup_name == "break_range"
    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.confidence == 100.0
    assert result.triggered_at == ctx.timestamp
    assert result.evidence["regime"] == "RANGE"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 2. SHORT triggered (espelho)
# ---------------------------------------------------------------------------


def test_break_range_short_triggered():
    """Espelho SHORT: close abaixo do Donchian prévio, volume 1.8×, close no
    terço inferior → triggered=True, SHORT."""
    df = _make_range_df(
        n=60,
        base=100.0,
        half_range=0.5,
        last_open=99.7,
        last_high=99.8,
        last_low=98.8,
        last_close=98.9,  # close muito perto do low → terço inferior
    )
    ctx = _build_ctx(
        close=98.9,
        high=99.8,
        low=98.8,
        atr=1.0,
        volume_ratio=1.8,
        donchian_upper_20=100.5,
        donchian_lower_20=98.8,  # inclui a atual
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is True
    assert result.direction == "SHORT"
    assert result.confidence == 100.0
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 3. SQUEEZE também dispara
# ---------------------------------------------------------------------------


def test_break_range_squeeze_regime():
    """Regime SQUEEZE também aceita o setup (precursor de rompimento)."""
    df = _make_range_df(
        n=60,
        base=100.0,
        half_range=0.5,
        last_open=100.3,
        last_high=101.2,
        last_low=100.2,
        last_close=101.1,
    )
    ctx = _build_ctx(
        close=101.1,
        high=101.2,
        low=100.2,
        atr=1.0,
        volume_ratio=1.8,
        donchian_upper_20=101.2,
        donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="SQUEEZE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.evidence["regime"] == "SQUEEZE"


# ---------------------------------------------------------------------------
# 4. Regime TREND_UP é pulado
# ---------------------------------------------------------------------------


def test_break_range_skips_in_trend():
    """Regime TREND_UP não roda — registra skip_reason."""
    df = _make_range_df(n=60, base=100.0, half_range=0.5, last_close=101.1)
    ctx = _build_ctx(
        close=101.1, high=101.2, low=100.2,
        atr=0.5, volume_ratio=1.8,
    )
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert "skip_reason" in result.evidence
    assert "TREND_UP" in result.evidence["skip_reason"]


# ---------------------------------------------------------------------------
# 5. Sem rompimento
# ---------------------------------------------------------------------------


def test_break_range_no_breakout():
    """Close dentro do range anterior → triggered=False, skip_reason=no_breakout."""
    df = _make_range_df(
        n=60, base=100.0, half_range=0.5,
        last_open=100.1, last_high=100.3, last_low=99.8, last_close=100.0,
    )
    ctx = _build_ctx(
        close=100.0, high=100.3, low=99.8,
        atr=0.5, volume_ratio=1.8,
        donchian_upper_20=100.5, donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    assert result.evidence["skip_reason"] == "no_breakout"
    # Mesmo sem rompimento, evidence expõe os níveis calculados
    assert "donchian_upper_prev" in result.evidence
    assert "donchian_lower_prev" in result.evidence
    assert "range_height" in result.evidence


# ---------------------------------------------------------------------------
# 6. Volume baixo
# ---------------------------------------------------------------------------


def test_break_range_low_volume():
    """Rompimento com volume_ratio < 1.5 → triggered=False."""
    df = _make_range_df(
        n=60, base=100.0, half_range=0.5,
        last_open=100.3, last_high=101.2, last_low=100.2, last_close=101.1,
    )
    ctx = _build_ctx(
        close=101.1, high=101.2, low=100.2,
        atr=1.0,
        volume_ratio=1.1,  # abaixo do mínimo 1.5
        donchian_upper_20=101.2, donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    # A direção foi detectada (close rompeu) mas conditions reprovaram
    conds = {c["name"]: c for c in result.evidence["conditions"]}
    assert conds["volume_ratio_breakout"]["passed"] is False


# ---------------------------------------------------------------------------
# 7. Wick de rompimento — close volta para dentro
# ---------------------------------------------------------------------------


def test_break_range_wick_breakout():
    """Close dentro do range anterior mas high ultrapassou Donchian
    → triggered=False (rompimento não convicto, apenas wick)."""
    df = _make_range_df(
        n=60, base=100.0, half_range=0.5,
        last_open=100.2, last_high=101.3, last_low=100.0, last_close=100.1,
    )
    ctx = _build_ctx(
        close=100.1,       # close voltou para dentro do range
        high=101.3,        # wick rompeu
        low=100.0,
        atr=0.5,
        volume_ratio=1.8,
        donchian_upper_20=101.3,   # reflete o high extremo
        donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    # Diagnóstico: não houve breakout no close
    assert result.evidence["skip_reason"] == "no_breakout"


# ---------------------------------------------------------------------------
# 8. Range largo — excede 2.5 × ATR
# ---------------------------------------------------------------------------


def test_break_range_wide_range():
    """Altura do Donchian > 2.5 × ATR → range_height reprovada, não dispara."""
    df = _make_range_df(
        n=60, base=100.0, half_range=3.0,  # range largo
        last_open=102.5, last_high=104.0, last_low=102.3, last_close=103.9,
    )
    ctx = _build_ctx(
        close=103.9, high=104.0, low=102.3,
        atr=0.5,  # 2.5 × atr = 1.25 << range 6.0
        volume_ratio=1.8,
        donchian_upper_20=104.0,
        donchian_lower_20=97.0,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is False
    conds = {c["name"]: c for c in result.evidence["conditions"]}
    assert conds["range_height_le_2p5_atr"]["passed"] is False


# ---------------------------------------------------------------------------
# 9. Confidence alto quando tudo passa
# ---------------------------------------------------------------------------


def test_break_range_confidence_score():
    """Todas conditions passam → confidence=100.0."""
    df = _make_range_df(
        n=60, base=100.0, half_range=0.5,
        last_open=100.3, last_high=101.2, last_low=100.2, last_close=101.1,
    )
    ctx = _build_ctx(
        close=101.1, high=101.2, low=100.2,
        atr=1.0, volume_ratio=2.5,
        donchian_upper_20=101.2, donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert result.triggered is True
    assert result.confidence == 100.0


# ---------------------------------------------------------------------------
# 10. Evidence expõe os níveis do Donchian e range
# ---------------------------------------------------------------------------


def test_break_range_evidence_includes_donchian_levels():
    """Evidence deve incluir donchian_upper_prev, donchian_lower_prev e range_height."""
    df = _make_range_df(
        n=60, base=100.0, half_range=0.5,
        last_open=100.3, last_high=101.2, last_low=100.2, last_close=101.1,
    )
    ctx = _build_ctx(
        close=101.1, high=101.2, low=100.2,
        atr=1.0, volume_ratio=1.8,
        donchian_upper_20=101.2, donchian_lower_20=99.5,
    )
    regime = FakeRegime(regime="RANGE")

    result = evaluate_break_range(df, ctx, regime)

    assert "donchian_upper_prev" in result.evidence
    assert "donchian_lower_prev" in result.evidence
    assert "range_height" in result.evidence
    assert result.evidence["donchian_upper_prev"] == 100.5
    assert result.evidence["donchian_lower_prev"] == 99.5
    # range_height vem do ctx.donchian_upper_20 - ctx.donchian_lower_20
    assert abs(result.evidence["range_height"] - (101.2 - 99.5)) < 1e-9
