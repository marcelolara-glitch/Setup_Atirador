"""Testes do setup breaker — retest em Breaker Block."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from setups.base import SetupResult
from setups.breaker import evaluate_breaker


# ---------------------------------------------------------------------------
# Fakes locais de MarketContext, RegimeClassification e BreakerBlock
# ---------------------------------------------------------------------------
# As dataclasses reais vivem em indicators.py, regime.py e smc_lib.py.
# Os setups só dependem de duck typing sobre os atributos acessados, então
# estes stubs mínimos bastam para exercitar a lógica.


@dataclass
class FakeCtx:
    symbol: str
    timestamp: pd.Timestamp
    close: float
    open: float
    wick_top_pct: float
    wick_bottom_pct: float
    volume_ratio: float
    volume_median_20: float


@dataclass
class FakeRegime:
    regime: str
    adx_value: float = 18.0
    ema_slope: float = 0.0
    ema_alignment: str = "mixed"
    bb_width_ratio: float = 1.0
    confidence: float = 50.0


@dataclass
class FakeBreaker:
    new_direction: str
    top: float
    bottom: float
    break_volume: float
    retest_count: int = 1
    invalidated: bool = False
    original_direction: str = "bearish"
    index_start: int = 0
    index_end: int = 1
    invalidation_index: Optional[int] = None


# ---------------------------------------------------------------------------
# Helpers de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_df(
    n: int = 30,
    low: float = 99.0,
    high: float = 101.0,
) -> pd.DataFrame:
    """DF OHLCV com ``low`` e ``high`` fixos — o preço "toca" qualquer breaker
    cujo intervalo intersecte [low, high] nas últimas 3 velas.
    """
    rows = [
        {
            "open": (low + high) / 2,
            "high": high,
            "low": low,
            "close": (low + high) / 2,
            "volume": 1000.0,
        }
        for _ in range(n)
    ]
    df = pd.DataFrame(rows)
    df.index = pd.date_range("2025-01-01", periods=n, freq="15min")
    return df


def _build_ctx_long(
    close: float = 100.5,
    open_: float = 100.0,
    wick_bottom_pct: float = 0.30,
    wick_top_pct: float = 0.05,
    volume_ratio: float = 1.5,
    volume_median_20: float = 1000.0,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        close=close,
        open=open_,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        volume_ratio=volume_ratio,
        volume_median_20=volume_median_20,
    )


def _build_ctx_short(
    close: float = 100.0,
    open_: float = 100.5,
    wick_top_pct: float = 0.30,
    wick_bottom_pct: float = 0.05,
    volume_ratio: float = 1.5,
    volume_median_20: float = 1000.0,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        close=close,
        open=open_,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        volume_ratio=volume_ratio,
        volume_median_20=volume_median_20,
    )


def _find_condition(result: SetupResult, name_substr: str) -> Optional[dict]:
    conditions = result.evidence.get("conditions")
    if not conditions:
        return None
    for c in conditions:
        if name_substr in c["name"]:
            return c
    return None


# ---------------------------------------------------------------------------
# 1. LONG triggered em bullish breaker
# ---------------------------------------------------------------------------


def test_breaker_long_triggered():
    """Bullish breaker ativo, preço toca zona, candle rejeição bullish → LONG."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
            retest_count=1,
        )
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert isinstance(result, SetupResult)
    assert result.setup_name == "breaker"
    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.confidence == 100.0
    assert result.triggered_at == ctx.timestamp
    assert result.evidence["new_direction"] == "bullish"
    assert result.evidence["direction_evaluated"] == "LONG"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 2. SHORT triggered em bearish breaker (espelho)
# ---------------------------------------------------------------------------


def test_breaker_short_triggered():
    """Bearish breaker ativo, preço toca zona, candle rejeição bearish → SHORT."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bearish",
            top=101.5,
            bottom=100.5,
            break_volume=2000.0,
            retest_count=1,
            original_direction="bullish",
        )
    ]
    ctx = _build_ctx_short()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.triggered is True
    assert result.direction == "SHORT"
    assert result.confidence == 100.0
    assert result.evidence["new_direction"] == "bearish"
    assert result.evidence["direction_evaluated"] == "SHORT"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 3. Skip sem breakers ativos (vazio ou todos invalidados)
# ---------------------------------------------------------------------------


def test_breaker_skips_no_active_breakers():
    """Lista vazia ou todos invalidados → skip com no_active_breakers."""
    df = _make_df(low=99.0, high=101.0)
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    # Lista vazia
    result_empty = evaluate_breaker(df, ctx, regime, [])
    assert result_empty.triggered is False
    assert result_empty.direction is None
    assert result_empty.confidence == 0.0
    assert result_empty.evidence["skip_reason"] == "no_active_breakers"

    # Todos invalidados
    invalidated = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
            invalidated=True,
        ),
        FakeBreaker(
            new_direction="bearish",
            top=101.5,
            bottom=100.5,
            break_volume=2000.0,
            invalidated=True,
        ),
    ]
    result_invalid = evaluate_breaker(df, ctx, regime, invalidated)
    assert result_invalid.triggered is False
    assert result_invalid.evidence["skip_reason"] == "no_active_breakers"


# ---------------------------------------------------------------------------
# 4. Skip quando preço está longe do breaker
# ---------------------------------------------------------------------------


def test_breaker_skips_not_touched():
    """Preço longe da zona do breaker → skip."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=50.0,
            bottom=45.0,
            break_volume=2000.0,
        )
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "no_breaker_touched_or_ambiguous"


# ---------------------------------------------------------------------------
# 5. Skip ambíguo (preço em bullish E bearish breakers)
# ---------------------------------------------------------------------------


def test_breaker_skips_ambiguous():
    """Preço intersecta bullish E bearish breakers → skip ambíguo."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
        ),
        FakeBreaker(
            new_direction="bearish",
            top=101.5,
            bottom=100.5,
            break_volume=2000.0,
        ),
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "no_breaker_touched_or_ambiguous"


# ---------------------------------------------------------------------------
# 6. retest_count > 3 → condition falha
# ---------------------------------------------------------------------------


def test_breaker_skips_too_many_retests():
    """retest_count = 5 → condition falha, triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
            retest_count=5,
        )
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.triggered is False
    assert result.direction is None
    cond = _find_condition(result, "retest_count")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 7. Sem candle de rejeição → triggered=False
# ---------------------------------------------------------------------------


def test_breaker_skips_no_rejection():
    """wick_bottom pequeno → triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
        )
    ]
    ctx = _build_ctx_long(wick_bottom_pct=0.10)
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.triggered is False
    cond = _find_condition(result, "bullish_rejection_candle")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 8. TREND_UP alinhado a LONG → confidence × 1.1
# ---------------------------------------------------------------------------


def test_breaker_no_boost_v91():
    """v9.1: boost de regime alinhado removido — confidence é binária 100/0.

    Todas as conditions passam nos dois regimes (RANGE e TREND_UP). Antes,
    TREND_UP+LONG aplicaria × 1.1 (cap em 100). Agora ambos retornam 100.0.
    """
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
            retest_count=1,
        )
    ]
    ctx = _build_ctx_long()

    regime_range = FakeRegime(regime="RANGE")
    result_range = evaluate_breaker(df, ctx, regime_range, breakers)

    regime_trend = FakeRegime(regime="TREND_UP")
    result_trend = evaluate_breaker(df, ctx, regime_trend, breakers)

    # Ambos triggered com confidence binária = 100.0 (sem boost)
    assert result_range.triggered is True
    assert result_trend.triggered is True
    assert result_range.confidence == 100.0
    assert result_trend.confidence == 100.0
    assert result_trend.evidence["regime"] == "TREND_UP"


# ---------------------------------------------------------------------------
# 9. Volume baixo → condition falha
# ---------------------------------------------------------------------------


def test_breaker_low_volume_fails():
    """volume_ratio < 1.1 → triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
        )
    ]
    ctx = _build_ctx_long(volume_ratio=0.8)
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.triggered is False
    cond = _find_condition(result, "volume_ratio_retest")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 10. Evidence contém informações do breaker
# ---------------------------------------------------------------------------


def test_breaker_evidence_includes_breaker_info():
    """Evidence tem breaker_top, breaker_bottom, original_direction,
    new_direction, retest_count."""
    df = _make_df(low=99.0, high=101.0)
    breakers = [
        FakeBreaker(
            new_direction="bullish",
            top=99.5,
            bottom=98.5,
            break_volume=2000.0,
            retest_count=2,
            original_direction="bearish",
        )
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_breaker(df, ctx, regime, breakers)

    assert result.evidence["breaker_top"] == 99.5
    assert result.evidence["breaker_bottom"] == 98.5
    assert result.evidence["original_direction"] == "bearish"
    assert result.evidence["new_direction"] == "bullish"
    assert result.evidence["retest_count"] == 2
    assert result.evidence["break_volume"] == 2000.0
    assert "regime" in result.evidence
    assert "conditions" in result.evidence
