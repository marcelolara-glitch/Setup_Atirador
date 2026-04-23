"""Testes do setup rev_zone — reversão em zona qualificada (OB)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from setups.base import SetupResult
from setups.rev_zone import evaluate_rev_zone


# ---------------------------------------------------------------------------
# Fakes locais de MarketContext, RegimeClassification e OrderBlock
# ---------------------------------------------------------------------------
# As dataclasses reais vivem em indicators.py, regime.py e smc_lib.py.
# Os setups só dependem de duck typing sobre os atributos acessados, então
# estes stubs mínimos bastam para exercitar a lógica.


@dataclass
class FakeCtx:
    symbol: str
    timestamp: pd.Timestamp
    rsi_14: float
    wick_top_pct: float
    wick_bottom_pct: float
    body_pct: float
    is_bullish: bool


@dataclass
class FakeRegime:
    regime: str
    adx_value: float = 18.0
    ema_slope: float = 0.0
    ema_alignment: str = "mixed"
    bb_width_ratio: float = 1.0
    confidence: float = 50.0


@dataclass
class FakeOB:
    direction: str
    top: float
    bottom: float
    strength_atr: float
    mitigated: bool = False
    index_start: int = 0
    index_end: int = 1
    total_volume: float = 0.0
    bull_volume_ratio: float = 0.5
    mitigation_index: Optional[int] = None


# ---------------------------------------------------------------------------
# Helpers de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_df(
    n: int = 30,
    low: float = 99.0,
    high: float = 101.0,
) -> pd.DataFrame:
    """DF OHLCV onde todas as velas têm ``low`` e ``high`` fixos — o preço
    "toca" qualquer OB cujo intervalo contenha ``low`` (LONG) ou ``high``
    (SHORT) nas últimas 5 velas.
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
    rsi_14: float = 40.0,
    wick_bottom_pct: float = 0.40,
    wick_top_pct: float = 0.05,
    body_pct: float = 0.30,
    is_bullish: bool = True,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        rsi_14=rsi_14,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        body_pct=body_pct,
        is_bullish=is_bullish,
    )


def _build_ctx_short(
    rsi_14: float = 60.0,
    wick_top_pct: float = 0.40,
    wick_bottom_pct: float = 0.05,
    body_pct: float = 0.30,
    is_bullish: bool = False,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        rsi_14=rsi_14,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        body_pct=body_pct,
        is_bullish=is_bullish,
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
# 1. LONG triggered em OB bullish ativo
# ---------------------------------------------------------------------------


def test_rev_zone_long_triggered_in_bullish_ob():
    """Preço toca OB bullish ativo, wick bottom grande, RSI 40 → triggered LONG."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8)]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert isinstance(result, SetupResult)
    assert result.setup_name == "rev_zone"
    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.confidence == 100.0
    assert result.triggered_at == ctx.timestamp
    assert result.evidence["ob_direction"] == "bullish"
    assert result.evidence["direction_evaluated"] == "LONG"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 2. SHORT triggered em OB bearish ativo (espelho)
# ---------------------------------------------------------------------------


def test_rev_zone_short_triggered_in_bearish_ob():
    """Preço toca OB bearish ativo, wick top grande, RSI 60 → triggered SHORT."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bearish", top=101.5, bottom=100.5, strength_atr=0.8)]
    ctx = _build_ctx_short()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is True
    assert result.direction == "SHORT"
    assert result.confidence == 100.0
    assert result.evidence["ob_direction"] == "bearish"
    assert result.evidence["direction_evaluated"] == "SHORT"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 3. Skip quando não há OBs ativos
# ---------------------------------------------------------------------------


def test_rev_zone_skips_no_active_obs():
    """Lista vazia ou só OBs mitigados → skip."""
    df = _make_df(low=99.0, high=101.0)
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    # Lista vazia
    result_empty = evaluate_rev_zone(df, ctx, regime, [])
    assert result_empty.triggered is False
    assert result_empty.direction is None
    assert result_empty.confidence == 0.0
    assert result_empty.evidence["skip_reason"] == "no_active_obs"

    # Só OBs mitigados
    mitigated = [
        FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8, mitigated=True),
        FakeOB(direction="bearish", top=101.5, bottom=100.5, strength_atr=0.8, mitigated=True),
    ]
    result_mitigated = evaluate_rev_zone(df, ctx, regime, mitigated)
    assert result_mitigated.triggered is False
    assert result_mitigated.evidence["skip_reason"] == "no_active_obs"


# ---------------------------------------------------------------------------
# 4. Skip quando preço está longe de qualquer OB
# ---------------------------------------------------------------------------


def test_rev_zone_skips_no_ob_touched():
    """Preço longe de qualquer OB → skip com no_ob_touched."""
    df = _make_df(low=99.0, high=101.0)
    obs = [
        FakeOB(direction="bullish", top=50.0, bottom=45.0, strength_atr=0.8),
        FakeOB(direction="bearish", top=200.0, bottom=195.0, strength_atr=0.8),
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "no_ob_touched"


# ---------------------------------------------------------------------------
# 5. OB fraco → condition de strength falha
# ---------------------------------------------------------------------------


def test_rev_zone_skips_weak_ob():
    """OB com strength_atr < 0.5 → condition falha, triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.3)]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is False
    assert result.direction is None
    cond = _find_condition(result, "ob_strength_atr")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 6. Sem candle de rejeição (wick pequeno) → condition falha
# ---------------------------------------------------------------------------


def test_rev_zone_skips_no_rejection():
    """wick_bottom < 30% → triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8)]
    ctx = _build_ctx_long(wick_bottom_pct=0.10)
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is False
    cond = _find_condition(result, "bullish_rejection_wick")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 7. LONG bloqueado em TREND_DOWN
# ---------------------------------------------------------------------------


def test_rev_zone_long_blocked_in_trend_down():
    """Regime TREND_DOWN + OB bullish → skip global, triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8)]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="TREND_DOWN")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "regime TREND_DOWN blocks LONG"
    assert result.evidence["ob_direction"] == "bullish"
    assert "conditions" not in result.evidence


# ---------------------------------------------------------------------------
# 8. SHORT bloqueado em TREND_UP
# ---------------------------------------------------------------------------


def test_rev_zone_short_blocked_in_trend_up():
    """Regime TREND_UP + OB bearish → skip global, triggered=False."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bearish", top=101.5, bottom=100.5, strength_atr=0.8)]
    ctx = _build_ctx_short()
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "regime TREND_UP blocks SHORT"
    assert result.evidence["ob_direction"] == "bearish"
    assert "conditions" not in result.evidence


# ---------------------------------------------------------------------------
# 9. LONG permitido em TREND_UP (pullback em OB profundo)
# ---------------------------------------------------------------------------


def test_rev_zone_long_allowed_in_trend_up():
    """Regime TREND_UP + OB bullish profundo + candle rejeição → triggered LONG."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8)]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.evidence["regime"] == "TREND_UP"


# ---------------------------------------------------------------------------
# 10. Evidence contém informações do OB
# ---------------------------------------------------------------------------


def test_rev_zone_evidence_contains_ob_info():
    """Evidence tem ob_top, ob_bottom, ob_strength_atr."""
    df = _make_df(low=99.0, high=101.0)
    obs = [FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8)]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.evidence["ob_top"] == 99.5
    assert result.evidence["ob_bottom"] == 98.5
    assert result.evidence["ob_strength_atr"] == 0.8
    assert "regime" in result.evidence
    assert "conditions" in result.evidence


# ---------------------------------------------------------------------------
# 11. OB bullish E bearish simultâneos → skip ambiguo
# ---------------------------------------------------------------------------


def test_rev_zone_ambiguous_skips():
    """Preço está em OB bullish E bearish simultaneamente → skip."""
    df = _make_df(low=99.0, high=101.0)
    obs = [
        FakeOB(direction="bullish", top=99.5, bottom=98.5, strength_atr=0.8),
        FakeOB(direction="bearish", top=101.5, bottom=100.5, strength_atr=0.8),
    ]
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_zone(df, ctx, regime, obs)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "ambiguous_ob_touch"
