"""Testes do setup rev_exaust — reversão por exaustão multi-TF."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from setups.base import SetupResult
from setups.rev_exaust import evaluate_rev_exaust


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
    rsi_3: float
    rsi_14: float
    williams_r_14: float
    bb_position: float
    wick_top_pct: float
    wick_bottom_pct: float
    is_bullish: bool


@dataclass
class FakeRegime:
    regime: str
    adx_value: float = 20.0
    ema_slope: float = 0.0
    ema_alignment: str = "mixed"
    bb_width_ratio: float = 1.0
    confidence: float = 50.0


# ---------------------------------------------------------------------------
# Helpers de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_rsi3_df(
    n: int = 60,
    direction: str = "down",
    base: float = 100.0,
) -> pd.DataFrame:
    """DF OHLCV cujo RSI(3) na última vela fica extremamente baixo ou alto.

    ``direction="down"`` produz uma queda monotônica dos últimos candles
    (RSI(3) → ~0). ``direction="up"`` produz uma alta monotônica
    (RSI(3) → ~100).
    """
    rows = []
    # Primeira metade: oscilação suave para não saturar a RSI cedo demais.
    for i in range(n - 6):
        price = base + (i % 3) * 0.1
        rows.append(
            {
                "open": price,
                "high": price + 0.2,
                "low": price - 0.2,
                "close": price,
                "volume": 1000.0,
            }
        )
    # Últimas 6 velas fortemente direcionais.
    last_price = base
    for i in range(6):
        if direction == "down":
            last_price -= 2.0
        else:
            last_price += 2.0
        rows.append(
            {
                "open": last_price + 0.2,
                "high": last_price + 0.3,
                "low": last_price - 0.3,
                "close": last_price,
                "volume": 1000.0,
            }
        )

    df = pd.DataFrame(rows)
    df.index = pd.date_range("2025-01-01", periods=n, freq="15min")
    return df


def _build_ctx_long(
    rsi_3: float = 5.0,
    rsi_14: float = 20.0,
    williams_r_14: float = -95.0,
    bb_position: float = 0.05,
    wick_bottom_pct: float = 0.40,
    is_bullish: bool = True,
    wick_top_pct: float = 0.05,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        rsi_3=rsi_3,
        rsi_14=rsi_14,
        williams_r_14=williams_r_14,
        bb_position=bb_position,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        is_bullish=is_bullish,
    )


def _build_ctx_short(
    rsi_3: float = 95.0,
    rsi_14: float = 80.0,
    williams_r_14: float = -5.0,
    bb_position: float = 0.95,
    wick_top_pct: float = 0.40,
    is_bullish: bool = False,
    wick_bottom_pct: float = 0.05,
) -> FakeCtx:
    return FakeCtx(
        symbol="BTCUSDT",
        timestamp=pd.Timestamp("2025-01-02 00:00:00"),
        rsi_3=rsi_3,
        rsi_14=rsi_14,
        williams_r_14=williams_r_14,
        bb_position=bb_position,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        is_bullish=is_bullish,
    )


def _find_condition(result: SetupResult, name_substr: str) -> Optional[dict]:
    for c in result.evidence["conditions"]:
        if name_substr in c["name"]:
            return c
    return None


# ---------------------------------------------------------------------------
# 1. LONG triggered com todos os TFs disponíveis
# ---------------------------------------------------------------------------


def test_rev_exaust_long_triggered_full_tf():
    """RSI extremo em 15m/5m/1m + candle bullish com wick grande → triggered LONG."""
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="TREND_DOWN")

    result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)

    assert isinstance(result, SetupResult)
    assert result.setup_name == "rev_exaust"
    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.confidence == 100.0
    assert result.triggered_at == ctx.timestamp
    assert result.evidence["multi_tf_available"] is True
    assert result.evidence["direction_evaluated"] == "LONG"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 2. SHORT triggered com todos os TFs disponíveis
# ---------------------------------------------------------------------------


def test_rev_exaust_short_triggered_full_tf():
    """Espelho SHORT — exaustão de alta + candle bearish com wick grande."""
    df_15m = _make_rsi3_df(direction="up")
    df_5m = _make_rsi3_df(direction="up")
    df_1m = _make_rsi3_df(direction="up")
    ctx = _build_ctx_short()
    regime = FakeRegime(regime="TREND_UP")

    result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)

    assert result.triggered is True
    assert result.direction == "SHORT"
    assert result.confidence == 100.0
    assert result.evidence["direction_evaluated"] == "SHORT"
    assert all(c["passed"] for c in result.evidence["conditions"])


# ---------------------------------------------------------------------------
# 3. Pré-filtro bloqueia quando RSI(3) não é extremo
# ---------------------------------------------------------------------------


def test_rev_exaust_skips_without_extreme_rsi():
    """RSI(3) = 50 (neutro) → skip com pre_filter_not_extreme."""
    df_15m = _make_rsi3_df()
    ctx = _build_ctx_long(rsi_3=50.0)
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_exaust(df_15m, None, None, ctx, regime)

    assert result.triggered is False
    assert result.direction is None
    assert result.confidence == 0.0
    assert result.evidence["skip_reason"] == "pre_filter_not_extreme"
    assert result.evidence["rsi_3"] == 50.0


# ---------------------------------------------------------------------------
# 4. Avaliação possível sem 5m/1m — com penalidade de confidence
# ---------------------------------------------------------------------------


def test_rev_exaust_triggers_without_5m_1m():
    """v9.1: df_5m e df_1m = None → setup avalia só no 15m, sem penalty.

    Penalidade × 0.75 removida; confidence é binária 100/0. O fato de o setup
    ter rodado single-TF fica registrado em evidence["multi_tf_available"].
    """
    df_15m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_exaust(df_15m, None, None, ctx, regime)

    assert result.triggered is True
    assert result.direction == "LONG"
    assert result.evidence["multi_tf_available"] is False
    # Sem penalty — confidence binária 100 quando todas conditions passam
    assert result.confidence == 100.0
    assert len(result.evidence["conditions"]) == 5


# ---------------------------------------------------------------------------
# 5. Ausência de candle de rejeição
# ---------------------------------------------------------------------------


def test_rev_exaust_no_rejection_candle():
    """RSI extremo, mas wick pequeno → condition de rejeição falha."""
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long(wick_bottom_pct=0.10)  # abaixo do mínimo
    regime = FakeRegime(regime="RANGE")

    result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)

    assert result.triggered is False
    cond = _find_condition(result, "rejection_candle")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 6. Candle direcionalmente oposto à reversão
# ---------------------------------------------------------------------------


def test_rev_exaust_wrong_direction_candle():
    """LONG: RSI baixo mas vela bearish → condition de rejeição falha."""
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long(wick_bottom_pct=0.40, is_bullish=False)
    regime = FakeRegime(regime="TREND_DOWN")

    result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)

    assert result.triggered is False
    cond = _find_condition(result, "bullish_rejection_candle")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 7. BB position no meio bloqueia
# ---------------------------------------------------------------------------


def test_rev_exaust_bb_position_filter():
    """bb_position no meio (0.5) → condition BB falha, setup não dispara."""
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long(bb_position=0.5)
    regime = FakeRegime(regime="TREND_DOWN")

    result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)

    assert result.triggered is False
    cond = _find_condition(result, "bb_position")
    assert cond is not None
    assert cond["passed"] is False


# ---------------------------------------------------------------------------
# 8. Penalidade de confidence quando só 15m disponível
# ---------------------------------------------------------------------------


def test_rev_exaust_no_penalty_v91():
    """v9.1: sem penalidade single-TF. Confidence é binária 100/0.

    Multi-TF e single-TF retornam 100.0 se conditions passam. O fato de ter
    rodado single-TF permanece observável via evidence["multi_tf_available"].
    """
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="RANGE")

    result_full = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)
    result_partial = evaluate_rev_exaust(df_15m, None, None, ctx, regime)

    assert result_full.triggered is True
    assert result_partial.triggered is True
    # Ambos 100 — sem penalty
    assert result_full.confidence == 100.0
    assert result_partial.confidence == 100.0
    # Diagnóstico preservado
    assert result_full.evidence["multi_tf_available"] is True
    assert result_partial.evidence["multi_tf_available"] is False


# ---------------------------------------------------------------------------
# 9. Roda em qualquer regime — independente de TREND_UP/SQUEEZE/RANGE/TREND_DOWN
# ---------------------------------------------------------------------------


def test_rev_exaust_runs_in_any_regime():
    """Setup dispara sob qualquer regime desde que conditions passem."""
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long()

    for regime_name in ("TREND_UP", "TREND_DOWN", "RANGE", "SQUEEZE"):
        regime = FakeRegime(regime=regime_name)
        result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)
        assert result.triggered is True, f"falhou em regime {regime_name}"
        assert result.evidence["regime"] == regime_name


# ---------------------------------------------------------------------------
# 10. Evidence inclui todas as conditions avaliadas
# ---------------------------------------------------------------------------


def test_rev_exaust_evidence_has_all_conditions():
    """Evidence traz lista completa de conditions com nome, value, threshold, passed."""
    df_15m = _make_rsi3_df(direction="down")
    df_5m = _make_rsi3_df(direction="down")
    df_1m = _make_rsi3_df(direction="down")
    ctx = _build_ctx_long()
    regime = FakeRegime(regime="TREND_DOWN")

    result = evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime)

    conditions = result.evidence["conditions"]
    assert len(conditions) == 6  # 5 base + 1 multi-TF
    expected_names = {
        "rsi_3_15m_oversold",
        "rsi_14_15m_oversold",
        "williams_r_15m_extreme",
        "bb_position_near_lower",
        "bullish_rejection_candle",
        "multi_tf_rsi3_oversold",
    }
    actual_names = {c["name"] for c in conditions}
    assert actual_names == expected_names
    for c in conditions:
        assert set(c.keys()) >= {
            "name",
            "value",
            "threshold",
            "operator",
            "passed",
            "weight",
        }
