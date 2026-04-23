"""setups/breaker.py — Setup de retest em Breaker Block.

Hipótese: um Order Block foi mitigado — o nível perdeu os defensores
originais e passa a operar na direção oposta. Quando o preço retorna
e testa essa zona invertida, frequentemente é rejeitado. É o retest do
"bearish/bullish breaker" — cenário onde institucionais reentram na
posição perdida.

Direções:
    LONG  — bullish breaker ativo (OB bearish mitigado) tocado por baixo
            com candle de rejeição bullish.
    SHORT — bearish breaker ativo (OB bullish mitigado) tocado por cima
            com candle de rejeição bearish.

Boost de confidence:
    Regime ``TREND_UP`` alinhado a LONG (ou ``TREND_DOWN`` a SHORT)
    recebe ``confidence × 1.1`` (cap em 100.0) — breakers confirmam a
    nova direção e ganham peso em tendência alinhada.

Conditions (LONG — bullish breaker; espelho invertido em SHORT):
    1. Breaker ativo tocado recentemente                        (weight 3.0)
    2. Retest count razoável: ``retest_count <= 3``             (weight 1.0)
    3. Break volume forte: ``>= 1.3 × volume_median_20``        (weight 1.5)
    4. Candle de rejeição bullish: ``wick_bottom_pct >= 0.25``
       e ``close > open``                                       (weight 2.0)
    5. Volume no retest: ``volume_ratio >= 1.1``                (weight 1.0)

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — função pura
    - Dependências: pandas, setups.base
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from setups.base import (
    SetupResult,
    calculate_setup_confidence,
    evaluate_condition,
)

if TYPE_CHECKING:
    from indicators import MarketContext
    from regime import RegimeClassification
    from smc_lib import BreakerBlock


__all__ = ["evaluate_breaker"]


SETUP_NAME: str = "breaker"

# Thresholds
RETEST_COUNT_MAX: int = 3
BREAK_VOLUME_MULT: float = 1.3
WICK_REJECTION_MIN: float = 0.25
VOLUME_RATIO_MIN: float = 1.1

# Multiplicador de confidence quando regime alinha com direção
TREND_ALIGNMENT_BOOST: float = 1.1
CONFIDENCE_CAP: float = 100.0


def _find_touched_breaker(
    active_breakers: list["BreakerBlock"],
    recent_low: float,
    recent_high: float,
    new_direction: str,
) -> "BreakerBlock | None":
    """Retorna o primeiro breaker ativo com ``new_direction`` tocado na janela.

    Um breaker é considerado tocado quando ``recent_high >= bottom`` e
    ``recent_low <= top``.
    """
    return next(
        (
            br
            for br in active_breakers
            if br.new_direction == new_direction
            and recent_high >= br.bottom
            and recent_low <= br.top
        ),
        None,
    )


def _build_conditions_long(
    ctx: "MarketContext",
    breaker: "BreakerBlock",
) -> list[dict[str, Any]]:
    """Monta as 5 conditions do lado LONG (bullish breaker retest)."""
    rejection_candle = float(
        ctx.wick_bottom_pct >= WICK_REJECTION_MIN and ctx.close > ctx.open
    )
    break_volume_threshold = ctx.volume_median_20 * BREAK_VOLUME_MULT

    return [
        evaluate_condition(
            "price_in_bullish_breaker",
            1.0,
            0.5,
            operator=">",
            weight=3.0,
        ),
        evaluate_condition(
            "retest_count_le_3",
            float(breaker.retest_count),
            float(RETEST_COUNT_MAX),
            operator="<=",
            weight=1.0,
        ),
        evaluate_condition(
            "break_volume_strong",
            breaker.break_volume,
            break_volume_threshold,
            operator=">=",
            weight=1.5,
        ),
        evaluate_condition(
            "bullish_rejection_candle",
            rejection_candle,
            0.5,
            operator=">",
            weight=2.0,
        ),
        evaluate_condition(
            "volume_ratio_retest",
            ctx.volume_ratio,
            VOLUME_RATIO_MIN,
            operator=">=",
            weight=1.0,
        ),
    ]


def _build_conditions_short(
    ctx: "MarketContext",
    breaker: "BreakerBlock",
) -> list[dict[str, Any]]:
    """Monta as 5 conditions do lado SHORT (bearish breaker retest)."""
    rejection_candle = float(
        ctx.wick_top_pct >= WICK_REJECTION_MIN and ctx.close < ctx.open
    )
    break_volume_threshold = ctx.volume_median_20 * BREAK_VOLUME_MULT

    return [
        evaluate_condition(
            "price_in_bearish_breaker",
            1.0,
            0.5,
            operator=">",
            weight=3.0,
        ),
        evaluate_condition(
            "retest_count_le_3",
            float(breaker.retest_count),
            float(RETEST_COUNT_MAX),
            operator="<=",
            weight=1.0,
        ),
        evaluate_condition(
            "break_volume_strong",
            breaker.break_volume,
            break_volume_threshold,
            operator=">=",
            weight=1.5,
        ),
        evaluate_condition(
            "bearish_rejection_candle",
            rejection_candle,
            0.5,
            operator=">",
            weight=2.0,
        ),
        evaluate_condition(
            "volume_ratio_retest",
            ctx.volume_ratio,
            VOLUME_RATIO_MIN,
            operator=">=",
            weight=1.0,
        ),
    ]


def evaluate_breaker(
    df: pd.DataFrame,
    ctx: "MarketContext",
    regime: "RegimeClassification",
    breaker_blocks: list["BreakerBlock"],
    lookback_retest: int = 3,
) -> SetupResult:
    """Setup de retest em Breaker Block.

    Parâmetros:
        df: OHLCV com colunas ``open, high, low, close, volume``. Usado
            apenas para verificar se o preço tocou a zona do breaker nas
            últimas ``lookback_retest`` velas — o candle de rejeição em
            si vem do ``ctx`` pré-calculado.
        ctx: :class:`MarketContext` pré-calculado do 15m.
        regime: :class:`RegimeClassification` pré-calculado. Regime
            alinhado à direção resultante multiplica confidence por
            :data:`TREND_ALIGNMENT_BOOST`.
        breaker_blocks: lista de :class:`BreakerBlock` candidatos. Apenas
            breakers não invalidados são considerados.
        lookback_retest: quantas velas anteriores à atual (inclusive) são
            consideradas para verificar se o preço tocou a zona.

    Retorno:
        :class:`SetupResult`. Skips possíveis em ``evidence["skip_reason"]``:

        * ``"no_active_breakers"``             — lista vazia ou apenas invalidados.
        * ``"no_breaker_touched_or_ambiguous"`` — nenhum breaker tocado, ou
          preço tocou simultaneamente bullish e bearish breakers.
    """
    active_breakers = [br for br in breaker_blocks if not br.invalidated]
    if not active_breakers:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": "no_active_breakers"},
            triggered_at=ctx.timestamp,
        )

    recent_low = float(df["low"].iloc[-lookback_retest:].min())
    recent_high = float(df["high"].iloc[-lookback_retest:].max())

    touched_bullish = _find_touched_breaker(
        active_breakers, recent_low, recent_high, "bullish"
    )
    touched_bearish = _find_touched_breaker(
        active_breakers, recent_low, recent_high, "bearish"
    )

    if touched_bullish is not None and touched_bearish is None:
        direction = "LONG"
        breaker = touched_bullish
    elif touched_bearish is not None and touched_bullish is None:
        direction = "SHORT"
        breaker = touched_bearish
    else:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": "no_breaker_touched_or_ambiguous"},
            triggered_at=ctx.timestamp,
        )

    if direction == "LONG":
        conditions = _build_conditions_long(ctx, breaker)
    else:
        conditions = _build_conditions_short(ctx, breaker)

    all_passed = all(c["passed"] for c in conditions)
    confidence = calculate_setup_confidence(conditions)

    if (direction == "LONG" and regime.regime == "TREND_UP") or (
        direction == "SHORT" and regime.regime == "TREND_DOWN"
    ):
        confidence = min(CONFIDENCE_CAP, round(confidence * TREND_ALIGNMENT_BOOST, 1))

    return SetupResult(
        setup_name=SETUP_NAME,
        triggered=all_passed,
        direction=direction if all_passed else None,
        confidence=confidence,
        evidence={
            "conditions": conditions,
            "breaker_top": breaker.top,
            "breaker_bottom": breaker.bottom,
            "original_direction": breaker.original_direction,
            "new_direction": breaker.new_direction,
            "retest_count": breaker.retest_count,
            "break_volume": breaker.break_volume,
            "regime": regime.regime,
            "direction_evaluated": direction,
        },
        triggered_at=ctx.timestamp,
    )
