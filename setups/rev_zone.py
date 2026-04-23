"""setups/rev_zone.py — Setup de reversão em zona qualificada (OB).

Hipótese: preço atingiu uma zona estruturalmente relevante (Order Block
ativo / não mitigado), forma-se candle de rejeição dentro da zona e a
estrutura mais recente confirma intenção de reversão.

Direções:
    LONG  — preço testou OB bullish ativo e forma candle de rejeição
    SHORT — preço testou OB bearish ativo e forma candle de rejeição

Filtro de regime (skip global, simetria com ``cont_pull`` e ``break_range``):
    * ``TREND_UP``   — apenas LONG (pullback em tendência alta)
    * ``TREND_DOWN`` — apenas SHORT (pullback em tendência baixa)
    * ``RANGE`` / ``SQUEEZE`` — LONG e SHORT permitidos
    Regime incompatível retorna ``triggered=False`` com ``skip_reason``,
    sem poluir os QUASEs com bloqueios estruturais.

Conditions (LONG — reversão de baixa em OB bullish; espelho em SHORT):
    1. Preço tocou OB bullish ativo nas últimas ``lookback_zone`` velas  (weight 3.0)
    2. OB forte: ``strength_atr >= 0.5``                                 (weight 1.5)
    3. Candle de rejeição: ``wick_bottom_pct >= 0.30``                   (weight 2.5)
    4. Close bullish OU doji (``body_pct < 0.15``)                       (weight 1.5)
    5. RSI(14) saindo de sobrevendido: ``30 < rsi_14 < 50``              (weight 1.0)

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
    from smc_lib import OrderBlock


__all__ = ["evaluate_rev_zone"]


SETUP_NAME: str = "rev_zone"

# Thresholds
OB_STRENGTH_ATR_MIN: float = 0.5
WICK_REJECTION_MIN: float = 0.30
DOJI_BODY_MAX: float = 0.15
RSI_LONG_LOW: float = 30.0
RSI_LONG_HIGH: float = 50.0
RSI_SHORT_LOW: float = 50.0
RSI_SHORT_HIGH: float = 70.0

# Regimes onde cada direção é permitida
_LONG_ALLOWED_REGIMES: frozenset[str] = frozenset({"TREND_UP", "RANGE", "SQUEEZE"})
_SHORT_ALLOWED_REGIMES: frozenset[str] = frozenset({"TREND_DOWN", "RANGE", "SQUEEZE"})


def _find_touched_ob(
    active_obs: list["OrderBlock"],
    recent_low: float,
    recent_high: float,
    direction: str,
) -> "OrderBlock | None":
    """Retorna o primeiro OB ativo da ``direction`` indicada tocado na janela.

    Para OB bullish: tocado quando ``bottom <= recent_low <= top``.
    Para OB bearish: tocado quando ``bottom <= recent_high <= top``.
    """
    if direction == "bullish":
        return next(
            (
                ob
                for ob in active_obs
                if ob.direction == "bullish" and ob.bottom <= recent_low <= ob.top
            ),
            None,
        )
    return next(
        (
            ob
            for ob in active_obs
            if ob.direction == "bearish" and ob.bottom <= recent_high <= ob.top
        ),
        None,
    )


def _build_conditions_long(
    ctx: "MarketContext",
    ob: "OrderBlock",
) -> list[dict[str, Any]]:
    """Monta as 5 conditions do lado LONG."""
    rejection_candle = float(
        ctx.is_bullish or ctx.body_pct < DOJI_BODY_MAX
    )
    rsi_in_band = float(RSI_LONG_LOW < ctx.rsi_14 < RSI_LONG_HIGH)

    return [
        evaluate_condition(
            "price_in_bullish_ob",
            1.0,
            0.5,
            operator=">",
            weight=3.0,
        ),
        evaluate_condition(
            "ob_strength_atr",
            ob.strength_atr,
            OB_STRENGTH_ATR_MIN,
            operator=">=",
            weight=1.5,
        ),
        evaluate_condition(
            "bullish_rejection_wick",
            ctx.wick_bottom_pct,
            WICK_REJECTION_MIN,
            operator=">=",
            weight=2.5,
        ),
        evaluate_condition(
            "bullish_close_or_doji",
            rejection_candle,
            0.5,
            operator=">",
            weight=1.5,
        ),
        evaluate_condition(
            "rsi14_leaving_oversold",
            rsi_in_band,
            0.5,
            operator=">",
            weight=1.0,
        ),
    ]


def _build_conditions_short(
    ctx: "MarketContext",
    ob: "OrderBlock",
) -> list[dict[str, Any]]:
    """Monta as 5 conditions do lado SHORT (espelho do LONG)."""
    rejection_candle = float(
        (not ctx.is_bullish) or ctx.body_pct < DOJI_BODY_MAX
    )
    rsi_in_band = float(RSI_SHORT_LOW < ctx.rsi_14 < RSI_SHORT_HIGH)

    return [
        evaluate_condition(
            "price_in_bearish_ob",
            1.0,
            0.5,
            operator=">",
            weight=3.0,
        ),
        evaluate_condition(
            "ob_strength_atr",
            ob.strength_atr,
            OB_STRENGTH_ATR_MIN,
            operator=">=",
            weight=1.5,
        ),
        evaluate_condition(
            "bearish_rejection_wick",
            ctx.wick_top_pct,
            WICK_REJECTION_MIN,
            operator=">=",
            weight=2.5,
        ),
        evaluate_condition(
            "bearish_close_or_doji",
            rejection_candle,
            0.5,
            operator=">",
            weight=1.5,
        ),
        evaluate_condition(
            "rsi14_leaving_overbought",
            rsi_in_band,
            0.5,
            operator=">",
            weight=1.0,
        ),
    ]


def evaluate_rev_zone(
    df: pd.DataFrame,
    ctx: "MarketContext",
    regime: "RegimeClassification",
    order_blocks: list["OrderBlock"],
    lookback_zone: int = 5,
) -> SetupResult:
    """Setup de reversão em zona qualificada (OB ou S&R).

    Parâmetros:
        df: OHLCV com colunas ``open, high, low, close, volume``. Usado
            apenas para verificar se o preço tocou a zona nas últimas
            ``lookback_zone`` velas — o candle de rejeição em si vem do
            ``ctx`` pré-calculado.
        ctx: :class:`MarketContext` pré-calculado do 15m.
        regime: :class:`RegimeClassification` pré-calculado. Regime
            incompatível com a direção do OB tocado é tratado como skip
            global (simetria com ``cont_pull`` e ``break_range``).
        order_blocks: lista de :class:`OrderBlock` candidatos. Apenas OBs
            não mitigados são considerados.
        lookback_zone: quantas velas anteriores à atual (inclusive) são
            consideradas para verificar se o preço tocou a zona.

    Retorno:
        :class:`SetupResult`. Skips possíveis em ``evidence["skip_reason"]``:

        * ``"no_active_obs"``       — nenhum OB ativo na lista.
        * ``"no_ob_touched"``       — nenhum OB bullish/bearish tocado na janela.
        * ``"ambiguous_ob_touch"``  — preço tocou simultaneamente OB bullish e
          bearish — evita direção ambígua.
        * ``"regime <X> blocks LONG/SHORT"`` — regime incompatível com a
          direção do OB tocado.
    """
    active_obs = [ob for ob in order_blocks if not ob.mitigated]
    if not active_obs:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": "no_active_obs"},
            triggered_at=ctx.timestamp,
        )

    recent_low = float(df["low"].iloc[-lookback_zone:].min())
    recent_high = float(df["high"].iloc[-lookback_zone:].max())

    touched_bullish_ob = _find_touched_ob(
        active_obs, recent_low, recent_high, "bullish"
    )
    touched_bearish_ob = _find_touched_ob(
        active_obs, recent_low, recent_high, "bearish"
    )

    if touched_bullish_ob is not None and touched_bearish_ob is not None:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": "ambiguous_ob_touch"},
            triggered_at=ctx.timestamp,
        )

    if touched_bullish_ob is not None:
        direction = "LONG"
        ob = touched_bullish_ob
        allowed_regimes = _LONG_ALLOWED_REGIMES
    elif touched_bearish_ob is not None:
        direction = "SHORT"
        ob = touched_bearish_ob
        allowed_regimes = _SHORT_ALLOWED_REGIMES
    else:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": "no_ob_touched"},
            triggered_at=ctx.timestamp,
        )

    if regime.regime not in allowed_regimes:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={
                "skip_reason": f"regime {regime.regime} blocks {direction}",
                "ob_top": ob.top,
                "ob_bottom": ob.bottom,
                "ob_strength_atr": ob.strength_atr,
                "ob_direction": ob.direction,
                "regime": regime.regime,
            },
            triggered_at=ctx.timestamp,
        )

    if direction == "LONG":
        conditions = _build_conditions_long(ctx, ob)
    else:
        conditions = _build_conditions_short(ctx, ob)

    all_passed = all(c["passed"] for c in conditions)
    confidence = calculate_setup_confidence(conditions)

    return SetupResult(
        setup_name=SETUP_NAME,
        triggered=all_passed,
        direction=direction if all_passed else None,
        confidence=confidence,
        evidence={
            "conditions": conditions,
            "ob_top": ob.top,
            "ob_bottom": ob.bottom,
            "ob_strength_atr": ob.strength_atr,
            "ob_direction": ob.direction,
            "regime": regime.regime,
            "direction_evaluated": direction,
        },
        triggered_at=ctx.timestamp,
    )
