"""setups/cont_pull.py — Setup de continuação em pullback na EMA21.

Hipótese: token em tendência clara faz pullback até a EMA21 e retoma na
mesma direção. Só dispara em regime ``TREND_UP`` (LONG) ou ``TREND_DOWN``
(SHORT); qualquer outro regime retorna ``triggered=False`` com o motivo
registrado em ``evidence["skip_reason"]``.

Conditions (LONG — espelho invertido em SHORT):
    1. EMA21 touch nos últimos 3 candles (weight 2.0)
    2. ``close > ema21`` no candle atual (weight 2.0)
    3. ``ema21 > ema50`` — tendência intacta (weight 1.5)
    4. ``volume_ratio >= 1.2`` no candle atual (weight 1.0)
    5. ``35 < rsi_14 < 65`` — momentum saindo de neutro (weight 1.0)

Regras de pureza (absolutas):
    - Zero I/O
    - Zero estado global — função pura
    - Dependências: pandas, setups.base
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from setups.base import (
    SetupResult,
    calculate_setup_confidence,
    evaluate_condition,
)

if TYPE_CHECKING:
    from indicators import MarketContext
    from regime import RegimeClassification


__all__ = ["evaluate_cont_pull"]


SETUP_NAME: str = "cont_pull"
EMA21_LENGTH: int = 21
TOUCH_LOOKBACK: int = 3
VOLUME_RATIO_MIN: float = 1.2
RSI_NEUTRAL_LOW: float = 35.0
RSI_NEUTRAL_HIGH: float = 65.0


def _compute_ema21_last_n(df: pd.DataFrame, n: int = TOUCH_LOOKBACK) -> pd.Series:
    """Calcula a EMA21 sobre ``df["close"]`` e retorna os últimos ``n`` valores.

    Usa ``ewm(span=21, adjust=False)`` — mesma fórmula que ``pandas_ta.ema``.
    """
    ema21 = df["close"].ewm(span=EMA21_LENGTH, adjust=False).mean()
    return ema21.iloc[-n:]


def _touched_ema_long(df: pd.DataFrame, ema21_last_n: pd.Series) -> bool:
    """Alguma low das últimas ``len(ema21_last_n)`` velas <= EMA21 correspondente."""
    lows = df["low"].iloc[-len(ema21_last_n):]
    return bool((lows.to_numpy() <= ema21_last_n.to_numpy()).any())


def _touched_ema_short(df: pd.DataFrame, ema21_last_n: pd.Series) -> bool:
    """Alguma high das últimas ``len(ema21_last_n)`` velas >= EMA21 correspondente."""
    highs = df["high"].iloc[-len(ema21_last_n):]
    return bool((highs.to_numpy() >= ema21_last_n.to_numpy()).any())


def evaluate_cont_pull(
    df: pd.DataFrame,
    ctx: "MarketContext",
    regime: "RegimeClassification",
) -> SetupResult:
    """Setup de continuação em pullback na EMA21.

    Parâmetros:
        df: OHLCV com colunas ``open, high, low, close, volume``. Usado apenas
            para verificar o touch da EMA21 nos últimos ``TOUCH_LOOKBACK``
            candles — o ctx já traz close/ema21/ema50/volume_ratio/rsi_14
            do candle atual.
        ctx: :class:`MarketContext` pré-calculado.
        regime: :class:`RegimeClassification` pré-calculado.

    Retorno:
        :class:`SetupResult`. Quando o regime não é ``TREND_UP`` nem
        ``TREND_DOWN`` retorna ``triggered=False`` e inclui
        ``skip_reason`` em ``evidence``.
    """
    if regime.regime == "TREND_UP":
        direction = "LONG"
    elif regime.regime == "TREND_DOWN":
        direction = "SHORT"
    else:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": f"regime {regime.regime} not trending"},
            triggered_at=ctx.timestamp,
        )

    ema21_last_n = _compute_ema21_last_n(df, TOUCH_LOOKBACK)

    if direction == "LONG":
        touched = _touched_ema_long(df, ema21_last_n)
        conditions = [
            evaluate_condition(
                "ema21_touch_last_3", float(touched), 0.5, operator=">", weight=2.0
            ),
            evaluate_condition(
                "close_above_ema21", ctx.close, ctx.ema21, operator=">", weight=2.0
            ),
            evaluate_condition(
                "trend_intact_ema21_gt_ema50",
                ctx.ema21,
                ctx.ema50,
                operator=">",
                weight=1.5,
            ),
            evaluate_condition(
                "volume_ratio_reactive",
                ctx.volume_ratio,
                VOLUME_RATIO_MIN,
                operator=">=",
                weight=1.0,
            ),
            evaluate_condition(
                "rsi14_in_neutral_band",
                1.0 if RSI_NEUTRAL_LOW < ctx.rsi_14 < RSI_NEUTRAL_HIGH else 0.0,
                0.5,
                operator=">",
                weight=1.0,
            ),
        ]
    else:  # SHORT
        touched = _touched_ema_short(df, ema21_last_n)
        conditions = [
            evaluate_condition(
                "ema21_touch_last_3", float(touched), 0.5, operator=">", weight=2.0
            ),
            evaluate_condition(
                "close_below_ema21", ctx.close, ctx.ema21, operator="<", weight=2.0
            ),
            evaluate_condition(
                "trend_intact_ema21_lt_ema50",
                ctx.ema21,
                ctx.ema50,
                operator="<",
                weight=1.5,
            ),
            evaluate_condition(
                "volume_ratio_reactive",
                ctx.volume_ratio,
                VOLUME_RATIO_MIN,
                operator=">=",
                weight=1.0,
            ),
            evaluate_condition(
                "rsi14_in_neutral_band",
                1.0 if RSI_NEUTRAL_LOW < ctx.rsi_14 < RSI_NEUTRAL_HIGH else 0.0,
                0.5,
                operator=">",
                weight=1.0,
            ),
        ]

    all_passed = all(c["passed"] for c in conditions)
    confidence = calculate_setup_confidence(conditions)

    return SetupResult(
        setup_name=SETUP_NAME,
        triggered=all_passed,
        direction=direction if all_passed else None,
        confidence=confidence,
        evidence={"conditions": conditions, "regime": regime.regime},
        triggered_at=ctx.timestamp,
    )
