"""setups/break_range.py — Setup de breakout de range (Donchian) com volume.

Hipótese: token consolidado em range estreito por ~20 velas, rompe o nível
de Donchian com volume de expansão e close convicto no terço superior (LONG)
ou inferior (SHORT) da vela. O rompimento indica entrada de fluxo direcional.

Só dispara em regime ``RANGE`` ou ``SQUEEZE`` (squeeze é precursor de
rompimento). Qualquer outro regime retorna ``triggered=False`` com o motivo
registrado em ``evidence["skip_reason"]``.

Direção:
    LONG  — ``close > donchian_upper_20_prev``
    SHORT — ``close < donchian_lower_20_prev``

O Donchian usado para decidir se "rompeu o range" é calculado sobre as
**20 velas anteriores à atual** (excluindo a vela em avaliação) para evitar
lookahead; o ``MarketContext.donchian_upper_20`` / ``donchian_lower_20``
inclui a vela atual e, por isso, só é usado para aferir a altura do range.

Conditions (LONG — espelho invertido em SHORT):
    1. Regime compatível ``RANGE`` | ``SQUEEZE``                    (weight 2.0)
    2. Range estreito: ``(upper_20 - lower_20) <= 2.5 × atr``       (weight 1.5)
    3. Close rompe Donchian upper das velas anteriores               (weight 2.5)
    4. Volume de rompimento: ``volume_ratio >= 1.5``                 (weight 2.0)
    5. Close no terço superior do range da vela (>= 0.66)            (weight 1.5)

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
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


__all__ = ["evaluate_break_range"]


SETUP_NAME: str = "break_range"
DONCHIAN_LOOKBACK: int = 20
RANGE_ATR_MAX: float = 2.5
VOLUME_RATIO_MIN: float = 1.5
CLOSE_CONVICTION_MIN: float = 0.66
_COMPATIBLE_REGIMES: tuple[str, ...] = ("RANGE", "SQUEEZE")


def _donchian_prev_bounds(df: pd.DataFrame) -> tuple[float, float]:
    """Donchian upper/lower das ``DONCHIAN_LOOKBACK`` velas anteriores à atual.

    Usa ``df["high"].iloc[-21:-1]`` e ``df["low"].iloc[-21:-1]`` — 20 velas
    imediatamente anteriores à vela em avaliação, excluindo-a.
    """
    start = -(DONCHIAN_LOOKBACK + 1)
    upper_prev = float(df["high"].iloc[start:-1].max())
    lower_prev = float(df["low"].iloc[start:-1].min())
    return upper_prev, lower_prev


def _close_position_in_candle(high: float, low: float, close: float) -> float:
    """Posição do close no range da vela atual, em [0.0, 1.0].

    Retorna 0.5 se ``high == low`` (vela de range zero — neutra).
    """
    span = high - low
    if span <= 0.0:
        return 0.5
    return (close - low) / span


def evaluate_break_range(
    df: pd.DataFrame,
    ctx: "MarketContext",
    regime: "RegimeClassification",
) -> SetupResult:
    """Setup de breakout de range com Donchian + volume.

    Parâmetros:
        df: OHLCV com colunas ``open, high, low, close, volume``. Precisa ter
            pelo menos ``DONCHIAN_LOOKBACK + 1`` velas — as 20 anteriores são
            usadas para calcular o Donchian prévio e a última é a vela em
            avaliação.
        ctx: :class:`MarketContext` pré-calculado. Campos consumidos:
            ``close``, ``high``, ``low``, ``atr``, ``volume_ratio``,
            ``donchian_upper_20``, ``donchian_lower_20``, ``timestamp``.
        regime: :class:`RegimeClassification` pré-calculado.

    Retorno:
        :class:`SetupResult`. Quando o regime não é ``RANGE`` nem ``SQUEEZE``
        retorna ``triggered=False`` com ``skip_reason`` em ``evidence``. Quando
        o close não rompe nenhum dos extremos retorna ``triggered=False`` com
        ``skip_reason="no_breakout"``.
    """
    if regime.regime not in _COMPATIBLE_REGIMES:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={"skip_reason": f"regime {regime.regime} not range/squeeze"},
            triggered_at=ctx.timestamp,
        )

    donchian_upper_prev, donchian_lower_prev = _donchian_prev_bounds(df)
    range_height = float(ctx.donchian_upper_20 - ctx.donchian_lower_20)

    if ctx.close > donchian_upper_prev:
        direction = "LONG"
    elif ctx.close < donchian_lower_prev:
        direction = "SHORT"
    else:
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={
                "skip_reason": "no_breakout",
                "donchian_upper_prev": donchian_upper_prev,
                "donchian_lower_prev": donchian_lower_prev,
                "range_height": range_height,
            },
            triggered_at=ctx.timestamp,
        )

    close_pos = _close_position_in_candle(ctx.high, ctx.low, ctx.close)
    regime_ok_value = 1.0 if regime.regime in _COMPATIBLE_REGIMES else 0.0
    range_atr_cap = RANGE_ATR_MAX * float(ctx.atr)

    if direction == "LONG":
        conditions = [
            evaluate_condition(
                "regime_range_or_squeeze",
                regime_ok_value,
                0.5,
                operator=">",
                weight=2.0,
            ),
            evaluate_condition(
                "range_height_le_2p5_atr",
                range_height,
                range_atr_cap,
                operator="<=",
                weight=1.5,
            ),
            evaluate_condition(
                "close_above_donchian_upper_prev",
                ctx.close,
                donchian_upper_prev,
                operator=">",
                weight=2.5,
            ),
            evaluate_condition(
                "volume_ratio_breakout",
                ctx.volume_ratio,
                VOLUME_RATIO_MIN,
                operator=">=",
                weight=2.0,
            ),
            evaluate_condition(
                "close_in_upper_third",
                close_pos,
                CLOSE_CONVICTION_MIN,
                operator=">=",
                weight=1.5,
            ),
        ]
    else:  # SHORT
        conditions = [
            evaluate_condition(
                "regime_range_or_squeeze",
                regime_ok_value,
                0.5,
                operator=">",
                weight=2.0,
            ),
            evaluate_condition(
                "range_height_le_2p5_atr",
                range_height,
                range_atr_cap,
                operator="<=",
                weight=1.5,
            ),
            evaluate_condition(
                "close_below_donchian_lower_prev",
                ctx.close,
                donchian_lower_prev,
                operator="<",
                weight=2.5,
            ),
            evaluate_condition(
                "volume_ratio_breakout",
                ctx.volume_ratio,
                VOLUME_RATIO_MIN,
                operator=">=",
                weight=2.0,
            ),
            evaluate_condition(
                "close_in_lower_third",
                1.0 - close_pos,
                CLOSE_CONVICTION_MIN,
                operator=">=",
                weight=1.5,
            ),
        ]

    all_passed = all(c["passed"] for c in conditions)
    confidence = calculate_setup_confidence(conditions)

    return SetupResult(
        setup_name=SETUP_NAME,
        triggered=all_passed,
        direction=direction if all_passed else None,
        confidence=confidence,
        evidence={
            "conditions": conditions,
            "regime": regime.regime,
            "donchian_upper_prev": donchian_upper_prev,
            "donchian_lower_prev": donchian_lower_prev,
            "range_height": range_height,
            "close_position": close_pos,
        },
        triggered_at=ctx.timestamp,
    )
