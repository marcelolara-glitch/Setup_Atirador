"""setups/rev_exaust.py — Setup de reversão por exaustão multi-timeframe.

Hipótese: token em sobrevenda/sobrecompra extrema em múltiplos timeframes
simultaneamente (15m + 5m + 1m), com candle de rejeição no 15m que confirma
o esgotamento do movimento. Captura capitulações que acontecem **fora de OB**,
onde setups estruturais falhariam.

Independente de regime — roda em ``TREND_UP``, ``TREND_DOWN``, ``RANGE`` e
``SQUEEZE``. A razão: exaustão extrema em alta tendência pode ser início de
reversão; em range, pode ser fundo/topo da faixa.

Direções:
    LONG  — ``rsi_3 < 15`` (queda exaustiva reverte)
    SHORT — ``rsi_3 > 85`` (alta exaustiva reverte)

Conditions (LONG — espelho invertido em SHORT):
    1. ``rsi_3 < 15`` no 15m                                (weight 2.0)
    2. ``rsi_14 < 25`` no 15m                               (weight 1.5)
    3. ``williams_r_14 < -90`` no 15m                       (weight 1.0)
    4. ``bb_position < 0.15`` no 15m                        (weight 1.5)
    5. ``wick_bottom_pct >= 0.25 AND is_bullish`` no 15m    (weight 2.0)
    6. (opcional) ``rsi_3 < 20`` no 5m E no 1m              (weight 1.5)

Se ``df_5m`` ou ``df_1m`` forem ``None`` a condition 6 é omitida e a
``confidence`` final recebe penalidade de ``× 0.75`` — isso permite fetch
sob demanda dos TFs menores só quando o pré-filtro 15m já passou.

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — função pura
    - Dependências: pandas, pandas_ta, setups.base
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
import pandas_ta_classic as pta

from setups.base import (
    SetupResult,
    calculate_setup_confidence,
    evaluate_condition,
)

if TYPE_CHECKING:
    from indicators import MarketContext
    from regime import RegimeClassification


__all__ = ["evaluate_rev_exaust"]


SETUP_NAME: str = "rev_exaust"

# Thresholds 15m
RSI_3_LONG_MAX: float = 15.0
RSI_3_SHORT_MIN: float = 85.0
RSI_14_LONG_MAX: float = 25.0
RSI_14_SHORT_MIN: float = 75.0
WILLIAMS_R_LONG_MAX: float = -90.0
WILLIAMS_R_SHORT_MIN: float = -10.0
BB_POSITION_LONG_MAX: float = 0.15
BB_POSITION_SHORT_MIN: float = 0.85
WICK_MIN: float = 0.25

# Thresholds multi-TF
RSI_3_MTF_LONG_MAX: float = 20.0
RSI_3_MTF_SHORT_MIN: float = 80.0

# Penalidade de confidence quando 5m/1m não disponíveis
NO_MULTITF_PENALTY: float = 0.75


def _rsi3_last(df: pd.DataFrame) -> float:
    """Calcula RSI(3) sobre ``df["close"]`` e retorna o último valor.

    Usa ``pandas_ta.rsi`` com mesmo engine dos demais módulos v9.
    Retorna ``float("nan")`` se não houver dados suficientes.
    """
    series = pta.rsi(df["close"], length=3)
    if series is None or series.empty:
        return float("nan")
    return float(series.iloc[-1])


def _pre_filter_passes(rsi_3: float) -> bool:
    """Pré-filtro 15m: RSI(3) em zona extrema (< 15 ou > 85)."""
    return rsi_3 < RSI_3_LONG_MAX or rsi_3 > RSI_3_SHORT_MIN


def _build_conditions_long(ctx: "MarketContext") -> list[dict[str, Any]]:
    """Conditions 1-5 (LONG), com base apenas no 15m (ctx)."""
    wick_and_bullish = float(ctx.wick_bottom_pct >= WICK_MIN and ctx.is_bullish)
    return [
        evaluate_condition(
            "rsi_3_15m_oversold",
            ctx.rsi_3,
            RSI_3_LONG_MAX,
            operator="<",
            weight=2.0,
        ),
        evaluate_condition(
            "rsi_14_15m_oversold",
            ctx.rsi_14,
            RSI_14_LONG_MAX,
            operator="<",
            weight=1.5,
        ),
        evaluate_condition(
            "williams_r_15m_extreme",
            ctx.williams_r_14,
            WILLIAMS_R_LONG_MAX,
            operator="<",
            weight=1.0,
        ),
        evaluate_condition(
            "bb_position_near_lower",
            ctx.bb_position,
            BB_POSITION_LONG_MAX,
            operator="<",
            weight=1.5,
        ),
        evaluate_condition(
            "bullish_rejection_candle",
            wick_and_bullish,
            0.5,
            operator=">",
            weight=2.0,
        ),
    ]


def _build_conditions_short(ctx: "MarketContext") -> list[dict[str, Any]]:
    """Conditions 1-5 (SHORT), com base apenas no 15m (ctx)."""
    wick_and_bearish = float(ctx.wick_top_pct >= WICK_MIN and not ctx.is_bullish)
    return [
        evaluate_condition(
            "rsi_3_15m_overbought",
            ctx.rsi_3,
            RSI_3_SHORT_MIN,
            operator=">",
            weight=2.0,
        ),
        evaluate_condition(
            "rsi_14_15m_overbought",
            ctx.rsi_14,
            RSI_14_SHORT_MIN,
            operator=">",
            weight=1.5,
        ),
        evaluate_condition(
            "williams_r_15m_extreme",
            ctx.williams_r_14,
            WILLIAMS_R_SHORT_MIN,
            operator=">",
            weight=1.0,
        ),
        evaluate_condition(
            "bb_position_near_upper",
            ctx.bb_position,
            BB_POSITION_SHORT_MIN,
            operator=">",
            weight=1.5,
        ),
        evaluate_condition(
            "bearish_rejection_candle",
            wick_and_bearish,
            0.5,
            operator=">",
            weight=2.0,
        ),
    ]


def _build_multi_tf_condition(
    direction: str,
    rsi3_5m: float,
    rsi3_1m: float,
) -> dict[str, Any]:
    """Condition 6 — confirmação RSI(3) em 5m e 1m."""
    if direction == "LONG":
        both_extreme = float(
            rsi3_5m < RSI_3_MTF_LONG_MAX and rsi3_1m < RSI_3_MTF_LONG_MAX
        )
        name = "multi_tf_rsi3_oversold"
    else:
        both_extreme = float(
            rsi3_5m > RSI_3_MTF_SHORT_MIN and rsi3_1m > RSI_3_MTF_SHORT_MIN
        )
        name = "multi_tf_rsi3_overbought"

    return evaluate_condition(
        name,
        both_extreme,
        0.5,
        operator=">",
        weight=1.5,
    )


def evaluate_rev_exaust(
    df_15m: pd.DataFrame,
    df_5m: pd.DataFrame | None,
    df_1m: pd.DataFrame | None,
    ctx: "MarketContext",
    regime: "RegimeClassification",
) -> SetupResult:
    """Setup de reversão por exaustão multi-TF.

    Parâmetros:
        df_15m: OHLCV 15m (colunas ``open, high, low, close, volume``).
            Presente apenas por consistência de interface — a avaliação do
            15m puro é feita através do ``ctx`` já pré-calculado.
        df_5m: OHLCV 5m. ``None`` é permitido (fetch sob demanda).
        df_1m: OHLCV 1m. ``None`` é permitido (fetch sob demanda).
        ctx: :class:`MarketContext` pré-calculado do 15m.
        regime: :class:`RegimeClassification` — mantido na assinatura para
            uniformidade com os demais setups; este setup é independente
            de regime e não filtra por ele.

    Retorno:
        :class:`SetupResult`. Quando ``df_5m`` ou ``df_1m`` forem ``None``
        a condition multi-TF é omitida e ``confidence`` recebe penalidade
        de ``× NO_MULTITF_PENALTY``.
    """
    # Pré-filtro: sem exaustão no 15m, nem vale buscar 5m/1m.
    if not _pre_filter_passes(ctx.rsi_3):
        return SetupResult(
            setup_name=SETUP_NAME,
            triggered=False,
            direction=None,
            confidence=0.0,
            evidence={
                "skip_reason": "pre_filter_not_extreme",
                "rsi_3": ctx.rsi_3,
            },
            triggered_at=ctx.timestamp,
        )

    direction = "LONG" if ctx.rsi_3 < RSI_3_LONG_MAX else "SHORT"

    if direction == "LONG":
        conditions = _build_conditions_long(ctx)
    else:
        conditions = _build_conditions_short(ctx)

    multi_tf_available = df_5m is not None and df_1m is not None
    if multi_tf_available:
        rsi3_5m = _rsi3_last(df_5m)
        rsi3_1m = _rsi3_last(df_1m)
        conditions.append(_build_multi_tf_condition(direction, rsi3_5m, rsi3_1m))

    all_passed = all(c["passed"] for c in conditions)
    confidence = calculate_setup_confidence(conditions)

    if not multi_tf_available:
        # Desconto fixo quando o setup rodou só no 15m.
        confidence = round(confidence * NO_MULTITF_PENALTY, 1)

    return SetupResult(
        setup_name=SETUP_NAME,
        triggered=all_passed,
        direction=direction if all_passed else None,
        confidence=confidence,
        evidence={
            "conditions": conditions,
            "regime": regime.regime,
            "multi_tf_available": multi_tf_available,
            "direction_evaluated": direction,
        },
        triggered_at=ctx.timestamp,
    )
