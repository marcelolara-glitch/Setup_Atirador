"""signals.py — orquestrador multi-setup (v9).

Décimo módulo da fundação v9. Para cada token, roda todos os setups
aplicáveis a partir do :class:`MarketContext` e da classificação de regime,
resolve a direção final, aplica filtros de proteção e constrói o
:class:`TradePlan` correspondente. O resultado é consolidado em um
:class:`SignalDecision` tipado que carrega também todos os
:class:`SetupResult` avaliados — o chamador (main/journal/telegram) não
precisa inspecionar estado interno para logar cada setup.

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — ``evaluate_token`` é pura dado seus inputs
    - Dependências: pandas, numpy, indicators, regime, risk, smc_lib, setups.*
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from indicators import build_market_context
from regime import classify_regime
from risk import TradePlan, calculate_trade_plan, validate_trade_plan
from setups.base import SetupResult
from setups.break_range import evaluate_break_range
from setups.breaker import evaluate_breaker
from setups.cont_pull import evaluate_cont_pull
from setups.rev_exaust import evaluate_rev_exaust
from setups.rev_zone import evaluate_rev_zone
from smc_lib import detect_breaker_blocks, detect_fvg, detect_order_blocks

__all__ = ["SignalDecision", "build_btc_context", "evaluate_token"]


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# CONFLUENCE_BOOST e MIN_CONFIDENCE removidos em v9.1 — confidence é binária.
# CONFIDENCE_CAP mantido por compat com cálculos pontuais.
CONFIDENCE_CAP: float = 100.0
MAX_ATR_PCT: float = 5.0
MAX_BTC_CHANGE_PCT: float = 2.0

_OB_SWING_LENGTH: int = 5
_OB_ATR_PERIOD: int = 14
_OB_MIN_STRENGTH_ATR: float = 0.3


# ---------------------------------------------------------------------------
# Dataclass de saída
# ---------------------------------------------------------------------------


@dataclass
class SignalDecision:
    """Resultado final da avaliação de um token em uma rodada.

    Campos:
        symbol: identificador do token.
        timestamp: timestamp do candle avaliado (vem do ``MarketContext``).
        timeframe: timeframe primário da decisão (sempre ``"15m"``).
        action: ``"CALL"`` se o token deve ser operado, ``"SKIP"`` caso
            contrário.
        direction: ``"LONG"`` | ``"SHORT"`` quando ``action=CALL``;
            ``None`` em SKIP antes da resolução de direção.
        signal_tag: nome do setup disparado (ou ``a+b+...`` ordenado em
            confluência).
        confluent_setups: lista de setups disparados (1 ou mais).
        confidence: score 0-100 consolidado — aplica boost de 15% em
            confluência, com cap em 100.
        trade_plan: :class:`TradePlan` quando ``action=CALL``;
            ``None`` em SKIP.
        regime: regime do momento avaliado.
        all_setup_results: todos os setups rodados (para log/journal).
        protection_filters: dict com o estado de cada filtro
            (True=passou, False=bloqueou, None=não checado).
        skip_reason: motivo principal quando ``action=SKIP``; ``None`` em CALL.
    """

    symbol: str
    timestamp: pd.Timestamp
    timeframe: str
    action: str
    direction: Optional[str]
    signal_tag: Optional[str]
    confluent_setups: list[str]
    confidence: float
    trade_plan: Optional[TradePlan]
    regime: str
    all_setup_results: list[SetupResult]
    protection_filters: dict
    skip_reason: Optional[str]


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------


def _skip(
    *,
    symbol: str,
    timestamp: pd.Timestamp,
    regime_label: str,
    skip_reason: str,
    all_results: list[SetupResult],
    protection: dict,
    direction: Optional[str] = None,
    signal_tag: Optional[str] = None,
    confluent_setups: Optional[list[str]] = None,
    confidence: float = 0.0,
) -> SignalDecision:
    """Constrói um :class:`SignalDecision` de SKIP com campos padronizados."""
    return SignalDecision(
        symbol=symbol,
        timestamp=timestamp,
        timeframe="15m",
        action="SKIP",
        direction=direction,
        signal_tag=signal_tag,
        confluent_setups=confluent_setups if confluent_setups is not None else [],
        confidence=confidence,
        trade_plan=None,
        regime=regime_label,
        all_setup_results=all_results,
        protection_filters=protection,
        skip_reason=skip_reason,
    )


def _run_setups(
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    df_1m: Optional[pd.DataFrame],
    ctx,
    regime_result,
    order_blocks,
    breaker_blocks,
    disabled: set[str],
) -> list[SetupResult]:
    """Executa cada setup não desabilitado e retorna a lista de resultados."""
    results: list[SetupResult] = []

    if "cont_pull" not in disabled:
        results.append(evaluate_cont_pull(df_15m, ctx, regime_result))
    if "rev_exaust" not in disabled:
        results.append(evaluate_rev_exaust(df_15m, df_5m, df_1m, ctx, regime_result))
    if "break_range" not in disabled:
        results.append(evaluate_break_range(df_15m, ctx, regime_result))
    if "rev_zone" not in disabled:
        results.append(evaluate_rev_zone(df_15m, ctx, regime_result, order_blocks))
    if "breaker" not in disabled:
        results.append(evaluate_breaker(df_15m, ctx, regime_result, breaker_blocks))

    return results


def _consolidate(
    triggered: list[SetupResult],
) -> tuple[str, float, str, list[str]]:
    """Consolida direção, confidence, signal_tag e confluent_setups.

    Confidence é binária (100.0 quando há setup triggered, conforme v9.1).
    Confluência fica registrada em ``confluent_setups`` e ``signal_tag`` —
    o número de setups simultâneos é o "score" real (consagrado em
    NFIX7/Jesse, ver CLAUDE.md §2). Sem boost numérico.
    """
    direction = triggered[0].direction
    confluent_setups = [r.setup_name for r in triggered]

    if len(triggered) == 1:
        return direction, triggered[0].confidence, triggered[0].setup_name, confluent_setups

    signal_tag = "+".join(sorted(r.setup_name for r in triggered))
    return direction, CONFIDENCE_CAP, signal_tag, confluent_setups


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def evaluate_token(
    symbol: str,
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_5m: Optional[pd.DataFrame] = None,
    df_1m: Optional[pd.DataFrame] = None,
    open_trades: Optional[list[str]] = None,
    btc_context: Optional[dict] = None,
    disabled_setups: Optional[set[str]] = None,
) -> SignalDecision:
    """Orquestra a avaliação completa de um token.

    Fluxo:
        1. Calcula :class:`MarketContext` 15m e :class:`RegimeClassification`.
        2. Detecta Order Blocks e Breaker Blocks via ``smc_lib``.
        3. Roda os 5 setups (exceto os listados em ``disabled_setups``).
        4. Consolida direção e confidence — 15% de boost em confluência.
        5. Aplica filtros de proteção (trade aberto, volatilidade, BTC,
           confidence mínima).
        6. Calcula e valida :class:`TradePlan`; emite CALL ou SKIP final.

    Parâmetros:
        symbol: identificador do token (ex.: ``"BTCUSDT"``).
        df_15m: OHLCV 15m (primário da decisão).
        df_1h: OHLCV 1h (reservado para setups HTF futuros).
        df_4h: OHLCV 4h (reservado para setups HTF futuros).
        df_5m: OHLCV 5m — usado pelo ``rev_exaust``; pode ser ``None``.
        df_1m: OHLCV 1m — usado pelo ``rev_exaust``; pode ser ``None``.
        open_trades: lista de símbolos com trade ativo — bloqueia duplicação.
        btc_context: dict produzido por :func:`build_btc_context` para o
            filtro de correlação BTC. ``None`` desativa o filtro.
        disabled_setups: conjunto de nomes de setups a não avaliar.

    Retorno:
        :class:`SignalDecision`. ``all_setup_results`` carrega todos os
        setups rodados (mesmo em SKIP), permitindo log granular upstream.
    """
    disabled = disabled_setups or set()
    open_list = open_trades or []

    ctx = build_market_context(df_15m, symbol=symbol, timeframe="15m")
    regime_result = classify_regime(df_15m)

    order_blocks = detect_order_blocks(
        df_15m,
        swing_length=_OB_SWING_LENGTH,
        atr_period=_OB_ATR_PERIOD,
        min_strength_atr=_OB_MIN_STRENGTH_ATR,
    )
    breaker_blocks = detect_breaker_blocks(df_15m, order_blocks)

    results = _run_setups(
        df_15m=df_15m,
        df_5m=df_5m,
        df_1m=df_1m,
        ctx=ctx,
        regime_result=regime_result,
        order_blocks=order_blocks,
        breaker_blocks=breaker_blocks,
        disabled=disabled,
    )

    triggered = [r for r in results if r.triggered]

    if not triggered:
        return _skip(
            symbol=symbol,
            timestamp=ctx.timestamp,
            regime_label=regime_result.regime,
            skip_reason="no_setup_triggered",
            all_results=results,
            protection={},
        )

    directions = {r.direction for r in triggered}
    if len(directions) > 1:
        return _skip(
            symbol=symbol,
            timestamp=ctx.timestamp,
            regime_label=regime_result.regime,
            skip_reason="conflicting_setups",
            all_results=results,
            protection={},
            confluent_setups=[r.setup_name for r in triggered],
        )

    direction, confidence, signal_tag, confluent_setups = _consolidate(triggered)

    protection: dict = {}

    if symbol in open_list:
        protection["already_open"] = False
        return _skip(
            symbol=symbol,
            timestamp=ctx.timestamp,
            regime_label=regime_result.regime,
            skip_reason="trade_already_open",
            all_results=results,
            protection=protection,
            direction=direction,
            signal_tag=signal_tag,
            confluent_setups=confluent_setups,
            confidence=confidence,
        )
    protection["already_open"] = True

    atr_pct = (ctx.atr / ctx.close) * 100.0 if ctx.close > 0 else 0.0
    if atr_pct > MAX_ATR_PCT:
        protection["volatility_ok"] = False
        return _skip(
            symbol=symbol,
            timestamp=ctx.timestamp,
            regime_label=regime_result.regime,
            skip_reason="extreme_volatility",
            all_results=results,
            protection=protection,
            direction=direction,
            signal_tag=signal_tag,
            confluent_setups=confluent_setups,
            confidence=confidence,
        )
    protection["volatility_ok"] = True

    if btc_context is not None:
        btc_change_pct = btc_context.get("change_pct_15m", 0.0)
        if abs(btc_change_pct) > MAX_BTC_CHANGE_PCT:
            protection["btc_stable"] = False
            return _skip(
                symbol=symbol,
                timestamp=ctx.timestamp,
                regime_label=regime_result.regime,
                skip_reason="btc_abrupt_move",
                all_results=results,
                protection=protection,
                direction=direction,
                signal_tag=signal_tag,
                confluent_setups=confluent_setups,
                confidence=confidence,
            )
        protection["btc_stable"] = True
    else:
        protection["btc_stable"] = None

    # Filtro confidence_threshold deprecated v9.1 — sempre passa para
    # preservar schema dos databases e simplificar análises retroativas.
    protection["confidence_threshold"] = True

    # FVGs detectados sob demanda — só usados pelo risk para alvos de TP.
    fvgs = detect_fvg(df_15m)

    trade_plan = calculate_trade_plan(
        df=df_15m,
        direction=direction,
        entry_price=ctx.close,
        setup_confidence=confidence,
        regime=regime_result.regime,
        order_blocks=order_blocks,
        breaker_blocks=breaker_blocks,
        fvgs=fvgs,
    )

    is_valid, invalid_reason = validate_trade_plan(trade_plan)
    if not is_valid:
        return _skip(
            symbol=symbol,
            timestamp=ctx.timestamp,
            regime_label=regime_result.regime,
            skip_reason=f"invalid_trade_plan:{invalid_reason}",
            all_results=results,
            protection=protection,
            direction=direction,
            signal_tag=signal_tag,
            confluent_setups=confluent_setups,
            confidence=confidence,
        )

    return SignalDecision(
        symbol=symbol,
        timestamp=ctx.timestamp,
        timeframe="15m",
        action="CALL",
        direction=direction,
        signal_tag=signal_tag,
        confluent_setups=confluent_setups,
        confidence=confidence,
        trade_plan=trade_plan,
        regime=regime_result.regime,
        all_setup_results=results,
        protection_filters=protection,
        skip_reason=None,
    )


def build_btc_context(df_btc_15m: pd.DataFrame) -> dict:
    """Constrói o dicionário de contexto do BTC para o filtro de correlação.

    Parâmetros:
        df_btc_15m: OHLCV 15m do BTC. Precisa atender ao mínimo exigido
            por :func:`indicators.build_market_context` e
            :func:`regime.classify_regime`.

    Retorno:
        dict com chaves:
            ``change_pct_15m`` — variação percentual do close da última
                vela em relação à penúltima.
            ``atr_pct`` — ATR em percentual do close atual.
            ``regime`` — regime classificado pelo ``classify_regime``.
    """
    ctx = build_market_context(df_btc_15m, symbol="BTCUSDT", timeframe="15m")
    regime_result = classify_regime(df_btc_15m)

    close_series = df_btc_15m["close"]
    if len(close_series) >= 2 and close_series.iloc[-2] != 0:
        change_pct_15m = float(
            (close_series.iloc[-1] - close_series.iloc[-2])
            / close_series.iloc[-2]
            * 100.0
        )
    else:
        change_pct_15m = 0.0

    atr_pct = (ctx.atr / ctx.close) * 100.0 if ctx.close > 0 else 0.0

    return {
        "change_pct_15m": change_pct_15m,
        "atr_pct": float(atr_pct),
        "regime": regime_result.regime,
    }
