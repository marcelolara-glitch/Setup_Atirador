"""risk.py — Sistema de gestão de risco/saídas para trades v9.

Calcula entry, SL, TP1/TP2/TP3 baseados em ATR e estrutura local. Implementa
saídas parciais e stop dinâmico (derisking V2 adaptado).

Terceiro módulo da fundação v9 — sucessor de smc_lib.py e regime.py.

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — todas as funções são puras
    - Input padrão: DataFrame pandas com colunas OHLCV em lowercase, além de
      dataclasses tipadas para os contratos de saída
    - Dependências mínimas: pandas, numpy, dataclasses, typing

Integração com ATR:
    O ATR usado aqui é Wilder's RMA (Running Moving Average) do True Range,
    calculado internamente. Equivale numericamente a
    ``pandas_ta.atr(high, low, close, length=period, mamode='rma')`` — mesma
    convenção adotada por ``smc_lib.py``. Manter a implementação interna evita
    dependência transitiva pesada para um cálculo trivial.

Colunas obrigatórias do DataFrame:
    open, high, low, close, volume   (indexado por timestamp)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constantes (defaults)
# ---------------------------------------------------------------------------

DEFAULT_ATR_PERIOD: int = 14

# Guard-rails ATR — SL não pode estar mais perto que MIN, nem mais longe que MAX
DEFAULT_SL_MIN_ATR_MULT: float = 0.8   # noise floor
DEFAULT_SL_MAX_PCT: float = 5.0         # catastrophe ceiling

# Multiplicadores ATR para SL/TP — usados como fallback quando estrutura ausente
DEFAULT_ATR_MULT_SL: float = 1.5

# TP escalado no SL real (R:R), não em ATR fixo — alinhamento com referências
DEFAULT_TP1_MIN_RR: float = 1.0   # TP1 mínimo R:R 1:1 (alinhado a SMC scalping)
DEFAULT_TP2_TARGET_RR: float = 2.0
DEFAULT_TP3_TARGET_RR: float = 3.5

DEFAULT_SWING_LOOKBACK: int = 20  # fallback quando smc_lib swings ausentes
DEFAULT_SWING_BUFFER_ATR: float = 0.3

POSITION_SPLIT: tuple[float, float, float] = (0.50, 0.30, 0.20)
MIN_LEVERAGE: float = 3.0
MAX_LEVERAGE: float = 15.0
DEFAULT_MIN_RR_TP1: float = 1.0      # subiu de 0.8 → 1.0 (alinhado a referências)
MAX_SL_DISTANCE_PCT: float = 10.0

_REQUIRED_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close")


# ---------------------------------------------------------------------------
# Dataclasses de saída
# ---------------------------------------------------------------------------


@dataclass
class TradePlan:
    """Plano completo de um trade — preços, risco e alavancagem.

    ``tp1_source`` e ``sl_source`` registram a origem do alvo/stop para
    observabilidade (auditoria SMC vs fallback). Defaults preservam compat
    com reconstrução a partir de DB legado (``journal._plan_from_row``).

    Valores possíveis:
        tp1_source: "structural_ob" | "structural_fvg" | "structural_breaker"
                    | "fallback_1r"
        sl_source:  "structural_swing" | "structural_ob" | "structural_breaker"
                    | "clamp_atr_min" | "clamp_pct_max"
    """

    direction: str
    entry_price: float
    sl_price: float
    sl_distance_pct: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    tp1_distance_pct: float
    tp2_distance_pct: float
    tp3_distance_pct: float
    risk_reward_tp1: float
    risk_reward_tp2: float
    risk_reward_tp3: float
    atr_value: float
    position_split: tuple = field(default_factory=lambda: POSITION_SPLIT)
    leverage: float = MIN_LEVERAGE
    tp1_source: str = "fallback_1r"
    sl_source: str = "structural_swing"
    # Diagnósticos do caminho de decisão (instrumentação para auditoria).
    # Defaults vazios preservam compat com TradePlan reconstruído de DB legado
    # (journal._plan_from_row).
    tp1_diagnostics: dict = field(default_factory=dict)
    sl_diagnostics: dict = field(default_factory=dict)


@dataclass
class TradeState:
    """Estado dinâmico de um trade em andamento."""

    trade_plan: TradePlan
    current_sl: float
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    position_remaining: float = 1.0
    status: str = "OPEN"


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------


def _validate_df(df: pd.DataFrame) -> None:
    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame ausente colunas obrigatórias: {missing}")


def _atr_series(df: pd.DataFrame, period: int) -> pd.Series:
    """ATR interno — Wilder's RMA do True Range.

    Equivale a ``pandas_ta.atr(..., mamode='rma')``. Alpha = 1/period
    (diferente da EMA padrão que usa 2/(span+1)).
    """
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(alpha=1 / period, adjust=False).mean()


def _last_swing_low(df: pd.DataFrame, lookback: int) -> float:
    """Retorna o menor low nas últimas `lookback` velas (estrutural)."""
    window = df["low"].iloc[-lookback:]
    return float(window.min())


def _last_swing_high(df: pd.DataFrame, lookback: int) -> float:
    """Retorna o maior high nas últimas `lookback` velas (estrutural)."""
    window = df["high"].iloc[-lookback:]
    return float(window.max())


# ---------------------------------------------------------------------------
# Leverage — 3 fatores ponderados (40/30/30)
# ---------------------------------------------------------------------------


def calculate_leverage(
    setup_confidence: float,
    regime: str,
    atr_value: float,
    entry_price: float,
    direction: str,
) -> float:
    """Calcula alavancagem recomendada entre 3x e 15x.

    Composta por três fatores ponderados:
        - Setup confidence (40%) — já 0-100
        - Alinhamento com regime (30%) — LONG em TREND_UP ou SHORT em
          TREND_DOWN pontua 100; RANGE = 60; SQUEEZE = 40; contra-tendência = 30
        - Volatilidade inversa (30%) — ATR/entry baixo pontua alto

    O score final mapeia linearmente para ``[MIN_LEVERAGE, MAX_LEVERAGE]``.

    Parâmetros:
        setup_confidence: 0-100, confidence do setup que disparou.
        regime: "TREND_UP" | "TREND_DOWN" | "RANGE" | "SQUEEZE".
        atr_value: ATR absoluto do ativo.
        entry_price: preço de entrada.
        direction: "LONG" | "SHORT".

    Retorno:
        Alavancagem em x, arredondada a 1 casa decimal.
    """
    confidence_score = float(setup_confidence)

    if direction == "LONG" and regime == "TREND_UP":
        regime_score = 100.0
    elif direction == "SHORT" and regime == "TREND_DOWN":
        regime_score = 100.0
    elif regime == "RANGE":
        regime_score = 60.0
    elif regime == "SQUEEZE":
        regime_score = 40.0
    else:
        regime_score = 30.0

    atr_pct = (atr_value / entry_price) * 100.0 if entry_price > 0 else 0.0
    if atr_pct < 0.5:
        vol_score = 100.0
    elif atr_pct < 1.0:
        vol_score = 75.0
    elif atr_pct < 2.0:
        vol_score = 50.0
    elif atr_pct < 3.0:
        vol_score = 30.0
    else:
        vol_score = 15.0

    final_score = confidence_score * 0.4 + regime_score * 0.3 + vol_score * 0.3
    leverage = MIN_LEVERAGE + (final_score / 100.0) * (MAX_LEVERAGE - MIN_LEVERAGE)
    return round(leverage, 1)


# ---------------------------------------------------------------------------
# 1. Trade plan — cálculo completo de preços, distâncias e alavancagem
# ---------------------------------------------------------------------------


def calculate_trade_plan(
    df: pd.DataFrame,
    direction: str,
    entry_price: float,
    setup_confidence: float,
    regime: str,
    atr_period: int = DEFAULT_ATR_PERIOD,
    atr_multiplier_sl: float = DEFAULT_ATR_MULT_SL,
    sl_min_atr_mult: float = DEFAULT_SL_MIN_ATR_MULT,
    sl_max_pct: float = DEFAULT_SL_MAX_PCT,
    tp1_min_rr: float = DEFAULT_TP1_MIN_RR,
    tp2_target_rr: float = DEFAULT_TP2_TARGET_RR,
    tp3_target_rr: float = DEFAULT_TP3_TARGET_RR,
    swing_lookback: int = DEFAULT_SWING_LOOKBACK,
    swing_buffer_atr: float = DEFAULT_SWING_BUFFER_ATR,
    order_blocks: Optional[list] = None,
    breaker_blocks: Optional[list] = None,
    fvgs: Optional[list] = None,
) -> TradePlan:
    """Calcula TradePlan híbrido estrutural (SMC) com guard-rails ATR.

    SL — prioridade estrutural, com guard-rails ATR e cap absoluto:
        1. Coleta candidatos estruturais (LONG):
           - Swing low das últimas ``swing_lookback`` velas (sempre disponível)
           - OB bottom de bullish OBs ativos com bottom < entry (se passados)
           - Breaker bottom de bullish breakers ativos (se passados)
           Cada candidato recebe buffer ``swing_buffer_atr × ATR``.
        2. Toma o ``min`` desses candidatos (mais conservador, fora do swing).
        3. Aplica guard-rails:
           - sl_min = entry - sl_min_atr_mult × ATR (proteção contra noise)
           - sl_max = entry × (1 - sl_max_pct/100) (proteção contra catástrofe)
           - sl_estrutural é clampado em [sl_max, sl_min]
        4. Para SHORT, lógica espelhada (max em vez de min, +buffer em vez de -).

    TP1 — prioridade estrutural com fallback R:R:
        1. Coleta candidatos estruturais (LONG, todos > entry):
           - Bearish OB bottoms (próxima resistência institucional)
           - Bearish breaker bottoms
           - Bullish FVG tops (gaps a preencher antes)
        2. tp1_estrutural = min desses candidatos > entry.
        3. Calcula R:R do candidato vs SL real (sl_distance = entry - sl_price).
        4. Se R:R >= ``tp1_min_rr``, usa o candidato; senão, fallback:
           tp1_price = entry + tp1_min_rr × sl_distance (garante R:R mínimo).

    TP2 e TP3 — escalas de R:R sobre o SL real (não ATR fixo):
        - tp2_price = entry + tp2_target_rr × sl_distance
        - tp3_price = entry + tp3_target_rr × sl_distance
        Garante R:R coerente independentemente de tamanho do SL.

    SHORT espelha integralmente a lógica.

    Parâmetros:
        df: DataFrame com OHLCV (>= atr_period + 1 velas recomendadas).
        direction: "LONG" | "SHORT".
        entry_price: preço de entrada.
        setup_confidence: 0-100, usado no cálculo de alavancagem.
        regime: regime corrente para ponderar alavancagem.
        atr_period: período do ATR.
        atr_multiplier_sl: fallback ATR quando estrutural não disponível.
        sl_min_atr_mult: SL não pode estar mais perto que isso (noise floor).
        sl_max_pct: SL não pode estar mais longe que isso em %.
        tp1_min_rr: R:R mínimo aceitável para TP1.
        tp2_target_rr: R:R alvo para TP2.
        tp3_target_rr: R:R alvo para TP3.
        swing_lookback: janela de swing low/high estrutural (fallback).
        swing_buffer_atr: buffer aplicado ao swing estrutural.
        order_blocks: lista de OrderBlock do smc_lib (opcional).
        breaker_blocks: lista de BreakerBlock do smc_lib (opcional).
        fvgs: lista de FairValueGap do smc_lib (opcional).

    Retorno:
        TradePlan populado.
    """
    _validate_df(df)
    if direction not in ("LONG", "SHORT"):
        raise ValueError(f"direction inválido: {direction!r}")
    if entry_price <= 0:
        raise ValueError(f"entry_price deve ser > 0, got {entry_price}")

    atr_series = _atr_series(df, period=atr_period)
    atr_value = float(atr_series.iloc[-1])
    if np.isnan(atr_value) or atr_value <= 0:
        raise ValueError("ATR inválido — DataFrame muito curto ou sem variação")

    obs = order_blocks or []
    brks = breaker_blocks or []
    fvgs_list = fvgs or []

    if direction == "LONG":
        # --- SL: prioridade estrutural ---
        sl_candidates: list[tuple[float, str]] = []

        # Swing estrutural (sempre disponível)
        swing_low = _last_swing_low(df, lookback=swing_lookback)
        sl_candidates.append(
            (swing_low - swing_buffer_atr * atr_value, "structural_swing")
        )

        # OBs bullish ativos com bottom < entry
        for ob in obs:
            if (
                getattr(ob, "direction", None) == "bullish"
                and not getattr(ob, "mitigated", True)
                and ob.bottom < entry_price
            ):
                sl_candidates.append(
                    (ob.bottom - swing_buffer_atr * atr_value, "structural_ob")
                )

        # Breakers bullish ativos com bottom < entry
        for br in brks:
            if (
                getattr(br, "new_direction", None) == "bullish"
                and not getattr(br, "invalidated", True)
                and br.bottom < entry_price
            ):
                sl_candidates.append(
                    (br.bottom - swing_buffer_atr * atr_value, "structural_breaker")
                )

        sl_estrutural, sl_winner_source = min(sl_candidates, key=lambda c: c[0])
        sl_source = sl_winner_source

        # Guard-rails ATR
        sl_min = entry_price - sl_min_atr_mult * atr_value
        sl_max = entry_price * (1.0 - sl_max_pct / 100.0)
        sl_price = max(sl_max, min(sl_estrutural, sl_min))

        # Detecta clamp e atualiza source_tag
        clamp_applied = None
        if sl_price != sl_estrutural:
            if sl_price == sl_min:
                sl_source = "clamp_atr_min"
                clamp_applied = "atr_min"
            elif sl_price == sl_max:
                sl_source = "clamp_pct_max"
                clamp_applied = "pct_max"

        sl_diag = {
            "sl_estrutural_pre_clamp": float(sl_estrutural),
            "sl_min_atr": float(sl_min),
            "sl_max_pct": float(sl_max),
            "clamp_applied": clamp_applied,
            "candidates_count": len(sl_candidates),
            "winner_source": sl_winner_source,
        }

        sl_distance = entry_price - sl_price

        # --- TP1: estrutural com fallback R:R + diagnostics ---
        tp1_diag: dict = {
            "candidates_total": len(obs) + len(brks) + len(fvgs_list),
            "candidates_after_direction": 0,
            "candidates_after_mitigated": 0,
            "candidates_after_rr": 0,
            "best_candidate_rr": None,
            "best_candidate_source": None,
            "fallback_reason": None,
        }
        # Filtro 1: tipo certo + lado certo do entry
        direction_ok: list[tuple[float, str, bool]] = []
        for ob in obs:
            if getattr(ob, "direction", None) == "bearish" and ob.bottom > entry_price:
                direction_ok.append(
                    (ob.bottom, "structural_ob", not getattr(ob, "mitigated", True))
                )
        for br in brks:
            if getattr(br, "new_direction", None) == "bearish" and br.bottom > entry_price:
                direction_ok.append(
                    (br.bottom, "structural_breaker", not getattr(br, "invalidated", True))
                )
        for fvg in fvgs_list:
            if getattr(fvg, "direction", None) == "bullish" and fvg.top > entry_price:
                direction_ok.append(
                    (fvg.top, "structural_fvg", not getattr(fvg, "filled", True))
                )
        tp1_diag["candidates_after_direction"] = len(direction_ok)

        # Filtro 2: ainda ativos (não mitigated/invalidated/filled)
        tp1_candidates: list[tuple[float, str]] = [
            (price, source) for price, source, active in direction_ok if active
        ]
        tp1_diag["candidates_after_mitigated"] = len(tp1_candidates)

        # Best R:R entre candidatos ativos (mesmo se rejeitado pela decisão)
        if tp1_candidates:
            rrs = [
                (price, source, (price - entry_price) / sl_distance)
                for price, source in tp1_candidates
            ]
            best_price, best_src, best_rr = max(rrs, key=lambda c: c[2])
            tp1_diag["best_candidate_rr"] = float(best_rr)
            tp1_diag["best_candidate_source"] = best_src
            tp1_diag["candidates_after_rr"] = sum(
                1 for _, _, rr in rrs if rr >= tp1_min_rr
            )

        tp1_fallback = entry_price + tp1_min_rr * sl_distance
        if tp1_candidates:
            tp1_estrutural, tp1_candidate_source = min(
                tp1_candidates, key=lambda c: c[0]
            )
            rr_estrutural = (tp1_estrutural - entry_price) / sl_distance
            # Aceita estrutural só se R:R no intervalo [tp1_min_rr, tp2_target_rr).
            # Quando o alvo estrutural fica além de tp2 (R:R > tp2_target_rr), usar
            # o estrutural como TP1 violaria a ordenação tp1 < tp2 (= entry + 2R).
            if tp1_min_rr <= rr_estrutural < tp2_target_rr:
                tp1_price = tp1_estrutural
                tp1_source = tp1_candidate_source
            else:
                tp1_price = tp1_fallback
                tp1_source = "fallback_1r"
        else:
            tp1_price = tp1_fallback
            tp1_source = "fallback_1r"

        if tp1_source == "fallback_1r":
            if tp1_diag["candidates_total"] == 0:
                tp1_diag["fallback_reason"] = "no_candidates_input"
            elif tp1_diag["candidates_after_direction"] == 0:
                tp1_diag["fallback_reason"] = "no_candidates_after_direction"
            elif tp1_diag["candidates_after_mitigated"] == 0:
                tp1_diag["fallback_reason"] = "no_candidates_after_mitigated"
            else:
                tp1_diag["fallback_reason"] = "best_rr_below_min"

        # --- TP2 e TP3: escalas R:R sobre o SL real ---
        tp2_price = entry_price + tp2_target_rr * sl_distance
        tp3_price = entry_price + tp3_target_rr * sl_distance

    else:  # SHORT
        # --- SL: prioridade estrutural (espelho) ---
        sl_candidates: list[tuple[float, str]] = []
        swing_high = _last_swing_high(df, lookback=swing_lookback)
        sl_candidates.append(
            (swing_high + swing_buffer_atr * atr_value, "structural_swing")
        )

        for ob in obs:
            if (
                getattr(ob, "direction", None) == "bearish"
                and not getattr(ob, "mitigated", True)
                and ob.top > entry_price
            ):
                sl_candidates.append(
                    (ob.top + swing_buffer_atr * atr_value, "structural_ob")
                )
        for br in brks:
            if (
                getattr(br, "new_direction", None) == "bearish"
                and not getattr(br, "invalidated", True)
                and br.top > entry_price
            ):
                sl_candidates.append(
                    (br.top + swing_buffer_atr * atr_value, "structural_breaker")
                )

        sl_estrutural, sl_winner_source = max(sl_candidates, key=lambda c: c[0])
        sl_source = sl_winner_source

        sl_min = entry_price + sl_min_atr_mult * atr_value
        sl_max = entry_price * (1.0 + sl_max_pct / 100.0)
        sl_price = min(sl_max, max(sl_estrutural, sl_min))

        clamp_applied = None
        if sl_price != sl_estrutural:
            if sl_price == sl_min:
                sl_source = "clamp_atr_min"
                clamp_applied = "atr_min"
            elif sl_price == sl_max:
                sl_source = "clamp_pct_max"
                clamp_applied = "pct_max"

        sl_diag = {
            "sl_estrutural_pre_clamp": float(sl_estrutural),
            "sl_min_atr": float(sl_min),
            "sl_max_pct": float(sl_max),
            "clamp_applied": clamp_applied,
            "candidates_count": len(sl_candidates),
            "winner_source": sl_winner_source,
        }

        sl_distance = sl_price - entry_price

        # --- TP1: estrutural com fallback R:R + diagnostics ---
        tp1_diag: dict = {
            "candidates_total": len(obs) + len(brks) + len(fvgs_list),
            "candidates_after_direction": 0,
            "candidates_after_mitigated": 0,
            "candidates_after_rr": 0,
            "best_candidate_rr": None,
            "best_candidate_source": None,
            "fallback_reason": None,
        }
        direction_ok: list[tuple[float, str, bool]] = []
        for ob in obs:
            if getattr(ob, "direction", None) == "bullish" and ob.top < entry_price:
                direction_ok.append(
                    (ob.top, "structural_ob", not getattr(ob, "mitigated", True))
                )
        for br in brks:
            if getattr(br, "new_direction", None) == "bullish" and br.top < entry_price:
                direction_ok.append(
                    (br.top, "structural_breaker", not getattr(br, "invalidated", True))
                )
        for fvg in fvgs_list:
            if getattr(fvg, "direction", None) == "bearish" and fvg.bottom < entry_price:
                direction_ok.append(
                    (fvg.bottom, "structural_fvg", not getattr(fvg, "filled", True))
                )
        tp1_diag["candidates_after_direction"] = len(direction_ok)

        tp1_candidates: list[tuple[float, str]] = [
            (price, source) for price, source, active in direction_ok if active
        ]
        tp1_diag["candidates_after_mitigated"] = len(tp1_candidates)

        if tp1_candidates:
            rrs = [
                (price, source, (entry_price - price) / sl_distance)
                for price, source in tp1_candidates
            ]
            best_price, best_src, best_rr = max(rrs, key=lambda c: c[2])
            tp1_diag["best_candidate_rr"] = float(best_rr)
            tp1_diag["best_candidate_source"] = best_src
            tp1_diag["candidates_after_rr"] = sum(
                1 for _, _, rr in rrs if rr >= tp1_min_rr
            )

        tp1_fallback = entry_price - tp1_min_rr * sl_distance
        if tp1_candidates:
            tp1_estrutural, tp1_candidate_source = max(
                tp1_candidates, key=lambda c: c[0]
            )
            rr_estrutural = (entry_price - tp1_estrutural) / sl_distance
            # Espelho do LONG: estrutural só é aceito se cabe entre tp1_min_rr
            # e tp2_target_rr (exclusive), preservando tp1 > tp2 > tp3.
            if tp1_min_rr <= rr_estrutural < tp2_target_rr:
                tp1_price = tp1_estrutural
                tp1_source = tp1_candidate_source
            else:
                tp1_price = tp1_fallback
                tp1_source = "fallback_1r"
        else:
            tp1_price = tp1_fallback
            tp1_source = "fallback_1r"

        if tp1_source == "fallback_1r":
            if tp1_diag["candidates_total"] == 0:
                tp1_diag["fallback_reason"] = "no_candidates_input"
            elif tp1_diag["candidates_after_direction"] == 0:
                tp1_diag["fallback_reason"] = "no_candidates_after_direction"
            elif tp1_diag["candidates_after_mitigated"] == 0:
                tp1_diag["fallback_reason"] = "no_candidates_after_mitigated"
            else:
                tp1_diag["fallback_reason"] = "best_rr_below_min"

        tp2_price = entry_price - tp2_target_rr * sl_distance
        tp3_price = entry_price - tp3_target_rr * sl_distance

    sl_distance_pct = abs(entry_price - sl_price) / entry_price * 100.0
    tp1_distance_pct = abs(tp1_price - entry_price) / entry_price * 100.0
    tp2_distance_pct = abs(tp2_price - entry_price) / entry_price * 100.0
    tp3_distance_pct = abs(tp3_price - entry_price) / entry_price * 100.0

    risk = abs(entry_price - sl_price)
    if direction == "LONG":
        rr1 = (tp1_price - entry_price) / risk if risk > 0 else 0.0
        rr2 = (tp2_price - entry_price) / risk if risk > 0 else 0.0
        rr3 = (tp3_price - entry_price) / risk if risk > 0 else 0.0
    else:
        rr1 = (entry_price - tp1_price) / risk if risk > 0 else 0.0
        rr2 = (entry_price - tp2_price) / risk if risk > 0 else 0.0
        rr3 = (entry_price - tp3_price) / risk if risk > 0 else 0.0

    leverage = calculate_leverage(
        setup_confidence=setup_confidence,
        regime=regime,
        atr_value=atr_value,
        entry_price=entry_price,
        direction=direction,
    )

    return TradePlan(
        direction=direction,
        entry_price=float(entry_price),
        sl_price=float(sl_price),
        sl_distance_pct=float(sl_distance_pct),
        tp1_price=float(tp1_price),
        tp2_price=float(tp2_price),
        tp3_price=float(tp3_price),
        tp1_distance_pct=float(tp1_distance_pct),
        tp2_distance_pct=float(tp2_distance_pct),
        tp3_distance_pct=float(tp3_distance_pct),
        risk_reward_tp1=float(rr1),
        risk_reward_tp2=float(rr2),
        risk_reward_tp3=float(rr3),
        atr_value=float(atr_value),
        position_split=POSITION_SPLIT,
        leverage=float(leverage),
        tp1_source=tp1_source,
        sl_source=sl_source,
        tp1_diagnostics=tp1_diag,
        sl_diagnostics=sl_diag,
    )


# ---------------------------------------------------------------------------
# 2. Stop dinâmico — derisking V2
# ---------------------------------------------------------------------------


def update_trade_state(
    trade_state: TradeState,
    current_candle: dict,
) -> TradeState:
    """Atualiza o estado de um trade com base no candle mais recente.

    Implementa o derisking V2:
        LONG
            - low <= current_sl → SL batido (LOSS_SL se nenhum TP hit;
              WIN parcial se TP1 já hit)
            - high >= tp1 → TP1 hit, 50% da posição realizada, SL = entry
            - high >= tp2 → TP2 hit, +30%, SL = tp1 (só após TP1)
            - high >= tp3 → TP3 hit, +20%, status WIN_TP3
        SHORT: espelho invertido.

    Precedência por candle:
        - SL batido antes de qualquer TP tem precedência (stop primeiro)
        - Se SL não bateu, avaliamos TPs em ordem (TP1 → TP2 → TP3).
          Um único candle pode disparar múltiplos níveis em sequência.

    Parâmetros:
        trade_state: estado atual (não mutado).
        current_candle: dict com keys open, high, low, close.

    Retorno:
        Novo TradeState com campos atualizados.
    """
    if trade_state.status in ("LOSS_SL", "WIN_TP1", "WIN_TP2", "WIN_TP3", "CLOSED"):
        return trade_state

    high = float(current_candle["high"])
    low = float(current_candle["low"])

    plan = trade_state.trade_plan
    direction = plan.direction

    current_sl = trade_state.current_sl
    tp1_hit = trade_state.tp1_hit
    tp2_hit = trade_state.tp2_hit
    tp3_hit = trade_state.tp3_hit
    position_remaining = trade_state.position_remaining
    status = trade_state.status

    if direction == "LONG":
        # Stop primeiro — protege contra reversões intra-candle
        if low <= current_sl:
            if tp2_hit:
                status = "WIN_TP2"
            elif tp1_hit:
                status = "WIN_TP1"
            else:
                status = "LOSS_SL"
            position_remaining = 0.0
            return TradeState(
                trade_plan=plan,
                current_sl=current_sl,
                tp1_hit=tp1_hit,
                tp2_hit=tp2_hit,
                tp3_hit=tp3_hit,
                position_remaining=position_remaining,
                status=status,
            )

        # TP1
        if not tp1_hit and high >= plan.tp1_price:
            tp1_hit = True
            position_remaining -= POSITION_SPLIT[0]
            current_sl = plan.entry_price

        # TP2 (só após TP1)
        if tp1_hit and not tp2_hit and high >= plan.tp2_price:
            tp2_hit = True
            position_remaining -= POSITION_SPLIT[1]
            current_sl = plan.tp1_price

        # TP3 — runner fecha
        if tp2_hit and not tp3_hit and high >= plan.tp3_price:
            tp3_hit = True
            position_remaining -= POSITION_SPLIT[2]
            status = "WIN_TP3"

    else:  # SHORT
        if high >= current_sl:
            if tp2_hit:
                status = "WIN_TP2"
            elif tp1_hit:
                status = "WIN_TP1"
            else:
                status = "LOSS_SL"
            position_remaining = 0.0
            return TradeState(
                trade_plan=plan,
                current_sl=current_sl,
                tp1_hit=tp1_hit,
                tp2_hit=tp2_hit,
                tp3_hit=tp3_hit,
                position_remaining=position_remaining,
                status=status,
            )

        if not tp1_hit and low <= plan.tp1_price:
            tp1_hit = True
            position_remaining -= POSITION_SPLIT[0]
            current_sl = plan.entry_price

        if tp1_hit and not tp2_hit and low <= plan.tp2_price:
            tp2_hit = True
            position_remaining -= POSITION_SPLIT[1]
            current_sl = plan.tp1_price

        if tp2_hit and not tp3_hit and low <= plan.tp3_price:
            tp3_hit = True
            position_remaining -= POSITION_SPLIT[2]
            status = "WIN_TP3"

    # Garante não-negatividade por floating point
    if position_remaining < 1e-9:
        position_remaining = 0.0

    return TradeState(
        trade_plan=plan,
        current_sl=float(current_sl),
        tp1_hit=tp1_hit,
        tp2_hit=tp2_hit,
        tp3_hit=tp3_hit,
        position_remaining=float(position_remaining),
        status=status,
    )


# ---------------------------------------------------------------------------
# 3. Risk/Reward
# ---------------------------------------------------------------------------


def estimate_risk_reward(trade_plan: TradePlan) -> tuple[float, float, float]:
    """Calcula R:R para cada TP relativo ao SL.

    Retorna (r1, r2, r3). Se risk for zero retorna (0, 0, 0).
    """
    risk = abs(trade_plan.entry_price - trade_plan.sl_price)
    if risk <= 0:
        return (0.0, 0.0, 0.0)

    if trade_plan.direction == "LONG":
        r1 = (trade_plan.tp1_price - trade_plan.entry_price) / risk
        r2 = (trade_plan.tp2_price - trade_plan.entry_price) / risk
        r3 = (trade_plan.tp3_price - trade_plan.entry_price) / risk
    else:
        r1 = (trade_plan.entry_price - trade_plan.tp1_price) / risk
        r2 = (trade_plan.entry_price - trade_plan.tp2_price) / risk
        r3 = (trade_plan.entry_price - trade_plan.tp3_price) / risk

    return (float(r1), float(r2), float(r3))


# ---------------------------------------------------------------------------
# 4. Validação do plano
# ---------------------------------------------------------------------------


def validate_trade_plan(
    trade_plan: TradePlan,
    min_rr_tp1: float = DEFAULT_MIN_RR_TP1,
) -> tuple[bool, str]:
    """Verifica se o TradePlan é operacional.

    Validações:
        1. Direção coerente: TPs e SL no lado correto do entry.
        2. R:R TP1 >= min_rr_tp1 (trade com retorno insuficiente para o risco).
        3. SL distance <= MAX_SL_DISTANCE_PCT (stop gigante = alavancagem
           destrutiva).
        4. Leverage dentro do range [MIN_LEVERAGE, MAX_LEVERAGE].

    Retorno:
        (is_valid, reason) — reason = "" se válido.
    """
    d = trade_plan.direction
    entry = trade_plan.entry_price

    if d == "LONG":
        if not (trade_plan.sl_price < entry < trade_plan.tp1_price
                <= trade_plan.tp2_price <= trade_plan.tp3_price):
            return False, "Preços incoerentes para LONG (SL < entry < TP1 <= TP2 <= TP3)"
    elif d == "SHORT":
        if not (trade_plan.sl_price > entry > trade_plan.tp1_price
                >= trade_plan.tp2_price >= trade_plan.tp3_price):
            return False, "Preços incoerentes para SHORT (SL > entry > TP1 >= TP2 >= TP3)"
    else:
        return False, f"Direção inválida: {d!r}"

    # Tolerância numérica: protege contra falso negativo quando rr_tp1 cai em
    # MIN_RR_TP1 - épsilon por arredondamento de float (ex: 0.99999...).
    if trade_plan.risk_reward_tp1 < min_rr_tp1 - 1e-9:
        return False, (
            f"R:R TP1 abaixo do mínimo ({trade_plan.risk_reward_tp1:.2f} < {min_rr_tp1})"
        )

    if trade_plan.sl_distance_pct > MAX_SL_DISTANCE_PCT:
        return False, (
            f"SL distance excessiva "
            f"({trade_plan.sl_distance_pct:.2f}% > {MAX_SL_DISTANCE_PCT}%)"
        )

    if not (MIN_LEVERAGE <= trade_plan.leverage <= MAX_LEVERAGE):
        return False, (
            f"Leverage fora do range [{MIN_LEVERAGE}, {MAX_LEVERAGE}]: "
            f"{trade_plan.leverage}"
        )

    return True, ""
