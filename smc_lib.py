"""smc_lib.py — Smart Money Concepts structure detection library (v9).

Biblioteca pura de detecção de estruturas Smart Money Concepts.
Primeiro módulo da fundação v9.

Lógicas portadas de referências Pine Script:
    - LuxAlgo — Smart Money Concepts (structure: BoS / CHoCH, swing detection)
    - Flux Charts — Price Action Toolkit
        - Volumized Order Blocks
        - Breaker Blocks
        - Fair Value Gaps

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — todas as funções são puras
    - Input padrão: DataFrame pandas com colunas OHLCV em lowercase
    - Output padrão: dataclasses ou dicts com tipagem clara
    - Dependências mínimas: pandas, numpy, dataclasses, typing

Colunas obrigatórias do DataFrame:
    open, high, low, close, volume   (indexado por timestamp)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constantes (defaults)
# ---------------------------------------------------------------------------

DEFAULT_SWING_LEFT: int = 3
DEFAULT_SWING_RIGHT: int = 3
DEFAULT_STRUCTURE_SWING_LENGTH: int = 5
DEFAULT_ATR_PERIOD: int = 14
DEFAULT_MIN_OB_STRENGTH_ATR: float = 0.3
DEFAULT_MIN_FVG_GAP_ATR: float = 0.1

_REQUIRED_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close", "volume")


# ---------------------------------------------------------------------------
# Dataclasses de saída
# ---------------------------------------------------------------------------


@dataclass
class OrderBlock:
    """Order Block volumétrico detectado a partir de quebra de estrutura."""

    direction: str
    index_start: int
    index_end: int
    top: float
    bottom: float
    total_volume: float
    bull_volume_ratio: float
    strength_atr: float
    mitigated: bool
    mitigation_index: Optional[int]


@dataclass
class BreakerBlock:
    """Breaker Block — OB mitigado que passa a valer na direção oposta."""

    original_direction: str
    new_direction: str
    index_start: int
    index_end: int
    top: float
    bottom: float
    break_volume: float
    retest_count: int
    invalidated: bool
    invalidation_index: Optional[int]


@dataclass
class FairValueGap:
    """Fair Value Gap — imbalance de 3 candles."""

    direction: str
    index: int
    top: float
    bottom: float
    size_atr: float
    filled: bool
    fill_index: Optional[int]


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------


def _validate_df(df: pd.DataFrame, need_volume: bool = True) -> None:
    required = _REQUIRED_COLUMNS if need_volume else _REQUIRED_COLUMNS[:4]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame ausente colunas obrigatórias: {missing}")


def _atr(df: pd.DataFrame, period: int = DEFAULT_ATR_PERIOD) -> pd.Series:
    """ATR interno — Wilder's RMA (Running Moving Average) do True Range.

    Wilder's RMA é o ATR canônico usado em Pine Script e na maioria dos
    sistemas de TA. Equivale a uma EMA com alpha=1/period (em vez do
    alpha=2/(span+1) da EMA padrão). Garante compatibilidade numérica com
    indicadores de referência (LuxAlgo, Flux Charts, pandas_ta).

    Usado apenas para normalização de forças/gaps. Não exportado publicamente.

    Parâmetros:
        df: DataFrame com colunas high, low, close.
        period: período do RMA (padrão 14).

    Retorno:
        pd.Series de ATR indexada como df. Primeiros valores podem ser NaN.
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


# ---------------------------------------------------------------------------
# 1. Swing points
# ---------------------------------------------------------------------------


def detect_swing_points(
    df: pd.DataFrame,
    left: int = DEFAULT_SWING_LEFT,
    right: int = DEFAULT_SWING_RIGHT,
) -> dict[str, pd.Series]:
    """Detecta swing highs e swing lows fractais.

    Um candle i é swing high se high[i] é o máximo em [i-left, i+right].
    Análogo para swing low.

    Parâmetros:
        df: DataFrame com colunas OHLC.
        left: candles à esquerda para confirmar (padrão 3).
        right: candles à direita para confirmar (padrão 3).

    Retorno:
        dict com duas pd.Series booleanas indexadas como df:
            swing_high: True onde candle é swing high.
            swing_low:  True onde candle é swing low.
    """
    _validate_df(df, need_volume=False)

    n = len(df)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)

    sh = np.zeros(n, dtype=bool)
    sl = np.zeros(n, dtype=bool)

    for i in range(left, n - right):
        window_hi = high[i - left : i + right + 1]
        window_lo = low[i - left : i + right + 1]
        if high[i] == window_hi.max() and (window_hi == high[i]).sum() == 1:
            sh[i] = True
        if low[i] == window_lo.min() and (window_lo == low[i]).sum() == 1:
            sl[i] = True

    return {
        "swing_high": pd.Series(sh, index=df.index),
        "swing_low": pd.Series(sl, index=df.index),
    }


# ---------------------------------------------------------------------------
# 2. Market structure — BoS / CHoCH
# ---------------------------------------------------------------------------


def detect_market_structure(
    df: pd.DataFrame,
    swing_length: int = DEFAULT_STRUCTURE_SWING_LENGTH,
) -> pd.DataFrame:
    """Detecta Break of Structure (BoS) e Change of Character (CHoCH).

    Portado do LuxAlgo SMC — tracker de último swing high/low confirmado
    e emissão de eventos quando o close os atravessa.

    Parâmetros:
        df: DataFrame com colunas OHLC.
        swing_length: sensibilidade dos swings (left=right=swing_length).

    Retorno:
        pd.DataFrame com mesmo índice do input e colunas:
            bos_up         : bool — continuação bullish
            bos_down       : bool — continuação bearish
            choch_up       : bool — reversão para bullish
            choch_down     : bool — reversão para bearish
            last_structure : str  — "bullish" | "bearish" | "neutral"
    """
    _validate_df(df, need_volume=False)

    n = len(df)
    swings = detect_swing_points(df, left=swing_length, right=swing_length)
    sh = swings["swing_high"].to_numpy()
    sl = swings["swing_low"].to_numpy()

    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)

    bos_up = np.zeros(n, dtype=bool)
    bos_down = np.zeros(n, dtype=bool)
    choch_up = np.zeros(n, dtype=bool)
    choch_down = np.zeros(n, dtype=bool)
    last_struct = np.empty(n, dtype=object)

    top_price: Optional[float] = None
    bottom_price: Optional[float] = None
    top_broken: bool = True
    bottom_broken: bool = True
    structure: str = "neutral"

    for i in range(n):
        # Swing detectado em i-swing_length só é confirmado agora (precisa dos
        # `right` candles futuros). Antes disso não pode ser usado como pivô.
        confirm_idx = i - swing_length
        if confirm_idx >= 0:
            if sh[confirm_idx]:
                top_price = float(high[confirm_idx])
                top_broken = False
            if sl[confirm_idx]:
                bottom_price = float(low[confirm_idx])
                bottom_broken = False

        # Quebra do último swing high confirmado
        if top_price is not None and not top_broken and close[i] > top_price:
            if structure == "bearish":
                choch_up[i] = True
            else:
                bos_up[i] = True
            structure = "bullish"
            top_broken = True

        # Quebra do último swing low confirmado
        if bottom_price is not None and not bottom_broken and close[i] < bottom_price:
            if structure == "bullish":
                choch_down[i] = True
            else:
                bos_down[i] = True
            structure = "bearish"
            bottom_broken = True

        last_struct[i] = structure

    return pd.DataFrame(
        {
            "bos_up": bos_up,
            "bos_down": bos_down,
            "choch_up": choch_up,
            "choch_down": choch_down,
            "last_structure": last_struct,
        },
        index=df.index,
    )


# ---------------------------------------------------------------------------
# 3. Order Blocks volumétricos
# ---------------------------------------------------------------------------


def detect_order_blocks(
    df: pd.DataFrame,
    swing_length: int = DEFAULT_STRUCTURE_SWING_LENGTH,
    atr_period: int = DEFAULT_ATR_PERIOD,
    min_strength_atr: float = DEFAULT_MIN_OB_STRENGTH_ATR,
) -> list[OrderBlock]:
    """Detecta Order Blocks volumétricos (bullish e bearish).

    Portado do Flux Charts Volumized Order Blocks.
        Bullish OB: identificado por BoS up. O candle com low mínimo entre o
        swing high quebrado e o candle do BoS define a zona.
            top    = max(open, close) desse candle
            bottom = low               desse candle
        Bearish OB: análogo invertido (candle com high máximo define a zona).

    Volume total = volume do candle do OB + 2 candles seguintes.
    bull_volume_ratio = proporção do volume de candles bullish (close>open)
    no conjunto das 3 velas — leitura semântica do "impulso" portada do
    Flux Charts.

    Mitigação:
        Bullish OB — close posterior < bottom.
        Bearish OB — close posterior > top.

    Parâmetros:
        df: DataFrame com colunas OHLCV.
        swing_length: sensibilidade dos swings.
        atr_period: período do ATR para normalização.
        min_strength_atr: força mínima (top-bottom)/ATR para manter o OB.

    Retorno:
        list[OrderBlock] ordenada do mais antigo ao mais recente.
    """
    _validate_df(df, need_volume=True)

    n = len(df)
    if n == 0:
        return []

    swings = detect_swing_points(df, left=swing_length, right=swing_length)
    sh = swings["swing_high"].to_numpy()
    sl = swings["swing_low"].to_numpy()
    atr = _atr(df, period=atr_period).to_numpy()

    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    open_ = df["open"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    volume = df["volume"].to_numpy(dtype=float)

    order_blocks: list[OrderBlock] = []

    top_price: Optional[float] = None
    top_source_idx: Optional[int] = None
    bottom_price: Optional[float] = None
    bottom_source_idx: Optional[int] = None
    top_broken: bool = True
    bottom_broken: bool = True

    for i in range(n):
        confirm_idx = i - swing_length
        if confirm_idx >= 0:
            if sh[confirm_idx]:
                top_price = float(high[confirm_idx])
                top_source_idx = confirm_idx
                top_broken = False
            if sl[confirm_idx]:
                bottom_price = float(low[confirm_idx])
                bottom_source_idx = confirm_idx
                bottom_broken = False

        # Bullish OB — disparado por quebra de swing high (BoS/CHoCH up)
        if (
            top_price is not None
            and top_source_idx is not None
            and not top_broken
            and close[i] > top_price
        ):
            ob = _build_bullish_ob(
                top_source_idx, i, open_, close, high, low, volume, atr,
                min_strength_atr,
            )
            if ob is not None:
                order_blocks.append(ob)
            top_broken = True

        # Bearish OB — disparado por quebra de swing low (BoS/CHoCH down)
        if (
            bottom_price is not None
            and bottom_source_idx is not None
            and not bottom_broken
            and close[i] < bottom_price
        ):
            ob = _build_bearish_ob(
                bottom_source_idx, i, open_, close, high, low, volume, atr,
                min_strength_atr,
            )
            if ob is not None:
                order_blocks.append(ob)
            bottom_broken = True

    # Passe de mitigação
    for ob in order_blocks:
        for j in range(ob.index_end + 1, n):
            if ob.direction == "bullish" and close[j] < ob.bottom:
                ob.mitigated = True
                ob.mitigation_index = j
                break
            if ob.direction == "bearish" and close[j] > ob.top:
                ob.mitigated = True
                ob.mitigation_index = j
                break

    return order_blocks


def _build_bullish_ob(
    swing_idx: int,
    break_idx: int,
    open_: np.ndarray,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    volume: np.ndarray,
    atr: np.ndarray,
    min_strength_atr: float,
) -> Optional[OrderBlock]:
    # Busca do candle com low mínimo estritamente entre o swing high e o
    # candle da quebra — a zona de "origem" do impulso bullish.
    n = len(close)
    if break_idx - swing_idx <= 1:
        return None
    lo_slice = low[swing_idx + 1 : break_idx]
    min_low_offset = int(np.argmin(lo_slice))
    ob_idx = swing_idx + 1 + min_low_offset

    ob_top = float(max(open_[ob_idx], close[ob_idx]))
    ob_bottom = float(low[ob_idx])

    formation = list(range(ob_idx, min(ob_idx + 3, n)))
    total_vol = float(sum(volume[k] for k in formation))
    bull_vol = float(
        sum(volume[k] for k in formation if close[k] > open_[k])
    )
    bull_ratio = bull_vol / total_vol if total_vol > 0 else 0.0

    atr_val = float(atr[break_idx]) if not np.isnan(atr[break_idx]) else 0.0
    strength = (ob_top - ob_bottom) / atr_val if atr_val > 0 else 0.0

    if strength < min_strength_atr:
        return None

    return OrderBlock(
        direction="bullish",
        index_start=ob_idx,
        index_end=break_idx,
        top=ob_top,
        bottom=ob_bottom,
        total_volume=total_vol,
        bull_volume_ratio=bull_ratio,
        strength_atr=strength,
        mitigated=False,
        mitigation_index=None,
    )


def _build_bearish_ob(
    swing_idx: int,
    break_idx: int,
    open_: np.ndarray,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    volume: np.ndarray,
    atr: np.ndarray,
    min_strength_atr: float,
) -> Optional[OrderBlock]:
    # Busca do candle com high máximo estritamente entre o swing low e o
    # candle da quebra — a zona de "origem" do impulso bearish.
    n = len(close)
    if break_idx - swing_idx <= 1:
        return None
    hi_slice = high[swing_idx + 1 : break_idx]
    max_high_offset = int(np.argmax(hi_slice))
    ob_idx = swing_idx + 1 + max_high_offset

    ob_top = float(high[ob_idx])
    ob_bottom = float(min(open_[ob_idx], close[ob_idx]))

    formation = list(range(ob_idx, min(ob_idx + 3, n)))
    total_vol = float(sum(volume[k] for k in formation))
    bull_vol = float(
        sum(volume[k] for k in formation if close[k] > open_[k])
    )
    bull_ratio = bull_vol / total_vol if total_vol > 0 else 0.0

    atr_val = float(atr[break_idx]) if not np.isnan(atr[break_idx]) else 0.0
    strength = (ob_top - ob_bottom) / atr_val if atr_val > 0 else 0.0

    if strength < min_strength_atr:
        return None

    return OrderBlock(
        direction="bearish",
        index_start=ob_idx,
        index_end=break_idx,
        top=ob_top,
        bottom=ob_bottom,
        total_volume=total_vol,
        bull_volume_ratio=bull_ratio,
        strength_atr=strength,
        mitigated=False,
        mitigation_index=None,
    )


# ---------------------------------------------------------------------------
# 4. Breaker Blocks
# ---------------------------------------------------------------------------


def detect_breaker_blocks(
    df: pd.DataFrame,
    order_blocks: list[OrderBlock],
) -> list[BreakerBlock]:
    """Detecta Breaker Blocks a partir de OBs mitigados.

    Portado do Flux Charts Breaker Blocks. Um OB mitigado vira uma zona ativa
    na direção oposta — bullish OB mitigado vira bearish breaker e vice-versa.

    Parâmetros:
        df: DataFrame com colunas OHLCV.
        order_blocks: saída de detect_order_blocks.

    Retorno:
        list[BreakerBlock] — um por OB mitigado.

    Invalidação:
        Bearish breaker (OB bullish mitigado) — close posterior > top.
        Bullish breaker (OB bearish mitigado) — close posterior < bottom.
    """
    _validate_df(df, need_volume=True)

    n = len(df)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    volume = df["volume"].to_numpy(dtype=float)

    breakers: list[BreakerBlock] = []

    for ob in order_blocks:
        if not ob.mitigated or ob.mitigation_index is None:
            continue

        new_direction = "bearish" if ob.direction == "bullish" else "bullish"
        break_vol = float(volume[ob.mitigation_index])

        retest_count = 0
        invalidated = False
        invalidation_index: Optional[int] = None

        for j in range(ob.mitigation_index + 1, n):
            if new_direction == "bearish" and close[j] > ob.top:
                invalidated = True
                invalidation_index = j
                break
            if new_direction == "bullish" and close[j] < ob.bottom:
                invalidated = True
                invalidation_index = j
                break
            # Retest — qualquer toque na zona (high ≥ bottom e low ≤ top)
            if high[j] >= ob.bottom and low[j] <= ob.top:
                retest_count += 1

        breakers.append(
            BreakerBlock(
                original_direction=ob.direction,
                new_direction=new_direction,
                index_start=ob.index_start,
                index_end=ob.index_end,
                top=ob.top,
                bottom=ob.bottom,
                break_volume=break_vol,
                retest_count=retest_count,
                invalidated=invalidated,
                invalidation_index=invalidation_index,
            )
        )

    return breakers


# ---------------------------------------------------------------------------
# 5. Fair Value Gaps
# ---------------------------------------------------------------------------


def detect_fvg(
    df: pd.DataFrame,
    min_gap_atr: float = DEFAULT_MIN_FVG_GAP_ATR,
) -> list[FairValueGap]:
    """Detecta Fair Value Gaps (imbalances de 3 candles).

    Portado do Flux Charts FVG.
        Bullish FVG em i: low[i+1] > high[i-1]
            zona: top = low[i+1], bottom = high[i-1]
        Bearish FVG em i: high[i+1] < low[i-1]
            zona: top = low[i-1], bottom = high[i+1]

    Preenchimento:
        Bullish — low posterior ≤ bottom.
        Bearish — high posterior ≥ top.

    Parâmetros:
        df: DataFrame com colunas OHLC.
        min_gap_atr: tamanho mínimo do gap relativo ao ATR.

    Retorno:
        list[FairValueGap] ordenada por índice crescente.
    """
    _validate_df(df, need_volume=False)

    n = len(df)
    if n < 3:
        return []

    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    atr = _atr(df, period=DEFAULT_ATR_PERIOD).to_numpy()

    fvgs: list[FairValueGap] = []

    for i in range(1, n - 1):
        atr_val = float(atr[i]) if not np.isnan(atr[i]) else 0.0

        if low[i + 1] > high[i - 1]:
            top = float(low[i + 1])
            bottom = float(high[i - 1])
            size = (top - bottom) / atr_val if atr_val > 0 else 0.0
            if size >= min_gap_atr:
                fvgs.append(
                    FairValueGap(
                        direction="bullish",
                        index=i,
                        top=top,
                        bottom=bottom,
                        size_atr=float(size),
                        filled=False,
                        fill_index=None,
                    )
                )
        elif high[i + 1] < low[i - 1]:
            top = float(low[i - 1])
            bottom = float(high[i + 1])
            size = (top - bottom) / atr_val if atr_val > 0 else 0.0
            if size >= min_gap_atr:
                fvgs.append(
                    FairValueGap(
                        direction="bearish",
                        index=i,
                        top=top,
                        bottom=bottom,
                        size_atr=float(size),
                        filled=False,
                        fill_index=None,
                    )
                )

    # Passe de preenchimento — inicia em i+2 (depois da formação das 3 velas)
    for fvg in fvgs:
        for j in range(fvg.index + 2, n):
            if fvg.direction == "bullish" and low[j] <= fvg.bottom:
                fvg.filled = True
                fvg.fill_index = j
                break
            if fvg.direction == "bearish" and high[j] >= fvg.top:
                fvg.filled = True
                fvg.fill_index = j
                break

    return fvgs
