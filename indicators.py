"""indicators.py — MarketContext unificado (v9).

Wrapper fino sobre ``pandas_ta`` que consolida todos os indicadores técnicos
consumidos pelos setups e pelo módulo ``risk``. A interface central é o
dataclass :class:`MarketContext`, um snapshot completo de um token em um
timestamp que é calculado uma única vez por rodada e reutilizado.

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — funções puras
    - Dependências: pandas, numpy, pandas_ta, smc_lib

Colunas obrigatórias do DataFrame de entrada:
    open, high, low, close, volume   (indexado por timestamp)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as pta

from smc_lib import detect_swing_points

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

MIN_CANDLES: int = 100
DEFAULT_SWING_LEFT: int = 3
DEFAULT_SWING_RIGHT: int = 3
DEFAULT_LOOKBACK_SWING: int = 50

_REQUIRED_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close", "volume")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class MarketContext:
    """Snapshot completo de indicadores para um token em um timestamp.

    Calculado uma vez por token por rodada; reutilizado por todos os setups.
    """

    # Metadata
    symbol: str
    timestamp: pd.Timestamp
    timeframe: str

    # Preços da última vela
    close: float
    open: float
    high: float
    low: float
    volume: float

    # Trend / momentum
    ema9: float
    ema21: float
    ema50: float
    ema_slope_21: float
    adx: float
    rsi_3: float
    rsi_14: float
    williams_r_14: float

    # Volatilidade / bandas
    atr: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    bb_width: float
    bb_position: float

    # Volume
    volume_median_20: float
    volume_ratio: float
    obv: float
    cmf: float

    # Structure
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]
    last_swing_high_idx: Optional[int]
    last_swing_low_idx: Optional[int]
    structure_bias: str

    # Channels
    donchian_upper_20: float
    donchian_lower_20: float
    donchian_mid_20: float

    # Candle atual
    wick_top_pct: float
    wick_bottom_pct: float
    body_pct: float
    is_bullish: bool


@dataclass
class MultiTFContext:
    """Contexto multi-timeframe: 15m completo + 1h/4h leves."""

    primary: MarketContext
    htf_1h: dict
    htf_4h: dict


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------


def _validate_df(df: pd.DataFrame, *, min_candles: int = MIN_CANDLES) -> None:
    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame ausente colunas obrigatórias: {missing}")
    if len(df) < min_candles:
        raise ValueError(
            f"DataFrame com {len(df)} candles; mínimo exigido: {min_candles}"
        )


def _safe_float(value: float, default: float = 0.0) -> float:
    """Converte para float retornando ``default`` em NaN/None."""
    if value is None:
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(f):
        return default
    return f


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _derive_structure_bias(
    last_sh_idx: Optional[int],
    last_sl_idx: Optional[int],
    ema21: float,
    ema50: float,
) -> str:
    """Combina ordem de swings e relação EMA21/EMA50.

    Regra:
        - Sinal de swings: último swing mais recente é high → bullish;
          se é low → bearish; se nenhum swing → neutral.
        - Sinal de EMAs: ema21 > ema50 → bullish; ema21 < ema50 → bearish;
          igualdade → neutral.
        - Retorno: "bullish" ou "bearish" apenas quando ambos concordam.
          Caso contrário, "neutral".
    """
    if last_sh_idx is None and last_sl_idx is None:
        swing_bias = "neutral"
    elif last_sh_idx is None:
        swing_bias = "bearish"
    elif last_sl_idx is None:
        swing_bias = "bullish"
    elif last_sh_idx > last_sl_idx:
        swing_bias = "bullish"
    elif last_sl_idx > last_sh_idx:
        swing_bias = "bearish"
    else:
        swing_bias = "neutral"

    if ema21 > ema50:
        ema_bias = "bullish"
    elif ema21 < ema50:
        ema_bias = "bearish"
    else:
        ema_bias = "neutral"

    if swing_bias == ema_bias and swing_bias != "neutral":
        return swing_bias
    return "neutral"


def _last_swing_prices(
    df: pd.DataFrame, left: int, right: int
) -> tuple[Optional[float], Optional[float], Optional[int], Optional[int]]:
    """Extrai preço e índice (posicional) das últimas swing high e swing low.

    Os índices retornados são posicionais dentro de ``df`` (0..len(df)-1),
    não labels do ``DatetimeIndex`` — callers que precisem de timestamp podem
    acessar ``df.index[idx]``.
    """
    swings = detect_swing_points(df, left=left, right=right)
    sh_mask = swings["swing_high"].to_numpy()
    sl_mask = swings["swing_low"].to_numpy()

    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)

    sh_positions = np.flatnonzero(sh_mask)
    sl_positions = np.flatnonzero(sl_mask)

    last_sh_price = float(high[sh_positions[-1]]) if sh_positions.size else None
    last_sh_idx = int(sh_positions[-1]) if sh_positions.size else None
    last_sl_price = float(low[sl_positions[-1]]) if sl_positions.size else None
    last_sl_idx = int(sl_positions[-1]) if sl_positions.size else None

    return last_sh_price, last_sl_price, last_sh_idx, last_sl_idx


def _build_htf_dict(df: pd.DataFrame) -> dict:
    """Resumo leve de um timeframe superior.

    Campos:
        ema21_above_ema50 : bool
        rsi_14            : float
        adx               : float
    """
    ema21 = _safe_float(pta.ema(df["close"], length=21).iloc[-1])
    ema50 = _safe_float(pta.ema(df["close"], length=50).iloc[-1])
    rsi_14 = _safe_float(pta.rsi(df["close"], length=14).iloc[-1], default=50.0)

    adx_df = pta.adx(df["high"], df["low"], df["close"], length=14)
    adx_val = 0.0
    if adx_df is not None and "ADX_14" in adx_df.columns:
        adx_val = _safe_float(adx_df["ADX_14"].iloc[-1])

    return {
        "ema21_above_ema50": bool(ema21 > ema50),
        "rsi_14": rsi_14,
        "adx": adx_val,
    }


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def build_market_context(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str = "15m",
) -> MarketContext:
    """Calcula todos os indicadores e retorna um :class:`MarketContext`.

    Parâmetros:
        df: DataFrame OHLCV com no mínimo ``MIN_CANDLES`` (100) candles,
            indexado por timestamp (ordem cronológica crescente).
        symbol: identificador do token (ex.: ``"BTCUSDT"``).
        timeframe: timeframe dos candles (padrão ``"15m"``).

    Retorno:
        :class:`MarketContext` com valores extraídos da última vela de ``df``.

    Exceções:
        ValueError se colunas obrigatórias estão ausentes ou
        ``len(df) < MIN_CANDLES``.
    """
    _validate_df(df)

    close = df["close"]
    high = df["high"]
    low = df["low"]
    open_ = df["open"]
    volume = df["volume"]

    # --- Trend / momentum --------------------------------------------------
    ema9_series = pta.ema(close, length=9)
    ema21_series = pta.ema(close, length=21)
    ema50_series = pta.ema(close, length=50)
    ema9 = _safe_float(ema9_series.iloc[-1])
    ema21 = _safe_float(ema21_series.iloc[-1])
    ema50 = _safe_float(ema50_series.iloc[-1])

    ema21_prev = (
        _safe_float(ema21_series.iloc[-6]) if len(ema21_series) >= 6 else 0.0
    )
    if ema21_prev > 0:
        ema_slope_21 = (ema21 - ema21_prev) / ema21_prev * 100.0
    else:
        ema_slope_21 = 0.0

    adx_df = pta.adx(high, low, close, length=14)
    adx = 0.0
    if adx_df is not None and "ADX_14" in adx_df.columns:
        adx = _safe_float(adx_df["ADX_14"].iloc[-1])

    rsi_3 = _safe_float(pta.rsi(close, length=3).iloc[-1], default=50.0)
    rsi_14 = _safe_float(pta.rsi(close, length=14).iloc[-1], default=50.0)
    williams_r_14 = _safe_float(
        pta.willr(high, low, close, length=14).iloc[-1], default=-50.0
    )

    # --- Volatilidade / bandas --------------------------------------------
    atr = _safe_float(pta.atr(high, low, close, length=14).iloc[-1])

    bb = pta.bbands(close, length=20, std=2)
    bb_upper = _safe_float(bb["BBU_20_2.0"].iloc[-1])
    bb_middle = _safe_float(bb["BBM_20_2.0"].iloc[-1])
    bb_lower = _safe_float(bb["BBL_20_2.0"].iloc[-1])
    bb_range = bb_upper - bb_lower
    bb_width = bb_range / bb_middle if bb_middle > 0 else 0.0
    last_close = _safe_float(close.iloc[-1])
    bb_position = (
        _clamp((last_close - bb_lower) / bb_range) if bb_range > 0 else 0.5
    )

    # --- Volume ------------------------------------------------------------
    volume_median_20 = _safe_float(volume.rolling(20).median().iloc[-1])
    last_volume = _safe_float(volume.iloc[-1])
    volume_ratio = (
        last_volume / volume_median_20 if volume_median_20 > 0 else 0.0
    )
    obv = _safe_float(pta.obv(close, volume).iloc[-1])
    cmf = _safe_float(pta.cmf(high, low, close, volume, length=20).iloc[-1])

    # --- Structure ---------------------------------------------------------
    last_sh_price, last_sl_price, last_sh_idx, last_sl_idx = _last_swing_prices(
        df, left=DEFAULT_SWING_LEFT, right=DEFAULT_SWING_RIGHT
    )
    structure_bias = _derive_structure_bias(
        last_sh_idx, last_sl_idx, ema21, ema50
    )

    # --- Channels ----------------------------------------------------------
    donchian_upper_20 = _safe_float(high.rolling(20).max().iloc[-1])
    donchian_lower_20 = _safe_float(low.rolling(20).min().iloc[-1])
    donchian_mid_20 = (donchian_upper_20 + donchian_lower_20) / 2.0

    # --- Candle atual ------------------------------------------------------
    last_open = _safe_float(open_.iloc[-1])
    last_high = _safe_float(high.iloc[-1])
    last_low = _safe_float(low.iloc[-1])
    rng = last_high - last_low
    if rng > 0:
        wick_top_pct = (last_high - max(last_open, last_close)) / rng
        wick_bottom_pct = (min(last_open, last_close) - last_low) / rng
        body_pct = abs(last_close - last_open) / rng
    else:
        wick_top_pct = 0.0
        wick_bottom_pct = 0.0
        body_pct = 0.0
    is_bullish = last_close > last_open

    return MarketContext(
        symbol=symbol,
        timestamp=df.index[-1],
        timeframe=timeframe,
        close=last_close,
        open=last_open,
        high=last_high,
        low=last_low,
        volume=last_volume,
        ema9=ema9,
        ema21=ema21,
        ema50=ema50,
        ema_slope_21=ema_slope_21,
        adx=adx,
        rsi_3=rsi_3,
        rsi_14=rsi_14,
        williams_r_14=williams_r_14,
        atr=atr,
        bb_upper=bb_upper,
        bb_middle=bb_middle,
        bb_lower=bb_lower,
        bb_width=bb_width,
        bb_position=bb_position,
        volume_median_20=volume_median_20,
        volume_ratio=volume_ratio,
        obv=obv,
        cmf=cmf,
        last_swing_high=last_sh_price,
        last_swing_low=last_sl_price,
        last_swing_high_idx=last_sh_idx,
        last_swing_low_idx=last_sl_idx,
        structure_bias=structure_bias,
        donchian_upper_20=donchian_upper_20,
        donchian_lower_20=donchian_lower_20,
        donchian_mid_20=donchian_mid_20,
        wick_top_pct=wick_top_pct,
        wick_bottom_pct=wick_bottom_pct,
        body_pct=body_pct,
        is_bullish=is_bullish,
    )


def build_multi_timeframe_context(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    symbol: str,
) -> MultiTFContext:
    """Contexto multi-timeframe: :class:`MarketContext` em 15m + resumos HTF.

    Parâmetros:
        df_15m, df_1h, df_4h: DataFrames OHLCV por timeframe.
        symbol: identificador do token.

    Retorno:
        :class:`MultiTFContext` com ``primary`` em 15m e dicts leves
        ``{"ema21_above_ema50", "rsi_14", "adx"}`` para 1h e 4h.
    """
    primary = build_market_context(df_15m, symbol, timeframe="15m")
    return MultiTFContext(
        primary=primary,
        htf_1h=_build_htf_dict(df_1h),
        htf_4h=_build_htf_dict(df_4h),
    )


def get_last_swing_levels(
    df: pd.DataFrame,
    lookback: int = DEFAULT_LOOKBACK_SWING,
) -> tuple[Optional[float], Optional[float], Optional[int], Optional[int]]:
    """Últimas swing high e swing low válidas na janela recente.

    Útil para SL estrutural sem precisar calcular o :class:`MarketContext`
    completo.

    Parâmetros:
        df: DataFrame OHLCV.
        lookback: número de candles finais a considerar (padrão 50).

    Retorno:
        Tupla ``(last_sh_price, last_sl_price, last_sh_idx, last_sl_idx)``.
        Os índices são posicionais dentro de ``df`` (0..len(df)-1).
        ``None`` nos campos onde nenhum swing foi encontrado na janela.
    """
    n = len(df)
    if n == 0:
        return None, None, None, None

    lookback = min(lookback, n)
    start = n - lookback
    recent = df.iloc[start:]

    sh_price, sl_price, sh_rel, sl_rel = _last_swing_prices(
        recent, left=DEFAULT_SWING_LEFT, right=DEFAULT_SWING_RIGHT
    )

    sh_idx = sh_rel + start if sh_rel is not None else None
    sl_idx = sl_rel + start if sl_rel is not None else None
    return sh_price, sl_price, sh_idx, sl_idx
