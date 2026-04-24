"""regime.py — Market regime classifier (v9).

Segundo módulo da fundação v9. Classifica o regime de mercado de um token
a partir de um DataFrame OHLCV, retornando um dos quatro estados:

    TREND_UP | TREND_DOWN | RANGE | SQUEEZE

Usado pelos setups v9 para decidir quais disparam em cada contexto.

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — todas as funções são puras
    - Input padrão: DataFrame pandas com colunas OHLCV em lowercase
    - Output padrão: dataclass RegimeClassification

Dependências: pandas, numpy, pandas_ta, dataclasses, typing.

Colunas obrigatórias do DataFrame:
    open, high, low, close, volume
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import pandas_ta_classic as ta

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

MIN_CANDLES: int = 100
BB_MEDIAN_WINDOW: int = 50
ADX_TREND_THRESHOLD: float = 25.0
EMA_ALIGNMENT_LOOKBACK: int = 3

_REQUIRED_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close", "volume")


# ---------------------------------------------------------------------------
# Dataclass de saída
# ---------------------------------------------------------------------------


@dataclass
class RegimeClassification:
    """Resultado da classificação de regime de mercado."""

    regime: str
    adx_value: float
    ema_slope: float
    ema_alignment: str
    bb_width_ratio: float
    confidence: float


# ---------------------------------------------------------------------------
# Helpers auxiliares
# ---------------------------------------------------------------------------


def get_ema_slope(series: pd.Series, periods: int = 5) -> float:
    """Inclinação percentual da EMA nos últimos `periods` candles.

    Parâmetros:
        series: série de valores de EMA já calculada.
        periods: quantidade de candles para medir a inclinação.

    Retorno:
        slope percentual: (ema[-1] - ema[-periods]) / ema[-periods] * 100.
        Retorna 0.0 se não houver dados suficientes ou base zero/NaN.
    """
    clean = series.dropna()
    if len(clean) <= periods:
        return 0.0

    current = float(clean.iloc[-1])
    base = float(clean.iloc[-periods])
    if base == 0.0 or not np.isfinite(base) or not np.isfinite(current):
        return 0.0

    return (current - base) / base * 100.0


def get_ema_alignment(ema_short: pd.Series, ema_long: pd.Series, periods: int = 3) -> str:
    """Relação entre EMA curta e EMA longa nas últimas `periods` velas.

    Parâmetros:
        ema_short: série da EMA curta.
        ema_long: série da EMA longa.
        periods: quantidade de velas consecutivas para confirmar alinhamento.

    Retorno:
        "bullish" se EMA_short > EMA_long em todas as `periods` velas,
        "bearish" se EMA_short < EMA_long em todas as `periods` velas,
        "mixed" caso contrário.
    """
    paired = pd.concat([ema_short, ema_long], axis=1).dropna()
    if len(paired) < periods:
        return "mixed"

    recent = paired.iloc[-periods:]
    short_vals = recent.iloc[:, 0].to_numpy()
    long_vals = recent.iloc[:, 1].to_numpy()

    if np.all(short_vals > long_vals):
        return "bullish"
    if np.all(short_vals < long_vals):
        return "bearish"
    return "mixed"


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------


def classify_regime(
    df: pd.DataFrame,
    adx_period: int = 14,
    ema_short: int = 21,
    ema_long: int = 50,
    bb_period: int = 20,
    squeeze_threshold: float = 0.7,
) -> RegimeClassification:
    """Classifica o regime de mercado a partir de um DataFrame OHLCV.

    Ordem de avaliação (primeira condição que der match vence):
        1. SQUEEZE — bb_width_ratio < squeeze_threshold
        2. TREND_UP — ADX > 25, EMAs alinhadas bullish nas últimas 3 velas,
           slope da EMA curta positivo
        3. TREND_DOWN — ADX > 25, EMAs alinhadas bearish nas últimas 3 velas,
           slope da EMA curta negativo
        4. RANGE — default

    Parâmetros:
        df: DataFrame com colunas OHLCV em lowercase. Mínimo 100 candles.
        adx_period: período do ADX (padrão 14).
        ema_short: período da EMA rápida (padrão 21).
        ema_long: período da EMA lenta (padrão 50).
        bb_period: período das Bollinger Bands (padrão 20).
        squeeze_threshold: BB width atual / mediana histórica abaixo deste
            valor é classificado como SQUEEZE (padrão 0.7).

    Retorno:
        RegimeClassification com regime, métricas intermediárias e confidence.

    Exceções:
        ValueError: se o DataFrame tiver menos de 100 candles ou faltarem
            colunas OHLCV obrigatórias.
    """
    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame faltando colunas OHLCV: {missing}")

    if len(df) < MIN_CANDLES:
        raise ValueError(
            f"DataFrame precisa ter pelo menos {MIN_CANDLES} candles (recebido: {len(df)})"
        )

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    # --- ADX ---------------------------------------------------------------
    adx_df = ta.adx(high, low, close, length=adx_period)
    adx_col = f"ADX_{adx_period}"
    if adx_df is None or adx_col not in adx_df.columns:
        adx_value = 0.0
    else:
        last_adx = adx_df[adx_col].dropna()
        adx_value = float(last_adx.iloc[-1]) if len(last_adx) else 0.0
    if not np.isfinite(adx_value):
        adx_value = 0.0

    # --- EMAs --------------------------------------------------------------
    ema_s = ta.ema(close, length=ema_short)
    ema_l = ta.ema(close, length=ema_long)

    ema_slope = get_ema_slope(ema_s, periods=5)
    ema_alignment = get_ema_alignment(ema_s, ema_l, periods=EMA_ALIGNMENT_LOOKBACK)

    # --- Bollinger Bands ---------------------------------------------------
    bb = ta.bbands(close, length=bb_period, std=2)
    if bb is None or bb.empty:
        bb_width_ratio = 1.0
    else:
        # Colunas: BBL, BBM, BBU, BBB, BBP (nessa ordem).
        bbl = bb.iloc[:, 0]
        bbm = bb.iloc[:, 1]
        bbu = bb.iloc[:, 2]
        width = (bbu - bbl) / bbm.replace(0.0, np.nan)
        width = width.dropna()
        if len(width) < BB_MEDIAN_WINDOW + 1:
            bb_width_ratio = 1.0
        else:
            current_width = float(width.iloc[-1])
            median_width = float(width.iloc[-BB_MEDIAN_WINDOW:].median())
            if median_width == 0.0 or not np.isfinite(median_width):
                bb_width_ratio = 1.0
            else:
                bb_width_ratio = current_width / median_width
    if not np.isfinite(bb_width_ratio):
        bb_width_ratio = 1.0

    # --- Classificação ------------------------------------------------------
    if bb_width_ratio < squeeze_threshold:
        regime = "SQUEEZE"
        confidence = 100.0 - (bb_width_ratio * 100.0)
    elif (
        adx_value > ADX_TREND_THRESHOLD
        and ema_alignment == "bullish"
        and ema_slope > 0.0
    ):
        regime = "TREND_UP"
        confidence = min(100.0, (adx_value - ADX_TREND_THRESHOLD) * 3.0)
    elif (
        adx_value > ADX_TREND_THRESHOLD
        and ema_alignment == "bearish"
        and ema_slope < 0.0
    ):
        regime = "TREND_DOWN"
        confidence = min(100.0, (adx_value - ADX_TREND_THRESHOLD) * 3.0)
    else:
        regime = "RANGE"
        confidence = 100.0 - min(adx_value, 100.0)

    confidence = max(0.0, min(100.0, float(confidence)))

    return RegimeClassification(
        regime=regime,
        adx_value=float(adx_value),
        ema_slope=float(ema_slope),
        ema_alignment=ema_alignment,
        bb_width_ratio=float(bb_width_ratio),
        confidence=float(confidence),
    )
