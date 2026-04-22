"""Testes do módulo smc_lib — detecção de estruturas SMC."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from smc_lib import (
    BreakerBlock,
    FairValueGap,
    OrderBlock,
    _atr,
    detect_breaker_blocks,
    detect_fvg,
    detect_market_structure,
    detect_order_blocks,
    detect_swing_points,
)


# ---------------------------------------------------------------------------
# Helpers de construção de fixtures sintéticas
# ---------------------------------------------------------------------------


def _make_df(candles: list[dict]) -> pd.DataFrame:
    """Constrói DataFrame OHLCV a partir de lista de dicts."""
    df = pd.DataFrame(candles)
    df.index = pd.date_range("2025-01-01", periods=len(df), freq="15min")
    return df


def _candle(
    o: float,
    h: float,
    l: float,
    c: float,
    v: float = 1000.0,
) -> dict:
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


# ---------------------------------------------------------------------------
# 1. Swing detection
# ---------------------------------------------------------------------------


def test_swing_detection_simple():
    """10 candles subindo + 10 descendo — swing high detectado no topo."""
    candles = []
    for i in range(10):
        price = 100 + i
        candles.append(_candle(price, price + 0.5, price - 0.5, price + 0.3))
    # topo em idx 9 — high 109.5, único na janela
    for i in range(10):
        price = 108 - i
        candles.append(_candle(price, price + 0.4, price - 0.5, price - 0.3))

    df = _make_df(candles)
    result = detect_swing_points(df, left=3, right=3)

    swing_highs = result["swing_high"]
    assert swing_highs.iloc[9], "Topo em idx 9 deve ser swing high"
    # Nenhum outro swing high no meio da subida/descida nítida
    assert not swing_highs.iloc[0:9].any()


def test_swing_detection_returns_series_indexed_as_df():
    candles = [_candle(100, 101, 99, 100) for _ in range(20)]
    df = _make_df(candles)
    result = detect_swing_points(df)
    assert list(result["swing_high"].index) == list(df.index)
    assert list(result["swing_low"].index) == list(df.index)


# ---------------------------------------------------------------------------
# 2. Market structure — BoS / CHoCH
# ---------------------------------------------------------------------------


def test_bos_up_detection():
    """Swing high, pullback, quebra do swing high → bos_up=True na quebra."""
    candles = []
    # subida inicial, topo em idx 5 (high=110)
    for i in range(5):
        p = 100 + i
        candles.append(_candle(p, p + 0.5, p - 0.5, p + 0.4))
    candles.append(_candle(104.5, 110.0, 104.0, 109.0))  # idx 5 — swing high
    # pullback largo o suficiente para confirmar swing (swing_length=2 → 2 barras)
    for i in range(5):
        p = 108 - i
        candles.append(_candle(p, p + 0.3, p - 0.5, p - 0.3))
    # idx 11+ impulso que quebra o swing high (close > 110)
    candles.append(_candle(104, 108, 103, 107.5))  # idx 11
    candles.append(_candle(107.5, 112, 107, 111.5))  # idx 12 — quebra!

    df = _make_df(candles)
    result = detect_market_structure(df, swing_length=2)

    assert result["bos_up"].iloc[12], "Quebra do swing high deve emitir bos_up"
    assert result["last_structure"].iloc[12] == "bullish"


def test_choch_down_detection():
    """Tendência de alta + quebra do último swing low → choch_down=True."""
    candles = []
    # subida criando estrutura bullish
    for i in range(5):
        p = 100 + i
        candles.append(_candle(p, p + 0.5, p - 0.5, p + 0.4))
    # idx 5 topo 110
    candles.append(_candle(104.5, 110.0, 104.0, 109.0))
    # pullback forma swing low em idx 8
    candles.append(_candle(108.5, 109, 106, 107))  # 6
    candles.append(_candle(107, 107.5, 104, 104.5))  # 7
    candles.append(_candle(104.5, 105, 103, 103.5))  # 8 — swing low candidato (low=103)
    candles.append(_candle(103.5, 106, 103.8, 105.5))  # 9 — low 103.8 > 103
    candles.append(_candle(105.5, 108, 105, 107.5))  # 10
    # quebra acima de 110 (BoS up, confirma bullish)
    candles.append(_candle(107.5, 112, 107, 111.5))  # 11 — BoS up
    # pullback e quebra abaixo do swing low 103 → CHoCH down
    candles.append(_candle(111.5, 112, 108, 109))  # 12
    candles.append(_candle(109, 110, 105, 105.5))  # 13
    candles.append(_candle(105.5, 106, 100, 100.5))  # 14 — quebra!

    df = _make_df(candles)
    result = detect_market_structure(df, swing_length=2)

    assert result["choch_down"].iloc[14], (
        f"Esperado choch_down em idx 14, got "
        f"bos_up={result['bos_up'].iloc[14]}, "
        f"bos_down={result['bos_down'].iloc[14]}, "
        f"choch_up={result['choch_up'].iloc[14]}, "
        f"choch_down={result['choch_down'].iloc[14]}, "
        f"last_structure={result['last_structure'].iloc[14]}"
    )
    assert result["last_structure"].iloc[14] == "bearish"


def test_market_structure_returns_same_index():
    candles = [_candle(100 + i * 0.1, 101 + i * 0.1, 99 + i * 0.1, 100 + i * 0.1)
               for i in range(30)]
    df = _make_df(candles)
    result = detect_market_structure(df, swing_length=3)
    assert list(result.index) == list(df.index)
    assert set(result.columns) == {
        "bos_up", "bos_down", "choch_up", "choch_down", "last_structure",
    }


# ---------------------------------------------------------------------------
# 3. Order Blocks
# ---------------------------------------------------------------------------


def test_order_block_bullish():
    """Candle bearish seguido de impulso bullish que quebra estrutura → OB bull."""
    candles = []
    # subida inicial leve, topo em idx 3 high=110
    for i in range(3):
        p = 100 + i * 0.5
        candles.append(_candle(p, p + 0.2, p - 0.3, p + 0.1))
    candles.append(_candle(101.5, 110.0, 101.0, 108.0))  # idx 3 topo 110
    # confirmar swing: precisamos de swing_length=2 barras depois
    candles.append(_candle(108, 108.5, 105, 105.5))  # 4
    candles.append(_candle(105.5, 106, 103, 103.5))  # 5 — base do OB (low mínimo)
    # impulso bullish 2 velas seguintes
    candles.append(_candle(103.5, 108, 103.5, 107.8, v=3000))  # 6
    candles.append(_candle(107.8, 113, 107.5, 112.5, v=4000))  # 7 — quebra 110
    # mais candles para ATR e histórico
    for _ in range(5):
        candles.append(_candle(112.5, 113, 112, 112.5))

    df = _make_df(candles)
    obs = detect_order_blocks(df, swing_length=2, atr_period=5, min_strength_atr=0.1)

    bull_obs = [o for o in obs if o.direction == "bullish"]
    assert len(bull_obs) >= 1, f"Esperado ao menos 1 OB bullish, got {obs}"
    ob = bull_obs[0]
    assert ob.bottom == pytest.approx(103.0), f"bottom esperado 103, got {ob.bottom}"
    assert ob.top == pytest.approx(max(105.5, 103.5)), f"top esperado 105.5, got {ob.top}"
    assert ob.strength_atr >= 0.1
    assert ob.total_volume > 0
    assert 0.0 <= ob.bull_volume_ratio <= 1.0


def test_order_block_mitigation():
    """OB bullish + close posterior < bottom → mitigated=True."""
    candles = []
    for i in range(3):
        p = 100 + i * 0.5
        candles.append(_candle(p, p + 0.2, p - 0.3, p + 0.1))
    candles.append(_candle(101.5, 110.0, 101.0, 108.0))  # idx 3 topo
    candles.append(_candle(108, 108.5, 105, 105.5))  # 4
    candles.append(_candle(105.5, 106, 103, 103.5))  # 5 — base OB bottom=103
    candles.append(_candle(103.5, 108, 103.5, 107.8, v=3000))  # 6
    candles.append(_candle(107.8, 113, 107.5, 112.5, v=4000))  # 7 — quebra
    # mitigação: close abaixo de 103
    candles.append(_candle(112.5, 112.5, 108, 109))  # 8
    candles.append(_candle(109, 110, 105, 105.5))  # 9
    candles.append(_candle(105.5, 106, 100, 100.5))  # 10 — close 100.5 < 103
    candles.append(_candle(100.5, 101, 98, 99))  # 11

    df = _make_df(candles)
    obs = detect_order_blocks(df, swing_length=2, atr_period=5, min_strength_atr=0.1)
    bull = next(o for o in obs if o.direction == "bullish")

    assert bull.mitigated is True
    assert bull.mitigation_index == 10
    assert bull.mitigation_index > bull.index_end


# ---------------------------------------------------------------------------
# 4. Breaker Blocks
# ---------------------------------------------------------------------------


def test_breaker_from_mitigated_ob():
    """OB bullish mitigado → breaker com new_direction=bearish e break_volume correto."""
    candles = []
    for i in range(3):
        p = 100 + i * 0.5
        candles.append(_candle(p, p + 0.2, p - 0.3, p + 0.1))
    candles.append(_candle(101.5, 110.0, 101.0, 108.0))  # idx 3 topo
    candles.append(_candle(108, 108.5, 105, 105.5))  # 4
    candles.append(_candle(105.5, 106, 103, 103.5))  # 5 — OB base
    candles.append(_candle(103.5, 108, 103.5, 107.8, v=3000))  # 6
    candles.append(_candle(107.8, 113, 107.5, 112.5, v=4000))  # 7 — quebra
    candles.append(_candle(112.5, 112.5, 108, 109))  # 8
    candles.append(_candle(109, 110, 105, 105.5))  # 9
    candles.append(_candle(105.5, 106, 100, 100.5, v=5500))  # 10 — mitigação
    candles.append(_candle(100.5, 101, 98, 99))  # 11

    df = _make_df(candles)
    obs = detect_order_blocks(df, swing_length=2, atr_period=5, min_strength_atr=0.1)
    breakers = detect_breaker_blocks(df, obs)

    bullish_source = [b for b in breakers if b.original_direction == "bullish"]
    assert len(bullish_source) >= 1
    br = bullish_source[0]
    assert br.new_direction == "bearish"
    assert br.break_volume == pytest.approx(5500.0)
    assert br.retest_count >= 0


def test_breaker_skipped_for_unmitigated_ob():
    """OB não mitigado não gera breaker."""
    candles = []
    for i in range(3):
        p = 100 + i * 0.5
        candles.append(_candle(p, p + 0.2, p - 0.3, p + 0.1))
    candles.append(_candle(101.5, 110.0, 101.0, 108.0))
    candles.append(_candle(108, 108.5, 105, 105.5))
    candles.append(_candle(105.5, 106, 103, 103.5))
    candles.append(_candle(103.5, 108, 103.5, 107.8, v=3000))
    candles.append(_candle(107.8, 113, 107.5, 112.5, v=4000))
    # nenhuma mitigação — só candles que seguem subindo
    for _ in range(5):
        candles.append(_candle(112.5, 114, 112, 113.5))

    df = _make_df(candles)
    obs = detect_order_blocks(df, swing_length=2, atr_period=5, min_strength_atr=0.1)
    breakers = detect_breaker_blocks(df, obs)

    assert all(b.original_direction != "bullish" for b in breakers)


# ---------------------------------------------------------------------------
# 5. Fair Value Gaps
# ---------------------------------------------------------------------------


def test_fvg_bullish():
    """3 candles onde low[i+1] > high[i-1] → FVG bullish."""
    candles = []
    # historico para ATR
    for i in range(15):
        p = 100 + i * 0.05
        candles.append(_candle(p, p + 0.3, p - 0.3, p))
    # gap bullish nos 3 candles finais:
    # idx n-3: high=101.5
    # idx n-2: candle de impulso
    # idx n-1: low=103 > high[n-3]=101.5
    candles.append(_candle(100.8, 101.5, 100.5, 101.2))  # i-1
    candles.append(_candle(101.3, 103.5, 101.2, 103.2))  # i — impulso
    candles.append(_candle(103.2, 104, 103.0, 103.8))  # i+1: low 103 > high 101.5

    df = _make_df(candles)
    fvgs = detect_fvg(df, min_gap_atr=0.01)

    bull_fvgs = [f for f in fvgs if f.direction == "bullish"]
    assert len(bull_fvgs) >= 1
    fvg = bull_fvgs[-1]
    assert fvg.top == pytest.approx(103.0)
    assert fvg.bottom == pytest.approx(101.5)
    assert fvg.size_atr >= 0.01
    assert fvg.filled is False


def test_fvg_fill():
    """Candle posterior entra na zona → filled=True."""
    candles = []
    for i in range(15):
        p = 100 + i * 0.05
        candles.append(_candle(p, p + 0.3, p - 0.3, p))
    candles.append(_candle(100.8, 101.5, 100.5, 101.2))
    candles.append(_candle(101.3, 103.5, 101.2, 103.2))
    candles.append(_candle(103.2, 104, 103.0, 103.8))
    # candle que retorna e atravessa bottom do FVG (101.5)
    candles.append(_candle(103.5, 103.8, 102, 102.2))  # não preenche ainda
    candles.append(_candle(102.2, 102.5, 101.0, 101.2))  # low 101 <= 101.5

    df = _make_df(candles)
    fvgs = detect_fvg(df, min_gap_atr=0.01)

    bull_fvgs = [f for f in fvgs if f.direction == "bullish"]
    assert len(bull_fvgs) >= 1
    fvg = bull_fvgs[-1]
    assert fvg.filled is True
    assert fvg.fill_index is not None
    assert fvg.fill_index > fvg.index


def test_fvg_below_min_size_discarded():
    """FVG com size_atr < min_gap_atr é descartado."""
    candles = [_candle(100, 100.1, 99.9, 100) for _ in range(20)]
    df = _make_df(candles)
    fvgs = detect_fvg(df, min_gap_atr=5.0)  # threshold absurdo
    assert fvgs == []


# ---------------------------------------------------------------------------
# ATR interno
# ---------------------------------------------------------------------------


def test_atr_returns_series_same_length():
    candles = [_candle(100, 101, 99, 100) for _ in range(30)]
    df = _make_df(candles)
    atr = _atr(df, period=14)
    assert len(atr) == len(df)
    assert isinstance(atr, pd.Series)


def test_atr_positive_values():
    candles = [_candle(100 + i * 0.1, 101 + i * 0.1, 99 + i * 0.1, 100 + i * 0.1)
               for i in range(30)]
    df = _make_df(candles)
    atr = _atr(df, period=14)
    # após o primeiro NaN, valores devem ser positivos
    assert (atr.iloc[1:] > 0).all()


# ---------------------------------------------------------------------------
# Validação de input
# ---------------------------------------------------------------------------


def test_missing_column_raises():
    df = pd.DataFrame({"open": [1, 2], "high": [1, 2], "low": [1, 2], "close": [1, 2]})
    with pytest.raises(ValueError):
        detect_order_blocks(df)


def test_empty_df_returns_empty():
    df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    assert detect_order_blocks(df) == []
    assert detect_fvg(df) == []


def test_dataclass_types():
    """Sanity: dataclasses exportam e são instanciáveis."""
    ob = OrderBlock("bullish", 0, 1, 1.0, 0.5, 100.0, 0.5, 1.0, False, None)
    br = BreakerBlock("bullish", "bearish", 0, 1, 1.0, 0.5, 100.0, 0, False, None)
    fvg = FairValueGap("bullish", 1, 1.0, 0.5, 0.2, False, None)
    assert ob.direction == "bullish"
    assert br.new_direction == "bearish"
    assert fvg.direction == "bullish"
