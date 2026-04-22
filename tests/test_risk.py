"""Testes do módulo risk — trade plan, stop dinâmico e saídas parciais."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from risk import (
    MAX_LEVERAGE,
    MIN_LEVERAGE,
    POSITION_SPLIT,
    TradePlan,
    TradeState,
    calculate_leverage,
    calculate_trade_plan,
    estimate_risk_reward,
    update_trade_state,
    validate_trade_plan,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _candle(o: float, h: float, l: float, c: float, v: float = 1000.0) -> dict:
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def _make_df(candles: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df.index = pd.date_range("2025-01-01", periods=len(df), freq="15min")
    return df


def _constant_atr_df(atr_target: float, n: int = 60, base_price: float = 100.0) -> pd.DataFrame:
    """DataFrame com true_range constante → Wilder's ATR converge para atr_target.

    Cada candle tem high-low = atr_target com close fixo em base_price. Como o
    True Range é constante, o EWM com alpha=1/period retorna exatamente o
    valor após a inicialização.
    """
    half = atr_target / 2.0
    candles = [
        _candle(base_price, base_price + half, base_price - half, base_price)
        for _ in range(n)
    ]
    return _make_df(candles)


def _make_plan(
    direction: str = "LONG",
    entry: float = 100.0,
    sl: float = 98.0,
    tp1: float = 101.0,
    tp2: float = 103.0,
    tp3: float = 105.0,
    leverage: float = 10.0,
    atr: float = 1.0,
) -> TradePlan:
    """Constrói um TradePlan diretamente — sem passar por calculate_trade_plan."""
    risk = abs(entry - sl)
    if direction == "LONG":
        rr1 = (tp1 - entry) / risk
        rr2 = (tp2 - entry) / risk
        rr3 = (tp3 - entry) / risk
    else:
        rr1 = (entry - tp1) / risk
        rr2 = (entry - tp2) / risk
        rr3 = (entry - tp3) / risk

    return TradePlan(
        direction=direction,
        entry_price=entry,
        sl_price=sl,
        sl_distance_pct=abs(entry - sl) / entry * 100,
        tp1_price=tp1,
        tp2_price=tp2,
        tp3_price=tp3,
        tp1_distance_pct=abs(tp1 - entry) / entry * 100,
        tp2_distance_pct=abs(tp2 - entry) / entry * 100,
        tp3_distance_pct=abs(tp3 - entry) / entry * 100,
        risk_reward_tp1=rr1,
        risk_reward_tp2=rr2,
        risk_reward_tp3=rr3,
        atr_value=atr,
        position_split=POSITION_SPLIT,
        leverage=leverage,
    )


# ---------------------------------------------------------------------------
# 1. calculate_trade_plan — LONG
# ---------------------------------------------------------------------------


def test_calculate_trade_plan_long():
    """LONG entry 100, ATR 2 → SL ~97, TP1 ~102, TP2 ~104, TP3 ~107."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=60.0,
        regime="TREND_UP",
    )
    assert plan.direction == "LONG"
    assert plan.atr_value == pytest.approx(2.0, rel=1e-3)
    assert plan.sl_price == pytest.approx(97.0, abs=0.05)
    assert plan.tp1_price == pytest.approx(102.0, abs=0.05)
    assert plan.tp2_price == pytest.approx(104.0, abs=0.05)
    assert plan.tp3_price == pytest.approx(107.0, abs=0.05)
    assert plan.position_split == POSITION_SPLIT


# ---------------------------------------------------------------------------
# 2. calculate_trade_plan — SHORT
# ---------------------------------------------------------------------------


def test_calculate_trade_plan_short():
    """SHORT entry 100, ATR 2 → SL ~103, TP1 ~98, TP2 ~96, TP3 ~93."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    plan = calculate_trade_plan(
        df=df,
        direction="SHORT",
        entry_price=100.0,
        setup_confidence=60.0,
        regime="TREND_DOWN",
    )
    assert plan.direction == "SHORT"
    assert plan.atr_value == pytest.approx(2.0, rel=1e-3)
    assert plan.sl_price == pytest.approx(103.0, abs=0.05)
    assert plan.tp1_price == pytest.approx(98.0, abs=0.05)
    assert plan.tp2_price == pytest.approx(96.0, abs=0.05)
    assert plan.tp3_price == pytest.approx(93.0, abs=0.05)


# ---------------------------------------------------------------------------
# 3. SL estrutural domina quando swing low é mais baixo que ATR-based SL
# ---------------------------------------------------------------------------


def test_sl_structural_long():
    """Quando swing low é mais baixo que ATR-based SL, estrutural domina (min).

    ATR ~2 → atr_sl = 100 - 1.5*2 = 97.
    Swing low profundo em 95 dentro da janela → struct_sl = 95 - 0.3*2 = 94.4.
    min(97, 94.4) = 94.4 → estrutural domina.
    """
    candles = []
    # 40 candles estáveis (ATR ~2)
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    # candle com swing low profundo em 95 (dentro da janela de 20)
    candles.append(_candle(100, 101, 95, 99))
    # mais 15 candles estáveis — o swing low de 95 ainda está na janela
    for _ in range(15):
        candles.append(_candle(100, 101, 99, 100))

    df = _make_df(candles)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=60.0,
        regime="TREND_UP",
        swing_lookback=20,
    )
    # SL estrutural deve dominar (ATR dá ~97, estrutural dá ~94.4)
    assert plan.sl_price < 97.0, f"SL estrutural deveria ser < 97, got {plan.sl_price}"
    assert plan.sl_price == pytest.approx(95.0 - 0.3 * plan.atr_value, abs=0.1)


# ---------------------------------------------------------------------------
# 4. Leverage alta — confidence alta, regime alinhado, ATR baixo
# ---------------------------------------------------------------------------


def test_leverage_scaling():
    """Confidence 80 + TREND_UP LONG + ATR baixo → leverage 12-15x."""
    lev = calculate_leverage(
        setup_confidence=80.0,
        regime="TREND_UP",
        atr_value=0.3,
        entry_price=100.0,
        direction="LONG",
    )
    assert 12.0 <= lev <= 15.0, f"Leverage alta esperada (12-15), got {lev}"


# ---------------------------------------------------------------------------
# 5. Leverage baixa — confidence baixa + regime contrário
# ---------------------------------------------------------------------------


def test_leverage_low_confidence():
    """Confidence 30 + regime contrário + ATR alto → leverage baixa (<= 7x).

    Nota: com a ponderação 40/30/30 e um floor de 3x, confidence 30 com tudo
    mínimo produz ~6x. Consideramos "baixa" qualquer valor ≤ 7.
    """
    lev = calculate_leverage(
        setup_confidence=30.0,
        regime="TREND_UP",
        atr_value=3.5,
        entry_price=100.0,
        direction="SHORT",
    )
    assert MIN_LEVERAGE <= lev <= 7.0, f"Leverage baixa esperada (<=7), got {lev}"


# ---------------------------------------------------------------------------
# 6. TradeState — TP1 hit move SL para entry
# ---------------------------------------------------------------------------


def test_trade_state_tp1_hit():
    """Candle com high >= tp1 → tp1_hit=True, SL = entry."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=101, tp2=103, tp3=105)
    state = TradeState(trade_plan=plan, current_sl=plan.sl_price)

    candle = _candle(100, 101.5, 99.5, 101.2)
    new_state = update_trade_state(state, candle)

    assert new_state.tp1_hit is True
    assert new_state.tp2_hit is False
    assert new_state.tp3_hit is False
    assert new_state.current_sl == pytest.approx(100.0)
    assert new_state.position_remaining == pytest.approx(0.5)
    assert new_state.status == "OPEN"


# ---------------------------------------------------------------------------
# 7. TradeState — TP2 hit após TP1 move SL para TP1
# ---------------------------------------------------------------------------


def test_trade_state_tp2_hit_after_tp1():
    """Depois de TP1, candle com high >= tp2 → tp2_hit=True, SL = tp1."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=101, tp2=103, tp3=105)
    state = TradeState(trade_plan=plan, current_sl=plan.sl_price)

    # Candle 1: atinge TP1
    state = update_trade_state(state, _candle(100, 101.5, 99.5, 101.2))
    assert state.tp1_hit is True
    assert state.current_sl == pytest.approx(100.0)

    # Candle 2: atinge TP2
    state = update_trade_state(state, _candle(101.2, 103.5, 101.0, 103.2))
    assert state.tp2_hit is True
    assert state.current_sl == pytest.approx(101.0)
    assert state.position_remaining == pytest.approx(0.20)
    assert state.status == "OPEN"


# ---------------------------------------------------------------------------
# 8. TradeState — SL batido após TP1 → WIN parcial
# ---------------------------------------------------------------------------


def test_trade_state_sl_after_tp1():
    """Após TP1, preço volta e bate SL (= entry) → WIN_TP1, não LOSS_SL."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=101, tp2=103, tp3=105)
    state = TradeState(trade_plan=plan, current_sl=plan.sl_price)

    # Candle 1: atinge TP1 → SL move para entry (100)
    state = update_trade_state(state, _candle(100, 101.5, 99.5, 101.2))
    assert state.tp1_hit is True

    # Candle 2: preço volta, low toca SL=100
    state = update_trade_state(state, _candle(101, 101.2, 99.5, 99.8))
    assert state.status == "WIN_TP1"
    assert state.position_remaining == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 9. TradeState — SL batido antes de qualquer TP → LOSS_SL
# ---------------------------------------------------------------------------


def test_trade_state_full_sl():
    """Preço bate SL antes de qualquer TP → LOSS_SL."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=101, tp2=103, tp3=105)
    state = TradeState(trade_plan=plan, current_sl=plan.sl_price)

    candle = _candle(100, 100.5, 97.5, 97.8)
    new_state = update_trade_state(state, candle)

    assert new_state.status == "LOSS_SL"
    assert new_state.tp1_hit is False
    assert new_state.position_remaining == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 10. Risk/Reward — cálculo matemático
# ---------------------------------------------------------------------------


def test_risk_reward_calculation():
    """r1 = (tp1-entry)/|entry-sl|, etc."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=102, tp2=104, tp3=107)
    r1, r2, r3 = estimate_risk_reward(plan)
    # risk = 2, ganhos = 2, 4, 7
    assert r1 == pytest.approx(1.0)
    assert r2 == pytest.approx(2.0)
    assert r3 == pytest.approx(3.5)

    short_plan = _make_plan(direction="SHORT", entry=100, sl=102, tp1=98, tp2=96, tp3=93)
    sr1, sr2, sr3 = estimate_risk_reward(short_plan)
    assert sr1 == pytest.approx(1.0)
    assert sr2 == pytest.approx(2.0)
    assert sr3 == pytest.approx(3.5)


# ---------------------------------------------------------------------------
# 11. Validate — R:R insuficiente
# ---------------------------------------------------------------------------


def test_validate_trade_plan_bad_rr():
    """R:R TP1 < 0.8 → invalid."""
    # risk = 10, tp1 = entry + 5 → r1 = 0.5
    plan = _make_plan(
        direction="LONG",
        entry=100,
        sl=90,
        tp1=105,
        tp2=110,
        tp3=115,
        leverage=10.0,
    )
    is_valid, reason = validate_trade_plan(plan, min_rr_tp1=0.8)
    assert is_valid is False
    assert "R:R" in reason or "R/R" in reason or "rr" in reason.lower()


# ---------------------------------------------------------------------------
# 12. Validate — SL > 10% distance
# ---------------------------------------------------------------------------


def test_validate_trade_plan_huge_sl():
    """SL distance > 10% → invalid."""
    # entry=100, sl=85 → 15%
    plan = _make_plan(
        direction="LONG",
        entry=100,
        sl=85,
        tp1=120,
        tp2=140,
        tp3=160,
        leverage=10.0,
    )
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is False
    assert "SL" in reason or "distance" in reason.lower()


# ---------------------------------------------------------------------------
# Extras de sanidade (não obrigatórios, mas baratos)
# ---------------------------------------------------------------------------


def test_validate_trade_plan_happy_path():
    """Plano válido passa."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=102, tp2=104, tp3=107)
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is True
    assert reason == ""


def test_calculate_trade_plan_rejects_invalid_direction():
    df = _constant_atr_df(atr_target=2.0)
    with pytest.raises(ValueError):
        calculate_trade_plan(
            df=df,
            direction="FLAT",
            entry_price=100.0,
            setup_confidence=50.0,
            regime="RANGE",
        )


def test_update_trade_state_closed_is_noop():
    """Estado terminal não muda mais."""
    plan = _make_plan()
    closed = TradeState(
        trade_plan=plan, current_sl=plan.sl_price, status="LOSS_SL",
        position_remaining=0.0,
    )
    result = update_trade_state(closed, _candle(100, 200, 50, 150))
    assert result.status == "LOSS_SL"
    assert result is closed  # retorno direto, sem reconstrução
