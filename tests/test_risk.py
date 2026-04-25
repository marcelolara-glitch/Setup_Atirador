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
    """LONG entry 100, ATR 2, sem SMC primitives → SL via swing_low - buffer.

    Sem OBs/FVGs/breakers, o único candidato estrutural é o swing_low (=99
    no DF constante). sl_estrutural = 99 - 0.3*2 = 98.4. O guard-rail
    sl_min = 100 - 0.8*2 = 98.4 resulta em sl_price=98.4, sl_distance=1.6.
    TP1/TP2/TP3 escalam sobre sl_distance (R:R 1.0/2.0/3.5).
    """
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
    assert plan.sl_price == pytest.approx(98.4, abs=0.05)
    # TP1 fallback R:R 1.0 × 1.6 = 1.6
    assert plan.tp1_price == pytest.approx(101.6, abs=0.05)
    # TP2 R:R 2.0 × 1.6 = 3.2
    assert plan.tp2_price == pytest.approx(103.2, abs=0.05)
    # TP3 R:R 3.5 × 1.6 = 5.6
    assert plan.tp3_price == pytest.approx(105.6, abs=0.05)
    assert plan.position_split == POSITION_SPLIT


# ---------------------------------------------------------------------------
# 2. calculate_trade_plan — SHORT
# ---------------------------------------------------------------------------


def test_calculate_trade_plan_short():
    """SHORT entry 100, ATR 2, sem SMC primitives → SL via swing_high + buffer.

    swing_high=101; sl_estrutural = 101 + 0.3*2 = 101.6. sl_min = 100 + 1.6
    = 101.6. SL=101.6, sl_distance=1.6. TPs espelhados.
    """
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
    assert plan.sl_price == pytest.approx(101.6, abs=0.05)
    assert plan.tp1_price == pytest.approx(98.4, abs=0.05)
    assert plan.tp2_price == pytest.approx(96.8, abs=0.05)
    assert plan.tp3_price == pytest.approx(94.4, abs=0.05)


# ---------------------------------------------------------------------------
# 3. SL estrutural domina quando swing low é mais baixo que ATR-based SL
# ---------------------------------------------------------------------------


def test_sl_structural_long():
    """Swing low moderado domina — sem catástrofe clamp.

    Swing low em 97.5 (a 2.5% de entry=100, dentro do cap de 5%).
    sl_estrutural = 97.5 - 0.3*ATR. Esperado: SL um pouco abaixo de 97.5.
    """
    candles = []
    # 40 candles estáveis (ATR ~2)
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    # candle com swing low moderado em 97.5 (~2.5%, dentro do sl_max)
    candles.append(_candle(100, 101, 97.5, 99))
    # mais 15 candles estáveis — o swing low ainda está na janela de 20
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
    # SL estrutural (swing - buffer) domina, pois está além do sl_min e dentro
    # do sl_max. ATR ~ 2 (com leve bump), então expected ~97.5 - 0.3*2 ~ 96.9.
    expected = 97.5 - 0.3 * plan.atr_value
    assert plan.sl_price == pytest.approx(expected, abs=0.15)
    assert plan.sl_price < 97.5


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
    """R:R TP1 < min_rr_tp1 → invalid."""
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
    # Default v9.1: DEFAULT_MIN_RR_TP1 = 1.0 — R:R 0.5 é inválido
    is_valid, reason = validate_trade_plan(plan)
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


# ---------------------------------------------------------------------------
# SMC structural — testes novos v9.1
# ---------------------------------------------------------------------------


from dataclasses import dataclass as _dc  # noqa: E402 — helpers locais


@_dc
class _FakeOB:
    direction: str
    top: float
    bottom: float
    mitigated: bool = False


@_dc
class _FakeBreaker:
    new_direction: str
    top: float
    bottom: float
    invalidated: bool = False


@_dc
class _FakeFVG:
    direction: str
    top: float
    bottom: float
    filled: bool = False


def test_calculate_trade_plan_estrutural_long_with_obs():
    """SL estrutural usa bottom do bullish OB quando mais distante que swing."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # OB bullish com bottom 97 (abaixo de swing_low=99, ainda dentro do sl_max=95)
    ob_bullish = _FakeOB(direction="bullish", top=99.5, bottom=97.0, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_bullish],
    )
    # sl_candidates = [swing_low(=99)-0.6, ob.bottom(=97)-0.6] = [98.4, 96.4]
    # sl_estrutural = 96.4. sl_min = 98.4 (mais perto do entry). min(96.4, 98.4) = 96.4.
    # sl_max = 95.0. max(95, 96.4) = 96.4.
    assert plan.sl_price == pytest.approx(96.4, abs=0.05)
    assert plan.sl_price < 98.4  # mais distante que o fallback de swing


def test_calculate_trade_plan_tp1_estrutural_with_fvg():
    """TP1 usa bullish FVG top quando R:R é viável."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # FVG bullish com top 102 — R:R com SL ~ 98.4 é (102-100)/(100-98.4) = 1.25 >= 1.0
    fvg = _FakeFVG(direction="bullish", top=102.0, bottom=101.5, filled=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        fvgs=[fvg],
    )
    # sl_distance = 100 - 98.4 = 1.6; rr_estrutural = 2.0 / 1.6 = 1.25 >= 1.0
    assert plan.tp1_price == pytest.approx(102.0, abs=0.01)


def test_calculate_trade_plan_tp1_fallback_when_no_structure():
    """Sem SMC primitives, TP1 cai no fallback R:R 1:1 sobre sl_distance."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
    )
    sl_distance = 100.0 - plan.sl_price
    expected_tp1 = 100.0 + 1.0 * sl_distance
    assert plan.tp1_price == pytest.approx(expected_tp1, abs=0.01)


def test_calculate_trade_plan_tp2_tp3_scaled_by_sl():
    """TP2 e TP3 escalam sobre sl_distance (R:R 2.0 e 3.5)."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
    )
    sl_distance = 100.0 - plan.sl_price
    expected_tp2 = 100.0 + 2.0 * sl_distance
    expected_tp3 = 100.0 + 3.5 * sl_distance
    assert plan.tp2_price == pytest.approx(expected_tp2, abs=0.01)
    assert plan.tp3_price == pytest.approx(expected_tp3, abs=0.01)


def test_calculate_trade_plan_sl_clamped_min():
    """SL não pode estar mais perto do entry que sl_min_atr_mult × ATR.

    Swing low muito próximo de entry (99.8) → sl_estrutural = 99.2; porém
    sl_min = entry - 0.8*ATR = 98.4. clamp puxa SL para 98.4.
    """
    candles = []
    # 40 candles com low=99
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    # 20 candles com low=99.8 (swing_low raso, < sl_min_atr_mult * ATR)
    for _ in range(20):
        candles.append(_candle(100, 101, 99.8, 100))

    df = _make_df(candles)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        swing_lookback=20,
    )
    # sl_estrutural = 99.8 - 0.3*ATR ≈ 99.2. sl_min = 100 - 0.8*ATR ≈ 98.4.
    # min(99.2, 98.4) = 98.4. max(sl_max=95, 98.4) = 98.4.
    assert plan.sl_price == pytest.approx(100.0 - 0.8 * plan.atr_value, abs=0.1)


def test_calculate_trade_plan_sl_clamped_max():
    """SL não pode estar mais longe que sl_max_pct."""
    candles = []
    # 40 candles estáveis
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    # candle com swing low a 90 (10% abaixo — bem além do sl_max_pct=5%)
    candles.append(_candle(100, 101, 90.0, 99))
    # mais 15 candles estáveis
    for _ in range(15):
        candles.append(_candle(100, 101, 99, 100))

    df = _make_df(candles)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        swing_lookback=20,
    )
    # sl_estrutural = 90 - buffer (bem fundo). sl_max = 100 * 0.95 = 95.
    # max(95, sl_estrutural) → clamp ao sl_max.
    assert plan.sl_price == pytest.approx(95.0, abs=0.05)


def test_calculate_trade_plan_estrutural_short_with_obs():
    """Espelho SHORT — bearish OB top define SL acima do swing_high."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # OB bearish com top 103 (acima de swing_high=101, ainda dentro do sl_max=105)
    ob_bearish = _FakeOB(direction="bearish", top=103.0, bottom=102.5, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="SHORT",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_bearish],
    )
    # sl_candidates = [swing_high(=101)+0.6, ob.top(=103)+0.6] = [101.6, 103.6]
    # sl_estrutural = max = 103.6. sl_min = 101.6 (mais perto). max(103.6, 101.6) = 103.6.
    # sl_max = 105.0. min(105, 103.6) = 103.6.
    assert plan.sl_price == pytest.approx(103.6, abs=0.05)


# ---------------------------------------------------------------------------
# tp1_source / sl_source — observabilidade v9.1.1
# ---------------------------------------------------------------------------


def test_tp1_source_structural_ob():
    """TP1 ancorado em bearish OB com R:R viável → tp1_source='structural_ob'."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # Bearish OB com bottom=102 (acima de entry=100) → R:R = (102-100)/1.6 = 1.25 ≥ 1.0
    ob_bearish = _FakeOB(direction="bearish", top=102.5, bottom=102.0, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_bearish],
    )
    assert plan.tp1_price == pytest.approx(102.0, abs=0.05)
    assert plan.tp1_source == "structural_ob"


def test_tp1_source_fallback_1r():
    """Sem candidatos estruturais, TP1 cai no fallback 1R e tp1_source='fallback_1r'."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
    )
    sl_distance = 100.0 - plan.sl_price
    expected_tp1 = 100.0 + 1.0 * sl_distance
    assert plan.tp1_price == pytest.approx(expected_tp1, abs=0.01)
    assert plan.tp1_source == "fallback_1r"


def test_sl_source_clamp_pct_max():
    """Swing estrutural distante demais → SL clampado em entry × (1 − sl_max_pct)."""
    candles = []
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    # Candle catastrófico: low=90 (10% abaixo, bem além de sl_max_pct=5%)
    candles.append(_candle(100, 101, 90.0, 99))
    for _ in range(15):
        candles.append(_candle(100, 101, 99, 100))

    df = _make_df(candles)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        swing_lookback=20,
    )
    # sl_max = 100 × 0.95 = 95.0
    assert plan.sl_price == pytest.approx(95.0, abs=0.05)
    assert plan.sl_source == "clamp_pct_max"


def test_sl_source_clamp_atr_min():
    """Swing estrutural próximo demais → SL clampado em entry − 0.8×ATR."""
    candles = []
    # 40 candles base (swing_low=99)
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    # 20 candles com swing_low=99.8 (raso → sl_estrutural mais perto que sl_min)
    for _ in range(20):
        candles.append(_candle(100, 101, 99.8, 100))

    df = _make_df(candles)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        swing_lookback=20,
    )
    # sl_min = entry − 0.8 × ATR
    expected_sl = 100.0 - 0.8 * plan.atr_value
    assert plan.sl_price == pytest.approx(expected_sl, abs=0.1)
    assert plan.sl_source == "clamp_atr_min"


# ---------------------------------------------------------------------------
# v9.1.2 — fixes de borda em validate_trade_plan
# ---------------------------------------------------------------------------


def test_validate_trade_plan_rr_exact_min():
    """R:R exatamente igual a MIN_RR_TP1 (1.0) deve passar."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=102, tp2=104, tp3=107)
    # rr1 = (102-100)/2 = 1.0 exato
    assert plan.risk_reward_tp1 == pytest.approx(1.0)
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is True, f"R:R = 1.0 (== min) deveria passar; reason={reason!r}"


def test_validate_trade_plan_rr_just_below_epsilon():
    """R:R em MIN_RR_TP1 − 5e-10 (dentro da tolerância 1e-9) passa."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=102, tp2=104, tp3=107)
    # Força rr_tp1 levemente abaixo de 1.0, dentro do épsilon (1e-9)
    plan.risk_reward_tp1 = 1.0 - 5e-10
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is True, f"R:R no épsilon deveria passar; reason={reason!r}"


def test_validate_trade_plan_rr_below_min_real():
    """R:R claramente abaixo do mínimo (0.95) deve falhar com motivo R:R."""
    plan = _make_plan(direction="LONG", entry=100, sl=98, tp1=101.9, tp2=104, tp3=107)
    # rr1 = 1.9/2 = 0.95
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is False
    assert "R:R" in reason


def test_calculate_trade_plan_avoids_incoherent_prices_long():
    """OB estrutural com R:R muito alto (>= tp2_target_rr) cai em fallback,
    preservando tp1 < tp2 < tp3 — protege contra 'Preços incoerentes'."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # Bearish OB com bottom 105 → R:R = (105-100)/1.6 = 3.125 (>= tp2_target_rr=2.0)
    ob_far = _FakeOB(direction="bearish", top=106.0, bottom=105.0, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_far],
    )
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is True, f"Plano deveria ser válido; reason={reason!r}"
    assert plan.tp1_price < plan.tp2_price < plan.tp3_price
    # Fallback foi acionado
    assert plan.tp1_source == "fallback_1r"


def test_calculate_trade_plan_avoids_incoherent_prices_short():
    """Espelho SHORT — bullish OB com R:R alto cai em fallback."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # Bullish OB com top 95 → R:R = (100-95)/1.6 = 3.125 (>= tp2_target_rr)
    ob_far = _FakeOB(direction="bullish", top=95.0, bottom=94.0, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="SHORT",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_far],
    )
    is_valid, reason = validate_trade_plan(plan)
    assert is_valid is True, f"Plano deveria ser válido; reason={reason!r}"
    assert plan.tp1_price > plan.tp2_price > plan.tp3_price
    assert plan.tp1_source == "fallback_1r"


# ---------------------------------------------------------------------------
# v9.1.2 — instrumentação tp1_diagnostics / sl_diagnostics
# ---------------------------------------------------------------------------


def test_tp1_diagnostics_no_candidates_input():
    """obs/brks/fvgs vazios → fallback_reason = no_candidates_input."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
    )
    assert plan.tp1_diagnostics["candidates_total"] == 0
    assert plan.tp1_diagnostics["fallback_reason"] == "no_candidates_input"
    assert plan.tp1_source == "fallback_1r"


def test_tp1_diagnostics_no_candidates_after_direction():
    """OBs todos do lado errado → fallback_reason = no_candidates_after_direction."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # Bearish OB com bottom < entry (lado errado para TP1 LONG)
    ob_wrong = _FakeOB(direction="bearish", top=99.0, bottom=98.5, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_wrong],
    )
    assert plan.tp1_diagnostics["candidates_total"] == 1
    assert plan.tp1_diagnostics["candidates_after_direction"] == 0
    assert plan.tp1_diagnostics["fallback_reason"] == "no_candidates_after_direction"


def test_tp1_diagnostics_best_rr_below_min():
    """OB do lado certo mas R:R 0.5 → fallback_reason = best_rr_below_min."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # sl_distance ~ 1.6; OB.bottom = 100.8 → R:R = 0.8/1.6 = 0.5
    ob_close = _FakeOB(direction="bearish", top=100.9, bottom=100.8, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_close],
    )
    assert plan.tp1_diagnostics["candidates_after_mitigated"] == 1
    assert plan.tp1_diagnostics["fallback_reason"] == "best_rr_below_min"
    assert plan.tp1_diagnostics["best_candidate_rr"] == pytest.approx(0.5, abs=0.05)
    assert plan.tp1_diagnostics["best_candidate_source"] == "structural_ob"


def test_tp1_diagnostics_structural_winner():
    """OB do lado certo com R:R 1.25 → tp1_source=structural_ob, fallback_reason=None."""
    df = _constant_atr_df(atr_target=2.0, n=60, base_price=100.0)
    # OB.bottom = 102 → R:R = 2/1.6 = 1.25
    ob_ok = _FakeOB(direction="bearish", top=102.5, bottom=102.0, mitigated=False)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        order_blocks=[ob_ok],
    )
    assert plan.tp1_source == "structural_ob"
    assert plan.tp1_diagnostics["fallback_reason"] is None
    assert plan.tp1_diagnostics["candidates_after_rr"] >= 1


def test_sl_diagnostics_clamp_pct_max_recorded():
    """Swing distante força clamp em pct_max — sl_diagnostics registra estado."""
    candles = []
    for _ in range(40):
        candles.append(_candle(100, 101, 99, 100))
    candles.append(_candle(100, 101, 90.0, 99))  # low catastrófico
    for _ in range(15):
        candles.append(_candle(100, 101, 99, 100))

    df = _make_df(candles)
    plan = calculate_trade_plan(
        df=df,
        direction="LONG",
        entry_price=100.0,
        setup_confidence=100.0,
        regime="RANGE",
        swing_lookback=20,
    )
    assert plan.sl_diagnostics["clamp_applied"] == "pct_max"
    # pré-clamp: swing_low (90) - buffer (~0.6) ~ 89.4
    assert plan.sl_diagnostics["sl_estrutural_pre_clamp"] < 95.0
    assert plan.sl_diagnostics["sl_max_pct"] == pytest.approx(95.0, abs=0.05)
    assert plan.sl_diagnostics["winner_source"] == "structural_swing"
