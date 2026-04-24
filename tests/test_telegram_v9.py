"""Testes do módulo telegram v9 (PR 13A).

Cobre as três APIs públicas — notify_call, notify_heartbeat, notify_error —
através das funções puras de formatação (_fmt_call, _fmt_heartbeat,
_fmt_error). As asserções focam na string gerada; o transporte _tg_send
é mockado (ou monitorado) para garantir que side effects não vazem.

IMPORTANTE: o harness do Claude Code não tem pandas_ta — este arquivo é
validado na VM. Rodar: pytest tests/test_telegram_v9.py -v
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import telegram
from risk import TradePlan
from signals import SignalDecision


# ---------------------------------------------------------------------------
# Helpers de construção — dataclasses fixtas para os testes
# ---------------------------------------------------------------------------


def _make_trade_plan(
    direction: str = "LONG",
    entry: float = 67420.50,
    sl: float = 66800.00,
    leverage: float = 8.5,
) -> TradePlan:
    return TradePlan(
        direction=direction,
        entry_price=entry,
        sl_price=sl,
        sl_distance_pct=0.92,
        tp1_price=67920.00,
        tp2_price=68350.00,
        tp3_price=69500.00,
        tp1_distance_pct=0.74,
        tp2_distance_pct=1.38,
        tp3_distance_pct=3.08,
        risk_reward_tp1=0.80,
        risk_reward_tp2=1.50,
        risk_reward_tp3=3.35,
        atr_value=135.20,
        position_split=(0.50, 0.30, 0.20),
        leverage=leverage,
    )


def _make_decision(
    symbol: str = "BTCUSDT",
    action: str = "CALL",
    direction: str = "LONG",
    signal_tag: str = "cont_pull",
    confluent_setups: list | None = None,
    confidence: float = 87.0,
    regime: str = "TREND_UP",
    trade_plan: TradePlan | None = None,
) -> SignalDecision:
    import pandas as pd

    if trade_plan is None and action == "CALL":
        trade_plan = _make_trade_plan(direction=direction)
    if confluent_setups is None:
        confluent_setups = (signal_tag or "").split("+") if signal_tag else []

    return SignalDecision(
        symbol=symbol,
        timestamp=pd.Timestamp("2026-04-23 14:45:00"),
        timeframe="15m",
        action=action,
        direction=direction if action == "CALL" else None,
        signal_tag=signal_tag,
        confluent_setups=confluent_setups,
        confidence=confidence,
        trade_plan=trade_plan,
        regime=regime,
        all_setup_results=[],
        protection_filters={},
        skip_reason=None if action == "CALL" else "test_skip",
    )


def _make_round_stats(
    setups_counter: dict | None = None,
    fgi: int | None = 64,
    btc_change_pct_15m: float = 0.42,
    elapsed_seconds: float = 18.0,
) -> telegram.RoundStats:
    if setups_counter is None:
        setups_counter = {"cont_pull": 3, "rev_zone": 2, "breaker": 2, "rev_exaust": 1}
    return telegram.RoundStats(
        n_universe=47,
        n_regime_eligible=31,
        n_triggered=8,
        n_calls=2,
        n_near_miss=4,
        setups_counter=setups_counter,
        fgi=fgi,
        btc_regime="TREND_UP",
        btc_change_pct_15m=btc_change_pct_15m,
        elapsed_seconds=elapsed_seconds,
        exchange="OKX",
    )


# ---------------------------------------------------------------------------
# 1. CALL — LONG, setup único
# ---------------------------------------------------------------------------


def test_format_call_long_single_setup():
    decision = _make_decision(direction="LONG", signal_tag="cont_pull")
    out = telegram._fmt_call(decision)

    assert "🟢 LONG CALL" in out
    assert "BTCUSDT" in out
    assert "Setup: cont_pull" in out
    assert " + " not in out.split("Setup: ")[1].split("\n")[0]
    assert "Regime: TREND_UP" in out
    assert "Confidence: 87/100" in out
    assert "Leverage: 8.5x" in out

    assert "SL      : 66800.0000  (-0.92%)" in out
    assert "TP1 50% : 67920.0000  (+0.74%  R:R 0.80)" in out
    assert "TP2 30% : 68350.0000  (+1.38%  R:R 1.50)" in out
    assert "TP3 20% : 69500.0000  (+3.08%  R:R 3.35)" in out

    assert "ATR 135.2000" in out
    assert "🛡 Gestão dinâmica" in out
    assert "TP3 = runner (20% mantido)" in out
    assert '<a href="https://www.tradingview.com/chart/?symbol=OKX:BTCUSDT.P&interval=15">15m</a>' in out
    assert '<a href="https://www.tradingview.com/chart/?symbol=OKX:BTCUSDT.P&interval=240">4H</a>' in out


# ---------------------------------------------------------------------------
# 2. CALL — SHORT, confluência
# ---------------------------------------------------------------------------


def test_format_call_short_confluence():
    decision = _make_decision(
        direction="SHORT",
        signal_tag="cont_pull+rev_zone",
        confluent_setups=["cont_pull", "rev_zone"],
    )
    out = telegram._fmt_call(decision)

    assert "🔴 SHORT CALL" in out
    assert "Setup: cont_pull + rev_zone" in out

    assert "SL      : 66800.0000  (+0.92%)" in out
    assert "TP1 50% : 67920.0000  (-0.74%" in out
    assert "TP2 30% : 68350.0000  (-1.38%" in out
    assert "TP3 20% : 69500.0000  (-3.08%" in out


# ---------------------------------------------------------------------------
# 3. SKIP → notify_call retorna False sem enviar
# ---------------------------------------------------------------------------


def test_format_call_skip_returns_false():
    decision = _make_decision(action="SKIP", signal_tag=None, trade_plan=None)
    with patch.object(telegram, "_tg_send") as mock_send:
        result = telegram.notify_call(decision)
    assert result is False
    mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# 4. HTML escape em campos textuais
# ---------------------------------------------------------------------------


def test_format_call_escapes_html():
    decision = _make_decision(symbol="FOO<BAR>USDT", signal_tag="cont_pull")
    out = telegram._fmt_call(decision)

    assert "FOO&lt;BAR&gt;USDT" in out
    assert "— FOO<BAR>USDT —" not in out


# ---------------------------------------------------------------------------
# 5. Heartbeat — setups_counter vazio vira em dash
# ---------------------------------------------------------------------------


def test_format_heartbeat_empty_setups():
    stats = _make_round_stats(setups_counter={})
    out = telegram._fmt_heartbeat(stats)
    assert "🎪 Setups         : —" in out


# ---------------------------------------------------------------------------
# 6. Heartbeat — setups_counter ordenado desc, alfabético ASC em empate
# ---------------------------------------------------------------------------


def test_format_heartbeat_setups_sorted():
    stats = _make_round_stats(setups_counter={"a": 1, "b": 3, "c": 2})
    out = telegram._fmt_heartbeat(stats)
    line = [ln for ln in out.splitlines() if ln.startswith("🎪")][0]
    assert "b:3 · c:2 · a:1" in line


def test_format_heartbeat_empate_alfabetico():
    stats = _make_round_stats(setups_counter={"z": 2, "a": 2, "m": 2})
    out = telegram._fmt_heartbeat(stats)
    line = [ln for ln in out.splitlines() if ln.startswith("🎪")][0]
    assert "a:2 · m:2 · z:2" in line


# ---------------------------------------------------------------------------
# 7. Heartbeat desabilitado via TELEGRAM_HEARTBEAT=False
# ---------------------------------------------------------------------------


def test_format_heartbeat_disabled(monkeypatch):
    monkeypatch.setattr(telegram, "TELEGRAM_HEARTBEAT", False)
    stats = _make_round_stats()
    with patch.object(telegram, "_tg_send") as mock_send:
        result = telegram.notify_heartbeat(stats)
    assert result is False
    mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Erro básico
# ---------------------------------------------------------------------------


def test_format_error_basic():
    out = telegram._fmt_error("Falha na rodada", "ZeroDivisionError: division by zero")
    assert "🚨 ERRO — Setup Atirador" in out
    assert "📍 Título: Falha na rodada" in out
    assert "<pre>ZeroDivisionError: division by zero</pre>" in out
    assert "⏰ Timestamp:" in out
    assert " BRT" in out


# ---------------------------------------------------------------------------
# 9. Erro com contexto
# ---------------------------------------------------------------------------


def test_format_error_with_context():
    out = telegram._fmt_error(
        "Falha na rodada",
        "traceback line",
        context={"symbol": "BTCUSDT", "stage": "calculate_trade_plan"},
    )
    assert "🔍 Contexto:" in out
    assert "  symbol: BTCUSDT" in out
    assert "  stage: calculate_trade_plan" in out


# ---------------------------------------------------------------------------
# 10. Erro sem contexto — não renderiza bloco
# ---------------------------------------------------------------------------


def test_format_error_no_context():
    out = telegram._fmt_error("X", "y", context=None)
    assert "🔍 Contexto" not in out

    out_empty = telegram._fmt_error("X", "y", context={})
    assert "🔍 Contexto" not in out_empty


# ---------------------------------------------------------------------------
# 11. Erro truncado acima de 1000 chars
# ---------------------------------------------------------------------------


def test_format_error_truncation():
    long_msg = "A" * 1500
    out = telegram._fmt_error("X", long_msg)
    assert "[truncado]" in out
    payload = out.split("<pre>")[1].split("</pre>")[0]
    assert payload.count("A") == 1000
    assert payload.endswith("[truncado]")


# ---------------------------------------------------------------------------
# 12. Config loading — assinatura e fallback env-var preservados do v8
# ---------------------------------------------------------------------------


def test_config_loading_preserved(monkeypatch, tmp_path):
    monkeypatch.setattr(telegram, "TELEGRAM_CONFIG_FILE", str(tmp_path / "no_primary.json"))
    monkeypatch.setattr(telegram, "TELEGRAM_CONFIG_FILE_LEGACY", str(tmp_path / "no_legacy.json"))
    monkeypatch.setenv("TELEGRAM_TOKEN", "token_from_env")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat_from_env")

    token, chat_id = telegram._load_telegram_config()
    assert token == "token_from_env"
    assert chat_id == "chat_from_env"


# ---------------------------------------------------------------------------
# Extras — garantem integração das APIs públicas com as funções puras
# ---------------------------------------------------------------------------


def test_notify_call_sends_formatted_message(monkeypatch):
    monkeypatch.setattr(telegram, "_TELEGRAM_TOKEN", "t")
    monkeypatch.setattr(telegram, "_TELEGRAM_CHAT_ID", "c")

    decision = _make_decision()
    captured = {}

    def fake_send(text: str) -> bool:
        captured["text"] = text
        return True

    monkeypatch.setattr(telegram, "_tg_send", fake_send)
    assert telegram.notify_call(decision) is True
    assert "🟢 LONG CALL" in captured["text"]


def test_notify_heartbeat_btc_negative_sign(monkeypatch):
    monkeypatch.setattr(telegram, "TELEGRAM_HEARTBEAT", True)
    monkeypatch.setattr(telegram, "_TELEGRAM_TOKEN", "t")
    monkeypatch.setattr(telegram, "_TELEGRAM_CHAT_ID", "c")

    stats = _make_round_stats(btc_change_pct_15m=-1.23, fgi=None)
    captured = {}

    def fake_send(text: str) -> bool:
        captured["text"] = text
        return True

    monkeypatch.setattr(telegram, "_tg_send", fake_send)
    assert telegram.notify_heartbeat(stats) is True
    assert "BTC: TREND_UP (-1.23%)" in captured["text"]
    assert "FGI: —" in captured["text"]


def test_format_heartbeat_btc_nan(monkeypatch):
    monkeypatch.setattr(telegram, "TELEGRAM_HEARTBEAT", True)
    monkeypatch.setattr(telegram, "_TELEGRAM_TOKEN", "t")
    monkeypatch.setattr(telegram, "_TELEGRAM_CHAT_ID", "c")

    stats = _make_round_stats(btc_change_pct_15m=float("nan"))
    captured = {}

    def fake_send(text: str) -> bool:
        captured["text"] = text
        return True

    monkeypatch.setattr(telegram, "_tg_send", fake_send)
    assert telegram.notify_heartbeat(stats) is True
    assert "BTC: TREND_UP (—)" in captured["text"]
    assert "nan" not in captured["text"].lower()


def test_format_heartbeat_btc_inf(monkeypatch):
    monkeypatch.setattr(telegram, "TELEGRAM_HEARTBEAT", True)
    monkeypatch.setattr(telegram, "_TELEGRAM_TOKEN", "t")
    monkeypatch.setattr(telegram, "_TELEGRAM_CHAT_ID", "c")

    stats = _make_round_stats(btc_change_pct_15m=float("inf"))
    captured = {}

    def fake_send(text: str) -> bool:
        captured["text"] = text
        return True

    monkeypatch.setattr(telegram, "_tg_send", fake_send)
    assert telegram.notify_heartbeat(stats) is True
    assert "BTC: TREND_UP (—)" in captured["text"]
    assert "inf" not in captured["text"].lower()


def test_notify_error_no_config_returns_false(monkeypatch):
    monkeypatch.setattr(telegram, "_TELEGRAM_TOKEN", "")
    monkeypatch.setattr(telegram, "_TELEGRAM_CHAT_ID", "")
    monkeypatch.setattr(telegram, "TELEGRAM_CONFIG_FILE", "/nonexistent/a")
    monkeypatch.setattr(telegram, "TELEGRAM_CONFIG_FILE_LEGACY", "/nonexistent/b")
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    with patch.object(telegram, "_tg_send") as mock_send:
        assert telegram.notify_error("oops", "boom") is False
    mock_send.assert_not_called()
