"""tests/test_signals.py — cobertura do orquestrador multi-setup v9.

Os testes isolam a lógica de orquestração mockando as dependências pesadas
(``build_market_context``, ``classify_regime``, ``detect_*``, ``evaluate_*``,
``calculate_trade_plan``, ``validate_trade_plan``). Cada cenário constrói
:class:`SetupResult` e estados sintéticos para cobrir um ramo específico de
``evaluate_token``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from unittest.mock import patch

import pandas as pd
import pytest

import signals as signals_mod
from setups.base import SetupResult


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


FAKE_TS = pd.Timestamp("2026-04-23 12:00:00", tz="UTC")


@dataclass
class _FakeCtx:
    """Stand-in mínimo para ``MarketContext`` no escopo dos testes."""

    symbol: str = "FOOUSDT"
    timestamp: pd.Timestamp = FAKE_TS
    timeframe: str = "15m"
    close: float = 100.0
    atr: float = 1.0  # 1% atr → abaixo do limite de 5%


@dataclass
class _FakeRegime:
    """Stand-in mínimo para ``RegimeClassification`` no escopo dos testes."""

    regime: str = "TREND_UP"
    adx_value: float = 30.0
    ema_slope: float = 1.0
    ema_alignment: str = "bullish"
    bb_width_ratio: float = 1.0
    confidence: float = 80.0


@dataclass
class _FakeTradePlan:
    """Stand-in mínimo para ``TradePlan`` — valores arbitrários."""

    direction: str = "LONG"
    entry_price: float = 100.0
    sl_price: float = 95.0
    tp1_price: float = 105.0
    tp2_price: float = 110.0
    tp3_price: float = 115.0
    leverage: float = 5.0


def _sr(
    name: str,
    triggered: bool,
    direction: Optional[str] = None,
    confidence: float = 0.0,
) -> SetupResult:
    return SetupResult(
        setup_name=name,
        triggered=triggered,
        direction=direction,
        confidence=confidence,
        evidence={},
        triggered_at=FAKE_TS,
    )


@pytest.fixture
def fake_df() -> pd.DataFrame:
    """Minimal OHLCV — as funções do pipeline estão mockadas."""
    return pd.DataFrame(
        {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [1.0]}
    )


class _Patches:
    """Context manager que instala os mocks padrão usados por cada teste."""

    def __init__(
        self,
        *,
        ctx: Optional[_FakeCtx] = None,
        regime: Optional[_FakeRegime] = None,
        setup_returns: Optional[dict[str, SetupResult]] = None,
        trade_plan: Optional[_FakeTradePlan] = None,
        validate_return: tuple[bool, str] = (True, ""),
    ) -> None:
        self.ctx = ctx or _FakeCtx()
        self.regime = regime or _FakeRegime()
        self.trade_plan = trade_plan if trade_plan is not None else _FakeTradePlan()
        self.validate_return = validate_return
        default = {
            "cont_pull": _sr("cont_pull", False),
            "rev_exaust": _sr("rev_exaust", False),
            "break_range": _sr("break_range", False),
            "rev_zone": _sr("rev_zone", False),
            "breaker": _sr("breaker", False),
        }
        if setup_returns:
            default.update(setup_returns)
        self.setup_returns = default
        self._patches: list = []

    def __enter__(self) -> "_Patches":
        self._patches = [
            patch.object(signals_mod, "build_market_context", return_value=self.ctx),
            patch.object(signals_mod, "classify_regime", return_value=self.regime),
            patch.object(signals_mod, "detect_order_blocks", return_value=[]),
            patch.object(signals_mod, "detect_breaker_blocks", return_value=[]),
            patch.object(
                signals_mod, "evaluate_cont_pull",
                return_value=self.setup_returns["cont_pull"],
            ),
            patch.object(
                signals_mod, "evaluate_rev_exaust",
                return_value=self.setup_returns["rev_exaust"],
            ),
            patch.object(
                signals_mod, "evaluate_break_range",
                return_value=self.setup_returns["break_range"],
            ),
            patch.object(
                signals_mod, "evaluate_rev_zone",
                return_value=self.setup_returns["rev_zone"],
            ),
            patch.object(
                signals_mod, "evaluate_breaker",
                return_value=self.setup_returns["breaker"],
            ),
            patch.object(
                signals_mod, "calculate_trade_plan",
                return_value=self.trade_plan,
            ),
            patch.object(
                signals_mod, "validate_trade_plan",
                return_value=self.validate_return,
            ),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        for p in self._patches:
            p.stop()


def _call(fake_df: pd.DataFrame, **kwargs):
    """Atalho para ``evaluate_token`` com defaults mínimos."""
    return signals_mod.evaluate_token(
        symbol=kwargs.pop("symbol", "FOOUSDT"),
        df_15m=fake_df,
        df_1h=fake_df,
        df_4h=fake_df,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# 1. Nenhum setup dispara
# ---------------------------------------------------------------------------


def test_evaluate_token_no_setup_triggered(fake_df):
    with _Patches():
        decision = _call(fake_df)

    assert decision.action == "SKIP"
    assert decision.skip_reason == "no_setup_triggered"
    assert decision.direction is None
    assert decision.signal_tag is None
    assert decision.confluent_setups == []
    assert decision.confidence == 0.0
    assert decision.trade_plan is None
    assert len(decision.all_setup_results) == 5


# ---------------------------------------------------------------------------
# 2. Apenas um setup dispara
# ---------------------------------------------------------------------------


def test_evaluate_token_single_setup_triggered(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 72.0)}
    with _Patches(setup_returns=returns):
        decision = _call(fake_df)

    assert decision.action == "CALL"
    assert decision.signal_tag == "cont_pull"
    assert decision.direction == "LONG"
    assert decision.confluent_setups == ["cont_pull"]
    assert decision.confidence == 72.0
    assert decision.trade_plan is not None
    assert decision.protection_filters["already_open"] is True
    assert decision.protection_filters["confidence_threshold"] is True


# ---------------------------------------------------------------------------
# 3. Confluência mesma direção — boost de 15%
# ---------------------------------------------------------------------------


def test_evaluate_token_confluence_same_direction(fake_df):
    returns = {
        "cont_pull": _sr("cont_pull", True, "LONG", 60.0),
        "break_range": _sr("break_range", True, "LONG", 70.0),
    }
    with _Patches(setup_returns=returns):
        decision = _call(fake_df)

    assert decision.action == "CALL"
    assert decision.direction == "LONG"
    assert sorted(decision.confluent_setups) == ["break_range", "cont_pull"]
    assert decision.signal_tag == "break_range+cont_pull"
    # 70 * 1.15 = 80.5
    assert decision.confidence == pytest.approx(80.5)


def test_evaluate_token_confluence_caps_at_100(fake_df):
    returns = {
        "cont_pull": _sr("cont_pull", True, "SHORT", 95.0),
        "breaker": _sr("breaker", True, "SHORT", 90.0),
    }
    with _Patches(setup_returns=returns):
        decision = _call(fake_df)

    assert decision.action == "CALL"
    assert decision.confidence == 100.0


# ---------------------------------------------------------------------------
# 4. Setups em conflito
# ---------------------------------------------------------------------------


def test_evaluate_token_conflicting_setups(fake_df):
    returns = {
        "cont_pull": _sr("cont_pull", True, "LONG", 65.0),
        "rev_zone": _sr("rev_zone", True, "SHORT", 70.0),
    }
    with _Patches(setup_returns=returns):
        decision = _call(fake_df)

    assert decision.action == "SKIP"
    assert decision.skip_reason == "conflicting_setups"
    assert decision.trade_plan is None
    assert sorted(decision.confluent_setups) == ["cont_pull", "rev_zone"]


# ---------------------------------------------------------------------------
# 5. Trade já aberto
# ---------------------------------------------------------------------------


def test_evaluate_token_open_trade_blocks(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 80.0)}
    with _Patches(setup_returns=returns):
        decision = _call(fake_df, symbol="FOOUSDT", open_trades=["FOOUSDT", "BARUSDT"])

    assert decision.action == "SKIP"
    assert decision.skip_reason == "trade_already_open"
    assert decision.trade_plan is None
    assert decision.protection_filters["already_open"] is False


# ---------------------------------------------------------------------------
# 6. Volatilidade extrema
# ---------------------------------------------------------------------------


def test_evaluate_token_extreme_volatility_blocks(fake_df):
    # ATR 10 sobre close 100 → 10% > 5% limite
    ctx = _FakeCtx(atr=10.0, close=100.0)
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 80.0)}
    with _Patches(ctx=ctx, setup_returns=returns):
        decision = _call(fake_df)

    assert decision.action == "SKIP"
    assert decision.skip_reason == "extreme_volatility"
    assert decision.protection_filters["already_open"] is True
    assert decision.protection_filters["volatility_ok"] is False


# ---------------------------------------------------------------------------
# 7. BTC em movimento abrupto
# ---------------------------------------------------------------------------


def test_evaluate_token_btc_abrupt_move_blocks(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 80.0)}
    btc_ctx = {"change_pct_15m": -3.0, "atr_pct": 1.5, "regime": "TREND_DOWN"}
    with _Patches(setup_returns=returns):
        decision = _call(fake_df, btc_context=btc_ctx)

    assert decision.action == "SKIP"
    assert decision.skip_reason == "btc_abrupt_move"
    assert decision.protection_filters["btc_stable"] is False


def test_evaluate_token_btc_context_none_not_checked(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 70.0)}
    with _Patches(setup_returns=returns):
        decision = _call(fake_df, btc_context=None)

    assert decision.action == "CALL"
    assert decision.protection_filters["btc_stable"] is None


# ---------------------------------------------------------------------------
# 8. Confidence mínima
# ---------------------------------------------------------------------------


def test_evaluate_token_low_confidence_blocks(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 40.0)}
    with _Patches(setup_returns=returns):
        decision = _call(fake_df)

    assert decision.action == "SKIP"
    assert decision.skip_reason == "low_confidence"
    assert decision.protection_filters["confidence_threshold"] is False


# ---------------------------------------------------------------------------
# 9. TradePlan inválido
# ---------------------------------------------------------------------------


def test_evaluate_token_invalid_trade_plan_blocks(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 80.0)}
    with _Patches(setup_returns=returns, validate_return=(False, "bad_rr")):
        decision = _call(fake_df)

    assert decision.action == "SKIP"
    assert decision.skip_reason is not None
    assert decision.skip_reason.startswith("invalid_trade_plan:")
    assert "bad_rr" in decision.skip_reason
    assert decision.trade_plan is None


# ---------------------------------------------------------------------------
# 10. Setup desabilitado não é avaliado
# ---------------------------------------------------------------------------


def test_evaluate_token_disabled_setup_not_evaluated(fake_df, monkeypatch):
    """``cont_pull`` desabilitado: não aparece em ``all_setup_results`` e
    ``evaluate_cont_pull`` não é chamado.

    Não usa ``_Patches`` para evitar que ``patch.object`` colida com a
    sobrescrita direta do mock rastreável — ``monkeypatch`` instala cada
    dependência de forma isolada e o ``MagicMock`` é o único objeto vivo
    durante a chamada, garantindo que o assert de ``call_count`` seja real.
    """
    from unittest.mock import MagicMock  # noqa: WPS433 — local em teste

    monkeypatch.setattr(signals_mod, "build_market_context", lambda *a, **k: _FakeCtx())
    monkeypatch.setattr(signals_mod, "classify_regime", lambda *a, **k: _FakeRegime())
    monkeypatch.setattr(signals_mod, "detect_order_blocks", lambda *a, **k: [])
    monkeypatch.setattr(signals_mod, "detect_breaker_blocks", lambda *a, **k: [])

    tracked_cont_pull = MagicMock(return_value=_sr("cont_pull", False))
    monkeypatch.setattr(signals_mod, "evaluate_cont_pull", tracked_cont_pull)
    monkeypatch.setattr(
        signals_mod, "evaluate_rev_exaust", lambda *a, **k: _sr("rev_exaust", False)
    )
    monkeypatch.setattr(
        signals_mod, "evaluate_break_range", lambda *a, **k: _sr("break_range", False)
    )
    monkeypatch.setattr(
        signals_mod, "evaluate_rev_zone", lambda *a, **k: _sr("rev_zone", False)
    )
    monkeypatch.setattr(
        signals_mod, "evaluate_breaker", lambda *a, **k: _sr("breaker", False)
    )

    decision = _call(fake_df, disabled_setups={"cont_pull"})

    names = [r.setup_name for r in decision.all_setup_results]
    assert "cont_pull" not in names
    assert len(decision.all_setup_results) == 4
    assert tracked_cont_pull.call_count == 0


# ---------------------------------------------------------------------------
# 11. all_setup_results sempre contém os setups rodados
# ---------------------------------------------------------------------------


def test_signal_decision_has_all_results_default(fake_df):
    with _Patches():
        decision = _call(fake_df)
    assert len(decision.all_setup_results) == 5


def test_signal_decision_has_all_results_with_disabled(fake_df):
    with _Patches():
        decision = _call(fake_df, disabled_setups={"rev_exaust", "breaker"})
    assert len(decision.all_setup_results) == 3
    names = {r.setup_name for r in decision.all_setup_results}
    assert "rev_exaust" not in names
    assert "breaker" not in names


# ---------------------------------------------------------------------------
# 12. trade_plan existe apenas em CALL
# ---------------------------------------------------------------------------


def test_signal_decision_trade_plan_when_call(fake_df):
    returns = {"cont_pull": _sr("cont_pull", True, "LONG", 80.0)}
    with _Patches(setup_returns=returns):
        decision = _call(fake_df)
    assert decision.action == "CALL"
    assert decision.trade_plan is not None


def test_signal_decision_trade_plan_none_when_skip(fake_df):
    with _Patches():
        decision = _call(fake_df)
    assert decision.action == "SKIP"
    assert decision.trade_plan is None
