"""Testes do módulo journal v9 — esqueleto + helpers puros (PR 12c.A).

Escopo deste PR: inicialização do DB, cálculo de pnl parcial (WIN_TP3) e
cálculo de métricas sobre lista vazia. Os testes dos métodos open_trade,
check_open_trades e performance vêm em 12c.B/C/D.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Optional

import pytest

from journal import (
    TradeJournalV9,
    _calc_metrics_v9,
    _calc_partial_pnl,
)


# ---------------------------------------------------------------------------
# Fixture — redireciona JOURNAL_DB_V9 para tmp
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_journal(tmp_path, monkeypatch):
    db_path = tmp_path / "journal" / "atirador_journal_v9.db"
    import config as cfg
    import journal as journal_mod
    monkeypatch.setattr(cfg, "JOURNAL_DB_V9", str(db_path), raising=True)
    monkeypatch.setattr(journal_mod, "JOURNAL_DB_V9", str(db_path), raising=True)
    return {"db_path": str(db_path), "journal_mod": journal_mod}


# ---------------------------------------------------------------------------
# 1. Inicialização — DB e schema
# ---------------------------------------------------------------------------


EXPECTED_COLUMNS = {
    "id", "timestamp", "symbol", "direction", "setup_name",
    "confluent_setups", "confidence", "regime_at_entry",
    "entry_price", "sl_price", "current_sl",
    "tp1_price", "tp2_price", "tp3_price",
    "leverage", "atr_value", "position_split",
    "tp1_hit", "tp2_hit", "tp3_hit", "position_remaining",
    "context_fgi", "context_btc_regime",
    "status", "exit_price", "exit_time", "pnl_pct",
    "max_runup", "max_drawdown", "timeout_hours", "evidence_json",
}


def test_journal_initialization_creates_trades_table_with_v9_schema(tmp_journal):
    """Inicializa journal em tmp path: tabela `trades` existe com schema v9 completo."""
    j = TradeJournalV9(tmp_journal["db_path"])
    assert j is not None

    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        cols = {
            row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()
        }
        # Campos novos v9 devem estar todos presentes
        assert EXPECTED_COLUMNS <= cols, (
            f"Faltam colunas v9: {EXPECTED_COLUMNS - cols}"
        )
        # Campos v8 removidos NÃO devem estar no schema
        removed = {"score", "is_hypothetical", "type", "pillars_json",
                   "kline_venue", "tv_venue", "venue_quality"}
        assert removed.isdisjoint(cols), (
            f"Campos v8 não removidos: {removed & cols}"
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. _calc_partial_pnl — WIN_TP3 LONG
# ---------------------------------------------------------------------------


def test_calc_partial_pnl_win_tp3_long():
    """WIN_TP3 LONG: pnl = 0.5*pct(tp1) + 0.3*pct(tp2) + 0.2*pct(tp3).

    Entry=100, tp1=102 (+2%), tp2=104 (+4%), tp3=107 (+7%).
    Esperado = 0.5*2 + 0.3*4 + 0.2*7 = 1.0 + 1.2 + 1.4 = 3.6%.
    """
    pnl = _calc_partial_pnl(
        status="WIN_TP3",
        direction="LONG",
        entry=100.0,
        tp1=102.0,
        tp2=104.0,
        tp3=107.0,
        current_sl=98.0,  # irrelevante para WIN_TP3
        position_split=(0.5, 0.3, 0.2),
    )
    assert pnl == pytest.approx(3.6, abs=0.01)


# ---------------------------------------------------------------------------
# 3. _calc_metrics_v9 — lista vazia retorna zeros coerentes
# ---------------------------------------------------------------------------


def test_calc_metrics_v9_empty_list_returns_zero_dict():
    """Lista vazia → dict com todos os campos em zero/vazio, sem divisão por zero."""
    m = _calc_metrics_v9([])
    assert m["total"] == 0
    assert m["wins"] == 0
    assert m["losses"] == 0
    assert m["win_rate"] == 0.0
    assert m["profit_factor"] == 0.0
    assert m["expectancy"] == 0.0
    assert m["breakdown"] == {}
    assert m["avg_pnl"] == 0.0
    assert m["avg_runup"] == 0.0
    assert m["avg_drawdown"] == 0.0


# ---------------------------------------------------------------------------
# Fakes para testar open_trade sem acoplar a signals.py / risk.py reais
# ---------------------------------------------------------------------------


@dataclass
class FakeTradePlan:
    direction: str = "LONG"
    entry_price: float = 100.0
    sl_price: float = 98.0
    tp1_price: float = 102.0
    tp2_price: float = 104.0
    tp3_price: float = 107.0
    leverage: float = 5.0
    atr_value: float = 1.5
    position_split: tuple = (0.5, 0.3, 0.2)


@dataclass
class FakeSetupResult:
    setup_name: str = "cont_pull"
    triggered: bool = True
    direction: Optional[str] = "LONG"
    confidence: float = 75.0
    evidence: dict = field(default_factory=lambda: {"foo": "bar"})


@dataclass
class FakeSignalDecision:
    symbol: str = "BTCUSDT"
    direction: Optional[str] = "LONG"
    action: str = "CALL"
    signal_tag: Optional[str] = "cont_pull"
    confluent_setups: list = field(default_factory=lambda: ["cont_pull"])
    confidence: float = 82.5
    trade_plan: Any = field(default_factory=FakeTradePlan)
    regime: str = "TREND_UP"
    all_setup_results: list = field(default_factory=lambda: [FakeSetupResult()])


# ---------------------------------------------------------------------------
# 4. open_trade — caso básico
# ---------------------------------------------------------------------------


def test_open_trade_basic_call_persists_all_fields(tmp_journal):
    """CALL com trade_plan → id retornado, linha persistida com campos v9."""
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()

    trade_id = j.open_trade(decision, fgi=55)
    assert trade_id is not None
    assert trade_id.startswith("BTCUSDT_LONG_")
    # Formato YYYYMMDD_HHMM no sufixo
    import re
    assert re.match(r"BTCUSDT_LONG_\d{8}_\d{4}$", trade_id)

    # Verifica persistência
    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        assert row is not None
        assert row["symbol"] == "BTCUSDT"
        assert row["direction"] == "LONG"
        assert row["setup_name"] == "cont_pull"
        assert row["confidence"] == 82.5
        assert row["regime_at_entry"] == "TREND_UP"
        assert row["entry_price"] == 100.0
        assert row["sl_price"] == 98.0
        # current_sl == sl_price ao abrir
        assert row["current_sl"] == 98.0
        assert row["tp1_price"] == 102.0
        assert row["tp2_price"] == 104.0
        assert row["tp3_price"] == 107.0
        assert row["leverage"] == 5.0
        assert row["atr_value"] == 1.5
        # position_split serializado como JSON
        import json as _json
        assert _json.loads(row["position_split"]) == [0.5, 0.3, 0.2]
        # tp_hit = 0, position_remaining = 1.0
        assert row["tp1_hit"] == 0
        assert row["tp2_hit"] == 0
        assert row["tp3_hit"] == 0
        assert row["position_remaining"] == 1.0
        # Status e contexto
        assert row["status"] == "OPEN"
        assert row["context_fgi"] == 55
        assert row["timeout_hours"] == 48
        # Evidence contém apenas setups triggered
        evidence = _json.loads(row["evidence_json"])
        assert isinstance(evidence, list)
        assert len(evidence) == 1
        assert evidence[0]["setup_name"] == "cont_pull"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 5. open_trade — rejeições (SKIP, trade_plan None, direction inválida)
# ---------------------------------------------------------------------------


def test_open_trade_rejects_skip_and_invalid(tmp_journal):
    """Decisões inválidas não persistem e retornam None."""
    j = TradeJournalV9(tmp_journal["db_path"])

    # action=SKIP
    skip_decision = FakeSignalDecision(action="SKIP", direction=None, trade_plan=None)
    assert j.open_trade(skip_decision) is None

    # trade_plan=None (mesmo com action=CALL)
    no_plan = FakeSignalDecision(trade_plan=None)
    assert j.open_trade(no_plan) is None

    # direction inválida
    bad_dir = FakeSignalDecision(direction="NEUTRAL")
    assert j.open_trade(bad_dir) is None

    # Nenhuma linha gravada
    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        assert count == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 6. open_trade — dedupe por (symbol, direction, OPEN)
# ---------------------------------------------------------------------------


def test_open_trade_dedupe_same_symbol_direction(tmp_journal):
    """Duas chamadas mesmo (symbol, direction) → segunda retorna id existente."""
    j = TradeJournalV9(tmp_journal["db_path"])
    d1 = FakeSignalDecision(symbol="BTCUSDT", direction="LONG", signal_tag="cont_pull")
    d2 = FakeSignalDecision(
        symbol="BTCUSDT", direction="LONG", signal_tag="cont_pull+rev_zone",
        confluent_setups=["cont_pull", "rev_zone"],
    )

    id1 = j.open_trade(d1)
    id2 = j.open_trade(d2)
    assert id1 is not None
    assert id2 == id1  # dedupe retorna o mesmo id

    # Apenas 1 linha no DB
    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        assert count == 1
        # E o setup_name do primeiro está preservado (não foi sobrescrito)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT setup_name FROM trades").fetchone()
        assert row["setup_name"] == "cont_pull"
    finally:
        conn.close()

    # Direções diferentes NÃO deduplizam (BTCUSDT LONG e BTCUSDT SHORT coexistem)
    d_short = FakeSignalDecision(
        symbol="BTCUSDT", direction="SHORT",
        trade_plan=FakeTradePlan(
            direction="SHORT", entry_price=100.0, sl_price=102.0,
            tp1_price=98.0, tp2_price=96.0, tp3_price=93.0,
        ),
    )
    id_short = j.open_trade(d_short)
    assert id_short is not None
    assert id_short != id1

    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        assert count == 2
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 7. get_open_trade — encontrado, não encontrado, normalização de símbolo
# ---------------------------------------------------------------------------


def test_get_open_trade_found_and_not_found_and_normalization(tmp_journal):
    """get_open_trade retorna dict quando OPEN, None quando não, normaliza símbolo."""
    j = TradeJournalV9(tmp_journal["db_path"])

    # Sem trades ainda → None
    assert j.get_open_trade("BTCUSDT") is None
    assert j.get_open_trade("BTCUSDT", direction="LONG") is None

    # Abre um trade LONG
    j.open_trade(FakeSignalDecision(symbol="BTCUSDT", direction="LONG"))

    # Encontrado sem direction
    row = j.get_open_trade("BTCUSDT")
    assert row is not None
    assert row["symbol"] == "BTCUSDT"
    assert row["direction"] == "LONG"
    assert row["status"] == "OPEN"

    # Encontrado com direction correta
    row_long = j.get_open_trade("BTCUSDT", direction="LONG")
    assert row_long is not None
    assert row_long["direction"] == "LONG"

    # Não encontrado com direction oposta
    assert j.get_open_trade("BTCUSDT", direction="SHORT") is None

    # Normalização — "BTC" → procura "BTCUSDT"
    row_norm = j.get_open_trade("BTC")
    assert row_norm is not None
    assert row_norm["symbol"] == "BTCUSDT"

    # Case-insensitive via upper()
    row_lower = j.get_open_trade("btcusdt")
    assert row_lower is not None
    assert row_lower["symbol"] == "BTCUSDT"

    # Símbolo que não existe
    assert j.get_open_trade("NONEXISTENTUSDT") is None
