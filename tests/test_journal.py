"""Testes do módulo journal v9 — esqueleto + helpers puros (PR 12c.A).

Escopo deste PR: inicialização do DB, cálculo de pnl parcial (WIN_TP3) e
cálculo de métricas sobre lista vazia. Os testes dos métodos open_trade,
check_open_trades e performance vêm em 12c.B/C/D.
"""

from __future__ import annotations

import sqlite3

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
