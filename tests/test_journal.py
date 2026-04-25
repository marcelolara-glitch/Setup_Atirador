"""Testes do módulo journal v9 — esqueleto + helpers puros (PR 12c.A).

Escopo deste PR: inicialização do DB, cálculo de pnl parcial (WIN_TP3) e
cálculo de métricas sobre lista vazia. Os testes dos métodos open_trade,
check_open_trades e performance vêm em 12c.B/C/D.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import pytest

from config import BRT
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
        # Evidence é dict top-level com tp1_source/sl_source + setups triggered
        evidence = _json.loads(row["evidence_json"])
        assert isinstance(evidence, dict)
        assert "tp1_source" in evidence
        assert "sl_source" in evidence
        assert isinstance(evidence["setups"], list)
        assert len(evidence["setups"]) == 1
        assert evidence["setups"][0]["setup_name"] == "cont_pull"
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


# ---------------------------------------------------------------------------
# Helpers de klines para testes de check_open_trades
# ---------------------------------------------------------------------------


def _make_candle(ts_ms: int, o: float, h: float, l: float, c: float, v: float = 1000.0) -> dict:
    """Factory de candle no formato v9 (dict com keys: ts, open, high, low, close, volume)."""
    return {"ts": ts_ms, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _klines_sequence(start_ts_ms: int, candles: list[dict], step_ms: int = 15 * 60 * 1000) -> list[dict]:
    """Atribui timestamps crescentes (a partir de start_ts_ms) a uma sequência de candles."""
    result = []
    for i, c in enumerate(candles):
        c2 = dict(c)
        c2["ts"] = start_ts_ms + (i + 1) * step_ms  # +1 para ser > timestamp de abertura
        result.append(c2)
    return result


def _open_trade_and_get_ts_ms(j: TradeJournalV9, decision) -> tuple[str, int]:
    """Abre um trade e retorna (trade_id, timestamp em ms da abertura)."""
    trade_id = j.open_trade(decision)
    assert trade_id is not None
    conn = sqlite3.connect(j.db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT timestamp FROM trades WHERE id=?", (trade_id,)).fetchone()
        ts_iso = row["timestamp"]
    finally:
        conn.close()
    from datetime import datetime as _dt
    ts_ms = int(_dt.fromisoformat(ts_iso).timestamp() * 1000)
    return trade_id, ts_ms


# ---------------------------------------------------------------------------
# 8. check_open_trades — end-to-end WIN_TP3 LONG
# ---------------------------------------------------------------------------


def test_check_open_trades_win_tp3_long_end_to_end(tmp_journal):
    """LONG abre em 100, candles subindo até TP3=107 → WIN_TP3.

    Plan: entry=100, sl=98, tp1=102, tp2=104, tp3=107, split (0.5, 0.3, 0.2).
    Esperado: pnl = 0.5*2 + 0.3*4 + 0.2*7 = 3.6%, exit_price = 107.
    """
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()  # LONG 100/98/102/104/107 default
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    # Sequência de candles: cada um atinge um TP progressivamente
    candles = [
        _make_candle(0, 100, 102.5, 99.5, 101.5),   # atinge TP1 (high=102.5 >= 102)
        _make_candle(0, 101, 104.5, 100.5, 103.5),  # atinge TP2 (high=104.5 >= 104)
        _make_candle(0, 103, 108.0, 102.5, 107.5),  # atinge TP3 (high=108 >= 107)
    ]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731 — lambda é suficiente aqui

    closed = j.check_open_trades(fetch_klines_fn=fetcher)
    assert closed == 1

    # Verifica estado final
    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        assert row["status"] == "WIN_TP3"
        assert row["tp1_hit"] == 1
        assert row["tp2_hit"] == 1
        assert row["tp3_hit"] == 1
        assert row["position_remaining"] == pytest.approx(0.0, abs=1e-6)
        assert row["exit_price"] == pytest.approx(107.0)
        assert row["pnl_pct"] == pytest.approx(3.6, abs=0.01)
        # max_runup >= pnl positivo
        assert row["max_runup"] >= 3.0
        # max_drawdown moderado (caiu até 99.5 no primeiro candle)
        assert row["max_drawdown"] >= 0.4
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 9. check_open_trades — LOSS_SL direto
# ---------------------------------------------------------------------------


def test_check_open_trades_loss_sl_long(tmp_journal):
    """LONG entry=100, candle com low=97.5 < SL=98 → LOSS_SL.

    Esperado: exit_price = 98 (SL), pnl ≈ -2% (pct do SL).
    """
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    candles = [_make_candle(0, 100, 100.5, 97.5, 97.8)]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731
    closed = j.check_open_trades(fetch_klines_fn=fetcher)
    assert closed == 1

    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        assert row["status"] == "LOSS_SL"
        assert row["tp1_hit"] == 0
        assert row["tp2_hit"] == 0
        assert row["tp3_hit"] == 0
        assert row["exit_price"] == pytest.approx(98.0)
        # pnl = pct_long(98, 100) = -2%
        assert row["pnl_pct"] == pytest.approx(-2.0, abs=0.01)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 10. check_open_trades — TP1 hit mas trade ainda OPEN (saída parcial)
# ---------------------------------------------------------------------------


def test_check_open_trades_tp1_hit_stays_open(tmp_journal):
    """LONG entry=100, candle atinge TP1=102 mas não TP2.
    → tp1_hit=1, current_sl=entry (breakeven), ainda OPEN, retorna 0.
    """
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    # Candle atinge TP1 (102) mas high não chega a TP2 (104)
    candles = [_make_candle(0, 100, 102.5, 99.8, 102.3)]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731
    closed = j.check_open_trades(fetch_klines_fn=fetcher)
    assert closed == 0  # trade não fechou

    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        assert row["status"] == "OPEN"
        assert row["tp1_hit"] == 1
        assert row["tp2_hit"] == 0
        assert row["tp3_hit"] == 0
        # SL moveu para breakeven (entry=100)
        assert row["current_sl"] == pytest.approx(100.0)
        # 50% da posição saiu
        assert row["position_remaining"] == pytest.approx(0.5, abs=1e-6)
        # exit_price ainda None
        assert row["exit_price"] is None
        assert row["pnl_pct"] is None
        # max_runup preservado
        assert row["max_runup"] >= 2.0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 11. check_open_trades — EXPIRED após timeout de 48h
# ---------------------------------------------------------------------------


def test_check_open_trades_expired_after_timeout(tmp_journal):
    """Trade aberto há >48h → EXPIRED, pnl=0, exit_price=entry."""
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id = j.open_trade(decision)
    assert trade_id is not None

    # Força timestamp do trade para 49h atrás
    from datetime import timedelta
    old_ts = (datetime.now(BRT) - timedelta(hours=49)).isoformat()
    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        conn.execute(
            "UPDATE trades SET timestamp=? WHERE id=?",
            (old_ts, trade_id),
        )
        conn.commit()
    finally:
        conn.close()

    # Fetcher nem precisa retornar candles — timeout é checado antes do fetch
    fetcher = lambda symbol: []  # noqa: E731
    closed = j.check_open_trades(fetch_klines_fn=fetcher)
    assert closed == 1

    conn = sqlite3.connect(tmp_journal["db_path"])
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        assert row["status"] == "EXPIRED"
        assert row["exit_price"] == pytest.approx(100.0)  # = entry
        assert row["pnl_pct"] == pytest.approx(0.0)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers para testes de get_performance
# ---------------------------------------------------------------------------


def _force_closed_trade(
    j: TradeJournalV9,
    symbol: str = "BTCUSDT",
    direction: str = "LONG",
    setup_name: str = "cont_pull",
    status: str = "WIN_TP1",
    pnl_pct: float = 1.0,
    days_ago: int = 0,
    max_runup: float = 1.2,
    max_drawdown: float = 0.3,
) -> str:
    """Insere um trade já fechado diretamente no DB para testes de performance.

    Bypassa open_trade/check_open_trades para controle total sobre os campos.
    Timestamp é ajustado para ``days_ago`` dias atrás.
    """
    from datetime import timedelta as _td
    ts = (datetime.now(BRT) - _td(days=days_ago)).isoformat()
    exit_time = ts  # simplificação — não importa para get_performance
    trade_id = f"{symbol}_{direction}_{ts[:10].replace('-', '')}_0000_{setup_name}"

    conn = sqlite3.connect(j.db_path)
    try:
        conn.execute(
            """INSERT INTO trades (
                   id, timestamp, symbol, direction, setup_name,
                   confluent_setups, confidence, regime_at_entry,
                   entry_price, sl_price, current_sl,
                   tp1_price, tp2_price, tp3_price,
                   leverage, atr_value, position_split,
                   tp1_hit, tp2_hit, tp3_hit, position_remaining,
                   context_fgi, context_btc_regime,
                   status, timeout_hours, evidence_json,
                   exit_price, exit_time, pnl_pct,
                   max_runup, max_drawdown
               ) VALUES (
                   ?, ?, ?, ?, ?,
                   '[]', 75.0, 'TREND_UP',
                   100.0, 98.0, 98.0,
                   102.0, 104.0, 107.0,
                   5.0, 1.5, '[0.5, 0.3, 0.2]',
                   ?, ?, ?, ?,
                   50, 'TREND_UP',
                   ?, 48, '[]',
                   ?, ?, ?,
                   ?, ?
               )""",
            (
                trade_id, ts, symbol, direction, setup_name,
                1 if status in ("WIN_TP1", "WIN_TP2", "WIN_TP3") else 0,
                1 if status in ("WIN_TP2", "WIN_TP3") else 0,
                1 if status == "WIN_TP3" else 0,
                0.0 if status.startswith("WIN_TP3") or status == "LOSS_SL" else 0.5,
                status,
                102.0, exit_time, pnl_pct,
                max_runup, max_drawdown,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return trade_id


# ---------------------------------------------------------------------------
# 12. get_performance — vazio quando não há trades
# ---------------------------------------------------------------------------


def test_get_performance_empty_db_returns_zeros(tmp_journal):
    """DB vazio → dict com total=0 e métricas zeradas (não None nem exception)."""
    j = TradeJournalV9(tmp_journal["db_path"])
    metrics = j.get_performance()
    assert metrics["total"] == 0
    assert metrics["wins"] == 0
    assert metrics["losses"] == 0
    assert metrics["win_rate"] == 0.0
    assert metrics["profit_factor"] == 0.0
    assert metrics["expectancy"] == 0.0
    assert metrics["breakdown"] == {
        "WIN_TP1": 0, "WIN_TP2": 0, "WIN_TP3": 0,
        "LOSS_SL": 0, "EXPIRED": 0,
    }


# ---------------------------------------------------------------------------
# 13. get_performance — agregação e filtros
# ---------------------------------------------------------------------------


def test_get_performance_aggregation_and_filters(tmp_journal):
    """3 trades fechados: 2 wins + 1 loss. Testa métricas globais e filtros."""
    j = TradeJournalV9(tmp_journal["db_path"])

    # Trade 1: LONG cont_pull WIN_TP2 (+3%)
    _force_closed_trade(
        j, symbol="BTCUSDT", direction="LONG", setup_name="cont_pull",
        status="WIN_TP2", pnl_pct=3.0, max_runup=4.0, max_drawdown=0.5,
    )
    # Trade 2: LONG rev_zone WIN_TP1 (+1%)
    _force_closed_trade(
        j, symbol="ETHUSDT", direction="LONG", setup_name="rev_zone",
        status="WIN_TP1", pnl_pct=1.0, max_runup=2.5, max_drawdown=0.2,
    )
    # Trade 3: SHORT cont_pull LOSS_SL (-2%)
    _force_closed_trade(
        j, symbol="SOLUSDT", direction="SHORT", setup_name="cont_pull",
        status="LOSS_SL", pnl_pct=-2.0, max_runup=0.3, max_drawdown=2.1,
    )

    # Sem filtros: total=3, wins=2, losses=1
    all_metrics = j.get_performance()
    assert all_metrics["total"] == 3
    assert all_metrics["wins"] == 2
    assert all_metrics["losses"] == 1
    assert all_metrics["win_rate"] == pytest.approx(66.666, abs=0.1)
    assert all_metrics["breakdown"]["WIN_TP1"] == 1
    assert all_metrics["breakdown"]["WIN_TP2"] == 1
    assert all_metrics["breakdown"]["LOSS_SL"] == 1

    # Filtro por setup_name=cont_pull: 2 trades (trade 1 + trade 3)
    cont_pull_metrics = j.get_performance(setup_name="cont_pull")
    assert cont_pull_metrics["total"] == 2
    assert cont_pull_metrics["wins"] == 1
    assert cont_pull_metrics["losses"] == 1

    # Filtro por direction=SHORT: só trade 3
    short_metrics = j.get_performance(direction="SHORT")
    assert short_metrics["total"] == 1
    assert short_metrics["losses"] == 1
    assert short_metrics["wins"] == 0

    # Filtro combinado setup_name + direction
    combo = j.get_performance(setup_name="cont_pull", direction="LONG")
    assert combo["total"] == 1
    assert combo["wins"] == 1


# ---------------------------------------------------------------------------
# 14. get_performance — janela de dias
# ---------------------------------------------------------------------------


def test_get_performance_days_window(tmp_journal):
    """Trades fora da janela de days NÃO são contabilizados."""
    j = TradeJournalV9(tmp_journal["db_path"])

    # Trade recente (hoje)
    _force_closed_trade(j, setup_name="cont_pull", status="WIN_TP1",
                        pnl_pct=1.0, days_ago=0)
    # Trade antigo (40 dias atrás)
    _force_closed_trade(j, setup_name="cont_pull", status="WIN_TP2",
                        pnl_pct=3.0, days_ago=40)

    # Default (30 dias): só o recente
    metrics_30 = j.get_performance()
    assert metrics_30["total"] == 1
    assert metrics_30["breakdown"]["WIN_TP1"] == 1
    assert metrics_30["breakdown"]["WIN_TP2"] == 0

    # Janela ampla (60 dias): ambos
    metrics_60 = j.get_performance(days=60)
    assert metrics_60["total"] == 2
    assert metrics_60["breakdown"]["WIN_TP1"] == 1
    assert metrics_60["breakdown"]["WIN_TP2"] == 1


# ---------------------------------------------------------------------------
# 15. get_performance_by_setup — split de confluências
# ---------------------------------------------------------------------------


def test_get_performance_by_setup_splits_confluences(tmp_journal):
    """Trade com setup_name='cont_pull+rev_zone' conta para AMBOS os setups."""
    j = TradeJournalV9(tmp_journal["db_path"])

    # Trade 1: só cont_pull — WIN_TP1
    _force_closed_trade(j, setup_name="cont_pull", status="WIN_TP1",
                        pnl_pct=1.0)
    # Trade 2: confluência cont_pull+rev_zone — WIN_TP3
    _force_closed_trade(j, setup_name="cont_pull+rev_zone",
                        status="WIN_TP3", pnl_pct=3.6)
    # Trade 3: só breaker — LOSS_SL
    _force_closed_trade(j, setup_name="breaker", status="LOSS_SL",
                        pnl_pct=-2.0)

    by_setup = j.get_performance_by_setup()

    # Chaves esperadas: cont_pull, rev_zone, breaker
    assert set(by_setup.keys()) == {"cont_pull", "rev_zone", "breaker"}

    # cont_pull: trades 1 e 2 (2 wins)
    assert by_setup["cont_pull"]["total"] == 2
    assert by_setup["cont_pull"]["wins"] == 2
    assert by_setup["cont_pull"]["breakdown"]["WIN_TP1"] == 1
    assert by_setup["cont_pull"]["breakdown"]["WIN_TP3"] == 1

    # rev_zone: trade 2 (1 win)
    assert by_setup["rev_zone"]["total"] == 1
    assert by_setup["rev_zone"]["wins"] == 1
    assert by_setup["rev_zone"]["breakdown"]["WIN_TP3"] == 1

    # breaker: trade 3 (1 loss)
    assert by_setup["breaker"]["total"] == 1
    assert by_setup["breaker"]["losses"] == 1
    assert by_setup["breaker"]["breakdown"]["LOSS_SL"] == 1

    # DB vazio → dict vazio
    j2 = TradeJournalV9(":memory:")
    assert j2.get_performance_by_setup() == {}


# ---------------------------------------------------------------------------
# Trailing SL history — sl_moved_to_be_at / sl_moved_to_tp1_at (v9.1.1)
# ---------------------------------------------------------------------------


def _read_sl_moves(db_path: str, trade_id: str) -> dict:
    """Lê os dois timestamps de trailing do trade."""
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT sl_moved_to_be_at, sl_moved_to_tp1_at, current_sl "
            "FROM trades WHERE id=?",
            (trade_id,),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def test_sl_moved_to_be_at_recorded(tmp_journal):
    """TP1 hit → sl_moved_to_be_at ISO 8601 preenchido, current_sl == entry."""
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    candles = [_make_candle(0, 100, 102.5, 99.8, 102.3)]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731
    j.check_open_trades(fetch_klines_fn=fetcher)

    moves = _read_sl_moves(tmp_journal["db_path"], trade_id)
    assert moves["sl_moved_to_be_at"] is not None
    # ISO 8601 parseável
    datetime.fromisoformat(moves["sl_moved_to_be_at"])
    assert moves["sl_moved_to_tp1_at"] is None
    assert moves["current_sl"] == pytest.approx(100.0)  # breakeven = entry


def test_sl_moved_to_tp1_at_recorded(tmp_journal):
    """TP1 + TP2 hit → ambos timestamps preenchidos, current_sl == tp1."""
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    # Um candle atinge TP1 e TP2 sequencialmente
    candles = [_make_candle(0, 100, 104.5, 99.8, 104.3)]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731
    j.check_open_trades(fetch_klines_fn=fetcher)

    moves = _read_sl_moves(tmp_journal["db_path"], trade_id)
    assert moves["sl_moved_to_be_at"] is not None
    assert moves["sl_moved_to_tp1_at"] is not None
    datetime.fromisoformat(moves["sl_moved_to_be_at"])
    datetime.fromisoformat(moves["sl_moved_to_tp1_at"])
    assert moves["current_sl"] == pytest.approx(102.0)  # = tp1_price


def test_sl_moved_at_is_idempotent(tmp_journal):
    """Re-chamar tracker após TP1 não sobrescreve sl_moved_to_be_at."""
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    candles = [_make_candle(0, 100, 102.5, 99.8, 102.3)]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731

    j.check_open_trades(fetch_klines_fn=fetcher)
    first = _read_sl_moves(tmp_journal["db_path"], trade_id)["sl_moved_to_be_at"]
    assert first is not None

    # Segunda chamada — guard SQL `WHERE col IS NULL` deve preservar valor.
    # Mesmo um _record_sl_move direto não deve sobrescrever.
    j._record_sl_move(trade_id, "sl_moved_to_be_at", "1999-01-01T00:00:00")
    second = _read_sl_moves(tmp_journal["db_path"], trade_id)["sl_moved_to_be_at"]
    assert second == first


def test_sl_moved_at_null_when_not_triggered(tmp_journal):
    """Trade sem TP hit → ambos timestamps NULL."""
    j = TradeJournalV9(tmp_journal["db_path"])
    decision = FakeSignalDecision()
    trade_id, ts_ms = _open_trade_and_get_ts_ms(j, decision)

    # Candles oscilando entre entry e SL — nenhum TP hit
    candles = [_make_candle(0, 100, 101.0, 99.0, 100.5)]
    klines = _klines_sequence(ts_ms, candles)
    fetcher = lambda symbol: klines  # noqa: E731
    j.check_open_trades(fetch_klines_fn=fetcher)

    moves = _read_sl_moves(tmp_journal["db_path"], trade_id)
    assert moves["sl_moved_to_be_at"] is None
    assert moves["sl_moved_to_tp1_at"] is None
