"""Testes do shadow DONCHIAN-A (shadow/donchian_a.py).

Valores fechados a mao; NAO dependem de pandas_ta (o topo do modulo e stdlib-
only). Cobrem: breakout long/short, nao-sinal, resolucao WIN/LOSS/EXPIRED,
empate SL/TP na mesma barra (pessimista => LOSS) e idempotencia (run_once 2x
=> 1 registro), alem de espacamento 48b/direcao e ciclo completo via run_once.

Espelho verificado contra a BANCADA:
  detector : backtest/stage1.py:52-71 (donchian_entries, canal estrito)
  bracket  : backtest/bracket.py:35-46 (first_touch, empate pessimista)
  ATR      : backtest/excursion.py:39-53 (Wilder semente)
"""

from __future__ import annotations

import logging
import sys
import types

from shadow import donchian_a as sd

BAR = sd.BAR_MS
QUIET = logging.getLogger("shadow_test")          # sem FileHandler: nao suja logs/
QUIET.addHandler(logging.NullHandler())


def _bar(ts, o, h, l, c, v=1.0):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


# --- primitivas puras --------------------------------------------------------
def test_wilder_atr_valores_fechados():
    # 15 barras, cada TR = 1.0 => ATR = media(14 TRs) = 1.0.
    candles = [_bar(i, 100.0, 100.5, 99.5, 100.0) for i in range(15)]
    assert abs(sd.wilder_atr(candles) - 1.0) < 1e-12


def test_wilder_atr_insuficiente():
    assert sd.wilder_atr([_bar(0, 1, 1, 1, 1)]) == 0.0


def test_donchian_breakout_long():
    closes = [100.0] * sd.N + [101.0]              # N anteriores=100, corrente>max
    assert sd.donchian_signal(closes) == "LONG"


def test_donchian_breakout_short():
    closes = [100.0] * sd.N + [99.0]
    assert sd.donchian_signal(closes) == "SHORT"


def test_donchian_nao_sinal_empate():
    closes = [100.0] * (sd.N + 1)                  # == max e == min => estrito, None
    assert sd.donchian_signal(closes) is None


def test_donchian_historico_curto():
    assert sd.donchian_signal([100.0] * sd.N) is None


# --- bracket (resolucao) -----------------------------------------------------
# LONG entry=101, ATR=1 => sl=99.5, tp=107, expiry_ts = entry_ts + 48*BAR.
_SL, _TP = 99.5, 107.0
_ENTRY_TS = 1_000_000_000_000
_EXPIRY = _ENTRY_TS + sd.H_BARS * BAR


def test_resolve_win():
    fwd = [_bar(_ENTRY_TS + BAR, 101, 107, 106, 106.5)]      # high toca tp, low ok
    assert sd.resolve_bracket("LONG", _SL, _TP, fwd, _EXPIRY) == (
        "WIN", _ENTRY_TS + BAR, _TP)


def test_resolve_loss():
    fwd = [_bar(_ENTRY_TS + BAR, 101, 100, 99.0, 99.5)]      # low toca sl
    assert sd.resolve_bracket("LONG", _SL, _TP, fwd, _EXPIRY) == (
        "LOSS", _ENTRY_TS + BAR, _SL)


def test_resolve_empate_sl_tp_pessimista():
    fwd = [_bar(_ENTRY_TS + BAR, 101, 107, 99.0, 103)]       # toca tp E sl na barra
    assert sd.resolve_bracket("LONG", _SL, _TP, fwd, _EXPIRY) == (
        "LOSS", _ENTRY_TS + BAR, _SL)                        # pessimista => STOP


def test_resolve_expired():
    # 48 barras sem toque; EXPIRED no fecho da 48a barra (expiry_ts).
    fwd = [_bar(_ENTRY_TS + k * BAR, 100, 100.4, 99.6, 100.0)
           for k in range(1, sd.H_BARS + 1)]
    assert sd.resolve_bracket("LONG", _SL, _TP, fwd, _EXPIRY) == (
        "EXPIRED", _EXPIRY, 100.0)


def test_resolve_ainda_open():
    fwd = [_bar(_ENTRY_TS + BAR, 100, 100.4, 99.6, 100.0)]   # sem toque, sem barra H
    assert sd.resolve_bracket("LONG", _SL, _TP, fwd, _EXPIRY) == (None, None, None)


def test_resolve_short_win():
    sl_s, tp_s = 102.0, 95.0                                  # SHORT entry=101
    fwd = [_bar(_ENTRY_TS + BAR, 100, 100, 95.0, 96.0)]       # low toca tp
    assert sd.resolve_bracket("SHORT", sl_s, tp_s, fwd, _EXPIRY) == (
        "WIN", _ENTRY_TS + BAR, tp_s)


def test_pnl_win_long():
    pnl, r = sd._pnl("LONG", 101.0, 107.0, 1.0)
    assert abs(pnl - (6.0 / 101.0 * 100.0)) < 1e-9
    assert abs(r - 4.0) < 1e-12                               # 6 ATR / 1.5 ATR


# --- run_once (integracao com fetch injetavel) -------------------------------
def _long_series(n_channel=sd.N + 1):
    """Canal plano (close=100) + barra de breakout (close=101). Cada TR=1.0."""
    bars = [_bar(k * BAR, 100.0, 100.5, 99.5, 100.0) for k in range(n_channel)]
    brk_ts = n_channel * BAR
    bars.append(_bar(brk_ts, 100.0, 101.0, 100.0, 101.0))    # TR=1.0, close novo max
    return bars, brk_ts


def test_run_once_abre_e_idempotente(tmp_path):
    db = str(tmp_path / "s.db")
    bars, brk_ts = _long_series()
    fetch = lambda sym: bars                                  # noqa: E731
    ok, falhas = sd.run_once(db, fetch_fn=fetch, symbols=["BTCUSDT"], log=QUIET)
    assert (ok, falhas) == (1, 0)
    conn = sd._connect(db)
    rows = conn.execute("SELECT * FROM shadow_trades").fetchall()
    assert len(rows) == 1
    r = rows[0]
    assert r["direction"] == "LONG" and r["status"] == "OPEN"
    assert r["bar_ts_entry"] == str(brk_ts)
    assert abs(r["entry_price"] - 101.0) < 1e-9
    assert abs(r["atr_at_entry"] - 1.0) < 1e-9
    assert abs(r["sl_price"] - 99.5) < 1e-9 and abs(r["tp_price"] - 107.0) < 1e-9
    assert r["expiry_bar_ts"] == str(brk_ts + sd.H_BARS * BAR)
    conn.close()
    # roda de novo na MESMA barra => nenhum duplicado (UNIQUE symbol,bar_ts).
    sd.run_once(db, fetch_fn=fetch, symbols=["BTCUSDT"], log=QUIET)
    conn = sd._connect(db)
    assert conn.execute("SELECT COUNT(*) FROM shadow_trades").fetchone()[0] == 1
    conn.close()


def test_run_once_ciclo_win_e_espacamento(tmp_path):
    db = str(tmp_path / "s.db")
    bars, brk_ts = _long_series()
    # run 1: abre a OPEN no breakout.
    sd.run_once(db, fetch_fn=lambda s: bars, symbols=["BTCUSDT"], log=QUIET)
    # run 2: uma barra forward que toca tp=107 => a OPEN resolve WIN; a nova barra
    # (close 106) volta a romper mas o espacamento 48b suprime => sem 2o trade.
    ext = bars + [_bar(brk_ts + BAR, 101, 107.0, 100.5, 106.0)]
    sd.run_once(db, fetch_fn=lambda s: ext, symbols=["BTCUSDT"], log=QUIET)
    conn = sd._connect(db)
    rows = conn.execute("SELECT * FROM shadow_trades").fetchall()
    assert len(rows) == 1                                    # espacamento: 1 so trade
    r = rows[0]
    assert r["status"] == "WIN"
    assert r["exit_ts"] == str(brk_ts + BAR)
    assert abs(r["exit_price"] - 107.0) < 1e-9
    assert abs(r["r_multiple"] - 4.0) < 1e-9
    conn.close()


def test_run_once_falha_total_isolada(tmp_path):
    # Todos os simbolos falham => notify_error chamado 1x; sem excecao propagada.
    # telegram real puxa numpy (fora do sandbox) => stub via sys.modules, mantendo
    # o teste independente de pandas_ta/numpy.
    db = str(tmp_path / "s.db")
    chamado = {"n": 0}

    def _boom(sym):
        raise RuntimeError("sem dados")

    fake = types.ModuleType("telegram")
    fake.notify_error = (
        lambda *a, **k: chamado.__setitem__("n", chamado["n"] + 1) or True)
    old = sys.modules.get("telegram")
    sys.modules["telegram"] = fake
    try:
        ok, falhas = sd.run_once(db, fetch_fn=_boom,
                                 symbols=["BTCUSDT", "ETHUSDT"], log=QUIET)
    finally:
        if old is not None:
            sys.modules["telegram"] = old
        else:
            sys.modules.pop("telegram", None)
    assert (ok, falhas) == (0, 2)
    assert chamado["n"] == 1
