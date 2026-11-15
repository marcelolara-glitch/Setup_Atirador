# tests/test_kis_meta.py — sandbox, etapa 4 (briefing 15/11/2026)
import math
import random

import pytest

from backtest import kis_meta as km
from backtest.kis_horizonte import entradas

BAR = 14_400_000


def _serie(n=1200, semente=7, base=100.0):
    rng = random.Random(semente)
    out, c = [], base
    for i in range(n):
        o = c
        c = max(1.0, o * (1 + 0.012 * math.sin(i / 60.0) + rng.gauss(0, 0.01)))
        h = max(o, c) * (1 + abs(rng.gauss(0, 0.004)))
        lo = min(o, c) * (1 - abs(rng.gauss(0, 0.004)))
        out.append({"ts": 1_700_000_000_000 + i * BAR, "open": o, "high": h,
                    "low": lo, "close": c, "volume": 1000 + rng.random() * 500})
    return out


def _espelho(cndl, m=400.0):
    return [{"ts": c["ts"], "open": m - c["open"], "high": m - c["low"],
             "low": m - c["high"], "close": m - c["close"],
             "volume": c["volume"]} for c in cndl]


def _iguais(a, b, tol=1e-12):
    if a is None or b is None:
        return a is b
    if isinstance(a, bool):
        return a is b
    return abs(a - b) <= tol * max(1.0, abs(a))


def _prefixo_ok(cndl, idxs):
    cheio = km.series_barra(cndl)
    for i in idxs:
        corte = km.series_barra(cndl[:i + 1])
        for k in cheio:
            if not _iguais(cheio[k][i], corte[k][i]):
                return False
    return True


IDXS = list(range(0, 1200, 41)) + [150, 151, 399, 1198]


# 1. invariancia a prefixo + mutacao obrigatoria -----------------------------
def test_invariancia_a_prefixo():
    assert _prefixo_ok(_serie(), IDXS)


def test_mutacao_rsi_le_i_mais_1_reprova(monkeypatch):
    original = km.rsi_wilder

    def rsi_vazado(closes, n=km.RSI_LEN):        # valor em i = RSI de i+1
        r = original(closes, n)
        return r[1:] + [None]
    monkeypatch.setattr(km, "rsi_wilder", rsi_vazado)
    assert not _prefixo_ok(_serie(), IDXS)


# 2. orientacao -------------------------------------------------------------
def _trades_de(cndl, long_quer):
    return [(ev["bar_ts"], "X", long_quer, [0.0] * 7, 0, False, False)
            for ev in entradas(cndl, *km.PAR)]


def test_orientacao_short_espelhado_igual_ao_long():
    a = _serie(semente=3)
    b = _espelho(a)
    # mesmos ts de entrada nas duas series (alvo espelha); forca o lado
    ta, tb = _trades_de(a, True), _trades_de(b, False)
    assert [t[0] for t in ta] == [t[0] for t in tb] and ta
    fa, ampa, _, _ = km.extrai_simbolo(a, ta)
    fb, ampb, _, _ = km.extrai_simbolo(b, tb)
    km.junta(ta, fa, {k: [int(v), 1] for k, v in ampa.items()}, {})
    km.junta(tb, fb, {k: [int(v), 1] for k, v in ampb.items()}, {})
    n_cmp = 0
    for x, y in zip(fa, fb):
        for k in ("rsi14_fav", "incl21_fav", "amplitude_fav"):
            assert (x[k] is None) == (y[k] is None)
            if x[k] is not None:
                assert abs(x[k] - y[k]) <= 1e-9, k
                n_cmp += 1
    assert n_cmp > 10


# 3. quintis de H1 ----------------------------------------------------------
def test_cortes_de_h1_nao_dependem_de_h2():
    rng = random.Random(1)
    rows = [{"metade": "H1" if j < 300 else "H2", "x": rng.random()}
            for j in range(600)]
    antes = km.cortes(rows, "x")
    for r in rows:
        if r["metade"] == "H2":
            r["x"] = rng.random() * 1e6 - 5e5
    assert km.cortes(rows, "x") == antes and antes[0] == "quintis"
    assert len(antes[1]) == 4


def test_poucos_valores_viram_classes():
    rows = [{"metade": "H1", "b": v} for v in (-1, 0, 1, 1, 0)]
    assert km.cortes(rows, "b") == ("valores", [-1, 0, 1])


# 4. ret_ant ----------------------------------------------------------------
def test_ret_ant_bate_com_o_trade_anterior_do_token():
    cndl = _serie(semente=11)
    evs = entradas(cndl, *km.PAR)
    assert len(evs) >= 6
    # simula o portao vetando uma inversao sim, outra nao
    trades = [(ev["bar_ts"], "X", ev["direction"] == "LONG",
               [10.0 * j, 1.0, -3.0 * j, 0, 0, 0, 0], 0, False, False)
              for j, ev in enumerate(evs) if j % 2 == 0]
    fs, _, _, viol = km.extrai_simbolo(cndl, trades)
    assert viol == 0
    assert fs[0]["ret_ant_e0"] is None and fs[0]["ret_ant_e13"] is None
    for k in range(1, len(trades)):
        assert fs[k]["ret_ant_e0"] == trades[k - 1][3][0]
        assert fs[k]["ret_ant_e13"] == trades[k - 1][3][2]
    # barras_desde_inv conta as inversoes vetadas: e o vizinho em `entradas`
    pos = {c["ts"]: i for i, c in enumerate(cndl)}
    for j, ev in enumerate(evs):
        if j % 2 == 0 and j > 0:
            f = fs[j // 2]
            assert f["barras_desde_inv"] == pos[ev["bar_ts"]] - \
                pos[evs[j - 1]["bar_ts"]]
    assert fs[0]["barras_desde_inv"] is None


def test_ret_ant_viola_vira_na_e_conta():
    cndl = _serie(semente=11)
    evs = entradas(cndl, *km.PAR)
    t = [(ev["bar_ts"], "X", True, [1.0] * 7, 0, False, False)
         for ev in evs[:3]]
    # trade "anterior" cujo hold atravessaria a inversao atual: inverte a ordem
    fs, _, _, viol = km.extrai_simbolo(cndl, [t[2], t[0]])
    assert viol == 1 and fs[1]["ret_ant_e0"] is None


# extras: placebo deterministico e spread ------------------------------------
def test_placebo_preserva_valores_por_direcao_e_e_deterministico():
    rng = random.Random(2)
    h2 = [{"metade": "H2", "mes": "2026-01", "direcao": "LONG" if j % 3 else
           "SHORT", "x": rng.random(), "r": rng.gauss(0, 50)}
          for j in range(200)]
    modo, cs = "quintis", [0.2, 0.4, 0.6, 0.8]
    a = km.placebos(h2, "x", "r", modo, cs)
    assert a == km.placebos(h2, "x", "r", modo, cs) and len(a) == 20


def test_resumo_indicador_sinal_real_bate_placebo_e_ruido_nao():
    rng = random.Random(5)
    rows = []
    for j in range(800):
        r = rng.gauss(0, 100)
        rows.append({"metade": "H1" if j < 400 else "H2",
                     "mes": f"2025-{1 + j % 12:02d}",
                     "direcao": "LONG" if j % 2 else "SHORT",
                     "real": r + rng.gauss(0, 5), "ruido": rng.random(),
                     "r": r})
    z = km.resumo_indicador(rows, "real", "r", "2025-01")
    assert z["mesmo_sinal"] == "s" and z["spread_H2"] > 0
    assert z["placebo_pct"] == 100.0
    zr = km.resumo_indicador(rows, "ruido", "r", "2025-01")
    assert zr["placebo_pct"] is not None and zr["n_placebo_validos"] == 20
