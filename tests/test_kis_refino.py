# tests/test_kis_refino.py — sandbox, briefing v3 de 22/11/2026. Testes de
# CORRECAO do codigo (paridade, regras de saida, limiares do T5). Nenhum
# criterio de decisao mora aqui.
import random

import pytest

from backtest import holdout_2026 as h
from backtest import kis_refino as kr
from backtest import kis_saida as ks
from backtest.candle_store import connect, read_candles, upsert_candles
from backtest.keepitsimple import _ema, states
from backtest.kis_regime import _atr_series

BAR = ks.BAR_4H_MS
SYMS = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]


def _store(path, syms, ate_iso, seed=11):
    conn = connect(path)
    rnd = random.Random(seed)
    for s in syms:
        p, vs, ts = 100.0, [], ks._ms("2024-03-01")
        while ts < ks._ms(ate_iso):
            o = p
            p = max(1.0, p * (1 + rnd.gauss(0.0003, 0.02)))
            vs.append({"ts": ts, "open": o, "close": p,
                       "high": max(o, p) * (1 + abs(rnd.gauss(0, .006))),
                       "low": min(o, p) * (1 - abs(rnd.gauss(0, .006))),
                       "volume": 1000.0})
            ts += BAR
        upsert_candles(conn, s, "4h", vs)
    conn.close()


# ---- 1. BASE reproduz kis_saida (E1-3) e holdout_2026 (KIS_E1-3) ------------
def test_base_256_reproduz_kis_saida_e_holdout(tmp_path):
    db = tmp_path / "c.db"
    _store(db, SYMS, "2026-11-20")
    teto = ks._ms(ks.TETO_ISO)
    conn = ks.conecta_com_teto(str(db), teto)
    ref = ks.run(conn, SYMS, "4h", None, teto - 1,
                 pares=((34, 89),))["por_par"][(34, 89)]
    sb = {s: read_candles(conn, s, "4h") for s in SYMS}
    conn.close()
    k3 = ks.NOMES.index("E1-3")
    meus, sem_teto = {}, {}
    for s in SYMS:
        r = kr.trades_simbolo(sb[s], s, "sandbox", kr.novo_cont())
        meus.update({(t["ts"], s): t for t in r["BASE_256"]})
        sem_teto.update({(t["ts"], s): t for t in r["BASE"]})
    assert len(ref) > 100 and len(ref) == len(meus)
    for t in ref:
        x = meus[(t[0], t[1])]
        assert x["ret6"] == t[3][k3]                 # identico
        assert x["long"] == t[2]
        assert (x["execs"][-1][3] == "stop") == bool(t[4] >> k3 & 1)
        y = sem_teto[(t[0], t[1])]                  # sem teto: igual se nao
        if x["execs"][-1][3] != "teto":             # foi cortado em 256
            assert y["ret6"] == x["ret6"]
    # periodo novo: igual ao holdout_2026
    conn = ks.conecta_com_teto(str(db), ks._ms(h.END_ISO))
    for s in SYMS:
        cn = read_candles(conn, s, "4h")
        ref = {t["ts"]: t["ret6"] for t in h.trades_kis(cn, s, h.novo_cont())
               if t["setup"] == "KIS_E1-3"}
        mine = {t["ts"]: t["ret6"] for t in
                kr.trades_simbolo(cn, s, "novo", kr.novo_cont())["BASE_256"]}
        assert ref and mine == ref
    conn.close()


# ---- velas manuais para o motor de saida (entry 100, ATR 1) -----------------
def _v(o, hi, lo, c, ts=0):
    return {"ts": ts, "open": o, "high": hi, "low": lo, "close": c}


def _serie(barras):
    out = [_v(100, 100, 100, 100)] + [_v(*b) for b in barras]
    for k, c in enumerate(out):
        c["ts"] = k * BAR
    return out


# ---- 2. T1 ------------------------------------------------------------------
def test_t1_sai_na_barra_n_sem_progresso():
    cn = _serie([(100, 100.5, 99.5, 100.2), (100.2, 100.9, 99.8, 100.4),
                 (100.4, 100.8, 99.6, 100.1), (100.1, 103, 99, 102)] * 2)
    ex, mfe, _ = kr.simula(cn, 0, 8, 100.0, 1.0, 1.0, tempo_n=3)
    assert ex == [(3, pytest.approx(0.1), 100.1, 1.0, "tempo")]
    assert mfe < 1.0


def test_t1_com_progresso_segue_ate_a_inversao():
    cn = _serie([(100, 100.5, 99.5, 100.2), (100.2, 101.2, 99.8, 100.4),
                 (100.4, 100.8, 99.6, 100.1), (100.1, 103, 99, 102)] * 2)
    ex, _, _ = kr.simula(cn, 0, 8, 100.0, 1.0, 1.0, tempo_n=3)
    assert len(ex) == 1 and ex[0][0] == 8 and ex[0][4] == "inversao"


# ---- 3. T3 ------------------------------------------------------------------
def test_t3_parciais_nos_niveis_e_resto_na_inversao():
    cn = _serie([(100, 104.5, 99.5, 103), (108.5, 109, 108, 108.6),
                 (108.6, 108.7, 101.5, 102)])
    ex, _, _ = kr.simula(cn, 0, 3, 100.0, 1.0, 1.0,
                         parciais=kr.T3_PARCIAIS)
    assert ex[0] == (1, 4.0, 104.0, 0.5, "parcial1")
    assert ex[1] == (2, 8.5, 108.5, 0.25, "parcial2")   # abriu alem: abertura
    assert ex[2] == (3, pytest.approx(2.0), 102, 0.25, "inversao")
    t = kr._trade("T3", "X", cn, 0, True, 100.0, 1.0, (ex, 0, 0))
    assert t["ret6"] == pytest.approx((0.5 * 4 + 0.25 * 8.5 + 0.25 * 2.0)
                                      * 1.0 / 100.0 * 1e4 - 6.0)


def test_t3_stop_e_alvo_na_mesma_barra_stop_primeiro():
    cn = _serie([(100, 104.5, 96.9, 103), (103, 109, 102, 108)])
    ex, _, mae = kr.simula(cn, 0, 2, 100.0, 1.0, 1.0,
                           parciais=kr.T3_PARCIAIS)
    assert ex == [(1, -3.0, 97.0, 1.0, "stop")]
    assert mae == 3.0


def test_t3_stop_depois_da_parcial_leva_so_o_resto():
    cn = _serie([(100, 104.5, 99.5, 103), (103, 103.5, 96.5, 97)])
    ex, _, _ = kr.simula(cn, 0, 2, 100.0, 1.0, 1.0,
                         parciais=kr.T3_PARCIAIS)
    assert [x[3] for x in ex] == [0.5, 0.5] and ex[1][4] == "stop"


# ---- 4. T4 ------------------------------------------------------------------
def test_t4_terceira_entrada_simultanea_no_mesmo_lado_e_pulada():
    ordem = {"AUSDT": 0, "BUSDT": 1, "CUSDT": 2, "DUSDT": 3}

    def tr(sym, lg, ts=1000, fim=9000):
        return {"sym": sym, "long": lg, "ts": ts,
                "execs": ((fim, 1.0, 1.0, "inversao"),)}
    base = [tr("CUSDT", True), tr("AUSDT", True), tr("BUSDT", True),
            tr("DUSDT", False), tr("CUSDT", True, ts=9000)]
    out = kr.teto_lado(base, 2, ordem, "T4_2")
    assert [(t["sym"], t["ts"]) for t in out] == [
        ("AUSDT", 1000), ("BUSDT", 1000), ("DUSDT", 1000), ("CUSDT", 9000)]
    assert all(t["cel"] == "T4_2" for t in out)


# ---- 5. T5: gatilho = estado, sem olhar a barra t ----------------------------
def _closes(n=300, seed=3):
    rnd, p, out = random.Random(seed), 100.0, []
    for _ in range(n):
        p *= 1 + rnd.gauss(0.0, 0.015)
        out.append(p)
    return out


def _cndl(closes):
    return [{"ts": k * BAR, "open": c, "high": c * 1.01, "low": c * 0.99,
             "close": c} for k, c in enumerate(closes)]


def _gat_mutante(cndl, closes=None, fast=34, slow=89):
    """MUTACAO: usa as EMAs da barra t (inclui o close de t) no lugar de t-1."""
    closes = [c["close"] for c in cndl]
    ef, es, st = _ema(closes, fast), _ema(closes, slow), states(closes, fast,
                                                                slow)
    a, b = 2.0 / (fast + 1.0), 2.0 / (slow + 1.0)
    gL, gS = [None] * len(closes), [None] * len(closes)
    for t in range(1, len(closes)):
        f, s = ef[t], es[t]
        if f is None or s is None:
            continue
        l2 = ((1.0 - b) * s - (1.0 - a) * f) / (a - b)
        if st[t - 1] == "VERDE":
            gL[t] = max(f, l2)
        elif st[t - 1] == "VERM":
            gS[t] = min(f, l2)
    return gL, gS, st


def _prefixo_ok(fn) -> bool:
    """O gatilho da barra t nao pode mudar quando so o close de t muda."""
    cl = _closes()
    gL, gS, _ = fn(_cndl(cl))
    for t in range(120, len(cl)):
        g = gL[t] if gL[t] is not None else gS[t]
        if g is None:
            continue
        for fator in (0.95, 1.05):
            cl2 = cl[:t] + [cl[t] * fator]
            gL2, gS2, _ = fn(_cndl(cl2))
            g2 = gL2[t] if gL[t] is not None else gS2[t]
            if g2 != g:
                return False
    return True


def test_t5_prefixo_e_mutacao():
    assert _prefixo_ok(kr.gatilhos)
    assert not _prefixo_ok(_gat_mutante)


def test_t5_gatilho_reproduz_o_estado_um_tick():
    cl = _closes()
    gL, gS, _ = kr.gatilhos(_cndl(cl))
    n_l = n_s = 0
    for t in range(120, len(cl)):
        for g, alvo in ((gL[t], "VERDE"), (gS[t], "VERM")):
            if g is None:
                continue
            tick = g * 1e-9
            dentro = g + tick if alvo == "VERDE" else g - tick
            fora = g - tick if alvo == "VERDE" else g + tick
            assert states(cl[:t] + [dentro], 34, 89)[t] == alvo
            assert states(cl[:t] + [fora], 34, 89)[t] != alvo
            n_l += alvo == "VERDE"
            n_s += alvo == "VERM"
    assert n_l > 10 and n_s > 10


def test_t5_limiar_do_portao_reproduz_a_inclinacao_com_atr_de_t_menos_1():
    cl = _closes()
    cn = _cndl(cl)
    pL, pS, _ = kr.limiares_portao(cn)
    atr = _atr_series(cn)
    for t in range(120, 200):
        for p_lim, lado in ((pL[t], 1), (pS[t], -1)):
            for d, passa in ((1e-9, True), (-1e-9, False)):
                p = p_lim * (1 + lado * d)
                e = _ema(cl[:t] + [p], 21)
                incl = (e[t] - e[t - 5]) / 5 / atr[t - 1]
                assert (lado * incl >= 0.02) is passa


def test_t5s_sai_no_gatilho_ou_na_abertura():
    cn = _serie([(100, 101, 99, 100.5), (100.2, 100.4, 98.5, 99)])
    ex, _, _ = kr.simula(cn, 0, 2, 100.0, 1.0, 1.0,
                         gat=lambda j: 99.5 if j == 2 else None)
    assert ex == [(2, -0.5, 99.5, 1.0, "inversao")]
    cn[2]["open"] = 99.2                      # abriu alem do gatilho
    ex, _, _ = kr.simula(cn, 0, 2, 100.0, 1.0, 1.0,
                         gat=lambda j: 99.5 if j == 2 else None)
    assert ex[0][2] == 99.2 and ex[0][1] == pytest.approx(-0.8)


def test_t2_amplitude_filtra_por_lado():
    base = [{"ts": 1, "sym": "A", "long": True, "cel": "BASE"},
            {"ts": 1, "sym": "B", "long": False, "cel": "BASE"},
            {"ts": 2, "sym": "A", "long": True, "cel": "BASE"},
            {"ts": 3, "sym": "A", "long": True, "cel": "BASE"}]
    amp = {1: [6, 10], 2: [4, 10]}            # ts 3 sem amplitude (NA)
    out, na = kr.filtro_t2(base, amp)
    assert [(t["ts"], t["sym"]) for t in out] == [(1, "A")]
    assert na == 1 and out[0]["cel"] == "T2"


def test_t5e_alvo_dos_toques_concorda_em_toda_inversao_do_fechamento():
    from backtest.kis_horizonte import entradas
    rnd, p, cn = random.Random(5), 100.0, []
    for k in range(6000):
        o = p
        p *= 1 + rnd.gauss(0.0, 0.02)
        cn.append({"ts": k * BAR, "open": o, "close": p,
                   "high": max(o, p) * (1 + abs(rnd.gauss(0, .006))),
                   "low": min(o, p) * (1 - abs(rnd.gauss(0, .006)))})
    gL, gS, _ = kr.gatilhos(cn)
    alvo, fl = [0] * len(cn), dict(kr.viradas_t5e(cn, gL, gS))
    for t in range(1, len(cn)):
        alvo[t] = fl.get(t, alvo[t - 1])
    evs = entradas(cn, 34, 89)
    assert len(evs) > 20 and len(fl) >= len(evs)
    for ev in evs:
        assert alvo[ev["bar_ts"] // BAR] == (1 if ev["direction"] == "LONG"
                                             else -1)
