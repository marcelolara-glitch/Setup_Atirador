# tests/test_aio_graduado.py — sandbox, 16/11/2026: AIO graduado (TradeIQ)
import random

import pytest

from backtest import aio_graduado as ag

T0 = 1_704_067_200_000                 # 2024-01-01 00:00 UTC
H4 = ag.BAR_4H_MS


def _velas(n=900, seed=5, t0=T0):
    rnd = random.Random(seed)
    p, out = 100.0, []
    for k in range(n):
        o = p
        p = max(1.0, p * (1 + rnd.gauss(0, 0.02)))
        out.append({"ts": t0 + k * H4, "open": o, "close": p,
                    "high": max(o, p) * (1 + abs(rnd.gauss(0, 0.004))),
                    "low": min(o, p) * (1 - abs(rnd.gauss(0, 0.004))),
                    "volume": rnd.uniform(50, 200)})
    return out


# ---- 1. motor do sinal, fechado a mao --------------------------------------
CLOSES = [10, 11, 12, 10.5, 12.5, 8, 7, 12, 13, 9, 8, 14]


def test_linha_e_viradas_a_mao():
    lin = ag.linha_trailing(CLOSES, [3.0] * len(CLOSES))
    # t0 ant=0 -> 7 | 8 | 9 | t3 10.5: max(9, 7.5)=9 (sens 3 nao vira) | 9.5 |
    # t5 8 < 9.5 vira: 11 | min(11,10)=10 | t7 12 vira: 9 | 10 | t9 9 vira: 12
    # | min(12,11)=11 | t11 14 vira: 11
    assert lin == [7, 8, 9, 9, 9.5, 11, 10, 9, 10, 12, 11, 11]
    assert ag.viradas(CLOSES, lin) == [(5, False), (7, True), (9, False),
                                       (11, True)]


def test_mutacao_sensibilidade_muda_viradas():
    v3 = ag.viradas(CLOSES, ag.linha_trailing(CLOSES, [3.0] * len(CLOSES)))
    v1 = ag.viradas(CLOSES, ag.linha_trailing(CLOSES, [1.0] * len(CLOSES)))
    assert v1[:2] == [(3, False), (4, True)]      # o recuo 12 -> 10,5 vira
    assert v1 != v3


def test_linha_none_sem_atr_e_sem_virada_na_primeira():
    lin = ag.linha_trailing([5, 6, 7, 2], [None, None, 1.0, 1.0])
    assert lin[:2] == [None, None]
    assert ag.viradas([5, 6, 7, 2], lin) == [(3, False)]


def test_grau():
    assert ag.grau(False, True, True) == 0
    assert ag.grau(True, False, True) == 0
    assert ag.grau(True, True, False) == 1
    assert ag.grau(True, True, True) == 2


def test_hold_ate_contraria_bruta():
    vs = [{"i": 5, "long": True}, {"i": 8, "long": False},
          {"i": 9, "long": False}, {"i": 15, "long": True}]
    assert ag.holds(vs, 40) == [(3, False), (7, False), (6, False),
                                (25, True)]


# ---- 2. invariancia a prefixo + mutacoes ----------------------------------
def _assinatura(sg, i):
    vs = [(v["i"], v["long"], v["grau"], v["vol_rel"], v["rib"], v["htf"])
          for v in sg["viradas"] if v["i"] <= i]
    return (sg["linha"][:i + 1], sg["vol_rel"][:i + 1], sg["rib"][:i + 1],
            sg["htf"][:i + 1], vs)


CORTES = (130, 361, 362, 500, 727, 899)    # 362 = 08:00, corta o dia no meio


def _prefixo_ok(c):
    full = ag.serie_sinal(c)
    return all(_assinatura(full, i) == _assinatura(ag.serie_sinal(c[:i + 1]), i)
               for i in CORTES)


def test_invariancia_a_prefixo():
    c = _velas()
    assert ag.serie_sinal(c)["viradas"]          # ha viradas para comparar
    assert _prefixo_ok(c)


def test_mutacao_diario_le_dia_corrente_reprova(monkeypatch):
    monkeypatch.setattr(ag, "DIA_LIDO", 0)
    assert not _prefixo_ok(_velas())


def _vol_rel_com_barra(vols, jan=20):
    out = [None] * len(vols)
    for i in range(jan - 1, len(vols)):
        m = sum(vols[i - jan + 1:i + 1]) / jan
        out[i] = vols[i] / m if m > 0 else None
    return out


def test_volume_media_das_anteriores_valor_fechado():
    # 20 barras de 100 e a 21a com 120: 120/100 = 1,2 exato -> passa
    vr = ag.vol_rel([100.0] * 20 + [120.0], 20)
    assert vr[20] == 1.2 and vr[20] >= ag.VOL_MIN


def test_mutacao_volume_inclui_barra_reprova(monkeypatch):
    # media com a barra do sinal e causal (prefixo nao pega); reprova pelo
    # valor fechado a mao: (19x100+120)/20 = 101 -> 1,188 < 1,2
    monkeypatch.setattr(ag, "vol_rel", _vol_rel_com_barra)
    c = _velas(60)
    for k, x in enumerate(c):
        x["volume"] = 120.0 if k == 40 else 100.0
    sg = ag.serie_sinal(c)
    assert sg["vol_rel"][40] < ag.VOL_MIN


# ---- 3. diario D-1 ----------------------------------------------------------
def test_diario_le_d_menos_1():
    c = []
    for d in range(60):
        px = 100.0 + d if d < 58 else 50.0       # dia 58 desaba
        for h in range(6):
            ts = T0 + d * ag.DIA_MS + h * H4
            c.append({"ts": ts, "open": px, "high": px, "low": px,
                      "close": px, "volume": 1.0})
    htf = ag.htf_dirs(c)
    i_20h = 58 * 6 + 5                           # dia D=58, 20:00
    i_00h = 59 * 6                               # dia D+1, 00:00
    assert c[i_20h]["ts"] % ag.DIA_MS == 20 * 3_600_000
    assert htf[i_20h] == 1                       # le D-1 = dia 57 (subindo)
    assert htf[i_00h] == -1                      # le D = dia 58 (desabou)
    assert htf[49 * 6] is None                   # D-1 = dia 48: EMA50 sem semente
    assert htf[50 * 6] == 1                      # D-1 = dia 49: 1o com EMA50


# ---- 4. placebo por retirada ------------------------------------------------
def _cont():
    return {"excl_borda": 0, "excl_hold": 0}


@pytest.mark.parametrize("cel", ["V", "F", "G1", "G2"])
def test_celula_e_subconjunto_do_r0(cel):
    c = _velas(1500, seed=11)
    r0 = ag.trades_simbolo(c, "X", _cont(), "R0")
    sub = ag.trades_simbolo(c, "X", _cont(), cel)
    assert sub and len(sub) < len(r0)
    chave = lambda t: (t[0], t[2], t[7]["hold"], t[3], t[4], t[5], t[6])  # noqa
    assert [chave(t) for t in sub] == [chave(t) for t in ag.filtra(r0, cel)]


def test_placebo_retira_mesma_quantidade_por_lado():
    c = _velas(1500, seed=11)
    G = ag.serie_sinal(c)["viradas"]
    for k, v in enumerate(G):
        v["g"] = k
    for cel in ("V", "G2"):
        vet = ag.vetos_placebo(G, cel, 3)
        for lado in (True, False):
            n_cel = sum(1 for v in G if v["long"] is lado
                        and not ag.passa(cel, v))
            assert sum(1 for g in vet if G[g]["long"] is lado) == n_cel
