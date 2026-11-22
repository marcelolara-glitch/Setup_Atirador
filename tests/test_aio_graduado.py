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


# ---- 5. entrada tardia (v2, 21/11/2026) -------------------------------------
N = 200
IK, J = 60, 90                       # virada de compra em 60, contraria em 90


def _planas(n=N, queda_em=None):
    """Velas com amplitude fixa; `queda_em` derruba o preco 20% dali em diante."""
    out = []
    for k in range(n):
        px = 100.0 if queda_em is None or k < queda_em else 80.0
        out.append({"ts": T0 + k * H4, "open": px, "close": px,
                    "high": px + 1.0, "low": px - 1.0, "volume": 100.0})
    return out


def _sinal(c, rib_de=None, vol_em=(), htf=1, vol_virada=True, rib_virada=1):
    """Sinal montado a mao: compra em IK, venda em J. `rib_de` = barra a partir
    da qual o ribbon fica a favor (None = nunca, fora a barra da virada)."""
    n = len(c)
    vr = [1.0] * n
    rib = [-1] * n
    rib[IK] = rib_virada
    if rib_de is not None:
        for t in range(rib_de, n):
            rib[t] = 1
    for t in vol_em:
        vr[t] = 1.5
    if vol_virada:
        vr[IK] = 1.5
    return ag.monta_sinal(c, [(IK, True), (J, False)], vr, rib, [htf] * n)


def _tr(c, sg, cel):
    return ag.trades_simbolo(c, "X", _cont(), cel, sg)


def test_tardia_igual_a_base_quando_passa_na_virada():
    c = _planas()
    sg = _sinal(c)                                   # volume e ribbon na virada
    g1 = [t for t in _tr(c, sg, "G1") if t[2]]
    assert len(g1) == 1 and g1[0][7]["hold"] == J - IK
    for cel in ag.TARDIAS:
        t = [x for x in _tr(c, sg, cel) if x[2]]
        assert len(t) == 1 and t[0][7]["atraso"] == 0
        assert (t[0][0], t[0][7]["hold"], t[0][3]) == \
            (g1[0][0], g1[0][7]["hold"], g1[0][3])


def test_tardia_entra_quando_o_ribbon_vira_e_hold_encolhe():
    c = _planas()
    sg = _sinal(c, rib_de=IK + 3, rib_virada=-1)     # volume ok, ribbon contra
    assert not [t for t in _tr(c, sg, "G1") if t[2]]
    (t,) = [x for x in _tr(c, sg, "G1T_sem") if x[2]]
    assert t[0] == c[IK + 3]["ts"] and t[7]["e"] == IK + 3
    assert t[7]["atraso"] == 3 and t[7]["hold"] == J - (IK + 3)


def test_tardia_volume_atrasa_a_entrada():
    c = _planas()
    sg = _sinal(c, rib_de=IK + 3, vol_em=(IK + 5,), rib_virada=-1)
    v = sg["viradas"][0]
    assert v["ent"]["G1T_sem"] == IK + 3 and v["ent"]["G1T_vol"] == IK + 5
    (ts,) = [x for x in _tr(c, sg, "G1T_sem") if x[2]]
    (tv,) = [x for x in _tr(c, sg, "G1T_vol") if x[2]]
    assert ts[7]["hold"] == J - IK - 3 and tv[7]["hold"] == J - IK - 5


def test_tardia_g2_exige_diario_na_barra():
    c = _planas()
    sg = _sinal(c, rib_de=IK + 3, htf=-1, rib_virada=-1)
    v = sg["viradas"][0]
    assert v["ent"]["G1T_sem"] == IK + 3 and v["ent"]["G2T_sem"] is None
    sg["htf"][IK + 7] = 1
    assert ag.entradas_tardias(sg, "G2T_sem")[0] == IK + 7
    sg["htf"][IK + 7] = None                         # None = nao passa na barra
    assert ag.entradas_tardias(sg, "G2T_sem")[0] is None


def test_tardia_uma_entrada_por_movimento_mesmo_com_stop():
    c = _planas(queda_em=IK + 6)                     # desaba 3 barras apos entrar
    sg = _sinal(c, rib_de=IK + 3, rib_virada=-1)     # condicao segue verdadeira
    assert all(ag.cond_tardia("G1T_sem", sg, t, 1) for t in range(IK + 3, J))
    longs = [x for x in _tr(c, sg, "G1T_sem") if x[2]]
    assert len(longs) == 1 and longs[0][4] == 2      # um trade so, parou no S2
    assert longs[0][3][1] < 0 and longs[0][7]["e"] == IK + 3


def test_tardia_sem_barra_valida_nao_ha_trade():
    c = _planas()
    sg = _sinal(c, rib_virada=-1)                    # ribbon nunca a favor
    assert all(e is None for e in sg["viradas"][0]["ent"].values())
    for cel in ag.TARDIAS:
        assert not [x for x in _tr(c, sg, cel) if x[2]]


def test_tardia_condicao_so_na_barra_da_virada_contraria():
    c = _planas()
    sg = _sinal(c, rib_virada=-1)
    sg["rib"][J] = 1                                 # so passa em j
    assert ag.entradas_tardias(sg, "G1T_sem")[0] is None
    sg["rib"][J - 1] = 1                             # controle: j - 1 entra
    assert ag.entradas_tardias(sg, "G1T_sem")[0] == J - 1


def test_tardia_busca_para_na_virada_seguinte_de_mesma_direcao():
    c = _planas()
    vr, rib = [1.5] * N, [-1] * N
    for t in range(75, N):
        rib[t] = 1
    sg = ag.monta_sinal(c, [(IK, True), (70, True), (J, False)], vr, rib,
                        [1] * N)
    e = ag.entradas_tardias(sg, "G1T_sem")
    assert e[0] is None and e[1] == 75               # nao duplica a barra 75
    assert ag.seguidas_mesma_direcao(sg["viradas"]) == 1


def _entradas(sg, n):
    return [(v["i"], cel, e) for v in sg["viradas"]
            for cel, e in sorted(v["ent"].items()) if e is not None and e <= n]


def _prefixo_tardio_ok(c):
    full = ag.serie_sinal(c)
    tardias = sorted({e for v in full["viradas"] for e in v["ent"].values()
                      if e is not None and e > v["i"]})
    assert len(tardias) > 20                         # ha entradas tardias
    return all(_entradas(full, n) == _entradas(ag.serie_sinal(c[:n + 1]), n)
               for n in tardias + list(CORTES))


def test_tardia_invariancia_a_prefixo():
    assert _prefixo_tardio_ok(_velas(1500, seed=11))


def test_mutacao_tardia_le_ribbon_da_barra_seguinte_reprova(monkeypatch):
    monkeypatch.setattr(ag, "RIB_LIDO", 1)
    assert not _prefixo_tardio_ok(_velas(1500, seed=11))


@pytest.mark.parametrize("cel", ag.TARDIAS)
def test_tardia_contem_a_base_na_serie_aleatoria(cel):
    c = _velas(1500, seed=11)
    sg = ag.serie_sinal(c)
    base = ag.trades_simbolo(c, "X", _cont(), ag.BASE[cel], sg)
    tard = ag.trades_simbolo(c, "X", _cont(), cel, sg)
    iguais = [t for t in tard if t[7]["atraso"] == 0]
    assert [ag._chave(t) for t in iguais] == [ag._chave(t) for t in base]
    assert len(tard) > len(base)                     # ha recuperados
    assert [t[0] for t in tard] == sorted(t[0] for t in tard)
    for t in tard:                                   # hold fecha na mesma barra
        v = t[7]["virada"]
        assert t[7]["e"] + t[7]["hold_bruto"] == v["i"] + \
            dict(zip([x["i"] for x in sg["viradas"]],
                     [h for h, _ in ag.holds(sg["viradas"], len(c) - 1)]))[v["i"]]


# ---- 6. portao de regime do KIS (v3, 21/11/2026) ----------------------------
from backtest import kis_regime as kr                         # noqa: E402
from backtest.keepitsimple import _adx                        # noqa: E402


def test_portao_parametros_fixos_da_celula_validada():
    assert (ag.PORTAO_LIMIAR, ag.PORTAO_ADX_MIN, ag.PORTAO_LIDO) == (0.02, 11, 0)
    assert (kr.EMA_SLOW, kr.INCL_LOOKBACK, kr.ATR_LEN, kr.ADX_LEN) == \
        (21, 5, 14, 13)


def test_portao_reusa_kis_regime_na_serie_sintetica():
    c = _velas(1500, seed=11)
    incl = kr.inclinacoes(c, [x["close"] for x in c])
    adx = _adx(c)
    vs = ag.serie_sinal(c)["viradas"]
    for v in vs:
        i = v["i"]
        assert (v["incl"], v["adx"]) == (incl[i], adx[i])
        assert v["portao"] is kr.passa("LONG" if v["long"] else "SHORT",
                                       incl[i], adx[i], 0.02, 11)
    ps = [v["portao"] for v in vs]
    assert any(ps) and not all(ps)                # o teste ve os dois casos


def test_portao_lado_e_piso_de_adx():
    assert ag.portao(True, 0.03, 15) and not ag.portao(False, 0.03, 15)
    assert ag.portao(False, -0.03, 15) and not ag.portao(True, -0.03, 15)
    assert not ag.portao(True, 0.01, 15) and not ag.portao(False, 0.01, 15)
    assert not ag.portao(True, 0.03, 10) and not ag.portao(False, -0.03, 10)
    assert ag.portao(True, 0.02, 11) and ag.portao(False, -0.02, 11)  # bordas
    for incl, adx in ((None, 15), (0.03, None), (None, None)):
        assert not ag.portao(True, incl, adx)     # None = nao passa
        assert not ag.portao(False, incl, adx)


@pytest.mark.parametrize("cel", ag.COM_PORTAO)
def test_celula_com_portao_e_subconjunto_da_base(cel):
    c = _velas(1500, seed=11)
    sg = ag.serie_sinal(c)
    base = ag.trades_simbolo(c, "X", _cont(), ag.BASE_PORTAO[cel], sg)
    sub = ag.trades_simbolo(c, "X", _cont(), cel, sg)
    assert sub and len(sub) < len(base)
    chave = lambda t: (t[0], t[2], t[7]["hold"], t[3], t[4], t[5], t[6])  # noqa
    assert [chave(t) for t in sub] == \
        [chave(t) for t in base if t[7]["virada"]["portao"]]
    assert [chave(t) for t in sub] == [chave(t) for t in ag.filtra(base, cel)]
    assert all(t[7]["atraso"] == 0 for t in sub)  # entra so na barra da virada


def test_portao_nao_muda_as_celulas_antigas():
    c = _velas(1500, seed=11)
    sg = ag.serie_sinal(c)
    sem = ag.monta_sinal(c, [(v["i"], v["long"]) for v in sg["viradas"]],
                         sg["vol_rel"], sg["rib"], sg["htf"])   # sem incl/adx
    assert not any(v["portao"] for v in sem["viradas"])
    for cel in ("R0", "V", "F", "G1", "G2") + ag.TARDIAS:
        a = ag.trades_simbolo(c, "X", _cont(), cel, sg)
        b = ag.trades_simbolo(c, "X", _cont(), cel, sem)
        assert [ag._chave(t) for t in a] == [ag._chave(t) for t in b]


def _portao_causal_ok(c):
    """O veto na barra i nao muda quando a serie e cortada em i."""
    full = ag.serie_sinal(c)["viradas"]
    assert any(v["portao"] for v in full)
    for v in full:
        u = ag.serie_sinal(c[:v["i"] + 1])["viradas"][-1]
        if (u["i"], u["incl"], u["adx"], u["portao"]) != \
                (v["i"], v["incl"], v["adx"], v["portao"]):
            return False
    return True


def test_portao_causal_no_corte_da_barra():
    assert _portao_causal_ok(_velas(1500, seed=11))


def test_mutacao_portao_le_barra_seguinte_reprova(monkeypatch):
    monkeypatch.setattr(ag, "PORTAO_LIDO", 1)
    assert not _portao_causal_ok(_velas(1500, seed=11))


def test_motivo_portao():
    v = lambda lg, i, a: {"long": lg, "incl": i, "adx": a,   # noqa: E731
                          "portao": ag.portao(lg, i, a)}
    assert ag.motivo_portao(v(True, 0.03, 15)) == ""
    assert ag.motivo_portao(v(True, 0.01, 15)) == "incl"
    assert ag.motivo_portao(v(True, -0.03, 15)) == "incl"
    assert ag.motivo_portao(v(False, -0.03, 10)) == "adx"
    assert ag.motivo_portao(v(False, 0.0, 10)) == "ambos"
    assert ag.motivo_portao(v(True, None, 15)) == "none"


def test_placebo_g1p_sorteia_so_no_g1_mesma_quantidade_por_lado():
    c = _velas(1500, seed=11)
    G = ag.serie_sinal(c)["viradas"]
    for k, v in enumerate(G):
        v["g"] = k
    assert ag.base_placebo("G1P") == "G1" and ag.base_placebo("R0P") == "R0"
    assert ag.base_placebo("G1") == "R0"
    tirou = 0
    for sem in (1, 2, 3):
        vet = ag.vetos_placebo(G, "G1P", sem)
        assert all(ag.passa("G1", G[g]) for g in vet)        # so trades do G1
        for lado in (True, False):
            n_p = sum(1 for v in G if v["long"] is lado and ag.passa("G1", v)
                      and not v["portao"])
            assert sum(1 for g in vet if G[g]["long"] is lado) == n_p
            tirou += n_p
    assert tirou > 0
    vet = ag.vetos_placebo(G, "R0P", 3)                       # R0P: sobre o R0
    for lado in (True, False):
        assert sum(1 for g in vet if G[g]["long"] is lado) == \
            sum(1 for v in G if v["long"] is lado and not v["portao"])
    # o agregado do G1P soma so trades do G1 que o placebo deixou
    r0 = ag.trades_simbolo(c, "X", _cont(), "R0")
    for t in r0:                       # r0 vem de outro serie_sinal: mesmo g
        t[7]["virada"]["g"] = next(v["g"] for v in G
                                   if v["i"] == t[7]["virada"]["i"])
    vet = ag.vetos_placebo(G, "G1P", 3)
    g1 = ag.filtra(r0, "G1")
    agg = ag.agrega_placebo(g1, vet)
    dentro = [t for t in g1 if ag._metade(t[0]) != "fora"]
    assert sum(x[0] for (s, _h), x in agg.items() if s == "S2") == \
        sum(1 for t in dentro if t[7]["virada"]["g"] not in vet)


def test_trimestres_a_10_bps():
    ts = lambda iso: ag._ms(iso)                              # noqa: E731
    tc = [(ts("2024-09-10"), "X", True, [0.0, 14.0], 0, False, False, {}),
          (ts("2024-09-20"), "X", True, [0.0, -5.0], 0, False, False, {}),
          (ts("2025-01-05"), "X", False, [0.0, 3.0], 0, False, False, {}),
          (ts("2024-01-05"), "X", False, [0.0, 99.0], 0, False, False, {})]
    assert ag.trimestres(tc) == {(2024, 3): [2, 1.0], (2025, 1): [1, -1.0]}
