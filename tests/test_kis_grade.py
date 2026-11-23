# tests/test_kis_grade.py — sandbox, briefing de 23/11/2026 (grade de
# sensibilidade das saidas do KIS 34/89). Testes de CORRECAO do codigo
# (paridade com o kis_refino, teto, fracoes, ordem stop/alvo). Nenhum
# criterio de decisao mora aqui.
import random

import pytest

from backtest import kis_grade as g
from backtest import kis_refino as kr

BAR = 14_400_000
T0 = 1_704_067_200_000                     # 2024-01-01 00:00 UTC


def _cn(n=6000, seed=7):
    rnd, p, out = random.Random(seed), 100.0, []
    for k in range(n):
        o = p
        p = max(1.0, p * (1 + rnd.gauss(0.0003, 0.02)))
        out.append({"ts": T0 + k * BAR, "open": o, "close": p,
                    "high": max(o, p) * (1 + abs(rnd.gauss(0, .006))),
                    "low": min(o, p) * (1 - abs(rnd.gauss(0, .006)))})
    return out


def _cel(nome):
    return next(c for c in g.celulas() if c["nome"] == nome)


def _grade(cn, nome, modo="novo", gat_arr=True):
    v = g.Velas(cn)
    gL, gS, _ = kr.gatilhos(cn)
    if gat_arr:
        gL, gS = g._garr(gL), g._garr(gS)
    evs = list(kr.eventos(cn, modo, kr.novo_cont(),
                          janela=(T0, T0 + 10 ** 13)))
    return g.trades_celula(_cel(nome), v, evs, gL, gS, "X")


def _chave(ts):
    return {(t["ts"], t["sym"]): (t["ret6"], t["execs"]) for t in ts}


# ---- 1. paridade com o kis_refino, trade a trade -----------------------------
@pytest.mark.parametrize("seed", [7, 8])
@pytest.mark.parametrize("modo", ["novo", "sandbox"])
def test_k3_p0_t0_igual_t5s_e_ref_base_igual_base(seed, modo):
    cn = _cn(seed=seed)
    jan = (T0, T0 + 10 ** 13)
    ref = kr.trades_simbolo(cn, "X", modo, kr.novo_cont(), janela=jan)
    v, (gL, gS, _) = g.Velas(cn), kr.gatilhos(cn)
    evs = list(kr.eventos(cn, modo, kr.novo_cont(), janela=jan))
    k3 = g.trades_celula(_cel("k3_p0_t0"), v, evs, g._garr(gL), g._garr(gS),
                         "X")
    rb = g.trades_celula(_cel("REF_BASE"), v, evs, gL, gS, "X")
    assert len(ref["T5s"]) > 50
    assert _chave(k3) == _chave(ref["T5s"])
    assert _chave(rb) == _chave(ref["BASE"])
    c = g.confere(k3, ref["T5s"])
    assert c["iguais"] == c["n_ref"] == c["n_meu"] and c["dif"] == 0.0


def test_velas_compactas_e_prefixo_dao_o_mesmo_que_a_lista():
    cn = _cn(400)
    v = g.Velas(cn)
    assert len(v) == 400 and v[123] == cn[123]
    p = v.prefixo(100)
    assert len(p) == 100 and p[99] == cn[99]
    with pytest.raises(IndexError):
        p[100]
    ts, hi, lo, cl = v.caminho(cn[300]["ts"])
    assert list(ts) == [c["ts"] for c in cn[300:]]
    assert list(cl) == [c["close"] for c in cn[300:]]


# ---- 2. teto -----------------------------------------------------------------
@pytest.mark.parametrize("teto", [128, 256, 384])
def test_teto_corta_no_fechamento_e_marca_teto(teto):
    cn = _cn()
    sem = _grade(cn, "k4_p0_t0")
    com = _grade(cn, f"k4_p0_t{teto}")
    assert len(sem) == len(com)
    assert max(t["hold"] for t in sem) > teto          # o teto tem o que cortar
    n_teto = 0
    for a, b in zip(sem, com):
        assert b["hold"] <= teto
        if a["hold"] > teto:
            n_teto += 1
            assert b["hold"] == teto and b["tipos"].endswith("teto")
            ts_fim = cn[(b["ts"] - T0) // BAR + teto]
            assert b["execs"][-1][:2] == (ts_fim["ts"], ts_fim["close"])
        else:
            assert b["execs"] == a["execs"]            # nada muda abaixo do teto
    assert n_teto > 0


# ---- 3. fracoes ----------------------------------------------------------------
def _serie(barras):
    out = [{"ts": 0, "open": 100, "high": 100, "low": 100, "close": 100}]
    for k, (o, hi, lo, c) in enumerate(barras, 1):
        out.append({"ts": k * BAR, "open": o, "high": hi, "low": lo,
                    "close": c})
    return out


def _ev(e, tipo="inversao"):
    return {"i": 0, "lg": True, "sg": 1.0, "en": 100.0, "atr": 1.0,
            "e_full": e, "t_full": tipo}


def test_fracoes_33_33_deixam_34_para_o_resto():
    cn = _serie([(100, 103.5, 99.5, 103), (103, 106.5, 102, 106),
                 (106, 106.2, 101, 101.5)])
    nada = [None] * len(cn)
    ex, _, _ = g.simula_celula(_cel("k3_p3-6_f33-33_t0"), cn, _ev(3), nada,
                               nada)
    assert [x[4] for x in ex] == ["parcial1", "parcial2", "inversao"]
    assert ex[0][3] == 0.33 and ex[1][3] == 0.33
    assert ex[2][3] == pytest.approx(0.34)
    assert sum(x[3] for x in ex) == pytest.approx(1.0)
    ex, _, _ = g.simula_celula(_cel("k3_p3-6_f50-25_t0"), cn, _ev(3), nada,
                               nada)
    assert [x[3] for x in ex] == [0.5, 0.25, 0.25]


# ---- 4. stop e alvo na mesma barra: stop primeiro (com mutacao) -------------
def _simula_mutante(cndl, i, e, entry, atr, sg, k=3.0, tempo_n=None,
                    parciais=(), gat=None, tipo_fim="inversao"):
    """MUTACAO: checa as parciais ANTES do stop na mesma barra."""
    execs, resto, pegos = [], 1.0, 0
    for idx in range(i + 1, e + 1):
        c = cndl[idx]
        fav, adv, op, _cl = kr._barra(c, entry, atr, sg)
        for n, (lvl, fr, nome) in enumerate(parciais):
            if not pegos >> n & 1 and fav >= lvl:
                execs.append((idx, lvl, entry + sg * lvl * atr, fr, nome))
                resto -= fr
                pegos |= 1 << n
        if -adv <= -k:
            execs.append((idx, -k, entry - sg * k * atr, resto, "stop"))
            return execs, 0.0, 0.0
    c = cndl[e]
    execs.append((e, sg * (c["close"] - entry) / atr, c["close"], resto,
                  tipo_fim))
    return execs, 0.0, 0.0


def _stop_primeiro_ok() -> bool:
    cn = _serie([(100, 103.5, 96.5, 103), (103, 106, 102, 105)])
    nada = [None] * len(cn)
    ex, _, _ = g.simula_celula(_cel("k3_p3-6_f50-25_t0"), cn, _ev(2), nada,
                               nada)
    return ex == [(1, -3.0, 97.0, 1.0, "stop")]


def test_stop_e_alvo_na_mesma_barra_stop_primeiro(monkeypatch):
    assert _stop_primeiro_ok()
    monkeypatch.setattr(kr, "simula", _simula_mutante)
    assert not _stop_primeiro_ok()


# ---- 5. grade e nomes ----------------------------------------------------------
def test_grade_tem_84_celulas_e_duas_referencias():
    cs = g.celulas()
    nomes = [c["nome"] for c in cs]
    assert len(cs) == 86 and len(set(nomes)) == 86
    assert nomes[:2] == ["REF_BASE", "REF_T5s"]
    assert "k3_p4-8_f50-25_t256" in nomes and "k2_p0_t0" in nomes
    grade = [c for c in cs if c["ref"] is None]
    assert len(grade) == 3 * 7 * 4
    assert all(c["toque"] for c in grade)
    assert sum(1 for c in grade if c["parc"] is None) == 3 * 4


def test_linha_de_resumo_tem_todas_as_colunas_e_liquidacao_soma():
    cn = _cn(3000)
    v = g.Velas(cn)
    cel = _cel("k4_p0_t0")
    tr = _grade(cn, "k4_p0_t0")
    for t in tr:
        t["sym"] = "X"
    cam = {"X": v.caminho(T0)}
    from backtest import carteira as cart
    rc = cart.roda([kr.para_carteira(t) for t in tr], cam)
    row = g.linha_resumo(cel, "NOVO", tr, rc, cam, 0.01)
    assert list(row) == g.CAMPOS_RESUMO
    assert int(row["n"]) == len(tr)
    liq = [cart.liquidacao(kr.para_carteira(t), cam["X"], 10)
           for t in tr]
    assert int(row["liq10_n"]) == sum(x[0] for x in liq)
    assert float(row["liq10_usd"]) == pytest.approx(
        sum(x[2] - x[1] for x in liq), abs=0.01)
    # distancia do stop: k x ATR / entrada
    d = sorted(4.0 * t["atr"] / t["entry"] * 100 for t in tr)
    assert float(row["stop_dist_max"]) == pytest.approx(d[-1], abs=1e-4)


# ---- 6. eixos por linha de comando (briefing v1 da sensibilidade do stop) ---
def _cels_args(argv):
    a = g.parser().parse_args(argv)
    return g.celulas(ks=a.ks, parciais=a.parciais, fracoes=a.fracoes,
                     tetos=a.tetos)


def test_sem_argumentos_a_grade_e_a_de_23_11():
    cs = _cels_args([])
    assert cs == g.celulas()
    nomes = [c["nome"] for c in cs]
    esperado = ["REF_BASE", "REF_T5s"]
    for k in (2, 3, 4):
        for pn in ("p0", "p3-6_f50-25", "p3-6_f33-33", "p4-8_f50-25",
                   "p4-8_f33-33", "p6-12_f50-25", "p6-12_f33-33"):
            for t in (0, 128, 256, 384):
                esperado.append(f"k{k}_{pn}_t{t}")
    assert nomes == esperado and len(nomes) == 86


def test_rodada_pedida_tem_12_celulas_com_k_decimal():
    cs = _cels_args(["--ks", "2.5", "3", "3.5", "--parciais", "3-6", "6-12",
                     "--fracoes", "50-25", "33-33", "--tetos", "0"])
    nomes = [c["nome"] for c in cs]
    assert len(nomes) == 14 and nomes[:2] == ["REF_BASE", "REF_T5s"]
    assert "k2.5_p6-12_f33-33_t0" in nomes and "k3.5_p3-6_f50-25_t0" in nomes
    k3 = [c for c in cs if c["nome"].startswith("k3_")]
    assert [c["nome"] for c in k3] == [
        "k3_p3-6_f50-25_t0", "k3_p3-6_f33-33_t0", "k3_p6-12_f50-25_t0",
        "k3_p6-12_f33-33_t0"]
    assert all(c == _cel(c["nome"]) for c in k3)   # = as celulas de 23/11


def test_ks_2_5_poe_o_stop_a_2_5_atr_da_barra_do_sinal():
    cel = next(c for c in _cels_args(["--ks", "2.5"])
               if c["nome"] == "k2.5_p0_t0")
    assert cel["k"] == 2.5
    # sintetico: adverso de 2,6 ATR sem gap -> stop exato em 100 - 2,5
    cn = _serie([(100, 100.5, 97.4, 98), (98, 99, 97.2, 98.5)])
    nada = [None] * len(cn)
    ex, _, _ = g.simula_celula(cel, cn, _ev(2), nada, nada)
    assert ex == [(1, -2.5, 97.5, 1.0, "stop")]
    # mesmo caminho com k 3 nao para (o adverso nao chega a 3 ATR)
    ex3, _, _ = g.simula_celula(_cel("k3_p0_t0"), cn, _ev(2), nada, nada)
    assert ex3[-1][4] == "inversao"
    # serie aleatoria: todo stop fica a 2,5 x ATR14 do sinal (ou na abertura)
    v = g.Velas(_cn())
    gL, gS, _ = kr.gatilhos(_cn())
    evs = list(kr.eventos(_cn(), "novo", kr.novo_cont(),
                          janela=(T0, T0 + 10 ** 13)))
    n = 0
    for ev in evs:
        ex, _, _ = g.simula_celula(cel, v, ev, g._garr(gL), g._garr(gS))
        idx, r, px, _, tipo = ex[-1]
        if tipo != "stop":
            continue
        n += 1
        nivel = ev["en"] - ev["sg"] * 2.5 * ev["atr"]
        if r == -2.5:
            assert px == pytest.approx(nivel)
        else:                                       # gap: executa na abertura
            assert px == v[idx]["open"] and r < -2.5
    assert n > 10


@pytest.mark.parametrize("argv", [["--parciais", "8-4"],
                                  ["--parciais", "3"],
                                  ["--fracoes", "60-50"],
                                  ["--fracoes", "0-25"],
                                  ["--ks", "0"], ["--ks", "x"],
                                  ["--tetos", "-1"]])
def test_eixo_invalido_aborta_antes_de_ler_o_store(argv, monkeypatch):
    def proibido(*a, **k):
        raise AssertionError("leu o store")
    monkeypatch.setattr(g, "conecta_com_teto", proibido)
    monkeypatch.setattr(g, "_guarda_relogio", proibido)
    with pytest.raises(SystemExit) as e:
        g.main(argv + ["--store", "/nao/existe.db", "--logs", "/nao/existe"])
    assert e.value.code == 2


def test_fracao_ate_100_aceita():
    a = g.parser().parse_args(["--fracoes", "50-50", "33-33"])
    assert a.fracoes == [(0.5, 0.5), (0.33, 0.33)]
