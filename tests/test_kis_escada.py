# tests/test_kis_escada.py — sandbox, briefing de 24/11/2026: escada de stop
# apos as parciais no kis_grade (`--escada 0 1`, motor `kis_refino.simula`).
# Caminhos sinteticos de preco, um caso por degrau (long, entrada 100, ATR 1,
# parciais 3/6 com 50/25, P3 = 9). Mutacao: sem cada degrau, um por vez, o
# caso dele falha. --escada 0 byte-identico: gabaritos de
# test_sem_travas_de_estudo (4h) e test_kis_grade_escala (1h).
import csv
import inspect

import pytest

from backtest import kis_grade as g
from backtest import kis_refino as kr
from tests.test_sem_travas_de_estudo import FIM, _store

BAR = 14_400_000
E1 = g._cel(3.0, (3.0, 6.0), (0.5, 0.25), 0, 1)
E0 = g._cel(3.0, (3.0, 6.0), (0.5, 0.25), 0, 0)


def _serie(barras):
    out = [{"ts": 0, "open": 100, "high": 100, "low": 100, "close": 100}]
    for k, (o, hi, lo, c) in enumerate(barras, 1):
        out.append({"ts": k * BAR, "open": o, "high": hi, "low": lo,
                    "close": c})
    return out


def _sim(cel, barras):
    cn = _serie(barras)
    nada = [None] * len(cn)
    ev = {"i": 0, "lg": True, "sg": 1.0, "en": 100.0, "atr": 1.0,
          "e_full": len(barras), "t_full": "inversao"}
    return g.simula_celula(cel, cn, ev, nada, nada)[0]


P1 = (100, 103.5, 99.5, 103)          # executa P1; minima abaixo da entrada
P2 = (103, 106.5, 102.5, 106)         # executa P2; minima abaixo de P1
P3 = (106, 109.2, 105.5, 108)         # toca P3 = 9; minima abaixo de P2

CASOS = {
    # degrau 1: volta a entrada depois de P1
    1: ([P1, (103, 103.2, 99.8, 100.5)],
        [(1, 3.0, 103.0, 0.5, "parcial1"), (2, 0.0, 100.0, 0.5, "escada1")]),
    # degrau 2: volta a P1 depois de P2
    2: ([P1, P2, (106, 106.2, 102.8, 103)],
        [(1, 3.0, 103.0, 0.5, "parcial1"), (2, 6.0, 106.0, 0.25, "parcial2"),
         (3, 3.0, 103.0, 0.25, "escada2")]),
    # degrau 3: toca P3, volta a P2
    3: ([P1, P2, P3, (108, 108.5, 105.8, 106)],
        [(1, 3.0, 103.0, 0.5, "parcial1"), (2, 6.0, 106.0, 0.25, "parcial2"),
         (4, 6.0, 106.0, 0.25, "escada3")]),
}


def _ok(n):
    barras, esperado = CASOS[n]
    return _sim(E1, barras) == esperado


@pytest.mark.parametrize("n", [1, 2, 3])
def test_degrau_move_o_stop_so_na_barra_seguinte(n):
    # a barra da parcial / do toque tem minima abaixo do novo stop e NAO sai
    assert _ok(n)


@pytest.mark.parametrize("n", [1, 2, 3])
def test_sem_escada_o_mesmo_caminho_segue_ate_o_fim(n):
    ex = _sim(E0, CASOS[n][0])
    assert ex[-1][4] == "inversao" and ex[-1][0] == len(CASOS[n][0])
    assert not any(x[4].startswith(("escada", "stop")) for x in ex)


@pytest.mark.parametrize("n", [1, 2, 3])
def test_mutacao_sem_cada_degrau_falha(n, monkeypatch):
    orig = kr.degraus_escada

    def mutante(parciais):
        return tuple(d for j, d in enumerate(orig(parciais), 1) if j != n)
    monkeypatch.setattr(kr, "degraus_escada", mutante)
    assert not _ok(n)
    assert all(_ok(m) for m in CASOS if m < n)   # os degraus de baixo seguem


def test_escada_para_no_terceiro_degrau_e_nunca_recua():
    # apos o toque em P3: sobe a 15 (nao move mais), recua a 6.5 sem sair
    # (stop em 6, nao em 9/12) e sai em 6 sem ter recuado a P1 = 3
    barras = [P1, P2, P3, (108, 115, 107, 114), (114, 114.5, 106.5, 107),
              (107, 107.2, 105.5, 106)]
    assert _sim(E1, barras)[-1] == (6, 6.0, 106.0, 0.25, "escada3")


def test_mutacao_stop_que_recua_falha(monkeypatch):
    # sem a trava "so sobe" no motor, os degraus de baixo (ainda validos)
    # puxam o stop de volta para P1 na barra sem toque em P3
    barras = [P1, P2, P3, (108, 108.5, 106.5, 107), (107, 107.2, 105.5, 106)]
    assert _sim(E1, barras)[-1] == (5, 6.0, 106.0, 0.25, "escada3")
    src = inspect.getsource(kr.simula)
    assert src.count("feito and novo > s_nivel") == 1
    ns = dict(vars(kr))
    exec(src.replace("feito and novo > s_nivel", "feito"), ns)
    monkeypatch.setattr(kr, "simula", ns["simula"])
    assert _sim(E1, barras)[-1] != (5, 6.0, 106.0, 0.25, "escada3")


def test_atravessamento_preenche_na_abertura():
    ex = _sim(E1, [P1, (99, 99.5, 98, 99)])
    assert ex[-1] == (2, -1.0, 99, 0.5, "escada1")
    ex = _sim(E1, [P1, P2, (102, 102.5, 101, 102)])
    assert ex[-1] == (3, 2.0, 102, 0.25, "escada2")


def test_short_espelhado():
    cn = _serie([(100, 100.5, 96.5, 97), (97, 100.2, 96.8, 99.5)])
    ev = {"i": 0, "lg": False, "sg": -1.0, "en": 100.0, "atr": 1.0,
          "e_full": 2, "t_full": "inversao"}
    nada = [None] * len(cn)
    ex = g.simula_celula(E1, cn, ev, nada, nada)[0]
    assert ex == [(1, 3.0, 97.0, 0.5, "parcial1"),
                  (2, 0.0, 100.0, 0.5, "escada1")]


def test_inversao_acima_do_stop_da_escada_ganha():
    cn = _serie([P1, (103, 103.2, 101.5, 102)])
    ev = {"i": 0, "lg": True, "sg": 1.0, "en": 100.0, "atr": 1.0,
          "e_full": 2, "t_full": "inversao"}
    gS = [None, None, 102.0]
    ex = g.simula_celula(E1, cn, ev, [None] * 3, gS)[0]
    assert ex[-1] == (2, 2.0, 102.0, 0.5, "inversao")


# ---- celulas e nomes --------------------------------------------------------
def test_celulas_da_rodada_pedida():
    cs = g.celulas(ks=[3], parciais=[(3.0, 6.0), (4.0, 8.0)],
                   fracoes=[(0.5, 0.25)], tetos=[0], escadas=[0, 1])
    assert [c["nome"] for c in cs] == [
        "REF_BASE", "REF_T5s", "k3_p3-6_f50-25_t0", "k3_p3-6_f50-25_t0_e1",
        "k3_p4-8_f50-25_t0", "k3_p4-8_f50-25_t0_e1"]


def test_sem_parciais_nao_gera_variante_e_default_igual():
    cs = g.celulas(ks=[3], parciais=[None, (3.0, 6.0)], fracoes=[(0.5, 0.25)],
                   tetos=[0], escadas=[0, 1])
    nomes = [c["nome"] for c in cs]
    assert "k3_p0_t0" in nomes and "k3_p0_t0_e1" not in nomes
    assert [c["nome"] for c in g.celulas()] == \
        [c["nome"] for c in g.celulas(escadas=[0])]
    assert not any(c["nome"].endswith("_e1") for c in g.celulas())


@pytest.mark.parametrize("x", ["2", "-1", "x", "0.5"])
def test_escada_invalida_aborta(x):
    with pytest.raises(SystemExit) as e:
        g.parser().parse_args(["--escada", x])
    assert e.value.code == 2


# ---- rodada ponta a ponta ---------------------------------------------------
def _ler(d, pref):
    with next(d.glob(pref + "*.csv")).open() as f:
        return list(csv.DictReader(f))


def test_rodada_com_escada_colunas_e_e0_igual(tmp_path):
    p = tmp_path / "c.db"
    _store(p, ["BTCUSDT", "ETHUSDT"], FIM)
    base = ["--store", str(p), "--symbols", "BTCUSDT", "ETHUSDT",
            "--sem-guarda-relogio", "--ks", "3", "--parciais", "3-6", "4-8",
            "--fracoes", "50-25", "--tetos", "0"]
    d0, d1 = tmp_path / "e0", tmp_path / "e1"
    d0.mkdir()
    d1.mkdir()
    assert g.main(base + ["--logs", str(d0)]) == 0
    assert g.main(base + ["--logs", str(d1), "--escada", "0", "1"]) == 0
    r0, r1 = _ler(d0, "kis_grade_resumo_"), _ler(d1, "kis_grade_resumo_")
    assert "n_toque_p3" not in r0[0] and "pct_escada1" not in r0[0]
    cols = list(r1[0])
    j = cols.index("pct_fim")
    assert cols[j + 1:j + 5] == g.CAMPOS_ESCADA
    assert {r["celula"] for r in r1} == {
        "REF_BASE", "REF_T5s", "k3_p3-6_f50-25_t0", "k3_p3-6_f50-25_t0_e1",
        "k3_p4-8_f50-25_t0", "k3_p4-8_f50-25_t0_e1"}
    # celulas e0 iguais nas duas rodadas (colunas comuns)
    for a, b in zip(r0, [r for r in r1 if not r["celula"].endswith("_e1")]):
        assert {k: a[k] for k in a} == {k: b[k] for k in a}
    t0 = _ler(d0, "kis_grade_2")
    t1 = [t for t in _ler(d1, "kis_grade_2")
          if not t["celula"].endswith("_e1")]
    assert t0 == t1
    # n_toque_p3 so em celula com parciais; a escada saiu por algum degrau
    for r in r1:
        assert (r["n_toque_p3"] == "") == r["celula"].startswith("REF")
    e1 = [t for t in _ler(d1, "kis_grade_2") if t["celula"].endswith("_e1")]
    assert any("escada" in t["saida"] for t in e1)
    assert not any("escada" in t["saida"] for t in t0)
