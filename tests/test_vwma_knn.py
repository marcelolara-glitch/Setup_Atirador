# tests/test_vwma_knn.py — sandbox, etapa 5 (15/11/2026): VWMA 13/19 + kNN
import random

import pytest

from backtest import kis_knn as kk
from backtest import vwma_knn as vk


def _g(ts, long, adx, mfi, opn, sym="A"):
    return {"ts": ts, "sym": sym, "long": long, "adx": adx, "mfi": mfi,
            "open": opn, "i": ts}


# ---- 1. serie sintetica com memoria fechada a mao ---------------------------
def _seis():
    # rotulos (gravados no cruzamento seguinte, pelo OPEN):
    #   g0->g1 110>=100 +1 | g1->g2 +1 | g2->g3 90<120 -1 | g3->g4 -1 |
    #   g4->g5 -1 | g5->g6 75>=70 +1
    return [_g(10, True, 10, 10, 100), _g(20, False, 10, 10, 110),
            _g(30, True, 10, 10, 120), _g(40, False, 80, 80, 90),
            _g(50, True, 80, 80, 80), _g(60, False, 80, 80, 70),
            _g(70, True, 10, 10, 75)]


def test_decisoes_memoria_a_mao():
    d = vk.decisoes(_seis(), k=2)
    # g0 vazia -> veta (soma 0) | g1 {+1} -> +1 veta | g2 {+1,+1} -> +2 LONG
    # g3 (80,80): 3 pontos equidistantes, empate inclui todos -> +1 veta
    # g4: g3 a d=0 + 3 empatados -> 0 veta | g5: g3,g4 a d=0 -> -2 SHORT
    # g6 (10,10): g0,g1,g2 a d=0 -> +1 veta
    assert [x[0] for x in d] == ["veta", "veta", "libera", "veta", "veta",
                                 "libera", "veta"]
    assert [x[2] for x in d] == [0, 1, 2, 1, 0, -2, 1]
    assert d[0][1] is True                      # vazia conta como fria


def test_entradas_e_vetos_no_caminho():
    g = _seis()
    lib = [x[0] == "libera" for x in vk.decisoes(g, k=2)]
    cruz = [(x["i"], x["long"]) for x in g]
    # LONG em 30 segura atraves dos vetos de 40 e 50, reverte em 60;
    # SHORT em 60 segura atraves do veto de 70 ate o fim (100)
    assert vk.caminho(cruz, lib, 100) == [(2, 30, False), (5, 40, True)]
    # controle: todo cruzamento reverte
    assert vk.caminho(cruz, [True] * 7, 100) == [
        (0, 10, False), (1, 10, False), (2, 10, False), (3, 10, False),
        (4, 10, False), (5, 10, False), (6, 30, True)]


def test_vwma_e_cruzamento_a_mao():
    c = [{"open": o, "volume": v} for o, v in
         [(10, 1), (20, 3), (30, 1), (40, 0), (0, 0)]]
    v2 = vk.vwma(c, 2)
    assert v2 == [None, pytest.approx(17.5), pytest.approx(22.5), 30.0, None]
    # t=1 empata (nada); t=2 sobe; t=3 empata; t=4 sobe DE NOVO a partir do
    # empate (crossover do Pine: a0 <= b0), sem cruzamento para baixo no meio
    assert vk.cruzamentos([1, 1, 3, 2, 2, 1], [2, 1, 2, 2, 1, 2]) == [
        (2, True), (4, True), (5, False)]
    assert vk.cruzamentos([1, None, 3], [2, 2, 2]) == []


# ---- 2. mutacao do rotulo ---------------------------------------------------
def _aleatorios(n=300, seed=3):
    rnd = random.Random(seed)
    return [_g(t, t % 2 == 0, rnd.uniform(5, 60), rnd.uniform(0, 100),
               100 + rnd.gauss(0, 10)) for t in range(n)]


def test_inverter_rotulo_inverte_decisoes():
    g = _aleatorios()
    for x in g:
        x["open_neg"] = -x["open"]            # sem empates: rotulo -> -rotulo
    k = 7
    a, b = vk.decisoes(g, k=k), vk.decisoes(g, k=k, campo="open_neg")
    quente = [i for i, x in enumerate(a) if not x[1]]
    assert len(quente) > 200
    lado = lambda s: 1 if s >= 2 else (-1 if s <= -2 else 0)   # noqa: E731
    for i in quente:
        assert b[i][2] == -a[i][2]
        assert lado(b[i][2]) == -lado(a[i][2])
    assert any(lado(a[i][2]) != 0 for i in quente)


# ---- 3. invariancia a prefixo + mutacao que olha o cruzamento seguinte -----
def _velas(n=900, seed=11):
    rnd = random.Random(seed)
    p, out = 100.0, []
    for t in range(n):
        o = p
        p = max(1.0, p * (1 + rnd.gauss(0, 0.02)))
        out.append({"ts": t, "open": o, "close": p,
                    "high": max(o, p) * (1 + abs(rnd.gauss(0, .005))),
                    "low": min(o, p) * (1 - abs(rnd.gauss(0, .005))),
                    "volume": 100 + rnd.random() * 900})
    return out


def _mutante(g):
    """Rotulo que le o open do cruzamento SEGUINTE AO SEGUINTE: o ponto p,
    gravado no cruzamento t, passa a comparar open(depois de t) com open(t)."""
    for j, x in enumerate(g):
        x["open_mut"] = g[j + 1]["open"] if j + 1 < len(g) else x["open"]
    return g


def _decide_prefixo(cndl, k, mut):
    g, _na = vk.gatilhos_token(cndl, "A")
    if mut:
        _mutante(g)
    d = vk.decisoes(g, k=k, campo="open_mut" if mut else "open")
    return {x["i"]: e for x, e in zip(g, d)}


@pytest.mark.parametrize("k", [5, 45])
def test_invariancia_a_prefixo(k):
    c = _velas()
    cheio = _decide_prefixo(c, k, False)
    assert len(cheio) > 40
    for corte in (200, 333, 517, 700, 899):
        parcial = _decide_prefixo(c[:corte + 1], k, False)
        assert parcial == {i: e for i, e in cheio.items() if i <= corte}


def test_mutacao_rotulo_do_seguinte_ao_seguinte_reprova():
    c = _velas()
    cheio = _decide_prefixo(c, 5, True)
    quebrou = 0
    for corte in (200, 333, 517, 700, 899):
        parcial = _decide_prefixo(c[:corte + 1], 5, True)
        quebrou += parcial != {i: e for i, e in cheio.items() if i <= corte}
    assert quebrou > 0


# ---- 4. hold com veto -----------------------------------------------------
def test_hold_atravessa_veto_e_mesma_direcao_nao_duplica():
    cruz = [(10, True), (20, False), (25, True), (30, True), (40, False)]
    # contrario em 20 vetado; 25 e 30 liberados na MESMA direcao: nada
    assert vk.caminho(cruz, [True, False, True, True, True], 60) == [
        (0, 30, False), (4, 20, True)]
    # nada liberado -> nenhuma posicao
    assert vk.caminho(cruz, [False] * 5, 60) == []


# ---- 5. placebo refaz o caminho -------------------------------------------
def test_placebo_refaz_caminho_e_hold_muda():
    g = [_g(10 * (j + 1), j % 2 == 0, 20.0, 50.0, 100.0 + j) for j in range(8)]
    est = [("libera", False, 0)] * 6 + [("veta", False, 0)] * 2   # 1 veto/lado
    cruz = [(x["i"], x["long"]) for x in g]
    base = vk.caminho(cruz, [e[0] == "libera" for e in est], 100)
    hold_base = {k: hb for k, hb, _f in base}
    achou = False
    for s in kk.SEMENTES:
        vet = kk.placebo(g, est, s)
        assert len(vet) == 2
        pl = vk.caminho(cruz, [j not in vet for j in range(8)], 100)
        for k, hb, _f in pl:
            if k + 1 in vet and k in hold_base and k + 1 < 6:
                # o veto caiu no cruzamento seguinte: hold cresce
                assert hb > hold_base[k]
                achou = True
        # apagar trades da lista NAO reproduz o caminho refeito
        apagado = [t for t in base if t[0] not in vet]
        if any(k + 1 in vet and k + 1 < 6 for k, _h, _f in apagado):
            assert apagado != pl
    assert achou


def test_concentracao():
    assert vk.concentracao([]) == (0, None, 0.0)
    nt, fat, sem = vk.concentracao([100.0] + [1.0] * 99)
    assert nt == 2 and fat == pytest.approx(100 * 101 / 199) and sem == 98.0
    assert vk.concentracao([5.0, -10.0])[1] is None       # total <= 0 -> NA


# ---- ponta a ponta + T1 ---------------------------------------------------
def test_main_ponta_a_ponta(tmp_path, capsys):
    from tests.test_kis_saida import _store
    db = tmp_path / "c.db"
    _store(db, ["BTCUSDT", "ETHUSDT", "AAAUSDT"], "2026-11-14")
    out = tmp_path / "o.csv"
    assert vk.main(["--store", str(db), "--symbols", "BTCUSDT", "ETHUSDT",
                    "AAAUSDT", "--out", str(out),
                    "--sem-guarda-relogio"]) == 0
    txt = capsys.readouterr().out
    assert txt.rstrip().splitlines()[-1].startswith("caveat:")
    assert "kNN acima de" in txt and "bate" in txt
    log = out.with_suffix(".log").read_text()
    assert log.rstrip().splitlines()[-1].startswith("caveat:")
    import csv
    rows = list(csv.DictReader(out.open()))
    cels = {r["celula"] for r in rows}
    assert {"C0", "K_tok", "K_agr", "placebo_K_tok",
            "placebo_K_agr"} <= cels
    assert sum(1 for r in rows if r["celula"] == "placebo_K_tok") == 40
    # C0 reverte em todo cruzamento: nenhum trade C0 truncado sem marca
    assert all(r["soma_knn"] == "" for r in rows if r["celula"] == "C0")


def test_end_alem_de_2026_08_26_e_aceito(tmp_path):
    # 23/11/2026: a recusa T1 saiu; o --end passa e a rodada so para na
    # validacao de entrada (store inexistente). Execucao completa com
    # --end alem do teto: tests/test_sem_travas_de_estudo.py.
    with pytest.raises(SystemExit, match="inexistente"):
        vk.main(["--end", "2026-09-01", "--store", str(tmp_path / "x.db"),
                 "--out", str(tmp_path / "o.csv")])
