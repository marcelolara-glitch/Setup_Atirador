# tests/test_sem_travas_de_estudo.py — sandbox, briefing de 23/11/2026
# (decisao do Marcelo): regra de estudo nao trava simulacao. Para CADA recusa
# retirada, o instrumento agora ACEITA o caso que antes recusava; e o que fica
# (integridade do dado e da maquina) continua disparando:
#   - detector de vazamento (`view deixou passar candle >= --end`), provado
#     por MUTACAO: sem a TEMP VIEW, todo instrumento aborta;
#   - teto de RSS, agora `--teto-rss` (default 250);
#   - validacao de entrada (tf desconhecido, store inexistente).
# Mais: KIS em 1h na mesma duracao das regras de 4h (`kis_refino.escala`) e o
# gabarito de byte-identidade do kis_grade em 4h contra o codigo de 8ec3423.
# Secao 8: --end antes do NOVO aceito (kis_grade, kis_refino, holdout_2026).
import hashlib
import random
import re
from datetime import datetime, timezone

import pytest

from backtest import carteira as cart
from backtest import kis_grade as g
from backtest import kis_refino as kr
from backtest import kis_saida as ks
from backtest import kis_trail
from backtest.candle_store import connect, read_candles, upsert_candles
from tests.test_kis_saida import _store

H = 3_600_000
FIM = "2026-11-20"                  # o store vai alem de qualquer --end usado
END_ALEM = "2026-11-14"             # > 2026-08-26, o antigo teto T1
SYMS = ["BTCUSDT", "ETHUSDT", "AAAUSDT"]


def _store_1h(path, syms, ate_iso, ini_iso="2024-06-17", seed=11):
    conn = connect(path)
    rnd = random.Random(seed)
    for s in syms:
        p, ts, fim, cs = 100.0, ks._ms(ini_iso), ks._ms(ate_iso), []
        while ts < fim:
            o = p
            p = max(1.0, p * (1 + rnd.gauss(0.0001, 0.01)))
            cs.append({"ts": ts, "open": o,
                       "high": max(o, p) * (1 + abs(rnd.gauss(0, .003))),
                       "low": min(o, p) * (1 - abs(rnd.gauss(0, .003))),
                       "close": p, "volume": 1000 + rnd.random() * 500})
            ts += H
        upsert_candles(conn, s, "1h", cs)
    conn.close()


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    """4h para SYMS e 1h para os dois primeiros, ate FIM."""
    p = tmp_path_factory.mktemp("st") / "c.db"
    _store(p, SYMS, FIM)
    _store_1h(p, SYMS[:2], FIM)
    return str(p)


def _maior_ts(txt: str) -> str:
    m = re.search(r"maior ts visivel[^\d-]*(\d{4}-\d\d-\d\d)", txt)
    assert m, "instrumento nao registrou o maior ts visivel"
    return m.group(1)


# ---- 1. T1: --end alem de 2026-08-26 e aceito e LIDO -------------------------
T1_INSTR = ["kis_saida", "kis_knn", "kis_meta", "kis_tf", "vwma_knn",
            "aio_graduado"]


@pytest.mark.parametrize("nome", T1_INSTR)
def test_t1_retirado_end_alem_do_antigo_teto_roda_e_le(nome, db, tmp_path,
                                                       capsys):
    import importlib
    mod = importlib.import_module(f"backtest.{nome}")
    rc = mod.main(["--end", END_ALEM, "--store", db, "--symbols", *SYMS,
                   "--out", str(tmp_path / "o.csv"), "--sem-guarda-relogio"])
    assert rc == 0
    txt = capsys.readouterr().out
    assert _maior_ts(txt) > "2026-08-26"           # leu alem do antigo teto
    assert _maior_ts(txt) < END_ALEM                # e nao alem do --end


# ---- 2. detector de vazamento: mutacao (sem a TEMP VIEW) aborta --------------
LEAK_INSTR = T1_INSTR + ["holdout_2026", "kis_refino", "kis_grade"]


@pytest.mark.parametrize("nome", LEAK_INSTR)
def test_detector_de_vazamento_dispara_com_dado_futuro(nome, db, tmp_path,
                                                       monkeypatch):
    import importlib
    mod = importlib.import_module(f"backtest.{nome}")

    def sem_view(store, teto_ms):              # MUTACAO: tabela crua
        return connect(store)
    monkeypatch.setattr(ks, "conecta_com_teto", sem_view)
    if hasattr(mod, "conecta_com_teto"):
        monkeypatch.setattr(mod, "conecta_com_teto", sem_view)
    out = (["--logs", str(tmp_path)] if nome == "kis_grade"
           else ["--out", str(tmp_path / "o.csv")])
    with pytest.raises(SystemExit, match="view deixou passar candle"):
        mod.main(["--end", "2026-10-01", "--store", db, "--symbols", *SYMS,
                  "--sem-guarda-relogio", *out])


def test_view_real_esconde_o_futuro(db):
    conn = ks.conecta_com_teto(db, ks._ms("2026-10-01"))
    assert conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0] < \
        ks._ms("2026-10-01")
    conn.close()
    raw = connect(db)
    assert raw.execute("SELECT MAX(ts) FROM candles").fetchone()[0] >= \
        ks._ms("2026-10-01")                    # o dado futuro existe
    raw.close()


# ---- 3. "so 4h" retirado -------------------------------------------------------
CEL_1H = ["REF_BASE", "REF_T5s", "k3_p0_t0", "k2_p3-6_f50-25_t512"]


def test_kis_grade_roda_em_1h_com_conferencia_e_tetos_na_mesma_duracao(
        db, tmp_path, capsys):
    assert g.main(["--tf", "1h", "--store", db, "--symbols", *SYMS[:2],
                   "--logs", str(tmp_path), "--sem-guarda-relogio",
                   "--celulas", *CEL_1H]) == 0
    log = next(tmp_path.glob("kis_grade_2*.log")).read_text()
    assert "| tf 1h |" in log                   # a coleta na VM separa por isso
    assert "borda 192" in log
    assert "--tetos 0 512 1024 1536" in log
    conf = re.findall(r"iguais (\d+) de (\d+) \| max\|dif\| 0\.00e\+00 bps \|"
                      r" so no kis_refino 0 \| so aqui 0", log)
    assert len(conf) == 6 and all(a == b and int(a) > 0 for a, b in conf)
    rs = next(tmp_path.glob("kis_grade_resumo_*.csv")).read_text()
    assert "k2_p3-6_f50-25_t512" in rs and rs.count("\n") == 1 + 4 * 3


def test_kis_refino_roda_em_1h_e_desliga_so_a_conferencia(db, tmp_path):
    out = tmp_path / "r.csv"
    assert kr.main(["--tf", "1h", "--store", db, "--symbols", *SYMS[:2],
                    "--out", str(out), "--sem-guarda-relogio",
                    "--carteira-celulas", "BASE"]) == 0
    log = out.with_suffix(".log").read_text()
    assert "DESLIGADA em 1h" in log and "| tf 1h |" in log
    cels = {ln.split(",")[0] for ln in out.read_text().splitlines()[1:]}
    assert {"BASE_1024", "T1_24", "T1_48", "T1_96"} <= cels
    assert "BASE_256" not in cels and "T1_6" not in cels


@pytest.mark.parametrize("nome", ["aio_graduado", "holdout_2026"])
def test_aio_e_holdout_rodam_em_1h_com_aviso_do_que_distorce(nome, db,
                                                             tmp_path):
    import importlib
    mod = importlib.import_module(f"backtest.{nome}")
    out = tmp_path / "o.csv"
    assert mod.main(["--tf", "1h", "--end", "2026-11-14 16:00", "--store", db,
                     "--symbols", *SYMS[:2], "--out", str(out),
                     "--sem-guarda-relogio"]) == 0
    log = out.with_suffix(".log").read_text()
    assert "[aviso] tf 1h" in log and "DISTORCIDO" in log


def test_kis_tf_roda_1h_abaixo_de_90pct_de_cobertura(tmp_path, capsys):
    from backtest import kis_tf as kt
    p = tmp_path / "c.db"
    _store(p, ["BTCUSDT", "ETHUSDT"], "2026-11-14")
    conn = connect(p)
    c1 = [dict(c, ts=c["ts"] + j * H) for c in
          read_candles(conn, "BTCUSDT", "4h") for j in range(4)]
    upsert_candles(conn, "BTCUSDT", "1h", c1)   # so 1 de 2 cobre (50%)
    conn.close()
    out = tmp_path / "o.csv"
    assert kt.main(["--store", str(p), "--symbols", "BTCUSDT", "ETHUSDT",
                    "--out", str(out), "--sem-guarda-relogio"]) == 0
    assert "AVISO: abaixo de 90%" in capsys.readouterr().out
    assert "1h" in {ln.split(",")[1] for ln in out.read_text().splitlines()[1:]}


# ---- 4. guarda de horario: aviso, nao recusa -----------------------------------
class _Relogio(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 11, 23, 4, 16, tzinfo=timezone.utc)


def test_guarda_de_horario_avisa_e_segue(db, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ks, "datetime", _Relogio)
    assert ks._guarda_relogio() is True
    assert "[aviso] 04:16 UTC" in capsys.readouterr().out
    assert ks.main(["--store", db, "--symbols", *SYMS, "--out",
                    str(tmp_path / "o.csv")]) == 0     # sem --sem-guarda


# ---- 5. teto de RSS: fica, como parametro -------------------------------------
def test_teto_rss_parametro_continua_parando_e_volta_ao_default(db, tmp_path):
    with pytest.raises(MemoryError, match="teto 1 "):
        ks.main(["--store", db, "--symbols", *SYMS, "--out",
                 str(tmp_path / "o.csv"), "--sem-guarda-relogio",
                 "--teto-rss", "1"])
    assert ks.main(["--store", db, "--symbols", *SYMS, "--out",
                    str(tmp_path / "o.csv"), "--sem-guarda-relogio"]) == 0
    assert kis_trail.teto_rss() == kis_trail.RSS_CEILING_MIB == 250.0


@pytest.mark.parametrize("argv", [["--teto-rss", "0"], ["--teto-rss", "x"],
                                  ["--tf", "6h"]])
def test_entrada_invalida_aborta_antes_do_store(argv, monkeypatch):
    def proibido(*a, **k):
        raise AssertionError("leu o store")
    monkeypatch.setattr(g, "conecta_com_teto", proibido)
    with pytest.raises(SystemExit) as e:
        g.main(argv + ["--store", "/nao/existe.db", "--logs", "/nao/existe"])
    assert e.value.code == 2


# ---- 6. 4h byte-identico ao codigo de 8ec3423 (gabarito) ----------------------
GABARITO_4H = {   # sha256 gerado com backtest/kis_grade.py de 8ec3423
    "kis_grade_resumo_":
        "bb56a4b12e9b8908009d708bd7c9679f401578a29e90c4abe190c5509f1e5235",
    "kis_grade_2":
        "8c5218244b06162f8502f0effd719d35d3d5284c4e27433d98e2ad99ec43b85f",
    "carteira_KIS_GRADE_":
        "7736609b1c418ecfb8606d8c9a0535b017f959a2bf1558d32141c95f60c131ff",
}


def test_kis_grade_4h_byte_identico_ao_de_antes(tmp_path):
    p = tmp_path / "c.db"
    _store(p, ["BTCUSDT", "ETHUSDT"], FIM)
    assert g.main(["--store", str(p), "--symbols", "BTCUSDT", "ETHUSDT",
                   "--logs", str(tmp_path), "--sem-guarda-relogio",
                   "--celulas", "REF_BASE", "REF_T5s", "k3_p0_t0",
                   "k2_p3-6_f50-25_t128", "k4_p6-12_f33-33_t384"]) == 0
    for pref, h in GABARITO_4H.items():
        f = next(tmp_path.glob(pref + "*.csv"))
        assert hashlib.sha256(f.read_bytes()).hexdigest() == h, pref


# ---- 7. escala do tf: so a DURACAO muda --------------------------------------
def test_escala_converte_duracoes_e_4h_fica_igual():
    e4, e1 = kr.escala("4h"), kr.escala("1h")
    assert (e4["fator"], e4["borda"], e4["cap"], e4["t1_ns"]) == \
        (1, 48, 256, (6, 12, 24))
    assert (e1["fator"], e1["borda"], e1["cap"], e1["t1_ns"]) == \
        (4, 192, 1024, (24, 48, 96))
    assert kr.escala("15m")["borda"] == 768
    assert kr.nomes_celulas(e4) == kr.CELULAS
    assert kr.nomes_simb(e4) == kr.CEL_SIMB
    assert g.tetos_do_tf(e1) == [0, 512, 1024, 1536]
    with pytest.raises(ValueError):
        kr.escala("6h")


def _serie_1h(n, seed=3):
    rnd, p, out = random.Random(seed), 100.0, []
    for k in range(n):
        o = p
        p = max(1.0, p * (1 + rnd.gauss(0.0002, 0.01)))
        out.append({"ts": ks._ms("2025-01-01") + k * H, "open": o, "close": p,
                    "high": max(o, p) * 1.002, "low": min(o, p) * 0.998})
    return out


def test_caminho_em_1h_comeca_na_barra_seguinte():
    from backtest.holdout_2026 import medir_sem_borda
    cn = _serie_1h(400)
    m = medir_sem_borda(cn, 100, "LONG", bar_ms=H, cap=1024)
    assert len(m["fwd_bar"]) == 299
    assert m["fwd_bar"][0] == pytest.approx(
        (cn[101]["close"] - cn[100]["close"]) / m["atr"])
    # a distorcao que o aviso do holdout/aio descreve: barra de 4h fixa pula 3
    d = medir_sem_borda(cn, 100, "LONG")
    assert d["fwd_bar"][0] == pytest.approx(
        (cn[104]["close"] - cn[100]["close"]) / d["atr"])


def test_borda_em_1h_e_192_barras():
    e1 = kr.escala("1h")
    e48 = dict(e1, borda=48)                 # so a borda muda
    cheia = _serie_1h(3000)
    i_ult = list(kr.eventos(cheia, "sandbox", kr.novo_cont(), esc=e48))[-1]["i"]
    cn = cheia[:i_ult + 101]                # 100 barras a frente: >= 48, < 192
    c48, c1 = kr.novo_cont(), kr.novo_cont()
    ev48 = list(kr.eventos(cn, "sandbox", c48, esc=e48))
    ev1 = list(kr.eventos(cn, "sandbox", c1, esc=e1))
    perto = [e for e in ev48 if len(cn) - 1 - e["i"] < 192]
    assert ev48[-1]["i"] == i_ult and perto
    assert c1["excl_borda"] == c48["excl_borda"] + len(perto)
    assert [e["i"] for e in ev1] == [e["i"] for e in ev48 if e not in perto]


def test_hold_generico_reproduz_o_hold_do_entradas_em_4h():
    from backtest.kis_horizonte import entradas
    from tests.test_kis_grade import _cn
    cn = _cn()                                  # serie de 4h
    pos = {c["ts"]: i for i, c in enumerate(cn)}
    hold_ent = {pos[x["bar_ts"]]: x["hold"] for x in entradas(cn, 34, 89)}
    for modo in ("sandbox", "novo"):
        evs = list(kr.eventos(cn, modo, kr.novo_cont(),
                              janela=(cn[0]["ts"], cn[-1]["ts"])))
        assert len(evs) > 50
        assert all(e["hold"] == hold_ent[e["i"]] for e in evs
                   if modo == "sandbox" or e["t256"] != "fim")


def test_gatilhos_sao_causais_e_podem_ser_compartilhados():
    cn = _serie_1h(2000)
    gL, gS, st = kr.gatilhos(cn)
    pL, pS, pst = kr.gatilhos(cn[:1500])
    assert gL[:1500] == pL and gS[:1500] == pS and st[:1500] == pst


def test_carteira_fecha_o_dia_na_ultima_barra_do_tf():
    d0 = ks._ms("2025-03-01")
    marcas = [(d0 + k * H, 10_000.0 + k, 0.0, 0.0, 0, 0, 0.0, 0.0, 0, 0)
              for k in range(24)]
    assert cart.diario_de(marcas, 10_000.0, H)[0]["banca_usd"] == 10_023.0
    assert cart.diario_de(marcas, 10_000.0)[0]["banca_usd"] == 10_020.0  # 4h


def test_caminho_sem_copia_da_a_mesma_carteira():
    cn = _serie_1h(1500)
    v = g.Velas(cn)
    mv = v.caminho(cn[200]["ts"])
    assert all(isinstance(x, memoryview) for x in mv)
    cp = cart.caminho(cn, cn[200]["ts"])
    assert all(list(a) == list(b) for a, b in zip(mv, cp))
    evs = list(kr.eventos(cn, "novo", kr.novo_cont(),
                          janela=(cn[200]["ts"], cn[-1]["ts"]),
                          esc=kr.escala("1h")))
    gL, gS, _ = kr.gatilhos(cn)
    tr = g.trades_celula(g._cel(3.0, None, None, 0), v, evs, gL, gS, "X")
    ct = [kr.para_carteira(t) for t in tr]
    assert tr
    a = cart.roda(ct, {"X": mv}, bar_ms=H)
    b = cart.roda(ct, {"X": cp}, bar_ms=H)
    assert a["metricas"] == b["metricas"] and a["diario"] == b["diario"]


# ---- 8. --end antes do NOVO (23/11/2026): aceito, periodo vazio no log --------
# Cada periodo e cortado em --end; o que fica vazio aparece como
# `vazio (--end <data>)`, sem linha no resumo e sem carteira, sem erro.
ENDS_CEDO = {"2026-08-26": ("H1", "H2"), "2025-08-26": ("H1",)}


def _periodos_csv(path, col="periodo"):
    import csv
    with open(path) as fh:
        return {r[col] for r in csv.DictReader(fh)}


@pytest.mark.parametrize("end", sorted(ENDS_CEDO))
def test_kis_grade_end_antes_do_novo_entrega_so_os_periodos_do_corte(
        end, db, tmp_path):
    esperado = set(ENDS_CEDO[end])
    assert g.main(["--end", end, "--store", db, "--symbols", *SYMS,
                   "--logs", str(tmp_path), "--sem-guarda-relogio",
                   "--celulas", "REF_BASE", "REF_T5s", "k3_p0_t0"]) == 0
    log = next(tmp_path.glob("kis_grade_2*.log")).read_text()
    for p in ("H1", "H2", "NOVO"):
        assert (f"  {p}: vazio (--end {end})" in log) is (p not in esperado)
    rs = next(tmp_path.glob("kis_grade_resumo_*.csv"))
    assert _periodos_csv(rs) == esperado
    assert rs.read_text().count("\n") == 1 + 3 * len(esperado)
    assert _periodos_csv(next(tmp_path.glob("carteira_*.csv"))) == esperado
    assert _maior_ts(log) < end
    conf = re.findall(r"^\s+(\w+) REF_\w+ x \w+ do kis_refino: iguais (\d+) de "
                      r"(\d+)", log, re.M)
    assert {p for p, _a, _b in conf} == esperado
    assert all(a == b for _p, a, b in conf)


@pytest.mark.parametrize("end", sorted(ENDS_CEDO))
def test_kis_refino_end_antes_do_novo_entrega_so_os_periodos_do_corte(
        end, db, tmp_path):
    esperado = set(ENDS_CEDO[end])
    out = tmp_path / "r.csv"
    assert kr.main(["--end", end, "--store", db, "--symbols", *SYMS,
                    "--out", str(out), "--sem-guarda-relogio",
                    "--carteira-celulas", "BASE", "T3"]) == 0
    log = out.with_suffix(".log").read_text()
    for p in ("H1", "H2", "NOVO"):
        assert (f"  {p}: vazio (--end {end})" in log) is (p not in esperado)
        assert (f"===== {p} — por celula" in log) is (p in esperado)
        assert (f"  BASE {p}:" in log) is (p in esperado)      # carteira
    assert _periodos_csv(out) <= esperado
    assert _periodos_csv(tmp_path.glob("carteira_*.csv").__next__()) == \
        esperado
    assert "NOVO BASE_256 x holdout" not in log
    m = re.search(r"sandbox BASE_256 x kis_saida E1-3: iguais (\d+) de (\d+) "
                  r"\| max\|dif\| 0\.00e\+00 bps \| so no kis_saida 0 \| so "
                  r"aqui 0", log)
    assert m and m.group(1) == m.group(2) and int(m.group(1)) > 0
    assert _maior_ts(log) < end


@pytest.mark.parametrize("end", sorted(ENDS_CEDO))
def test_holdout_end_antes_do_novo_roda_vazio(end, db, tmp_path):
    from backtest import holdout_2026 as ho
    out = tmp_path / "h.csv"
    assert ho.main(["--end", end, "--store", db, "--symbols", *SYMS,
                    "--out", str(out), "--sem-guarda-relogio"]) == 0
    log = out.with_suffix(".log").read_text()
    assert f"  NOVO: vazio (--end {end})" in log
    assert "RESULTADO POR SETUP" not in log and "PASSIVO" not in log
    assert out.read_text().count("\n") == 1                 # so o cabecalho
    assert _maior_ts(log) < end


@pytest.mark.parametrize("end", sorted(ENDS_CEDO))
@pytest.mark.parametrize("nome", ["holdout_2026", "kis_refino", "kis_grade"])
def test_end_antes_do_novo_detector_de_vazamento_dispara(nome, end, db,
                                                         tmp_path,
                                                         monkeypatch):
    import importlib
    mod = importlib.import_module(f"backtest.{nome}")

    def sem_view(store, teto_ms):              # MUTACAO: tabela crua
        return connect(store)
    monkeypatch.setattr(ks, "conecta_com_teto", sem_view)
    monkeypatch.setattr(mod, "conecta_com_teto", sem_view)
    out = (["--logs", str(tmp_path)] if nome == "kis_grade"
           else ["--out", str(tmp_path / "o.csv")])
    with pytest.raises(SystemExit, match="view deixou passar candle"):
        mod.main(["--end", end, "--store", db, "--symbols", *SYMS,
                  "--sem-guarda-relogio", *out])
