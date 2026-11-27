# tests/test_kis_grade_tf_maior.py — sandbox, briefing de 27/11/2026:
# kis_grade em 8h, 12h e 1D, velas agregadas em memoria do 4h
# (`kis_tf.agregar` via `kis_refino.le_velas`), duracoes na mesma regua do 4h
# (`kis_refino.escala` com fator fracao, arredondado para cima) e NOVO pela
# barra de fechamento do tf. 4h e 1h byte-identicos: gabaritos de
# test_sem_travas_de_estudo e test_kis_grade_escala (inalterados).
import re

import pytest

from backtest import kis_grade as g
from backtest import kis_refino as kr
from backtest import kis_saida as ks
from backtest import kis_tf as kt
from backtest.candle_store import connect, upsert_candles
from tests.test_kis_saida import _store

H = 3_600_000
DIA = 24 * H
FIM = "2026-11-20"
SYMS = ["BTCUSDT", "ETHUSDT"]


# ---- agregacao ----------------------------------------------------------------
def _dia_4h(d0: int) -> list:
    """6 velas de 4h de um dia UTC (00:00..20:00), extremos em velas do meio."""
    px = [(10, 11, 9.5, 10.5), (10.5, 14, 10, 13), (13, 13.5, 8, 9),
          (9, 10, 8.5, 9.8), (9.8, 12, 9.7, 11), (11, 11.5, 10.8, 11.2)]
    return [{"ts": d0 + k * 4 * H, "open": o, "high": h, "low": lo,
             "close": c, "volume": 1.0} for k, (o, h, lo, c) in enumerate(px)]


def _store_agregado(path, cs):
    conn = connect(str(path))
    upsert_candles(conn, "BTCUSDT", "4h", cs)
    return conn


def _confere_1d(conn, d0):
    """Le pelo caminho de producao (`le_velas`) e confere a vela 1D."""
    vs = kr.le_velas(conn, "BTCUSDT", kr.escala("1D"), d0 + 3 * DIA)
    assert len(vs) == 1, vs
    v = vs[0]
    assert (v["ts"], v["open"], v["high"], v["low"], v["close"]) == \
        (d0, 10, 14, 8, 11.2)


def test_seis_velas_de_4h_viram_uma_1d_e_incompleto_e_descartado(tmp_path):
    d0 = ks._ms("2026-03-10")
    cs = _dia_4h(d0) + _dia_4h(d0 + DIA)[:5]       # 2o dia sem a vela das 20h
    conn = _store_agregado(tmp_path / "a.db", cs)
    try:
        _confere_1d(conn, d0)
        # end no meio do 1o dia: bloco incompleto na ponta tambem sai
        assert kr.le_velas(conn, "BTCUSDT", kr.escala("1D"),
                           d0 + 16 * H) == []
    finally:
        conn.close()


def test_mutacao_bloco_desalinhado_em_4h_falha(tmp_path, monkeypatch):
    d0 = ks._ms("2026-03-10")
    conn = _store_agregado(tmp_path / "m.db",
                           _dia_4h(d0) + _dia_4h(d0 + DIA)[:5])
    orig = kt.agregar

    def desalinhado(c4h, n):                     # blocos comecam as 04:00
        s = 4 * H
        return [dict(c, ts=c["ts"] - s)
                for c in orig([dict(c, ts=c["ts"] + s) for c in c4h], n)]
    monkeypatch.setattr(kr, "agregar", desalinhado)
    try:
        with pytest.raises(AssertionError):
            _confere_1d(conn, d0)
    finally:
        conn.close()


@pytest.mark.parametrize("tf,n", [("8h", 2), ("12h", 3), ("1D", 6)])
def test_blocos_8h_12h_alinhados_a_00_utc(tf, n, tmp_path):
    d0 = ks._ms("2026-03-10")
    conn = _store_agregado(tmp_path / "b.db", _dia_4h(d0))
    try:
        vs = kr.le_velas(conn, "BTCUSDT", kr.escala(tf), d0 + DIA)
    finally:
        conn.close()
    assert [v["ts"] for v in vs] == [d0 + k * n * 4 * H for k in range(6 // n)]


# ---- escala -------------------------------------------------------------------
@pytest.mark.parametrize("tf,borda,cap,t1,tetos", [
    ("4h", 48, 256, (6, 12, 24), [0, 128, 256, 384]),
    ("1h", 192, 1024, (24, 48, 96), [0, 512, 1024, 1536]),
    ("8h", 24, 128, (3, 6, 12), [0, 64, 128, 192]),
    ("12h", 16, 86, (2, 4, 8), [0, 43, 86, 128]),
    ("1D", 8, 43, (1, 2, 4), [0, 22, 43, 64]),
])
def test_escala_borda_e_ext_cap_por_tf(tf, borda, cap, t1, tetos):
    e = kr.escala(tf)
    assert (e["borda"], e["cap"], e["t1_ns"]) == (borda, cap, t1)
    assert g.tetos_do_tf(e) == tetos
    assert all(isinstance(x, int) for x in (e["borda"], e["cap"], *t1, *tetos))
    assert e["bar_ms"] == {"4h": 4, "1h": 1, "8h": 8, "12h": 12,
                           "1D": 24}[tf] * H


@pytest.mark.parametrize("tf", ["2h", "6h", "3D", "x"])
def test_escala_recusa_tf_fora_da_regua(tf):
    with pytest.raises(ValueError):
        kr.escala(tf)


def test_janela_do_novo_pela_barra_de_fechamento():
    j = kr.JANELA
    assert kr.janela_tf(kr.escala("4h")) == j       # de sempre
    assert kr.janela_tf(kr.escala("1h")) == j
    for tf in ("8h", "12h", "1D"):
        e = kr.escala(tf)
        a, b = kr.janela_tf(e)
        # fechamento da barra (ts + bar) na faixa de fechamentos do 4h
        assert (a + e["bar_ms"], b + e["bar_ms"]) == \
            (j[0] + ks.BAR_4H_MS, j[1] + ks.BAR_4H_MS)
    fecha = lambda ts, tf: kr.na_janela(ts, kr.janela_tf(kr.escala(tf)))  # noqa
    assert fecha(ks._ms("2026-11-14 08:00"), "8h")      # fecha 16:00
    assert not fecha(ks._ms("2026-11-14 16:00"), "8h")  # fecha 24:00
    assert fecha(ks._ms("2026-11-13"), "1D")            # fecha 11-14 00:00
    assert not fecha(ks._ms("2026-11-14"), "1D")
    assert fecha(ks._ms("2026-08-31"), "1D")            # fecha 09-01 00:00
    assert not fecha(ks._ms("2026-08-30"), "1D")        # fecha 08-31 00:00


# ---- ponta a ponta ------------------------------------------------------------
@pytest.fixture(scope="module")
def db(tmp_path_factory):
    p = tmp_path_factory.mktemp("tfm") / "c.db"
    _store(p, SYMS, FIM)
    return str(p)


@pytest.mark.parametrize("tf,borda,cap", [("8h", 24, 128), ("12h", 16, 86),
                                          ("1D", 8, 43)])
def test_kis_grade_roda_com_conferencia_ligada(tf, borda, cap, db, tmp_path):
    assert g.main(["--tf", tf, "--store", db, "--symbols", *SYMS,
                   "--logs", str(tmp_path), "--sem-guarda-relogio",
                   "--ks", "3", "--parciais", "3-6", "--fracoes", "50-25",
                   "--tetos", "0"]) == 0
    log = next(tmp_path.glob("kis_grade_2*.log")).read_text()
    assert f"| tf {tf} |" in log                 # a coleta na VM separa por isso
    assert f"borda {borda})" in log and f"caminho/hold 256 -> {cap}" in log
    assert "agregadas em memoria" in log
    conf = re.findall(r"iguais (\d+) de (\d+) \| max\|dif\| 0\.00e\+00 bps \|"
                      r" so no kis_refino 0 \| so aqui 0", log)
    assert len(conf) == 6 and all(a == b for a, b in conf), log
    assert sum(int(a) for a, _ in conf) > 0
    rs = next(tmp_path.glob("kis_grade_resumo_*.csv")).read_text()
    assert "k3_p3-6_f50-25_t0" in rs and rs.count("\n") == 1 + 3 * 3
    # nenhuma entrada do NOVO com barra fechando depois de 2026-11-14 16:00
    bar = kr.escala(tf)["bar_ms"]
    tr = next(tmp_path.glob("kis_grade_2*.csv")).read_text().splitlines()
    cab = tr[0].split(",")
    i_p, i_ts = cab.index("periodo"), cab.index("bar_ts_iso")
    novos = [ln.split(",") for ln in tr[1:] if ln.split(",")[i_p] == "NOVO"]
    assert novos
    assert all(ks._ms(x[i_ts]) + bar <= ks._ms("2026-11-14 16:00")
               for x in novos)


def test_detector_de_vazamento_dispara_em_1d(db, tmp_path, monkeypatch):
    monkeypatch.setattr(g, "conecta_com_teto", lambda p, e: connect(p))
    with pytest.raises(SystemExit, match="vazamento|deixou passar"):
        g.main(["--tf", "1D", "--store", db, "--symbols", *SYMS,
                "--logs", str(tmp_path), "--sem-guarda-relogio",
                "--celulas", "REF_BASE"])
