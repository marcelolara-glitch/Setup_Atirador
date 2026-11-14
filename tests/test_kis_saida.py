# tests/test_kis_saida.py — sandbox, etapa 1 do briefing v3 (14/11/2026)
import random

import pytest

from backtest import kis_saida as ks
from backtest.candle_store import connect, read_candles, upsert_candles

T0 = ks._ms("2024-06-17")
BAR = ks.BAR_4H_MS


def _store(path, syms, ate_iso, seed=7):
    conn = connect(path)
    rnd = random.Random(seed)
    fim = ks._ms(ate_iso)
    for s in syms:
        p, ks_ = 100.0, []
        ts = T0
        while ts < fim:
            o = p
            p = max(1.0, p * (1 + rnd.gauss(0.0004, 0.02)))
            hi, lo = max(o, p) * (1 + abs(rnd.gauss(0, .006))), \
                min(o, p) * (1 - abs(rnd.gauss(0, .006)))
            ks_.append({"ts": ts, "open": o, "high": hi, "low": lo, "close": p,
                        "volume": 1000 + rnd.random() * 500})
            ts += BAR
        upsert_candles(conn, s, "4h", ks_)
    conn.close()


# ---- regra de saida ---------------------------------------------------------
def test_sem_stop_sai_na_inversao():
    r, parou = ks.stop_exit([1, 1, 1], [0.5, 0.5, 0.5], [0, 0, 0],
                            [0.1, 0.2, 0.3], 3, 2, False)
    assert (r, parou) == (0.3, False)


def test_stop_fixo_no_nivel_e_no_gap():
    r, p = ks.stop_exit([0, 0], [1, 2.5], [0, -1], [0, -2.4], 2, 2, False)
    assert (r, p) == (-2, True)                 # abre acima do nivel -> nivel
    r, p = ks.stop_exit([0, 0], [1, 3.5], [0, -3], [0, -3.4], 2, 2, False)
    assert (r, p) == (-3, True)                 # abre alem do nivel -> abertura


def test_chandelier_usa_so_barras_anteriores():
    # barra 0 sobe a 5 e desce a -1.5: nivel da barra 0 e -2 (melhor=0) -> nao para
    # barra 1 desce a 2.5 com nivel 5-2=3 -> para em 3 (abre em 4)
    r, p = ks.stop_exit([5, 4], [1.5, -2.5], [0.2, 4], [1, 2.6], 2, 2, True)
    assert (r, p) == (3, True)
    # mesma serie com stop FIXO nao para
    r, p = ks.stop_exit([5, 4], [1.5, -2.5], [0.2, 4], [1, 2.6], 2, 2, False)
    assert (r, p) == (2.6, False)


# ---- T1 ---------------------------------------------------------------------
def test_view_do_teto_esconde_holdout(tmp_path):
    db = tmp_path / "c.db"
    _store(db, ["AAAUSDT"], "2026-11-14")
    teto = ks._ms(ks.TETO_ISO)
    conn = ks.conecta_com_teto(str(db), teto)
    todas = read_candles(conn, "AAAUSDT", "4h")
    assert todas and max(c["ts"] for c in todas) < teto
    assert conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0] < teto
    from backtest.excursion import measure_event
    m = measure_event(conn, "AAAUSDT", teto - 60 * BAR, "LONG", tf="4h",
                      path_cap=256)
    assert len(m["fwd_bar"]) == 59              # para no teto, nao em 256
    conn.close()
    # a tabela real continua intacta (prova de que o filtro e da view)
    raw = connect(str(db))
    assert raw.execute("SELECT MAX(ts) FROM candles").fetchone()[0] >= teto
    raw.close()


def test_end_alem_do_teto_aborta(tmp_path):
    with pytest.raises(SystemExit, match="T1"):
        ks.main(["--end", "2026-08-27", "--store", str(tmp_path / "x.db")])


def test_store_inexistente_aborta(tmp_path):
    with pytest.raises(SystemExit, match="inexistente"):
        ks.conecta_com_teto(str(tmp_path / "nao.db"), 0)


# ---- reuso: E0 e o kis_horizonte, bit a bit ---------------------------------
def test_e0_igual_kis_horizonte(tmp_path):
    from backtest import kis_horizonte as kh
    db = tmp_path / "c.db"
    syms = ["AAAUSDT", "BBBUSDT"]
    _store(db, syms, "2026-03-01")
    conn = ks.conecta_com_teto(str(db), ks._ms(ks.TETO_ISO))
    meu = ks.run(conn, syms, "4h", ks._ms("2024-05-22"), ks._ms("2025-12-01"),
                 pares=((34, 89), (8, 21)))
    conn.close()
    dele = kh.run(str(db), syms, "2024-05-22", "2025-12-01", "4h")
    for par in ((34, 89), (8, 21)):
        a = [(t[0], t[3][0]) for t in meu["por_par"][par]]
        b = sorted(((ts, r[0]) for s in syms
                    for ts, r in kh.celula(dele["por_sym"][s][par], 1)))
        assert len(a) > 5 and sorted(a) == b


def test_main_ponta_a_ponta(tmp_path, capsys):
    db = tmp_path / "c.db"
    _store(db, ["BTCUSDT", "ETHUSDT", "AAAUSDT"], "2026-11-14")
    out = tmp_path / "o.csv"
    assert ks.main(["--store", str(db), "--symbols", "BTCUSDT", "ETHUSDT",
                    "AAAUSDT", "--out", str(out), "--sem-guarda-relogio"]) == 0
    txt = capsys.readouterr().out
    assert "CONFERENCIA" in txt
    assert txt.rstrip().splitlines()[-1].startswith("caveat")
    linhas = out.read_text().splitlines()
    assert linhas[0].startswith("recorte,par,celula,metade")
    # 5 recortes (UNIVERSO, TIER1, 3 simbolos) x 2 pares x 7 celulas x 2 metades
    assert len(linhas) - 1 == 5 * 2 * 7 * 2
