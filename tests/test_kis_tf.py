# tests/test_kis_tf.py — sandbox, etapa 3 do briefing v3 (14/11/2026)
import pytest

from backtest import kis_saida as ks
from backtest import kis_tf as kt
from tests.test_kis_saida import _store

H = kt.H_MS


def _c(ts, o, h, l, c, v=1.0):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_agregar_alinha_em_00utc_e_descarta_incompleto():
    d0 = ks._ms("2025-01-01")
    c4 = [_c(d0 - 4 * H, 1, 1, 1, 1)]                       # 20:00 da vespera
    c4 += [_c(d0 + j * 4 * H, 10 + j, 20 + j, 5 - j, 11 + j, 2) for j in range(6)]
    c4 += [_c(d0 + 24 * H, 1, 1, 1, 1), _c(d0 + 32 * H, 1, 1, 1, 1)]  # buraco
    d = kt.agregar(c4, 6)
    assert d == [{"ts": d0, "open": 10, "high": 25, "low": 0, "close": 16,
                  "volume": 12}]
    doze = kt.agregar(c4, 3)
    assert [x["ts"] for x in doze] == [d0, d0 + 12 * H]
    assert doze[1]["open"] == 13 and doze[1]["close"] == 16


def test_medir_igual_measure_event(tmp_path):
    from backtest.candle_store import read_candles
    from backtest.excursion import measure_event
    db = tmp_path / "c.db"
    _store(db, ["AAAUSDT"], "2025-06-01")
    conn = ks.conecta_com_teto(str(db), ks._ms(ks.TETO_ISO))
    cs = read_candles(conn, "AAAUSDT", "4h")
    for i in (40, 500, len(cs) - 100, len(cs) - 49, len(cs) - 47):
        for d in ("LONG", "SHORT"):
            a = measure_event(conn, "AAAUSDT", cs[i]["ts"], d, tf="4h",
                              path_cap=256)
            b = kt.medir(cs, i, d, 4 * H)
            if a is None:
                assert b is None
                continue
            for key in ("atr", "entry", "fwd_bar", "open_bar", "fav_bar",
                        "adv_bar"):
                assert a[key] == b[key]
    conn.close()


def test_4h_bate_com_etapa1(tmp_path):
    db = tmp_path / "c.db"
    syms = ["AAAUSDT", "BBBUSDT"]
    _store(db, syms, "2026-11-14")
    conn = ks.conecta_com_teto(str(db), ks._ms(ks.TETO_ISO))
    ref = ks.run(conn, syms, "4h", None, ks._ms(ks.TETO_ISO) - 1,
                 pares=((34, 89),))["por_par"][(34, 89)]
    from backtest.candle_store import read_candles
    meu, cont = [], {"inversoes": 0, "excl_borda": 0, "excl_hold": 0}
    for s in syms:
        meu += kt.trades_simbolo(read_candles(conn, s, "4h",
                                              end_ms=ks._ms(ks.TETO_ISO) - 1),
                                 s, 4 * H, cont)
    conn.close()
    meu = sorted((t for t in meu if t[7]), key=lambda t: t[0])
    assert len(meu) == len(ref) > 10
    for a, b in zip(meu, ref):
        assert a[:3] == b[:3] and a[3] == [b[3][0], b[3][2]]


def test_main_sem_1h_avisa_e_nao_tem_linha_1h(tmp_path, capsys):
    # 23/11/2026: a cobertura abaixo de 90% virou AVISO (era "NAO RODA"); sem
    # nenhuma vela 1h no store, o CSV continua sem linha de 1h.
    db = tmp_path / "c.db"
    _store(db, ["BTCUSDT", "ETHUSDT"], "2026-11-14")
    out = tmp_path / "o.csv"
    assert kt.main(["--store", str(db), "--symbols", "BTCUSDT", "ETHUSDT",
                    "--out", str(out), "--sem-guarda-relogio"]) == 0
    txt = capsys.readouterr().out
    assert "NAO RODA" not in txt and "AVISO: abaixo de 90%" in txt
    assert txt.rstrip().splitlines()[-1].startswith("caveat")
    tfs = {l.split(",")[1] for l in out.read_text().splitlines()[1:]}
    assert tfs == {"4h", "8h", "12h", "1D"}


def test_main_com_1h_roda(tmp_path, capsys):
    from backtest.candle_store import connect, read_candles, upsert_candles
    db = tmp_path / "c.db"
    _store(db, ["BTCUSDT"], "2026-11-14")
    conn = connect(str(db))
    c4 = read_candles(conn, "BTCUSDT", "4h")
    c1 = []
    for c in c4:                       # 1h sintetico: 4 velas por 4h
        for j in range(4):
            c1.append(_c(c["ts"] + j * H, c["open"], c["high"], c["low"],
                         c["close"], c["volume"] / 4))
    upsert_candles(conn, "BTCUSDT", "1h", c1)
    conn.close()
    out = tmp_path / "o.csv"
    assert kt.main(["--store", str(db), "--symbols", "BTCUSDT", "--out",
                    str(out), "--sem-guarda-relogio"]) == 0
    assert "RODA (so os que cobrem)" in capsys.readouterr().out
    assert "1h" in {l.split(",")[1] for l in out.read_text().splitlines()[1:]}


def test_end_alem_de_2026_08_26_e_aceito(tmp_path):
    # 23/11/2026: a recusa T1 saiu; o --end passa e a rodada so para na
    # validacao de entrada (store inexistente). Execucao completa com
    # --end alem do teto: tests/test_sem_travas_de_estudo.py.
    with pytest.raises(SystemExit, match="inexistente"):
        kt.main(["--end", "2026-10-01", "--store", str(tmp_path / "x.db"),
                 "--out", str(tmp_path / "o.csv")])
