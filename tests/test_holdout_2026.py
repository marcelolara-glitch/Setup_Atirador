# tests/test_holdout_2026.py — sandbox, briefing v2 de 22/11/2026.
# Testes de CORRECAO do codigo (paridade, periodo, marcacao no fim). Nenhum
# criterio de decisao mora aqui.
import csv
import random

from backtest import aio_graduado as ag
from backtest import holdout_2026 as h
from backtest import kis_saida as ks
from backtest.candle_store import connect, read_candles, upsert_candles

BAR = ks.BAR_4H_MS
T0 = ks._ms("2024-06-17")
SYMS = ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"]
BORDA = 48


def _store(path, syms, ate_iso, seed=7):
    conn = connect(path)
    rnd = random.Random(seed)
    fim = ks._ms(ate_iso)
    for s in syms:
        p, vs, ts = 100.0, [], T0
        while ts < fim:
            o = p
            p = max(1.0, p * (1 + rnd.gauss(0.0004, 0.02)))
            vs.append({"ts": ts, "open": o, "close": p,
                       "high": max(o, p) * (1 + abs(rnd.gauss(0, .006))),
                       "low": min(o, p) * (1 - abs(rnd.gauss(0, .006))),
                       "volume": 1000 * 2.718281828 ** rnd.gauss(0, 0.6)})
            ts += BAR
        upsert_candles(conn, s, "4h", vs)
    conn.close()


def _chave(trades, setup):
    return {(t["ts"], t["sym"]): t for t in trades if t["setup"] == setup}


# ---- 1. paridade com o sandbox, serie cortada em 2026-08-26 ----------------
def _sandbox_e_meu(tmp_path):
    db = tmp_path / "c.db"
    _store(db, SYMS, "2026-11-14")              # o teto esconde o resto
    teto = ks._ms(ks.TETO_ISO)
    conn = ks.conecta_com_teto(str(db), teto)
    velas = {s: read_candles(conn, s, "4h") for s in SYMS}
    assert all(v[-1]["ts"] == teto - BAR for v in velas.values())
    return conn, velas


def test_paridade_kis_e0_e_e1_3(tmp_path):
    conn, velas = _sandbox_e_meu(tmp_path)
    ref = ks.run(conn, SYMS, "4h", None, ks._ms(ks.TETO_ISO) - 1,
                 pares=((34, 89),))["por_par"][(34, 89)]
    conn.close()
    meus = []
    for s in SYMS:
        meus += h.trades_kis(velas[s], s, h.novo_cont(), janela=None)
    k3 = ks.NOMES.index("E1-3")
    for setup, k in (("KIS_E0", 0), ("KIS_E1-3", k3)):
        m = _chave(meus, setup)
        assert len(ref) > 100
        for t in ref:                           # tudo que o sandbox mede
            x = m.pop((t[0], t[1]))
            assert x["ret6"] == t[3][k]         # identico, sem tolerancia
            assert x["long"] == t[2]
            assert x["parou_stop"] == bool(t[4] >> k & 1)
            assert x["marcado_fim"] == (t[6] and not x["parou_stop"])
        # o que sobra e SO o que a borda de 48 barras jogava fora
        assert m
        for (ts, s), _x in m.items():
            assert ts > velas[s][-1]["ts"] - BORDA * BAR


def test_paridade_aio_g1_s2(tmp_path):
    conn, velas = _sandbox_e_meu(tmp_path)
    conn.close()
    n_ref = 0
    for s in SYMS:
        ref = ag.trades_simbolo(velas[s], s, {"excl_borda": 0, "excl_hold": 0},
                                celula="G1")
        m = _chave(h.trades_aio(velas[s], s, h.novo_cont(), janela=None),
                   "AIO_G1")
        n_ref += len(ref)
        for t in ref:
            x = m.pop((t[0], t[1]))
            assert x["ret6"] == t[3][1]         # coluna S2
            assert x["long"] == t[2]
            assert x["parou_stop"] == (t[4] == 2)
            if not x["parou_stop"]:
                assert x["hold"] == t[7]["hold"]
        for (ts, _s), _x in m.items():
            assert ts > velas[s][-1]["ts"] - BORDA * BAR
    assert n_ref > 50


# ---- 2. periodo -------------------------------------------------------------
def test_pontas_do_periodo():
    assert not h.na_janela(ks._ms("2026-08-31 12:00"))
    assert h.na_janela(ks._ms("2026-08-31 16:00"))
    assert h.na_janela(ks._ms("2026-11-14 12:00"))
    assert not h.na_janela(ks._ms("2026-11-14 16:00"))


def _velas_planas(ate_iso, n=600, queda=None):
    """Preco 100 com barras de amplitude 1 (ATR14 = 1); `queda` = {idx: low}."""
    fim = ks._ms(ate_iso)
    out = []
    for k in range(n):
        out.append({"ts": fim - (n - 1 - k) * BAR, "open": 100.0,
                    "close": 100.0, "high": 100.5,
                    "low": (queda or {}).get(k, 99.5), "volume": 1000.0})
    return out


def _forca_kis(monkeypatch, cndl, eventos):
    """Entradas KIS postas a mao: [(ts_iso, direcao)], hold ate a proxima."""
    pos = {c["ts"]: i for i, c in enumerate(cndl)}
    idx = [pos[ks._ms(iso)] for iso, _d in eventos] + [len(cndl) - 1]
    evs = [{"bar_ts": ks._ms(iso), "direction": d,
            "hold": min(max(idx[k + 1] - idx[k], 1), h.EXT_CAP)}
           for k, (iso, d) in enumerate(eventos)]
    monkeypatch.setattr(h, "entradas", lambda *a, **kw: evs)
    monkeypatch.setattr(h, "passa", lambda *a, **kw: True)


def test_entrada_1200_fica_fora_1600_entra_e_ultima_barra_entra(monkeypatch):
    c = _velas_planas("2026-11-14 12:00")
    _forca_kis(monkeypatch, c, [("2026-08-31 12:00", "LONG"),
                                ("2026-08-31 16:00", "SHORT"),
                                ("2026-11-14 12:00", "LONG")])
    cont = h.novo_cont()
    e0 = _chave(h.trades_kis(c, "AAAUSDT", cont), "KIS_E0")
    assert sorted(ts for ts, _s in e0) == [ks._ms("2026-08-31 16:00"),
                                           ks._ms("2026-11-14 12:00")]
    assert cont["kis_inversoes"] == 2           # a de 12:00 nem e contada
    ult = e0[(ks._ms("2026-11-14 12:00"), "AAAUSDT")]
    assert (ult["hold"], ult["marcado_fim"]) == (0, True)
    assert (ult["ret6"], ult["ret10"]) == (-6.0, -10.0)
    # a posicao de 16:00 fecha na inversao seguinte, nao no fim do dado
    assert not e0[(ks._ms("2026-08-31 16:00"), "AAAUSDT")]["marcado_fim"]


def test_view_do_fim_e_main_de_ponta_a_ponta(tmp_path):
    db, out = tmp_path / "c.db", tmp_path / "h.csv"
    _store(db, SYMS, "2026-11-20")              # ha dado DEPOIS do --end
    assert h.main(["--store", str(db), "--symbols", *SYMS, "--out", str(out),
                   "--sem-guarda-relogio"]) == 0
    rows = list(csv.DictReader(out.open()))
    assert list(rows[0]) == h.CAMPOS
    assert {r["setup"] for r in rows} == set(h.SETUPS)
    assert all("2026-08-31 16:00" <= r["bar_ts_iso"] <= "2026-11-14 12:00"
               for r in rows)
    log = out.with_suffix(".log").read_text().splitlines()
    assert log[-1].startswith("caveat:")
    assert any("maior ts lido 2026-11-14 12:00" in ln for ln in log)
    assert any("completos 4 de 4" in ln for ln in log)


def test_cobertura_acusa_token_com_falta(tmp_path):
    db = tmp_path / "c.db"
    _store(db, SYMS, "2026-11-14 16:00")
    conn = connect(db)
    conn.execute("DELETE FROM candles WHERE symbol='BBBUSDT' AND ts=?",
                 (ks._ms("2026-10-01 00:00"),))
    conn.commit()
    cob = h.cobertura(conn, SYMS, "4h", h.JANELA[0], ks._ms(h.END_ISO))
    conn.close()
    assert cob == {**dict.fromkeys(SYMS, 450), "BBBUSDT": 449}


# ---- 3. marcado no fim ------------------------------------------------------
def test_aberto_ate_o_fim_sai_no_close_da_ultima_barra(monkeypatch):
    c = _velas_planas("2026-11-14 12:00")
    c[-1]["close"] = 101.0                      # +1 ATR na ultima barra
    c[-1]["high"] = 101.0
    _forca_kis(monkeypatch, c, [("2026-11-10 00:00", "LONG")])
    tr = h.trades_kis(c, "AAAUSDT", h.novo_cont())
    assert len(tr) == 2
    for t in tr:                                # E0 e E1-3: nenhum stop toca
        assert (t["marcado_fim"], t["parou_stop"]) == (True, False)
        assert t["hold"] == 27                  # 10/11 00:00 -> 14/11 12:00
        assert abs(t["ret6"] - (100.0 - 6.0)) < 1e-9    # 1% = 100 bps, - custo
        assert abs(t["ret10"] - (100.0 - 10.0)) < 1e-9


def test_stop_antes_do_fim_sai_no_stop(monkeypatch):
    c = _velas_planas("2026-11-14 12:00")
    i = next(k for k, x in enumerate(c) if x["ts"] == ks._ms("2026-11-10 00:00"))
    c[i + 5]["low"] = 96.0                      # -4 ATR na 5a barra a frente
    c[-1]["close"] = 101.0
    c[-1]["high"] = 101.0
    _forca_kis(monkeypatch, c, [("2026-11-10 00:00", "LONG")])
    tr = {t["setup"]: t for t in h.trades_kis(c, "AAAUSDT", h.novo_cont())}
    e0, e13 = tr["KIS_E0"], tr["KIS_E1-3"]
    assert (e0["marcado_fim"], e0["parou_stop"], e0["hold"]) == (True, False, 27)
    assert (e13["marcado_fim"], e13["parou_stop"], e13["hold"]) == (False, True, 5)
    assert abs(e13["ret6"] - (-300.0 - 6.0)) < 1e-6     # -3 ATR = -3% = -300 bps


def test_medir_sem_borda_igual_ao_medir_longe_da_borda():
    from backtest.kis_tf import medir
    rnd = random.Random(3)
    c = _velas_planas("2026-11-14 12:00", n=300)
    for x in c:
        x["close"] = 100 + rnd.uniform(-0.4, 0.4)
    assert h.medir_sem_borda(c, 100, "SHORT") == medir(c, 100, "SHORT", BAR)
    assert medir(c, 280, "LONG", BAR) is None           # borda do sandbox
    assert len(h.medir_sem_borda(c, 280, "LONG")["fwd_bar"]) == 19
    assert h.medir_sem_borda(c, 299, "LONG")["fwd_bar"] == []
