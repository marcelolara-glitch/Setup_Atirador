# tests/test_kis_knn.py — sandbox, etapa 2 do briefing v3 (14/11/2026)
import pytest

from backtest import kis_knn as kk


def _g(ts, sym, long, close, adx=50.0, mfi=50.0):
    return {"ts": ts, "sym": sym, "long": long, "adx": adx, "mfi": mfi,
            "close": close}


def _cenario():
    # A@1 (close 100) -> A@3 (close 50): rotulo -1, resolve em 3
    # A@3 (close 50)  -> A@10 (close 40): rotulo -1, resolve em 10 (= t: vale)
    # B@5 (close 100) -> B@20 (close 200): rotulo +1, resolve em 20 > 10
    # Decisao de A@10 SHORT: memoria correta {-1,-1} soma -2 -> LIBERA.
    # Com o rotulo de B vazado: soma -1 -> VETA.
    return [_g(1, "A", True, 100), _g(3, "A", True, 50), _g(5, "B", True, 100),
            _g(10, "A", False, 40), _g(20, "B", False, 200)]


def test_lookahead_rotulo_resolvido_depois_nao_entra():
    est = kk.decidir(_cenario())
    assert est[3][0] == "libera" and est[3][2] == -2


def test_mutacao_ordenar_por_gatilho_reprova():
    est = kk.decidir(_cenario(), chave_memoria="gatilho")
    assert est[3][0] == "veta"           # o teste acima pega essa mutacao


def test_memoria_vazia_e_sem_feature_passam():
    g = [_g(1, "A", True, 100), _g(2, "A", False, 90, adx=None)]
    est = kk.decidir(g)
    assert est[0][0] == "vazia" and est[1][0] == "sem_feature"


def test_empate_no_k_esimo_entra_todo():
    # 3 pontos a distancia 0 (+1,+1,-1) e k=2: empate -> soma +1 (veta LONG);
    # sem incluir empates, k=2 poderia dar +2 e liberar.
    g = [_g(1, "A", True, 10), _g(2, "B", True, 10), _g(3, "C", True, 10),
         _g(4, "A", True, 11), _g(4, "B", True, 11), _g(4, "C", True, 9),
         _g(5, "D", True, 1)]
    est = kk.decidir(g, k=2)
    assert est[6][2] == 1 and est[6][0] == "veta"


def test_placebo_mesma_contagem_por_lado():
    g = [_g(i, "S%d" % (i % 3), i % 2 == 0, 100 + (i * 7) % 13,
            adx=float(i % 40), mfi=float((i * 3) % 90)) for i in range(200)]
    est = kk.decidir(g, k=5)
    for s in (0, 1, 2):
        v = kk.placebo(g, est, s)
        for lado in (True, False):
            nk = sum(1 for gg, e in zip(g, est) if gg["long"] is lado
                     and e[0] == "veta")
            assert sum(1 for i in v if g[i]["long"] is lado) == nk
            assert all(est[i][0] in ("libera", "veta") for i in v)
    assert kk.placebo(g, est, 0) == kk.placebo(g, est, 0)


def test_rma_semente_sma():
    assert kk._rma([None, 2, 4, 6, 8], 3) == [None, None, None, 4.0,
                                            4 + (8 - 4) / 3]


def test_mfi_extremos():
    up = [{"open": p, "high": p, "low": p, "close": p, "volume": 1.0}
          for p in range(1, 30)]
    assert kk.mfi_knn(up)[13] == 100.0 and kk.mfi_knn(up)[12] is None
    dn = list(reversed(up))
    assert kk.mfi_knn(dn)[20] == 0.0


def test_adx_tendencia_pura_vai_a_100():
    c = [{"open": p, "high": p + 1, "low": p - 1, "close": p, "volume": 1}
         for p in range(1, 80)]
    a = kk.adx_knn(c)
    assert a[17] is None or a[17] > 99          # so plusDM: ratio 1
    assert a[-1] == pytest.approx(100.0)


def test_main_ponta_a_ponta(tmp_path, capsys):
    from tests.test_kis_saida import _store
    db = tmp_path / "c.db"
    _store(db, ["BTCUSDT", "ETHUSDT", "AAAUSDT"], "2026-11-14")
    out = tmp_path / "o.csv"
    assert kk.main(["--store", str(db), "--symbols", "BTCUSDT", "ETHUSDT",
                    "AAAUSDT", "--out", str(out),
                    "--sem-guarda-relogio"]) == 0
    txt = capsys.readouterr().out
    assert txt.rstrip().splitlines()[-1].startswith("caveat")
    assert "kNN acima de" in txt


def test_end_alem_do_teto_aborta(tmp_path):
    with pytest.raises(SystemExit, match="T1"):
        kk.main(["--end", "2026-09-01", "--store", str(tmp_path / "x.db")])
