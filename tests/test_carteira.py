# tests/test_carteira.py — carteira de referencia (backtest/carteira.py).
# Briefing v3 de 22/11/2026, secao 5, e ajustes da grade de 23/11/2026
# (rebaixamento em duas medidas, liquidacao, CSV diario unico). Numeros
# esperados calculados a mao.
import pytest

from backtest import carteira as cart
from backtest.kis_saida import _ms

H = 3_600_000
D0 = _ms("2025-01-01")
D1, D2, D3 = D0 + 24 * H, D0 + 48 * H, D0 + 72 * H


def _cam(closes: dict, ini=D0, fim=D3):
    """closes = {ts: close} vigente a partir de ts; high/low = close +/- 1."""
    velas, c = [], None
    for ts in range(ini, fim, 4 * H):
        c = closes.get(ts, c)
        velas.append({"ts": ts, "high": c + 1, "low": c - 1, "close": c})
    return cart.caminho(velas)


def _caminhos():
    a = _cam({D0: 100.0, D1 + 12 * H: 110.0})
    b = _cam({D0: 50.0, D1 + 4 * H: 46.0, D2 + 8 * H: 52.0})
    return {"A": a, "B": b}


def _trades(rot="S/X"):
    return [
        {"rotulo": rot, "sym": "A", "direcao": "LONG", "ts_entrada": D0 + 8 * H,
         "preco_entrada": 100.0, "saidas": [(D1 + 12 * H, 110.0, 1.0)]},
        {"rotulo": rot, "sym": "B", "direcao": "SHORT",
         "ts_entrada": D0 + 20 * H, "preco_entrada": 50.0,
         "saidas": [(D1 + 4 * H, 45.0, 0.5), (D2 + 8 * H, 52.0, 0.5)]},
    ]


def test_banca_diaria_rebaixamento_exposicao_a_mao():
    r = cart.roda(_trades(), _caminhos())
    d = {x["data"]: x for x in r["diario"]}
    assert list(d) == ["2025-01-01", "2025-01-02", "2025-01-03"]
    # dia 0 (marca das 20:00): duas entradas, custo 0,15 cada, nada andou
    x = d["2025-01-01"]
    assert x["banca_usd"] == pytest.approx(10_000 - 0.30)
    assert x["realizado_dia_usd"] == pytest.approx(-0.30)
    assert (x["n_abertas"], x["n_long"], x["n_short"]) == (2, 1, 1)
    assert x["nocional_long_usd"] == pytest.approx(300.0)      # 3 x 100
    assert x["nocional_short_usd"] == pytest.approx(300.0)     # 6 x 50
    assert x["exposicao_x_banca"] == pytest.approx(600.0 / 9_999.70)
    assert (x["trades_abertos_dia"], x["trades_fechados_dia"]) == (2, 0)
    # dia 1: A sai +10 (3 x 10 - 3 x 110 x 5bps); B parcial 50% em 45
    rz_a = 30.0 - 0.165
    rz_b = 15.0 - 0.0675
    x = d["2025-01-02"]
    assert x["realizado_dia_usd"] == pytest.approx(rz_a + rz_b)
    assert x["nao_realizado_usd"] == pytest.approx(12.0)       # 3 un x 4
    pico = 10_000 - 0.30 + rz_a + rz_b + 12.0
    assert x["banca_usd"] == pytest.approx(pico)
    assert (x["n_abertas"], x["n_short"], x["trades_fechados_dia"]) == (1, 1, 1)
    assert x["nocional_short_usd"] == pytest.approx(3 * 46.0)
    # dia 2: resto de B sai em 52 (perde 2 x 3 un + custo)
    fim = 10_000 - 0.30 + rz_a + rz_b - 6.0 - 0.078
    x = d["2025-01-03"]
    assert x["banca_usd"] == pytest.approx(fim)
    assert x["rebaixamento_usd"] == pytest.approx(pico - fim)
    assert x["rebaixamento_pct"] == pytest.approx(100 * (pico - fim) / pico)
    assert x["n_abertas"] == 0
    m = r["metricas"]
    assert m["banca_final"] == pytest.approx(fim)
    assert m["dd4_usd"] == pytest.approx(pico - fim)
    assert m["dd4_usd_pico"] == D1 + 12 * H and m["dd4_usd_vale"] == D2 + 8 * H
    assert m["dd4_pct"] == pytest.approx(100 * (pico - fim) / pico)
    assert m["ddd_usd"] == pytest.approx(pico - fim)
    assert m["pos_total"][0] == 2 and m["pos_long"][0] == 1
    assert m["x_total"][0] == pytest.approx(600.0 / 9_999.70)
    # movimento adverso: A minima 99 (1%); B sai no close de D2 08:00 -> barra
    # inteira, maxima 53 (6%)
    assert r["adverso"] == pytest.approx([1.0, 6.0])


def test_dia_sem_trade_repete_a_banca():
    cam = {"A": _cam({D0: 100.0, D0 + 12 * H: 104.0})}
    tr = [{"rotulo": "S/X", "sym": "A", "direcao": "LONG",
           "ts_entrada": D0 + 4 * H, "preco_entrada": 100.0,
           "saidas": [(D0 + 12 * H, 104.0, 1.0)]},
          {"rotulo": "S/X", "sym": "A", "direcao": "SHORT",
           "ts_entrada": D2 + 4 * H, "preco_entrada": 104.0,
           "saidas": [(D2 + 12 * H, 104.0, 1.0)]}]
    d = {x["data"]: x for x in cart.roda(tr, cam)["diario"]}
    assert d["2025-01-02"]["banca_usd"] == d["2025-01-01"]["banca_usd"]
    assert d["2025-01-02"]["realizado_dia_usd"] == 0.0
    assert d["2025-01-02"]["trades_abertos_dia"] == 0


def test_registro_diario_usa_o_close_das_20h():
    # entrada as 20:00 entra no dia; o salto do preco a 00:00 fica no dia
    # seguinte, nao no fechamento do dia anterior
    cam = {"A": _cam({D0: 100.0, D1: 120.0})}
    tr = [{"rotulo": "S/X", "sym": "A", "direcao": "LONG",
           "ts_entrada": D0 + 20 * H, "preco_entrada": 100.0,
           "saidas": [(D1 + 8 * H, 120.0, 1.0)]}]
    d = {x["data"]: x for x in cart.roda(tr, cam)["diario"]}
    assert d["2025-01-01"]["trades_abertos_dia"] == 1
    assert d["2025-01-01"]["n_abertas"] == 1
    assert d["2025-01-01"]["nao_realizado_usd"] == 0.0
    assert d["2025-01-02"]["realizado_dia_usd"] == pytest.approx(
        60.0 - 3 * 120 * 5e-4)


def test_rotulos_misturados_levantam_erro():
    tr = _trades("S/X")
    tr[1]["rotulo"] = "S/Y"
    with pytest.raises(ValueError):
        cart.roda(tr, _caminhos())


def test_duas_celulas_em_separado_nao_se_tocam():
    cam = _caminhos()
    a1 = cart.roda(_trades("S/X")[:1], cam)
    b = cart.roda([dict(t, rotulo="S/Y") for t in _trades()[1:]], cam)
    a2 = cart.roda(_trades("S/X")[:1], cam)
    assert a1["marcas"] == a2["marcas"] and a1["diario"] == a2["diario"]
    assert b["marcas"][0][1] == pytest.approx(10_000 - 0.15)
    assert a1["metricas"]["banca_final"] == pytest.approx(
        10_000 - 0.15 + 30.0 - 0.165)


def test_fracoes_que_nao_somam_um_sao_erro():
    tr = _trades()
    tr[1]["saidas"] = [(D1, 45.0, 0.5)]
    with pytest.raises(ValueError):
        cart.roda(tr, _caminhos())


# ---- ajustes de 23/11/2026 --------------------------------------------------
def test_maior_rebaixamento_em_pct_e_em_usd_em_datas_diferentes():
    # 10000 -> 8000 (-20%, 2000 USD) -> 20000 -> 17500 (-12,5%, 2500 USD)
    serie = [(1, 10_000.0), (2, 8_000.0), (3, 20_000.0), (4, 17_500.0)]
    d = cart._dd(serie)
    assert d["pct"] == pytest.approx(20.0)
    assert (d["pct_pico"], d["pct_vale"]) == (1, 2)
    assert d["usd"] == pytest.approx(2_500.0)
    assert (d["usd_pico"], d["usd_vale"]) == (3, 4)


def test_dias_seguidos_abaixo_do_pico():
    di = [{"rebaixamento_usd": x} for x in (0, 5, 3, 0, 1, 2, 4, 0)]
    assert cart.dias_abaixo_pico(di) == 3


def _liq_trade(saidas, direcao="LONG"):
    return {"rotulo": "S/X", "sym": "A", "direcao": direcao,
            "ts_entrada": D0, "preco_entrada": 100.0, "saidas": saidas}


def _liq_cam():
    # entrada no close de D0 (100); barra 1 vai a 112 (high); barra 2 desce a
    # 88 (low: -12%); stop executado na barra 3 em 85
    v = [{"ts": D0, "high": 100.0, "low": 100.0, "close": 100.0},
         {"ts": D0 + 4 * H, "high": 112.0, "low": 99.0, "close": 106.0},
         {"ts": D0 + 8 * H, "high": 100.0, "low": 88.0, "close": 90.0},
         {"ts": D0 + 12 * H, "high": 90.0, "low": 84.0, "close": 84.0}]
    return cart.caminho(v)


def test_liquidacao_l10_perde_a_margem_l3_nao_liquida():
    t = _liq_trade([(D0 + 12 * H, 85.0, 1.0)])
    cam = _liq_cam()
    real = -0.15 + 3 * (-15.0 - 85.0 * 5e-4)
    liq, r, x = cart.liquidacao(t, cam, 10)       # limiar 1/10 - 1% = 9%
    assert liq and r == pytest.approx(real)
    assert x == pytest.approx(-0.15 - 30.0 - 3 * 91.0 * 5e-4)   # 30 + custos
    liq, r, x = cart.liquidacao(t, cam, 3)        # limiar 32,3%: nao chega
    assert not liq and x == r == pytest.approx(real)


def test_parcial_antes_da_liquidacao_reduz_a_perda():
    cam = _liq_cam()
    t = _liq_trade([(D0 + 4 * H, 110.0, 0.5), (D0 + 12 * H, 85.0, 0.5)])
    liq, _r, x = cart.liquidacao(t, cam, 10)
    parcial = 3 * 0.5 * (10.0 - 110.0 * 5e-4)
    assert liq
    assert x == pytest.approx(-0.15 + parcial - 15.0 - 3 * 0.5 * 91 * 5e-4)
    sem, = [cart.liquidacao(_liq_trade([(D0 + 12 * H, 85.0, 1.0)]), cam,
                            10)[2]]
    assert x > sem
    # parcial na MESMA barra da liquidacao nao conta (liquidacao primeiro)
    t2 = _liq_trade([(D0 + 8 * H, 101.0, 0.5), (D0 + 12 * H, 85.0, 0.5)])
    assert cart.liquidacao(t2, cam, 10)[2] == pytest.approx(sem)


def test_liquidacao_short_e_saida_antes_do_toque():
    cam = _liq_cam()
    # SHORT: high 112 na barra 1 = +12% contra -> liquida com L=10
    t = _liq_trade([(D0 + 12 * H, 85.0, 1.0)], "SHORT")
    assert cart.liquidacao(t, cam, 10)[0]
    # LONG que sai dentro da barra 2 em 95 (antes da minima): nao liquida
    t = _liq_trade([(D0 + 8 * H, 95.0, 1.0)])
    assert not cart.liquidacao(t, cam, 10)[0]


def test_duas_celulas_no_mesmo_csv_diario_nao_se_tocam(tmp_path):
    import csv
    cam = _caminhos()
    a = cart.roda(_trades("S/X")[:1], cam)
    b = cart.roda([dict(t, rotulo="S/Y") for t in _trades()[1:]], cam)
    a_so = cart.roda(_trades("S/X")[:1], cam)
    arq = tmp_path / "carteira_S_20261123_0000.csv"
    with cart.DiarioUnico(arq) as d:
        d.anexa("X", "H1", a["diario"])
        d.anexa("Y", "H1", b["diario"])
    rows = list(csv.DictReader(arq.open()))
    assert list(rows[0])[:2] == ["celula", "periodo"]
    xs = [r for r in rows if r["celula"] == "X"]
    ys = [r for r in rows if r["celula"] == "Y"]
    assert len(xs) == len(a_so["diario"]) and len(ys) == len(b["diario"])
    for r, dd in zip(xs, a_so["diario"]):
        assert float(r["banca_usd"]) == pytest.approx(dd["banca_usd"], abs=1e-4)
    for r, dd in zip(ys, b["diario"]):
        assert float(r["banca_usd"]) == pytest.approx(dd["banca_usd"], abs=1e-4)
