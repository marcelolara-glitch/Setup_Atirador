# tests/test_carteira.py — carteira de referencia (backtest/carteira.py).
# Briefing v3 de 22/11/2026, secao 5. Numeros esperados calculados a mao.
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
    assert m["dd4_pico"] == D1 + 12 * H and m["dd4_vale"] == D2 + 8 * H
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
