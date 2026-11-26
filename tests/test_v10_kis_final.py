"""Motor do `kis_final` (porte de kis_refino.simula/gatilhos) — caminhos à mão.

Cada regra tem o seu teste, com valores fechados a mão (entrada 100, ATR 1,
stop 3 ATR = 97, parciais 50% em +3 = 103 e 25% em +6 = 106). Desligar a regra
no motor faz o teste dela falhar (mutação conferida no PR).
"""

from __future__ import annotations

import logging
from dataclasses import replace

import pytest

from backtest.kis_regime import avaliar, passa
from shadow.donchian_a import wilder_atr
from v10.exits import ADAPTADORES, adaptador
from v10.kis_final import (KisFinal, anotar_kis_final, detector_kis_final,
                           gatilhos)
from v10.registro import (KIS_3489_FINAL_4H, PARAMS_KIS_FINAL, REGISTRO,
                          SYMBOLS_KIS_3489)
from v10.runner import rodar
from v10.schema import TABELA, connect

BAR = 14_400_000
QUIET = logging.getLogger("v10_kis_final_test")
QUIET.addHandler(logging.NullHandler())
SPEC = replace(KIS_3489_FINAL_4H, exit_params={
    k: v for k, v in KIS_3489_FINAL_4H.exit_params.items()
    if k not in ("alav", "mmr")})                     # liquidação desligada
SPEC_LIQ = KIS_3489_FINAL_4H                          # alav 5, mmr 0,01


def _v(n, o, h, l, c, **kw):
    return dict(ts=n * BAR, open=o, high=h, low=l, close=c, **kw)


def _abre(direction="LONG", entry=100.0, atr=1.0, spec=SPEC):
    return KisFinal.abrir(spec, {"direction": direction, "entry_price": entry,
                                 "atr_value": atr}, _v(0, entry, entry, entry,
                                                       entry))


def _roda(velas, direction="LONG", entry=100.0, atr=1.0, spec=SPEC):
    return KisFinal.avaliar(spec, _abre(direction, entry, atr, spec), velas)


# --- stop ---------------------------------------------------------------------
def test_stop_fixo_em_3_atr():
    novo, st, px = _roda([_v(1, 99, 99.5, 96.5, 98)])
    assert (st, px) == ("STOP", 97.0)
    assert novo["exit_state"]["pnl_pct"] == pytest.approx(-3.0)


def test_stop_atravessado_na_abertura_sai_na_abertura():
    _, st, px = _roda([_v(1, 96, 96.5, 95, 96)])
    assert (st, px) == ("STOP", 96.0)


# --- parciais -----------------------------------------------------------------
def test_parcial_1_em_mais_3_atr_com_50_porcento():
    novo, st, _ = _roda([_v(1, 100, 103.2, 99.5, 103)])
    s = novo["exit_state"]
    assert st is None
    assert [(x["nome"], x["fracao"], x["preco"]) for x in s["parciais"]] == \
        [("parcial1", 0.5, 103.0)]
    assert s["fracao_restante"] == pytest.approx(0.5)
    assert s["pnl_realizado_pct"] == pytest.approx(1.5)


def test_parcial_2_em_mais_6_atr_com_25_porcento():
    novo, _, _ = _roda([_v(1, 100, 103.2, 99.5, 103),
                        _v(2, 103, 106.4, 102, 106)])
    s = novo["exit_state"]
    assert [(x["nome"], x["fracao"], x["preco"]) for x in s["parciais"]] == \
        [("parcial1", 0.5, 103.0), ("parcial2", 0.25, 106.0)]
    assert s["fracao_restante"] == pytest.approx(0.25)


def test_parcial_atravessada_na_abertura_sai_na_abertura():
    novo, _, _ = _roda([_v(1, 104, 104.5, 103.5, 104)])
    assert novo["exit_state"]["parciais"][0]["preco"] == 104.0


def test_parcial_nao_executa_duas_vezes_na_reavaliacao():
    velas = [_v(1, 100, 103.2, 99.5, 103)]
    novo, _, _ = KisFinal.avaliar(SPEC, _abre(), velas)
    novo, _, _ = KisFinal.avaliar(SPEC, novo, velas + [_v(2, 103, 103.5, 102,
                                                          103)])
    assert len(novo["exit_state"]["parciais"]) == 1


# --- stop x parcial na mesma barra: stop primeiro ------------------------------
def test_stop_e_parcial_na_mesma_barra_stop_primeiro():
    novo, st, px = _roda([_v(1, 100, 104, 96, 100)])
    assert (st, px) == ("STOP", 97.0)
    assert novo["exit_state"]["parciais"] == []
    assert novo["exit_state"]["pnl_pct"] == pytest.approx(-3.0)


# --- toque do gatilho -----------------------------------------------------------
def test_toque_do_gatilho_fecha_o_resto():
    novo, st, px = _roda([_v(1, 100, 100.5, 99.0, 100.2, kis_gS=99.5)])
    assert (st, px) == ("FLIP", 99.5)
    assert novo["exit_state"]["motivo"] == "flip"


def test_gatilho_abaixo_do_stop_nao_conta():
    _, st, px = _roda([_v(1, 100, 100.5, 96.0, 99, kis_gS=96.5)])
    assert (st, px) == ("STOP", 97.0)


def test_gatilho_atravessado_na_abertura():
    _, st, px = _roda([_v(1, 99.0, 99.2, 98.5, 99, kis_gS=99.5)])
    assert (st, px) == ("FLIP", 99.0)


def test_inversao_no_fechamento_fecha_o_resto_no_close():
    _, st, px = _roda([_v(1, 100, 100.5, 99.8, 100.1, kis_inv=-1)])
    assert (st, px) == ("FLIP", 100.1)


def test_pnl_ponderado_pelas_fracoes():
    velas = [_v(1, 100, 103.2, 99.5, 103), _v(2, 103, 106.4, 102, 106),
             _v(3, 106, 106.2, 100.5, 101, kis_gS=101.0)]
    novo, st, px = _roda(velas)
    assert (st, px) == ("FLIP", 101.0)
    # 0,5 x 3% + 0,25 x 6% + 0,25 x 1% (o preço sozinho daria 1%)
    assert novo["exit_state"]["pnl_pct"] == pytest.approx(3.25)


# --- short espelhado -------------------------------------------------------------
def test_short_espelhado():
    velas = [_v(1, 100, 100.5, 96.8, 97), _v(2, 97, 97.5, 93.6, 94),
             _v(3, 94, 99.5, 93.8, 99, kis_gL=99.0)]
    novo, st, px = _roda(velas, "SHORT")
    s = novo["exit_state"]
    assert [x["preco"] for x in s["parciais"]] == [97.0, 94.0]
    assert (st, px) == ("FLIP", 99.0)
    assert s["pnl_pct"] == pytest.approx(0.5 * 3 + 0.25 * 6 + 0.25 * 1)


def test_short_stop_acima():
    _, st, px = _roda([_v(1, 100, 103.5, 99.5, 101)], "SHORT")
    assert (st, px) == ("STOP", 103.0)


# --- liquidação (só medida) -------------------------------------------------------
def test_liquidacao_antes_do_stop():
    # ATR 8: stop em 76 (-24%), liquidação a 5x/mmr 1% em -19% = 81.
    novo, st, px = _roda([_v(1, 100, 101, 80, 85)], atr=8.0, spec=SPEC_LIQ)
    assert (st, px) == ("LIQUIDACAO", pytest.approx(81.0))
    assert novo["exit_state"]["pnl_pct"] == pytest.approx(-20.0)   # margem


def test_sem_alav_nao_liquida():
    _, st, _ = _roda([_v(1, 100, 101, 80, 85)], atr=8.0, spec=SPEC)
    assert st is None


def test_parcial_na_barra_da_liquidacao_nao_conta():
    # +3 ATR = 124 tocado na mesma barra que vai a 80.
    novo, st, _ = _roda([_v(1, 100, 125, 80, 90)], atr=8.0, spec=SPEC_LIQ)
    assert st == "LIQUIDACAO"
    assert novo["exit_state"]["parciais"] == []
    assert novo["exit_state"]["pnl_pct"] == pytest.approx(-20.0)


def test_liquidacao_so_da_fracao_restante():
    velas = [_v(1, 100, 125, 99, 120), _v(2, 120, 121, 80, 85)]
    novo, st, _ = _roda(velas, atr=8.0, spec=SPEC_LIQ)
    assert st == "LIQUIDACAO"
    assert novo["exit_state"]["pnl_pct"] == pytest.approx(0.5 * 24 - 0.5 * 20)


def test_stop_antes_da_liquidacao_nao_liquida():
    _, st, px = _roda([_v(1, 100, 101, 80, 85)], spec=SPEC_LIQ)
    assert (st, px) == ("STOP", 97.0)


def test_liquidacao_short():
    _, st, px = _roda([_v(1, 100, 120, 99, 115)], "SHORT", atr=8.0,
                      spec=SPEC_LIQ)
    assert (st, px) == ("LIQUIDACAO", pytest.approx(119.0))


# --- run perdida: ideal x real, como o reverse --------------------------------------
def test_shadow_com_atraso_sai_no_close_corrente():
    velas = [_v(1, 99, 99.5, 96.5, 98), _v(2, 98, 99, 97.5, 98.5)]
    novo, st, px = _roda(velas)
    s = novo["exit_state"]
    assert (st, px) == ("STOP", 98.5)
    assert (s["preco_ideal"], s["preco_real"], s["barras_atraso"]) == \
        (97.0, 98.5, 1)


def test_backtest_com_atraso_sai_no_ideal():
    velas = [_v(1, 99, 99.5, 96.5, 98), _v(2, 98, 99, 97.5, 98.5)]
    _, st, px = _roda(velas, spec=replace(SPEC, mode="backtest"))
    assert (st, px) == ("STOP", 97.0)


# --- gatilhos: a recursão da EMA acerta o lado do fechamento -------------------------
def _serie(n=400, seed=20261125):
    x, p, out = seed, 10_000, []
    for i in range(n):
        x = (1103515245 * x + 12345) % 2147483648
        p = max(2000, p + (x % 301) - 150)
        out.append({"ts": i * BAR, "open": p / 100, "high": (p + 80) / 100,
                    "low": (p - 80) / 100, "close": p / 100, "volume": 1.0})
    return out


def test_gatilho_long_e_o_preco_que_poe_o_estado_em_verde():
    from backtest.keepitsimple import states
    closes = [v["close"] for v in _serie()]
    gL, gS, _ = gatilhos(closes, 34, 89, 2)
    vistos = 0
    for t in range(100, len(closes)):
        for g, alvo in ((gL[t], "VERDE"), (gS[t], "VERM")):
            if g is None:
                continue
            vistos += 1
            sobe, desce = closes[:t] + [g * 1.0001], closes[:t] + [g * 0.9999]
            dentro, fora = (sobe, desce) if alvo == "VERDE" else (desce, sobe)
            assert states(dentro, 34, 89)[t] == alvo
            assert states(fora, 34, 89)[t] != alvo
    assert vistos > 50


# --- detector e anotador -------------------------------------------------------------
def test_detector_e_inversao_com_portao_e_atr_de_wilder():
    velas, p, n = _serie(), PARAMS_KIS_FINAL, 0
    for i in range(120, len(velas)):
        j = velas[:i + 1]
        s = detector_kis_final(j, p)
        _alvo, d, incl, adx = avaliar(j, 34, 89, 2, 13, 14, 5, 21)
        esperado = d if d and passa(d, incl, adx, 0.02, 11) else None
        assert (s or {}).get("direction") == esperado
        if s:
            n += 1
            assert s["entry_price"] == j[-1]["close"]
            assert s["atr_value"] == wilder_atr(j, 14)
    assert n >= 1


def test_anotador_marca_inversao_e_nao_muta():
    velas = _serie()
    out = anotar_kis_final(velas, PARAMS_KIS_FINAL)
    assert "kis_inv" not in velas[-1]
    invs = [i for i, v in enumerate(out) if v["kis_inv"]]
    assert invs and all(i >= 89 for i in invs)
    for i in invs:
        assert avaliar(velas[:i + 1], 34, 89, 2)[1] == \
            ("LONG" if out[i]["kis_inv"] > 0 else "SHORT")


# --- ficha e registro -------------------------------------------------------------------
def test_ficha_declara_o_que_o_briefing_pediu():
    s = KIS_3489_FINAL_4H
    assert (s.setup_id, s.tf, s.warmup_barras, s.exit_model) == \
        ("kis_3489_final_4h", "4H", 89, "kis_final")
    assert s.symbols == SYMBOLS_KIS_3489 and len(s.symbols) == 65
    assert s.exit_params == {"stop_k": 3, "parciais": [[3, 0.5], [6, 0.25]],
                             "gatilho": "toque", "janela_barras": 1000,
                             "mao_usdt": 450, "banca_usdt": 10000, "alav": 5,
                             "mmr": 0.01}
    assert (s.mode, s.executar, s.estado_ciclo) == ("shadow", False,
                                                    "proposto")
    assert s.custo_bps_por_perna == 5.0
    assert REGISTRO["kis_3489_final_4h"] is s
    assert adaptador("kis_final") is KisFinal and "kis_final" in ADAPTADORES


# --- integração com o runner: pnl ponderado e marcação ---------------------------------
def test_runner_grava_pnl_ponderado_e_marca_aberta(tmp_path):
    entrada = _v(0, 100, 100, 100, 100)

    def det(velas, params):
        if velas[-1]["ts"] == 0:
            return {"direction": "LONG", "entry_price": 100.0,
                    "atr_value": 1.0}
        return None
    spec = replace(SPEC, detector=det, symbols=["X"], warmup_barras=1)
    trilha = [entrada, _v(1, 100, 103.2, 99.5, 102)]
    conn = connect(tmp_path / "j.db")
    rodar(spec, conn, agora_ms=BAR, velas_fn=lambda *a: trilha[:1], log=QUIET)
    rodar(spec, conn, agora_ms=2 * BAR, velas_fn=lambda *a: trilha, log=QUIET)
    r = conn.execute(f"SELECT status, exit_state_json FROM {TABELA}").fetchone()
    import json
    s = json.loads(r["exit_state_json"])
    assert r["status"] == "OPEN"
    assert s["pnl_corrente_pct"] == pytest.approx(0.5 * 3 + 0.5 * 2)
    trilha.append(_v(2, 102, 102.5, 100.5, 101, kis_gS=101.0))
    rodar(spec, conn, agora_ms=3 * BAR, velas_fn=lambda *a: trilha, log=QUIET)
    r = conn.execute(f"SELECT status, exit_price, pnl_pct, pnl_bps_liq "
                     f"FROM {TABELA}").fetchone()
    assert (r["status"], r["exit_price"]) == ("FLIP", 101.0)
    assert r["pnl_pct"] == pytest.approx(0.5 * 3 + 0.5 * 1)
    assert r["pnl_bps_liq"] == pytest.approx(200.0 - 10.0)
