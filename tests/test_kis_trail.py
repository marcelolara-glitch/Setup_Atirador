"""Testes puros do trail armado (backtest/kis_trail.py).

O topo de backtest/kis_trail.py e stdlib-only (candle_store/excursion sao
imports lazy dentro de `run`), entao este arquivo coleta no sandbox sem
pandas_ta. A parte que toca `measure_event` fica atras de um importorskip.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backtest.kis_trail import (ARMES, COST_BPS, GRADE, MIN_TRADES, RECUOS,
                                aprovada, linhas, metricas, trail_exit)


def _ts(dia: str) -> int:
    """'2024-06-01' -> epoch-ms UTC, mesma convencao de _iso_to_ms."""
    return int(datetime.fromisoformat(dia)
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


# ------------------------------------------------------------ grade fechada

def test_grade_tem_17_celulas_com_um_unico_controle():
    assert len(GRADE) == 17
    assert GRADE[0] == (0, 0)                       # controle: sem trail
    assert GRADE.count((0, 0)) == 1                 # arme=0 ignora recuo
    assert set(GRADE[1:]) == {(a, r) for a in ARMES for r in RECUOS}
    assert len(set(GRADE)) == len(GRADE)


def test_grade_e_a_do_briefing():
    assert ARMES == (1, 2, 3, 4) and RECUOS == (1, 2, 3, 4)


# --------------------------------------------------------------- trail_exit

def test_controle_arme_zero_sai_no_fim_do_hold():
    """arme=0 e o primario `extremos` do stage1: fecha na barra hold-1, sem
    olhar fav/adv. Qualquer caminho, mesmo um que sangraria em qualquer trail."""
    fav = [9.0, 9.0, 9.0, 9.0]
    adv = [9.0, 9.0, 9.0, 9.0]
    fwd = [0.1, 0.2, 0.3, 0.4]
    for recuo in RECUOS:
        assert trail_exit(fav, adv, fwd, 3, 0, recuo) == 0.3
    assert trail_exit(fav, adv, fwd, 1, 0, 1) == 0.1
    assert trail_exit(fav, adv, fwd, 4, 0, 1) == 0.4


def test_nunca_arma_cai_no_fim_do_hold():
    """MFE nunca alcanca `arme` -> nao ha stop, a saida e a inversao."""
    fav = [0.5, 0.9, 0.4]
    adv = [0.2, 0.1, 0.3]
    fwd = [0.1, 0.2, 0.25]
    assert trail_exit(fav, adv, fwd, 3, 2, 1) == 0.25


def test_arma_e_para_no_nivel_do_stop():
    """Arma na barra 1 (fav=3 >= arme=2); barra 2 mergulha e sai no stop
    3.0 - 1.0 = 2.0, nao no fechamento."""
    fav = [1.0, 3.0, 0.0]
    adv = [0.0, 0.0, 5.0]
    fwd = [0.1, 2.9, -4.0]
    assert trail_exit(fav, adv, fwd, 3, 2, 1) == 2.0


def test_stop_encostado_no_nivel_exato_sai():
    """Toque exato conta como saida (<=), igual a convencao do bracket."""
    fav = [4.0, 0.0]
    adv = [0.0, -3.0]        # minima da barra 1 exatamente em +3.0 ATR
    fwd = [3.9, 3.5]
    assert trail_exit(fav, adv, fwd, 2, 2, 1) == 3.0


def test_ordem_intrabarra_a_barra_que_arma_nao_para_em_si_mesma():
    """A barra 0 arma (fav=5) E tem minima em -9 ATR. Nao da para saber a
    ordem dentro da barra, entao o stop dela so vale a partir da barra 1 —
    e ate la a maxima da barra 1 ja subiu o nivel para 8.0 - 1.0 = 7.0.
    Checar depois de atualizar o MFE daria 4.0; a convencao documentada da 7.0."""
    fav = [5.0, 8.0, 6.0]
    adv = [9.0, -4.5, -6.0]  # barra 1 inteira ACIMA de +4.5 ATR: nao toca 4.0
    fwd = [1.0, 7.5, 6.2]
    assert trail_exit(fav, adv, fwd, 3, 2, 1) == 7.0


def test_recuo_maior_segura_o_trade_por_mais_tempo():
    """Mesmo caminho, recuo maior => stop mais longe => sobrevive ate o fim."""
    fav = [3.0, 0.0]
    adv = [0.0, 0.5]         # minima da barra 1 em -0.5 ATR
    fwd = [2.9, -0.4]
    assert trail_exit(fav, adv, fwd, 2, 2, 1) == 2.0      # stop 3-1=2.0: toca
    assert trail_exit(fav, adv, fwd, 2, 2, 4) == -0.4     # stop 3-4=-1.0: nao


def test_arme_maior_nao_arma_e_devolve_o_controle():
    fav = [3.0, 0.0]
    adv = [0.0, 2.0]
    fwd = [2.9, -1.5]
    assert trail_exit(fav, adv, fwd, 2, 5, 1) == trail_exit(fav, adv, fwd, 2, 0, 1)


def test_hold_limita_a_leitura_do_caminho():
    """So as `hold` primeiras barras existem para o trade; o resto do caminho
    (que pertence ao trade seguinte, ja invertido) nao pode vazar."""
    fav = [1.0, 1.0, 9.0]
    adv = [0.0, -0.5, 0.0]   # barra 1 nao encosta no stop (0.5 > 1.0-1.0)
    fwd = [0.4, 0.5, 8.0]
    assert trail_exit(fav, adv, fwd, 2, 1, 1) == 0.5      # nao ve a barra 2


# ----------------------------------------------------------------- metricas

def _trade(dia: str, ret: float):
    """Trade sintetico: as 17 celulas com o MESMO retorno, para isolar as
    metricas da regra de saida."""
    return (_ts(dia), [ret] * len(GRADE))


def test_metricas_separa_as_duas_metades_pela_data_de_entrada():
    trades = [_trade("2024-06-01", 10.0), _trade("2025-06-06", 5.0),
              _trade("2025-06-07", -3.0), _trade("2026-01-15", 8.0)]
    m = metricas(trades, 0)
    assert m["n_trades"] == 4
    assert m["ret_1a_metade_bps"] == 15.0      # 06-06 fecha a 1a metade
    assert m["ret_2a_metade_bps"] == 5.0       # 06-07 abre a 2a
    assert m["ret_total_bps"] == 20.0


def test_metricas_acerto_conta_so_retorno_estritamente_positivo():
    trades = [_trade("2024-06-01", 10.0), _trade("2024-07-01", 0.0),
              _trade("2024-08-01", -1.0), _trade("2024-09-01", 4.0)]
    assert metricas(trades, 0)["acerto_pct"] == 50.0


def test_metricas_drawdown_e_a_maior_queda_desde_o_topo():
    """Curva acumulada: 10, 4, -1, 9. Topo 10, fundo -1 => mergulho de 11."""
    trades = [_trade("2024-06-01", 10.0), _trade("2024-06-02", -6.0),
              _trade("2024-06-03", -5.0), _trade("2024-06-04", 10.0)]
    assert metricas(trades, 0)["dd_max_bps"] == 11.0


def test_metricas_drawdown_zero_em_curva_so_de_ganho():
    trades = [_trade("2024-06-01", 3.0), _trade("2024-06-02", 4.0)]
    assert metricas(trades, 0)["dd_max_bps"] == 0.0


def test_metricas_trimestres_por_data_de_entrada():
    """2024Q2 (+10), 2024Q3 (-2 no saldo), 2025Q1 (+1) => 2 de 3 positivos."""
    trades = [_trade("2024-06-01", 10.0), _trade("2024-08-01", 3.0),
              _trade("2024-09-01", -5.0), _trade("2025-02-01", 1.0)]
    m = metricas(trades, 0)
    assert (m["trimestres_pos"], m["trimestres_total"]) == (2, 3)


def test_metricas_de_lista_vazia_nao_explode():
    m = metricas([], 0)
    assert m["n_trades"] == 0 and m["acerto_pct"] == 0.0
    assert m["trimestres_total"] == 0 and m["dd_max_bps"] == 0.0


def test_metricas_le_a_celula_pedida():
    trades = [(_ts("2024-06-01"), [float(k) for k in range(len(GRADE))])]
    assert metricas(trades, 5)["ret_total_bps"] == 5.0


# ----------------------------------------------------------------- criterio

_OK = {"n_trades": 40, "ret_1a_metade_bps": 5.0, "ret_2a_metade_bps": 5.0,
       "trimestres_pos": 5, "trimestres_total": 8, "dd_max_bps": 9999.0}


def test_criterio_aprova_o_caso_completo():
    assert aprovada(dict(_OK)) is True


def test_criterio_reprova_poucos_trades():
    assert aprovada(dict(_OK, n_trades=MIN_TRADES - 1)) is False
    assert aprovada(dict(_OK, n_trades=MIN_TRADES)) is True


def test_criterio_reprova_metade_nao_positiva():
    assert aprovada(dict(_OK, ret_1a_metade_bps=-1.0)) is False
    assert aprovada(dict(_OK, ret_2a_metade_bps=0.0)) is False


def test_criterio_reprova_minoria_de_trimestres():
    assert aprovada(dict(_OK, trimestres_pos=3, trimestres_total=8)) is False
    assert aprovada(dict(_OK, trimestres_pos=4, trimestres_total=8)) is True


def test_criterio_ignora_drawdown_que_e_decisao_do_marcelo():
    assert aprovada(dict(_OK, dd_max_bps=10.0)) is True
    assert aprovada(dict(_OK, dd_max_bps=1e9)) is True


# -------------------------------------------------------------------- saida

def test_linhas_cobre_cada_simbolo_e_o_universo():
    por_sym = {"AAA": [_trade("2024-06-01", 2.0)],
               "BBB": [_trade("2025-07-01", 4.0)]}
    rows = linhas(por_sym)
    assert len(rows) == 3 * len(GRADE)                     # AAA, BBB, UNIVERSO
    uni = [r for r in rows if r["symbol"] == "UNIVERSO"]
    assert len(uni) == len(GRADE)
    assert uni[0]["n_trades"] == 2 and uni[0]["ret_total_bps"] == 6.0


def test_universo_ordena_por_data_e_nao_pela_ordem_dos_simbolos():
    """A curva do universo e a da carteira: o drawdown tem de ser calculado na
    ordem cronologica real, nao simbolo a simbolo."""
    por_sym = {"AAA": [_trade("2024-06-01", 10.0), _trade("2024-06-05", 10.0)],
               "BBB": [_trade("2024-06-03", -18.0)]}
    uni = [r for r in linhas(por_sym) if r["symbol"] == "UNIVERSO"][0]
    assert uni["dd_max_bps"] == 18.0        # 10 -> -8, e nao 20 -> 2


def test_custo_e_o_mesmo_do_stage1():
    from backtest import stage1
    assert COST_BPS == stage1.GATE_COST


# ------------------------------------ caminho longo de fav/adv em measure_event

def test_path_cap_e_aditivo_e_simetrico(monkeypatch):
    """path_cap=0 (default) nao muda NADA no dict devolvido; path_cap>0
    acrescenta fav_bar/adv_bar coerentes com fav/adv nas 48 primeiras barras,
    no comprimento de fwd_bar, e com a mesma simetria LONG<->SHORT que o
    stage1 ja assere na 1a barra elegivel de cada simbolo."""
    exc = pytest.importorskip("backtest.excursion")
    bar = exc._BAR_MS["4h"]
    serie = [{"ts": i * bar, "open": 100.0 + i, "high": 101.0 + i,
              "low": 99.0 + i, "close": 100.0 + i, "volume": 1.0}
             for i in range(400)]

    def fake_read(conn, symbol, tf, start_ms=None, end_ms=None):
        return [c for c in serie
                if (start_ms is None or c["ts"] >= start_ms)
                and (end_ms is None or c["ts"] <= end_ms)]

    monkeypatch.setattr(exc, "read_candles", fake_read)
    ts = 100 * bar
    base = exc.measure_event(None, "AAA", ts, "LONG", tf="4h")
    assert "fav_bar" not in base and "adv_bar" not in base

    longo = exc.measure_event(None, "AAA", ts, "LONG", tf="4h", path_cap=256)
    assert set(longo) - set(base) == {"fav_bar", "adv_bar"}
    for k in base:
        assert longo[k] == base[k]                    # nada antigo se moveu
    assert len(longo["fav_bar"]) == len(longo["fwd_bar"]) > max(exc.HORIZONS)
    assert longo["fav_bar"][:len(base["fav"])] == base["fav"]
    assert longo["adv_bar"][:len(base["adv"])] == base["adv"]

    short = exc.measure_event(None, "AAA", ts, "SHORT", tf="4h", path_cap=256)
    assert all(abs(a - b) < 1e-9
               for a, b in zip(short["fav_bar"], longo["adv_bar"]))
    assert all(abs(a - b) < 1e-9
               for a, b in zip(short["adv_bar"], longo["fav_bar"]))
