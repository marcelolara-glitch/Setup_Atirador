"""Runner do v10 + adaptador `bracket_simples`. Valores fechados A MAO.

Nada aqui depende de rede, de pandas_ta ou do banco da VM: as velas sao
sinteticas e a fonte e injetada. O que se trava:

  - `bracket_simples` reproduz as tres decisoes de shadow.donchian_a
    (empate pessimista, EXPIRED no fecho da barra H, barra de expiracao
    ausente => segue OPEN) e o denominador do r_multiple;
  - o runner so avalia barra FECHADA, respeita a cadencia na grade absoluta,
    resolve aberta ANTES de abrir nova, aplica espacamento por direcao e
    isola falha de simbolo e de setup.
"""

from __future__ import annotations

import logging

import pytest

from v10.exits import ADAPTADORES, BracketSimples, adaptador
from v10.runner import _bar_ms, _pnl_pct, rodar, rodar_todos
from v10.schema import TABELA, connect
from v10.spec import SetupSpec

BAR = 14_400_000                                   # 4h em ms
QUIET = logging.getLogger("v10_runner_test")
QUIET.addHandler(logging.NullHandler())


def _b(ts, o=100.0, h=100.0, l=100.0, c=100.0, **extra):
    return dict(ts=ts, open=o, high=h, low=l, close=c, volume=1.0, **extra)


def _spec(**kw) -> SetupSpec:
    base = dict(setup_id="t", detector=lambda v: None, tf="4H", cadencia_barras=1,
                symbols=["BTCUSDT"], warmup_barras=2, exit_model="bracket_simples",
                exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4,
                             "bar_ms": BAR},
                custo_bps_por_perna=5.0)
    base.update(kw)
    return SetupSpec(**base)


def _abrir(direction="LONG", entry=100.0, atr=1.0, ts=0, **cfg):
    spec = _spec(exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4,
                              "bar_ms": BAR, **cfg})
    sinal = {"direction": direction, "entry_price": entry, "atr_value": atr}
    return spec, BracketSimples.abrir(spec, sinal, _b(ts))


# --- bracket_simples: precos derivados ---------------------------------------
def test_sl_e_tp_saem_do_atr():
    _, e = _abrir()                                # 100 - 1.5*1 ; 100 + 6*1
    assert e["exit_params"]["sl_price"] == 98.5
    assert e["exit_params"]["tp_price"] == 106.0
    assert e["exit_params"]["expiry_ts_ms"] == 4 * BAR


def test_short_e_o_espelho_invertido():
    _, e = _abrir("SHORT")
    assert e["exit_params"]["sl_price"] == 101.5
    assert e["exit_params"]["tp_price"] == 94.0


def test_abrir_nao_inventa_coluna_de_saida():
    _, e = _abrir()
    assert set(e) == {"exit_params", "exit_state"}
    assert e["exit_state"]["status"] == "OPEN"
    assert e["exit_state"]["r_multiple"] is None


# --- bracket_simples: resolucao ----------------------------------------------
def test_win_no_alvo():
    spec, e = _abrir()
    velas = [_b(BAR, h=101.0, l=99.0, c=100.0), _b(2 * BAR, h=106.5, l=105.0, c=106.0)]
    _, status, preco = BracketSimples.avaliar(spec, e, velas)
    assert (status, preco) == ("WIN", 106.0)


def test_loss_no_stop():
    spec, e = _abrir()
    _, status, preco = BracketSimples.avaliar(spec, e, [_b(BAR, h=101.0, l=98.0)])
    assert (status, preco) == ("LOSS", 98.5)


def test_empate_alvo_e_stop_na_mesma_vela_e_stop():
    # A decisao pessimista de shadow.donchian_a.resolve_bracket, verbatim.
    spec, e = _abrir()
    _, status, preco = BracketSimples.avaliar(spec, e, [_b(BAR, h=107.0, l=98.0)])
    assert (status, preco) == ("LOSS", 98.5)


def test_expired_sai_no_close_da_barra_h():
    spec, e = _abrir()
    velas = [_b(i * BAR, h=101.0, l=99.0, c=100.0 + i) for i in range(1, 5)]
    _, status, preco = BracketSimples.avaliar(spec, e, velas)
    assert (status, preco) == ("EXPIRED", 104.0)


def test_barra_de_expiracao_ausente_segue_aberto():
    # Buraco de dado NAO expira o trade — o shadow devolve None nesse caso.
    spec, e = _abrir()
    velas = [_b(i * BAR, h=101.0, l=99.0, c=100.0) for i in (1, 2, 3)]
    novo, status, preco = BracketSimples.avaliar(spec, e, velas)
    assert (status, preco) == (None, None)
    assert novo["exit_state"]["status"] == "OPEN"


def test_vela_depois_do_horizonte_e_ignorada():
    spec, e = _abrir()
    velas = [_b(5 * BAR, h=200.0, l=1.0)]          # fora da janela: nao decide
    assert BracketSimples.avaliar(spec, e, velas)[1] is None


def test_vela_anterior_a_entrada_nao_decide():
    spec, e = _abrir(ts=2 * BAR)
    assert BracketSimples.avaliar(spec, e, [_b(BAR, h=200.0, l=1.0)])[1] is None


def test_velas_fora_de_ordem_sao_ordenadas():
    spec, e = _abrir()
    fora = [_b(2 * BAR, h=107.0, l=99.0), _b(BAR, h=101.0, l=98.0)]
    assert BracketSimples.avaliar(spec, e, fora)[1] == "LOSS"   # a de ts menor


def test_trade_ja_fechado_nao_reabre():
    spec, e = _abrir()
    e["exit_state"]["status"] = "WIN"
    novo, status, preco = BracketSimples.avaliar(spec, e, [_b(BAR, h=200.0, l=1.0)])
    assert (status, preco) == ("WIN", None)


def test_vela_malformada_e_pulada():
    spec, e = _abrir()
    ruim = dict(_b(BAR), high=None)
    _, status, _p = BracketSimples.avaliar(spec, e, [ruim, _b(2 * BAR, h=107.0, l=99.0)])
    assert status == "WIN"


def test_extremos_acumulam_incluindo_a_vela_da_saida():
    spec, e = _abrir()
    novo, status, _p = BracketSimples.avaliar(spec, e, [_b(BAR, h=103.0, l=98.0)])
    assert status == "LOSS"
    assert novo["exit_state"]["max_runup"] == pytest.approx(3.0)
    assert novo["exit_state"]["max_drawdown"] == pytest.approx(2.0)


# --- r_multiple: o denominador e s_atr*atr -----------------------------------
def test_r_multiple_no_stop_e_menos_um():
    spec, e = _abrir()
    novo, _s, _p = BracketSimples.avaliar(spec, e, [_b(BAR, h=101.0, l=98.0)])
    assert novo["exit_state"]["r_multiple"] == pytest.approx(-1.0)


def test_r_multiple_no_alvo_e_t_sobre_s():
    spec, e = _abrir()
    novo, _s, _p = BracketSimples.avaliar(spec, e, [_b(BAR, h=107.0, l=99.5)])
    assert novo["exit_state"]["r_multiple"] == pytest.approx(6.0 / 1.5)


def test_r_multiple_com_atr_zero_e_zero_nao_levanta():
    p = {"s_atr": 1.5, "atr_value": 0.0, "entry_price": 100.0, "direction": "LONG"}
    assert BracketSimples.r_multiple(p, 110.0) == 0.0


def test_short_r_multiple_tem_o_sinal_invertido():
    spec, e = _abrir("SHORT")
    novo, status, _p = BracketSimples.avaliar(spec, e, [_b(BAR, h=99.0, l=93.5)])
    assert status == "WIN"
    assert novo["exit_state"]["r_multiple"] == pytest.approx(4.0)


# --- registro dos adaptadores ------------------------------------------------
def test_bracket_simples_esta_no_registro():
    assert adaptador("bracket_simples") is BracketSimples
    assert set(ADAPTADORES) == {"bracket_multi", "reverse", "bracket_simples"}


def test_os_tres_adaptadores_tem_a_mesma_interface():
    for nome, adap in ADAPTADORES.items():
        assert callable(adap.abrir) and callable(adap.avaliar), nome


# --- helpers do runner -------------------------------------------------------
def test_bar_ms_conhece_os_tfs_e_e_case_insensitive():
    assert _bar_ms("4H") == _bar_ms("4h") == BAR


def test_bar_ms_desconhecido_e_erro_explicito():
    with pytest.raises(KeyError, match="tf desconhecido"):
        _bar_ms("3d")


def test_pnl_pct_espelha_a_conta_do_shadow():
    assert _pnl_pct("LONG", 100.0, 106.0) == pytest.approx(6.0)
    assert _pnl_pct("SHORT", 100.0, 94.0) == pytest.approx(6.0)
    assert _pnl_pct("LONG", 0.0, 10.0) == 0.0        # entry zero nao divide


# --- runner: laco ------------------------------------------------------------
def _fonte(series: dict):
    def velas_fn(symbol, tf, n):
        return list(series.get(symbol, []))[-n:]
    return velas_fn


def _sempre(direction="LONG", entry=100.0, atr=1.0):
    return lambda velas: {"direction": direction, "entry_price": entry,
                          "atr_value": atr}


def _linhas(conn):
    return conn.execute(
        f"SELECT * FROM {TABELA} ORDER BY CAST(entry_ts AS INTEGER)").fetchall()


def test_abre_e_grava_setup_id_e_config_hash(tmp_path):
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre())
    velas = [_b(i * BAR) for i in range(3)]
    r = rodar(spec, conn, agora_ms=3 * BAR, velas_fn=_fonte({"BTCUSDT": velas}),
              log=QUIET)
    assert (r["ok"], r["abertos"], r["falhas"]) == (1, 1, 0)
    linha = _linhas(conn)[0]
    assert linha["setup_id"] == "t" and linha["config_hash"] == spec.config_hash
    assert linha["entry_ts"] == str(2 * BAR) and linha["mode"] == "shadow"


def test_a_barra_corrente_nao_e_avaliada(tmp_path):
    # agora_ms cai NO MEIO da barra 2: a ultima fechada e a barra 1.
    conn = connect(tmp_path / "j.db")
    velas = [_b(i * BAR) for i in range(3)]
    rodar(_spec(detector=_sempre()), conn, agora_ms=2 * BAR + BAR // 2,
          velas_fn=_fonte({"BTCUSDT": velas}), log=QUIET)
    assert _linhas(conn)[0]["entry_ts"] == str(BAR)


def test_sem_barras_suficientes_o_simbolo_falha_sozinho(tmp_path):
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre(), symbols=["AAA", "BTCUSDT"], warmup_barras=3)
    velas = [_b(i * BAR) for i in range(4)]
    r = rodar(spec, conn, agora_ms=4 * BAR,
              velas_fn=_fonte({"AAA": velas[:1], "BTCUSDT": velas}), log=QUIET)
    assert (r["ok"], r["falhas"], r["falha_symbols"]) == (1, 1, ["AAA"])
    assert [l["symbol"] for l in _linhas(conn)] == ["BTCUSDT"]


def test_fonte_que_levanta_nao_derruba_os_outros(tmp_path):
    conn = connect(tmp_path / "j.db")
    velas = [_b(i * BAR) for i in range(3)]

    def fonte(symbol, tf, n):
        if symbol == "AAA":
            raise RuntimeError("rede caiu")
        return velas

    r = rodar(_spec(detector=_sempre(), symbols=["AAA", "BTCUSDT"]), conn,
              agora_ms=3 * BAR, velas_fn=fonte, log=QUIET)
    assert (r["ok"], r["falhas"]) == (1, 1)


def test_cadencia_ancorada_na_grade_absoluta(tmp_path):
    # cadencia 2: so barras cujo indice na grade e par entram no detector.
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre(), cadencia_barras=2)
    velas = [_b(i * BAR) for i in range(4)]
    fonte = _fonte({"BTCUSDT": velas})
    rodar(spec, conn, agora_ms=4 * BAR, velas_fn=fonte, log=QUIET)   # barra 3: impar
    assert _linhas(conn) == []
    rodar(spec, conn, agora_ms=5 * BAR, velas_fn=_fonte(
        {"BTCUSDT": velas + [_b(4 * BAR)]}), log=QUIET)              # barra 4: par
    assert len(_linhas(conn)) == 1


def test_idempotente_na_mesma_barra(tmp_path):
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre())
    fonte = _fonte({"BTCUSDT": [_b(i * BAR) for i in range(3)]})
    rodar(spec, conn, agora_ms=3 * BAR, velas_fn=fonte, log=QUIET)
    r = rodar(spec, conn, agora_ms=3 * BAR, velas_fn=fonte, log=QUIET)
    assert r["abertos"] == 0 and len(_linhas(conn)) == 1


def test_espacamento_por_direcao_barra_a_reabertura(tmp_path):
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre(),
                 exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4,
                              "bar_ms": BAR, "espacamento_barras": 4})
    serie = [_b(i * BAR) for i in range(9)]
    for fim in (3, 5, 8):                          # barras 2, 4 e 7
        rodar(spec, conn, agora_ms=fim * BAR,
              velas_fn=_fonte({"BTCUSDT": serie[:fim]}), log=QUIET)
    # barra 2 abre; barra 4 esta a 2 barras (< 4) => vetada; barra 7 reabre.
    assert [l["entry_ts"] for l in _linhas(conn)] == [str(2 * BAR), str(7 * BAR)]


def test_espacamento_e_por_direcao_nao_por_simbolo(tmp_path):
    conn = connect(tmp_path / "j.db")
    cfg = {"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4, "bar_ms": BAR,
           "espacamento_barras": 4}
    serie = [_b(i * BAR) for i in range(5)]
    rodar(_spec(detector=_sempre("LONG"), exit_params=cfg), conn,
          agora_ms=3 * BAR, velas_fn=_fonte({"BTCUSDT": serie[:3]}), log=QUIET)
    rodar(_spec(detector=_sempre("SHORT"), exit_params=cfg), conn,
          agora_ms=4 * BAR, velas_fn=_fonte({"BTCUSDT": serie[:4]}), log=QUIET)
    assert {l["direction"] for l in _linhas(conn)} == {"LONG", "SHORT"}


def test_direcao_fora_das_permitidas_nao_abre(tmp_path):
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre("SHORT"), direcoes=("LONG",))
    rodar(spec, conn, agora_ms=3 * BAR,
          velas_fn=_fonte({"BTCUSDT": [_b(i * BAR) for i in range(3)]}), log=QUIET)
    assert _linhas(conn) == []


def test_resolve_a_aberta_antes_de_abrir_nova(tmp_path):
    # A barra que fecha o trade e a MESMA que dispara o proximo sinal: a ordem
    # do shadow (resolver -> detectar) tem de valer aqui tambem.
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre(),
                 exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4,
                              "bar_ms": BAR, "espacamento_barras": 1})
    serie = [_b(i * BAR) for i in range(3)] + [_b(3 * BAR, h=107.0, l=99.0, c=106.0)]
    rodar(spec, conn, agora_ms=3 * BAR,
          velas_fn=_fonte({"BTCUSDT": serie[:3]}), log=QUIET)
    r = rodar(spec, conn, agora_ms=4 * BAR, velas_fn=_fonte({"BTCUSDT": serie}),
              log=QUIET)
    assert (r["fechados"], r["abertos"]) == (1, 1)
    fechada, nova = _linhas(conn)
    assert fechada["status"] == "WIN" and fechada["exit_price"] == 106.0
    assert nova["status"] == "OPEN"


def test_pnl_liquido_desconta_as_duas_pernas(tmp_path):
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre(), custo_bps_por_perna=5.0)
    serie = [_b(i * BAR) for i in range(3)] + [_b(3 * BAR, h=107.0, l=99.0)]
    rodar(spec, conn, agora_ms=3 * BAR, velas_fn=_fonte({"BTCUSDT": serie[:3]}),
          log=QUIET)
    rodar(spec, conn, agora_ms=4 * BAR, velas_fn=_fonte({"BTCUSDT": serie}),
          log=QUIET)
    linha = _linhas(conn)[0]
    assert linha["pnl_pct"] == pytest.approx(6.0)          # 100 -> 106
    assert linha["pnl_bps_liq"] == pytest.approx(600.0 - 10.0)


def test_r_multiple_fica_no_exit_state_json(tmp_path):
    import json
    conn = connect(tmp_path / "j.db")
    spec = _spec(detector=_sempre())
    serie = [_b(i * BAR) for i in range(3)] + [_b(3 * BAR, h=101.0, l=98.0)]
    rodar(spec, conn, agora_ms=3 * BAR, velas_fn=_fonte({"BTCUSDT": serie[:3]}),
          log=QUIET)
    rodar(spec, conn, agora_ms=4 * BAR, velas_fn=_fonte({"BTCUSDT": serie}),
          log=QUIET)
    estado = json.loads(_linhas(conn)[0]["exit_state_json"])
    assert estado["r_multiple"] == pytest.approx(-1.0)


def test_anotar_do_detector_alimenta_o_reverse(tmp_path):
    # `reverse` le `alvo` da vela; quem carimba e o `anotar` do detector.
    conn = connect(tmp_path / "j.db")

    def detector(velas):
        return {"direction": "LONG", "entry_price": velas[-1]["close"]}

    detector.anotar = lambda velas: [
        dict(v, alvo=(-1 if int(v["ts"]) >= 3 * BAR else 1)) for v in velas]
    spec = _spec(detector=detector, exit_model="reverse", exit_params={})
    serie = [_b(i * BAR, c=100.0 + i) for i in range(4)]
    rodar(spec, conn, agora_ms=3 * BAR, velas_fn=_fonte({"BTCUSDT": serie[:3]}),
          log=QUIET)
    r = rodar(spec, conn, agora_ms=4 * BAR, velas_fn=_fonte({"BTCUSDT": serie}),
              log=QUIET)
    assert r["fechados"] == 1
    assert _linhas(conn)[0]["status"] == "REVERTIDO"


def test_setup_que_estoura_nao_derruba_os_outros(tmp_path):
    conn = connect(tmp_path / "j.db")
    quebrado = _spec(setup_id="quebrado", tf="3d")           # tf fora do mapa
    bom = _spec(setup_id="bom", detector=_sempre())
    saida = rodar_todos([quebrado, bom], conn, agora_ms=3 * BAR,
                        velas_fn=_fonte({"BTCUSDT": [_b(i * BAR) for i in range(3)]}),
                        log=QUIET)
    assert "erro" in saida[0] and saida[0]["setup_id"] == "quebrado"
    assert saida[1]["abertos"] == 1
