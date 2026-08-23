"""Adaptadores de saida: valores fechados a mao + espelho contra o v9.

Tres camadas de prova:
  1. Valores calculados a mao para os dois adaptadores (LOSS_SL, WIN_TP1 apos
     SL->BE, WIN_TP2 apos SL->TP1, WIN_TP3, EXPIRED, aberto, inversao).
  2. Espelho DIFERENCIAL: 20 trades gerados com semente fixa, resolvidos pela
     orquestracao de journal._check_one_trade (montada aqui a partir dos
     PROPRIOS helpers do journal) e pelo adaptador. max|dif| = 0 em status e
     preco. A maquina de estado por vela e a mesma funcao nos dois lados de
     proposito; o que este espelho prova e a ORQUESTRACAO em volta dela —
     ordem do timeout, filtro de velas novas, break na primeira saida,
     acumulacao de extremos e o mapa status -> preco.
  3. Espelho contra dados REAIS: as linhas fechadas da tabela `trades`. Roda
     na VM, onde o .db existe; pula no harness (journal/ so tem .gitkeep).
"""

from __future__ import annotations

import json
import random
import sqlite3
from pathlib import Path

import pytest

import journal
from config import JOURNAL_DB_V9
from risk import update_trade_state
from v10.exits import ADAPTADORES, BracketMulti, Reverse, adaptador
from v10.spec import SetupSpec

M15 = 900_000            # 15m em ms
HORA = 3_600_000


def _spec(exit_model="bracket_multi", **params) -> SetupSpec:
    return SetupSpec(setup_id="t", detector=lambda v: None, tf="15m",
                     cadencia_barras=1, symbols=["BTCUSDT"], warmup_barras=0,
                     exit_model=exit_model, exit_params=params)


def _plano(direction="LONG", entry=100.0, sl=95.0, tp1=104.0, tp2=110.0, tp3=120.0):
    return {"direction": direction, "entry_price": entry, "sl_price": sl,
            "tp1_price": tp1, "tp2_price": tp2, "tp3_price": tp3,
            "atr_value": 2.0, "position_split": (0.5, 0.3, 0.2), "leverage": 3.0}


def _v(i, hi, lo, close=None, **extra):
    return {"ts": i * M15, "open": lo, "high": hi, "low": lo,
            "close": close if close is not None else hi, "volume": 1.0, **extra}


def _abre(adap, spec, plano):
    return adap.abrir(spec, plano, {"ts": 0})


# --- bracket_multi: valores fechados a mao -----------------------------------
def test_bracket_loss_sl_puro():
    # LONG entry 100 / SL 95. Vela low 94 <= 95 -> stop antes de qualquer TP.
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, preco = BracketMulti.avaliar(sp, est, [_v(1, 101.0, 94.0)])
    assert (status, preco) == ("LOSS_SL", 95.0)
    assert novo["exit_state"]["max_runup"] == 1.0        # (101-100)/100
    assert novo["exit_state"]["max_drawdown"] == 6.0     # (100-94)/100
    assert novo["exit_state"]["position_remaining"] == 0.0


def test_bracket_tp1_depois_sl_no_breakeven():
    # v1: high 105 >= tp1 104 -> tp1_hit, 50% realizado, SL vai para 100 (BE).
    # v2: low 99 <= 100 -> para no BE com TP1 no bolso -> WIN_TP1, saida em tp1.
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, preco = BracketMulti.avaliar(
        sp, est, [_v(1, 105.0, 99.0), _v(2, 106.0, 99.0)])
    s = novo["exit_state"]
    assert (status, preco) == ("WIN_TP1", 104.0)
    assert s["tp1_hit"] is True and s["tp2_hit"] is False
    assert s["current_sl"] == 100.0                      # SL -> BE
    # Ao bater o SL, update_trade_state zera a posicao — o que restava dos 50%
    # saiu no BE. O "50% realizado" ja esta no preco de saida (tp1), nao aqui.
    assert s["position_remaining"] == 0.0
    assert s["max_runup"] == 6.0 and s["max_drawdown"] == 1.0


def test_bracket_tp1_e_tp2_na_mesma_vela_depois_sl_no_tp1():
    # v1 (high 111) dispara TP1 e TP2 em sequencia; SL sobe para tp1 = 104.
    # v2 (low 103) bate esse SL -> WIN_TP2, saida em tp2 = 110.
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, preco = BracketMulti.avaliar(
        sp, est, [_v(1, 111.0, 99.0), _v(2, 112.0, 103.0)])
    s = novo["exit_state"]
    assert (status, preco) == ("WIN_TP2", 110.0)
    assert s["current_sl"] == 104.0                      # SL -> TP1
    assert s["position_remaining"] == 0.0                # o runner saiu no SL
    assert s["max_runup"] == 12.0
    assert s["max_drawdown"] == 1.0                      # v2 nunca ficou negativa


def test_bracket_tp3_fecha_o_runner():
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, preco = BracketMulti.avaliar(sp, est, [_v(1, 121.0, 99.0)])
    s = novo["exit_state"]
    assert (status, preco) == ("WIN_TP3", 120.0)
    assert s["tp1_hit"] and s["tp2_hit"] and s["tp3_hit"]
    assert abs(s["position_remaining"]) < 1e-12
    assert s["max_runup"] == 21.0


def test_bracket_short_e_o_espelho_invertido():
    sp = _spec()
    est = _abre(BracketMulti, sp,
                _plano("SHORT", 100.0, 105.0, 96.0, 90.0, 80.0))
    novo, status, preco = BracketMulti.avaliar(
        sp, est, [_v(1, 101.0, 95.0), _v(2, 101.0, 99.0)])
    s = novo["exit_state"]
    assert (status, preco) == ("WIN_TP1", 96.0)
    assert s["current_sl"] == 100.0
    assert s["max_runup"] == 5.0                         # (100-95)/100 no SHORT
    assert s["max_drawdown"] == 1.0                      # (101-100)/100


def test_bracket_expired_sai_no_entry():
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, preco = BracketMulti.avaliar(
        sp, est, [_v(1, 101.0, 99.0)], agora_ms=48 * HORA)
    assert (status, preco) == ("EXPIRED", 100.0)         # pnl 0 por construcao
    assert novo["exit_state"]["status"] == "EXPIRED"


def test_bracket_timeout_e_avaliado_antes_das_velas():
    # Vela que bateria TP3, mas o trade ja venceu: EXPIRED tem precedencia,
    # como no v9 (timeout e checado ANTES de buscar klines).
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    _, status, preco = BracketMulti.avaliar(
        sp, est, [_v(1, 500.0, 99.0)], agora_ms=48 * HORA)
    assert (status, preco) == ("EXPIRED", 100.0)


def test_bracket_timeout_configuravel_pelo_spec():
    sp = _spec(timeout_hours=4)
    est = _abre(BracketMulti, sp, _plano())
    assert est["exit_params"]["timeout_hours"] == 4.0
    _, status, _ = BracketMulti.avaliar(sp, est, [_v(1, 101.0, 99.0)],
                                        agora_ms=4 * HORA)
    assert status == "EXPIRED"


def test_bracket_sem_agora_ms_o_relogio_e_a_ultima_vela():
    sp = _spec(timeout_hours=1)
    est = _abre(BracketMulti, sp, _plano())
    # 4 velas de 15m = exatamente 1h -> vence sem precisar de wall-clock.
    _, status, _ = BracketMulti.avaliar(sp, est, [_v(i, 101.0, 99.0)
                                                  for i in range(1, 5)])
    assert status == "EXPIRED"


def test_bracket_segue_aberto():
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, preco = BracketMulti.avaliar(sp, est, [_v(1, 103.0, 96.0)])
    assert (status, preco) == (None, None)
    assert novo["exit_state"]["status"] == "OPEN"
    assert novo["exit_state"]["position_remaining"] == 1.0




def test_bracket_ignora_vela_nao_posterior_a_entrada():
    # A vela da propria entrada (ts == entry_ts) nao pode resolver o trade —
    # e o filtro `ts > open_ts_ms` do v9. Sem ele, um pavio da barra de entrada
    # fecharia o trade no mesmo instante em que ele abriu.
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    novo, status, _ = BracketMulti.avaliar(sp, est, [_v(0, 130.0, 80.0)])
    assert status is None and novo["exit_state"]["status"] == "OPEN"


def test_bracket_velas_fora_de_ordem_sao_ordenadas():
    # Mesmo conjunto, ordens diferentes: TP1 antes do SL em BE nos dois casos.
    sp = _spec()
    velas = [_v(2, 106.0, 99.0), _v(1, 105.0, 99.0)]
    a = BracketMulti.avaliar(sp, _abre(BracketMulti, sp, _plano()), velas)
    b = BracketMulti.avaliar(sp, _abre(BracketMulti, sp, _plano()), velas[::-1])
    assert a[1:] == b[1:] == ("WIN_TP1", 104.0)


def test_bracket_vela_malformada_e_pulada():
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    ruim = {"ts": 1 * M15, "high": None, "low": None, "close": 100.0}
    _, status, preco = BracketMulti.avaliar(sp, est, [ruim, _v(2, 121.0, 99.0)])
    assert (status, preco) == ("WIN_TP3", 120.0)


def test_bracket_trade_ja_fechado_nao_reabre():
    # `avaliar` e puro: devolve estado NOVO e nao muta o que recebeu. Quem
    # chama persiste o retorno; realimentar um estado fechado nao reabre nada.
    sp = _spec()
    est = _abre(BracketMulti, sp, _plano())
    fechado, status, _ = BracketMulti.avaliar(sp, est, [_v(1, 101.0, 94.0)])
    assert status == "LOSS_SL"
    assert est["exit_state"]["status"] == "OPEN"         # entrada intacta
    novo, status2, preco2 = BracketMulti.avaliar(sp, fechado, [_v(2, 121.0, 120.0)])
    assert (status2, preco2) == ("LOSS_SL", None)
    assert novo["exit_state"]["status"] == "LOSS_SL"


def test_bracket_abrir_nao_inventa_coluna_de_saida():
    # O contrato do schema: tudo de bracket cabe em exit_params/exit_state.
    est = _abre(BracketMulti, _spec(), _plano())
    assert set(est) == {"exit_params", "exit_state"}
    assert json.loads(json.dumps(est)) == est            # serializavel em JSON


# --- reverse: valores fechados a mao -----------------------------------------
def test_reverse_fecha_quando_o_alvo_inverte():
    sp = _spec("reverse")
    est = _abre(Reverse, sp, _plano())
    novo, status, preco = Reverse.avaliar(sp, est, [
        _v(1, 105.0, 99.0, close=105.0, alvo=1),         # segue LONG
        _v(2, 103.0, 90.0, close=98.0, alvo=-1),         # inverteu -> fecha
        _v(3, 200.0, 1.0, close=150.0, alvo=-1),         # nunca chega aqui
    ])
    assert (status, preco) == ("REVERTIDO", 98.0)
    assert novo["exit_state"]["alvo_saida"] == -1
    assert novo["exit_state"]["max_runup"] == 5.0        # (105-100)/100
    assert novo["exit_state"]["max_drawdown"] == 10.0    # (100-90)/100


def test_reverse_alvo_neutro_nao_fecha():
    # 0 e "parei de opinar", nao "mudei de lado". Fechar no 0 seria um stop
    # disfarcado — e este adaptador nao tem stop.
    sp = _spec("reverse")
    est = _abre(Reverse, sp, _plano())
    novo, status, preco = Reverse.avaliar(
        sp, est, [_v(1, 105.0, 90.0, close=91.0, alvo=0)])
    assert (status, preco) == (None, None)
    assert novo["exit_state"]["status"] == "OPEN"


def test_reverse_sem_stop_e_sem_alvo():
    # Queda de 60% sem inverter o alvo: continua aberto. E o ponto do modelo.
    sp = _spec("reverse")
    est = _abre(Reverse, sp, _plano())
    _, status, _ = Reverse.avaliar(sp, est, [_v(1, 101.0, 40.0, close=40.0, alvo=1)])
    assert status is None


def test_reverse_short_inverte_para_long():
    sp = _spec("reverse")
    est = _abre(Reverse, sp, _plano("SHORT"))
    assert est["exit_state"]["alvo"] == -1
    novo, status, preco = Reverse.avaliar(sp, est, [
        _v(1, 101.0, 95.0, close=96.0, alvo=-1),
        _v(2, 104.0, 99.0, close=103.0, alvo=1),
    ])
    assert (status, preco) == ("REVERTIDO", 103.0)
    assert novo["exit_state"]["max_runup"] == 5.0        # SHORT: (100-95)/100
    assert novo["exit_state"]["max_drawdown"] == 4.0     # SHORT: (104-100)/100


def test_reverse_vela_sem_alvo_nao_decide():
    sp = _spec("reverse")
    est = _abre(Reverse, sp, _plano())
    _, status, _ = Reverse.avaliar(sp, est, [_v(1, 101.0, 99.0, close=99.0)])
    assert status is None


def test_reverse_ignora_vela_nao_posterior_a_entrada():
    sp = _spec("reverse")
    est = _abre(Reverse, sp, _plano())
    _, status, _ = Reverse.avaliar(sp, est, [_v(0, 101.0, 99.0, close=99.0, alvo=-1)])
    assert status is None


# --- registro ----------------------------------------------------------------
def test_os_dois_adaptadores_tem_a_mesma_interface():
    for nome, adap in ADAPTADORES.items():
        assert adaptador(nome) is adap
        for metodo in ("abrir", "avaliar"):
            assert callable(getattr(adap, metodo)), f"{nome}.{metodo}"


def test_exit_model_desconhecido_e_erro_explicito():
    with pytest.raises(KeyError, match="exit_model desconhecido"):
        adaptador("trailing_que_nao_existe")


# --- espelho 1: diferencial contra a orquestracao do v9 ----------------------
def _referencia_v9(row: dict, klines: list, agora_ms: int) -> tuple:
    """journal._check_one_trade remontado com os PROPRIOS helpers do journal.

    Nao e uma reescrita: `_plan_from_row`, `_state_from_row`,
    `update_trade_state` e `_exit_price_for_status` sao importados de la. O que
    esta escrito aqui e so a ordem em que o v9 os chama — que e exatamente o
    que o adaptador precisa reproduzir.
    """
    entry, direction = float(row["entry_price"]), row["direction"]
    abertura = int(row["_entry_ts_ms"])
    if (agora_ms - abertura) / HORA >= float(row["timeout_hours"]):
        return "EXPIRED", entry, 0.0, 0.0                     # passo 1
    novas = sorted((k for k in klines if int(k["ts"]) > abertura),
                   key=lambda k: int(k["ts"]))                # passo 3
    plan = journal._plan_from_row(row)                        # passo 4
    state = journal._state_from_row(row, plan)
    mr = md = 0.0
    for c in novas:                                           # passo 5
        hi, lo = float(c["high"]), float(c["low"])
        if entry > 0:
            if direction == "LONG":
                ru, dd = (hi - entry) / entry * 100.0, (entry - lo) / entry * 100.0
            else:
                ru, dd = (entry - lo) / entry * 100.0, (hi - entry) / entry * 100.0
            mr, md = max(mr, ru), max(md, dd)
        state = update_trade_state(state, c)
        if state.status != "OPEN":
            break
    if state.status == "OPEN":
        return None, None, mr, md
    preco = journal.TradeJournalV9._exit_price_for_status(state, plan, row)
    return state.status, preco, mr, md                        # passo 7


ROTEIROS = ("SL", "TP1_BE", "TP2_TP1", "TP3", "ABERTO")


def _amostra(roteiros=ROTEIROS, semente=1337):
    """20 trades: cada roteiro x LONG/SHORT x duas escalas de preco.

    Trajetoria CONSTRUIDA, nao sorteada. Um passeio aleatorio quase nunca
    alcanca TP3 (3.5 ATR) antes de encostar no SL (0.8-1.5 ATR) — foi medido:
    nenhuma das 400 primeiras sementes cobriu os cinco desfechos. Um espelho
    que nunca ve WIN_TP3 nao prova nada sobre WIN_TP3. As folgas vem de uma
    semente fixa, entao os numeros nao sao redondos nem simetricos, mas os
    desfechos sao os cinco, sempre.
    """
    rnd = random.Random(semente)
    for roteiro in roteiros:
        for direction in ("LONG", "SHORT"):
            for escala in (0.7431, 28_413.19):
                s = 1.0 if direction == "LONG" else -1.0
                entry = round(escala * rnd.uniform(0.9, 1.1), 8)
                atr = entry * rnd.uniform(0.004, 0.03)
                sl = entry - s * atr * rnd.uniform(0.8, 1.5)
                tp1, tp2, tp3 = (entry + s * atr * k for k in (0.8, 1.5, 3.5))
                row = {
                    "direction": direction, "entry_price": entry, "sl_price": sl,
                    "tp1_price": tp1, "tp2_price": tp2, "tp3_price": tp3,
                    "current_sl": sl, "atr_value": atr, "leverage": 3.0,
                    "position_split": "[0.5, 0.3, 0.2]", "tp1_hit": 0,
                    "tp2_hit": 0, "tp3_hit": 0, "position_remaining": 1.0,
                    "status": "OPEN", "timeout_hours": 48,
                    "_entry_ts_ms": 1_700_000_000_000, "_roteiro": roteiro,
                }

                def marco(alvo, folga):
                    """Vela cujo extremo FAVORAVEL alcanca `alvo`; o extremo
                    contrario para em `entry +- folga*atr`, longe do SL."""
                    eps = atr * rnd.uniform(0.05, 0.2)
                    fav = alvo + s * eps
                    con = entry - s * atr * folga
                    return (max(fav, con), min(fav, con))

                def recua(nivel, teto):
                    """Vela que volta e encosta em `nivel` (SL movido) sem
                    passar do `teto`."""
                    eps = atr * rnd.uniform(0.05, 0.2)
                    a, b = nivel - s * eps, teto
                    return (max(a, b), min(a, b))

                if roteiro == "SL":
                    passos = [(max(sl - s * atr * 0.1, entry + s * atr * 0.3),
                               min(sl - s * atr * 0.1, entry + s * atr * 0.3))]
                elif roteiro == "TP1_BE":
                    passos = [marco(tp1, 0.1), recua(entry, entry + s * atr * 0.3)]
                elif roteiro == "TP2_TP1":
                    passos = [marco(tp2, 0.1), recua(tp1, tp1 + s * atr * 0.3)]
                elif roteiro == "TP3":
                    passos = [marco(tp3, 0.1)]
                else:                                   # ABERTO
                    passos = [(entry + atr * 0.4, entry - atr * 0.4)] * 2

                velas = [{"ts": row["_entry_ts_ms"] + (i + 1) * M15,
                          "open": entry, "high": hi, "low": lo,
                          "close": (hi + lo) / 2.0, "volume": 1.0}
                         for i, (hi, lo) in enumerate(passos)]
                rnd.shuffle(velas)                # fora de ordem de proposito
                yield row, velas


def test_espelho_bracket_reproduz_a_orquestracao_do_v9():
    sp = _spec()
    casos = list(_amostra())
    assert len(casos) == 20
    difs, vistos, por_roteiro = [], set(), {}
    for row, velas in casos:
        agora = row["_entry_ts_ms"] + 47 * HORA
        esperado = _referencia_v9(row, velas, agora)
        est = BracketMulti.abrir(sp, row, {"ts": row["_entry_ts_ms"]})
        novo, status, preco = BracketMulti.avaliar(sp, est, velas, agora_ms=agora)
        s = novo["exit_state"]
        assert status == esperado[0], f"status divergiu: {status} != {esperado[0]}"
        difs += [abs((preco or 0.0) - (esperado[1] or 0.0)),
                 abs(s["max_runup"] - esperado[2]),
                 abs(s["max_drawdown"] - esperado[3])]
        vistos.add(status)
        por_roteiro.setdefault(row["_roteiro"], set()).add(status)
    assert max(difs) == 0.0, f"max|dif| = {max(difs)}, deveria ser 0"
    # Um espelho que nunca viu um desfecho nao provou nada sobre ele.
    assert vistos == {"LOSS_SL", "WIN_TP1", "WIN_TP2", "WIN_TP3", None}, vistos
    # Cada roteiro tem que dar UM desfecho, e o desfecho que ele diz que da.
    # Sem isto, uma trajetoria mal construida vira "tudo LOSS_SL" e o espelho
    # continua verde medindo sempre o mesmo ramo.
    assert por_roteiro == {"SL": {"LOSS_SL"}, "TP1_BE": {"WIN_TP1"},
                           "TP2_TP1": {"WIN_TP2"}, "TP3": {"WIN_TP3"},
                           "ABERTO": {None}}


def test_espelho_cobre_tambem_o_ramo_expired():
    sp = _spec()
    row, velas = next(iter(_amostra(("TP3",))))
    agora = row["_entry_ts_ms"] + 48 * HORA
    esperado = _referencia_v9(row, velas, agora)
    est = BracketMulti.abrir(sp, row, {"ts": row["_entry_ts_ms"]})
    _, status, preco = BracketMulti.avaliar(sp, est, velas, agora_ms=agora)
    assert esperado[0] == status == "EXPIRED"
    assert abs(preco - esperado[1]) == 0.0


# --- espelho 2: dados REAIS da tabela `trades` (roda na VM) ------------------
def _trades_reais(limite=20):
    caminho = Path(JOURNAL_DB_V9)
    if not caminho.exists():
        pytest.skip(f"{caminho} ausente (harness); espelho real roda na VM")
    conn = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        linhas = conn.execute(
            "SELECT * FROM trades WHERE status != 'OPEN' AND exit_price IS NOT NULL"
            " ORDER BY timestamp DESC LIMIT ?", (limite,)).fetchall()
    except sqlite3.OperationalError as e:
        pytest.skip(f"tabela `trades` indisponivel: {e}")
    finally:
        conn.close()
    if not linhas:
        pytest.skip("tabela `trades` sem trade fechado")
    return [dict(r) for r in linhas]


def test_espelho_preco_de_saida_contra_trades_reais():
    """max|dif| = 0 entre o exit_price gravado pelo v9 e o do adaptador.

    Somente LEITURA (`mode=ro`): a tabela `trades` e historico congelado.
    Reproduzir tambem o STATUS exigiria as klines que o v9 viu ao vivo, que nao
    ficaram guardadas — o que se prova aqui e o mapa status -> preco sobre os
    numeros reais (entry/tp1/tp2/tp3/current_sl de cada linha).
    """
    linhas = _trades_reais()
    difs = []
    for row in linhas:
        params = {k: float(row[k]) for k in
                  ("entry_price", "tp1_price", "tp2_price", "tp3_price")}
        obtido = BracketMulti.preco_saida(row["status"], params,
                                          float(row["current_sl"]))
        difs.append(abs(obtido - float(row["exit_price"])))
    assert max(difs) == 0.0, f"max|dif| = {max(difs)} em {len(linhas)} trades reais"
