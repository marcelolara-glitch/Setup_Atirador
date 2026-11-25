"""Camada 0 (research/REGIME.md): guarda em `_abrir` e determinismo do sinal.

Nada aqui toca a rede: `v10.data._coletar`/`_coletar_antes` e o `velas_fn` do
runner sao as costuras.

O que se trava:

  - GUARDA: com uma OPEN do mesmo setup/config/simbolo, `_abrir` nao grava
    (qualquer direcao) e o simbolo sai NOMEADO em `r["guarda"]`;
  - JANELA: `janela_barras` em `exit_params` aumenta o pedido de velas; sem
    ele, o pedido e o de sempre (warmup + folga). Nenhuma ficha muda de hash;
  - DETERMINISMO: o detector do KIS 34/89 na MESMA barra da o mesmo resultado
    com janelas de 1000 e 1200 barras — e NAO da com 169 (prova de que o teste
    discrimina: e o `alvo=-1` x `alvo_saida=+1` da camada 0);
  - PAGINACAO: `velas(n=1000)` emenda paginas sem buraco nem repeticao.
"""

from __future__ import annotations

import logging
import random

import pytest

from backtest.kis_regime import avaliar
from v10 import data
from v10.registro import PARAMS_KIS_3489, REGISTRO, detector_kis_3489
from v10.runner import FOLGA_BARRAS, rodar
from v10.schema import TABELA, connect
from v10.spec import SetupSpec

BAR = 14_400_000
QUIET = logging.getLogger("v10_camada0_test")
QUIET.addHandler(logging.NullHandler())


def _b(ts, c=100.0):
    return dict(ts=ts, open=c, high=c, low=c, close=c, volume=1.0)


def _spec(**kw) -> SetupSpec:
    base = dict(setup_id="t", detector=lambda v: None, tf="4H", cadencia_barras=1,
                symbols=["XLMUSDT"], warmup_barras=2, exit_model="bracket_simples",
                exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 50,
                             "bar_ms": BAR},
                custo_bps_por_perna=5.0)
    base.update(kw)
    return SetupSpec(**base)


def _sinal(direction):
    return lambda v: {"direction": direction, "entry_price": 100.0,
                      "atr_value": 1.0}


# --- 1. guarda -----------------------------------------------------------------
@pytest.mark.parametrize("segunda", ["SHORT", "LONG"])
def test_guarda_nao_abre_com_posicao_open(tmp_path, segunda):
    conn = connect(tmp_path / "j.db")
    velas = [_b(i * BAR) for i in range(5)]
    fonte = lambda s, tf, n: velas[-n:]              # noqa: E731
    r1 = rodar(_spec(detector=_sinal("SHORT")), conn, agora_ms=3 * BAR,
               velas_fn=fonte, log=QUIET)
    assert r1["abertos"] == 1 and r1["guarda"] == []
    # Velas planas a 100 e stop a 1.5 ATR: a primeira segue OPEN na run seguinte.
    r2 = rodar(_spec(detector=_sinal(segunda)), conn, agora_ms=5 * BAR,
               velas_fn=fonte, log=QUIET)
    linhas = conn.execute(f"SELECT status FROM {TABELA}").fetchall()
    assert [x["status"] for x in linhas] == ["OPEN"]   # nao gravou a segunda
    assert r2["abertos"] == 0 and r2["guarda"] == ["XLMUSDT"]


def test_guarda_nao_conta_aberta_sem_sinal(tmp_path):
    # Aberta sem sinal novo e o estado normal do `reverse` entre inversoes.
    conn = connect(tmp_path / "j.db")
    velas = [_b(i * BAR) for i in range(5)]
    fonte = lambda s, tf, n: velas[-n:]              # noqa: E731
    rodar(_spec(detector=_sinal("SHORT")), conn, agora_ms=3 * BAR,
          velas_fn=fonte, log=QUIET)
    r = rodar(_spec(), conn, agora_ms=5 * BAR, velas_fn=fonte, log=QUIET)
    assert r["guarda"] == []


# --- 2. janela -------------------------------------------------------------------
@pytest.mark.parametrize("extra,esperado", [({}, 89 + FOLGA_BARRAS),
                                            ({"janela_barras": 1000}, 1000),
                                            ({"janela_barras": 50}, 89 + FOLGA_BARRAS)])
def test_janela_barras_define_o_pedido(tmp_path, extra, esperado):
    pedidos = []

    def fonte(s, tf, n):
        pedidos.append(n)
        return [_b(i * BAR) for i in range(n)]

    spec = _spec(warmup_barras=89,
                 exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4,
                              "bar_ms": BAR, **extra})
    rodar(spec, connect(tmp_path / "j.db"), agora_ms=2000 * BAR, velas_fn=fonte,
          log=QUIET)
    assert pedidos == [esperado]


def test_hashes_do_registro_nao_mudam():
    assert {k: s.config_hash for k, s in REGISTRO.items()} == {
        "kis_regime_4h": "e63ec120e131", "kis_3489_60t_4h": "82488baa3086",
        "donchian_a_4h": "250170cc8dc0"}
    assert not any("janela_barras" in (s.exit_params or {})
                   for s in REGISTRO.values())


# --- 3. determinismo ---------------------------------------------------------------
def _serie(seed=0, n=1400):
    rnd, p, out = random.Random(seed), 100.0, []
    for i in range(n):
        o = p
        p = max(1.0, p * (1 + rnd.gauss(0, 0.02)))
        h = max(o, p) * (1 + abs(rnd.gauss(0, 0.005)))
        lo = min(o, p) * (1 - abs(rnd.gauss(0, 0.005)))
        out.append({"ts": i * BAR, "open": o, "high": h, "low": lo, "close": p,
                    "volume": 1.0})
    return out


def _av(v):
    P = PARAMS_KIS_3489
    return avaliar(v, ema_fast=P["ema_fast"], ema_slow=P["ema_slow"],
                   confirmacao=P["confirmacao"], adx_len=P["adx_len"],
                   atr_len=P["atr_len"], incl_ema_slow=P["incl_ema_slow"])


BARRA = 1314        # semente 0: inversao SHORT que a janela curta nao enxerga


def test_mesma_barra_mesmo_sinal_com_janela_longa():
    ate = _serie()[:BARRA + 1]
    a, b = _av(ate[-1000:]), _av(ate[-1200:])
    assert a[:2] == b[:2] == (-1, "SHORT")
    assert a[2] == pytest.approx(b[2], abs=1e-9)
    assert a[3] == pytest.approx(b[3], abs=1e-9)
    assert (detector_kis_3489(ate[-1000:], dict(PARAMS_KIS_3489))
            == detector_kis_3489(ate[-1200:], dict(PARAMS_KIS_3489)))


def test_janela_de_169_diverge():
    ate = _serie()[:BARRA + 1]
    longa, curta = _av(ate[-1000:]), _av(ate[-(89 + FOLGA_BARRAS):])
    assert curta[:2] == (1, None) != longa[:2]          # alvo trocado de lado


# --- 4. paginacao -------------------------------------------------------------------
TOTAL = 1500


def _fonte_paginada(monkeypatch, venue="okx", pagina=300):
    serie = [_b(i * BAR, c=float(i)) for i in range(TOTAL)]
    chamadas = []
    monkeypatch.setattr(data, "_coletar",
                        lambda s, tf, lim: (serie[-min(lim, 300):], venue))

    def antes(s, tf, after_ms):
        chamadas.append(after_ms)
        velhas = [c for c in serie if c["ts"] < after_ms]
        return velhas[-pagina:], "okx"

    monkeypatch.setattr(data, "_coletar_antes", antes)
    return chamadas


def test_paginacao_sem_buraco_nem_repeticao(monkeypatch):
    chamadas = _fonte_paginada(monkeypatch)
    v = data.velas("XLMUSDT", "4H", 1000)
    ts = [c["ts"] for c in v]
    assert len(v) == 1000 and v.venue == "okx"
    assert ts == list(range((TOTAL - 1000) * BAR, TOTAL * BAR, BAR))
    assert len(chamadas) == 3                       # 300 + 3 x 300 >= 1000


def test_paginacao_com_ate(monkeypatch):
    _fonte_paginada(monkeypatch, pagina=100)
    v = data.velas("XLMUSDT", "4H", 400, ate=1400 * BAR)
    assert [c["ts"] for c in v] == list(range(1001 * BAR, 1401 * BAR, BAR))


def test_historico_que_acaba_devolve_menos_sem_laco(monkeypatch):
    _fonte_paginada(monkeypatch)
    assert len(data.velas("XLMUSDT", "4H", 5000)) == TOTAL


def test_ate_300_nao_pagina(monkeypatch):
    chamadas = _fonte_paginada(monkeypatch)
    assert len(data.velas("XLMUSDT", "4H", 169)) == 169 and chamadas == []


def test_janela_longa_de_fallback_e_falha(monkeypatch):
    _fonte_paginada(monkeypatch, venue="bitget")
    with pytest.raises(data.FalhaDeColeta, match="XLMUSDT"):
        data.velas("XLMUSDT", "4H", 1000)
