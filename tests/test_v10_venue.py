"""Venue no rodape: simbolo servido pelo fallback nao passa por simbolo normal.

Nada aqui toca a rede — a fonte de velas e injetada, como em test_v10_coleta —
e por isso este arquivo NAO usa `importorskip`: quem foi servido por outra
corretora tem de reprovar com nome, nao sumir do relato.
"""

from __future__ import annotations

import logging

import pytest

from v10 import data, relatorio, runner
from v10.schema import connect
from v10.spec import SetupSpec

BAR = 14_400_000                                   # 4h em ms
QUIET = logging.getLogger("v10_venue_test")
QUIET.addHandler(logging.NullHandler())


def _b(ts, c=100.0):
    return dict(ts=ts, open=c, high=c, low=c, close=c, volume=1.0)


@pytest.fixture
def conn(tmp_path):
    c = connect(str(tmp_path / "t.db"))
    yield c
    c.close()


def _spec(**kw) -> SetupSpec:
    base = dict(setup_id="t", detector=lambda v: None, tf="4H", cadencia_barras=1,
                symbols=["BTCUSDT", "PENGUUSDT"], warmup_barras=2,
                exit_model="bracket_simples",
                exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4, "bar_ms": BAR},
                custo_bps_por_perna=5.0)
    base.update(kw)
    return SetupSpec(**base)


def test_config_hash_dos_tres_setups_inalterados(capsys):
    """Este PR e sobre de onde veio a vela, nao sobre o que a ficha mede."""
    from v10.registro import REGISTRO

    esperado = {"kis_regime_4h": "e63ec120e131",
                "kis_3489_60t_4h": "82488baa3086",
                "donchian_a_4h": "250170cc8dc0"}
    obtido = {sid: s.config_hash for sid, s in REGISTRO.items()}
    with capsys.disabled():
        print("\n  config_hash apos o PR do venue:")
        for sid, h in obtido.items():
            print(f"    {sid:<20} {h}  ({'OK' if esperado.get(sid) == h else 'MUDOU'})")
    assert obtido == esperado


def test_velas_carrega_o_venue_e_continua_sendo_lista(monkeypatch):
    monkeypatch.setattr(data.time, "sleep", lambda s: None)
    monkeypatch.setattr(data, "_coletar",
                        lambda s, tf, lim: ([_b(0), _b(BAR)], "bitget"))
    v = data.velas("PENGUUSDT", "4H", 2)
    assert v == [_b(0), _b(BAR)]                   # chamador antigo nao muda
    assert v.venue == "bitget"


def test_bitget_sai_nomeado_no_log_e_contado_no_rodape_okx_nao(conn, caplog):
    def _fonte(symbol, tf, n, ate=None):
        v = data.Velas([_b(0), _b(BAR)])
        v.venue = "bitget" if symbol == "PENGUUSDT" else "okx"
        return v

    log = logging.getLogger("v10.venue.no_teste")
    with caplog.at_level(logging.WARNING, logger=log.name):
        r = runner.rodar(_spec(), conn, 3 * BAR, velas_fn=_fonte, log=log)
    assert r["ok"] == 2 and r["falhas"] == 0       # os dois coletaram
    assert r["venue_alt"] == ["PENGUUSDT@bitget"]
    assert "PENGUUSDT" in caplog.text and "bitget" in caplog.text
    assert "BTCUSDT" not in caplog.text            # servido pela primaria: calado

    linha = relatorio.bloco_delta(conn, _spec(), 0, 9 * BAR,
                                  venue_alt=r["venue_alt"])
    assert "venue 1 fora da primária" in linha and "PENGUUSDT@bitget" in linha
    assert "BTCUSDT" not in linha


def test_sem_informacao_de_venue_o_rodape_nao_afirma_nada(conn):
    # Fonte que devolve lista crua (versao de store, fixture de teste): ausencia
    # de venue NAO pode ser lida como "tudo OKX".
    r = runner.rodar(_spec(), conn, 3 * BAR, log=QUIET,
                     velas_fn=lambda symbol, tf, n, ate=None: [_b(0), _b(BAR)])
    assert r["venue_alt"] == []
    assert "venue" not in relatorio.bloco_delta(conn, _spec(), 0, 9 * BAR,
                                                venue_alt=r["venue_alt"])


def test_build_delta_roteia_o_venue_por_setup_id(conn):
    msg = relatorio.build_delta(conn, [_spec(setup_id="a"), _spec(setup_id="b")],
                                0, 9 * BAR, venue={"a": ["PENGUUSDT@bitget"]})
    linha_a = [x for x in msg.splitlines() if "<b>a</b>" in x][0]
    linha_b = [x for x in msg.splitlines() if "<b>b</b>" in x][0]
    assert "venue 1" in linha_a and "venue" not in linha_b
