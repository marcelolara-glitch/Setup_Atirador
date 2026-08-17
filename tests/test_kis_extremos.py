"""Testes puros do detector kis_extremos (stdlib no topo -> coleta no sandbox).

Ancoragem. A serie-espec `_SERIE` e a mesma de test_keepitsimple.py, cujo mapa
de estados ja esta conferido a mao la. As trocas de estado dela sao:

    41 VERDE | 61 AZUL | 65 VERM | 85 ROXO | 86 VERDE | 110 VERM

Aplicando a regra Extremos (confirmacao de 2 barras no MESMO estado antes de o
extremo valer) a esse mapa, barra a barra:

    41 VERDE barras=1 -> nao confirma, alvo carrega 0
    42 VERDE barras=2 -> alvo +1  => ENTRADA LONG  (alvo[41] era 0)
    43..60 VERDE                     alvo +1, sem troca
    61..64 AZUL                      NAO AGE, carrega +1
    65 VERM  barras=1 -> carrega +1
    66 VERM  barras=2 -> alvo -1  => ENTRADA SHORT (inversao)
    85 ROXO                          NAO AGE, carrega -1
    86 VERDE barras=1 -> carrega -1
    87 VERDE barras=2 -> alvo +1  => ENTRADA LONG  (inversao)
    110 VERM barras=1 -> carrega +1
    111 VERM barras=2 -> alvo -1  => ENTRADA SHORT (inversao)

Ou seja, cada entrada do keepitsimple desloca exatamente +1 barra pela
confirmacao — e nenhuma entrada NOVA aparece.
"""

import pytest

from backtest.keepitsimple import (EXT_CAP, EXT_CONF, WARMUP, _alvo_extremos,
                                   extremos_entries, keepitsimple_entries,
                                   states)

_SERIE = ([100.0] * 41 + [110.0] * 20 + [108.0] * 4 + [90.0] * 20
          + [105.0] * 25 + [80.0] * 20)
_ENTRADAS = [(42, "LONG"), (66, "SHORT"), (87, "LONG"), (111, "SHORT")]


def _cndl(closes, tss=None):
    """Candles OHLCV no formato de producao (flat: O=H=L=C)."""
    tss = tss if tss is not None else [i * 1000 for i in range(len(closes))]
    return [{"ts": t, "open": c, "high": c, "low": c, "close": c,
             "volume": 1.0} for t, c in zip(tss, closes)]


def _idx(evs, tss=None):
    tss = tss if tss is not None else [i * 1000 for i in range(400)]
    pos = {t: i for i, t in enumerate(tss)}
    return [(pos[e["bar_ts"]], e["direction"]) for e in evs]


# ------------------------------------------------------- mapa de entradas

def test_indices_exatos_das_entradas():
    assert _idx(extremos_entries(_cndl(_SERIE))) == _ENTRADAS


def test_confirmacao_desloca_cada_entrada_do_irmao_em_uma_barra():
    # nao e so "deslocou": o CONJUNTO de entradas e o mesmo, uma a uma
    kis = _idx(keepitsimple_entries(_cndl(_SERIE), sep_bars=0, bar_ms=1000))
    ext = _idx(extremos_entries(_cndl(_SERIE)))
    assert [(i + 1, d) for i, d in kis] == ext


def test_verde_para_verm_gera_inversao():
    ext = _idx(extremos_entries(_cndl(_SERIE)))
    assert ext[0][1] == "LONG" and ext[1] == (66, "SHORT")


def test_direcoes_sempre_alternam():
    # consequencia da posicao unica: nunca duas entradas do mesmo lado seguidas
    dirs = [d for _i, d in _idx(extremos_entries(_cndl(_SERIE)))]
    assert all(a != b for a, b in zip(dirs, dirs[1:]))


# ----------------------------------- O PONTO CENTRAL: VERDE -> AZUL -> VERDE

def _serie_verde_azul_verde():
    """VERDE 41-48, AZUL na 49 (uma barra), VERDE de novo a partir da 50 — a
    EMA longa nunca e cruzada. E a serie de test_keepitsimple, onde o irmao
    ENTRA duas vezes."""
    closes = [100.0] * 41 + [110.0] * 8 + [104.0] + [112.0] * 10
    st = states(closes)
    assert (st[48], st[49], st[50]) == ("VERDE", "AZUL", "VERDE")
    return closes


def test_azul_no_meio_nao_gera_segunda_entrada():
    # REGRA CENTRAL: AZUL nao age, so CARREGA. O alvo continua +1 durante a 49
    # e volta a +1 na 51 — sem troca de alvo, sem entrada nova.
    closes = _serie_verde_azul_verde()
    assert _idx(extremos_entries(_cndl(closes))) == [(42, "LONG")]


def test_contraprova_o_irmao_entra_duas_vezes_na_mesma_serie():
    closes = _serie_verde_azul_verde()
    longs = [i for i, d in _idx(keepitsimple_entries(_cndl(closes), 0, 1000))
             if d == "LONG"]
    assert longs == [41, 50]        # o keepitsimple reentra; o extremos, nao


def test_alvo_carrega_atraves_do_azul():
    closes = _serie_verde_azul_verde()
    alvo = _alvo_extremos(states(closes))
    assert alvo[48] == alvo[49] == alvo[50] == alvo[51] == 1


def test_roxo_e_cinza_tambem_so_carregam():
    # 85 e ROXO no meio de VERM(65-84) -> VERDE(86-): o alvo segue -1 na 85
    alvo = _alvo_extremos(states(_SERIE))
    assert states(_SERIE)[85] == "ROXO" and alvo[84] == alvo[85] == -1
    assert all(a == 0 for a in alvo[:42])     # CINZA do warmup nao cria alvo


# ------------------------------------------------------------ confirmacao

def _serie_extremo_de_uma_barra():
    """Pico de UMA barra: VERDE so na 41, AZUL ja na 42. O extremo nao dura o
    suficiente para confirmar."""
    closes = [100.0] * 41 + [130.0] + [100.0] * 20
    st = states(closes)
    assert (st[40], st[41], st[42]) == ("CINZA", "VERDE", "AZUL")
    return closes


def test_extremo_de_uma_barra_nao_gera_entrada():
    closes = _serie_extremo_de_uma_barra()
    assert 41 not in [i for i, _d in _idx(extremos_entries(_cndl(closes)))]


def test_contraprova_o_irmao_entra_no_extremo_de_uma_barra():
    closes = _serie_extremo_de_uma_barra()
    assert (41, "LONG") in _idx(keepitsimple_entries(_cndl(closes), 0, 1000))


def test_confirmacao_e_de_duas_barras():
    assert EXT_CONF == 2


# ------------------------------------------------------------------ hold

def test_hold_e_a_distancia_ate_a_proxima_entrada():
    evs = extremos_entries(_cndl(_SERIE))
    idx = [i for i, _d in _idx(evs)]
    for k, e in enumerate(evs[:-1]):
        assert e["hold"] == idx[k + 1] - idx[k]


def test_hold_da_ultima_entrada_vai_ate_o_fim_da_serie():
    evs = extremos_entries(_cndl(_SERIE))
    assert evs[-1]["hold"] == (len(_SERIE) - 1) - _idx(evs)[-1][0]


def test_hold_nunca_e_zero_nem_negativo():
    evs = extremos_entries(_cndl(_SERIE))
    assert evs and all(1 <= e["hold"] <= EXT_CAP for e in evs)


def test_hold_no_teto_marca_truncado():
    # uma unica entrada seguida de 400 barras sem inversao: bruto > EXT_CAP
    closes = [100.0] * 41 + [110.0 + 0.5 * i for i in range(400)]
    evs = extremos_entries(_cndl(closes))
    assert len(evs) == 1 and evs[0]["direction"] == "LONG"
    assert evs[0]["hold"] == EXT_CAP and evs[0]["hold_truncado"] is True


def test_serie_espec_nao_trunca_nenhuma_entrada():
    assert not any(e["hold_truncado"] for e in extremos_entries(_cndl(_SERIE)))


# ------------------------------------------------------- SEP = 0 estrutural

def test_entradas_nunca_se_sobrepoem():
    # cada posicao fecha exatamente onde a proxima abre — SEP=0 por construcao
    evs = extremos_entries(_cndl(_SERIE))
    idx = [i for i, _d in _idx(evs)]
    for k, e in enumerate(evs[:-1]):
        assert idx[k] + e["hold"] == idx[k + 1]


# ---------------------------------------------------------------- warmup

def test_warmup_suprime_entradas_antes_da_barra_21():
    # serie que estaria confirmada cedo se o warmup nao existisse
    evs = extremos_entries(_cndl([100.0 + i for i in range(60)]))
    tss = [i * 1000 for i in range(60)]
    assert all(e["bar_ts"] >= WARMUP * 1000 for e in evs), tss[:1]


# ------------------------------------------------------- gap de timestamp

def test_gap_de_timestamp_nao_desloca_entradas():
    # buraco de 50 barras no meio da serie: a indexacao e por bar_ts, entao as
    # entradas caem nas MESMAS barras (mesma posicao), com outro carimbo
    tss = [i * 1000 if i < 70 else (i + 50) * 1000
           for i in range(len(_SERIE))]
    evs = extremos_entries(_cndl(_SERIE, tss))
    assert _idx(evs, tss) == _ENTRADAS


def test_bar_ts_sai_da_propria_serie_nunca_calculado():
    tss = [i * 1000 if i < 70 else (i + 50) * 1000
           for i in range(len(_SERIE))]
    evs = extremos_entries(_cndl(_SERIE, tss))
    assert {e["bar_ts"] for e in evs} <= set(tss)


# ------------------------------------------------- contrato com o soquete

def test_contrato_de_saida_igual_ao_dos_irmaos():
    evs = extremos_entries(_cndl(_SERIE))
    assert evs and all(isinstance(e["bar_ts"], int) for e in evs)
    assert all(e["direction"] in ("LONG", "SHORT") for e in evs)
    assert [e["bar_ts"] for e in evs] == sorted(e["bar_ts"] for e in evs)


def test_stage1_registra_o_detector():
    from backtest import stage1
    assert stage1.EXT_CAP == 256
    assert (stage1.KIS_MONEY_BPS, stage1.KIS_SKILL) == (5.0, 0.005)


def test_stage1_aceita_kis_extremos_no_cli():
    import argparse
    from backtest import stage1
    src = stage1.main.__code__.co_consts
    assert any(isinstance(c, tuple) and "kis_extremos" in c for c in src), (
        argparse.__name__)


# ---------------------------------- horizonte estendido de fwd_bar (PASSO 2)

def _store_com(n_barras, d):
    """Store temporario 4h com uma serie monotona; devolve (conn, bar)."""
    from backtest.candle_store import connect, upsert_candles
    bar = 14_400_000
    conn = connect(d + "/t.db")
    upsert_candles(conn, "AAA", "4h",
                   [{"ts": i * bar, "open": 100.0 + i, "high": 101.0 + i,
                     "low": 99.0 + i, "close": 100.5 + i, "volume": 1.0}
                    for i in range(n_barras)])
    return conn, bar


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_invariante_fwd_bar_igual_fwd_no_horizonte_estendido(direction):
    # INVARIANTE OBRIGATORIA: fwd_bar[h-1] == fwd[h] p/ todo h em HORIZONS,
    # nas DUAS direcoes. A cadeia excursion -> replay -> exchanges exige
    # aiohttp (ausente na sandbox 3.11); na VM roda de verdade.
    pytest.importorskip("aiohttp")
    import tempfile

    from backtest.excursion import FWD_BAR_CAP, HORIZONS, measure_event

    with tempfile.TemporaryDirectory() as d:
        conn, bar = _store_com(400, d)
        m = measure_event(conn, "AAA", 50 * bar, direction, tf="4h")
        conn.close()
    assert m is not None
    assert len(m["fwd_bar"]) == FWD_BAR_CAP          # longe da borda: cheio
    for h in HORIZONS:
        assert m["fwd_bar"][h - 1] == pytest.approx(m["fwd"][h])


def test_fwd_bar_encurta_perto_da_borda_direita_sem_excluir_o_evento():
    # 120 barras, entrada na 50 -> so 69 barras de futuro. O evento continua
    # medido (a exclusao de borda e 48, INALTERADA) e fwd_bar vem com 69.
    pytest.importorskip("aiohttp")
    import tempfile

    from backtest.excursion import HORIZONS, measure_event

    with tempfile.TemporaryDirectory() as d:
        conn, bar = _store_com(120, d)
        m = measure_event(conn, "AAA", 50 * bar, "LONG", tf="4h")
        conn.close()
    assert m is not None and len(m["fwd_bar"]) == 69
    for h in HORIZONS:
        assert m["fwd_bar"][h - 1] == pytest.approx(m["fwd"][h])


def test_reguas_antigas_seguem_no_horizonte_de_48():
    # fav/adv NAO foram estendidos: a leitura mais longa nao pode vazar neles
    pytest.importorskip("aiohttp")
    import tempfile

    from backtest.excursion import HORIZONS, measure_event

    with tempfile.TemporaryDirectory() as d:
        conn, bar = _store_com(400, d)
        m = measure_event(conn, "AAA", 50 * bar, "LONG", tf="4h")
        conn.close()
    assert len(m["fav"]) == len(m["adv"]) == max(HORIZONS) == 48
    assert set(m["mfe"]) == set(m["mae"]) == set(m["fwd"]) == set(HORIZONS)


def test_exclusao_de_borda_continua_em_48_e_nao_em_256():
    # com 100 barras e entrada na 51 sobram 48 -> AINDA e medido. A extensao
    # de fwd_bar nao pode ter apertado o criterio de exclusao.
    pytest.importorskip("aiohttp")
    import tempfile

    from backtest.excursion import measure_event

    with tempfile.TemporaryDirectory() as d:
        conn, bar = _store_com(100, d)
        ok = measure_event(conn, "AAA", 51 * bar, "LONG", tf="4h")
        nao = measure_event(conn, "AAA", 52 * bar, "LONG", tf="4h")
        conn.close()
    assert ok is not None and len(ok["fwd_bar"]) == 48
    assert nao is None                                # 47 barras -> excluido
