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

from backtest.keepitsimple import (EMA_SLOW, EXT_CAP, EXT_CONF, WARMUP,
                                   _alvo_extremos, extremos_entries,
                                   keepitsimple_entries, states)

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


# Serie que ENTRA em VERDE o mais cedo que as EMAs permitem e NAO sai mais: e
# o caso em que o alvo carregado poderia chegar em WARMUP-1 ja nao-nulo e
# engolir a primeira posicao do simbolo (a entrada exige MUDANCA de valor).
_SOBE = [100.0 + 2.0 * i for i in range(60)]


def test_primeiro_estado_nao_cinza_nunca_vem_antes_de_warmup_menos_um():
    # states() nao consegue emitir extremo antes de EMA_SLOW-1: a EMA longa so
    # ganha semente no indice EMA_SLOW-1, e antes disso o estado e CINZA.
    st = states(_SOBE)
    assert next(i for i, s in enumerate(st) if s != "CINZA") == EMA_SLOW - 1
    assert all(s == "CINZA" for s in st[:EMA_SLOW - 1])


def test_alvo_em_warmup_menos_um_e_sempre_zero():
    # CONSEQUENCIA: na barra WARMUP-1 o extremo tem barras_no_estado == 1 (a
    # anterior era CINZA), entao nao confirma e o alvo carrega 0. Nao existe
    # primeira posicao descartada — o descarte e estruturalmente impossivel
    # ENQUANTO WARMUP == EMA_SLOW. Se alguem desacoplar os dois, este teste cai.
    assert WARMUP == EMA_SLOW
    for serie in (_SOBE, [100.0 - 2.0 * i for i in range(60)], _SERIE):
        assert _alvo_extremos(states(serie))[WARMUP - 1] == 0


def test_a_primeira_posicao_do_simbolo_entra_na_barra_warmup():
    # o par dos dois testes acima, no comportamento observavel: a serie que
    # sobe desde a barra 0 gera entrada EXATAMENTE em WARMUP, nao depois
    evs = extremos_entries(_cndl(_SOBE))
    assert _idx(evs, [i * 1000 for i in range(60)]) == [(WARMUP, "LONG")]


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


def _run_ext(d, candles, fim="2027-06-21"):
    """stage1.run no modo kis_extremos sobre um store sintetico de 1 simbolo."""
    import random

    from backtest import stage1
    from backtest.candle_store import connect, upsert_candles
    db = d + "/s.db"
    conn = connect(db)
    upsert_candles(conn, "AAAUSDT", "4h", candles)
    conn.close()
    random.seed(1337)
    return stage1.run(db, ["AAAUSDT"], "2024-05-22", fim, "4h",
                      detector="kis_extremos")


_T0 = 1_716_336_000_000
_BAR4H = 14_400_000


def _ohlc(closes, tss):
    return [{"ts": t, "open": c, "high": c + 0.5, "low": c - 0.5,
             "close": c, "volume": 1.0} for t, c in zip(tss, closes)]


def test_sem_buraco_nenhum_evento_e_excluido_por_hold():
    # PROPRIEDADE: numa serie contigua, hold <= len(fwd_bar) SEMPRE. hold conta
    # candles ate a proxima inversao (<= fim da serie) e fwd_bar le do store,
    # que vai pelo menos tao longe. A exclusao existe para o caso do buraco.
    pytest.importorskip("aiohttp")
    import tempfile

    closes = [100.0 + 30.0 * ((i // 150) % 2) + 0.05 * (i % 150)
              for i in range(700)]
    with tempfile.TemporaryDirectory() as d:
        r = _run_ext(d, _ohlc(closes, [_T0 + i * _BAR4H for i in range(700)]))
    assert r["n_entries"] > 0 and r["excluida_hold"] == 0


def test_buraco_no_store_exclui_o_evento_em_vez_de_medi_lo_curto():
    # O caso REAL (perfil TON/delistagem): 120 barras, buraco de 400, mais 200.
    # `hold` conta CANDLES ate a inversao; `fwd_bar` cobre 256 larguras de
    # barra em TEMPO. Atravessando o buraco, o hold nao cabe — antes desta
    # correcao o evento era medido como se tivesse saido cedo.
    pytest.importorskip("aiohttp")
    import tempfile

    closes = ([100.0 + 0.3 * i for i in range(120)]
              + [160.0 - 0.3 * i for i in range(200)])
    tss = ([_T0 + i * _BAR4H for i in range(120)]
           + [_T0 + (i + 520) * _BAR4H for i in range(200)])
    with tempfile.TemporaryDirectory() as d:
        r = _run_ext(d, _ohlc(closes, tss))
    assert r["excluida_hold"] == 1
    # e o evento excluido nao vazou para a autopsia nem para as entradas
    assert r["n_entries"] == len(r["autopsia"]) == 1


def test_cache_curto_mede_a_exposicao_do_nulo():
    # barras perto da borda direita do store nao tem os EXT_CAP inteiros; o
    # nulo desloca sinais para elas, entao a contagem tem que ser reportada
    pytest.importorskip("aiohttp")
    import tempfile

    closes = [100.0 + 30.0 * ((i // 150) % 2) + 0.05 * (i % 150)
              for i in range(700)]
    with tempfile.TemporaryDirectory() as d:
        r = _run_ext(d, _ohlc(closes, [_T0 + i * _BAR4H for i in range(700)]))
    assert 0 < r["cache_curto"] < EXT_CAP     # ~as ultimas EXT_CAP elegiveis


def test_keepitsimple_nao_ganha_exclusao_por_hold():
    # o clamp e inofensivo no irmao (hold <= NAT_CAP == len) e a contagem nova
    # nem e aplicada la: o contador fica em zero e nada e descartado
    pytest.importorskip("aiohttp")
    import random
    import tempfile

    from backtest import stage1
    from backtest.candle_store import connect, upsert_candles

    closes = [100.0 + 30.0 * ((i // 150) % 2) + 0.05 * (i % 150)
              for i in range(700)]
    with tempfile.TemporaryDirectory() as d:
        conn = connect(d + "/s.db")
        upsert_candles(conn, "AAAUSDT", "4h",
                       _ohlc(closes, [_T0 + i * _BAR4H for i in range(700)]))
        conn.close()
        random.seed(1337)
        r = stage1.run(d + "/s.db", ["AAAUSDT"], "2024-05-22", "2027-06-21",
                       "4h", detector="keepitsimple")
    assert r["excluida_hold"] == 0 and r["cache_curto"] == 0
    assert [g[0] for g in r["gates"]] == ["nativo", "bracket"]


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


# ------------------------------------- array('d') no cache do stage1 (item 4)

@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_array_d_preserva_cada_bit_do_fwd_bar(direction):
    # O slot e[1] do cache passou de lista Python para array('d') por memoria
    # (920 -> 206 MiB numa rodada TIER1). float do Python E um C double e
    # array('d') guarda C double: a conversao tem que ser BIT-A-BIT identica,
    # nao "aproximadamente igual". Comparado com float.hex(), que expoe a
    # mantissa inteira — approx() aqui esconderia exatamente o que se teme.
    pytest.importorskip("aiohttp")
    import tempfile
    from array import array

    from backtest.excursion import measure_event

    with tempfile.TemporaryDirectory() as d:
        conn, bar = _store_com(400, d)
        vistos = 0
        for i in range(30, 120):
            m = measure_event(conn, "AAA", i * bar, direction, tf="4h")
            if m is None:
                continue
            vistos += 1
            lista = m["fwd_bar"][:EXT_CAP]
            arr = array("d", lista)
            assert len(arr) == len(lista)
            assert [x.hex() for x in arr] == [x.hex() for x in lista]
            assert all(a == b for a, b in zip(arr, lista))
        conn.close()
    assert vistos > 50


def test_array_d_suporta_os_quatro_usos_do_slot_no_stage1():
    # os 4 consumidores de e[1], um a um: indexacao em vals, len() no clamp e
    # na exclusao por hold, zip() na guarda de simetria, e negacao do valor
    # indexado na simetria SHORT. Se algum nao aceitasse array, o stage1
    # quebraria so na VM, com o store real.
    from array import array
    lista = [0.5, -1.25, 3.75, -0.125]
    arr = array("d", lista)
    assert arr[2] == lista[2] and arr[len(arr) - 1] == lista[-1]  # indexacao
    assert len(arr) == 4                                          # len()
    assert list(zip(arr, lista)) == list(zip(lista, lista))       # zip()
    assert -arr[1] == 1.25 and isinstance(arr[0], float)          # negacao
    assert arr[:2].tolist() == lista[:2]      # fatiamento devolve outro array


def test_slot_nao_kis_continua_escalar():
    # so o modo kis guarda serie; classifier/tsmom/donchian guardam m["fwd"][h],
    # um float solto. A troca de tipo NAO pode ter alcancado esse caminho —
    # `abs(msf + e[1])` na guarda de simetria depende de e[1] ser escalar.
    import inspect

    from backtest import stage1
    src = inspect.getsource(stage1.run)
    assert 'array("d", m["fwd_bar"][:cap]) if kis else m["fwd"][temp_h]' in src
