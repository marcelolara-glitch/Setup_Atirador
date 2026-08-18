"""Testes puros do detector keepitsimple (stdlib no topo -> coleta no sandbox).

A serie-espec `_SERIE` e o ancoramento de todos os testes de transicao. Os
numeros de EMA citados nos comentarios foram conferidos a mao:

    barra 41 (close salta 100 -> 110, alpha8 = 2/9, alpha21 = 2/22)
        curta = 2/9*110  + 7/9*100  = 24.4444 + 77.7778 = 102.2222
        longa = 2/22*110 + 20/22*100 = 10.0000 + 90.9091 = 100.9091
        close 110 > curta 102.2222 e curta > longa -> VERDE (estado[40]=CINZA)

Mapa completo de estados da serie (transicoes verificadas uma a uma):
    i=0   CINZA (empate exato: close == curta == longa na serie constante)
    i=41  VERDE  curta=102.2222 longa=100.9091   -> ENTRADA LONG
    i=61  AZUL   curta=109.5045 longa=108.4669   (close<curta, curta>longa)
    i=65  VERM   curta=104.5506 longa=106.6825   -> ENTRADA SHORT
    i=85  ROXO   curta= 93.4288 longa= 93.8434   (close>curta, curta<longa)
    i=86  VERDE  curta= 96.0002 longa= 94.8576   -> ENTRADA LONG
    i=110 VERM   curta= 99.4228 longa=101.6976   -> ENTRADA SHORT
"""

import pytest

from backtest.keepitsimple import (BB_LEN, NAT_CAP, WARMUP, _adx, _descritivos,
                                   _ema, _sma, keepitsimple_entries, states)

_SERIE = ([100.0] * 41 + [110.0] * 20 + [108.0] * 4 + [90.0] * 20
          + [105.0] * 25 + [80.0] * 20)
_ENTRADAS = [(41, "LONG"), (65, "SHORT"), (86, "LONG"), (110, "SHORT")]


def _cndl(closes, tss=None):
    """Candles OHLCV no formato de producao (flat: O=H=L=C)."""
    tss = tss if tss is not None else [i * 1000 for i in range(len(closes))]
    return [{"ts": t, "open": c, "high": c, "low": c, "close": c,
             "volume": 1.0} for t, c in zip(tss, closes)]


def _bbw(closes):
    """Somente o bb_width_rel do payload descritivo."""
    return _descritivos(_cndl(closes), closes)[1]


def _idx(evs, tss=None):
    tss = tss if tss is not None else [i * 1000 for i in range(200)]
    pos = {t: i for i, t in enumerate(tss)}
    return [(pos[e["bar_ts"]], e["direction"]) for e in evs]


# --------------------------------------------------------------- EMA / SMA

def test_ema_semente_e_sma_das_n_primeiras():
    # semente em i = n-1 = media simples de [1..4]; antes disso, None
    out = _ema([1.0, 2.0, 3.0, 4.0, 5.0], 4)
    assert out[:3] == [None, None, None]
    assert out[3] == pytest.approx(2.5)


def test_ema_alpha_canonico():
    # alpha = 2/(4+1) = 0.4 -> e[4] = 0.4*5 + 0.6*2.5 = 3.5
    out = _ema([1.0, 2.0, 3.0, 4.0, 5.0], 4)
    assert out[4] == pytest.approx(3.5)


def test_ema_curta_demais_tudo_none():
    assert _ema([1.0, 2.0], 8) == [None, None]


def test_sma_none_antes_da_janela():
    out = _sma([1.0, 2.0, 3.0], 3)
    assert out[:2] == [None, None] and out[2] == pytest.approx(2.0)


# ------------------------------------------------------ tabela de estados

def test_cinco_estados_aparecem_na_serie_espec():
    assert sorted(set(states(_SERIE))) == ["AZUL", "CINZA", "ROXO", "VERDE",
                                           "VERM"]


def test_estados_nas_barras_ancoradas():
    st = states(_SERIE)
    assert (st[40], st[41]) == ("CINZA", "VERDE")
    assert (st[61], st[65]) == ("AZUL", "VERM")
    assert (st[85], st[86]) == ("ROXO", "VERDE")
    assert st[110] == "VERM"


def test_estado_coerente_com_as_emas_barra_a_barra():
    # a tabela verdade e re-derivada das EMAs; nenhum estado pode contradiza-la
    st, f, s = states(_SERIE), _ema(_SERIE, 8), _ema(_SERIE, 21)
    for i, c in enumerate(_SERIE):
        if f[i] is None or s[i] is None or c == f[i] or f[i] == s[i]:
            assert st[i] == "CINZA"
        elif c > f[i]:
            assert st[i] == ("VERDE" if f[i] > s[i] else "ROXO")
        else:
            assert st[i] == ("AZUL" if f[i] > s[i] else "VERM")


# ------------------------------------------------------------- entradas

def test_indices_exatos_das_entradas():
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert _idx(evs) == _ENTRADAS


def test_azul_e_roxo_nao_geram_entrada():
    # 61 (AZUL) e 85 (ROXO) sao transicoes de estado, mas nao sao gatilho
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert 61 not in [i for i, _d in _idx(evs)]
    assert 85 not in [i for i, _d in _idx(evs)]


def test_verde_persistente_nao_reentra():
    # VERDE de 41 a 60: uma unica entrada em todo o bloco
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert [i for i, d in _idx(evs) if d == "LONG" and i < 61] == [41]


def test_espacamento_por_direcao_em_tempo():
    # sep_bars=30 suprime a 2a LONG (86-41=45 barras < 30? nao) — usa 60
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=60, bar_ms=1000)
    longs = [i for i, d in _idx(evs) if d == "LONG"]
    assert longs == [41]                       # 86-41 = 45 < 60 -> suprimida


def test_espacamento_nao_afeta_direcao_oposta():
    # SHORT em 65 entra mesmo com a LONG de 41 dentro do gap (last e por direcao)
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=60, bar_ms=1000)
    assert (65, "SHORT") in _idx(evs)


# ------------------------------------------------------------ SEP = 0

def _serie_duas_verdes_coladas():
    """Duas transicoes para VERDE separadas por UMA barra: a serie sobe (VERDE),
    cai uma unica barra abaixo da EMA curta (AZUL — a EMA longa nao e cruzada)
    e volta a subir (VERDE de novo na barra seguinte)."""
    closes = [100.0] * 41 + [110.0] * 8 + [104.0] + [112.0] * 10
    st = states(closes)
    i = next(k for k in range(50, len(st)) if st[k] == "VERDE"
             and st[k - 1] == "AZUL")
    assert st[i - 1] == "AZUL" and st[i - 2] == "VERDE"   # gap de 1 barra
    return closes, i


def test_sep_zero_admite_duas_verdes_separadas_por_uma_barra():
    closes, i = _serie_duas_verdes_coladas()
    evs = keepitsimple_entries(_cndl(closes), sep_bars=0, bar_ms=1000)
    longs = [k for k, d in _idx(evs) if d == "LONG"]
    assert longs == [41, i] and i - 41 == 9      # ambas entram, coladas


def test_sep_padrao_suprimiria_a_segunda():
    # contraprova: com SEP_BARS=48 (o dos irmaos) a 2a VERDE some
    closes, i = _serie_duas_verdes_coladas()
    evs = keepitsimple_entries(_cndl(closes), sep_bars=48, bar_ms=1000)
    assert [k for k, d in _idx(evs) if d == "LONG"] == [41] and i not in [
        k for k, _d in _idx(evs)]


def test_sep_zero_nunca_suprime_nada_na_serie_espec():
    # com sep=0 a guarda `ts - last[d] < 0` e sempre falsa -> no-op
    a = keepitsimple_entries(_cndl(_SERIE), sep_bars=0, bar_ms=1000)
    st = states(_SERIE)
    esperado = [i for i in range(WARMUP, len(_SERIE))
                if (st[i] == "VERDE" and st[i - 1] != "VERDE")
                or (st[i] == "VERM" and st[i - 1] != "VERM")]
    assert [i for i, _d in _idx(a)] == esperado


def test_stage1_usa_kis_sep_zero():
    from backtest import stage1
    assert stage1.KIS_SEP == 0


def test_stage1_limiares_de_gate_por_detector():
    from backtest import stage1
    assert (stage1.GATE_MONEY_BPS, stage1.GATE_SKILL) == (0.0, 0.025)
    assert (stage1.KIS_MONEY_BPS, stage1.KIS_SKILL) == (5.0, 0.005)


# --------------------------------------------------------------- empate

def test_empate_exato_vira_cinza():
    # serie constante: close == curta == longa em float exato -> CINZA
    st = states([100.0] * 40)
    assert set(st) == {"CINZA"}


def test_empate_nao_gera_entrada():
    evs = keepitsimple_entries(_cndl([100.0] * 60), sep_bars=1, bar_ms=1000)
    assert evs == []


# --------------------------------------------------------------- warmup

def test_warmup_suprime_transicao_na_barra_20():
    # estado[19]=CINZA (EMA21 sem semente) e estado[20]=VERDE -> transicao
    # existe na barra 20, mas o warmup de 21 barras a descarta.
    closes = [100.0] * 20 + [110.0] * 10
    assert states(closes)[20] == "VERDE"
    assert keepitsimple_entries(_cndl(closes), sep_bars=1, bar_ms=1000) == []


def test_primeira_entrada_possivel_e_a_barra_21():
    # mesma serie, salto adiado em 1 barra -> a transicao cai exatamente em 21
    closes = [100.0] * 21 + [110.0] * 10
    evs = keepitsimple_entries(_cndl(closes), sep_bars=1, bar_ms=1000)
    assert _idx(evs) == [(WARMUP, "LONG")] and WARMUP == 21


def test_nenhuma_entrada_antes_do_warmup_na_serie_espec():
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert all(i >= WARMUP for i, _d in _idx(evs))


# ------------------------------------------------------ indexacao por ts

def test_gap_de_timestamp_nao_desloca_as_entradas():
    # buraco de 5 barras a partir da posicao 50: os ts deixam de ser i*1000.
    tss = [i * 1000 if i < 50 else (i + 5) * 1000 for i in range(len(_SERIE))]
    evs = keepitsimple_entries(_cndl(_SERIE, tss), sep_bars=1, bar_ms=1000)
    assert _idx(evs, tss) == _ENTRADAS
    # e os bar_ts emitidos sao os ts REAIS das barras, nao a grade uniforme
    assert [e["bar_ts"] for e in evs] == [tss[i] for i, _d in _ENTRADAS]
    assert [e["bar_ts"] for e in evs] != [i * 1000 for i, _d in _ENTRADAS]


def test_gap_nao_muda_os_estados():
    # o detector le closes por posicao; o gap so muda o rotulo temporal
    tss = [i * 1000 if i < 50 else (i + 5) * 1000 for i in range(len(_SERIE))]
    a = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    b = keepitsimple_entries(_cndl(_SERIE, tss), sep_bars=1, bar_ms=1000)
    assert [e["direction"] for e in a] == [e["direction"] for e in b]
    assert [e["hold"] for e in a] == [e["hold"] for e in b]


# ----------------------------------------------------------- saida nativa

def test_hold_conta_barras_ate_o_estado_mudar():
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    holds = {i: e["hold"] for (i, _d), e in zip(_idx(evs), evs)}
    assert holds[41] == 61 - 41       # VERDE de 41 a 60, sai em 61 (AZUL)
    assert holds[65] == 85 - 65       # VERM de 65 a 84, sai em 85 (ROXO)
    assert holds[86] == 110 - 86      # VERDE de 86 a 109, sai em 110 (VERM)


def test_hold_capado_em_nat_cap():
    # a ultima entrada (110) fica VERM ate o fim da serie -> cap
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert evs[-1]["hold"] == NAT_CAP == 48


def test_hold_sempre_entre_1_e_nat_cap():
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert all(1 <= e["hold"] <= NAT_CAP for e in evs)


# ------------------------------------------------------- payload autopsia

def test_estado_origem_e_o_estado_da_barra_anterior():
    st = states(_SERIE)
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    for (i, _d), e in zip(_idx(evs), evs):
        assert e["estado_origem"] == st[i - 1]


def test_descritivos_devolve_forca_e_bb_alinhados_com_as_barras():
    forca, bbw = _descritivos(_cndl(_SERIE), _SERIE)
    assert len(forca) == len(bbw) == len(_SERIE)
    assert forca[:25] == [None] * 25 and forca[25] is not None


def test_payload_descritivo_presente_em_toda_entrada():
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    for e in evs:
        assert set(e) == {"bar_ts", "direction", "hold", "estado_origem",
                          "forca_subindo", "bb_width_rel"}
        assert e["forca_subindo"] in (True, False, None)


def test_payload_nao_altera_a_decisao():
    # zerar high/low (destroi ADX -> forca_subindo) nao move nenhuma entrada
    flat = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    cnd = _cndl(_SERIE)
    for c in cnd:
        c["high"] = c["low"] = c["close"]
    assert _idx(keepitsimple_entries(cnd, 1, 1000)) == _idx(flat) == _ENTRADAS


def test_bb_width_rel_none_antes_da_janela_e_positivo_depois():
    bbw = _bbw(_SERIE)
    assert bbw[:BB_LEN - 1] == [None] * (BB_LEN - 1)
    assert bbw[BB_LEN - 1] == pytest.approx(0.0)      # serie constante -> sd=0
    assert bbw[45] > 0.0


def test_bb_width_rel_formula():
    # 2 * 1.3 * desvio_populacional / EMA21
    closes = _SERIE[:60]
    i = 55
    win = closes[i - BB_LEN + 1:i + 1]
    mu = sum(win) / BB_LEN
    sd = (sum((x - mu) ** 2 for x in win) / BB_LEN) ** 0.5
    esperado = 2.0 * 1.3 * sd / _ema(closes, BB_LEN)[i]
    assert _bbw(closes)[i] == pytest.approx(esperado)


# ----------------------------------------------------------------- ADX

def test_adx_none_ate_a_primeira_leitura_definida():
    cnd = _cndl(_SERIE)
    adx = _adx(cnd, 13)
    assert adx[:25] == [None] * 25 and adx[25] is not None


def test_adx_serie_sem_range_e_zero():
    # O=H=L=C -> TR=0 e DM=0 -> DX definido como 0.0 (guarda de divisao)
    adx = _adx(_cndl([100.0] * 60), 13)
    assert adx[25] == pytest.approx(0.0)


def test_adx_tendencia_monotona_satura():
    # alta estritamente monotona: +DM domina, -DM = 0 -> DX = 100
    closes = [100.0 + i for i in range(60)]
    cnd = [{"ts": i * 1000, "open": c, "high": c + 0.5, "low": c - 0.5,
            "close": c, "volume": 1.0} for i, c in enumerate(closes)]
    assert _adx(cnd, 13)[40] == pytest.approx(100.0)


def test_adx_curto_demais_tudo_none():
    assert _adx(_cndl([100.0] * 10), 13) == [None] * 10


# ------------------------------------------------- contrato com o soquete

def test_contrato_de_saida_igual_ao_dos_irmaos():
    # mesmas chaves obrigatorias que tsmom/donchian, mesma codificacao
    evs = keepitsimple_entries(_cndl(_SERIE), sep_bars=1, bar_ms=1000)
    assert evs and all(isinstance(e["bar_ts"], int) for e in evs)
    assert all(e["direction"] in ("LONG", "SHORT") for e in evs)
    assert [e["bar_ts"] for e in evs] == sorted(e["bar_ts"] for e in evs)


def test_stage1_registra_o_detector_e_o_pre_registro():
    from backtest import stage1
    assert (stage1.KIS_S, stage1.KIS_T, stage1.KIS_BRK_H) == (1.5, 3.0, 24)
    assert stage1.NAT_CAP == 48 and stage1.WARMUP == 21


def test_fwd_bar_e_consistente_com_fwd():
    # invariante da chave aditiva de measure_event: fwd_bar[h-1] == fwd[h].
    # A cadeia excursion -> replay -> exchanges exige aiohttp (ausente na
    # sandbox 3.11); na VM roda de verdade.
    pytest.importorskip("aiohttp")
    import sqlite3
    import tempfile

    from backtest.candle_store import connect, upsert_candles
    from backtest.excursion import HORIZONS, measure_event

    with tempfile.TemporaryDirectory() as d:
        db = f"{d}/t.db"
        conn = connect(db)
        bar = 14_400_000
        ks = [{"ts": i * bar, "open": 100.0 + i, "high": 101.0 + i,
               "low": 99.0 + i, "close": 100.5 + i, "volume": 1.0}
              for i in range(120)]
        upsert_candles(conn, "AAA", "4h", ks)
        m = measure_event(conn, "AAA", 50 * bar, "LONG", tf="4h")
        conn.close()
    # 120 barras com entrada na 50 -> 69 barras de futuro. Antes do horizonte
    # estendido (FWD_BAR_CAP=256) esta lista vinha com 48; hoje o teto e mais
    # alto que o futuro disponivel, entao ela vem com o que existe. O que o
    # keepitsimple consome (NAT_CAP=48) nao muda: o stage1 fatia [:48].
    assert m is not None and len(m["fwd_bar"]) == 69
    assert len(m["fwd_bar"][:NAT_CAP]) == 48
    for h in HORIZONS:
        assert m["fwd_bar"][h - 1] == pytest.approx(m["fwd"][h])
    assert isinstance(conn, sqlite3.Connection)
