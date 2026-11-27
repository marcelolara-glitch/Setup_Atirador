# backtest/kis_refino.py — Setup Atirador (SANDBOX, camada 2) — KIS 34/89 E1-3:
# cinco refinamentos e a carteira alavancada. Briefing v3 de 22/11/2026.
#
# O instrumento SO MEDE E DECOMPOE. Sem veredito, sem criterio de aceite.
#
# BASE (nao muda): KIS regra Extremos, EMA 34/89, confirmacao 2, portao (EMA21
# fixa, 5 barras, >= 0,02 ATR/barra a favor, ADX13 >= 11), saida na inversao ou
# stop fixo de 3 x ATR14 da barra do sinal, 4h, SYMBOLS_KIS_3489, custo 10 e 6
# bps por trade (ret10 = ret6 - 4).
#
# REUSADO, nao reescrito: entradas = `kis_horizonte.entradas`; portao =
# `kis_regime.inclinacoes` + `keepitsimple._adx` + `kis_regime.passa`; caminho =
# `holdout_2026.medir_sem_borda` (entry, ATR14 de 30 velas, fwd ate 256);
# populacao e marcacao no fim do periodo novo = mesma regra do
# `holdout_2026.trades_kis` (`_hold_visivel`); teto de leitura = `kis_saida.
# conecta_com_teto`; amplitude = `kis_meta.series_barra(...)["amp"]` agregada
# como no `kis_meta.coleta` e lida por `kis_meta.junta` (amplitude_fav).
# Conferencias chamam `kis_saida.run` e `holdout_2026.trades_kis` de verdade.
#
# MOTOR DE SAIDA UNICO (`simula`): barra a barra, nas MESMAS expressoes do
# `medir_sem_borda` (fav/adv/abertura/fechamento em ATR da barra do sinal).
# Ordem dentro da barra: (1) nivel adverso (stop -3 ATR; no T5, o gatilho da
# inversao se estiver acima do stop — o mais alto e tocado primeiro), preco
# min(nivel, abertura) como o `kis_saida.stop_exit`; (2) parciais do T3, preco
# max(nivel, abertura); (3) MFE/MAE com a barra inteira; (4) saida por tempo do
# T1 no fechamento. Stop e alvo na mesma barra: o stop vem primeiro (1 antes de
# 2). BASE com teto 256 reproduz `stop_exit` (teste de paridade exata).
#
# PERIODOS: sandbox H1/H2 (kis_saida.H1/H2, por ENTRADA, velas < 2026-08-26,
# exclusao de borda de 48 barras como o kis_saida); NOVO = entradas de
# 2026-08-31 16:00 a 2026-11-14 12:00 (holdout_2026.JANELA), velas < --end,
# sem exclusao de borda, aberto no fim marcado no fechamento da ultima barra.
# Uma leitura por simbolo (ate --end); o sandbox usa o prefixo ts < teto.
# Qualquer --end e aceito (23/11/2026): cada periodo e cortado em --end; o que
# fica vazio sai no log como `vazio (--end <data>)`, sem resumo nem carteira.
#
# TETO DE HOLD: `TETO_HOLD` = 256 so na celula BASE_256 (conferencia). Todas as
# outras celulas sem teto: o trade vai ate a proxima inversao (vetada ou nao)
# ou ate o fim do dado. `corte_teto` conta quantos o teto teria cortado.
#
# T5 (execucao sem esperar o fechamento) — ver `gatilhos`/`limiares_portao`:
# limiares de preco calculados SO com valores de t-1 (recursao da EMA). A
# confirmacao de 2 barras exige que a barra anterior ja tenha FECHADO no estado
# extremo. T5s: entradas da BASE (no fechamento); sai na 1a barra em que o
# preco toca o gatilho contrario (ou o stop). T5e: maquina de alvo propria
# dirigida pelos toques (o alvo vira no toque, com ou sem portao); entrada no
# toque de max(gatilho KIS, limiar do portao) — o mais exigente —, com ATR14 e
# ADX13 da barra t-1 (APROXIMACAO: os da barra t so existem no fechamento); o
# stop usa o ATR14 de 30 velas ate t-1. Na propria barra de entrada do T5e o
# stop e checado de forma conservadora (minima/maxima da barra pode ter vindo
# antes da entrada; assume-se que veio depois). Toque falso = o toque aconteceu
# e o fechamento daquela barra nao poria o estado no extremo.
#
# T3: custo por trade inalterado (5 bps na entrada + 5 bps x fracao em cada
# perna de saida = 10; idem 6). T2: amplitude NA veta (contado).
#
# VM 956 MiB: velas de um simbolo por vez; carteira com arrays ts/high/low/
# close por simbolo; teto de RSS do kis_trail (`--teto-rss`); guarda de
# horario do kis_saida (aviso).
#
# TEMPO GRAFICO (23/11/2026): `--tf` 15m/1h/4h. Duracoes definidas em barras de
# 4h (borda 48, teto 256, T1 6/12/24) viram a mesma duracao em barras do tf
# (`escala`); os nomes das celulas levam o numero em barras do tf (em 1h:
# BASE_1024, T1_24/48/96). Conferencias contra kis_saida/holdout_2026 so em 4h.
# 27/11/2026: 8h/12h/1D agregados do 4h (`le_velas`), fator fracao com
# duracao arredondada para cima (`em_barras`); NOVO pela barra de fechamento
# do tf (`janela_tf`).
#
# ESCALA DOS INDICADORES (24/11/2026): `escala(tf, ind, r)` / `indicadores`
# multiplicam por `ind` os comprimentos (EMA 34/89, EMA21 e lookback 5 do
# portao, ADX13, confirmacao 2, ATR14) e usam ATR efetivo = r x ATR(14 ind);
# limiar do portao 0,02 / ind. Honrado por `eventos`, `gatilhos` (confirmacao
# generica), `trades_simbolo` e `trades_t5e`. So o kis_grade expoe na linha de
# comando (--escala-indicadores); o main daqui e as celulas de universo
# (T2/T4) seguem em x1. ind = 1, r = 1: caminhos de sempre, byte-identicos.
#
# Uso (VM): python -m backtest.kis_refino --store <candles_v9.db>
from __future__ import annotations

import argparse
import csv
import math
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest import carteira as cart                                # noqa: E402
from backtest.aio_graduado import _Log                               # noqa: E402
from backtest.holdout_2026 import (END_ISO, JANELA, _hold_visivel,   # noqa: E402
                                   medir_sem_borda, na_janela)
from backtest.keepitsimple import (ADX_LEN, EXT_CAP, EXT_CONF,      # noqa: E402
                                   _adx, _ema, states)
from backtest.kis_horizonte import (PORTAO_ADX_MIN, PORTAO_LIMIAR,   # noqa: E402
                                    entradas)
from backtest.kis_knn import SEMENTES                                # noqa: E402
from backtest.kis_meta import junta, series_barra                    # noqa: E402
from backtest.kis_regime import (ATR_LEN, INCL_LOOKBACK,            # noqa: E402
                                 _atr_series, inclinacoes, passa)
from backtest.kis_saida import (BAR_4H_MS, COST_FWD_BPS, H1, H2,   # noqa: E402
                                TETO_ISO, _guarda_relogio, _iso, _ms,
                                conecta_com_teto)
from backtest.kis_tf import AGREGA, CTX, agregar                      # noqa: E402
from backtest.kis_trail import (COST_BPS, _dentro, _quarter,      # noqa: E402
                                _rss_mib, add_teto_rss, define_teto_rss,
                                teto_rss)
from backtest.vwma_knn import TOP_FRAC, concentracao                 # noqa: E402

# ----------------------------------------------------------- parametros -----
PAR = (34, 89)
EMA_REF = 21                       # EMA do portao (fixa)
STOP_K = 3.0
TETO_HOLD = EXT_CAP                # 256, so na BASE_256
BORDA = 48                         # exclusao de borda do sandbox (kis_saida)
T1_NS = (6, 12, 24)
T1_PROGRESSO = 1.0                 # ATR a favor (MFE intrabarra) ate a barra N
T2_LIMIAR = 50.0                   # % do universo com EMA34 > EMA89 (a favor)
T3_PARCIAIS = ((4.0, 0.50, "parcial1"), (8.0, 0.25, "parcial2"))
T4_KS = (20, 40)
SETUP = "KIS_E1-3"
CELULAS = (("BASE_256", "BASE") + tuple(f"T1_{n}" for n in T1_NS) +
           ("T2", "T3") + tuple(f"T4_{k}" for k in T4_KS) + ("T5s", "T5e"))
PERIODOS = ("H1", "H2", "NOVO")
DELTA_10 = COST_FWD_BPS - COST_BPS
TIPOS = ("stop", "inversao", "tempo", "parcial1", "parcial2", "teto", "fim",
         "escada1", "escada2", "escada3")


# ------------------------------------------------- escala do tempo grafico --
def escala(tf: str, ind: int = 1, r: float = 1.0) -> dict:
    """Duracoes definidas em barras de 4h convertidas para o tf, MESMA
    duracao no tempo (decisao do Marcelo, 23/11/2026). Indicadores, portao,
    confirmacao e ATR seguem em BARRAS do tf (KIS em 1h = o mesmo setup em
    barras de 1h); stop e parciais sao multiplos de ATR (ja relativos ao tf).
    So muda o que e DURACAO: borda de exclusao do sandbox (BORDA = 48 barras
    de 4h = 8 dias), teto do caminho e do hold (EXT_CAP = 256 de 4h = 42,7
    dias; BASE_<cap>) e as saidas por tempo do T1 (6/12/24 de 4h = 24/48/96
    h). Em 4h, fator 1: tudo identico ao de antes. tf que divide 4h (15m, 1h)
    tem fator inteiro; tf multiplo de 4h (8h, 12h, 1D, 27/11/2026: velas
    agregadas do 4h por `le_velas`) tem fator fracao 1/2, 1/3, 1/6, e a
    duracao em barras arredonda PARA CIMA (`em_barras`: 256 -> 128/86/43).
    Outro tf -> ValueError."""
    from fractions import Fraction
    from backtest.excursion import _BAR_MS            # tardio, como no kis_tf
    bar = _BAR_MS.get(tf)
    if bar is None and tf in AGREGA:
        bar = AGREGA[tf] * BAR_4H_MS
    if bar is None or (BAR_4H_MS % bar and bar % BAR_4H_MS):
        raise ValueError(f"tf '{tf}': use um de {sorted(_BAR_MS)} ou "
                         f"{sorted(AGREGA)}")
    f = BAR_4H_MS // bar if BAR_4H_MS % bar == 0 else Fraction(BAR_4H_MS, bar)
    if not (isinstance(ind, int) and ind >= 1 and r > 0):
        raise ValueError(f"escala de indicadores {ind!r} / r {r!r}: inteiro "
                         ">= 1 e r > 0")
    esc = {"tf": tf, "bar_ms": bar, "fator": f}
    return {**esc, "borda": em_barras(BORDA, esc),
            "cap": em_barras(EXT_CAP, esc),
            "t1_ns": tuple(em_barras(n, esc) for n in T1_NS),
            **indicadores(ind, r)}


def em_barras(n4h: int, esc: dict) -> int:
    """n barras de 4h na MESMA duracao em barras do tf de `esc`. Fator inteiro
    (15m, 1h, 4h): n x fator, a conta de sempre. Fator fracao (8h, 12h, 1D):
    arredondado para cima (12h: 256 -> 86)."""
    f = esc["fator"]
    return n4h * f if isinstance(f, int) else math.ceil(n4h * f)


def agregado(esc: dict):
    """Barras de 4h por barra do tf quando o tf e agregado do 4h (8h 2, 12h 3,
    1D 6); None quando o tf vem direto do store."""
    return AGREGA.get(esc["tf"])


def le_velas(conn, sym: str, esc: dict, end_ms: int) -> list:
    """Velas do simbolo ate end_ms (exclusivo). tf agregado: le o 4h e agrega
    em memoria por `kis_tf.agregar` (blocos alinhados a 00:00 UTC; bloco
    incompleto, nas pontas ou por buraco, descartado: nenhuma barra do tf
    termina depois de end_ms se end_ms e multiplo de 4h). Outro tf: a leitura
    de sempre."""
    from backtest.candle_store import read_candles
    n = agregado(esc)
    if n is None:
        return read_candles(conn, sym, esc["tf"], start_ms=None,
                            end_ms=end_ms - 1)
    return agregar(read_candles(conn, sym, "4h", start_ms=None,
                                end_ms=end_ms - 1), n)


def janela_tf(esc: dict, janela=JANELA) -> tuple:
    """Janela de entrada do NOVO no tf. Ate 4h: a de sempre (ts da barra).
    tf > 4h (27/11/2026): a entrada vale pelo FECHAMENTO da barra do tf, na
    mesma faixa de fechamentos do 4h (2026-08-31 20:00 .. 2026-11-14 16:00):
    a janela em ts de abertura recua bar_ms - 4h nas duas pontas."""
    d = esc["bar_ms"] - BAR_4H_MS
    if d <= 0 or janela is None:
        return janela
    return (janela[0] - d, janela[1] - d)


def indicadores(ind: int = 1, r: float = 1.0) -> dict:
    """Comprimentos dos indicadores e do sinal x `ind` (24/11/2026,
    `--escala-indicadores F`): EMA 34/89 -> 34F/89F (estados e gatilhos), EMA
    do portao 21 -> 21F, lookback da inclinacao 5 -> 5F, ADX 13 -> 13F (minimo
    11 inalterado), confirmacao 2 -> 2F, ATR 14 -> 14F. ATR efetivo = r x
    ATR(14F) (r = mediana de ATR14 de 4h / ATR(14F) do tf, medida fora daqui)
    em tudo que usa ATR: stop, parciais, portao, unidades de R/MFE/MAE. Limiar
    do portao e por barra: 0,02 / F. ind = 1 e r = 1: os valores de sempre."""
    return {"ind": ind, "r": r, "ema": (PAR[0] * ind, PAR[1] * ind),
            "ema_ref": EMA_REF * ind, "lb": INCL_LOOKBACK * ind,
            "adx": ADX_LEN * ind, "conf": EXT_CONF * ind,
            "atr": ATR_LEN * ind,
            "limiar": PORTAO_LIMIAR if ind == 1 else PORTAO_LIMIAR / ind}


def _ind(esc: dict) -> dict:
    """Indicadores de `esc`; dict de escala sem as chaves (antigo) = x1."""
    return esc if "ind" in esc else indicadores()


def incl_adx(cndl: list, closes: list, esc: dict) -> tuple:
    """(inclinacao, ADX) do portao na escala de `esc`. x1: as chamadas de
    sempre (byte-identico). xF: inclinacao da EMA(21F) em 5F barras sobre o
    ATR efetivo r x ATR(14F); ADX(13F)."""
    x = _ind(esc)
    if x["ind"] == 1 and x["r"] == 1.0:
        return inclinacoes(cndl, closes), _adx(cndl)
    incl = inclinacoes(cndl, closes, x["atr"], x["ema_ref"], x["lb"])
    if x["r"] != 1.0:
        incl = [None if v is None else v / x["r"] for v in incl]
    return incl, _adx(cndl, x["adx"])


def atr_ef(cndl: list, esc: dict) -> float:
    """ATR efetivo da ULTIMA vela de `cndl` (fatia ate a barra): r x
    ATR(14F) de `excursion._wilder_atr`; 0.0 se nao houver velas."""
    from backtest.excursion import _wilder_atr        # tardio, como no kis_tf
    x = _ind(esc)
    a = _wilder_atr(cndl, x["atr"])
    return a * x["r"] if x["r"] != 1.0 else a


def nomes_celulas(esc: dict = None) -> tuple:
    """CELULAS no tf de `esc` (numeros = barras do tf). Em 4h = CELULAS."""
    esc = esc or escala("4h")
    return ((f"BASE_{esc['cap']}", "BASE") +
            tuple(f"T1_{n}" for n in esc["t1_ns"]) + ("T2", "T3") +
            tuple(f"T4_{k}" for k in T4_KS) + ("T5s", "T5e"))


def nomes_simb(esc: dict = None) -> tuple:
    """CEL_SIMB no tf de `esc`. Em 4h = CEL_SIMB."""
    esc = esc or escala("4h")
    return ((f"BASE_{esc['cap']}", "BASE") +
            tuple(f"T1_{n}" for n in esc["t1_ns"]) + ("T3", "T5s", "T5e"))


# ------------------------------------------------------- limiares (T5) ------
def gatilhos(cndl: list, closes: list = None, fast: int = PAR[0],
             slow: int = PAR[1], conf: int = EXT_CONF) -> tuple:
    """(gL, gS, st). gL[t] = preco acima do qual o fechamento de t poe o estado
    em VERDE, definido so se a barra t-1 FECHOU em VERDE (a confirmacao de 2
    se completaria em t); gS[t] idem VERM. So usa EMAs de t-1:
      close > EMA34_t        <=> p > EMA34_{t-1}
      EMA34_t > EMA89_t      <=> p > [(1-b)EMA89_{t-1} - (1-a)EMA34_{t-1}]/(a-b)
    gL = max dos dois; gS = min (VERM: p < os dois). Confirmacao `conf`
    generica: gL[t] definido se o estado VERDE ja dura >= conf-1 barras
    FECHADAS ate t-1 (conf 2: st[t-1] == VERDE, a regra de sempre)."""
    closes = closes if closes is not None else [c["close"] for c in cndl]
    ef, es, st = _ema(closes, fast), _ema(closes, slow), states(closes, fast,
                                                                slow)
    a, b = 2.0 / (fast + 1.0), 2.0 / (slow + 1.0)
    n = len(closes)
    gL, gS = [None] * n, [None] * n
    run = 0                                  # barras no estado st[t-1]
    for t in range(1, n):
        run = run + 1 if t > 1 and st[t - 1] == st[t - 2] else 1
        f, s = ef[t - 1], es[t - 1]
        if f is None or s is None or run < conf - 1:
            continue
        l2 = ((1.0 - b) * s - (1.0 - a) * f) / (a - b)
        if st[t - 1] == "VERDE":
            gL[t] = max(f, l2)
        elif st[t - 1] == "VERM":
            gS[t] = min(f, l2)
    return gL, gS, st


def limiares_portao(cndl: list, closes: list = None, ref: int = EMA_REF,
                    lb: int = INCL_LOOKBACK, limiar: float = PORTAO_LIMIAR,
                    adx_min: float = PORTAO_ADX_MIN, atr_len: int = ATR_LEN,
                    adx_len: int = ADX_LEN, r: float = 1.0) -> tuple:
    """(pL, pS, ok_adx). Inclinacao da EMA21 com a recursao: LONG passa se
    p >= pL[t], SHORT se p <= pS[t], com ATR14 de t-1 no denominador e
    ok_adx[t] = ADX13[t-1] >= adx_min (aproximacao documentada). Com
    indicadores escalados: ATR(atr_len) x r, ADX(adx_len)."""
    closes = closes if closes is not None else [c["close"] for c in cndl]
    e, atr, adx = (_ema(closes, ref), _atr_series(cndl, atr_len),
                   _adx(cndl, adx_len))
    c = 2.0 / (ref + 1.0)
    n = len(closes)
    pL, pS, ok = [None] * n, [None] * n, [False] * n
    for t in range(lb + 1, n):
        a, e1, e5 = atr[t - 1], e[t - 1], e[t - lb]
        if a is not None and r != 1.0:
            a *= r
        if a is None or a <= 0 or e1 is None or e5 is None:
            continue
        base = e5 - (1.0 - c) * e1
        pL[t] = (base + lb * limiar * a) / c
        pS[t] = (base - lb * limiar * a) / c
        ok[t] = adx[t - 1] is not None and adx[t - 1] >= adx_min
    return pL, pS, ok


# ------------------------------------------------------------ motor ---------
def _barra(c: dict, entry: float, atr: float, sg: float) -> tuple:
    """(fav, adv, abertura, fechamento) em ATR — expressoes do medir_sem_borda."""
    if sg > 0:
        fav, adv = (c["high"] - entry) / atr, (entry - c["low"]) / atr
    else:
        fav, adv = (entry - c["low"]) / atr, (c["high"] - entry) / atr
    return fav, adv, sg * (c["open"] - entry) / atr, \
        sg * (c["close"] - entry) / atr


def nivel_p3(parciais) -> float:
    """P3 da escada = P2 + (P2 - P1), em ATR."""
    (a, _fa, _na), (b, _fb, _nb) = parciais
    return b + (b - a)


def degraus_escada(parciais) -> tuple:
    """Escada de stop (briefing de 24/11/2026): ((bit de parcial ou None,
    gatilho em ATR, novo stop em ATR, tipo)). Degrau 1: P1 executada -> stop
    na entrada; 2: P2 executada -> stop em P1; 3: toque em P3 -> stop em P2."""
    (a, _fa, _na), (b, _fb, _nb) = parciais
    return ((0, a, 0.0, "escada1"), (1, b, a, "escada2"),
            (None, nivel_p3(parciais), b, "escada3"))


def simula(cndl: list, i: int, e: int, entry: float, atr: float, sg: float,
           k: float = STOP_K, tempo_n: int = None, parciais=(), gat=None,
           tipo_fim: str = "inversao", escada: bool = False) -> tuple:
    """Barras i+1..e. Devolve ([(idx, r_atr, preco, fracao, tipo)], mfe, mae).
    gat(idx) -> preco do gatilho de saida (T5) ou None. `escada` (so com 2
    parciais): o stop sobe a partir da barra SEGUINTE a cada degrau de
    `degraus_escada`, so sobe; saida pelo stop movido tem o tipo do degrau."""
    execs, resto, pegos, mfe, mae = [], 1.0, 0, 0.0, 0.0
    px_stop = entry - sg * k * atr
    s_nivel, s_tipo = -k, "stop"
    degraus = degraus_escada(parciais) if escada and parciais else ()
    for idx in range(i + 1, e + 1):
        c = cndl[idx]
        fav, adv, op, cl = _barra(c, entry, atr, sg)
        nivel, px_n, tipo = s_nivel, px_stop, s_tipo
        g = gat(idx) if gat is not None else None
        if g is not None:
            gl = sg * (g - entry) / atr
            if gl > nivel:
                nivel, px_n, tipo = gl, g, "inversao"
        if -adv <= nivel:
            r, px = (op, c["open"]) if op < nivel else (nivel, px_n)
            execs.append((idx, r, px, resto, tipo))
            return execs, mfe, max(mae, -r)
        for n, (lvl, fr, nome) in enumerate(parciais):
            if not pegos >> n & 1 and fav >= lvl:
                r, px = ((op, c["open"]) if op > lvl
                         else (lvl, entry + sg * lvl * atr))
                execs.append((idx, r, px, fr, nome))
                resto -= fr
                pegos |= 1 << n
        for bit, gat_lvl, novo, nome in degraus:      # vale da proxima barra
            feito = pegos >> bit & 1 if bit is not None else fav >= gat_lvl
            if feito and novo > s_nivel:
                s_nivel, s_tipo = novo, nome
                px_stop = entry + sg * novo * atr
        mfe, mae = max(mfe, fav), max(mae, adv)
        if (tempo_n and idx < e and idx - i == tempo_n
                and mfe < T1_PROGRESSO):
            execs.append((idx, cl, c["close"], resto, "tempo"))
            return execs, mfe, mae
    c = cndl[e]
    execs.append((e, sg * (c["close"] - entry) / atr, c["close"], resto,
                  tipo_fim))
    return execs, mfe, mae


def _trade(cel: str, sym: str, cndl: list, i: int, lg: bool, entry: float,
           atr: float, sim: tuple, falso_e: bool = False,
           falso_s: bool = False, teto_hold: int = TETO_HOLD) -> dict:
    execs, mfe, mae = sim
    r = 0.0
    for x in execs:
        r += x[3] * x[1]
    ret6 = r * (atr / entry) * 1e4 - COST_BPS
    return {"cel": cel, "sym": sym, "long": lg, "ts": cndl[i]["ts"],
            "entry": entry, "atr": atr, "hold": execs[-1][0] - i,
            "execs": tuple((cndl[x[0]]["ts"], x[2], x[3], x[4]) for x in execs),
            "tipos": "+".join(x[4] for x in execs), "ret6": ret6,
            "ret10": ret6 - DELTA_10, "mfe": mfe, "mae": mae,
            "corte": execs[-1][0] - i > teto_hold,
            "falso_e": falso_e, "falso_s": falso_s}


def novo_cont() -> dict:
    return {"inversoes": 0, "vetadas": 0, "atr0": 0, "excl_borda": 0,
            "excl_hold": 0, "t5e_viradas": 0, "t5e_vetadas": 0,
            "t5e_borda": 0, "t5e_atr0": 0, "t5e_stop_na_entrada": 0}


CEL_SIMB = ("BASE_256", "BASE") + tuple(f"T1_{n}" for n in T1_NS) + \
    ("T3", "T5s", "T5e")


def eventos(cndl: list, modo: str, cont: dict, janela=JANELA,
            incl=None, adx=None, esc: dict = None):
    """Entradas aceitas pelo portao, com o fim de cada trade, nas regras do
    kis_saida.run (sandbox: borda 48) e do holdout_2026 (novo: janela, sem
    borda). Gera dicts {i, lg, sg, en, atr, e_inv, e_full, t_full, hold,
    t256}. Fonte unica das entradas do kis_refino e do kis_grade. `esc` =
    `escala(tf)` (default 4h): borda, teto do caminho e duracao da barra.
    `hold` = barras ate a proxima inversao, piso 1 e teto esc["cap"] (em 4h
    = o `hold` do `kis_horizonte.entradas`, mesma conta)."""
    esc = esc or escala("4h")
    cap, borda, x = esc["cap"], esc["borda"], _ind(esc)
    if incl is None:
        incl, adx = incl_adx(cndl, [c["close"] for c in cndl], esc)
    pos = {c["ts"]: i for i, c in enumerate(cndl)}
    evs = entradas(cndl, x["ema"][0], x["ema"][1], x["conf"])
    idx = [pos[ev["bar_ts"]] for ev in evs]
    ult = len(cndl) - 1
    for n_ev, ev in enumerate(evs):
        i = idx[n_ev]
        if modo == "novo" and not na_janela(ev["bar_ts"], janela):
            continue
        cont["inversoes"] += 1
        if not passa(ev["direction"], incl[i], adx[i], x["limiar"],
                     PORTAO_ADX_MIN):
            cont["vetadas"] += 1
            continue
        m = medir_sem_borda(cndl, i, ev["direction"], bar_ms=esc["bar_ms"],
                            cap=cap, periodo_atr=x["atr"], mult_atr=x["r"])
        if m is None:
            cont["atr0"] += 1
            continue
        ultimo = n_ev == len(evs) - 1
        e_inv = None if ultimo else idx[n_ev + 1]
        dist = (ult if ultimo else e_inv) - i
        hold_b = min(max(dist, 1), cap)           # 4h: = ev["hold"]
        if modo == "sandbox":                     # regra do kis_saida.run
            if len(m["fwd_bar"]) < borda:
                cont["excl_borda"] += 1
                continue
            hold = hold_b
            if hold > len(m["fwd_bar"]):
                cont["excl_hold"] += 1
                continue
            t256 = "fim" if ultimo and i + hold == ult else \
                ("teto" if dist > cap else "inversao")
        else:                                     # regra do holdout_2026
            fim = ultimo and ult - i <= cap
            hold = _hold_visivel(m, min(hold_b, ult - i) if fim
                                 else hold_b, fim, cont)
            if hold is None:
                continue
            t256 = "fim" if fim else ("teto" if dist > cap else "inversao")
        lg = ev["direction"] == "LONG"
        yield {"i": i, "lg": lg, "sg": 1.0 if lg else -1.0,
               "en": m["entry"], "atr": m["atr"], "e_inv": e_inv,
               "e_full": ult if ultimo else e_inv,
               "t_full": "fim" if ultimo else "inversao",
               "hold": hold, "t256": t256}


def trades_simbolo(cndl: list, sym: str, modo: str, cont: dict,
                   janela=JANELA, esc: dict = None) -> dict:
    """Trades de um simbolo nas celulas que dependem so dele. modo 'sandbox'
    (borda 48, sem janela) ou 'novo' (janela de entrada, sem borda). `esc` =
    `escala(tf)` (default 4h); chaves = `nomes_simb(esc)`."""
    esc = esc or escala("4h")
    nomes = nomes_simb(esc)
    out = {c: [] for c in nomes}
    if not cndl:
        return out
    closes = [c["close"] for c in cndl]
    x = _ind(esc)
    incl, adx = incl_adx(cndl, closes, esc)
    gL, gS, st = gatilhos(cndl, closes, *x["ema"], x["conf"])
    for ev in eventos(cndl, modo, cont, janela, incl, adx, esc):
        i, lg, sg, en, atr = ev["i"], ev["lg"], ev["sg"], ev["en"], ev["atr"]
        e_inv, e_full, t_full = ev["e_inv"], ev["e_full"], ev["t_full"]
        add = lambda cel, sim, **kw: out[cel].append(            # noqa: E731
            _trade(cel, sym, cndl, i, lg, en, atr, sim,
                   teto_hold=esc["cap"], **kw))
        add(nomes[0], simula(cndl, i, i + ev["hold"], en, atr, sg,
                             tipo_fim=ev["t256"]))
        add("BASE", simula(cndl, i, e_full, en, atr, sg, tipo_fim=t_full))
        for n in esc["t1_ns"]:
            add(f"T1_{n}", simula(cndl, i, e_full, en, atr, sg, tempo_n=n,
                                  tipo_fim=t_full))
        add("T3", simula(cndl, i, e_full, en, atr, sg, parciais=T3_PARCIAIS,
                         tipo_fim=t_full))
        g = gS if lg else gL
        sim = simula(cndl, i, e_full, en, atr, sg, gat=lambda j: g[j],
                     tipo_fim=t_full)
        x = sim[0][-1]
        add("T5s", sim, falso_s=x[4] == "inversao" and (
            e_inv is None or x[0] < e_inv))
    out["T5e"] = trades_t5e(cndl, sym, modo, cont, closes, gL, gS, st, janela,
                            esc)
    return out


def viradas_t5e(cndl: list, gL: list, gS: list, ini: int = PAR[1]) -> list:
    """Maquina de alvo dirigida pelos toques: [(t, +1/-1)]."""
    alvo, out = 0, []
    for t in range(ini, len(cndl)):
        if alvo != 1 and gL[t] is not None and cndl[t]["high"] >= gL[t]:
            out.append((t, 1))
            alvo = 1
        elif alvo != -1 and gS[t] is not None and cndl[t]["low"] <= gS[t]:
            out.append((t, -1))
            alvo = -1
    return out


def trades_t5e(cndl: list, sym: str, modo: str, cont: dict, closes: list,
               gL: list, gS: list, st: list, janela=JANELA,
               esc: dict = None) -> list:
    from backtest.excursion import _wilder_atr        # tardio, como no kis_tf
    esc = esc or escala("4h")
    xi = _ind(esc)
    um = xi["ind"] == 1 and xi["r"] == 1.0
    if um:
        pL, pS, ok = limiares_portao(cndl, closes)
        fl = viradas_t5e(cndl, gL, gS)
    else:
        pL, pS, ok = limiares_portao(cndl, closes, xi["ema_ref"], xi["lb"],
                                     xi["limiar"], PORTAO_ADX_MIN, xi["atr"],
                                     xi["adx"], xi["r"])
        fl = viradas_t5e(cndl, gL, gS, xi["ema"][1])
    ult, out = len(cndl) - 1, []
    for k, (t, sd) in enumerate(fl):
        c = cndl[t]
        if modo == "novo" and not na_janela(c["ts"], janela):
            continue
        if modo == "sandbox" and ult - t < esc["borda"]:
            cont["t5e_borda"] += 1
            continue
        cont["t5e_viradas"] += 1
        lim = pL[t] if sd > 0 else pS[t]
        if not ok[t] or lim is None:
            cont["t5e_vetadas"] += 1
            continue
        g_ent = max(gL[t], lim) if sd > 0 else min(gS[t], lim)
        if (c["high"] < g_ent) if sd > 0 else (c["low"] > g_ent):
            cont["t5e_vetadas"] += 1
            continue
        en = max(g_ent, c["open"]) if sd > 0 else min(g_ent, c["open"])
        atr = (_wilder_atr(cndl[max(0, t - CTX):t]) if um
               else atr_ef(cndl[max(0, t - xi["atr"] - 1):t], esc))
        if atr <= 0:
            cont["t5e_atr0"] += 1
            continue
        sg, lg = float(sd), sd > 0
        falso_e = st[t] != ("VERDE" if lg else "VERM")
        px_stop = en - sg * STOP_K * atr
        if (c["low"] <= px_stop) if lg else (c["high"] >= px_stop):
            cont["t5e_stop_na_entrada"] += 1
            sim, falso_s = ([(t, -STOP_K, px_stop, 1.0, "stop")], 0.0,
                            STOP_K), False
        else:
            nxt = fl[k + 1][0] if k + 1 < len(fl) else None
            if nxt is None:
                sim = simula(cndl, t, ult, en, atr, sg, tipo_fim="fim")
            else:
                gx = gS[nxt] if lg else gL[nxt]
                sim = simula(cndl, t, nxt, en, atr, sg,
                             gat=lambda j, n=nxt, g=gx: g if j == n else None)
            x = sim[0][-1]
            falso_s = (nxt is not None and x[4] == "inversao" and
                       st[x[0]] != ("VERM" if lg else "VERDE"))
        out.append(_trade("T5e", sym, cndl, t, lg, en, atr, sim,
                          falso_e=falso_e, falso_s=falso_s,
                          teto_hold=esc["cap"]))
    return out


# ------------------------------------------------- celulas de universo ------
def filtro_t2(base: list, amp: dict) -> tuple:
    """T2 = BASE com amplitude_fav >= T2_LIMIAR (kis_meta.junta). (lista,
    n_na)."""
    feats = [{} for _ in base]
    junta([(t["ts"], t["sym"], t["long"]) for t in base], feats, amp, {})
    out, na = [], 0
    for t, f in zip(base, feats):
        a = f["amplitude_fav"]
        if a is None:
            na += 1
        elif a >= T2_LIMIAR:
            out.append(dict(t, cel="T2"))
    return out, na


def teto_lado(base: list, k: int, ordem: dict, cel: str) -> list:
    """T4: pula a entrada se ja ha k posicoes abertas no mesmo lado. Ordem por
    ts e, no mesmo ts, pela ordem de SYMBOLS_KIS_3489. Aberta = aceita antes e
    com barra da saida final > ts da nova entrada."""
    abertas = {True: [], False: []}
    out = []
    for t in sorted(base, key=lambda t: (t["ts"], ordem.get(t["sym"], 1e9),
                                         t["sym"])):
        lado = [x for x in abertas[t["long"]] if x > t["ts"]]
        abertas[t["long"]] = lado
        if len(lado) >= k:
            continue
        lado.append(t["execs"][-1][0])
        out.append(dict(t, cel=cel))
    return out


def periodos_ativos(end_ms: int) -> tuple:
    """Periodos com alguma entrada possivel antes de --end (23/11/2026: --end
    livre; regra de estudo nao trava simulacao). Periodo cujo inicio nao e
    anterior a --end fica vazio: sem linha no resumo e sem carteira."""
    ini = {"H1": _ms(H1[0]), "H2": _ms(H2[0]), "NOVO": JANELA[0]}
    return tuple(p for p in PERIODOS if ini[p] < end_ms)


def log_vazios(log, ativos: tuple, end_iso: str) -> None:
    for p in PERIODOS:
        if p not in ativos:
            log(f"  {p}: vazio (--end {end_iso})")


def periodo(t: dict, modo: str):
    if modo == "novo":
        return "NOVO"
    return "H1" if _dentro(t["ts"], H1) else ("H2" if _dentro(t["ts"], H2)
                                              else None)


# ------------------------------------------------------------ numeros -------
def resumo(tr: list) -> dict:
    n = len(tr)
    r10 = [t["ret10"] for t in tr]
    r6 = [t["ret6"] for t in tr]
    cum = pico = dd = 0.0
    for r in r10:
        cum += r
        pico, dd = max(pico, cum), max(dd, max(pico, cum) - cum)
    tri, mes = {}, {}
    for t in tr:
        tri[_quarter(t["ts"])] = tri.get(_quarter(t["ts"]), 0.0) + t["ret10"]
        mm = _iso(t["ts"])[:7]
        mes[mm] = mes.get(mm, 0.0) + t["ret10"]
    g = [r for r in r10 if r > 0]
    p = [r for r in r10 if r <= 0]
    fim = Counter(t["execs"][-1][3] for t in tr)
    mm = max(mes, key=mes.get) if mes else None
    lados = {}
    for nome, lado in (("L", True), ("S", False)):
        s = [t["ret10"] for t in tr if t["long"] is lado]
        lados[nome] = (len(s), sum(s), sum(s) / len(s) if s else 0.0)
    return {
        "n": n, "tot10": sum(r10), "tot6": sum(r6),
        "bpt10": sum(r10) / n if n else 0.0, "bpt6": sum(r6) / n if n else 0.0,
        "acerto": 100.0 * len(g) / n if n else 0.0,
        "ganho": mean(g) if g else 0.0, "perda": mean(p) if p else 0.0,
        "pico": pico, "dd": dd,
        "tri_pos": sum(1 for v in tri.values() if v > 0), "tri_tot": len(tri),
        "melhor_mes": mm, "sem_mes": sum(r10) - (mes[mm] if mm else 0.0),
        "sem_top": concentracao(r10)[2], "n_top": concentracao(r10)[0],
        "saidas": {k: 100.0 * fim[k] / n if n else 0.0 for k in TIPOS},
        "p1": sum(1 for t in tr if "parcial1" in t["tipos"]),
        "p2": sum(1 for t in tr if "parcial2" in t["tipos"]),
        "corte": sum(1 for t in tr if t["corte"]),
        "teto": fim["teto"], "fim": fim["fim"], "lados": lados,
        "falso_e": sum(t["falso_e"] for t in tr),
        "falso_s": sum(t["falso_s"] for t in tr),
    }


def placebo_t2(base: list, t2: list, sementes=SEMENTES) -> list:
    """Total @10 de vetos ao acaso: por lado, a mesma quantidade que o T2."""
    out = []
    for s in sementes:
        rng, tot = random.Random(s), 0.0
        for lado in (True, False):
            pop = [t for t in base if t["long"] is lado]
            n_v = len(pop) - sum(1 for t in t2 if t["long"] is lado)
            vet = set(rng.sample(range(len(pop)), n_v))
            tot += sum(t["ret10"] for j, t in enumerate(pop) if j not in vet)
        out.append(tot)
    return out


CAMPOS = ["celula", "periodo", "bar_ts_iso", "mes", "sym", "direcao", "hold",
          "saida", "ret6", "ret10", "mfe_atr", "mae_atr"]


def linha_csv(t: dict, per: str) -> dict:
    iso = _iso(t["ts"])
    return {"celula": t["cel"], "periodo": per, "bar_ts_iso": iso,
            "mes": iso[:7], "sym": t["sym"],
            "direcao": "LONG" if t["long"] else "SHORT", "hold": t["hold"],
            "saida": t["tipos"], "ret6": f"{t['ret6']:.2f}",
            "ret10": f"{t['ret10']:.2f}", "mfe_atr": f"{t['mfe']:.3f}",
            "mae_atr": f"{t['mae']:.3f}"}


def para_carteira(t: dict) -> dict:
    return {"rotulo": f"{SETUP}/{t['cel']}", "sym": t["sym"],
            "direcao": "LONG" if t["long"] else "SHORT",
            "ts_entrada": t["ts"], "preco_entrada": t["entry"],
            "saidas": [(ts, px, f) for ts, px, f, _tp in t["execs"]]}


# ------------------------------------------------------------ coleta --------
def coleta(conn, symbols: list, tf: str, teto_ms: int, end_ms: int,
           conferir: bool = True, esc: dict = None) -> dict:
    """Uma leitura por simbolo (ate end_ms); sandbox = prefixo ts < teto.
    `conferir` liga a referencia do holdout_2026 (so faz sentido em 4h: o
    `trades_kis` mede em barras de 4h)."""
    from backtest.holdout_2026 import novo_cont as nc_h, trades_kis
    esc = esc or escala(tf)
    jan = janela_tf(esc)
    tr = {m: {c: [] for c in nomes_simb(esc)} for m in ("sandbox", "novo")}
    cont = {m: novo_cont() for m in ("sandbox", "novo")}
    cam = {"sandbox": {}, "novo": {}}
    amp, ref_novo, sem, rss = {}, {}, [], _rss_mib()
    for sym in symbols:
        cndl = le_velas(conn, sym, esc, end_ms)
        if not cndl:
            sem.append(sym)
            continue
        sb = [c for c in cndl if c["ts"] < teto_ms]
        for modo, cv in (("sandbox", sb), ("novo", cndl)):
            for cel, lst in trades_simbolo(cv, sym, modo, cont[modo], jan,
                                           esc=esc).items():
                tr[modo][cel].extend(lst)
        if conferir:
            for t in trades_kis(cndl, sym, nc_h()):
                if t["setup"] == SETUP:
                    ref_novo[(t["ts"], sym)] = t["ret6"]
        a = series_barra(cndl)["amp"]
        for c, up in zip(cndl, a):
            if up is not None:
                x = amp.setdefault(c["ts"], [0, 0])
                x[0] += 1 if up else 0
                x[1] += 1
        cam["sandbox"][sym] = cart.caminho(sb, _ms(H1[0]))
        cam["novo"][sym] = cart.caminho(cndl, jan[0])
        del cndl, sb, a
        rss = _rss_mib()
        if rss > teto_rss():
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {teto_rss():.0f} "
                              f"apos {sym}; PARANDO.")
    return {"tr": tr, "cont": cont, "cam": cam, "amp": amp,
            "ref_novo": ref_novo, "sem": sem, "rss": rss}


def completa(tr: dict, amp: dict, ordem: dict) -> dict:
    """Celulas de universo (T2, T4) sobre a BASE (sem teto) de cada modo."""
    info = {}
    for modo, cels in tr.items():
        base = cels["BASE"]
        cels["T2"], info[modo] = filtro_t2(base, amp)
        for k in T4_KS:
            cels[f"T4_{k}"] = teto_lado(base, k, ordem, f"T4_{k}")
    return info


def compara(meus: dict, ref: dict) -> tuple:
    """(iguais, max |dif|, so_ref, so_meu) entre {chave: ret6}."""
    comuns = set(meus) & set(ref)
    dif = max((abs(meus[k] - ref[k]) for k in comuns), default=0.0)
    return (sum(1 for k in comuns if meus[k] == ref[k]), dif,
            len(set(ref) - set(meus)), len(set(meus) - set(ref)))


# ------------------------------------------------------------ log -----------
def _fmt_cel(cel: str, r: dict, base_cap: str = "BASE_256") -> str:
    sd = " ".join(f"{k[:4]} {v:.0f}" for k, v in r["saidas"].items() if v)
    return (f"  {cel:>8} | {r['n']:>5} | {r['tot10']:>+9.0f} {r['bpt10']:>+7.1f}"
            f" | {r['tot6']:>+9.0f} {r['bpt6']:>+7.1f} | {r['acerto']:>5.1f} | "
            f"{r['ganho']:>+7.0f} {r['perda']:>+7.0f} | {r['pico']:>+8.0f} "
            f"{r['dd']:>7.0f} | {r['tri_pos']:>2}/{r['tri_tot']:<2} | "
            f"{r['sem_mes']:>+9.0f} ({r['melhor_mes']}) | {r['sem_top']:>+9.0f}"
            f" | L {r['lados']['L'][0]:>4} {r['lados']['L'][1]:>+8.0f} "
            f"S {r['lados']['S'][0]:>4} {r['lados']['S'][1]:>+8.0f} | "
            f"teto256 {r['corte'] if cel != base_cap else r['teto']:>3} | "
            f"{sd}")


def _arg_tf(txt: str) -> str:
    """Validacao de entrada (nao regra de estudo): tf com duracao conhecida."""
    try:
        escala(txt)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e))
    return txt


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="KIS 34/89 E1-3: refinamentos "
                                 "e carteira (sandbox)")
    ap.add_argument("--tf", default="4h", type=_arg_tf,
                    help="tempo grafico (15m, 1h, 4h; 8h, 12h, 1D agregados "
                         "do 4h em memoria); default 4h")
    ap.add_argument("--end", default=END_ISO,
                    help='exclusivo, UTC; default "%(default)s"')
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None,
                    help="default: SYMBOLS_KIS_3489 (65)")
    ap.add_argument("--out", default=None, help="CSV; log ao lado (.log)")
    ap.add_argument("--carteira-celulas", nargs="+", default=None,
                    help="default: todas as celulas do tf")
    ap.add_argument("--sem-conferencia", action="store_true")
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    add_teto_rss(ap)
    args = ap.parse_args(argv)
    define_teto_rss(args.teto_rss)
    esc = escala(args.tf)
    nomes_cel = nomes_celulas(esc)
    base_cap = nomes_cel[0]
    if args.carteira_celulas is None:
        args.carteira_celulas = list(nomes_cel)
    ruins = set(args.carteira_celulas) - set(nomes_cel)
    if ruins:
        raise SystemExit(f"[kis_refino] celulas desconhecidas: {sorted(ruins)}")
    teto_ms, end_ms = _ms(TETO_ISO), _ms(args.end)
    ativos = periodos_ativos(end_ms)
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    ordem = {s: k for k, s in enumerate(SYMBOLS_KIS_3489)}
    stamp = f"{datetime.now(timezone.utc):%Y%m%d_%H%M}"
    out = Path(args.out) if args.out else ROOT / "logs" / f"kis_refino_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    log = _Log(out.with_suffix(".log"))
    t0 = time.monotonic()
    log(f"[kis_refino] {SETUP} (KIS {PAR[0]}/{PAR[1]}, conf 2, portao "
        f"{PORTAO_LIMIAR}/ADX {PORTAO_ADX_MIN}, stop {STOP_K:g} ATR14) | tf "
        f"{args.tf} | universo {len(symbols)} | sandbox H1 {H1[0]}..{H1[1]}, "
        f"H2 {H2[0]}..{H2[1]} (velas < {TETO_ISO}) | NOVO entradas "
        f"{_iso(JANELA[0])}..{_iso(JANELA[1])} (velas < {args.end}) | custo "
        f"10 e 6 bps/trade | parametros: T1 N={esc['t1_ns']} progresso "
        f"{T1_PROGRESSO:g} ATR; T2 {T2_LIMIAR:g}%; T3 {T3_PARCIAIS}; T4 "
        f"K={T4_KS}; TETO_HOLD {esc['cap']} so na {base_cap}")
    if esc["fator"] != 1:
        log(f"  tf {args.tf}: duracoes de 4h convertidas para a mesma duracao "
            f"em barras de {args.tf} (x{esc['fator']}): borda {BORDA} -> "
            f"{esc['borda']}, teto do caminho/hold {EXT_CAP} -> {esc['cap']} "
            f"(BASE_256 -> {base_cap}), T1 {T1_NS} -> {esc['t1_ns']}. "
            f"Indicadores, portao e confirmacao em barras de {args.tf}.")
    log_vazios(log, ativos, args.end)
    conn = conecta_com_teto(args.store, end_ms)
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= end_ms:
            raise SystemExit("[kis_refino] view deixou passar candle >= --end")
        conferir = not args.sem_conferencia and esc["fator"] == 1
        A = coleta(conn, symbols, args.tf, teto_ms, end_ms,
                   conferir=conferir, esc=esc)
    finally:
        conn.close()
    tr = A["tr"]
    t2info = completa(tr, A["amp"], ordem)
    log(f"  maior ts visivel {_iso(mx) if mx else '-'} | sem candle: "
        f"{' '.join(A['sem']) or 'nenhum'}")
    for modo, c in A["cont"].items():
        log(f"  {modo}: inversoes {c['inversoes']} | vetadas {c['vetadas']} | "
            f"atr0 {c['atr0']} | excl_borda {c['excl_borda']} | excl_hold "
            f"{c['excl_hold']} | T5e: viradas {c['t5e_viradas']} vetadas "
            f"{c['t5e_vetadas']} borda {c['t5e_borda']} atr0 {c['t5e_atr0']} "
            f"stop na barra de entrada {c['t5e_stop_na_entrada']} | T2 "
            f"amplitude NA {t2info[modo]}")

    # ---- conferencias ---------------------------------------------------------
    log("\n===== CONFERENCIAS =====")
    if args.sem_conferencia:
        log("  pulada (--sem-conferencia)")
    elif not conferir:
        log(f"  DESLIGADA em {args.tf}: as referencias (kis_saida.run, borda "
            f"48 e caminho 256 em barras do tf; holdout_2026.trades_kis, "
            f"barra de 4h fixa) medem em barras de 4h e nao conferem a "
            f"conversao de duracao. Em 4h continua ligada.")
    else:
        from backtest import kis_saida as ks
        corte = min(teto_ms, end_ms)        # sandbox = velas < teto e < --end
        ct = conecta_com_teto(args.store, corte)
        try:
            ref = ks.run(ct, [s for s in symbols if s not in A["sem"]],
                         args.tf, None, corte - 1,
                         pares=(PAR,))["por_par"][PAR]
        finally:
            ct.close()
        k3 = ks.NOMES.index("E1-3")
        r = compara({(t["ts"], t["sym"]): t["ret6"]
                     for t in tr["sandbox"][base_cap]},
                    {(t[0], t[1]): t[3][k3] for t in ref})
        log(f"  sandbox BASE_256 x kis_saida E1-3: iguais {r[0]} de "
            f"{len(ref)} | max|dif| {r[1]:.2e} bps | so no kis_saida {r[2]} | "
            f"so aqui {r[3]}")
    if conferir and "NOVO" in ativos:
        r = compara({(t["ts"], t["sym"]): t["ret6"]
                     for t in tr["novo"][base_cap]}, A["ref_novo"])
        log(f"  NOVO BASE_256 x holdout_2026 KIS_E1-3: iguais {r[0]} de "
            f"{len(A['ref_novo'])} | max|dif| {r[1]:.2e} bps | so no holdout "
            f"{r[2]} | so aqui {r[3]}")
        sem_teto = {(t["ts"], t["sym"]): t["ret6"] for t in tr["novo"]["BASE"]}
        dif = [k for k, v in A["ref_novo"].items() if sem_teto.get(k) != v]
        log(f"  NOVO BASE (sem teto) x holdout: diferem {len(dif)} trades "
            "(esperado: so os que o teto de 256 cortou ou que ficaram abertos "
            "alem dele)")

    # ---- por periodo ------------------------------------------------------------
    por = {(cel, p): [] for cel in nomes_cel for p in PERIODOS}
    for modo, cels in tr.items():
        for cel, lst in cels.items():
            for t in lst:
                p = periodo(t, modo)
                if p:
                    por[(cel, p)].append(t)
    for v in por.values():
        v.sort(key=lambda t: (t["ts"], ordem.get(t["sym"], 1e9), t["sym"]))
    res = {k: resumo(v) for k, v in por.items()}
    cab = (f"  {'celula':>8} | {'n':>5} | {'tot@10':>9} {'bpt':>7} | "
           f"{'tot@6':>9} {'bpt':>7} | {'acer%':>5} | {'ganho':>7} {'perda':>7}"
           f" | {'pico':>8} {'dd':>7} | tri   | sem melhor mes      | "
           f"{'sem 2%':>9} | LONG n tot@10 SHORT n tot@10 | teto | saidas %")
    for p in ativos:
        log(f"\n===== {p} — por celula (bps somados por trade; curva @10 na "
            "ordem de entrada) =====")
        log(cab)
        for cel in nomes_cel:
            log(_fmt_cel(cel, res[(cel, p)], base_cap))
        x = res[("T3", p)]
        log(f"  T3: trades com parcial1 {x['p1']} | com parcial2 {x['p2']}")
    log("\n===== DIFERENCA CONTRA A BASE (sem teto), @10: total | bps/trade "
        "=====")
    for p in ativos:
        b = res[("BASE", p)]
        for cel in nomes_cel:
            if cel == "BASE":
                continue
            r = res[(cel, p)]
            ls = []
            for lado in ("L", "S"):
                nb, tb, pb = b["lados"][lado]
                nr, trr, pr = r["lados"][lado]
                ls.append(f"{lado} {trr - tb:>+8.0f} | {pr - pb:>+7.1f}")
            log(f"  {p:>4} {cel:>8}: todos {r['tot10'] - b['tot10']:>+8.0f} | "
                f"{r['bpt10'] - b['bpt10']:>+7.1f} || " + " || ".join(ls))
    log("\n===== T2 — placebo (20 sementes; veta ao acaso a mesma quantidade "
        "por lado; so informativo) =====")
    for p in ativos:
        b, t2 = por[("BASE", p)], por[("T2", p)]
        pl = placebo_t2(b, t2)
        tot = res[("T2", p)]["tot10"]
        log(f"  {p}: T2 @10 {tot:+.0f} (n {len(t2)} de {len(b)}) | placebo "
            f"media {mean(pl):+.0f} min {min(pl):+.0f} max {max(pl):+.0f} | "
            f"placebos abaixo do T2: {sum(1 for x in pl if x < tot)}/{len(pl)}")
    log("\n===== T5 — toques falsos (o toque aconteceu, o fechamento nao "
        "confirmaria) =====")
    for p in ativos:
        for cel in ("T5s", "T5e"):
            for nome, lado in (("LONG", True), ("SHORT", False)):
                sub = [t for t in por[(cel, p)] if t["long"] is lado]
                fe = sum(t["falso_e"] for t in sub)
                fs = sum(t["falso_s"] for t in sub)
                log(f"  {p:>4} {cel} {nome:>5}: trades {len(sub):>5} | "
                    f"entradas falsas {fe:>4} | saidas falsas {fs:>4}")

    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        n_csv = 0
        for cel in nomes_cel:
            for p in ativos:
                for t in por[(cel, p)]:
                    w.writerow(linha_csv(t, p))
                    n_csv += 1
    log(f"\n  CSV de trades: {out} ({n_csv} linhas)")

    # ---- carteira (uma celula por vez, uma banca por celula e periodo) --------
    log(f"\n===== CARTEIRA — banca {cart.BANCA_INICIAL:,.0f} USD por celula e "
        f"periodo, {cart.NOCIONAL:.0f} USD de nocional por trade, "
        f"{cart.CUSTO_BPS:g} bps por perna, sem funding =====")
    arq = out.parent / f"carteira_{SETUP}_{stamp}.csv"
    with cart.DiarioUnico(arq) as dia:
        for cel in args.carteira_celulas:
            for p in ativos:
                cam = A["cam"]["novo" if p == "NOVO" else "sandbox"]
                rc = cart.roda([para_carteira(t) for t in por[(cel, p)]], cam,
                               bar_ms=esc["bar_ms"])
                dia.anexa(cel, p, rc["diario"])
                log(f"  {cel} {p}:")
                for s in cart.linhas_log(rc, args.tf):
                    log(s)
                del rc
    log(f"  CSV diario unico da carteira: {arq} ({dia.n} linhas)")
    log(f"\n  RSS de pico {max(A['rss'], _rss_mib()):.0f} MiB (teto "
        f"{teto_rss():.0f}) | {time.monotonic() - t0:.0f} s")
    log("caveat: so medicao, sem veredito. Custo por trade @10 = @6 - 4 bps; "
        f"T3 paga o mesmo custo por trade (pernas proporcionais). {base_cap} "
        f"= conferencia (teto de {esc['cap']} barras); demais celulas sem "
        "teto, "
        "trade aberto no fim do dado marcado no ultimo fechamento. Sandbox "
        f"com exclusao de borda de {esc['borda']} barras (kis_saida); NOVO "
        "sem. Stop e alvo na "
        "mesma barra: stop primeiro. T5: limiares de preco com valores de t-1; "
        "T5e usa ATR14/ADX13 de t-1 no portao e no stop (aproximacao) e checa "
        "o stop na propria barra de entrada de forma conservadora. Curva e "
        "rebaixamento da tabela sobre trades ordenados por entrada; carteira "
        f"marca a mercado a cada {args.tf}, sem funding, nocional fixo de "
        "300 USD, "
        "uma banca isolada por celula e periodo; movimento adverso da "
        "carteira nao ve a barra de entrada (entrada dentro da barra no T5e "
        "fica subestimada).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
