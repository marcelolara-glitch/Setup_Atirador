"""TRAVA — superficie de importacao congelada de backtest/keepitsimple.py.

shadow/kis_regime.py roda em PRODUCAO importando daqui: WARMUP, states,
_alvo_extremos, _adx. Assinatura, valor e comportamento PADRAO desses quatro
NAO mudam. Par de EMA variavel vai em modulo NOVO, ou como argumento OPCIONAL
com default identico ao de hoje (8, 21) — nunca alterando o que a chamada sem
argumentos extras devolve.

Este arquivo e o cadeado. Se um teste daqui falhar, a mudanca quebra o coletor
em producao: nao se conserta o teste, se desfaz a mudanca (ou se move a
variacao para modulo novo).

ADENDO (ledger 22/08) — a checagem de nomes nao impedia o coletor de trocar a
ORIGEM: importar o detector de um modulo NOVO passava batido. Fechado por LISTA
BRANCA de fontes: detector e portao de shadow/kis_regime.py so podem vir de
{backtest.keepitsimple, backtest.kis_regime}. Qualquer outra origem de primeira
parte reprova. O modulo novo de horizonte (EMA 13/34, 21/55, 34/89) NAO entra na
lista — quando ele existir, quem o consumir e outro coletor, com trava propria.

A serie do golden nao e decorativa. As 78 primeiras barras sao aritmetica
simples e cobrem os 5 estados; as 78 seguintes foram CONSTRUIDAS barra a barra
para cair DENTRO das frestas entre as EMAs (close entre EMA8 e EMA9; EMA8 entre
EMA21 e EMA22), porque so ali um ±1 no periodo troca o lado da comparacao. Sem
essa cauda o golden passa com EMA_FAST=9 — foi medido, nao suposto. Verificado:
trocar (8,21) por (7,21)/(9,21)/(8,20)/(8,22) muda 8/55/12/10 barras.
Valores literais e determinismo: so somas e multiplicacoes de decimais exatos,
sem RNG e sem funcao transcendental, entao o golden e estavel entre maquinas.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
from pathlib import Path

from backtest import keepitsimple as k

ROOT = Path(__file__).resolve().parents[1]
CONGELADOS = ("WARMUP", "states", "_alvo_extremos", "_adx")
# LISTA BRANCA (adendo 22/08). De onde o coletor pode tirar detector e portao:
FONTES_SINAL = {"backtest.keepitsimple", "backtest.kis_regime"}
# Infra do proprio shadow (DB, fetch, formatadores do [VIGIA]) — nao e sinal.
FONTES_INFRA = {"shadow.donchian_a", "shadow.vigia"}

# Cauda construida (ver docstring): fresta entre EMA8/EMA9 e entre EMA21/EMA22.
_CAUDA = [
    89.212653, 89.209844, 89.207629, 89.205881, 89.204502, 89.203414,
    89.202556, 89.201879, 89.201345, 89.200924, 89.200591, 89.200329,
    89.200122, 89.199959, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2,
    89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2,
    89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2, 89.2,
    89.2, 89.2, 89.2, 89.2, 89.2, 89.2005, 89.201, 89.2015, 89.202,
    89.2025, 89.203, 89.2035, 89.204, 89.239, 89.2335, 89.217, 89.2165,
    89.2145, 89.2165, 89.217, 89.2145, 89.215, 89.217, 89.2175, 89.214,
    89.2145, 89.2175, 89.218, 89.214, 89.2145, 89.215, 89.2155, 89.216,
    89.2165, 89.217
]
STATES_GOLDEN = ("-------------------------VVVVVVVVVVVVVVVVVVVVVVVAAAAAAAMMMMMMMMMMMMM"
                 "MMMRRRRRRRMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMRRRRRRRRRR"
                 "VVMVVMMVVMMVVMMMRRVV")
STATES_SHA = "7217f08ba9895bb435dad5ea7c998ae0df32a72a2d4039c5360f6616bf837818"
ALVO_GOLDEN = ("00000000000000000000000000++++++++++++++++++++++++++++++------------"
                 "--------------------------------------------------------------------"
                 "-+++++--++--++-----+")
ALVO_SHA = "8d6ee3aa51d8cdddd07f03353c7f2735a843c9b8dde0f34929b86cd70b1bbc77"
ADX_GOLDEN = {
              25: 7.692307692307693,
              40: 72.21526238697471,
              55: 53.19456259773866,
              70: 63.46296583368105,
              100: 12.851050793698562,
              130: 7.730416082812302,
              155: 4.566167189823854,
}
COD_ST = {"VERDE": "V", "AZUL": "A", "ROXO": "R", "VERM": "M", "CINZA": "-"}
COD_AL = {1: "+", -1: "-", 0: "0"}


def _serie():
    c = [100.0] * 25                                    # plano  -> CINZA/warmup
    c += [c[-1] + 2.0 * i for i in range(1, 21)]        # alta forte  -> VERDE
    c += [c[-1] - 1.5 * i for i in range(1, 9)]         # pullback    -> AZUL
    c += [c[-1] - 4.0 * i for i in range(1, 16)]        # queda       -> VERM
    c += [c[-1] + 3.0 * i for i in range(1, 11)]        # repique     -> ROXO
    return c + _CAUDA                                   # frestas das EMAs


def _candles(closes):
    return [{"ts": i, "open": c, "high": c * 1.01, "low": c * 0.99,
             "close": c, "volume": 1.0} for i, c in enumerate(closes)]


def _estados(closes, fast, slow):
    """Replica `states` com o par de EMAs EXPLICITO — so para provar que o
    golden discrimina o periodo. Nao substitui `states` em teste nenhum."""
    f, s = k._ema(closes, fast), k._ema(closes, slow)
    o = []
    for i, x in enumerate(closes):
        a, b = f[i], s[i]
        if a is None or b is None or x == a or a == b:
            o.append("-")
        elif x > a:
            o.append("V" if a > b else "R")
        else:
            o.append("A" if a > b else "M")
    return "".join(o)


# --- constantes congeladas ---------------------------------------------------
def test_warmup_e_21():
    assert k.WARMUP == 21


def test_pares_de_ema_congelados():
    assert (k.EMA_FAST, k.EMA_SLOW) == (8, 21)
    assert k.WARMUP == k.EMA_SLOW                       # WARMUP segue a EMA longa


def test_adx_len_e_ext_conf_congelados():
    assert k.ADX_LEN == 13 and k.EXT_CONF == 2


# --- comportamento padrao byte-identico --------------------------------------
def test_fixture_intacta():
    # Guarda contra "consertar" o golden mexendo na serie em vez do codigo.
    c = _serie()
    assert len(c) == 156 and len(_CAUDA) == 78
    assert c[0] == 100.0 and c[24] == 100.0


def test_states_sem_argumentos_extras_byte_identico():
    s = "".join(COD_ST[x] for x in k.states(_serie()))  # UMA posicional, so
    assert s == STATES_GOLDEN
    assert hashlib.sha256(s.encode()).hexdigest() == STATES_SHA


def test_states_cobre_os_cinco_estados():
    # Se o golden deixar de exercitar um estado, ele para de travar esse ramo.
    assert set(k.states(_serie())) == {"VERDE", "AZUL", "ROXO", "VERM", "CINZA"}


def test_golden_discrimina_o_par_de_emas():
    # O cadeado so vale se a serie REAGIR a ±1 no periodo. Medido, nao suposto:
    # sem esta prova, um EMA_FAST=9 passaria despercebido pelo golden.
    c = _serie()
    base = _estados(c, 8, 21)
    assert base == STATES_GOLDEN                        # ancora nas duas contas
    for fast, slow, esperado in ((7, 21, 8), (9, 21, 55), (8, 20, 12),
                                 (8, 22, 10)):
        dif = sum(1 for x, y in zip(base, _estados(c, fast, slow)) if x != y)
        assert dif == esperado, f"({fast},{slow}): {dif} barras, nao {esperado}"


def test_alvo_extremos_byte_identico():
    a = "".join(COD_AL[x] for x in k._alvo_extremos(k.states(_serie())))
    assert a == ALVO_GOLDEN
    assert hashlib.sha256(a.encode()).hexdigest() == ALVO_SHA


def test_alvo_cobre_long_short_e_neutro():
    assert set(k._alvo_extremos(k.states(_serie()))) == {1, -1, 0}


def test_adx_default_byte_identico():
    ax = k._adx(_candles(_serie()))                     # sem passar n: default 13
    for i, esperado in ADX_GOLDEN.items():
        assert abs(ax[i] - esperado) < 1e-12, f"ADX[{i}] mudou"
    assert ax[:2 * k.ADX_LEN - 1] == [None] * (2 * k.ADX_LEN - 1)


def test_adx_default_igual_ao_explicito():
    c = _candles(_serie())
    assert k._adx(c) == k._adx(c, 13)


# --- guarda de assinatura ----------------------------------------------------
def _params(fn):
    return list(inspect.signature(fn).parameters.values())


def test_states_primeiro_parametro_e_closes():
    p = _params(k.states)
    assert p[0].name == "closes" and p[0].default is inspect.Parameter.empty


def test_alvo_extremos_primeiro_parametro_e_st():
    p = _params(k._alvo_extremos)
    assert p[0].name == "st" and p[0].default is inspect.Parameter.empty


def test_nenhum_parametro_novo_obrigatorio():
    # Parametro novo E PERMITIDO — desde que OPCIONAL. Um obrigatorio quebraria
    # a chamada de producao em shadow/kis_regime.py na hora.
    for nome in ("states", "_alvo_extremos", "_adx"):
        for p in _params(getattr(k, nome))[1:]:
            assert p.default is not inspect.Parameter.empty, \
                f"{nome}: parametro '{p.name}' novo sem default"


def test_defaults_de_ema_e_adx_se_existirem():
    # Se a variacao de EMA entrar como argumento opcional, o default e 8/21.
    esperado = {"fast": 8, "slow": 21, "ema_fast": 8, "ema_slow": 21,
                "curta": 8, "longa": 21, "n_fast": 8, "n_slow": 21}
    for p in _params(k.states)[1:]:
        if p.name in esperado:
            assert p.default == esperado[p.name], f"states: {p.name} != default"
    assert _params(k._adx)[1].default == 13             # n=13 de Wilder


# --- contrato com o consumidor em producao -----------------------------------
def test_quatro_nomes_existem_e_sao_chamaveis():
    assert all(hasattr(k, n) for n in CONGELADOS)
    assert isinstance(k.WARMUP, int)
    assert all(callable(getattr(k, n)) for n in CONGELADOS[1:])


def test_kis_regime_importa_exatamente_a_lista_congelada():
    # Se o coletor passar a importar um quinto nome, ele entra na TRAVA de
    # proposito — atualizando CONGELADOS aqui —, nunca por acidente.
    arv = ast.parse((ROOT / "shadow" / "kis_regime.py").read_text())
    nomes = {a.name for no in ast.walk(arv)
             if isinstance(no, ast.ImportFrom)
             and no.module == "backtest.keepitsimple" for a in no.names}
    assert nomes == set(CONGELADOS)


def _origens_de(rel: str) -> set:
    """Todo modulo de PRIMEIRA PARTE importado por `rel`, inclusive os lazy
    dentro de funcao (ast.walk ve o arquivo inteiro). Stdlib fica de fora pela
    lista oficial do interpretador — nao por uma lista minha, que envelheceria."""
    import sys
    arv = ast.parse((ROOT / rel).read_text())
    fora = set()
    for no in ast.walk(arv):
        if isinstance(no, ast.ImportFrom):
            assert no.level == 0, f"{rel}: import relativo ({no.module}) proibido"
            mods = [no.module]
        elif isinstance(no, ast.Import):
            mods = [a.name for a in no.names]
        else:
            continue
        for m in mods:
            if m and m.split(".")[0] not in sys.stdlib_module_names:
                fora.add(m)
    return fora


def test_lista_branca_e_exatamente_estas_duas_fontes():
    # Alargar a lista exige EDITAR esta linha — que e o "reapontar a trava de
    # proposito" do adendo. Nunca acontece por acidente num import solto.
    assert FONTES_SINAL == {"backtest.keepitsimple", "backtest.kis_regime"}


def test_kis_regime_nao_importa_fonte_fora_da_lista_branca():
    fora = _origens_de("shadow/kis_regime.py") - FONTES_SINAL - FONTES_INFRA
    assert not fora, f"origem fora da lista branca: {sorted(fora)}"


def test_kis_regime_importa_de_backtest_so_a_lista_branca():
    # O caso que o adendo mira: detector vindo de um modulo NOVO de horizonte
    # (backtest.kis_horizonte, p.ex.) reprova aqui, mesmo com os nomes iguais.
    de_backtest = {m for m in _origens_de("shadow/kis_regime.py")
                   if m.split(".")[0] == "backtest"}
    assert de_backtest == FONTES_SINAL


def test_portao_vem_de_kis_regime_da_bancada():
    arv = ast.parse((ROOT / "shadow" / "kis_regime.py").read_text())
    nomes = {a.name for no in ast.walk(arv)
             if isinstance(no, ast.ImportFrom)
             and no.module == "backtest.kis_regime" for a in no.names}
    assert nomes == {"inclinacoes", "passa"}
