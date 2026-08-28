"""v10/registro.py — os setups do ciclo v10, declarados como ficha.

Nada aqui é lógica nova. O detector e o portão de cada setup são IMPORTADOS da
bancada (`backtest/`) e, no caso do DONCHIAN-A, do módulo que roda em produção
(`shadow/donchian_a.py`). Reimplementar seria abrir espaço para o v10 divergir
em silêncio da instância que está medindo — o oposto do que este registro serve.
As funções `detector_*` aqui são TRADUÇÃO: recebem velas MAIS os parâmetros da
ficha, chamam o que já existe, devolvem o sinal no contrato do runner. Zero
decisão própria e — desde o PR-E — zero parâmetro próprio: nenhum número que
mude o que o detector mede vive dentro dele. Todos saem de
`SetupSpec.detector_params`, que é o que os põe dentro do `config_hash`.

KIS+REGIME NÃO IMPORTA MAIS DO SHADOW. O cron de `shadow/kis_regime.py` foi
desativado em 23/08; a máquina oficial não pode depender de um apêndice
aposentado. A conta de `avaliar` foi para `backtest/kis_regime.py` — a bancada,
que já era a dona do portão (`passa`) e da rampa (`inclinacoes`) e é stdlib
puro. O shadow fica INTOCADO com a cópia dele, e um teste trava as duas contas
uma contra a outra para que "cópia" nunca vire "divergência".

A instância em produção de cada shadow NÃO muda: cron, módulo e banco seguem
como estão. O v10 roda em paralelo, no `trades_v10`, e no caso do DONCHIAN-A só
LÊ o banco do shadow — para provar que reproduz o histórico dele barra a barra.

DIFERENÇA CONHECIDA (KIS+REGIME) — RESOLVIDA no PR-C: o shadow fecha a posição
comparando o alvo VIGENTE na última barra, então uma run perdida ATRASA a saída
para a barra seguinte. O `reverse` DETECTA a inversão na barra em que ela de
fato ocorreu, mas em `mode="shadow"`/`"live"` SAI no close da barra corrente —
o mesmo preço que o shadow consegue, porque é o único que ainda existe. Os dois
números voltam a ser comparáveis. O preço da barra da inversão continua gravado
como `preco_ideal` em `exit_state_json`, ao lado do `preco_real`: a diferença
entre eles é o custo da run perdida, medido em vez de suposto.
"""

from __future__ import annotations

from backtest.kis_regime import alvos, avaliar, passa
from shadow.donchian_a import (ATR_PERIOD, BAR_MS, CTX_BARS, H_BARS, N, S_ATR,
                               T_ATR, donchian_signal, wilder_atr)
from shadow.donchian_a import SYMBOLS as SYMBOLS_DONCHIAN
from v10.spec import SetupSpec

__all__ = ["ATIVOS", "DONCHIAN_A_4H", "KIS_3489_60T_4H", "KIS_REGIME_4H",
           "PARAMS_DONCHIAN_A", "PARAMS_KIS_3489", "PARAMS_KIS_REGIME",
           "REGISTRO", "SYMBOLS_KIS", "SYMBOLS_KIS_3489",
           "detector_donchian_a", "detector_kis_3489", "detector_kis_regime"]

TF = "4H"          # barra no formato da API; o `tf` do shadow ("4h") é rótulo
TAKER_BPS = 5.0    # OKX perp taker não-VIP — mesmo valor de shadow/vigia.py:27


# --- KIS + REGIME ------------------------------------------------------------
# A CONFIGURAÇÃO do detector, por extenso e num lugar só. Célula congelada em
# 21/08 (limiar 0.02 / ADX 11) mais os períodos que a compõem: EMA 8/21 e
# confirmação de 2 barras vêm do `keepitsimple`, ATR 14 e ADX 13 da rampa e do
# Wilder. Nenhum valor MUDA aqui — eles só saíram de dentro das funções e
# entraram na ficha, que é o que os põe dentro do `config_hash`. Mexer em
# qualquer um destes números passa a mudar o hash, e é exatamente esse o ponto:
# a série de trades de uma configuração não se mistura com a de outra.
PARAMS_KIS_REGIME = {"limiar": 0.02, "adx_min": 11, "ema_fast": 8,
                     "ema_slow": 21, "atr_len": 14, "adx_len": 13,
                     "confirmacao": 2}
# Universo e warmup do coletor, verbatim de shadow/kis_regime.py:35,37 — agora
# declarados aqui porque são campos da ficha (`symbols`, `warmup_barras`) e já
# entravam no hash por conta própria. MIN_BARS: ADX(13) fecha em 2n-1=25, o
# resto é folga para EMA21 + ATR14.
SYMBOLS_KIS = ["ARBUSDT", "BNBUSDT", "BTCUSDT", "SUIUSDT", "TRXUSDT", "WLDUSDT"]
MIN_BARS_KIS = 60


def detector_kis_regime(velas: list, params: dict):
    """Inversão do alvo do keepitsimple, filtrada pelo portão de regime.

    `avaliar` (bancada) devolve (alvo, direction, inclinação, ADX) e só põe
    `direction` na barra em que o alvo MUDOU; `passa` (bancada) é o portão, e
    warmup (None) VETA. Vetar não adia: a próxima entrada é a próxima inversão.

    `params` é OBRIGATÓRIO e vem de `spec.detector_params`. Não há default: um
    default aqui seria justamente o parâmetro fora do hash que este PR fecha.
    """
    alvo, direction, incl, adx = avaliar(
        velas, ema_fast=params["ema_fast"], ema_slow=params["ema_slow"],
        confirmacao=params["confirmacao"], adx_len=params["adx_len"],
        atr_len=params["atr_len"])
    if direction is None or not passa(direction, incl, adx,
                                      params["limiar"], params["adx_min"]):
        return None
    return {"direction": direction, "entry_price": velas[-1]["close"],
            "evidence": {"inclinacao": incl, "adx": adx,
                         "limiar": params["limiar"],
                         "adx_min": params["adx_min"], "alvo": alvo,
                         "close": velas[-1]["close"], "tf": TF}}


def _anotar_kis_regime(velas: list, params: dict) -> list:
    """Carimba o alvo vigente em cada vela — é o que o `reverse` lê para saber
    que o detector mudou de lado. Cópia rasa: não muta a lista do chamador.

    Os MESMOS períodos do detector, pela mesma ficha: se o alvo do anotador e o
    do detector pudessem ser calculados com pares de EMA diferentes, a saída
    inverteria num lugar em que a entrada nunca inverteu."""
    alvo = alvos([v["close"] for v in velas], params["ema_fast"],
                 params["ema_slow"], params["confirmacao"])
    return [dict(v, alvo=alvo[i]) for i, v in enumerate(velas)]


detector_kis_regime.anotar = _anotar_kis_regime

KIS_REGIME_4H = SetupSpec(
    setup_id="kis_regime_4h",
    detector=detector_kis_regime,
    tf=TF,
    cadencia_barras=1,
    symbols=list(SYMBOLS_KIS),
    warmup_barras=MIN_BARS_KIS,
    exit_model="reverse",
    exit_params={},
    detector_params=dict(PARAMS_KIS_REGIME),
    custo_bps_por_perna=TAKER_BPS,
    mode="shadow",
    estado_ciclo="reprovado_holdout",
    aviso=("hold-out de 65 dias: -769 bps em 66 trades, 1 de 6 tokens positivo, "
           "contra critério travado ANTES do número. SEM stop e SEM cap de "
           "tempo: a cauda de perda de um trade é ILIMITADA. Coletor de dados, "
           "não desenho operável — qualquer leitura sai com esta linha junto."),
)


# --- KIS 34/89 em 65 tokens --------------------------------------------------
# Celula do EIXO 1 (varredura de horizonte de `backtest/kis_horizonte.py`), com
# o par LONGO e o universo largo. NAO substitui o `kis_regime_4h`: as duas
# fichas coexistem, com `config_hash` proprio cada uma, e por isso as series de
# `trades_v10` nunca se misturam. O que muda em relacao a ficha curta e o PAR
# (34/89 no lugar de 8/21), o WARMUP (89, a EMA lenta do par) e o UNIVERSO (65
# no lugar de 6). Todo o resto — portao, ATR, confirmacao, custo, saida — e
# identico, de proposito: um eixo por vez.
#
# `incl_ema_slow` E O PONTO DELICADO DESTA FICHA. O portao NAO varia com o par:
# a rampa le a EMA21 SEMPRE, inclusive aqui, porque (limiar 0.02, adx_min 11)
# sobre a EMA21 e o componente validado em dado virgem e entra EXATAMENTE como
# foi validado — e e assim que `kis_horizonte` mede a grade (chama `inclinacoes`
# com os DEFAULTS em toda celula, ver cabecalho de la). Deixar a rampa seguir a
# EMA lenta do par leria a EMA89 e mediria um portao que nunca existiu; o
# numero do eixo 1 deixaria de ser reproduzivel por esta ficha. O campo esta no
# `detector_params` — e portanto no hash — justamente para que essa escolha
# fique escrita, e nao escondida num default.
PARAMS_KIS_3489 = {"limiar": 0.02, "adx_min": 11, "ema_fast": 34,
                   "ema_slow": 89, "atr_len": 14, "adx_len": 13,
                   "confirmacao": 2, "incl_ema_slow": 21}
# Universo LITERAL. Nao sai de arquivo nem de API de proposito: uma lista lida
# em runtime mudaria o universo sem mudar o `config_hash`, e trades de dois
# universos diferentes cairiam na MESMA serie — o buraco exato que a ficha
# fecha. Congelada em 27/08.
#
# TONUSDT esta FORA, e a ausencia fica registrada pelo NOME: delistada em
# 07/2026, ultimo candle em 05/06/2026. Exclusao por delistagem, nao por
# resultado — sem esta linha, a lista de 65 pareceria uma escolha de merito.
SYMBOLS_KIS_3489 = [
    "AAVEUSDT", "ADAUSDT", "AGLDUSDT", "APTUSDT", "ARBUSDT", "ATOMUSDT",
    "AVAXUSDT", "BCHUSDT", "BLURUSDT", "BNBUSDT", "BONKUSDT", "BTCUSDT",
    "CHZUSDT", "CRVUSDT", "DOGEUSDT", "DOTUSDT", "EIGENUSDT", "ETCUSDT",
    "ETHFIUSDT", "ETHUSDT", "FARTCOINUSDT", "FILUSDT", "GALAUSDT",
    "GRASSUSDT", "HBARUSDT", "HMSTRUSDT", "HYPEUSDT", "ICPUSDT", "IMXUSDT",
    "INJUSDT", "IOTAUSDT", "JTOUSDT", "KAITOUSDT", "LDOUSDT", "LINKUSDT",
    "LTCUSDT", "MOODENGUSDT", "MORPHOUSDT", "NEARUSDT", "ONDOUSDT", "OPUSDT",
    "ORDIUSDT", "PARTIUSDT", "PENGUUSDT", "PEPEUSDT", "PLUMEUSDT", "PYTHUSDT",
    "RENDERUSDT", "SEIUSDT", "SHIBUSDT", "SOLUSDT", "STXUSDT", "SUIUSDT",
    "TAOUSDT", "THETAUSDT", "TIAUSDT", "TRUMPUSDT", "TRXUSDT", "UNIUSDT",
    "VIRTUALUSDT", "WIFUSDT", "WLDUSDT", "XLMUSDT", "XRPUSDT", "ZROUSDT",
]
# Warmup = EMA LENTA DO PAR, a mesma regra por celula de `kis_horizonte`
# (`max(ema_slow, 1)`): e o warmup que produziu o resultado do eixo 1. Nao e o
# MIN_BARS de 60 da ficha curta — 60 barras nao fecham uma EMA89.
MIN_BARS_KIS_3489 = 89


def detector_kis_3489(velas: list, params: dict):
    """O MESMO detector da ficha curta, com o par longo e a rampa desacoplada.

    Funcao separada de `detector_kis_regime` por uma razao so: aquela ficha tem
    `config_hash` congelado (`e63ec120e131`), e o jeito mais barato de provar
    que este PR nao a tocou e nao tocar nem no corpo dela. As duas chamam a
    MESMA `avaliar` da bancada — nao ha segunda conta em lugar nenhum.

    `params["incl_ema_slow"]` e OBRIGATORIO e nao tem default: a EMA da rampa e
    decisao de configuracao (ver comentario do bloco), e um default a poria
    fora do hash.
    """
    alvo, direction, incl, adx = avaliar(
        velas, ema_fast=params["ema_fast"], ema_slow=params["ema_slow"],
        confirmacao=params["confirmacao"], adx_len=params["adx_len"],
        atr_len=params["atr_len"], incl_ema_slow=params["incl_ema_slow"])
    if direction is None or not passa(direction, incl, adx,
                                      params["limiar"], params["adx_min"]):
        return None
    return {"direction": direction, "entry_price": velas[-1]["close"],
            "evidence": {"inclinacao": incl, "adx": adx,
                         "limiar": params["limiar"],
                         "adx_min": params["adx_min"], "alvo": alvo,
                         "incl_ema_slow": params["incl_ema_slow"],
                         "close": velas[-1]["close"], "tf": TF}}


# O anotador e o MESMO: ele ja le o par da ficha, entao carimba o alvo de 34/89
# aqui e o de 8/21 la, sem uma linha de diferenca. Uma copia so criaria a
# chance de as duas divergirem.
detector_kis_3489.anotar = _anotar_kis_regime

KIS_3489_60T_4H = SetupSpec(
    setup_id="kis_3489_60t_4h",
    detector=detector_kis_3489,
    tf=TF,
    cadencia_barras=1,
    symbols=list(SYMBOLS_KIS_3489),
    warmup_barras=MIN_BARS_KIS_3489,
    exit_model="reverse",
    exit_params={},
    detector_params=dict(PARAMS_KIS_3489),
    custo_bps_por_perna=TAKER_BPS,
    mode="shadow",
    # NAO EXECUTA neste PR. A ficha entra primeiro, sozinha, para que o
    # `config_hash` seja revisado ANTES de existir a primeira linha de trade
    # sob ele — religar depois devolve as linhas novas a MESMA serie, porque
    # `executar` esta em FORA_DO_HASH. Ligar e um PR proprio, com o cron e o
    # custo de 65 simbolos por rodada medidos junto.
    executar=False,
    estado_ciclo="proposto",
    aviso=("celula do EIXO 1 (varredura de horizonte), ainda NAO validada em "
           "hold-out proprio: o resultado que a motiva saiu da MESMA janela em "
           "que a grade foi varrida, entao e hipotese, nao evidencia. Herda "
           "integralmente a ressalva do `kis_regime_4h`: SEM stop e SEM cap de "
           "tempo, a cauda de perda de um trade e ILIMITADA. TONUSDT fora do "
           "universo por delistagem (07/2026), nao por resultado."),
)


# --- DONCHIAN-A --------------------------------------------------------------
# O que era IMPLÍCITO no detector: canal de N=120 barras, ATR de Wilder de 14 e
# a janela de contexto de 30 barras que alimenta esse ATR. Os valores continuam
# vindo de `shadow/donchian_a.py`, que é a instância que está medindo a janela
# pré-registrada — a ficha COPIA, não decide. `donchian_signal` lê o N dele
# próprio (o shadow está congelado por sha256 e não recebe parâmetro), então a
# igualdade dos dois é travada em teste: mexer aqui sem mexer lá muda o hash sem
# mudar o comportamento, que seria a mentira exata que este campo existe para
# impedir. O que é da SAÍDA (s_atr/t_atr/h_bars) fica em `exit_params`.
PARAMS_DONCHIAN_A = {"n": N, "atr_period": ATR_PERIOD, "ctx_barras": CTX_BARS}


def detector_donchian_a(velas: list, params: dict):
    """Rompimento do canal Donchian de N barras, com ATR de Wilder na entrada.

    Canal (`donchian_signal`) e ATR (`wilder_atr`) vêm do shadow. Empate não é
    sinal; ATR <= 0 anula o sinal — sem unidade de risco não há trade.

    Duas coisas do shadow NÃO são replicadas na evidência, de propósito: o
    snapshot de best bid/ask (é I/O ao vivo, e o runner não fala com a
    corretora) e o par `s_atr`/`t_atr`. Esses dois não são do detector — são da
    SAÍDA, e já vão gravados em `exit_params_json` na MESMA linha. Repeti-los
    aqui obrigaria o detector a ler configuração de saída de constante de
    módulo, que é justamente o que a ficha veio desfazer.
    """
    n, ctx = int(params["n"]), int(params["ctx_barras"])
    closes = [v["close"] for v in velas]
    direction = donchian_signal(closes)
    if direction is None:
        return None
    atr = wilder_atr(velas[-ctx:], int(params["atr_period"]))
    if atr <= 0:
        return None
    janela = closes[-(n + 1):-1]
    return {"direction": direction, "entry_price": closes[-1], "atr_value": atr,
            "evidence": {"channel_high": max(janela), "channel_low": min(janela),
                         "close": closes[-1], "atr": atr, "n": n, "tf": TF}}


DONCHIAN_A_4H = SetupSpec(
    setup_id="donchian_a_4h",
    detector=detector_donchian_a,
    tf=TF,
    cadencia_barras=1,
    symbols=list(SYMBOLS_DONCHIAN),
    warmup_barras=N + 2,               # mínimo do shadow: N+1 p/ o canal + folga
    exit_model="bracket_simples",
    # h_bars é horizonte E espaçamento por direção — no shadow é o mesmo 48.
    exit_params={"s_atr": S_ATR, "t_atr": T_ATR, "h_bars": H_BARS,
                 "bar_ms": BAR_MS, "espacamento_barras": H_BARS},
    detector_params=dict(PARAMS_DONCHIAN_A),
    custo_bps_por_perna=TAKER_BPS,
    mode="shadow",
    # NÃO EXECUTA. A ficha fica no registro — o relatório diário a lista, e ela
    # documenta que o DONCHIAN-A existe e o que ele é. Mas rodá-la aqui abriria
    # uma SEGUNDA instância do mesmo desenho, e o Estágio 2 mede uma janela
    # pré-registrada cujo relógio de observação não pode reiniciar: duas
    # instâncias em paralelo não somam evidência, dividem a leitura. A instância
    # que conta é a legada (shadow/donchian_a.py, banco próprio, cron próprio),
    # e ela segue intocada até a janela fechar em out/2026.
    executar=False,
    estado_ciclo="estagio2_em_janela",
    aviso=("janela pré-registrada fecha out/2026 — a INSTÂNCIA em produção "
           "segue no caminho legado (shadow/donchian_a.py, banco próprio, cron "
           "próprio). Esta ficha é espelho de verificação, não substituição: "
           "trocar a instância no meio da janela reinicia o relógio do zero."),
)

REGISTRO = {s.setup_id: s
            for s in (KIS_REGIME_4H, KIS_3489_60T_4H, DONCHIAN_A_4H)}

# O que o cron de fato roda. Derivado do REGISTRO, nunca uma segunda lista
# escrita à mão: uma ficha nova entra aqui pelo próprio campo `executar`, e não
# há como esquecer de acrescentá-la em dois lugares.
ATIVOS = [s for s in REGISTRO.values() if s.executar]
