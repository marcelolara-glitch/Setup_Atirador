"""v10/registro.py — os dois setups do ciclo v10, declarados como ficha.

Nada aqui é lógica nova. O detector e o portão de cada setup são IMPORTADOS do
módulo que já roda em produção (`shadow/kis_regime.py`, `shadow/donchian_a.py`)
e da bancada (`backtest/`). Reimplementar seria abrir espaço para o v10 divergir
em silêncio da instância que está medindo — o oposto do que este registro serve.
As funções `detector_*` aqui são TRADUÇÃO: recebem velas, chamam o que já
existe, devolvem o sinal no contrato do runner. Zero decisão própria.

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

from backtest.keepitsimple import _alvo_extremos, states
from backtest.kis_regime import passa
from shadow.donchian_a import (BAR_MS, CTX_BARS, H_BARS, N, S_ATR, T_ATR,
                               donchian_signal, wilder_atr)
from shadow.donchian_a import SYMBOLS as SYMBOLS_DONCHIAN
from shadow.kis_regime import ADX_MIN, LIMIAR, MIN_BARS
from shadow.kis_regime import SYMBOLS as SYMBOLS_KIS
from shadow.kis_regime import avaliar as avaliar_kis
from v10.spec import SetupSpec

__all__ = ["ATIVOS", "DONCHIAN_A_4H", "KIS_REGIME_4H", "REGISTRO",
           "detector_donchian_a", "detector_kis_regime"]

TF = "4H"          # barra no formato da API; o `tf` do shadow ("4h") é rótulo
TAKER_BPS = 5.0    # OKX perp taker não-VIP — mesmo valor de shadow/vigia.py:27


# --- KIS + REGIME ------------------------------------------------------------
def detector_kis_regime(velas: list):
    """Inversão do alvo do keepitsimple, filtrada pelo portão de regime.

    `avaliar` (shadow) devolve (alvo, direction, inclinação, ADX) e só põe
    `direction` na barra em que o alvo MUDOU; `passa` (bancada) é o portão, e
    warmup (None) VETA. Vetar não adia: a próxima entrada é a próxima inversão.
    """
    alvo, direction, incl, adx = avaliar_kis(velas)
    if direction is None or not passa(direction, incl, adx, LIMIAR, ADX_MIN):
        return None
    return {"direction": direction, "entry_price": velas[-1]["close"],
            "evidence": {"inclinacao": incl, "adx": adx, "limiar": LIMIAR,
                         "adx_min": ADX_MIN, "alvo": alvo,
                         "close": velas[-1]["close"], "tf": TF}}


def _anotar_kis_regime(velas: list) -> list:
    """Carimba o alvo vigente em cada vela — é o que o `reverse` lê para saber
    que o detector mudou de lado. Cópia rasa: não muta a lista do chamador."""
    alvo = _alvo_extremos(states([v["close"] for v in velas]))
    return [dict(v, alvo=alvo[i]) for i, v in enumerate(velas)]


detector_kis_regime.anotar = _anotar_kis_regime

KIS_REGIME_4H = SetupSpec(
    setup_id="kis_regime_4h",
    detector=detector_kis_regime,
    tf=TF,
    cadencia_barras=1,
    symbols=list(SYMBOLS_KIS),
    warmup_barras=MIN_BARS,
    exit_model="reverse",
    exit_params={},
    custo_bps_por_perna=TAKER_BPS,
    mode="shadow",
    estado_ciclo="reprovado_holdout",
    aviso=("hold-out de 65 dias: -769 bps em 66 trades, 1 de 6 tokens positivo, "
           "contra critério travado ANTES do número. SEM stop e SEM cap de "
           "tempo: a cauda de perda de um trade é ILIMITADA. Coletor de dados, "
           "não desenho operável — qualquer leitura sai com esta linha junto."),
)


# --- DONCHIAN-A --------------------------------------------------------------
def detector_donchian_a(velas: list):
    """Rompimento do canal Donchian de N barras, com ATR de Wilder na entrada.

    Canal (`donchian_signal`) e ATR (`wilder_atr`) vêm do shadow. Empate não é
    sinal; ATR <= 0 anula o sinal — sem unidade de risco não há trade.

    Uma coisa do shadow NÃO é replicada de propósito: o snapshot de best
    bid/ask na evidência. É I/O ao vivo, e o runner não fala com a corretora.
    """
    closes = [v["close"] for v in velas]
    direction = donchian_signal(closes)
    if direction is None:
        return None
    atr = wilder_atr(velas[-CTX_BARS:])
    if atr <= 0:
        return None
    janela = closes[-(N + 1):-1]
    return {"direction": direction, "entry_price": closes[-1], "atr_value": atr,
            "evidence": {"channel_high": max(janela), "channel_low": min(janela),
                         "close": closes[-1], "atr": atr, "n": N,
                         "s_atr": S_ATR, "t_atr": T_ATR, "tf": TF}}


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

REGISTRO = {s.setup_id: s for s in (KIS_REGIME_4H, DONCHIAN_A_4H)}

# O que o cron de fato roda. Derivado do REGISTRO, nunca uma segunda lista
# escrita à mão: uma ficha nova entra aqui pelo próprio campo `executar`, e não
# há como esquecer de acrescentá-la em dois lugares.
ATIVOS = [s for s in REGISTRO.values() if s.executar]
