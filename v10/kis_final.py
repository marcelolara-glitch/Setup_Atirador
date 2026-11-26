"""v10/kis_final.py — o setup final do KIS 34/89 (camada 3, paper).

PORTE, não reescrita. A fonte da verdade é o motor do sandbox em
`backtest/kis_refino.py` (branch `sandbox`, commit 9bb1f68), medido na célula
`k3_p3-6_f50-25_t0` do `backtest/kis_grade.py`. A `main` não tem esses
arquivos; vem para cá só o necessário, com a MESMA aritmética, e cada função
diz de onde veio. O que a `main` já tem e é byte a byte a mesma conta da
bancada (`alvos`, `inclinacoes`, `passa`, `_adx`, `_ema`, `states`, o ATR de
Wilder de 15 velas) é IMPORTADO, não copiado.

Três peças, no contrato do runner:

1. :func:`detector_kis_final` — entrada E1 na última barra fechada: inversão
   do alvo (Extremos, EMA 34/89, confirmação 2) que passa no portão (EMA21,
   inclinação >= 0,02 ATR/barra em 5 barras, ADX13 >= 11). Entrada no close,
   ATR14 da barra do sinal.
2. :func:`anotar_kis_final` — carimba em cada vela o gatilho de saída T5s
   (`kis_gL`/`kis_gS`, só valores de t-1) e a inversão no fechamento
   (`kis_inv`), que é tudo o que :class:`KisFinal` precisa ler.
3. :class:`KisFinal` (``exit_model="kis_final"``) — stop fixo k x ATR,
   parciais limite, resto sai no toque do gatilho; liquidação só medida.

Parâmetros: TODOS da ficha (`detector_params` / `exit_params`). Nenhum número
de configuração vive aqui.
"""

from __future__ import annotations

from typing import Any

from backtest.keepitsimple import _ema, states
from backtest.kis_regime import alvos, avaliar, passa
from shadow.donchian_a import wilder_atr     # espelho de excursion._wilder_atr
from v10.exits import ADAPTADORES, _acumular, _campo, _ts

__all__ = ["KisFinal", "anotar_kis_final", "detector_kis_final", "gatilhos",
           "passo"]


# --- entrada ----------------------------------------------------------------
def detector_kis_final(velas: list, params: dict):
    """Entrada E1 na ÚLTIMA barra fechada, ou None.

    Porta de `kis_refino.eventos` (escala x1) restrita à última barra:
    inversão = `entradas` (alvo[i] != alvo[i-1], alvo != 0, i >= ema_slow) —
    é o que `kis_regime.avaliar` devolve em `direction`; portão = `passa` com
    `inclinacoes(EMA21, lookback 5, ATR14)` e `_adx(13)` na barra do sinal;
    entrada/ATR = `holdout_2026.medir_sem_borda`: close da barra e ATR de
    Wilder das últimas atr_len+1 velas (`_wilder_atr`); ATR <= 0 descarta
    (o `atr0` do sandbox).
    """
    alvo, direction, incl, adx = avaliar(
        velas, ema_fast=params["ema_fast"], ema_slow=params["ema_slow"],
        confirmacao=params["confirmacao"], adx_len=params["adx_len"],
        atr_len=params["atr_len"], incl_lookback=params["incl_lookback"],
        incl_ema_slow=params["incl_ema_slow"])
    if direction is None or not passa(direction, incl, adx, params["limiar"],
                                      params["adx_min"]):
        return None
    atr = wilder_atr(velas, params["atr_len"])
    if atr <= 0:
        return None
    entry = float(velas[-1]["close"])
    return {"direction": direction, "entry_price": entry, "atr_value": atr,
            "evidence": {"inclinacao": incl, "adx": adx, "alvo": alvo,
                         "atr": atr, "close": entry,
                         "limiar": params["limiar"],
                         "adx_min": params["adx_min"]}}


def gatilhos(closes: list, fast: int, slow: int, conf: int) -> tuple:
    """(gL, gS, st). Porta LITERAL de `kis_refino.gatilhos`.

    gL[t] = preço acima do qual o fechamento de t põe o estado em VERDE,
    definido só se a barra t-1 FECHOU em VERDE; gS[t] idem VERM. Só usa EMAs
    de t-1 (recursão da EMA):
      close > EMA34_t      <=> p > EMA34_{t-1}
      EMA34_t > EMA89_t    <=> p > [(1-b)EMA89_{t-1} - (1-a)EMA34_{t-1}]/(a-b)
    gL = max dos dois; gS = min. Confirmação `conf`: definido se o estado já
    dura >= conf-1 barras FECHADAS até t-1.
    """
    ef, es, st = _ema(closes, fast), _ema(closes, slow), states(closes, fast,
                                                                slow)
    a, b = 2.0 / (fast + 1.0), 2.0 / (slow + 1.0)
    n = len(closes)
    gL, gS = [None] * n, [None] * n
    run = 0
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


def anotar_kis_final(velas: list, params: dict) -> list:
    """Carimba em cada vela o que a saída lê. Cópia rasa: não muta a entrada.

    - ``kis_gL``/``kis_gS``: gatilhos T5s de :func:`gatilhos` (LONG sai em
      gS, SHORT em gL — `kis_refino.trades_simbolo`, ``g = gS if lg else gL``).
    - ``kis_inv``: novo alvo (+1/-1) na barra da inversão, 0 nas outras —
      regra de `kis_horizonte.entradas`. É o ``e_full`` de `eventos`: o trade
      que ainda não saiu no toque sai no fechamento da próxima inversão.
    """
    closes = [v["close"] for v in velas]
    gL, gS, _st = gatilhos(closes, params["ema_fast"], params["ema_slow"],
                           params["confirmacao"])
    alvo = alvos(closes, params["ema_fast"], params["ema_slow"],
                 params["confirmacao"])
    ini = max(params["ema_slow"], 1)
    out = []
    for i, v in enumerate(velas):
        inv = alvo[i] if (i >= ini and alvo[i] != alvo[i - 1]) else 0
        out.append(dict(v, kis_gL=gL[i], kis_gS=gS[i], kis_inv=inv))
    return out


detector_kis_final.anotar = anotar_kis_final


# --- saída ------------------------------------------------------------------
def _barra(v: Any, entry: float, atr: float, sg: float) -> tuple:
    """(fav, adv, abertura, fechamento) em ATR. Porta de `kis_refino._barra`."""
    h, lo = float(_campo(v, "high")), float(_campo(v, "low"))
    if sg > 0:
        fav, adv = (h - entry) / atr, (entry - lo) / atr
    else:
        fav, adv = (entry - lo) / atr, (h - entry) / atr
    return (fav, adv, sg * (float(_campo(v, "open")) - entry) / atr,
            sg * (float(_campo(v, "close")) - entry) / atr)


def passo(p: dict, pegos: int, v: Any) -> tuple:
    """Uma barra do motor. -> (parciais_novas, saida|None, pegos).

    Corpo do laço de `kis_refino.simula` (sem T1 e sem escada), mesma ordem:
    (1) nível adverso = stop -k ATR, ou o gatilho se estiver acima do stop
    (o mais alto é tocado primeiro); preço min(nível, abertura) na
    orientação da posição — stop e parcial na mesma barra: stop primeiro;
    (2) parciais limite em max(nível, abertura); (3) a inversão no fechamento
    (`kis_inv` contrário) fecha o resto no close — é o fim em ``e_full``.
    parciais_novas = [(nome, nivel_atr, fracao, preco)]; saida = (motivo,
    preco) com motivo ``stop`` ou ``flip``.
    """
    entry, atr = float(p["entry_price"]), float(p["atr_value"])
    sg = 1.0 if p["direction"] == "LONG" else -1.0
    fav, adv, op, _cl = _barra(v, entry, atr, sg)
    k = float(p["stop_k"])
    nivel, px_n, tipo = -k, entry - sg * k * atr, "stop"
    g = _campo(v, "kis_gS" if sg > 0 else "kis_gL") if p["gatilho"] == "toque" \
        else None
    if g is not None:
        gl = sg * (float(g) - entry) / atr
        if gl > nivel:
            nivel, px_n, tipo = gl, float(g), "flip"
    if -adv <= nivel:
        px = float(_campo(v, "open")) if op < nivel else px_n
        return [], (tipo, px), pegos
    novas = []
    for n, (lvl, fr) in enumerate(p["parciais"]):
        if not pegos >> n & 1 and fav >= lvl:
            px = (float(_campo(v, "open")) if op > lvl
                  else entry + sg * lvl * atr)
            novas.append((f"parcial{n + 1}", float(lvl), float(fr), px))
            pegos |= 1 << n
    inv = _campo(v, "kis_inv", 0) or 0
    if int(inv) == -int(sg):
        return novas, ("flip", float(_campo(v, "close"))), pegos
    return novas, None, pegos


def liquida(p: dict, v: Any, saida) -> bool:
    """A barra liquida a margem isolada? Regra de `carteira.liquidacao`.

    Limiar de movimento adverso = 1/alav - mmr, medido do preço de entrada.
    Extremo da barra = mínima (LONG) / máxima (SHORT); na barra da SAÍDA, se o
    preço de saída não for o close, o extremo é o próprio preço de saída (a
    posição não viveu o resto da barra). A barra de entrada nunca entra: o
    motor só vê barras posteriores. Sem ``alav`` na ficha, nunca.
    """
    alav = p.get("alav")
    if not alav:
        return False
    lim = 1.0 / float(alav) - float(p["mmr"])
    if lim <= 0:
        raise ValueError(f"alav {alav} com mmr {p['mmr']}: limiar <= 0")
    entry = float(p["entry_price"])
    sg = 1.0 if p["direction"] == "LONG" else -1.0
    ext = float(_campo(v, "low" if sg > 0 else "high"))
    if saida is not None and saida[1] != float(_campo(v, "close")):
        ext = saida[1]
    return sg * (entry - ext) / entry >= lim


def _ret_pct(p: dict, preco: float) -> float:
    entry = float(p["entry_price"])
    sg = 1.0 if p["direction"] == "LONG" else -1.0
    return sg * (preco - entry) / entry * 100.0


class KisFinal:
    """Stop fixo + duas parciais limite + resto na inversão por toque.

    ESTADO INCREMENTAL: ao contrário do `reverse` (que relê tudo desde a
    entrada), parcial executada não pode executar de novo, então
    ``ts_proc_ms`` marca a última barra já processada e só o que vem depois
    entra. ``pnl_pct`` final = soma das pernas ponderadas pela fração
    (parciais + resto); o runner usa esse valor em vez de recalcular por um
    preço só. Custo: 5 bps por perna com pernas proporcionais = 2 pernas
    cheias, a mesma conta do runner.

    PREÇO DE SAÍDA DO RESTO, como no `reverse`: ``preco_ideal`` = o do motor
    na barra da saída; ``preco_real`` = o close da barra CORRENTE (a última
    processada). Com ``barras_atraso`` = 0 coincidem. ``mode="backtest"`` sai
    no ideal; ``shadow``/``live``, no real. Parciais ficam no preço do motor
    (ordem limite), e a liquidação perde a margem nos dois modos.
    """

    nome = "kis_final"

    @staticmethod
    def abrir(spec: Any, sinal: Any, vela: Any) -> dict:
        cfg = spec.exit_params or {}
        direction = _campo(sinal, "direction")
        entry = float(_campo(sinal, "entry_price"))
        atr = float(_campo(sinal, "atr_value"))
        sg = 1.0 if direction == "LONG" else -1.0
        params = {
            "direction": direction, "entry_price": entry, "atr_value": atr,
            "entry_ts_ms": _ts(vela), "stop_k": float(cfg["stop_k"]),
            "stop_price": entry - sg * float(cfg["stop_k"]) * atr,
            "parciais": [[float(a), float(f)] for a, f in cfg["parciais"]],
            "gatilho": cfg["gatilho"], "alav": cfg.get("alav"),
            "mmr": cfg.get("mmr"),
        }
        estado = {
            "status": "OPEN", "stop_vigente": params["stop_price"],
            "parciais": [], "pegos": 0, "fracao_restante": 1.0,
            "pnl_realizado_pct": 0.0, "pnl_pct": None, "motivo": None,
            "ts_proc_ms": params["entry_ts_ms"], "max_runup": 0.0,
            "max_drawdown": 0.0, "preco_ideal": None, "preco_real": None,
            "barras_atraso": 0, "exit_ts_ms": None,
        }
        return {"exit_params": params, "exit_state": estado}

    @staticmethod
    def avaliar(spec: Any, estado: dict, velas_novas: list,
                agora_ms=None) -> tuple:
        p, s = estado["exit_params"], dict(estado["exit_state"])
        s["parciais"] = list(s["parciais"])
        novo = {"exit_params": p, "exit_state": s}
        if s["status"] != "OPEN":
            return novo, s["status"], None
        ref = s.get("ts_proc_ms")
        velas = sorted((v for v in velas_novas
                        if ref is None or (_ts(v) or 0) > int(ref)),
                       key=lambda v: _ts(v) or 0)
        backtest = str(_campo(spec, "mode", "shadow")) == "backtest"
        entry, direction = float(p["entry_price"]), p["direction"]
        for i, v in enumerate(velas):
            novas, saida, pegos = passo(p, int(s["pegos"]), v)
            _acumular(s, direction, entry, v)
            s["ts_proc_ms"] = _ts(v)
            if liquida(p, v, saida):        # parcial da mesma barra não conta
                resto = s["fracao_restante"]
                alav = float(p["alav"])
                preco = entry * (1.0 - (1.0 if direction == "LONG" else -1.0)
                                 * (1.0 / alav - float(p["mmr"])))
                s["pnl_pct"] = s["pnl_realizado_pct"] - resto * 100.0 / alav
                return KisFinal._fechar(novo, "liquidacao", preco, preco,
                                        velas, i, backtest)
            for nome, lvl, fr, px in novas:
                s["parciais"].append({"ts_ms": _ts(v), "nivel_atr": lvl,
                                      "fracao": fr, "preco": px,
                                      "nome": nome})
                s["fracao_restante"] -= fr
                s["pnl_realizado_pct"] += fr * _ret_pct(p, px)
            s["pegos"] = pegos
            if saida is not None:
                ideal = saida[1]
                real = float(_campo(velas[-1], "close")) if i < len(velas) - 1 \
                    else ideal
                px = ideal if backtest else real
                s["pnl_pct"] = (s["pnl_realizado_pct"]
                                + s["fracao_restante"] * _ret_pct(p, px))
                return KisFinal._fechar(novo, saida[0], ideal, real, velas,
                                        i, backtest)
        return novo, None, None

    @staticmethod
    def _fechar(novo: dict, motivo: str, ideal: float, real: float, velas,
                i: int, backtest: bool) -> tuple:
        """Grava a saída e devolve (novo, STATUS, preço). Fora do backtest os
        extremos das barras até a corrente entram (a posição seguiu nas mãos
        até ali) — mesma regra do `reverse`. Liquidação sai na barra dela, no
        preço de liquidação, em qualquer modo (quem fecha é a corretora)."""
        p, s = novo["exit_params"], novo["exit_state"]
        liq = motivo == "liquidacao"
        atraso = len(velas) - 1 - i
        no_ideal = backtest or liq
        if not no_ideal:
            for w in velas[i + 1:]:
                _acumular(s, p["direction"], float(p["entry_price"]), w)
        s.update(status=motivo.upper(), motivo=motivo, preco_ideal=ideal,
                 preco_real=real, barras_atraso=int(atraso),
                 exit_ts_ms=_ts(velas[i] if no_ideal else velas[-1]))
        return novo, motivo.upper(), ideal if no_ideal else real


ADAPTADORES[KisFinal.nome] = KisFinal
