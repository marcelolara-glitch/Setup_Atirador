# backtest/carteira.py — Setup Atirador (SANDBOX, camada 2) — CARTEIRA DE
# REFERENCIA. Briefing v3 de 22/11/2026 (KIS 34/89 E1-3, cinco refinamentos),
# secao 5. Este modulo passa a ser a carteira de referencia dos proximos testes,
# de QUALQUER setup: nao conhece o KIS nem regra de saida nenhuma.
#
# ENTRADA (generica): lista de trades de UM setup/celula, cada um um dict
#   {rotulo, sym, direcao ("LONG"/"SHORT"), ts_entrada, preco_entrada,
#    saidas: [(ts, preco, fracao), ...]}
# `ts` = ts de ABERTURA da barra de 4h em que a execucao acontece (dentro dela
# ou no fechamento); a execucao entra na banca na marcacao do fechamento dessa
# barra. As fracoes somam 1. `caminhos` = {sym: (ts, high, low, close)} com
# array('q')/array('d') (nao dicts: VM de 956 MiB), lidos do store.
#
# UMA BANCA POR SETUP, ISOLADA (regra do Marcelo): uma chamada recebe os trades
# de UM unico setup/celula e devolve UMA banca. Trades com rotulos diferentes
# na mesma chamada -> ValueError. Carteira consolidada (camadas 4 e 5) NAO
# existe aqui de proposito.
#
# PARAMETROS (defaults do Marcelo): banca 10.000 USD; 300 USD de nocional por
# trade, fixo, sem composicao, independente de alavancagem; custo de 5 bps por
# perna sobre o nocional EXECUTADO (entrada: 300; cada saida: qtd x fracao x
# preco); sem funding.
#
# CALCULO: marcacao a mercado no fechamento de cada barra de 4h da uniao dos
# caminhos, com todas as posicoes abertas: banca = inicial + realizado + nao
# realizado. Preco de marcacao = ultimo close do simbolo com ts <= marca.
# Nocional por lado = qtd x fracao restante x close da marca (marcado).
#
# REGISTRO DIARIO: fechamento do dia = marca da barra das 20:00 UTC (fecha a
# 00:00 UTC = 21:00 em Brasilia). Dia sem barra das 20:00 usa a ultima marca
# anterior. Rebaixamento do CSV diario contra o pico dos fechamentos diarios.
#
# MOVIMENTO ADVERSO POR POSICAO (% do preco de entrada ate a saida final):
# barras APOS a barra de entrada ate a barra da saida final. Na barra da saida
# final: se o preco de saida e o close da barra (saida no fechamento), conta a
# barra inteira; senao (stop/ordem dentro da barra) conta so ate o preco de
# saida. A barra de entrada nao conta (entrada no fechamento; para entrada
# dentro da barra isso subestima — caveat do chamador).
from __future__ import annotations

import csv
from array import array
from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

BANCA_INICIAL = 10_000.0
NOCIONAL = 300.0
CUSTO_BPS = 5.0
BAR_MS = 14_400_000
HORA_FECHO_DIA = 20             # barra das 20:00 UTC fecha o dia
CAMPOS_DIA = ["data", "banca_usd", "realizado_dia_usd", "nao_realizado_usd",
              "rebaixamento_usd", "rebaixamento_pct", "n_abertas", "n_long",
              "n_short", "nocional_long_usd", "nocional_short_usd",
              "exposicao_x_banca", "trades_abertos_dia", "trades_fechados_dia"]


def _iso(ts_ms: int, fmt: str = "%Y-%m-%d %H:%M") -> str:
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime(fmt)


def caminho(cndl: list, ini_ms: int = None) -> tuple:
    """(ts, high, low, close) em arrays compactos, a partir de ini_ms."""
    sel = [c for c in cndl if ini_ms is None or c["ts"] >= ini_ms]
    return (array("q", (c["ts"] for c in sel)),
            array("d", (c["high"] for c in sel)),
            array("d", (c["low"] for c in sel)),
            array("d", (c["close"] for c in sel)))


def percentil(xs: list, q: float):
    """Posto mais proximo (q em 0..100). Lista vazia -> None."""
    if not xs:
        return None
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(-(-q * len(s) // 100)) - 1))
    return s[k]


def _valida(trades: list) -> str:
    rot = {t["rotulo"] for t in trades}
    if len(rot) > 1:
        raise ValueError(f"carteira: trades de {len(rot)} rotulos na mesma "
                         f"banca ({sorted(rot)}); uma banca por setup/celula")
    for t in trades:
        if t["direcao"] not in ("LONG", "SHORT"):
            raise ValueError(f"direcao invalida: {t['direcao']}")
        if not t["saidas"]:
            raise ValueError(f"{t['sym']} {t['ts_entrada']}: trade sem saida")
        if abs(sum(f for _ts, _p, f in t["saidas"]) - 1.0) > 1e-9:
            raise ValueError(f"{t['sym']} {t['ts_entrada']}: fracoes != 1")
        if min(ts for ts, _p, _f in t["saidas"]) < t["ts_entrada"]:
            raise ValueError(f"{t['sym']} {t['ts_entrada']}: saida antes da "
                             "entrada")
    return next(iter(rot)) if rot else ""


def adverso_pct(t: dict, cam: tuple) -> float:
    """Movimento adverso maximo da posicao, em % do preco de entrada (regra no
    cabecalho). 0 se nunca andou contra."""
    ts, hi, lo, cl = cam
    sg = 1.0 if t["direcao"] == "LONG" else -1.0
    e = t["preco_entrada"]
    # saida final = a de maior ts; empate (parcial e final na mesma barra) ->
    # a ultima da lista (sorted e estavel)
    fim_ts, fim_px = sorted(t["saidas"], key=lambda x: x[0])[-1][:2]
    pior = 0.0
    a, b = bisect_right(ts, t["ts_entrada"]), bisect_right(ts, fim_ts)
    for k in range(a, b):
        ext = lo[k] if sg > 0 else hi[k]
        if ts[k] == fim_ts and fim_px != cl[k]:
            ext = fim_px
        pior = max(pior, sg * (e - ext) / e * 100.0)
    return pior


def roda(trades: list, caminhos: dict, banca: float = BANCA_INICIAL,
         nocional: float = NOCIONAL, custo_bps: float = CUSTO_BPS,
         ini_ms: int = None, fim_ms: int = None) -> dict:
    """Uma banca para os trades de UM setup/celula. Devolve {rotulo, marcas,
    diario, metricas, adverso}. Funcao pura: nada fica guardado entre chamadas,
    logo duas celulas rodadas em separado nao se tocam."""
    rotulo = _valida(trades)
    custo = custo_bps / 1e4
    ordem = sorted(trades, key=lambda t: (t["ts_entrada"], t["sym"]))
    if not ordem:
        return {"rotulo": rotulo, "marcas": [], "diario": [],
                "metricas": metricas([], [], banca), "adverso": []}
    ini = ini_ms if ini_ms is not None else ordem[0]["ts_entrada"]
    fim = fim_ms if fim_ms is not None else max(
        x[0] for t in ordem for x in t["saidas"])
    syms = {t["sym"] for t in ordem}
    marcas_ts = sorted({x for s in syms for x in caminhos[s][0]
                        if ini <= x <= fim})
    execs = sorted(((x[0], n, x[1], x[2]) for n, t in enumerate(ordem)
                    for x in t["saidas"]))
    qtd = [nocional / t["preco_entrada"] for t in ordem]
    resto = [1.0] * len(ordem)
    sg = [1.0 if t["direcao"] == "LONG" else -1.0 for t in ordem]
    abertas, realizado, n_ab, n_fe = set(), 0.0, 0, 0
    pe = px = 0
    marcas = []
    for m in marcas_ts:
        while pe < len(ordem) and ordem[pe]["ts_entrada"] <= m:
            abertas.add(pe)
            realizado -= nocional * custo
            n_ab += 1
            pe += 1
        while px < len(execs) and execs[px][0] <= m:
            _ts, n, p, f = execs[px]
            realizado += qtd[n] * f * (sg[n] * (p - ordem[n]["preco_entrada"])
                                       - p * custo)
            resto[n] -= f
            if resto[n] <= 1e-12:
                abertas.discard(n)
                n_fe += 1
            px += 1
        nr = nl = ns = 0.0
        cl, cs = 0, 0
        for n in abertas:
            cts, _h, _l, ccl = caminhos[ordem[n]["sym"]]
            k = bisect_right(cts, m) - 1
            c = ccl[k] if k >= 0 else ordem[n]["preco_entrada"]
            nr += qtd[n] * resto[n] * sg[n] * (c - ordem[n]["preco_entrada"])
            if sg[n] > 0:
                nl += qtd[n] * resto[n] * c
                cl += 1
            else:
                ns += qtd[n] * resto[n] * c
                cs += 1
        marcas.append((m, banca + realizado + nr, realizado, nr, cl, cs, nl,
                       ns, n_ab, n_fe))
    diario = diario_de(marcas, banca)
    adv = [adverso_pct(t, caminhos[t["sym"]]) for t in ordem]
    return {"rotulo": rotulo, "marcas": marcas, "diario": diario,
            "metricas": metricas(marcas, diario, banca), "adverso": adv}


def diario_de(marcas: list, banca: float) -> list:
    """Uma linha por dia UTC: marca da barra das 20:00 (ou a ultima antes)."""
    if not marcas:
        return []
    dia_ms = 86_400_000
    d0 = marcas[0][0] - marcas[0][0] % dia_ms
    d1 = marcas[-1][0] - marcas[-1][0] % dia_ms
    out, k, pico = [], -1, None
    ant = (0.0, 0, 0)                        # realizado, n_ab, n_fe do dia ant.
    for d in range(d0, d1 + dia_ms, dia_ms):
        corte = d + HORA_FECHO_DIA * 3_600_000
        while k + 1 < len(marcas) and marcas[k + 1][0] <= corte:
            k += 1
        if k < 0:
            continue
        _m, b, rz, nr, cl, cs, nl, ns, n_ab, n_fe = marcas[k]
        pico = b if pico is None else max(pico, b)
        out.append({"data": _iso(d, "%Y-%m-%d"), "banca_usd": b,
                    "realizado_dia_usd": rz - ant[0],
                    "nao_realizado_usd": nr, "rebaixamento_usd": pico - b,
                    "rebaixamento_pct": 100.0 * (pico - b) / pico if pico else 0.0,
                    "n_abertas": cl + cs, "n_long": cl, "n_short": cs,
                    "nocional_long_usd": nl, "nocional_short_usd": ns,
                    "exposicao_x_banca": (nl + ns) / b if b else 0.0,
                    "trades_abertos_dia": n_ab - ant[1],
                    "trades_fechados_dia": n_fe - ant[2]})
        ant = (rz, n_ab, n_fe)
    return out


def _dd(serie: list) -> tuple:
    """(dd_usd, dd_pct, ts_pico, ts_vale) da serie [(ts, banca)]."""
    pico, t_pico, best = None, None, (0.0, 0.0, None, None)
    for ts, b in serie:
        if pico is None or b > pico:
            pico, t_pico = b, ts
        if pico - b > best[0]:
            best = (pico - b, 100.0 * (pico - b) / pico, t_pico, ts)
    return best


def metricas(marcas: list, diario: list, banca: float) -> dict:
    if not marcas:
        return {"banca_final": banca, "retorno_pct": 0.0, "n_marcas": 0}
    fim = marcas[-1][1]
    dd4 = _dd([(m[0], m[1]) for m in marcas])
    ddd = _dd([(d["data"], d["banca_usd"]) for d in diario])
    dias = [(diario[i]["data"], diario[i]["banca_usd"] -
             (diario[i - 1]["banca_usd"] if i else banca))
            for i in range(len(diario))]
    pior = min(dias, key=lambda x: x[1]) if dias else (None, 0.0)

    def mx_md(xs):
        return (max(xs), median(xs)) if xs else (0, 0)
    return {
        "banca_final": fim, "retorno_pct": 100.0 * (fim - banca) / banca,
        "n_marcas": len(marcas), "dd4_usd": dd4[0], "dd4_pct": dd4[1],
        "dd4_pico": dd4[2], "dd4_vale": dd4[3], "ddd_usd": ddd[0],
        "ddd_pct": ddd[1], "ddd_pico": ddd[2], "ddd_vale": ddd[3],
        "pior_dia": pior[0], "pior_dia_usd": pior[1],
        "pos_long": mx_md([m[4] for m in marcas]),
        "pos_short": mx_md([m[5] for m in marcas]),
        "pos_total": mx_md([m[4] + m[5] for m in marcas]),
        "x_long": mx_md([m[6] / m[1] for m in marcas if m[1]]),
        "x_short": mx_md([m[7] / m[1] for m in marcas if m[1]]),
        "x_total": mx_md([(m[6] + m[7]) / m[1] for m in marcas if m[1]]),
    }


def escreve_diario(path: Path, diario: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS_DIA)
        w.writeheader()
        for d in diario:
            w.writerow({k: (f"{v:.4f}" if isinstance(v, float) else v)
                        for k, v in d.items()})


def linhas_log(res: dict) -> list:
    """Texto do registro 2 e 3 (secao 5) para UMA banca."""
    m, adv = res["metricas"], res["adverso"]
    if not m.get("n_marcas"):
        return ["    sem trades"]
    f = lambda ts: _iso(ts) if isinstance(ts, int) else (ts or "-")  # noqa
    return [
        f"    banca final {m['banca_final']:,.2f} USD | retorno "
        f"{m['retorno_pct']:+.2f}% | marcas de 4h {m['n_marcas']}",
        f"    rebaixamento (marcas 4h): {m['dd4_usd']:,.2f} USD "
        f"({m['dd4_pct']:.2f}%) pico {f(m['dd4_pico'])} vale "
        f"{f(m['dd4_vale'])}",
        f"    rebaixamento (fechamentos diarios): {m['ddd_usd']:,.2f} USD "
        f"({m['ddd_pct']:.2f}%) pico {f(m['ddd_pico'])} vale "
        f"{f(m['ddd_vale'])} | pior dia {f(m['pior_dia'])} "
        f"{m['pior_dia_usd']:+,.2f} USD",
        f"    exposicao (max / mediana): posicoes LONG {m['pos_long'][0]} / "
        f"{m['pos_long'][1]:.0f}, SHORT {m['pos_short'][0]} / "
        f"{m['pos_short'][1]:.0f}, total {m['pos_total'][0]} / "
        f"{m['pos_total'][1]:.0f} | x banca LONG {m['x_long'][0]:.2f} / "
        f"{m['x_long'][1]:.2f}, SHORT {m['x_short'][0]:.2f} / "
        f"{m['x_short'][1]:.2f}, total {m['x_total'][0]:.2f} / "
        f"{m['x_total'][1]:.2f}",
        f"    movimento adverso por posicao (% do preco de entrada, n "
        f"{len(adv)}): mediana {median(adv):.2f} | p90 "
        f"{percentil(adv, 90):.2f} | p99 {percentil(adv, 99):.2f} | max "
        f"{max(adv):.2f}" if adv else "    movimento adverso: sem trades",
    ]
