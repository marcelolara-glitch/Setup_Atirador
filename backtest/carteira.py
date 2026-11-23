# backtest/carteira.py — Setup Atirador (SANDBOX, camada 2) — CARTEIRA DE
# REFERENCIA. Briefing v3 de 22/11/2026 (KIS 34/89 E1-3, cinco refinamentos),
# secao 5. Este modulo passa a ser a carteira de referencia dos proximos testes,
# de QUALQUER setup: nao conhece o KIS nem regra de saida nenhuma.
#
# ENTRADA (generica): lista de trades de UM setup/celula, cada um um dict
#   {rotulo, sym, direcao ("LONG"/"SHORT"), ts_entrada, preco_entrada,
#    saidas: [(ts, preco, fracao), ...]}
# `ts` = ts de ABERTURA da barra (do tf) em que a execucao acontece (dentro
# dela ou no fechamento); a execucao entra na banca na marcacao do fechamento
# dessa barra. As fracoes somam 1. `caminhos` = {sym: (ts, high, low,
# close)} com array('q')/array('d') ou memoryview deles (nao dicts: VM de 956
# MiB), lidos do store.
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
# CALCULO: marcacao a mercado no fechamento de cada barra (do tf) da uniao dos
# caminhos, com todas as posicoes abertas: banca = inicial + realizado + nao
# realizado. Preco de marcacao = ultimo close do simbolo com ts <= marca.
# Nocional por lado = qtd x fracao restante x close da marca (marcado).
#
# REGISTRO DIARIO: fechamento do dia = marca da ULTIMA barra do dia UTC, a que
# fecha a 00:00 UTC = 21:00 em Brasilia (4h: barra das 20:00; 1h: das 23:00;
# `bar_ms` do `roda`, 23/11/2026). Dia sem essa barra usa a ultima marca
# anterior. Rebaixamento do CSV diario contra o pico dos fechamentos diarios.
#
# MOVIMENTO ADVERSO POR POSICAO (% do preco de entrada ate a saida final):
# barras APOS a barra de entrada ate a barra da saida final. Na barra da saida
# final: se o preco de saida e o close da barra (saida no fechamento), conta a
# barra inteira; senao (stop/ordem dentro da barra) conta so ate o preco de
# saida. A barra de entrada nao conta (entrada no fechamento; para entrada
# dentro da barra isso subestima — caveat do chamador).
#
# AJUSTES DE 23/11/2026 (briefing da grade KIS 34/89; valem daqui em diante):
# (1) rebaixamento em DUAS medidas separadas: o maior em % (com o pico e o vale
#     DESSA medida) e o maior em USD (idem) — antes o % era o do momento do
#     maior em USD e escondia o pior %; mais a maior sequencia de dias (CSV
#     diario) abaixo do pico. (2) um unico CSV diario por rodada
#     (`DiarioUnico`), com `celula, periodo` na frente; as bancas continuam
#     isoladas, so o arquivo e um. (3) liquidacao com margem isolada (so
#     medida, `liquidacao`): margem = nocional / L; liquida quando o movimento
#     contra, dentro da barra, atinge 1/L - MMR antes da saida final (mesmas
#     barras do movimento adverso); parcial com ts ANTERIOR a barra da
#     liquidacao ja saiu da conta (na mesma barra: liquidacao primeiro, como
#     stop antes de alvo). Trade liquidado: perde a margem da parte aberta,
#     mais o custo da entrada e o da perna liquidada (5 bps sobre o nocional
#     no preco de liquidacao), mais o resultado das parciais ja executadas.
#     Nocional fixo -> efeito no resultado final = soma por trade, exata.
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
BAR_MS = 14_400_000             # default 4h; outro tf: `roda(..., bar_ms=)`
DIA_MS = 86_400_000
MMR = 0.01                      # manutencao (fracao); o Marcelo confirma a OKX
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
         ini_ms: int = None, fim_ms: int = None,
         bar_ms: int = BAR_MS) -> dict:
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
    diario = diario_de(marcas, banca, bar_ms)
    adv = [adverso_pct(t, caminhos[t["sym"]]) for t in ordem]
    return {"rotulo": rotulo, "marcas": marcas, "diario": diario,
            "metricas": metricas(marcas, diario, banca), "adverso": adv}


def diario_de(marcas: list, banca: float, bar_ms: int = BAR_MS) -> list:
    """Uma linha por dia UTC: marca da ultima barra do dia (a que abre em
    24h - bar_ms; 4h: 20:00), ou a ultima antes dela."""
    if not marcas:
        return []
    dia_ms = DIA_MS
    d0 = marcas[0][0] - marcas[0][0] % dia_ms
    d1 = marcas[-1][0] - marcas[-1][0] % dia_ms
    out, k, pico = [], -1, None
    ant = (0.0, 0, 0)                        # realizado, n_ab, n_fe do dia ant.
    for d in range(d0, d1 + dia_ms, dia_ms):
        corte = d + dia_ms - bar_ms
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


def _dd(serie: list) -> dict:
    """Maior rebaixamento da serie [(ts, banca)] em DUAS medidas separadas:
    {usd, usd_pico, usd_vale, pct, pct_pico, pct_vale}. Cada medida tem o seu
    proprio pico e vale (o maior em % pode cair em outra data que o maior em
    USD)."""
    pico, t_pico = None, None
    out = {"usd": 0.0, "usd_pico": None, "usd_vale": None,
           "pct": 0.0, "pct_pico": None, "pct_vale": None}
    for ts, b in serie:
        if pico is None or b > pico:
            pico, t_pico = b, ts
        if pico - b > out["usd"]:
            out.update(usd=pico - b, usd_pico=t_pico, usd_vale=ts)
        pct = 100.0 * (pico - b) / pico if pico else 0.0
        if pct > out["pct"]:
            out.update(pct=pct, pct_pico=t_pico, pct_vale=ts)
    return out


def dias_abaixo_pico(diario: list) -> int:
    """Maior sequencia de dias seguidos do CSV diario com banca abaixo do pico
    dos fechamentos diarios."""
    mx = cur = 0
    for d in diario:
        cur = cur + 1 if d["rebaixamento_usd"] > 1e-9 else 0
        mx = max(mx, cur)
    return mx


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
    out = {
        "banca_final": fim, "retorno_pct": 100.0 * (fim - banca) / banca,
        "n_marcas": len(marcas),
        "dias_abaixo_pico_max": dias_abaixo_pico(diario),
        "pior_dia": pior[0], "pior_dia_usd": pior[1],
        "pos_long": mx_md([m[4] for m in marcas]),
        "pos_short": mx_md([m[5] for m in marcas]),
        "pos_total": mx_md([m[4] + m[5] for m in marcas]),
        "x_long": mx_md([m[6] / m[1] for m in marcas if m[1]]),
        "x_short": mx_md([m[7] / m[1] for m in marcas if m[1]]),
        "x_total": mx_md([(m[6] + m[7]) / m[1] for m in marcas if m[1]]),
    }
    for pre, d in (("dd4", dd4), ("ddd", ddd)):
        out.update({f"{pre}_{k}": v for k, v in d.items()})
    return out


class DiarioUnico:
    """UM CSV diario por rodada: `celula, periodo` + CAMPOS_DIA. Cada `anexa`
    escreve o diario de UMA banca ja calculada (isolada); nada e somado entre
    bancas. Uso: `with DiarioUnico(path) as d: d.anexa(cel, per, diario)`."""
    CAMPOS = ["celula", "periodo"] + CAMPOS_DIA

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = self.path.open("w", newline="")
        self.w = csv.DictWriter(self.fh, fieldnames=self.CAMPOS)
        self.w.writeheader()
        self.n = 0

    def anexa(self, celula: str, periodo: str, diario: list) -> None:
        for d in diario:
            row = {k: (f"{v:.4f}" if isinstance(v, float) else v)
                   for k, v in d.items()}
            self.w.writerow(dict(row, celula=celula, periodo=periodo))
            self.n += 1
        self.fh.flush()

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        self.fh.close()


def resultado_usd(t: dict, nocional: float = NOCIONAL,
                  custo_bps: float = CUSTO_BPS) -> float:
    """Resultado realizado do trade em USD, com custos (mesma conta do
    `roda`)."""
    custo, e = custo_bps / 1e4, t["preco_entrada"]
    sg, q = (1.0 if t["direcao"] == "LONG" else -1.0), nocional / e
    return -nocional * custo + sum(q * f * (sg * (p - e) - p * custo)
                                   for _ts, p, f in t["saidas"])


def liquidacao(t: dict, cam: tuple, alav: float, mmr: float = MMR,
               nocional: float = NOCIONAL,
               custo_bps: float = CUSTO_BPS) -> tuple:
    """(liquidado, resultado_real_usd, resultado_com_liquidacao_usd) com margem
    isolada e alavancagem `alav` (regra no cabecalho). Sem liquidacao os dois
    resultados sao iguais."""
    lim = 1.0 / alav - mmr
    if lim <= 0:
        raise ValueError(f"alavancagem {alav} com MMR {mmr}: limiar <= 0")
    real = resultado_usd(t, nocional, custo_bps)
    ts, hi, lo, cl = cam
    sg = 1.0 if t["direcao"] == "LONG" else -1.0
    e = t["preco_entrada"]
    fim_ts, fim_px = sorted(t["saidas"], key=lambda x: x[0])[-1][:2]
    liq_ts = None
    for k in range(bisect_right(ts, t["ts_entrada"]), bisect_right(ts, fim_ts)):
        ext = lo[k] if sg > 0 else hi[k]
        if ts[k] == fim_ts and fim_px != cl[k]:
            ext = fim_px
        if sg * (e - ext) / e >= lim:
            liq_ts = ts[k]
            break
    if liq_ts is None:
        return False, real, real
    custo, q = custo_bps / 1e4, nocional / e
    feitas = [(p, f) for x_ts, p, f in t["saidas"] if x_ts < liq_ts]
    resto = 1.0 - sum(f for _p, f in feitas)
    px_liq = e * (1.0 - sg * lim)
    liq = (-nocional * custo
           + sum(q * f * (sg * (p - e) - p * custo) for p, f in feitas)
           - nocional / alav * resto - q * resto * px_liq * custo)
    return True, real, liq


def linhas_log(res: dict, tf: str = "4h") -> list:
    """Texto do registro 2 e 3 (secao 5) para UMA banca."""
    m, adv = res["metricas"], res["adverso"]
    if not m.get("n_marcas"):
        return ["    sem trades"]
    f = lambda ts: _iso(ts) if isinstance(ts, int) else (ts or "-")  # noqa

    def dd(pre, nome):
        return (f"    rebaixamento ({nome}): maior em % {m[pre + '_pct']:.2f}% "
                f"pico {f(m[pre + '_pct_pico'])} vale {f(m[pre + '_pct_vale'])}"
                f" | maior em USD {m[pre + '_usd']:,.2f} pico "
                f"{f(m[pre + '_usd_pico'])} vale {f(m[pre + '_usd_vale'])}")
    return [
        f"    banca final {m['banca_final']:,.2f} USD | retorno "
        f"{m['retorno_pct']:+.2f}% | marcas de {tf} {m['n_marcas']}",
        dd("dd4", f"marcas {tf}"),
        dd("ddd", "fechamentos diarios") +
        f" | dias seguidos abaixo do pico {m['dias_abaixo_pico_max']} | pior "
        f"dia {f(m['pior_dia'])} {m['pior_dia_usd']:+,.2f} USD",
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
