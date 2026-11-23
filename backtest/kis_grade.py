# backtest/kis_grade.py — Setup Atirador (SANDBOX, camada 2) — KIS 34/89:
# grade de sensibilidade das saidas (v1). Briefing de 23/11/2026.
#
# O instrumento SO MEDE E DECOMPOE. Sem veredito, sem celula "otima", sem
# criterio de aceite. A leitura e a decisao sao do Marcelo.
#
# FIXO EM TODAS AS CELULAS (reusado do kis_refino, nada reimplementado):
# entradas e portao = `kis_refino.eventos` (KIS_E1: Extremos, EMA 34/89, conf 2,
# portao EMA21 / 0,02 ATR/barra / ADX13 >= 11, entrada no fechamento, 4h,
# SYMBOLS_KIS_3489); saida na inversao SEM esperar o fechamento (regra do T5s:
# gatilho de `kis_refino.gatilhos`, recursao da EMA com valores de t-1); motor
# de saida = `kis_refino.simula` (stop k x ATR14 da barra do sinal executado em
# min(stop, abertura); parciais como ordens limite em max(nivel, abertura);
# stop e alvo na mesma barra: stop primeiro); custo 10 e 6 bps por trade com
# pernas proporcionais (`kis_refino._trade`).
#
# GRADE DEFAULT (84 celulas): stop k em {2, 3, 4} x parciais {sem; 3/6; 4/8;
# 6/12} com fracoes {50/25; 33/33} (so com parciais) x teto de duracao {sem;
# 128; 256; 384} barras. Teto T: o trade ainda aberto sai no FECHAMENTO da
# barra i+T, tipo "teto" (o `simula` termina em e = min(fim, i+T)). Fracoes
# 33/33 deixam 34% para stop/inversao/teto/fim.
#
# REFERENCIAS (fora da grade): REF_BASE = KIS_E1-3 com inversao no fechamento,
# stop 3, sem teto, calculada AQUI pelo motor da grade (= BASE do kis_refino);
# REF_T5s = a celula T5s calculada pelo PROPRIO `kis_refino.trades_simbolo`
# (deve ser identica a k3_p0_t0). Conferencias trade a trade nos tres
# periodos: REF_BASE x BASE do kis_refino e k3_p0_t0 x T5s do kis_refino. As
# entradas vem da mesma `eventos` nos dois lados: a conferencia prova a
# parametrizacao da grade, nao a regra de entrada (essa ja conferida em 23/11
# contra kis_saida e holdout_2026).
#
# PERIODOS: os do kis_refino (H1, H2 no sandbox com velas < 2026-08-26 e
# exclusao de borda de 48 barras; NOVO de 31/08 a --end).
#
# CARTEIRA: `backtest.carteira`, uma banca isolada de 10.000 USD por celula e
# periodo, 300 USD por trade; um unico CSV diario por rodada; rebaixamento do
# resumo nas marcas de 4h (as duas medidas, % e USD, cada uma com o seu pico e
# vale); dias abaixo do pico nos fechamentos diarios; liquidacao (so medida)
# com L = 3, 5, 10 e MMR --mmr; distancia do stop em % do preco de entrada.
#
# VM 956 MiB: velas em arrays compactos (`Velas`), uma leitura por simbolo;
# celulas em sequencia, uma por vez, trades da celula descartados ao fim dela;
# teto de RSS de 250 MiB que interrompe; guarda de horario do kis_saida.
#
# EIXOS PARAMETRIZAVEIS (briefing de 23/11/2026, v1 da sensibilidade do
# stop): --ks, --parciais (0 = sem), --fracoes, --tetos; defaults = a grade
# acima (sem argumentos a rodada de 23/11 se reproduz). As duas referencias
# rodam sempre; a conferencia k3_p0_t0 x T5s tambem: se k3_p0_t0 nao estiver
# na grade pedida, ela e calculada so para conferir e nao vai aos CSVs.
# Eixos invalidos (parcial decrescente, fracoes > 100%) abortam antes do store.
#
# Uso (VM): python -m backtest.kis_grade --store <candles_v9.db>
#   [--ks 2.5 3 3.5] [--parciais 3-6 6-12] [--fracoes 50-25 33-33] [--tetos 0]
from __future__ import annotations

import argparse
import csv
import gc
import math
import sys
import time
from array import array
from bisect import bisect_left
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest import carteira as cart                                # noqa: E402
from backtest import kis_refino as kr                                # noqa: E402
from backtest.aio_graduado import _Log                               # noqa: E402
from backtest.holdout_2026 import END_ISO, JANELA                    # noqa: E402
from backtest.kis_saida import (H1, H2, TETO_ISO, _guarda_relogio,   # noqa: E402
                                _iso, _ms, conecta_com_teto)
from backtest.kis_trail import RSS_CEILING_MIB, _rss_mib             # noqa: E402

# ----------------------------------------------------------- parametros -----
SETUP = "KIS_GRADE"
STOPS = (2.0, 3.0, 4.0)
PARCIAIS = (None, (3.0, 6.0), (4.0, 8.0), (6.0, 12.0))   # None = sem
FRACOES = ((0.50, 0.25), (0.33, 0.33))
TETOS = (0, 128, 256, 384)                  # 0 = sem teto
CONF_T5s = "k3_p0_t0"                       # celula conferida contra o T5s
ALAVS = (3, 5, 10)
PERIODOS = kr.PERIODOS
REFS = ("REF_BASE", "REF_T5s")


def _n(x: float) -> str:
    return f"{x:g}"


def _par(txt: str, oque: str) -> tuple:
    a, sep, b = txt.partition("-")
    try:
        x, y = float(a), float(b)
    except ValueError:
        x = y = math.nan
    if not sep or not (math.isfinite(x) and math.isfinite(y)):
        raise argparse.ArgumentTypeError(f"{oque} '{txt}': formato A-B")
    return x, y


def arg_k(txt: str) -> float:
    try:
        k = float(txt)
    except ValueError:
        k = math.nan
    if not (math.isfinite(k) and k > 0):
        raise argparse.ArgumentTypeError(f"stop k '{txt}': numero > 0")
    return k


def arg_parcial(txt: str):
    """'0' = sem parciais; 'A-B' com 0 < A < B (em ATR)."""
    if txt.strip() == "0":
        return None
    a, b = _par(txt, "parcial")
    if not 0 < a < b:
        raise argparse.ArgumentTypeError(f"parcial '{txt}': exige 0 < A < B")
    return a, b


def arg_fracao(txt: str) -> tuple:
    """'A-B' em % da posicao; A, B > 0 e A + B <= 100."""
    a, b = _par(txt, "fracao")
    if not (a > 0 and b > 0 and a + b <= 100):
        raise argparse.ArgumentTypeError(f"fracao '{txt}': exige A, B > 0 e "
                                         "A + B <= 100")
    return round(a / 100, 6), round(b / 100, 6)


def arg_teto(txt: str) -> int:
    try:
        t = int(txt)
    except ValueError:
        t = -1
    if t < 0:
        raise argparse.ArgumentTypeError(f"teto '{txt}': inteiro >= 0")
    return t


def _cel(k: float, p, f, t: int) -> dict:
    pn = (f"p{_n(p[0])}-{_n(p[1])}_f{round(f[0] * 100)}-"
          f"{round(f[1] * 100)}" if p else "p0")
    return {"nome": f"k{_n(k)}_{pn}_t{t}", "k": float(k), "parc": p,
            "frac": f if p else None, "teto": t, "toque": True, "ref": None}


def celulas(ks=STOPS, parciais=PARCIAIS, fracoes=FRACOES,
            tetos=TETOS) -> list:
    """Referencias + grade. Cada celula: {nome, k, parc, frac, teto, toque,
    ref}. parc = niveis em ATR ou None; frac = fracoes ou None. Ordem: k,
    depois parciais na ordem dada (cada uma com todas as fracoes), depois
    teto. Duplicatas de eixo sao descartadas (primeira ocorrencia)."""
    out = [{"nome": "REF_BASE", "k": 3.0, "parc": None, "frac": None,
            "teto": 0, "toque": False, "ref": "BASE"},
           {"nome": "REF_T5s", "k": 3.0, "parc": None, "frac": None,
            "teto": 0, "toque": True, "ref": "T5s"}]
    combos = []
    for p in dict.fromkeys(parciais):
        combos += [(None, None)] if p is None else \
            [(p, f) for f in dict.fromkeys(fracoes)]
    for k in dict.fromkeys(float(x) for x in ks):
        for p, f in combos:
            for t in dict.fromkeys(tetos):
                out.append(_cel(k, p, f, t))
    return out


def niveis(cel: dict) -> tuple:
    """Parciais no formato do `kis_refino.simula`."""
    if not cel["parc"]:
        return ()
    (a, b), (fa, fb) = cel["parc"], cel["frac"]
    return ((a, fa, "parcial1"), (b, fb, "parcial2"))


# ------------------------------------------------------- velas compactas ----
class Velas:
    """Velas de um simbolo em arrays; `v[k]` devolve o dict que o
    `kis_refino.simula` le. `prefixo(n)` = as n primeiras, sem copiar."""
    __slots__ = ("ts", "o", "h", "l", "c", "n")

    def __init__(self, cndl: list = None):
        if cndl is None:
            return
        self.ts = array("q", (c["ts"] for c in cndl))
        self.o = array("d", (c["open"] for c in cndl))
        self.h = array("d", (c["high"] for c in cndl))
        self.l = array("d", (c["low"] for c in cndl))     # noqa: E741
        self.c = array("d", (c["close"] for c in cndl))
        self.n = len(cndl)

    def prefixo(self, n: int) -> "Velas":
        v = Velas()
        v.ts, v.o, v.h, v.l, v.c, v.n = self.ts, self.o, self.h, self.l, \
            self.c, n
        return v

    def __len__(self) -> int:
        return self.n

    def __getitem__(self, k: int) -> dict:
        if not 0 <= k < self.n:
            raise IndexError(k)
        return {"ts": self.ts[k], "open": self.o[k], "high": self.h[k],
                "low": self.l[k], "close": self.c[k]}

    def caminho(self, ini_ms: int) -> tuple:
        """(ts, high, low, close) da carteira, a partir de ini_ms."""
        a = bisect_left(self.ts, ini_ms, 0, self.n)
        return (self.ts[a:self.n], self.h[a:self.n], self.l[a:self.n],
                self.c[a:self.n])


def _garr(g: list) -> array:
    return array("d", (math.nan if x is None else x for x in g))


# ------------------------------------------------------------ motor ---------
def simula_celula(cel: dict, v, ev: dict, gL, gS) -> tuple:
    """Um trade numa celula. gL/gS indexaveis (lista com None ou array com
    nan)."""
    i, e, tipo = ev["i"], ev["e_full"], ev["t_full"]
    if cel["teto"] and e - i > cel["teto"]:
        e, tipo = i + cel["teto"], "teto"
    gat = None
    if cel["toque"]:
        g = gS if ev["lg"] else gL

        def gat(j):
            x = g[j]
            return None if x is None or x != x else x
    return kr.simula(v, i, e, ev["en"], ev["atr"], ev["sg"], k=cel["k"],
                     parciais=niveis(cel), gat=gat, tipo_fim=tipo)


def trades_celula(cel: dict, v, evs: list, gL, gS, sym: str) -> list:
    out = []
    for ev in evs:
        sim = simula_celula(cel, v, ev, gL, gS)
        out.append(kr._trade(cel["nome"], sym, v, ev["i"], ev["lg"], ev["en"],
                             ev["atr"], sim))
    return out


# ------------------------------------------------------------ coleta --------
def _ev_compacto(ev: dict) -> dict:
    return {k: ev[k] for k in ("i", "lg", "sg", "en", "atr", "e_full",
                               "t_full")}


def coleta(conn, symbols: list, tf: str, teto_ms: int, end_ms: int,
           conferir: bool = True) -> dict:
    """Uma leitura por simbolo. Guarda velas compactas, eventos e gatilhos por
    modo, e o BASE/T5s do proprio kis_refino (conferencia e REF_T5s)."""
    from backtest.candle_store import read_candles
    dados, cont, sem = {}, {m: kr.novo_cont() for m in ("sandbox", "novo")}, []
    ref = {"BASE": {"sandbox": [], "novo": []},
           "T5s": {"sandbox": [], "novo": []}}
    rss = _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=None, end_ms=end_ms - 1)
        if not cndl:
            sem.append(sym)
            continue
        n_sb = bisect_left([c["ts"] for c in cndl], teto_ms)
        v = Velas(cndl)
        d = {"v": v, "n_sb": n_sb}
        for modo, cv in (("sandbox", cndl[:n_sb]), ("novo", cndl)):
            gL, gS, _st = kr.gatilhos(cv)
            d[modo] = {"evs": [_ev_compacto(e) for e in
                               kr.eventos(cv, modo, cont[modo])],
                       "gL": _garr(gL), "gS": _garr(gS)}
            if conferir:
                r = kr.trades_simbolo(cv, sym, modo, kr.novo_cont())
                ref["BASE"][modo].extend(r["BASE"])
                ref["T5s"][modo].extend(r["T5s"])
                del r
            del cv, gL, gS, _st
        dados[sym] = d
        del cndl
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} "
                              f"apos {sym}; PARANDO.")
    cam = {"sandbox": {s: d["v"].prefixo(d["n_sb"]).caminho(_ms(H1[0]))
                       for s, d in dados.items()},
           "novo": {s: d["v"].caminho(JANELA[0]) for s, d in dados.items()}}
    return {"dados": dados, "cont": cont, "sem": sem, "ref": ref, "cam": cam,
            "rss": rss}


def por_periodo(cel: dict, A: dict, ordem: dict) -> dict:
    """{periodo: trades ordenados} de UMA celula."""
    por = {p: [] for p in PERIODOS}
    for modo in ("sandbox", "novo"):
        if cel["nome"] == "REF_T5s":
            lst = [dict(t, cel="REF_T5s") for t in A["ref"]["T5s"][modo]]
        else:
            lst = []
            for sym, d in A["dados"].items():
                v = d["v"].prefixo(d["n_sb"]) if modo == "sandbox" else d["v"]
                x = d[modo]
                lst.extend(trades_celula(cel, v, x["evs"], x["gL"], x["gS"],
                                         sym))
        for t in lst:
            p = kr.periodo(t, modo)
            if p:
                por[p].append(t)
    for v in por.values():
        v.sort(key=lambda t: (t["ts"], ordem.get(t["sym"], 1e9), t["sym"]))
    return por


def confere(meus: list, ref: list) -> dict:
    """Trade a trade por (ts, sym): iguais (ret6 e execucoes identicos),
    max |dif| de ret6, so de um lado."""
    a = {(t["ts"], t["sym"]): t for t in meus}
    b = {(t["ts"], t["sym"]): t for t in ref}
    com = set(a) & set(b)
    return {"n_ref": len(b), "n_meu": len(a),
            "iguais": sum(1 for k in com if a[k]["ret6"] == b[k]["ret6"]
                          and a[k]["execs"] == b[k]["execs"]),
            "dif": max((abs(a[k]["ret6"] - b[k]["ret6"]) for k in com),
                       default=0.0),
            "so_ref": len(set(b) - set(a)), "so_meu": len(set(a) - set(b))}


# ------------------------------------------------------------ resumo --------
def _desc(cel: dict) -> dict:
    p, f = cel["parc"], cel["frac"]
    return {"stop_k": _n(cel["k"]),
            "parciais": f"{_n(p[0])}/{_n(p[1])}" if p else "0",
            "fracoes": f"{round(f[0] * 100)}/{round(f[1] * 100)}" if f
            else "-", "teto": cel["teto"]}


CAMPOS_TRADES = kr.CAMPOS + ["stop_k", "parciais", "fracoes", "teto"]
CAMPOS_RESUMO = (
    ["celula", "periodo", "stop_k", "parciais", "fracoes", "teto",
     "n", "n_long", "n_short", "tot10", "bpt10", "tot6", "acerto",
     "ganho_medio", "perda_medio", "tot10_long", "tot10_short",
     "sem_melhor_mes", "sem_2pct", "tri_pos", "tri_total", "pct_stop",
     "pct_inversao", "pct_parcial1", "pct_parcial2", "pct_teto", "pct_fim",
     "banca_final", "retorno_pct", "dd_pct_max", "dd_pct_pico", "dd_pct_vale",
     "dd_usd_max", "dd_usd_pico", "dd_usd_vale", "dias_abaixo_pico_max",
     "pior_dia_usd", "pior_dia_data", "exp_max_x", "exp_mediana_x",
     "pos_max", "pos_mediana",
     "stop_dist_p50", "stop_dist_p90", "stop_dist_p99", "stop_dist_max",
     "adv_p50", "adv_p90", "adv_p99", "adv_max"] +
    [f"liq{L}_{x}" for L in ALAVS for x in ("n", "usd")])


def _f(x, nd=2):
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return x


def _ts(x):
    return _iso(x) if isinstance(x, int) else (x or "")


def linha_resumo(cel: dict, per: str, tr: list, rc: dict, cam: dict,
                 mmr: float) -> dict:
    row = {"celula": cel["nome"], "periodo": per, **_desc(cel), "n": len(tr)}
    if tr:
        r = kr.resumo(tr)
        sd = r["saidas"]
        row.update({
            "n_long": r["lados"]["L"][0], "n_short": r["lados"]["S"][0],
            "tot10": r["tot10"], "bpt10": r["bpt10"], "tot6": r["tot6"],
            "acerto": r["acerto"], "ganho_medio": r["ganho"],
            "perda_medio": r["perda"], "tot10_long": r["lados"]["L"][1],
            "tot10_short": r["lados"]["S"][1], "sem_melhor_mes": r["sem_mes"],
            "sem_2pct": r["sem_top"], "tri_pos": r["tri_pos"],
            "tri_total": r["tri_tot"], "pct_stop": sd["stop"],
            "pct_inversao": sd["inversao"],
            "pct_parcial1": 100.0 * r["p1"] / len(tr),
            "pct_parcial2": 100.0 * r["p2"] / len(tr),
            "pct_teto": sd["teto"], "pct_fim": sd["fim"]})
    m = rc["metricas"]
    row.update({"banca_final": m["banca_final"],
                "retorno_pct": m["retorno_pct"]})
    if m.get("n_marcas"):
        row.update({
            "dd_pct_max": m["dd4_pct"], "dd_pct_pico": _ts(m["dd4_pct_pico"]),
            "dd_pct_vale": _ts(m["dd4_pct_vale"]), "dd_usd_max": m["dd4_usd"],
            "dd_usd_pico": _ts(m["dd4_usd_pico"]),
            "dd_usd_vale": _ts(m["dd4_usd_vale"]),
            "dias_abaixo_pico_max": m["dias_abaixo_pico_max"],
            "pior_dia_usd": m["pior_dia_usd"], "pior_dia_data": m["pior_dia"],
            "exp_max_x": m["x_total"][0], "exp_mediana_x": m["x_total"][1],
            "pos_max": m["pos_total"][0], "pos_mediana": m["pos_total"][1]})
    sd_pct = [cel["k"] * t["atr"] / t["entry"] * 100.0 for t in tr]
    for nome, xs in (("stop_dist", sd_pct), ("adv", rc["adverso"])):
        for q in (50, 90, 99):
            row[f"{nome}_p{q}"] = cart.percentil(xs, q)
        row[f"{nome}_max"] = max(xs) if xs else None
    ct = [kr.para_carteira(t) for t in tr]
    for L in ALAVS:
        n = usd = 0
        for t in ct:
            liq, real, x = cart.liquidacao(t, cam[t["sym"]], L, mmr)
            if liq:
                n += 1
                usd += x - real
        row[f"liq{L}_n"], row[f"liq{L}_usd"] = n, float(usd)
    return {k: _f(row.get(k), 4 if k.startswith(("stop_dist", "adv")) else 2)
            for k in CAMPOS_RESUMO}


def linha_trade(t: dict, per: str, cel: dict) -> dict:
    return {**kr.linha_csv(t, per), **_desc(cel)}


# ------------------------------------------------------------ main ----------
def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="KIS 34/89: grade de "
                                 "sensibilidade das saidas (sandbox)")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=END_ISO,
                    help='exclusivo, UTC; default "%(default)s"')
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None,
                    help="default: SYMBOLS_KIS_3489 (65)")
    ap.add_argument("--mmr", type=float, default=cart.MMR,
                    help="manutencao, fracao; default %(default)s")
    ap.add_argument("--ks", nargs="+", type=arg_k, default=list(STOPS),
                    help="stop k em ATR14; default 2 3 4")
    ap.add_argument("--parciais", nargs="+", type=arg_parcial,
                    default=list(PARCIAIS),
                    help="A-B em ATR, 0 = sem; default 0 3-6 4-8 6-12")
    ap.add_argument("--fracoes", nargs="+", type=arg_fracao,
                    default=list(FRACOES),
                    help="A-B em %% da posicao; default 50-25 33-33")
    ap.add_argument("--tetos", nargs="+", type=arg_teto, default=list(TETOS),
                    help="barras, 0 = sem; default 0 128 256 384")
    ap.add_argument("--celulas", nargs="+", default=None,
                    help="subconjunto (default: todas as da grade)")
    ap.add_argument("--logs", default=str(ROOT / "logs"))
    ap.add_argument("--sem-conferencia", action="store_true")
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    return ap


def main(argv=None) -> int:
    args = parser().parse_args(argv)          # eixos validados aqui
    if args.tf != "4h":
        raise SystemExit("[kis_grade] so 4h")
    eixos = dict(ks=args.ks, parciais=args.parciais, fracoes=args.fracoes,
                 tetos=args.tetos)
    todas = celulas(**eixos)
    n_grade = len(todas) - len(REFS)
    if args.celulas:
        ruins = set(args.celulas) - {c["nome"] for c in todas}
        if ruins:
            raise SystemExit(f"[kis_grade] celulas desconhecidas: "
                             f"{sorted(ruins)}")
        todas = [c for c in todas if c["nome"] in args.celulas]
    if args.sem_conferencia:
        todas = [c for c in todas if c["nome"] != "REF_T5s"]
    teto_ms, end_ms = _ms(TETO_ISO), _ms(args.end)
    if end_ms <= JANELA[0]:
        raise SystemExit(f"[kis_grade] --end {args.end} nao passa do inicio "
                         "do periodo novo. ABORTADO.")
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    ordem = {s: k for k, s in enumerate(SYMBOLS_KIS_3489)}
    stamp = f"{datetime.now(timezone.utc):%Y%m%d_%H%M}"
    logs = Path(args.logs)
    logs.mkdir(parents=True, exist_ok=True)
    p_tr = logs / f"kis_grade_{stamp}.csv"
    p_rs = logs / f"kis_grade_resumo_{stamp}.csv"
    p_dia = logs / f"carteira_{SETUP}_{stamp}.csv"
    log = _Log(p_tr.with_suffix(".log"))
    t0 = time.monotonic()
    log(f"[kis_grade] KIS 34/89 E1 (conf 2, portao {kr.PORTAO_LIMIAR}/ADX "
        f"{kr.PORTAO_ADX_MIN}/EMA{kr.EMA_REF}) | saida na inversao sem esperar "
        f"o fechamento (T5s) | tf {args.tf} | universo {len(symbols)} | "
        f"sandbox H1 {H1[0]}..{H1[1]}, H2 {H2[0]}..{H2[1]} (velas < "
        f"{TETO_ISO}, borda {kr.BORDA}) | NOVO entradas {_iso(JANELA[0])}.."
        f"{_iso(JANELA[1])} (velas < {args.end}) | custo 10 e 6 bps/trade")
    e_k = " ".join(_n(k) for k in args.ks)
    e_p = " ".join("0" if p is None else f"{_n(p[0])}-{_n(p[1])}"
                   for p in args.parciais)
    e_f = " ".join(f"{round(a * 100)}-{round(b * 100)}"
                   for a, b in args.fracoes)
    log(f"  grade: --ks {e_k} --parciais {e_p} --fracoes {e_f} --tetos "
        f"{' '.join(map(str, args.tetos))} (fracoes so com parciais; 0 = sem)"
        f" = {n_grade} celulas + "
        f"{len(REFS)} referencias | rodando {len(todas)} | carteira "
        f"{cart.BANCA_INICIAL:,.0f} USD por celula e periodo, "
        f"{cart.NOCIONAL:.0f} USD/trade, {cart.CUSTO_BPS:g} bps/perna | "
        f"liquidacao L {ALAVS}, MMR {args.mmr:g}")
    conn = conecta_com_teto(args.store, end_ms)
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= end_ms:
            raise SystemExit("[kis_grade] view deixou passar candle >= --end")
        A = coleta(conn, symbols, args.tf, teto_ms, end_ms,
                   conferir=not args.sem_conferencia)
    finally:
        conn.close()
    log(f"  maior ts visivel {_iso(mx) if mx else '-'} | sem candle: "
        f"{' '.join(A['sem']) or 'nenhum'} | coleta {time.monotonic() - t0:.0f}"
        f" s, RSS {A['rss']:.0f} MiB")
    for modo, c in A["cont"].items():
        log(f"  {modo}: inversoes {c['inversoes']} | vetadas {c['vetadas']} | "
            f"atr0 {c['atr0']} | excl_borda {c['excl_borda']} | excl_hold "
            f"{c['excl_hold']}")

    conf, refs_log, rss = [], [], A["rss"]

    def _confere(nome: str, por: dict) -> None:
        alvo = "BASE" if nome == "REF_BASE" else "T5s"
        for p in PERIODOS:
            modo = "novo" if p == "NOVO" else "sandbox"
            ref = [t for t in A["ref"][alvo][modo]
                   if kr.periodo(t, modo) == p]
            conf.append((nome, alvo, p, confere(por[p], ref)))

    if not args.sem_conferencia and CONF_T5s not in {c["nome"] for c in todas}:
        _confere(CONF_T5s, por_periodo(_cel(3.0, None, None, 0), A, ordem))
        gc.collect()
    with p_tr.open("w", newline="") as f_tr, \
            p_rs.open("w", newline="") as f_rs, cart.DiarioUnico(p_dia) as dia:
        w_tr = csv.DictWriter(f_tr, fieldnames=CAMPOS_TRADES)
        w_rs = csv.DictWriter(f_rs, fieldnames=CAMPOS_RESUMO)
        w_tr.writeheader()
        w_rs.writeheader()
        n_tr = 0
        for n_c, cel in enumerate(todas, 1):
            tc = time.monotonic()
            por = por_periodo(cel, A, ordem)
            if not args.sem_conferencia and cel["nome"] in ("REF_BASE",
                                                            CONF_T5s):
                _confere(cel["nome"], por)
            for p in PERIODOS:
                cam = A["cam"]["novo" if p == "NOVO" else "sandbox"]
                tr = por[p]
                rc = cart.roda([kr.para_carteira(t) for t in tr], cam)
                dia.anexa(cel["nome"], p, rc["diario"])
                w_rs.writerow(linha_resumo(cel, p, tr, rc, cam, args.mmr))
                for t in tr:
                    w_tr.writerow(linha_trade(t, p, cel))
                    n_tr += 1
                if cel["nome"] in REFS:
                    refs_log.append((cel["nome"], p, kr.resumo(tr) if tr
                                     else None, rc["metricas"]))
                del rc, tr
            del por
            gc.collect()
            rss = max(rss, _rss_mib())
            if rss > RSS_CEILING_MIB:
                raise MemoryError(f"RSS {rss:.0f} MiB > teto "
                                  f"{RSS_CEILING_MIB:.0f} na celula "
                                  f"{cel['nome']}; PARANDO.")
            print(f"  [{n_c}/{len(todas)}] {cel['nome']} "
                  f"{time.monotonic() - tc:.1f} s | total "
                  f"{time.monotonic() - t0:.0f} s | RSS {rss:.0f} MiB",
                  flush=True)
        f_rs.flush()

    log("\n===== CONFERENCIAS (trade a trade: ret6 e execucoes identicos) "
        "=====")
    if args.sem_conferencia:
        log("  pulada (--sem-conferencia)")
    for nome, alvo, p, c in conf:
        log(f"  {p:>4} {nome} x {alvo} do kis_refino: iguais {c['iguais']} de "
            f"{c['n_ref']} | max|dif| {c['dif']:.2e} bps | so no kis_refino "
            f"{c['so_ref']} | so aqui {c['so_meu']}")
    log("\n===== REFERENCIAS (os numeros de todas as celulas moram no CSV de "
        "resumo) =====")
    for nome, p, r, m in refs_log:
        if r is None:
            log(f"  {p:>4} {nome}: sem trades")
            continue
        log(f"  {p:>4} {nome}: n {r['n']} | @10 {r['tot10']:+.0f} "
            f"({r['bpt10']:+.1f}/trade) | @6 {r['tot6']:+.0f} | acerto "
            f"{r['acerto']:.1f}% | banca {m['banca_final']:,.2f} USD | dd "
            f"{m.get('dd4_pct', 0):.2f}% / {m.get('dd4_usd', 0):,.2f} USD")
    log(f"\n  CSV de trades: {p_tr} ({n_tr} linhas)")
    log(f"  CSV de resumo: {p_rs} ({len(todas) * len(PERIODOS)} linhas)")
    log(f"  CSV diario unico da carteira: {p_dia} ({dia.n} linhas)")
    log(f"  RSS de pico {max(rss, _rss_mib()):.0f} MiB (teto "
        f"{RSS_CEILING_MIB:.0f}) | {time.monotonic() - t0:.0f} s")
    log("caveat: so medicao, sem veredito e sem celula escolhida. Entradas no "
        "fechamento (KIS_E1); saida na inversao no toque do gatilho (valores "
        "de t-1; toque falso conta como saida), exceto REF_BASE (no "
        "fechamento). Stop k x ATR14 da barra do sinal em min(stop, "
        "abertura); parciais limite; stop e alvo na mesma barra: stop "
        "primeiro; teto sai no fechamento da barra i+T. Custo por trade @10 "
        "= @6 - 4 bps, pernas proporcionais. Sandbox com exclusao de borda "
        "de 48 barras; NOVO sem, trade aberto no fim marcado no ultimo "
        "fechamento. Rebaixamento do resumo nas marcas de 4h (% e USD, cada "
        "um com o seu pico e vale); dias abaixo do pico nos fechamentos "
        "diarios. Liquidacao so medida: margem isolada, nocional fixo, MMR "
        f"{args.mmr:g}, barras do movimento adverso (sem a barra de entrada),"
        " parcial na mesma barra da liquidacao nao conta. "
        f"{n_grade} celulas lidas juntas = comparacoes multiplas: a melhor "
        "celula de um periodo tende a regredir no outro.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
