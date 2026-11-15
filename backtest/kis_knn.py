# backtest/kis_knn.py — Setup Atirador (SANDBOX, camada 2) — KIS: FILTRO kNN
# Briefing v3 de 14/11/2026, ETAPA 2. Exploratorio: nada aqui promove nada.
#
# ORIGEM: "VWMA with kNN Machine Learning: MFI/ADX" (lastguru, Pine v4),
# auditado em 31/10. O gatilho deixa de ser o cruzamento de VWMA e passa a ser a
# inversao do alvo do KIS que PASSA no portao (0.02 / 11): sao exatamente as
# entradas candidatas da estrategia. Inversao vetada pelo PORTAO nao e gatilho
# (nao entra na memoria nem rotula ninguem); continua fechando o trade aberto,
# como sempre (o `hold` do kis_horizonte vai ate a proxima inversao qualquer).
#
# FEATURES (conta NOVA, fiel ao Pine; NAO e o `_adx` 13/13 do repo, que segue
# so no portao): ADX = 100*rma(|diP-diM|/soma, 6) com DI em rma 13; MFI 13 sobre
# ohlc4 com volume. rma = Wilder (alfa 1/n), semente = SMA dos n primeiros.
# Feature indefinida (warmup) => gatilho fora da memoria e NAO filtrado.
#
# ROTULO do gatilho i do simbolo s: +1 se close(gatilho i+1 de s) >= close(i),
# senao -1. Existe so a partir do timestamp do gatilho i+1 (RESOLUCAO).
# MEMORIA unica para todos os simbolos, desde o 1o candle do store; guarda TODOS
# os gatilhos (operados ou vetados pelo kNN). Ordem global por timestamp: antes
# de decidir em t entram os pontos com resolucao <= t (o close de t ja e
# conhecido na decisao em t — e o que o Pine faz ao rotular o ponto anterior no
# proprio cruzamento). Ponto com resolucao > t NUNCA e visto em t (teste 3.6).
# DECISAO: euclidiana bruta em (ADX, MFI), k = 45 com empates no 45o incluidos;
# soma >= +2 libera LONG, <= -2 libera SHORT, resto veta. Memoria < 45: usa
# todos; vazia: nao filtra.
# VETO = semantica do portao: a inversao fecha o trade anterior; vetada, nao
# abre — carteira zerada ate a proxima inversao liberada. kNN e filtro de
# ENTRADA puro: o retorno de cada trade e o mesmo do kis_saida.
# PLACEBO: veto aleatorio com a MESMA contagem de vetos do kNN, por lado, sobre a
# MESMA populacao filtravel (feature definida e memoria nao vazia); 20 sementes.
#
# T1: mesma trava do kis_saida (`--end` <= 2026-08-26 e TEMP VIEW na conexao).
# Uso (VM): python -m backtest.kis_knn --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import heapq
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP, _adx                   # noqa: E402
from backtest.kis_horizonte import (PORTAO_ADX_MIN, PORTAO_LIMIAR,  # noqa: E402
                                    entradas)
from backtest.kis_regime import inclinacoes, passa                # noqa: E402
from backtest.kis_saida import (H1, H2, TETO_ISO, _guarda_relogio,  # noqa: E402
                                _iso, _ms, conecta_com_teto, resumo,
                                stop_exit)
from backtest.kis_trail import (COST_BPS, RSS_CEILING_MIB, _dentro,  # noqa: E402
                                _rss_mib, trail_exit)

K_VIZ = 45
LIMIAR_SOMA = 2
DI_LEN, ADX_SUAV, MFI_LEN = 13, 6, 13
SEMENTES = tuple(range(20))
PARES = ((34, 89), (8, 21))
# rets por trade: indice 0 = E0, 1..3 = E1-2, E1-3, E1-4 (stop fixo k x ATR)
NOMES_RET = ("E0", "E1-2", "E1-3", "E1-4")
# Saidas escolhidas no resultado da etapa 1 (Marcelo, 14/11): 34/89 -> E1-3 com
# as vizinhas E1-2/E1-4 reportadas junto; 8/21 (controle) -> E1-2.
SAIDAS = {(34, 89): ("E0", "E1-3", "E1-2", "E1-4"), (8, 21): ("E0", "E1-2")}


# ---------------------------------------------------------------- features ---
def _rma(xs: list, n: int) -> list:
    """Wilder (alfa 1/n), semente = SMA dos n primeiros valores definidos."""
    out, buf, r = [None] * len(xs), [], None
    for i, x in enumerate(xs):
        if x is None:
            continue
        if r is None:
            buf.append(x)
            if len(buf) == n:
                r = sum(buf) / n
                out[i] = r
        else:
            r = r + (x - r) / n
            out[i] = r
    return out


def adx_knn(candles: list, di_len: int = DI_LEN, suav: int = ADX_SUAV) -> list:
    n = len(candles)
    tr, pdm, mdm = [None] * n, [None] * n, [None] * n
    for i in range(1, n):
        c, p = candles[i], candles[i - 1]
        up, down = c["high"] - p["high"], p["low"] - c["low"]
        pdm[i] = up if up > down and up > 0 else 0.0
        mdm[i] = down if down > up and down > 0 else 0.0
        tr[i] = max(c["high"] - c["low"], abs(c["high"] - p["close"]),
                    abs(c["low"] - p["close"]))
    trur, rp, rm = _rma(tr, di_len), _rma(pdm, di_len), _rma(mdm, di_len)
    ratio = [None] * n
    for i in range(n):
        if trur[i] is None or rp[i] is None or rm[i] is None or trur[i] <= 0:
            continue
        dip, dim = 100 * rp[i] / trur[i], 100 * rm[i] / trur[i]
        soma = dip + dim
        ratio[i] = abs(dip - dim) / (soma if soma != 0 else 1)
    return [None if v is None else 100 * v for v in _rma(ratio, suav)]


def mfi_knn(candles: list, n_len: int = MFI_LEN) -> list:
    n = len(candles)
    o4 = [(c["open"] + c["high"] + c["low"] + c["close"]) / 4 for c in candles]
    out = [None] * n
    for i in range(n_len, n):
        up = dn = 0.0
        for j in range(i - n_len + 1, i + 1):
            chg = o4[j] - o4[j - 1]
            if chg > 0:
                up += candles[j]["volume"] * o4[j]
            elif chg < 0:
                dn += candles[j]["volume"] * o4[j]
        out[i] = 100.0 if dn == 0 else (0.0 if up == 0 else
                                         100.0 - 100.0 / (1.0 + up / dn))
    return out


# ------------------------------------------------------------------ motor ---
def decidir(gatilhos: list, k: int = K_VIZ, chave_memoria: str = "resolucao",
            campo_rotulo: str = "close"):
    """`gatilhos` = [{ts, sym, long, adx, mfi, close}]. Devolve lista paralela
    de estados: 'libera' | 'veta' | 'sem_feature' | 'vazia' e, para os
    filtraveis, se a memoria tinha < k pontos (frio). PURA (sem I/O).
    `chave_memoria` = 'gatilho' existe SO para o teste de mutacao 3.6: insere o
    ponto pelo timestamp do gatilho (vazando o rotulo do futuro).
    `campo_rotulo` = preco que rotula: +1 se campo(proximo) >= campo(este).
    Default 'close' (etapa 2, inalterado); a etapa 5 (vwma_knn) usa 'open',
    como o script original."""
    import numpy as np
    ordem = sorted(range(len(gatilhos)),
                   key=lambda i: (gatilhos[i]["ts"], gatilhos[i]["sym"]))
    prox = {}                       # i -> indice do proximo gatilho do simbolo
    ult_por_sym = {}
    for i in ordem:
        s = gatilhos[i]["sym"]
        if s in ult_por_sym:
            prox[ult_por_sym[s]] = i
        ult_por_sym[s] = i
    pend = []                       # (chave_ts, seq, adx, mfi, rotulo)
    for i, j in prox.items():
        g, h = gatilhos[i], gatilhos[j]
        if g["adx"] is None or g["mfi"] is None:
            continue
        rot = 1 if h[campo_rotulo] >= g[campo_rotulo] else -1
        chave = h["ts"] if chave_memoria == "resolucao" else g["ts"]
        pend.append((chave, i, g["adx"], g["mfi"], rot))
    heapq.heapify(pend)
    N = len(pend)
    A, M, R = np.empty(N), np.empty(N), np.empty(N, dtype=np.int8)
    n_mem = 0
    out = [None] * len(gatilhos)
    for i in ordem:
        g = gatilhos[i]
        while pend and pend[0][0] <= g["ts"]:
            _c, _s, a, m, r = heapq.heappop(pend)
            A[n_mem], M[n_mem], R[n_mem] = a, m, r
            n_mem += 1
        if g["adx"] is None or g["mfi"] is None:
            out[i] = ("sem_feature", False, 0)
            continue
        if n_mem == 0:
            out[i] = ("vazia", True, 0)
            continue
        d2 = (A[:n_mem] - g["adx"]) ** 2 + (M[:n_mem] - g["mfi"]) ** 2
        if n_mem > k:
            lim = np.partition(d2, k - 1)[k - 1]
            soma = int(R[:n_mem][d2 <= lim].sum())
        else:
            soma = int(R[:n_mem].sum())
        ok = soma >= LIMIAR_SOMA if g["long"] else soma <= -LIMIAR_SOMA
        out[i] = ("libera" if ok else "veta", n_mem < k, soma)
    return out


def placebo(gatilhos: list, estados: list, semente: int) -> set:
    """Indices vetados pelo placebo: por lado, sorteia entre os filtraveis
    (libera/veta) EXATAMENTE quantos o kNN vetou nesse lado."""
    rng = random.Random(semente)
    vet = set()
    for lado in (True, False):
        pop = [i for i, g in enumerate(gatilhos) if g["long"] is lado
               and estados[i][0] in ("libera", "veta")]
        n_v = sum(1 for i in pop if estados[i][0] == "veta")
        vet.update(rng.sample(pop, n_v))
    return vet


# ---------------------------------------------------------------- dados -----
def run(conn, symbols: list, tf: str, end_ms: int, pares=PARES) -> dict:
    """Por par: lista de gatilhos (com features) e, para cada gatilho que vira
    trade, a tupla no formato do kis_saida.resumo com rets [E0, E1-2..4]."""
    from backtest.candle_store import read_candles
    from backtest.excursion import measure_event
    gat = {p: [] for p in pares}
    trd = {p: {} for p in pares}          # indice do gatilho -> trade
    cont = {p: {"inversoes": 0, "vetadas_portao": 0, "excl_borda": 0,
                "excl_hold": 0} for p in pares}
    rss = _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=None, end_ms=end_ms)
        if not cndl:
            continue
        closes = [c["close"] for c in cndl]
        incl, adx_p = inclinacoes(cndl, closes), _adx(cndl)
        adk, mfk = adx_knn(cndl), mfi_knn(cndl)
        pos = {c["ts"]: i for i, c in enumerate(cndl)}
        ult = len(cndl) - 1
        for par in pares:
            evs = entradas(cndl, par[0], par[1])
            for n_ev, ev in enumerate(evs):
                cont[par]["inversoes"] += 1
                i = pos[ev["bar_ts"]]
                if not passa(ev["direction"], incl[i], adx_p[i],
                             PORTAO_LIMIAR, PORTAO_ADX_MIN):
                    cont[par]["vetadas_portao"] += 1
                    continue
                is_long = ev["direction"] == "LONG"
                gi = len(gat[par])
                gat[par].append({"ts": ev["bar_ts"], "sym": sym,
                                 "long": is_long, "adx": adk[i],
                                 "mfi": mfk[i], "close": closes[i]})
                m = measure_event(conn, sym, ev["bar_ts"], ev["direction"],
                                  tf=tf, path_cap=EXT_CAP)
                if m is None:
                    cont[par]["excl_borda"] += 1
                    continue
                hold = ev["hold"]
                if hold > len(m["fwd_bar"]):
                    cont[par]["excl_hold"] += 1
                    continue
                ap = m["atr"] / m["entry"]
                rets = [trail_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                                   m["fwd_bar"], hold, 0, 0)
                        * ap * 1e4 - COST_BPS]
                bits = 0
                for b, kk in enumerate((2, 3, 4), start=1):
                    r, parou = stop_exit(m["fav_bar"], m["adv_bar"],
                                         m["open_bar"], m["fwd_bar"], hold,
                                         kk, False)
                    rets.append(r * ap * 1e4 - COST_BPS)
                    bits |= (1 << b) if parou else 0
                trd[par][gi] = (ev["bar_ts"], sym, is_long, rets, bits,
                                hold >= EXT_CAP,
                                n_ev == len(evs) - 1 and i + hold == ult)
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} "
                              f"apos {sym}; PARANDO antes de estourar a VM.")
    return {"gat": gat, "trd": trd, "cont": cont, "rss": rss}


def _sub(trd: dict, vetados: set, syms: set) -> list:
    return sorted((t for gi, t in trd.items()
                   if gi not in vetados and t[1] in syms), key=lambda t: t[0])


def _pct(xs: list, q: float) -> float:
    """Posto mais proximo (nearest-rank): P90 de 20 = 18o valor ordenado."""
    s = sorted(xs)
    return s[max(0, math.ceil(round(q * len(s), 9)) - 1)]


CAMPOS = ["recorte", "par", "filtro", "saida", "metade", "n", "veto_pct",
          "ret6", "ret10", "bpt6", "bpt10", "dd", "tri_pos", "tri_tot",
          "n_long", "ret_long", "n_short", "ret_short", "top3", "melhor_mes",
          "ret_sem_melhor_mes", "n_stop", "n_trunc256", "n_marcado_fim"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="KIS: filtro kNN (etapa 2)")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=TETO_ISO)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    args = ap.parse_args(argv)
    if args.end > TETO_ISO:
        raise SystemExit(f"[kis_knn] T1: --end {args.end} passa do teto "
                         f"{TETO_ISO}. Hold-out trancado. ABORTADO.")
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from backtest.sweep import TIER1
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    teto_ms = _ms(args.end)
    conn = conecta_com_teto(args.store, teto_ms)
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[kis_knn] T1 VIOLADO. ABORTADO.")
        firsts = {s: conn.execute(
            "SELECT MIN(ts) FROM candles WHERE symbol=? AND timeframe=?",
            (s, args.tf)).fetchone()[0] for s in symbols}
        print(f"[kis_knn] teto {args.end} (exclusivo) | maior ts visivel "
              f"{_iso(mx) if mx else '-'} | simbolos {len(symbols)} | sem "
              f"candles: {[s for s, v in firsts.items() if v is None]}")
        fs = [v for v in firsts.values() if v is not None]
        if fs:
            print(f"  primeiro candle: min {_iso(min(fs))} | max {_iso(max(fs))}"
                  " (memoria do kNN comeca no primeiro de cada simbolo)")
        r = run(conn, [s for s in symbols if firsts.get(s)], args.tf,
                teto_ms - 1)
    finally:
        conn.close()

    recortes = {"UNIVERSO": set(symbols), "TIER1": set(TIER1) & set(symbols)}
    recortes.update({s: {s} for s in symbols})
    rows, ranks = [], []
    print("\n===== GATILHOS E kNN =====")
    for par in r["gat"]:
        g, trd, p = r["gat"][par], r["trd"][par], f"{par[0]}/{par[1]}"
        est = decidir(g)
        c = r["cont"][par]
        cnt = {e: sum(1 for x in est if x[0] == e)
               for e in ("libera", "veta", "sem_feature", "vazia")}
        frio = sum(1 for x in est if x[0] in ("libera", "veta") and x[1])
        print(f"  {p}: inversoes {c['inversoes']} | vetadas pelo portao "
              f"{c['vetadas_portao']} | gatilhos {len(g)} | trades {len(trd)} "
              f"(excl_borda {c['excl_borda']}, excl_hold {c['excl_hold']})")
        print(f"        kNN: libera {cnt['libera']} | veta {cnt['veta']} | "
              f"sem feature (passa) {cnt['sem_feature']} | memoria vazia "
              f"(passa) {cnt['vazia']} | inicio frio (<{K_VIZ} pontos) {frio}")
        for lado, nome in ((True, "LONG"), (False, "SHORT")):
            pop = [x for gg, x in zip(g, est) if gg["long"] is lado
                   and x[0] in ("libera", "veta")]
            nv = sum(1 for x in pop if x[0] == "veta")
            print(f"        veto kNN {nome}: {nv}/{len(pop)} "
                  f"({100 * nv / len(pop) if pop else 0:.1f}%)")
        vet_knn = {i for i, x in enumerate(est) if x[0] == "veta"}
        filtros = [("sem", set()), ("knn", vet_knn)] + [
            (f"placebo_s{s:02d}", placebo(g, est, s)) for s in SEMENTES]
        for rec, syms in recortes.items():
            base = {h: sum(1 for t in _sub(trd, set(), syms)
                           if _dentro(t[0], f)) for h, f in (("H1", H1),
                                                             ("H2", H2))}
            for nome_f, vet in filtros:
                if rec not in ("UNIVERSO", "TIER1") and \
                        nome_f.startswith("placebo"):
                    continue
                sub = _sub(trd, vet, syms)
                for saida in SAIDAS[par]:
                    k = NOMES_RET.index(saida)
                    for h, f in (("H1", H1), ("H2", H2)):
                        m = resumo(sub, k, f)
                        rows.append({"recorte": rec, "par": p,
                                     "filtro": nome_f, "saida": saida,
                                     "metade": h, "veto_pct": (
                                         100 * (1 - m["n"] / base[h])
                                         if base[h] else 0.0), **m})

    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"kis_knn_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({c: (f"{row[c]:.1f}" if isinstance(row[c], float)
                            else row[c]) for c in CAMPOS})
    print(f"  RSS de pico {r['rss']:.0f} MiB | CSV: {out}")

    def pega(rec, p, filt, s, h):
        return next(x for x in rows if x["recorte"] == rec and x["par"] == p
                    and x["filtro"] == filt and x["saida"] == s
                    and x["metade"] == h)

    for rec in ("UNIVERSO", "TIER1"):
        print(f"\n===== {rec} ({len(recortes[rec])} simbolos) — H1 "
              f"{H1[0]}..{H1[1]} | H2 {H2[0]}..{H2[1]} =====")
        print(f"{'par':>5} {'saida':>5} {'filtro':>8} H  | {'n':>4} | {'veto':>5} "
              f"| {'ret@6':>8} | {'ret@10':>8} | {'bpt@6':>7} | {'dd':>7} | "
              f"{'tri':>5} | {'nL':>3} {'retL':>8} | {'nS':>3} {'retS':>8} | "
              f"{'sem melhor mes':>22} | top3")
        for par in r["gat"]:
            p = f"{par[0]}/{par[1]}"
            for s in SAIDAS[par]:
                for h in ("H1", "H2"):
                    for filt in ("sem", "knn"):
                        x = pega(rec, p, filt, s, h)
                        print(f"{p:>5} {s:>5} {filt:>8} {h} | {x['n']:>4} | "
                              f"{x['veto_pct']:>4.1f}% | {x['ret6']:>+8.0f} | "
                              f"{x['ret10']:>+8.0f} | {x['bpt6']:>+7.1f} | "
                              f"{x['dd']:>7.0f} | {x['tri_pos']:>2}/"
                              f"{x['tri_tot']:<2} | {x['n_long']:>3} "
                              f"{x['ret_long']:>+8.0f} | {x['n_short']:>3} "
                              f"{x['ret_short']:>+8.0f} | "
                              f"{x['ret_sem_melhor_mes']:>+8.0f} "
                              f"({x['melhor_mes']}) | {x['top3']}")
                    pl = [pega(rec, p, f"placebo_s{q:02d}", s, h)
                          for q in SEMENTES]
                    v = [x["ret6"] for x in pl]
                    kn = pega(rec, p, "knn", s, h)
                    abaixo = sum(1 for y in v if y < kn["ret6"])
                    ranks.append((rec, p, s, h, abaixo, kn["ret6"] > _pct(v, .9)))
                    print(f"{p:>5} {s:>5} {'placebo':>8} {h} | {median(x['n'] for x in pl):>4.0f} | "
                          f"{median(x['veto_pct'] for x in pl):>4.1f}% | "
                          f"P10 {_pct(v, .1):>+7.0f} | med {median(v):>+7.0f} | "
                          f"P90 {_pct(v, .9):>+7.0f} | kNN acima de {abaixo}/20 "
                          f"placebos | sem melhor mes med "
                          f"{median(x['ret_sem_melhor_mes'] for x in pl):>+7.0f}"
                          f" | tri+ med {median(x['tri_pos'] for x in pl):.0f}")

    print("\n===== POR MES — UNIVERSO, ret@6, saida escolhida: sem | kNN =====")
    for par in r["gat"]:
        p, s = f"{par[0]}/{par[1]}", SAIDAS[par][1]
        k = NOMES_RET.index(s)
        est = decidir(r["gat"][par])
        vet = {i for i, x in enumerate(est) if x[0] == "veta"}
        a = _sub(r["trd"][par], set(), recortes["UNIVERSO"])
        b = _sub(r["trd"][par], vet, recortes["UNIVERSO"])
        print(f"  par {p} saida {s}")
        for mm in sorted({_iso(t[0])[:7] for t in a}):
            sa = [t for t in a if _iso(t[0])[:7] == mm]
            sb = [t for t in b if _iso(t[0])[:7] == mm]
            print(f"  {mm} | sem n {len(sa):>3} {sum(t[3][k] for t in sa):>+8.0f}"
                  f" | kNN n {len(sb):>3} {sum(t[3][k] for t in sb):>+8.0f}")

    print("\n===== RESUMO DO kNN CONTRA O PLACEBO (ret@6) =====")
    for rec, p, s, h, ab, p90 in ranks:
        print(f"  {rec:>8} {p:>5} {s:>5} {h}: kNN acima de {ab:>2}/20 | "
              f"{'ACIMA do P90' if p90 else 'nao passa do P90'}")
    print("\ncaveat: EXPLORATORIO (camada 2), nada promove. Teto T1 "
          f"{TETO_ISO}. Gatilho = inversao do KIS que passa no portao; memoria "
          "unica 65 simbolos, ponto entra na RESOLUCAO do rotulo (<= t). "
          "Features ADX/MFI em conta nova fiel ao Pine. Placebo = mesma "
          "contagem de vetos por lado na mesma populacao filtravel, 20 "
          "sementes; P90 por posto mais proximo (18o de 20). Retorno de cada "
          "trade identico ao kis_saida; kNN so remove entradas. Metades por "
          "data de ENTRADA; mergulho sobre a curva de trades. @10 = @6 - 4n. "
          "Celula exploratoria 'segurar atraves do veto' NAO rodada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
