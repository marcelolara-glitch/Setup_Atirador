# backtest/vwma_knn.py — Setup Atirador (SANDBOX, camada 2) — VWMA 13/19 + kNN
# Briefing de 15/11/2026, ETAPA 5. Exploratorio: nada aqui promove nada.
#
# PERGUNTA: o setup "VWMA with kNN Machine Learning: MFI/ADX" (lastguru, Pine
# v4), reproduzido fielmente em 4h sobre os 65 tokens, ganha dinheiro nas duas
# metades? E o filtro kNN dele bate um veto aleatorio com a mesma taxa?
#
# REGRAS (defaults do autor, NAO mexer nesta etapa: 13/19/13/6/45/2):
#   VWMA(open, n) = soma(open*vol)/soma(vol) nas ultimas n barras, n = 13 e 19.
#     soma(vol) <= 0 -> NA (contado no log; esperado 0 no store de 15/11).
#   Cruzamento em t: para cima se V13[t] > V19[t] e V13[t-1] <= V19[t-1]
#     (`crossover` do Pine); para baixo o simetrico. NA em t ou t-1 -> nada.
#   Features no candle do cruzamento: `kis_knn.adx_knn[t]`, `kis_knn.mfi_knn[t]`.
#   Rotulo do cruzamento anterior p, gravado no cruzamento seguinte t do MESMO
#     token: +1 se open[t] >= open[p], senao -1. Gravado ANTES da previsao de t
#     (`kis_knn.decidir(..., campo_rotulo="open")`: insercao na resolucao).
#   Previsao: soma dos rotulos dos k=45 vizinhos (empates no corte inclusos;
#     memoria < 45 usa toda). Para cima com soma >= +2 -> LONG; para baixo com
#     soma <= -2 -> SHORT; senao VETADO. Memoria VAZIA (soma 0) e feature NA
#     tambem sao VETO aqui (no Pine nao ha previsao -> nao opera). Isso difere
#     da etapa 2, onde 'vazia' passava; o `decidir` nao muda, so o mapeamento.
#   Saida: stop-and-reverse, sem stop, sem alvo, sem tempo. Cruzamento contrario
#     vetado -> a posicao CONTINUA. Cruzamento liberado na mesma direcao da
#     posicao -> nada. `hold` = da entrada ate o proximo cruzamento LIBERADO na
#     direcao oposta; sem ele, ate a ultima barra visivel (trade marcado no fim).
#     Hold > EXT_CAP (256) e truncado em 256 como no kis_horizonte (contado).
#
# DESVIO CONHECIDO: execucao no close de t (convencao do motor: entry =
#   `measure_event` close da barra do sinal); o original executa no open de
#   t+1. Em cripto 24/7 o open de t+1 ~ close de t; diferenca pequena, nao zero.
#
# CELULAS: C0 (todo cruzamento reverte, controle), K_tok (memoria por token,
#   desde o 1o candle do store = o original), K_agr (memoria unica dos 65,
#   ordem (ts, simbolo), como na etapa 2).
# PLACEBO (K_tok e K_agr, 20 sementes = kis_knn.SEMENTES): veta ao acaso, POR
#   LADO, a MESMA quantidade de cruzamentos que o kNN vetou na celula
#   (`kis_knn.placebo` sobre todos os cruzamentos). ATENCAO: aqui o veto MUDA o
#   hold dos outros trades, porque a posicao continua atraves do veto. Por isso
#   cada placebo REFAZ o caminho de posicoes (`caminho`) com os vetos sorteados;
#   apagar trades da lista do kNN daria o resultado errado (teste 5).
#
# MEDICAO: cada cruzamento que vira entrada e medido UMA vez por
#   `excursion.measure_event(path_cap=EXT_CAP)`; o retorno de qualquer hold sai
#   de `kis_trail.trail_exit(..., hold, 0, 0)` * atr/entry * 1e4 - COST_BPS (6),
#   a mesma conta do kis_saida. 10 bps = 6 bps - 4 x n. Metades por ENTRADA.
#
# DUAS PASSADAS (VM 956 MiB): A) por simbolo le as velas, guarda so os
#   cruzamentos (ts, lado, adx, mfi, open) e escalares; B) por simbolo mede e
#   percorre os 43 caminhos (3 celulas + 40 placebos). Placebos guardam so
#   agregados. Teto de RSS do kis_trail checado a cada simbolo.
# T1: `--end` <= 2026-08-26 e TEMP VIEW (`kis_saida.conecta_com_teto`).
# Uso (VM): python -m backtest.vwma_knn --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import math
import sys
import time
from array import array
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP                          # noqa: E402
from backtest.kis_knn import (K_VIZ, SEMENTES, _pct, adx_knn,      # noqa: E402
                              decidir, mfi_knn, placebo)
from backtest.kis_saida import (H1, H2, TETO_ISO, _guarda_relogio,  # noqa: E402
                                _iso, _ms, conecta_com_teto, resumo)
from backtest.kis_trail import (COST_BPS, RSS_CEILING_MIB, _dentro,  # noqa: E402
                                _rss_mib, trail_exit)

N_RAPIDA, N_LENTA = 13, 19
CELULAS = ("C0", "K_tok", "K_agr")
METADES = (("H1", H1), ("H2", H2))
TOP_FRAC = 0.02


# ------------------------------------------------------------ puras ---------
def vwma(cndl: list, n: int) -> list:
    """VWMA sobre o OPEN. Soma direta da janela (sem acumulado rolante, para
    nao carregar erro de ponto flutuante). soma(vol) <= 0 -> None (NA)."""
    out = [None] * len(cndl)
    for t in range(n - 1, len(cndl)):
        sv = spv = 0.0
        for c in cndl[t - n + 1:t + 1]:
            sv += c["volume"]
            spv += c["open"] * c["volume"]
        out[t] = spv / sv if sv > 0 else None
    return out


def cruzamentos(va: list, vb: list) -> list:
    """[(t, is_long)] com `crossover`/`crossunder` do Pine; NA -> nada."""
    out = []
    for t in range(1, len(va)):
        a, b, a0, b0 = va[t], vb[t], va[t - 1], vb[t - 1]
        if a is None or b is None or a0 is None or b0 is None:
            continue
        if a > b and a0 <= b0:
            out.append((t, True))
        elif a < b and a0 >= b0:
            out.append((t, False))
    return out


def gatilhos_token(cndl: list, sym: str) -> tuple:
    """(gatilhos, n_barras_vwma_na) de UM token. Gatilho = {ts, sym, long,
    adx, mfi, open, i}; formato que `kis_knn.decidir` le. Causal: tudo em t
    usa so barras <= t."""
    va, vb = vwma(cndl, N_RAPIDA), vwma(cndl, N_LENTA)
    na = sum(1 for t in range(N_LENTA - 1, len(cndl))
             if va[t] is None or vb[t] is None)
    adk, mfk = adx_knn(cndl), mfi_knn(cndl)
    g = [{"ts": cndl[t]["ts"], "sym": sym, "long": lg, "adx": adk[t],
          "mfi": mfk[t], "open": cndl[t]["open"], "i": t}
         for t, lg in cruzamentos(va, vb)]
    return g, na


def decisoes(gatilhos: list, k: int = K_VIZ, campo: str = "open") -> list:
    """Lista paralela de (estado, frio, soma), estado em {'libera', 'veta'}.
    'vazia' e 'sem_feature' do decidir viram 'veta' com soma 0 (sem previsao
    no Pine). frio = memoria < k pontos no momento da decisao."""
    out = []
    for e, frio, soma in decidir(gatilhos, k, campo_rotulo=campo):
        if e == "libera":
            out.append(("libera", frio, soma))
        else:
            out.append(("veta", frio or e == "vazia", soma))
    return out


def caminho(cruz: list, liberado: list, ult: int) -> list:
    """Stop-and-reverse de UM token. `cruz` = [(i, is_long)] ordenado,
    `liberado` paralelo. Devolve [(k_entrada, hold_bruto, marcado_fim)]:
    contrario vetado -> a posicao continua; liberado na mesma direcao -> nada;
    sem contrario liberado -> segura ate `ult` (marcado no fim)."""
    out, pos = [], None
    for k, ((i, lg), ok) in enumerate(zip(cruz, liberado)):
        if not ok:
            continue
        if pos is None:
            pos = k
        elif cruz[pos][1] == lg:
            continue
        else:
            out.append((pos, i - cruz[pos][0], False))
            pos = k
    if pos is not None:
        out.append((pos, ult - cruz[pos][0], True))
    return out


def concentracao(rets: list) -> tuple:
    """(n_top, fatia_% do total vinda dos 2% melhores, resultado sem eles)."""
    if not rets:
        return 0, None, 0.0
    s = sorted(rets, reverse=True)
    nt = max(1, math.ceil(TOP_FRAC * len(s)))
    tot, top = sum(s), sum(s[:nt])
    # fatia so tem leitura com total > 0; com total <= 0 sai None (NA)
    return nt, (100.0 * top / tot if tot > 0 else None), tot - top


def _metade(ts: int) -> str:
    return next((h for h, f in METADES if _dentro(ts, f)), "fora")


# ------------------------------------------------------------ dados ---------
def coleta(conn, symbols: list, tf: str, end_ms: int) -> dict:
    """PASSADA A. Cruzamentos de todos os tokens (sem velas) + escalares."""
    from backtest.candle_store import read_candles
    G, fatia, meta, sem = [], {}, {}, []
    rss = _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=None, end_ms=end_ms)
        if not cndl:
            sem.append(sym)
            continue
        g, na = gatilhos_token(cndl, sym)
        a = len(G)
        G.extend(g)
        fatia[sym] = (a, len(G))
        faixas, bh = {}, {}
        for h, f in METADES:
            ix = [j for j, c in enumerate(cndl) if _dentro(c["ts"], f)]
            faixas[h] = (ix[0], ix[-1]) if ix else None
            bh[h] = (1e4 * (cndl[ix[-1]]["close"] / cndl[ix[0]]["close"] - 1)
                     if len(ix) >= 2 and cndl[ix[0]]["close"] > 0 else None)
        meta[sym] = {"ult": len(cndl) - 1, "na_vwma": na, "faixas": faixas,
                     "bh": bh, "n": len(cndl), "first": cndl[0]["ts"]}
        del cndl
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} "
                              f"apos {sym} (passada A); PARANDO.")
    return {"G": G, "fatia": fatia, "meta": meta, "sem_candles": sem,
            "rss": rss}


def _ocupa(i: int, hb: int, faixa) -> int:
    """Barras i+1..i+hb dentro de [a, b] (tempo posicionado, hold BRUTO)."""
    if faixa is None or hb <= 0:
        return 0
    return max(0, min(i + hb, faixa[1]) - max(i + 1, faixa[0]) + 1)


def mede(conn, tf: str, A: dict, configs: list) -> dict:
    """PASSADA B. `configs` = [(celula, semente|None, vetos:set de indices
    globais de G)]. Celulas principais guardam trade a trade; placebos so
    agregados por metade. O caminho e REFEITO por config (veto muda hold)."""
    from backtest.excursion import measure_event
    G, meta = A["G"], A["meta"]
    trades = {c: [] for c, s, _v in configs if s is None}
    agg = {(c, s, h): {"n": 0, "ret6": 0.0, "n_long": 0, "ret_long": 0.0,
                       "n_trunc": 0}
           for c, s, _v in configs if s is not None for h, _fx in METADES}
    cont = {(c, s): {"excl_borda": 0, "excl_hold": 0, "trunc": 0, "fim": 0,
                     "entradas": 0} for c, s, _v in configs}
    tempo = {(c, h): [0, 0] for c, s, _v in configs if s is None
             for h, _fx in METADES}
    rss = _rss_mib()
    for sym, (a, b) in A["fatia"].items():
        if a == b:
            continue
        cruz = [(G[j]["i"], G[j]["long"]) for j in range(a, b)]
        ult, fx = meta[sym]["ult"], meta[sym]["faixas"]
        cache = {}

        def ret_de(k, hold):
            if k not in cache:
                g = G[a + k]
                m = measure_event(conn, sym, g["ts"],
                                  "LONG" if g["long"] else "SHORT", tf=tf,
                                  path_cap=EXT_CAP)
                cache[k] = None if m is None else (
                    m["atr"] / m["entry"], array("d", m["fwd_bar"]))
            c = cache[k]
            if c is None:
                return "borda"
            if hold > len(c[1]):
                return "hold"
            return trail_exit(None, None, None, c[1], hold, 0, 0) \
                * c[0] * 1e4 - COST_BPS

        for cel, sem, vet in configs:
            lib = [(a + k) not in vet for k in range(b - a)]
            ct = cont[(cel, sem)]
            for k, hb, fim in caminho(cruz, lib, ult):
                i, lg = cruz[k]
                ct["entradas"] += 1
                if sem is None:
                    for h, _fx in METADES:
                        tempo[(cel, h)][0 if lg else 1] += _ocupa(i, hb, fx[h])
                hold, tr = min(max(hb, 1), EXT_CAP), hb > EXT_CAP
                r = ret_de(k, hold)
                if r == "borda" or r == "hold":
                    ct["excl_" + r] += 1
                    continue
                ct["trunc"] += tr
                ct["fim"] += fim
                g = G[a + k]
                if sem is None:
                    trades[cel].append((g["ts"], sym, lg, [r], 0, tr, fim,
                                        hb, a + k))
                else:
                    h = _metade(g["ts"])
                    if h == "fora":
                        continue
                    x = agg[(cel, sem, h)]
                    x["n"] += 1
                    x["ret6"] += r
                    x["n_long"] += lg
                    x["ret_long"] += r if lg else 0.0
                    x["n_trunc"] += tr
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} "
                              f"apos {sym} (passada B); PARANDO.")
    for c in trades:
        trades[c].sort(key=lambda t: (t[0], t[1]))
    return {"trades": trades, "agg": agg, "cont": cont, "tempo": tempo,
            "rss": rss}


# ------------------------------------------------------------ saida ---------
class _Log:
    def __init__(self, path: Path):
        self.fh = path.open("w")

    def __call__(self, *a):
        s = " ".join(str(x) for x in a)
        print(s)
        self.fh.write(s + "\n")
        self.fh.flush()


CAMPOS = ["celula", "semente", "bar_ts_iso", "mes", "metade", "sym", "direcao",
          "hold", "truncado", "marcado_fim", "ret6", "adx", "mfi", "soma_knn",
          "memoria_fria", "n"]


def _f(x, fmt="+.1f"):
    return "NA" if x is None else format(x, fmt)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="VWMA 13/19 + kNN (etapa 5)")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=TETO_ISO)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None)
    ap.add_argument("--out", default=None, help="CSV; o log vai ao lado (.log)")
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    args = ap.parse_args(argv)
    if args.end > TETO_ISO:                                      # T1 camada 1
        raise SystemExit(f"[vwma_knn] T1: --end {args.end} passa do teto "
                         f"{TETO_ISO}. Hold-out trancado. ABORTADO.")
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"vwma_knn_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    log = _Log(out.with_suffix(".log"))
    teto_ms = _ms(args.end)
    t0 = time.monotonic()
    conn = conecta_com_teto(args.store, teto_ms)                 # T1 camada 2
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[vwma_knn] T1 VIOLADO. ABORTADO.")
        log(f"[vwma_knn] teto {args.end} (exclusivo) | maior ts visivel "
            f"{_iso(mx) if mx else '-'} | tf {args.tf} | simbolos "
            f"{len(symbols)} | VWMA {N_RAPIDA}/{N_LENTA} open | k {K_VIZ}")
        # ---- faltas ESPERADAS, declaradas ANTES de contar (fonte: SQL) -----
        vol0 = {s: conn.execute(
            "SELECT COUNT(*) FROM candles WHERE symbol=? AND timeframe=? AND "
            "volume <= 0", (s, args.tf)).fetchone()[0] for s in symbols}
        esp_vwma = 0 if not any(vol0.values()) else None
        log("\n===== FALTAS ESPERADAS (declaradas antes da contagem) =====")
        log(f"  velas com volume <= 0 no store: {sum(vol0.values())} "
            f"{ {s: v for s, v in vol0.items() if v} or ''}")
        log(f"  barras VWMA NA esperadas: "
            f"{esp_vwma if esp_vwma is not None else 'nao determinado (ha vol<=0)'}")
        log(f"  features NA nos cruzamentos esperadas: 0 (cruzamento exige "
            f"t >= {N_LENTA}; ADX(13,6) definido desde a barra 18 e MFI(13) "
            f"desde a 13, salvo barras sem amplitude)")
        A = coleta(conn, symbols, args.tf, teto_ms - 1)
        t_a = time.monotonic() - t0
        G = A["G"]
        # ---- decisoes ---------------------------------------------------
        est = {"C0": [("libera", False, 0)] * len(G), "K_agr": decisoes(G),
               "K_tok": [None] * len(G)}
        for sym, (a, b) in A["fatia"].items():
            est["K_tok"][a:b] = decisoes(G[a:b])
        configs = [("C0", None, set())]
        for c in ("K_tok", "K_agr"):
            configs.append((c, None, {j for j, e in enumerate(est[c])
                                      if e[0] == "veta"}))
        for c in ("K_tok", "K_agr"):
            for s in SEMENTES:
                configs.append((c, s, placebo(G, est[c], s)))
        B = mede(conn, args.tf, A, configs)
    finally:
        conn.close()
    t_b = time.monotonic() - t0 - t_a

    # ---- 1. CONTAGENS ---------------------------------------------------
    meta = A["meta"]
    log("\n===== 1. CONTAGENS =====")
    log(f"  sem candles: {A['sem_candles'] or 'nenhum'} | primeiro candle: "
        + (f"min {_iso(min(m['first'] for m in meta.values()))} | max "
           f"{_iso(max(m['first'] for m in meta.values()))}" if meta else "-"))
    na_v = {s: m["na_vwma"] for s, m in meta.items() if m["na_vwma"]}
    na_f = {}
    for g in G:
        if g["adx"] is None or g["mfi"] is None:
            na_f[g["sym"]] = na_f.get(g["sym"], 0) + 1
    log(f"  barras VWMA NA observadas: {sum(na_v.values())} {na_v or ''}")
    log(f"  features NA nos cruzamentos observadas: {sum(na_f.values())} "
        f"{na_f or ''}")
    alerta = []
    if esp_vwma is not None and sum(na_v.values()) != esp_vwma:
        alerta.append("VWMA NA")
    if sum(na_f.values()) != 0:
        alerta.append("features NA")
    log("  conferencia esperado x observado: " + (
        f"ALERTA — diferenca em {alerta}: e BUG, nao resultado"
        if alerta else "bate"))
    n_up = sum(1 for g in G if g["long"])
    log(f"  cruzamentos: {len(G)} (para cima {n_up}, para baixo "
        f"{len(G) - n_up})")
    for c in CELULAS:
        e = est[c]
        vl = sum(1 for g, x in zip(G, e) if g["long"] and x[0] == "veta")
        vs = sum(1 for g, x in zip(G, e) if not g["long"] and x[0] == "veta")
        fr = sum(1 for x in e if x[1])
        ct = B["cont"][(c, None)]
        log(f"  {c:>5}: vetados LONG {vl}/{n_up} | SHORT {vs}/{len(G) - n_up}"
            f" | memoria fria (<{K_VIZ}) {fr if c != 'C0' else '-'} | "
            f"entradas {ct['entradas']} | excl_borda {ct['excl_borda']} | "
            f"excl_hold {ct['excl_hold']} | truncados 256 {ct['trunc']} | "
            f"marcados no fim {ct['fim']}")
    for c in ("K_tok", "K_agr"):
        tr = [B["cont"][(c, s)]["trunc"] for s in SEMENTES]
        log(f"  placebo {c}: truncados 256 por semente min {min(tr)} | "
            f"mediana {median(tr):.0f} | max {max(tr)}")
    log(f"  RSS de pico {B['rss']:.0f} MiB (teto {RSS_CEILING_MIB:.0f}) | "
        f"tempo passada A {t_a:.0f}s, passada B {t_b:.0f}s")

    # ---- CSV ------------------------------------------------------------
    rows = []
    for c in CELULAS:
        for t in B["trades"][c]:
            j = t[8]
            e = est[c][j] if c != "C0" else None
            rows.append({
                "celula": c, "semente": "", "bar_ts_iso": _iso(t[0]),
                "mes": _iso(t[0])[:7], "metade": _metade(t[0]), "sym": t[1],
                "direcao": "LONG" if t[2] else "SHORT", "hold": min(
                    max(t[7], 1), EXT_CAP), "truncado": int(t[5]),
                "marcado_fim": int(t[6]), "ret6": f"{t[3][0]:.2f}",
                "adx": _f(G[j]["adx"], ".3f"), "mfi": _f(G[j]["mfi"], ".3f"),
                "soma_knn": "" if e is None else e[2],
                "memoria_fria": "" if e is None else int(e[1]), "n": ""})
    for (c, s, h), x in sorted(B["agg"].items()):
        rows.append({k: "" for k in CAMPOS} | {
            "celula": f"placebo_{c}", "semente": s, "metade": h,
            "ret6": f"{x['ret6']:.2f}", "n": x["n"]})
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        w.writerows(rows)

    # ---- 2. RESULTADO POR CELULA E METADE --------------------------------
    tok_barras = {h: sum(f[h][1] - f[h][0] + 1 for f in
                         (m["faixas"] for m in meta.values()) if f[h])
                  for h, _fx in METADES}
    log(f"\n===== 2. RESULTADO POR CELULA — H1 {H1[0]}..{H1[1]} | H2 "
        f"{H2[0]}..{H2[1]} | custo 6 bps (10 = 6 - 4n) =====")
    log(f"{'cel':>5} H  | {'n':>5} | {'ret@6':>9} | {'bpt@6':>7} | "
        f"{'bpt@10':>7} | {'venc':>5} | {'nL':>4} {'retL':>9} | {'nS':>4} "
        f"{'retS':>9} | {'sem melhor mes':>23} | {'dd':>8} | {'%comp':>5} "
        f"{'%vend':>5} | top3")
    res = {}
    for c in CELULAS:
        for h, f in METADES:
            x = resumo(B["trades"][c], 0, f)
            sel = [t[3][0] for t in B["trades"][c] if _dentro(t[0], f)]
            x["venc"] = 100.0 * sum(1 for r in sel if r > 0) / len(sel) \
                if sel else 0.0
            x["conc"] = concentracao(sel)
            res[(c, h)] = x
            tl, ts_ = B["tempo"][(c, h)]
            den = tok_barras[h] or 1
            log(f"{c:>5} {h} | {x['n']:>5} | {x['ret6']:>+9.0f} | "
                f"{x['bpt6']:>+7.1f} | {x['bpt10']:>+7.1f} | "
                f"{x['venc']:>4.1f}% | {x['n_long']:>4} {x['ret_long']:>+9.0f} "
                f"| {x['n_short']:>4} {x['ret_short']:>+9.0f} | "
                f"{x['ret_sem_melhor_mes']:>+9.0f} ({x['melhor_mes']:>7}) | "
                f"{x['dd']:>8.0f} | {100 * tl / den:>5.1f} {100 * ts_ / den:>5.1f}"
                f" | {x['top3']}")

    # ---- 3. CONCENTRACAO ------------------------------------------------
    log(f"\n===== 3. CONCENTRACAO — {TOP_FRAC:.0%} melhores trades =====")
    for c in CELULAS:
        for h, _fx in METADES:
            x = res[(c, h)]
            nt, fat, sem_top = x["conc"]
            log(f"  {c:>5} {h}: n {x['n']:>5} | top {nt:>3} trades | fatia do "
                f"resultado {_f(fat, '.0f') + '%' if fat is not None else 'NA (total <= 0)'} | resultado sem eles "
                f"{sem_top:>+9.0f} ({sem_top / max(1, x['n'] - nt):>+.1f} "
                f"bps/trade)")

    # ---- 4. PLACEBO -----------------------------------------------------
    log("\n===== 4. kNN CONTRA O PLACEBO (ret@6, 20 sementes, caminho refeito) "
        "=====")
    for c in ("K_tok", "K_agr"):
        for h, _fx in METADES:
            v = [B["agg"][(c, s, h)]["ret6"] for s in SEMENTES]
            nn = [B["agg"][(c, s, h)]["n"] for s in SEMENTES]
            kn = res[(c, h)]["ret6"]
            ab = sum(1 for y in v if y < kn)
            log(f"  {c:>5} {h}: kNN {kn:>+9.0f} (n {res[(c, h)]['n']}) | "
                f"placebo n med {median(nn):.0f} | P10 {_pct(v, .1):>+8.0f} | "
                f"med {median(v):>+8.0f} | P90 {_pct(v, .9):>+8.0f} | kNN acima "
                f"de {ab}/20 placebos (percentil ~{100 * ab / 20:.0f})")
    log("  nota: o placebo iguala a CONTAGEM de cruzamentos vetados, nao a de "
        "trades; vetos aleatorios quebram sequencias e costumam gerar mais "
        "reversoes (compare n do kNN com o n mediano do placebo)")

    # ---- 5. CONTROLE PASSIVO ------------------------------------------
    log("\n===== 5. CONTROLE PASSIVO — comprar e segurar a cesta, pesos iguais,"
        " sem custo =====")
    for h, _fx in METADES:
        vs = [m["bh"][h] for m in meta.values() if m["bh"][h] is not None]
        log(f"  {h}: media {sum(vs) / len(vs) if vs else 0:>+8.0f} bps por "
            f"token | tokens com dado {len(vs)}/{len(meta)}")

    log(f"\n  CSV: {out}")
    log("caveat: EXPLORATORIO (camada 2), nada promove; defaults do autor "
        f"intactos. Teto T1 {TETO_ISO}. Entrada no close do cruzamento (o "
        "original usa o open seguinte). Memoria vazia/feature NA = veto. "
        "Placebo refaz o caminho (veto muda hold). ret por trade em bps de "
        "6 de custo, mergulho sobre a curva de trades por entrada (nao marca a "
        "mercado nem considera simultaneos). Hold > 256 truncado; ultimo trade "
        "de cada token marcado no ultimo close antes do teto. % do tempo usa "
        "hold bruto sobre barras-token da metade. Controle passivo sem custo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
