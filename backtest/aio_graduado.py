# backtest/aio_graduado.py — Setup Atirador (SANDBOX, camada 2) — AIO graduado
# Briefing de 16/11/2026. Exploratorio: nada aqui promove nada.
#
# PERGUNTA: o sinal do "All-in-One Toolkit: Graded Signals & Money Flow
# [TradeIQ]" (Pine v6, MPL 2.0, lido em 16/11/2026) ganha dinheiro nas duas
# metades em 4h sobre os 65 tokens? Cada filtro da graduacao (volume, ribbon,
# diario) bate um veto aleatorio com a mesma taxa?
#
# REGRAS (defaults do autor, NAO mexer nesta rodada: 3,0/10/1,2/20/13/34/50/2,0)
#   Linha: perda[t] = SENS x ATR10[t]; ant = linha[t-1] (0 antes da 1a barra
#     com ATR). Ambos closes acima de ant -> max(ant, close-perda); ambos abaixo
#     -> min(ant, close+perda); senao close-perda se close > ant, close+perda
#     caso contrario. Sem ATR -> linha None.
#   Virada: compra se close[t] > linha[t] e close[t-1] <= linha[t-1]; venda o
#     simetrico. linha[t-1] None -> nada (crossover do Pine com na = falso).
#   Volume: vol_rel(20)[t] >= 1,2 (media das 20 barras ANTERIORES,
#     `kis_meta.vol_rel`). None = nao passa, contado.
#   Ribbon: +1 se EMA13 > EMA34, senao -1; passa se e a direcao da virada.
#     EMA34 ainda None (t < 33) = nao passa, contado (no Pine daria -1).
#   Diario: `kis_tf.agregar(c4h, 6)` (dia incompleto descartado), EMA50 sobre o
#     fechamento diario. Barra de 4h aberta no dia D le D-1, inclusive a ultima
#     barra do dia. D-1 inexistente ou EMA50 None = nao passa (veto, contado).
#     A EMA corre sobre os dias COMPLETOS em sequencia; um dia descartado no
#     meio e pulado (no Pine o dia existiria). Contado nas faltas.
#   Grau: 0 se volume ou ribbon falha; 2 se ambos passam e o diario passa;
#     1 nos demais. A faixa cinza do ribbon e so cor, nao entra.
#   Entrada: close da barra do sinal (convencao do motor).
#     DESVIO CONHECIDO: o autor entra no open da barra seguinte.
#   Saida: na proxima virada contraria BRUTA, de qualquer grau, no close dela.
#     hold = distancia em barras ate ela; sem ela, ate a ultima barra visivel
#     (trade marcado no fim). hold > EXT_CAP (256) truncado e contado.
#     SUPOSICAO: o video so diz "fecha ou reverte no sinal oposto"; a ancora
#     do log (BTC 05-07/11/2024) testa essa leitura.
#   Stop: S0 sem stop; S2 stop fixo 2 x ATR14 da barra do sinal
#     (`kis_saida.stop_exit(..., k=2, movel=False)`, fill min(stop, open)).
#
# ATR: a linha usa `kis_regime._atr_series(candles, 10)` (Wilder, semente em
#   i=10 = media de TR[1..10]); o Pine semeia em i=9 com TR[0]=h-l. Diferenca
#   some em poucas dezenas de barras (decaimento de Wilder). O stop e a regua
#   de bps usam o ATR14 de `excursion._wilder_atr`, como todo o motor.
#
# CELULAS: R0 (toda virada, controle), V (volume passa), F (ribbon passa),
#   G1 (grau >= 1, default do indicador), G2 (grau = 2, "Strong").
# PLACEBO (V, F, G1, G2; 20 sementes = kis_knn.SEMENTES): retira ao acaso, POR
#   LADO, a mesma quantidade de viradas que a celula vetou (`kis_knn.placebo`
#   sobre todas as viradas do universo). Aqui cada trade vai de uma virada a
#   seguinte contraria BRUTA e o veto NAO muda o hold dos outros; logo basta
#   retirar trades da lista do R0 (garantido pelo teste 4). Diferente da etapa
#   5 (vwma_knn), onde o veto mudava o caminho.
#
# MEDICAO: em memoria, `kis_tf.medir` (= measure_event: entry close, ATR14 de
#   Wilder em 30 velas, borda de 48 barras, caminho ate 256); retorno = saida em
#   ATR x atr/entry x 1e4 - COST_BPS (6). 10 bps = 6 - 4n. Metades por ENTRADA.
# VM 956 MiB: velas de UM simbolo por vez; teto de RSS do kis_trail checado a
#   cada simbolo. T1: --end <= 2026-08-26 e TEMP VIEW (`conecta_com_teto`).
# Uso (VM): python -m backtest.aio_graduado --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP, _ema                    # noqa: E402
from backtest.kis_knn import SEMENTES, _pct, placebo               # noqa: E402
from backtest.kis_meta import vol_rel                              # noqa: E402
from backtest.kis_regime import _atr_series                        # noqa: E402
from backtest.kis_saida import (H1, H2, TETO_ISO, _guarda_relogio,  # noqa: E402
                                _iso, _ms, conecta_com_teto, resumo,
                                stop_exit)
from backtest.kis_tf import agregar, medir                         # noqa: E402
from backtest.kis_trail import (COST_BPS, RSS_CEILING_MIB, _dentro,  # noqa: E402
                                _rss_mib, trail_exit)
from backtest.vwma_knn import concentracao                         # noqa: E402

SENS, ATR_N = 3.0, 10
VOL_JAN, VOL_MIN = 20, 1.2
RIB_RAP, RIB_LEN = 13, 34
HTF_EMA = 50
STOP_K = 2.0
BAR_4H_MS = 14_400_000
DIA_MS = 86_400_000
DIA_LIDO = 1                 # a barra do dia D le o dia D - DIA_LIDO (mutacao: 0)
CELULAS = ("R0", "V", "F", "G1", "G2")
FILTRADAS = CELULAS[1:]
STOPS = (("S0", 0), ("S2", 1))
METADES = (("H1", H1), ("H2", H2))
VIDEO = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
ANCORA = ("BTCUSDT", "2024-11-05", "2024-11-07")


# ------------------------------------------------------------ puras ---------
def linha_trailing(closes: list, perdas: list) -> list:
    """Linha de trailing do indicador. perdas[t] None -> linha[t] None."""
    out = [None] * len(closes)
    for t, (c, p) in enumerate(zip(closes, perdas)):
        if p is None:
            continue
        ant = out[t - 1] if t > 0 and out[t - 1] is not None else 0.0
        c0 = closes[t - 1] if t > 0 else None
        if c0 is not None and c > ant and c0 > ant:
            out[t] = max(ant, c - p)
        elif c0 is not None and c < ant and c0 < ant:
            out[t] = min(ant, c + p)
        elif c > ant:
            out[t] = c - p
        else:
            out[t] = c + p
    return out


def viradas(closes: list, linha: list) -> list:
    """[(t, is_long)] — cruzamento do close sobre a linha."""
    out = []
    for t in range(1, len(closes)):
        a, a0 = linha[t], linha[t - 1]
        if a is None or a0 is None:
            continue
        if closes[t] > a and closes[t - 1] <= a0:
            out.append((t, True))
        elif closes[t] < a and closes[t - 1] >= a0:
            out.append((t, False))
    return out


def ribbon(closes: list) -> list:
    """+1 / -1 por barra; None enquanto EMA34 nao existe."""
    ra, rl = _ema(closes, RIB_RAP), _ema(closes, RIB_LEN)
    return [None if a is None or b is None else (1 if a > b else -1)
            for a, b in zip(ra, rl)]


def htf_dirs(cndl: list) -> list:
    """Direcao diaria por barra de 4h: dia D le D - DIA_LIDO. None = veto."""
    dias = agregar(cndl, 6)
    ema = _ema([d["close"] for d in dias], HTF_EMA)
    mapa = {d["ts"]: (None if e is None else (1 if d["close"] > e else -1))
            for d, e in zip(dias, ema)}
    return [mapa.get(c["ts"] - c["ts"] % DIA_MS - DIA_LIDO * DIA_MS)
            for c in cndl]


def grau(vol_ok: bool, rib_ok: bool, htf_ok: bool) -> int:
    if not (vol_ok and rib_ok):
        return 0
    return 2 if htf_ok else 1


def serie_sinal(cndl: list, sens: float = SENS) -> dict:
    """Tudo POR BARRA (causal) e as viradas com features. Cada virada:
    {i, ts, long, vol_rel, rib, htf, vol_ok, rib_ok, htf_ok, grau}."""
    closes = [c["close"] for c in cndl]
    atr10 = _atr_series(cndl, ATR_N)
    lin = linha_trailing(closes, [None if a is None else sens * a
                                  for a in atr10])
    vr = vol_rel([c["volume"] for c in cndl], VOL_JAN)
    rib, htf = ribbon(closes), htf_dirs(cndl)
    vs = []
    for t, lg in viradas(closes, lin):
        d = 1 if lg else -1
        vo = vr[t] is not None and vr[t] >= VOL_MIN
        ro = rib[t] == d
        ho = htf[t] == d
        vs.append({"i": t, "ts": cndl[t]["ts"], "long": lg, "vol_rel": vr[t],
                   "rib": rib[t], "htf": htf[t], "vol_ok": vo, "rib_ok": ro,
                   "htf_ok": ho, "grau": grau(vo, ro, ho)})
    return {"linha": lin, "atr10": atr10, "vol_rel": vr, "rib": rib,
            "htf": htf, "viradas": vs}


def passa(cel: str, v: dict) -> bool:
    return {"R0": True, "V": v["vol_ok"], "F": v["rib_ok"],
            "G1": v["grau"] >= 1, "G2": v["grau"] == 2}[cel]


def holds(vs: list, ult: int) -> list:
    """[(hold_bruto, marcado_fim)] paralelo a `vs`: ate a proxima virada
    contraria BRUTA (de qualquer grau); sem ela, ate `ult`."""
    out, prox = [None] * len(vs), {True: None, False: None}
    for k in range(len(vs) - 1, -1, -1):
        v = vs[k]
        j = prox[not v["long"]]
        out[k] = (ult - v["i"], True) if j is None else (j - v["i"], False)
        prox[v["long"]] = v["i"]
    return out


def trades_simbolo(cndl: list, sym: str, cont: dict, celula: str = "R0",
                   sinal: dict = None) -> list:
    """Trades da `celula` num simbolo. Tupla no formato do kis_saida.resumo:
    (ts, sym, is_long, [ret_s0, ret_s2], bits, truncado, marcado_fim, extra).
    O hold NAO depende da celula (saida na virada contraria bruta)."""
    sg = sinal or serie_sinal(cndl)
    vs = sg["viradas"]
    out = []
    for v, (hb, fim) in zip(vs, holds(vs, len(cndl) - 1)):
        if not passa(celula, v):
            continue
        m = medir(cndl, v["i"], "LONG" if v["long"] else "SHORT", BAR_4H_MS)
        if m is None:
            cont["excl_borda"] += 1
            continue
        hold, tr = min(max(hb, 1), EXT_CAP), hb > EXT_CAP
        if hold > len(m["fwd_bar"]):
            cont["excl_hold"] += 1
            continue
        ap = m["atr"] / m["entry"]
        s0 = trail_exit(None, None, None, m["fwd_bar"], hold, 0, 0)
        s2, parou = stop_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                              m["fwd_bar"], hold, STOP_K, False)
        sai = cndl[v["i"] + hold]
        out.append((v["ts"], sym, v["long"],
                    [s0 * ap * 1e4 - COST_BPS, s2 * ap * 1e4 - COST_BPS],
                    2 if parou else 0, tr, fim,
                    {"hold": hold, "hold_bruto": hb, "virada": v,
                     "entry": m["entry"], "atr14": m["atr"],
                     "atr10": sg["atr10"][v["i"]], "saida_ts": sai["ts"],
                     "saida_px": sai["close"]}))
    return out


def filtra(trades: list, celula: str) -> list:
    return [t for t in trades if passa(celula, t[7]["virada"])]


def vetos_placebo(G: list, celula: str, semente: int) -> set:
    """Indices globais de virada retirados pelo placebo da celula."""
    est = [("libera",) if passa(celula, v) else ("veta",) for v in G]
    return placebo(G, est, semente)


def _metade(ts: int) -> str:
    return next((h for h, f in METADES if _dentro(ts, f)), "fora")


def bh_metades(cndl: list) -> dict:
    """Comprar e segurar por metade (mesma conta do vwma_knn.coleta)."""
    bh = {}
    for h, f in METADES:
        ix = [j for j, c in enumerate(cndl) if _dentro(c["ts"], f)]
        bh[h] = (1e4 * (cndl[ix[-1]]["close"] / cndl[ix[0]]["close"] - 1)
                 if len(ix) >= 2 and cndl[ix[0]]["close"] > 0 else None)
    return bh


# ------------------------------------------------------------ dados ---------
def faltas_esperadas(conn, symbols: list, tf: str) -> dict:
    """Declaradas ANTES de contar, por SQL (caminho independente do agregar):
    {sym: {"dias_ok": dias D (em ms) cuja D-1 tem diario com EMA50,
    "vol0": velas com volume <= 0}}."""
    out = {}
    for s in symbols:
        rows = conn.execute(
            "SELECT ts / ? AS d, COUNT(*) FROM candles WHERE symbol=? AND "
            "timeframe=? GROUP BY d ORDER BY d", (DIA_MS, s, tf)).fetchall()
        compl = [d for d, n in rows if n == 6]
        vol0 = conn.execute(
            "SELECT COUNT(*) FROM candles WHERE symbol=? AND timeframe=? AND "
            "volume <= 0", (s, tf)).fetchone()[0]
        out[s] = {"dias_ok": {(d + 1) * DIA_MS for d in compl[HTF_EMA - 1:]},
                  "vol0": vol0, "dias_incompletos": len(rows) - len(compl)}
    return out


def coleta(conn, symbols: list, tf: str, end_ms: int, esp: dict) -> dict:
    """Uma passada por simbolo: sinal, trades R0, B&H, conferencias."""
    from backtest.candle_store import read_candles
    G, trades, meta, sem = [], [], {}, []
    cont = {"excl_borda": 0, "excl_hold": 0}
    conf = {"htf_div": {}, "vol_div": {}, "vol_none": {}, "htf_none": {},
            "rib_none": {}}
    rss = _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=None, end_ms=end_ms)
        if not cndl:
            sem.append(sym)
            continue
        sg = serie_sinal(cndl)
        base = len(G)
        for k, v in enumerate(sg["viradas"]):
            v["g"], v["sym"] = base + k, sym
            G.append(v)
            dia = v["ts"] - v["ts"] % DIA_MS
            if v["vol_rel"] is None:
                conf["vol_none"][sym] = conf["vol_none"].get(sym, 0) + 1
            if v["htf"] is None:
                conf["htf_none"][sym] = conf["htf_none"].get(sym, 0) + 1
            if v["rib"] is None:
                conf["rib_none"][sym] = conf["rib_none"].get(sym, 0) + 1
            # esperado x observado, virada a virada
            if (v["htf"] is None) != (dia not in esp[sym]["dias_ok"]):
                conf["htf_div"][sym] = conf["htf_div"].get(sym, 0) + 1
            if esp[sym]["vol0"] == 0 and \
                    (v["vol_rel"] is None) != (v["i"] < VOL_JAN):
                conf["vol_div"][sym] = conf["vol_div"].get(sym, 0) + 1
        trades.extend(trades_simbolo(cndl, sym, cont, "R0", sg))
        anos = {}
        for v in sg["viradas"]:
            y = _iso(v["ts"])[:4]
            anos[y] = anos.get(y, 0) + 1
        meta[sym] = {"n": len(cndl), "first": cndl[0]["ts"],
                     "last": cndl[-1]["ts"], "bh": bh_metades(cndl),
                     "viradas": len(sg["viradas"]), "anos": anos,
                     "vir_metade": {h: sum(1 for v in sg["viradas"]
                                           if _dentro(v["ts"], f))
                                    for h, f in METADES}}
        del cndl, sg
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} "
                              f"apos {sym}; PARANDO.")
    trades.sort(key=lambda t: (t[0], t[1]))
    return {"G": G, "trades": trades, "meta": meta, "sem_candles": sem,
            "cont": cont, "conf": conf, "rss": rss}


def agrega_placebo(trades: list, vet: set) -> dict:
    """{(stop, metade): [n, ret6]} dos trades do R0 fora de `vet`."""
    out = {(s, h): [0, 0.0] for s, _k in STOPS for h, _f in METADES}
    for t in trades:
        if t[7]["virada"]["g"] in vet:
            continue
        h = _metade(t[0])
        if h == "fora":
            continue
        for s, k in STOPS:
            x = out[(s, h)]
            x[0] += 1
            x[1] += t[3][k]
    return out


# ------------------------------------------------------------ saida ---------
class _Log:
    def __init__(self, path: Path):
        self.fh = path.open("w")

    def __call__(self, *a):
        s = " ".join(str(x) for x in a)
        print(s)
        self.fh.write(s + "\n")
        self.fh.flush()


CAMPOS = ["bar_ts_iso", "mes", "metade", "sym", "direcao", "hold", "truncado",
          "marcado_fim", "ret_s0", "ret_s2", "parou_s2", "vol_rel", "rib_dir",
          "htf_dir", "grau"]
CAMPOS_PLACEBO = ["celula", "semente", "stop", "metade", "n", "ret6"]


def _f(x, fmt="+.1f"):
    return "NA" if x is None else format(x, fmt)


def _tabela(log, trades: list, titulo: str) -> dict:
    log(f"\n===== {titulo} — H1 {H1[0]}..{H1[1]} | H2 {H2[0]}..{H2[1]} | "
        f"custo 6 bps (10 = 6 - 4n) =====")
    log(f"{'cel':>3} {'st':>2} H  | {'n':>5} | {'ret@6':>8} | {'bpt@6':>7} | "
        f"{'bpt@10':>7} | {'venc':>5} | {'nL':>4} {'retL':>8} | {'nS':>4} "
        f"{'retS':>8} | {'sem melhor mes':>22} | {'dd':>7} | {'stop':>4} | top3")
    res = {}
    for c in CELULAS:
        tc = filtra(trades, c)
        for s, k in STOPS:
            for h, f in METADES:
                x = resumo(tc, k, f)
                sel = [t[3][k] for t in tc if _dentro(t[0], f)]
                x["venc"] = 100.0 * sum(1 for r in sel if r > 0) / len(sel) \
                    if sel else 0.0
                x["conc"] = concentracao(sel)
                res[(c, s, h)] = x
                log(f"{c:>3} {s} {h} | {x['n']:>5} | {x['ret6']:>+8.0f} | "
                    f"{x['bpt6']:>+7.1f} | {x['bpt10']:>+7.1f} | "
                    f"{x['venc']:>4.1f}% | {x['n_long']:>4} "
                    f"{x['ret_long']:>+8.0f} | {x['n_short']:>4} "
                    f"{x['ret_short']:>+8.0f} | {x['ret_sem_melhor_mes']:>+8.0f}"
                    f" ({x['melhor_mes']:>7}) | {x['dd']:>7.0f} | "
                    f"{x['n_stop'] if k else '-':>4} | {x['top3']}")
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="AIO graduado (sandbox)")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=TETO_ISO)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None)
    ap.add_argument("--out", default=None, help="CSV; o log vai ao lado (.log)")
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    args = ap.parse_args(argv)
    if args.tf != "4h":
        raise SystemExit("[aio_graduado] so 4h nesta rodada (diario = 6 barras)")
    if args.end > TETO_ISO:                                      # T1 camada 1
        raise SystemExit(f"[aio_graduado] T1: --end {args.end} passa do teto "
                         f"{TETO_ISO}. Hold-out trancado. ABORTADO.")
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    out = Path(args.out) if args.out else (
        ROOT / "logs" /
        f"aio_graduado_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    log = _Log(out.with_suffix(".log"))
    teto_ms = _ms(args.end)
    t0 = time.monotonic()
    conn = conecta_com_teto(args.store, teto_ms)                 # T1 camada 2
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[aio_graduado] T1 VIOLADO. ABORTADO.")
        log(f"[aio_graduado] teto {args.end} (exclusivo) | maior ts visivel "
            f"{_iso(mx) if mx else '-'} | tf {args.tf} | simbolos "
            f"{len(symbols)} | linha {SENS}xATR{ATR_N} | vol {VOL_MIN}x/"
            f"{VOL_JAN} | ribbon EMA{RIB_RAP}/{RIB_LEN} | diario EMA{HTF_EMA} "
            f"(D-{DIA_LIDO}) | stop S2 {STOP_K}xATR14")
        # ---- faltas ESPERADAS, declaradas ANTES de contar (fonte: SQL) -----
        esp = faltas_esperadas(conn, symbols, args.tf)
        log("\n===== FALTAS ESPERADAS (declaradas antes da contagem) =====")
        v0 = {s: e["vol0"] for s, e in esp.items() if e["vol0"]}
        di = {s: e["dias_incompletos"] for s, e in esp.items()
              if e["dias_incompletos"]}
        log(f"  volume: None so em viradas com i < {VOL_JAN} (aquecimento); "
            f"velas com volume <= 0 no store: {sum(v0.values())} {v0 or ''}"
            + (" -> nesses tokens a regra extra nao e conferida" if v0 else ""))
        log(f"  diario: None em toda virada cujo dia D nao tem D-1 completo com "
            f"EMA{HTF_EMA} (por SQL: dias com 6 velas de 4h, a partir do "
            f"{HTF_EMA}o). Dias incompletos no store: {sum(di.values())} "
            f"{di or ''}")
        log(f"  ribbon: None so em viradas com i < {RIB_LEN - 1} (EMA34 sem "
            f"semente); 1a linha possivel em i = {ATR_N}")
        A = coleta(conn, symbols, args.tf, teto_ms - 1, esp)
    finally:
        conn.close()
    t_a = time.monotonic() - t0
    G, R0, meta, conf = A["G"], A["trades"], A["meta"], A["conf"]

    # ---- placebos ------------------------------------------------------
    plac = {}
    for c in FILTRADAS:
        for s in SEMENTES:
            plac[(c, s)] = agrega_placebo(R0, vetos_placebo(G, c, s))
    t_b = time.monotonic() - t0 - t_a

    # ---- 1. CONTAGENS ----------------------------------------------------
    log("\n===== 1. CONTAGENS =====")
    log(f"  sem candles: {A['sem_candles'] or 'nenhum'} | primeiro candle: "
        + (f"min {_iso(min(m['first'] for m in meta.values()))} | max "
           f"{_iso(max(m['first'] for m in meta.values()))}" if meta else "-"))
    for nome, ch in (("volume", "vol_none"), ("diario", "htf_none"),
                     ("ribbon", "rib_none")):
        d = conf[ch]
        log(f"  faltas {nome} observadas (viradas): {sum(d.values())} "
            f"{dict(sorted(d.items())) or ''}")
    alerta = {"diario": conf["htf_div"], "volume": conf["vol_div"],
              "ribbon": sum(1 for v in G
                            if (v["rib"] is None) != (v["i"] < RIB_LEN - 1))}
    alerta = {k: d for k, d in alerta.items() if d}
    log("  conferencia esperado x observado (virada a virada): " + (
        f"ALERTA — divergencia {alerta}: e BUG, nao resultado"
        if alerta else "bate (diario, volume, ribbon)"))
    nl = sum(1 for v in G if v["long"])
    log(f"  viradas no store inteiro: {len(G)} (compra {nl}, venda "
        f"{len(G) - nl})")
    log(f"  {'cel':>3} | {'L tot':>6} {'S tot':>6} | {'L H1':>5} {'S H1':>5} | "
        f"{'L H2':>5} {'S H2':>5} | vetadas L/S")
    for c in CELULAS:
        sel = [v for v in G if passa(c, v)]
        cnt = lambda lg, f=None: sum(  # noqa: E731
            1 for v in sel if v["long"] is lg and (f is None or
                                                   _dentro(v["ts"], f)))
        log(f"  {c:>3} | {cnt(True):>6} {cnt(False):>6} | {cnt(True, H1):>5} "
            f"{cnt(False, H1):>5} | {cnt(True, H2):>5} {cnt(False, H2):>5} | "
            f"{nl - cnt(True)}/{len(G) - nl - cnt(False)}")
    gr = {g: sum(1 for v in G if v["grau"] == g) for g in (0, 1, 2)}
    log(f"  grau: 0 {gr[0]} | 1 {gr[1]} | 2 {gr[2]} | passa volume "
        f"{sum(v['vol_ok'] for v in G)} | ribbon {sum(v['rib_ok'] for v in G)}"
        f" | diario {sum(v['htf_ok'] for v in G)}")
    ct = A["cont"]
    log(f"  trades R0: {len(R0)} | excl_borda {ct['excl_borda']} | excl_hold "
        f"{ct['excl_hold']} | truncados {EXT_CAP} {sum(1 for t in R0 if t[5])}"
        f" | marcados no fim {sum(1 for t in R0 if t[6])} | fora das metades "
        f"{sum(1 for t in R0 if _metade(t[0]) == 'fora')}")
    log("  viradas por token por ano (R0) — o video fala em ~50:")
    for s in VIDEO:
        m = meta.get(s)
        if not m:
            log(f"    {s}: sem dados")
            continue
        anos_dado = (m["last"] - m["first"]) / (365.25 * DIA_MS)
        log(f"    {s}: H1 {m['vir_metade']['H1']} | H2 {m['vir_metade']['H2']}"
            f" | store {m['viradas']} em {anos_dado:.2f} anos = "
            f"{m['viradas'] / anos_dado if anos_dado else 0:.0f}/ano | por ano "
            f"civil {dict(sorted(m['anos'].items()))}")
    log(f"  RSS de pico {A['rss']:.0f} MiB (teto {RSS_CEILING_MIB:.0f}) | "
        f"tempo coleta {t_a:.0f}s, placebos {t_b:.0f}s")

    # ---- 2. ANCORA DO VIDEO --------------------------------------------
    sa, d0, d1 = ANCORA
    log(f"\n===== 2. ANCORA DO VIDEO — {sa}, entrada {d0}..{d1} (video: compra "
        f"'Strong' ~74.363, ATR ~1.360, saida 25/11/2024 ~94.651) =====")
    anc = [t for t in R0 if t[1] == sa and d0 <= _iso(t[0])[:10] <= d1]
    for t in anc:
        x, v = t[7], t[7]["virada"]
        log(f"  {_iso(t[0])} {'LONG' if t[2] else 'SHORT'} grau {v['grau']} | "
            f"entrada {x['entry']:.1f} | ATR10 {_f(x['atr10'], '.1f')} | ATR14 "
            f"{x['atr14']:.1f} | vol_rel {_f(v['vol_rel'], '.2f')} rib "
            f"{v['rib']} htf {v['htf']} | saida S0 {_iso(x['saida_ts'])} "
            f"{x['saida_px']:.1f} (hold {x['hold']}) | S0 {t[3][0]:+.0f} bps "
            f"S2 {t[3][1]:+.0f} bps{' (parou)' if t[4] else ''}")
    if not anc:
        log("  NENHUMA virada medida de BTCUSDT nessa janela")

    # ---- CSV ------------------------------------------------------------
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for t in R0:
            x, v = t[7], t[7]["virada"]
            w.writerow({"bar_ts_iso": _iso(t[0]), "mes": _iso(t[0])[:7],
                        "metade": _metade(t[0]), "sym": t[1],
                        "direcao": "LONG" if t[2] else "SHORT",
                        "hold": x["hold"], "truncado": int(t[5]),
                        "marcado_fim": int(t[6]), "ret_s0": f"{t[3][0]:.2f}",
                        "ret_s2": f"{t[3][1]:.2f}", "parou_s2": int(bool(t[4])),
                        "vol_rel": _f(v["vol_rel"], ".4f"),
                        "rib_dir": _f(v["rib"], "d"),
                        "htf_dir": _f(v["htf"], "d"), "grau": v["grau"]})
    out_p = out.with_name(out.stem + "_placebo.csv")
    with out_p.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS_PLACEBO)
        w.writeheader()
        for (c, sd), ag in plac.items():
            for (s, h), (n, r) in ag.items():
                w.writerow({"celula": c, "semente": sd, "stop": s,
                            "metade": h, "n": n, "ret6": f"{r:.2f}"})

    # ---- 3/4. RESULTADO --------------------------------------------------
    res = _tabela(log, R0, f"3. RESULTADO — UNIVERSO ({len(meta)} tokens)")
    _tabela(log, [t for t in R0 if t[1] in VIDEO],
            "4. RECORTE BTC/ETH/SOL (tokens do video)")

    # ---- 5. CONCENTRACAO -------------------------------------------------
    log("\n===== 5. CONCENTRACAO — 2% melhores trades (UNIVERSO) =====")
    for c in CELULAS:
        for s, _k in STOPS:
            for h, _f2 in METADES:
                x = res[(c, s, h)]
                nt, fat, sem_top = x["conc"]
                log(f"  {c:>3} {s} {h}: n {x['n']:>5} | top {nt:>3} | fatia "
                    f"{_f(fat, '.0f') + '%' if fat is not None else 'NA (total <= 0)'}"
                    f" | sem eles {sem_top:>+8.0f} "
                    f"({sem_top / max(1, x['n'] - nt):>+.1f} bps/trade)")

    # ---- 6. PLACEBO --------------------------------------------------------
    log("\n===== 6. FILTRO CONTRA O PLACEBO (ret@6, 20 sementes, retirada de "
        "trades do R0) =====")
    for c in FILTRADAS:
        for s, _k in STOPS:
            for h, _f2 in METADES:
                v = [plac[(c, sd)][(s, h)][1] for sd in SEMENTES]
                nn = [plac[(c, sd)][(s, h)][0] for sd in SEMENTES]
                cel = res[(c, s, h)]
                ab = sum(1 for y in v if y < cel["ret6"])
                log(f"  {c:>3} {s} {h}: celula {cel['ret6']:>+8.0f} (n "
                    f"{cel['n']}) | placebo n med {median(nn):.0f} | P10 "
                    f"{_pct(v, .1):>+8.0f} | med {median(v):>+8.0f} | P90 "
                    f"{_pct(v, .9):>+8.0f} | acima de {ab}/20 (percentil "
                    f"~{100 * ab / 20:.0f})")

    # ---- 7. CONTROLE PASSIVO ----------------------------------------------
    log("\n===== 7. CONTROLE PASSIVO — comprar e segurar a cesta, pesos iguais,"
        " sem custo =====")
    for h, _f2 in METADES:
        vs = [m["bh"][h] for m in meta.values() if m["bh"][h] is not None]
        log(f"  {h}: media {sum(vs) / len(vs) if vs else 0:>+8.0f} bps por "
            f"token | tokens com dado {len(vs)}/{len(meta)}")

    log(f"\n  CSV: {out} | placebos: {out_p}")
    log("caveat: EXPLORATORIO (camada 2), nada promove; defaults do autor "
        f"intactos. Teto T1 {TETO_ISO}. Entrada no close da virada (autor: "
        "open seguinte). Saida na virada contraria bruta = leitura suposta do "
        "video (ver ancora). ATR10 da linha semeado 1 barra depois do Pine. "
        "Diario por dias completos (dia incompleto descartado = veto). "
        "Placebo por retirada de trades do R0 (veto nao muda hold). Mergulho "
        "sobre a curva de trades por entrada (nao marca a mercado nem "
        "considera simultaneos). Hold > 256 truncado; ultimo trade de cada "
        "token marcado no ultimo close antes do teto; ultimas viradas a < 48 "
        "barras do teto excluidas (borda). Controle passivo sem custo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
