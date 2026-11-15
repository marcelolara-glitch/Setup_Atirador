# backtest/kis_meta.py — Setup Atirador (SANDBOX, camada 2) — KIS: META (descritivo)
# Briefing de 15/11/2026, ETAPA 4. Exploratorio: nada aqui promove nada.
#
# PERGUNTA: no candle do sinal do KIS 34/89 (portao ligado), que indicadores
# separam os trades vencedores dos perdedores, e a separacao aparece nas duas
# metades? Passo DESCRITIVO do meta-labeling: o lado do trade continua sendo o
# das medias; so se mede se algum indicador informa o RESULTADO. Sem modelo,
# sem limiar, sem vencedor.
#
# MOTOR REUSADO, nao reescrito: os trades saem de `kis_saida.run` (par 34/89,
# so quem passa no portao; rets liquidos a 6 bps, ordem `kis_saida.CELULAS`:
# indice 0 = E0, indice 2 = E1-3). Teto T1 = `kis_saida.conecta_com_teto` +
# `--end` acima do teto ABORTA. Metades = `kis_saida.H1/H2` por ENTRADA.
#
# DUAS PASSADAS, uma vela de simbolo por vez (VM de 956 MiB):
#   A) `kis_saida.run` -> lista de trades (escalares).
#   B) por simbolo, le as velas com os MESMOS limites (start None, end teto-1;
#      logo os mesmos indices de barra), calcula as series causais, extrai os
#      indicadores dos trades daquele simbolo e acumula dois mapas cruzados:
#      ts -> [n com EMA34>EMA89, n validos] e ts -> estado do BTC. As velas sao
#      descartadas antes do proximo simbolo. Junta btc_fav/amplitude no fim.
#
# INDICADORES (todos no FECHAMENTO da barra i do sinal, so com dados <= i;
# direcionais orientados A FAVOR: valor alto = a favor do trade):
#   adx13            `_adx(cndl)[i]` (o do portao)
#   rsi14_fav        RSI(14) Wilder sobre o close; SHORT: 100 - RSI
#   atr_pct100       100 * #{j em i-99..i : ATR14[j] < ATR14[i]} / 99 (0..100;
#                    NA se algum ATR da janela ainda nao existe)
#   incl21_fav       `inclinacoes(...)[i]`; SHORT: -incl
#   btc_fav          BTC: +1 close>EMA89 e EMA89[i]>EMA89[i-5]; -1 close<EMA89 e
#                    EMA89[i]<EMA89[i-5]; 0 resto; x(+1 LONG / -1 SHORT). NA se
#                    nao ha barra do BTC no mesmo ts (ou EMA89 em warmup)
#   amplitude_fav    % dos simbolos do universo com EMA34>EMA89 no mesmo ts,
#                    entre os que tem a barra e i >= 89; SHORT: 100 - %
#   barras_desde_inv i - indice da inversao anterior do token (vetadas incluidas)
#   ret_ant_e0/_e13  bps liquidos do trade ANTERIOR do mesmo token (34/89,
#                    portao), saidas E0 e E1-3
#   vol_rel20        volume[i] / media(volume[i-20..i-1])
#
# RET_ANT E CAUSAL — confirmacao no codigo (pedida no briefing):
#   * `kis_horizonte.entradas`: hold = (indice da PROXIMA inversao, vetada ou
#     nao) - p, com teto EXT_CAP. Logo p + hold <= indice da proxima inversao.
#   * `excursion.measure_event` le o caminho a partir de bar_ts + 1 barra, entao
#     fwd_bar[hold-1] e o close da barra p + hold. E0 = `trail_exit(arme=0)` =
#     fwd_bar[hold-1]; E1-3 = `kis_saida.stop_exit`, que so sai em j < hold.
#   * O trade atual nasce numa inversao i >= proxima inversao apos p. Portanto o
#     trade anterior termina em p + hold <= i: o resultado e conhecido no
#     fechamento de i. Isso NAO e so assumido: `extrai_simbolo` confere
#     p + hold <= i trade a trade; se falhar, a coluna fica NA e o caso e
#     contado no log (`ret_ant_violacao`).
#
# QUINTIS: cortes por posto mais proximo SO sobre H1 (direcoes juntas, sem NA),
# P20/P40/P60/P80, duplicados removidos; classe = bisect_right(cortes, v).
# Coluna com <= 5 valores distintos em H1 usa os proprios valores como classe.
# spread = bps/trade da classe mais alta - da mais baixa (classes de H1).
# PLACEBO: embaralha os valores (nao-NA) do indicador entre os trades de H2 da
# MESMA direcao; cortes de H1 intactos; sementes = kis_knn.SEMENTES (0..19).
# placebo_pct = 100 * #{placebos com |spread| < |spread_H2|} / 20.
#
# Uso (VM): python -m backtest.kis_meta --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import math
import random
import sys
from bisect import bisect_right
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest import kis_saida as ks                              # noqa: E402
from backtest.keepitsimple import _adx, _ema                      # noqa: E402
from backtest.kis_horizonte import entradas                       # noqa: E402
from backtest.kis_knn import SEMENTES                             # noqa: E402
from backtest.kis_regime import _atr_series, inclinacoes          # noqa: E402
from backtest.kis_trail import RSS_CEILING_MIB, _dentro, _rss_mib  # noqa: E402

PAR = (34, 89)
K_E0, K_E13 = ks.NOMES.index("E0"), ks.NOMES.index("E1-3")       # 0 e 2
RSI_LEN, ATR_LEN, ATR_JAN, VOL_JAN = 14, 14, 100, 20
BTC, BTC_EMA, BTC_LB = "BTCUSDT", 89, 5
POUCOS_DISTINTOS = 5
INDICADORES = ["adx13", "rsi14_fav", "atr_pct100", "incl21_fav", "btc_fav",
               "amplitude_fav", "barras_desde_inv", "ret_ant_e0",
               "ret_ant_e13", "vol_rel20"]
ROTULOS = (("E0", "ret_e0", "ganhou_e0"), ("E1-3", "ret_e13", "ganhou_e13"))
CAMPOS = ["bar_ts_iso", "mes", "metade", "sym", "direcao", "ret_e0", "ret_e13",
          "ganhou_e0", "ganhou_e13"] + INDICADORES


# ------------------------------------------------------- series causais ----
def rsi_wilder(closes: list, n: int = RSI_LEN) -> list:
    """RSI de Wilder. Semente = media simples dos n primeiros ganhos/perdas;
    out[i] definido a partir de i = n. Perda media 0: 100 (ou 50 se ganho 0)."""
    out = [None] * len(closes)
    if len(closes) <= n:
        return out
    d = [closes[k] - closes[k - 1] for k in range(1, len(closes))]
    g = sum(x for x in d[:n] if x > 0) / n
    p = sum(-x for x in d[:n] if x < 0) / n
    for i in range(n, len(closes)):
        if i > n:
            x = d[i - 1]
            g = (g * (n - 1) + (x if x > 0 else 0.0)) / n
            p = (p * (n - 1) + (-x if x < 0 else 0.0)) / n
        out[i] = (50.0 if g == 0 else 100.0) if p == 0 else \
            100.0 - 100.0 / (1.0 + g / p)
    return out


def atr_pct(atr: list, jan: int = ATR_JAN) -> list:
    out = [None] * len(atr)
    for i in range(jan - 1, len(atr)):
        w = atr[i - jan + 1:i + 1]
        if any(a is None for a in w):
            continue
        out[i] = 100.0 * sum(1 for a in w if a < atr[i]) / (jan - 1)
    return out


def vol_rel(vols: list, jan: int = VOL_JAN) -> list:
    out = [None] * len(vols)
    for i in range(jan, len(vols)):
        m = sum(vols[i - jan:i]) / jan
        out[i] = vols[i] / m if m > 0 else None
    return out


def series_barra(cndl: list) -> dict:
    """Todas as series POR BARRA usadas pelos indicadores. Cada valor em i so
    depende de cndl[:i+1] (testado por invariancia a prefixo)."""
    closes = [c["close"] for c in cndl]
    n = len(cndl)
    e34, e89 = _ema(closes, PAR[0]), _ema(closes, PAR[1])
    amp = [None if i < PAR[1] or e34[i] is None or e89[i] is None
           else e34[i] > e89[i] for i in range(n)]
    eb = _ema(closes, BTC_EMA)
    btc = [None] * n
    for i in range(BTC_LB, n):
        if eb[i] is None or eb[i - BTC_LB] is None:
            continue
        sobe, cai = eb[i] > eb[i - BTC_LB], eb[i] < eb[i - BTC_LB]
        btc[i] = 1 if closes[i] > eb[i] and sobe else \
            (-1 if closes[i] < eb[i] and cai else 0)
    # inversoes = mesmos indices de `entradas` (o hold nao entra aqui)
    tss = [c["ts"] for c in cndl]
    pos = {t: i for i, t in enumerate(tss)}
    inv = sorted(pos[ev["bar_ts"]] for ev in entradas(cndl, *PAR))
    desde, k = [None] * n, -1
    for i in range(n):
        while k + 1 < len(inv) and inv[k + 1] < i:
            k += 1
        desde[i] = i - inv[k] if k >= 0 else None
    return {"adx": _adx(cndl), "rsi": rsi_wilder(closes),
            "atrp": atr_pct(_atr_series(cndl, ATR_LEN)),
            "incl": inclinacoes(cndl, closes),
            "volrel": vol_rel([c["volume"] for c in cndl]),
            "amp": amp, "btc": btc, "desde": desde}


# ------------------------------------------------- extracao por simbolo ----
def extrai_simbolo(cndl: list, trades_sym: list) -> tuple:
    """`trades_sym` = tuplas do kis_saida deste simbolo, ordenadas por bar_ts.
    Devolve (feats paralelos aos trades, {ts: bool EMA34>EMA89}, {ts: estado
    BTC}, n_violacao_ret_ant). btc_fav e amplitude ficam para `junta`."""
    s = series_barra(cndl)
    pos = {c["ts"]: i for i, c in enumerate(cndl)}
    hold = {ev["bar_ts"]: ev["hold"] for ev in entradas(cndl, *PAR)}
    feats, viol, ant = [], 0, None
    for t in trades_sym:
        ts, is_long, rets = t[0], t[2], t[3]
        i, sg = pos[ts], 1 if is_long else -1
        if ts not in hold:
            raise RuntimeError(f"{t[1]} {ks._iso(ts)}: trade sem inversao em "
                               "`entradas` — base desalinhada. ABORTADO.")
        f = {"adx13": s["adx"][i],
             "rsi14_fav": None if s["rsi"][i] is None else
             (s["rsi"][i] if is_long else 100.0 - s["rsi"][i]),
             "atr_pct100": s["atrp"][i],
             "incl21_fav": None if s["incl"][i] is None else sg * s["incl"][i],
             "barras_desde_inv": s["desde"][i],
             "vol_rel20": s["volrel"][i],
             "ret_ant_e0": None, "ret_ant_e13": None, "_i": i}
        if ant is not None:
            p_ts, p_rets = ant
            if pos[p_ts] + hold[p_ts] <= i:
                f["ret_ant_e0"], f["ret_ant_e13"] = p_rets[K_E0], p_rets[K_E13]
            else:
                viol += 1
        ant = (ts, rets)
        feats.append(f)
    amp = {c["ts"]: s["amp"][i] for i, c in enumerate(cndl)
           if s["amp"][i] is not None}
    btc = {c["ts"]: s["btc"][i] for i, c in enumerate(cndl)}
    return feats, amp, btc, viol


def junta(trades: list, feats: list, amp: dict, btc: dict) -> None:
    """Preenche btc_fav e amplitude_fav in place. amp = {ts: [n_up, n_val]}."""
    for t, f in zip(trades, feats):
        sg = 1 if t[2] else -1
        b = btc.get(t[0])
        f["btc_fav"] = None if b is None else sg * b
        a = amp.get(t[0])
        if a is None or a[1] == 0:
            f["amplitude_fav"] = None
        else:
            pct = 100.0 * a[0] / a[1]
            f["amplitude_fav"] = pct if t[2] else 100.0 - pct


def coleta(conn, symbols: list, tf: str, end_ms: int, trades: list) -> dict:
    """Passada B. `trades` = lista global do kis_saida (ordenada por ts)."""
    from backtest.candle_store import read_candles
    por_sym = {}
    for j, t in enumerate(trades):
        por_sym.setdefault(t[1], []).append(j)
    feats, ibar = [None] * len(trades), [None] * len(trades)
    amp, btc, viol, rss = {}, None, Counter(), _rss_mib()
    lista = list(symbols) + ([BTC] if BTC not in symbols else [])
    for sym in lista:
        cndl = read_candles(conn, sym, tf, start_ms=None, end_ms=end_ms)
        if not cndl:
            continue
        idx = por_sym.get(sym, []) if sym in symbols else []
        fs, a, b, v = extrai_simbolo(cndl, [trades[j] for j in idx])
        del cndl
        for j, f in zip(idx, fs):
            ibar[j] = f.pop("_i")
            feats[j] = f
        if v:
            viol[sym] += v
        if sym in symbols:
            for ts, up in a.items():
                x = amp.setdefault(ts, [0, 0])
                x[0] += 1 if up else 0
                x[1] += 1
        if sym == BTC:
            btc = b
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} "
                              f"apos {sym}; PARANDO antes de estourar a VM.")
    if any(f is None for f in feats):
        raise RuntimeError("trade sem indicadores apos a passada B. ABORTADO.")
    junta(trades, feats, amp, btc or {})
    return {"feats": feats, "ibar": ibar, "viol": viol, "rss": rss,
            "btc_ts": set(btc or {}), "tem_btc": btc is not None}


def linhas_csv(trades: list, feats: list) -> list:
    out = []
    for t, f in zip(trades, feats):
        h = "H1" if _dentro(t[0], ks.H1) else ("H2" if _dentro(t[0], ks.H2)
                                                else None)
        if h is None:
            continue
        e0, e13 = t[3][K_E0], t[3][K_E13]
        out.append({"bar_ts_iso": ks._iso(t[0]), "mes": ks._iso(t[0])[:7],
                    "metade": h, "sym": t[1],
                    "direcao": "LONG" if t[2] else "SHORT", "ret_e0": e0,
                    "ret_e13": e13, "ganhou_e0": int(e0 > 0),
                    "ganhou_e13": int(e13 > 0), **f})
    return out


# ------------------------------------------------------------- quintis ----
def cortes(rows: list, col: str) -> tuple:
    """(modo, cortes) SO de H1. modo 'valores': cortes = valores distintos;
    'quintis': P20..P80 por posto mais proximo, sem duplicados."""
    v = sorted(r[col] for r in rows if r["metade"] == "H1"
               and r[col] is not None)
    if not v:
        return "vazio", []
    dist = sorted(set(v))
    if len(dist) <= POUCOS_DISTINTOS:
        return "valores", dist
    n = len(v)
    return "quintis", sorted({v[max(0, math.ceil(round(q * n, 9)) - 1)]
                              for q in (0.2, 0.4, 0.6, 0.8)})


def classe(v, modo: str, cs: list):
    if v is None or modo == "vazio":
        return None
    if modo == "valores":
        return cs.index(v) if v in cs else None
    return bisect_right(cs, v)


def n_classes(modo: str, cs: list) -> int:
    return {"vazio": 0, "valores": len(cs)}.get(modo, len(cs) + 1)


def rotulo_classe(k: int, modo: str, cs: list) -> str:
    if modo == "valores":
        return f"={cs[k]:g}"
    if k == 0:
        return f"Q1 <= {cs[0]:.4g}"
    if k == len(cs):
        return f"Q{k + 1} > {cs[-1]:.4g}"
    return f"Q{k + 1} ({cs[k - 1]:.4g}, {cs[k]:.4g}]"


def estat(rows: list, col: str, ret: str, modo: str, cs: list,
          vals=None) -> list:
    """Por classe: (n, bps/trade, % vencedores). `vals` substitui os valores
    da coluna (placebo)."""
    nc = n_classes(modo, cs)
    acc = [[0, 0.0, 0] for _ in range(nc)]
    for j, r in enumerate(rows):
        k = classe(r[col] if vals is None else vals[j], modo, cs)
        if k is None:
            continue
        acc[k][0] += 1
        acc[k][1] += r[ret]
        acc[k][2] += 1 if r[ret] > 0 else 0
    return [(n, s / n if n else None, 100.0 * w / n if n else None)
            for n, s, w in acc]


def spread(est: list):
    if len(est) < 2 or est[0][1] is None or est[-1][1] is None:
        return None
    return est[-1][1] - est[0][1]


def melhor_mes(rows: list, ret: str) -> str:
    m = Counter()
    for r in rows:
        m[r["mes"]] += r[ret]
    return max(m, key=m.get) if m else ""


def placebos(h2: list, col: str, ret: str, modo: str, cs: list) -> list:
    out = []
    for sem in SEMENTES:
        rng = random.Random(sem)
        vals = [r[col] for r in h2]
        for d in ("LONG", "SHORT"):
            ix = [j for j, r in enumerate(h2)
                  if r["direcao"] == d and r[col] is not None]
            vs = [vals[j] for j in ix]
            rng.shuffle(vs)
            for j, v in zip(ix, vs):
                vals[j] = v
        out.append(spread(estat(h2, col, ret, modo, cs, vals)))
    return out


def resumo_indicador(rows: list, col: str, ret: str, mes_fora: str) -> dict:
    modo, cs = cortes(rows, col)
    h1 = [r for r in rows if r["metade"] == "H1"]
    h2 = [r for r in rows if r["metade"] == "H2"]
    s1 = spread(estat(h1, col, ret, modo, cs))
    s2 = spread(estat(h2, col, ret, modo, cs))
    s2x = spread(estat([r for r in h2 if r["mes"] != mes_fora], col, ret,
                       modo, cs))
    pl = [p for p in placebos(h2, col, ret, modo, cs) if p is not None]
    pct = (100.0 * sum(1 for p in pl if abs(p) < abs(s2)) / len(SEMENTES)
           if s2 is not None and pl else None)
    mesmo = None if s1 is None or s2 is None else (
        "s" if s1 * s2 > 0 else "n")
    return {"modo": modo, "cortes": cs, "spread_H1": s1, "spread_H2": s2,
            "mesmo_sinal": mesmo, "spread_H2_sem_melhor_mes": s2x,
            "placebo_pct": pct, "n_placebo_validos": len(pl)}


# ---------------------------------------------------------------- saida ----
class _Log:
    def __init__(self, path: Path):
        self.fh = path.open("w")

    def __call__(self, *a):
        s = " ".join(str(x) for x in a)
        print(s)
        self.fh.write(s + "\n")
        self.fh.flush()


def _f(x, fmt="+.1f"):
    return "NA" if x is None else format(x, fmt)


def _tabela(log, rows: list, col: str, ret: str, modo: str, cs: list) -> None:
    h1 = [r for r in rows if r["metade"] == "H1"]
    h2 = [r for r in rows if r["metade"] == "H2"]
    e1, e2 = estat(h1, col, ret, modo, cs), estat(h2, col, ret, modo, cs)
    for k in range(n_classes(modo, cs)):
        a, b = e1[k], e2[k]
        log(f"    {rotulo_classe(k, modo, cs):>26} | H1 n {a[0]:>4} bpt "
            f"{_f(a[1]):>7} venc {_f(a[2], '.0f'):>3}% | H2 n {b[0]:>4} bpt "
            f"{_f(b[1]):>7} venc {_f(b[2], '.0f'):>3}%")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="KIS 34/89: meta descritivo (etapa 4)")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=ks.TETO_ISO)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None)
    ap.add_argument("--out", default=None, help="CSV; o log vai ao lado (.log)")
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    args = ap.parse_args(argv)
    if args.end > ks.TETO_ISO:                                     # T1 camada 1
        raise SystemExit(f"[kis_meta] T1: --end {args.end} passa do teto "
                         f"{ks.TETO_ISO}. Hold-out trancado. ABORTADO.")
    if not args.sem_guarda_relogio:
        ks._guarda_relogio()
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"kis_meta_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    log = _Log(out.with_suffix(".log"))
    teto_ms = ks._ms(args.end)
    conn = ks.conecta_com_teto(args.store, teto_ms)                # T1 camada 2
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[kis_meta] T1 VIOLADO. ABORTADO.")
        ini = ks.inicio_por_simbolo(conn, symbols, args.tf)
        log(f"[kis_meta] teto {args.end} (exclusivo) | maior ts visivel "
            f"{ks._iso(mx) if mx else '-'} | tf {args.tf} | simbolos "
            f"{len(symbols)} | sem candles: {[s for s, v in ini.items() if not v]}")
        com = [s for s in symbols if ini.get(s)]
        r = ks.run(conn, com, args.tf, None, teto_ms - 1, pares=(PAR,))
        trades = r["por_par"][PAR]
        c = coleta(conn, com, args.tf, teto_ms - 1, trades)
    finally:
        conn.close()
    rows = linhas_csv(trades, c["feats"])

    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for row in rows:
            w.writerow({k: ("NA" if row[k] is None else
                            f"{row[k]:.6g}" if isinstance(row[k], float)
                            else row[k]) for k in CAMPOS})

    # ---- 1. CONTAGENS -----------------------------------------------------
    cont = r["cont"][PAR]
    log("\n===== 1. CONTAGENS =====")
    log(f"  34/89: inversoes {cont['inversoes']} | vetadas pelo portao "
        f"{cont['vetadas_portao']} | excl_borda {cont['excl_borda']} | "
        f"excl_hold {cont['excl_hold']} | trades {len(trades)} | em H1 U H2 "
        f"{len(rows)} | fora das metades {len(trades) - len(rows)}")
    for h in ("H1", "H2"):
        sub = [x for x in rows if x["metade"] == h]
        nl = sum(1 for x in sub if x["direcao"] == "LONG")
        log(f"  {h}: n {len(sub)} | LONG {nl} | SHORT {len(sub) - nl}")
    # expectativas declaradas ANTES de contar, de fontes independentes
    dentro = {id(t) for t in trades if _dentro(t[0], ks.H1)
              or _dentro(t[0], ks.H2)}
    primeiro = {}
    for t in trades:
        primeiro.setdefault(t[1], t)
    esp_ant = sum(1 for t in primeiro.values() if id(t) in dentro)
    esp_btc = sum(1 for t in trades if id(t) in dentro
                  and t[0] not in c["btc_ts"])
    esp_atr = sum(1 for t, i in zip(trades, c["ibar"]) if id(t) in dentro
                  and i < ATR_LEN + ATR_JAN - 1)
    esperado = {k: 0 for k in INDICADORES}
    esperado.update(ret_ant_e0=esp_ant, ret_ant_e13=esp_ant, btc_fav=esp_btc,
                    atr_pct100=esp_atr)
    log("  NA ESPERADO (declarado antes da contagem):")
    log(f"    ret_ant_*: {esp_ant} (1o trade do token cai em H1 U H2; "
        f"violacoes p+hold>i esperadas 0) | barras_desde_inv: so na 1a "
        f"inversao do token (<= 1 por token) | btc_fav: {esp_btc} (ts sem "
        f"barra do BTC{'' if c['tem_btc'] else '; BTC SEM CANDLES'}) | "
        f"atr_pct100: {esp_atr} (trade antes da barra {ATR_LEN + ATR_JAN - 1} "
        "do token: ATR14 + janela de 100; so token listado tarde) | resto: 0")
    log("  NA OBSERVADO:")
    alerta = False
    for k in INDICADORES:
        na = [x["sym"] for x in rows if x[k] is None]
        ok = (len(na) <= len(set(x["sym"] for x in rows))
              and max(Counter(na).values() or [0]) <= 1) \
            if k == "barras_desde_inv" else len(na) == esperado[k]
        alerta |= not ok
        det = " ".join(f"{s.replace('USDT', '')}:{n}"
                       for s, n in sorted(Counter(na).items()))
        log(f"    {k:>17}: {len(na):>4} {'ok' if ok else 'FORA DO ESPERADO'}"
            + (f" | {det}" if na else ""))
    nv = sum(c["viol"].values())
    log(f"    ret_ant_violacao (p+hold > i): {nv}"
        + (" | " + " ".join(f"{s}:{n}" for s, n in c["viol"].items()) if nv
           else ""))
    alerta |= nv > 0
    log(f"  RSS de pico {c['rss']:.0f} MiB (teto {RSS_CEILING_MIB:.0f}) | CSV "
        f"{out}" + (" | ALERTA: contagem fora do esperado acima" if alerta
                    else ""))

    # ---- 2. CONCILIACAO ----------------------------------------------------
    log("\n===== 2. CONCILIACAO com kis_saida.resumo (mesma execucao) =====")
    for h, faixa in (("H1", ks.H1), ("H2", ks.H2)):
        for nome, k, col in (("E0", K_E0, "ret_e0"), ("E1-3", K_E13, "ret_e13")):
            ref = ks.resumo(trades, k, faixa)
            sub = [x[col] for x in rows if x["metade"] == h]
            dif = abs(sum(sub) - ref["ret6"])
            log(f"  {h} {nome:>4}: n {len(sub)} vs {ref['n']} | soma "
                f"{sum(sub):+.1f} vs {ref['ret6']:+.1f} | dif {dif:.2e}")
            if len(sub) != ref["n"] or dif > 1e-6:
                log("  CONCILIACAO FALHOU. ABORTADO.")
                raise SystemExit(2)

    # ---- 3. QUINTIS --------------------------------------------------------
    log("\n===== 3. TABELA DE QUINTIS (cortes so de H1, direcoes juntas) =====")
    for col in INDICADORES:
        modo, cs = cortes(rows, col)
        for nome, ret, _g in ROTULOS:
            for d in (None, "LONG", "SHORT"):
                sub = [x for x in rows if d is None or x["direcao"] == d]
                log(f"  {col} | {nome} | {d or 'TODOS'} | {modo}")
                _tabela(log, sub, col, ret, modo, cs)

    # ---- 4. RESUMO ---------------------------------------------------------
    log("\n===== 4. RESUMO POR INDICADOR (spread = bpt classe alta - baixa) =====")
    h2 = [x for x in rows if x["metade"] == "H2"]
    for nome, ret, _g in ROTULOS:
        mm = melhor_mes(h2, ret)
        log(f"  rotulo {nome} | melhor mes de H2 (retirado): {mm or '-'}")
        log(f"  {'indicador':>17} | {'spr_H1':>7} | {'spr_H2':>7} | mesmo | "
            f"{'H2_sem_mm':>9} | placebo_pct")
        for col in INDICADORES:
            z = resumo_indicador(rows, col, ret, mm)
            log(f"  {col:>17} | {_f(z['spread_H1']):>7} | "
                f"{_f(z['spread_H2']):>7} | {z['mesmo_sinal'] or 'NA':>5} | "
                f"{_f(z['spread_H2_sem_melhor_mes']):>9} | "
                f"{_f(z['placebo_pct'], '.0f'):>4}"
                + ("" if z["n_placebo_validos"] == len(SEMENTES) else
                   f" (placebos validos {z['n_placebo_validos']}/20)"))

    log("\ncaveat: DESCRITIVO (camada 2), nada promove; 10 colunas x 2 rotulos "
        "= 20 spreads sem correcao de multiplicidade: sob H0 cada um da "
        "placebo_pct >= 95 com P = 2/21, entao ~2 de 20 por acaso. Teto T1 " + ks.TETO_ISO + "; trades do kis_saida "
        "34/89 com portao; metades por ENTRADA; cortes so de H1; placebo "
        "embaralha so H2 dentro da direcao; spread de classe com n pequeno e "
        "ruido; E0 e E1-3 sao o MESMO conjunto de trades (rotulos correlatos).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
