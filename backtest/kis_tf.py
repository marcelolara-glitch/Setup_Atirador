# backtest/kis_tf.py — Setup Atirador (SANDBOX, camada 2) — KIS: TEMPO GRAFICO
# Briefing v3, ETAPA 3 (14/11/2026). Exploratorio: nada aqui promove nada.
#
# PERGUNTA: o 34/89 (E0 e E1-3) melhora, piora ou some quando muda o tempo
# grafico? Leitura por PLATO ao longo dos tempos graficos, nao pelo melhor.
#
# DADO: 4h do store (referencia, deve bater com a etapa 1); 8h/12h/1D AGREGADOS
# em memoria dos 4h (2/3/6 barras, blocos alinhados em 00:00 UTC, open do 1o,
# high max, low min, close do ultimo, volume somado, bloco incompleto
# descartado — nada e gravado no store); 1h do store SO se a cobertura cobrir as
# duas metades (regra abaixo), senao so a cobertura e reportada.
#
# MEDICAO EM MEMORIA: `excursion.measure_event` le o store pelo tf e so conhece
# 15m/1h/4h. `medir` abaixo e a MESMA regra sobre uma lista de velas: entry =
# close da barra do sinal, ATR = `_wilder_atr` (o mesmo) sobre as 30 velas ate a
# barra, forward = velas com ts em [bar+1 barra, bar + 256 barras] (por TEMPO,
# como no store), borda = < 48 barras forward -> fora. Ha teste provando que, no
# 4h, `medir` devolve exatamente o que `measure_event` devolve.
# CONSEQUENCIA DA REGRA EM BARRAS (nao alterada): no 1D a borda de 48 barras
# corta entradas dos ultimos 48 DIAS antes do teto; no 12h, 24 dias; 8h, 16.
# O log conta as exclusoes por tempo grafico.
#
# CELULAS por tempo grafico: par 34/89 x {E0, E1-3} x portao {ligado 0.02/11,
# desligado}. ATR(14), EMA21 da rampa e ADX(13) do portao no PROPRIO tempo
# grafico (limiar calibrado no 4h; nao recalibrado — dai a celula desligada).
# Warmup = 89 barras do tempo grafico; hold teto 256 barras do tempo grafico.
#
# Teto de leitura: TEMP VIEW no `--end` (`kis_saida.conecta_com_teto`);
# qualquer `--end` e aceito desde 23/11/2026, vazamento ABORTA.
# Uso (VM): python -m backtest.kis_tf --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP, _adx                   # noqa: E402
from backtest.kis_horizonte import (PORTAO_ADX_MIN, PORTAO_LIMIAR,  # noqa: E402
                                    entradas)
from backtest.kis_regime import inclinacoes, passa                # noqa: E402
from backtest.kis_saida import (H1, H2, TETO_ISO, _guarda_relogio,  # noqa: E402
                                _iso, _ms, conecta_com_teto, resumo,
                                stop_exit)
from backtest.kis_trail import (COST_BPS, _dentro, _rss_mib,      # noqa: E402
                                add_teto_rss, define_teto_rss, teto_rss,
                                trail_exit)

PAR = (34, 89)
H_MS = 3_600_000
TFS = {"1h": 1, "4h": 4, "8h": 8, "12h": 12, "1D": 24}   # horas por barra
AGREGA = {"8h": 2, "12h": 3, "1D": 6}                     # barras de 4h
ORDEM_TF = ("1h", "4h", "8h", "12h", "1D")
CELULAS = ("E0", "E1-3")
BORDA = 48                         # barras forward minimas (regra do excursion)
CTX = 30                           # velas de contexto p/ entry/ATR (idem)
TRANCADO = ("2026-08-31", "2026-11-14")   # SO a duracao e usada (projecao)
COBERTURA_MIN = 0.9                # abaixo disso: AVISO (era recusa ate 23/11)


def agregar(c4h: list, n: int) -> list:
    """Agrega velas de 4h em blocos de n barras alinhados em 00:00 UTC. Bloco
    sem TODAS as n velas (buraco ou borda) e descartado."""
    bloco = n * 4 * H_MS
    out, cur, ini = [], [], None
    for c in c4h:
        b = c["ts"] - c["ts"] % bloco
        if b != ini:
            if cur and len(cur) == n:
                out.append(_junta(ini, cur))
            cur, ini = [], b
        cur.append(c)
    if cur and len(cur) == n:
        out.append(_junta(ini, cur))
    return out


def _junta(ts: int, cs: list) -> dict:
    return {"ts": ts, "open": cs[0]["open"],
            "high": max(c["high"] for c in cs),
            "low": min(c["low"] for c in cs), "close": cs[-1]["close"],
            "volume": sum(c["volume"] for c in cs)}


def medir(cndl: list, i: int, direction: str, bar_ms: int,
          cap: int = EXT_CAP):
    """measure_event em memoria (ver cabecalho). None = atr<=0 ou borda."""
    from backtest.excursion import _wilder_atr
    ctx = cndl[max(0, i - CTX + 1):i + 1]
    entry, atr = ctx[-1]["close"], _wilder_atr(ctx)
    if atr <= 0:
        return None
    t0, lim = cndl[i]["ts"], cndl[i]["ts"] + max(BORDA, cap) * bar_ms
    fwd = []
    for c in cndl[i + 1:i + 1 + max(BORDA, cap)]:
        if c["ts"] > lim:
            break
        if c["ts"] >= t0 + bar_ms:
            fwd.append(c)
    if len(fwd) < BORDA:
        return None
    sg = 1.0 if direction == "LONG" else -1.0
    longa = fwd[:cap]
    if sg > 0:
        fav = [(c["high"] - entry) / atr for c in longa]
        adv = [(entry - c["low"]) / atr for c in longa]
    else:
        fav = [(entry - c["low"]) / atr for c in longa]
        adv = [(c["high"] - entry) / atr for c in longa]
    return {"atr": atr, "entry": entry,
            "fwd_bar": [sg * (c["close"] - entry) / atr for c in fwd[:cap]],
            "open_bar": [sg * (c["open"] - entry) / atr for c in longa],
            "fav_bar": fav, "adv_bar": adv}


def trades_simbolo(cndl: list, sym: str, bar_ms: int, cont: dict) -> list:
    """Uma passada: (bar_ts, sym, is_long, [E0, E1-3], bits, trunc, ultimo,
    passou_portao). A celula sem portao usa todos; com portao, o subconjunto."""
    if not cndl:
        return []
    closes = [c["close"] for c in cndl]
    incl, adx = inclinacoes(cndl, closes), _adx(cndl)
    pos = {c["ts"]: i for i, c in enumerate(cndl)}
    evs = entradas(cndl, PAR[0], PAR[1])
    out, ult = [], len(cndl) - 1
    for n_ev, ev in enumerate(evs):
        cont["inversoes"] += 1
        i = pos[ev["bar_ts"]]
        m = medir(cndl, i, ev["direction"], bar_ms)
        if m is None:
            cont["excl_borda"] += 1
            continue
        hold = ev["hold"]
        if hold > len(m["fwd_bar"]):
            cont["excl_hold"] += 1
            continue
        ap = m["atr"] / m["entry"]
        e0 = trail_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                        m["fwd_bar"], hold, 0, 0) * ap * 1e4 - COST_BPS
        r3, parou = stop_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                              m["fwd_bar"], hold, 3, False)
        out.append((ev["bar_ts"], sym, ev["direction"] == "LONG",
                    [e0, r3 * ap * 1e4 - COST_BPS], 2 if parou else 0,
                    hold >= EXT_CAP, n_ev == len(evs) - 1 and i + hold == ult,
                    passa(ev["direction"], incl[i], adx[i], PORTAO_LIMIAR,
                          PORTAO_ADX_MIN)))
    return out


def cobertura_1h(conn, symbols: list) -> tuple:
    """(cobre?, linhas). Cobre H1+H2 = primeiro candle 1h ate H1 - 89 barras e
    ultimo >= H2 fim 00:00. `cobre?` = >= COBERTURA_MIN do universo cobre; so
    informa (aviso no log): desde 23/11/2026 o 1h roda sempre, com os
    simbolos que cobrem."""
    ini_max = _ms(H1[0]) - 89 * H_MS
    fim_min = _ms(H2[1])
    linhas, ok = [], 0
    for s in symbols:
        a, b, n = conn.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM candles "
                               "WHERE symbol=? AND timeframe='1h'",
                               (s,)).fetchone()
        cob = a is not None and a <= ini_max and b >= fim_min
        ok += cob
        linhas.append((s, a, b, n, cob))
    return ok >= COBERTURA_MIN * len(symbols), linhas


def _meses(sel: list, k: int) -> tuple:
    mes = {}
    for t in sel:
        mm = _iso(t[0])[:7]
        mes[mm] = mes.get(mm, 0.0) + t[3][k]
    return sum(1 for v in mes.values() if v > 0), len(mes)


CAMPOS = ["recorte", "tf", "portao", "celula", "metade", "n", "tokens",
          "trades_por_token", "ret6", "ret10", "bpt6", "bpt10", "dd",
          "tri_pos", "tri_tot", "meses_pos", "meses_tot", "n_long", "ret_long",
          "n_short", "ret_short", "top3", "melhor_mes", "ret_sem_melhor_mes",
          "n_stop", "n_trunc256", "n_marcado_fim"]


def linhas(por_tf: dict, tokens_tf: dict, recortes: dict) -> list:
    out = []
    for tf, trades in por_tf.items():
        for rec, syms in recortes.items():
            ntok = len(syms & tokens_tf[tf])
            for portao in (1, 0):
                sub = sorted((t for t in trades if t[1] in syms
                              and (t[7] or not portao)), key=lambda t: t[0])
                for k, cel in enumerate(CELULAS):
                    for h, f in (("H1", H1), ("H2", H2)):
                        m = resumo(sub, k, f)
                        mp, mt = _meses([t for t in sub if _dentro(t[0], f)], k)
                        out.append({"recorte": rec, "tf": tf, "portao": portao,
                                    "celula": cel, "metade": h, "tokens": ntok,
                                    "trades_por_token": (m["n"] / ntok
                                                         if ntok else 0.0),
                                    "meses_pos": mp, "meses_tot": mt, **m})
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="KIS 34/89 por tempo grafico")
    ap.add_argument("--end", default=TETO_ISO)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    add_teto_rss(ap)
    args = ap.parse_args(argv)
    define_teto_rss(args.teto_rss)
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from backtest.candle_store import read_candles
    from backtest.sweep import TIER1
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    teto_ms = _ms(args.end)
    por_tf = {tf: [] for tf in ORDEM_TF}
    tokens_tf = {tf: set() for tf in ORDEM_TF}
    cont = {tf: {"inversoes": 0, "excl_borda": 0, "excl_hold": 0}
            for tf in ORDEM_TF}
    conn = conecta_com_teto(args.store, teto_ms)
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[kis_tf] VAZAMENTO: view deixou passar "
                             "candle >= --end. ABORTADO.")
        print(f"[kis_tf] teto {args.end} (exclusivo) | maior ts visivel "
              f"{_iso(mx) if mx else '-'} | simbolos {len(symbols)} | par "
              f"{PAR[0]}/{PAR[1]}")

        cobre_min, cob = cobertura_1h(conn, symbols)
        print(f"\n===== COBERTURA 1h (precisa: 1o candle <= "
              f"{_iso(_ms(H1[0]) - 89 * H_MS)} e ultimo >= {H2[1]} 00:00) =====")
        for s, a, b, n, ok in cob:
            print(f"  {s:>14}: " + (f"{_iso(a)} -> {_iso(b)} n={n}" if a
                                    else "SEM 1h") + ("  cobre" if ok else ""))
        n_ok = sum(1 for x in cob if x[4])
        print(f"  cobrem: {n_ok}/{len(symbols)} -> 1h RODA (so os que cobrem)"
              + ("" if cobre_min else
                 f" | AVISO: abaixo de {COBERTURA_MIN:.0%} do universo"))
        cobre_1h = {x[0] for x in cob if x[4]}

        desalinh = 0
        for sym in symbols:
            c4 = read_candles(conn, sym, "4h", end_ms=teto_ms - 1)
            desalinh += sum(1 for c in c4 if c["ts"] % (4 * H_MS))
            series = {"4h": c4}
            for tf, n in AGREGA.items():
                series[tf] = agregar(c4, n)
            if sym in cobre_1h:
                series["1h"] = read_candles(conn, sym, "1h", end_ms=teto_ms - 1)
            for tf, cs in series.items():
                if cs:
                    tokens_tf[tf].add(sym)
                    por_tf[tf].extend(trades_simbolo(cs, sym, TFS[tf] * H_MS,
                                                     cont[tf]))
                series[tf] = None
            rss = _rss_mib()
            if rss > teto_rss():
                raise MemoryError(f"RSS {rss:.0f} MiB > teto apos {sym}.")
        print(f"  velas 4h fora do alinhamento de 4h UTC: {desalinh}")
    finally:
        conn.close()
    if not tokens_tf["1h"]:                # sem vela 1h: nada a mostrar
        del por_tf["1h"]
    for tf in por_tf:
        por_tf[tf].sort(key=lambda t: t[0])

    recortes = {"UNIVERSO": set(symbols), "TIER1": set(TIER1) & set(symbols)}
    recortes.update({s: {s} for s in symbols})
    rows = linhas(por_tf, tokens_tf, recortes)
    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"kis_tf_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({c: (f"{row[c]:.1f}" if isinstance(row[c], float)
                            else row[c]) for c in CAMPOS})

    print("\n===== CONTAGENS =====")
    for tf in por_tf:
        c, tr = cont[tf], por_tf[tf]
        print(f"  {tf:>3}: tokens {len(tokens_tf[tf])} | inversoes "
              f"{c['inversoes']} | excl_borda {c['excl_borda']} | excl_hold "
              f"{c['excl_hold']} | trades s/ portao {len(tr)} | c/ portao "
              f"{sum(1 for t in tr if t[7])} | hold no teto 256 "
              f"{sum(1 for t in tr if t[5])} | marcados no fim "
              f"{sum(1 for t in tr if t[6])}")
    print(f"  RSS de pico {_rss_mib():.0f} MiB | CSV: {out}")

    def pega(rec, tf, pt, cel, h):
        return next(x for x in rows if x["recorte"] == rec and x["tf"] == tf
                    and x["portao"] == pt and x["celula"] == cel
                    and x["metade"] == h)

    for rec in ("UNIVERSO", "TIER1"):
        print(f"\n===== {rec} — H1 {H1[0]}..{H1[1]} | H2 {H2[0]}..{H2[1]} =====")
        print(f"{'tf':>3} pt {'cel':>4} H  | {'n':>4} | {'t/tok':>5} | "
              f"{'ret@6':>8} | {'ret@10':>8} | {'bpt@6':>7} | {'bpt@10':>7} | "
              f"{'dd':>7} | {'tri':>5} | {'mes+':>5} | {'nL':>3} {'retL':>8} | "
              f"{'nS':>3} {'retS':>8} | {'sem melhor mes':>22} | top3")
        for tf in por_tf:
            for pt in (1, 0):
                for cel in CELULAS:
                    for h in ("H1", "H2"):
                        x = pega(rec, tf, pt, cel, h)
                        print(f"{tf:>3} {pt:>2} {cel:>4} {h} | {x['n']:>4} | "
                              f"{x['trades_por_token']:>5.1f} | "
                              f"{x['ret6']:>+8.0f} | {x['ret10']:>+8.0f} | "
                              f"{x['bpt6']:>+7.1f} | {x['bpt10']:>+7.1f} | "
                              f"{x['dd']:>7.0f} | {x['tri_pos']:>2}/"
                              f"{x['tri_tot']:<2} | {x['meses_pos']:>2}/"
                              f"{x['meses_tot']:<2} | {x['n_long']:>3} "
                              f"{x['ret_long']:>+8.0f} | {x['n_short']:>3} "
                              f"{x['ret_short']:>+8.0f} | "
                              f"{x['ret_sem_melhor_mes']:>+8.0f} "
                              f"({x['melhor_mes']}) | {x['top3']}")

    print("\n===== PLATO — UNIVERSO, bps/trade @6 (n) por tempo grafico =====")
    print(f"{'pt':>2} {'cel':>4} H  | " + " | ".join(f"{tf:>14}" for tf in por_tf))
    for pt in (1, 0):
        for cel in CELULAS:
            for h in ("H1", "H2"):
                print(f"{pt:>2} {cel:>4} {h} | " + " | ".join(
                    "{:>+7.1f} ({:>4})".format(pega("UNIVERSO", tf, pt, cel, h)
                                               ["bpt6"],
                                               pega("UNIVERSO", tf, pt, cel, h)
                                               ["n"]) for tf in por_tf))

    dias_h2 = (_ms(H2[1]) - _ms(H2[0])) / 86_400_000 + 1
    dias_tr = (_ms(TRANCADO[1]) - _ms(TRANCADO[0])) / 86_400_000 + 1
    print(f"\n===== CONCLUSAO — trades esperados no trecho trancado "
          f"{TRANCADO[0]}->{TRANCADO[1]} ({dias_tr:.0f} dias), pela media de "
          f"H2 ({dias_h2:.0f} dias); trecho NAO lido =====")
    for tf in por_tf:
        n1 = pega("UNIVERSO", tf, 1, "E0", "H2")
        n0 = pega("UNIVERSO", tf, 0, "E0", "H2")
        mes = 30.44 / dias_h2
        print(f"  {tf:>3}: c/ portao {n1['n'] * mes:>6.1f} trades/mes -> "
              f"~{n1['n'] * dias_tr / dias_h2:>5.0f} no trancado "
              f"({n1['n'] * dias_tr / dias_h2 / max(n1['tokens'], 1):.2f}/token)"
              f" | s/ portao {n0['n'] * mes:>6.1f}/mes -> "
              f"~{n0['n'] * dias_tr / dias_h2:>5.0f} "
              f"({n0['n'] * dias_tr / dias_h2 / max(n0['tokens'], 1):.2f}/token)")
    print("\ncaveat: EXPLORATORIO (camada 2), nada promove. Teto de leitura "
          f"{args.end}. 8h/12h/1D agregados em memoria dos 4h (blocos 00:00 "
          "UTC, incompletos fora). Portao 0.02/11 calibrado no 4h, aplicado sem "
          "recalibrar (celula pt=0 = desligado). Borda de 48 barras do tf corta "
          "o fim de H2 (1D: ~48 dias). Warmup de 89 barras do tf come o inicio "
          "de H1 nos tf longos (1D: ate ~set/2024). Metades por data de "
          "ENTRADA; mergulho sobre a curva de trades. @10 = @6 - 4n. Projecao "
          "do trancado = taxa de H2, nao leitura.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
