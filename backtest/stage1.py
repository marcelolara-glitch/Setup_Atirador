# backtest/stage1.py — Setup Atirador v9 (BANCADA) — Estagio 1
# Julga os DOIS primarios pre-registrados no tf escolhido (default 4h): temporal
# (H=TEMP_H, saida a mercado) e bracket (S/T/H fixos). Dois gates com correcao x2
# (2 primarios) embutida nos limiares: MONEY = block bootstrap por bin de
# calendario, blockP2.5>0 (=> alpha 0.025); SKILL = nulo por deslocamento
# circular, p_shift<0.025 (preserva o tilt). A regua e measure_event (SHORT por
# simetria fwd->-fwd/fav<->adv, com guarda na 1a barra elegivel). No wrap do
# nulo, 1 par por simbolo/replica pode violar o espacamento (desprezivel).
# pandas_ta via juice/regime -> execucao so na VM; imports pesados lazy p/ os
# testes coletarem no sandbox. NAO toca runtime, NAO altera null_model.py.
# Uso: python -m backtest.stage1 --tf 4h --start ... --end ...
from __future__ import annotations
import argparse
import random
import statistics
import sys
from array import array
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import (EXT_CAP, NAT_CAP, WARMUP,  # noqa: E402
                                   extremos_entries, keepitsimple_entries)

PRIM_S, PRIM_T, PRIM_H = 1.5, 6.0, 4      # bracket primario
KIS_S, KIS_T, KIS_BRK_H = 1.5, 3.0, 24    # bracket pre-registrado do keepitsimple
KIS_SEP = 0                               # keepitsimple nao espaca entradas
GATE_MONEY_BPS, GATE_SKILL = 0.0, 0.025   # limiares dos irmaos (intocados)
KIS_MONEY_BPS, KIS_SKILL = 5.0, 0.005     # pedagio de multiplicidade (10/08)
TEMP_H = 4                                 # temporal primario
COSTS = (12.0, 6.0, 0.0)                   # tabela informativa
GATE_COST = 6.0                            # custo dos gates
N_SHIFT = 1000                             # replicas do nulo circular
N_BLOCK = 2000                             # replicas do block bootstrap
BLOCK_DAYS = 14                            # bin de calendario
SEED = 1337


def tsmom_entries(closes, tss, lookback, sep_bars, bar_ms):
    """Entradas TSMOM canonicas (pre-registro 08/07): sinal por barra =
    closes[i]/closes[i-lookback] - 1 (LONG>0, SHORT<0, ==0 sem sinal).
    Espacamento por direcao em TEMPO, identico a _trend_entries
    (last persiste entre clusters; 1a barra de cada troca entra se o gap
    da MESMA direcao permitir)."""
    last = {"LONG": None, "SHORT": None}
    out = []
    for i in range(lookback, len(closes)):
        r = closes[i] / closes[i - lookback] - 1.0
        if r == 0.0:
            continue
        d = "LONG" if r > 0 else "SHORT"
        ts = tss[i]
        if last[d] is None or ts - last[d] >= sep_bars * bar_ms:
            out.append({"bar_ts": ts, "direction": d})
            last[d] = ts
    return out


def donchian_entries(closes, tss, channel, sep_bars, bar_ms):
    """Entradas Donchian canonicas (pre-registro 14/07): na barra i,
    LONG se closes[i] > max das `channel` barras ANTERIORES; SHORT se
    < min delas; empate (== max ou == min) NAO e sinal (estrito).
    Espacamento por direcao em TEMPO, identico aos irmaos (last persiste)."""
    last = {"LONG": None, "SHORT": None}
    out = []
    for i in range(channel, len(closes)):
        win = closes[i - channel:i]
        if closes[i] > max(win):
            d = "LONG"
        elif closes[i] < min(win):
            d = "SHORT"
        else:
            continue
        ts = tss[i]
        if last[d] is None or ts - last[d] >= sep_bars * bar_ms:
            out.append({"bar_ts": ts, "direction": d})
            last[d] = ts
    return out


def shift_idx(i: int, offset: int, n: int) -> int:
    """Deslocamento circular de indice dentro de n barras elegiveis."""
    return (i + offset) % n


def bin_of(ts: int, t0: int, block_ms: int) -> int:
    """Bin de calendario do timestamp (ancorado em t0)."""
    return (ts - t0) // block_ms


def run(store, symbols, start_iso, end_iso, tf,
        detector="classifier", lookback=84, hold=48, channel=120):
    """Pre-computa barras elegiveis (measure_event LONG) por simbolo, deriva os
    dois primarios por entrada e roda os gates MONEY/SKILL. `detector` escolhe
    o gerador de entradas: classifier (regime TREND-aligned, intocado), tsmom
    (momentum time-series, pre-registro 08/07), donchian (rompimento de canal,
    pre-registro 14/07), keepitsimple (EMA 8/21; primarios `nativo` — saida
    quando o estado deixa VERDE/VERM, cap NAT_CAP — e `bracket` KIS_*) ou
    kis_extremos (mesma maquina de estados, primario UNICO `extremos`: ultimo
    estado extremo confirmado, saida por inversao, cap EXT_CAP, sem bracket)."""
    from backtest.candle_store import connect, read_candles          # noqa: E402
    from backtest.excursion import _iso_to_ms, measure_event         # noqa: E402
    from backtest.juice import (_boot_mean, _regime_timeline,        # noqa: E402
                                 _trend_entries, SEP_BARS, _BAR_MS)
    from backtest.bracket import first_touch                         # noqa: E402
    conn = connect(store)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)
    block_ms = BLOCK_DAYS * 86_400_000
    gc = GATE_COST / 1e4
    # perfil de horizontes: classifier mantem o pre-registro (H=4); tsmom usa
    # o hold escolhido nos dois primarios. S/T ficam PRIM_S/PRIM_T sempre.
    # kis_extremos compartilha a maquina de estados e a serie por barra do
    # keepitsimple, mas tem UM primario so (`extremos`, saida por inversao) e
    # nenhum bracket. Os gates ficam com o pedagio KIS_*, que ja embute a
    # correcao x2 de 2 primarios: com 1 primario, e conservador de proposito.
    ext = detector == "kis_extremos"
    kis = detector == "keepitsimple" or ext
    cap = EXT_CAP if ext else NAT_CAP
    temp_h = TEMP_H if detector == "classifier" else hold
    brk_h = KIS_BRK_H if kis else (PRIM_H if detector == "classifier" else hold)
    brk_s, brk_t = (KIS_S, KIS_T) if kis else (PRIM_S, PRIM_T)

    def vals(entry, direction, hold_bars=None):
        """(temporal_ATR, bracket_ATR, atr_pct) da barra, simetria p/ SHORT. No
        keepitsimple `fwdh` e a serie por barra: `hold_bars` (propriedade do
        sinal, viaja com ele no nulo) escolhe a saida do nativo; o FLAT do
        bracket continua no fechamento de brk_h.

        O clamp por len(fwdh) NAO e alcancavel pelo evento OBSERVADO: no
        keepitsimple hold_bars <= NAT_CAP == len(fwdh) por construcao, e no
        kis_extremos o evento cujo hold nao cabe no futuro disponivel e
        EXCLUIDO na coleta (`excluida_hold`) em vez de medido curto. Sobra o
        nulo, que desloca o sinal para uma barra arbitraria do simbolo: la o
        clamp ainda morde, limitado pelas `cache_curto` barras de fwd_bar
        truncado — o cabecalho reporta as duas contagens."""
        atr_pct, fwdh, favh, advh = entry
        temp, flat = ((fwdh[min(hold_bars, len(fwdh)) - 1], fwdh[brk_h - 1])
                      if kis else (fwdh, fwdh))
        if direction == "SHORT":
            temp, flat, favh, advh = -temp, -flat, advh, favh
        if ext:                 # UM primario: sai por inversao, sem bracket
            return temp, 0.0, atr_pct
        brk, _c = first_touch(favh, advh, flat, brk_s, brk_t, brk_h)
        return temp, brk, atr_pct
    elig, entries, excluida, autopsia = {}, [], 0, []
    excluida_hold = cache_curto = 0
    for sym in symbols:
        if detector == "classifier":
            tl = _regime_timeline(conn, sym, tf, start_ms, end_ms)
            cand_ts = sorted(tl)
            evs = _trend_entries(tl, tf)
        else:
            cndl = read_candles(conn, sym, tf, start_ms=start_ms, end_ms=end_ms)
            closes = [c["close"] for c in cndl]
            tss = [c["ts"] for c in cndl]
            if detector == "donchian":
                cand_ts = tss[channel:]
                evs = donchian_entries(closes, tss, channel,
                                       SEP_BARS, _BAR_MS[tf])
            elif kis:
                cand_ts = tss[WARMUP:]
                evs = (extremos_entries(cndl) if ext else
                       keepitsimple_entries(cndl, KIS_SEP, _BAR_MS[tf]))
            else:
                cand_ts = tss[lookback:]
                evs = tsmom_entries(closes, tss, lookback,
                                    SEP_BARS, _BAR_MS[tf])
        cache, cts = [], []
        for ts in cand_ts:
            m = measure_event(conn, sym, ts, "LONG", tf=tf)
            if m is None:
                continue
            # array('d') e nao lista. Medido no perfil TIER1 (20 simbolos x
            # ~4560 barras 4h): lista Python com fwd_bar[:256] pede 928 MiB de
            # cache, e a VM tem 956 MiB TOTAIS com o cron de producao na mesma
            # maquina — deficit garantido. array('d') guarda o MESMO C double
            # do float Python (zero perda de precisao) em 8 bytes crus, sem o
            # objeto float nem o ponteiro por elemento: 206 MiB, abaixo ate do
            # baseline de 374 MiB do keepitsimple. Os 4 usos do slot — indexar,
            # len(), zip() na guarda de simetria e negar o valor indexado no
            # SHORT — funcionam identicos. So o modo kis guarda serie; nos
            # irmaos o slot segue sendo o escalar m["fwd"][temp_h].
            e = (m["atr"] / m["entry"],
                 array("d", m["fwd_bar"][:cap]) if kis else m["fwd"][temp_h],
                 () if ext else m["fav"][:brk_h],   # extremos nao tem bracket:
                 () if ext else m["adv"][:brk_h])   # nao paga fav/adv no cache
            if not cache:   # guarda de simetria na 1a barra elegivel do simbolo
                ms = measure_event(conn, sym, ts, "SHORT", tf=tf)
                msf = ms["fwd_bar"][:cap] if kis else ms["fwd"][temp_h]
                assert (all(abs(a + b) < 1e-9 for a, b in zip(msf, e[1]))
                        if kis else abs(msf + e[1]) < 1e-9) and all(
                    abs(a - b) < 1e-9
                    for a, b in zip(ms["fav"][:brk_h], m["adv"][:brk_h])
                ) and all(
                    abs(a - b) < 1e-9
                    for a, b in zip(ms["adv"][:brk_h], m["fav"][:brk_h])
                ), f"simetria quebrada em {sym}"
            cache.append(e)
            cts.append(ts)
        elig[sym] = cache
        if ext:   # exposicao do nulo ao clamp: barras sem os EXT_CAP inteiros
            cache_curto += sum(1 for c in cache if len(c[1]) < EXT_CAP)
        pos = {t: i for i, t in enumerate(cts)}
        for ev in evs:
            i = pos.get(ev["bar_ts"])
            if i is None:
                excluida += 1
            elif ext and ev["hold"] > len(cache[i][1]):
                # A MESMA regra de borda direita ja travada, so que aplicada ao
                # horizonte REAL do evento em vez do fixo de 48: sem futuro
                # para o hold inteiro, medir seria fingir uma saida antecipada
                # — e o vies cairia justo nos holds longos, que num seguidor de
                # tendencia carregam o payoff. Excluir e a unica leitura honesta.
                excluida_hold += 1
            else:
                entries.append((sym, ev["direction"], i, ev["bar_ts"],
                                ev.get("hold")))
                if kis:
                    autopsia.append(dict(ev, symbol=sym))
    conn.close()
    # estrategia: bruto em fracao do nocional (custo aplicado depois)
    ts_list, g_temp, g_brk, by_sym = [], [], [], {}
    for sym, d, i, ts, hb in entries:
        fwd4, brk, ap = vals(elig[sym][i], d, hb)
        ts_list.append(ts)
        g_temp.append(fwd4 * ap)
        g_brk.append(brk * ap)
        by_sym.setdefault(sym, []).append((d, i, hb))
    t0 = min(ts_list) if ts_list else 0

    def block_p25(gross):   # MONEY: reamostra bins de calendario c/ reposicao
        bins = {}
        for ts, g in zip(ts_list, gross):
            bins.setdefault(bin_of(ts, t0, block_ms), []).append(g - gc)
        keys = list(bins.values())
        nb = len(keys)
        if nb == 0:
            return 0.0
        means = []
        for _ in range(N_BLOCK):
            pool = []
            for _ in range(nb):
                pool.extend(keys[random.randrange(nb)])
            means.append(statistics.fmean(pool))
        means.sort()
        return means[int(0.025 * N_BLOCK)]
    bp_temp, bp_brk = block_p25(g_temp), block_p25(g_brk)
    # nulo circular (SKILL): offset por simbolo/replica, direcao preservada
    sm_temp = statistics.fmean([g - gc for g in g_temp]) if g_temp else 0.0
    sm_brk = statistics.fmean([g - gc for g in g_brk]) if g_brk else 0.0
    ge_t = ge_b = 0
    for _ in range(N_SHIFT if entries else 0):
        rt, rb = [], []
        for sym, evs in by_sym.items():
            cache = elig[sym]
            n = len(cache)
            off = random.randrange(n)
            for d, i, hb in evs:
                fwd4, brk, ap = vals(cache[shift_idx(i, off, n)], d, hb)
                rt.append(fwd4 * ap - gc)
                rb.append(brk * ap - gc)
        ge_t += statistics.fmean(rt) >= sm_temp
        ge_b += statistics.fmean(rb) >= sm_brk
    ps_temp = ge_t / N_SHIFT if entries else 1.0
    ps_brk = ge_b / N_SHIFT if entries else 1.0
    # tabela informativa (iid via _boot_mean) — apos gates p/ ordem RNG fixa
    table = []
    tname = ("extremos" if ext else "nativo") if kis else "temporal"
    # bp_brk/ps_brk seguem CALCULADOS mesmo no extremos — mexer nisso mudaria a
    # ordem de consumo do RNG e quebraria a regressao dos irmaos. O que muda e
    # so o que entra no relatorio: com UM primario, nao ha linha de bracket.
    prims = [(tname, g_temp)] if ext else [(tname, g_temp), ("bracket", g_brk)]
    for name, gross in prims:
        for cost in COSTS:
            m, _lo, _hi, p = _boot_mean([g - cost / 1e4 for g in gross])
            table.append((name, cost, len(gross), m * 1e4, p))
    g_money, g_skill = ((KIS_MONEY_BPS, KIS_SKILL) if kis
                        else (GATE_MONEY_BPS, GATE_SKILL))
    gates = [(tname, sm_temp * 1e4, bp_temp * 1e4, bp_temp * 1e4 > g_money,
              ps_temp, ps_temp < g_skill)]
    if not ext:
        gates.append(("bracket", sm_brk * 1e4, bp_brk * 1e4,
                      bp_brk * 1e4 > g_money, ps_brk, ps_brk < g_skill))
    return {"tf": tf, "start": start_iso, "end": end_iso,
            "n_symbols": len(symbols), "n_entries": len(entries),
            "excluida": excluida, "table": table, "gates": gates,
            "excluida_hold": excluida_hold, "cache_curto": cache_curto,
            "detector": detector, "lookback": lookback, "hold": hold,
            "channel": channel, **({"autopsia": autopsia} if kis else {})}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Estagio 1 — nulo circular + block bootstrap sobre primarios")
    ap.add_argument("--tf", choices=["15m", "1h", "4h"], default="4h")
    ap.add_argument("--start", required=True, help="data ISO, ex. 2024-05-22")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-06-21")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    ap.add_argument("--detector",
                    choices=["classifier", "tsmom", "donchian", "keepitsimple",
                             "kis_extremos"],
                    default="classifier")
    ap.add_argument("--lookback", type=int, choices=[42, 84, 168], default=84,
                    help="efeito apenas com --detector tsmom")
    ap.add_argument("--hold", type=int, choices=[16, 32, 48], default=48,
                    help="efeito apenas com --detector tsmom")
    ap.add_argument("--channel", type=int, choices=[42, 120, 240], default=120,
                    help="efeito apenas com --detector donchian")
    args = ap.parse_args()
    from backtest.sweep import TIER1        # noqa: E402
    symbols = args.symbols if args.symbols else TIER1
    random.seed(SEED)
    if args.detector == "tsmom":
        suffix = f" detector=tsmom L={args.lookback} H={args.hold}"
    elif args.detector == "donchian":
        suffix = f" detector=donchian N={args.channel} H={args.hold}"
    elif args.detector == "keepitsimple":
        suffix = " detector=keepitsimple EMA 8/21"
    elif args.detector == "kis_extremos":
        suffix = " detector=kis_extremos EMA 8/21 conf>=2"
    else:
        suffix = ""
    print(f"[stage1] tf={args.tf} janela {args.start}->{args.end} "
          f"symbols={len(symbols)}{suffix}", file=sys.stderr)
    r = run(args.store, symbols, args.start, args.end, args.tf,
            detector=args.detector, lookback=args.lookback, hold=args.hold,
            channel=args.channel)

    print(f"\n===== STAGE1 (nulo circular + block bootstrap, {r['tf']}) =====")
    print(f"janela {r['start']} -> {r['end']} | {r['n_symbols']} simbolos | "
          f"entradas: {r['n_entries']} | excluidas_borda: {r['excluida']}"
          + (f" | excluidas_hold: {r['excluida_hold']}"
             if r["detector"] == "kis_extremos" else ""))
    if r["detector"] == "kis_extremos":
        tot = r["n_entries"] + r["excluida_hold"]
        pct = 100.0 * r["excluida_hold"] / tot if tot else 0.0
        print(f"excluidas_hold = eventos cujo hold NAO cabe no futuro "
              f"disponivel do store ({pct:.1f}% de {tot}); e a borda direita "
              f"aplicada ao horizonte real do evento, nao ao fixo de 48. "
              + ("ACIMA DE 3%: PARAR, EXT_CAP pode estar curto."
                 if pct > 3.0 else "Abaixo do teto de 3%."))
        print(f"  barras do cache com fwd_bar < {EXT_CAP}: {r['cache_curto']} "
              f"— exposicao residual do NULO ao clamp (ver nota em vals)")
    if r["detector"] == "donchian":
        print(f"primarios: DONCHIAN N={r['channel']} | temporal H={r['hold']} | "
              f"bracket S={PRIM_S} T={PRIM_T} H={r['hold']}")
    elif r["detector"] == "tsmom":
        print(f"primarios: TSMOM L={r['lookback']} | temporal H={r['hold']} | "
              f"bracket S={PRIM_S} T={PRIM_T} H={r['hold']}")
    elif r["detector"] == "keepitsimple":
        print(f"primarios: KEEPITSIMPLE EMA 8/21 | nativo saida-por-estado "
              f"H<={NAT_CAP} | bracket S={KIS_S} T={KIS_T} H={KIS_BRK_H}")
    elif r["detector"] == "kis_extremos":
        print(f"primario UNICO: KIS_EXTREMOS EMA 8/21 | ultimo estado EXTREMO "
              f"confirmado (>=2 barras), stop-and-reverse | saida por inversao "
              f"H<={EXT_CAP} | SEM bracket")
    else:
        print(f"primarios: temporal H={TEMP_H} | "
              f"bracket S={PRIM_S} T={PRIM_T} H={PRIM_H}")
    print(f"\n{'primario':>9} | {'custo':>5} | {'n':>5} | {'EV(bps)':>8} | "
          f"{'iid_p':>6}   (tabela informativa)")
    for name, cost, n, ev, p in r["table"]:
        print(f"{name:>9} | {cost:>5.1f} | {n:>5} | {ev:>8.1f} | {p:>6.3f}")
    gm, gs = ((KIS_MONEY_BPS, KIS_SKILL)
              if r["detector"] in ("keepitsimple", "kis_extremos")
              else (GATE_MONEY_BPS, GATE_SKILL))
    print(f"\n{'primario':>9} | {'EV@6(bps)':>9} | {'blockP2.5':>9} | "
          f"{'MONEY':>5} | {'p_shift':>7} | {'SKILL':>5}   (gates custo 6 | "
          f"MONEY: blockP2.5 > +{gm:.1f} bps | SKILL: p_shift < {gs:.3f})")
    for name, ev6, bp, money, ps, skill in r["gates"]:
        print(f"{name:>9} | {ev6:>9.1f} | {bp:>9.1f} | "
              f"{('sim' if money else 'nao'):>5} | {ps:>7.3f} | "
              f"{('sim' if skill else 'nao'):>5}")
    if r.get("autopsia"):   # payload descritivo — NAO entra em decisao alguma
        a = r["autopsia"]
        sub = [x["forca_subindo"] for x in a if x["forca_subindo"] is not None]
        bw = [x["bb_width_rel"] for x in a if x["bb_width_rel"] is not None]
        dirs = Counter(x["direction"] for x in a)
        print(f"\nautopsia (descritiva, nao decisional): "
              f"LONG={dirs['LONG']} SHORT={dirs['SHORT']} | hold_mediano="
              f"{statistics.median(x['hold'] for x in a):.0f} | forca_subindo="
              f"{100 * sum(sub) / len(sub) if sub else 0:.0f}% (n={len(sub)}) |"
              f" bb_width_rel_med={statistics.median(bw) if bw else 0:.4f} | "
              f"origem={Counter(x['estado_origem'] for x in a).most_common()}")
        if r["detector"] == "kis_extremos":
            trunc = sum(1 for x in a if x.get("hold_truncado"))
            longos = Counter(x["symbol"] for x in a if x["hold"] > NAT_CAP)
            print(f"  hold_max={max(x['hold'] for x in a)} | "
                  f"truncados_no_teto_{EXT_CAP}={trunc} — esperado ~0; "
                  f"contagem alta = teto curto demais, nao resultado")
            print(f"  hold>{NAT_CAP} (entradas que SO existem pelo horizonte "
                  f"estendido): {sum(longos.values())} de {len(a)} | "
                  f"por simbolo: {longos.most_common()}")
    exploratorio = (
        (r["detector"] == "tsmom" and (r["lookback"], r["hold"]) != (84, 48))
        or (r["detector"] == "donchian"
            and (r["channel"], r["hold"]) != (120, 48)))
    if exploratorio:
        if r["detector"] == "donchian":
            cell, preg = f"N={r['channel']}, H={r['hold']}", "14/07"
        else:
            cell, preg = f"L={r['lookback']}, H={r['hold']}", "08/07"
        print(f"\nEXPLORATORIO ({cell}): celulas nao promoviveis "
              f"(pre-registro {preg}) — gates acima sao informativos.")
    else:
        forte = [g[0] for g in r["gates"] if g[3] and g[5]]
        money = [g[0] for g in r["gates"] if g[3]]
        print("\n===== VEREDITO (regras pre-registradas) =====")
        if forte:
            print(f"FORTE: {', '.join(forte)} com MONEY e SKILL — edge apos custo, "
                  "presente no timing.")
        elif money:
            print(f"BETA: {', '.join(money)} com MONEY, nenhum com SKILL — avanca "
                  "com anotacao: valor no tilt/regime, nao no timing.")
        else:
            print("MORTO: nenhum primario com MONEY.")
    print("caveat: agregado 2a (OOS ja aprovado no gate anterior); correlacao "
          "cross-symbol tratada por bins de calendario; skill-null preserva "
          "tilt.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
