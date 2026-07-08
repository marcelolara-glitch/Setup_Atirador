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
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PRIM_S, PRIM_T, PRIM_H = 1.5, 6.0, 4      # bracket primario
TEMP_H = 4                                 # temporal primario
COSTS = (12.0, 6.0, 0.0)                   # tabela informativa
GATE_COST = 6.0                            # custo dos gates
N_SHIFT = 1000                             # replicas do nulo circular
N_BLOCK = 2000                             # replicas do block bootstrap
BLOCK_DAYS = 14                            # bin de calendario
SEED = 1337


def shift_idx(i: int, offset: int, n: int) -> int:
    """Deslocamento circular de indice dentro de n barras elegiveis."""
    return (i + offset) % n


def bin_of(ts: int, t0: int, block_ms: int) -> int:
    """Bin de calendario do timestamp (ancorado em t0)."""
    return (ts - t0) // block_ms


def run(store, symbols, start_iso, end_iso, tf):
    """Pre-computa barras elegiveis (measure_event LONG) por simbolo, deriva os
    dois primarios por entrada TREND-aligned e roda os gates MONEY/SKILL."""
    from backtest.candle_store import connect                        # noqa: E402
    from backtest.excursion import _iso_to_ms, measure_event         # noqa: E402
    from backtest.juice import (_boot_mean, _regime_timeline,        # noqa: E402
                                 _trend_entries)
    from backtest.bracket import first_touch                         # noqa: E402
    conn = connect(store)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)
    block_ms = BLOCK_DAYS * 86_400_000
    gc = GATE_COST / 1e4

    def vals(entry, direction):
        """(temporal_ATR, bracket_ATR, atr_pct) da barra, simetria p/ SHORT."""
        atr_pct, fwd4, fav4, adv4 = entry
        if direction == "SHORT":
            fwd4, fav4, adv4 = -fwd4, adv4, fav4
        brk, _c = first_touch(fav4, adv4, fwd4, PRIM_S, PRIM_T, PRIM_H)
        return fwd4, brk, atr_pct
    elig, entries, excluida = {}, [], 0
    for sym in symbols:
        tl = _regime_timeline(conn, sym, tf, start_ms, end_ms)
        cache, tss = [], []
        for ts in sorted(tl):
            m = measure_event(conn, sym, ts, "LONG", tf=tf)
            if m is None:
                continue
            e = (m["atr"] / m["entry"], m["fwd"][TEMP_H],
                 m["fav"][:PRIM_H], m["adv"][:PRIM_H])
            if not cache:   # guarda de simetria na 1a barra elegivel do simbolo
                ms = measure_event(conn, sym, ts, "SHORT", tf=tf)
                assert abs(ms["fwd"][TEMP_H] + e[1]) < 1e-9 and all(
                    abs(a - b) < 1e-9 for a, b in zip(ms["fav"][:PRIM_H], e[3])
                ) and all(
                    abs(a - b) < 1e-9 for a, b in zip(ms["adv"][:PRIM_H], e[2])
                ), f"simetria quebrada em {sym}"
            cache.append(e)
            tss.append(ts)
        elig[sym] = cache
        pos = {t: i for i, t in enumerate(tss)}
        for ev in _trend_entries(tl, tf):
            i = pos.get(ev["bar_ts"])
            if i is None:
                excluida += 1
            else:
                entries.append((sym, ev["direction"], i, ev["bar_ts"]))
    conn.close()
    # estrategia: bruto em fracao do nocional (custo aplicado depois)
    ts_list, g_temp, g_brk, by_sym = [], [], [], {}
    for sym, d, i, ts in entries:
        fwd4, brk, ap = vals(elig[sym][i], d)
        ts_list.append(ts)
        g_temp.append(fwd4 * ap)
        g_brk.append(brk * ap)
        by_sym.setdefault(sym, []).append((d, i))
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
            for d, i in evs:
                fwd4, brk, ap = vals(cache[shift_idx(i, off, n)], d)
                rt.append(fwd4 * ap - gc)
                rb.append(brk * ap - gc)
        ge_t += statistics.fmean(rt) >= sm_temp
        ge_b += statistics.fmean(rb) >= sm_brk
    ps_temp = ge_t / N_SHIFT if entries else 1.0
    ps_brk = ge_b / N_SHIFT if entries else 1.0
    # tabela informativa (iid via _boot_mean) — apos gates p/ ordem RNG fixa
    table = []
    for name, gross in (("temporal", g_temp), ("bracket", g_brk)):
        for cost in COSTS:
            m, _lo, _hi, p = _boot_mean([g - cost / 1e4 for g in gross])
            table.append((name, cost, len(gross), m * 1e4, p))
    gates = [("temporal", sm_temp * 1e4, bp_temp * 1e4, bp_temp > 0,
              ps_temp, ps_temp < 0.025),
             ("bracket", sm_brk * 1e4, bp_brk * 1e4, bp_brk > 0,
              ps_brk, ps_brk < 0.025)]
    return {"tf": tf, "start": start_iso, "end": end_iso,
            "n_symbols": len(symbols), "n_entries": len(entries),
            "excluida": excluida, "table": table, "gates": gates}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Estagio 1 — nulo circular + block bootstrap sobre primarios")
    ap.add_argument("--tf", choices=["15m", "1h", "4h"], default="4h")
    ap.add_argument("--start", required=True, help="data ISO, ex. 2024-05-22")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-06-21")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    args = ap.parse_args()
    from backtest.sweep import TIER1        # noqa: E402
    symbols = args.symbols if args.symbols else TIER1
    random.seed(SEED)
    print(f"[stage1] tf={args.tf} janela {args.start}->{args.end} "
          f"symbols={len(symbols)}", file=sys.stderr)
    r = run(args.store, symbols, args.start, args.end, args.tf)

    print(f"\n===== STAGE1 (nulo circular + block bootstrap, {r['tf']}) =====")
    print(f"janela {r['start']} -> {r['end']} | {r['n_symbols']} simbolos | "
          f"entradas: {r['n_entries']} | excluidas_borda: {r['excluida']}")
    print(f"primarios: temporal H={TEMP_H} | "
          f"bracket S={PRIM_S} T={PRIM_T} H={PRIM_H}")
    print(f"\n{'primario':>9} | {'custo':>5} | {'n':>5} | {'EV(bps)':>8} | "
          f"{'iid_p':>6}   (tabela informativa)")
    for name, cost, n, ev, p in r["table"]:
        print(f"{name:>9} | {cost:>5.1f} | {n:>5} | {ev:>8.1f} | {p:>6.3f}")
    print(f"\n{'primario':>9} | {'EV@6(bps)':>9} | {'blockP2.5':>9} | "
          f"{'MONEY':>5} | {'p_shift':>7} | {'SKILL':>5}   (gates custo 6)")
    for name, ev6, bp, money, ps, skill in r["gates"]:
        print(f"{name:>9} | {ev6:>9.1f} | {bp:>9.1f} | "
              f"{('sim' if money else 'nao'):>5} | {ps:>7.3f} | "
              f"{('sim' if skill else 'nao'):>5}")
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
