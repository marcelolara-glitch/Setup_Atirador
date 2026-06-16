# backtest/juice.py — Setup Atirador v9 (BANCADA)
# Teste "tem suco?": expectativa APOS CUSTOS da captura de drift. Pergunta: estar
# do-lado-certo-de-uma-tendencia-forte, com saida SA, cobre taxa+slippage?
#
# Entradas = TREND_UP->long / TREND_DOWN->short, espacadas em SEP_BARS. E a MESMA
# populacao do nulo (random_detector com n gigante = TODAS as barras TREND), que
# ja provamos equivalente aos setups (sem edge de timing). Nao e setup: drift puro.
# Saida = TEMPORAL: segura H bars, sai a mercado. ZERO TP/SL -> nao da pra
# overfitar. Mede drift bruto sobre H bars vs custo. Se nem o bruto cobre o custo,
# nenhuma saida cria suco do nada -> arena morta. Compara contra BREAK-EVEN (zero).
#   .venv/bin/python -m backtest.juice --start 2026-04-26 --end 2026-06-05 [--cost-bps 12]

from __future__ import annotations

import argparse
import random
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import connect, read_candles            # noqa: E402
from backtest.excursion import HORIZONS                            # noqa: E402
from backtest.null_model import (                                  # noqa: E402
    regime_timeline, random_detector, _iso_to_ms)
from backtest.sweep import TIER1                                   # noqa: E402

SEED = 1337
N_BOOT = 1000
_ALL = 10 ** 9                     # n gigante -> random_detector pega TODAS as TREND


def juice_symbol(conn, symbol: str, start_ms: int, end_ms: int,
                 cost: float) -> dict:
    """H -> lista de retornos LIQUIDOS por trade: sign*(close[i+H]-close[i])/
    close[i] - cost. sign=+1 long, -1 short. Entradas espacadas (SEP_BARS) ->
    trades nao-sobrepostos p/ H<=48 -> independentes."""
    tl = regime_timeline(conn, symbol, start_ms, end_ms)
    events = random_detector(conn, symbol, tl, start_ms, end_ms, _ALL)
    cndl = read_candles(conn, symbol, "15m", end_ms=end_ms)
    idx = {c["ts"]: i for i, c in enumerate(cndl)}
    out = {h: [] for h in HORIZONS}
    for ev in events:
        i = idx.get(ev["bar_ts"])
        if i is None:
            continue
        c0 = cndl[i]["close"]
        if c0 <= 0:
            continue
        sign = 1.0 if ev["direction"] == "LONG" else -1.0
        for h in HORIZONS:
            j = i + h
            if j >= len(cndl):
                continue
            gross = sign * (cndl[j]["close"] - c0) / c0
            out[h].append(gross - cost)
    return out


def _boot_mean(vals: list, n_boot: int = N_BOOT):
    """(media, ic_lo, ic_hi, p_<=0) por bootstrap da media."""
    if not vals:
        return 0.0, 0.0, 0.0, 1.0
    k = len(vals)
    m = statistics.fmean(vals)
    means = []
    for _ in range(n_boot):
        s = [vals[random.randrange(k)] for _ in range(k)]
        means.append(statistics.fmean(s))
    means.sort()
    lo = means[int(0.025 * n_boot)]
    hi = means[int(0.975 * n_boot)]
    p_le0 = sum(1 for x in means if x <= 0.0) / n_boot
    return m, lo, hi, p_le0


def main() -> int:
    ap = argparse.ArgumentParser(description="teste tem-suco (drift apos custo)")
    ap.add_argument("--db", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--cost-bps", type=float, default=12.0,
                    help="custo round-trip em bps (default 12 = 0.12%%; OKX taker "
                         "~10 + slippage; funding e drag extra nao incluso)")
    args = ap.parse_args()

    random.seed(SEED)
    start_ms, end_ms = _iso_to_ms(args.start), _iso_to_ms(args.end)
    cost = args.cost_bps / 10000.0
    conn = connect(args.db)

    print(f"[juice] janela {args.start} -> {args.end} custo={args.cost_bps}bps "
          f"symbols={len(TIER1)}", file=sys.stderr)
    agg = {h: [] for h in HORIZONS}
    for sym in TIER1:
        rs = juice_symbol(conn, sym, start_ms, end_ms, cost)
        n0 = len(rs[HORIZONS[0]]) if HORIZONS else 0
        print(f"[juice] {sym}: trades={n0}", file=sys.stderr)
        for h in HORIZONS:
            agg[h].extend(rs[h])

    print("\n========== VEREDITO JUICE (drift-capture, saida temporal) ==========")
    print(f"custo round-trip: {args.cost_bps:.1f} bps | saida = segura H bars, "
          f"sai a mercado")
    print("retornos em % do nocional (sem alavancagem; alavancagem so escala)")
    print("--- expectativa por horizonte ---")
    print(f"{'H':>4} | {'trades':>6} | {'bruto%':>7} | {'liquido%':>8} | "
          f"{'win%':>5} | {'p(<=0)':>7}")
    for h in HORIZONS:
        vals = agg[h]
        m, lo, hi, p0 = _boot_mean(vals)
        gross = m + cost
        win = (sum(1 for v in vals if v > 0) / len(vals) * 100) if vals else 0.0
        print(f"{h:>4} | {len(vals):>6} | {gross * 100:>7.3f} | {m * 100:>8.3f} "
              f"| {win:>5.1f} | {p0:>7.3f}")
    print("[juice] liquido%>0 com p(<=0) baixo = tem suco (saida quebrada era o "
          "problema).")
    print("[juice] liquido%<=0 ou p alto = drift nao cobre custo -> arena cara, "
          "trocar arena.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
