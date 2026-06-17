# backtest/juice.py — Setup Atirador v9 (BANCADA) — multi-timeframe
# Teste "tem suco?": expectativa APOS CUSTOS da captura de drift, por timeframe
# (15m/1h/4h). Mede se estar do-lado-certo de uma tendencia forte, segurando H
# barras e saindo a mercado, cobre taxa+slippage. Compara contra BREAK-EVEN.
# Entradas = TREND_UP->long / TREND_DOWN->short, espacadas em SEP_BARS barras
# DAQUELE TF (espacamento TF-aware, nao 15m fixo). Saida temporal, zero TP/SL ->
# nao overfita. Reusa SO classify_regime (agnostico a TF); o resto e TF-aware aqui
# pra nao herdar o BAR_15M_MS fixo do collapse_events.
# Checagem de regressao: --tf 15m deve reproduzir os numeros do juice antigo.
#   .venv/bin/python -m backtest.juice --tf 1h --start ... --end ... [--cost-bps 12]

from __future__ import annotations

import argparse
import random
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from exchanges import klines_to_dataframe                          # noqa: E402
from regime import MIN_CANDLES, classify_regime                    # noqa: E402
from backtest.candle_store import connect, read_candles            # noqa: E402
from backtest.excursion import HORIZONS                            # noqa: E402
from backtest.null_model import _iso_to_ms                         # noqa: E402
from backtest.sweep import TIER1                                   # noqa: E402

SEED = 1337
N_BOOT = 1000
_WINDOW = 150                      # velas p/ classificar regime (= null_model)
SEP_BARS = max(HORIZONS)           # 48 — separacao p/ forward nao sobreposto
_BAR_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
_DIR = {"TREND_UP": "LONG", "TREND_DOWN": "SHORT"}


def _regime_timeline(conn, symbol: str, tf: str, start_ms: int,
                     end_ms: int) -> dict:
    """bar_ts -> regime p/ cada barra do TF em [start,end], classificada sobre a
    janela de _WINDOW velas terminando na barra (no-lookahead). Mesmo
    classify_regime da producao; so muda o TF das velas."""
    allc = read_candles(conn, symbol, tf, end_ms=end_ms)
    out: dict = {}
    for i, c in enumerate(allc):
        ts = c["ts"]
        if ts < start_ms or ts > end_ms:
            continue
        win = allc[max(0, i - _WINDOW + 1): i + 1]
        if len(win) < MIN_CANDLES:
            continue
        try:
            out[ts] = classify_regime(klines_to_dataframe(win)).regime
        except ValueError:
            continue
    return out


def _trend_entries(timeline: dict, tf: str) -> list:
    """TREND_UP->LONG / TREND_DOWN->SHORT, espacadas em SEP_BARS barras DO TF
    (intervalo por tempo com a duracao certa da barra). Mantem o 1o de cada
    cluster por direcao -> trades nao-sobrepostos p/ H<=SEP_BARS."""
    sep_ms = SEP_BARS * _BAR_MS[tf]
    cands = sorted(
        ({"bar_ts": ts, "direction": _DIR[reg]}
         for ts, reg in timeline.items() if reg in _DIR),
        key=lambda s: s["bar_ts"])
    last: dict = {}
    out = []
    for s in cands:
        d = s["direction"]
        prev = last.get(d)
        if prev is not None and (s["bar_ts"] - prev) < sep_ms:
            continue
        last[d] = s["bar_ts"]
        out.append(s)
    return out


def juice_symbol(conn, symbol: str, tf: str, start_ms: int, end_ms: int,
                 cost: float) -> dict:
    """H -> retornos LIQUIDOS por trade no TF: sign*(close[i+H]-close[i])/close[i]
    - cost. Entradas espacadas em SEP_BARS barras do TF -> nao-sobrepostas."""
    tl = _regime_timeline(conn, symbol, tf, start_ms, end_ms)
    events = _trend_entries(tl, tf)
    cndl = read_candles(conn, symbol, tf, end_ms=end_ms)
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
    ap.add_argument("--tf", choices=["15m", "1h", "4h"], default="15m")
    ap.add_argument("--db", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--cost-bps", type=float, default=12.0,
                    help="custo round-trip em bps (default 12 = 0.12%%)")
    args = ap.parse_args()

    random.seed(SEED)
    start_ms, end_ms = _iso_to_ms(args.start), _iso_to_ms(args.end)
    cost = args.cost_bps / 10000.0
    conn = connect(args.db)
    hold = {h: h * _BAR_MS[args.tf] // 3_600_000 for h in HORIZONS}  # horas

    print(f"[juice] tf={args.tf} janela {args.start}->{args.end} "
          f"custo={args.cost_bps}bps symbols={len(TIER1)}", file=sys.stderr)
    agg = {h: [] for h in HORIZONS}
    for sym in TIER1:
        rs = juice_symbol(conn, sym, args.tf, start_ms, end_ms, cost)
        n0 = len(rs[HORIZONS[0]]) if HORIZONS else 0
        print(f"[juice] {sym}: trades={n0}", file=sys.stderr)
        for h in HORIZONS:
            agg[h].extend(rs[h])

    print(f"\n===== VEREDITO JUICE tf={args.tf} (drift-capture, saida temporal) =====")
    print(f"custo round-trip: {args.cost_bps:.1f} bps")
    print("retornos em % do nocional (sem alavancagem)")
    print(f"{'H':>4} | {'hold_h':>6} | {'trades':>6} | {'bruto%':>7} | "
          f"{'liquido%':>8} | {'win%':>5} | {'p(<=0)':>7}")
    for h in HORIZONS:
        vals = agg[h]
        m, lo, hi, p0 = _boot_mean(vals)
        gross = m + cost
        win = (sum(1 for v in vals if v > 0) / len(vals) * 100) if vals else 0.0
        print(f"{h:>4} | {hold[h]:>6} | {len(vals):>6} | {gross*100:>7.3f} | "
              f"{m*100:>8.3f} | {win:>5.1f} | {p0:>7.3f}")
    print("[juice] liquido%>0 com p(<=0) baixo = tem suco nesse TF.")
    print("[juice] liquido%<=0 ou p alto = drift nao cobre custo nesse TF.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
