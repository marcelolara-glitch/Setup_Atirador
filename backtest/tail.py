# backtest/tail.py — Setup Atirador v9 (BANCADA)
"""Estagio 0 — viabilidade de exit convexo (bracket) sobre excursao 15m.

Um exit convexo (stop fixo -S*ATR, alvo +T*ATR, senao sai no fim do horizonte)
sobre entradas TREND cobre o custo? Usa SO os extremos {MFE, MAE} e o retorno
terminal `ret` que excursion.measure_event ja produz — nao o caminho. Por isso
NAO e um trailing fiel e nao se sabe a ORDEM em que alvo/stop foram tocados quando
ambos caem na janela; o modulo calcula dois BOUNDS, nao um numero: otimista (alvo
primeiro, +T) e pessimista (stop primeiro, -S). O desfecho fiel cai entre eles.
15m-only: horizontes de HORIZONS em barras de 15m. Custo = round-trip UNICO no
desfecho (o exit real tem custo assimetrico por perna — diferido). E um VETO
BARATO antes do sim de caminho caro: se nem o bound otimista cobre o custo, morre.
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import connect                                # noqa: E402
from backtest.excursion import HORIZONS, measure_event, collapse_events  # noqa: E402
from backtest.null_model import regime_timeline, _iso_to_ms             # noqa: E402
from backtest.sweep import TIER1                                         # noqa: E402
from backtest.juice import _boot_mean   # reuso do bootstrap do juice    # noqa: E402

_SEED = 1337
_DIR = {"TREND_UP": "LONG", "TREND_DOWN": "SHORT"}
SEP_BARS = max(HORIZONS)   # 48 — janelas forward nao sobrepostas
_QS = [("p10", .10), ("p25", .25), ("p50", .50), ("p75", .75), ("p90", .90),
       ("p99", .99)]   # verbosidade enxuta (p95 cortado p/ caber no budget)


def _trend_entries(timeline: dict) -> list:
    """TREND_UP->LONG / TREND_DOWN->SHORT, colapsadas em SEP_BARS barras 15m por
    direcao (mantem o 1o de cada cluster). Espelha o juice via a timeline 15m
    publica e no-lookahead do null_model."""
    sigs = sorted(({"bar_ts": ts, "direction": _DIR[r]}
                   for ts, r in timeline.items() if r in _DIR),
                  key=lambda s: s["bar_ts"])
    return collapse_events(sigs, SEP_BARS)


def _bracket_outcome(mfe, mae, ret, S, T, optimistic):
    """Tudo em multiplos de ATR. Stop -S, alvo +T, senao sai no horizonte (ret).
       Quando alvo E stop sao tocados: optimistic=alvo primeiro (+T), senao -S."""
    tgt, stp = mfe >= T, mae >= S
    if tgt and stp:   return  T if optimistic else -S
    if tgt:           return  T
    if stp:           return -S
    return ret


def _pct(sorted_vals: list, q: float) -> float:
    """Percentil q (0..1) por indice direto sobre lista ja ordenada (sem numpy)."""
    if not sorted_vals:
        return float("nan")
    return sorted_vals[int(q * (len(sorted_vals) - 1))]


def collect(conn, symbols: list, start_ms: int, end_ms: int) -> list:
    """Por simbolo: timeline 15m -> entradas TREND -> measure_event (descarta
    None/entry<=0). Retorna [{mfe{H}, mae{H}, ret{H}, atr_pct}]."""
    events: list = []
    for sym in symbols:
        timeline = regime_timeline(conn, sym, start_ms, end_ms)
        kept = 0
        for ev in _trend_entries(timeline):
            m = measure_event(conn, sym, ev["bar_ts"], ev["direction"])
            if m is None or m["entry"] <= 0:
                continue
            events.append({"mfe": m["mfe"], "mae": m["mae"], "ret": m["ret"],
                           "atr_pct": m["atr"] / m["entry"]})
            kept += 1
        print(f"[tail] {sym}: timeline={len(timeline)} eventos={kept}",
              file=sys.stderr)
    return events


def _print_distribution(events: list) -> None:
    """Saida A: percentis + max de MFE/MAE/ret (ATR) por horizonte, com N."""
    print("\n--- distribuicao por horizonte (ATR) ---")
    head = " | ".join(f"{lbl:>6}" for lbl, _ in _QS)
    print(f"{'kind':>4} | {'H':>4} | {'N':>5} | {head} | {'max':>6}")
    for kind in ("mfe", "mae", "ret"):
        for h in HORIZONS:
            vals = sorted(e[kind][h] for e in events)
            cells = " | ".join(f"{_pct(vals, q):>6.2f}" for _, q in _QS)
            mx = vals[-1] if vals else float("nan")
            print(f"{kind:>4} | {h:>4} | {len(vals):>5} | {cells} | {mx:>6.2f}")


def _bracket_grid(events: list, horizon: int, stops: list, targets: list,
                  cost_frac: float) -> None:
    """Saida C: expectativa liquida (%) nos dois bounds, por par (S,T) no
    horizonte fixo, com IC95 e p(<=0) do bootstrap. Imprime o veredito."""
    print(f"\n--- bracket @ H={horizon} (custo={cost_frac * 10000:.1f}bps) ---")
    print(f"{'bound':>5} | {'T/S':>9} | {'trades':>6} | {'win%':>5} | "
          f"{'liq%':>7} | {'ic95(%)':>17} | {'p(<=0)':>7}")
    any_opt_pos = any_pes_pos = False
    for S in stops:
        for T in targets:
            for optimistic in (True, False):
                nets = [_bracket_outcome(e["mfe"][horizon], e["mae"][horizon],
                                         e["ret"][horizon], S, T, optimistic)
                        * e["atr_pct"] - cost_frac for e in events]
                m, lo, hi, p0 = _boot_mean(nets)
                win = (sum(1 for v in nets if v > 0) / len(nets) * 100) if nets else 0.0
                any_opt_pos |= optimistic and m > 0
                any_pes_pos |= (not optimistic) and m > 0
                print(f"{'otim' if optimistic else 'pess':>5} | "
                      f"{T:>4.1f}/{S:>4.1f} | {len(nets):>6} | {win:>5.1f} | "
                      f"{m * 100:>7.3f} | [{lo * 100:>6.3f},{hi * 100:>6.3f}] | "
                      f"{p0:>7.3f}")
    print()
    if not any_opt_pos:
        print("[tail] VEREDITO: convexidade nao cobre custo nem no melhor caso "
              "(15m) -> descartar exit convexo.")
    elif any_pes_pos:
        print("[tail] VEREDITO: robusto (pessimista>0 em algum par) -> seguir.")
    else:
        print("[tail] VEREDITO: inconclusivo -> exige sim de caminho fiel "
              "(PR posterior).")


def _floats(s: str) -> list:
    return [float(x) for x in s.split()]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Estagio 0 — bracket convexo (bounds) sobre excursao 15m")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=TIER1)
    ap.add_argument("--start", required=True, help="data ISO, ex. 2026-05-15")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-05-22")
    ap.add_argument("--cost-bps", type=float, default=12.0,
                    help="custo round-trip em bps (default 12; passe 4 p/ maker)")
    ap.add_argument("--horizon", type=int, default=max(HORIZONS),
                    help="horizonte do bracket (barra 15m)")
    ap.add_argument("--stops", default="0.5 1.0 1.5", help="grade de S (ATR)")
    ap.add_argument("--targets", default="2 3 4 5", help="grade de T (ATR)")
    args = ap.parse_args()

    stops, targets = _floats(args.stops), _floats(args.targets)
    if args.horizon not in HORIZONS:
        print(f"[tail] AVISO: horizon={args.horizon} fora de HORIZONS={HORIZONS}; "
              f"measure_event so popula esses.", file=sys.stderr)
    cost_frac = args.cost_bps / 10000.0

    print(f"[tail] store={args.store} {args.start}->{args.end} "
          f"custo={args.cost_bps}bps horizon={args.horizon} stops={stops} "
          f"targets={targets} symbols={len(args.symbols)}", file=sys.stderr)

    random.seed(_SEED)
    conn = connect(args.store)
    start_ms, end_ms = _iso_to_ms(args.start), _iso_to_ms(args.end)
    events = collect(conn, args.symbols, start_ms, end_ms)
    conn.close()

    print("\n========== TAIL — bracket convexo sobre excursao 15m ==========")
    print(f"eventos TREND medidos: {len(events)}")
    if not events:
        print("[tail] sem eventos — nada a julgar.")
        return 0
    _print_distribution(events)
    _bracket_grid(events, args.horizon, stops, targets, cost_frac)
    return 0


if __name__ == "__main__":
    sys.exit(main())
