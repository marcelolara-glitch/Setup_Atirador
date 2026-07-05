# backtest/bracket.py — Setup Atirador v9 (BANCADA) — Estagio 0.5
# Resolucao EXATA barra-a-barra do primeiro toque (alvo T*ATR vs stop S*ATR)
# sobre a excursao no timeframe escolhido (--tf 15m/1h/4h, default 15m),
# usando o caminho fav/adv por barra exposto por measure_event. Ambiguidade
# residual existe APENAS quando alvo e stop caem na MESMA barra do tf ->
# resolvida pessimisticamente (-S) e reportada AMB_SB.
# Multi-custo numa unica execucao (default: 12 bps taker, 6 bps maker), com
# reconciliacao interna path x extremos que aborta se as reguas divergirem.
# Entradas TREND-aligned (mesma geracao do juice). Resultado positivo NAO e
# claim de edge (janela unica, 36 celulas sem correcao, simbolos
# correlacionados) — decide apenas o papel do 15m no Estagio 1.
# Roda na VM (cadeia pandas_ta via juice/regime); imports pesados sao lazy
# para permitir teste de first_touch no sandbox. NAO toca runtime.
# Uso: .venv/bin/python -m backtest.bracket --start 2026-05-15 --end 2026-06-15

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

GRID_S = (0.75, 1.0, 1.5)
GRID_T = (2.0, 3.0, 4.0, 6.0)
# H em BARRAS do tf; grade por tf cobre janelas de holding comparaveis
# (15m: 4h-12h | 1h: 8h-32h | 4h: 16h-64h). Chaves devem existir em HORIZONS.
GRID_H = {"15m": (16, 32, 48), "1h": (8, 16, 32), "4h": (4, 8, 16)}
SEED = 1337
CATS = ("ALVO", "STOP", "FLAT", "AMB_SB")


def first_touch(fav, adv, fwd_h, s, t, h):
    """Resolucao exata barra-a-barra, em ATR. Retorna (resultado, categoria).
    Ambiguidade residual = alvo e stop na MESMA barra -> pessimista (-s)."""
    for i in range(h):
        a, d = fav[i] >= t, adv[i] >= s
        if a and d:
            return -s, "AMB_SB"
        if d:
            return -s, "STOP"
        if a:
            return t, "ALVO"
    return fwd_h, "FLAT"


def run(store: str, symbols: list, start_iso: str, end_iso: str,
        tf: str = "15m") -> dict:
    """Por simbolo: timeline de regime no `tf` -> entradas TREND-aligned ->
    measure_event -> first_touch em cada celula S x T x H (H = GRID_H[tf]).
    Acumula retornos BRUTOS (fracao do nocional); custos so no main()."""
    from backtest.candle_store import connect as store_connect   # noqa: E402
    from backtest.excursion import _iso_to_ms, measure_event     # noqa: E402
    from backtest.juice import _regime_timeline, _trend_entries  # noqa: E402

    conn = store_connect(store)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)
    grid_h = GRID_H[tf]
    cells = {(s, t, h): {"ret": [], "cat": {c: 0 for c in CATS}}
             for s in GRID_S for t in GRID_T for h in grid_h}
    total = excluidos_borda = 0

    for sym in symbols:
        tl = _regime_timeline(conn, sym, tf, start_ms, end_ms)
        for ev in _trend_entries(tl, tf):
            m = measure_event(conn, sym, ev["bar_ts"], ev["direction"], tf=tf)
            if m is None:
                excluidos_borda += 1
                continue
            assert abs(max(0.0, max(m["fav"][:48])) - m["mfe"][48]) < 1e-9, \
                "path != extremos"
            total += 1
            atr_pct = m["atr"] / m["entry"]
            for (s, t, h), c in cells.items():
                res, cat = first_touch(m["fav"], m["adv"], m["fwd"][h],
                                       s, t, h)
                c["ret"].append(res * atr_pct)
                c["cat"][cat] += 1

    conn.close()
    return {"cells": cells, "total": total,
            "excluidos_borda": excluidos_borda}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Estagio 0.5 — primeiro toque exato sobre excursao por tf")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    ap.add_argument("--tf", choices=sorted(GRID_H), default="15m",
                    help="timeframe do instrumento (default: 15m)")
    ap.add_argument("--start", required=True, help="data ISO, ex. 2026-05-15")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-06-15")
    ap.add_argument("--cost-bps", nargs="+", type=float, default=[12.0, 6.0],
                    help="custos round-trip em bps (default: 12 taker, 6 maker)")
    args = ap.parse_args()

    from backtest.juice import _boot_mean   # noqa: E402
    from backtest.sweep import TIER1        # noqa: E402
    symbols = args.symbols if args.symbols else TIER1

    r = run(args.store, symbols, args.start, args.end, tf=args.tf)
    if r["total"] == 0:
        print("SEM ENTRADAS — veredito nao emitido")
        return 1

    print(f"\n===== BRACKET ESTAGIO 0.5 (primeiro toque exato, {args.tf}) =====")
    print(f"janela {args.start} -> {args.end} | {len(symbols)} simbolos | "
          f"entradas: {r['total']} | excluidos_borda: {r['excluidos_borda']}")

    stats = {}   # custo_bps -> {celula: (media, lo95, hi95, p)}
    for cost_bps in args.cost_bps:
        cost = cost_bps / 10000.0
        random.seed(SEED)   # derivabilidade entre custos preservada
        st = {}
        print(f"\n----- custo {cost_bps:.1f} bps -----")
        print(f"{'S':>5} {'T':>5} {'H':>3} | {'n':>5} | "
              f"{'%alvo':>6} {'%stop':>6} {'%flat':>6} {'%amb_sb':>7} | "
              f"{'EV(bps)':>8} {'lo95':>8} {'hi95':>8} {'p':>6}")
        for key in sorted(r["cells"]):
            c = r["cells"][key]
            n = len(c["ret"])
            st[key] = _boot_mean([x - cost for x in c["ret"]])
            m, lo, hi, p = st[key]
            pct = {cat: (c["cat"][cat] / n * 100 if n else 0.0) for cat in CATS}
            s, t, h = key
            print(f"{s:>5.2f} {t:>5.1f} {h:>3} | {n:>5} | "
                  f"{pct['ALVO']:>6.1f} {pct['STOP']:>6.1f} "
                  f"{pct['FLAT']:>6.1f} {pct['AMB_SB']:>7.1f} | "
                  f"{m * 10000:>8.1f} {lo * 10000:>8.1f} "
                  f"{hi * 10000:>8.1f} {p:>6.3f}")
        stats[cost_bps] = st
        print(f"custo {cost_bps:.1f}: celulas EV>0 = "
              f"{sum(1 for v in st.values() if v[0] > 0)} | lo95>0 = "
              f"{sum(1 for v in st.values() if v[1] > 0)}")

    print("\n===== VEREDITO (regras pre-registradas) =====")
    maker, taker = min(args.cost_bps), max(args.cost_bps)
    ev_maker = [k for k, v in stats[maker].items() if v[0] > 0]
    lo_maker = [k for k, v in stats[maker].items() if v[1] > 0]
    lo_taker = [k for k, v in stats[taker].items() if v[1] > 0]
    if not ev_maker:
        print(f"MORTO-15m: nenhuma celula com EV>0 no custo {maker:.0f} —"
              " convexidade intraday nao paga nem maker; Estagio 1"
              " exclusivamente 4h+.")
    elif lo_taker:
        print(f"CANDIDATO-TAKER: lo95>0 no custo {taker:.0f}. Celulas:")
        for k in lo_taker:
            print(f"  S={k[0]} T={k[1]} H={k[2]}")
    elif lo_maker:
        print(f"CANDIDATO-MAKER: lo95>0 no custo {maker:.0f} mas nenhuma no"
              f" {taker:.0f}. Celulas:")
        for k in lo_maker:
            print(f"  S={k[0]} T={k[1]} H={k[2]}")
    else:
        print(f"SEM CANDIDATO: EV>0 existe no custo {maker:.0f} mas nenhuma"
              " celula com lo95>0 em custo algum — tratar como MORTO-15m"
              " para o Estagio 1.")
    print("caveat: janela unica, 36 celulas sem correcao, simbolos"
          " correlacionados -> IC otimista; Estagio 1 fara nulo casado + OOS"
          " + bootstrap por bloco temporal.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
