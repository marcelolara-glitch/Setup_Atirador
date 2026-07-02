# backtest/bracket.py — Setup Atirador v9 (BANCADA) — Estagio 0
# Viabilidade de exit convexo (bracket) sobre a excursao 15m: stop fixo S*ATR,
# alvo T*ATR, senao saida a mercado no horizonte H. Entradas TREND-aligned
# (mesma geracao do juice). measure_event guarda so os extremos {MFE, MAE},
# nao o caminho -> quando alvo E stop sao tocados no mesmo horizonte a ordem
# e desconhecida: este modulo calcula BOUNDS (otimista/pessimista). 15m-only,
# por design: veto barato antes de construir simulador de caminho fiel.
# Resultado positivo NAO e claim de edge (sem nulo, sem OOS, 36 celulas sem
# correcao de multiplos testes) — apenas autoriza o Estagio 1.
# Roda na VM (cadeia pandas_ta via juice/regime); imports pesados sao lazy
# para permitir teste de resolve_bracket no sandbox. NAO toca runtime.
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
GRID_H = (16, 32, 48)   # 4h, 8h, 12h — H de 4/8 barras nao expressa convexidade
SEED = 1337
CATS = ("ALVO", "STOP", "FLAT", "AMBIGUO")


def resolve_bracket(mfe: float, mae: float, fwd: float, s: float, t: float):
    """Tudo em unidades de ATR. Retorna (otimista, pessimista, categoria)."""
    alvo, stop = mfe >= t, mae >= s
    if alvo and stop:
        return t, -s, "AMBIGUO"    # ordem desconhecida -> bounds
    if alvo:
        return t, t, "ALVO"
    if stop:
        return -s, -s, "STOP"
    return fwd, fwd, "FLAT"        # sai no horizonte; -s < fwd < t garantido


def run(store: str, symbols: list, start_iso: str, end_iso: str,
        cost: float) -> dict:
    """Por simbolo: timeline de regime 15m -> entradas TREND-aligned ->
    measure_event -> resolve_bracket em cada celula S x T x H. Acumula
    retornos LIQUIDOS por bound (fracao do nocional) e categorias."""
    from backtest.candle_store import connect as store_connect   # noqa: E402
    from backtest.excursion import _iso_to_ms, measure_event     # noqa: E402
    from backtest.juice import _regime_timeline, _trend_entries  # noqa: E402

    conn = store_connect(store)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)
    cells = {(s, t, h): {"ot": [], "pe": [], "cat": {c: 0 for c in CATS}}
             for s in GRID_S for t in GRID_T for h in GRID_H}
    total = excluidos_borda = 0

    for sym in symbols:
        tl = _regime_timeline(conn, sym, "15m", start_ms, end_ms)
        for ev in _trend_entries(tl, "15m"):
            m = measure_event(conn, sym, ev["bar_ts"], ev["direction"])
            if m is None:
                excluidos_borda += 1
                continue
            total += 1
            atr_pct = m["atr"] / m["entry"]
            for (s, t, h), c in cells.items():
                ot, pe, cat = resolve_bracket(
                    m["mfe"][h], m["mae"][h], m["fwd"][h], s, t)
                c["ot"].append(ot * atr_pct - cost)
                c["pe"].append(pe * atr_pct - cost)
                c["cat"][cat] += 1

    conn.close()
    return {"cells": cells, "total": total,
            "excluidos_borda": excluidos_borda}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Estagio 0 — bounds de exit convexo sobre excursao 15m")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    ap.add_argument("--start", required=True, help="data ISO, ex. 2026-05-15")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-06-15")
    ap.add_argument("--cost-bps", type=float, default=12.0,
                    help="custo round-trip em bps (default 12 = 0.12%%)")
    args = ap.parse_args()

    from backtest.juice import _boot_mean   # noqa: E402
    from backtest.sweep import TIER1        # noqa: E402
    symbols = args.symbols if args.symbols else TIER1

    random.seed(SEED)
    cost = args.cost_bps / 10000.0
    r = run(args.store, symbols, args.start, args.end, cost)

    print("\n===== BRACKET ESTAGIO 0 (bounds sobre excursao 15m) =====")
    print(f"janela {args.start} -> {args.end} | custo {args.cost_bps:.1f} bps | "
          f"{len(symbols)} simbolos")
    print(f"entradas medidas: {r['total']} | "
          f"excluidos_borda: {r['excluidos_borda']}")
    print(f"\n{'S':>5} {'T':>5} {'H':>3} | {'n':>5} | "
          f"{'%alvo':>5} {'%stop':>5} {'%flat':>5} {'%amb':>5} | "
          f"{'EVot(bps)':>9} {'p_ot':>5} | {'EVpe(bps)':>9} {'p_pe':>5}")

    stats = {}
    for key in sorted(r["cells"]):
        c = r["cells"][key]
        n = len(c["ot"])
        stats[key] = {"ot": _boot_mean(c["ot"]), "pe": _boot_mean(c["pe"])}
        m_ot, _, _, p_ot = stats[key]["ot"]
        m_pe, _, _, p_pe = stats[key]["pe"]
        pct = {cat: (c["cat"][cat] / n * 100 if n else 0.0) for cat in CATS}
        s, t, h = key
        print(f"{s:>5.2f} {t:>5.1f} {h:>3} | {n:>5} | "
              f"{pct['ALVO']:>5.1f} {pct['STOP']:>5.1f} "
              f"{pct['FLAT']:>5.1f} {pct['AMBIGUO']:>5.1f} | "
              f"{m_ot * 10000:>9.1f} {p_ot:>5.3f} | "
              f"{m_pe * 10000:>9.1f} {p_pe:>5.3f}")

    print("\n===== VEREDITO (regras pre-registradas) =====")
    pos_ot = [k for k, v in stats.items() if v["ot"][0] > 0]
    forte = [k for k, v in stats.items() if v["pe"][1] > 0]
    if not pos_ot:
        print("MATA: EVot <= 0 em TODAS as celulas — a convexidade nao cobre"
              " o custo nem no bound superior. NAO construir simulador de"
              " caminho.")
    elif forte:
        print("FORTE: lo95 do PESSIMISTA > 0 em pelo menos uma celula —"
              " Estagio 1 (sim de caminho fiel + nulo casado + OOS) com"
              " prioridade. Celulas:")
        for k in forte:
            print(f"  S={k[0]} T={k[1]} H={k[2]}")
    else:
        print("CINZA: autoriza PR-2 (sim de caminho) APENAS nas celulas com"
              " EVot > 0:")
        for k in pos_ot:
            print(f"  S={k[0]} T={k[1]} H={k[2]}")

    for label, b in (("otimista", "ot"), ("pessimista", "pe")):
        k = max(stats, key=lambda key: stats[key][b][0])
        m, lo, hi, _ = stats[k][b]
        print(f"melhor celula {label}: S={k[0]} T={k[1]} H={k[2]} -> "
              f"({m * 10000:.1f}, {lo * 10000:.1f}, {hi * 10000:.1f}) bps")
    print("caveat: 36 celulas sem correcao de multiplos testes — Estagio 0"
          " e veto, nao claim de edge.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
