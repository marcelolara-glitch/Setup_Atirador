# backtest/gabarito.py — Setup Atirador v9
# Gabarito: reproduz os trades reais do journal via replay_signal (codigo de
# producao) e mede a fidelidade do motor de backtest. Roda na VM (pandas_ta).
# NAO toca runtime. Uso: .venv/bin/python -m backtest.gabarito

from __future__ import annotations

import argparse
import glob
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import connect as store_connect   # noqa: E402
from backtest.replay import find_entry_bar, replay_signal    # noqa: E402

FIDELITY_TARGET = 95.0
_MISS_SAMPLE = 8   # quantos misses de cada tipo imprimir pra diagnostico


def _load_trades(journal_db: str, symbols: set, since: str = "") -> list:
    c = sqlite3.connect(journal_db)
    ph = ",".join("?" * len(symbols))
    q = (f"SELECT id, timestamp, symbol, direction, setup_name, entry_price "
         f"FROM trades WHERE symbol IN ({ph}) AND entry_price IS NOT NULL")
    params = list(symbols)
    if since:
        q += " AND timestamp >= ?"   # ISO ordena lexicograficamente; floor por data funciona
        params.append(since)
    q += " ORDER BY timestamp"
    rows = c.execute(q, params).fetchall()
    c.close()
    return [
        {"id": r[0], "timestamp": r[1], "symbol": r[2], "direction": r[3],
         "setup_name": r[4], "entry_price": r[5]}
        for r in rows
    ]


def _classify(store_conn, t: dict) -> tuple:
    """(resultado, detalhe). resultado in {'match','miss'}."""
    bar = find_entry_bar(store_conn, t["symbol"], t["timestamp"], t["entry_price"])
    if bar is None:
        return "miss", "bar_nao_encontrado"
    dec = replay_signal(store_conn, t["symbol"], bar)
    if dec is None:
        return "miss", "historico_insuficiente"
    if dec.action != "CALL":
        return "miss", f"virou_SKIP:{dec.skip_reason}"
    if dec.direction != t["direction"]:
        return "miss", f"direcao_errada:{t['direction']}->{dec.direction}"
    expected = set(t["setup_name"].split("+"))
    got = set(dec.confluent_setups)
    if expected != got:
        return "miss", (f"setup_errado:{'+'.join(sorted(expected))}"
                        f"->{'+'.join(sorted(got))}")
    return "match", ""


def main_run(store_db: str, journal_db: str, since: str = "") -> int:
    store_conn = store_connect(store_db)
    symbols = {r[0] for r in store_conn.execute("SELECT DISTINCT symbol FROM candles")}
    trades = _load_trades(journal_db, symbols, since)
    if not trades:
        print("[gabarito] nenhum trade nos simbolos do store", file=sys.stderr)
        return 1

    total = len(trades)
    matches = 0
    miss_reasons: Counter = Counter()
    miss_samples: dict = defaultdict(list)
    by_setup_tot: Counter = Counter()
    by_setup_ok: Counter = Counter()
    by_symbol_tot: Counter = Counter()
    by_symbol_ok: Counter = Counter()

    for i, t in enumerate(trades, 1):
        res, detail = _classify(store_conn, t)
        by_setup_tot[t["setup_name"]] += 1
        by_symbol_tot[t["symbol"]] += 1
        if res == "match":
            matches += 1
            by_setup_ok[t["setup_name"]] += 1
            by_symbol_ok[t["symbol"]] += 1
        else:
            key = detail if detail.startswith("virou_SKIP") else detail.split(":")[0]
            miss_reasons[key] += 1
            if len(miss_samples[key]) < _MISS_SAMPLE:
                miss_samples[key].append(f"{t['id']} | {detail}")
        if i % 50 == 0:
            print(f"[gabarito] {i}/{total}...", file=sys.stderr, flush=True)

    pct = 100.0 * matches / total
    print("\n========== GABARITO ==========")
    if since:
        print(f"janela          : timestamp >= {since}")
    print(f"trades testados : {total}")
    print(f"match           : {matches} ({pct:.1f}%)")
    status = "OK" if pct >= FIDELITY_TARGET else "ABAIXO DO ALVO"
    print(f"alvo            : >= {FIDELITY_TARGET:.0f}%  -> {status}")

    print("\n--- misses por tipo ---")
    for cat, n in miss_reasons.most_common():
        print(f"  {cat:<28s} {n}")

    print("\n--- match por setup ---")
    for s in sorted(by_setup_tot):
        tot, ok = by_setup_tot[s], by_setup_ok[s]
        print(f"  {s:<22s} {ok:>4d}/{tot:<4d} {100.0*ok/tot:5.1f}%")

    print("\n--- match por simbolo ---")
    for s in sorted(by_symbol_tot):
        tot, ok = by_symbol_tot[s], by_symbol_ok[s]
        print(f"  {s:<10s} {ok:>4d}/{tot:<4d} {100.0*ok/tot:5.1f}%")

    print(f"\n--- amostra de misses (ate {_MISS_SAMPLE} por tipo) ---")
    for cat in miss_reasons:
        for line in miss_samples[cat]:
            print(f"  {line}")

    store_conn.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Gabarito: fidelidade do replay vs journal")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--journal", default="")
    ap.add_argument("--since", default="", help="floor de data ISO, ex. 2026-04-26")
    args = ap.parse_args()
    journal = args.journal
    if not journal:
        cands = glob.glob(str(ROOT / "**" / "atirador_journal_v9.db"), recursive=True)
        journal = cands[0] if cands else str(ROOT / "journal" / "atirador_journal_v9.db")
    print(f"[gabarito] store={args.store} journal={journal}", file=sys.stderr)
    return main_run(args.store, journal, args.since)


if __name__ == "__main__":
    sys.exit(main())
