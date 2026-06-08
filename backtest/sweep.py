# backtest/sweep.py — Setup Atirador v9
# Varredura: roda replay_signal sobre TODAS as barras de um intervalo (nao so
# nas ancoras do journal) e prova que esse caminho novo e fiel, usando cont_pull
# (setup conhecido) como quantum de teste. Destrava o teste de receita offline.
# Roda na VM (replay_signal depende de pandas_ta). NAO toca runtime.
# Uso: .venv/bin/python -m backtest.sweep --start 2026-04-26 --end 2026-05-10

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import connect as store_connect   # noqa: E402
from backtest.replay import BAR_15M_MS, find_entry_bar, replay_signal  # noqa: E402

TIER1 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "TRXUSDT", "LTCUSDT",
    "BCHUSDT", "SUIUSDT", "TONUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT",
    "OPUSDT", "WLDUSDT",
]
_MISS_SAMPLE = 10   # quantos misses listar no relatorio


def _iso_to_ms(iso: str) -> int:
    """ISO -> epoch-ms (UTC). Mesma convencao de find_entry_bar."""
    return int(datetime.fromisoformat(iso)
               .astimezone(timezone.utc).timestamp() * 1000)


def sweep_symbol(conn, symbol: str, start_ms: int, end_ms: int,
                 step_bars: int = 1) -> list:
    """Itera bar_ts de start_ms a end_ms (inclusive) em passos de
    step_bars * BAR_15M_MS. Em cada bar chama replay_signal; coleta os CALLs.

    Sem dedup, sem open_trades: emite em TODA barra qualificante, logo e um
    superset das entradas do journal (re-fires sao esperados, nao erro)."""
    out = []
    step = step_bars * BAR_15M_MS
    bar_ts = start_ms
    while bar_ts <= end_ms:
        dec = replay_signal(conn, symbol, bar_ts)
        if dec is not None and dec.action == "CALL":
            out.append({
                "bar_ts": bar_ts,
                "direction": dec.direction,
                "confluent_setups": list(dec.confluent_setups),
            })
        bar_ts += step
    return out


def _load_cont_pull_trades(journal_db: str, symbols: list,
                           start_iso: str, end_iso: str) -> list:
    """Entradas cont_pull em tier1 dentro da janela [start_iso, end_iso).
    ISO ordena lexicograficamente, entao o filtro por string funciona."""
    c = sqlite3.connect(journal_db)
    ph = ",".join("?" * len(symbols))
    q = (f"SELECT timestamp, symbol, direction, setup_name, entry_price "
         f"FROM trades WHERE symbol IN ({ph}) AND entry_price IS NOT NULL "
         f"AND timestamp >= ? AND timestamp < ? ORDER BY timestamp")
    rows = c.execute(q, list(symbols) + [start_iso, end_iso]).fetchall()
    c.close()
    return [
        {"timestamp": r[0], "symbol": r[1], "direction": r[2],
         "setup_name": r[3], "entry_price": r[4]}
        for r in rows
        if "cont_pull" in (r[3] or "").split("+")
    ]


def cont_pull_fidelity(store_db: str, journal_db: str, symbols: list,
                       start_iso: str, end_iso: str,
                       step_bars: int = 1) -> dict:
    """Prova UNIDIRECIONAL journal subseteq varredura: cada entrada cont_pull
    real do journal tem de aparecer na varredura. Os extras da varredura sao
    re-fires esperados (NAO fazer igualdade de conjuntos)."""
    store_conn = store_connect(store_db)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)

    # Indexa CALLs varridos com cont_pull num set de chaves.
    swept_keys: set = set()
    swept_cont_pull_total = 0
    for sym in symbols:
        for sig in sweep_symbol(store_conn, sym, start_ms, end_ms, step_bars):
            if "cont_pull" in sig["confluent_setups"]:
                swept_cont_pull_total += 1
                swept_keys.add((sym, sig["bar_ts"], sig["direction"]))

    trades = _load_cont_pull_trades(journal_db, symbols, start_iso, end_iso)

    encontrado = 0
    miss = 0
    fora_de_cobertura = 0
    miss_samples: list = []
    for t in trades:
        anchor = find_entry_bar(store_conn, t["symbol"],
                                t["timestamp"], t["entry_price"])
        if anchor is None:
            # borda stale do store -- benigno, NAO e miss.
            fora_de_cobertura += 1
            continue
        if (t["symbol"], anchor, t["direction"]) in swept_keys:
            encontrado += 1
        else:
            miss += 1
            if len(miss_samples) < _MISS_SAMPLE:
                miss_samples.append(
                    f"{t['symbol']} / {t['timestamp']} / anchor={anchor}")

    store_conn.close()
    return {
        "journal_total": len(trades),
        "encontrado": encontrado,
        "MISS": miss,
        "fora_de_cobertura": fora_de_cobertura,
        "swept_cont_pull_total": swept_cont_pull_total,
        "_miss_samples": miss_samples,
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Varredura cont_pull: prova fidelidade journal subseteq varredura")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--journal",
                    default=str(ROOT / "journal" / "atirador_journal_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=TIER1)
    ap.add_argument("--start", required=True, help="data ISO, ex. 2026-04-26")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-05-10")
    ap.add_argument("--step", type=int, default=1, help="passo em barras 15m")
    args = ap.parse_args()

    print(f"[sweep] store={args.store} journal={args.journal}", file=sys.stderr)
    print(f"[sweep] janela {args.start} -> {args.end} step={args.step} "
          f"symbols={len(args.symbols)}", file=sys.stderr)

    r = cont_pull_fidelity(args.store, args.journal, args.symbols,
                           args.start, args.end, args.step)

    in_cov = r["journal_total"] - r["fora_de_cobertura"]
    print("\n========== SWEEP cont_pull ==========")
    print(f"journal_total         : {r['journal_total']}")
    pct = (100.0 * r["encontrado"] / in_cov) if in_cov else 0.0
    print(f"encontrado            : {r['encontrado']} ({pct:.1f}% da cobertura)")
    print(f"MISS                  : {r['MISS']}")
    print(f"fora_de_cobertura     : {r['fora_de_cobertura']} (borda stale, benigno)")
    print(f"swept_cont_pull_total : {r['swept_cont_pull_total']}")
    ratio = (r["swept_cont_pull_total"] / in_cov) if in_cov else float("nan")
    print(f"razao de re-fire      : {ratio:.2f}x  (sanidade: > 1 e finita)")
    status = "OK -- caminho de varredura provado" if r["MISS"] == 0 else \
        "FALHA -- MISS > 0 e bug no caminho de varredura"
    print(f"status                : {status}")

    if r["_miss_samples"]:
        print(f"\n--- amostra de MISS (ate {_MISS_SAMPLE}) ---")
        for line in r["_miss_samples"]:
            print(f"  {line}")

    return 0 if r["MISS"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
