# backtest/backfill_okx.py — Setup Atirador v9
# Backfill historico de candles OKX (/market/history-candles) para o backtest offline.
# Roda na VM (Marcelo via SSH). NAO faz parte do runtime de producao.
# Reusa api_get_async de producao (trata 429) e grava via candle_store (idempotente).
#
# Uso na VM:
#   .venv/bin/python -m backtest.backfill_okx
#   .venv/bin/python -m backtest.backfill_okx --days 90

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import MAX_CONCURRENT_FETCHES                                  # noqa: E402
from exchanges import api_get_async                                        # noqa: E402
from backtest.candle_store import connect, upsert_candles, coverage_report  # noqa: E402

OKX_HISTORY_CANDLES_URL = "https://www.okx.com/api/v5/market/history-candles"
OKX_PAGE_LIMIT = 100  # cap do /history-candles
DEFAULT_DAYS = 90

# Universo do piloto: 20 tier1 congelados (lista fixa, NAO filtro dinamico).
PILOT_TOKENS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    "TRXUSDT", "LTCUSDT", "BCHUSDT", "SUIUSDT", "TONUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "WLDUSDT",
]

# label interno do store -> (bar no formato OKX, duracao da vela em ms)
TIMEFRAMES = {
    "15m": ("15m", 15 * 60 * 1000),
    "1h":  ("1H", 60 * 60 * 1000),
    "4h":  ("4H", 4 * 60 * 60 * 1000),
}


def _inst_id(symbol: str) -> str:
    """BTCUSDT -> BTC-USDT-SWAP (mesmo padrao do exchanges.py)."""
    return f"{symbol.replace('USDT', '')}-USDT-SWAP"


async def _fetch_history(session, inst_id: str, bar: str,
                         start_ms: int, end_ms: int) -> "list[dict]":
    """Pagina /market/history-candles cobrindo [start_ms, end_ms] (inclusive),
    descartando a vela aberta (c[8]=='0'). Retorna dicts em ordem ascendente,
    formato de producao {ts, open, high, low, close, volume}."""
    out: "dict[int, dict]" = {}
    cursor = end_ms + 1                     # `after` e exclusivo (retorna ts mais antigos)
    while True:
        url = (f"{OKX_HISTORY_CANDLES_URL}?instId={inst_id}&bar={bar}"
               f"&limit={OKX_PAGE_LIMIT}&after={cursor}")
        data = await api_get_async(session, url)
        if not data or not data.get("data"):
            break
        batch = data["data"]
        for c in batch:
            if len(c) > 8 and c[8] == "0":   # vela aberta -> pula (igual producao)
                continue
            ts = int(c[0])
            if ts < start_ms or ts > end_ms:
                continue
            out[ts] = {"ts": ts, "open": float(c[1]), "high": float(c[2]),
                       "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
        oldest = int(batch[-1][0])           # OKX devolve decrescente -> ultimo = mais antigo
        if oldest <= start_ms or len(batch) < OKX_PAGE_LIMIT:
            break
        cursor = oldest
    return [out[k] for k in sorted(out)]


async def _fetch_job(session, sem, symbol, tf_label, bar, start_ms, end_ms):
    async with sem:
        try:
            klines = await _fetch_history(session, _inst_id(symbol), bar, start_ms, end_ms)
            return symbol, tf_label, klines, None
        except Exception as e:
            return symbol, tf_label, [], f"{type(e).__name__}: {e}"


async def main_async(days: int, db_path: str) -> int:
    now_ms = int(time.time() * 1000)
    # janela por TF, calculada UMA vez (todas as moedas do mesmo TF compartilham)
    windows = {}  # tf_label -> (bar, bar_ms, start_ms, end_ms)
    for tf_label, (bar, bar_ms) in TIMEFRAMES.items():
        end_ms = (now_ms // bar_ms) * bar_ms - bar_ms        # ultima vela fechada
        start_ms = end_ms - days * 24 * 60 * 60 * 1000
        windows[tf_label] = (bar, bar_ms, start_ms, end_ms)

    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    async with aiohttp.ClientSession() as session:
        jobs = [
            _fetch_job(session, sem, symbol, tf_label, bar, start_ms, end_ms)
            for symbol in PILOT_TOKENS
            for tf_label, (bar, bar_ms, start_ms, end_ms) in windows.items()
        ]
        results = await asyncio.gather(*jobs)

    conn = connect(db_path)
    failed = 0
    for symbol, tf_label, klines, err in results:
        if err:
            failed += 1
            print(f"[backfill] FAIL {symbol} {tf_label}: {err}", file=sys.stderr)
            continue
        n = upsert_candles(conn, symbol, tf_label, klines)
        print(f"[backfill] {symbol:<10s} {tf_label:<3s} {n:>6d} candles", file=sys.stderr)

    print("\n[backfill] === COVERAGE ===", file=sys.stderr)
    for tf_label, (bar, bar_ms, start_ms, end_ms) in windows.items():
        for r in coverage_report(conn, tf_label, start_ms, end_ms, bar_ms):
            flag = "" if r["missing"] == 0 else f"  <-- FALTAM {r['missing']}"
            print(f"  {tf_label:<3s} {r['symbol']:<10s} {r['count']:>6d}/{r['expected']:<6d} "
                  f"{r['pct']:>5.1f}%{flag}", file=sys.stderr)
    conn.close()
    print(f"\n[backfill] concluido | {len(PILOT_TOKENS)} tokens x {len(TIMEFRAMES)} TFs | "
          f"{failed} jobs falharam", file=sys.stderr)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill historico OKX para backtest offline")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS)
    ap.add_argument("--db", type=str, default=str(ROOT / "backtest" / "candles_v9.db"))
    args = ap.parse_args()
    return asyncio.run(main_async(args.days, args.db))


if __name__ == "__main__":
    sys.exit(main())
