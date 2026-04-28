"""Coleta one-shot de klines para investigação de qualidade de entrada (abr/2026).

Lê os trades fechados de `atirador_journal_v9.db`, baixa klines 15m do par
em janela [-50 velas pré-entrada, +50 velas pós-saída] e uma janela única
de BTCUSDT (23-28/04/2026), e serializa tudo em um JSON único.

Script descartável, isolado de produção. Não importa de setups/, signals,
risk, regime, indicators. Reusa exchanges.api_get_async para consistência
de tratamento de rate-limit.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config import JOURNAL_DB_V9, MAX_CONCURRENT_FETCHES, URLS  # noqa: E402
from exchanges import api_get_async  # noqa: E402

OUT_PATH = Path(os.path.expanduser("~/research_output/qualidade_entrada_2026_04.json"))
BTC_START = datetime(2026, 4, 23, 0, 0, tzinfo=timezone.utc)
BTC_END = datetime(2026, 4, 28, 0, 0, tzinfo=timezone.utc)
BAR_MS = 15 * 60 * 1000
PRE_CANDLES = 50
POST_CANDLES = 50
OKX_PAGE_LIMIT = 300

TRADE_FIELDS = (
    "id, timestamp, exit_time, symbol, direction, setup_name, regime_at_entry, "
    "status, entry_price, sl_price, tp1_price, atr_value, pnl_pct, "
    "max_runup, max_drawdown, evidence_json"
)


def _to_ms(s: str | None) -> int | None:
    if s is None:
        return None
    txt = s.replace("Z", "+00:00") if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _to_iso_z(s: str | None) -> str | None:
    ms = _to_ms(s)
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _f(v):
    return float(v) if v is not None else None


def load_trades() -> list[dict]:
    conn = sqlite3.connect(JOURNAL_DB_V9)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT {TRADE_FIELDS} FROM trades WHERE status NOT IN ('OPEN', 'EXPIRED')"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def _fetch_okx_range(
    session: aiohttp.ClientSession, symbol: str, start_ms: int, end_ms: int
) -> list[list[float]]:
    """Pagina /market/candles OKX cobrindo [start_ms, end_ms] em 15m."""
    inst = f"{symbol.replace('USDT', '')}-USDT-SWAP"
    out: dict[int, list[float]] = {}
    cursor = end_ms + BAR_MS  # `after` é exclusivo → +1 bar para incluir end_ms
    while True:
        url = (f"{URLS['okx_klines']}?instId={inst}&bar=15m"
               f"&limit={OKX_PAGE_LIMIT}&after={cursor}")
        data = await api_get_async(session, url)
        if not data or not data.get("data"):
            break
        batch = data["data"]
        for c in batch:
            if len(c) > 8 and c[8] == "0":  # confirm flag — pula candle aberto
                continue
            ts = int(c[0])
            if ts < start_ms or ts > end_ms:
                continue
            out[ts] = [ts, float(c[1]), float(c[2]), float(c[3]),
                       float(c[4]), float(c[5])]
        oldest = int(batch[-1][0])
        if oldest <= start_ms or len(batch) < OKX_PAGE_LIMIT:
            break
        cursor = oldest
    return [out[k] for k in sorted(out)]


async def _fetch_with_sem(session, sem, symbol, start_ms, end_ms):
    async with sem:
        try:
            klines = await _fetch_okx_range(session, symbol, start_ms, end_ms)
            if not klines:
                return None, "empty_response"
            return klines, None
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"


async def main() -> int:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    trades = load_trades()
    print(f"[coleta] {len(trades)} trades carregados de {JOURNAL_DB_V9}", file=sys.stderr)

    btc_start_ms = int(BTC_START.timestamp() * 1000)
    btc_end_ms = int(BTC_END.timestamp() * 1000) - BAR_MS

    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    async with aiohttp.ClientSession() as session:
        btc_task = _fetch_with_sem(session, sem, "BTCUSDT", btc_start_ms, btc_end_ms)
        trade_tasks = []
        for t in trades:
            entry = _to_ms(t["timestamp"])
            exit_ms = _to_ms(t["exit_time"]) or entry
            s_ms = entry - PRE_CANDLES * BAR_MS
            e_ms = exit_ms + POST_CANDLES * BAR_MS
            trade_tasks.append(_fetch_with_sem(session, sem, t["symbol"], s_ms, e_ms))
        btc_klines, btc_err = await btc_task
        results = await asyncio.gather(*trade_tasks)

    if btc_err:
        print(f"[coleta] BTC falhou: {btc_err}", file=sys.stderr)

    failed = 0
    enriched = []
    for t, (klines, err) in zip(trades, results):
        if err:
            failed += 1
            print(f"[coleta] FAIL id={t['id']} {t['symbol']}: {err}", file=sys.stderr)
        enriched.append({
            "id": t["id"],
            "timestamp": _to_iso_z(t["timestamp"]),
            "exit_time": _to_iso_z(t["exit_time"]),
            "symbol": t["symbol"],
            "direction": t["direction"],
            "setup_name": t["setup_name"],
            "regime_at_entry": t["regime_at_entry"],
            "status": t["status"],
            "entry_price": _f(t["entry_price"]),
            "sl_price": _f(t["sl_price"]),
            "tp1_price": _f(t["tp1_price"]),
            "atr_value": _f(t["atr_value"]),
            "pnl_pct": _f(t["pnl_pct"]),
            "max_runup": _f(t["max_runup"]),
            "max_drawdown": _f(t["max_drawdown"]),
            "evidence_json": t["evidence_json"],
            "klines": klines,
            "error": err,
        })

    payload = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "trade_count": len(trades),
            "trade_count_with_klines": len(trades) - failed,
            "trade_count_failed": failed,
            "btc_window_start": BTC_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "btc_window_end": BTC_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "btc_candle_count": len(btc_klines or []),
        },
        "btc_klines": btc_klines or [],
        "trades": enriched,
    }
    OUT_PATH.write_text(json.dumps(payload))
    print(f"[coleta] wrote {OUT_PATH} | {failed}/{len(trades)} falharam", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
