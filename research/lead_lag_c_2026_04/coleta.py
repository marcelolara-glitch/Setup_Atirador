"""Coleta histórica de 60d × top-50 OKX (15m) para investigação lead-lag.

Hipótese a testar: lead-lag BTC → altcoins (He et al. 2026; Liu et al. 2024).
Insumo bruto para backtest. Não toca produção, isolado, descartável.

Etapas:
  1. Lista top 50 perpétuos USDT-SWAP por volCcy24h (proxy de liquidez),
     excluindo stablecoins e wrapped. Garante BTC presente.
  2. Pagina /market/candles 15m por 60 dias para cada token + BTC.
  3. Serializa em ~/research_output/lead_lag_c_2026_04/dataset.json.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config import MAX_CONCURRENT_FETCHES, URLS  # noqa: E402
from exchanges import api_get_async  # noqa: E402

OUT_DIR = Path(os.path.expanduser("~/research_output/lead_lag_c_2026_04"))
DATASET_PATH = OUT_DIR / "dataset.json"
TOP50_PATH = OUT_DIR / "top50.json"

BAR_MS = 15 * 60 * 1000
WINDOW_DAYS = 60
TARGET_CANDLES = WINDOW_DAYS * 24 * 4  # 5760
OKX_PAGE_LIMIT = 300
TOP_N = 50

STABLES = {"USDC", "DAI", "TUSD", "FDUSD", "USDD", "USDP", "PYUSD"}
WRAPPED = {"WBTC", "WETH", "STETH", "WSTETH", "CBETH", "RETH"}


def _base_symbol(inst_id: str) -> str | None:
    if not inst_id.endswith("-USDT-SWAP"):
        return None
    return inst_id[: -len("-USDT-SWAP")]


async def fetch_top50(session: aiohttp.ClientSession) -> list[dict]:
    """Lista top 50 base symbols por volCcy24h, BTC garantido."""
    data = await api_get_async(session, URLS["okx_tickers"])
    if not data or not data.get("data"):
        raise RuntimeError("OKX tickers retornou vazio")

    candidates: list[dict] = []
    for t in data["data"]:
        base = _base_symbol(t.get("instId", ""))
        if base is None or base in STABLES or base in WRAPPED:
            continue
        try:
            vol = float(t.get("volCcy24h") or 0.0)
        except (TypeError, ValueError):
            continue
        candidates.append({"base": base, "inst_id": t["instId"], "vol_ccy_24h_usdt": vol})

    candidates.sort(key=lambda x: x["vol_ccy_24h_usdt"], reverse=True)
    top = candidates[:TOP_N]
    if not any(c["base"] == "BTC" for c in top):
        btc = next((c for c in candidates if c["base"] == "BTC"), None)
        if btc is None:
            raise RuntimeError("BTC-USDT-SWAP não encontrado nos tickers OKX")
        top.append(btc)
        print("[coleta] BTC fora do top 50, adicionado manualmente", file=sys.stderr)

    print(f"[coleta] top {len(top)} (base, volCcy24h USDT):", file=sys.stderr)
    for c in top:
        print(f"  {c['base']:<12s} {c['vol_ccy_24h_usdt']:>20,.0f}", file=sys.stderr)
    return top


async def _fetch_okx_range(
    session: aiohttp.ClientSession, inst_id: str, start_ms: int, end_ms: int
) -> list[list[float]]:
    """Pagina /market/candles cobrindo [start_ms, end_ms] em 15m."""
    out: dict[int, list[float]] = {}
    cursor = end_ms + BAR_MS  # `after` é exclusivo
    while True:
        url = (f"{URLS['okx_klines']}?instId={inst_id}&bar=15m"
               f"&limit={OKX_PAGE_LIMIT}&after={cursor}")
        data = await api_get_async(session, url)
        if not data or not data.get("data"):
            break
        batch = data["data"]
        for c in batch:
            if len(c) > 8 and c[8] == "0":  # candle aberto
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


async def _fetch_with_sem(session, sem, inst_id, start_ms, end_ms):
    async with sem:
        try:
            klines = await _fetch_okx_range(session, inst_id, start_ms, end_ms)
            if not klines:
                return None, "empty_response"
            return klines, None
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"


async def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000) // BAR_MS * BAR_MS - BAR_MS
    start_ms = end_ms - (TARGET_CANDLES - 1) * BAR_MS

    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    async with aiohttp.ClientSession() as session:
        top = await fetch_top50(session)
        TOP50_PATH.write_text(json.dumps(top, indent=2))

        btc_inst = next(c["inst_id"] for c in top if c["base"] == "BTC")
        alts = [c for c in top if c["base"] != "BTC"]

        btc_task = _fetch_with_sem(session, sem, btc_inst, start_ms, end_ms)
        alt_tasks = [
            _fetch_with_sem(session, sem, c["inst_id"], start_ms, end_ms) for c in alts
        ]
        btc_klines, btc_err = await btc_task
        results = await asyncio.gather(*alt_tasks)

    if btc_err:
        print(f"[coleta] BTC falhou: {btc_err}", file=sys.stderr)

    failed = 0
    tokens: dict[str, dict] = {}
    for c, (klines, err) in zip(alts, results):
        if err:
            failed += 1
            print(f"[coleta] FAIL {c['base']}: {err}", file=sys.stderr)
        tokens[f"{c['base']}USDT"] = {
            "vol_ccy_24h_usdt": c["vol_ccy_24h_usdt"],
            "candles_count": len(klines or []),
            "klines": klines or [],
            "error": err,
        }

    payload = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_start_ms": start_ms,
            "window_end_ms": end_ms,
            "timeframe": "15m",
            "candles_per_token_target": TARGET_CANDLES,
            "tokens_collected": len(alts) - failed,
            "tokens_failed": failed,
        },
        "btc_klines": btc_klines or [],
        "tokens": tokens,
    }
    DATASET_PATH.write_text(json.dumps(payload))
    print(
        f"[coleta] wrote {DATASET_PATH} | btc={len(btc_klines or [])} candles | "
        f"{failed}/{len(alts)} tokens falharam",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
