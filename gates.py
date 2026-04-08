# gates.py — TradingView Scanner e gate direcional (módulo v8)
import asyncio
import json
import logging
import time
from typing import Any

import aiohttp

from config import URLS

LOG = logging.getLogger("atirador")

_TV_HEADERS = {
    "User-Agent"  : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Origin"      : "https://www.tradingview.com",
    "Referer"     : "https://www.tradingview.com/",
}


def recommendation_from_value(val: float | None) -> str:
    if val is None:    return "NEUTRAL"
    if val >= 0.5:     return "STRONG_BUY"
    elif val >= 0.1:   return "BUY"
    elif val >= -0.1:  return "NEUTRAL"
    elif val >= -0.5:  return "SELL"
    else:              return "STRONG_SELL"


async def fetch_tv_batch_async(
    session: aiohttp.ClientSession,
    symbols: list[str],
    columns: list[str],
    retries: int = 3,
) -> tuple[dict, dict]:
    """Busca indicadores TradingView. Retorna (result, tv_venues)."""
    if not symbols:
        return {}, {}

    tv_url = URLS["tradingview"]
    tickers_bybit = [f"BYBIT:{s}.P" for s in symbols]
    payload = {"symbols": {"tickers": tickers_bybit, "query": {"types": []}},
               "columns": columns}

    result    = {}
    tv_venues: dict[str, str] = {}
    for attempt in range(retries):
        try:
            t0 = time.time()
            async with session.post(tv_url, json=payload,
                                    headers=_TV_HEADERS, timeout=25) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                data    = json.loads(raw.decode("utf-8"))
                for item in (data.get("data") or []):
                    sym  = item["s"].replace("BYBIT:", "").replace(".P", "")
                    vals = item["d"]
                    result[sym]    = dict(zip(columns, vals))
                    tv_venues[sym] = "bybit"

                missing = [s for s in symbols if s not in result]
                LOG.debug(f"  ✅  TV batch BYBIT: {len(result)}/{len(symbols)} retornados | {elapsed:.2f}s")

                if missing:
                    LOG.warning(f"  ⚠️  TV BYBIT: {len(missing)} sem retorno: {missing}")
                    tickers_bitget = [f"BITGET:{s}.P" for s in missing]
                    payload_fb = {"symbols": {"tickers": tickers_bitget, "query": {"types": []}},
                                  "columns": columns}
                    try:
                        async with session.post(tv_url, json=payload_fb,
                                                headers=_TV_HEADERS, timeout=15) as resp_fb:
                            raw_fb  = await resp_fb.read()
                            data_fb = json.loads(raw_fb.decode("utf-8"))
                            for item in data_fb.get("data", []):
                                sym_fb = item["s"].replace("BITGET:", "").replace(".P", "")
                                if sym_fb in missing:
                                    result[sym_fb]    = dict(zip(columns, item["d"]))
                                    tv_venues[sym_fb] = "bitget"
                    except Exception as e_fb:
                        LOG.warning(f"  ⚠️  TV BITGET: fallback falhou: {e_fb}")

                return result, tv_venues

        except Exception as e:
            LOG.warning(f"  ⚠️  TV batch tentativa {attempt+1}/{retries}: {type(e).__name__}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))

    LOG.error(f"  ❌  TV batch falhou após {retries} tentativas")
    return {}, {}
