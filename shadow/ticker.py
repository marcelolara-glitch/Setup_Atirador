# shadow/ticker.py — captura de best bid/ask (ticker) OKX para o shadow.
# FASE 0.2: exchanges.py NAO tem endpoint de ticker por simbolo — so
# `_fetch_okx_tickers_with_oi` (universo inteiro + OI/funding, pesado) e o
# `market/ticker` embutido em `_fetch_token_okx` (exchanges.py:639), nao
# exportado. Helper NOVO, mesmo host/estilo do fetch existente (OKX v5,
# instId `<base>-USDT-SWAP`, api_get_async reusado). PROIBIDO editar
# exchanges.py. Best-effort: NUNCA levanta — retorna dict exec ou None.
from __future__ import annotations

import asyncio
import time

# Mesmo host de URLS["okx_klines"] (config.py:129); endpoint de ticker por
# instrumento (retorna bidPx/askPx), como exchanges.py:639.
OKX_TICKER_URL = "https://www.okx.com/api/v5/market/ticker"


async def _fetch_ticker_async(symbol: str):
    import aiohttp                                       # lazy: fora do sandbox
    from exchanges import api_get_async

    inst = f"{symbol.replace('USDT', '')}-USDT-SWAP"     # BTCUSDT -> BTC-USDT-SWAP
    url = f"{OKX_TICKER_URL}?instId={inst}"
    async with aiohttp.ClientSession() as session:
        data = await api_get_async(session, url)
    rows = data.get("data") if isinstance(data, dict) else None
    if not rows:
        return None
    bid, ask = float(rows[0].get("bidPx") or 0), float(rows[0].get("askPx") or 0)
    if bid <= 0 or ask <= 0 or ask < bid:                # ticker vazio/cruzado
        return None
    mid = (bid + ask) / 2.0
    return {"bid": bid, "ask": ask, "mid": mid,
            "spread_bps": (ask - bid) / mid * 1e4,
            "ts_capture": int(time.time() * 1000)}


def fetch_best_bid_ask(symbol: str):
    """Sync: dict {bid,ask,mid,spread_bps,ts_capture} no instante da chamada,
    ou None se indisponivel. Best-effort — jamais levanta (a abertura do trade
    no shadow nunca pode depender da captura de execucao)."""
    try:
        return asyncio.run(_fetch_ticker_async(symbol))
    except Exception:
        return None
