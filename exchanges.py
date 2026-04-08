# exchanges.py — Módulo de busca de dados de mercado — Setup Atirador v8
# Extração das funções de acesso a exchanges do monolito.
# Correção Bug #1: fetch_perpetuals retorna tuple[list[dict], str] onde str é
# o nome da fonte ativa ("OKX", "Gate.io" ou "Bitget"), não int.

import asyncio
import json
import logging
import os
import time
from typing import Any, Callable

import aiohttp
import requests

from config import (
    MIN_TURNOVER_24H,
    MIN_OI_USD,
    KLINE_TOP_N,
    KLINE_TOP_N_LIGHT,
    KLINE_LIMIT,
    KLINE_CACHE_TTL_H,
    TICKER_TIMEOUT,
    _GATE_MULTIPLIERS_TTL,
    BITGET_PRODUCT_TYPE,
    URLS,
)

LOG = logging.getLogger("atirador")

# ---------------------------------------------------------------------------
# Estado privado do módulo
# ---------------------------------------------------------------------------
_gate_multipliers: dict[str, float] = {}
_gate_multipliers_ts: float = 0.0
_data_source_attempts: list[dict] = []

# ---------------------------------------------------------------------------
# Headers internos — não exportados
# ---------------------------------------------------------------------------
_BITGET_HEADERS: dict = {
    "Accept-Encoding": "gzip, deflate",
    "Accept":          "application/json",
    "User-Agent":      "Mozilla/5.0",
}
_HTTP_HEADERS: dict = {
    "Accept-Encoding": "gzip, deflate",
    "User-Agent":      "scanner/8.0",
}


# ===========================================================================
# Utilitários
# ===========================================================================

def sf(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None and val != "" else default
    except Exception:
        return default


def _build_venue_info(kline_venue: str | None, tv_venue: str | None) -> dict:
    if kline_venue is None or tv_venue is None:
        return {"kline_venue": kline_venue, "tv_venue": tv_venue,
                "mixed": False, "quality": "unknown"}
    if kline_venue == tv_venue or (kline_venue == "okx" and tv_venue == "bybit"):
        return {"kline_venue": kline_venue, "tv_venue": tv_venue,
                "mixed": False, "quality": "clean"}
    return {"kline_venue": kline_venue, "tv_venue": tv_venue,
            "mixed": True, "quality": "mixed"}


# ===========================================================================
# HTTP helpers
# ===========================================================================

async def api_get_async(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = 3,
    headers: dict | None = None
) -> dict | None:
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            t0 = time.time()
            async with session.get(url, timeout=20, headers=headers) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                status  = resp.status
                if status != 200:
                    LOG.warning(f"  ⚠️  HTTP {status} para {short_url}")
                    if i < retries - 1:
                        await asyncio.sleep(2)
                        continue
                    return None
                data = json.loads(raw.decode("utf-8"))
                return data
        except asyncio.TimeoutError:
            LOG.warning(f"  ⏱️  Timeout (tentativa {i+1}/{retries}): {short_url}")
        except json.JSONDecodeError as e:
            LOG.error(f"  ❌  JSON inválido: {e} | URL: {short_url}")
            return None
        except Exception as e:
            LOG.warning(f"  ⚠️  Erro tentativa {i+1}/{retries}: {type(e).__name__}: {e}")
        if i < retries - 1:
            await asyncio.sleep(2 ** (i + 1))
    LOG.error(f"  ❌  Falha após {retries} tentativas: {short_url}")
    return None


def api_get(url: str, retries: int = 3) -> dict:
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20,
                                headers={"Accept-Encoding": "gzip, deflate"})
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            LOG.warning(f"  ⚠️  api_get tentativa {i+1}/{retries}: {e}")
            if i < retries - 1:
                time.sleep(2)
            else:
                LOG.error(f"  ❌  api_get falhou: {short_url}")
                raise


# ===========================================================================
# Klines
# ===========================================================================

async def fetch_klines_async(
    session: aiohttp.ClientSession,
    symbol: str,
    granularity: str = "15m",
    limit: int = 60
) -> tuple[list[dict], str | None]:
    """Busca klines com fallback Bitget → OKX. Retorna (candles, venue)."""
    url_bitget = (
        f"{URLS['bitget_klines']}"
        f"?productType={BITGET_PRODUCT_TYPE}&symbol={symbol}"
        f"&granularity={granularity}&limit={limit}"
    )
    try:
        data = await api_get_async(session, url_bitget, headers=_BITGET_HEADERS)
        if data and "data" in data and data["data"]:
            raw_candles = data["data"]
            result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                       "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                      for c in raw_candles]
            result.reverse()
            return result, "bitget"
    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async Bitget {symbol} {granularity}: {e}")

    base_coin  = symbol.replace("USDT", "")
    okx_instid = f"{base_coin}-USDT-SWAP"
    url_okx    = (f"{URLS['okx_klines']}"
                  f"?instId={okx_instid}&bar={granularity}&limit={limit}")
    try:
        data_okx = await api_get_async(session, url_okx)
        if data_okx and "data" in data_okx and data_okx["data"]:
            raw = data_okx["data"]
            result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                       "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                      for c in raw]
            result.reverse()
            return result, "okx"
        else:
            return [], None
    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async OKX {symbol} {granularity}: {e}")
        return [], None


async def fetch_klines_cached_async(
    session: aiohttp.ClientSession,
    symbol: str,
    granularity: str = "4H",
    limit: int = 60
) -> list[dict]:
    """Klines com cache local."""
    cache_dir  = "/tmp/atirador_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = f"{cache_dir}/klines_{symbol}_{granularity}.json"

    if os.path.exists(cache_file):
        age_h = (time.time() - os.path.getmtime(cache_file)) / 3600
        try:
            with open(cache_file) as f:
                cached = json.load(f)
            if cached and age_h < KLINE_CACHE_TTL_H and len(cached) >= 20:
                return cached
        except Exception:
            pass

    klines, _ = await fetch_klines_async(session, symbol, granularity, limit)
    if klines:
        try:
            with open(cache_file, "w") as f:
                json.dump(klines, f)
        except Exception:
            pass
    return klines


# ===========================================================================
# Tickers e universo — hierarquia OKX → Gate.io → Bitget
# ===========================================================================

def _log_source_attempt(
    fonte: str,
    url: str,
    status: int,
    elapsed: float,
    tokens_brutos: int,
    qualificados: int,
    motivo_falha: str | None = None
) -> None:
    entrada = {"fonte": fonte, "url": url[:80], "status": status,
               "elapsed_s": round(elapsed, 2), "tokens_brutos": tokens_brutos,
               "qualificados": qualificados, "falha": motivo_falha}
    _data_source_attempts.append(entrada)
    if motivo_falha:
        LOG.warning(f"  ⛔  [{fonte}] FALHOU | {elapsed:.2f}s | {motivo_falha}")
    else:
        LOG.info(f"  ✅  [{fonte}] OK | {elapsed:.2f}s | {tokens_brutos} → {qualificados}")


async def _fetch_gate_multipliers() -> dict[str, float]:
    global _gate_multipliers, _gate_multipliers_ts
    agora = time.time()
    if _gate_multipliers and (agora - _gate_multipliers_ts) < _GATE_MULTIPLIERS_TTL:
        return _gate_multipliers
    url = URLS["gate_contracts"]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=TICKER_TIMEOUT),
                headers=_HTTP_HEADERS,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    mults = {}
                    for c in data:
                        sym  = c.get("name", "").replace("_", "")
                        mult = sf(c.get("quanto_multiplier", 1.0))
                        if mult <= 0:
                            mult = 1.0
                        if sym.endswith("USDT"):
                            mults[sym] = mult
                    _gate_multipliers    = mults
                    _gate_multipliers_ts = agora
                    return mults
    except Exception as e:
        LOG.warning(f"  ⚠️  [Gate.io/contracts] {e}")
    return {}


async def _parse_gateio_tickers(
    items: list[dict],
    multipliers: dict
) -> tuple[list[dict], int, int]:
    qualified = []
    rej_vol = rej_oi = 0
    for t in items:
        sym = t.get("contract", "").replace("_", "")
        if not sym.endswith("USDT"):
            continue
        turnover = sf(t.get("volume_24h_quote", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1
            continue
        price      = sf(t.get("last", 0) or t.get("mark_price", 0))
        mark_price = sf(t.get("mark_price", 0) or price)
        if price <= 0:
            continue
        total_size  = sf(t.get("total_size", 0))
        mult        = multipliers.get(sym, 1.0)
        oi_usd      = total_size * mark_price * mult
        oi_estimado = False
        if oi_usd <= 0:
            oi_usd = turnover * 0.1
            oi_estimado = True
        if oi_usd < MIN_OI_USD:
            rej_oi += 1
            continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol": sym, "base_coin": base, "price": price,
            "turnover_24h": turnover, "oi_usd": oi_usd, "oi_estimado": oi_estimado,
            "volume_24h": turnover, "funding_rate": sf(t.get("funding_rate", 0)),
            "price_change_24h": sf(t.get("change_percentage", 0)),
        })
    return qualified, rej_vol, rej_oi


async def _fetch_okx_funding_rates(
    symbols_okx: list[str]
) -> dict[str, float]:
    def _sync_fetch() -> dict[str, float]:
        fr_map   = {}
        base_url = URLS["okx_funding"]
        for sym_okx in symbols_okx:
            try:
                resp = requests.get(
                    f"{base_url}?instId={sym_okx}",
                    timeout=3,
                    headers=_HTTP_HEADERS,
                )
                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    if data:
                        fr = sf(data[0].get("fundingRate", 0))
                        sym_internal = sym_okx.replace("-USDT-SWAP", "") + "USDT"
                        fr_map[sym_internal] = fr
            except Exception:
                pass
        return fr_map

    return await asyncio.to_thread(_sync_fetch)


async def _fetch_okx_tickers_with_oi() -> list[dict] | None:
    try:
        async with aiohttp.ClientSession(headers=_HTTP_HEADERS) as session:
            async with session.get(
                URLS["okx_tickers"],
                timeout=aiohttp.ClientTimeout(total=TICKER_TIMEOUT),
            ) as tickers_resp:
                tickers_resp.raise_for_status()
                tickers_data = (await tickers_resp.json(content_type=None)).get("data", [])

            async with session.get(
                URLS["okx_oi"],
                timeout=aiohttp.ClientTimeout(total=TICKER_TIMEOUT),
            ) as oi_resp:
                oi_resp.raise_for_status()
                oi_data = (await oi_resp.json(content_type=None)).get("data", [])

        oi_dict = {item["instId"]: item for item in oi_data}

        for ticker in tickers_data:
            inst_id = ticker.get("instId")
            if inst_id in oi_dict:
                ticker["oiUsd"]   = float(oi_dict[inst_id]["oiUsd"])
                ticker["oi_real"] = True
            else:
                ticker["oiUsd"]   = 0
                ticker["oi_real"] = False

        swap_instids = [t.get("instId") for t in tickers_data
                        if t.get("instId", "").endswith("-USDT-SWAP")]
        fr_map = await _fetch_okx_funding_rates(swap_instids)
        for ticker in tickers_data:
            inst_id      = ticker.get("instId", "")
            sym_internal = inst_id.replace("-USDT-SWAP", "") + "USDT"
            if sym_internal in fr_map:
                ticker["fundingRate"] = fr_map[sym_internal]

        return tickers_data
    except Exception as e:
        LOG.error(f"  ❌ [OKX] Erro: {type(e).__name__}: {e}")
        return None


async def _parse_okx_tickers(
    items: list[dict]
) -> tuple[list[dict], int, int]:
    qualified = []
    rej_vol = rej_oi = 0
    for t in items:
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        sym      = inst_id.replace("-USDT-SWAP", "") + "USDT"
        turnover = sf(t.get("volCcy24h", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1
            continue
        price = sf(t.get("last", 0))
        if price <= 0:
            continue
        oi_usd      = sf(t.get("oiUsd", 0))
        oi_real     = t.get("oi_real", False)
        oi_estimado = False
        if oi_usd <= 0:
            oi_usd = turnover * 0.1
            oi_estimado = True
        if oi_usd < MIN_OI_USD:
            rej_oi += 1
            continue
        open24h      = sf(t.get("open24h", 0))
        price_change = ((price - open24h) / open24h * 100) if open24h > 0 else 0.0
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol": sym, "base_coin": base, "price": price,
            "turnover_24h": turnover, "oi_usd": oi_usd,
            "oi_estimado": oi_estimado and not oi_real,
            "volume_24h": sf(t.get("vol24h", 0)),
            "funding_rate": sf(t.get("fundingRate", 0)),
            "price_change_24h": price_change,
        })
    return qualified, rej_vol, rej_oi


async def _parse_bitget_tickers(
    items: list[dict]
) -> tuple[list[dict], int, int]:
    qualified = []
    rej_vol = rej_oi = 0
    for t in items:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        turnover = sf(t.get("usdtVolume"))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1
            continue
        price   = sf(t.get("lastPr"))
        holding = sf(t.get("holdingAmount"))
        oi_usd  = holding * price
        if oi_usd < MIN_OI_USD:
            rej_oi += 1
            continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol": sym, "base_coin": base, "price": price,
            "turnover_24h": turnover, "oi_usd": oi_usd, "oi_estimado": False,
            "volume_24h": sf(t.get("baseVolume")),
            "funding_rate": sf(t.get("fundingRate")),
            "price_change_24h": sf(t.get("change24h")) * 100,
        })
    return qualified, rej_vol, rej_oi


async def _try_source(
    nome: str,
    url: str,
    parse_fn: Callable,
    extract_fn: Callable,
    timeout: int | None = None,
    parse_kwargs: dict | None = None
) -> tuple[list[dict], int] | None:
    t_used = timeout or TICKER_TIMEOUT
    LOG.info(f"  📡  [{nome}] Tentando: {url[:80]}...")
    t0 = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=t_used),
                headers=_HTTP_HEADERS,
            ) as resp:
                elapsed = time.time() - t0
                status  = resp.status
                if status != 200:
                    _log_source_attempt(nome, url, status, elapsed, 0, 0, f"HTTP {status}")
                    return None
                data  = await resp.json(content_type=None)
                items = extract_fn(data)
                if not items:
                    _log_source_attempt(nome, url, status, elapsed, 0, 0, "resposta vazia")
                    return None
                kwargs              = parse_kwargs or {}
                qualified, rej_vol, rej_oi = await parse_fn(items, **kwargs)
                if not qualified:
                    _log_source_attempt(nome, url, status, elapsed, len(items), 0, "nenhum qualificado")
                    return None
                _log_source_attempt(nome, url, status, elapsed, len(items), len(qualified))
                return qualified, len(items)
    except (aiohttp.ServerTimeoutError, asyncio.TimeoutError):
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"Timeout {elapsed:.1f}s")
        return None
    except Exception as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"{type(e).__name__}: {str(e)[:80]}")
        return None


async def fetch_perpetuals() -> tuple[list[dict], str]:
    """Busca perpetuals USDT — hierarquia OKX → Gate.io → Bitget.

    Retorna (tickers_qualificados, nome_da_fonte).
    Correção Bug #1: segundo elemento é str (nome da fonte ativa),
    não int (total raw de tickers).
    """
    global _data_source_attempts
    _data_source_attempts = []
    LOG.info("📡 [v8.0.0] Iniciando busca de tickers — hierarquia OKX → Gate.io → Bitget")

    tickers_with_oi = await _fetch_okx_tickers_with_oi()
    if tickers_with_oi:
        qualified, rej_vol, rej_oi = await _parse_okx_tickers(tickers_with_oi)
        if qualified:
            qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
            LOG.info(f"  ✅  [OKX] {len(tickers_with_oi)} brutos → {len(qualified)} qualificados")
            return qualified, "OKX"

    multipliers = await _fetch_gate_multipliers()
    resultado   = await _try_source(
        nome="Gate.io",
        url=URLS["gate_tickers"],
        parse_fn=_parse_gateio_tickers,
        extract_fn=lambda d: d if isinstance(d, list) else [],
        parse_kwargs={"multipliers": multipliers},
    )
    if resultado:
        qualified, _total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        return qualified, "Gate.io"

    resultado = await _try_source(
        nome="Bitget",
        url=URLS["bitget_tickers"],
        parse_fn=_parse_bitget_tickers,
        extract_fn=lambda d: d.get("data", []),
        timeout=20,
    )
    if resultado:
        qualified, _total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        return qualified, "Bitget"

    raise RuntimeError("Todas as fontes de tickers falharam. Scan abortado.")


# ===========================================================================
# Fear & Greed e token individual
# ===========================================================================

async def fetch_fear_greed_async(
    session: aiohttp.ClientSession
) -> dict:
    try:
        data = await api_get_async(session, URLS["fear_greed"])
        if data and "data" in data:
            v  = data["data"][0]
            fg = {"value": int(v["value"]), "classification": v["value_classification"]}
            LOG.info(f"  📊 Fear & Greed: {fg['value']} ({fg['classification']})")
            return fg
    except Exception as e:
        LOG.warning(f"  ⚠️  Fear & Greed falhou: {e}")
    return {"value": 50, "classification": "Neutral"}


async def _fetch_token_okx_async(
    session: aiohttp.ClientSession,
    symbol: str
) -> dict | None:
    base    = symbol.replace("USDT", "")
    inst_id = f"{base}-USDT-SWAP"
    hdrs    = {"User-Agent": "scanner/8.0", "Accept-Encoding": "gzip"}

    async def _get(url: str) -> dict:
        try:
            async with session.get(url, headers=hdrs,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json(content_type=None)
        except Exception as exc:
            LOG.debug(f"    _fetch_token_okx: {url} → {exc}")
            return {}

    ticker_r, oi_r, fr_r = await asyncio.gather(
        _get(f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"),
        _get(f"{URLS['okx_oi']}&instId={inst_id}"),
        _get(f"{URLS['okx_funding']}?instId={inst_id}"),
    )

    tickers = ticker_r.get("data", []) if isinstance(ticker_r, dict) else []
    if not tickers:
        return None
    t     = tickers[0]
    price = sf(t.get("last", 0))
    if price <= 0:
        return None

    turnover = sf(t.get("volCcy24h", 0))
    open24h  = sf(t.get("open24h", 0))
    pct_chg  = ((price - open24h) / open24h * 100) if open24h > 0 else 0.0

    oi_items = oi_r.get("data", []) if isinstance(oi_r, dict) else []
    oi_usd   = sf(oi_items[0].get("oiUsd", 0)) if oi_items else 0.0
    fr_items = fr_r.get("data", []) if isinstance(fr_r, dict) else []
    fr_val   = sf(fr_items[0].get("fundingRate", 0)) if fr_items else 0.0

    return {
        "symbol": symbol, "base_coin": base, "price": price,
        "turnover_24h": turnover, "oi_usd": oi_usd, "oi_estimado": oi_usd <= 0,
        "volume_24h": sf(t.get("vol24h", 0)), "funding_rate": fr_val,
        "price_change_24h": pct_chg,
    }
