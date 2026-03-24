#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v4.9 - Scanner Profissional de Criptomoedas
=============================================================================
Arquitetura Multi-Timeframe com 3 Camadas Independentes:

  CAMADA 1 — 4H "Qual é a direção do mercado?" (contexto macro)
  CAMADA 2 — 1H "Estamos num bom ponto de entrada?" (estrutura)
  CAMADA 3 — 15m "O timing de entrada está correto agora?" (gatilho)

Score máximo: 26 pts
Thresholds: Favorável ≥16 | Moderado ≥18 | Cauteloso(Bear/Medo Extremo) ≥20

v4.9 — Perpétuos puros + Estratégia de Recuperação de Banca:

  [FIX 1 — PERPÉTUOS] TradingView: todos os símbolos agora usam sufixo .P
    BYBIT:BTCUSDT.P em vez de BYBIT:BTCUSDT — força uso exclusivo do
    contrato perpétuo, eliminando ambiguidade com spot. Resolve o problema
    dos 10 tokens sem retorno (TAO, SIREN, ZEC, RIVER, XAU, XAG, etc.)
    O retorno ainda é mapeado por base_coin para lookup interno.

  [FIX 2 — SWING POINTS] find_swing_points com fallback automático:
    Tenta window=5 primeiro; se retornar < 3 pontos, tenta window=3.
    Resolve P5 "dados insuficientes" e P-1H "longe de suportes" com
    60 candles disponíveis.

  [FIX 3 — LOG KLINES] Período dos klines exibido corretamente no log:
    Antes: "03/21 → 03/11" (invertido). Agora: "03/11 → 03/21" (cronológico).

  [ESTRATÉGIA — RECUPERAÇÃO DE BANCA]
    Risco fixo de $5,00 por trade (era $0.75 = 0.75% da banca).
    Alavancagem escalonada por score — quanto mais forte o setup,
    maior a alavancagem permitida:
      Score 20–21 → alavancagem até 10x
      Score 22–23 → alavancagem até 20x
      Score 24–25 → alavancagem até 30x
      Score 26    → alavancagem até 50x
    Alavancagem mínima: 1x (antes gerava 0.8x — inexecutável)
    Limite de perda diária: $10 (2 losses de $5 = stop do dia)
    Ganho esperado por trade (RR 1:2): $10 por winner

  [TRACKING] Registro de resultado de trades no estado diário:
    Histórico de trades com score, alavancagem, resultado e data
    para calibração futura da taxa de acerto real do sistema.

v4.8: FGI≤20 CAUTELOSO, tokens sem dados excluídos, Bollinger robusto
v4.7: Sistema de log completo, cache anti-corrupção
v4.5: Fix encoding Brotli | v4.3: Gate 1H principal, símbolos TV

Autor: Manus AI | v4.1→v4.9 (revisão Claude/Anthropic)
=============================================================================
"""

import json
import requests
import time
import os
import sys
import logging
import numpy as np
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

# Fuso horário BRT (Brasília, UTC-3)
BRT = timezone(timedelta(hours=-3))

# ===========================================================================
# SISTEMA DE LOG CENTRALIZADO [v4.7]
# ===========================================================================
# Grava em arquivo E terminal simultaneamente.
# Um arquivo de log por execução: /tmp/atirador_YYYYMMDD_HHMM.log
# Níveis: DEBUG (detalhes internos) | INFO (fluxo normal) | WARNING | ERROR

LOG_DIR = "/tmp/atirador_logs"

def setup_logger():
    """
    Configura logger com saída dupla: arquivo + terminal.
    [v4.8 FIX 1] Nome do arquivo: atirador_LOG_YYYYMMDD_HHMM.log
    [v4.8 FIX 3] Timestamps do log em BRT (não UTC) via converter customizado
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    ts_brt  = datetime.now(BRT)
    ts_str  = ts_brt.strftime("%Y%m%d_%H%M")
    logfile = f"{LOG_DIR}/atirador_LOG_{ts_str}.log"

    logger = logging.getLogger("atirador")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # [v4.8 FIX 3] Converter customizado para que %(asctime)s use BRT, não UTC
    def brt_converter(timestamp, *args):
        return datetime.fromtimestamp(timestamp, BRT).timetuple()

    # Formato completo para arquivo (com timestamp BRT e nível)
    fmt_file = logging.Formatter(
        "%(asctime)s BRT [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fmt_file.converter = brt_converter

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt_file)

    # Formato compacto para terminal (só mensagem)
    fmt_term = logging.Formatter("%(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt_term)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"📋 Log iniciado: {logfile}")
    return logger, logfile, ts_str   # retorna ts_str para usar no nome do relatório

# Logger global — inicializado em run_scan_async
LOG      = None
LOG_FILE = None
TS_SCAN  = None   # timestamp da execução — usado no nome do relatório

def log_section(title):
    """Separador visual de seção no log."""
    LOG.info(f"\n{'─'*55}")
    LOG.info(f"  {title}")
    LOG.info(f"{'─'*55}")



# ===========================================================================
# CONFIGURAÇÃO
# ===========================================================================

# Filtros Institucionais
MIN_TURNOVER_24H = 5_000_000
MIN_OI_USD       = 10_000_000
TOP_N            = 30

# ===========================================================================
# GESTÃO DE RISCO — ESTRATÉGIA DE RECUPERAÇÃO DE BANCA [v4.9]
# ===========================================================================
# Risco fixo por trade: $5.00 (independente da banca ou alavancagem)
# Alavancagem escalonada por score: quanto mais forte o setup,
# maior a alavancagem permitida para maximizar o retorno.
#
# Matemática da recuperação:
#   Risco/trade:  $5.00  (loss máximo por operação)
#   Ganho/trade:  $10.00 (RR 1:2 → $5 de risco → $10 de ganho)
#   Para dobrar banca ($100→$200): ~13 trades vencedores
#   Expected value (55% acerto): +$3.25/trade
#
# Alavancagem por score:
#   Score 20–21 pts → até 10x  (setup válido, mercado cauteloso)
#   Score 22–23 pts → até 20x  (setup forte)
#   Score 24–25 pts → até 30x  (setup muito forte)
#   Score 26    pts → até 50x  (setup perfeito — todos pilares confirmados)
#
BANKROLL              = 100.0
RISCO_POR_TRADE_USD   = 5.00    # [v4.9] Risco fixo em $ por trade (era 0.75% = $0.75)
MAX_PERDA_DIARIA_USD  = 10.00   # [v4.9] 2 losses = stop do dia ($10 = 10% da banca)
MAX_TRADES_ABERTOS    = 2
ALAVANCAGEM_MIN       = 1.0     # [v4.9] Mínimo executável em qualquer exchange
ALAVANCAGEM_MAX       = 50.0    # Teto absoluto
RR_MINIMO             = 2.0     # Risk:Reward mínimo 1:2

# Tabela de alavancagem máxima por score [v4.9]
# Score → alavancagem máxima permitida
ALAV_POR_SCORE = {
    (20, 21): 10.0,
    (22, 23): 20.0,
    (24, 25): 30.0,
    (26, 26): 50.0,
}

def get_alav_max_por_score(score: int) -> float:
    """Retorna a alavancagem máxima permitida para o score dado."""
    for (sc_min, sc_max), alav_max in ALAV_POR_SCORE.items():
        if sc_min <= score <= sc_max:
            return alav_max
    return ALAVANCAGEM_MIN  # Score abaixo de 20 → mínimo (não deveria chegar aqui)

# Performance
KLINE_TOP_N        = 10       # Análise completa (com klines 1H + 4H)
KLINE_TOP_N_LIGHT  = 20       # Análise leve (sem klines)
KLINE_LIMIT        = 60
KLINE_CACHE_TTL_H  = 3        # Cache klines 4H em horas

# Análise Técnica
SWING_WINDOW     = 5
SR_PROXIMITY_PCT = 1.0        # Suporte válido se preço está a ≤1% acima
OB_IMPULSE_N     = 3          # Candles para medir impulso após OB
OB_IMPULSE_PCT   = 1.5        # Impulso mínimo para qualificar OB (%)
OB_PROXIMITY_PCT = 1.5        # OB válido se preço está a ≤1.5% do meio do OB

# Filtro de Pump
PUMP_WARN_24H        = 20     # Penalidade -2 pts
PUMP_WARN_24H_STRONG = 30     # Penalidade -3 pts
PUMP_BLOCK_24H       = 40     # Descarte total

# Estado Diário
STATE_FILE = "/tmp/atirador_state.json"

# ===========================================================================
# GERENCIAMENTO DE ESTADO DIÁRIO
# ===========================================================================

def load_daily_state():
    """Carrega estado do dia. Reseta automaticamente em novo dia."""
    today = datetime.now(BRT).strftime("%Y-%m-%d")
    default = {
        "date": today,
        "trades_abertos": [],
        "pnl_dia": 0.0,
        "trades_executados": 0,
        "bloqueado": False,
        "motivo_bloqueio": "",
        "historico": [],
    }
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            if state.get("date") != today:
                default["historico"] = state.get("historico", [])
                save_daily_state(default)
                return default
            return state
    except Exception:
        pass
    save_daily_state(default)
    return default

def save_daily_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        LOG.warning(f"⚠️  Erro ao salvar estado diário: {e}")

def check_risk_limits(state):
    """
    Verifica limites de risco diário.
    Retorna (pode_operar: bool, motivo: str)
    """
    if state.get("bloqueado"):
        return False, state.get("motivo_bloqueio", "Estado bloqueado")

    perda_max = MAX_PERDA_DIARIA_USD
    pnl = state.get("pnl_dia", 0.0)
    if pnl <= -perda_max:
        motivo = f"Perda máxima diária atingida (${abs(pnl):.2f} / ${perda_max:.2f})"
        state["bloqueado"] = True
        state["motivo_bloqueio"] = motivo
        save_daily_state(state)
        return False, motivo

    n_abertos = len(state.get("trades_abertos", []))
    if n_abertos >= MAX_TRADES_ABERTOS:
        return False, f"Máx. trades abertos atingido ({n_abertos}/{MAX_TRADES_ABERTOS})"

    return True, "OK"

# ===========================================================================
# TRADINGVIEW SCANNER API
# ===========================================================================

TV_URL     = "https://scanner.tradingview.com/crypto/scan"
TV_HEADERS = {
    "User-Agent"   : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type" : "application/json",
    "Origin"       : "https://www.tradingview.com",
    "Referer"      : "https://www.tradingview.com/",
}

# ---------------------------------------------------------------------------
# Convenção de símbolo — IMPORTANTE
# ---------------------------------------------------------------------------
# TradingView Scanner exige o símbolo COMPLETO do par: "BYBIT:BTCUSDT"
# O script passa d["symbol"] (ex: "BTCUSDT") e fetch_tv_batch_async monta
# "BYBIT:BTCUSDT" internamente. O resultado é devolvido com chave "BTCUSDT"
# (sem prefixo), permitindo tv_4h.get("BTCUSDT", {}) funcionar corretamente.
#
# ATENÇÃO: base_coin ("BTC") NÃO é um símbolo válido para o TradingView.
# Usar base_coin causava retorno vazio silencioso em todos os gates — era o
# bug original que fazia zero tokens passarem. Corrigido nesta versão.
#
# Bitget klines também usam d["symbol"] (ex: "BTCUSDT") — mesmo campo.
# ---------------------------------------------------------------------------

# CAMADA 1: Gate 4H — direção macro
COLS_4H = [
    "Recommend.All|240",   # Gate: descarta SELL/STRONG_SELL
    "RSI|240",             # Contexto de força macro (não pontuado)
]

# CAMADA 2: Gate 1H — velocidade de entrada
# [v4.3 FIX] Coluna duplicada removida. A API TV retorna valores por posição;
# colunas duplicadas desalinham o zip e causam retorno silenciosamente errado.
COLS_1H = [
    "Recommend.All|60",   # Gate: exige BUY ou STRONG_BUY
]

# CAMADA 3: Gatilho 15m
# Apenas indicadores NÃO presentes no TV Recommend interno.
# RSI, Stoch, CCI, MACD, ADX foram removidos — o TV Recommend do gate 1H
# já os computa internamente; reincluí-los seria dupla contagem.
COLS_15M = [
    "BB.upper|15", "BB.lower|15",   # P1 Bollinger — posição no canal
    "ATR|15",                        # Cálculo do SL dinâmico
    "Candle.Engulfing.Bullish|15",   # P2 Candles — price action puro
    "Candle.Hammer|15",
    "Candle.MorningStar|15",
    "Candle.3WhiteSoldiers|15",
    "Candle.Harami.Bullish|15",
    "Candle.Doji.Dragonfly|15",
]

def recommendation_from_value(val):
    if val is None:    return "NEUTRAL"
    if val >= 0.5:     return "STRONG_BUY"
    elif val >= 0.1:   return "BUY"
    elif val >= -0.1:  return "NEUTRAL"
    elif val >= -0.5:  return "SELL"
    else:              return "STRONG_SELL"

async def fetch_tv_batch_async(session, symbols, columns, retries=3):
    """
    Busca indicadores do TradingView — SOMENTE contratos perpétuos.
    [v4.9 FIX 1] Sufixo .P força contrato perpétuo: BYBIT:BTCUSDT.P
    Resolve os 10 tokens sem retorno (TAO, SIREN, ZEC, RIVER, XAU, etc.)
    que só existem como perpétuo no scanner TV, não como spot.
    O retorno é mapeado sem o .P para manter lookup interno consistente.
    """
    if not symbols: return {}
    tickers = [f"BYBIT:{s}.P" for s in symbols]   # [v4.9] .P = perpétuo exclusivo
    payload = {"symbols": {"tickers": tickers, "query": {"types": []}},
               "columns": columns}

    LOG.debug(f"  TV batch: {len(symbols)} tokens (.P perpétuo) | cols: {columns}")

    for attempt in range(retries):
        try:
            t0 = time.time()
            async with session.post(TV_URL, json=payload,
                                    headers=TV_HEADERS, timeout=25) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                data    = json.loads(raw.decode("utf-8"))
                result  = {}
                for item in data.get("data", []):
                    # Strip "BYBIT:" e ".P" → chave = símbolo original (ex: "BTCUSDT")
                    sym  = item["s"].replace("BYBIT:", "").replace(".P", "")
                    vals = item["d"]
                    none_cols = [c for c, v in zip(columns, vals) if v is None]
                    if none_cols:
                        LOG.debug(f"    ⚠️  {sym}: valores None em: {none_cols}")
                    result[sym] = dict(zip(columns, vals))

                missing = [s for s in symbols if s not in result]
                if missing:
                    LOG.warning(f"  ⚠️  TV: {len(missing)} token(s) sem retorno: {missing}")
                LOG.debug(f"  ✅  TV batch: {len(result)}/{len(symbols)} retornados | {elapsed:.2f}s")
                return result

        except Exception as e:
            LOG.warning(f"  ⚠️  TV batch tentativa {attempt+1}/{retries}: {type(e).__name__}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))

    LOG.error(f"  ❌  TV batch falhou após {retries} tentativas")
    return {}

    for attempt in range(retries):
        try:
            t0 = time.time()
            async with session.post(TV_URL, json=payload,
                                    headers=TV_HEADERS, timeout=25) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                data    = json.loads(raw.decode("utf-8"))
                result  = {}
                for item in data.get("data", []):
                    sym  = item["s"].replace("BYBIT:", "")
                    vals = item["d"]
                    none_cols = [c for c, v in zip(columns, vals) if v is None]
                    if none_cols:
                        LOG.debug(f"    ⚠️  {sym}: valores None em: {none_cols}")
                    result[sym] = dict(zip(columns, vals))

                missing = [s for s in symbols if s not in result]
                if missing:
                    LOG.warning(f"  ⚠️  TV: {len(missing)} token(s) sem retorno: {missing}")
                LOG.debug(f"  ✅  TV batch: {len(result)}/{len(symbols)} retornados | {elapsed:.2f}s")
                return result

        except Exception as e:
            LOG.warning(f"  ⚠️  TV batch tentativa {attempt+1}/{retries}: {type(e).__name__}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))

    LOG.error(f"  ❌  TV batch falhou após {retries} tentativas")
    return {}

# ===========================================================================
# HELPERS
# ===========================================================================

def sf(val, default=0.0):
    try: return float(val) if val is not None and val != "" else default
    except: return default

# Headers para Bitget — desabilita Brotli explicitamente.
# O aiohttp negocia "br" por padrão; sem pacote brotli instalado,
# a decodificação falha silenciosamente. Forçar gzip/deflate resolve.
BITGET_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Accept":          "application/json",
    "User-Agent":      "Mozilla/5.0",
}

async def api_get_async(session, url, retries=3, headers=None):
    """
    GET assíncrono com decodificação explícita e log completo.
    Loga: URL, tentativa, status HTTP, tamanho da resposta, erros.
    """
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            t0 = time.time()
            async with session.get(url, timeout=20, headers=headers) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                status  = resp.status
                size_kb = len(raw) / 1024
                encoding = resp.headers.get("Content-Encoding", "none")

                LOG.debug(f"  GET {short_url}")
                LOG.debug(f"      → HTTP {status} | {size_kb:.1f}KB | enc:{encoding} | {elapsed:.2f}s")

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
            wait = 2 ** (i + 1)
            LOG.debug(f"  ↻  Aguardando {wait}s antes de retry...")
            await asyncio.sleep(wait)

    LOG.error(f"  ❌  Falha após {retries} tentativas: {short_url}")
    return None

def api_get(url, retries=3):
    """GET síncrono (usado para fetch_perpetuals na inicialização)."""
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            t0   = time.time()
            resp = requests.get(url, timeout=20,
                                headers={"Accept-Encoding": "gzip, deflate"})
            elapsed = time.time() - t0
            LOG.debug(f"  GET(sync) {short_url} → HTTP {resp.status_code} | {elapsed:.2f}s")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            LOG.warning(f"  ⚠️  api_get tentativa {i+1}/{retries}: {e}")
            if i < retries - 1: time.sleep(2)
            else:
                LOG.error(f"  ❌  api_get falhou: {short_url}")
                raise

async def fetch_klines_async(session, symbol, granularity="15m", limit=60):
    """
    Busca klines da Bitget com log completo de diagnóstico.
    Loga: URL, candles retornados, faixa de datas, erros.
    """
    url = (f"https://api.bitget.com/api/v2/mix/market/candles"
           f"?productType=USDT-FUTURES&symbol={symbol}"
           f"&granularity={granularity}&limit={limit}")
    try:
        data = await api_get_async(session, url, headers=BITGET_HEADERS)

        if data is None:
            LOG.error(f"  ❌  Klines {symbol} {granularity}: resposta None (falha HTTP)")
            return []

        if "data" not in data:
            LOG.error(f"  ❌  Klines {symbol} {granularity}: campo 'data' ausente")
            LOG.debug(f"      Resposta recebida: {str(data)[:120]}")
            return []

        raw_candles = data["data"]
        if not raw_candles:
            LOG.warning(f"  ⚠️  Klines {symbol} {granularity}: 'data' vazio ([] da API)")
            return []

        result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                   "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                  for c in raw_candles]
        result.reverse()

        # [v4.9 FIX 3] Período exibido em ordem cronológica: mais antigo → mais recente
        # result[0] = candle mais antigo (após reverse()), result[-1] = mais recente
        ts_ini = datetime.fromtimestamp(result[0]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
        ts_fim = datetime.fromtimestamp(result[-1]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
        LOG.debug(f"  ✅  Klines {symbol} {granularity}: {len(result)} candles | {ts_ini} → {ts_fim}")
        return result

    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async {symbol} {granularity}: {type(e).__name__}: {e}")
        return []

async def fetch_klines_cached_async(session, symbol, granularity="4H", limit=60):
    """
    Klines com cache local + log completo de diagnóstico de cache.
    [v4.7 FIX] Não grava cache com lista vazia.
    [v4.7 FIX] Invalida cache que contenha lista vazia (cache corrompido).
    [v4.7 LOG] Loga: HIT/MISS/CORROMPIDO, idade do cache, candles encontrados.
    """
    cache_dir  = "/tmp/atirador_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = f"{cache_dir}/klines_{symbol}_{granularity}.json"

    if os.path.exists(cache_file):
        age_h   = (time.time() - os.path.getmtime(cache_file)) / 3600
        age_min = age_h * 60
        try:
            with open(cache_file) as f:
                cached = json.load(f)

            # [v4.7 FIX] Cache corrompido: lista vazia gravada por versão anterior
            if not cached:
                LOG.warning(f"  🗑️  Cache CORROMPIDO {symbol} {granularity}: "
                            f"arquivo contém lista vazia — descartando e rebuscando")
                os.remove(cache_file)
                # Cai para o fetch abaixo

            elif age_h >= KLINE_CACHE_TTL_H:
                LOG.debug(f"  ⏰  Cache EXPIRADO {symbol} {granularity}: "
                          f"{age_min:.0f}min > {KLINE_CACHE_TTL_H*60:.0f}min — rebuscando")
                # Cai para o fetch abaixo

            else:
                LOG.debug(f"  💾  Cache HIT {symbol} {granularity}: "
                          f"{len(cached)} candles | idade {age_min:.0f}min")
                return cached

        except (json.JSONDecodeError, Exception) as e:
            LOG.warning(f"  ⚠️  Cache INVÁLIDO {symbol} {granularity}: {e} — rebuscando")

    else:
        LOG.debug(f"  📡  Cache MISS {symbol} {granularity}: arquivo não existe — buscando")

    klines = await fetch_klines_async(session, symbol, granularity, limit)

    if klines:
        try:
            with open(cache_file, "w") as f:
                json.dump(klines, f)
            LOG.debug(f"  💾  Cache GRAVADO {symbol} {granularity}: {len(klines)} candles")
        except Exception as e:
            LOG.warning(f"  ⚠️  Falha ao gravar cache {symbol} {granularity}: {e}")
    else:
        # [v4.7 FIX] NÃO grava cache vazio — evita corromper para próximas execuções
        LOG.warning(f"  🚫  Cache NÃO gravado {symbol} {granularity}: "
                    f"klines vazios — próxima execução rebuscará da API")

    return klines

# ===========================================================================
# DADOS DE MERCADO (Bitget API)
# ===========================================================================

def fetch_perpetuals():
    """Busca perpetuals USDT com filtros institucionais."""
    LOG.info("📡 Buscando tickers Bitget (perpetuals USDT)...")
    data  = api_get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
    items = data.get("data", [])
    LOG.debug(f"  Total tickers recebidos: {len(items)}")

    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0
    for t in items:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            rej_symbol += 1; continue
        turnover = sf(t.get("usdtVolume"))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue
        price   = sf(t.get("lastPr"))
        holding = sf(t.get("holdingAmount"))
        oi_usd  = holding * price
        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "volume_24h"      : sf(t.get("baseVolume")),
            "funding_rate"    : sf(t.get("fundingRate")),
            "price_change_24h": sf(t.get("change24h")) * 100,
        })

    qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
    LOG.info(f"  ✅  {len(qualified)} tokens qualificados | "
             f"Rejeitados: {rej_vol} vol<{MIN_TURNOVER_24H/1e6:.0f}M, "
             f"{rej_oi} OI<{MIN_OI_USD/1e6:.0f}M")
    LOG.debug(f"  TOP 5 por volume: {[d['base_coin'] for d in qualified[:5]]}")
    return qualified, len(items)

async def fetch_fear_greed_async(session):
    """Fear & Greed Index global."""
    LOG.debug("  Buscando Fear & Greed Index...")
    try:
        data = await api_get_async(session, "https://api.alternative.me/fng/?limit=1")
        if data and "data" in data:
            v = data["data"][0]
            fg = {"value": int(v["value"]), "classification": v["value_classification"]}
            LOG.info(f"  📊 Fear & Greed: {fg['value']} ({fg['classification']})")
            return fg
    except Exception as e:
        LOG.warning(f"  ⚠️  Fear & Greed falhou: {e}")
    LOG.warning("  ⚠️  Fear & Greed: usando fallback 50 (Neutral)")
    return {"value": 50, "classification": "Neutral"}

# ===========================================================================
# ANÁLISE TÉCNICA — UTILITÁRIOS
# ===========================================================================

def find_swing_points(candles, window=None):
    """
    Detecta swing highs e swing lows.
    [v4.9 FIX 2] Fallback automático: tenta window=5 primeiro.
    Se retornar < 3 pontos em qualquer lista, tenta window=3.
    Resolve P5 "dados insuficientes" e P-1H "longe de suportes"
    que ocorriam mesmo com 60 candles disponíveis.
    """
    if window is None: window = SWING_WINDOW

    def _detect(candles, w):
        if len(candles) < w * 2 + 1: return [], []
        highs = np.array([c["high"] for c in candles])
        lows  = np.array([c["low"]  for c in candles])
        sh, sl = [], []
        for i in range(w, len(candles) - w):
            if highs[i] == np.max(highs[i - w:i + w + 1]):
                sh.append({"index": i, "price": highs[i]})
            if lows[i]  == np.min(lows[i  - w:i + w + 1]):
                sl.append({"index": i, "price": lows[i]})
        return sh, sl

    sh, sl = _detect(candles, window)

    # Fallback: se qualquer lista tiver < 3 pontos e window > 3, tenta menor
    if (len(sh) < 3 or len(sl) < 3) and window > 3:
        sh_fb, sl_fb = _detect(candles, 3)
        # Usa o fallback apenas se trouxer mais pontos
        if len(sh_fb) >= len(sh): sh = sh_fb
        if len(sl_fb) >= len(sl): sl = sl_fb
        if len(sh_fb) >= 3 and len(sl_fb) >= 3:
            LOG.debug(f"    find_swing_points: fallback window=3 aplicado "
                      f"(window={window} insuficiente: {len(sh)}H/{len(sl)}L → "
                      f"{len(sh_fb)}H/{len(sl_fb)}L)")

    return sh, sl

def detect_order_blocks(candles):
    """
    Order Blocks bullish: último candle bearish antes de impulso ≥ OB_IMPULSE_PCT.
    Retorna lista de {'high', 'low', 'index'}.
    """
    obs = []
    n = OB_IMPULSE_N
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] >= c["open"]: continue          # precisa ser bearish
        ref = c["close"]
        if ref <= 0: continue
        max_close   = max(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (max_close - ref) / ref * 100
        if impulse_pct >= OB_IMPULSE_PCT:
            obs.append({
                "high" : max(c["open"], c["close"]),
                "low"  : min(c["open"], c["close"]),
                "index": i,
            })
    return obs

# ===========================================================================
# CAMADA 2 — PILAR 1H: SUPORTE / ORDER BLOCK em klines 1H
# ===========================================================================

def analyze_support_1h(candles_1h, current_price):
    """
    Camada 2 — P-1H: avalia se o preço está num ponto de entrada válido
    dentro da tendência (suporte de swing ou Order Block no 1H).

    Este pilar é genuinamente diferente do TV Recommend.All 1H:
    o gate 1H diz "os indicadores estão bullish",
    este pilar diz "o preço está perto de um suporte real — é um bom ponto de entrada?"

    Pontuação máxima: 4 pts
      +2  Preço sobre suporte S/R 1H (swing low recente, ≤1%)
      +2  Preço sobre Order Block 1H (último OB ativo, ≤1.5%)
      +0  Bônus removido — 4 pts já é o máximo justificável para esta camada
    """
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"

    sh, sl = find_swing_points(candles_1h)
    score   = 0
    details = []

    # --- Suporte por Swing Low 1H ---
    if sl:
        for s in reversed(sl):  # mais recentes primeiro
            dist_pct = (current_price - s["price"]) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 2
                details.append(f"Suporte 1H em {s['price']:.4f} ({dist_pct:.2f}% abaixo)")
                break

    # --- Order Block 1H ---
    obs = detect_order_blocks(candles_1h)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = (current_price - ob_mid) / current_price * 100
            if -OB_PROXIMITY_PCT <= dist_pct <= OB_PROXIMITY_PCT:
                score += 2
                details.append(f"Order Block 1H ({ob['low']:.4f}–{ob['high']:.4f})")
                break

    if not details:
        return 0, "Preço longe de suportes no 1H"
    return min(score, 4), " | ".join(details)

# ===========================================================================
# CAMADA 3 — PILARES 15m
# ===========================================================================

# P1 — Bollinger Bands 15m
def score_bollinger(d):
    """
    Posição do preço no canal de Bollinger 15m.
    Mede se estamos perto da banda inferior — zona de sobrevenda no canal.
    Max: 3 pts.

    [v4.8 FIX 8] Bug corrigido: BB_upper e BB_lower chegavam como 0.0
    (não None) quando o TradingView não tinha o valor — a condição
    `if not (price and bbl and bbu ...)` descartava corretamente quando
    zero, MAS `bbu - bbl` poderia ser um valor minúsculo (ruído float)
    gerando pos fora da escala (ex: pos=104% como no log do BTC).
    Corrigido: valida se bbl e bbu são > 0 E se a banda tem largura mínima.
    Adicionado log explícito diferenciando "dados ausentes" de "dados ruins".
    """
    price = d.get("price", 0)
    bbl   = d.get("bb_lower_15m", 0)
    bbu   = d.get("bb_upper_15m", 0)
    sym   = d.get("base_coin", "?")

    # Validação explícita — cada condição logada separadamente
    if not price or price <= 0:
        LOG.debug(f"    BB {sym}: preço inválido ({price})")
        return 0, "BB N/A (preço inválido)"
    if not bbl or bbl <= 0:
        LOG.debug(f"    BB {sym}: BB_lower ausente ou zero ({bbl})")
        return 0, "BB N/A (BB_lower ausente)"
    if not bbu or bbu <= 0:
        LOG.debug(f"    BB {sym}: BB_upper ausente ou zero ({bbu})")
        return 0, "BB N/A (BB_upper ausente)"

    banda = bbu - bbl
    # Banda mínima: pelo menos 0.1% do preço para ser significativa
    banda_min = price * 0.001
    if banda <= banda_min:
        LOG.debug(f"    BB {sym}: banda muito estreita ({banda:.6f} < {banda_min:.6f}) — possível dado ruim")
        return 0, f"BB N/A (banda estreita: {banda:.4f})"

    pos = (price - bbl) / banda

    # pos > 1.0 ou < 0.0 indica que o preço saiu das bandas — dado anômalo
    if pos < 0 or pos > 1.5:
        LOG.warning(f"    BB {sym}: pos={pos:.0%} fora do range esperado "
                    f"(price={price:.4f} bbl={bbl:.4f} bbu={bbu:.4f}) — descartando")
        return 0, f"BB N/A (pos anômala: {pos:.0%})"

    if pos < 0.05:   return 3, f"BB extremo inferior ({pos:.0%})"
    elif pos < 0.15: return 2, f"BB inferior ({pos:.0%})"
    elif pos < 0.25: return 1, f"BB baixa ({pos:.0%})"
    else:            return 0, f"BB neutro ({pos:.0%})"

# P2 — Padrões de Candle 15m
def score_candles(ind):
    """
    Price action puro no 15m. Padrões bullish de reversão/continuação.
    Max: 4 pts (cap).
    """
    if not ind: return [], 0
    checks = {
        "Candle.Engulfing.Bullish|15": ("Engulfing Bullish", 2),
        "Candle.Hammer|15"           : ("Hammer",            2),
        "Candle.MorningStar|15"      : ("Morning Star",      2),
        "Candle.3WhiteSoldiers|15"   : ("3 White Soldiers",  2),
        "Candle.Harami.Bullish|15"   : ("Harami Bullish",    1),
        "Candle.Doji.Dragonfly|15"   : ("Dragonfly Doji",    1),
    }
    patterns, score = [], 0
    for key, (name, pts) in checks.items():
        v = ind.get(key)
        if v and v != 0:
            patterns.append(name)
            score += pts
    return patterns, min(score, 4)

# P3 — Funding Rate
def score_funding_rate(fr):
    """
    Funding rate negativo indica shorts dominando — pressão de short squeeze.
    Funding positivo alto indica longs excessivos — risco de liquidação.
    Max: 2 pts.
    """
    if fr < -0.0005: return 2, f"{fr:.4%} (squeeze potencial)"
    elif fr < 0:     return 1, f"{fr:.4%} (leve negativo)"
    elif fr > 0.0005: return -1, f"{fr:.4%} (longs excessivos)"
    else:             return 0, f"{fr:.4%} (neutro)"

# P8 — Volume 15m adaptativo
def score_volume_15m(candles_15m, fg_value=50):
    """
    Confirma se o volume da vela atual sustenta o movimento.
    Threshold adaptativo ao regime de mercado (Fear & Greed).
    Max: 2 pts.
    """
    if len(candles_15m) < 21: return 1, "Volume N/A (fallback)"
    current_vol = candles_15m[-1]["volume"]
    avg_vol     = np.mean([c["volume"] for c in candles_15m[-21:-1]])
    if avg_vol == 0: return 1, "Volume N/A (avg zero)"

    # Threshold adaptativo
    if fg_value <= 30:   threshold = 1.2  # Bull: exige volume crescente
    elif fg_value <= 70: threshold = 1.0  # Neutro: volume = média é suficiente
    else:                threshold = 0.8  # Bear: volume baixo é normal

    ratio = current_vol / avg_vol
    if ratio >= threshold * 1.5: return 2, f"Volume forte ({ratio:.1f}x média)"
    elif ratio >= threshold:     return 1, f"Volume adequado ({ratio:.1f}x média)"
    else:                        return 0, f"Volume fraco ({ratio:.1f}x < {threshold:.1f}x)"

# ===========================================================================
# CAMADA 1 — PILARES 4H (estrutura de preço, não indicadores)
# ===========================================================================

# P4 — Zonas de Liquidez 4H (S/R + Order Blocks)
def analyze_liquidity_zones_4h(candles_4h, current_price):
    """
    Detecta zonas de suporte e Order Blocks no 4H.
    Informação de ESTRUTURA — diferente dos osciladores do TV Recommend.
    Max: 3 pts.
      +1 Suporte S/R próximo (≤1%)
      +1 Order Block ativo (≤1.5% do meio)
      +1 Confluência S/R + OB
    """
    sh, sl = find_swing_points(candles_4h)
    score, details = 0, []

    sr_hit = False
    if sl:
        for s in reversed(sl):
            dist_pct = (current_price - s["price"]) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score  += 1
                sr_hit  = True
                details.append(f"Suporte 4H {s['price']:.4f} ({dist_pct:.2f}%)")
                break

    ob_hit = False
    obs = detect_order_blocks(candles_4h)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = (current_price - ob_mid) / current_price * 100
            if -OB_PROXIMITY_PCT <= dist_pct <= OB_PROXIMITY_PCT:
                score  += 1
                ob_hit  = True
                details.append(f"OB 4H ({ob['low']:.4f}–{ob['high']:.4f})")
                break

    if sr_hit and ob_hit:
        score  += 1
        details.append("Confluência S/R+OB")

    if not details:
        return 0, "Longe de zonas de liquidez 4H"
    return min(score, 3), " | ".join(details)

# P5 — Figuras Gráficas 4H
def analyze_chart_patterns_4h(candles_4h):
    """
    Detecta padrões de compressão e reversão no 4H.
    Max: 2 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    if len(sh) < 3 or len(sl) < 3:
        return 0, "Dados insuficientes para figuras"

    sh_p = [s["price"] for s in sh[-3:]]
    sl_p = [s["price"] for s in sl[-3:]]

    highs_lower = sh_p[0] > sh_p[1] > sh_p[2]
    highs_flat  = abs(sh_p[0] - sh_p[2]) / sh_p[0] < 0.015
    lows_higher = sl_p[0] < sl_p[1] < sl_p[2]
    lows_lower  = sl_p[0] > sl_p[1] > sl_p[2]

    # Falling Wedge — reversão bullish (fundos caem menos que topos)
    if highs_lower and lows_lower:
        high_drop = (sh_p[0] - sh_p[2]) / sh_p[0]
        low_drop  = (sl_p[0] - sl_p[2]) / sl_p[0]
        if low_drop < high_drop * 0.8:
            return 2, "Falling Wedge (reversão bullish)"

    # Triângulo Simétrico — compressão, breakout iminente
    if highs_lower and lows_higher:
        return 2, "Triângulo Simétrico (compressão)"

    # Triângulo Ascendente — acumulação com resistência flat
    if highs_flat and lows_higher:
        return 2, "Triângulo Ascendente (acumulação bullish)"

    # Cunha Descendente — pullback saudável em tendência de alta
    if highs_lower and not lows_higher:
        return 1, "Cunha Descendente (pullback)"

    return 0, "Sem figuras claras no 4H"

# P6 — CHOCH / BOS 4H
def analyze_choch_bos_4h(candles_4h, current_price):
    """
    Smart Money Concepts no 4H.
    Detecta mudança de caráter (CHOCH) e ruptura de estrutura (BOS).
    Max: 3 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    if len(sh) < 2 or len(sl) < 2:
        return 0, "Dados insuficientes para estrutura 4H"

    last_sh  = sh[-1]["price"]
    prev_sh  = sh[-2]["price"]
    last_sl  = sl[-1]["price"]
    prev_sl  = sl[-2]["price"]

    # CHOCH Bullish: downtrend confirmado + rompimento de swing high
    in_downtrend = (prev_sh < sh[-3]["price"]) if len(sh) >= 3 else False
    if in_downtrend and current_price > last_sh:
        return 3, "CHOCH Bullish 4H (reversão confirmada)"

    # BOS Bullish: Higher Highs + Higher Lows + rompimento
    if last_sh > prev_sh and last_sl > prev_sl and current_price > last_sh:
        return 2, "BOS Bullish 4H (continuação de alta)"

    # Estrutura Saudável: Higher Lows consecutivos (acumulação)
    if last_sl > prev_sl and len(sl) >= 3 and prev_sl > sl[-3]["price"]:
        return 1, "Estrutura 4H saudável (Higher Lows)"

    return 0, "Sem estrutura bullish no 4H"

# P7 — Filtro de Pump
def score_pump_filter(price_change_24h):
    """
    Evita armadilhas de liquidez em ativos muito estendidos.
    Bloqueio total acima de 40% | Penalidade gradual de 20% a 39%.
    """
    if price_change_24h >= PUMP_BLOCK_24H:
        return None, f"PUMP BLOCK: +{price_change_24h:.1f}% em 24h"
    elif price_change_24h >= PUMP_WARN_24H_STRONG:
        return -3, f"Pump forte ({price_change_24h:.1f}% > 30%)"
    elif price_change_24h >= PUMP_WARN_24H:
        return -2, f"Pump moderado ({price_change_24h:.1f}% > 20%)"
    else:
        return 0, f"OK ({price_change_24h:.1f}%)"

# ===========================================================================
# GESTÃO DE RISCO — TRADE PARAMS
# ===========================================================================

def calc_trade_params(price, atr, score=0):
    """
    Calcula parâmetros de trade com estratégia de recuperação de banca.

    [v4.9] Estratégia nova:
      - Risco FIXO de $5.00 por trade (independente de %)
      - SL baseado em ATR (1.5x ATR) — adapta à volatilidade real
      - Alavancagem calculada para que a perda no SL seja exatamente $5
      - Alavancagem MÁXIMA escalonada pelo score do setup:
          Score 20–21 → máx 10x | Score 22–23 → máx 20x
          Score 24–25 → máx 30x | Score 26    → máx 50x
      - Alavancagem MÍNIMA: 1x (executável em qualquer exchange)
      - Ganho esperado por trade: $10 (RR 1:2)

    Lógica de cálculo:
      sl_dist_pct = (1.5 × ATR) / price × 100
      alav = RISCO_FIXO_USD / (BANKROLL × sl_dist_pct / 100)
           = $5 / (perda_nominal_sem_alav)
    """
    if not price or not atr or atr <= 0: return None

    sl_dist_pct = (1.5 * atr) / price * 100     # SL = 1.5x ATR em %
    if sl_dist_pct < 0.05: return None           # SL < 0.05% = ruído de mercado

    sl = price * (1 - sl_dist_pct / 100)

    # Alavancagem para que perda no SL = $5 exatos
    # perda_sem_alav = BANKROLL × sl_dist_pct/100
    # alav = RISCO_FIXO / perda_sem_alav
    perda_sem_alav = BANKROLL * sl_dist_pct / 100
    if perda_sem_alav <= 0: return None

    alav_calculada = RISCO_POR_TRADE_USD / perda_sem_alav

    # Cap pela tabela de alavancagem máxima por score
    alav_max_score = get_alav_max_por_score(score)
    alav_final     = max(ALAVANCAGEM_MIN,
                         min(alav_calculada, alav_max_score))

    # Tamanho da posição e risco real (com alavancagem aplicada)
    posicao_usd  = BANKROLL * alav_final
    risco_real   = posicao_usd * sl_dist_pct / 100
    ganho_rr2    = risco_real * RR_MINIMO

    LOG.debug(f"    TradeParams: SL={sl_dist_pct:.2f}% | "
              f"alav_calc={alav_calculada:.1f}x → cap_score={alav_max_score:.0f}x → "
              f"alav_final={alav_final:.1f}x | "
              f"risco_real=${risco_real:.2f} | ganho_esperado=${ganho_rr2:.2f}")

    return {
        "entry"          : price,
        "sl"             : sl,
        "sl_distance_pct": sl_dist_pct,
        "tp1"            : price * (1 + sl_dist_pct / 100),        # RR 1:1 (fechar 50%)
        "tp2"            : price * (1 + sl_dist_pct * 2 / 100),    # RR 1:2 (fechar 30%)
        "tp3"            : price * (1 + sl_dist_pct * 3 / 100),    # RR 1:3 (fechar 20%)
        "rr"             : RR_MINIMO,
        "alavancagem"    : round(alav_final, 1),
        "alav_max_score" : alav_max_score,
        "risco_usd"      : round(risco_real, 2),
        "ganho_rr2_usd"  : round(ganho_rr2, 2),
        "atr"            : atr,
    }

# ===========================================================================
# SISTEMA DE SCORE v4.3
# ===========================================================================

def calculate_score(d, candles_15m=None, candles_1h=None, candles_4h=None,
                    fg_value=50, log_breakdown=True):
    """
    Score com 3 camadas independentes. Max: 26 pts.

    Parâmetros:
      log_breakdown: bool — se False, suprime o log de breakdown de pilares.
                    Usar False no score parcial (ETAPA 3b) para evitar poluir
                    o log com pilares zerados por ausência de klines.

    Retornos especiais:
      -1  = Token descartado pelo gate 4H (SELL/STRONG_SELL)
      -99 = Token descartado por PUMP BLOCK
      ≥0  = Score válido
    """
    sc       = 0
    reasons  = []
    breakdown = []

    # -----------------------------------------------------------------------
    # GATE CAMADA 1: 4H — direção macro
    # SELL/STRONG_SELL descarta. NEUTRAL/BUY/STRONG_BUY segue.
    # -----------------------------------------------------------------------
    s4h = d.get("summary_4h", "NEUTRAL")
    if "STRONG_SELL" in s4h or s4h == "SELL":
        return -1, [f"4H {s4h} — descartado pelo gate macro"], []
    breakdown.append(("Contexto 4H", 0, 0, f"{s4h} (contexto, não pontuado)"))

    # -----------------------------------------------------------------------
    # CAMADA 1 — Pilares 4H (estrutura de preço, klines)
    # -----------------------------------------------------------------------
    price = d.get("price", 0)

    # P4 — Zonas de Liquidez 4H
    if candles_4h:
        lz_sc, lz_det = analyze_liquidity_zones_4h(candles_4h, price)
    else:
        lz_sc, lz_det = 0, "Klines 4H indisponíveis"
    sc += lz_sc
    breakdown.append(("P4 Liquidez 4H", lz_sc, 3, lz_det))
    if lz_sc >= 2: reasons.append("Zona liquidez 4H")

    # P5 — Figuras Gráficas 4H
    if candles_4h:
        cp_sc, cp_det = analyze_chart_patterns_4h(candles_4h)
    else:
        cp_sc, cp_det = 0, "Klines 4H indisponíveis"
    sc += cp_sc
    breakdown.append(("P5 Figuras 4H", cp_sc, 2, cp_det))
    if cp_sc > 0: reasons.append(cp_det.split(" (")[0])

    # P6 — CHOCH / BOS 4H
    if candles_4h:
        cb_sc, cb_det = analyze_choch_bos_4h(candles_4h, price)
    else:
        cb_sc, cb_det = 0, "Klines 4H indisponíveis"
    sc += cb_sc
    breakdown.append(("P6 CHOCH/BOS 4H", cb_sc, 3, cb_det))
    if cb_sc > 0: reasons.append("Estrutura 4H bullish")

    # -----------------------------------------------------------------------
    # CAMADA 2 — Pilar 1H (posição de preço, klines)
    # -----------------------------------------------------------------------
    if candles_1h:
        s1h_sc, s1h_det = analyze_support_1h(candles_1h, price)
    else:
        s1h_sc, s1h_det = 0, "Klines 1H indisponíveis"
    sc += s1h_sc
    breakdown.append(("P-1H Suporte 1H", s1h_sc, 4, s1h_det))
    if s1h_sc >= 2: reasons.append("Suporte/OB 1H confirmado")

    # -----------------------------------------------------------------------
    # CAMADA 3 — Pilares 15m (gatilho de entrada)
    # -----------------------------------------------------------------------

    # P1 — Bollinger Bands 15m
    bb_sc, bb_det = score_bollinger(d)
    sc += bb_sc
    breakdown.append(("P1 Bollinger 15m", bb_sc, 3, bb_det))
    if bb_sc >= 2: reasons.append("BB inferior")

    # P2 — Padrões de Candle 15m
    ind_15m = d.get("_ind_15m", {})
    cp_list, ca_sc = score_candles(ind_15m)
    sc += ca_sc
    breakdown.append(("P2 Candles 15m", ca_sc, 4,
                       f"Padrões: {', '.join(cp_list)}" if cp_list else "Nenhum"))
    if cp_list: reasons.append(f"Candle: {cp_list[0]}")

    # P3 — Funding Rate
    fr = d.get("funding_rate", 0)
    fr_sc, fr_det = score_funding_rate(fr)
    sc += fr_sc
    breakdown.append(("P3 Funding Rate", fr_sc, 2, fr_det))
    if fr_sc >= 2: reasons.append("FR squeeze")

    # P7 — Filtro de Pump (pode descartar ou penalizar)
    pump_sc, pump_det = score_pump_filter(d.get("price_change_24h", 0))
    if pump_sc is None:
        return -99, ["PUMP BLOCK"], []
    sc += pump_sc
    breakdown.append(("P7 Filtro Pump", pump_sc, 0, pump_det))

    # P8 — Volume 15m
    if candles_15m:
        vol_sc, vol_det = score_volume_15m(candles_15m, fg_value)
    else:
        vol_sc, vol_det = 0, "Candles 15m indisponíveis"
    sc += vol_sc
    breakdown.append(("P8 Volume 15m", vol_sc, 2, vol_det))
    if vol_sc >= 2: reasons.append("Volume forte")

    # [v4.6 FIX] Removido max(sc, 0) — score pode ser 0 após penalidades.
    # Retornar 0 é diferente de retornar -1 (gate reject).
    # O relatório filtra por threshold; não cabe ao score mascarar valores baixos.
    final_sc = max(sc, 0)
    if not reasons: reasons.append(f"Score {final_sc}/26 (sem sinal dominante)")

    # Breakdown completo apenas quando klines estão disponíveis (score final)
    # Suprimido no score parcial via log_breakdown=False para não poluir o log
    if log_breakdown:
        sym = d.get("base_coin", "?")
        LOG.debug(f"  SCORE {sym}: {final_sc}/26 | klines: "
                  f"15m={'✅' if candles_15m else '❌'} "
                  f"1H={'✅' if candles_1h else '❌'} "
                  f"4H={'✅' if candles_4h else '❌'}")
        for pilar, pts, max_pts, detail in breakdown:
            bar = "█" * pts if pts > 0 else ("▒" * abs(pts) if pts < 0 else "·")
            LOG.debug(f"    {pilar:<22} {pts:>+3}/{max_pts} {bar} {detail}")

    return final_sc, reasons, breakdown

# ===========================================================================
# CONTEXTO DE MERCADO E THRESHOLD ADAPTATIVO
# ===========================================================================

def analyze_market_context(fg, btc_4h_str):
    """
    Threshold adaptativo baseado em BTC 4H e Fear & Greed.
    Score máximo é 26 pts — thresholds calibrados para esse teto.

    [v4.8 FIX 4] FGI ≤ 20 (Medo Extremo) → CAUTELOSO independente do BTC.
                 FGI=11 não pode ser MODERADO — é estado de capitulação.
    [v4.8 FIX 5] FGI ≤ 20 não contribui positivamente para Risk Score.
                 Medo Extremo indica risco sistêmico, não oportunidade.
    """
    fg_val     = fg.get("value", 50)
    risk_score = 0

    # [v4.8 FIX 5] FGI ≤ 20: risco elevado — neutro no score, não positivo
    if fg_val <= 20:   risk_score += 0    # Medo Extremo: cautela, não bônus
    elif fg_val <= 25: risk_score += 1    # Medo forte: bônus reduzido
    elif fg_val <= 50: risk_score += 2    # Medo moderado: potencial reversão
    elif fg_val >= 75: risk_score -= 1    # Ganância: risco de topo

    if "STRONG_BUY" in btc_4h_str: risk_score += 2
    elif "BUY" in btc_4h_str:      risk_score += 1
    elif "SELL" in btc_4h_str:     risk_score -= 2

    # Mercado desfavorável — bot desligado
    if fg_val >= 80 and "SELL" in btc_4h_str:
        return {"verdict": "DESFAVORÁVEL (Bot Desligado)", "threshold": 99,
                "risk_score": risk_score, "fg": fg_val, "btc": btc_4h_str}

    # [v4.8 FIX 4] Threshold adaptativo corrigido
    # FGI ≤ 20 força CAUTELOSO independente do BTC (Medo Extremo = Bear)
    if fg_val <= 20:
        threshold = 20; verdict = "CAUTELOSO (Medo Extremo)"
    elif fg_val <= 30 and "BUY" in btc_4h_str:
        threshold = 14; verdict = "FAVORÁVEL (Bull)"
    elif fg_val >= 75 or "SELL" in btc_4h_str:
        threshold = 20; verdict = "CAUTELOSO (Bear)"
    else:
        threshold = 16; verdict = "MODERADO (Neutro)"

    LOG.debug(f"  Contexto: FGI={fg_val} | BTC={btc_4h_str} | "
              f"verdict={verdict} | threshold={threshold} | risk_score={risk_score}")

    return {"verdict": verdict, "threshold": threshold,
            "risk_score": risk_score, "fg": fg_val, "btc": btc_4h_str}

# ===========================================================================
# EXECUÇÃO PRINCIPAL
# ===========================================================================

async def run_scan_async():
    global LOG, LOG_FILE, TS_SCAN
    LOG, LOG_FILE, TS_SCAN = setup_logger()

    LOG.info("🚀 Setup Atirador v4.8 | Arquitetura 3 Camadas | Iniciando scan...")
    t_start = time.time()

    state = load_daily_state()
    pode_operar, motivo_risco = check_risk_limits(state)
    LOG.info(f"💼 Estado diário: P&L={state.get('pnl_dia',0):+.2f} | "
             f"Trades abertos={len(state.get('trades_abertos',[]))}/{MAX_TRADES_ABERTOS} | "
             f"Pode operar={'✅' if pode_operar else '❌ '+motivo_risco}")

    async with aiohttp.ClientSession() as session:

        # -------------------------------------------------------------------
        # ETAPA 1: Tickers + Fear & Greed (paralelo)
        # -------------------------------------------------------------------
        log_section("ETAPA 1 — Tickers Bitget + Fear & Greed")
        perpetuals, total_items = fetch_perpetuals()
        symbols    = [d["symbol"] for d in perpetuals[:TOP_N]]
        LOG.info(f"  Analisando TOP {TOP_N}: {[d['base_coin'] for d in perpetuals[:TOP_N]]}")

        tv_4h_task = fetch_tv_batch_async(session, symbols, COLS_4H)
        fg_task    = fetch_fear_greed_async(session)
        tv_4h, fg  = await asyncio.gather(tv_4h_task, fg_task)

        # -------------------------------------------------------------------
        # GATE 1 — Camada 4H
        # -------------------------------------------------------------------
        log_section("GATE 1 — Direção 4H (descarta SELL/STRONG_SELL)")
        gate1_passed    = []
        gate1_rejected  = 0
        tokens_sem_dados = []   # [v4.8 FIX 6] tokens com val=None do TradingView

        for d in perpetuals[:TOP_N]:
            sym    = d["symbol"]
            ind_4h = tv_4h.get(sym, {})
            raw_val = ind_4h.get("Recommend.All|240")
            rsi_4h  = sf(ind_4h.get("RSI|240"), default=50.0)

            # [v4.8 FIX 6] Distingue NEUTRAL real de NEUTRAL por ausência de dados
            if raw_val is None:
                tokens_sem_dados.append(d["base_coin"])
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 4H=SEM_DADOS (val=None) "
                            f"— excluído do pipeline (ausência de dados TV)")
                continue   # Exclui silenciosamente — não é NEUTRAL, é ausência

            s4h = recommendation_from_value(raw_val)
            d["summary_4h"] = s4h
            d["rsi_4h"]     = rsi_4h

            if "SELL" in s4h:
                gate1_rejected += 1
                LOG.debug(f"  ❌  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) "
                          f"RSI={rsi_4h:.1f} — REJEITADO (SELL)")
            else:
                gate1_passed.append(d)
                LOG.debug(f"  ✅  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) "
                          f"RSI={rsi_4h:.1f} — PASSOU")

        LOG.info(f"  Gate 4H: {len(gate1_passed)} passaram | "
                 f"{gate1_rejected} rejeitados (SELL) | "
                 f"{len(tokens_sem_dados)} sem dados TV")
        if tokens_sem_dados:
            LOG.info(f"  Sem dados TV: {tokens_sem_dados}")

        # -------------------------------------------------------------------
        # GATE 2 — Camada 1H
        # -------------------------------------------------------------------
        log_section("GATE 2 — Estrutura 1H (exige BUY/STRONG_BUY)")
        symbols_1h = [d["symbol"] for d in gate1_passed]
        tv_1h      = await fetch_tv_batch_async(session, symbols_1h, COLS_1H)

        gate2_passed   = []
        gate2_rejected = 0
        for d in gate1_passed:
            sym    = d["symbol"]
            ind_1h = tv_1h.get(sym, {})
            raw_1h = ind_1h.get("Recommend.All|60")

            # [v4.8 FIX 6] val=None no 1H → sem dados, não NEUTRAL
            if raw_1h is None:
                if d["base_coin"] not in tokens_sem_dados:
                    tokens_sem_dados.append(d["base_coin"])
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 1H=SEM_DADOS (val=None) "
                            f"— excluído do pipeline (ausência de dados TV)")
                gate2_rejected += 1
                continue

            s1h = recommendation_from_value(raw_1h)
            d["summary_1h"] = s1h

            if "BUY" not in s1h:
                gate2_rejected += 1
                LOG.debug(f"  ❌  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — REJEITADO")
            else:
                gate2_passed.append(d)
                LOG.debug(f"  ✅  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — PASSOU")

        LOG.info(f"  Gate 1H: {len(gate2_passed)} passaram | {gate2_rejected} rejeitados")

        if not gate2_passed:
            LOG.warning("  ⚠️  Nenhum token passou os 2 gates — encerrando scan")
            ts_full = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
            btc     = recommendation_from_value(tv_4h.get("BTCUSDT", {}).get("Recommend.All|240"))
            ctx     = analyze_market_context(fg, btc)
            report  = f"{'='*55}\n"
            report += f"🎯 SETUP ATIRADOR v4.8\n"
            report += f"📅 {ts_full}\n"
            report += f"📋 Log: {os.path.basename(LOG_FILE)}\n"
            report += f"{'='*55}\n"
            report += f"📊 Contexto: {ctx['verdict']} | FGI: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
            report += f"⏱️  Execução: {time.time()-t_start:.1f}s | Analisados: {total_items}\n"
            report += f"\n⚠️  Nenhum token passou os dois gates.\n"
            report += f"   Gate 4H (não SELL): {len(gate1_passed)}/{TOP_N}\n"
            report += f"   Gate 1H (BUY/STRONG_BUY): 0/{len(gate1_passed)}\n"
            if tokens_sem_dados:
                report += f"   Sem dados TV ({len(tokens_sem_dados)}): {', '.join(tokens_sem_dados)}\n"
            report += f"\n   Aguarde próximo scan ou verifique o TradingView manualmente.\n"
            report += f"\n📋 Log completo: {LOG_FILE}\n"
            LOG.info(report)

            output_path = f"/tmp/atirador_SCAN_{TS_SCAN}.txt"
            with open(output_path, "w") as f: f.write(report)
            return report

        # -------------------------------------------------------------------
        # ETAPA 3: Indicadores 15m TradingView
        # -------------------------------------------------------------------
        log_section("ETAPA 3 — Indicadores 15m (TradingView)")
        symbols_15m = [d["symbol"] for d in gate2_passed]
        tv_15m      = await fetch_tv_batch_async(session, symbols_15m, COLS_15M)
        for d in gate2_passed:
            sym               = d["symbol"]
            ind_15m           = tv_15m.get(sym, {})
            d["_ind_15m"]     = ind_15m
            d["bb_upper_15m"] = sf(ind_15m.get("BB.upper|15"))
            d["bb_lower_15m"] = sf(ind_15m.get("BB.lower|15"))
            d["atr_15m"]      = sf(ind_15m.get("ATR|15"))
            LOG.debug(f"  {d['base_coin']:<8} ATR={d['atr_15m']:.4f} | "
                      f"BB_lower={d['bb_lower_15m']:.4f} | BB_upper={d['bb_upper_15m']:.4f} | "
                      f"FR={d['funding_rate']:.5f}")

        # Score parcial para ordenar antes de buscar klines
        # [v4.8] Score parcial é calculado SEM klines — apenas P1/P2/P3/P7/P8
        # O breakdown completo (com pilares 4H e 1H zerados) seria enganoso no log.
        # Suprimido: calculate_score já loga o breakdown — aqui só queremos o total.
        log_section("ETAPA 3b — Score parcial (sem klines, para ordenação)")
        for d in gate2_passed:
            sc_p, _, _ = calculate_score(d, fg_value=fg.get("value", 50),
                                         log_breakdown=False)   # suprime breakdown no parcial
            d["_partial_score"] = sc_p
            # Log resumido: apenas o score parcial, sem breakdown dos pilares
            LOG.debug(f"  {d['base_coin']:<8} score parcial (sem klines): {sc_p}/26 "
                      f"[FR={d.get('funding_rate',0):.4%} BB={d.get('bb_lower_15m',0):.4f}-{d.get('bb_upper_15m',0):.4f}]")

        gate2_passed.sort(key=lambda x: x["_partial_score"], reverse=True)
        gate2_passed = [d for d in gate2_passed if d["_partial_score"] >= 0]
        LOG.info(f"  Ordem por score parcial: {[d['base_coin'] for d in gate2_passed]}")

        gate2_passed.sort(key=lambda x: x["_partial_score"], reverse=True)
        gate2_passed = [d for d in gate2_passed if d["_partial_score"] >= 0]

        # -------------------------------------------------------------------
        # ETAPA 4: Klines (15m, 1H, 4H) para TOP N
        # -------------------------------------------------------------------
        log_section(f"ETAPA 4 — Klines + Score completo (TOP {KLINE_TOP_N})")
        top_full  = gate2_passed[:KLINE_TOP_N]
        top_light = gate2_passed[KLINE_TOP_N:KLINE_TOP_N_LIGHT]

        results     = []
        observacoes = []

        if top_full:
            LOG.info(f"  Buscando klines para: {[d['base_coin'] for d in top_full]}")
            tasks_15m = [fetch_klines_async(session, d["symbol"], "15m") for d in top_full]
            tasks_1h  = [fetch_klines_cached_async(session, d["symbol"], "1H") for d in top_full]
            tasks_4h  = [fetch_klines_cached_async(session, d["symbol"], "4H") for d in top_full]

            k15m_all, k1h_all, k4h_all = await asyncio.gather(
                asyncio.gather(*tasks_15m),
                asyncio.gather(*tasks_1h),
                asyncio.gather(*tasks_4h),
            )

            for i, d in enumerate(top_full):
                k15m = k15m_all[i]
                k1h  = k1h_all[i]
                k4h  = k4h_all[i]
                sym  = d["base_coin"]

                LOG.info(f"  ─ Analisando {sym}: "
                         f"15m={len(k15m)} candles | "
                         f"1H={len(k1h)} candles | "
                         f"4H={len(k4h)} candles")

                if not k15m:
                    LOG.warning(f"  ⚠️  {sym}: klines 15m vazios — pulando token")
                    continue
                if not k1h:
                    LOG.warning(f"  ⚠️  {sym}: klines 1H vazios — P-1H será 0")
                if not k4h:
                    LOG.warning(f"  ⚠️  {sym}: klines 4H vazios — P4/P5/P6 serão 0")

                sc, reasons, bd = calculate_score(
                    d,
                    candles_15m=k15m,
                    candles_1h=k1h,
                    candles_4h=k4h,
                    fg_value=fg.get("value", 50),
                )
                d["score"]     = sc
                d["reasons"]   = reasons
                d["breakdown"] = bd

                trade    = calc_trade_params(d["price"], d.get("atr_15m", 0), score=d.get("score", 0))
                trade_ok = trade is not None

                if trade_ok:
                    LOG.info(f"  📊 {sym}: score={sc}/26 | "
                             f"entry={trade['entry']:.4f} | SL={trade['sl_distance_pct']:.2f}% | "
                             f"alav={trade['alavancagem']}x | trade_params=✅")
                    d["trade"] = trade
                    results.append(d)
                else:
                    LOG.warning(f"  📊 {sym}: score={sc}/26 | trade_params=❌ "
                                f"(ATR={d.get('atr_15m',0):.4f} — inválido para SL dinâmico)")

        # Análise leve (sem klines) — seção Observações
        log_section("ETAPA 4b — Análise leve (sem klines)")
        for d in top_light:
            sc, reasons, bd = calculate_score(d, fg_value=fg.get("value", 50),
                                              log_breakdown=False)  # sem klines, breakdown enganoso
            d["score"]     = sc
            d["reasons"]   = reasons
            d["breakdown"] = bd
            trade = calc_trade_params(d["price"], d.get("atr_15m", 0), score=d.get("score", 0))
            if trade:
                d["trade"] = trade
                observacoes.append(d)
                LOG.debug(f"  {d['base_coin']:<8} score parcial={sc}/26 → Em Observação")

        # -------------------------------------------------------------------
        # ETAPA 5: Contexto e Relatório
        # -------------------------------------------------------------------
        log_section("ETAPA 5 — Contexto de Mercado e Relatório")
        results.sort(key=lambda x: x["score"], reverse=True)
        observacoes.sort(key=lambda x: x["score"], reverse=True)

        btc_4h_val = tv_4h.get("BTCUSDT", {}).get("Recommend.All|240")
        btc_4h_str = recommendation_from_value(btc_4h_val)
        ctx        = analyze_market_context(fg, btc_4h_str)

        LOG.info(f"  BTC 4H: {btc_4h_str} (val={btc_4h_val}) | Threshold: {ctx['threshold']} pts")
        LOG.info(f"  Results: {len(results)} tokens | Observações: {len(observacoes)} tokens")
        for r in results:
            LOG.info(f"    {r['base_coin']}: {r['score']}/26 | {', '.join(r['reasons'][:3])}")

        ts_full     = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")   # Para o relatório
        ts_file     = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")       # Para o nome do arquivo
        risco_usd   = RISCO_POR_TRADE_USD
        perda_max   = MAX_PERDA_DIARIA_USD
        pnl_dia     = state.get("pnl_dia", 0.0)
        n_abertos   = len(state.get("trades_abertos", []))

        report  = f"{'='*58}\n"
        report += f"🎯 SETUP ATIRADOR v4.8\n"
        report += f"📅 {ts_full}\n"
        report += f"📋 Log: {os.path.basename(LOG_FILE)}\n"
        report += f"{'='*58}\n"
        report += f"📊 CONTEXTO DE MERCADO: {ctx['verdict']}\n"
        report += f"   Fear & Greed: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        report += f"   Threshold alerta: {ctx['threshold']} pts | Risk Score: {ctx['risk_score']}\n"
        report += f"{'='*58}\n\n"

        report += f"💼 GESTÃO DE RISCO — Estratégia de Recuperação\n"
        report += f"   Banca: ${BANKROLL:.2f} | Risco fixo/trade: ${risco_usd:.2f} | Perda máx/dia: ${perda_max:.2f}\n"
        report += f"   Ganho/trade (RR1:2): ${risco_usd * RR_MINIMO:.2f} | Para dobrar banca: ~{int(BANKROLL/risco_usd/RR_MINIMO)+1} winners\n"
        report += f"   P&L hoje: ${pnl_dia:+.2f} | Trades abertos: {n_abertos}/{MAX_TRADES_ABERTOS}\n"
        if not pode_operar:
            report += f"   🛑 NOVAS ENTRADAS BLOQUEADAS: {motivo_risco}\n"
        else:
            report += f"   ✅ Pode operar — {MAX_TRADES_ABERTOS - n_abertos} slot(s) disponível(is)\n"
        report += f"\n"

        report += f"🔍 PIPELINE\n"
        report += f"   Universo: {total_items} tokens | Inst. filter: {len(perpetuals[:TOP_N])}\n"
        report += f"   Gate 4H (não SELL): {len(gate1_passed)} | Gate 1H (BUY+): {len(gate2_passed)}\n"
        report += f"   Análise completa: {len(top_full)} | Análise leve: {len(top_light)}\n"
        # [v4.8 FIX 7] Transparência: tokens excluídos por ausência de dados TV
        if tokens_sem_dados:
            report += f"   ⚠️  Sem dados TradingView ({len(tokens_sem_dados)}): "
            report += f"{', '.join(tokens_sem_dados)}\n"
        report += "\n"

        # --- Alertas Fortes ---
        alertas = [r for r in results if r["score"] >= ctx["threshold"]]

        if ctx["threshold"] == 99:
            report += "🛑 BOT DESLIGADO — Mercado desfavorável para LONGs.\n"
            report += f"   FGI ≥ 80 + BTC 4H SELL = risco extremo de operar.\n"
        elif not alertas:
            # [v4.9] Mostra score máximo obtido para referência
            max_sc = max((r["score"] for r in results), default=0)
            report += f"ℹ️  Nenhum alerta forte (score ≥ {ctx['threshold']}) no momento.\n"
            if results:
                report += f"   Score máximo desta execução: {max_sc}/26 "
                report += f"(faltam {ctx['threshold'] - max_sc} pts para alerta)\n"
        else:
            report += f"🔥 {len(alertas)} ALERTA(S) FORTE(S) — Score ≥ {ctx['threshold']}/26:\n\n"
            for r in alertas:
                t       = r["trade"]
                bloq    = " ⛔ BLOQUEADO" if not pode_operar else ""
                s4h_ico = "🟢" if "BUY" in r["summary_4h"] else "🟡"
                report += f"🚀 {r['base_coin']}{bloq}\n"
                report += f"   Score: {r['score']}/26 | {s4h_ico} 4H: {r['summary_4h']} | 1H: {r['summary_1h']}\n"
                report += f"   Razões: {', '.join(r['reasons'][:4])}\n"
                report += f"   Preço: ${r['price']:.4f} | Vol 24h: ${r['turnover_24h']/1e6:.1f}M | OI: ${r['oi_usd']/1e6:.1f}M\n"
                # [v4.9] Campos expandidos com nova estratégia
                report += f"   Alavancagem: {t['alavancagem']}x (máx score: {t['alav_max_score']:.0f}x)\n"
                report += f"   Risco: ${t['risco_usd']:.2f} | Ganho esperado (RR1:2): ${t['ganho_rr2_usd']:.2f}\n"
                report += f"   SL:  ${t['sl']:.4f}  (-{t['sl_distance_pct']:.2f}%)\n"
                report += f"   TP1: ${t['tp1']:.4f} (+{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
                report += f"   TP2: ${t['tp2']:.4f} (+{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
                report += f"   TP3: ${t['tp3']:.4f} (+{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
                report += f"   Trailing: ativar no TP1 → SL para breakeven +0.5%\n\n"

        # --- Oportunidades em Formação ---
        # [v4.6 FIX] Threshold inferior reduzido de 10 para 5 pts para capturar
        # tokens em Bear Market (score máximo sem klines 4H/1H é ~11 pts).
        # Separado em duas faixas para clareza.
        oport_media  = [r for r in results if ctx["threshold"] > r["score"] >= 10]
        oport_baixa  = [r for r in results if 5 <= r["score"] < 10]

        if oport_media:
            report += f"\n📈 OPORTUNIDADES EM FORMAÇÃO ({len(oport_media)}) — Score 10–{ctx['threshold']-1}:\n"
            for r in oport_media[:5]:
                report += (f"   ▶ {r['base_coin']} | {r['score']}/26 | 4H: {r['summary_4h']} | "
                           f"{', '.join(r['reasons'][:2])}\n")

        if oport_baixa:
            report += f"\n🔎 RADAR (score baixo, análise completa) — Score 5–9:\n"
            report += f"   ⚠️  Não operar — insuficiente. Monitorar para confluência.\n"
            for r in oport_baixa[:5]:
                report += f"   · {r['base_coin']} | {r['score']}/26 | {r['summary_4h']} 4H\n"

        # --- Em Observação (análise leve — sem klines) ---
        obs_relevantes = [o for o in observacoes if o["score"] >= 8]
        if obs_relevantes:
            report += f"\n👁️  EM OBSERVAÇÃO — análise leve, sem klines ({len(obs_relevantes)} tokens):\n"
            report += f"   ⚠️  Scores parciais — não são alertas confirmados.\n"
            for o in obs_relevantes[:5]:
                report += f"   · {o['base_coin']} | Score parcial: {o['score']}/26 | {o['summary_4h']} 4H\n"

        elapsed = time.time() - t_start
        report += f"\n{'-'*58}\n"
        report += f"⏱️  Execução: {elapsed:.1f}s | Analisados: {total_items} tokens\n"
        report += f"📁 Estado diário: {STATE_FILE}\n"
        report += f"📋 Log completo: {LOG_FILE}\n"

        # [v4.8 FIX 2] Nome do relatório com mesmo timestamp do log
        # atirador_SCAN_YYYYMMDD_HHMM.txt ↔ atirador_LOG_YYYYMMDD_HHMM.log
        output_path = f"/tmp/atirador_SCAN_{TS_SCAN}.txt"
        with open(output_path, "w") as f: f.write(report)

        LOG.info(report)
        LOG.info(f"✅ Scan concluído em {elapsed:.1f}s | Relatório: {output_path} | Log: {LOG_FILE}")
        return report


def main():
    # Logger inicializado dentro de run_scan_async (precisa do timestamp de execução)
    asyncio.run(run_scan_async())

if __name__ == "__main__":
    main()
