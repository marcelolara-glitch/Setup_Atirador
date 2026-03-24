#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v5.2 - Scanner Profissional de Criptomoedas
=============================================================================
Arquitetura Multi-Timeframe com 3 Camadas Independentes:

  CAMADA 1 — 4H "Qual é a direção do mercado?" (contexto macro)
  CAMADA 2 — 1H "Estamos num bom ponto de entrada?" (estrutura)
  CAMADA 3 — 15m "O timing de entrada está correto agora?" (gatilho)

Score máximo: 26 pts
Thresholds: Favorável ≥16 | Moderado ≥18 | Cauteloso(Bear/Medo Extremo) ≥20

v5.2 — Fix CoinGecko parser + Fallback de klines OKX:

  [v5.2 — FIX CoinGecko parser]
    Problema identificado (rodadas 22/03): CoinGecko retornava 21.094 itens
    com HTTP 200, mas TODOS eram rejeitados pelo parser.
    Causa: endpoint /derivatives agrega contratos de centenas de exchanges
    com formatos de símbolo inconsistentes (BTC_USDT, BTC/USDT, BTC-USDT)
    e volumes em unidades heterogêneas (base coin, contratos, USD).

    Solução: trocar para endpoint específico por exchange.
    Novo endpoint: /derivatives/exchanges/{id}/tickers
    Estratégia: tentar as 3 principais exchanges com melhor cobertura:
      - bybit_futures  → ~400 contratos USDT perpetuais
      - okex_swap      → ~300 contratos USDT perpetuais
      - binance_futures → ~400 contratos USDT perpetuais (maior cobertura)
    O primeiro que retornar dados válidos é usado.
    Formato é limpo e consistente — mesmo layout para todas as exchanges.
    Resultado esperado: 300-400 tokens qualificados, sem rejeição em massa.

  [v5.2 — FIX Klines OKX fallback]
    Problema: tokens listados na OKX mas não na Bitget (ex: BEAT, LIGHT,
    TRUTH, ZAMA) geravam HTTP 400 na busca de klines, sendo descartados.
    Solução: quando Bitget retorna HTTP 400 (símbolo desconhecido), o
    fetch_klines_async tenta automaticamente a OKX como fallback.
    Endpoint OKX: /api/v5/market/candles?instId=BEAT-USDT-SWAP&bar=15m
    Granularidade mapeada: 15m→15m | 1H→1H | 4H→4H (formato idêntico)
    Log registra claramente qual fonte forneceu cada kline.

v5.1: Hierarquia 3 fontes, timeout 8s, pump bloqueados no relatório, RSI>80
v5.0: Fonte dual Bybit/Bitget, filtros relaxados ($2M/$5M), TOP_N removido
v4.9: Perpétuos puros + Estratégia de Recuperação de Banca
v4.8: FGI≤20 CAUTELOSO, tokens sem dados excluídos, Bollinger robusto
v4.7: Sistema de log completo, cache anti-corrupção

Autor: Manus AI | v4.1→v5.2 (revisão Claude/Anthropic)
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

# Filtros Institucionais [v5.0]
# MIN_TURNOVER_24H: $5M → $2M  (captura mais altcoins com liquidez real em perpetuals)
# MIN_OI_USD:      $10M → $5M  (OI de $5M garante execução sem slippage relevante)
# TOP_N removido: todos os qualificados entram no pipeline.
#   Gargalo real = KLINE_TOP_N (busca de klines), não o universo de entrada.
MIN_TURNOVER_24H = 2_000_000
MIN_OI_USD       = 5_000_000

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
    [v5.1 FIX]   Fallback de prefixo: tenta BITGET:XUSDT.P para tokens
                 sem retorno no BYBIT: — reduz tokens sem dados TV quando
                 a fonte de tickers é Bitget ou token só existe na Bitget.
    """
    if not symbols: return {}

    # Tentativa 1: prefixo BYBIT (padrão)
    tickers_bybit = [f"BYBIT:{s}.P" for s in symbols]
    payload = {"symbols": {"tickers": tickers_bybit, "query": {"types": []}},
               "columns": columns}

    LOG.debug(f"  TV batch: {len(symbols)} tokens (.P perpétuo) | cols: {columns}")

    result = {}
    for attempt in range(retries):
        try:
            t0 = time.time()
            async with session.post(TV_URL, json=payload,
                                    headers=TV_HEADERS, timeout=25) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                data    = json.loads(raw.decode("utf-8"))
                for item in data.get("data", []):
                    sym  = item["s"].replace("BYBIT:", "").replace(".P", "")
                    vals = item["d"]
                    none_cols = [c for c, v in zip(columns, vals) if v is None]
                    if none_cols:
                        LOG.debug(f"    ⚠️  {sym}: valores None em: {none_cols}")
                    result[sym] = dict(zip(columns, vals))

                missing = [s for s in symbols if s not in result]
                LOG.debug(f"  ✅  TV batch BYBIT: {len(result)}/{len(symbols)} retornados | {elapsed:.2f}s")

                # [v5.1 FIX] Fallback BITGET: para tokens sem retorno no BYBIT:
                if missing:
                    LOG.warning(f"  ⚠️  TV BYBIT: {len(missing)} sem retorno: {missing}")
                    LOG.info(f"  🔄  [v5.1] Tentando prefixo BITGET: para {len(missing)} tokens sem dados...")
                    tickers_bitget = [f"BITGET:{s}.P" for s in missing]
                    payload_fb = {"symbols": {"tickers": tickers_bitget, "query": {"types": []}},
                                  "columns": columns}
                    try:
                        t1 = time.time()
                        async with session.post(TV_URL, json=payload_fb,
                                                headers=TV_HEADERS, timeout=15) as resp_fb:
                            elapsed_fb = time.time() - t1
                            raw_fb     = await resp_fb.read()
                            data_fb    = json.loads(raw_fb.decode("utf-8"))
                            recovered  = []
                            for item in data_fb.get("data", []):
                                sym_fb = item["s"].replace("BITGET:", "").replace(".P", "")
                                if sym_fb in missing:
                                    result[sym_fb] = dict(zip(columns, item["d"]))
                                    recovered.append(sym_fb)
                            still_missing = [s for s in missing if s not in result]
                            LOG.info(f"  ✅  TV BITGET: recuperados {len(recovered)}: {recovered} | {elapsed_fb:.2f}s")
                            if still_missing:
                                LOG.warning(f"  ⚠️  TV sem dados (ambos prefixos): {still_missing}")
                    except Exception as e_fb:
                        LOG.warning(f"  ⚠️  TV BITGET: fallback falhou: {type(e_fb).__name__}: {e_fb}")

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
    [v5.2] Busca klines com fallback OKX quando Bitget retorna 400.

    Fluxo:
      1. Tenta Bitget (comportamento original)
      2. Se Bitget retorna HTTP 400 (símbolo desconhecido), tenta OKX
         ex: BEATUSDT → BEAT-USDT-SWAP na OKX
      3. Loga claramente qual fonte forneceu os klines

    Mapeamento de granularidade Bitget → OKX:
      15m → 15m | 1H → 1H | 4H → 4H (formato idêntico)

    HTTP 400 da Bitget = símbolo existe na OKX mas não na Bitget.
    Isso ocorre com tokens listados exclusivamente na OKX (ex: BEAT, LIGHT).
    """
    # --- Tentativa 1: Bitget ---
    url_bitget = (f"https://api.bitget.com/api/v2/mix/market/candles"
                  f"?productType=USDT-FUTURES&symbol={symbol}"
                  f"&granularity={granularity}&limit={limit}")
    try:
        data = await api_get_async(session, url_bitget, headers=BITGET_HEADERS)

        if data is None:
            # api_get_async retorna None em HTTP != 200 (inclui 400)
            # Verificar se é 400 pelo log já feito — tentar OKX
            LOG.debug(f"  🔄  [{symbol} {granularity}] Bitget sem resposta → tentando OKX")
        elif "data" not in data:
            LOG.error(f"  ❌  Klines {symbol} {granularity}: campo 'data' ausente na Bitget")
            LOG.debug(f"      Resposta recebida: {str(data)[:120]}")
        else:
            raw_candles = data["data"]
            if raw_candles:
                result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                           "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                          for c in raw_candles]
                result.reverse()
                ts_ini = datetime.fromtimestamp(result[0]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
                ts_fim = datetime.fromtimestamp(result[-1]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
                LOG.debug(f"  ✅  Klines {symbol} {granularity}: {len(result)} candles | {ts_ini} → {ts_fim}")
                return result
            else:
                LOG.warning(f"  ⚠️  Klines {symbol} {granularity}: 'data' vazio na Bitget")

    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async Bitget {symbol} {granularity}: {type(e).__name__}: {e}")

    # --- Tentativa 2: OKX fallback [v5.2 FIX] ---
    # Converte símbolo: BEATUSDT → BEAT-USDT-SWAP
    # Converte granularidade: 15m→15m | 1H→1H | 4H→4H
    base_coin  = symbol.replace("USDT", "")
    okx_instid = f"{base_coin}-USDT-SWAP"
    okx_bar    = granularity  # OKX usa mesmo formato: 15m, 1H, 4H
    url_okx    = (f"https://www.okx.com/api/v5/market/candles"
                  f"?instId={okx_instid}&bar={okx_bar}&limit={limit}")

    LOG.info(f"  🔄  [v5.2] Klines {symbol} {granularity}: Bitget falhou → tentando OKX ({okx_instid})")
    try:
        data_okx = await api_get_async(session, url_okx)
        if data_okx and "data" in data_okx and data_okx["data"]:
            raw = data_okx["data"]
            # OKX candles: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
            result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                       "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                      for c in raw]
            result.reverse()
            ts_ini = datetime.fromtimestamp(result[0]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
            ts_fim = datetime.fromtimestamp(result[-1]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
            LOG.info(f"  ✅  Klines {symbol} {granularity} via OKX: {len(result)} candles | {ts_ini} → {ts_fim}")
            return result
        else:
            LOG.warning(f"  ⚠️  Klines {symbol} {granularity}: OKX também sem dados — token descartado")
            return []

    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async OKX {symbol} {granularity}: {type(e).__name__}: {e}")
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
# DADOS DE MERCADO — Hierarquia de Fontes [v5.1]
# ===========================================================================
# Problema identificado (logs 21/03 e 22/03):
#   Bybit: timeout 92s na rodada 1 / HTTP 403 geo-block na rodada 2
#   Causa raiz: exchanges bloqueiam bots não autenticados por design.
#
# Nova hierarquia (ordem de tentativa):
#   Fonte 1: CoinGecko /derivatives  → agregador neutro, sem geo-block
#   Fonte 2: OKX /market/tickers     → exchange liberal, 600+ perpetuals
#   Fonte 3: Bitget                  → último recurso, estável e já integrado
#   Bybit: REMOVIDA (2/2 falhas em produção)
#
# Klines: continuam da Bitget (endpoint separado, nunca falhou).
#
# Logging de diagnóstico: cada tentativa registra URL, HTTP status,
# tempo de resposta, motivo de falha e tokens retornados.
# Objetivo: identificar rapidamente qual fonte está bloqueando.
# ===========================================================================

# Timeout reduzido para tickers [v5.1 FIX]
# 8s é suficiente para tickers (payload ~50-200KB). Se não responde em 8s,
# cai para próxima fonte imediatamente. Antes: 20s (deixava Bybit travar 92s).
TICKER_TIMEOUT = 8

DATA_SOURCE          = "desconhecida"   # fonte que efetivamente respondeu
DATA_SOURCE_ATTEMPTS = []               # histórico de tentativas desta execução

def _log_source_attempt(fonte, url, status, elapsed, tokens_brutos,
                         qualificados, motivo_falha=None):
    """
    Registra no LOG e em DATA_SOURCE_ATTEMPTS o diagnóstico completo
    de cada tentativa de fonte. Fundamental para debug de bloqueios.
    """
    entrada = {
        "fonte"        : fonte,
        "url"          : url[:80],
        "status"       : status,
        "elapsed_s"    : round(elapsed, 2),
        "tokens_brutos": tokens_brutos,
        "qualificados" : qualificados,
        "falha"        : motivo_falha,
    }
    DATA_SOURCE_ATTEMPTS.append(entrada)

    if motivo_falha:
        LOG.warning(f"  ⛔  [{fonte}] FALHOU | HTTP {status} | "
                    f"{elapsed:.2f}s | motivo: {motivo_falha}")
        LOG.warning(f"      URL: {url[:100]}")
    else:
        LOG.info(f"  ✅  [{fonte}] OK | HTTP {status} | {elapsed:.2f}s | "
                 f"{tokens_brutos} brutos → {qualificados} qualificados")


def _parse_coingecko_tickers(items):
    """
    [v5.2 FIX] Normaliza tickers do CoinGecko /derivatives/exchanges/{id}/tickers.

    Endpoint específico por exchange — retorna apenas contratos daquela exchange
    com formato limpo e consistente (diferente do /derivatives genérico que
    misturava 21.094 itens de centenas de exchanges com formatos inconsistentes).

    Campos do endpoint específico:
      symbol      → ex: "BTCUSDT" (já no formato correto, sem separadores)
      last        → preço atual
      index       → preço do índice (fallback se last=0)
      volume_24h  → volume em USD das últimas 24h
      open_interest_usd → OI em USD direto
      funding_rate → taxa de financiamento decimal
      price_change_percentage_24h → variação em %
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0
    for t in items:
        sym = str(t.get("symbol", "")).strip()
        # Normalizar: remover separadores comuns
        sym = sym.replace("/", "").replace("-", "").replace("_", "").upper()
        if not sym.endswith("USDT"):
            rej_symbol += 1; continue
        # Volume: usar o campo em USD
        turnover = sf(t.get("volume_24h", 0) or t.get("h24_volume", 0))
        if turnover <= 0:
            rej_vol += 1; continue
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue
        # OI: campo direto em USD no endpoint específico
        oi_usd = sf(t.get("open_interest_usd", 0))
        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue
        price = sf(t.get("last", 0) or t.get("index", 0))
        if price <= 0:
            rej_symbol += 1; continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "volume_24h"      : turnover,
            "funding_rate"    : sf(t.get("funding_rate", 0)),
            "price_change_24h": sf(t.get("price_change_percentage_24h", 0)),
        })
    return qualified, rej_vol, rej_oi


def _parse_okx_tickers(items):
    """
    Normaliza tickers da OKX /v5/market/tickers?instType=SWAP.
    OKX retorna SWAPs (perpétuos) com campos:
      instId      → ex: "BTC-USDT-SWAP" → normalizamos para "BTCUSDT"
      volCcy24h   → volume em USDT nas últimas 24h
      oi          → open interest em contratos (× ctVal × last = USD)
      last        → preço atual
      fundingRate → taxa de financiamento
      chg24h      → variação % 24h decimal (ex: 0.0123 = 1.23%)
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0
    for t in items:
        inst = t.get("instId", "")
        if not inst.endswith("-USDT-SWAP"):
            rej_symbol += 1; continue
        sym  = inst.replace("-USDT-SWAP", "") + "USDT"
        base = sym.replace("USDT", "")
        turnover = sf(t.get("volCcy24h", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue
        price  = sf(t.get("last", 0))
        # OI da OKX: em contratos × ctVal (tamanho do contrato)
        # volCcy24h já está em USDT, mas oi está em contratos
        # Estimamos OI em USD como: oi_contracts × ctVal × price
        # ctVal não está no ticker — usamos volCcy24h/20 como proxy conservador
        oi_usd = sf(t.get("openInterest", 0)) * price
        if oi_usd == 0:
            # fallback: estimativa pelo volume (OI tipicamente ~5-20% do vol diário)
            oi_usd = turnover * 0.1
        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "volume_24h"      : turnover,
            "funding_rate"    : sf(t.get("fundingRate", 0)),
            "price_change_24h": sf(t.get("chg24h", 0)) * 100,
        })
    return qualified, rej_vol, rej_oi


def _parse_bitget_tickers(items):
    """
    Normaliza tickers Bitget para a estrutura interna padrão (comportamento v4.9).
    """
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
    return qualified, rej_vol, rej_oi


def _try_source(nome, url, parse_fn, extract_fn, timeout=None):
    """
    Tenta buscar tickers de uma fonte com diagnóstico completo.
    Retorna (qualified, total_brutos) em caso de sucesso, ou None em falha.

    Parâmetros:
      nome       : label da fonte para o log ("CoinGecko", "OKX", "Bitget")
      url        : URL completa do endpoint
      parse_fn   : função que normaliza os itens brutos
      extract_fn : função que extrai a lista de items do JSON retornado
      timeout    : timeout em segundos (padrão: TICKER_TIMEOUT)
    """
    t_used = timeout or TICKER_TIMEOUT
    LOG.info(f"  📡  [{nome}] Tentando: {url[:80]}{'...' if len(url) > 80 else ''}")
    LOG.debug(f"       timeout={t_used}s | filtros: vol≥${MIN_TURNOVER_24H/1e6:.1f}M OI≥${MIN_OI_USD/1e6:.0f}M")
    t0 = time.time()
    try:
        resp = requests.get(url, timeout=t_used,
                            headers={"Accept-Encoding": "gzip, deflate",
                                     "User-Agent": "Mozilla/5.0 (compatible; scanner/5.1)"})
        elapsed = time.time() - t0
        status  = resp.status_code
        size_kb = len(resp.content) / 1024

        LOG.debug(f"       → HTTP {status} | {size_kb:.1f}KB | {elapsed:.2f}s")

        if status != 200:
            motivo = f"HTTP {status} — {'Forbidden/geo-block' if status == 403 else 'Erro do servidor' if status >= 500 else 'Erro cliente'}"
            _log_source_attempt(nome, url, status, elapsed, 0, 0, motivo)
            return None

        data  = resp.json()
        items = extract_fn(data)

        if not items:
            motivo = "Resposta JSON vazia ou sem campo esperado"
            _log_source_attempt(nome, url, status, elapsed, 0, 0, motivo)
            LOG.warning(f"       ⚠️  [{nome}] JSON OK mas lista vazia — verifique estrutura da resposta")
            return None

        qualified, rej_vol, rej_oi = parse_fn(items)
        LOG.debug(f"       Rejeitados: {rej_vol} vol<${MIN_TURNOVER_24H/1e6:.1f}M | "
                  f"{rej_oi} OI<${MIN_OI_USD/1e6:.0f}M")
        LOG.debug(f"       TOP 5 por volume: {[d['base_coin'] for d in sorted(qualified, key=lambda x: x['turnover_24h'], reverse=True)[:5]]}")

        if not qualified:
            motivo = f"Nenhum token passou os filtros ({len(items)} brutos, todos rejeitados)"
            _log_source_attempt(nome, url, status, elapsed, len(items), 0, motivo)
            return None

        _log_source_attempt(nome, url, status, elapsed, len(items), len(qualified))
        return qualified, len(items)

    except requests.exceptions.Timeout:
        elapsed = time.time() - t0
        motivo  = f"Timeout após {elapsed:.1f}s (limite={t_used}s)"
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, motivo)
        return None
    except requests.exceptions.ConnectionError as e:
        elapsed = time.time() - t0
        motivo  = f"Erro de conexão: {str(e)[:80]}"
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, motivo)
        return None
    except (json.JSONDecodeError, ValueError) as e:
        elapsed = time.time() - t0
        motivo  = f"JSON inválido: {str(e)[:80]}"
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, motivo)
        return None
    except Exception as e:
        elapsed = time.time() - t0
        motivo  = f"{type(e).__name__}: {str(e)[:80]}"
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, motivo)
        return None


def fetch_perpetuals():
    """
    [v5.1] Busca perpetuals USDT com hierarquia de fontes robusta.

    Tenta cada fonte em ordem, registrando diagnóstico completo no LOG.
    Para na primeira que retornar dados válidos.

    Hierarquia:
      1. CoinGecko /derivatives  — agregador neutro, sem geo-block
      2. OKX /market/tickers     — exchange liberal, 600+ tokens
      3. Bitget                  — último recurso, funcional e estável
      Bybit: REMOVIDA (timeout 92s na rodada 1, HTTP 403 na rodada 2)

    Cada tentativa loga:
      - URL tentada e timeout aplicado
      - HTTP status e tempo de resposta
      - Total bruto recebido e qualificados após filtros
      - Motivo detalhado em caso de falha
    """
    global DATA_SOURCE, DATA_SOURCE_ATTEMPTS
    DATA_SOURCE_ATTEMPTS = []   # reset para esta execução

    LOG.info("📡 [v5.2] Iniciando busca de tickers — hierarquia de 3 fontes")
    LOG.info(f"   Filtros: vol≥${MIN_TURNOVER_24H/1e6:.1f}M | OI≥${MIN_OI_USD/1e6:.0f}M | timeout={TICKER_TIMEOUT}s/fonte")
    LOG.info(f"   Ordem: CoinGecko → OKX → Bitget")

    # ------------------------------------------------------------------
    # FONTE 1: CoinGecko /derivatives/exchanges/{id}/tickers  [v5.2 FIX]
    # Endpoint ESPECÍFICO por exchange — retorna apenas contratos daquela
    # exchange com formato limpo. Diferente do /derivatives genérico que
    # misturava 21k itens de centenas de fontes (bug corrigido na v5.2).
    #
    # Tentamos 3 exchanges em ordem de cobertura:
    #   bybit_futures  → ~400 contratos USDT perpétuos
    #   okex_swap      → ~300 contratos USDT perpétuos
    #   binance_futures → ~400 contratos (maior cobertura global)
    # ------------------------------------------------------------------
    cg_exchanges = [
        ("bybit_futures",   "Bybit via CoinGecko"),
        ("okex_swap",       "OKX via CoinGecko"),
        ("binance_futures", "Binance via CoinGecko"),
    ]
    for cg_id, cg_label in cg_exchanges:
        resultado = _try_source(
            nome      = f"CoinGecko/{cg_id}",
            url       = f"https://api.coingecko.com/api/v3/derivatives/exchanges/{cg_id}/tickers",
            parse_fn  = _parse_coingecko_tickers,
            extract_fn= lambda d: d.get("tickers", []) if isinstance(d, dict) else (d if isinstance(d, list) else []),
        )
        if resultado:
            DATA_SOURCE = f"CoinGecko ({cg_label})"
            qualified, total = resultado
            qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
            LOG.info(f"  ✅  [CoinGecko/{cg_id}] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
            return qualified, total

    # ------------------------------------------------------------------
    # FONTE 2: OKX /market/tickers?instType=SWAP
    # API pública sem autenticação, 600+ perpétuos USDT.
    # Menos restritiva que Bybit/Binance em geo-blocks.
    # ------------------------------------------------------------------
    resultado = _try_source(
        nome      = "OKX",
        url       = "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
        parse_fn  = _parse_okx_tickers,
        extract_fn= lambda d: d.get("data", []),
    )
    if resultado:
        DATA_SOURCE = "OKX"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [OKX] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        return qualified, total

    # ------------------------------------------------------------------
    # FONTE 3: Bitget (comportamento v4.9/v5.0 preservado)
    # Último recurso. Funcional e estável em todas as rodadas anteriores.
    # ------------------------------------------------------------------
    LOG.warning("  ⚠️  [v5.1] Fontes 1 e 2 indisponíveis — usando Bitget (último recurso)")
    resultado = _try_source(
        nome      = "Bitget",
        url       = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
        parse_fn  = _parse_bitget_tickers,
        extract_fn= lambda d: d.get("data", []),
        timeout   = 20,  # Bitget pode ser mais lento — mantém timeout maior
    )
    if resultado:
        DATA_SOURCE = "Bitget"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [Bitget] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        return qualified, total

    # ------------------------------------------------------------------
    # TODAS AS FONTES FALHARAM — erro crítico, aborta o scan
    # ------------------------------------------------------------------
    DATA_SOURCE = "NENHUMA"
    LOG.error("  ❌  TODAS AS 3 FONTES FALHARAM — scan abortado")
    LOG.error("  Resumo de tentativas:")
    for a in DATA_SOURCE_ATTEMPTS:
        LOG.error(f"    [{a['fonte']}] HTTP {a['status']} | {a['elapsed_s']}s | {a['falha']}")
    LOG.error("  Ações sugeridas:")
    LOG.error("    1. Verificar conectividade de rede")
    LOG.error("    2. Verificar se o IP está bloqueado (testar manualmente no browser)")
    LOG.error("    3. Aguardar 15-30 min e tentar novamente (rate-limit temporário)")
    raise RuntimeError("Todas as fontes de tickers falharam. Scan abortado.")

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

    # [v5.0 FIX] Para score < 20, a tabela não é consultada — loga N/A
    # para não confundir com cap real pela tabela (que seria "cap_score=10x" etc.)
    if alav_max_score == ALAVANCAGEM_MIN and score < 20:
        cap_label = f"N/A(score<20)→min={ALAVANCAGEM_MIN:.0f}x"
    else:
        cap_label = f"{alav_max_score:.0f}x"

    LOG.debug(f"    TradeParams: SL={sl_dist_pct:.2f}% | "
              f"alav_calc={alav_calculada:.1f}x → cap_score={cap_label} → "
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

    LOG.info("🚀 Setup Atirador v5.2 | Arquitetura 3 Camadas | Iniciando scan...")
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
        log_section("ETAPA 1 — Tickers (Bybit/Bitget) + Fear & Greed")
        perpetuals, total_items = fetch_perpetuals()
        # [v5.0] TOP_N removido — todos os qualificados entram no pipeline.
        # O gargalo de performance é KLINE_TOP_N (etapa 4), não o universo de entrada.
        symbols = [d["symbol"] for d in perpetuals]
        LOG.info(f"  Fonte de dados: {DATA_SOURCE} | "
                 f"Universo: {total_items} brutos → {len(perpetuals)} qualificados")
        LOG.info(f"  Analisando todos os {len(perpetuals)} tokens qualificados: "
                 f"{[d['base_coin'] for d in perpetuals[:10]]}{'...' if len(perpetuals) > 10 else ''}")

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

        for d in perpetuals:
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
                # [v5.1 FIX] Aviso de RSI extremo — ativo muito estendido
                if rsi_4h > 80:
                    LOG.warning(f"  ✅⚠️  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) "
                                f"RSI={rsi_4h:.1f} — PASSOU mas RSI EXTREMO (>{80}) — rally estendido, risco de reversão")
                    d["rsi_extremo"] = True
                else:
                    LOG.debug(f"  ✅  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) "
                              f"RSI={rsi_4h:.1f} — PASSOU")
                    d["rsi_extremo"] = False

        LOG.info(f"  Gate 4H: {len(gate1_passed)} passaram | "
                 f"{gate1_rejected} rejeitados (SELL) | "
                 f"{len(tokens_sem_dados)} sem dados TV | "
                 f"universo: {len(perpetuals)} tokens")
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
            report += f"🎯 SETUP ATIRADOR v5.2\n"
            report += f"📅 {ts_full}\n"
            report += f"📋 Log: {os.path.basename(LOG_FILE)}\n"
            report += f"{'='*55}\n"
            report += f"📊 Contexto: {ctx['verdict']} | FGI: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
            report += f"⏱️  Execução: {time.time()-t_start:.1f}s | Analisados: {total_items}\n"
            report += f"\n⚠️  Nenhum token passou os dois gates.\n"
            report += f"   Gate 4H (não SELL): {len(gate1_passed)}/{len(perpetuals)}\n"
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
        pump_bloqueados = []   # [v5.1 FIX] tokens bloqueados por pump nesta etapa
        for d in gate2_passed:
            sc_p, _, _ = calculate_score(d, fg_value=fg.get("value", 50),
                                         log_breakdown=False)
            d["_partial_score"] = sc_p
            if sc_p == -99:
                pump_bloqueados.append(d)
                LOG.warning(f"  🚫  {d['base_coin']:<8} BLOQUEADO POR PUMP | "
                            f"variação 24h={d.get('price_change_24h', 0):.1f}% | "
                            f"passou Gate 4H ({d.get('summary_4h','?')}) e Gate 1H mas descartado aqui")
            else:
                LOG.debug(f"  {d['base_coin']:<8} score parcial (sem klines): {sc_p}/26 "
                          f"[FR={d.get('funding_rate',0):.4%} BB={d.get('bb_lower_15m',0):.4f}-{d.get('bb_upper_15m',0):.4f}]")

        gate2_passed.sort(key=lambda x: x["_partial_score"], reverse=True)
        gate2_passed = [d for d in gate2_passed if d["_partial_score"] >= 0]
        LOG.info(f"  Ordem por score parcial: {[d['base_coin'] for d in gate2_passed]}")
        if pump_bloqueados:
            LOG.info(f"  Bloqueados por pump: {[d['base_coin'] for d in pump_bloqueados]}")

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
        report += f"🎯 SETUP ATIRADOR v5.2\n"
        report += f"📅 {ts_full}\n"
        report += f"📋 Log: {os.path.basename(LOG_FILE)}\n"
        report += f"{'='*58}\n"
        report += f"📊 CONTEXTO DE MERCADO: {ctx['verdict']}\n"
        report += f"   Fear & Greed: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        report += f"   Threshold alerta: {ctx['threshold']} pts | Risk Score: {ctx['risk_score']}\n"
        report += f"{'='*58}\n\n"

        report += f"💼 GESTÃO DE RISCO — Estratégia de Recuperação\n"
        report += f"   Banca: ${BANKROLL:.2f} | Risco fixo/trade: ${risco_usd:.2f} | Perda máx/dia: ${perda_max:.2f}\n"
        ganho_por_trade = risco_usd * RR_MINIMO
        winners_para_dobrar = int(BANKROLL / ganho_por_trade)
        report += f"   Ganho/trade (RR1:2): ${ganho_por_trade:.2f} | Para dobrar banca: ~{winners_para_dobrar} winners\n"
        report += f"   P&L hoje: ${pnl_dia:+.2f} | Trades abertos: {n_abertos}/{MAX_TRADES_ABERTOS}\n"
        if not pode_operar:
            report += f"   🛑 NOVAS ENTRADAS BLOQUEADAS: {motivo_risco}\n"
        else:
            report += f"   ✅ Pode operar — {MAX_TRADES_ABERTOS - n_abertos} slot(s) disponível(is)\n"
        report += f"\n"

        report += f"🔍 PIPELINE\n"
        report += f"   Fonte de dados: {DATA_SOURCE} (perpetuals USDT)\n"
        report += f"   Universo: {total_items} tokens | Qualificados (vol≥${MIN_TURNOVER_24H/1e6:.1f}M, OI≥${MIN_OI_USD/1e6:.0f}M): {len(perpetuals)}\n"
        report += f"   Gate 4H (não SELL): {len(gate1_passed)} | Gate 1H (BUY+): {len(gate2_passed)}\n"
        report += f"   Análise completa: {len(top_full)} | Análise leve: {len(top_light)}\n"

        # [v5.1] Diagnóstico de fontes tentadas nesta execução
        if len(DATA_SOURCE_ATTEMPTS) > 1 or DATA_SOURCE != DATA_SOURCE_ATTEMPTS[0]["fonte"] if DATA_SOURCE_ATTEMPTS else False:
            report += f"   📡 Fontes tentadas:\n"
            for a in DATA_SOURCE_ATTEMPTS:
                status_str = f"HTTP {a['status']}" if a['status'] else "sem resposta"
                if a['falha']:
                    report += f"      ⛔ {a['fonte']}: {status_str} em {a['elapsed_s']}s — {a['falha']}\n"
                else:
                    report += f"      ✅ {a['fonte']}: {status_str} em {a['elapsed_s']}s — {a['qualificados']} qualificados\n"
        elif DATA_SOURCE_ATTEMPTS:
            a = DATA_SOURCE_ATTEMPTS[0]
            report += f"   📡 Fonte ativa: {a['fonte']} | HTTP {a['status']} | {a['elapsed_s']}s | {a['tokens_brutos']} brutos\n"

        # Tokens sem dados TV
        if tokens_sem_dados:
            report += f"   ⚠️  Sem dados TradingView ({len(tokens_sem_dados)}): "
            report += f"{', '.join(tokens_sem_dados)}\n"

        # [v5.1 FIX] Tokens bloqueados por pump — visível no relatório
        if pump_bloqueados:
            pump_str = ", ".join(f"{d['base_coin']}({d.get('price_change_24h',0):.0f}%)" for d in pump_bloqueados)
            report += f"   🚫 Bloqueados por pump ({len(pump_bloqueados)}): {pump_str}\n"

        # [v5.1 FIX] Tokens com RSI extremo no Gate 4H
        rsi_extremos = [d for d in gate1_passed if d.get("rsi_extremo")]
        if rsi_extremos:
            rsi_str = ", ".join(f"{d['base_coin']}(RSI={d['rsi_4h']:.0f})" for d in rsi_extremos)
            report += f"   ⚠️  RSI 4H extremo (>80): {rsi_str}\n"

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
        LOG.info(f"✅ Scan v5.2 concluído em {elapsed:.1f}s | Fonte: {DATA_SOURCE} | "
                 f"Relatório: {output_path} | Log: {LOG_FILE}")
        return report


def main():
    # Logger inicializado dentro de run_scan_async (precisa do timestamp de execução)
    asyncio.run(run_scan_async())

if __name__ == "__main__":
    main()
