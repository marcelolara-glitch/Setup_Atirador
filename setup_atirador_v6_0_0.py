#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v6.0 - Scanner Profissional de Criptomoedas
=============================================================================
Arquitetura Multi-Timeframe com 3 Camadas Independentes:

  CAMADA 1 — 4H "Qual é a direção do mercado?" (contexto macro)
  CAMADA 2 — 1H "Estamos num bom ponto de entrada?" (estrutura)
  CAMADA 3 — 15m "O timing de entrada está correto agora?" (gatilho)

Score máximo: 26 pts (LONG) | 26 pts (SHORT)
Thresholds: Favorável ≥16 | Moderado ≥18 | Cauteloso(Bear/Medo Extremo) ≥20

v6.0 — Operações de SHORT (bidirectional scanner):

  [v6.0 — ARQUITETURA BIDIRECIONAL]
    O scanner agora identifica oportunidades em AMBAS as direções.
    Os dados já coletados (tickers, klines, TV) são reutilizados sem
    custo adicional de API. Apenas o scoring muda.

    Pipeline SHORT usa os mesmos dados, gates invertidos:
      Gate 4H SHORT: SELL/STRONG_SELL (antes eram descartados)
      Gate 1H SHORT: SELL/STRONG_SELL (antes eram descartados)
      Score SHORT:   pilares bearish espelhados dos bullish

    Parâmetro direction="LONG"|"SHORT" em calculate_score() e funções
    de análise técnica. Cada pilar recebe direction e retorna pontuação
    para a direção solicitada. Sem duplicação de código.

  [v6.0 — PILARES SHORT (espelho dos LONG)]
    P4 Liquidez 4H SHORT: Swing High + OB bearish (resistência)
    P5 Figuras 4H SHORT:  Rising Wedge, H&S, Triângulo Descendente
    P6 CHOCH/BOS SHORT:   CHOCH Bearish, BOS de baixa, Lower Highs
    P-1H Resistência 1H:  Swing High + OB bearish no 1H
    P1 Bollinger SHORT:   Preço perto da banda SUPERIOR (sobrecomprado)
    P2 Candles SHORT:     Padrões bearish (Engulfing Bear, Shooting Star,
                          Evening Star, 3 Black Crows, Harami Bear)
    P3 Funding SHORT:     Positivo alto = longs excessivos (squeeze short)

  [v6.0 — Order Blocks bearish]
    detect_order_blocks_bearish(): último candle bullish antes de
    impulso de queda ≥ OB_IMPULSE_PCT. Espelho do OB bullish existente.

  [v6.0 — Trade params SHORT]
    SL acima da entrada: sl = price × (1 + sl_dist_pct/100)
    TPs abaixo:          tp = price × (1 - sl_dist_pct × N / 100)
    Alavancagem:         mesma lógica por score — tabela idêntica ao LONG.

  [v6.0 — Gestão de risco com posições mistas]
    Regra nova: LONG e SHORT no mesmo ativo são mutuamente exclusivos.
    Se HYPE tem LONG aberto, qualquer sinal SHORT de HYPE é bloqueado.
    check_risk_limits() verifica símbolos em aberto antes de liberar.
    MAX_TRADES_ABERTOS=2 inclui LONGs + SHORTs combinados.

  [v6.0 — Relatório bidirecional]
    Seção LONG: alertas e radar bullish (comportamento anterior)
    Seção SHORT: alertas e radar bearish (novo)
    Contexto de mercado orienta ambos: Bear favorece SHORT, Bull favorece LONG.

v5.3.1: OKX como Fonte 1, Gate.io como Fonte 2 (reversão empírica)
v5.3: Gate.io como Fonte 1 (revertido)
v5.2: Fix CoinGecko parser + klines OKX fallback
v5.1: Hierarquia 3 fontes, timeout 8s, pump bloqueados, RSI>80
v5.0: Fonte dual Bybit/Bitget, filtros relaxados ($2M/$5M), TOP_N removido
v4.9: Perpétuos puros + Estratégia de Recuperação de Banca

Autor: Manus AI | v4.1→v6.0 (revisão Claude/Anthropic)
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
# DADOS DE MERCADO — Hierarquia de Fontes [v5.3]
# ===========================================================================
# Histórico de problemas com Fonte 1:
#   v5.0 Bybit:       geo-block Brasil (HTTP 403 / timeout 92s)
#   v5.1 CoinGecko:   parser incompatível (21.094 itens, todos rejeitados)
#   v5.2 CoinGecko/exchange-specific: HTTP 404 (IDs errados + endpoint pago)
#   v5.3 Gate.io:     confirmada acessível do Brasil ✅ (teste 22/03/2026)
#
# Pesquisa exaustiva de fontes (22/03/2026):
#   Binance fapi: geo-block Brasil (igual à Bybit)
#   CoinGlass:    pago (sem free tier com API)
#   Sem fonte gratuita de dados AGREGADOS acessível do Brasil.
#   Decisão: melhor fonte LOCAL = Gate.io.
#
# Hierarquia final: Gate.io → OKX → Bitget
#
# Gate.io requer 2 chamadas:
#   /futures/usdt/tickers  → volume, OI (contratos), preço, funding
#   /futures/usdt/contracts → quanto_multiplier para converter OI a USD
# O /contracts é cacheado por 24h (muda raramente).
#
# Normalização crítica: Gate usa BTC_USDT → normalizar para BTCUSDT
# (TV Scanner e klines Bitget/OKX usam BTCUSDT sem separador)
# ===========================================================================

TICKER_TIMEOUT = 8   # segundos por fonte — cai imediatamente se não responder

DATA_SOURCE          = "desconhecida"
DATA_SOURCE_ATTEMPTS = []

# Cache de quanto_multiplier da Gate.io (válido por 24h)
_GATE_MULTIPLIERS      = {}
_GATE_MULTIPLIERS_TS   = 0.0
_GATE_MULTIPLIERS_TTL  = 86400   # 24 horas em segundos

def _log_source_attempt(fonte, url, status, elapsed, tokens_brutos,
                        qualificados, motivo_falha=None):
    """
    Registra diagnóstico completo de cada tentativa de fonte.
    Fundamental para debug de bloqueios e monitoramento de estabilidade.
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


def _fetch_gate_multipliers():
    """
    [v5.3] Busca e cacheia o quanto_multiplier de cada contrato Gate.io.

    quanto_multiplier converte OI de contratos para USD:
      oi_usd = total_size × mark_price × quanto_multiplier

    Valores variam por ativo (ex: BTC pode ser 0.0001, USDT-settled pode ser 1).
    Cache de 24h evita chamada extra a cada execução (muda raramente).
    Retorna dict: {"BTCUSDT": 0.0001, "ETHUSDT": 0.01, ...}
    """
    global _GATE_MULTIPLIERS, _GATE_MULTIPLIERS_TS

    agora = time.time()
    if _GATE_MULTIPLIERS and (agora - _GATE_MULTIPLIERS_TS) < _GATE_MULTIPLIERS_TTL:
        age_h = (agora - _GATE_MULTIPLIERS_TS) / 3600
        LOG.debug(f"  💾  [Gate.io/contracts] Cache HIT | {len(_GATE_MULTIPLIERS)} contratos | "
                  f"idade {age_h:.1f}h")
        return _GATE_MULTIPLIERS

    LOG.debug("  📡  [Gate.io/contracts] Buscando quanto_multiplier...")
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    t0  = time.time()
    try:
        resp    = requests.get(url, timeout=TICKER_TIMEOUT,
                               headers={"Accept-Encoding": "gzip, deflate",
                                        "User-Agent": "Mozilla/5.0 (compatible; scanner/5.3)"})
        elapsed = time.time() - t0
        LOG.debug(f"       → HTTP {resp.status_code} | {len(resp.content)/1024:.1f}KB | {elapsed:.2f}s")

        if resp.status_code == 200:
            contratos = resp.json()
            mults = {}
            for c in contratos:
                nome = c.get("name", "")
                # Normaliza BTC_USDT → BTCUSDT
                sym = nome.replace("_", "")
                mult = sf(c.get("quanto_multiplier", 1.0))
                if mult <= 0:
                    mult = 1.0
                if sym.endswith("USDT"):
                    mults[sym] = mult
            _GATE_MULTIPLIERS    = mults
            _GATE_MULTIPLIERS_TS = agora
            LOG.debug(f"  💾  [Gate.io/contracts] Cache GRAVADO | {len(mults)} contratos")
            return mults
        else:
            LOG.warning(f"  ⚠️  [Gate.io/contracts] HTTP {resp.status_code} — usando multiplier=1 como fallback")
            return {}
    except Exception as e:
        elapsed = time.time() - t0
        LOG.warning(f"  ⚠️  [Gate.io/contracts] Erro após {elapsed:.1f}s: {type(e).__name__}: {e}")
        return {}


def _parse_gateio_tickers(items, multipliers):
    """
    [v5.3] Normaliza tickers Gate.io para estrutura interna padrão.

    Gate.io campos:
      contract           → "BTC_USDT" (normalizar → "BTCUSDT")
      volume_24h_quote   → volume 24h em USDT direto
      last               → preço atual
      mark_price         → mark price (usado para cálculo de OI)
      total_size         → OI em contratos (× mark_price × quanto_multiplier = USD)
      funding_rate       → taxa de financiamento decimal
      change_percentage  → variação % 24h (ex: -7.76 = -7.76%)

    OI em USD = total_size × mark_price × quanto_multiplier
    Para contratos onde quanto_multiplier não foi obtido, usa 1.0 como fallback.
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0

    for t in items:
        contrato = t.get("contract", "")
        # Normaliza BTC_USDT → BTCUSDT (crítico para TV Scanner e klines)
        sym = contrato.replace("_", "")
        if not sym.endswith("USDT"):
            rej_symbol += 1; continue

        turnover = sf(t.get("volume_24h_quote", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue

        price      = sf(t.get("last", 0) or t.get("mark_price", 0))
        mark_price = sf(t.get("mark_price", 0) or price)
        if price <= 0:
            rej_symbol += 1; continue

        # OI: total_size (contratos) × mark_price × quanto_multiplier
        total_size = sf(t.get("total_size", 0))
        mult       = multipliers.get(sym, 1.0)
        oi_usd     = total_size * mark_price * mult

        # Fallback se OI zerado: estimativa via volume (conservador)
        if oi_usd <= 0:
            oi_usd = turnover * 0.1

        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue

        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "volume_24h"      : turnover,
            "funding_rate"    : sf(t.get("funding_rate", 0)),
            "price_change_24h": sf(t.get("change_percentage", 0)),
        })
    return qualified, rej_vol, rej_oi


def _parse_okx_tickers(items):
    """
    Normaliza tickers da OKX /v5/market/tickers?instType=SWAP.
    instId: "BTC-USDT-SWAP" → "BTCUSDT"
    volCcy24h: volume em USDT | openInterest × last: OI estimado USD
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0
    for t in items:
        inst = t.get("instId", "")
        if not inst.endswith("-USDT-SWAP"):
            rej_symbol += 1; continue
        sym      = inst.replace("-USDT-SWAP", "") + "USDT"
        base     = sym.replace("USDT", "")
        turnover = sf(t.get("volCcy24h", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue
        price  = sf(t.get("last", 0))
        oi_usd = sf(t.get("openInterest", 0)) * price
        if oi_usd == 0:
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
    Normaliza tickers Bitget (comportamento original v4.9).
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


def _try_source(nome, url, parse_fn, extract_fn, timeout=None, parse_kwargs=None):
    """
    Tenta buscar tickers de uma fonte com diagnóstico completo.
    Retorna (qualified, total_brutos) em caso de sucesso, ou None em falha.
    parse_kwargs: argumentos extras para parse_fn (ex: multipliers da Gate.io)
    """
    t_used = timeout or TICKER_TIMEOUT
    LOG.info(f"  📡  [{nome}] Tentando: {url[:80]}{'...' if len(url) > 80 else ''}")
    LOG.debug(f"       timeout={t_used}s | filtros: vol≥${MIN_TURNOVER_24H/1e6:.1f}M OI≥${MIN_OI_USD/1e6:.0f}M")
    t0 = time.time()
    try:
        resp    = requests.get(url, timeout=t_used,
                               headers={"Accept-Encoding": "gzip, deflate",
                                        "User-Agent": "Mozilla/5.0 (compatible; scanner/5.3)"})
        elapsed = time.time() - t0
        status  = resp.status_code
        size_kb = len(resp.content) / 1024
        LOG.debug(f"       → HTTP {status} | {size_kb:.1f}KB | {elapsed:.2f}s")

        if status != 200:
            motivo = (f"HTTP {status} — "
                      f"{'Geo-block/Forbidden' if status == 403 else 'Not Found' if status == 404 else 'Erro servidor' if status >= 500 else 'Erro cliente'}")
            _log_source_attempt(nome, url, status, elapsed, 0, 0, motivo)
            return None

        data  = resp.json()
        items = extract_fn(data)
        if not items:
            motivo = "Resposta JSON vazia ou sem campo esperado"
            _log_source_attempt(nome, url, status, elapsed, 0, 0, motivo)
            return None

        kwargs    = parse_kwargs or {}
        qualified, rej_vol, rej_oi = parse_fn(items, **kwargs)
        LOG.debug(f"       Rejeitados: {rej_vol} vol<${MIN_TURNOVER_24H/1e6:.1f}M | "
                  f"{rej_oi} OI<${MIN_OI_USD/1e6:.0f}M")
        top5 = [d['base_coin'] for d in sorted(qualified, key=lambda x: x['turnover_24h'], reverse=True)[:5]]
        LOG.debug(f"       TOP 5 por volume: {top5}")

        if not qualified:
            motivo = f"Nenhum token passou os filtros ({len(items)} brutos, todos rejeitados)"
            _log_source_attempt(nome, url, status, elapsed, len(items), 0, motivo)
            return None

        _log_source_attempt(nome, url, status, elapsed, len(items), len(qualified))
        return qualified, len(items)

    except requests.exceptions.Timeout:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"Timeout após {elapsed:.1f}s (limite={t_used}s)")
        return None
    except requests.exceptions.ConnectionError as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"Erro de conexão: {str(e)[:80]}")
        return None
    except (json.JSONDecodeError, ValueError) as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"JSON inválido: {str(e)[:80]}")
        return None
    except Exception as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"{type(e).__name__}: {str(e)[:80]}")
        return None


def fetch_perpetuals():
    """
    [v5.3.1/v6.0] Busca perpetuals USDT — hierarquia OKX → Gate.io → Bitget.

    ╔══════════════════════════════════════════════════════════════════╗
    ║  NOTA TÉCNICA — LIMITAÇÃO RECONHECIDA                           ║
    ║  A solução ideal seria a API da CoinGlass (coinglass.com),      ║
    ║  que retorna OI AGREGADO de 30+ exchanges em 1 chamada.         ║
    ║  Não implementada: sem plano gratuito com acesso à API.         ║
    ║  Planos a partir de ~$35/mês (Hobby). Ver docstring do módulo.  ║
    ╚══════════════════════════════════════════════════════════════════╝

    OKX (Fonte 1): melhor universo disponível gratuitamente no Brasil.
      100+ tokens qualificados. Gates técnicos filtram meme coins.
    Gate.io (Fonte 2): fallback com boa qualidade de TOP 5, mas universo
      reduzido (~22 qualificados vs ~100 OKX). Confirmada acessível do BR.
    Bitget (Fonte 3): último recurso, estável desde v4.x.

    CoinGecko: REMOVIDA (nunca funcionou em produção — v5.1 e v5.2).
    Bybit/Binance: geo-block Brasil confirmado — não implementadas.
    """
    global DATA_SOURCE, DATA_SOURCE_ATTEMPTS
    DATA_SOURCE_ATTEMPTS = []

    LOG.info("📡 [v6.0] Iniciando busca de tickers — hierarquia OKX → Gate.io → Bitget")
    LOG.info(f"   Filtros: vol≥${MIN_TURNOVER_24H/1e6:.1f}M | OI≥${MIN_OI_USD/1e6:.0f}M | timeout={TICKER_TIMEOUT}s/fonte")
    LOG.info("   [Nota] Fonte ideal seria CoinGlass API (pago ~$35/mês) — ver docstring")

    # ------------------------------------------------------------------
    # FONTE 1: OKX
    # Melhor universo gratuito disponível no Brasil (~100 qualificados).
    # Meme coins no top são filtrados pelos gates técnicos (4H/1H).
    # ------------------------------------------------------------------
    resultado = _try_source(
        nome       = "OKX",
        url        = "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
        parse_fn   = _parse_okx_tickers,
        extract_fn = lambda d: d.get("data", []),
    )
    if resultado:
        DATA_SOURCE = "OKX"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [OKX] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        return qualified, total

    # ------------------------------------------------------------------
    # FONTE 2: Gate.io
    # Boa qualidade de TOP 5 (ETH/BTC/SOL no topo, sem meme coins).
    # Universo menor (~22 qualificados). Latência ~7s por chamada.
    # Requer 2 chamadas: tickers + contracts (contracts cacheado 24h).
    # ------------------------------------------------------------------
    multipliers = _fetch_gate_multipliers()
    resultado   = _try_source(
        nome         = "Gate.io",
        url          = "https://api.gateio.ws/api/v4/futures/usdt/tickers",
        parse_fn     = _parse_gateio_tickers,
        extract_fn   = lambda d: d if isinstance(d, list) else [],
        parse_kwargs = {"multipliers": multipliers},
    )
    if resultado:
        DATA_SOURCE = "Gate.io"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [Gate.io] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        LOG.warning("  ⚠️  [Gate.io] Universo reduzido (~22 qualificados vs ~100 OKX) — cobertura limitada")
        return qualified, total

    # ------------------------------------------------------------------
    # FONTE 3: Bitget — último recurso, estável desde v4.x
    # ------------------------------------------------------------------
    LOG.warning("  ⚠️  OKX e Gate.io indisponíveis — usando Bitget (último recurso)")
    resultado = _try_source(
        nome       = "Bitget",
        url        = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
        parse_fn   = _parse_bitget_tickers,
        extract_fn = lambda d: d.get("data", []),
        timeout    = 20,
    )
    if resultado:
        DATA_SOURCE = "Bitget"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [Bitget] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        return qualified, total

    # ------------------------------------------------------------------
    # TODAS AS FONTES FALHARAM
    # ------------------------------------------------------------------
    DATA_SOURCE = "NENHUMA"
    LOG.error("  ❌  TODAS AS 3 FONTES FALHARAM — scan abortado")
    LOG.error("  Resumo de tentativas:")
    for a in DATA_SOURCE_ATTEMPTS:
        LOG.error(f"    [{a['fonte']}] HTTP {a['status']} | {a['elapsed_s']}s | {a['falha']}")
    LOG.error("  Ações sugeridas:")
    LOG.error("    1. Verificar conectividade de rede")
    LOG.error("    2. Testar: curl https://www.okx.com/api/v5/market/tickers?instType=SWAP")
    LOG.error("    3. Aguardar 15-30 min (possível rate-limit temporário)")
    raise RuntimeError("Todas as fontes de tickers falharam. Scan abortado.")
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


def detect_order_blocks_bearish(candles):
    """
    [v6.0] Order Blocks bearish: último candle bullish antes de impulso de QUEDA
    ≥ OB_IMPULSE_PCT. Espelho de detect_order_blocks() para operações SHORT.
    Retorna lista de {'high', 'low', 'index'}.
    """
    obs = []
    n = OB_IMPULSE_N
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] <= c["open"]: continue          # precisa ser bullish
        ref = c["close"]
        if ref <= 0: continue
        min_close   = min(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (ref - min_close) / ref * 100   # queda em %
        if impulse_pct >= OB_IMPULSE_PCT:
            obs.append({
                "high" : max(c["open"], c["close"]),
                "low"  : min(c["open"], c["close"]),
                "index": i,
            })
    return obs

# ===========================================================================
# CAMADA 2 — PILAR 1H: SUPORTE / ORDER BLOCK em klines 1H  (LONG)
# ===========================================================================

def analyze_support_1h(candles_1h, current_price):
    """
    LONG — P-1H: preço perto de suporte (swing low ou OB bullish) no 1H.
    Pontuação máxima: 4 pts.
    """
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"

    sh, sl = find_swing_points(candles_1h)
    score   = 0
    details = []

    if sl:
        for s in reversed(sl):
            dist_pct = (current_price - s["price"]) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 2
                details.append(f"Suporte 1H em {s['price']:.4f} ({dist_pct:.2f}% abaixo)")
                break

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


def analyze_resistance_1h(candles_1h, current_price):
    """
    [v6.0] SHORT — P-1H: preço perto de resistência (swing high ou OB bearish) no 1H.
    Espelho de analyze_support_1h() para operações SHORT.
    Pontuação máxima: 4 pts.
      +2  Preço abaixo de swing high recente (≤1% acima)
      +2  Preço perto de Order Block bearish (≤1.5% do meio)
    """
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"

    sh, sl = find_swing_points(candles_1h)
    score   = 0
    details = []

    # Resistência por Swing High 1H — preço subiu até perto do topo recente
    if sh:
        for s in reversed(sh):
            dist_pct = (s["price"] - current_price) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 2
                details.append(f"Resistência 1H em {s['price']:.4f} ({dist_pct:.2f}% acima)")
                break

    # Order Block bearish 1H
    obs = detect_order_blocks_bearish(candles_1h)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = abs(current_price - ob_mid) / current_price * 100
            if dist_pct <= OB_PROXIMITY_PCT:
                score += 2
                details.append(f"OB Bearish 1H ({ob['low']:.4f}–{ob['high']:.4f})")
                break

    if not details:
        return 0, "Preço longe de resistências no 1H"
    return min(score, 4), " | ".join(details)

# ===========================================================================
# CAMADA 3 — PILARES 15m
# ===========================================================================

# P1 — Bollinger Bands 15m  (LONG: banda inferior | SHORT: banda superior)
def score_bollinger(d, direction="LONG"):
    """
    Posição do preço no canal de Bollinger 15m.
    LONG:  pontuação quando preço perto da banda INFERIOR (sobrevenda)
    SHORT: pontuação quando preço perto da banda SUPERIOR (sobrecompra)
    Max: 3 pts.
    """
    price = d.get("price", 0)
    bbl   = d.get("bb_lower_15m", 0)
    bbu   = d.get("bb_upper_15m", 0)
    sym   = d.get("base_coin", "?")

    if not price or price <= 0:
        return 0, "BB N/A (preço inválido)"
    if not bbl or bbl <= 0:
        return 0, "BB N/A (BB_lower ausente)"
    if not bbu or bbu <= 0:
        return 0, "BB N/A (BB_upper ausente)"

    banda = bbu - bbl
    banda_min = price * 0.001
    if banda <= banda_min:
        return 0, f"BB N/A (banda estreita: {banda:.4f})"

    pos = (price - bbl) / banda

    if pos < 0 or pos > 1.5:
        LOG.warning(f"    BB {sym}: pos={pos:.0%} fora do range esperado — descartando")
        return 0, f"BB N/A (pos anômala: {pos:.0%})"

    if direction == "SHORT":
        # SHORT: pontuação cresce quanto mais próximo da banda superior
        if pos > 0.95:   return 3, f"BB extremo superior ({pos:.0%})"
        elif pos > 0.85: return 2, f"BB superior ({pos:.0%})"
        elif pos > 0.75: return 1, f"BB alta ({pos:.0%})"
        else:            return 0, f"BB neutro ({pos:.0%})"
    else:
        # LONG: pontuação cresce quanto mais próximo da banda inferior
        if pos < 0.05:   return 3, f"BB extremo inferior ({pos:.0%})"
        elif pos < 0.15: return 2, f"BB inferior ({pos:.0%})"
        elif pos < 0.25: return 1, f"BB baixa ({pos:.0%})"
        else:            return 0, f"BB neutro ({pos:.0%})"


# P2 — Padrões de Candle 15m  (LONG: bullish | SHORT: bearish)
def score_candles(ind, direction="LONG"):
    """
    Price action puro no 15m.
    LONG:  padrões bullish de reversão/continuação
    SHORT: padrões bearish de reversão/continuação
    Max: 4 pts (cap).
    """
    if not ind: return [], 0

    if direction == "SHORT":
        checks = {
            "Candle.Engulfing.Bearish|15" : ("Engulfing Bearish",  2),
            "Candle.ShootingStar|15"      : ("Shooting Star",      2),
            "Candle.EveningStar|15"       : ("Evening Star",       2),
            "Candle.3BlackCrows|15"       : ("3 Black Crows",      2),
            "Candle.Harami.Bearish|15"    : ("Harami Bearish",     1),
            "Candle.Doji.GraveStone|15"   : ("Gravestone Doji",    1),
        }
    else:
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


# P3 — Funding Rate  (LONG: negativo bom | SHORT: positivo alto bom)
def score_funding_rate(fr, direction="LONG"):
    """
    LONG:  funding negativo = shorts dominando = squeeze potencial de alta
    SHORT: funding positivo alto = longs excessivos = squeeze potencial de baixa
    Max: 2 pts.
    """
    if direction == "SHORT":
        if fr > 0.0005:  return 2, f"{fr:.4%} (longs excessivos — squeeze short potencial)"
        elif fr > 0:     return 1, f"{fr:.4%} (leve positivo)"
        elif fr < -0.0005: return -1, f"{fr:.4%} (shorts dominando — desfavorável para short)"
        else:            return 0, f"{fr:.4%} (neutro)"
    else:
        if fr < -0.0005: return 2, f"{fr:.4%} (squeeze potencial)"
        elif fr < 0:     return 1, f"{fr:.4%} (leve negativo)"
        elif fr > 0.0005: return -1, f"{fr:.4%} (longs excessivos)"
        else:            return 0, f"{fr:.4%} (neutro)"

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
# CAMADA 1 — PILARES 4H  (LONG e SHORT)
# ===========================================================================

# P4 — Zonas de Liquidez 4H
def analyze_liquidity_zones_4h(candles_4h, current_price, direction="LONG"):
    """
    LONG:  suporte 4H + OB bullish abaixo do preço
    SHORT: resistência 4H + OB bearish acima do preço
    Max: 3 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    score, details = 0, []

    if direction == "SHORT":
        # Resistência por Swing High 4H
        sr_hit = False
        if sh:
            for s in reversed(sh):
                dist_pct = (s["price"] - current_price) / current_price * 100
                if 0 < dist_pct <= SR_PROXIMITY_PCT:
                    score  += 1
                    sr_hit  = True
                    details.append(f"Resistência 4H {s['price']:.4f} ({dist_pct:.2f}% acima)")
                    break

        # OB bearish 4H
        ob_hit = False
        obs = detect_order_blocks_bearish(candles_4h)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = abs(current_price - ob_mid) / current_price * 100
                if dist_pct <= OB_PROXIMITY_PCT:
                    score  += 1
                    ob_hit  = True
                    details.append(f"OB Bearish 4H ({ob['low']:.4f}–{ob['high']:.4f})")
                    break

        if sr_hit and ob_hit:
            score += 1
            details.append("Confluência Res+OB Bearish")

    else:
        # LONG (comportamento original)
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
            score += 1
            details.append("Confluência S/R+OB")

    if not details:
        label = "resistências" if direction == "SHORT" else "zonas de liquidez"
        return 0, f"Longe de {label} 4H"
    return min(score, 3), " | ".join(details)


# P5 — Figuras Gráficas 4H
def analyze_chart_patterns_4h(candles_4h, direction="LONG"):
    """
    LONG:  Falling Wedge, Triângulo Simétrico, Triângulo Ascendente, Cunha Desc.
    SHORT: Rising Wedge, H&S (approx), Triângulo Descendente, Cunha Ascendente
    Max: 2 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    if len(sh) < 3 or len(sl) < 3:
        return 0, "Dados insuficientes para figuras"

    sh_p = [s["price"] for s in sh[-3:]]
    sl_p = [s["price"] for s in sl[-3:]]

    highs_lower  = sh_p[0] > sh_p[1] > sh_p[2]
    highs_higher = sh_p[0] < sh_p[1] < sh_p[2]
    highs_flat   = abs(sh_p[0] - sh_p[2]) / sh_p[0] < 0.015
    lows_higher  = sl_p[0] < sl_p[1] < sl_p[2]
    lows_lower   = sl_p[0] > sl_p[1] > sl_p[2]
    lows_flat    = abs(sl_p[0] - sl_p[2]) / sl_p[0] < 0.015

    if direction == "SHORT":
        # Rising Wedge — reversão bearish (topos sobem menos que fundos)
        if highs_higher and lows_higher:
            high_rise = (sh_p[2] - sh_p[0]) / sh_p[0]
            low_rise  = (sl_p[2] - sl_p[0]) / sl_p[0]
            if high_rise < low_rise * 0.8:
                return 2, "Rising Wedge (reversão bearish)"

        # Triângulo Simétrico — compressão (bearish se contexto SELL)
        if highs_lower and lows_higher:
            return 2, "Triângulo Simétrico (compressão bearish)"

        # Triângulo Descendente — suporte cedendo
        if lows_flat and highs_lower:
            return 2, "Triângulo Descendente (distribuição bearish)"

        # Cunha Ascendente — pullback bearish em tendência de baixa
        if highs_higher and not lows_lower:
            return 1, "Cunha Ascendente (pullback bearish)"

        return 0, "Sem figuras bearish claras no 4H"

    else:
        # LONG (comportamento original)
        if highs_lower and lows_lower:
            high_drop = (sh_p[0] - sh_p[2]) / sh_p[0]
            low_drop  = (sl_p[0] - sl_p[2]) / sl_p[0]
            if low_drop < high_drop * 0.8:
                return 2, "Falling Wedge (reversão bullish)"

        if highs_lower and lows_higher:
            return 2, "Triângulo Simétrico (compressão)"

        if highs_flat and lows_higher:
            return 2, "Triângulo Ascendente (acumulação bullish)"

        if highs_lower and not lows_higher:
            return 1, "Cunha Descendente (pullback)"

        return 0, "Sem figuras claras no 4H"


# P6 — CHOCH / BOS 4H
def analyze_choch_bos_4h(candles_4h, current_price, direction="LONG"):
    """
    Smart Money Concepts no 4H.
    LONG:  CHOCH Bullish, BOS Bullish, Higher Lows
    SHORT: CHOCH Bearish, BOS Bearish, Lower Highs
    Max: 3 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    if len(sh) < 2 or len(sl) < 2:
        return 0, "Dados insuficientes para estrutura 4H"

    last_sh = sh[-1]["price"]
    prev_sh = sh[-2]["price"]
    last_sl = sl[-1]["price"]
    prev_sl = sl[-2]["price"]

    if direction == "SHORT":
        # CHOCH Bearish: uptrend confirmado + rompimento de swing low
        in_uptrend = (prev_sl > sl[-3]["price"]) if len(sl) >= 3 else False
        if in_uptrend and current_price < last_sl:
            return 3, "CHOCH Bearish 4H (reversão confirmada)"

        # BOS Bearish: Lower Lows + Lower Highs + rompimento para baixo
        if last_sl < prev_sl and last_sh < prev_sh and current_price < last_sl:
            return 2, "BOS Bearish 4H (continuação de baixa)"

        # Estrutura Bearish: Lower Highs consecutivos (distribuição)
        if last_sh < prev_sh and len(sh) >= 3 and prev_sh < sh[-3]["price"]:
            return 1, "Estrutura 4H bearish (Lower Highs)"

        return 0, "Sem estrutura bearish no 4H"

    else:
        # LONG (comportamento original)
        in_downtrend = (prev_sh < sh[-3]["price"]) if len(sh) >= 3 else False
        if in_downtrend and current_price > last_sh:
            return 3, "CHOCH Bullish 4H (reversão confirmada)"

        if last_sh > prev_sh and last_sl > prev_sl and current_price > last_sh:
            return 2, "BOS Bullish 4H (continuação de alta)"

        if last_sl > prev_sl and len(sl) >= 3 and prev_sl > sl[-3]["price"]:
            return 1, "Estrutura 4H saudável (Higher Lows)"

        return 0, "Sem estrutura bullish no 4H"


# P7 — Filtro de Dump/Pump
def score_pump_filter(price_change_24h, direction="LONG"):
    """
    LONG:  pump excessivo bloqueia ou penaliza (ativo muito estendido pra cima)
    SHORT: dump excessivo bloqueia ou penaliza (ativo muito estendido pra baixo)
    Bloqueio total: ±40% | Penalidade gradual: ±20% a ±39%
    """
    if direction == "SHORT":
        change = -price_change_24h   # inverte: queda é "pump" para o short
        if change >= PUMP_BLOCK_24H:
            return None, f"DUMP BLOCK: {price_change_24h:.1f}% em 24h (ativo exausto)"
        elif change >= PUMP_WARN_24H_STRONG:
            return -3, f"Dump forte ({price_change_24h:.1f}% > -30%)"
        elif change >= PUMP_WARN_24H:
            return -2, f"Dump moderado ({price_change_24h:.1f}% > -20%)"
        else:
            return 0, f"OK ({price_change_24h:.1f}%)"
    else:
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
    """LONG — SL abaixo, TPs acima. Mesma lógica desde v4.9."""
    if not price or not atr or atr <= 0: return None
    sl_dist_pct = (1.5 * atr) / price * 100
    if sl_dist_pct < 0.05: return None
    sl = price * (1 - sl_dist_pct / 100)
    perda_sem_alav = BANKROLL * sl_dist_pct / 100
    if perda_sem_alav <= 0: return None
    alav_calculada = RISCO_POR_TRADE_USD / perda_sem_alav
    alav_max_score = get_alav_max_por_score(score)
    alav_final     = max(ALAVANCAGEM_MIN, min(alav_calculada, alav_max_score))
    posicao_usd  = BANKROLL * alav_final
    risco_real   = posicao_usd * sl_dist_pct / 100
    ganho_rr2    = risco_real * RR_MINIMO
    cap_label = f"N/A(score<20)→min={ALAVANCAGEM_MIN:.0f}x" if alav_max_score == ALAVANCAGEM_MIN and score < 20 else f"{alav_max_score:.0f}x"
    LOG.debug(f"    TradeParams LONG: SL={sl_dist_pct:.2f}% | "
              f"alav_calc={alav_calculada:.1f}x → cap={cap_label} → "
              f"alav_final={alav_final:.1f}x | risco=${risco_real:.2f} | ganho=${ganho_rr2:.2f}")
    return {
        "direction"      : "LONG",
        "entry"          : price,
        "sl"             : sl,
        "sl_distance_pct": sl_dist_pct,
        "tp1"            : price * (1 + sl_dist_pct / 100),
        "tp2"            : price * (1 + sl_dist_pct * 2 / 100),
        "tp3"            : price * (1 + sl_dist_pct * 3 / 100),
        "rr"             : RR_MINIMO,
        "alavancagem"    : round(alav_final, 1),
        "alav_max_score" : alav_max_score,
        "risco_usd"      : round(risco_real, 2),
        "ganho_rr2_usd"  : round(ganho_rr2, 2),
        "atr"            : atr,
    }


def calc_trade_params_short(price, atr, score=0):
    """
    [v6.0] SHORT — SL ACIMA da entrada, TPs ABAIXO.
    Mesma lógica de risco/alavancagem do LONG, direção invertida.
      sl  = price × (1 + sl_dist_pct/100)   ← acima
      tp1 = price × (1 - sl_dist_pct/100)   ← abaixo (RR 1:1)
      tp2 = price × (1 - sl_dist_pct×2/100) ← abaixo (RR 1:2)
      tp3 = price × (1 - sl_dist_pct×3/100) ← abaixo (RR 1:3)
    """
    if not price or not atr or atr <= 0: return None
    sl_dist_pct = (1.5 * atr) / price * 100
    if sl_dist_pct < 0.05: return None
    sl = price * (1 + sl_dist_pct / 100)    # SL ACIMA para SHORT
    perda_sem_alav = BANKROLL * sl_dist_pct / 100
    if perda_sem_alav <= 0: return None
    alav_calculada = RISCO_POR_TRADE_USD / perda_sem_alav
    alav_max_score = get_alav_max_por_score(score)
    alav_final     = max(ALAVANCAGEM_MIN, min(alav_calculada, alav_max_score))
    posicao_usd  = BANKROLL * alav_final
    risco_real   = posicao_usd * sl_dist_pct / 100
    ganho_rr2    = risco_real * RR_MINIMO
    cap_label = f"N/A(score<20)→min={ALAVANCAGEM_MIN:.0f}x" if alav_max_score == ALAVANCAGEM_MIN and score < 20 else f"{alav_max_score:.0f}x"
    LOG.debug(f"    TradeParams SHORT: SL={sl_dist_pct:.2f}% | "
              f"alav_calc={alav_calculada:.1f}x → cap={cap_label} → "
              f"alav_final={alav_final:.1f}x | risco=${risco_real:.2f} | ganho=${ganho_rr2:.2f}")
    return {
        "direction"      : "SHORT",
        "entry"          : price,
        "sl"             : sl,                                         # acima
        "sl_distance_pct": sl_dist_pct,
        "tp1"            : price * (1 - sl_dist_pct / 100),           # abaixo
        "tp2"            : price * (1 - sl_dist_pct * 2 / 100),
        "tp3"            : price * (1 - sl_dist_pct * 3 / 100),
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
                    fg_value=50, log_breakdown=True, direction="LONG"):
    """
    Score com 3 camadas independentes. Max: 26 pts.
    [v6.0] Parâmetro direction="LONG"|"SHORT" inverte a lógica dos pilares.

    LONG:  gates 4H/1H exigem BUY | pilares medem suporte/bullish
    SHORT: gates 4H/1H exigem SELL | pilares medem resistência/bearish

    Retornos especiais:
      -1  = Token descartado pelo gate de direção
      -99 = Token descartado por PUMP/DUMP BLOCK
      ≥0  = Score válido
    """
    sc        = 0
    reasons   = []
    breakdown = []

    # -----------------------------------------------------------------------
    # GATE CAMADA 1: 4H — direção macro
    # LONG:  SELL/STRONG_SELL descarta
    # SHORT: BUY/STRONG_BUY descarta
    # -----------------------------------------------------------------------
    s4h = d.get("summary_4h", "NEUTRAL")
    if direction == "SHORT":
        if "BUY" in s4h:
            return -1, [f"4H {s4h} — descartado (tendência bullish, não short)"], []
        breakdown.append(("Contexto 4H", 0, 0, f"{s4h} (contexto SHORT, não pontuado)"))
    else:
        if "STRONG_SELL" in s4h or s4h == "SELL":
            return -1, [f"4H {s4h} — descartado pelo gate macro"], []
        breakdown.append(("Contexto 4H", 0, 0, f"{s4h} (contexto, não pontuado)"))

    # -----------------------------------------------------------------------
    # CAMADA 1 — Pilares 4H (estrutura de preço, klines)
    # -----------------------------------------------------------------------
    price = d.get("price", 0)

    # P4 — Zonas de Liquidez 4H
    if candles_4h:
        lz_sc, lz_det = analyze_liquidity_zones_4h(candles_4h, price, direction)
    else:
        lz_sc, lz_det = 0, "Klines 4H indisponíveis"
    sc += lz_sc
    breakdown.append(("P4 Liquidez 4H", lz_sc, 3, lz_det))
    if lz_sc >= 2: reasons.append("Zona liquidez 4H")

    # P5 — Figuras Gráficas 4H
    if candles_4h:
        cp_sc, cp_det = analyze_chart_patterns_4h(candles_4h, direction)
    else:
        cp_sc, cp_det = 0, "Klines 4H indisponíveis"
    sc += cp_sc
    breakdown.append(("P5 Figuras 4H", cp_sc, 2, cp_det))
    if cp_sc > 0: reasons.append(cp_det.split(" (")[0])

    # P6 — CHOCH / BOS 4H
    if candles_4h:
        cb_sc, cb_det = analyze_choch_bos_4h(candles_4h, price, direction)
    else:
        cb_sc, cb_det = 0, "Klines 4H indisponíveis"
    sc += cb_sc
    breakdown.append(("P6 CHOCH/BOS 4H", cb_sc, 3, cb_det))
    label_4h = "Estrutura 4H bearish" if direction == "SHORT" else "Estrutura 4H bullish"
    if cb_sc > 0: reasons.append(label_4h)

    # -----------------------------------------------------------------------
    # CAMADA 2 — Pilar 1H (posição de preço, klines)
    # -----------------------------------------------------------------------
    if candles_1h:
        if direction == "SHORT":
            s1h_sc, s1h_det = analyze_resistance_1h(candles_1h, price)
        else:
            s1h_sc, s1h_det = analyze_support_1h(candles_1h, price)
    else:
        s1h_sc, s1h_det = 0, "Klines 1H indisponíveis"
    sc += s1h_sc
    label_1h = "P-1H Resistência 1H" if direction == "SHORT" else "P-1H Suporte 1H"
    breakdown.append((label_1h, s1h_sc, 4, s1h_det))
    label_1h_reason = "Resistência/OB 1H confirmado" if direction == "SHORT" else "Suporte/OB 1H confirmado"
    if s1h_sc >= 2: reasons.append(label_1h_reason)

    # -----------------------------------------------------------------------
    # CAMADA 3 — Pilares 15m (gatilho de entrada)
    # -----------------------------------------------------------------------

    # P1 — Bollinger Bands 15m
    bb_sc, bb_det = score_bollinger(d, direction)
    sc += bb_sc
    breakdown.append(("P1 Bollinger 15m", bb_sc, 3, bb_det))
    bb_label = "BB superior" if direction == "SHORT" else "BB inferior"
    if bb_sc >= 2: reasons.append(bb_label)

    # P2 — Padrões de Candle 15m
    ind_15m = d.get("_ind_15m", {})
    cp_list, ca_sc = score_candles(ind_15m, direction)
    sc += ca_sc
    breakdown.append(("P2 Candles 15m", ca_sc, 4,
                       f"Padrões: {', '.join(cp_list)}" if cp_list else "Nenhum"))
    if cp_list: reasons.append(f"Candle: {cp_list[0]}")

    # P3 — Funding Rate
    fr = d.get("funding_rate", 0)
    fr_sc, fr_det = score_funding_rate(fr, direction)
    sc += fr_sc
    breakdown.append(("P3 Funding Rate", fr_sc, 2, fr_det))
    fr_label = "FR squeeze short" if direction == "SHORT" else "FR squeeze"
    if fr_sc >= 2: reasons.append(fr_label)

    # P7 — Filtro de Pump/Dump
    pump_sc, pump_det = score_pump_filter(d.get("price_change_24h", 0), direction)
    if pump_sc is None:
        return -99, ["PUMP/DUMP BLOCK"], []
    sc += pump_sc
    breakdown.append(("P7 Filtro Pump/Dump", pump_sc, 0, pump_det))

    # P8 — Volume 15m
    if candles_15m:
        vol_sc, vol_det = score_volume_15m(candles_15m, fg_value)
    else:
        vol_sc, vol_det = 0, "Candles 15m indisponíveis"
    sc += vol_sc
    breakdown.append(("P8 Volume 15m", vol_sc, 2, vol_det))
    if vol_sc >= 2: reasons.append("Volume forte")

    final_sc = max(sc, 0)
    if not reasons: reasons.append(f"Score {final_sc}/26 (sem sinal dominante)")

    if log_breakdown:
        sym = d.get("base_coin", "?")
        LOG.debug(f"  SCORE {sym} [{direction}]: {final_sc}/26 | klines: "
                  f"15m={'✅' if candles_15m else '❌'} "
                  f"1H={'✅' if candles_1h else '❌'} "
                  f"4H={'✅' if candles_4h else '❌'}")
        for pilar, pts, max_pts, detail in breakdown:
            bar = "█" * pts if pts > 0 else ("▒" * abs(pts) if pts < 0 else "·")
            LOG.debug(f"    {pilar:<24} {pts:>+3}/{max_pts} {bar} {detail}")

    return final_sc, reasons, breakdown

# ===========================================================================
# CONTEXTO DE MERCADO E THRESHOLD ADAPTATIVO
# ===========================================================================

def analyze_market_context(fg, btc_4h_str):
    """
    [v6.0] Threshold adaptativo para LONG e SHORT.
    LONG:  Bear/Medo Extremo eleva threshold (mais seletivo)
    SHORT: Bear/Medo Extremo reduz threshold SHORT (mercado favorece short)
           Bull eleva threshold SHORT (mais difícil operar contra a tendência)
    """
    fg_val     = fg.get("value", 50)
    risk_score = 0

    if fg_val <= 20:   risk_score += 0
    elif fg_val <= 25: risk_score += 1
    elif fg_val <= 50: risk_score += 2
    elif fg_val >= 75: risk_score -= 1

    if "STRONG_BUY" in btc_4h_str: risk_score += 2
    elif "BUY" in btc_4h_str:      risk_score += 1
    elif "SELL" in btc_4h_str:     risk_score -= 2

    # Mercado desfavorável para LONG — bot desligado para LONG
    if fg_val >= 80 and "SELL" in btc_4h_str:
        verdict_long  = "DESFAVORÁVEL (Bot Desligado)"
        threshold_long = 99
    elif fg_val <= 20:
        threshold_long = 20; verdict_long = "CAUTELOSO (Medo Extremo)"
    elif fg_val <= 30 and "BUY" in btc_4h_str:
        threshold_long = 14; verdict_long = "FAVORÁVEL (Bull)"
    elif fg_val >= 75 or "SELL" in btc_4h_str:
        threshold_long = 20; verdict_long = "CAUTELOSO (Bear)"
    else:
        threshold_long = 16; verdict_long = "MODERADO (Neutro)"

    # [v6.0] Threshold SHORT: inverso do LONG
    # Bear/Medo favorece SHORT → threshold menor (mais fácil ativar)
    # Bull desfavorece SHORT → threshold maior (mais difícil)
    if fg_val >= 80 and "BUY" in btc_4h_str:
        threshold_short = 99; verdict_short = "DESFAVORÁVEL PARA SHORT (Mercado Bull Extremo)"
    elif fg_val <= 20 or "SELL" in btc_4h_str:
        threshold_short = 14; verdict_short = "FAVORÁVEL PARA SHORT (Bear/Medo)"
    elif fg_val >= 75:
        threshold_short = 16; verdict_short = "MODERADO PARA SHORT (Ganância)"
    else:
        threshold_short = 20; verdict_short = "CAUTELOSO PARA SHORT (Neutro)"

    LOG.debug(f"  Contexto: FGI={fg_val} | BTC={btc_4h_str} | "
              f"LONG={verdict_long}(thr={threshold_long}) | "
              f"SHORT={verdict_short}(thr={threshold_short}) | risk_score={risk_score}")

    return {
        "verdict"        : verdict_long,
        "threshold"      : threshold_long,
        "verdict_short"  : verdict_short,
        "threshold_short": threshold_short,
        "risk_score"     : risk_score,
        "fg"             : fg_val,
        "btc"            : btc_4h_str,
    }

# ===========================================================================
# EXECUÇÃO PRINCIPAL
# ===========================================================================

async def run_scan_async():
    global LOG, LOG_FILE, TS_SCAN
    LOG, LOG_FILE, TS_SCAN = setup_logger()

    LOG.info("🚀 Setup Atirador v6.0 | Arquitetura 3 Camadas (LONG+SHORT) | Iniciando scan...")
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
        log_section("GATE 1 — Direção 4H (LONG: não SELL | SHORT: não BUY)")
        gate1_passed    = []   # LONG candidates
        gate1_short     = []   # SHORT candidates
        gate1_rejected  = 0
        tokens_sem_dados = []

        for d in perpetuals:
            sym    = d["symbol"]
            ind_4h = tv_4h.get(sym, {})
            raw_val = ind_4h.get("Recommend.All|240")
            rsi_4h  = sf(ind_4h.get("RSI|240"), default=50.0)

            if raw_val is None:
                tokens_sem_dados.append(d["base_coin"])
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 4H=SEM_DADOS (val=None) — excluído")
                continue

            s4h = recommendation_from_value(raw_val)
            d["summary_4h"] = s4h
            d["rsi_4h"]     = rsi_4h

            # LONG gate: passa se não SELL
            if "SELL" not in s4h:
                gate1_passed.append(d)
                if rsi_4h > 80:
                    LOG.warning(f"  ✅⚠️  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — LONG PASSOU (RSI EXTREMO)")
                    d["rsi_extremo"] = True
                else:
                    LOG.debug(f"  ✅  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — LONG PASSOU")
                    d["rsi_extremo"] = False
            else:
                gate1_rejected += 1
                LOG.debug(f"  ❌  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — LONG REJEITADO")

            # SHORT gate: passa se SELL/STRONG_SELL
            if "SELL" in s4h:
                gate1_short.append(d)
                LOG.debug(f"  📉  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — SHORT CANDIDATO")

        LOG.info(f"  Gate 4H: LONG={len(gate1_passed)} | SHORT={len(gate1_short)} | "
                 f"sem dados TV={len(tokens_sem_dados)} | universo={len(perpetuals)}")
        if tokens_sem_dados:
            LOG.info(f"  Sem dados TV: {tokens_sem_dados}")

        # -------------------------------------------------------------------
        # GATE 2 — Camada 1H
        # -------------------------------------------------------------------
        log_section("GATE 2 — Estrutura 1H (LONG: BUY+ | SHORT: SELL+)")
        # Busca TV 1H para todos os candidatos (LONG + SHORT, sem duplicatas)
        all_gate1 = list({d["symbol"]: d for d in gate1_passed + gate1_short}.values())
        symbols_1h = [d["symbol"] for d in all_gate1]
        tv_1h      = await fetch_tv_batch_async(session, symbols_1h, COLS_1H)

        gate2_passed   = []   # LONG
        gate2_short    = []   # SHORT
        gate2_rejected = 0

        for d in gate1_passed:
            sym    = d["symbol"]
            ind_1h = tv_1h.get(sym, {})
            raw_1h = ind_1h.get("Recommend.All|60")
            if raw_1h is None:
                if d["base_coin"] not in tokens_sem_dados:
                    tokens_sem_dados.append(d["base_coin"])
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 1H=SEM_DADOS — excluído LONG")
                gate2_rejected += 1; continue
            s1h = recommendation_from_value(raw_1h)
            d["summary_1h"] = s1h
            if "BUY" in s1h:
                gate2_passed.append(d)
                LOG.debug(f"  ✅  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — LONG PASSOU")
            else:
                gate2_rejected += 1
                LOG.debug(f"  ❌  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — LONG REJEITADO")

        for d in gate1_short:
            sym    = d["symbol"]
            ind_1h = tv_1h.get(sym, {})
            raw_1h = ind_1h.get("Recommend.All|60")
            if raw_1h is None:
                # [v6.0 FIX] Loga explicitamente — não descarta silenciosamente
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 1H=SEM_DADOS — excluído SHORT (sem dados TV no 1H)")
                continue
            s1h = recommendation_from_value(raw_1h)
            d["summary_1h"] = s1h
            if "SELL" in s1h:
                gate2_short.append(d)
                LOG.debug(f"  📉  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — SHORT PASSOU")
            else:
                LOG.debug(f"  ❌  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — SHORT REJEITADO (não SELL no 1H)")

        LOG.info(f"  Gate 1H: LONG={len(gate2_passed)} | SHORT={len(gate2_short)} | rejeitados={gate2_rejected}")

        if not gate2_passed and not gate2_short:
            LOG.warning("  ⚠️  Nenhum token passou os 2 gates (LONG ou SHORT) — encerrando scan")
            ts_full = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
            btc     = recommendation_from_value(tv_4h.get("BTCUSDT", {}).get("Recommend.All|240"))
            ctx     = analyze_market_context(fg, btc)
            report  = f"{'='*58}\n🎯 SETUP ATIRADOR v6.0\n📅 {ts_full}\n📋 Log: {os.path.basename(LOG_FILE)}\n{'='*58}\n"
            report += f"📊 Contexto: {ctx['verdict']} | FGI: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
            report += f"⏱️  Execução: {time.time()-t_start:.1f}s | Analisados: {total_items}\n"
            report += f"\n⚠️  Nenhum token passou os dois gates (LONG ou SHORT).\n"
            report += f"   Gate 4H: LONG={len(gate1_passed)} | SHORT={len(gate1_short)}\n"
            report += f"   Gate 1H: LONG=0/{len(gate1_passed)} | SHORT=0/{len(gate1_short)}\n"
            if tokens_sem_dados:
                report += f"   Sem dados TV ({len(tokens_sem_dados)}): {', '.join(tokens_sem_dados)}\n"
            report += f"\n   Aguarde próximo scan ou verifique o TradingView manualmente.\n"
            report += f"\n📋 Log completo: {LOG_FILE}\n"
            LOG.info(report)
            output_path = f"/tmp/atirador_SCAN_{TS_SCAN}.txt"
            with open(output_path, "w") as f: f.write(report)
            return report

        # -------------------------------------------------------------------
        # ETAPA 3: Indicadores 15m TradingView (LONG + SHORT juntos)
        # -------------------------------------------------------------------
        log_section("ETAPA 3 — Indicadores 15m (TradingView) — LONG + SHORT")
        # Sem duplicatas: SHORT pode incluir tokens não presentes no LONG
        all_gate2 = list({d["symbol"]: d for d in gate2_passed + gate2_short}.values())
        symbols_15m = [d["symbol"] for d in all_gate2]
        tv_15m      = await fetch_tv_batch_async(session, symbols_15m, COLS_15M)
        for d in all_gate2:
            sym               = d["symbol"]
            ind_15m           = tv_15m.get(sym, {})
            d["_ind_15m"]     = ind_15m
            d["bb_upper_15m"] = sf(ind_15m.get("BB.upper|15"))
            d["bb_lower_15m"] = sf(ind_15m.get("BB.lower|15"))
            d["atr_15m"]      = sf(ind_15m.get("ATR|15"))
            LOG.debug(f"  {d['base_coin']:<8} ATR={d['atr_15m']:.4f} | "
                      f"BB_lower={d['bb_lower_15m']:.4f} | BB_upper={d['bb_upper_15m']:.4f} | "
                      f"FR={d['funding_rate']:.5f}")

        # Score parcial LONG para ordenação
        log_section("ETAPA 3b — Score parcial (sem klines, para ordenação)")
        pump_bloqueados      = []
        pump_bloqueados_short = []

        for d in gate2_passed:
            sc_p, _, _ = calculate_score(d, fg_value=fg.get("value", 50),
                                         log_breakdown=False, direction="LONG")
            d["_partial_score"] = sc_p
            if sc_p == -99:
                pump_bloqueados.append(d)
                LOG.warning(f"  🚫  {d['base_coin']:<8} PUMP BLOCK LONG | {d.get('price_change_24h',0):.1f}%")
            else:
                LOG.debug(f"  {d['base_coin']:<8} score parcial LONG: {sc_p}/26 "
                          f"[FR={d.get('funding_rate',0):.4%} "
                          f"BB={d.get('bb_lower_15m',0):.4f}–{d.get('bb_upper_15m',0):.4f}]")

        for d in gate2_short:
            sc_p, _, _ = calculate_score(d, fg_value=fg.get("value", 50),
                                         log_breakdown=False, direction="SHORT")
            d["_partial_score_short"] = sc_p
            if sc_p == -99:
                pump_bloqueados_short.append(d)
                LOG.warning(f"  🚫  {d['base_coin']:<8} DUMP BLOCK SHORT | {d.get('price_change_24h',0):.1f}%")
            else:
                LOG.debug(f"  {d['base_coin']:<8} score parcial SHORT: {sc_p}/26 "
                          f"[FR={d.get('funding_rate',0):.4%} "
                          f"BB={d.get('bb_lower_15m',0):.4f}–{d.get('bb_upper_15m',0):.4f}]")

        gate2_passed.sort(key=lambda x: x["_partial_score"], reverse=True)
        gate2_passed = [d for d in gate2_passed if d["_partial_score"] >= 0]
        gate2_short.sort(key=lambda x: x.get("_partial_score_short", 0), reverse=True)
        gate2_short = [d for d in gate2_short if d.get("_partial_score_short", 0) >= 0]

        LOG.info(f"  Ordem LONG: {[d['base_coin'] for d in gate2_passed]}")
        LOG.info(f"  Ordem SHORT: {[d['base_coin'] for d in gate2_short]}")
        if pump_bloqueados:
            LOG.info(f"  Bloqueados pump LONG: {[d['base_coin'] for d in pump_bloqueados]}")
        if pump_bloqueados_short:
            LOG.info(f"  Bloqueados dump SHORT: {[d['base_coin'] for d in pump_bloqueados_short]}")

        # -------------------------------------------------------------------
        # ETAPA 4: Klines — TOP N LONG + TOP N SHORT
        # Regra de exclusividade: mesmo token não pode ter LONG e SHORT juntos
        # -------------------------------------------------------------------
        log_section(f"ETAPA 4 — Klines + Score completo (TOP {KLINE_TOP_N} LONG + SHORT)")

        # Símbolos já abertos — bloqueia direção oposta no mesmo ativo
        syms_abertos_long  = {t.get("symbol") for t in state.get("trades_abertos", [])
                              if t.get("direction") == "LONG"}
        syms_abertos_short = {t.get("symbol") for t in state.get("trades_abertos", [])
                              if t.get("direction") == "SHORT"}

        # [v6.0] Loga quais símbolos estão bloqueados por exclusividade
        if syms_abertos_long:
            LOG.info(f"  🔒 Trades LONG abertos — bloqueiam SHORT: {sorted(syms_abertos_long)}")
        if syms_abertos_short:
            LOG.info(f"  🔒 Trades SHORT abertos — bloqueiam LONG: {sorted(syms_abertos_short)}")

        # Remover conflitos: LONG aberto bloqueia SHORT e vice-versa
        bloqueados_por_exclusividade_long  = [d for d in gate2_passed if d["symbol"] in syms_abertos_short]
        bloqueados_por_exclusividade_short = [d for d in gate2_short  if d["symbol"] in syms_abertos_long]
        for d in bloqueados_por_exclusividade_long:
            LOG.warning(f"  🔒  {d['base_coin']:<8} LONG bloqueado — SHORT aberto no mesmo ativo")
        for d in bloqueados_por_exclusividade_short:
            LOG.warning(f"  🔒  {d['base_coin']:<8} SHORT bloqueado — LONG aberto no mesmo ativo")

        gate2_passed = [d for d in gate2_passed if d["symbol"] not in syms_abertos_short]
        gate2_short  = [d for d in gate2_short  if d["symbol"] not in syms_abertos_long]

        top_full_long  = gate2_passed[:KLINE_TOP_N]
        top_light_long = gate2_passed[KLINE_TOP_N:KLINE_TOP_N_LIGHT]
        top_full_short = gate2_short[:KLINE_TOP_N]

        # Busca klines para todos de uma vez (sem duplicatas)
        all_top = list({d["symbol"]: d for d in top_full_long + top_full_short}.values())
        results      = []
        results_short = []
        observacoes  = []

        if all_top:
            LOG.info(f"  Buscando klines para: {[d['base_coin'] for d in all_top]}")
            tasks_15m = [fetch_klines_async(session, d["symbol"], "15m") for d in all_top]
            tasks_1h  = [fetch_klines_cached_async(session, d["symbol"], "1H") for d in all_top]
            tasks_4h  = [fetch_klines_cached_async(session, d["symbol"], "4H") for d in all_top]
            k15m_all, k1h_all, k4h_all = await asyncio.gather(
                asyncio.gather(*tasks_15m),
                asyncio.gather(*tasks_1h),
                asyncio.gather(*tasks_4h),
            )
            klines_map = {d["symbol"]: (k15m_all[i], k1h_all[i], k4h_all[i])
                          for i, d in enumerate(all_top)}

            # Score LONG
            for d in top_full_long:
                k15m, k1h, k4h = klines_map.get(d["symbol"], ([], [], []))
                sym = d["base_coin"]
                LOG.info(f"  ─ LONG {sym}: 15m={len(k15m)} | 1H={len(k1h)} | 4H={len(k4h)}")
                if not k15m:
                    LOG.warning(f"  ⚠️  {sym}: klines 15m vazios — pulando LONG")
                    continue
                sc, reasons, bd = calculate_score(
                    d, candles_15m=k15m, candles_1h=k1h, candles_4h=k4h,
                    fg_value=fg.get("value", 50), direction="LONG")
                d["score"] = sc; d["reasons"] = reasons; d["breakdown"] = bd
                trade = calc_trade_params(d["price"], d.get("atr_15m", 0), score=sc)
                if trade:
                    LOG.info(f"  📈 LONG {sym}: score={sc}/26 | entry={trade['entry']:.4f} | "
                             f"SL={trade['sl_distance_pct']:.2f}% | alav={trade['alavancagem']}x ✅")
                    d["trade"] = trade; d["direction"] = "LONG"
                    results.append(d)
                else:
                    LOG.warning(f"  📈 LONG {sym}: score={sc}/26 | trade_params=❌ "
                                f"(ATR={d.get('atr_15m',0):.4f} — inválido para SL dinâmico)")

            # Score SHORT
            for d in top_full_short:
                k15m, k1h, k4h = klines_map.get(d["symbol"], ([], [], []))
                sym = d["base_coin"]
                LOG.info(f"  ─ SHORT {sym}: 15m={len(k15m)} | 1H={len(k1h)} | 4H={len(k4h)}")
                if not k15m:
                    LOG.warning(f"  ⚠️  {sym}: klines 15m vazios — pulando SHORT")
                    continue
                sc, reasons, bd = calculate_score(
                    d, candles_15m=k15m, candles_1h=k1h, candles_4h=k4h,
                    fg_value=fg.get("value", 50), direction="SHORT")
                d["score_short"] = sc; d["reasons_short"] = reasons; d["breakdown_short"] = bd
                trade = calc_trade_params_short(d["price"], d.get("atr_15m", 0), score=sc)
                if trade:
                    LOG.info(f"  📉 SHORT {sym}: score={sc}/26 | entry={trade['entry']:.4f} | "
                             f"SL={trade['sl_distance_pct']:.2f}% | alav={trade['alavancagem']}x ✅")
                    d["trade_short"] = trade; d["direction"] = "SHORT"
                    results_short.append(d)
                else:
                    LOG.warning(f"  📉 SHORT {sym}: score={sc}/26 | trade_params=❌ "
                                f"(ATR={d.get('atr_15m',0):.4f} — inválido para SL dinâmico)")

        # Análise leve LONG (sem klines)
        log_section("ETAPA 4b — Análise leve LONG (sem klines)")
        for d in top_light_long:
            sc, reasons, bd = calculate_score(d, fg_value=fg.get("value", 50),
                                              log_breakdown=False, direction="LONG")
            d["score"] = sc; d["reasons"] = reasons; d["breakdown"] = bd
            trade = calc_trade_params(d["price"], d.get("atr_15m", 0), score=sc)
            if trade:
                d["trade"] = trade; d["direction"] = "LONG"
                observacoes.append(d)
                LOG.debug(f"  {d['base_coin']:<8} score parcial LONG={sc}/26 → Em Observação")

        # -------------------------------------------------------------------
        # ETAPA 5: Contexto e Relatório bidirecional
        # -------------------------------------------------------------------
        log_section("ETAPA 5 — Contexto de Mercado e Relatório Bidirecional")
        results.sort(key=lambda x: x["score"], reverse=True)
        results_short.sort(key=lambda x: x.get("score_short", 0), reverse=True)
        observacoes.sort(key=lambda x: x["score"], reverse=True)

        btc_4h_val = tv_4h.get("BTCUSDT", {}).get("Recommend.All|240")
        btc_4h_str = recommendation_from_value(btc_4h_val)
        ctx        = analyze_market_context(fg, btc_4h_str)

        LOG.info(f"  BTC 4H: {btc_4h_str} | Thr LONG: {ctx['threshold']} ({ctx['verdict']}) | "
                 f"Thr SHORT: {ctx['threshold_short']} ({ctx['verdict_short']})")
        LOG.info(f"  Results LONG: {len(results)} | SHORT: {len(results_short)} | Obs: {len(observacoes)}")

        ts_full   = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
        risco_usd = RISCO_POR_TRADE_USD
        perda_max = MAX_PERDA_DIARIA_USD
        pnl_dia   = state.get("pnl_dia", 0.0)
        n_abertos = len(state.get("trades_abertos", []))

        report  = f"{'='*58}\n"
        report += f"🎯 SETUP ATIRADOR v6.0\n"
        report += f"📅 {ts_full}\n"
        report += f"📋 Log: {os.path.basename(LOG_FILE)}\n"
        report += f"{'='*58}\n"
        report += f"📊 CONTEXTO DE MERCADO\n"
        report += f"   Fear & Greed: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        report += f"   LONG:  {ctx['verdict']} | Threshold: {ctx['threshold']} pts\n"
        report += f"   SHORT: {ctx['verdict_short']} | Threshold: {ctx['threshold_short']} pts\n"
        report += f"   Risk Score: {ctx['risk_score']}\n"
        report += f"{'='*58}\n\n"

        report += f"💼 GESTÃO DE RISCO — Estratégia de Recuperação\n"
        report += f"   Banca: ${BANKROLL:.2f} | Risco fixo/trade: ${risco_usd:.2f} | Perda máx/dia: ${perda_max:.2f}\n"
        ganho_por_trade     = risco_usd * RR_MINIMO
        winners_para_dobrar = int(BANKROLL / ganho_por_trade)
        report += f"   Ganho/trade (RR1:2): ${ganho_por_trade:.2f} | Para dobrar banca: ~{winners_para_dobrar} winners\n"
        report += f"   P&L hoje: ${pnl_dia:+.2f} | Trades abertos: {n_abertos}/{MAX_TRADES_ABERTOS}\n"
        if not pode_operar:
            report += f"   🛑 NOVAS ENTRADAS BLOQUEADAS: {motivo_risco}\n"
        else:
            report += f"   ✅ Pode operar — {MAX_TRADES_ABERTOS - n_abertos} slot(s) disponível(is)\n"
        report += "\n"

        report += f"🔍 PIPELINE\n"
        report += f"   Fonte de dados: {DATA_SOURCE} (perpetuals USDT)\n"
        report += f"   Universo: {total_items} tokens | Qualificados: {len(perpetuals)}\n"
        report += f"   Gate 4H: LONG={len(gate1_passed)} | SHORT={len(gate1_short)}\n"
        report += f"   Gate 1H: LONG={len(gate2_passed)} | SHORT={len(gate2_short)}\n"
        report += f"   Análise completa: LONG={len(top_full_long)} | SHORT={len(top_full_short)}\n"

        if len(DATA_SOURCE_ATTEMPTS) > 1:
            report += f"   📡 Fontes tentadas:\n"
            for a in DATA_SOURCE_ATTEMPTS:
                status_str = f"HTTP {a['status']}" if a['status'] else "sem resposta"
                if a['falha']:
                    report += f"      ⛔ {a['fonte']}: {status_str} em {a['elapsed_s']}s — {a['falha']}\n"
                else:
                    report += f"      ✅ {a['fonte']}: {status_str} em {a['elapsed_s']}s — {a['qualificados']} qualificados\n"
        elif DATA_SOURCE_ATTEMPTS:
            a = DATA_SOURCE_ATTEMPTS[0]
            report += f"   📡 Fonte ativa: {a['fonte']} | HTTP {a['status']} | {a['elapsed_s']}s\n"

        if tokens_sem_dados:
            report += f"   ⚠️  Sem dados TradingView ({len(tokens_sem_dados)}): {', '.join(tokens_sem_dados)}\n"

        all_pump = pump_bloqueados + pump_bloqueados_short
        if all_pump:
            pump_str = ", ".join(f"{d['base_coin']}({d.get('price_change_24h',0):.0f}%)" for d in all_pump)
            report += f"   🚫 Bloqueados pump/dump: {pump_str}\n"

        rsi_extremos = [d for d in gate1_passed if d.get("rsi_extremo")]
        if rsi_extremos:
            rsi_str = ", ".join(f"{d['base_coin']}(RSI={d['rsi_4h']:.0f})" for d in rsi_extremos)
            report += f"   ⚠️  RSI 4H extremo (>80): {rsi_str}\n"

        report += "\n"

        # ─── SEÇÃO LONG ───────────────────────────────────────────────────
        report += f"{'─'*58}\n📈 OPERAÇÕES LONG\n{'─'*58}\n"
        alertas_long = [r for r in results if r["score"] >= ctx["threshold"]]

        if ctx["threshold"] == 99:
            report += "🛑 LONG DESLIGADO — Mercado desfavorável.\n"
        elif not alertas_long:
            max_sc = max((r["score"] for r in results), default=0)
            report += f"ℹ️  Nenhum alerta LONG forte (score ≥ {ctx['threshold']}) no momento.\n"
            if results:
                report += f"   Score máximo: {max_sc}/26 (faltam {ctx['threshold'] - max_sc} pts)\n"
        else:
            report += f"🔥 {len(alertas_long)} ALERTA(S) LONG — Score ≥ {ctx['threshold']}/26:\n\n"
            for r in alertas_long:
                t    = r["trade"]
                bloq = " ⛔ BLOQUEADO" if not pode_operar else ""
                report += f"🚀 LONG {r['base_coin']}{bloq}\n"
                report += f"   Score: {r['score']}/26 | 4H: {r['summary_4h']} | 1H: {r['summary_1h']}\n"
                report += f"   Razões: {', '.join(r['reasons'][:4])}\n"
                report += f"   Preço: ${r['price']:.4f} | Vol 24h: ${r['turnover_24h']/1e6:.1f}M\n"
                report += f"   Alavancagem: {t['alavancagem']}x | Risco: ${t['risco_usd']:.2f} | Ganho: ${t['ganho_rr2_usd']:.2f}\n"
                report += f"   SL:  ${t['sl']:.4f}  (-{t['sl_distance_pct']:.2f}%)\n"
                report += f"   TP1: ${t['tp1']:.4f} (+{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
                report += f"   TP2: ${t['tp2']:.4f} (+{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
                report += f"   TP3: ${t['tp3']:.4f} (+{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
                report += f"   Trailing: ativar no TP1 → SL breakeven +0.5%\n\n"

        oport_long = [r for r in results if ctx["threshold"] > r["score"] >= 10]
        radar_long = [r for r in results if 5 <= r["score"] < 10]
        if oport_long:
            report += f"\n📈 OPORTUNIDADES LONG em Formação — Score 10–{ctx['threshold']-1}:\n"
            for r in oport_long[:5]:
                report += f"   ▶ {r['base_coin']} | {r['score']}/26 | {', '.join(r['reasons'][:2])}\n"
        if radar_long:
            report += f"\n🔎 RADAR LONG — Score 5–9:\n"
            report += f"   ⚠️  Não operar — insuficiente. Monitorar.\n"
            for r in radar_long[:5]:
                report += f"   · {r['base_coin']} | {r['score']}/26 | {r['summary_4h']} 4H\n"

        # ─── SEÇÃO SHORT ──────────────────────────────────────────────────
        report += f"\n{'─'*58}\n📉 OPERAÇÕES SHORT\n{'─'*58}\n"
        alertas_short = [r for r in results_short
                         if r.get("score_short", 0) >= ctx["threshold_short"]]

        if ctx["threshold_short"] == 99:
            report += "🛑 SHORT DESLIGADO — Mercado muito bullish, risco extremo de operar contra.\n"
        elif not alertas_short:
            max_sc_s = max((r.get("score_short", 0) for r in results_short), default=0)
            report += f"ℹ️  Nenhum alerta SHORT forte (score ≥ {ctx['threshold_short']}) no momento.\n"
            if results_short:
                report += f"   Score máximo SHORT: {max_sc_s}/26 (faltam {ctx['threshold_short'] - max_sc_s} pts)\n"
        else:
            report += f"🔥 {len(alertas_short)} ALERTA(S) SHORT — Score ≥ {ctx['threshold_short']}/26:\n\n"
            for r in alertas_short:
                t    = r["trade_short"]
                bloq = " ⛔ BLOQUEADO" if not pode_operar else ""
                report += f"📉 SHORT {r['base_coin']}{bloq}\n"
                report += f"   Score: {r['score_short']}/26 | 4H: {r['summary_4h']} | 1H: {r['summary_1h']}\n"
                report += f"   Razões: {', '.join(r.get('reasons_short', [])[:4])}\n"
                report += f"   Preço: ${r['price']:.4f} | Vol 24h: ${r['turnover_24h']/1e6:.1f}M\n"
                report += f"   Alavancagem: {t['alavancagem']}x | Risco: ${t['risco_usd']:.2f} | Ganho: ${t['ganho_rr2_usd']:.2f}\n"
                report += f"   SL:  ${t['sl']:.4f}  (+{t['sl_distance_pct']:.2f}%) ← ACIMA\n"
                report += f"   TP1: ${t['tp1']:.4f} (-{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
                report += f"   TP2: ${t['tp2']:.4f} (-{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
                report += f"   TP3: ${t['tp3']:.4f} (-{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
                report += f"   Trailing: ativar no TP1 → SL breakeven -0.5%\n\n"

        radar_short = [r for r in results_short
                       if 5 <= r.get("score_short", 0) < ctx["threshold_short"]]
        if radar_short:
            report += f"\n🔎 RADAR SHORT — Score 5–{ctx['threshold_short']-1}:\n"
            report += f"   ⚠️  Não operar — insuficiente. Monitorar.\n"
            for r in radar_short[:5]:
                report += f"   · {r['base_coin']} | {r.get('score_short',0)}/26 | {r['summary_4h']} 4H\n"

        # Em Observação (LONG leve)
        obs_relevantes = [o for o in observacoes if o["score"] >= 8]
        if obs_relevantes:
            report += f"\n👁️  EM OBSERVAÇÃO LONG — análise leve ({len(obs_relevantes)} tokens):\n"
            for o in obs_relevantes[:5]:
                report += f"   · {o['base_coin']} | Score parcial: {o['score']}/26 | {o['summary_4h']} 4H\n"

        elapsed = time.time() - t_start
        report += f"\n{'-'*58}\n"
        report += f"⏱️  Execução: {elapsed:.1f}s | Analisados: {total_items} tokens\n"
        report += f"📁 Estado diário: {STATE_FILE}\n"
        report += f"📋 Log completo: {LOG_FILE}\n"

        output_path = f"/tmp/atirador_SCAN_{TS_SCAN}.txt"
        with open(output_path, "w") as f: f.write(report)

        LOG.info(report)
        LOG.info(f"✅ Scan v6.0 concluído em {elapsed:.1f}s | Fonte: {DATA_SOURCE} | "
                 f"Relatório: {output_path} | Log: {LOG_FILE}")
        return report


def main():
    # Logger inicializado dentro de run_scan_async (precisa do timestamp de execução)
    asyncio.run(run_scan_async())

if __name__ == "__main__":
    main()
