#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v4.3 - Scanner Profissional de Criptomoedas
=============================================================================
Arquitetura Multi-Timeframe com 3 Camadas Independentes:

  CAMADA 1 — 4H "Qual é a direção do mercado?" (contexto macro)
    Gate: TV Recommend.All 4H
      - SELL ou STRONG_SELL → descarte imediato
      - NEUTRAL / BUY / STRONG_BUY → segue para Gate 2
    Modificador de qualidade (não entra no score diretamente):
      - Registrado no relatório como contexto
      - BTC 4H afeta threshold adaptativo de alerta

  CAMADA 2 — 1H "Estamos num bom ponto de entrada?" (estrutura)
    Gate: TV Recommend.All 1H
      - BUY ou STRONG_BUY obrigatório → passa
      - NEUTRAL / SELL → descarte
    Pilar no score:
      - P-1H: Suporte/Order Block no 1H (klines) — posição de preço
        genuinamente diferente do TV Recommend

  CAMADA 3 — 15m "O timing de entrada está correto agora?" (gatilho)
    Score composto por 8 pilares independentes:
      P1 — Bollinger Bands 15m       (posição no canal de volatilidade)
      P2 — Padrões de Candle 15m     (price action / gatilho visual)
      P3 — Funding Rate              (sentimento de derivativos)
      P4 — Zonas de Liquidez 4H      (S/R + Order Blocks — estrutura de preço)
      P5 — Figuras Gráficas 4H       (geometria de compressão/breakout)
      P6 — CHOCH/BOS 4H              (Smart Money Concepts)
      P7 — Filtro de Pump            (proteção contra armadilha)
      P8 — Volume 15m adaptativo     (confirmação de força)

Score máximo: 26 pts
Thresholds: Favorável=16, Moderado=18, Cauteloso=21

v4.3 vs v4.2:
  - Gate principal migrado de 4H para 1H (velocidade para scalp)
  - 4H vira modificador de contexto, não gate binário (exceto SELL)
  - 6 pilares redundantes removidos (RSI, Stoch, CCI, MACD, ADX, Tendência 4H)
  - Novo pilar 1H: suporte/OB em klines 1H (informação genuinamente nova)
  - Score recalibrado sem inflação por dupla contagem
  - Todos os bugs e melhorias da v4.2 mantidos

Autor: Manus AI | v4.1 → v4.2 (revisão Claude) → v4.3 (refatoramento Claude)
=============================================================================
"""

import json
import requests
import time
import os
import numpy as np
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

# Fuso horário BRT (Brasília, UTC-3)
BRT = timezone(timedelta(hours=-3))

# ===========================================================================
# CONFIGURAÇÃO
# ===========================================================================

# Filtros Institucionais
MIN_TURNOVER_24H = 5_000_000
MIN_OI_USD       = 10_000_000
TOP_N            = 30

# Gestão de Risco
BANKROLL             = 100.0
RISCO_POR_TRADE_PCT  = 0.75   # 0.75% da banca por trade = $0.75
MAX_PERDA_DIARIA_PCT = 4.0    # 4% = $4.00 — bloqueia novas entradas
MAX_TRADES_ABERTOS   = 2
ALAVANCAGEM_MAX      = 50
RR_MINIMO            = 2.0    # Risk:Reward mínimo 1:2

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
        print(f"⚠️  Erro ao salvar estado: {e}")

def check_risk_limits(state):
    """
    Verifica limites de risco diário.
    Retorna (pode_operar: bool, motivo: str)
    """
    if state.get("bloqueado"):
        return False, state.get("motivo_bloqueio", "Estado bloqueado")

    perda_max = BANKROLL * MAX_PERDA_DIARIA_PCT / 100
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
    Busca indicadores do TradingView de forma assíncrona.
    symbols: lista de símbolos COMPLETOS, ex: ["BTCUSDT", "ETHUSDT"]
    Monta "BYBIT:BTCUSDT" internamente e devolve resultado com chave "BTCUSDT".
    """
    if not symbols: return {}
    tickers = [f"BYBIT:{s}" for s in symbols]
    payload = {
        "symbols": {"tickers": tickers, "query": {"types": []}},
        "columns": columns,
    }
    for attempt in range(retries):
        try:
            async with session.post(TV_URL, json=payload, headers=TV_HEADERS, timeout=25) as resp:
                resp.raise_for_status()
                data = await resp.json()
                result = {}
                for item in data.get("data", []):
                    # item["s"] = "BYBIT:BTCUSDT" → chave = "BTCUSDT"
                    sym = item["s"].replace("BYBIT:", "")
                    result[sym] = dict(zip(columns, item["d"]))
                return result
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))
            else:
                print(f"  ⚠️  ERRO TV batch: {e}")
                return {}

# ===========================================================================
# HELPERS
# ===========================================================================

def sf(val, default=0.0):
    try: return float(val) if val is not None and val != "" else default
    except: return default

async def api_get_async(session, url, retries=3):
    for i in range(retries):
        try:
            async with session.get(url, timeout=20) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            if i < retries - 1: await asyncio.sleep(2)
            else: return None

def api_get(url, retries=3):
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if i < retries - 1: time.sleep(2)
            else: raise

# ===========================================================================
# DADOS DE MERCADO (Bitget API)
# ===========================================================================

def fetch_perpetuals():
    """Busca perpetuals USDT com filtros institucionais."""
    data = api_get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
    items = data.get("data", [])
    qualified = []
    for t in items:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"): continue
        turnover = sf(t.get("usdtVolume"))
        if turnover < MIN_TURNOVER_24H: continue
        price   = sf(t.get("lastPr"))
        holding = sf(t.get("holdingAmount"))
        oi_usd  = holding * price
        if oi_usd < MIN_OI_USD: continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"         : sym,
            "base_coin"      : base,
            "price"          : price,
            "turnover_24h"   : turnover,
            "oi_usd"         : oi_usd,
            "volume_24h"     : sf(t.get("baseVolume")),
            "funding_rate"   : sf(t.get("fundingRate")),
            "price_change_24h": sf(t.get("change24h")) * 100,
        })
    qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
    return qualified, len(items)

async def fetch_fear_greed_async(session):
    """Fear & Greed Index global."""
    try:
        data = await api_get_async(session, "https://api.alternative.me/fng/?limit=1")
        if data and "data" in data:
            v = data["data"][0]
            return {"value": int(v["value"]), "classification": v["value_classification"]}
    except: pass
    return {"value": 50, "classification": "Neutral"}

async def fetch_klines_async(session, symbol, granularity="15m", limit=60):
    """Busca klines da Bitget."""
    try:
        url = (f"https://api.bitget.com/api/v2/mix/market/candles"
               f"?productType=USDT-FUTURES&symbol={symbol}"
               f"&granularity={granularity}&limit={limit}")
        data = await api_get_async(session, url)
        if not data or "data" not in data: return []
        result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                   "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                  for c in data["data"]]
        result.reverse()
        return result
    except: return []

async def fetch_klines_cached_async(session, symbol, granularity="4H", limit=60):
    """Klines com cache local para timeframes maiores."""
    cache_dir  = "/tmp/atirador_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = f"{cache_dir}/klines_{symbol}_{granularity}.json"
    if os.path.exists(cache_file):
        age_h = (time.time() - os.path.getmtime(cache_file)) / 3600
        if age_h < KLINE_CACHE_TTL_H:
            try:
                with open(cache_file) as f: return json.load(f)
            except: pass
    klines = await fetch_klines_async(session, symbol, granularity, limit)
    if klines:
        try:
            with open(cache_file, "w") as f: json.dump(klines, f)
        except: pass
    return klines

# ===========================================================================
# ANÁLISE TÉCNICA — UTILITÁRIOS
# ===========================================================================

def find_swing_points(candles, window=None):
    """Detecta swing highs e swing lows."""
    if window is None: window = SWING_WINDOW
    if len(candles) < window * 2 + 1: return [], []
    highs = np.array([c["high"]  for c in candles])
    lows  = np.array([c["low"]   for c in candles])
    sh, sl = [], []
    for i in range(window, len(candles) - window):
        if highs[i] == np.max(highs[i - window:i + window + 1]):
            sh.append({"index": i, "price": highs[i]})
        if lows[i]  == np.min(lows[i  - window:i + window + 1]):
            sl.append({"index": i, "price": lows[i]})
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
    """
    price = d.get("price", 0)
    bbl   = d.get("bb_lower_15m", 0)
    bbu   = d.get("bb_upper_15m", 0)
    if not (price and bbl and bbu and (bbu - bbl) > 0):
        return 0, "BB N/A"
    pos = (price - bbl) / (bbu - bbl)
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

def calc_trade_params(price, atr):
    """
    Calcula SL/TP baseados em ATR (volatilidade real).
    Alavancagem dinâmica para manter risco fixo em $ independente do SL.
    """
    if not price or not atr or atr <= 0: return None

    sl_dist_pct = (1.5 * atr) / price * 100          # SL = 1.5x ATR
    sl          = price * (1 - sl_dist_pct / 100)
    tp_dist_pct = sl_dist_pct * RR_MINIMO             # TP = 2x risco (RR 1:2)

    # Alavancagem dinâmica: garante que o risco $ seja sempre RISCO_POR_TRADE_PCT
    if sl_dist_pct < 0.1: return None                 # SL muito próximo = ruído
    alav = min(RISCO_POR_TRADE_PCT / sl_dist_pct, ALAVANCAGEM_MAX)

    return {
        "entry"          : price,
        "sl"             : sl,
        "sl_distance_pct": sl_dist_pct,
        "tp1"            : price * (1 + sl_dist_pct / 100),        # RR 1:1 (50%)
        "tp2"            : price * (1 + sl_dist_pct * 2 / 100),    # RR 1:2 (30%)
        "tp3"            : price * (1 + sl_dist_pct * 3 / 100),    # RR 1:3 (20%)
        "rr"             : RR_MINIMO,
        "alavancagem"    : round(alav, 1),
        "atr"            : atr,
    }

# ===========================================================================
# SISTEMA DE SCORE v4.3
# ===========================================================================

def calculate_score(d, candles_15m=None, candles_1h=None, candles_4h=None, fg_value=50):
    """
    Score com 3 camadas independentes. Max: 26 pts.

    Camada 1 — 4H (estrutura de preço, klines):
      P4  Zonas de Liquidez 4H       max 3 pts
      P5  Figuras Gráficas 4H        max 2 pts
      P6  CHOCH/BOS 4H               max 3 pts
      Subtotal camada 1:             max 8 pts

    Camada 2 — 1H (posição de preço, klines):
      P-1H  Suporte / OB 1H          max 4 pts
      Subtotal camada 2:             max 4 pts

    Camada 3 — 15m (gatilho de entrada):
      P1  Bollinger Bands            max 3 pts
      P2  Padrões de Candle          max 4 pts
      P3  Funding Rate               max 2 pts  (pode ser -1)
      P7  Filtro de Pump             max 0 pts  (penalidade até -3, block = descarte)
      P8  Volume 15m                 max 2 pts
      Subtotal camada 3:             max 11 pts (antes de penalidades)

    Contexto de mercado (fora do score, afeta threshold no relatório):
      - 4H summary do token: SELL → descarte | NEUTRAL/BUY/STRONG_BUY → segue
      - BTC 4H: threshold adaptativo
      - Fear & Greed: threshold adaptativo
    """
    sc       = 0
    reasons  = []
    breakdown = []

    # -----------------------------------------------------------------------
    # GATE CAMADA 1: 4H — direção macro
    # SELL descarta. NEUTRAL permite. Não entra no score.
    # -----------------------------------------------------------------------
    s4h = d.get("summary_4h", "NEUTRAL")
    if "SELL" in s4h and "STRONG" in s4h:
        return -1, [f"4H STRONG_SELL — descartado"], []
    if s4h == "SELL":
        return -1, [f"4H SELL — descartado"], []
    # Registra contexto macro sem pontuar
    breakdown.append(("Contexto 4H", 0, 0, f"{s4h} (contexto, não pontuado)"))

    # -----------------------------------------------------------------------
    # CAMADA 1 — Pilares 4H (estrutura de preço)
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
    # CAMADA 2 — Pilar 1H (posição de preço)
    # -----------------------------------------------------------------------

    # P-1H — Suporte / Order Block 1H
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
    if bb_sc >= 2: reasons.append(f"BB inferior")

    # P2 — Padrões de Candle 15m
    ind_15m = d.get("_ind_15m", {})
    cp_list, ca_sc = score_candles(ind_15m)
    sc += ca_sc
    breakdown.append(("P2 Candles 15m", ca_sc, 4,
                       f"Padrões: {', '.join(cp_list)}" if cp_list else "Nenhum"))
    if cp_list: reasons.append(f"Candle: {cp_list[0]}")

    # P3 — Funding Rate
    fr     = d.get("funding_rate", 0)
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

    return max(sc, 0), reasons, breakdown

# ===========================================================================
# CONTEXTO DE MERCADO E THRESHOLD ADAPTATIVO
# ===========================================================================

def analyze_market_context(fg, btc_4h_str):
    """
    Threshold adaptativo baseado em BTC 4H e Fear & Greed.
    Score máximo é 26 pts — thresholds calibrados para esse teto.
    """
    fg_val     = fg.get("value", 50)
    risk_score = 0

    if fg_val <= 25:   risk_score += 2
    elif fg_val <= 50: risk_score += 1
    elif fg_val >= 75: risk_score -= 1

    if "STRONG_BUY" in btc_4h_str: risk_score += 2
    elif "BUY" in btc_4h_str:      risk_score += 1
    elif "SELL" in btc_4h_str:     risk_score -= 2

    # Mercado desfavorável — bot desligado
    if fg_val >= 80 and "SELL" in btc_4h_str:
        return {"verdict": "DESFAVORÁVEL (Bot Desligado)", "threshold": 99,
                "risk_score": risk_score, "fg": fg_val, "btc": btc_4h_str}

    # Threshold adaptativo
    if fg_val <= 30 and "BUY" in btc_4h_str:
        threshold = 14; verdict = "FAVORÁVEL (Bull)"
    elif fg_val >= 75 or "SELL" in btc_4h_str:
        threshold = 20; verdict = "CAUTELOSO (Bear)"
    else:
        threshold = 16; verdict = "MODERADO (Neutro)"

    return {"verdict": verdict, "threshold": threshold,
            "risk_score": risk_score, "fg": fg_val, "btc": btc_4h_str}

# ===========================================================================
# EXECUÇÃO PRINCIPAL
# ===========================================================================

async def run_scan_async():
    print("🚀 Setup Atirador v4.3 | Arquitetura 3 Camadas | Iniciando scan...")
    t_start = time.time()

    state = load_daily_state()
    pode_operar, motivo_risco = check_risk_limits(state)

    async with aiohttp.ClientSession() as session:

        # -------------------------------------------------------------------
        # ETAPA 1: Tickers + Fear & Greed (paralelo)
        # -------------------------------------------------------------------
        perpetuals, total_items = fetch_perpetuals()
        # [v4.3 FIX] TradingView exige símbolo completo: "BTCUSDT", não "BTC"
        # Usamos d["symbol"] (ex: "BTCUSDT") em todas as chamadas ao TV Scanner.
        symbols = [d["symbol"] for d in perpetuals[:TOP_N]]

        tv_4h_task = fetch_tv_batch_async(session, symbols, COLS_4H)
        fg_task    = fetch_fear_greed_async(session)
        tv_4h, fg  = await asyncio.gather(tv_4h_task, fg_task)

        # -------------------------------------------------------------------
        # GATE 1 — Camada 4H: descarta SELL/STRONG_SELL
        # Aceita NEUTRAL, BUY, STRONG_BUY
        # -------------------------------------------------------------------
        gate1_passed  = []
        gate1_rejected = 0
        for d in perpetuals[:TOP_N]:
            sym    = d["symbol"]                          # "BTCUSDT"
            ind_4h = tv_4h.get(sym, {})
            s4h    = recommendation_from_value(ind_4h.get("Recommend.All|240"))
            rsi_4h = sf(ind_4h.get("RSI|240"), default=50.0)
            d["summary_4h"] = s4h
            d["rsi_4h"]     = rsi_4h
            if "SELL" in s4h:
                gate1_rejected += 1
                continue
            gate1_passed.append(d)

        print(f"  Gate 4H: {len(gate1_passed)} passaram | {gate1_rejected} rejeitados (SELL)")

        # -------------------------------------------------------------------
        # GATE 2 — Camada 1H: exige BUY ou STRONG_BUY
        # -------------------------------------------------------------------
        symbols_1h = [d["symbol"] for d in gate1_passed]   # "BTCUSDT", "ETHUSDT" ...
        tv_1h      = await fetch_tv_batch_async(session, symbols_1h, COLS_1H)

        gate2_passed  = []
        gate2_rejected = 0
        for d in gate1_passed:
            sym    = d["symbol"]
            ind_1h = tv_1h.get(sym, {})
            s1h    = recommendation_from_value(ind_1h.get("Recommend.All|60"))
            d["summary_1h"] = s1h
            if "BUY" not in s1h:
                gate2_rejected += 1
                continue
            gate2_passed.append(d)

        print(f"  Gate 1H: {len(gate2_passed)} passaram | {gate2_rejected} rejeitados (não BUY)")

        if not gate2_passed:
            ts  = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")
            # BTC lookup também usa symbol completo
            btc = recommendation_from_value(tv_4h.get("BTCUSDT", {}).get("Recommend.All|240"))
            ctx = analyze_market_context(fg, btc)
            report  = f"🎯 SETUP ATIRADOR v4.3 - {ts}\n{'='*55}\n"
            report += f"📊 Contexto: {ctx['verdict']} | FGI: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
            report += f"⏱️  Execução: {time.time()-t_start:.1f}s | Analisados: {total_items}\n"
            report += f"\n⚠️  Nenhum token passou os dois gates.\n"
            report += f"   Gate 4H (não SELL): {len(gate1_passed)}/{TOP_N}\n"
            report += f"   Gate 1H (BUY/STRONG_BUY): 0/{len(gate1_passed)}\n"
            report += f"\n   Aguarde próximo scan ou verifique o TradingView manualmente.\n"
            print(report)
            output_path = "/tmp/ultimo_scan_atirador_v43.txt"
            with open(output_path, "w") as f: f.write(report)
            return report

        # -------------------------------------------------------------------
        # ETAPA 3: Fetch dados 15m do TradingView (apenas qualificados)
        # -------------------------------------------------------------------
        symbols_15m = [d["symbol"] for d in gate2_passed]
        tv_15m      = await fetch_tv_batch_async(session, symbols_15m, COLS_15M)
        for d in gate2_passed:
            ind_15m           = tv_15m.get(d["symbol"], {})
            d["_ind_15m"]     = ind_15m
            d["bb_upper_15m"] = sf(ind_15m.get("BB.upper|15"))
            d["bb_lower_15m"] = sf(ind_15m.get("BB.lower|15"))
            d["atr_15m"]      = sf(ind_15m.get("ATR|15"))

        # Score parcial para ordenar antes de buscar klines
        for d in gate2_passed:
            sc_p, _, _ = calculate_score(d, fg_value=fg.get("value", 50))
            d["_partial_score"] = sc_p

        gate2_passed.sort(key=lambda x: x["_partial_score"], reverse=True)
        gate2_passed = [d for d in gate2_passed if d["_partial_score"] >= 0]

        # -------------------------------------------------------------------
        # ETAPA 4: Klines para TOP N (análise completa)
        # -------------------------------------------------------------------
        top_full  = gate2_passed[:KLINE_TOP_N]
        top_light = gate2_passed[KLINE_TOP_N:KLINE_TOP_N_LIGHT]

        results      = []   # análise completa (alertas elegíveis)
        observacoes  = []   # análise leve (em observação)

        if top_full:
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

                if not k15m: continue

                sc, reasons, bd = calculate_score(
                    d,
                    candles_15m=k15m,
                    candles_1h=k1h,
                    candles_4h=k4h,
                    fg_value=fg.get("value", 50),
                )
                d["score"]    = sc
                d["reasons"]  = reasons
                d["breakdown"] = bd

                trade = calc_trade_params(d["price"], d.get("atr_15m", 0))
                if trade:
                    d["trade"] = trade
                    results.append(d)

        # Análise leve (sem klines) — seção Observações
        for d in top_light:
            sc, reasons, bd = calculate_score(d, fg_value=fg.get("value", 50))
            d["score"]     = sc
            d["reasons"]   = reasons
            d["breakdown"] = bd
            trade = calc_trade_params(d["price"], d.get("atr_15m", 0))
            if trade:
                d["trade"] = trade
                observacoes.append(d)

        # -------------------------------------------------------------------
        # ETAPA 5: Contexto e Relatório
        # -------------------------------------------------------------------
        results.sort(key=lambda x: x["score"], reverse=True)
        observacoes.sort(key=lambda x: x["score"], reverse=True)

        btc_4h_val = tv_4h.get("BTCUSDT", {}).get("Recommend.All|240")  # [v4.3 FIX] símbolo completo
        btc_4h_str = recommendation_from_value(btc_4h_val)
        ctx        = analyze_market_context(fg, btc_4h_str)

        ts          = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")
        risco_usd   = BANKROLL * RISCO_POR_TRADE_PCT / 100
        perda_max   = BANKROLL * MAX_PERDA_DIARIA_PCT / 100
        pnl_dia     = state.get("pnl_dia", 0.0)
        n_abertos   = len(state.get("trades_abertos", []))

        report  = f"🎯 SETUP ATIRADOR v4.3 — {ts}\n"
        report += f"{'='*58}\n"
        report += f"📊 CONTEXTO DE MERCADO: {ctx['verdict']}\n"
        report += f"   Fear & Greed: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        report += f"   Threshold alerta: {ctx['threshold']} pts | Risk Score: {ctx['risk_score']}\n"
        report += f"{'='*58}\n\n"

        report += f"💼 GESTÃO DE RISCO\n"
        report += f"   Banca: ${BANKROLL:.2f} | Risco/trade: ${risco_usd:.2f} | Perda máx/dia: ${perda_max:.2f}\n"
        report += f"   P&L hoje: ${pnl_dia:+.2f} | Trades abertos: {n_abertos}/{MAX_TRADES_ABERTOS}\n"
        if not pode_operar:
            report += f"   🛑 NOVAS ENTRADAS BLOQUEADAS: {motivo_risco}\n"
        else:
            report += f"   ✅ Pode operar — {MAX_TRADES_ABERTOS - n_abertos} slot(s) disponível(is)\n"
        report += f"\n"

        report += f"🔍 PIPELINE\n"
        report += f"   Universo: {total_items} tokens | Inst. filter: {len(perpetuals[:TOP_N])}\n"
        report += f"   Gate 4H (não SELL): {len(gate1_passed)} | Gate 1H (BUY+): {len(gate2_passed)}\n"
        report += f"   Análise completa: {len(top_full)} | Análise leve: {len(top_light)}\n\n"

        # --- Alertas Fortes ---
        alertas = [r for r in results if r["score"] >= ctx["threshold"]]

        if ctx["threshold"] == 99:
            report += "🛑 BOT DESLIGADO — Mercado desfavorável para LONGs.\n"
        elif not alertas:
            report += f"ℹ️  Nenhum alerta forte (score ≥ {ctx['threshold']}) no momento.\n"
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
                report += f"   Alav: {t['alavancagem']}x | Risco: ${risco_usd:.2f}\n"
                report += f"   SL:  ${t['sl']:.4f}  (-{t['sl_distance_pct']:.2f}%)\n"
                report += f"   TP1: ${t['tp1']:.4f} (+{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
                report += f"   TP2: ${t['tp2']:.4f} (+{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
                report += f"   TP3: ${t['tp3']:.4f} (+{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
                report += f"   Trailing: ativar no TP1 → SL para breakeven +0.5%\n\n"

        # --- Oportunidades em Formação (análise completa, score abaixo do threshold) ---
        oportunidades = [r for r in results if 10 <= r["score"] < ctx["threshold"]]
        if oportunidades:
            report += f"\n📈 OPORTUNIDADES EM FORMAÇÃO ({len(oportunidades)}) — Score 10–{ctx['threshold']-1}:\n"
            for r in oportunidades[:5]:
                report += f"   ▶ {r['base_coin']} | {r['score']}/26 | {', '.join(r['reasons'][:2])}\n"

        # --- Em Observação (análise leve — sem klines) ---
        obs_relevantes = [o for o in observacoes if o["score"] >= 8]
        if obs_relevantes:
            report += f"\n👁️  EM OBSERVAÇÃO — análise leve, sem klines ({len(obs_relevantes)} tokens):\n"
            report += f"   ⚠️  Scores parciais — não são alertas confirmados.\n"
            for o in obs_relevantes[:5]:
                report += f"   · {o['base_coin']} | Score parcial: {o['score']}/26 | {o['summary_4h']} 4H\n"

        report += f"\n{'-'*58}\n"
        report += f"⏱️  Execução: {time.time()-t_start:.1f}s | Analisados: {total_items} tokens\n"
        report += f"📁 Estado diário: {STATE_FILE}\n"

        output_path = "/tmp/ultimo_scan_atirador_v43.txt"
        with open(output_path, "w") as f: f.write(report)

        print(report)
        print(f"✅ Scan concluído. Relatório salvo em {output_path}")
        return report


def main():
    asyncio.run(run_scan_async())

if __name__ == "__main__":
    main()
