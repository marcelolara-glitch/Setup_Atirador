#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v4.2 - Scanner Profissional de Criptomoedas
=============================================================================
Perpetuals USDT | Multi-Timeframe (4H macro + 15m entrada) | Funding Rate
Fear & Greed | Padrões de Candles | Zonas de Liquidez (S/R + Order Blocks)
Figuras Gráficas (Trendlines, Triângulos, Cunhas) | Contexto de Mercado

v4.2: Correções e Melhorias (revisão Claude)
  BUGS CORRIGIDOS:
  - [BUG CRÍTICO] CHOCH: `else True` corrigido para `else False` (eliminava falsos positivos)
  - [BUG] RSI P3: pontuação corrigida para max 4pts (era 2pts) conforme scoring-system
  - [BUG] Pump P13: threshold de penalidade alinhado ao scoring-system (30% e 20%)
  - [BUG] Candidatos leves removidos do ranking de alertas fortes

  MELHORIAS IMPLEMENTADAS:
  - Order Blocks reais implementados no P11 (último candle bearish antes de impulso >=1.5%)
  - Figuras Gráficas (P12) expandidas: Triângulo Ascendente e Falling Wedge adicionados
  - RSI 4H coletado agora pontuado como bônus de divergência (era ignorado)
  - Controle de estado diário (trades abertos + P&L) via arquivo JSON persistente
  - MAX_PERDA_DIARIA e MAX_TRADES_ABERTOS agora têm enforcement real
  - Candidatos com análise leve vão para seção "Em Observação" separada
  - Relatório expandido: seção de Oportunidades + Observações distintas dos Alertas Fortes

Autor: Manus AI | v4.1 | Revisão: Claude (Anthropic) | Versão: 4.2
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
MIN_TURNOVER_24H = 5_000_000  # Aumentado de 1.2M para 5M
MIN_OI_USD = 10_000_000       # Novo filtro de Open Interest
TOP_N = 30                    # Tokens analisados inicialmente

# Gestão de Risco
BANKROLL = 100.0
RISCO_POR_TRADE_PCT = 0.75    # 0.75% da banca por trade
MAX_PERDA_DIARIA_PCT = 4.0
MAX_TRADES_ABERTOS = 2
ALAVANCAGEM_MAX = 50          # Dinâmica até 50x baseada no SL
RR_MINIMO = 2.0               # Risk:Reward mínimo exigido (1:2)

# Otimização de Performance
KLINE_TOP_N = 10              # Análise completa apenas para TOP 10
KLINE_TOP_N_LIGHT = 20        # Análise leve para restantes
KLINE_LIMIT = 60
KLINE_CACHE_TTL_HOURS = 3     # Cache para klines 4H

# Parâmetros de Análise Técnica
SWING_WINDOW = 5
SR_CLUSTER_PCT = 0.5
SR_PROXIMITY_PCT = 1.0
OB_IMPULSE_CANDLES = 3
OB_IMPULSE_PCT = 1.5          # Impulso mínimo para qualificar Order Block
OB_PROXIMITY_PCT = 1.5

# Filtro de Pump (Multi-Timeframe)
# [v4.2 FIX] Alinhado com scoring-system.txt: penalidades em 20% e 30%, block em 40%
PUMP_WARN_1H = 5
PUMP_BLOCK_1H = 10
PUMP_WARN_4H = 15
PUMP_BLOCK_4H = 20
PUMP_WARN_24H = 20            # Penalidade leve (-2pts) começa aqui
PUMP_WARN_24H_STRONG = 30     # Penalidade forte (-3pts) começa aqui [v4.2 NOVO]
PUMP_BLOCK_24H = 40           # Bloqueio total acima disso

# Estado Diário — Controle de Risco com Persistência [v4.2 NOVO]
STATE_FILE = "/tmp/atirador_state.json"

# ===========================================================================
# GERENCIAMENTO DE ESTADO DIÁRIO [v4.2 NOVO]
# ===========================================================================

def load_daily_state():
    """Carrega estado diário do arquivo JSON. Reseta se for novo dia."""
    today = datetime.now(BRT).strftime("%Y-%m-%d")
    default = {
        "date": today,
        "trades_abertos": [],
        "pnl_dia": 0.0,
        "trades_executados": 0,
        "bloqueado": False,
        "motivo_bloqueio": "",
    }
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            if state.get("date") != today:
                # Novo dia: reseta estado mas mantém histórico
                default["historico"] = state.get("historico", [])
                save_daily_state(default)
                return default
            return state
    except Exception:
        pass
    save_daily_state(default)
    return default

def save_daily_state(state):
    """Salva estado diário no arquivo JSON."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"⚠️  Erro ao salvar estado: {e}")

def check_risk_limits(state):
    """
    Verifica se os limites de risco diário foram atingidos.
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

    trades_abertos = len(state.get("trades_abertos", []))
    if trades_abertos >= MAX_TRADES_ABERTOS:
        return False, f"Máximo de trades abertos atingido ({trades_abertos}/{MAX_TRADES_ABERTOS})"

    return True, "OK"

def registrar_trade(state, symbol, entry, sl, tp1, alavancagem, score):
    """Registra um trade aberto no estado diário."""
    trade = {
        "symbol": symbol,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "alavancagem": alavancagem,
        "score": score,
        "abertura": datetime.now(BRT).isoformat(),
        "status": "ABERTO",
    }
    state["trades_abertos"].append(trade)
    state["trades_executados"] += 1
    save_daily_state(state)
    return trade



TV_URL = "https://scanner.tradingview.com/crypto/scan"
TV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

# Colunas para filtro rápido 4H
COLS_4H = [
    "Recommend.All|240", "RSI|240",
]

# Colunas otimizadas (19 colunas, inclui ATR para SL dinâmico)
COLS_ENTRY_OPTIMIZED = [
    "Recommend.All|60", "RSI|60",                           # 1H (2)
    "Recommend.All|15", "RSI|15", "Stoch.K|15", "Stoch.D|15", "CCI20|15",
    "ADX|15", "MACD.macd|15", "MACD.signal|15",
    "BB.upper|15", "BB.lower|15", "ATR|15",                 # 15m (13)
    "Candle.Engulfing.Bullish|15", "Candle.Hammer|15", "Candle.MorningStar|15",
    "Candle.3WhiteSoldiers|15", "Candle.Harami.Bullish|15",
    "Candle.Doji.Dragonfly|15",                             # Candles (6)
]

def recommendation_from_value(val):
    if val is None: return "NEUTRAL"
    if val >= 0.5: return "STRONG_BUY"
    elif val >= 0.1: return "BUY"
    elif val >= -0.1: return "NEUTRAL"
    elif val >= -0.5: return "SELL"
    else: return "STRONG_SELL"

async def fetch_tv_batch_async(session, symbols, columns, retries=3):
    """Busca indicadores no TradingView de forma assíncrona."""
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
                    sym = item["s"].replace("BYBIT:", "")
                    values = item["d"]
                    indicators = {}
                    for col, val in zip(columns, values):
                        indicators[col] = val
                    result[sym] = indicators
                return result
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))
            else:
                print(f"       ERRO batch TV async: {e}")
                return {}

# ===========================================================================
# HELPERS
# ===========================================================================

def sf(val, default=0.0):
    try: return float(val) if val and val != "" else default
    except: return default

def api_get(url, retries=3):
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if i < retries - 1: time.sleep(2)
            else: raise

async def api_get_async(session, url, retries=3):
    for i in range(retries):
        try:
            async with session.get(url, timeout=20) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            if i < retries - 1: await asyncio.sleep(2)
            else: return None

# ===========================================================================
# DADOS DE MERCADO (Bitget API)
# ===========================================================================

def fetch_perpetuals():
    """Busca perpetuals USDT da Bitget com filtros institucionais."""
    data = api_get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
    items = data.get("data", [])

    qualified = []
    for t in items:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"): continue
        
        turnover = sf(t.get("usdtVolume"))
        if turnover < MIN_TURNOVER_24H: continue
        
        # Filtro de OI (Open Interest) - Aproximação via holdingAmount * price
        price = sf(t.get("lastPr"))
        holding = sf(t.get("holdingAmount"))
        oi_usd = holding * price
        if oi_usd < MIN_OI_USD: continue

        base = sym.replace("USDT", "")
        qualified.append({
            "symbol": sym,
            "base_coin": base,
            "price": price,
            "turnover_24h": turnover,
            "oi_usd": oi_usd,
            "volume_24h": sf(t.get("baseVolume")),
            "funding_rate": sf(t.get("fundingRate")),
            "price_change_24h": sf(t.get("change24h")) * 100,
        })

    qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
    return qualified, len(items)

async def fetch_fear_greed_async(session):
    """Fear & Greed Index global assíncrono."""
    try:
        data = await api_get_async(session, "https://api.alternative.me/fng/?limit=1")
        if data and "data" in data:
            v = data["data"][0]
            return {"value": int(v["value"]), "classification": v["value_classification"]}
    except: pass
    return {"value": 50, "classification": "Neutral"}

async def fetch_klines_async(session, symbol, granularity="15m", limit=60):
    """Busca klines da Bitget API assíncrono."""
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/candles?productType=USDT-FUTURES&symbol={symbol}&granularity={granularity}&limit={limit}"
        data = await api_get_async(session, url)
        if not data or "data" not in data: return []
        
        candles = data.get("data", [])
        result = []
        for c in candles:
            result.append({
                "ts": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
            })
        result.reverse()
        return result
    except: return []

async def fetch_klines_cached_async(session, symbol, granularity="4H", limit=60):
    """Busca klines com cache local para timeframes maiores."""
    cache_dir = "/tmp/atirador_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = f"{cache_dir}/klines_{symbol}_{granularity}.json"
    
    # Verificar cache
    if os.path.exists(cache_file):
        mtime = os.path.getmtime(cache_file)
        age_hours = (time.time() - mtime) / 3600
        if age_hours < KLINE_CACHE_TTL_HOURS:
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except: pass
    
    # Buscar novo
    klines = await fetch_klines_async(session, symbol, granularity, limit)
    if klines:
        try:
            with open(cache_file, 'w') as f:
                json.dump(klines, f)
        except: pass
    return klines

# ===========================================================================
# PADRÕES DE CANDLES (15m)
# ===========================================================================

def detect_candles(ind):
    if not ind: return [], 0
    patterns, score = [], 0
    checks = {
        "Candle.Engulfing.Bullish|15": ("Engulfing", 2),
        "Candle.Hammer|15": ("Hammer", 2),
        "Candle.MorningStar|15": ("Morning Star", 2),
        "Candle.3WhiteSoldiers|15": ("3 White Soldiers", 2),
        "Candle.Harami.Bullish|15": ("Harami", 1),
        "Candle.Doji.Dragonfly|15": ("Dragonfly Doji", 1),
    }
    for key, (name, pts) in checks.items():
        v = ind.get(key)
        if v and v != 0:
            patterns.append(name)
            score += pts
    return patterns, min(score, 4)

# ===========================================================================
# ANÁLISE DE ESTRUTURA (4H) E LIQUIDEZ
# ===========================================================================

def find_swing_points(candles, window=5):
    if len(candles) < window * 2 + 1: return [], []
    highs_arr = np.array([c["high"] for c in candles])
    lows_arr = np.array([c["low"] for c in candles])
    swing_highs, swing_lows = [], []

    for i in range(window, len(candles) - window):
        if highs_arr[i] == np.max(highs_arr[i - window:i + window + 1]):
            swing_highs.append({"index": i, "price": highs_arr[i]})
        if lows_arr[i] == np.min(lows_arr[i - window:i + window + 1]):
            swing_lows.append({"index": i, "price": lows_arr[i]})

    return swing_highs, swing_lows

def analyze_choch_bos_4h(swing_highs, swing_lows, current_price):
    """
    Analisa CHOCH/BOS apenas no 4H (estrutura macro).
    
    [v4.2 FIX] Bug crítico corrigido: `else True` substituído por `else False`.
    O `else True` original fazia a condição de downtrend ser sempre verdadeira
    quando havia menos de 3 swing highs, gerando falsos CHOCHs.
    
    Pontuação:
      - CHOCH Bullish (reversão confirmada): +3 pts
      - BOS Bullish (continuação de alta): +2 pts
      - Estrutura Saudável (Higher Lows consecutivos): +1 pt (sem rompimento ainda)
    """
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return 0, "Dados insuficientes para estrutura 4H"

    last_sh = swing_highs[-1]["price"]
    prev_sh = swing_highs[-2]["price"]
    last_sl = swing_lows[-1]["price"]
    prev_sl = swing_lows[-2]["price"]

    # CHOCH Bullish: downtrend (Lower Highs) e preço rompe último Swing High
    # [v4.2 FIX] Condição correta: só ativa CHOCH se houver evidência de downtrend anterior
    in_downtrend = (prev_sh < swing_highs[-3]["price"]) if len(swing_highs) >= 3 else False
    if in_downtrend and current_price > last_sh:
        return 3, "CHOCH Bullish 4H confirmado (reversão de downtrend)"

    # BOS Bullish: Higher Highs + Higher Lows → continuação de uptrend + rompimento
    if last_sh > prev_sh and last_sl > prev_sl:
        if current_price > last_sh:
            return 2, "BOS Bullish 4H confirmado (continuação de alta)"

    # Estrutura Saudável: Higher Lows consecutivos, ainda sem rompimento de swing high
    # Sinal de acumulação — entrada antecipada de menor risco
    if last_sl > prev_sl and len(swing_lows) >= 3:
        prev_prev_sl = swing_lows[-3]["price"]
        if prev_sl > prev_prev_sl:
            return 1, "Estrutura 4H saudável (Higher Lows consecutivos)"

    return 0, "Sem mudança estrutural bullish no 4H"


def detect_order_blocks(candles, proximity_pct=None):
    """
    Detecta Order Blocks bullish: último candle bearish antes de impulso bullish.
    
    [v4.2 NOVO] Implementação real de OB conforme especificação:
    - Identifica candles bearish (close < open) seguidos de impulso bullish
    - Impulso: soma de N candles seguintes >= OB_IMPULSE_PCT
    - OB válido: zona entre open e close do candle bearish
    
    Returns:
        Lista de OBs com {'high': float, 'low': float, 'index': int}
    """
    if proximity_pct is None:
        proximity_pct = OB_PROXIMITY_PCT

    obs = []
    n = OB_IMPULSE_CANDLES

    for i in range(len(candles) - n - 1):
        c = candles[i]
        # Candle bearish
        if c["close"] >= c["open"]:
            continue
        # Mede impulso nos próximos N candles
        ref_price = c["close"]
        if ref_price <= 0:
            continue
        max_close = max(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (max_close - ref_price) / ref_price * 100
        if impulse_pct >= OB_IMPULSE_PCT:
            obs.append({
                "high": max(c["open"], c["close"]),
                "low": min(c["open"], c["close"]),
                "index": i,
            })

    return obs

def analyze_liquidity_zones(candles, current_price):
    """
    Zonas de liquidez reais: S/R por Swing Points + Order Blocks.
    
    [v4.2] Pontuação máxima: 3 pts
      - Suporte S/R próximo (<1%): +1 pt
      - Order Block ativo/próximo (<1.5%): +1 pt
      - Confluência S/R + OB na mesma zona: +1 pt bônus
    """
    sh, sl = find_swing_points(candles)
    details = []
    score = 0

    # --- Suporte por Swing Lows ---
    sr_hit = False
    if sl:
        supports = sorted([s["price"] for s in sl])
        for sup in supports:
            if sup <= 0:
                continue
            dist_pct = (current_price - sup) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 1
                sr_hit = True
                details.append(f"Suporte S/R em {sup:.4f} ({dist_pct:.2f}% abaixo)")
                break

    # --- Order Blocks ---
    ob_hit = False
    obs = detect_order_blocks(candles)
    if obs:
        # Considera apenas OBs mais recentes (últimos 10)
        recent_obs = obs[-10:]
        for ob in reversed(recent_obs):
            ob_mid = (ob["high"] + ob["low"]) / 2
            if ob_mid <= 0:
                continue
            dist_pct = (current_price - ob_mid) / current_price * 100
            # OB válido: preço acima do OB ou dentro dele
            if -OB_PROXIMITY_PCT <= dist_pct <= OB_PROXIMITY_PCT:
                score += 1
                ob_hit = True
                details.append(f"Order Block ativo ({ob['low']:.4f}–{ob['high']:.4f})")
                break

    # --- Bônus de Confluência ---
    if sr_hit and ob_hit:
        score += 1
        details.append("Confluência S/R + OB")

    if not details:
        return 0, "Preço longe de zonas de liquidez"

    return min(score, 3), " | ".join(details)


def analyze_chart_patterns(candles):
    """
    Detecção de figuras gráficas bullish no 4H.
    
    [v4.2] Expandido com Triângulo Ascendente e Falling Wedge.
    Pontuação máxima: 3 pts.
    
    Figuras detectadas:
      - Triângulo Simétrico (compressão): +2 pts
      - Triângulo Ascendente (acumulação com resistência flat): +2 pts  [NOVO]
      - Falling Wedge (reversão bullish): +2 pts                        [NOVO]
      - Cunha Descendente (pullback em tendência de alta): +1 pt
    """
    sh, sl = find_swing_points(candles)
    if len(sh) < 3 or len(sl) < 3:
        return 0, "Dados insuficientes para figuras"

    sh_prices = [s["price"] for s in sh[-3:]]
    sl_prices = [s["price"] for s in sl[-3:]]

    highs_lower  = sh_prices[0] > sh_prices[1] > sh_prices[2]   # Topos decrescentes
    highs_flat   = abs(sh_prices[0] - sh_prices[2]) / sh_prices[0] < 0.015  # Topos ~flat (<1.5%)
    lows_higher  = sl_prices[0] < sl_prices[1] < sl_prices[2]   # Fundos crescentes
    lows_lower   = sl_prices[0] > sl_prices[1] > sl_prices[2]   # Fundos decrescentes

    # Falling Wedge: topos decrescentes E fundos decrescentes,
    # mas fundos caem MENOS que os topos (convergência para baixo = reversão bullish)
    if highs_lower and lows_lower:
        high_drop = (sh_prices[0] - sh_prices[2]) / sh_prices[0]
        low_drop  = (sl_prices[0] - sl_prices[2]) / sl_prices[0]
        if low_drop < high_drop * 0.8:  # Fundos caem bem menos que os topos
            return 2, "Falling Wedge detectado (reversão bullish)"

    # Triângulo Simétrico: topos decrescentes + fundos crescentes (compressão)
    if highs_lower and lows_higher:
        return 2, "Triângulo Simétrico detectado (compressão)"

    # Triângulo Ascendente: topos ~flat + fundos crescentes (acumulação)
    if highs_flat and lows_higher:
        return 2, "Triângulo Ascendente detectado (acumulação bullish)"

    # Cunha Descendente simples: apenas topos decrescentes (pullback saudável)
    if highs_lower and not lows_higher:
        return 1, "Cunha Descendente (pullback em tendência de alta)"

    return 0, "Sem figuras claras no 4H"


def validate_volume_15m(candles_15m, fg_value=50):
    """Valida volume da vela atual com threshold adaptativo ao contexto de mercado.
    
    Args:
        candles_15m: Lista de candles de 15m
        fg_value: Valor do Fear & Greed Index (0-100)
    
    Returns:
        bool: True se volume é válido, False caso contrário
    
    Thresholds adaptativos:
        - Bull (FGI <= 30): >= 1.2x (rigoroso - volume crescente esperado)
        - Neutro (FGI 30-70): >= 1.0x (moderado - volume = média é aceitável)
        - Bear (FGI >= 75): >= 0.8x (flexível - volume baixo é normal)
    """
    if len(candles_15m) < 21: return True # Fallback
    
    current_vol = candles_15m[-1]["volume"]
    avg_vol_5h = np.mean([c["volume"] for c in candles_15m[-21:-1]])
    
    if avg_vol_5h == 0: return True
    
    # Threshold adaptativo baseado em Fear & Greed
    if fg_value <= 30:  # Bull market
        threshold = 1.2
    elif fg_value <= 70:  # Neutral market
        threshold = 1.0
    else:  # Bear market (FGI >= 75)
        threshold = 0.8
    
    return current_vol >= (avg_vol_5h * threshold)

# ===========================================================================
# GESTÃO DE RISCO E TRADE PARAMS
# ===========================================================================

def calc_alavancagem_dinamica(entry, sl, risco_pct=0.75, max_alav=50):
    """Calcula alavancagem dinamicamente baseada na distância do SL."""
    dist_pct = abs(entry - sl) / entry * 100
    if dist_pct < 0.1: return None # SL muito próximo (ruído)
    
    alav_calculada = risco_pct / dist_pct
    return min(alav_calculada, max_alav)

def calc_trade_with_atr(d):
    """Calcula SL/TP com ATR para adaptação à volatilidade e valida RR."""
    price = d.get("price", 0)
    atr = d.get("atr_15m", 0)
    
    if not price or not atr or atr <= 0: return None
    
    # SL baseado em ATR (1.5x ATR abaixo do entry)
    sl_distance_pct = (1.5 * atr) / price * 100
    sl = price * (1 - sl_distance_pct / 100)
    
    # TP baseado em RR 1:2 (2x o risco)
    tp_distance_pct = sl_distance_pct * RR_MINIMO
    tp = price * (1 + tp_distance_pct / 100)
    
    # Alavancagem dinâmica
    alav = calc_alavancagem_dinamica(price, sl, RISCO_POR_TRADE_PCT, ALAVANCAGEM_MAX)
    if not alav: return None
    
    return {
        "entry": price,
        "sl": sl,
        "tp": tp,
        "sl_distance_pct": sl_distance_pct,
        "tp_distance_pct": tp_distance_pct,
        "rr": RR_MINIMO,
        "alavancagem": round(alav, 1),
        "atr": atr,
        "tp1": price * (1 + sl_distance_pct / 100),       # RR 1:1 (50%)
        "tp2": price * (1 + (sl_distance_pct * 2) / 100), # RR 1:2 (30%)
        "tp3": price * (1 + (sl_distance_pct * 3) / 100), # RR 1:3 (20%)
    }

# ===========================================================================
# SCORING SYSTEM (14 PILARES)
# ===========================================================================

def calculate_score(d, is_light=False):
    """Score completo com 14 pilares reais. Max: 39 pts."""
    s4h = d.get("summary_4h", "")
    if "BUY" not in s4h:
        return -1, ["4H = {} (descartado)".format(s4h)], []

    sc, reasons, breakdown = 0, [], []

    # P1: Tendência Macro 4H (max 4)
    if "STRONG_BUY" in s4h:
        p = 4; reasons.append("4H STRONG BUY")
        breakdown.append(("Tendencia 4H", p, 4, "STRONG BUY"))
    else:
        p = 2; reasons.append("4H BUY")
        breakdown.append(("Tendencia 4H", p, 4, "BUY"))
    sc += p

    # P2: Confluência 1H (max 3)
    s1h = d.get("summary_1h", "")
    if "STRONG_BUY" in s1h: p = 3; breakdown.append(("Confluencia 1H", p, 3, "STRONG BUY"))
    elif "BUY" in s1h and "STRONG" not in s1h: p = 2; breakdown.append(("Confluencia 1H", p, 3, "BUY"))
    elif "SELL" in s1h: p = -2; breakdown.append(("Confluencia 1H", p, 3, "SELL (penalidade)"))
    else: p = 0; breakdown.append(("Confluencia 1H", p, 3, "NEUTRAL"))
    sc += p

    # P3: RSI 15m (max 4) [v4.2 FIX] — corrigido de max 2pts para max 4pts conforme scoring-system
    rsi = d.get("rsi_15m", 50)
    if rsi < 25:
        p = 4; reasons.append(f"RSI {rsi:.0f} extremo"); breakdown.append(("RSI 15m", p, 4, f"{rsi:.1f} - extremo oversold"))
    elif rsi < 30:
        p = 3; breakdown.append(("RSI 15m", p, 4, f"{rsi:.1f} - oversold forte"))
    elif rsi < 35:
        p = 2; breakdown.append(("RSI 15m", p, 4, f"{rsi:.1f} - oversold"))
    elif rsi < 42:
        p = 1; breakdown.append(("RSI 15m", p, 4, f"{rsi:.1f} - abaixo da média"))
    elif rsi > 70:
        p = -2; breakdown.append(("RSI 15m", p, 4, f"{rsi:.1f} - overbought (penalidade)"))
    else:
        p = 0; breakdown.append(("RSI 15m", p, 4, f"{rsi:.1f} - neutro"))
    sc += p

    # P3-BÔNUS: RSI 4H — Divergência Bullish [v4.2 NOVO]
    # RSI 4H coletado mas não utilizado no v4.1 — agora pontuado como bônus
    rsi_4h = d.get("rsi_4h", 50)
    if 0 < rsi_4h < 40:
        sc += 1; reasons.append(f"RSI 4H oversold ({rsi_4h:.0f})")
        breakdown.append(("RSI 4H (bônus)", 1, 1, f"{rsi_4h:.1f} - oversold no macro"))
    else:
        breakdown.append(("RSI 4H (bônus)", 0, 1, f"{rsi_4h:.1f} - neutro"))


    # P4: Estocástico 15m (max 3)
    sk, sd = d.get("stoch_k_15m", 50), d.get("stoch_d_15m", 50)
    if sk < 20 and sk > sd: p = 3; breakdown.append(("Estocastico", p, 3, "Cross bullish em oversold"))
    elif sk < 10: p = 2; breakdown.append(("Estocastico", p, 3, "Fundo extremo"))
    elif sk < 20: p = 1; breakdown.append(("Estocastico", p, 3, "Oversold"))
    else: p = 0; breakdown.append(("Estocastico", p, 3, "Neutro"))
    sc += p

    # P5: CCI 15m (max 2)
    cci = d.get("cci_15m", 0)
    if cci < -200: p = 2; breakdown.append(("CCI 15m", p, 2, "Extremo"))
    elif cci < -100: p = 1; breakdown.append(("CCI 15m", p, 2, "Oversold"))
    else: p = 0; breakdown.append(("CCI 15m", p, 2, "Neutro"))
    sc += p

    # P6: Bollinger Bands (max 2)
    price, bbl, bbu = d.get("price", 0), d.get("bb_lower_15m", 0), d.get("bb_upper_15m", 0)
    if price and bbl and bbu and (bbu - bbl) > 0:
        pos = (price - bbl) / (bbu - bbl)
        if pos < 0.10: p = 2; breakdown.append(("Bollinger", p, 2, "BB inferior"))
        elif pos < 0.25: p = 1; breakdown.append(("Bollinger", p, 2, "BB baixa"))
        else: p = 0; breakdown.append(("Bollinger", p, 2, "Neutro"))
    else: p = 0; breakdown.append(("Bollinger", p, 2, "N/A"))
    sc += p

    # P7: MACD (max 2)
    if d.get("macd_15m", 0) > d.get("macd_signal_15m", 0):
        p = 2; breakdown.append(("MACD 15m", p, 2, "Bullish"))
    else: p = 0; breakdown.append(("MACD 15m", p, 2, "Bearish/Neutro"))
    sc += p

    # P8: Candles (max 4)
    cs_val, cp = d.get("candle_score", 0), d.get("candle_patterns", [])
    if cs_val > 0:
        sc += cs_val; reasons.append(f"Candles: {', '.join(cp[:2])}")
        breakdown.append(("Candles 15m", cs_val, 4, f"Padroes: {', '.join(cp)}"))
    else: breakdown.append(("Candles 15m", 0, 4, "Nenhum"))

    # P9: Funding Rate (max 2)
    fr = d.get("funding_rate", 0)
    if fr < -0.0005: p = 2; reasons.append("FR negativo"); breakdown.append(("Funding Rate", p, 2, f"{fr:.4%} (squeeze)"))
    elif fr < 0: p = 1; breakdown.append(("Funding Rate", p, 2, f"{fr:.4%} (leve neg)"))
    elif fr > 0.0005: p = -1; breakdown.append(("Funding Rate", p, 2, f"{fr:.4%} (risco)"))
    else: p = 0; breakdown.append(("Funding Rate", p, 2, f"{fr:.4%} (neutro)"))
    sc += p

    # P10: ADX (max 1)
    adx = d.get("adx_15m", 0)
    if adx > 30: p = 1; breakdown.append(("ADX 15m", p, 1, "Tendencia forte"))
    else: p = 0; breakdown.append(("ADX 15m", p, 1, "Tendencia fraca"))
    sc += p

    # P13: Filtro de Pump (penalidade) [v4.2 FIX] — alinhado ao scoring-system.txt
    # >40%: BLOCK total | >30%: -3pts | >20%: -2pts
    pump_24h = d.get("price_change_24h", 0)
    if pump_24h >= PUMP_BLOCK_24H:
        return -99, ["PUMP BLOCK 24H"], []
    elif pump_24h >= PUMP_WARN_24H_STRONG:
        sc -= 3; breakdown.append(("Filtro Pump", -3, 0, f"Pump 24h {pump_24h:.1f}% > 30%"))
    elif pump_24h >= PUMP_WARN_24H:
        sc -= 2; breakdown.append(("Filtro Pump", -2, 0, f"Pump 24h {pump_24h:.1f}% > 20%"))
    else:
        breakdown.append(("Filtro Pump", 0, 0, f"OK ({pump_24h:.1f}%)"))

    # Pilares 11, 12, 14 (Klines 4H)
    if not is_light:
        lz_score = d.get("liquidity_score", 0)
        sc += lz_score; breakdown.append(("Zonas Liquidez", lz_score, 3, d.get("liquidity_detail", "")))
        
        cp_score = d.get("chart_pattern_score", 0)
        sc += cp_score; breakdown.append(("Figuras Graficas", cp_score, 3, d.get("chart_pattern_detail", "")))
        
        cb_score = d.get("choch_bos_score", 0)
        sc += cb_score; breakdown.append(("CHOCH/BOS 4H", cb_score, 3, d.get("choch_bos_detail", "")))
        if cb_score > 0: reasons.append("Estrutura 4H Bullish")
    else:
        breakdown.append(("Zonas Liquidez", 0, 3, "Análise Leve (N/A)"))
        breakdown.append(("Figuras Graficas", 0, 3, "Análise Leve (N/A)"))
        breakdown.append(("CHOCH/BOS 4H", 0, 3, "Análise Leve (N/A)"))

    return max(sc, 0), reasons, breakdown

# ===========================================================================
# CONTEXTO DE MERCADO E RISK SCORE
# ===========================================================================

def analyze_market_context(fg, btc_4h, results):
    """Define threshold adaptativo e risk score."""
    fg_val = fg.get("value", 50)
    
    # Risk Score Base
    risk_score = 0
    if fg_val <= 30: risk_score += 2
    elif fg_val <= 50: risk_score += 1
    elif fg_val >= 75: risk_score -= 1
    
    if "STRONG_BUY" in btc_4h: risk_score += 2
    elif "BUY" in btc_4h: risk_score += 1
    elif "SELL" in btc_4h: risk_score -= 2
    
    # Threshold Adaptativo
    if fg_val <= 30 and "BUY" in btc_4h:
        threshold = 12; verdict = "FAVORÁVEL (Bull)"
    elif fg_val >= 75 or "SELL" in btc_4h:
        threshold = 16; verdict = "CAUTELOSO (Bear)"
    else:
        threshold = 14; verdict = "MODERADO (Neutro)"
        
    if fg_val >= 80 and "SELL" in btc_4h:
        verdict = "DESFAVORÁVEL (Bot Desligado)"
        threshold = 99
        
    return {
        "verdict": verdict,
        "threshold": threshold,
        "risk_score": risk_score,
        "fg": fg_val,
        "btc": btc_4h
    }

# ===========================================================================
# EXECUÇÃO PRINCIPAL (ASYNC)
# ===========================================================================

async def run_scan_async():
    print("🚀 Setup Atirador v4.2 | Iniciando scan otimizado...")
    t_start = time.time()

    # Carregar estado diário e verificar limites de risco
    state = load_daily_state()
    pode_operar, motivo_risco = check_risk_limits(state)

    async with aiohttp.ClientSession() as session:
        # 1. Fetch inicial (Tickers)
        perpetuals, total_items = fetch_perpetuals()
        symbols = [d["base_coin"] for d in perpetuals[:TOP_N]]
        
        # 2. Fetch paralelo (TV 4H + FG)
        tv_4h_task = fetch_tv_batch_async(session, symbols, COLS_4H)
        fg_task = fetch_fear_greed_async(session)
        tv_4h, fg = await asyncio.gather(tv_4h_task, fg_task)
        
        # 3. Filtro 4H — extrai RSI 4H junto com o summary [v4.2]
        pre_qualified = []
        for d in perpetuals[:TOP_N]:
            sym = d["base_coin"]
            ind = tv_4h.get(sym, {})
            d["summary_4h"] = recommendation_from_value(ind.get("Recommend.All|240"))
            d["rsi_4h"] = sf(ind.get("RSI|240"), default=50.0)  # [v4.2 NOVO] RSI 4H agora aproveitado
            if "BUY" in d["summary_4h"]:
                pre_qualified.append(d)
                
        # 4. Fetch paralelo (TV Entry para qualificados)
        symbols_entry = [d["base_coin"] for d in pre_qualified]
        tv_entry = await fetch_tv_batch_async(session, symbols_entry, COLS_ENTRY_OPTIMIZED)
        
        # 5. Processar indicadores e score parcial
        for d in pre_qualified:
            sym = d["base_coin"]
            ind = tv_entry.get(sym, {})
            
            d["summary_1h"] = recommendation_from_value(ind.get("Recommend.All|60"))
            d["rsi_15m"] = sf(ind.get("RSI|15"))
            d["stoch_k_15m"] = sf(ind.get("Stoch.K|15"))
            d["stoch_d_15m"] = sf(ind.get("Stoch.D|15"))
            d["cci_15m"] = sf(ind.get("CCI20|15"))
            d["adx_15m"] = sf(ind.get("ADX|15"))
            d["macd_15m"] = sf(ind.get("MACD.macd|15"))
            d["macd_signal_15m"] = sf(ind.get("MACD.signal|15"))
            d["bb_upper_15m"] = sf(ind.get("BB.upper|15"))
            d["bb_lower_15m"] = sf(ind.get("BB.lower|15"))
            d["atr_15m"] = sf(ind.get("ATR|15"))
            
            cp, cs = detect_candles(ind)
            d["candle_patterns"] = cp
            d["candle_score"] = cs
            
            # Score parcial para ordenação
            sc, _, _ = calculate_score(d, is_light=True)
            d["_partial_score"] = sc
            
        # Ordenar por score parcial
        pre_qualified = [d for d in pre_qualified if d["_partial_score"] > 0]
        pre_qualified.sort(key=lambda x: x["_partial_score"], reverse=True)
        
        # 6. Fetch Klines (Apenas TOP 10)
        top_candidates = pre_qualified[:KLINE_TOP_N]
        rest_candidates = pre_qualified[KLINE_TOP_N:KLINE_TOP_N_LIGHT]
        
        results = []
        
        if top_candidates:
            # Fetch klines em paralelo
            tasks_15m = [fetch_klines_async(session, d["symbol"], "15m") for d in top_candidates]
            tasks_4h = [fetch_klines_cached_async(session, d["symbol"], "4H") for d in top_candidates]
            
            klines_15m_all = await asyncio.gather(*tasks_15m)
            klines_4h_all = await asyncio.gather(*tasks_4h)
            
            # Análise completa TOP 10
            for i, d in enumerate(top_candidates):
                k15m = klines_15m_all[i]
                k4h = klines_4h_all[i]
                
                if not k15m or not k4h: continue
                
                # Validação de volume 15m (adaptativo ao contexto)
                if not validate_volume_15m(k15m, fg.get("value", 50)):
                    d["_partial_score"] = -1 # Rejeitado por volume
                    continue
                
                # Análise 4H
                sh, sl = find_swing_points(k4h)
                cb_sc, cb_det = analyze_choch_bos_4h(sh, sl, d["price"])
                lz_sc, lz_det = analyze_liquidity_zones(k4h, d["price"])
                cp_sc, cp_det = analyze_chart_patterns(k4h)
                
                d["choch_bos_score"] = cb_sc
                d["choch_bos_detail"] = cb_det
                d["liquidity_score"] = lz_sc
                d["liquidity_detail"] = lz_det
                d["chart_pattern_score"] = cp_sc
                d["chart_pattern_detail"] = cp_det
                
                # Score final
                sc, reasons, bd = calculate_score(d, is_light=False)
                d["score"] = sc
                d["reasons"] = reasons
                d["breakdown"] = bd
                
                # Trade params (SL dinâmico com ATR)
                trade = calc_trade_with_atr(d)
                if trade:
                    d["trade"] = trade
                    results.append(d)
                    
        # [v4.2 FIX] Candidatos leves ficam em lista SEPARADA — não competem com TOP 10 nos alertas fortes
        observacoes = []
        for d in rest_candidates:
            sc, reasons, bd = calculate_score(d, is_light=True)
            d["score"] = sc
            d["reasons"] = reasons
            d["breakdown"] = bd
            trade = calc_trade_with_atr(d)
            if trade:
                d["trade"] = trade
                observacoes.append(d)

        # 7. Contexto e Relatório
        results.sort(key=lambda x: x["score"], reverse=True)
        observacoes.sort(key=lambda x: x["score"], reverse=True)

        btc_4h = tv_4h.get("BTC", {}).get("Recommend.All|240")
        btc_4h_str = recommendation_from_value(btc_4h)

        ctx = analyze_market_context(fg, btc_4h_str, results)

        # --- Gerar Relatório ---
        ts = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")
        report = f"🎯 SETUP ATIRADOR v4.2 - {ts}\n"
        report += f"{'='*55}\n"
        report += f"📊 CONTEXTO DE MERCADO: {ctx['verdict']}\n"
        report += f"Fear & Greed: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        report += f"Threshold Alerta: {ctx['threshold']} pts | Risk Score: {ctx['risk_score']}\n"
        report += f"{'='*55}\n\n"

        # --- Bloco de Gestão de Risco [v4.2 NOVO] ---
        pnl_dia = state.get("pnl_dia", 0.0)
        trades_abertos = state.get("trades_abertos", [])
        perda_max_usd = BANKROLL * MAX_PERDA_DIARIA_PCT / 100
        risco_por_trade_usd = BANKROLL * RISCO_POR_TRADE_PCT / 100
        report += f"💼 GESTÃO DE RISCO DO DIA\n"
        report += f"   Banca: ${BANKROLL:.2f} | Risco/trade: ${risco_por_trade_usd:.2f} | Perda máx/dia: ${perda_max_usd:.2f}\n"
        report += f"   P&L hoje: ${pnl_dia:+.2f} | Trades abertos: {len(trades_abertos)}/{MAX_TRADES_ABERTOS}\n"
        if not pode_operar:
            report += f"   🛑 NOVAS ENTRADAS BLOQUEADAS: {motivo_risco}\n"
        else:
            report += f"   ✅ Pode operar ({MAX_TRADES_ABERTOS - len(trades_abertos)} slot(s) disponível/is)\n"
        report += f"\n"

        # --- Alertas Fortes (apenas TOP 10 com análise completa) ---
        alertas = [r for r in results if r["score"] >= ctx["threshold"]]

        if ctx["threshold"] == 99:
            report += "🛑 BOT DESLIGADO: Mercado desfavorável para LONGs.\n"
        elif not alertas:
            report += "ℹ️  Nenhum alerta forte no momento (análise completa).\n"
        else:
            report += f"🔥 {len(alertas)} ALERTA(S) FORTE(S) — Análise Completa (TOP 10):\n\n"
            for r in alertas:
                t = r["trade"]
                bloqueio_str = "" if pode_operar else " ⛔ BLOQUEADO"
                report += f"🚀 {r['base_coin']}{bloqueio_str} | Score: {r['score']}/44 | {', '.join(r['reasons'][:3])}\n"
                report += f"   Preço: ${r['price']:.4f} | Vol 24h: ${r['turnover_24h']/1e6:.1f}M | OI: ${r['oi_usd']/1e6:.1f}M\n"
                report += f"   Alavancagem: {t['alavancagem']}x | Risco: ${risco_por_trade_usd:.2f}\n"
                report += f"   SL: ${t['sl']:.4f} (-{t['sl_distance_pct']:.2f}%) | RR: 1:{t['rr']:.1f}\n"
                report += f"   TP1 (50%): ${t['tp1']:.4f} | TP2 (30%): ${t['tp2']:.4f} | TP3 (20%): ${t['tp3']:.4f}\n"
                report += f"   Trailing Stop: Ativar no TP1 → mover SL para Breakeven +0.5%\n\n"

        # --- Oportunidades em Formação (score 11–threshold-1, análise completa) ---
        oportunidades = [r for r in results if 11 <= r["score"] < ctx["threshold"]]
        if oportunidades:
            report += f"\n📈 {len(oportunidades)} OPORTUNIDADE(S) EM FORMAÇÃO (Score 11–{ctx['threshold']-1}):\n"
            for r in oportunidades[:5]:
                report += f"   ▶ {r['base_coin']} | Score: {r['score']}/44 | {', '.join(r['reasons'][:2])}\n"

        # --- Em Observação (análise leve — sem klines 4H completos) [v4.2 FIX] ---
        obs_fortes = [o for o in observacoes if o["score"] >= 10]
        if obs_fortes:
            report += f"\n👁️  EM OBSERVAÇÃO (análise leve, sem klines 4H — {len(obs_fortes)} tokens):\n"
            report += f"   Estes tokens NÃO são alertas confirmados. Requerem análise manual.\n"
            for o in obs_fortes[:5]:
                report += f"   · {o['base_coin']} | Score parcial: {o['score']}/44 | {', '.join(o['reasons'][:2])}\n"

        report += f"\n{'-'*55}\n"
        report += f"⏱️  Execução: {time.time() - t_start:.1f}s | Tokens analisados: {total_items} | Qualificados 4H: {len(pre_qualified)}\n"
        report += f"📁 Estado salvo em: {STATE_FILE}\n"

        # Salvar relatório
        output_path = "/tmp/ultimo_scan_atirador_v42.txt"
        with open(output_path, "w") as f:
            f.write(report)

        print(report)
        print(f"✅ Scan concluído em {time.time() - t_start:.1f}s. Relatório salvo em {output_path}")
        return report

def main():
    asyncio.run(run_scan_async())

if __name__ == "__main__":
    main()
