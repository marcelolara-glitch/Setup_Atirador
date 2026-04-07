#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v7.0.0 - Scanner Profissional de Criptomoedas "Price Action"
=============================================================================
Arquitetura Price Action com 3 Camadas Sequenciais:

  CAMADA 0 — Universo e liquidez (OKX → Gate.io → Bitget, filtros vol+OI)
  CAMADA 1 — Gate de Direção 4H (STRICT: BUY/STRONG_BUY ou SELL/STRONG_SELL)
  CAMADA 2 — Zona de Decisão (OB + S/R em 4H e 1H)
  CAMADA 3 — Confirmação Price Action 15m (Check A + B + C)

Sinal nasce exclusivamente da ação do preço no 15m dentro de uma zona.
Silêncio é posição válida — zero sinais é comportamento correto.

=============================================================================
HISTÓRICO DE VERSÕES
=============================================================================

v7.0.0 (03/04/2026):
  - Eliminação completa do sistema de score por pilares (P1-P9).
  - Gate 4H STRICT: NEUTRAL rejeitado para ambas as direções.
    LONG: apenas BUY ou STRONG_BUY. SHORT: apenas SELL ou STRONG_SELL.
  - Gate 1H: mantido como CONTEXTO (não filtra). NEUTRAL no 1H pode indicar
    pullback ideal para scalp — token avança independentemente.
  - Zona de Decisão: 6 níveis de qualidade (MAXIMA/ALTA_OB4H/ALTA_OB1H/
    MEDIA/BASE/NENHUMA). Sem zona = DROP imediato (sem sinal).
  - Check A (Rejeição 15m): candle fechado bearish/bullish com sombra ≥40%.
    Obrigatório. Falha → RADAR (sem notificação).
  - Check B (Estrutura Direcional 15m): ≥5/7 lower highs ou higher lows
    nos últimos 8 candles fechados. Obrigatório para CALL.
  - Check C (Força e Pressão): 4 sub-checks (BB, Volume, CVD, OI) = 0-4 pts.
    Threshold: 2 pts para zonas ALTA, 3 pts para MEDIA/BASE.
  - P7 Pump/Dump removido: Gate 4H + zona + Check A já filtram tokens anômalos.
  - Novo banco de dados: scan_log_v7.db (scan_log.db da v6.x preservado).
  - Novo logger: logger_v7.py com schema check_a/b/c por token.
  - Mensagens Telegram redesenhadas: CALL, QUASE, Heartbeat v7.

v6.6.6 (02/04/2026): observabilidade de venue, tracking de exchange por token.
v6.6.5 (01/04/2026): logging estruturado (logger.py + journal.py).
v6.6.2 (24/03/2026): arquitetura 3 tipos de mensagem Telegram.
v6.0   (21/03/2026): arquitetura bidirecional LONG+SHORT.

Autor: Manus AI | v4.1→v7.0.0 (revisão Claude/Anthropic)
=============================================================================
"""
import html
import json
import math
import requests
import time
import os
import sys
import logging
import numpy as np
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))
VERSION = "7.0.0"

# [v6.6.5] Módulos de observabilidade
try:
    from logger_v7 import RoundLoggerV7
    _OBSERVABILITY_V7 = True
except ImportError:
    _OBSERVABILITY_V7 = False

try:
    from journal import TradeJournal
    _OBSERVABILITY_JOURNAL = True
except ImportError:
    _OBSERVABILITY_JOURNAL = False

_round_logger_v7 = None
_trade_journal   = None

# ===========================================================================
# SISTEMA DE LOG CENTRALIZADO
# ===========================================================================
LOG_DIR = os.path.expanduser("~/Setup_Atirador/logs")

def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    ts_brt  = datetime.now(BRT)
    ts_str  = ts_brt.strftime("%Y%m%d_%H%M")
    logfile = f"{LOG_DIR}/atirador_LOG_{ts_str}.log"

    logger = logging.getLogger("atirador")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    def brt_converter(timestamp, *args):
        return datetime.fromtimestamp(timestamp, BRT).timetuple()

    fmt_file = logging.Formatter(
        "%(asctime)s BRT [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fmt_file.converter = brt_converter

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt_file)

    fmt_term = logging.Formatter("%(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt_term)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"📋 Log iniciado: {logfile}")
    return logger, logfile, ts_str

LOG      = logging.getLogger("atirador")   # fallback (sem handlers até setup_logger())
LOG_FILE = None
TS_SCAN  = None

def log_section(title):
    LOG.info(f"\n{'─'*55}")
    LOG.info(f"  {title}")
    LOG.info(f"{'─'*55}")


# ===========================================================================
# MÓDULO TELEGRAM
# ===========================================================================

def _tg_send(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id"                  : TELEGRAM_CHAT_ID,
            "text"                     : text,
            "parse_mode"               : "HTML",
            "disable_web_page_preview" : True,
        }, timeout=8)
        if resp.status_code != 200:
            LOG.warning(f"  ⚠️  Telegram: HTTP {resp.status_code} — {resp.text[:80]}")
            return False
        return True
    except Exception as e:
        LOG.warning(f"  ⚠️  Telegram: falha ao enviar — {type(e).__name__}: {e}")
        return False


def _fmt_price(p: float) -> str:
    if p == 0:
        return "0"
    mag = -math.floor(math.log10(abs(p)))
    decimals = max(4, mag + 2)
    return f"{p:.{decimals}f}"


def _tv_links(symbol: str) -> tuple:
    """Retorna (link_15m, link_4h) para TradingView."""
    tv_sym = f"OKX:{symbol}.P"
    link_15m = f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval=15"
    link_4h  = f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval=240"
    return link_15m, link_4h


def _chk(passed: bool) -> str:
    return "✅" if passed else "❌"


def _tg_call_v7(r: dict, direction: str, fg_val: int) -> str:
    """Mensagem de CALL v7.0.0."""
    sym  = r["base_coin"]
    zona_q = r.get("zona_qualidade", "?")
    zona_d = r.get("zona_descricao", "")
    s4h    = r.get("summary_4h", "?")
    s1h    = r.get("summary_1h", "?")
    ca_razao = r.get("check_a_razao", "")
    cb_razao = r.get("check_b_razao", "")
    cc_total = r.get("check_c_total", 0)
    det      = r.get("check_c_detalhes", {})

    ico = "🔴" if direction == "SHORT" else "🟢"

    t = r.get("trade") or r.get("trade_short")
    link_15m, link_4h = _tv_links(r["symbol"])

    niveis = ""
    if t:
        entry = _fmt_price(t["entry"])
        sl    = _fmt_price(t["sl"])
        tp1   = _fmt_price(t["tp1"])
        tp2   = _fmt_price(t["tp2"])
        tp3   = _fmt_price(t["tp3"])
        slpct = t["sl_distance_pct"]
        if direction == "SHORT":
            sign_sl, sign_tp = "+", "-"
        else:
            sign_sl, sign_tp = "-", "+"
        niveis = (
            f"\n📈 Níveis\n"
            f"   Entrada : {entry}\n"
            f"   SL      : {sl} ({sign_sl}{slpct:.2f}%)\n"
            f"   TP1     : {tp1} ({sign_tp}{slpct:.2f}%)\n"
            f"   TP2     : {tp2} ({sign_tp}{slpct*2:.2f}%)\n"
            f"   TP3     : {tp3} ({sign_tp}{slpct*3:.2f}%)"
        )

    msg = (
        f"{ico} {direction} CALL — {sym}USDT\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Zona: {html.escape(zona_d)} [{zona_q}]\n"
        f"\n⚡ Confirmação 15m\n"
        f"   A — Rejeição: {_chk(True)} {html.escape(ca_razao)}\n"
        f"   B — Estrutura: {_chk(True)} {html.escape(cb_razao)}\n"
        f"   C — Força: {cc_total}/4\n"
        f"      BB: {det.get('c1_bb', '—')}\n"
        f"      Volume: {det.get('c2_vol', '—')}\n"
        f"      CVD: {det.get('c3_cvd', '—')}\n"
        f"      OI: {det.get('c4_oi', '—')}\n"
        f"\n📊 Contexto\n"
        f"   4H: {s4h} | 1H: {s1h} | FGI: {fg_val}"
        f"{niveis}\n"
        f"\n🔗 <a href=\"{link_15m}\">15m</a> · <a href=\"{link_4h}\">4H</a>"
    )
    vi = r.get("venue_info", {})
    if vi.get("mixed"):
        msg += f"\n⚠️ Venue mista (klines: {vi.get('kline_venue')} | TV: {vi.get('tv_venue')})"
    return msg


def _tg_quase_v7(r: dict, direction: str, fg_val: int) -> str:
    """Mensagem de QUASE v7.0.0."""
    sym  = r["symbol"].replace("USDT", "")
    zona_q = r.get("zona_qualidade", "?")
    zona_d = r.get("zona_descricao", "")
    s4h    = r.get("rec_4h", "?")
    s1h    = r.get("rec_1h", "?")
    ca     = r.get("check_a_ok", False)
    ca_razao = r.get("check_a_reason", "")
    cb     = r.get("check_b_ok")
    cb_razao = r.get("check_b_reason", "não avaliado")
    cc_total = r.get("check_c_total", 0) or 0
    det      = r.get("check_c_det", {}) or {}

    ico = "🟡"
    link_15m, link_4h = _tv_links(r["symbol"])

    cb_line = f"   B — Estrutura: {_chk(cb)} {html.escape(cb_razao or '')}" if cb is not None else "   B — Estrutura: — (não avaliado — Check A falhou)"

    msg = (
        f"{ico} {direction} QUASE — {sym}USDT\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Zona: {html.escape(zona_d)} [{zona_q}]\n"
        f"\n⚡ Confirmação 15m\n"
        f"   A — Rejeição: {_chk(ca)} {html.escape(ca_razao)}\n"
        f"{cb_line}\n"
        f"   C — Força: {cc_total}/4\n"
        f"      BB: {det.get('c1_bb', '—')}\n"
        f"      Volume: {det.get('c2_vol', '—')}\n"
        f"      CVD: {det.get('c3_cvd', '—')}\n"
        f"      OI: {det.get('c4_oi', '—')}\n"
        f"\n📊 Contexto\n"
        f"   4H: {s4h} | 1H: {s1h} | FGI: {fg_val}\n"
        f"\n🔗 <a href=\"{link_15m}\">15m</a> · <a href=\"{link_4h}\">4H</a>"
    )
    vi = r.get("venue_info", {})
    if vi.get("mixed"):
        msg += f"\n⚠️ Venue mista (klines: {vi.get('kline_venue')} | TV: {vi.get('tv_venue')})"
    return msg


def _tg_heartbeat_v7(n_univ: int, n_gate_short: int, n_gate_long: int,
                      n_zona_short: int, n_zona_long: int,
                      n_calls: int, n_quase: int,
                      fg_val: int, btc_4h: str,
                      elapsed: float, exchange: str) -> str:
    return (
        f"💓 <b>Atirador v{VERSION}</b> — rodada concluída\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 Universo  : {n_univ} tokens\n"
        f"🔽 Gate 4H   : {n_gate_short} SHORT | {n_gate_long} LONG\n"
        f"🎯 Em zona   : {n_zona_short} SHORT  | {n_zona_long} LONG\n"
        f"⚡ Com sinal : {n_calls} CALL   | {n_quase} QUASE\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 FGI: {fg_val} | BTC 4H: {html.escape(btc_4h)}\n"
        f"⏱ Exec: {elapsed:.0f}s | Exchange: {exchange}"
    )


def tg_notify_v7(results: list, fg_val: int,
                  n_univ: int, n_gate_short: int, n_gate_long: int,
                  n_zona_short: int, n_zona_long: int,
                  elapsed: float, exchange: str, btc_4h: str):
    """Envia heartbeat → QUASEs → CALLs."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        LOG.debug("  📵  Telegram não configurado — notificações desativadas")
        return

    calls  = [r for r in results if r.get("status") == "CALL"]
    quases = [r for r in results if r.get("status") == "QUASE"]
    n_calls = len(calls)
    n_quase = len(quases)
    n_env   = 0

    # 1. Heartbeat
    if TELEGRAM_HEARTBEAT:
        hb = _tg_heartbeat_v7(n_univ, n_gate_short, n_gate_long,
                               n_zona_short, n_zona_long,
                               n_calls, n_quase,
                               fg_val, btc_4h, elapsed, exchange)
        if _tg_send(hb):
            n_env += 1
            LOG.info("  📲  Telegram heartbeat: enviado ✅")

    # 2. QUASEs
    for r in quases:
        msg = _tg_quase_v7(r, r["direction"], fg_val)
        if _tg_send(msg):
            n_env += 1
            LOG.info(f"  📲  Telegram QUASE {r['direction']} {r['symbol']}: enviado ✅")

    # 3. CALLs
    for r in calls:
        msg = _tg_call_v7(r, r["direction"], fg_val)
        if _tg_send(msg):
            n_env += 1
            LOG.info(f"  📲  Telegram CALL {r['direction']} {r['base_coin']}: enviado ✅")

    total = (1 if TELEGRAM_HEARTBEAT else 0) + n_quase + n_calls
    LOG.info(f"  📲  Telegram: {n_env}/{total} mensagens enviadas")


# ===========================================================================
# CONFIGURAÇÃO
# ===========================================================================
MIN_TURNOVER_24H = 2_000_000
MIN_OI_USD       = 5_000_000

BANKROLL              = 100.0
RISCO_POR_TRADE_USD   = 5.00
MARGEM_MAX_POR_TRADE  = 35.0
ALAVANCAGEM_MIN       = 2.0
ALAVANCAGEM_MAX       = 50.0
RR_MINIMO             = 2.0

ALAV_POR_SCORE = {
    (14, 15): 5.0,
    (16, 17): 10.0,
    (18, 19): 15.0,
    (20, 21): 20.0,
    (22, 23): 30.0,
    (24, 25): 40.0,
    (26, 28): 50.0,
}

def get_alav_max_por_score(score: int) -> float:
    for (sc_min, sc_max), alav_max in ALAV_POR_SCORE.items():
        if sc_min <= score <= sc_max:
            return alav_max
    return ALAVANCAGEM_MIN

KLINE_TOP_N        = 20
KLINE_TOP_N_LIGHT  = 30
KLINE_LIMIT        = 60
KLINE_CACHE_TTL_H  = 1

SWING_WINDOW        = 5
SR_PROXIMITY_PCT    = 2.5
OB_IMPULSE_N        = 3
OB_IMPULSE_PCT      = 1.5
OB_PROXIMITY_PCT    = 2.5
ZONE_PROXIMITY_PCT  = 1.5   # v7: proximidade máxima para qualificação de zona (%)

SCORE_HISTORY_MAX_ROUNDS = 48
SCORE_HISTORY_TTL_H      = 25

STATE_FILE = os.path.expanduser("~/Setup_Atirador/states/atirador_state.json")

# ===========================================================================
# CONFIGURAÇÃO TELEGRAM
# ===========================================================================
TELEGRAM_CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".atirador_telegram_config.json")
TELEGRAM_CONFIG_FILE_LEGACY = "/tmp/atirador_telegram_config.json"


def _load_telegram_config():
    for cfg_path, is_legacy in [
        (TELEGRAM_CONFIG_FILE, False),
        (TELEGRAM_CONFIG_FILE_LEGACY, True),
    ]:
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, "r") as f:
                    cfg = json.load(f)
                token   = cfg.get("telegram_token", "")
                chat_id = cfg.get("telegram_chat_id", "")
                if token and chat_id:
                    if is_legacy:
                        _migrate_telegram_config(token, chat_id, cfg_path)
                    return token, chat_id
            except Exception:
                pass
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    return token, chat_id


def _migrate_telegram_config(token, chat_id, source_path):
    try:
        cfg = {
            "telegram_token"  : token,
            "telegram_chat_id": chat_id,
            "migrated_from"   : source_path,
            "migrated_at"     : datetime.now(timezone.utc).isoformat(),
        }
        with open(TELEGRAM_CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception:
        pass


TELEGRAM_TOKEN, TELEGRAM_CHAT_ID = _load_telegram_config()
TELEGRAM_HEARTBEAT = True


def save_telegram_config(token, chat_id):
    config = {
        "telegram_token"  : token,
        "telegram_chat_id": chat_id,
        "telegram_enabled": bool(token and chat_id),
        "created_at"      : datetime.now(timezone.utc).isoformat(),
        "last_updated"    : datetime.now(timezone.utc).isoformat(),
    }
    for path in [TELEGRAM_CONFIG_FILE, TELEGRAM_CONFIG_FILE_LEGACY]:
        try:
            os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
            with open(path, "w") as f:
                json.dump(config, f, indent=2)
        except Exception:
            pass


# ===========================================================================
# GERENCIAMENTO DE ESTADO DIÁRIO
# ===========================================================================

def load_daily_state():
    default = {"score_history": {}, "oi_history": {}}
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            if "score_history" not in state: state["score_history"] = {}
            if "oi_history"    not in state: state["oi_history"]    = {}
            return state
    except Exception:
        pass
    save_daily_state(default)
    return default


def save_daily_state(state):
    canonical = {
        "date":          state.get("date", datetime.now(BRT).strftime("%Y-%m-%d")),
        "score_history": state.get("score_history", {}),
        "oi_history":    state.get("oi_history", {}),
    }
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(canonical, f, indent=2)
    except Exception as e:
        LOG.warning(f"⚠️  Erro ao salvar estado diário: {e}")


def update_score_history(state: dict, results: list, ts: str):
    """Atualiza histórico de OI por token (necessário para C4)."""
    sh = state.setdefault("score_history", {})
    oh = state.setdefault("oi_history", {})
    seen = {}
    for r in results:
        sym = r.get("symbol", "")
        if sym:
            seen.setdefault(sym, {"oi": r.get("oi_usd", 0)})
    for sym, vals in seen.items():
        oi_entry = {"ts": ts, "oi": vals["oi"]}
        oi_hist  = oh.get(sym, [])
        oi_hist.append(oi_entry)
        oh[sym] = oi_hist[-SCORE_HISTORY_MAX_ROUNDS:]


def cleanup_score_history(state: dict):
    sh    = state.get("score_history", {})
    oh    = state.get("oi_history", {})
    agora = datetime.now(BRT)
    for sym in list(oh.keys()):
        hist = oh[sym]
        if not hist:
            del oh[sym]; sh.pop(sym, None); continue
        try:
            last_ts = datetime.fromisoformat(hist[-1]["ts"])
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=BRT)
            age_h = (agora - last_ts).total_seconds() / 3600
            if age_h > SCORE_HISTORY_TTL_H:
                del oh[sym]; sh.pop(sym, None)
        except Exception:
            del oh[sym]; sh.pop(sym, None)


def get_score_trend(state: dict, symbol: str, direction: str = "LONG") -> str:
    field     = "long" if direction == "LONG" else "short"
    opp_field = "short" if direction == "LONG" else "long"
    hist = state.get("score_history", {}).get(symbol, [])
    if len(hist) <= 1:
        return "🆕"
    last = hist[-1]
    if last.get(opp_field, 0) > last.get(field, 0):
        return "🔄"
    delta = hist[-1].get(field, 0) - hist[-2].get(field, 0)
    if delta >= 3:    return "↑↑"
    elif delta >= 1:  return "↑"
    elif delta <= -3: return "↓↓"
    elif delta <= -1: return "↓"
    else:             return "→"


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

# Gate 4H: direção macro (STRICT em v7 — NEUTRAL rejeitado)
COLS_4H = [
    "Recommend.All|240",
    "RSI|240",
]

# 1H: contexto apenas (não filtra em v7)
COLS_1H = [
    "Recommend.All|60",
]

# 15m: BB + ATR para C1 e SL
COLS_15M_TECH = [
    "BB.upper|15", "BB.lower|15",
    "ATR|15",
]


def recommendation_from_value(val):
    if val is None:    return "NEUTRAL"
    if val >= 0.5:     return "STRONG_BUY"
    elif val >= 0.1:   return "BUY"
    elif val >= -0.1:  return "NEUTRAL"
    elif val >= -0.5:  return "SELL"
    else:              return "STRONG_SELL"


async def fetch_tv_batch_async(session, symbols, columns, retries=3):
    """Busca indicadores TradingView. Retorna (result, tv_venues)."""
    if not symbols: return {}, {}

    tickers_bybit = [f"BYBIT:{s}.P" for s in symbols]
    payload = {"symbols": {"tickers": tickers_bybit, "query": {"types": []}},
               "columns": columns}

    result    = {}
    tv_venues = {}
    for attempt in range(retries):
        try:
            t0 = time.time()
            async with session.post(TV_URL, json=payload,
                                    headers=TV_HEADERS, timeout=25) as resp:
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
                        async with session.post(TV_URL, json=payload_fb,
                                                headers=TV_HEADERS, timeout=15) as resp_fb:
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


# ===========================================================================
# HELPERS
# ===========================================================================

def sf(val, default=0.0):
    try: return float(val) if val is not None and val != "" else default
    except: return default


def _build_venue_info(kline_venue, tv_venue):
    if kline_venue is None or tv_venue is None:
        return {"kline_venue": kline_venue, "tv_venue": tv_venue,
                "mixed": False, "quality": "unknown"}
    if kline_venue == tv_venue or (kline_venue == "okx" and tv_venue == "bybit"):
        return {"kline_venue": kline_venue, "tv_venue": tv_venue,
                "mixed": False, "quality": "clean"}
    return {"kline_venue": kline_venue, "tv_venue": tv_venue,
            "mixed": True, "quality": "mixed"}


BITGET_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Accept":          "application/json",
    "User-Agent":      "Mozilla/5.0",
}


async def api_get_async(session, url, retries=3, headers=None):
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


def api_get(url, retries=3):
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20,
                                headers={"Accept-Encoding": "gzip, deflate"})
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            LOG.warning(f"  ⚠️  api_get tentativa {i+1}/{retries}: {e}")
            if i < retries - 1: time.sleep(2)
            else:
                LOG.error(f"  ❌  api_get falhou: {short_url}")
                raise


async def fetch_klines_async(session, symbol, granularity="15m", limit=60):
    """Busca klines com fallback OKX. Retorna (candles, venue)."""
    url_bitget = (f"https://api.bitget.com/api/v2/mix/market/candles"
                  f"?productType=USDT-FUTURES&symbol={symbol}"
                  f"&granularity={granularity}&limit={limit}")
    try:
        data = await api_get_async(session, url_bitget, headers=BITGET_HEADERS)
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
    url_okx    = (f"https://www.okx.com/api/v5/market/candles"
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


async def fetch_klines_cached_async(session, symbol, granularity="4H", limit=60):
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
# DADOS DE MERCADO — Hierarquia OKX → Gate.io → Bitget
# ===========================================================================

TICKER_TIMEOUT = 8
DATA_SOURCE          = "desconhecida"
DATA_SOURCE_ATTEMPTS = []

_GATE_MULTIPLIERS      = {}
_GATE_MULTIPLIERS_TS   = 0.0
_GATE_MULTIPLIERS_TTL  = 86400


def _log_source_attempt(fonte, url, status, elapsed, tokens_brutos, qualificados, motivo_falha=None):
    entrada = {"fonte": fonte, "url": url[:80], "status": status,
               "elapsed_s": round(elapsed, 2), "tokens_brutos": tokens_brutos,
               "qualificados": qualificados, "falha": motivo_falha}
    DATA_SOURCE_ATTEMPTS.append(entrada)
    if motivo_falha:
        LOG.warning(f"  ⛔  [{fonte}] FALHOU | {elapsed:.2f}s | {motivo_falha}")
    else:
        LOG.info(f"  ✅  [{fonte}] OK | {elapsed:.2f}s | {tokens_brutos} → {qualificados}")


def _fetch_gate_multipliers():
    global _GATE_MULTIPLIERS, _GATE_MULTIPLIERS_TS
    agora = time.time()
    if _GATE_MULTIPLIERS and (agora - _GATE_MULTIPLIERS_TS) < _GATE_MULTIPLIERS_TTL:
        return _GATE_MULTIPLIERS
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    t0  = time.time()
    try:
        resp = requests.get(url, timeout=TICKER_TIMEOUT,
                            headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "scanner/7.0"})
        if resp.status_code == 200:
            mults = {}
            for c in resp.json():
                sym  = c.get("name", "").replace("_", "")
                mult = sf(c.get("quanto_multiplier", 1.0))
                if mult <= 0: mult = 1.0
                if sym.endswith("USDT"):
                    mults[sym] = mult
            _GATE_MULTIPLIERS    = mults
            _GATE_MULTIPLIERS_TS = agora
            return mults
    except Exception as e:
        LOG.warning(f"  ⚠️  [Gate.io/contracts] {e}")
    return {}


def _parse_gateio_tickers(items, multipliers):
    qualified  = []
    rej_vol = rej_oi = 0
    for t in items:
        sym = t.get("contract", "").replace("_", "")
        if not sym.endswith("USDT"): continue
        turnover = sf(t.get("volume_24h_quote", 0))
        if turnover < MIN_TURNOVER_24H: rej_vol += 1; continue
        price      = sf(t.get("last", 0) or t.get("mark_price", 0))
        mark_price = sf(t.get("mark_price", 0) or price)
        if price <= 0: continue
        total_size  = sf(t.get("total_size", 0))
        mult        = multipliers.get(sym, 1.0)
        oi_usd      = total_size * mark_price * mult
        oi_estimado = False
        if oi_usd <= 0:
            oi_usd = turnover * 0.1; oi_estimado = True
        if oi_usd < MIN_OI_USD: rej_oi += 1; continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol": sym, "base_coin": base, "price": price,
            "turnover_24h": turnover, "oi_usd": oi_usd, "oi_estimado": oi_estimado,
            "volume_24h": turnover, "funding_rate": sf(t.get("funding_rate", 0)),
            "price_change_24h": sf(t.get("change_percentage", 0)),
        })
    return qualified, rej_vol, rej_oi


def _fetch_okx_funding_rates(symbols_okx: list) -> dict:
    fr_map = {}
    base_url = "https://www.okx.com/api/v5/public/funding-rate"
    headers  = {"Accept-Encoding": "gzip, deflate", "User-Agent": "scanner/7.0"}
    for sym_okx in symbols_okx:
        try:
            resp = requests.get(f"{base_url}?instId={sym_okx}", timeout=3, headers=headers)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    fr = sf(data[0].get("fundingRate", 0))
                    sym_internal = sym_okx.replace("-USDT-SWAP", "") + "USDT"
                    fr_map[sym_internal] = fr
        except Exception:
            pass
    return fr_map


def _fetch_okx_tickers_with_oi():
    try:
        tickers_resp = requests.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=TICKER_TIMEOUT, headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "scanner/7.0"}
        )
        tickers_resp.raise_for_status()
        tickers_data = tickers_resp.json().get("data", [])

        oi_resp = requests.get(
            "https://www.okx.com/api/v5/public/open-interest?instType=SWAP",
            timeout=TICKER_TIMEOUT, headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "scanner/7.0"}
        )
        oi_resp.raise_for_status()
        oi_data = oi_resp.json().get("data", [])
        oi_dict = {item["instId"]: item for item in oi_data}

        for ticker in tickers_data:
            inst_id = ticker.get("instId")
            if inst_id in oi_dict:
                ticker["oiUsd"]    = float(oi_dict[inst_id]["oiUsd"])
                ticker["oi_real"]  = True
            else:
                ticker["oiUsd"]   = 0
                ticker["oi_real"] = False

        swap_instids = [t.get("instId") for t in tickers_data
                        if t.get("instId", "").endswith("-USDT-SWAP")]
        fr_map = _fetch_okx_funding_rates(swap_instids)
        for ticker in tickers_data:
            inst_id = ticker.get("instId", "")
            sym_internal = inst_id.replace("-USDT-SWAP", "") + "USDT"
            if sym_internal in fr_map:
                ticker["fundingRate"] = fr_map[sym_internal]

        return tickers_data
    except Exception as e:
        LOG.error(f"  ❌ [OKX] Erro: {type(e).__name__}: {e}")
        return None


def _parse_okx_tickers(items):
    qualified = []
    rej_vol = rej_oi = 0
    for t in items:
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"): continue
        sym      = inst_id.replace("-USDT-SWAP", "") + "USDT"
        turnover = sf(t.get("volCcy24h", 0))
        if turnover < MIN_TURNOVER_24H: rej_vol += 1; continue
        price = sf(t.get("last", 0))
        if price <= 0: continue
        oi_usd      = sf(t.get("oiUsd", 0))
        oi_real     = t.get("oi_real", False)
        oi_estimado = False
        if oi_usd <= 0:
            oi_usd = turnover * 0.1; oi_estimado = True
        if oi_usd < MIN_OI_USD: rej_oi += 1; continue
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


def _parse_bitget_tickers(items):
    qualified = []
    rej_vol = rej_oi = 0
    for t in items:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"): continue
        turnover = sf(t.get("usdtVolume"))
        if turnover < MIN_TURNOVER_24H: rej_vol += 1; continue
        price   = sf(t.get("lastPr"))
        holding = sf(t.get("holdingAmount"))
        oi_usd  = holding * price
        if oi_usd < MIN_OI_USD: rej_oi += 1; continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol": sym, "base_coin": base, "price": price,
            "turnover_24h": turnover, "oi_usd": oi_usd, "oi_estimado": False,
            "volume_24h": sf(t.get("baseVolume")),
            "funding_rate": sf(t.get("fundingRate")),
            "price_change_24h": sf(t.get("change24h")) * 100,
        })
    return qualified, rej_vol, rej_oi


def _try_source(nome, url, parse_fn, extract_fn, timeout=None, parse_kwargs=None):
    t_used = timeout or TICKER_TIMEOUT
    LOG.info(f"  📡  [{nome}] Tentando: {url[:80]}...")
    t0 = time.time()
    try:
        resp    = requests.get(url, timeout=t_used,
                               headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "scanner/7.0"})
        elapsed = time.time() - t0
        status  = resp.status_code
        if status != 200:
            _log_source_attempt(nome, url, status, elapsed, 0, 0, f"HTTP {status}")
            return None
        data     = resp.json()
        items    = extract_fn(data)
        if not items:
            _log_source_attempt(nome, url, status, elapsed, 0, 0, "resposta vazia")
            return None
        kwargs   = parse_kwargs or {}
        qualified, rej_vol, rej_oi = parse_fn(items, **kwargs)
        if not qualified:
            _log_source_attempt(nome, url, status, elapsed, len(items), 0, "nenhum qualificado")
            return None
        _log_source_attempt(nome, url, status, elapsed, len(items), len(qualified))
        return qualified, len(items)
    except requests.exceptions.Timeout:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"Timeout {elapsed:.1f}s")
        return None
    except Exception as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"{type(e).__name__}: {str(e)[:80]}")
        return None


def fetch_perpetuals():
    """Busca perpetuals USDT — hierarquia OKX → Gate.io → Bitget."""
    global DATA_SOURCE, DATA_SOURCE_ATTEMPTS
    DATA_SOURCE_ATTEMPTS = []
    LOG.info("📡 [v7.0.0] Iniciando busca de tickers — hierarquia OKX → Gate.io → Bitget")

    tickers_with_oi = _fetch_okx_tickers_with_oi()
    if tickers_with_oi:
        qualified, rej_vol, rej_oi = _parse_okx_tickers(tickers_with_oi)
        if qualified:
            DATA_SOURCE = "OKX"
            qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
            LOG.info(f"  ✅  [OKX] {len(tickers_with_oi)} brutos → {len(qualified)} qualificados")
            return qualified, len(tickers_with_oi)

    multipliers = _fetch_gate_multipliers()
    resultado   = _try_source(
        nome="Gate.io",
        url="https://api.gateio.ws/api/v4/futures/usdt/tickers",
        parse_fn=_parse_gateio_tickers,
        extract_fn=lambda d: d if isinstance(d, list) else [],
        parse_kwargs={"multipliers": multipliers},
    )
    if resultado:
        DATA_SOURCE = "Gate.io"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        return qualified, total

    resultado = _try_source(
        nome="Bitget",
        url="https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
        parse_fn=_parse_bitget_tickers,
        extract_fn=lambda d: d.get("data", []),
        timeout=20,
    )
    if resultado:
        DATA_SOURCE = "Bitget"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        return qualified, total

    DATA_SOURCE = "NENHUMA"
    raise RuntimeError("Todas as fontes de tickers falharam. Scan abortado.")


async def fetch_fear_greed_async(session):
    try:
        data = await api_get_async(session, "https://api.alternative.me/fng/?limit=1")
        if data and "data" in data:
            v  = data["data"][0]
            fg = {"value": int(v["value"]), "classification": v["value_classification"]}
            LOG.info(f"  📊 Fear & Greed: {fg['value']} ({fg['classification']})")
            return fg
    except Exception as e:
        LOG.warning(f"  ⚠️  Fear & Greed falhou: {e}")
    return {"value": 50, "classification": "Neutral"}


async def _fetch_token_okx_async(session, symbol):
    base    = symbol.replace("USDT", "")
    inst_id = f"{base}-USDT-SWAP"
    hdrs    = {"User-Agent": "scanner/7.0", "Accept-Encoding": "gzip"}

    async def _get(url):
        try:
            async with session.get(url, headers=hdrs,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json(content_type=None)
        except Exception as exc:
            LOG.debug(f"    _fetch_token_okx: {url} → {exc}")
            return {}

    ticker_r, oi_r, fr_r = await asyncio.gather(
        _get(f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"),
        _get(f"https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId={inst_id}"),
        _get(f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"),
    )

    tickers = ticker_r.get("data", []) if isinstance(ticker_r, dict) else []
    if not tickers: return None
    t       = tickers[0]
    price   = sf(t.get("last", 0))
    if price <= 0: return None

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


# ===========================================================================
# TRAVA DE CANDLE FECHADO
# ===========================================================================
CANDLE_15M_SECONDS    = 900
CANDLE_CLOSED_GRACE_S = 60


def get_candle_lock_status() -> dict:
    now_ts              = time.time()
    seconds_in_period   = now_ts % CANDLE_15M_SECONDS
    seconds_since_close = seconds_in_period
    closed              = seconds_since_close >= CANDLE_CLOSED_GRACE_S
    use_prev            = not closed
    next_close          = CANDLE_15M_SECONDS - seconds_since_close
    ts_last = datetime.fromtimestamp(now_ts - seconds_since_close, BRT).strftime("%H:%M:%S BRT")
    return {
        "closed"       : closed,
        "use_prev"     : use_prev,
        "seconds_open" : seconds_since_close,
        "seconds_ago"  : seconds_since_close,
        "next_close"   : next_close,
        "ts_last_close": ts_last,
    }


def apply_candle_lock(candles_15m: list, lock: dict) -> list:
    if not candles_15m or len(candles_15m) < 2:
        return candles_15m
    if lock["use_prev"]:
        return candles_15m[:-1]
    return candles_15m


# ===========================================================================
# ANÁLISE TÉCNICA — UTILITÁRIOS
# ===========================================================================

def find_swing_points(candles, window=None):
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
    if (len(sh) < 3 or len(sl) < 3) and window > 3:
        sh_fb, sl_fb = _detect(candles, 3)
        if len(sh_fb) >= len(sh): sh = sh_fb
        if len(sl_fb) >= len(sl): sl = sl_fb
    return sh, sl


def detect_order_blocks(candles):
    """OBs bullish: último candle bearish antes de impulso ≥ OB_IMPULSE_PCT."""
    obs = []
    n = OB_IMPULSE_N
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] >= c["open"]: continue
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
    """OBs bearish: último candle bullish antes de impulso de queda ≥ OB_IMPULSE_PCT."""
    obs = []
    n = OB_IMPULSE_N
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] <= c["open"]: continue
        ref = c["close"]
        if ref <= 0: continue
        min_close   = min(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (ref - min_close) / ref * 100
        if impulse_pct >= OB_IMPULSE_PCT:
            obs.append({
                "high" : max(c["open"], c["close"]),
                "low"  : min(c["open"], c["close"]),
                "index": i,
            })
    return obs


def analyze_support_1h(candles_1h, current_price):
    """Detecta suporte (swing low + OB bullish) no 1H para LONG."""
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"
    sh, sl = find_swing_points(candles_1h)
    score, details = 0, []
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
                details.append(f"Order Block 1H ({ob['low']:.4f}-{ob['high']:.4f})")
                break
    if not details:
        return 0, "Preço longe de suportes no 1H"
    return min(score, 4), " | ".join(details)


def analyze_resistance_1h(candles_1h, current_price):
    """Detecta resistência (swing high + OB bearish) no 1H para SHORT."""
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"
    sh, sl = find_swing_points(candles_1h)
    score, details = 0, []
    if sh:
        for s in reversed(sh):
            dist_pct = (s["price"] - current_price) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 2
                details.append(f"Resistência 1H em {_fmt_price(s['price'])} ({dist_pct:.2f}% acima)")
                break
    obs = detect_order_blocks_bearish(candles_1h)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = abs(current_price - ob_mid) / current_price * 100
            if dist_pct <= OB_PROXIMITY_PCT:
                score += 2
                details.append(f"OB Bearish 1H ({ob['low']:.4f}-{ob['high']:.4f})")
                break
    if not details:
        return 0, "Preço longe de resistências no 1H"
    return min(score, 4), " | ".join(details)


def analyze_liquidity_zones_4h(candles_4h, current_price, direction="LONG"):
    """Detecta zonas de liquidez 4H (suportes/resistências + OBs)."""
    sh, sl = find_swing_points(candles_4h)
    score, details = 0, []
    if direction == "SHORT":
        sr_hit = False
        if sh:
            for s in reversed(sh):
                dist_pct = (s["price"] - current_price) / current_price * 100
                if 0 < dist_pct <= SR_PROXIMITY_PCT:
                    score += 1; sr_hit = True
                    details.append(f"Resistência 4H {_fmt_price(s['price'])} ({dist_pct:.2f}% acima)")
                    break
        ob_hit = False
        obs = detect_order_blocks_bearish(candles_4h)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = abs(current_price - ob_mid) / current_price * 100
                if dist_pct <= OB_PROXIMITY_PCT:
                    score += 1; ob_hit = True
                    details.append(f"OB Bearish 4H ({ob['low']:.4f}-{ob['high']:.4f})")
                    break
        if sr_hit and ob_hit:
            score += 1; details.append("Confluência Res+OB Bearish")
    else:
        sr_hit = False
        if sl:
            for s in reversed(sl):
                dist_pct = (current_price - s["price"]) / current_price * 100
                if 0 < dist_pct <= SR_PROXIMITY_PCT:
                    score += 1; sr_hit = True
                    details.append(f"Suporte 4H {s['price']:.4f} ({dist_pct:.2f}%)")
                    break
        ob_hit = False
        obs = detect_order_blocks(candles_4h)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = (current_price - ob_mid) / current_price * 100
                if -OB_PROXIMITY_PCT <= dist_pct <= OB_PROXIMITY_PCT:
                    score += 1; ob_hit = True
                    details.append(f"OB 4H ({ob['low']:.4f}-{ob['high']:.4f})")
                    break
        if sr_hit and ob_hit:
            score += 1; details.append("Confluência S/R+OB")
    if not details:
        label = "resistências" if direction == "SHORT" else "zonas de liquidez"
        return 0, f"Longe de {label} 4H"
    return min(score, 3), " | ".join(details)


# ===========================================================================
# IDENTIFICAÇÃO DE ZONA v7.0.0
# ===========================================================================

def identify_zona(candles_4h, candles_1h, current_price, direction) -> tuple:
    """
    Identifica a zona de decisão onde o preço se encontra.
    Retorna (zona_qualidade, zona_descricao).

    Hierarquia SHORT (bearish zones):
      MAXIMA   — preço dentro de OB Bearish 4H E dentro de ZONE_PROXIMITY_PCT% de resistência 4H
      ALTA_OB4H — preço dentro de OB Bearish 4H
      ALTA_OB1H — preço dentro de OB Bearish 1H
      MEDIA    — preço dentro de ZONE_PROXIMITY_PCT% acima de resistência 4H
      BASE     — preço dentro de ZONE_PROXIMITY_PCT% acima de resistência 1H
      NENHUMA  — fora de qualquer zona

    Hierarquia LONG (bullish zones) — espelho simétrico com suportes.
    """
    if not candles_4h or not candles_1h:
        return "NENHUMA", "Klines insuficientes"

    sh4, sl4 = find_swing_points(candles_4h)
    sh1, sl1 = find_swing_points(candles_1h)

    if direction == "SHORT":
        # Verifica OB Bearish 4H (preço dentro do corpo do OB)
        obs_4h_b  = detect_order_blocks_bearish(candles_4h)
        in_ob_4h  = False
        ob_4h_desc = ""
        for ob in reversed(obs_4h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_4h   = True
                ob_4h_desc = f"OB Bearish 4H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                break

        # Verifica resistência 4H (swing high dentro de ZONE_PROXIMITY_PCT% acima)
        near_res_4h  = False
        res_4h_price = 0.0
        if sh4:
            for s in reversed(sh4):
                if s["price"] >= current_price:
                    dist_pct = (s["price"] - current_price) / current_price * 100
                    if dist_pct <= ZONE_PROXIMITY_PCT:
                        near_res_4h  = True
                        res_4h_price = s["price"]
                        break

        # Verifica OB Bearish 1H
        obs_1h_b  = detect_order_blocks_bearish(candles_1h)
        in_ob_1h  = False
        ob_1h_desc = ""
        for ob in reversed(obs_1h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_1h   = True
                ob_1h_desc = f"OB Bearish 1H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                break

        # Verifica resistência 1H
        near_res_1h  = False
        res_1h_price = 0.0
        if sh1:
            for s in reversed(sh1):
                if s["price"] >= current_price:
                    dist_pct = (s["price"] - current_price) / current_price * 100
                    if dist_pct <= ZONE_PROXIMITY_PCT:
                        near_res_1h  = True
                        res_1h_price = s["price"]
                        break

        # Hierarquia
        if in_ob_4h and near_res_4h:
            return "MAXIMA", f"{ob_4h_desc} + Res 4H: {_fmt_price(res_4h_price)}"
        if in_ob_4h:
            return "ALTA_OB4H", ob_4h_desc
        if in_ob_1h:
            return "ALTA_OB1H", ob_1h_desc
        if near_res_4h:
            return "MEDIA", f"Resistência 4H: {_fmt_price(res_4h_price)}"
        if near_res_1h:
            return "BASE", f"Resistência 1H: {_fmt_price(res_1h_price)}"
        return "NENHUMA", "Fora de zona"

    else:  # LONG
        # OB Bullish 4H
        obs_4h_b  = detect_order_blocks(candles_4h)
        in_ob_4h  = False
        ob_4h_desc = ""
        for ob in reversed(obs_4h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_4h   = True
                ob_4h_desc = f"OB Bullish 4H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                break

        # Suporte 4H
        near_sup_4h  = False
        sup_4h_price = 0.0
        if sl4:
            for s in reversed(sl4):
                if s["price"] <= current_price:
                    dist_pct = (current_price - s["price"]) / current_price * 100
                    if dist_pct <= ZONE_PROXIMITY_PCT:
                        near_sup_4h  = True
                        sup_4h_price = s["price"]
                        break

        # OB Bullish 1H
        obs_1h_b  = detect_order_blocks(candles_1h)
        in_ob_1h  = False
        ob_1h_desc = ""
        for ob in reversed(obs_1h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_1h   = True
                ob_1h_desc = f"OB Bullish 1H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                break

        # Suporte 1H
        near_sup_1h  = False
        sup_1h_price = 0.0
        if sl1:
            for s in reversed(sl1):
                if s["price"] <= current_price:
                    dist_pct = (current_price - s["price"]) / current_price * 100
                    if dist_pct <= ZONE_PROXIMITY_PCT:
                        near_sup_1h  = True
                        sup_1h_price = s["price"]
                        break

        # Hierarquia
        if in_ob_4h and near_sup_4h:
            return "MAXIMA", f"{ob_4h_desc} + Sup 4H: {_fmt_price(sup_4h_price)}"
        if in_ob_4h:
            return "ALTA_OB4H", ob_4h_desc
        if in_ob_1h:
            return "ALTA_OB1H", ob_1h_desc
        if near_sup_4h:
            return "MEDIA", f"Suporte 4H: {_fmt_price(sup_4h_price)}"
        if near_sup_1h:
            return "BASE", f"Suporte 1H: {_fmt_price(sup_1h_price)}"
        return "NENHUMA", "Fora de zona"



# ── Check A — Rejeição presente (OBRIGATÓRIO) ─────────────────────────────────

def check_rejeicao_presente(candles_15m: list, direction: str) -> tuple[bool, str]:
    """
    Check A: última vela fechada deve mostrar rejeição direcional.
    Rejeição = shadow oposta ≥ 40% do range total da vela.
    SHORT → shadow superior (upper wick) ≥ 40% do range
    LONG  → shadow inferior (lower wick) ≥ 40% do range

    Returns (passed: bool, reason: str)
    """
    if not candles_15m:
        return False, "Sem velas 15m"
    c = candles_15m[-1]  # última vela fechada
    o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    rng = h - l
    if rng <= 0:
        return False, "Range zero"

    if direction == "SHORT":
        upper_wick = h - max(o, cl)
        pct = upper_wick / rng
        if pct >= 0.40:
            return True, f"Wick sup {pct:.0%} do range"
        return False, f"Wick sup {pct:.0%} < 40%"
    else:  # LONG
        lower_wick = min(o, cl) - l
        pct = lower_wick / rng
        if pct >= 0.40:
            return True, f"Wick inf {pct:.0%} do range"
        return False, f"Wick inf {pct:.0%} < 40%"


# ── Check B — Estrutura direcional 15m (OBRIGATÓRIO para CALL) ───────────────

def check_estrutura_direcional(candles_15m: list, direction: str,
                                janela: int = 8) -> tuple[bool, str]:
    """
    Check B: nas últimas `janela` velas fechadas, ≥ 5 devem ser direcionais.
    Direcional = vela na direção esperada (close > open para LONG,
                                           close < open para SHORT).

    Returns (passed: bool, reason: str)
    """
    if len(candles_15m) < janela:
        return False, f"Apenas {len(candles_15m)} velas (mín {janela})"
    recentes = candles_15m[-janela:]
    if direction == "SHORT":
        count = sum(1 for c in recentes if float(c["close"]) < float(c["open"]))
    else:
        count = sum(1 for c in recentes if float(c["close"]) > float(c["open"]))
    passed = count >= 5
    return passed, f"{count}/{janela} velas direcionais"


# ── score_oi_trend — C4 (mantido do v6) ──────────────────────────────────────

def score_oi_trend(symbol: str, direction: str, state: dict) -> tuple[int, str]:
    """
    C4: analisa tendência de OI via score_history (herdado do v6).
    Retorna (score 0|1, reason).
    """
    history = state.get("score_history", {}).get(symbol, {})
    oi_vals = history.get("oi_history", [])
    if len(oi_vals) < 4:
        return 0, "OI insuf"
    recent = oi_vals[-4:]
    increasing = sum(1 for i in range(1, len(recent)) if recent[i] > recent[i-1])
    decreasing = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i-1])
    if direction == "LONG" and increasing >= 3:
        return 1, f"OI↑ {increasing}/3 períodos"
    if direction == "SHORT" and decreasing >= 3:
        return 1, f"OI↓ {decreasing}/3 períodos"
    return 0, "OI sem tendência"


# ── Check C — Força e pressão do movimento (AMPLIFICADOR) ────────────────────

def check_forca_movimento(candles_15m: list, d: dict, state: dict,
                           direction: str) -> tuple[int, dict]:
    """
    Check C: 4 sub-checks amplificadores (0–4 pts).
    C1: BB position (>75% do range BB para SHORT, <25% para LONG)
    C2: Volume ≥ 1.5× média 8 velas
    C3: CVD proxy — ≥3/4 últimas velas direcionais
    C4: OI trend via score_oi_trend

    Returns (total: int, detalhes: dict com keys c1..c4 e reasons)
    """
    symbol = d.get("symbol", "")
    detalhes = {}
    total = 0

    # C1 — Bollinger Band position
    try:
        close  = float(d.get("close", 0))
        bb_up  = float(d.get("BB.upper|15", 0))
        bb_lo  = float(d.get("BB.lower|15", 0))
        bb_rng = bb_up - bb_lo
        if bb_rng > 0:
            pos = (close - bb_lo) / bb_rng  # 0=low, 1=high
            if direction == "SHORT" and pos > 0.75:
                total += 1
                detalhes["c1_bb"] = True
                detalhes["c1_reason"] = f"BB pos {pos:.0%} > 75%"
            elif direction == "LONG" and pos < 0.25:
                total += 1
                detalhes["c1_bb"] = True
                detalhes["c1_reason"] = f"BB pos {pos:.0%} < 25%"
            else:
                detalhes["c1_bb"] = False
                detalhes["c1_reason"] = f"BB pos {pos:.0%}"
        else:
            detalhes["c1_bb"] = False
            detalhes["c1_reason"] = "BB sem range"
    except Exception:
        detalhes["c1_bb"] = False
        detalhes["c1_reason"] = "BB erro"

    # C2 — Volume ≥ 1.5× média 8 velas
    try:
        vols = [float(c.get("volume", 0)) for c in candles_15m[-9:]]
        if len(vols) >= 2:
            last_vol = vols[-1]
            avg_vol  = sum(vols[:-1]) / len(vols[:-1])
            if avg_vol > 0 and last_vol >= 1.5 * avg_vol:
                total += 1
                detalhes["c2_vol"] = True
                detalhes["c2_reason"] = f"Vol {last_vol/avg_vol:.1f}× média"
            else:
                detalhes["c2_vol"] = False
                ratio = last_vol / avg_vol if avg_vol > 0 else 0
                detalhes["c2_reason"] = f"Vol {ratio:.1f}× média"
        else:
            detalhes["c2_vol"] = False
            detalhes["c2_reason"] = "Vol insuf"
    except Exception:
        detalhes["c2_vol"] = False
        detalhes["c2_reason"] = "Vol erro"

    # C3 — CVD proxy: ≥3/4 últimas velas direcionais
    try:
        ult4 = candles_15m[-4:]
        if direction == "SHORT":
            count = sum(1 for c in ult4 if float(c["close"]) < float(c["open"]))
        else:
            count = sum(1 for c in ult4 if float(c["close"]) > float(c["open"]))
        if count >= 3:
            total += 1
            detalhes["c3_cvd"] = True
            detalhes["c3_reason"] = f"CVD {count}/4 direcionais"
        else:
            detalhes["c3_cvd"] = False
            detalhes["c3_reason"] = f"CVD {count}/4 direcionais"
    except Exception:
        detalhes["c3_cvd"] = False
        detalhes["c3_reason"] = "CVD erro"

    # C4 — OI trend
    try:
        oi_sc, oi_reason = score_oi_trend(symbol, direction, state)
        if oi_sc > 0:
            total += 1
            detalhes["c4_oi"] = True
        else:
            detalhes["c4_oi"] = False
        detalhes["c4_reason"] = oi_reason
    except Exception:
        detalhes["c4_oi"] = False
        detalhes["c4_reason"] = "OI erro"

    return total, detalhes


# ── Zone → leverage score ─────────────────────────────────────────────────────

def _zone_to_score(zona_qualidade: str, check_c_total: int) -> int:
    """
    Retorna score 1–5 para cálculo de alavancagem via ALAV_POR_SCORE.
    Zona alta + check_c alto → alavancagem máxima.
    """
    zona_base = {
        "MAXIMA"   : 5,
        "ALTA_OB4H": 4,
        "ALTA_OB1H": 3,
        "MEDIA"    : 3,
        "BASE"     : 2,
        "NENHUMA"  : 1,
    }.get(zona_qualidade, 1)
    # Check C amplifica (até +1)
    if check_c_total >= 3:
        return min(5, zona_base + 1)
    return zona_base


# ── SL anchoring helpers ──────────────────────────────────────────────────────

def _get_nearest_resistance_zone(candles_4h, candles_1h, current_price: float) -> float | None:
    """Retorna o preço da zona de resistência mais próxima acima do preço atual."""
    candidates = []
    # OB Bearish 4H
    obs4b = detect_order_blocks_bearish(candles_4h)
    for ob in obs4b[-10:]:
        if ob["high"] > current_price:
            candidates.append(ob["high"])
    # Resistência 4H
    rl4, _ = find_swing_points(candles_4h)
    for r in rl4:
        if r["price"] > current_price:
            candidates.append(r["price"])
    # Resistência 1H
    rl1 = analyze_resistance_1h(candles_1h)
    for r in rl1:
        if r["price"] > current_price:
            candidates.append(r["price"])
    return min(candidates) if candidates else None


def _get_nearest_support_zone(candles_4h, candles_1h, current_price: float) -> float | None:
    """Retorna o preço da zona de suporte mais próxima abaixo do preço atual."""
    candidates = []
    # OB Bullish 4H
    obs4b = detect_order_blocks(candles_4h)
    for ob in obs4b[-10:]:
        if ob["low"] < current_price:
            candidates.append(ob["low"])
    # Suporte 4H
    _, sl4 = find_swing_points(candles_4h)
    for s in sl4:
        if s["price"] < current_price:
            candidates.append(s["price"])
    # Suporte 1H
    sl1 = analyze_support_1h(candles_1h)
    for s in sl1:
        if s["price"] < current_price:
            candidates.append(s["price"])
    return max(candidates) if candidates else None


# ── Trade params (adaptado para v7) ──────────────────────────────────────────

def calc_trade_params(symbol: str, current_price: float,
                      zona_qualidade: str, check_c_total: int,
                      candles_4h, candles_1h) -> dict | None:
    """
    Calcula parâmetros de trade LONG para v7.
    SL anchored to nearest support zone or 2% below price.
    """
    atr_pct = 0.015  # fallback
    # Tenta obter SL da zona de suporte mais próxima
    sup = _get_nearest_support_zone(candles_4h, candles_1h, current_price)
    if sup and sup < current_price:
        sl_price = sup * 0.998  # 0.2% abaixo da zona
        stop_pct = (current_price - sl_price) / current_price
    else:
        stop_pct = atr_pct
        sl_price = current_price * (1 - stop_pct)

    if stop_pct <= 0 or stop_pct > 0.15:
        stop_pct = 0.02
        sl_price = current_price * 0.98

    notional   = RISCO_POR_TRADE_USD / stop_pct
    score_alav = _zone_to_score(zona_qualidade, check_c_total)
    max_alav   = ALAV_POR_SCORE.get(score_alav, 3)
    alav       = min(max_alav, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
    alav       = max(1, alav)

    tp1 = current_price * (1 + stop_pct * 1.5)
    tp2 = current_price * (1 + stop_pct * 2.5)
    tp3 = current_price * (1 + stop_pct * 4.0)

    margem = notional / alav
    if margem > BANKROLL * MARGEM_MAX_POR_TRADE / 100:
        alav = max(1, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
        margem = notional / alav

    return {
        "entry"    : current_price,
        "sl"       : sl_price,
        "tp1"      : tp1,
        "tp2"      : tp2,
        "tp3"      : tp3,
        "stop_pct" : stop_pct * 100,
        "notional" : notional,
        "margem"   : margem,
        "alav"     : alav,
        "rr1"      : round(stop_pct * 1.5 / stop_pct, 1),
        "rr2"      : round(stop_pct * 2.5 / stop_pct, 1),
        "rr3"      : round(stop_pct * 4.0 / stop_pct, 1),
    }


def calc_trade_params_short(symbol: str, current_price: float,
                             zona_qualidade: str, check_c_total: int,
                             candles_4h, candles_1h) -> dict | None:
    """
    Calcula parâmetros de trade SHORT para v7.
    SL anchored to nearest resistance zone or 2% above price.
    """
    res = _get_nearest_resistance_zone(candles_4h, candles_1h, current_price)
    if res and res > current_price:
        sl_price = res * 1.002  # 0.2% acima da zona
        stop_pct = (sl_price - current_price) / current_price
    else:
        stop_pct = 0.02
        sl_price = current_price * (1 + stop_pct)

    if stop_pct <= 0 or stop_pct > 0.15:
        stop_pct = 0.02
        sl_price = current_price * 1.02

    notional   = RISCO_POR_TRADE_USD / stop_pct
    score_alav = _zone_to_score(zona_qualidade, check_c_total)
    max_alav   = ALAV_POR_SCORE.get(score_alav, 3)
    alav       = min(max_alav, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
    alav       = max(1, alav)

    tp1 = current_price * (1 - stop_pct * 1.5)
    tp2 = current_price * (1 - stop_pct * 2.5)
    tp3 = current_price * (1 - stop_pct * 4.0)

    margem = notional / alav
    if margem > BANKROLL * MARGEM_MAX_POR_TRADE / 100:
        alav = max(1, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
        margem = notional / alav

    return {
        "entry"    : current_price,
        "sl"       : sl_price,
        "tp1"      : tp1,
        "tp2"      : tp2,
        "tp3"      : tp3,
        "stop_pct" : stop_pct * 100,
        "notional" : notional,
        "margem"   : margem,
        "alav"     : alav,
        "rr1"      : 1.5,
        "rr2"      : 2.5,
        "rr3"      : 4.0,
    }



# ── analisar_token_async — lógica v7 ─────────────────────────────────────────

async def analisar_token_async(session: aiohttp.ClientSession,
                                symbol: str, d_4h: dict, d_1h: dict,
                                current_price: float, state: dict,
                                exchange: str) -> dict | None:
    """
    Pipeline v7 por token:
    1. Gate 4H (strict — NEUTRAL → DROP)
    2. Gate 1H (context only)
    3. identify_zona — sem zona → DROP
    4. Check A (obrigatório) → RADAR se falhou
    5. Check B (obrigatório para CALL) → QUASE se falhou
    6. Check C (amplificador, threshold varia por zona)
    7. Decisão: CALL | QUASE
    """
    rec_4h = recommendation_from_value(d_4h.get("Recommend.All|240", 0))
    rec_1h = recommendation_from_value(d_1h.get("Recommend.All|60", 0))

    # ── Gate 4H strict ───────────────────────────────────────────────────────
    direction_4h = None
    if rec_4h in ("BUY", "STRONG_BUY"):
        direction_4h = "LONG"
    elif rec_4h in ("SELL", "STRONG_SELL"):
        direction_4h = "SHORT"
    else:
        return None  # NEUTRAL → drop silencioso (não logado)

    direction = direction_4h

    # ── Gate 1H (contexto) ───────────────────────────────────────────────────
    gate_1h_ok = (
        (direction == "LONG"  and rec_1h in ("BUY", "STRONG_BUY", "NEUTRAL")) or
        (direction == "SHORT" and rec_1h in ("SELL", "STRONG_SELL", "NEUTRAL"))
    )

    # ── Klines 4H e 1H ──────────────────────────────────────────────────────
    candles_4h = await fetch_klines_cached_async(session, symbol, "4H", 50)
    candles_1h = await fetch_klines_cached_async(session, symbol, "1H", 50)
    if not candles_4h or not candles_1h:
        return None
    if len(candles_4h) < 20 or len(candles_1h) < 20:
        return None

    # ── Identify zona ────────────────────────────────────────────────────────
    zona_qualidade, zona_descricao = identify_zona(candles_4h, candles_1h, current_price, direction)
    if zona_qualidade == "NENHUMA":
        return None  # Fora de zona → DROP

    # ── Klines 15m ───────────────────────────────────────────────────────────
    candles_15m = await fetch_klines_cached_async(session, symbol, "15m", 20)
    if not candles_15m:
        return None

    # ── TV 15m data ───────────────────────────────────────────────────────────
    d_15m = {}  # será preenchido pelo run_scan_async via batch

    # ── Check A ───────────────────────────────────────────────────────────────
    check_a_ok, check_a_reason = check_rejeicao_presente(candles_15m, direction)

    # ── Check B ───────────────────────────────────────────────────────────────
    check_b_ok, check_b_reason = check_estrutura_direcional(candles_15m, direction)

    # ── Check C ───────────────────────────────────────────────────────────────
    # d_15m pode estar vazio aqui; check_forca_movimento usa candles_15m + d para BB
    check_c_total, check_c_det = check_forca_movimento(candles_15m, d_15m, state, direction)

    # Threshold C depende da zona
    thr_c = 2 if zona_qualidade in ("MAXIMA", "ALTA_OB4H", "ALTA_OB1H") else 3

    # ── Decisão ──────────────────────────────────────────────────────────────
    if not check_a_ok:
        status = "RADAR"
    elif not check_b_ok:
        status = "QUASE"
    elif check_c_total >= thr_c:
        status = "CALL"
    else:
        status = "QUASE"

    # ── Trade params ─────────────────────────────────────────────────────────
    params = None
    if status == "CALL":
        if direction == "LONG":
            params = calc_trade_params(symbol, current_price, zona_qualidade,
                                       check_c_total, candles_4h, candles_1h)
        else:
            params = calc_trade_params_short(symbol, current_price, zona_qualidade,
                                             check_c_total, candles_4h, candles_1h)

    return {
        "symbol"          : symbol,
        "direction"       : direction,
        "status"          : status,
        "rec_4h"          : rec_4h,
        "rec_1h"          : rec_1h,
        "gate_1h_ok"      : gate_1h_ok,
        "zona_qualidade"  : zona_qualidade,
        "zona_descricao"  : zona_descricao,
        "check_a_ok"      : check_a_ok,
        "check_a_reason"  : check_a_reason,
        "check_b_ok"      : check_b_ok,
        "check_b_reason"  : check_b_reason,
        "check_c_total"   : check_c_total,
        "check_c_det"     : check_c_det,
        "check_c_thr"     : thr_c,
        "price"           : current_price,
        "params"          : params,
        "rec_1h_raw"      : rec_1h,
    }



# ── run_scan_async — loop principal v7 ───────────────────────────────────────

async def run_scan_async():
    """
    Pipeline principal v7:
    1. Fetch perpetuals (universo)
    2. TV batch 4H + 1H (Gate 4H strict + 1H context)
    3. Gate 4H filter (LONG/SHORT, NEUTRAL → drop)
    4. TV batch 15m (para BB/vol do Check C)
    5. Por token: identify_zona → Check A/B/C → decisão
    6. Notificações Telegram
    7. Log RoundLoggerV7
    """
    import time
    t_start = time.time()

    log = RoundLoggerV7(version=VERSION)
    state = load_daily_state()

    # ── Fear & Greed ──────────────────────────────────────────────────────────
    async with aiohttp.ClientSession() as session:
        fg = await fetch_fear_greed_async(session)
        fg_val   = fg["value"]
        fg_class = fg["classification"]

    fg_val = fg_val or 50
    LOG.info(f"[v7] FGI={fg_val} ({fg_class})")

    # ── Perpetuals (universo) ─────────────────────────────────────────────────
    perps, exchange = fetch_perpetuals()
    if not perps:
        LOG.error("[v7] Sem perpetuals — abortando")
        return

    symbols = [p["symbol"] for p in perps]
    prices  = {p["symbol"]: p["price"] for p in perps}
    LOG.info(f"[v7] Universo: {len(symbols)} símbolos ({exchange})")

    # ── TV batch 4H ───────────────────────────────────────────────────────────
    async with aiohttp.ClientSession() as session:
        tv4h, _ = await fetch_tv_batch_async(session, symbols, COLS_4H)

    # ── Gate 4H strict ────────────────────────────────────────────────────────
    gate_long_syms  = []
    gate_short_syms = []
    for sym in symbols:
        d4 = tv4h.get(sym, {})
        rec = recommendation_from_value(d4.get("Recommend.All|240", 0))
        if rec in ("BUY", "STRONG_BUY"):
            gate_long_syms.append(sym)
        elif rec in ("SELL", "STRONG_SELL"):
            gate_short_syms.append(sym)
        # NEUTRAL → drop

    n_gate_long  = len(gate_long_syms)
    n_gate_short = len(gate_short_syms)
    gate_syms    = gate_long_syms + gate_short_syms
    LOG.info(f"[v7] Gate 4H: {n_gate_long} LONG, {n_gate_short} SHORT")

    # ── TV batch 1H ───────────────────────────────────────────────────────────
    async with aiohttp.ClientSession() as session:
        tv1h, _ = await fetch_tv_batch_async(session, gate_syms, COLS_1H)

    # ── Candle lock ───────────────────────────────────────────────────────────
    candle_lock = get_candle_lock_status()
    if candle_lock["use_prev"]:
        gate_syms = apply_candle_lock(gate_syms, candle_lock)
        LOG.info(f"[v7] Candle lock ativo — vela em formação ({candle_lock['seconds_open']:.0f}s), próximo fechamento em {candle_lock['next_close']:.0f}s")

    # ── TV batch 15m ─────────────────────────────────────────────────────────
    async with aiohttp.ClientSession() as session:
        tv15m, _ = await fetch_tv_batch_async(session, gate_syms, COLS_15M_TECH)

    # ── Análise por token ─────────────────────────────────────────────────────
    results = []
    n_zona_long  = 0
    n_zona_short = 0

    async with aiohttp.ClientSession() as session:
        tasks = []
        for sym in gate_syms:
            d4  = tv4h.get(sym, {})
            d1  = tv1h.get(sym, {})
            d15 = tv15m.get(sym, {})
            price = prices.get(sym, 0.0)
            if not price:
                continue
            tasks.append((sym, d4, d1, d15, price))

        for sym, d4, d1, d15, price in tasks:
            try:
                r = await analisar_token_async(session, sym, d4, d1, price, state, exchange)
                if r is None:
                    continue
                # Inject 15m TV data for BB (Check C C1)
                if "check_c_det" in r and not r["check_c_det"].get("c1_bb"):
                    # Re-run C1 with proper d_15m
                    d15m = tv15m.get(sym, {})
                    if d15m:
                        candles_15m_recheck = await fetch_klines_cached_async(
                            session, sym, "15m", 20)
                        if candles_15m_recheck:
                            c_total, c_det = check_forca_movimento(
                                candles_15m_recheck, d15m, state, r["direction"])
                            r["check_c_total"] = c_total
                            r["check_c_det"]   = c_det
                            thr_c = r["check_c_thr"]
                            # Re-decide
                            if r["check_a_ok"] and r["check_b_ok"]:
                                r["status"] = "CALL" if c_total >= thr_c else "QUASE"
                                if r["status"] == "CALL" and not r.get("params"):
                                    if r["direction"] == "LONG":
                                        candles_4h = await fetch_klines_cached_async(
                                            session, sym, "4H", 50)
                                        candles_1h = await fetch_klines_cached_async(
                                            session, sym, "1H", 50)
                                        r["params"] = calc_trade_params(
                                            sym, price, r["zona_qualidade"],
                                            c_total, candles_4h, candles_1h)
                                    else:
                                        candles_4h = await fetch_klines_cached_async(
                                            session, sym, "4H", 50)
                                        candles_1h = await fetch_klines_cached_async(
                                            session, sym, "1H", 50)
                                        r["params"] = calc_trade_params_short(
                                            sym, price, r["zona_qualidade"],
                                            c_total, candles_4h, candles_1h)

                if r["direction"] == "LONG":
                    n_zona_long += 1
                else:
                    n_zona_short += 1
                results.append(r)
            except Exception as e:
                LOG.warning(f"[v7] Erro em {sym}: {e}", exc_info=True)

    # ── BTC 4H trend (busca independente do universo) ────────────────────────
    btc_4h = "–"
    try:
        async with aiohttp.ClientSession() as session:
            btc_tv, _ = await fetch_tv_batch_async(session, ["BTCUSDT"], COLS_4H)
        btc_d4 = btc_tv.get("BTCUSDT", {})
        if btc_d4:
            btc_rec = recommendation_from_value(btc_d4.get("Recommend.All|240", 0))
            btc_4h  = btc_rec
    except Exception:
        pass

    # ── Notificações ─────────────────────────────────────────────────────────
    n_calls = sum(1 for r in results if r["status"] == "CALL")
    n_quase = sum(1 for r in results if r["status"] == "QUASE")
    elapsed = time.time() - t_start

    tg_notify_v7(
        results      = results,
        fg_val       = fg_val,
        n_univ       = len(symbols),
        n_gate_short = n_gate_short,
        n_gate_long  = n_gate_long,
        n_zona_short = n_zona_short,
        n_zona_long  = n_zona_long,
        elapsed      = elapsed,
        exchange     = exchange,
        btc_4h       = btc_4h,
    )

    # ── Estado ───────────────────────────────────────────────────────────────
    ts_now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    update_score_history(state, perps, ts_now)
    cleanup_score_history(state)
    save_daily_state(state)

    # ── Logging ───────────────────────────────────────────────────────────────
    log.set_meta(
        fgi          = fg_val,
        btc_4h       = btc_4h,
        exchange     = exchange,
        candle_locked= candle_lock.get("use_prev", False),
    )
    log.set_pipeline(
        universe      = len(symbols),
        gate_4h_long  = n_gate_long,
        gate_4h_short = n_gate_short,
        in_zona_long  = n_zona_long,
        in_zona_short = n_zona_short,
    )

    ZONA_ORDER = ["MAXIMA", "ALTA_OB4H", "ALTA_OB1H", "MEDIA", "BASE"]
    for r in results:
        det = r.get("check_c_det", {})
        log.add_token(
            symbol           = r["symbol"],
            direction        = r["direction"],
            zona_qualidade   = r["zona_qualidade"],
            zona_descricao   = r["zona_descricao"],
            check_a          = r["check_a_ok"],
            check_a_razao    = r["check_a_reason"],
            check_b          = r["check_b_ok"],
            check_b_razao    = r["check_b_reason"],
            check_c_total    = r["check_c_total"],
            check_c_detalhes = det,
            status           = r["status"],
        )

    for r in results:
        if r["status"] in ("CALL", "QUASE"):
            log.add_event(
                r["status"],
                symbol    = r["symbol"],
                direction = r["direction"],
                zona      = r["zona_qualidade"],
                check_c   = r["check_c_total"],
            )

    log.set_exec_seconds(elapsed)
    log.commit()

    LOG.info(f"[v7] Rodada concluída em {elapsed:.1f}s — "
             f"{n_calls} CALL, {n_quase} QUASE")



# ── main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description=f"Setup Atirador v{VERSION}")
    parser.add_argument("--once", action="store_true",
                        help="Executa uma rodada e sai (modo cron)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Não envia Telegram, apenas loga")
    args = parser.parse_args()

    global LOG, LOG_FILE
    LOG, LOG_FILE, _ = setup_logger()

    if args.dry_run:
        import os as _os
        _os.environ["DRY_RUN"] = "1"

    LOG.info(f"[v7] Setup Atirador v{VERSION} iniciando...")

    try:
        asyncio.run(run_scan_async())
    except KeyboardInterrupt:
        LOG.info("[v7] Interrompido pelo usuário")
    except Exception as e:
        LOG.exception(f"[v7] Erro fatal: {e}")
        raise


if __name__ == "__main__":
    main()
