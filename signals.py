# signals.py -- Calculo de parametros de trade e pipeline por token
# Extraido de setup_atirador_v7_0_0.py -- PR 7 modular-v8
#
# Bug #3 corrigido:
#   _get_nearest_resistance_zone / _get_nearest_support_zone agora chamam
#   analyze_resistance_1h / analyze_support_1h com (candles_1h, current_price)
#   e tratam o retorno como tuple[int, str] -- nao iterado como lista de precos.
#
# Bug #2 corrigido:
#   calc_trade_params / calc_trade_params_short usam get_alav_max_por_score()
#   em vez de ALAV_POR_SCORE.get().
import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

import aiohttp

from config import (
    BANKROLL,
    BRT,
    COLS_1H,
    COLS_4H,
    COLS_15M_TECH,
    MARGEM_MAX_POR_TRADE,
    RISCO_POR_TRADE_USD,
    ALAVANCAGEM_MIN,
    ALAVANCAGEM_MAX,
    RR_MINIMO,
    ALAV_POR_SCORE,
    VERSION,
)
from config import get_alav_max_por_score
from exchanges import fetch_klines_cached_async, fetch_perpetuals, fetch_fear_greed_async
from gates import fetch_tv_batch_async, recommendation_from_value
from indicators import (
    analyze_resistance_1h,
    analyze_support_1h,
    apply_candle_lock,
    detect_order_blocks,
    detect_order_blocks_bearish,
    find_swing_points,
    get_candle_lock_status,
    identify_zona,
    identify_zona_rich,
    ZONA_ORDER,
)
from scoring import (
    _zone_to_score,
    check_estrutura_direcional,
    check_forca_movimento,
    check_rejeicao_presente,
)
from state import cleanup_score_history, load_daily_state, save_daily_state, update_score_history

try:
    from logger_v7 import RoundLoggerV7
    _HAS_LOGGER = True
except ImportError:
    RoundLoggerV7 = None  # type: ignore[assignment,misc]
    _HAS_LOGGER = False

try:
    from telegram_v8 import tg_notify_v7
    _HAS_TELEGRAM = True
except ImportError:
    tg_notify_v7 = None  # type: ignore[assignment]
    _HAS_TELEGRAM = False

LOG = logging.getLogger("atirador")


# ---------------------------------------------------------------------------
# SL anchoring helpers
# ---------------------------------------------------------------------------

def _get_nearest_resistance_zone(
    candles_4h: list[dict],
    candles_1h: list[dict],
    current_price: float,
) -> float | None:
    """Retorna o preco da zona de resistencia mais proxima ACIMA do preco atual.

    Combina OBs bearish e swing highs do 4H.
    Chama analyze_resistance_1h(candles_1h, current_price) corretamente (Bug #3
    fix): dois argumentos, retorno tratado como tuple[int, str] -- nao iterado
    como lista de precos.
    """
    candidates: list[float] = []

    # OB Bearish 4H -- highs acima do preco atual
    obs4b = detect_order_blocks_bearish(candles_4h)
    for ob in obs4b[-10:]:
        if ob["high"] > current_price:
            candidates.append(ob["high"])

    # Swing highs 4H -- precos acima do preco atual
    swing_highs, _ = find_swing_points(candles_4h)
    for sh in swing_highs:
        if sh["price"] > current_price:
            candidates.append(sh["price"])

    # Resistencia 1H -- chamada correta com current_price (Bug #3 fix)
    # Retorno e tuple[int, str]: (score, details) -- nao uma lista de precos
    _score_1h, _details_1h = analyze_resistance_1h(candles_1h, current_price)
    # score_1h indica se ha resistencia proxima no 1H; nao expoe preco exato.
    # Os niveis de preco do 4H acima sao suficientes para ancoragem de SL.

    return min(candidates) if candidates else None


def _get_nearest_support_zone(
    candles_4h: list[dict],
    candles_1h: list[dict],
    current_price: float,
) -> float | None:
    """Retorna o preco da zona de suporte mais proxima ABAIXO do preco atual.

    Combina OBs bullish e swing lows do 4H.
    Chama analyze_support_1h(candles_1h, current_price) corretamente (Bug #3
    fix): dois argumentos, retorno tratado como tuple[int, str] -- nao iterado
    como lista de precos.
    """
    candidates: list[float] = []

    # OB Bullish 4H -- lows abaixo do preco atual
    obs4b = detect_order_blocks(candles_4h)
    for ob in obs4b[-10:]:
        if ob["low"] < current_price:
            candidates.append(ob["low"])

    # Swing lows 4H -- precos abaixo do preco atual
    _, swing_lows = find_swing_points(candles_4h)
    for sl in swing_lows:
        if sl["price"] < current_price:
            candidates.append(sl["price"])

    # Suporte 1H -- chamada correta com current_price (Bug #3 fix)
    # Retorno e tuple[int, str]: (score, details) -- nao uma lista de precos
    _score_1h, _details_1h = analyze_support_1h(candles_1h, current_price)
    # score_1h indica se ha suporte proximo no 1H; nao expoe preco exato.
    # Os niveis de preco do 4H abaixo sao suficientes para ancoragem de SL.

    return max(candidates) if candidates else None


# ---------------------------------------------------------------------------
# Trade params
# ---------------------------------------------------------------------------

def calc_trade_params(
    symbol: str,
    current_price: float,
    zona_qualidade: str,
    check_c_total: int,
    candles_4h: list[dict],
    candles_1h: list[dict],
) -> dict | None:
    """Calcula parametros de trade LONG.

    SL ancorado na zona de suporte mais proxima ou 2% abaixo do preco.
    Alavancagem via get_alav_max_por_score() -- nunca ALAV_POR_SCORE.get().
    """
    atr_pct = 0.015  # fallback

    # Tenta obter SL da zona de suporte mais proxima
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
    max_alav   = get_alav_max_por_score(score_alav)  # Bug #2 fix
    alav       = min(max_alav, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
    alav       = max(1, alav)

    tp1 = current_price * (1 + stop_pct * 1.5)
    tp2 = current_price * (1 + stop_pct * 2.5)
    tp3 = current_price * (1 + stop_pct * 4.0)

    margem = notional / alav
    if margem > BANKROLL * MARGEM_MAX_POR_TRADE / 100:
        alav   = max(1, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
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


def calc_trade_params_short(
    symbol: str,
    current_price: float,
    zona_qualidade: str,
    check_c_total: int,
    candles_4h: list[dict],
    candles_1h: list[dict],
) -> dict | None:
    """Calcula parametros de trade SHORT.

    SL ancorado na zona de resistencia mais proxima ou 2% acima do preco.
    Alavancagem via get_alav_max_por_score() -- nunca ALAV_POR_SCORE.get().
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
    max_alav   = get_alav_max_por_score(score_alav)  # Bug #2 fix
    alav       = min(max_alav, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
    alav       = max(1, alav)

    tp1 = current_price * (1 - stop_pct * 1.5)
    tp2 = current_price * (1 - stop_pct * 2.5)
    tp3 = current_price * (1 - stop_pct * 4.0)

    margem = notional / alav
    if margem > BANKROLL * MARGEM_MAX_POR_TRADE / 100:
        alav   = max(1, int(notional / (BANKROLL * MARGEM_MAX_POR_TRADE / 100)))
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


# ---------------------------------------------------------------------------
# Pipeline por token
# ---------------------------------------------------------------------------

async def analisar_token_async(
    session: aiohttp.ClientSession,
    symbol: str,
    d_4h: dict,
    d_1h: dict,
    current_price: float,
    state: dict,
    exchange: str,
    candle_lock: dict | None = None,
) -> dict | None:
    """Pipeline v7 por token:

    1. Gate 4H strict -- NEUTRAL -> DROP
    2. Gate 1H (contexto apenas)
    3. identify_zona -- sem zona -> DROP
    4. Check A (obrigatorio) -> RADAR se falhou
    5. Check B (obrigatorio para CALL) -> QUASE se falhou
    6. Check C (amplificador, threshold varia por zona)
    7. Decisao: CALL | QUASE | RADAR
    """
    rec_4h = recommendation_from_value(d_4h.get("Recommend.All|240", 0))
    rec_1h = recommendation_from_value(d_1h.get("Recommend.All|60", 0))

    # Gate 4H strict
    direction_4h = None
    if rec_4h in ("BUY", "STRONG_BUY"):
        direction_4h = "LONG"
    elif rec_4h in ("SELL", "STRONG_SELL"):
        direction_4h = "SHORT"
    else:
        return None  # NEUTRAL -> drop silencioso

    direction = direction_4h

    # Gate 1H (contexto)
    gate_1h_ok = (
        (direction == "LONG"  and rec_1h in ("BUY", "STRONG_BUY", "NEUTRAL")) or
        (direction == "SHORT" and rec_1h in ("SELL", "STRONG_SELL", "NEUTRAL"))
    )

    # Klines 4H e 1H
    candles_4h = await fetch_klines_cached_async(session, symbol, "4H", 50)
    candles_1h = await fetch_klines_cached_async(session, symbol, "1H", 50)
    if not candles_4h or not candles_1h:
        return None
    if len(candles_4h) < 20 or len(candles_1h) < 20:
        return None

    # Identify zona
    zona_qualidade, zona_descricao = identify_zona(candles_4h, candles_1h, current_price, direction)
    if zona_qualidade == "NENHUMA":
        return None  # Fora de zona -> DROP

    zona_rich = identify_zona_rich(candles_4h, candles_1h, current_price, direction)

    # Klines 15m
    candles_15m = await fetch_klines_cached_async(session, symbol, "15m", 20)
    if not candles_15m:
        return None
    if candle_lock:
        candles_15m = apply_candle_lock(candles_15m, candle_lock)
    if not candles_15m:
        return None

    # d_15m vazio -- sera preenchido pelo run_scan_async via batch (injecao pos-call)
    d_15m: dict = {}

    # Check A
    check_a_ok, check_a_reason, check_a_ev = check_rejeicao_presente(candles_15m, direction)

    # Check B
    check_b_ok, check_b_reason, check_b_ev = check_estrutura_direcional(candles_15m, direction)

    # Check C (d_15m pode estar vazio; C1 BB re-avaliado em run_scan_async)
    check_c_total, check_c_det = check_forca_movimento(candles_15m, d_15m, state, direction)

    # Threshold C depende da zona
    thr_c = 2 if zona_qualidade in ("MAXIMA", "ALTA_OB4H", "ALTA_OB1H") else 3

    # Decisao
    if not check_a_ok:
        status = "RADAR"
    elif not check_b_ok:
        status = "QUASE" if check_c_total >= 1 else "RADAR"
    elif check_c_total >= thr_c:
        status = "CALL"
    else:
        status = "QUASE" if check_c_total >= 1 else "RADAR"

    # Trade params (CALL e QUASE)
    params = None
    if status in ("CALL", "QUASE"):
        if direction == "LONG":
            params = calc_trade_params(
                symbol, current_price, zona_qualidade, check_c_total,
                candles_4h, candles_1h,
            )
        else:
            params = calc_trade_params_short(
                symbol, current_price, zona_qualidade, check_c_total,
                candles_4h, candles_1h,
            )

    return {
        "symbol"         : symbol,
        "direction"      : direction,
        "status"         : status,
        "rec_4h"         : rec_4h,
        "rec_1h"         : rec_1h,
        "gate_1h_ok"     : gate_1h_ok,
        "zona_qualidade" : zona_qualidade,
        "zona_descricao" : zona_descricao,
        "check_a_ok"     : check_a_ok,
        "check_a_reason" : check_a_reason,
        "check_a_ev"     : check_a_ev,
        "check_b_ok"     : check_b_ok,
        "check_b_reason" : check_b_reason,
        "check_b_ev"     : check_b_ev,
        "check_c_total"  : check_c_total,
        "check_c_det"    : check_c_det,
        **(check_c_det or {}),
        "check_c_thr"    : thr_c,
        "price"          : current_price,
        "params"         : params,
        "rec_1h_raw"     : rec_1h,
        "exchange"       : exchange,
        "zona_rich"      : zona_rich,
        "candle_ref"     : candles_15m[-1] if candles_15m else {},
    }


# ---------------------------------------------------------------------------
# Pipeline principal do scan
# ---------------------------------------------------------------------------

async def run_scan_async() -> None:
    """Pipeline principal v7 (extraido para signals.py -- sera movido para
    main.py no PR 9):

    1. Fetch perpetuals (universo) -- exchange: str via fetch_perpetuals() PR 3
    2. TV batch 4H + gate 4H strict (LONG/SHORT, NEUTRAL -> drop)
    3. TV batch 1H (contexto)
    4. Candle lock 15m
    5. TV batch 15m (BB/vol para Check C)
    6. Por token: analisar_token_async -> injecao 15m -> decisao
    7. BTC 4H trend
    8. Notificacoes Telegram
    9. Estado, logging, watchdog
    """
    t_start = time.time()

    log = RoundLoggerV7(version=VERSION) if _HAS_LOGGER and RoundLoggerV7 else None
    state = load_daily_state()

    # Fear & Greed
    async with aiohttp.ClientSession() as session:
        fg = await fetch_fear_greed_async(session)

    fg_val   = fg.get("value") or 50
    fg_class = fg.get("classification", "")
    LOG.info(f"[v8] FGI={fg_val} ({fg_class})")

    # Perpetuals (universo) -- exchange e str (Bug #1 corrigido no PR 3)
    perps, exchange = fetch_perpetuals()
    if not perps:
        LOG.error("[v8] Sem perpetuals -- abortando")
        return

    symbols = [p["symbol"] for p in perps]
    prices  = {p["symbol"]: p["price"] for p in perps}
    LOG.info(f"[v8] Universo: {len(symbols)} simbolos ({exchange})")

    # TV batch 4H
    async with aiohttp.ClientSession() as session:
        tv4h, _ = await fetch_tv_batch_async(session, symbols, COLS_4H)

    # Gate 4H strict
    gate_long_syms:  list[str] = []
    gate_short_syms: list[str] = []
    for sym in symbols:
        d4  = tv4h.get(sym, {})
        rec = recommendation_from_value(d4.get("Recommend.All|240", 0))
        if rec in ("BUY", "STRONG_BUY"):
            gate_long_syms.append(sym)
        elif rec in ("SELL", "STRONG_SELL"):
            gate_short_syms.append(sym)
        # NEUTRAL -> drop

    n_gate_long  = len(gate_long_syms)
    n_gate_short = len(gate_short_syms)
    gate_syms    = gate_long_syms + gate_short_syms
    LOG.info(f"[v8] Gate 4H: {n_gate_long} LONG, {n_gate_short} SHORT")

    # TV batch 1H
    async with aiohttp.ClientSession() as session:
        tv1h, _ = await fetch_tv_batch_async(session, gate_syms, COLS_1H)

    # Candle lock 15m
    candle_lock = get_candle_lock_status()
    if candle_lock["use_prev"]:
        LOG.info(
            f"[v8] Candle lock ativo -- vela em formacao "
            f"({candle_lock['seconds_open']:.0f}s), "
            f"proximo fechamento em {candle_lock['next_close']:.0f}s"
        )

    # TV batch 15m
    async with aiohttp.ClientSession() as session:
        tv15m, _ = await fetch_tv_batch_async(session, gate_syms, COLS_15M_TECH)

    # Analise por token
    results: list[dict] = []
    n_zona_long  = 0
    n_zona_short = 0

    async with aiohttp.ClientSession() as session:
        tasks = []
        for sym in gate_syms:
            d4    = tv4h.get(sym, {})
            d1    = tv1h.get(sym, {})
            price = prices.get(sym, 0.0)
            if not price:
                continue
            tasks.append((sym, d4, d1, price))

        for sym, d4, d1, price in tasks:
            try:
                r = await analisar_token_async(session, sym, d4, d1, price, state, exchange, candle_lock)
                if r is None:
                    continue

                # Injecao de dados 15m para re-avaliacao do Check C (C1 BB)
                if "check_c_det" not in r:
                    d15m = tv15m.get(sym, {})
                    if d15m:
                        candles_15m_recheck = await fetch_klines_cached_async(
                            session, sym, "15m", 20)
                        if candles_15m_recheck:
                            candles_15m_recheck = apply_candle_lock(candles_15m_recheck, candle_lock)
                        if candles_15m_recheck:
                            c_total, c_det = check_forca_movimento(
                                candles_15m_recheck, d15m, state, r["direction"])
                            r["check_c_total"] = c_total
                            r["check_c_det"]   = c_det
                            r.update(c_det or {})
                            thr_c = r["check_c_thr"]
                            # Re-decide
                            if r["check_a_ok"] and r["check_b_ok"]:
                                r["status"] = "CALL" if c_total >= thr_c else "QUASE"
                                if r["status"] == "CALL" and not r.get("params"):
                                    candles_4h = await fetch_klines_cached_async(
                                        session, sym, "4H", 50)
                                    candles_1h = await fetch_klines_cached_async(
                                        session, sym, "1H", 50)
                                    if r["direction"] == "LONG":
                                        r["params"] = calc_trade_params(
                                            sym, price, r["zona_qualidade"],
                                            c_total, candles_4h, candles_1h)
                                    else:
                                        r["params"] = calc_trade_params_short(
                                            sym, price, r["zona_qualidade"],
                                            c_total, candles_4h, candles_1h)

                if r["direction"] == "LONG":
                    n_zona_long += 1
                else:
                    n_zona_short += 1
                results.append(r)

            except Exception as e:
                LOG.warning(f"[v8] Erro em {sym}: {e}", exc_info=True)

    # BTC 4H trend (busca independente do universo)
    btc_4h = "-"
    try:
        async with aiohttp.ClientSession() as session:
            btc_tv, _ = await fetch_tv_batch_async(session, ["BTCUSDT"], COLS_4H)
        btc_d4 = btc_tv.get("BTCUSDT", {})
        if btc_d4:
            btc_4h = recommendation_from_value(btc_d4.get("Recommend.All|240", 0))
    except Exception:
        pass

    # Notificacoes Telegram
    n_calls  = sum(1 for r in results if r["status"] == "CALL")
    n_quase  = sum(1 for r in results if r["status"] == "QUASE")
    elapsed  = time.time() - t_start

    if _HAS_TELEGRAM and tg_notify_v7 is not None:
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
    else:
        LOG.debug("[v8] tg_notify_v7 indisponivel -- modulo telegram nao extraido ainda")

    # Estado
    ts_now = datetime.now(BRT).strftime("%Y-%m-%dT%H:%M")
    update_score_history(state, perps, ts_now)
    cleanup_score_history(state)
    save_daily_state(state)

    # Logging estruturado
    if log is not None:
        log.set_meta(
            fgi           = fg_val,
            btc_4h        = btc_4h,
            exchange      = exchange,
            candle_locked = candle_lock.get("use_prev", False),
        )
        log.set_pipeline(
            universe      = len(symbols),
            gate_4h_long  = n_gate_long,
            gate_4h_short = n_gate_short,
            in_zona_long  = n_zona_long,
            in_zona_short = n_zona_short,
        )

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

    # Watchdog -- registra ultima execucao bem-sucedida
    try:
        _wd = {
            "last_run": datetime.now(BRT).isoformat(),
            "version" : f"v{VERSION}",
        }
        with open("/tmp/atirador_last_run.json", "w") as _f:
            json.dump(_wd, _f)
    except Exception:
        pass

    LOG.info(
        f"[v8] Rodada concluida em {elapsed:.1f}s -- "
        f"{n_calls} CALL, {n_quase} QUASE"
    )
