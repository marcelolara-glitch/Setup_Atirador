#!/usr/bin/env python3
"""
analisar_token.py — Análise individual de token
================================================
Uso: python analisar_token.py BTCUSDT
     python analisar_token.py ETHUSDT SOLUSDT

Aplica o sistema de scoring completo (todos os 9 pilares) para um token
específico, ignorando os gates de direção (4H/1H). Os gates são exibidos
como informativos (✅/❌) para orientar o trader, mas não bloqueiam a análise.

Inclui por direção (LONG e SHORT):
  - Status dos gates (4H / 1H)
  - Score pilar a pilar (P1–P9)
  - Veredicto vs threshold adaptativo
  - Parâmetros de trade: SL seguro (1.5×ATR), TPs, alavancagem, margem

Resultado: mensagem Telegram + saída no terminal.
"""
import sys
import asyncio
import aiohttp
import time
from datetime import datetime

# ── Import do módulo principal ────────────────────────────────────────────────
# Inicializa LOG imediatamente antes de qualquer call que use LOG.xxx
import setup_atirador_v6_6_2 as a
a.LOG, a.LOG_FILE, a.TS_SCAN = a.setup_logger()


# ===========================================================================
# SCORE FORÇADO — todos os pilares sem gate de rejeição
# ===========================================================================

def _score_forcado(d: dict, candles_15m, candles_1h, candles_4h,
                   fg_value: float, state: dict, direction: str) -> tuple:
    """
    Calcula score com todos os 9 pilares, sem rejeitar pelo gate 4H/1H.

    Retorna (score, breakdown, data_quality, gate_4h_ok, gate_1h_ok).
    Os gates são avaliados como informativos mas NÃO descartam o token.
    O breakdown inclui 2 entradas extra no início para os gates.
    """
    sc           = 0
    breakdown    = []
    data_missing = 0

    # ── Gates (informativos) ─────────────────────────────────────────────
    s4h = d.get("summary_4h", "NEUTRAL")
    if direction == "SHORT":
        gate_4h_ok = "BUY" not in s4h        # SHORT quer SELL
    else:
        gate_4h_ok = "SELL" not in s4h       # LONG quer não-SELL
    status_4h = "✅ OK" if gate_4h_ok else "❌ falhou (análise forçada)"
    breakdown.append(("Gate 4H", 0, 0, f"{s4h} — {status_4h}"))

    s1h = d.get("summary_1h", "NEUTRAL")
    if direction == "SHORT":
        gate_1h_ok = "SELL" in s1h
    else:
        gate_1h_ok = "BUY" in s1h
    status_1h = "✅ OK" if gate_1h_ok else "❌ falhou (análise forçada)"
    breakdown.append(("Gate 1H", 0, 0, f"{s1h} — {status_1h}"))

    price = d.get("price", 0)

    # ── P4 — Zonas de Liquidez 4H ────────────────────────────────────────
    if candles_4h:
        lz_sc, lz_det = a.analyze_liquidity_zones_4h(candles_4h, price, direction)
    else:
        lz_sc, lz_det = 0, "⚠️ DADO AUSENTE — klines 4H"
        data_missing += 1
    sc += lz_sc
    breakdown.append(("P4 Liquidez 4H", lz_sc, 3, lz_det))

    # ── P5 — Figuras Gráficas 4H ─────────────────────────────────────────
    if candles_4h:
        cp_sc, cp_det = a.analyze_chart_patterns_4h(candles_4h, direction)
    else:
        cp_sc, cp_det = 0, "⚠️ DADO AUSENTE — klines 4H"
        data_missing += 1
    sc += cp_sc
    breakdown.append(("P5 Figuras 4H", cp_sc, 2, cp_det))

    # ── P6 — CHOCH / BOS 4H ──────────────────────────────────────────────
    if candles_4h:
        cb_sc, cb_det = a.analyze_choch_bos_4h(candles_4h, price, direction)
    else:
        cb_sc, cb_det = 0, "⚠️ DADO AUSENTE — klines 4H"
        data_missing += 1
    sc += cb_sc
    breakdown.append(("P6 CHOCH/BOS 4H", cb_sc, 3, cb_det))

    # ── P-1H — Suporte / Resistência 1H ─────────────────────────────────
    if candles_1h:
        if direction == "SHORT":
            s1h_sc, s1h_det = a.analyze_resistance_1h(candles_1h, price)
        else:
            s1h_sc, s1h_det = a.analyze_support_1h(candles_1h, price)
    else:
        s1h_sc, s1h_det = 0, "⚠️ DADO AUSENTE — klines 1H"
        data_missing += 1
    sc += s1h_sc
    label_1h = "P-1H Resistência 1H" if direction == "SHORT" else "P-1H Suporte 1H"
    breakdown.append((label_1h, s1h_sc, 4, s1h_det))

    # ── P1 — Bollinger Bands 15m ─────────────────────────────────────────
    bb_sc, bb_det = a.score_bollinger(d, direction)
    sc += bb_sc
    breakdown.append(("P1 Bollinger 15m", bb_sc, 3, bb_det))

    # ── P2 — Padrões de Candle 15m ───────────────────────────────────────
    ind_15m = d.get("_ind_15m", {})
    cp_list, ca_sc = a.score_candles(ind_15m, direction)
    sc += ca_sc
    det_candles = f"Padrões: {', '.join(cp_list)}" if cp_list else "Nenhum padrão detectado"
    breakdown.append(("P2 Candles 15m", ca_sc, 4, det_candles))

    # ── P3 — Funding Rate ─────────────────────────────────────────────────
    fr = d.get("funding_rate", 0)
    fr_sc, fr_det = a.score_funding_rate(fr, direction)
    sc += fr_sc
    breakdown.append(("P3 Funding Rate", fr_sc, 2, fr_det))

    # ── P7 — Filtro Pump/Dump (informativo) ──────────────────────────────
    pump_sc_raw, pump_det = a.score_pump_filter(d.get("price_change_24h", 0), direction)
    if pump_sc_raw is None:
        # PUMP/DUMP BLOCK — informativo na análise forçada, não descarta
        pump_sc  = 0
        pump_det = f"🚫 PUMP/DUMP BLOCK — {pump_det}"
    else:
        pump_sc = pump_sc_raw
    sc += pump_sc
    breakdown.append(("P7 Pump/Dump", pump_sc, 0, pump_det))

    # ── P8 — Volume 15m ──────────────────────────────────────────────────
    if candles_15m:
        vol_sc, vol_det = a.score_volume_15m(candles_15m, fg_value)
    else:
        vol_sc, vol_det = 0, "⚠️ DADO AUSENTE — klines 15m"
        data_missing += 1
    sc += vol_sc
    breakdown.append(("P8 Volume 15m", vol_sc, 2, vol_det))

    # ── P9 — OI Crescente ────────────────────────────────────────────────
    if state is not None:
        oi_sc, oi_det = a.score_oi_trend(d.get("oi_usd", 0), d.get("symbol", ""), state, direction)
    else:
        oi_sc, oi_det = 0, "OI sem histórico (primeiro scan)"
    sc += oi_sc
    breakdown.append(("P9 OI Crescente", oi_sc, 2, oi_det))

    final_sc     = max(sc, 0)
    total_klines = 5   # P4, P5, P6, P-1H, P8
    data_quality = round(1.0 - data_missing / total_klines, 2)
    return final_sc, breakdown, data_quality, gate_4h_ok, gate_1h_ok


# ===========================================================================
# FETCH DE DADOS DO TOKEN (OKX direto, símbolo único)
# ===========================================================================

async def _fetch_token_okx(session: aiohttp.ClientSession, symbol: str) -> dict | None:
    """
    Busca dados de um token via OKX (ticker + OI + FR) em paralelo.
    symbol: formato BTCUSDT → converte para BTC-USDT-SWAP internamente.
    Retorna dict no mesmo formato do _parse_okx_tickers(), ou None se falhar.
    """
    base    = symbol.replace("USDT", "")
    inst_id = f"{base}-USDT-SWAP"
    hdrs    = {"User-Agent": "scanner/6.6.2", "Accept-Encoding": "gzip"}

    async def _get(url):
        try:
            async with session.get(url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json(content_type=None)
        except Exception as exc:
            a.LOG.debug(f"    OKX fetch {url}: {exc}")
            return {}

    ticker_r, oi_r, fr_r = await asyncio.gather(
        _get(f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"),
        _get(f"https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId={inst_id}"),
        _get(f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"),
    )

    tickers = ticker_r.get("data", []) if isinstance(ticker_r, dict) else []
    if not tickers:
        return None

    t      = tickers[0]
    price  = a.sf(t.get("last", 0))
    if price <= 0:
        return None

    turnover = a.sf(t.get("volCcy24h", 0))
    open24h  = a.sf(t.get("open24h", 0))
    pct_chg  = ((price - open24h) / open24h * 100) if open24h > 0 else 0.0

    oi_items = oi_r.get("data", []) if isinstance(oi_r, dict) else []
    oi_usd   = a.sf(oi_items[0].get("oiUsd", 0)) if oi_items else 0.0

    fr_items = fr_r.get("data", []) if isinstance(fr_r, dict) else []
    fr_val   = a.sf(fr_items[0].get("fundingRate", 0)) if fr_items else 0.0

    return {
        "symbol"          : symbol,
        "base_coin"       : base,
        "price"           : price,
        "turnover_24h"    : turnover,
        "oi_usd"          : oi_usd,
        "oi_estimado"     : oi_usd <= 0,
        "volume_24h"      : a.sf(t.get("vol24h", 0)),
        "funding_rate"    : fr_val,
        "price_change_24h": pct_chg,
    }


# ===========================================================================
# FORMATAÇÃO DA MENSAGEM TELEGRAM
# ===========================================================================

def _veredicto(score: int, thr: int) -> str:
    """Veredicto de uma direção vs threshold."""
    if thr == 99:
        return "🔴 BOT OFF"
    diff = score - thr
    if   diff >= 0:  return f"✅ CALL  ({score}/{thr})"
    elif diff >= -4: return f"⚠️ QUASE ({score}/{thr}, faltam {-diff} pts)"
    elif diff >= -8: return f"🟡 MONIT ({score}/{thr}, faltam {-diff} pts)"
    else:            return f"🔵 AGUARDAR ({score}/{thr}, faltam {-diff} pts)"


def _fmt_pilares(bd: list) -> str:
    """
    Formata o breakdown pilar a pilar.
    Gates (max_pts=0 + 'Gate') são exibidos como linha de contexto.
    P7 (max_pts=0 + 'Pump') é exibido apenas se penalizou.
    Pilares pontuáveis exibem barra de progresso.
    """
    linhas = []
    for nome, pts, max_pts, det in bd:
        det_s = det[:45].split("|")[0].strip()

        if max_pts == 0:
            # Gates ou P7
            if "Gate" in nome:
                ico = "✅" if "✅ OK" in det else "❌"
                linhas.append(f"  {ico} <b>{nome}</b>: {det_s}")
            elif pts < 0:
                linhas.append(f"  🔻 P7 Pump/Dump: {det_s}")
            elif "BLOCK" in det:
                linhas.append(f"  🚫 P7 Pump/Dump: {det_s}")
            # P7 normal (pts=0, sem block/penalidade) — não exibir
        else:
            filled = int(round(pts / max_pts * 4)) if pts > 0 else 0
            bar    = "█" * filled + "░" * (4 - filled)
            ico    = "✅" if pts > 0 else ("⚠️" if "AUSENTE" in det else "⬜")
            linhas.append(f"  {ico} {nome:<18} {bar} {pts:>+}/{max_pts}  {det_s}")
    return "\n".join(linhas)


def _fmt_params(t: dict | None, direction: str) -> str:
    """Formata parâmetros de trade (entry, SL, TPs, sizing)."""
    if not t:
        return "  ⚠️ ATR indisponível — sem parâmetros calculáveis"
    sl_sinal = "+" if direction == "SHORT" else "−"
    sl_nota  = "← ACIMA" if direction == "SHORT" else "← abaixo"
    tp_sinal = "−" if direction == "SHORT" else "+"
    aviso    = " ⚠️" if t.get("margem_excedida") else ""
    return (
        f"  Entrada: <b>${a._fmt_price(t['entry'])}</b>  "
        f"(ATR 15m: ${a._fmt_price(t['atr'])})\n"
        f"  🛑 SL: ${a._fmt_price(t['sl'])}  "
        f"({sl_sinal}{t['sl_distance_pct']:.2f}%) {sl_nota}\n"
        f"  🎯 TP1: ${a._fmt_price(t['tp1'])}  ({tp_sinal}{t['sl_distance_pct']:.2f}%) → 50%\n"
        f"  🎯 TP2: ${a._fmt_price(t['tp2'])}  ({tp_sinal}{t['sl_distance_pct']*2:.2f}%) → 30%\n"
        f"  🎯 TP3: ${a._fmt_price(t['tp3'])}  ({tp_sinal}{t['sl_distance_pct']*3:.2f}%) → 20%\n"
        f"  ⚡ {t['alavancagem']}x{aviso}  |  "
        f"Margem ${t['margem_usd']:.0f}  |  "
        f"Risco ${t['risco_usd']:.2f}  |  Ganho ${t['ganho_rr2_usd']:.2f}"
    )


def _fmt_analise_tg(symbol: str, d: dict, ctx: dict,
                    score_l: int, bd_l: list, dq_l: float,
                    score_s: int, bd_s: list, dq_s: float,
                    trade_l: dict | None, trade_s: dict | None,
                    elapsed: float) -> str:
    """Monta a mensagem completa de análise individual."""
    sep   = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    base  = d.get("base_coin", symbol.replace("USDT", ""))
    price = d.get("price", 0)
    fr    = d.get("funding_rate", 0)
    oi    = d.get("oi_usd", 0)
    vol   = d.get("turnover_24h", 0)
    chg   = d.get("price_change_24h", 0)
    ts    = datetime.now(a.BRT).strftime("%d/%m/%Y %H:%M BRT")

    thr_l = ctx["threshold"]
    thr_s = ctx["threshold_short"]

    # Pilares que ainda podem pontuar (score = 0, max > 0, sem AUSENTE)
    def fontes_disponiveis(bd):
        return [f"{n} (+{mx})" for n, p, mx, det in bd
                if p == 0 and mx > 0 and "AUSENTE" not in det]

    fontes_l = ", ".join(fontes_disponiveis(bd_l)[:4]) or "—"
    fontes_s = ", ".join(fontes_disponiveis(bd_s)[:4]) or "—"

    dq_warn_l = f" ⚠️ dados incompletos ({dq_l:.0%})" if dq_l < 1.0 else ""
    dq_warn_s = f" ⚠️ dados incompletos ({dq_s:.0%})" if dq_s < 1.0 else ""

    msg = (
        f"🔍 <b>ANÁLISE — {base}</b>  |  {ts}\n"
        f"{sep}\n"
        f"📊 FGI {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        f"📈 LONG: {ctx['verdict']} (thr ≥{thr_l})\n"
        f"📉 SHORT: {ctx['verdict_short']} (thr ≥{thr_s})\n"
        f"{sep}\n"
        f"💹 Preço: <b>${a._fmt_price(price)}</b>  |  Var 24h: {chg:+.2f}%\n"
        f"📡 FR: {fr:.4%}  |  OI: ${oi/1e6:.1f}M  |  Vol: ${vol/1e6:.1f}M\n"
        f"{sep}\n"
        f"📈 <b>LONG — {_veredicto(score_l, thr_l)}</b>{dq_warn_l}\n"
        f"{_fmt_pilares(bd_l)}\n"
        f"  → Pode pontuar: {fontes_l}\n"
        f"{sep}\n"
        f"📉 <b>SHORT — {_veredicto(score_s, thr_s)}</b>{dq_warn_s}\n"
        f"{_fmt_pilares(bd_s)}\n"
        f"  → Pode pontuar: {fontes_s}\n"
        f"{sep}\n"
        f"⚙️ <b>PARÂMETROS LONG</b>\n"
        f"{_fmt_params(trade_l, 'LONG')}\n"
        f"{sep}\n"
        f"⚙️ <b>PARÂMETROS SHORT</b>\n"
        f"{_fmt_params(trade_s, 'SHORT')}\n"
        f"{sep}\n"
        f"⏱ {elapsed:.1f}s  |  Análise forçada — gates ignorados para {base}"
    )
    return msg


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

async def analisar_async(symbol: str):
    """Executa análise completa de um token e envia resultado via Telegram."""
    t_start = time.time()
    a.LOG.info(f"\n🔍 ANÁLISE INDIVIDUAL — {symbol}")
    a.LOG.info("=" * 55)

    state = a.load_daily_state()

    async with aiohttp.ClientSession() as session:

        # ── ETAPA 1: FGI + ticker OKX + TV 4H (inclui BTC para contexto) ──
        a.LOG.info("  1/4 — FGI + OKX ticker + TradingView 4H...")
        symbols_4h = list({symbol, "BTCUSDT"})  # BTC sempre para contexto de mercado

        fg, d, tv_4h = await asyncio.gather(
            a.fetch_fear_greed_async(session),
            _fetch_token_okx(session, symbol),
            a.fetch_tv_batch_async(session, symbols_4h, a.COLS_4H),
        )

        if d is None:
            msg = (
                f"❌ Token <b>{symbol}</b> não encontrado na OKX.\n"
                f"Verifique o símbolo (ex: BTCUSDT, ETHUSDT) e tente novamente.\n"
                f"O token precisa ter contrato perpétuo USDT na OKX."
            )
            a._tg_send(msg)
            a.LOG.error(f"  Token {symbol} não encontrado na OKX")
            return

        # 4H do token analisado
        ind_4h = tv_4h.get(symbol, {})
        raw_4h = ind_4h.get("Recommend.All|240")
        rsi_4h = a.sf(ind_4h.get("RSI|240"), default=50.0)
        s4h    = a.recommendation_from_value(raw_4h) if raw_4h is not None else "NEUTRAL"
        d["summary_4h"] = s4h
        d["rsi_4h"]     = rsi_4h
        a.LOG.info(f"  4H: {s4h} (RSI={rsi_4h:.1f})")

        # BTC 4H para contexto de mercado
        btc_raw    = tv_4h.get("BTCUSDT", {}).get("Recommend.All|240")
        btc_4h_str = a.recommendation_from_value(btc_raw) if btc_raw is not None else "NEUTRAL"
        ctx        = a.analyze_market_context(fg, btc_4h_str)
        fg_val     = fg.get("value", 50)
        a.LOG.info(f"  Mercado: FGI={fg_val} | BTC={btc_4h_str} | "
                   f"thr_LONG={ctx['threshold']} | thr_SHORT={ctx['threshold_short']}")

        # ── ETAPA 2: TV 1H + 15m (dois requests independentes para 15m) ───
        a.LOG.info("  2/4 — TradingView 1H + 15m (tech + candles)...")
        tv_1h, tv_tech, tv_candles = await asyncio.gather(
            a.fetch_tv_batch_async(session, [symbol], a.COLS_1H),
            a.fetch_tv_batch_async(session, [symbol], a.COLS_15M_TECH),
            a.fetch_tv_batch_async(session, [symbol], a.COLS_15M_CANDLES),
        )

        ind_1h = tv_1h.get(symbol, {})
        raw_1h = ind_1h.get("Recommend.All|60")
        s1h    = a.recommendation_from_value(raw_1h) if raw_1h is not None else "NEUTRAL"
        d["summary_1h"] = s1h
        a.LOG.info(f"  1H: {s1h}")

        ind_tech    = tv_tech.get(symbol, {})
        ind_candles = tv_candles.get(symbol, {})
        ind_15m     = {**ind_tech, **ind_candles}
        d["_ind_15m"]     = ind_15m
        d["bb_upper_15m"] = a.sf(ind_15m.get("BB.upper|15"))
        d["bb_lower_15m"] = a.sf(ind_15m.get("BB.lower|15"))
        d["atr_15m"]      = a.sf(ind_15m.get("ATR|15"))
        a.LOG.info(f"  15m: ATR={d['atr_15m']:.6f} | "
                   f"BB [{d['bb_lower_15m']:.4f} – {d['bb_upper_15m']:.4f}]")

        # ── ETAPA 3: Klines 15m / 1H / 4H ───────────────────────────────
        a.LOG.info("  3/4 — Klines 15m / 1H / 4H...")
        candle_lock = a.get_candle_lock_status()
        if candle_lock["use_prev"]:
            a.LOG.warning(f"  ⚠️ Candle lock ativo — usando penúltima vela 15m")

        k15m, k1h, k4h = await asyncio.gather(
            a.fetch_klines_async(session, symbol, "15m"),
            a.fetch_klines_cached_async(session, symbol, "1H"),
            a.fetch_klines_cached_async(session, symbol, "4H"),
        )
        k15m = a.apply_candle_lock(k15m or [], candle_lock)
        a.LOG.info(f"  Klines: 15m={len(k15m)} | 1H={len(k1h or [])} | 4H={len(k4h or [])}")

        # ── ETAPA 4: Score forçado LONG + SHORT ──────────────────────────
        a.LOG.info("  4/4 — Score forçado (LONG + SHORT)...")

        score_l, bd_l, dq_l, g4l, g1l = _score_forcado(
            d, k15m, k1h, k4h, fg_val, state, "LONG"
        )
        score_s, bd_s, dq_s, g4s, g1s = _score_forcado(
            d, k15m, k1h, k4h, fg_val, state, "SHORT"
        )

        thr_l = ctx["threshold"]
        thr_s = ctx["threshold_short"]
        a.LOG.info(f"  LONG:  {score_l}/25 (thr≥{thr_l}) | gate4H={'✅' if g4l else '❌'} "
                   f"| gate1H={'✅' if g1l else '❌'} | dq={dq_l:.0%}")
        a.LOG.info(f"  SHORT: {score_s}/25 (thr≥{thr_s}) | gate4H={'✅' if g4s else '❌'} "
                   f"| gate1H={'✅' if g1s else '❌'} | dq={dq_s:.0%}")

        # ── Parâmetros de trade ───────────────────────────────────────────
        price   = d.get("price", 0)
        atr_val = d.get("atr_15m", 0)
        trade_l = a.calc_trade_params(
            price, atr_val, score=score_l, threshold=thr_l
        )
        trade_s = a.calc_trade_params_short(
            price, atr_val, score=score_s, threshold=thr_s
        )

        # ── Formata + envia ───────────────────────────────────────────────
        elapsed = time.time() - t_start
        msg     = _fmt_analise_tg(
            symbol, d, ctx,
            score_l, bd_l, dq_l,
            score_s, bd_s, dq_s,
            trade_l, trade_s,
            elapsed,
        )

        a.LOG.info(f"\n{'='*55}")
        a.LOG.info(f"  LONG {score_l}/25 (thr≥{thr_l}) | SHORT {score_s}/25 (thr≥{thr_s})")
        a.LOG.info(f"  Mensagem: {len(msg)} chars | {elapsed:.1f}s")
        a.LOG.info(f"{'='*55}")

        # Truncar em 4096 chars (limite Telegram) com aviso
        if len(msg) > 4096:
            msg = msg[:4050] + "\n\n⚠️ [msg truncada — 4096 chars]"

        sent = a._tg_send(msg)
        if sent:
            a.LOG.info("  ✅ Telegram enviado com sucesso")
        else:
            a.LOG.warning("  ⚠️  Falha ao enviar Telegram — exibindo no terminal")
            print("\n" + msg)


def main():
    if len(sys.argv) < 2:
        print("Uso: python analisar_token.py BTCUSDT")
        print("     python analisar_token.py ETHUSDT SOLUSDT BTCUSDT")
        sys.exit(1)

    # Normalizar: maiúsculo, sem espaços, sem / ou -, garantir sufixo USDT
    symbols = []
    for raw in sys.argv[1:]:
        s = raw.upper().strip().replace("/", "").replace("-", "")
        if not s.endswith("USDT"):
            s += "USDT"
        symbols.append(s)

    for sym in symbols:
        asyncio.run(analisar_async(sym))


if __name__ == "__main__":
    main()
