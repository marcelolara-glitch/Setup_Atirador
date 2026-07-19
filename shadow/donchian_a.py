# shadow/donchian_a.py — ESTAGIO 2 paper-trading forward do DONCHIAN-A (ledger
# 18/07). Espelho FIEL da BANCADA (stage1/bracket/excursion), REIMPLEMENTADO e
# nao importado p/ nao arrastar pandas_ta e manter o runtime v9 INTOCADO.
# Citacoes de linha da BANCADA no relatorio do PR. Sem cron/relatorio (PR-9B).
# PROIBIDO: signals/setups/risk/regime/journal.
from __future__ import annotations
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# CONFIG imutavel durante a observacao (mudar => o relogio reinicia do zero).
SYMBOLS = [   # TIER1 congelada — verbatim de backtest/sweep.py:22-27
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT",
    "AVAXUSDT", "LINKUSDT", "DOTUSDT", "TRXUSDT", "LTCUSDT", "BCHUSDT", "SUIUSDT",
    "TONUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "WLDUSDT",
]
N = 120                 # canal Donchian (barras 4h) — Turtle S1
S_ATR = 1.5             # stop em ATR
T_ATR = 6.0             # alvo em ATR
H_BARS = 48             # horizonte/exit temporal + espacamento por direcao (barras)
TF = "4h"
GRANULARITY = "4H"      # parametro de barra OKX/Bitget (exchanges.py)
ATR_PERIOD = 14
CTX_BARS = 30           # historico p/ ancorar entry+ATR (excursion.py:27)
BAR_MS = 14_400_000     # _BAR_MS["4h"]
FETCH_LIMIT = 200       # >= N + forward(48) + folga; 1 chamada/simbolo
DB_PATH = str(ROOT / "journal" / "shadow_donchian_a.db")
LOG_DIR = str(ROOT / "logs")
_DDL = ("CREATE TABLE IF NOT EXISTS shadow_trades (id TEXT PRIMARY KEY, "
        "symbol TEXT, direction TEXT, bar_ts_entry TEXT, entry_price REAL, "
        "atr_at_entry REAL, sl_price REAL, tp_price REAL, expiry_bar_ts TEXT, "
        "status TEXT, exit_ts TEXT, exit_price REAL, pnl_pct REAL, "
        "r_multiple REAL, evidence_json TEXT, UNIQUE(symbol, bar_ts_entry))")


def _setup_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")   # UTC no nome, como v9
    log = logging.getLogger("shadow_donchian_a")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(
            os.path.join(LOG_DIR, f"shadow_donchian_{day}.log"), encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(fh)
    return log


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(_DDL)
    conn.commit()
    return conn


def wilder_atr(candles: list, period: int = ATR_PERIOD) -> float:
    # ATR de Wilder semente (media simples de `period` TRs) — espelho excursion.py.
    if period <= 0 or len(candles) < period + 1:
        return 0.0
    window = candles[-(period + 1):]
    trs = []
    for i in range(1, len(window)):
        h, l, cprev = window[i]["high"], window[i]["low"], window[i - 1]["close"]
        trs.append(max(h - l, abs(h - cprev), abs(l - cprev)))
    atr = sum(trs) / period
    return atr if atr > 0 else 0.0


def donchian_signal(closes: list):
    # LONG se close > max(N anteriores); SHORT se < min; empate NAO e sinal.
    if len(closes) < N + 1:
        return None
    win, c = closes[-(N + 1):-1], closes[-1]      # N anteriores, exclui a corrente
    if c > max(win):
        return "LONG"
    if c < min(win):
        return "SHORT"
    return None


def resolve_bracket(direction: str, sl_price: float, tp_price: float,
                    fwd_bars: list, expiry_ts: int):
    # Espelho bracket.first_touch: empate alvo+stop na MESMA barra => STOP
    # (pessimista); STOP antes de ALVO; sem toque em H barras => EXPIRED no fecho
    # da barra H. -> (status, exit_ts, exit_price) ou None*3 (ainda OPEN).
    is_long = direction == "LONG"
    for b in fwd_bars:
        if is_long:
            target, stop = b["high"] >= tp_price, b["low"] <= sl_price
        else:
            target, stop = b["low"] <= tp_price, b["high"] >= sl_price
        if stop:                                  # cobre empate (AMB_SB) e STOP puro
            return "LOSS", b["ts"], sl_price
        if target:
            return "WIN", b["ts"], tp_price
    expiry_bar = next((b for b in fwd_bars if b["ts"] == expiry_ts), None)
    if expiry_bar is not None:
        return "EXPIRED", expiry_bar["ts"], expiry_bar["close"]
    return None, None, None


def _pnl(direction: str, entry: float, exit_price: float, atr: float):
    move = (exit_price - entry) if direction == "LONG" else (entry - exit_price)
    return (move / entry * 100.0 if entry else 0.0,
            move / (S_ATR * atr) if atr > 0 else 0.0)   # R = risco (S_ATR*ATR)


def _fetch_confirmed(symbol: str):
    # Klines 4h crescentes, SO fechadas. OKX ja dropa confirm=='0'; reforco por
    # tempo cobre Bitget (venue-independente). Indexacao por ts, nunca posicao.
    import aiohttp                                      # lazy: fora do sandbox
    from exchanges import fetch_klines_async

    async def _go():
        async with aiohttp.ClientSession() as session:
            return await fetch_klines_async(
                session, symbol, granularity=GRANULARITY, limit=FETCH_LIMIT)
    candles, _venue = asyncio.run(_go())
    now_ms = int(time.time() * 1000)
    closed = [c for c in candles if c["ts"] + BAR_MS <= now_ms]
    closed.sort(key=lambda c: c["ts"])
    return closed


def _update_open(conn, symbol, by_ts, log):
    for r in conn.execute(
            "SELECT * FROM shadow_trades WHERE symbol=? AND status='OPEN'",
            (symbol,)).fetchall():
        entry_ts, expiry_ts = int(r["bar_ts_entry"]), int(r["expiry_bar_ts"])
        fwd = [by_ts[ts] for ts in sorted(by_ts)
               if entry_ts < ts <= expiry_ts]           # forward por timestamp
        status, exit_ts, exit_price = resolve_bracket(
            r["direction"], r["sl_price"], r["tp_price"], fwd, expiry_ts)
        if status is None:
            continue
        pnl_pct, r_mult = _pnl(r["direction"], r["entry_price"],
                               exit_price, r["atr_at_entry"])
        conn.execute(
            "UPDATE shadow_trades SET status=?, exit_ts=?, exit_price=?, "
            "pnl_pct=?, r_multiple=? WHERE id=?",
            (status, str(exit_ts), exit_price, pnl_pct, r_mult, r["id"]))
        conn.commit()
        log.info(f"  [{symbol}] {r['direction']} -> {status} "
                 f"pnl={pnl_pct:.3f}% R={r_mult:.2f}")


def _detect_and_open(conn, symbol, closed, log):
    closes = [c["close"] for c in closed]
    direction = donchian_signal(closes)
    if direction is None:
        return
    bar_ts = closed[-1]["ts"]
    row = conn.execute(                                 # espacamento 48b/direcao
        "SELECT MAX(CAST(bar_ts_entry AS INTEGER)) AS last_ts FROM "
        "shadow_trades WHERE symbol=? AND direction=?", (symbol, direction)
    ).fetchone()
    if row["last_ts"] is not None and bar_ts - int(row["last_ts"]) < H_BARS * BAR_MS:
        return
    atr = wilder_atr(closed[-CTX_BARS:])                # janela terminando no sinal
    if atr <= 0:
        log.info(f"  [{symbol}] ATR<=0 — sinal {direction} ignorado")
        return
    entry = closed[-1]["close"]
    sign = 1.0 if direction == "LONG" else -1.0
    sl, tp = entry - sign * S_ATR * atr, entry + sign * T_ATR * atr
    expiry_ts = bar_ts + H_BARS * BAR_MS
    evidence = json.dumps({"channel_high": max(win := closes[-(N + 1):-1]),
                           "channel_low": min(win), "close": entry, "atr": atr,
                           "n": N, "s_atr": S_ATR, "t_atr": T_ATR, "tf": TF})
    try:
        conn.execute(
            "INSERT INTO shadow_trades (id, symbol, direction, bar_ts_entry, "
            "entry_price, atr_at_entry, sl_price, tp_price, expiry_bar_ts, "
            "status, evidence_json) VALUES (?,?,?,?,?,?,?,?,?,'OPEN',?)",
            (f"{symbol}-{bar_ts}", symbol, direction, str(bar_ts), entry, atr,
             sl, tp, str(expiry_ts), evidence))
        conn.commit()
        log.info(f"  [{symbol}] OPEN {direction} @ {entry} atr={atr:.6f} "
                 f"sl={sl:.6f} tp={tp:.6f}")
    except sqlite3.IntegrityError:
        pass                                            # (symbol,bar_ts) idempotente


def run_once(db_path: str = DB_PATH, fetch_fn=None, symbols=None, log=None):
    # Por simbolo (try/except isolado): fetch -> resolve OPEN -> detecta.
    # Falha de 1 = warning; universo inteiro = notify_error. -> (ok, falhas).
    log = log or _setup_logger()
    fetch_fn = fetch_fn or _fetch_confirmed
    symbols = symbols or SYMBOLS
    conn = _connect(db_path)
    ok = falhas = 0
    for symbol in symbols:
        try:
            closed = fetch_fn(symbol)
            if len(closed) < N + 2:
                raise ValueError(f"barras insuficientes: {len(closed)} < {N + 2}")
            _update_open(conn, symbol, {c["ts"]: c for c in closed}, log)
            _detect_and_open(conn, symbol, closed, log)
            ok += 1
        except Exception as e:
            falhas += 1
            log.warning(f"  [{symbol}] falha: {type(e).__name__}: {e}")
    conn.close()
    log.info(f"[shadow] run_once fim — ok={ok} falhas={falhas}")
    if ok == 0 and symbols:
        try:
            from telegram import notify_error
            notify_error("shadow DONCHIAN-A: falha total do universo",
                         f"{falhas} simbolos falharam no run_once — sem dados.")
        except Exception as e:
            log.warning(f"  notify_error indisponivel: {type(e).__name__}: {e}")
    return ok, falhas


if __name__ == "__main__":
    run_once()
