# backtest/replay.py — Setup Atirador v9
# Replay de sinal: dado um trade do journal, reconstrói os inputs que a
# producao viu no bar de entrada e chama evaluate_token (codigo de producao).
# Base do gabarito (PR-3b). Roda na VM (depende de pandas_ta). NAO toca runtime.

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import KLINE_LIMIT_15M, KLINE_LIMIT_1H, KLINE_LIMIT_4H  # noqa: E402
from exchanges import klines_to_dataframe                            # noqa: E402
from signals import evaluate_token, build_btc_context                # noqa: E402
from backtest.candle_store import read_candles                       # noqa: E402

BTC_SYMBOL = "BTCUSDT"
BAR_15M_MS = 15 * 60 * 1000
DISABLED = {"rev_exaust"}          # usa 5m/1m que nao baixamos; 0 disparos no journal
_MIN_CANDLES = 100                 # classify_regime exige >= 100
_PRICE_TOL = 1e-4                  # tolerancia relativa pra casar entry_price com close


def _tail(rows: list, n: int) -> list:
    """Ultimas n linhas (read_candles ja devolve ascendente)."""
    return rows[-n:] if len(rows) > n else rows


def find_entry_bar(conn, symbol: str, journal_ts_iso: str,
                   entry_price: float) -> Optional[int]:
    """ts (epoch-ms, abertura) do bar de entrada, ancorado pelo TIMESTAMP.

    Bug corrigido: ancorar pelo entry_price e ambiguo -- em mercado lateral
    varios bars fecham no mesmo valor, e a busca por close-mais-proximo pinava
    vizinhos 1-2 bars off (candle errado, mesmo close). O log sai ~2min apos o
    fechamento do bar avaliado: floor_15m(log_utc) e o fechamento do bar de
    entrada, -1 bar e sua abertura. O entry_price vira VERIFICACAO do close,
    nao chave de busca. Fallback estreito (+/-2 bars) cobre rodada anomala que
    cruze o grid (raro: rodadas sao 116-129s). Retorna None se nada plausivel.
    """
    loc = int(datetime.fromisoformat(journal_ts_iso)
              .astimezone(timezone.utc).timestamp() * 1000)
    bar_ts = (loc // BAR_15M_MS) * BAR_15M_MS - BAR_15M_MS   # ultimo bar fechado (abertura)

    def _matches(ts: int) -> bool:
        c = read_candles(conn, symbol, "15m", start_ms=ts, end_ms=ts)
        if not c or not entry_price:
            return False
        return abs(c[0]["close"] - entry_price) / entry_price <= _PRICE_TOL

    if _matches(bar_ts):
        return bar_ts
    for d in (-1, 1, -2, 2):                  # rodada anomala: bar real costuma ficar atras
        ts = bar_ts + d * BAR_15M_MS
        if _matches(ts):
            return ts
    return None


def replay_signal(conn, symbol: str, entry_bar_ts: int):
    """Reconstrói os inputs no bar e chama evaluate_token (producao).

    Alimenta a MESMA contagem de velas que o live (KLINE_LIMIT_*), terminando
    no bar de entrada (inclusive). HTF (1h/4h) e passado real mas inerte em
    v9.1 (satisfaz a assinatura). 5m/1m=None, rev_exaust off, open_trades=[]
    (isola a perna de entrada).

    Retorna o SignalDecision, ou None se faltar historico minimo de 15m.
    """
    k15 = _tail(read_candles(conn, symbol, "15m", end_ms=entry_bar_ts), KLINE_LIMIT_15M)
    if len(k15) < _MIN_CANDLES:
        return None
    k1h = _tail(read_candles(conn, symbol, "1h", end_ms=entry_bar_ts), KLINE_LIMIT_1H)
    k4h = _tail(read_candles(conn, symbol, "4h", end_ms=entry_bar_ts), KLINE_LIMIT_4H)
    kbtc = _tail(read_candles(conn, BTC_SYMBOL, "15m", end_ms=entry_bar_ts), KLINE_LIMIT_15M)

    df_15m = klines_to_dataframe(k15)
    df_1h = klines_to_dataframe(k1h)
    df_4h = klines_to_dataframe(k4h)
    btc_context = (build_btc_context(klines_to_dataframe(kbtc))
                   if len(kbtc) >= _MIN_CANDLES else None)

    return evaluate_token(
        symbol=symbol,
        df_15m=df_15m, df_1h=df_1h, df_4h=df_4h,
        df_5m=None, df_1m=None,
        open_trades=[],
        btc_context=btc_context,
        disabled_setups=DISABLED,
    )
