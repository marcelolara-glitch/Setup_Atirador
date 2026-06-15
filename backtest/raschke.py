# backtest/raschke.py — Setup Atirador v9 (BANCADA, nao-producao)
# Detector do "Holy Grail" (Raschke/Connors, Street Smarts) — CANONICO:
# ADX(14) + EMA(20) + primeiro pullback + gatilho de rompimento. SEM Donchian
# (filtro nosso, estagio 2). NAO entra no evaluate_token, NAO dispara Telegram.
# Roda so pela maquina do null_model, contra o nulo casado, pra decidir edge
# offline antes de qualquer promocao pra setups/.
#
# Mesma assinatura/estilo de random_detector: recebe `timeline` (regime por bar,
# no-lookahead, computado 1x em null_model.regime_timeline) e devolve eventos ja
# colapsados em SEP_BARS. Direcao vem do regime (TREND_UP->LONG, TREND_DOWN->
# SHORT), casando o evento com a celula do nulo. ADX/EMA usam as MESMAS contas
# da producao (ta.adx length=14 col ADX_14; ta.ema length=20) pra o edge
# transferir na promocao. Roda na VM (pandas_ta_classic).
#   .venv/bin/python -m backtest.null_model --detector raschke --start ... --end ...

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas_ta_classic as ta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from exchanges import klines_to_dataframe                         # noqa: E402
from backtest.candle_store import read_candles                    # noqa: E402
from backtest.excursion import HORIZONS, collapse_events          # noqa: E402

SEP_BARS = max(HORIZONS)          # 48 — mesma separacao do null_model
_ADX_LEN = 14                     # canonico
_ADX_MIN = 30.0                   # canonico (Raschke: ADX > 30)
_EMA_LEN = 20                     # canonico
_PULLBACK_MAX = 6                 # [SUP] candles entre toque na EMA e gatilho
_FIRST_TOUCH_LB = 10              # [SUP] candles do lado certo da EMA antes do toque


def _series(conn, symbol: str, end_ms: int):
    """Le todos os 15m ate end_ms; devolve (candles, ema20, adx14) alinhados por
    posicao. ADX/EMA sao causais (cada valor usa so passado+presente) -> sem
    lookahead. Burn-in folgado: o store tem ~90d antes da janela de teste."""
    cndl = read_candles(conn, symbol, "15m", end_ms=end_ms)
    if len(cndl) < 60:
        return cndl, None, None
    df = klines_to_dataframe(cndl)
    ema = ta.ema(df["close"], length=_EMA_LEN)
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=_ADX_LEN)
    adx_col = f"ADX_{_ADX_LEN}"
    if ema is None or adx_df is None or adx_col not in adx_df.columns:
        return cndl, None, None
    return cndl, ema.to_numpy(), adx_df[adx_col].to_numpy()


def _long_signal(cndl, ema, adx, i: int) -> bool:
    """Raschke long no bar i (usa so bars <= i). Direcao/TREND conferida fora.
      gate   : ADX[i] > 30.
      setup  : bar t em [i-6, i-1] que tocou a EMA por cima (low<=EMA<=close),
               sendo o PRIMEIRO toque (os 10 bars antes de t ficaram acima da EMA).
      gatilho: high[i] rompe high[t] e i e o PRIMEIRO bar a romper (dedup)."""
    if not (np.isfinite(adx[i]) and adx[i] > _ADX_MIN and np.isfinite(ema[i])):
        return False
    t = None
    for k in range(i - 1, max(-1, i - 1 - _PULLBACK_MAX), -1):
        if k < 0 or not np.isfinite(ema[k]):
            continue
        if cndl[k]["low"] <= ema[k] <= cndl[k]["close"]:
            t = k
            break
    if t is None:
        return False
    for k in range(t - 1, max(-1, t - 1 - _FIRST_TOUCH_LB), -1):
        if k < 0 or not np.isfinite(ema[k]):
            return False               # historico insuficiente -> nao confia
        if cndl[k]["low"] <= ema[k]:
            return False               # ja tinha tocado -> nao e o primeiro
    if not (cndl[i]["high"] > cndl[t]["high"]):
        return False
    for k in range(t + 1, i):
        if cndl[k]["high"] > cndl[t]["high"]:
            return False               # ja rompeu antes -> i nao e o gatilho
    return True


def _short_signal(cndl, ema, adx, i: int) -> bool:
    """Espelho do long: toque por baixo (high>=EMA>=close), primeiro toque (10
    bars abaixo da EMA), gatilho low[i] rompe low[t] e e o primeiro a romper."""
    if not (np.isfinite(adx[i]) and adx[i] > _ADX_MIN and np.isfinite(ema[i])):
        return False
    t = None
    for k in range(i - 1, max(-1, i - 1 - _PULLBACK_MAX), -1):
        if k < 0 or not np.isfinite(ema[k]):
            continue
        if cndl[k]["high"] >= ema[k] >= cndl[k]["close"]:
            t = k
            break
    if t is None:
        return False
    for k in range(t - 1, max(-1, t - 1 - _FIRST_TOUCH_LB), -1):
        if k < 0 or not np.isfinite(ema[k]):
            return False
        if cndl[k]["high"] >= ema[k]:
            return False
    if not (cndl[i]["low"] < cndl[t]["low"]):
        return False
    for k in range(t + 1, i):
        if cndl[k]["low"] < cndl[t]["low"]:
            return False
    return True


def raschke_detector(conn, symbol: str, timeline: dict, start_ms: int,
                     end_ms: int) -> list:
    """Eventos Raschke canonicos em [start_ms, end_ms]. Direcao vem do regime
    (TREND_UP->LONG, TREND_DOWN->SHORT); RANGE/SQUEEZE nao disparam. Retorna
    [{bar_ts, direction}] ja colapsado em SEP_BARS (mesmo padrao dos outros
    detectores)."""
    cndl, ema, adx = _series(conn, symbol, end_ms)
    if ema is None:
        return []
    raw: list = []
    for i in range(len(cndl)):
        ts = cndl[i]["ts"]
        if ts < start_ms or ts > end_ms:
            continue
        reg = timeline.get(ts)
        if reg == "TREND_UP" and _long_signal(cndl, ema, adx, i):
            raw.append({"bar_ts": ts, "direction": "LONG"})
        elif reg == "TREND_DOWN" and _short_signal(cndl, ema, adx, i):
            raw.append({"bar_ts": ts, "direction": "SHORT"})
    return [{"bar_ts": s["bar_ts"], "direction": s["direction"]}
            for s in collapse_events(raw, SEP_BARS)]
