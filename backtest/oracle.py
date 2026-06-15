# backtest/oracle.py — Setup Atirador v9 (CONTROLE POSITIVO, nao-operavel)
# Detector TRAPACEIRO: usa LOOKAHEAD de proposito. So existe pra validar que a
# maquina do null_model DETECTA edge quando ele existe (e nao so que zera o
# aleatorio). Entra LONG num bar TREND_UP so se o preco REALMENTE subiu
# _LOOKAHEAD bars a frente (close[i+L] > close[i]); SHORT espelhado em TREND_DOWN.
# Por construcao tem edge gigante -> a maquina TEM que cuspir effect grande e
# p ~ 0. Se nao cuspir, a maquina esta furada (falso negativo). IMPOSSIVEL de
# operar (usa o futuro). Mesma assinatura/saida dos outros detectores.
#   .venv/bin/python -m backtest.null_model --detector oracle --start ... --end ...

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import read_candles                    # noqa: E402
from backtest.excursion import HORIZONS, collapse_events          # noqa: E402

SEP_BARS = max(HORIZONS)          # 48 — mesma separacao do null_model
_LOOKAHEAD = 8                    # olha 8 bars a frente (casa com o horizonte H8)


def oracle_detector(conn, symbol: str, timeline: dict, start_ms: int,
                    end_ms: int) -> list:
    """Cheat: emite LONG em TREND_UP se close[i+L] > close[i]; SHORT em
    TREND_DOWN se close[i+L] < close[i]. Direcao casa com a celula do nulo. Pula
    bars sem L candles a frente. Retorna [{bar_ts, direction}] colapsado em
    SEP_BARS. Edge esperado: GRANDE e positivo, p ~ 0 (controle positivo)."""
    cndl = read_candles(conn, symbol, "15m", end_ms=end_ms)
    n = len(cndl)
    raw: list = []
    for i in range(n):
        ts = cndl[i]["ts"]
        if ts < start_ms or ts > end_ms:
            continue
        if i + _LOOKAHEAD >= n:
            continue                   # sem futuro suficiente -> nao emite
        reg = timeline.get(ts)
        c0 = cndl[i]["close"]
        cf = cndl[i + _LOOKAHEAD]["close"]
        if reg == "TREND_UP" and cf > c0:
            raw.append({"bar_ts": ts, "direction": "LONG"})
        elif reg == "TREND_DOWN" and cf < c0:
            raw.append({"bar_ts": ts, "direction": "SHORT"})
    return [{"bar_ts": s["bar_ts"], "direction": s["direction"]}
            for s in collapse_events(raw, SEP_BARS)]
