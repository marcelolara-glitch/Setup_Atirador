"""
scoring.py — Módulo de scoring para v8.1.0
Checks A, B, C e conversão de zona → score final.
Depende de: config.py, indicators.py (IndicatorParams)
"""
import logging
import time
from typing import Any

from config import (
    ZONA_ORDER,
)
from indicators import IndicatorParams

LOG = logging.getLogger("atirador")

# Scores por zona — derivado da ordenação de ZONA_ORDER
_ZONA_BASE_SCORE: dict[str, int] = dict(zip(ZONA_ORDER, [5, 4, 3, 3, 2]))
_ZONA_BASE_SCORE["NENHUMA"] = 1


# ---------------------------------------------------------------------------
# Check A
# ---------------------------------------------------------------------------

def check_rejeicao_presente(
    candles_15m: list[dict],
    direction: str,
) -> tuple[bool, str, dict]:
    """
    Check A: última vela fechada deve mostrar rejeição direcional.
    Rejeição = shadow oposta ≥ 40% do range total da vela.
    SHORT → shadow superior (upper wick) ≥ 40% do range
    LONG  → shadow inferior (lower wick) ≥ 40% do range

    Returns (passed: bool, reason: str, evidencias: dict)
    """
    if not candles_15m:
        return False, "Sem velas 15m", {}
    c = candles_15m[-1]  # última vela fechada
    o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    rng = h - l
    if rng <= 0:
        return False, "Range zero", {}

    if direction == "SHORT":
        upper_wick = h - max(o, cl)
        pct = upper_wick / rng
        ev = {"wick_pct": round(pct, 4), "range": round(rng, 8), "wick_abs": round(upper_wick, 8)}
        if pct >= 0.40:
            return True, f"Wick sup {pct:.0%} do range", ev
        return False, f"Wick sup {pct:.0%} < 40%", ev
    else:  # LONG
        lower_wick = min(o, cl) - l
        pct = lower_wick / rng
        ev = {"wick_pct": round(pct, 4), "range": round(rng, 8), "wick_abs": round(lower_wick, 8)}
        if pct >= 0.40:
            return True, f"Wick inf {pct:.0%} do range", ev
        return False, f"Wick inf {pct:.0%} < 40%", ev


# ---------------------------------------------------------------------------
# Check B
# ---------------------------------------------------------------------------

def check_estrutura_direcional(
    candles_15m: list[dict],
    direction: str,
    janela: int = 8,
) -> tuple[bool, str, dict]:
    """
    Check B: nas últimas `janela` velas fechadas, ≥ 5 devem ser direcionais.
    Direcional = vela na direção esperada (close > open para LONG,
                                           close < open para SHORT).

    Returns (passed: bool, reason: str, evidencias: dict)
    """
    if len(candles_15m) < janela:
        return False, f"Apenas {len(candles_15m)} velas (mín {janela})", {}
    recentes = candles_15m[-janela:]
    if direction == "SHORT":
        count = sum(1 for c in recentes if float(c["close"]) < float(c["open"]))
    else:
        count = sum(1 for c in recentes if float(c["close"]) > float(c["open"]))
    passed = count >= 6
    ev = {"direcionais": count, "janela": janela, "ratio": round(count / janela, 4)}
    return passed, f"{count}/{janela} velas direcionais", ev


# ---------------------------------------------------------------------------
# Check C — sub-checks
# ---------------------------------------------------------------------------

def score_oi_trend(
    symbol: str,
    direction: str,
    state: dict,
) -> tuple[int, str]:
    """
    C4: analisa tendência de OI via score_history (schema corrigido no PR 2).
    Lê state["score_history"][symbol]["oi_history"].
    Retorna (score 0|1, reason).
    """
    history = state.get("score_history", {}).get(symbol, {})
    oi_vals = history.get("oi_history", [])
    if len(oi_vals) < 4:
        return 0, "OI insuf"
    recent = oi_vals[-4:]
    increasing = sum(1 for i in range(1, len(recent)) if recent[i] > recent[i - 1])
    decreasing = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i - 1])
    if direction == "LONG" and increasing >= 3:
        return 1, f"OI↑ {increasing}/3 períodos"
    if direction == "SHORT" and decreasing >= 3:
        return 1, f"OI↓ {decreasing}/3 períodos"
    return 0, "OI sem tendência"


def check_forca_movimento(
    candles_15m: list[dict],
    d: dict,
    state: dict,
    direction: str,
) -> tuple[int, dict]:
    """
    Check C: 4 sub-checks amplificadores (0–4 pts).
    C1: BB position (>75% do range BB para SHORT, <25% para LONG)
    C2: Volume ≥ 1.5× média 8 velas
    C3: CVD proxy — ≥3/4 últimas velas direcionais
    C4: OI trend via score_oi_trend

    Returns (total: int, detalhes: dict com keys c1..c4 e reasons)
    """
    symbol = d.get("symbol", "")
    detalhes: dict = {}
    total = 0

    # C1 — Bollinger Band position
    try:
        close = float(d.get("close", 0))
        bb_up = float(d.get("BB.upper|15", 0))
        bb_lo = float(d.get("BB.lower|15", 0))
        bb_rng = bb_up - bb_lo
        if bb_rng > 0:
            pos = (close - bb_lo) / bb_rng  # 0=low, 1=high
            detalhes["c1_bb_pos"] = round(pos, 4)
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
            detalhes["c1_bb_pos"] = None
    except Exception:
        detalhes["c1_bb"] = False
        detalhes["c1_reason"] = "BB erro"
        detalhes["c1_bb_pos"] = None

    # C2 — Volume ≥ 1.5× média 8 velas
    try:
        vols = [float(c.get("volume", 0)) for c in candles_15m[-9:]]
        if len(vols) >= 2:
            last_vol = vols[-1]
            avg_vol = sum(vols[:-1]) / len(vols[:-1])
            ratio = last_vol / avg_vol if avg_vol > 0 else 0
            detalhes["c2_vol_ratio"] = round(ratio, 2)
            if avg_vol > 0 and last_vol >= 1.5 * avg_vol:
                total += 1
                detalhes["c2_vol"] = True
                detalhes["c2_reason"] = f"Vol {last_vol / avg_vol:.1f}× média"
            else:
                detalhes["c2_vol"] = False
                detalhes["c2_reason"] = f"Vol {ratio:.1f}× média"
        else:
            detalhes["c2_vol"] = False
            detalhes["c2_reason"] = "Vol insuf"
    except Exception:
        detalhes["c2_vol"] = False
        detalhes["c2_reason"] = "Vol erro"
        detalhes["c2_vol_ratio"] = None

    # C3 — CVD proxy: ≥3/4 últimas velas direcionais
    try:
        ult4 = candles_15m[-4:]
        if direction == "SHORT":
            count = sum(1 for c in ult4 if float(c["close"]) < float(c["open"]))
        else:
            count = sum(1 for c in ult4 if float(c["close"]) > float(c["open"]))
        detalhes["c3_cvd_count"] = count
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
        detalhes["c3_cvd_count"] = None

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


# ---------------------------------------------------------------------------
# Conversão zona → score
# ---------------------------------------------------------------------------

def _zone_to_score(zona_qualidade: str, check_c_total: int) -> int:
    """
    Retorna score 1–5 para cálculo de alavancagem via ALAV_POR_SCORE.
    Zona alta + check_c alto → alavancagem máxima.
    Usa _ZONA_BASE_SCORE derivado de ZONA_ORDER (config.py).
    """
    zona_base = _ZONA_BASE_SCORE.get(zona_qualidade, 1)
    # Check C amplifica (até +1)
    if check_c_total >= 3:
        return min(5, zona_base + 1)
    return zona_base
