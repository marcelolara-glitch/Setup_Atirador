# indicators.py — Funções de análise técnica puras
# Extraído de setup_atirador_v7_0_0.py — PR 5 modular-v8
#
# Regras de pureza:
#   - Zero leitura/escrita de estado global
#   - Zero I/O (sem rede, arquivo ou banco)
#   - LOG.debug apenas — o chamador decide o que logar
#   - Determinístico: mesmos inputs → mesmo output
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np

from config import (
    BRT,
    CANDLE_15M_SECONDS,
    CANDLE_CLOSED_GRACE_S,
    OB_IMPULSE_N,
    OB_IMPULSE_PCT,
    OB_PROXIMITY_PCT,
    SR_PROXIMITY_PCT,
    SWING_WINDOW,
    ZONE_PROXIMITY_PCT,
    ZONA_ORDER,
)

LOG = logging.getLogger("atirador")


# ---------------------------------------------------------------------------
# IndicatorParams — objeto de configuração injetável
# ---------------------------------------------------------------------------

@dataclass
class IndicatorParams:
    """Parâmetros de análise técnica injetáveis.

    Uso em produção: não passar — usa defaults de config.py.
    Uso em backtest/sensitivity: instanciar com valores customizados.

    Exemplo:
        params = IndicatorParams(zone_proximity_pct=2.0)
        resultado = identify_zona(c4h, c1h, price, direction, params=params)
    """
    zone_proximity_pct: float = field(default_factory=lambda: ZONE_PROXIMITY_PCT)
    sr_proximity_pct: float = field(default_factory=lambda: SR_PROXIMITY_PCT)
    ob_impulse_n: int = field(default_factory=lambda: OB_IMPULSE_N)
    ob_impulse_pct: float = field(default_factory=lambda: OB_IMPULSE_PCT)
    ob_proximity_pct: float = field(default_factory=lambda: OB_PROXIMITY_PCT)
    swing_window: int = field(default_factory=lambda: SWING_WINDOW)


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------

def _fmt_price(p: float) -> str:
    if p == 0:
        return "0"
    mag = -math.floor(math.log10(abs(p)))
    decimals = max(4, mag + 2)
    return f"{p:.{decimals}f}"


# ---------------------------------------------------------------------------
# Funções de análise técnica
# ---------------------------------------------------------------------------

def find_swing_points(
    candles: list[dict],
    window: int | None = None,
    params: IndicatorParams | None = None,
) -> tuple[list[dict], list[dict]]:
    """Detecta swing highs e swing lows.

    window tem precedência sobre params.swing_window quando fornecido.
    """
    p = params if params is not None else IndicatorParams()
    if window is None:
        window = p.swing_window

    def _detect(candles, w):
        if len(candles) < w * 2 + 1:
            return [], []
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


def detect_order_blocks(
    candles: list[dict],
    params: IndicatorParams | None = None,
) -> list[dict]:
    """OBs bullish: último candle bearish antes de impulso ≥ OB_IMPULSE_PCT."""
    p = params if params is not None else IndicatorParams()
    obs = []
    n = p.ob_impulse_n
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] >= c["open"]: continue
        ref = c["close"]
        if ref <= 0: continue
        max_close   = max(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (max_close - ref) / ref * 100
        if impulse_pct >= p.ob_impulse_pct:
            obs.append({
                "high"       : max(c["open"], c["close"]),
                "low"        : min(c["open"], c["close"]),
                "index"      : i,
                "impulso_pct": round(impulse_pct, 2),
            })
    return obs


def detect_order_blocks_bearish(
    candles: list[dict],
    params: IndicatorParams | None = None,
) -> list[dict]:
    """OBs bearish: último candle bullish antes de impulso de queda ≥ OB_IMPULSE_PCT."""
    p = params if params is not None else IndicatorParams()
    obs = []
    n = p.ob_impulse_n
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] <= c["open"]: continue
        ref = c["close"]
        if ref <= 0: continue
        min_close   = min(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (ref - min_close) / ref * 100
        if impulse_pct >= p.ob_impulse_pct:
            obs.append({
                "high"       : max(c["open"], c["close"]),
                "low"        : min(c["open"], c["close"]),
                "index"      : i,
                "impulso_pct": round(impulse_pct, 2),
            })
    return obs


def analyze_support_1h(
    candles_1h: list[dict],
    current_price: float,
    params: IndicatorParams | None = None,
) -> tuple[int, str]:
    """Detecta suporte (swing low + OB bullish) no 1H para LONG."""
    p = params if params is not None else IndicatorParams()
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"
    sh, sl = find_swing_points(candles_1h, params=p)
    score, details = 0, []
    if sl:
        for s in reversed(sl):
            dist_pct = (current_price - s["price"]) / current_price * 100
            if 0 < dist_pct <= p.sr_proximity_pct:
                score += 2
                details.append(f"Suporte 1H em {s['price']:.4f} ({dist_pct:.2f}% abaixo)")
                break
    obs = detect_order_blocks(candles_1h, params=p)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = (current_price - ob_mid) / current_price * 100
            if -p.ob_proximity_pct <= dist_pct <= p.ob_proximity_pct:
                score += 2
                details.append(f"Order Block 1H ({ob['low']:.4f}-{ob['high']:.4f})")
                break
    if not details:
        return 0, "Preço longe de suportes no 1H"
    return min(score, 4), " | ".join(details)


def analyze_resistance_1h(
    candles_1h: list[dict],
    current_price: float,
    params: IndicatorParams | None = None,
) -> tuple[int, str]:
    """Detecta resistência (swing high + OB bearish) no 1H para SHORT."""
    p = params if params is not None else IndicatorParams()
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"
    sh, sl = find_swing_points(candles_1h, params=p)
    score, details = 0, []
    if sh:
        for s in reversed(sh):
            dist_pct = (s["price"] - current_price) / current_price * 100
            if 0 < dist_pct <= p.sr_proximity_pct:
                score += 2
                details.append(f"Resistência 1H em {_fmt_price(s['price'])} ({dist_pct:.2f}% acima)")
                break
    obs = detect_order_blocks_bearish(candles_1h, params=p)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = abs(current_price - ob_mid) / current_price * 100
            if dist_pct <= p.ob_proximity_pct:
                score += 2
                details.append(f"OB Bearish 1H ({ob['low']:.4f}-{ob['high']:.4f})")
                break
    if not details:
        return 0, "Preço longe de resistências no 1H"
    return min(score, 4), " | ".join(details)


def analyze_liquidity_zones_4h(
    candles_4h: list[dict],
    current_price: float,
    direction: str = "LONG",
    params: IndicatorParams | None = None,
) -> tuple[int, str]:
    """Detecta zonas de liquidez 4H (suportes/resistências + OBs)."""
    p = params if params is not None else IndicatorParams()
    sh, sl = find_swing_points(candles_4h, params=p)
    score, details = 0, []
    if direction == "SHORT":
        sr_hit = False
        if sh:
            for s in reversed(sh):
                dist_pct = (s["price"] - current_price) / current_price * 100
                if 0 < dist_pct <= p.sr_proximity_pct:
                    score += 1; sr_hit = True
                    details.append(f"Resistência 4H {_fmt_price(s['price'])} ({dist_pct:.2f}% acima)")
                    break
        ob_hit = False
        obs = detect_order_blocks_bearish(candles_4h, params=p)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = abs(current_price - ob_mid) / current_price * 100
                if dist_pct <= p.ob_proximity_pct:
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
                if 0 < dist_pct <= p.sr_proximity_pct:
                    score += 1; sr_hit = True
                    details.append(f"Suporte 4H {s['price']:.4f} ({dist_pct:.2f}%)")
                    break
        ob_hit = False
        obs = detect_order_blocks(candles_4h, params=p)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = (current_price - ob_mid) / current_price * 100
                if -p.ob_proximity_pct <= dist_pct <= p.ob_proximity_pct:
                    score += 1; ob_hit = True
                    details.append(f"OB 4H ({ob['low']:.4f}-{ob['high']:.4f})")
                    break
        if sr_hit and ob_hit:
            score += 1; details.append("Confluência S/R+OB")
    if not details:
        label = "resistências" if direction == "SHORT" else "zonas de liquidez"
        return 0, f"Longe de {label} 4H"
    return min(score, 3), " | ".join(details)


def identify_zona(
    candles_4h: list[dict],
    candles_1h: list[dict],
    current_price: float,
    direction: str,
    params: IndicatorParams | None = None,
) -> tuple[str, str]:
    """Identifica a zona de decisão onde o preço se encontra.

    Retorna (zona_qualidade, zona_descricao).

    Hierarquia SHORT (bearish zones):
      MAXIMA    — preço dentro de OB Bearish 4H E dentro de ZONE_PROXIMITY_PCT% de resistência 4H
      ALTA_OB4H — preço dentro de OB Bearish 4H
      ALTA_OB1H — preço dentro de OB Bearish 1H
      MEDIA     — preço dentro de ZONE_PROXIMITY_PCT% acima de resistência 4H
      BASE      — preço dentro de ZONE_PROXIMITY_PCT% acima de resistência 1H
      NENHUMA   — fora de qualquer zona

    Hierarquia LONG (bullish zones) — espelho simétrico com suportes.
    """
    p = params if params is not None else IndicatorParams()
    if not candles_4h or not candles_1h:
        return "NENHUMA", "Klines insuficientes"

    sh4, sl4 = find_swing_points(candles_4h, params=p)
    sh1, sl1 = find_swing_points(candles_1h, params=p)

    if direction == "SHORT":
        # Verifica OB Bearish 4H (preço dentro do corpo do OB)
        obs_4h_b   = detect_order_blocks_bearish(candles_4h, params=p)
        in_ob_4h   = False
        ob_4h_desc = ""
        for ob in reversed(obs_4h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_4h   = True
                ob_4h_desc = f"OB Bearish 4H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                break

        # Verifica resistência 4H (swing high dentro de zone_proximity_pct% acima)
        near_res_4h  = False
        res_4h_price = 0.0
        if sh4:
            for s in reversed(sh4):
                if s["price"] >= current_price:
                    dist_pct = (s["price"] - current_price) / current_price * 100
                    if dist_pct <= p.zone_proximity_pct:
                        near_res_4h  = True
                        res_4h_price = s["price"]
                        break

        # Verifica OB Bearish 1H
        obs_1h_b   = detect_order_blocks_bearish(candles_1h, params=p)
        in_ob_1h   = False
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
                    if dist_pct <= p.zone_proximity_pct:
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
        obs_4h_b   = detect_order_blocks(candles_4h, params=p)
        in_ob_4h   = False
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
                    if dist_pct <= p.zone_proximity_pct:
                        near_sup_4h  = True
                        sup_4h_price = s["price"]
                        break

        # OB Bullish 1H
        obs_1h_b   = detect_order_blocks(candles_1h, params=p)
        in_ob_1h   = False
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
                    if dist_pct <= p.zone_proximity_pct:
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


_ZONA_SCORE_RICH: dict[str, int] = {
    "MAXIMA": 4, "ALTA_OB4H": 3, "ALTA_OB1H": 3,
    "MEDIA": 2, "BASE": 1, "NENHUMA": 0,
}


def identify_zona_rich(
    candles_4h: list[dict],
    candles_1h: list[dict],
    current_price: float,
    direction: str,
    params: IndicatorParams | None = None,
) -> dict:
    """Versão rica de identify_zona() — mesma lógica, retorna evidências detalhadas.

    identify_zona() original não é alterada — continua sendo a fonte de
    zona_qualidade e zona_descricao para o pipeline de decisão.

    Retorna dict com zona, descricao, score_contribuicao e evidencias.
    """
    p = params if params is not None else IndicatorParams()

    def _build(zona: str, descricao: str, ev: dict) -> dict:
        return {
            "zona"              : zona,
            "descricao"         : descricao,
            "score_contribuicao": _ZONA_SCORE_RICH.get(zona, 0),
            "evidencias"        : ev,
        }

    _empty_ev: dict = {"ob_4h": None, "ob_1h": None, "sr_4h": None, "sr_1h": None}

    if not candles_4h or not candles_1h:
        return _build("NENHUMA", "Klines insuficientes", _empty_ev)

    sh4, sl4 = find_swing_points(candles_4h, params=p)
    sh1, sl1 = find_swing_points(candles_1h, params=p)

    ev_ob_4h: dict | None = None
    ev_ob_1h: dict | None = None
    ev_sr_4h: dict | None = None
    ev_sr_1h: dict | None = None

    if direction == "SHORT":
        # OB Bearish 4H
        obs_4h_b    = detect_order_blocks_bearish(candles_4h, params=p)
        in_ob_4h    = False
        ob_4h_desc  = ""
        found_ob_4h = None
        for ob in reversed(obs_4h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_4h    = True
                ob_4h_desc  = f"OB Bearish 4H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                found_ob_4h = ob
                break
        if found_ob_4h is not None:
            ob_mid   = (found_ob_4h["high"] + found_ob_4h["low"]) / 2
            ev_ob_4h = {
                "low"          : found_ob_4h["low"],
                "high"         : found_ob_4h["high"],
                "impulso_pct"  : found_ob_4h.get("impulso_pct", 0.0),
                "distancia_pct": round(abs(current_price - ob_mid) / current_price * 100, 4),
                "preco_dentro" : found_ob_4h["low"] <= current_price <= found_ob_4h["high"],
            }

        # Resistência 4H
        near_res_4h  = False
        res_4h_price = 0.0
        if sh4:
            for s in reversed(sh4):
                if s["price"] >= current_price:
                    dist_pct = (s["price"] - current_price) / current_price * 100
                    if dist_pct <= p.zone_proximity_pct:
                        near_res_4h  = True
                        res_4h_price = s["price"]
                        break
        if near_res_4h:
            ev_sr_4h = {
                "price"        : res_4h_price,
                "distancia_pct": round(abs(current_price - res_4h_price) / current_price * 100, 4),
                "dentro_zona"  : True,
            }

        # OB Bearish 1H
        obs_1h_b    = detect_order_blocks_bearish(candles_1h, params=p)
        in_ob_1h    = False
        ob_1h_desc  = ""
        found_ob_1h = None
        for ob in reversed(obs_1h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_1h    = True
                ob_1h_desc  = f"OB Bearish 1H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                found_ob_1h = ob
                break
        if found_ob_1h is not None:
            ob_mid   = (found_ob_1h["high"] + found_ob_1h["low"]) / 2
            ev_ob_1h = {
                "low"          : found_ob_1h["low"],
                "high"         : found_ob_1h["high"],
                "impulso_pct"  : found_ob_1h.get("impulso_pct", 0.0),
                "distancia_pct": round(abs(current_price - ob_mid) / current_price * 100, 4),
                "preco_dentro" : found_ob_1h["low"] <= current_price <= found_ob_1h["high"],
            }

        # Resistência 1H
        near_res_1h  = False
        res_1h_price = 0.0
        if sh1:
            for s in reversed(sh1):
                if s["price"] >= current_price:
                    dist_pct = (s["price"] - current_price) / current_price * 100
                    if dist_pct <= p.zone_proximity_pct:
                        near_res_1h  = True
                        res_1h_price = s["price"]
                        break
        if near_res_1h:
            ev_sr_1h = {
                "price"        : res_1h_price,
                "distancia_pct": round(abs(current_price - res_1h_price) / current_price * 100, 4),
                "dentro_zona"  : True,
            }

        ev = {"ob_4h": ev_ob_4h, "ob_1h": ev_ob_1h, "sr_4h": ev_sr_4h, "sr_1h": ev_sr_1h}
        if in_ob_4h and near_res_4h:
            return _build("MAXIMA",    f"{ob_4h_desc} + Res 4H: {_fmt_price(res_4h_price)}", ev)
        if in_ob_4h:
            return _build("ALTA_OB4H", ob_4h_desc, ev)
        if in_ob_1h:
            return _build("ALTA_OB1H", ob_1h_desc, ev)
        if near_res_4h:
            return _build("MEDIA",     f"Resistência 4H: {_fmt_price(res_4h_price)}", ev)
        if near_res_1h:
            return _build("BASE",      f"Resistência 1H: {_fmt_price(res_1h_price)}", ev)
        return _build("NENHUMA", "Fora de zona", ev)

    else:  # LONG
        # OB Bullish 4H
        obs_4h_b    = detect_order_blocks(candles_4h, params=p)
        in_ob_4h    = False
        ob_4h_desc  = ""
        found_ob_4h = None
        for ob in reversed(obs_4h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_4h    = True
                ob_4h_desc  = f"OB Bullish 4H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                found_ob_4h = ob
                break
        if found_ob_4h is not None:
            ob_mid   = (found_ob_4h["high"] + found_ob_4h["low"]) / 2
            ev_ob_4h = {
                "low"          : found_ob_4h["low"],
                "high"         : found_ob_4h["high"],
                "impulso_pct"  : found_ob_4h.get("impulso_pct", 0.0),
                "distancia_pct": round(abs(current_price - ob_mid) / current_price * 100, 4),
                "preco_dentro" : found_ob_4h["low"] <= current_price <= found_ob_4h["high"],
            }

        # Suporte 4H
        near_sup_4h  = False
        sup_4h_price = 0.0
        if sl4:
            for s in reversed(sl4):
                if s["price"] <= current_price:
                    dist_pct = (current_price - s["price"]) / current_price * 100
                    if dist_pct <= p.zone_proximity_pct:
                        near_sup_4h  = True
                        sup_4h_price = s["price"]
                        break
        if near_sup_4h:
            ev_sr_4h = {
                "price"        : sup_4h_price,
                "distancia_pct": round(abs(current_price - sup_4h_price) / current_price * 100, 4),
                "dentro_zona"  : True,
            }

        # OB Bullish 1H
        obs_1h_b    = detect_order_blocks(candles_1h, params=p)
        in_ob_1h    = False
        ob_1h_desc  = ""
        found_ob_1h = None
        for ob in reversed(obs_1h_b[-10:]):
            if ob["low"] <= current_price <= ob["high"]:
                in_ob_1h    = True
                ob_1h_desc  = f"OB Bullish 1H: {_fmt_price(ob['low'])}–{_fmt_price(ob['high'])}"
                found_ob_1h = ob
                break
        if found_ob_1h is not None:
            ob_mid   = (found_ob_1h["high"] + found_ob_1h["low"]) / 2
            ev_ob_1h = {
                "low"          : found_ob_1h["low"],
                "high"         : found_ob_1h["high"],
                "impulso_pct"  : found_ob_1h.get("impulso_pct", 0.0),
                "distancia_pct": round(abs(current_price - ob_mid) / current_price * 100, 4),
                "preco_dentro" : found_ob_1h["low"] <= current_price <= found_ob_1h["high"],
            }

        # Suporte 1H
        near_sup_1h  = False
        sup_1h_price = 0.0
        if sl1:
            for s in reversed(sl1):
                if s["price"] <= current_price:
                    dist_pct = (current_price - s["price"]) / current_price * 100
                    if dist_pct <= p.zone_proximity_pct:
                        near_sup_1h  = True
                        sup_1h_price = s["price"]
                        break
        if near_sup_1h:
            ev_sr_1h = {
                "price"        : sup_1h_price,
                "distancia_pct": round(abs(current_price - sup_1h_price) / current_price * 100, 4),
                "dentro_zona"  : True,
            }

        ev = {"ob_4h": ev_ob_4h, "ob_1h": ev_ob_1h, "sr_4h": ev_sr_4h, "sr_1h": ev_sr_1h}
        if in_ob_4h and near_sup_4h:
            return _build("MAXIMA",    f"{ob_4h_desc} + Sup 4H: {_fmt_price(sup_4h_price)}", ev)
        if in_ob_4h:
            return _build("ALTA_OB4H", ob_4h_desc, ev)
        if in_ob_1h:
            return _build("ALTA_OB1H", ob_1h_desc, ev)
        if near_sup_4h:
            return _build("MEDIA",     f"Suporte 4H: {_fmt_price(sup_4h_price)}", ev)
        if near_sup_1h:
            return _build("BASE",      f"Suporte 1H: {_fmt_price(sup_1h_price)}", ev)
        return _build("NENHUMA", "Fora de zona", ev)


def get_candle_lock_status() -> dict:
    """[v6.3.0 A4] Verifica se o candle 15m atual está fechado e propagado."""
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


def apply_candle_lock(
    candles_15m: list[dict],
    lock: dict,
) -> list[dict]:
    """[v6.3.0 A4] Aplica a trava de candle fechado à lista de klines 15m."""
    if not candles_15m or len(candles_15m) < 2:
        return candles_15m
    if lock["use_prev"]:
        return candles_15m[:-1]
    return candles_15m
