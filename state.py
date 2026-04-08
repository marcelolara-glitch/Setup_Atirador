# state.py — Persistência de estado do Setup Atirador v8
# Responsável por carregar, salvar e atualizar o estado diário do scanner.

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

from config import (
    STATE_FILE,
    SCORE_HISTORY_MAX_ROUNDS,
    SCORE_HISTORY_TTL_H,
    BRT,
)

LOG = logging.getLogger("atirador")


def load_daily_state() -> dict:
    """Carrega estado do arquivo JSON. Retorna estado inicial limpo se não existir.

    Migração automática (Bug #4): se state["oi_history"] existir (schema antigo),
    copia os dados para state["score_history"][symbol]["oi_history"] e remove a
    chave obsoleta.
    """
    default = {
        "date": datetime.now(BRT).strftime("%Y-%m-%d"),
        "score_history": {},
    }
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        if not os.path.exists(STATE_FILE):
            save_daily_state(default)
            return default
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        if "score_history" not in state:
            state["score_history"] = {}
        # Migração Bug #4: mover oi_history de chave de topo para dentro de score_history
        if "oi_history" in state:
            for symbol, oi_data in state["oi_history"].items():
                if symbol in state["score_history"]:
                    sh_entry = state["score_history"][symbol]
                    if isinstance(sh_entry, dict) and not sh_entry.get("oi_history"):
                        sh_entry["oi_history"] = oi_data
            del state["oi_history"]
            save_daily_state(state)
            LOG.info("[state] Migração de schema oi_history concluída")
        return state
    except Exception as e:
        LOG.warning(f"[state] Erro ao carregar estado: {e}")
        return default


def save_daily_state(state: dict) -> None:
    """Salva estado no arquivo JSON. Falha silenciosa com LOG.warning."""
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        LOG.warning(f"[state] Erro ao salvar estado: {e}")


def update_score_history(state: dict, results: list[dict], ts: str) -> None:
    """Atualiza histórico de scores e OI por símbolo.

    Schema v8: state["score_history"][symbol] = {
        "scores":     [{"ts": ..., "long": ..., "short": ..., ...}, ...],
        "oi_history": [{"ts": ..., "oi_usd": ...}, ...],
    }
    Corrige Bug #4: oi_history gravado dentro de score_history[symbol],
    não em chave separada de topo.
    """
    sh = state.setdefault("score_history", {})
    for r in results:
        sym = r.get("symbol", "")
        if not sym:
            continue
        entry = sh.setdefault(sym, {"scores": [], "oi_history": []})
        if "scores" not in entry:
            entry["scores"] = []
        if "oi_history" not in entry:
            entry["oi_history"] = []

        # Atualiza oi_history
        oi_usd = r.get("oi_usd", 0)
        entry["oi_history"].append({"ts": ts, "oi_usd": oi_usd})
        entry["oi_history"] = entry["oi_history"][-SCORE_HISTORY_MAX_ROUNDS:]

        # Atualiza scores: coleta campos numéricos de score presentes no resultado
        score_entry: dict[str, Any] = {"ts": ts}
        for field in ("long", "short", "score_long", "score_short"):
            if field in r:
                score_entry[field] = r[field]
        entry["scores"].append(score_entry)
        entry["scores"] = entry["scores"][-SCORE_HISTORY_MAX_ROUNDS:]


def cleanup_score_history(state: dict) -> None:
    """Remove entradas mais antigas que SCORE_HISTORY_TTL_H horas."""
    sh = state.get("score_history", {})
    agora = datetime.now(BRT)
    for sym in list(sh.keys()):
        entry = sh[sym]
        if not isinstance(entry, dict):
            del sh[sym]
            continue
        # Determina timestamp da última entrada (prefere scores, fallback oi_history)
        last_ts_str = None
        for key in ("scores", "oi_history"):
            hist = entry.get(key, [])
            if hist and isinstance(hist[-1], dict):
                last_ts_str = hist[-1].get("ts")
                break
        if not last_ts_str:
            del sh[sym]
            continue
        try:
            last_ts = datetime.fromisoformat(last_ts_str)
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=BRT)
            age_h = (agora - last_ts).total_seconds() / 3600
            if age_h > SCORE_HISTORY_TTL_H:
                del sh[sym]
        except Exception:
            del sh[sym]


def get_score_trend(state: dict, symbol: str, direction: str = "LONG") -> str:
    """Retorna tendência de score do símbolo nas últimas rounds.

    Retorna: "UP", "DOWN", "STABLE", "INSUFFICIENT_DATA"
    """
    field = "long" if direction == "LONG" else "short"
    sym_entry = state.get("score_history", {}).get(symbol)
    if not isinstance(sym_entry, dict):
        return "INSUFFICIENT_DATA"
    scores = sym_entry.get("scores", [])
    if len(scores) < 2:
        return "INSUFFICIENT_DATA"
    last = scores[-1].get(field)
    prev = scores[-2].get(field)
    if last is None or prev is None:
        return "INSUFFICIENT_DATA"
    delta = last - prev
    if delta > 0:
        return "UP"
    elif delta < 0:
        return "DOWN"
    else:
        return "STABLE"
