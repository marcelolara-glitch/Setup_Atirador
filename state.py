"""state.py — Persistência de estado do Setup Atirador v9.

Camada de persistência alinhada à arquitetura multi-setup da v9. Substitui
completamente o ``state.py`` do v8: schema é conceitualmente diferente
(``setups_history`` com snapshots de :class:`SignalDecision` ao invés de
``score_history`` com score 0-25), e não migra dados legados.

Arquivo alvo: :data:`STATE_FILE_V9` (``~/Setup_Atirador/states/atirador_state_v9.json``).

Schema do state (v9.0.0-beta)::

    {
      "version": "9.0.0-beta",
      "date": "YYYY-MM-DD",
      "last_run": "<iso BRT>" | null,
      "setups_history": {
          "BTCUSDT": {
              "rounds": [
                  {
                      "ts": "<iso BRT>",
                      "regime": "TREND_UP",
                      "action": "CALL" | "SKIP",
                      "direction": "LONG" | "SHORT" | null,
                      "signal_tag": "cont_pull" | "a+b" | null,
                      "confluent_setups": ["cont_pull", ...],
                      "confidence": 82.5,
                      "skip_reason": null | "no_setup_triggered" | ...
                  },
                  ...
              ]
          }
      },
      "oi_history": {
          "BTCUSDT": [
              {"ts": "<iso BRT>", "oi_usd": 12345678.0},
              ...
          ]
      }
    }

Princípios:
    - Falhas silenciosas em I/O (nunca crasha o scan por problema de estado).
    - Sem migrações — v9 começa do zero e rejeita schemas antigos.
    - ``setups_history`` guarda só o snapshot leve; ``all_setup_results`` e
      ``trade_plan`` vão para o logger, não para o state.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Iterable

from config import (
    BRT,
    OI_HISTORY_MAX_ROUNDS,
    SETUPS_HISTORY_MAX_ROUNDS,
    STATE_FILE_V9,
    STATE_TTL_HOURS,
)

LOG = logging.getLogger("atirador")

STATE_VERSION = "9.0.0-beta"


def _default_state() -> dict:
    """Retorna o dict de estado inicial para v9 (arquivo ausente/corrompido)."""
    return {
        "version": STATE_VERSION,
        "date": datetime.now(BRT).strftime("%Y-%m-%d"),
        "last_run": None,
        "setups_history": {},
        "oi_history": {},
    }


def load_state() -> dict:
    """Carrega o estado v9 do disco ou retorna o default.

    - Se ``STATE_FILE_V9`` não existir: retorna default (sem gravar).
    - Se existir mas o JSON estiver corrompido: loga warning e retorna default.
    - Se detectar schema v8 (presença da chave ``score_history``): loga erro
      claro e retorna default. Não migra dados legados.
    - Se ``version`` ausente ou incompatível com v9.*: loga warning e segue,
      reforçando apenas a robustez estrutural (chaves esperadas presentes).

    Nunca levanta exceção.
    """
    try:
        if not os.path.exists(STATE_FILE_V9):
            return _default_state()
        with open(STATE_FILE_V9, "r") as f:
            state = json.load(f)
    except Exception as e:
        LOG.warning(f"[state] Erro ao carregar estado v9: {e}")
        return _default_state()

    if not isinstance(state, dict):
        LOG.warning(f"[state] Conteúdo de {STATE_FILE_V9} não é dict — usando default.")
        return _default_state()

    if "score_history" in state:
        LOG.error(
            f"[state] ERRO: arquivo v8 detectado em STATE_FILE_V9. "
            f"Deve estar em STATE_FILE. Usando default."
        )
        return _default_state()

    version = state.get("version", "")
    if not isinstance(version, str) or not version.startswith("9."):
        LOG.warning(
            f"[state] Versão do state incompatível com v9 "
            f"(encontrada: {version!r}). Seguindo com robustez estrutural."
        )

    setups_history = state.get("setups_history")
    if not isinstance(setups_history, dict):
        state["setups_history"] = {}
    oi_history = state.get("oi_history")
    if not isinstance(oi_history, dict):
        state["oi_history"] = {}

    state.setdefault("version", STATE_VERSION)
    state.setdefault("date", datetime.now(BRT).strftime("%Y-%m-%d"))
    state.setdefault("last_run", None)
    return state


def save_state(state: dict) -> None:
    """Grava o estado em ``STATE_FILE_V9``. Falha silenciosa.

    Atualiza ``state['last_run']`` com timestamp BRT ISO antes de persistir.
    Cria o diretório pai se necessário. Erros de I/O vão para LOG.warning.
    """
    try:
        state["last_run"] = datetime.now(BRT).isoformat()
        os.makedirs(os.path.dirname(STATE_FILE_V9), exist_ok=True)
        with open(STATE_FILE_V9, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        LOG.warning(f"[state] Erro ao salvar estado v9: {e}")


def update_setups_history(
    state: dict,
    decisions: Iterable[Any],
    ts: str,
) -> None:
    """Anexa um snapshot de cada :class:`SignalDecision` ao histórico por símbolo.

    Schema do snapshot (subset do ``SignalDecision``)::

        {
            "ts": ts,
            "regime": decision.regime,
            "action": decision.action,            # "CALL" | "SKIP"
            "direction": decision.direction,      # "LONG" | "SHORT" | None
            "signal_tag": decision.signal_tag,    # str | None
            "confluent_setups": list[str],
            "confidence": float,
            "skip_reason": str | None,
        }

    Trunca ``rounds`` para os últimos :data:`SETUPS_HISTORY_MAX_ROUNDS` itens.
    Se uma ``decision`` não tiver ``symbol``, loga warning e pula.
    Mutação in-place — não retorna valor.
    """
    history = state.setdefault("setups_history", {})
    for decision in decisions:
        try:
            symbol = getattr(decision, "symbol", None)
            if not symbol:
                LOG.warning("[state] SignalDecision sem 'symbol' — entrada ignorada.")
                continue
            snapshot = {
                "ts": ts,
                "regime": getattr(decision, "regime", None),
                "action": getattr(decision, "action", None),
                "direction": getattr(decision, "direction", None),
                "signal_tag": getattr(decision, "signal_tag", None),
                "confluent_setups": list(getattr(decision, "confluent_setups", []) or []),
                "confidence": float(getattr(decision, "confidence", 0.0) or 0.0),
                "skip_reason": getattr(decision, "skip_reason", None),
            }
        except Exception as e:
            LOG.warning(f"[state] Erro ao montar snapshot de decision: {e}")
            continue

        entry = history.setdefault(symbol, {"rounds": []})
        rounds = entry.setdefault("rounds", [])
        rounds.append(snapshot)
        entry["rounds"] = rounds[-SETUPS_HISTORY_MAX_ROUNDS:]


def update_oi_history(
    state: dict,
    perpetuals: Iterable[dict],
    ts: str,
) -> None:
    """Anexa uma leitura de OI por símbolo. Trunca para ``OI_HISTORY_MAX_ROUNDS``.

    Cada item de ``perpetuals`` é um dict com pelo menos ``symbol`` e
    ``oi_usd`` (ausência de ``oi_usd`` cai para ``0``). Itens sem ``symbol``
    são ignorados.
    """
    oi_history = state.setdefault("oi_history", {})
    for p in perpetuals:
        try:
            symbol = p.get("symbol")
            if not symbol:
                continue
            oi_usd = float(p.get("oi_usd", 0) or 0)
        except Exception as e:
            LOG.warning(f"[state] Erro ao ler perpetual: {e}")
            continue
        series = oi_history.setdefault(symbol, [])
        series.append({"ts": ts, "oi_usd": oi_usd})
        oi_history[symbol] = series[-OI_HISTORY_MAX_ROUNDS:]


def _last_ts(entries: list[dict] | None) -> datetime | None:
    """Retorna o datetime da última entrada com ``ts`` parseável, ou None."""
    if not entries or not isinstance(entries, list):
        return None
    last = entries[-1]
    if not isinstance(last, dict):
        return None
    ts_str = last.get("ts")
    if not isinstance(ts_str, str):
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BRT)
    return dt


def cleanup_state(state: dict) -> None:
    """Remove símbolos inativos há mais de :data:`STATE_TTL_HOURS` horas.

    Um símbolo é candidato a remoção se a última entrada disponível em
    ``setups_history`` e em ``oi_history`` (o que for mais recente) é mais
    antiga que o TTL. Se presente em apenas um dos dois, usa o que existe.

    Timestamps não-parseáveis são tratados como dados corrompidos: o símbolo
    é removido das duas estruturas. Mutação in-place.
    """
    setups_history = state.setdefault("setups_history", {})
    oi_history = state.setdefault("oi_history", {})
    now = datetime.now(BRT)
    ttl_seconds = STATE_TTL_HOURS * 3600

    all_symbols = set(setups_history.keys()) | set(oi_history.keys())
    for symbol in all_symbols:
        setup_entry = setups_history.get(symbol)
        setup_rounds = setup_entry.get("rounds") if isinstance(setup_entry, dict) else None
        setup_last = _last_ts(setup_rounds)
        oi_last = _last_ts(oi_history.get(symbol))

        candidates = [dt for dt in (setup_last, oi_last) if dt is not None]
        if not candidates:
            setups_history.pop(symbol, None)
            oi_history.pop(symbol, None)
            continue

        newest = max(candidates)
        age = (now - newest).total_seconds()
        if age > ttl_seconds:
            setups_history.pop(symbol, None)
            oi_history.pop(symbol, None)


def get_recent_setups(state: dict, symbol: str, n: int = 5) -> list[dict]:
    """Retorna até ``n`` snapshots mais recentes de ``symbol`` (mais novo primeiro).

    Lista vazia se o símbolo não existir ou não houver ``rounds``.
    Não muta o state.
    """
    entry = state.get("setups_history", {}).get(symbol)
    if not isinstance(entry, dict):
        return []
    rounds = entry.get("rounds")
    if not isinstance(rounds, list) or not rounds:
        return []
    if n <= 0:
        return []
    return list(reversed(rounds[-n:]))


def get_oi_trend(state: dict, symbol: str, lookback: int = 3) -> str:
    """Compara média dos últimos ``lookback`` OIs com os ``lookback`` anteriores.

    Retorna:
        - ``"UP"``                  — média recente > anterior em mais de 3%.
        - ``"DOWN"``                — média recente < anterior em mais de 3%.
        - ``"STABLE"``              — diferença relativa entre -3% e +3%.
        - ``"INSUFFICIENT_DATA"``   — menos de ``2 * lookback`` entradas,
          ou média anterior <= 0 (divisão inválida).

    Não muta o state.
    """
    series = state.get("oi_history", {}).get(symbol)
    if not isinstance(series, list) or len(series) < 2 * lookback:
        return "INSUFFICIENT_DATA"

    try:
        recent_vals = [float(s["oi_usd"]) for s in series[-lookback:]]
        prev_vals = [float(s["oi_usd"]) for s in series[-2 * lookback:-lookback]]
    except (KeyError, TypeError, ValueError):
        return "INSUFFICIENT_DATA"

    prev_avg = sum(prev_vals) / len(prev_vals)
    recent_avg = sum(recent_vals) / len(recent_vals)
    if prev_avg <= 0:
        return "INSUFFICIENT_DATA"

    delta_pct = (recent_avg - prev_avg) / prev_avg * 100
    if delta_pct > 3.0:
        return "UP"
    if delta_pct < -3.0:
        return "DOWN"
    return "STABLE"
