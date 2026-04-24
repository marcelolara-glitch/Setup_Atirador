# telegram.py — Módulo de notificações Telegram para Setup Atirador v9.
#
# Apresentação pura: monta strings HTML a partir de dataclasses tipados e
# despacha via Bot API. Sem I/O persistente (banco, journal) e sem cálculo
# de risco/confidence — tudo chega pronto em SignalDecision / TradePlan /
# RoundStats.

from __future__ import annotations

import html
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from config import (
    TELEGRAM_CONFIG_FILE,
    TELEGRAM_CONFIG_FILE_LEGACY,
    TELEGRAM_HEARTBEAT,
    VERSION,
)
from risk import TradePlan
from signals import SignalDecision

__all__ = [
    "RoundStats",
    "notify_call",
    "notify_error",
    "notify_heartbeat",
    "save_telegram_config",
]

LOG = logging.getLogger("atirador")
_BRT = timezone(timedelta(hours=-3))

_ERROR_MAX_CHARS = 1000

# ---------------------------------------------------------------------------
# Estado privado do módulo — não exportar
# ---------------------------------------------------------------------------
_TELEGRAM_TOKEN: str = ""
_TELEGRAM_CHAT_ID: str = ""


# ---------------------------------------------------------------------------
# Dataclass de entrada — heartbeat
# ---------------------------------------------------------------------------

@dataclass
class RoundStats:
    """Estatísticas consolidadas de uma rodada — consumidas pelo heartbeat.

    Produzida por main.py v9 ao fim de cada rodada. ``setups_counter``
    conta tokens distintos por setup considerando todos os setups
    triggered (inclusive os que viraram near-miss).
    """

    n_universe: int
    n_regime_eligible: int
    n_triggered: int
    n_calls: int
    n_near_miss: int
    setups_counter: dict
    fgi: Optional[int]
    btc_regime: str
    btc_change_pct_15m: float
    elapsed_seconds: float
    exchange: str


# ---------------------------------------------------------------------------
# Configuração — preservada do v8
# ---------------------------------------------------------------------------

def _ensure_config() -> None:
    """Garante que token e chat_id estão carregados."""
    global _TELEGRAM_TOKEN, _TELEGRAM_CHAT_ID
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        _TELEGRAM_TOKEN, _TELEGRAM_CHAT_ID = _load_telegram_config()


def _load_telegram_config() -> tuple[str, str]:
    for cfg_path, is_legacy in [
        (TELEGRAM_CONFIG_FILE, False),
        (TELEGRAM_CONFIG_FILE_LEGACY, True),
    ]:
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, "r") as f:
                    cfg = json.load(f)
                token   = cfg.get("telegram_token", "")
                chat_id = cfg.get("telegram_chat_id", "")
                if token and chat_id:
                    if is_legacy:
                        _migrate_telegram_config(token, chat_id, cfg_path)
                    return token, chat_id
            except Exception:
                pass
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    return token, chat_id


def _migrate_telegram_config(token: str, chat_id: str, source_path: str) -> None:
    try:
        cfg = {
            "telegram_token"  : token,
            "telegram_chat_id": chat_id,
            "migrated_from"   : source_path,
            "migrated_at"     : datetime.now(timezone.utc).isoformat(),
        }
        with open(TELEGRAM_CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception:
        pass


def save_telegram_config(token: str, chat_id: str) -> None:
    config = {
        "telegram_token"  : token,
        "telegram_chat_id": chat_id,
        "telegram_enabled": bool(token and chat_id),
        "created_at"      : datetime.now(timezone.utc).isoformat(),
        "last_updated"    : datetime.now(timezone.utc).isoformat(),
    }
    for path in [TELEGRAM_CONFIG_FILE, TELEGRAM_CONFIG_FILE_LEGACY]:
        try:
            os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
            with open(path, "w") as f:
                json.dump(config, f, indent=2)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Transporte e helpers — preservados do v8
# ---------------------------------------------------------------------------

def _tg_send(text: str) -> bool:
    _ensure_config()
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        return False
    try:
        url  = f"https://api.telegram.org/bot{_TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id"                  : _TELEGRAM_CHAT_ID,
            "text"                     : text,
            "parse_mode"               : "HTML",
            "disable_web_page_preview" : True,
        }, timeout=8)
        if resp.status_code != 200:
            LOG.warning(f"  ⚠️  Telegram: HTTP {resp.status_code} — {resp.text[:80]}")
            return False
        return True
    except Exception as e:
        LOG.warning(f"  ⚠️  Telegram: falha ao enviar — {type(e).__name__}: {e}")
        return False


def _fmt_price(p: float) -> str:
    if p == 0:
        return "0"
    mag = -math.floor(math.log10(abs(p)))
    decimals = max(4, mag + 2)
    return f"{p:.{decimals}f}"


def _tv_links(symbol: str) -> tuple[str, str]:
    """Retorna (link_15m, link_4h) para TradingView."""
    tv_sym   = f"OKX:{symbol}.P"
    link_15m = f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval=15"
    link_4h  = f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval=240"
    return link_15m, link_4h


def _now_brt_str() -> str:
    return datetime.now(_BRT).strftime("%d/%m %H:%M")


# ---------------------------------------------------------------------------
# Formatação pura — retornam strings; não fazem I/O
# ---------------------------------------------------------------------------

def _fmt_call(decision: SignalDecision) -> str:
    """Monta a mensagem HTML de uma CALL a partir do SignalDecision."""
    tp  = decision.trade_plan
    dir_ = tp.direction

    ico = "🟢" if dir_ == "LONG" else "🔴"

    setup_display = (decision.signal_tag or "").replace("+", " + ")
    regime_safe   = html.escape(decision.regime)
    setup_safe    = html.escape(setup_display)
    symbol_safe   = html.escape(decision.symbol)

    leverage_str   = f"{tp.leverage:.1f}x"

    if dir_ == "LONG":
        sign_sl, sign_tp = "-", "+"
    else:
        sign_sl, sign_tp = "+", "-"

    split = tp.position_split
    pct1 = int(round(split[0] * 100))
    pct2 = int(round(split[1] * 100))
    pct3 = int(round(split[2] * 100))

    link_15m, link_4h = _tv_links(decision.symbol)

    lines = [
        f"{ico} {dir_} CALL — {symbol_safe} — {_now_brt_str()} BRT",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📊 Setup: {setup_safe}",
        f"   Regime: {regime_safe} · Leverage: {leverage_str}",
    ]

    # Conditions detalhadas por setup triggered (diagnóstico v9.1)
    for setup_result in decision.all_setup_results:
        if not setup_result.triggered:
            continue
        lines.append("")
        setup_name_safe = html.escape(setup_result.setup_name)
        lines.append(f"✅ <b>{setup_name_safe}</b>:")
        conditions = setup_result.evidence.get("conditions", [])
        for cond in conditions:
            name = html.escape(str(cond.get("name", "?")))
            passed_mark = "✓" if cond.get("passed") else "✗"
            value = float(cond.get("value", 0.0))
            threshold = float(cond.get("threshold", 0.0))
            operator = html.escape(str(cond.get("operator", ">")))
            lines.append(
                f"  • {name} {passed_mark} ({value:.2f} {operator} {threshold:.2f})"
            )

    lines.extend([
        "",
        f"📈 Níveis (ATR {_fmt_price(tp.atr_value)})",
        f"   Entrada : {_fmt_price(tp.entry_price)}",
        f"   SL      : {_fmt_price(tp.sl_price)}  ({sign_sl}{tp.sl_distance_pct:.2f}%)",
        f"   TP1 {pct1}% : {_fmt_price(tp.tp1_price)}  ({sign_tp}{tp.tp1_distance_pct:.2f}%  R:R {tp.risk_reward_tp1:.2f})",
        f"   TP2 {pct2}% : {_fmt_price(tp.tp2_price)}  ({sign_tp}{tp.tp2_distance_pct:.2f}%  R:R {tp.risk_reward_tp2:.2f})",
        f"   TP3 {pct3}% : {_fmt_price(tp.tp3_price)}  ({sign_tp}{tp.tp3_distance_pct:.2f}%  R:R {tp.risk_reward_tp3:.2f})",
        "",
        "🛡 Gestão dinâmica",
        "   TP1 hit → SL move para BE",
        "   TP2 hit → SL move para TP1",
        f"   TP3 = runner ({pct3}% mantido)",
        "",
        f"🔗 <a href=\"{link_15m}\">15m</a> · <a href=\"{link_4h}\">4H</a>",
    ])
    return "\n".join(lines)


def _fmt_heartbeat(stats: RoundStats) -> str:
    """Monta o heartbeat de fim de rodada."""
    if stats.setups_counter:
        items = sorted(
            stats.setups_counter.items(),
            key=lambda kv: (-kv[1], kv[0]),
        )
        setups_line = " · ".join(f"{html.escape(str(name))}:{int(count)}" for name, count in items)
    else:
        setups_line = "—"

    fgi_str = "—" if stats.fgi is None else str(int(stats.fgi))

    pct = stats.btc_change_pct_15m
    if math.isnan(pct) or math.isinf(pct):
        btc_pct_str = "—"
    else:
        sign = "+" if pct >= 0 else ""
        btc_pct_str = f"{sign}{pct:.2f}%"
    btc_regime_safe = html.escape(stats.btc_regime)

    exchange_safe = html.escape(stats.exchange)

    lines = [
        f"💓 Atirador v{VERSION} — {_now_brt_str()} BRT",
        "━━━━━━━━━━━━━━━━━━━",
        f"🌍 Universo       : {stats.n_universe} tokens",
        f"🎯 Regime OK      : {stats.n_regime_eligible} tokens",
        f"⚡ Com setup      : {stats.n_triggered} tokens",
        f"📤 CALLs emitidos : {stats.n_calls} · Near-miss: {stats.n_near_miss}",
        f"🎪 Setups         : {setups_line}",
        "━━━━━━━━━━━━━━━━━━━",
        f"📊 FGI: {fgi_str} · BTC: {btc_regime_safe} ({btc_pct_str})",
        f"⏱ Exec: {int(round(stats.elapsed_seconds))}s · Exchange: {exchange_safe}",
    ]
    return "\n".join(lines)


def _fmt_error(title: str, error_message: str, context: Optional[dict] = None) -> str:
    """Monta mensagem de erro crítico. Trunca detalhes em 1000 chars."""
    title_safe = html.escape(title)

    msg = error_message if isinstance(error_message, str) else str(error_message)
    if len(msg) > _ERROR_MAX_CHARS:
        msg = msg[:_ERROR_MAX_CHARS] + "\n... [truncado]"
    msg_safe = html.escape(msg)

    lines = [
        "🚨 ERRO — Setup Atirador",
        "━━━━━━━━━━━━━━━━━━━",
        f"📍 Título: {title_safe}",
        f"⏰ Timestamp: {_now_brt_str()} BRT",
        "",
        "💥 Detalhes:",
        f"<pre>{msg_safe}</pre>",
    ]

    if context:
        lines.append("")
        lines.append("🔍 Contexto:")
        for k, v in context.items():
            lines.append(f"  {html.escape(str(k))}: {html.escape(str(v))}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# API pública — três notificações tipadas
# ---------------------------------------------------------------------------

def notify_call(decision: SignalDecision) -> bool:
    """Envia mensagem de CALL. Retorna True se enviada com sucesso."""
    if getattr(decision, "action", None) != "CALL":
        return False
    if getattr(decision, "trade_plan", None) is None:
        LOG.warning("  ⚠️  notify_call: SignalDecision.action=CALL sem trade_plan — ignorado")
        return False

    _ensure_config()
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        LOG.debug("  📵  Telegram não configurado — notify_call ignorado")
        return False

    try:
        text = _fmt_call(decision)
    except Exception as e:
        LOG.warning(f"  ⚠️  notify_call: falha ao formatar — {type(e).__name__}: {e}")
        return False

    ok = _tg_send(text)
    if ok:
        LOG.info(f"  📲  Telegram CALL {decision.trade_plan.direction} {decision.symbol}: enviado ✅")
    return ok


def notify_heartbeat(stats: RoundStats) -> bool:
    """Envia heartbeat de fim de rodada. No-op se TELEGRAM_HEARTBEAT=False."""
    if not TELEGRAM_HEARTBEAT:
        LOG.debug("  📵  TELEGRAM_HEARTBEAT=False — notify_heartbeat ignorado")
        return False

    _ensure_config()
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        LOG.debug("  📵  Telegram não configurado — notify_heartbeat ignorado")
        return False

    try:
        text = _fmt_heartbeat(stats)
    except Exception as e:
        LOG.warning(f"  ⚠️  notify_heartbeat: falha ao formatar — {type(e).__name__}: {e}")
        return False

    ok = _tg_send(text)
    if ok:
        LOG.info("  📲  Telegram heartbeat: enviado ✅")
    return ok


def notify_error(
    title: str,
    error_message: str,
    context: Optional[dict] = None,
) -> bool:
    """Envia alerta de erro crítico. Independente de estado de rodada."""
    _ensure_config()
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        LOG.warning("  ⚠️  notify_error: Telegram não configurado — alerta não enviado")
        return False

    try:
        text = _fmt_error(title, error_message, context)
    except Exception as e:
        LOG.warning(f"  ⚠️  notify_error: falha ao formatar — {type(e).__name__}: {e}")
        return False

    ok = _tg_send(text)
    if ok:
        LOG.info(f"  📲  Telegram ERRO «{title}»: enviado ✅")
    return ok
