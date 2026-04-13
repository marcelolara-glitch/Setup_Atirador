# telegram.py — Módulo de notificações Telegram para Setup Atirador v8
# Extração pura das funções de formatação e envio de mensagens do monolito v7.

import html
import json
import logging
import math
import os
import requests
from datetime import datetime, timezone, timedelta

from config import (
    TELEGRAM_CONFIG_FILE,
    TELEGRAM_CONFIG_FILE_LEGACY,
    TELEGRAM_HEARTBEAT,
    VERSION,
)

LOG = logging.getLogger("atirador")
_BRT = timezone(timedelta(hours=-3))

# ---------------------------------------------------------------------------
# Estado privado do módulo — não exportar
# ---------------------------------------------------------------------------
_TELEGRAM_TOKEN: str = ""
_TELEGRAM_CHAT_ID: str = ""


def _ensure_config() -> None:
    """Garante que token e chat_id estão carregados."""
    global _TELEGRAM_TOKEN, _TELEGRAM_CHAT_ID
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        _TELEGRAM_TOKEN, _TELEGRAM_CHAT_ID = _load_telegram_config()


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

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
# Utilitários de envio e formatação
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


def _chk(passed: bool) -> str:
    return "✅" if passed else "❌"


# ---------------------------------------------------------------------------
# Formatação de mensagens
# ---------------------------------------------------------------------------

def _fmt_ev(val, suffix: str = "", scale: float = 1.0) -> str:
    """Formata valor de evidência para exibição inline. Retorna '' se None."""
    if val is None:
        return ""
    v = val * scale
    if suffix == '%':
        return f"({v:.0f}%)"
    if suffix == '×':
        return f"({v:.1f}×)"
    if suffix == '/4':
        return f"({int(v)}/4)"
    return f"({v})"


def _fmt_zona_ev(zona_rich: dict | None) -> str:
    """Formata bloco de evidências de zona. Retorna '' se None."""
    if not zona_rich:
        return ""
    ev = zona_rich.get("evidencias", {})
    lines = []
    ob4 = ev.get("ob_4h")
    if ob4:
        lines.append(
            f"   OB 4H: {_fmt_price(ob4['low'])}–{_fmt_price(ob4['high'])}"
            f"  imp {ob4['impulso_pct']}%  dist {ob4['distancia_pct']:.2f}%"
        )
    ob1 = ev.get("ob_1h")
    if ob1:
        lines.append(
            f"   OB 1H: {_fmt_price(ob1['low'])}–{_fmt_price(ob1['high'])}"
            f"  imp {ob1['impulso_pct']}%  dist {ob1['distancia_pct']:.2f}%"
        )
    sr4 = ev.get("sr_4h")
    if sr4:
        lines.append(
            f"   S/R 4H: {_fmt_price(sr4['price'])}"
            f"  dist {sr4['distancia_pct']:.2f}%"
        )
    sr1 = ev.get("sr_1h")
    if sr1:
        lines.append(
            f"   S/R 1H: {_fmt_price(sr1['price'])}"
            f"  dist {sr1['distancia_pct']:.2f}%"
        )
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _tg_call_v7(r: dict, direction: str, fg_val: int) -> str:
    """Mensagem de CALL v7.0.0."""
    sym      = r.get("base_coin") or r["symbol"].replace("USDT", "")
    zona_q   = r.get("zona_qualidade", "?")
    zona_d   = r.get("zona_descricao", "")
    s4h      = r.get("summary_4h", "?")
    s1h      = r.get("summary_1h", "?")
    ca_razao = r.get("check_a_razao", "")
    cb_razao = r.get("check_b_razao", "")
    cc_total = r.get("check_c_total", 0)
    det      = r.get("check_c_detalhes", {})

    _cr      = r.get("candle_ref") or {}
    _cr_ts   = _cr.get("ts") if isinstance(_cr, dict) else None
    _cr_str  = ""
    if _cr_ts:
        try:
            _cr_dt  = datetime.fromtimestamp(_cr_ts / 1000, tz=_BRT)
            _cr_str = f"  [candle: {_cr_dt.strftime('%d/%m %H:%M')} BRT]"
        except Exception:
            pass

    ico = "🔴" if direction == "SHORT" else "🟢"

    t = r.get("trade") or r.get("trade_short")
    link_15m, link_4h = _tv_links(r["symbol"])

    niveis = ""
    if t:
        entry = _fmt_price(t["entry"])
        sl    = _fmt_price(t["sl"])
        tp1   = _fmt_price(t["tp1"])
        tp2   = _fmt_price(t["tp2"])
        tp3   = _fmt_price(t["tp3"])
        slpct = t["sl_distance_pct"]
        if direction == "SHORT":
            sign_sl, sign_tp = "+", "-"
        else:
            sign_sl, sign_tp = "-", "+"
        niveis = (
            f"\n📈 Níveis\n"
            f"   Entrada : {entry}\n"
            f"   SL      : {sl} ({sign_sl}{slpct:.2f}%)\n"
            f"   TP1     : {tp1} ({sign_tp}{slpct:.2f}%)\n"
            f"   TP2     : {tp2} ({sign_tp}{slpct*2:.2f}%)\n"
            f"   TP3     : {tp3} ({sign_tp}{slpct*3:.2f}%)"
        )

    msg = (
        f"{ico} {direction} CALL — {sym}USDT\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Zona: {html.escape(zona_d)} [{zona_q}]\n"
        f"{_fmt_zona_ev(r.get('zona_rich'))}"
        f"\n⚡ Confirmação 15m{_cr_str}\n"
        f"   A — Rejeição: {_chk(True)} {html.escape(ca_razao)}\n"
        f"   B — Estrutura: {_chk(True)} {html.escape(cb_razao)}\n"
        f"   C — Força: {cc_total}/4\n"
        f"      BB: {det.get('c1_bb', '—')}  {_fmt_ev(det.get('c1_bb_pos'), '%', scale=100)}\n"
        f"      Volume: {det.get('c2_vol', '—')}  {_fmt_ev(det.get('c2_vol_ratio'), '×')}\n"
        f"      CVD: {det.get('c3_cvd', '—')}  {_fmt_ev(det.get('c3_cvd_count'), '/4')}\n"
        f"      OI: {det.get('c4_oi', '—')}  {det.get('c4_reason', '')}\n"
        f"\n📊 Contexto\n"
        f"   4H: {s4h} | 1H: {s1h} | FGI: {fg_val}"
        f"{niveis}\n"
        f"\n🔗 <a href=\"{link_15m}\">15m</a> · <a href=\"{link_4h}\">4H</a>"
    )
    vi = r.get("venue_info", {})
    if vi.get("mixed"):
        msg += f"\n⚠️ Venue mista (klines: {vi.get('kline_venue')} | TV: {vi.get('tv_venue')})"
    return msg


def _tg_quase_v7(r: dict, direction: str, fg_val: int) -> str:
    """Mensagem de QUASE v7.0.0."""
    sym      = r["symbol"].replace("USDT", "")
    zona_q   = r.get("zona_qualidade", "?")
    zona_d   = r.get("zona_descricao", "")
    s4h      = r.get("rec_4h", "?")
    s1h      = r.get("rec_1h", "?")
    ca       = r.get("check_a_ok", False)
    ca_razao = r.get("check_a_reason", "")
    cb       = r.get("check_b_ok")
    cb_razao = r.get("check_b_reason", "não avaliado")
    cc_total = r.get("check_c_total", 0) or 0
    det      = r.get("check_c_det", {}) or {}

    _cr      = r.get("candle_ref") or {}
    _cr_ts   = _cr.get("ts") if isinstance(_cr, dict) else None
    _cr_str  = ""
    if _cr_ts:
        try:
            _cr_dt  = datetime.fromtimestamp(_cr_ts / 1000, tz=_BRT)
            _cr_str = f"  [candle: {_cr_dt.strftime('%d/%m %H:%M')} BRT]"
        except Exception:
            pass

    ico = "🟡"
    link_15m, link_4h = _tv_links(r["symbol"])

    cb_line = (
        f"   B — Estrutura: {_chk(cb)} {html.escape(cb_razao or '')}"
        if cb is not None
        else "   B — Estrutura: — (não avaliado — Check A falhou)"
    )

    msg = (
        f"{ico} {direction} QUASE — {sym}USDT\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Zona: {html.escape(zona_d)} [{zona_q}]\n"
        f"{_fmt_zona_ev(r.get('zona_rich'))}"
        f"\n⚡ Confirmação 15m{_cr_str}\n"
        f"   A — Rejeição: {_chk(ca)} {html.escape(ca_razao)}\n"
        f"{cb_line}\n"
        f"   C — Força: {cc_total}/4\n"
        f"      BB: {det.get('c1_bb', '—')}  {_fmt_ev(det.get('c1_bb_pos'), '%', scale=100)}\n"
        f"      Volume: {det.get('c2_vol', '—')}  {_fmt_ev(det.get('c2_vol_ratio'), '×')}\n"
        f"      CVD: {det.get('c3_cvd', '—')}  {_fmt_ev(det.get('c3_cvd_count'), '/4')}\n"
        f"      OI: {det.get('c4_oi', '—')}  {det.get('c4_reason', '')}\n"
        f"\n📊 Contexto\n"
        f"   4H: {s4h} | 1H: {s1h} | FGI: {fg_val}\n"
        f"\n🔗 <a href=\"{link_15m}\">15m</a> · <a href=\"{link_4h}\">4H</a>"
    )
    vi = r.get("venue_info", {})
    if vi.get("mixed"):
        msg += f"\n⚠️ Venue mista (klines: {vi.get('kline_venue')} | TV: {vi.get('tv_venue')})"
    return msg


def _tg_heartbeat_v7(
    n_univ: int,
    n_gate_short: int,
    n_gate_long: int,
    n_zona_short: int,
    n_zona_long: int,
    n_calls: int,
    n_quase: int,
    fg_val: int,
    btc_4h: str,
    elapsed: float,
    exchange: str,
) -> str:
    return (
        f"💓 <b>Atirador v{VERSION}</b> — rodada concluída\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 Universo  : {n_univ} tokens\n"
        f"🔽 Gate 4H   : {n_gate_short} SHORT | {n_gate_long} LONG\n"
        f"🎯 Em zona   : {n_zona_short} SHORT  | {n_zona_long} LONG\n"
        f"⚡ Com sinal : {n_calls} CALL   | {n_quase} QUASE\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 FGI: {fg_val} | BTC 4H: {html.escape(btc_4h)}\n"
        f"⏱ Exec: {elapsed:.0f}s | Exchange: {exchange}"
    )


# ---------------------------------------------------------------------------
# Notificação principal
# ---------------------------------------------------------------------------

def tg_notify_v7(
    results: list[dict],
    fg_val: int,
    n_univ: int,
    n_gate_short: int,
    n_gate_long: int,
    n_zona_short: int,
    n_zona_long: int,
    elapsed: float,
    exchange: str,
    btc_4h: str,
) -> None:
    """Envia heartbeat → QUASEs → CALLs."""
    _ensure_config()
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        LOG.debug("  📵  Telegram não configurado — notificações desativadas")
        return

    calls   = [r for r in results if r.get("status") == "CALL"]
    quases  = [r for r in results if r.get("status") == "QUASE"]
    n_calls = len(calls)
    n_quase = len(quases)
    n_env   = 0

    # 1. Heartbeat
    if TELEGRAM_HEARTBEAT:
        hb = _tg_heartbeat_v7(n_univ, n_gate_short, n_gate_long,
                               n_zona_short, n_zona_long,
                               n_calls, n_quase,
                               fg_val, btc_4h, elapsed, exchange)
        if _tg_send(hb):
            n_env += 1
            LOG.info("  📲  Telegram heartbeat: enviado ✅")

    # 2. QUASEs
    for r in quases:
        msg = _tg_quase_v7(r, r["direction"], fg_val)
        if _tg_send(msg):
            n_env += 1
            LOG.info(f"  📲  Telegram QUASE {r['direction']} {r['symbol']}: enviado ✅")

    # 3. CALLs
    for r in calls:
        msg = _tg_call_v7(r, r["direction"], fg_val)
        if _tg_send(msg):
            n_env += 1
            LOG.info(f"  📲  Telegram CALL {r['direction']} {r.get('base_coin') or r['symbol'].replace('USDT', '')}: enviado ✅")

    total = (1 if TELEGRAM_HEARTBEAT else 0) + n_quase + n_calls
    LOG.info(f"  📲  Telegram: {n_env}/{total} mensagens enviadas")
