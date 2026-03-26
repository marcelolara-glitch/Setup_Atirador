#!/usr/bin/env python3
"""
telegram_bot.py — Bot Telegram bidirecional via GitHub Actions polling
======================================================================
Roda a cada 30 min (repo privado) ou 5 min (repo público) via cron do Actions.
Lê getUpdates, processa comandos e responde no chat configurado.

Comandos suportados:
  /ajuda   — lista de comandos
  /status  — último scan e sizing de risco
  /radar   — ranking dos tokens com setas de tendência
  /pilares — explicação dos pilares do score
  /scan    — dispara workflow_dispatch do scan imediato

Estado persistido em states/bot_state.json (last_update_id).
Log de transações em logs/bot_YYYYMMDD.log.

Variáveis de ambiente necessárias:
  TELEGRAM_TOKEN     — token do bot
  TELEGRAM_CHAT_ID   — chat ID autorizado
  GITHUB_TOKEN       — token para disparar workflow_dispatch
  GITHUB_REPOSITORY  — ex: "marcelolara-glitch/setup_atirador"
"""
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import requests

# ── Configuração ──────────────────────────────────────────────────────────────
BRT               = timezone(timedelta(hours=-3))
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
GITHUB_TOKEN      = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")

BOT_STATE_FILE    = "states/bot_state.json"
STATE_FILE        = "states/atirador_state.json"
LOG_DIR           = "logs"

# Cron do scan (UTC): minuto 1, a cada 2 horas — 5,7,9,11,13,15,17,19,21,23,1,3
SCAN_HOURS_UTC = [5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 1, 3]
SCAN_MINUTE    = 1


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_brt() -> datetime:
    return datetime.now(BRT)


def _ts() -> str:
    return _now_brt().strftime("%Y-%m-%d %H:%M BRT")


def _log(line: str):
    """Persiste uma linha no log diário do bot."""
    os.makedirs(LOG_DIR, exist_ok=True)
    fname = os.path.join(LOG_DIR, f"bot_{_now_brt().strftime('%Y%m%d')}.log")
    with open(fname, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


def _tg_register_commands():
    """Registra o menu de comandos visível no Telegram (setMyCommands)."""
    commands = [
        {"command": "status",   "description": "Último scan e sizing de risco"},
        {"command": "radar",    "description": "Ranking dos tokens do último scan"},
        {"command": "pilares",  "description": "Explicação dos pilares do score"},
        {"command": "scan",     "description": "Disparar scan imediato"},
        {"command": "ajuda",    "description": "Esta mensagem"},
    ]
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setMyCommands",
            json={"commands": commands},
            timeout=10,
        )
    except Exception:
        pass


def _tg_send(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _load_bot_state() -> dict:
    try:
        if os.path.exists(BOT_STATE_FILE):
            with open(BOT_STATE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {"last_update_id": 0}


def _save_bot_state(state: dict):
    os.makedirs("states", exist_ok=True)
    with open(BOT_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _load_atirador_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _next_scan_brt() -> str:
    """Calcula horário do próximo scan em BRT."""
    now_utc = datetime.now(timezone.utc)
    # Próxima hora de scan UTC
    for h in sorted(SCAN_HOURS_UTC):
        candidate = now_utc.replace(hour=h, minute=SCAN_MINUTE, second=0, microsecond=0)
        if candidate > now_utc:
            break
    else:
        # Wraparound: menor hora do próximo dia
        min_h = min(SCAN_HOURS_UTC)
        candidate = (now_utc + timedelta(days=1)).replace(
            hour=min_h, minute=SCAN_MINUTE, second=0, microsecond=0
        )

    delta = candidate - now_utc
    mins  = int(delta.total_seconds() / 60)
    brt   = candidate.astimezone(BRT)
    return f"~{brt.strftime('%H:%M')} BRT (em ~{mins} min)"


def _score_trend(hist: list, direction: str) -> str:
    """Calcula seta de tendência igual ao scan principal."""
    key = "long" if direction == "LONG" else "short"
    vals = [e.get(key, 0) for e in hist]
    if len(vals) < 2:
        return "🆕"
    delta = vals[-1] - vals[-2]
    if   delta >= 3:  return "↑↑"
    elif delta >= 1:  return "↑"
    elif delta <= -3: return "↓↓"
    elif delta <= -1: return "↓"
    else:             return "→"


# ── Handlers ──────────────────────────────────────────────────────────────────

def cmd_ajuda() -> str:
    return (
        "🤖 <b>ATIRADOR — Comandos</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/status   — último scan e sizing de risco\n"
        "/radar    — ranking dos tokens do último scan\n"
        "/pilares  — explicação dos pilares do score\n"
        "/scan     — disparar scan imediato\n"
        "/ajuda    — esta mensagem"
    )


def cmd_pilares() -> str:
    return (
        "🎯 <b>PILARES DO SCORE — /25 pts</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n"
        "🏗 <b>ESTRUTURA 4H</b> — até 8 pts\n"
        "<b>P4</b> Liquidez   +3 — preço próximo de Res/OB (confluência = máx)\n"
        "<b>P5</b> Figuras    +2 — padrão gráfico de reversão (Cunha, Wedge)\n"
        "<b>P6</b> CHOCH/BOS  +3 — mudança de estrutura confirmada no 4H\n"
        "\n"
        "🔍 <b>CONFIRMAÇÃO 1H</b> — até 4 pts\n"
        "<b>P-1H</b> Res/OB 1H +4 — preço na zona de res/sup + OB alinhado\n"
        "\n"
        "⚡ <b>GATILHO 15m</b> — até 7 pts\n"
        "<b>P1</b> Bollinger  +3 — preço esticado na banda sup/inf (≥95% = máx)\n"
        "<b>P2</b> Candles    +4 — padrão de reversão 15m (Shooting Star...)\n"
        "\n"
        "🌐 <b>CONTEXTO</b> — até 6 pts\n"
        "<b>P3</b> Funding    +2/-1 — funding favorável/desfavorável à direção\n"
        "<b>P8</b> Volume 15m +2 — volume ≥1.2x média (confirmação)\n"
        "<b>P9</b> OI         +2 — Open Interest crescendo (dinheiro entrando)\n"
        "\n"
        "🚫 <b>FILTRO</b> (não pontua — apenas veta)\n"
        "<b>P7</b> Pump/Dump — bloqueia entrada após variação excessiva recente\n"
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Thr: LONG ≥20 (cauteloso) | SHORT ≥16 (moderado)"
    )


def cmd_status() -> str:
    state = _load_atirador_state()
    sh    = state.get("score_history", {})

    # Último timestamp registrado no histórico
    ultimo_ts = None
    for hist in sh.values():
        if hist:
            ts = hist[-1].get("ts", "")
            if ultimo_ts is None or ts > ultimo_ts:
                ultimo_ts = ts

    if ultimo_ts:
        try:
            dt = datetime.fromisoformat(ultimo_ts).astimezone(BRT)
            ultimo_str = dt.strftime("%d/%m/%Y %H:%M BRT")
        except Exception:
            ultimo_str = ultimo_ts
    else:
        ultimo_str = "—"

    proximo = _next_scan_brt()

    # Sizing (lê constantes padrão — sem importar o módulo principal)
    bankroll    = 100.0
    risco       = 5.0
    margem_max  = 35.0

    return (
        f"🤖 <b>ATIRADOR v6.6.2</b> | Status\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Último scan: {ultimo_str}\n"
        f"⏭  Próximo: {proximo}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 Banca: ${bankroll:.0f}  |  Risco/trade: ${risco:.2f}  |  Margem máx: ${margem_max:.0f}"
    )


def cmd_radar() -> str:
    state = _load_atirador_state()
    sh    = state.get("score_history", {})

    if not sh:
        return "📊 Sem dados de scan disponíveis. Use /scan para atualizar."

    # Última entrada por token
    tokens = []
    for sym, hist in sh.items():
        if not hist:
            continue
        last   = hist[-1]
        ts     = last.get("ts", "")
        score_l = last.get("long", 0)
        score_s = last.get("short", 0)
        trend_l = _score_trend(hist, "LONG")
        trend_s = _score_trend(hist, "SHORT")
        base    = sym.replace("USDT", "").replace("PERP", "")
        tokens.append({
            "sym": base, "ts": ts,
            "long": score_l, "short": score_s,
            "tl": trend_l,   "ts_": trend_s,
        })

    if not tokens:
        return "📊 Sem tokens no histórico. Use /scan para atualizar."

    # Horário do último scan
    ultimo_ts = max(t["ts"] for t in tokens)
    try:
        dt_last = datetime.fromisoformat(ultimo_ts).astimezone(BRT)
        last_str = dt_last.strftime("%H:%M BRT")
    except Exception:
        last_str = ultimo_ts

    top_long  = sorted(tokens, key=lambda x: x["long"],  reverse=True)[:5]
    top_short = sorted(tokens, key=lambda x: x["short"], reverse=True)[:5]

    lines  = [f"📊 <b>RADAR</b> — último scan {last_str}"]
    lines += ["━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    lines += ["📈 <b>LONG — top 5:</b>"]
    for t in top_long:
        lines.append(f"  · {t['sym']:<6} {t['long']:>2} {t['tl']}")
    lines += ["━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    lines += ["📉 <b>SHORT — top 5:</b>"]
    for t in top_short:
        lines.append(f"  · {t['sym']:<6} {t['short']:>2} {t['ts_']}")
    lines += ["━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    lines += ["⚠️ Scores do último scan — use /scan para atualizar"]

    return "\n".join(lines)


def cmd_scan() -> str:
    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        return "❌ GITHUB_TOKEN ou GITHUB_REPOSITORY não configurados."
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/workflows/scan.yml/dispatches",
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"ref": "main"},
            timeout=15,
        )
        if resp.status_code in (204, 200):
            return (
                "⚡ <b>Scan disparado!</b>\n"
                "O heartbeat com os resultados chega em ~2 min."
            )
        return (
            f"❌ Falha ao disparar scan (HTTP {resp.status_code}).\n"
            "Tente novamente ou acesse o GitHub Actions manualmente."
        )
    except Exception as exc:
        return f"❌ Erro ao disparar scan: {exc}"


# ── Polling principal ─────────────────────────────────────────────────────────

HANDLERS = {
    "/ajuda":    cmd_ajuda,
    "/help":     cmd_ajuda,
    "/status":   cmd_status,
    "/radar":    cmd_radar,
    "/scan":     cmd_scan,
    "/pilares":  cmd_pilares,
}


def _extract_command(text: str) -> str | None:
    """Extrai /comando do texto, ignorando @botname."""
    if not text or not text.startswith("/"):
        return None
    m = re.match(r"^(/\w+)(?:@\w+)?", text.strip())
    return m.group(1).lower() if m else None


def main():
    if not TELEGRAM_TOKEN:
        print("⚠  TELEGRAM_TOKEN não definido — bot inativo.")
        sys.exit(0)
    if not TELEGRAM_CHAT_ID:
        print("⚠  TELEGRAM_CHAT_ID não definido — bot inativo.")
        sys.exit(0)

    _tg_register_commands()

    bot_state = _load_bot_state()
    offset    = bot_state.get("last_update_id", 0) + 1

    # Busca updates pendentes
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": offset, "timeout": 5, "limit": 100},
            timeout=15,
        )
        data = resp.json()
    except Exception as exc:
        print(f"⚠  getUpdates falhou: {exc}")
        sys.exit(0)

    updates = data.get("result", [])
    last_update_id = offset - 1
    allowed_chat   = int(TELEGRAM_CHAT_ID)

    if not updates:
        bot_state["last_update_id"] = last_update_id
        bot_state["last_run"] = _ts()
        _save_bot_state(bot_state)
        print(f"[bot] Sem updates. Offset: {last_update_id}")
        sys.exit(0)

    for update in updates:
        uid = update.get("update_id", 0)
        if uid > last_update_id:
            last_update_id = uid

        msg = update.get("message") or update.get("edited_message")
        if not msg:
            continue

        chat_id = msg.get("chat", {}).get("id")
        if chat_id != allowed_chat:
            _log(f"{_ts()} | update_id={uid} | chat {chat_id} não autorizado — ignorado")
            continue

        text    = msg.get("text", "")
        command = _extract_command(text)
        if not command:
            _tg_send(cmd_ajuda())
            _log(f"{_ts()} | mensagem sem comando → ajuda enviada")
            continue

        handler = HANDLERS.get(command)
        if handler is None:
            _tg_send(cmd_ajuda())
            _log(f"{_ts()} | {command} → desconhecido → ajuda enviada")
            continue

        response = handler()
        ok = _tg_send(response)
        status = "enviado ✅" if ok else "falhou ❌"
        _log(f"{_ts()} | {command} → {status}")

    # Persiste offset atualizado
    bot_state["last_update_id"] = last_update_id
    bot_state["last_run"] = _ts()
    _save_bot_state(bot_state)
    print(f"[bot] Processados {len(updates)} update(s). Offset salvo: {last_update_id}")


if __name__ == "__main__":
    main()
