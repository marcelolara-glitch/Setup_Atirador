#!/usr/bin/env python3
"""
telegram_bot.py — Bot Telegram bidirecional
============================================
Dois modos de operação:
  - single-shot (padrão): processa updates pendentes e sai. Compatível com
    GitHub Actions cron (legado) ou chamada manual.
  - daemon (--daemon): long-polling contínuo via getUpdates timeout=60.
    Resposta quase instantânea. Recomendado para Oracle Cloud VM via systemd.

Comandos suportados:
  /ajuda        — lista de comandos
  /status       — último scan e sizing de risco
  /radar        — ranking dos tokens com setas de tendência
  /pilares      — explicação dos pilares do score
  /scan         — dispara workflow_dispatch do scan imediato
  /log_last     — detalhes da última rodada registrada
  /log_token X  — histórico 48h de um token
  /log_quase    — pilares dos QUASEs da última rodada
  /log_calls Nd — lista de CALLs recentes (padrão: 7d)
  /perf         — métricas de performance das CALLs
  /perf_quase   — calibração do threshold via QUASEs
  /trade X      — trade aberto para um símbolo
  /log_export   — exporta scan_log.jsonl via Telegram

Estado persistido em states/bot_state.json (last_update_id).
Log de transações em logs/bot_YYYYMMDD.log.

Variáveis de ambiente necessárias:
  TELEGRAM_TOKEN     — token do bot
  TELEGRAM_CHAT_ID   — chat ID autorizado
  GITHUB_TOKEN       — token para disparar workflow_dispatch
  GITHUB_REPOSITORY  — ex: "marcelolara-glitch/setup_atirador"
"""
import io
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
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

# [v6.6.5] Bancos de dados de observabilidade
_SCAN_DB     = os.path.expanduser("~/Setup_Atirador/logs/scan_log.db")
_SCAN_JSONL  = os.path.expanduser("~/Setup_Atirador/logs/scan_log.jsonl")
_JOURNAL_DB  = os.path.expanduser("~/Setup_Atirador/journal/atirador_journal.db")

# Frequência do scan na VM (minutos)
SCAN_INTERVAL_MIN = 30


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
        {"command": "status",      "description": "Último scan e sizing de risco"},
        {"command": "radar",       "description": "Ranking dos tokens do último scan"},
        {"command": "pilares",     "description": "Explicação dos pilares do score"},
        {"command": "scan",        "description": "Disparar scan imediato"},
        {"command": "analisar",    "description": "Análise completa de um token (ex: /analisar BTCUSDT)"},
        {"command": "log_last",    "description": "Detalhes da última rodada"},
        {"command": "log_token",   "description": "Histórico 48h de um token (ex: /log_token AVAX)"},
        {"command": "log_quase",   "description": "Pilares dos QUASEs da última rodada"},
        {"command": "log_calls",   "description": "CALLs recentes (ex: /log_calls 7d)"},
        {"command": "log_export",  "description": "Exportar scan_log.jsonl"},
        {"command": "perf",        "description": "Performance das CALLs (30d)"},
        {"command": "perf_quase",  "description": "Calibração do threshold via QUASEs"},
        {"command": "trade",       "description": "Trade aberto para um símbolo (ex: /trade AVAX)"},
        {"command": "ajuda",       "description": "Esta mensagem"},
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


# ── DB helpers [v6.6.5] ──────────────────────────────────────────────────────

def _scan_db_conn():
    """Abre conexão read-only com scan_log.db. Retorna None se não existir."""
    if not os.path.exists(_SCAN_DB):
        return None
    try:
        conn = sqlite3.connect(f"file:{_SCAN_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _journal_db_conn():
    """Abre conexão read-only com atirador_journal.db. Retorna None se não existir."""
    if not os.path.exists(_JOURNAL_DB):
        return None
    try:
        conn = sqlite3.connect(f"file:{_JOURNAL_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _fmt_dt(iso: str, fmt: str = "%d/%m %H:%M") -> str:
    """Formata ISO timestamp para exibição em BRT."""
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BRT)
        return dt.astimezone(BRT).strftime(fmt)
    except Exception:
        return iso or "—"


def _fmt_exec(secs: float) -> str:
    """Formata segundos como 'Xmin Ys'."""
    if not secs:
        return "—"
    m = int(secs // 60)
    s = int(secs % 60)
    return f"{m}min {s:02d}s" if m else f"{s}s"


def _tg_send_document(filename: str, content: bytes, caption: str = "") -> bool:
    """Envia arquivo via sendDocument da API Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
            files={"document": (filename, content, "application/octet-stream")},
            timeout=30,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _next_scan_brt() -> str:
    """Calcula horário do próximo scan em BRT (cron a cada SCAN_INTERVAL_MIN)."""
    now = datetime.now(BRT)
    mins_to_add = SCAN_INTERVAL_MIN - (now.minute % SCAN_INTERVAL_MIN)
    if mins_to_add == 0:
        mins_to_add = SCAN_INTERVAL_MIN
    nxt = (now + timedelta(minutes=mins_to_add)).replace(second=0, microsecond=0)
    delta_min = int((nxt - now).total_seconds() / 60)
    return f"~{nxt.strftime('%H:%M')} BRT (em ~{delta_min} min)"


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
        "🤖 <b>ATIRADOR v6.6.5 — Comandos</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/status           — último scan e sizing de risco\n"
        "/radar            — ranking dos tokens do último scan\n"
        "/pilares          — explicação dos pilares do score\n"
        "/scan             — disparar scan imediato\n"
        "/analisar SYMBOL  — análise completa de 1 token\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/log_last         — detalhes da última rodada\n"
        "/log_token SYMBOL — histórico 48h de um token\n"
        "/log_quase        — pilares dos QUASEs da última rodada\n"
        "/log_calls [Nd]   — CALLs recentes (padrão: 7d)\n"
        "/log_export       — exportar scan_log.jsonl\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/perf             — performance das CALLs (30d)\n"
        "/perf_quase       — calibração do threshold\n"
        "/trade SYMBOL     — trade aberto para um símbolo\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/ajuda            — esta mensagem"
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
        f"🤖 <b>ATIRADOR v6.6.5</b> | Status\n"
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


def cmd_analisar(symbol: str | None) -> str:
    """
    Dispara análise individual de um token via workflow_dispatch.
    Retorna mensagem de confirmação ou erro.
    """
    if not symbol:
        return (
            "⚠️ Informe o símbolo do token.\n"
            "Exemplo: /analisar BTCUSDT"
        )

    # Normaliza símbolo
    sym = symbol.upper().strip().replace("/", "").replace("-", "")
    if not sym.endswith("USDT"):
        sym += "USDT"

    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        return "❌ GITHUB_TOKEN ou GITHUB_REPOSITORY não configurados."

    try:
        resp = requests.post(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/workflows/analisar.yml/dispatches",
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"ref": "main", "inputs": {"symbol": sym}},
            timeout=15,
        )
        if resp.status_code in (204, 200):
            return (
                f"🔍 <b>Análise de {sym} disparada!</b>\n"
                f"Resultado completo (score LONG + SHORT, SL, TPs) chega em ~2 min."
            )
        return (
            f"❌ Falha ao disparar análise (HTTP {resp.status_code}).\n"
            "Tente novamente ou acesse o GitHub Actions manualmente."
        )
    except Exception as exc:
        return f"❌ Erro ao disparar análise de {sym}: {exc}"


# ── Comandos de observabilidade [v6.6.5] ─────────────────────────────────────

SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


def cmd_log_last() -> str:
    """Detalhes da última rodada registrada no scan_log.db."""
    conn = _scan_db_conn()
    if not conn:
        return "📋 <b>log_last</b>\n" + SEP + "\n⚠️ scan_log.db não encontrado. Execute pelo menos um scan."
    try:
        row = conn.execute(
            "SELECT * FROM rounds ORDER BY round_id DESC LIMIT 1"
        ).fetchone()
        if not row:
            conn.close()
            return "📋 Nenhuma rodada registrada ainda."
        evs = conn.execute(
            "SELECT type, symbol, direction, score, gap FROM round_events WHERE round_id=?",
            (row["round_id"],)
        ).fetchall()
        conn.close()
    except Exception as e:
        return f"❌ Erro ao consultar scan_log.db: {e}"

    dt_str = _fmt_dt(row["ts"], "%d/%m %H:%M BRT")
    exch   = (row["exchange"] or "—").capitalize()
    cl_ico = "🔒" if row["candle_locked"] else "✅"
    cl_lbl = "Candle OK"

    lines = [
        f"🕐 <b>Última Rodada</b>  {dt_str}",
        SEP,
        f"📡 Exchange    {exch}  |  {cl_ico} {cl_lbl}",
        f"⏱️ Execução   {_fmt_exec(row['exec_secs'])}",
        "📊 Pipeline",
        f"  Universo    {row['univ_count'] or '—'}",
        f"  Gate 4H  →   {row['gate_4h'] or '—'}",
        f"  Gate 1H  →   {row['gate_1h'] or '—'}",
        f"  Score 15m →  {row['scored_15m'] or '—'}",
        SEP,
        f"🌡️ FGI {row['fgi'] or '—'}  |  BTC 4H {row['btc_4h'] or '—'}",
        f"📈 Thr LONG ≥{row['thr_long'] or '—'}  |  📉 Thr SHORT ≥{row['thr_short'] or '—'}",
        SEP,
        "📣 Eventos",
    ]
    if evs:
        for ev in evs:
            ico = "🚀" if ev["direction"] == "LONG" else "📉"
            tag = "CALL" if ev["type"] == "CALL" else "⚠️ QUASE"
            sym = (ev["symbol"] or "").replace("USDT", "")
            lines.append(f"  {ico} {tag} {ev['direction']}   {sym}  {ev['score']}/25")
    else:
        lines.append("  📣 Nenhuma CALL ou QUASE nesta rodada")
    return "\n".join(lines)


def cmd_log_token(symbol: str | None) -> str:
    """Histórico de scores de um token nas últimas 48h."""
    if not symbol:
        return "⚠️ Informe o símbolo.\nEx: /log_token AVAX"
    sym = symbol.upper().strip()
    if not sym.endswith("USDT"):
        sym += "USDT"

    conn = _scan_db_conn()
    if not conn:
        return "⚠️ scan_log.db não encontrado."
    try:
        rows = conn.execute(
            """SELECT ts, direction, score_total, threshold, status
               FROM token_scores
               WHERE symbol=? AND ts >= datetime('now', '-48 hours')
               ORDER BY ts DESC LIMIT 96""",
            (sym,)
        ).fetchall()
        conn.close()
    except Exception as e:
        return f"❌ Erro: {e}"

    base = sym.replace("USDT", "")
    if not rows:
        return f"📈 <b>{base}</b> — sem dados nas últimas 48h."

    # Agrupar por round (ts + par de LONG/SHORT)
    rounds: dict = {}
    for r in rows:
        key = r["ts"][:16]
        if key not in rounds:
            rounds[key] = {}
        rounds[key][r["direction"]] = (r["score_total"], r["status"])

    lines = [f"📈 <b>{base} — Histórico 48h</b>", SEP]
    _status_ico = {"CALL": "✅CALL", "QUASE": "⚠️QUASE", "RADAR": "⬜", "DROP": "⬜"}
    for ts_key in sorted(rounds.keys(), reverse=True)[:24]:
        dt  = _fmt_dt(ts_key + ":00")
        ld  = rounds[ts_key].get("LONG",  (0, ""))
        sd  = rounds[ts_key].get("SHORT", (0, ""))
        l_ico = _status_ico.get(ld[1], "⬜")
        s_ico = _status_ico.get(sd[1], "⬜")
        lines.append(f"  {dt}  LONG {ld[0]:>2} {l_ico}  SHORT {sd[0]:>2} {s_ico}")
    lines += [SEP, f"  Rodadas registradas: {len(rounds)}"]
    return "\n".join(lines)


def cmd_log_quase() -> str:
    """Mostra breakdown dos QUASEs da última rodada. Uma msg por token."""
    conn = _scan_db_conn()
    if not conn:
        return "⚠️ scan_log.db não encontrado."
    try:
        last = conn.execute(
            "SELECT round_id FROM rounds ORDER BY round_id DESC LIMIT 1"
        ).fetchone()
        if not last:
            conn.close()
            return "📋 Nenhuma rodada registrada."
        quases = conn.execute(
            """SELECT * FROM token_scores
               WHERE round_id=? AND status='QUASE'
               ORDER BY score_total DESC""",
            (last["round_id"],)
        ).fetchall()
        conn.close()
    except Exception as e:
        return f"❌ Erro: {e}"

    if not quases:
        return "✅ Nenhum QUASE na última rodada."

    _PILAR_NAMES = [
        ("P1",  "Bollinger",    3), ("P2",  "Volume",       4),
        ("P3",  "Funding",      2), ("P4",  "Liquidez 4H",  3),
        ("P5",  "Figuras 4H",   2), ("P6",  "CHOCH/BOS 4H", 3),
        ("P7",  "Pump/Dump",    0), ("P8",  "TV Recommend",  2),
        ("P9",  "RSI/OI",       2), ("P1H", "Estrutura 1H", 4),
    ]

    msgs = []
    for q in quases:
        q = dict(q)
        sym  = (q["symbol"] or "").replace("USDT", "")
        thr  = q["threshold"] or 0
        sc   = q["score_total"] or 0
        gap  = thr - sc
        dirc = "🚀 LONG" if q["direction"] == "LONG" else "📉 SHORT"
        lines = [
            f"⚠️ <b>QUASE {dirc} {sym}</b>  {sc}/25  (falta {gap})",
            SEP,
        ]
        max_avail = 0
        for key, name, max_pts in _PILAR_NAMES:
            pts     = q.get(f"p{key.lower()}", None) or q.get(key.lower(), None)
            # Try both naming conventions
            col     = f"p{key.lower()}" if key != "P1H" else "p1h"
            pts     = q.get(col)
            reason  = q.get(f"{col}_reason") or ""
            if pts is None:
                continue
            ico = "✅" if (pts or 0) > 0 else "⬜"
            lines.append(f"  {ico} {key:<3} {name:<14} +{pts or 0}/{max_pts}  {reason}")
            if (pts or 0) == 0 and max_pts > 0:
                max_avail += max_pts
        lines += [SEP, f"  → Potencial disponível: +{max_avail} pts"]
        msgs.append("\n".join(lines))

    return "\n\n".join(msgs)


def cmd_log_calls(arg: str | None = None) -> str:
    """Lista CALLs recentes. Argumento: '7d' ou '30d' (padrão: 7d)."""
    days = 7
    if arg:
        m = re.match(r"(\d+)d?", arg.strip())
        if m:
            days = max(1, min(90, int(m.group(1))))

    conn = _journal_db_conn()
    if not conn:
        return "⚠️ atirador_journal.db não encontrado. Execute pelo menos um scan."
    try:
        rows = conn.execute(
            """SELECT timestamp, symbol, direction, score, status, type
               FROM trades
               WHERE is_hypothetical=0 AND timestamp >= datetime('now', ?)
               ORDER BY timestamp DESC LIMIT 50""",
            (f"-{days} days",)
        ).fetchall()
        conn.close()
    except Exception as e:
        return f"❌ Erro: {e}"

    _st_ico = {"WIN_TP1": "✅", "WIN_TP2": "✅", "WIN_TP3": "✅",
               "LOSS_SL": "❌", "EXPIRED": "🟡", "OPEN": "🟡"}
    n_win  = sum(1 for r in rows if (r["status"] or "").startswith("WIN"))
    n_loss = sum(1 for r in rows if r["status"] in ("LOSS_SL", "EXPIRED"))
    n_open = sum(1 for r in rows if r["status"] == "OPEN")

    lines = [f"📋 <b>CALLs — últimos {days}d</b>", SEP]
    for r in rows[:20]:
        dt  = _fmt_dt(r["timestamp"])
        ico = "🚀" if r["direction"] == "LONG" else "📉"
        sym = (r["symbol"] or "").replace("USDT", "")
        st  = _st_ico.get(r["status"], "⬜")
        lines.append(f"  {dt}  {ico} {r['direction']:<5} {sym:<6} {r['score'] or '?'}pts → {st}{r['status'] or '?'}")
    lines += [
        SEP,
        f"  Total: {len(rows)}  |  ✅ {n_win}  ❌ {n_loss}  🟡 {n_open}",
    ]
    return "\n".join(lines)


def cmd_perf() -> str:
    """Métricas de performance das CALLs dos últimos 30 dias."""
    conn = _journal_db_conn()
    if not conn:
        return "⚠️ atirador_journal.db não encontrado."
    try:
        all_rows = conn.execute(
            """SELECT direction, status, pnl_pct FROM trades
               WHERE is_hypothetical=0 AND timestamp >= datetime('now', '-30 days')
               AND status != 'OPEN'"""
        ).fetchall()
        open_ct = conn.execute(
            "SELECT COUNT(*) as n FROM trades WHERE is_hypothetical=0 AND status='OPEN'"
        ).fetchone()["n"]
        conn.close()
    except Exception as e:
        return f"❌ Erro: {e}"

    total = len(all_rows)
    if total < 10:
        return (
            "📊 <b>Performance — CALLs</b>\n" + SEP + "\n"
            f"⚠️ Dados insuficientes para métricas confiáveis ({total} trades fechados)"
        )

    def _metrics(rows):
        n    = len(rows)
        wins = [r for r in rows if (r["status"] or "").startswith("WIN")]
        pnls = [r["pnl_pct"] for r in rows if r["pnl_pct"] is not None]
        wr   = round(len(wins) / n * 100) if n else 0
        sum_w = sum(abs(r["pnl_pct"] or 0) for r in wins)
        sum_l = sum(abs(r["pnl_pct"] or 0) for r in rows if r["status"] in ("LOSS_SL", "EXPIRED"))
        pf    = round(sum_w / sum_l, 2) if sum_l else 0.0
        exp   = round(sum(pnls) / len(pnls), 2) if pnls else 0.0
        return n, len(wins), wr, pf, exp

    longs  = [r for r in all_rows if r["direction"] == "LONG"]
    shorts = [r for r in all_rows if r["direction"] == "SHORT"]
    n_l, w_l, wr_l, pf_l, ex_l = _metrics(longs)
    n_s, w_s, wr_s, pf_s, ex_s = _metrics(shorts)

    bkd: dict = {}
    for r in all_rows:
        s = r["status"] or "UNKNOWN"
        bkd[s] = bkd.get(s, 0) + 1

    lines = [
        "📊 <b>Performance — CALLs</b>", SEP,
        f"  Período      últimos 30d",
        f"  Total calls  {total}",
        f"  Em aberto    {open_ct}",
        SEP,
        "  🚀 LONG",
        f"    Win Rate    {wr_l}%  ({w_l}/{n_l})",
        f"    Profit Fct  {pf_l}",
        f"    Expectancy  {ex_l:+.2f}%",
        "",
        "  📉 SHORT",
        f"    Win Rate    {wr_s}%  ({w_s}/{n_s})",
        f"    Profit Fct  {pf_s}",
        f"    Expectancy  {ex_s:+.2f}%",
        SEP,
    ]
    bkd_line = "  "
    for k, v in sorted(bkd.items()):
        ico = "✅" if k.startswith("WIN") else "❌" if k == "LOSS_SL" else "🟡"
        bkd_line += f"{ico} {k}  {v}  |  "
    lines.append(bkd_line.rstrip("  |  "))
    return "\n".join(lines)


def cmd_perf_quase() -> str:
    """Calibração do threshold via comparação CALL vs QUASE."""
    conn = _journal_db_conn()
    if not conn:
        return "⚠️ atirador_journal.db não encontrado."
    try:
        calls  = conn.execute(
            """SELECT status FROM trades WHERE is_hypothetical=0
               AND timestamp >= datetime('now', '-30 days') AND status != 'OPEN'"""
        ).fetchall()
        quases = conn.execute(
            """SELECT status, score, threshold FROM trades WHERE is_hypothetical=1
               AND timestamp >= datetime('now', '-30 days') AND status != 'OPEN'"""
        ).fetchall()
        conn.close()
    except Exception as e:
        return f"❌ Erro: {e}"

    def _wr(rows):
        n = len(rows)
        if not n:
            return 0, 0
        w = sum(1 for r in rows if (r["status"] or "").startswith("WIN"))
        return round(w / n * 100), n

    wr_c, n_c = _wr(calls)
    wr_q, n_q = _wr(quases)

    # Diagnóstico
    if n_q < 5:
        diag = "⚠️ Dados insuficientes para diagnóstico confiável"
    elif wr_q >= wr_c:
        diag = "→ Threshold pode estar conservador ⚠️"
    elif wr_q >= wr_c * 0.8:
        diag = "→ Threshold atual calibrado ✅"
    else:
        diag = "→ Threshold adequado — QUASEs são mais fracas ✅"

    # Gap médio e estimativa de sinais extras
    gaps = [((r.get("threshold") or 0) - (r.get("score") or 0)) for r in quases if r.get("threshold")]
    avg_gap    = round(sum(gaps) / len(gaps), 1) if gaps else 0.0
    extra_mo   = round(n_q * 30 / 30) if n_q else 0   # n_q é em 30d

    lines = [
        "🔍 <b>Calibração — QUASEs</b>", SEP,
        f"  Total QUASEs  {n_q}",
        f"  Win Rate      {wr_q}%  ({round(wr_q*n_q/100) if n_q else 0}/{n_q})",
        SEP,
        f"  Win Rate CALLs   {wr_c}%",
        f"  Win Rate QUASEs  {wr_q}%",
        f"  {diag}",
        SEP,
        f"  Gap médio até threshold  {avg_gap} pts",
        f"  Se threshold −2: +{extra_mo} sinais/mês (est.)",
    ]
    return "\n".join(lines)


def cmd_trade(symbol: str | None) -> str:
    """Mostra trade aberto para um símbolo."""
    if not symbol:
        return "⚠️ Informe o símbolo.\nEx: /trade AVAX"
    sym = symbol.upper().strip()
    if not sym.endswith("USDT"):
        sym += "USDT"
    base = sym.replace("USDT", "")

    conn = _journal_db_conn()
    if not conn:
        return "⚠️ atirador_journal.db não encontrado."
    try:
        row = conn.execute(
            "SELECT * FROM trades WHERE symbol=? AND status='OPEN' AND is_hypothetical=0 LIMIT 1",
            (sym,)
        ).fetchone()
        conn.close()
    except Exception as e:
        return f"❌ Erro: {e}"

    if not row:
        return f"🔎 Nenhum trade aberto para {base}"

    row = dict(row)
    now = datetime.now(BRT)
    try:
        ts_open  = datetime.fromisoformat(row["timestamp"])
        if ts_open.tzinfo is None:
            ts_open = ts_open.replace(tzinfo=BRT)
        elapsed  = now - ts_open.astimezone(BRT)
        tot_h    = int(elapsed.total_seconds() // 3600)
        tot_m    = int((elapsed.total_seconds() % 3600) // 60)
        elapsed_str = f"{tot_h}h{tot_m:02d}m"
        timeout_h = row.get("timeout_hours", 48)
        rem_secs  = max(0, timeout_h * 3600 - elapsed.total_seconds())
        rem_h     = int(rem_secs // 3600)
        rem_m     = int((rem_secs % 3600) // 60)
        rem_str   = f"{rem_h}h{rem_m:02d}m"
        open_str  = _fmt_dt(row["timestamp"], "%d/%m %H:%M BRT")
    except Exception:
        elapsed_str = "—"; rem_str = "—"; open_str = "—"

    entry = row.get("entry_price") or 0
    sl    = row.get("sl_price")    or 0
    tp1   = row.get("tp1_price")   or 0
    tp2   = row.get("tp2_price")   or 0
    tp3   = row.get("tp3_price")   or 0
    runup = row.get("max_runup")   or 0
    ddwn  = row.get("max_drawdown")or 0
    dirc  = row.get("direction", "")

    def _pct(a, b):
        if not a or not b:
            return "—"
        return f"{(b - a) / a * 100:+.1f}%"

    sl_pct  = _pct(entry, sl)  if dirc == "LONG" else _pct(entry, sl)
    tp1_pct = _pct(entry, tp1) if dirc == "LONG" else _pct(entry, tp1)
    tp2_pct = _pct(entry, tp2) if dirc == "LONG" else _pct(entry, tp2)
    tp3_pct = _pct(entry, tp3) if dirc == "LONG" else _pct(entry, tp3)

    lines = [
        f"🔎 <b>Trade Aberto — {base} {dirc}</b>", SEP,
        f"  Aberto   {open_str}",
        f"  Entrada  ${entry:.4f}",
        f"  Status   🟡 OPEN  (há {elapsed_str})",
        SEP,
        f"  🛑 SL    ${sl:.4f}  ({sl_pct})",
        f"  🎯 TP1   ${tp1:.4f}  ({tp1_pct})",
        f"  🎯 TP2   ${tp2:.4f}  ({tp2_pct})",
        f"  🎯 TP3   ${tp3:.4f}  ({tp3_pct})",
        SEP,
        f"  📈 Max runup     {runup:+.2f}%",
        f"  📉 Max drawdown  {ddwn:.2f}%",
        f"  ⏳ Expira em    {rem_str}",
    ]
    return "\n".join(lines)


def cmd_log_export() -> str:
    """Envia scan_log.jsonl via Telegram sendDocument."""
    if not os.path.exists(_SCAN_JSONL):
        return "⚠️ scan_log.jsonl não encontrado. Execute pelo menos um scan."

    try:
        size = os.path.getsize(_SCAN_JSONL)
        MAX_BYTES = 50 * 1024 * 1024  # 50MB

        if size <= MAX_BYTES:
            with open(_SCAN_JSONL, "rb") as f:
                content = f.read()
            filename = "scan_log.jsonl"
        else:
            # Últimas 1.000 linhas como arquivo temporário
            with open(_SCAN_JSONL, "r", encoding="utf-8") as f:
                lines = f.readlines()
            tail = lines[-1000:]
            content = "".join(tail).encode("utf-8")
            filename = "scan_log_tail1000.jsonl"

        ok = _tg_send_document(filename, content, caption=f"scan_log — {_fmt_dt(datetime.now(BRT).isoformat())}")
        return "" if ok else "❌ Falha ao enviar arquivo."
    except Exception as e:
        return f"❌ Erro ao exportar: {e}"


# ── Polling principal ─────────────────────────────────────────────────────────

HANDLERS = {
    "/ajuda":       cmd_ajuda,
    "/help":        cmd_ajuda,
    "/status":      cmd_status,
    "/radar":       cmd_radar,
    "/scan":        cmd_scan,
    "/pilares":     cmd_pilares,
    "/log_last":    cmd_log_last,
    "/log_quase":   cmd_log_quase,
    "/perf":        cmd_perf,
    "/perf_quase":  cmd_perf_quase,
    # comandos com argumento obrigatório tratados separadamente no loop:
    # /analisar, /log_token, /log_calls, /trade, /log_export
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

        # Comandos com argumento opcional tratados separadamente
        parts = text.strip().split(maxsplit=1)
        arg   = parts[1] if len(parts) > 1 else None

        if command == "/analisar":
            response = cmd_analisar(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /analisar {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/log_token":
            response = cmd_log_token(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /log_token {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/log_calls":
            response = cmd_log_calls(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /log_calls {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/trade":
            response = cmd_trade(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /trade {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/log_export":
            response = cmd_log_export()
            if response:    # vazio = documento já enviado via sendDocument
                _tg_send(response)
            _log(f"{_ts()} | /log_export → {'enviado ✅' if not response else 'falhou'}")
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


def _process_updates(updates: list, bot_state: dict, offset: int) -> int:
    """Processa lista de updates e retorna o novo offset. Usado pelo modo daemon."""
    allowed_chat   = int(TELEGRAM_CHAT_ID)
    last_update_id = offset - 1

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

        parts = text.strip().split(maxsplit=1)
        arg   = parts[1] if len(parts) > 1 else None

        if command == "/analisar":
            response = cmd_analisar(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /analisar {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/log_token":
            response = cmd_log_token(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /log_token {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/log_calls":
            response = cmd_log_calls(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /log_calls {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/trade":
            response = cmd_trade(arg)
            ok = _tg_send(response)
            _log(f"{_ts()} | /trade {arg} → {'enviado ✅' if ok else 'falhou ❌'}")
            continue
        if command == "/log_export":
            response = cmd_log_export()
            if response:
                _tg_send(response)
            _log(f"{_ts()} | /log_export → {'enviado ✅' if not response else 'falhou'}")
            continue

        handler = HANDLERS.get(command)
        if handler is None:
            _tg_send(cmd_ajuda())
            _log(f"{_ts()} | {command} → desconhecido → ajuda enviada")
            continue

        response = handler()
        ok = _tg_send(response)
        _log(f"{_ts()} | {command} → {'enviado ✅' if ok else 'falhou ❌'}")

    bot_state["last_update_id"] = last_update_id
    bot_state["last_run"] = _ts()
    _save_bot_state(bot_state)
    return last_update_id + 1


def main_daemon():
    """Modo daemon: long-polling contínuo com getUpdates timeout=60.
    Resposta quase instantânea. Recomendado para Oracle Cloud VM via systemd."""
    if not TELEGRAM_TOKEN:
        print("⚠  TELEGRAM_TOKEN não definido — bot inativo.")
        sys.exit(1)
    if not TELEGRAM_CHAT_ID:
        print("⚠  TELEGRAM_CHAT_ID não definido — bot inativo.")
        sys.exit(1)

    _tg_register_commands()
    _log(f"{_ts()} | [daemon] iniciado — long-polling ativo")

    bot_state = _load_bot_state()
    offset    = bot_state.get("last_update_id", 0) + 1

    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 60, "limit": 10},
                timeout=70,
            )
            updates = resp.json().get("result", [])
            if updates:
                offset = _process_updates(updates, bot_state, offset)
                print(f"[daemon] Processados {len(updates)} update(s). Offset: {offset - 1}")
        except Exception as exc:
            _log(f"{_ts()} | [daemon] erro: {exc} — aguardando 5s")
            time.sleep(5)


if __name__ == "__main__":
    if "--daemon" in sys.argv:
        main_daemon()
    else:
        main()
