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
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta

import requests

# ── Configuração ──────────────────────────────────────────────────────────────
BRT               = timezone(timedelta(hours=-3))
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
GITHUB_TOKEN      = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
BOT_VERSION       = "8.2.1"

BOT_STATE_FILE    = "states/bot_state.json"
STATE_FILE        = "states/atirador_state.json"
LOG_DIR           = "logs"

# Bancos de dados de observabilidade
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
        {"command": "status",        "description": "Saúde do sistema e última rodada"},
        {"command": "scan",          "description": "Disparar scan imediato (GitHub Actions)"},
        {"command": "analisar",      "description": "Análise individual de um token (ex: /analisar BTCUSDT)"},
        {"command": "log_last",      "description": "Detalhes da última rodada"},
        {"command": "log_token",     "description": "Histórico 48h de um token (ex: /log_token AVAX)"},
        {"command": "log_quase",     "description": "Breakdown dos QUASEs da última rodada"},
        {"command": "log_calls",     "description": "CALLs recentes (ex: /log_calls 7d)"},
        {"command": "log_export",    "description": "Exportar scan_log.jsonl"},
        {"command": "health_export", "description": "Relatório completo de saúde + debug"},
        {"command": "perf",          "description": "Métricas das CALLs (30d)"},
        {"command": "perf_quase",    "description": "Qualidade dos sinais QUASE (30d)"},
        {"command": "trade",         "description": "Trade aberto para um símbolo (ex: /trade AVAX)"},
        {"command": "ajuda",         "description": "Lista de comandos"},
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


# ── DB helpers ───────────────────────────────────────────────────────────────

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
    """
    OBJETIVO:
        Listar todos os comandos ativos com descrição de uma linha cada,
        agrupados por categoria. Ser a referência rápida do operador.

    FONTE DE DADOS:
        Nenhuma — texto estático.

    LIMITAÇÕES CONHECIDAS:
        Não valida se os bancos estão ativos. Apenas lista comandos.

    NÃO FAZER:
        Não mencionar comandos removidos (/radar, /pilares).
        Não hardcodar versão — usar BOT_VERSION.
    """
    return (
        f"🤖 <b>ATIRADOR v{BOT_VERSION} — Comandos</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚙️ <b>Sistema</b>\n"
        "/status           — saúde do sistema e última rodada\n"
        "/scan             — disparar scan imediato (GitHub Actions)\n"
        "/analisar SYMBOL  — análise individual de um token\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>Observabilidade</b>\n"
        "/log_last         — detalhes da última rodada\n"
        "/log_token SYMBOL — histórico 48h de um token\n"
        "/log_quase        — breakdown dos QUASEs da última rodada\n"
        "/log_calls [Nd]   — CALLs recentes (ex: /log_calls 7d)\n"
        "/log_export       — exportar scan_log.jsonl\n"
        "/health_export    — relatório completo de saúde + debug\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📊 <b>Performance</b>\n"
        "/perf             — métricas das CALLs (30d)\n"
        "/perf_quase       — qualidade dos sinais QUASE (30d)\n"
        "/trade SYMBOL     — status de trade aberto\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/ajuda            — esta mensagem"
    )



def cmd_status() -> str:
    """
    OBJETIVO:
        Snapshot operacional completo: quando rodou, se está rodando
        no prazo, saúde do logging layer e recursos da VM. Permite
        detectar rapidamente se o sistema está travado, com disco
        cheio ou logging parado.

    FONTE DE DADOS:
        - scan_log.jsonl (último registro: exec_seconds, ts)
        - scan_log.db tabela rounds (contagem, última escrita)
        - atirador_journal.db tabela trades (trades abertos)
        - /proc/meminfo (memória disponível)
        - /proc/loadavg (carga CPU)
        - shutil.disk_usage (disco livre)

    LIMITAÇÕES CONHECIDAS:
        Próximo scan é estimativa baseada no cron */30 — não lê
        o cron real.
        Não detecta se o processo Python travou mid-scan.

    NÃO FAZER:
        Não ler score_history do state.json — dado v6 congelado.
        Não exibir gate_1h — campo legado sem significado na v8.
        Não chamar subprocess.
    """
    lines: list[str] = []

    # ── Seção 1: Operacional ─────────────────────────────────────────────────
    # Último scan: leitura direta da tabela rounds (JSONL tem problema de parse)
    ultimo_ts    = None
    exec_secs    = None
    duracao_str  = "—"
    duracao_icon = ""
    try:
        conn = _scan_db_conn()
        if conn:
            row = conn.execute(
                "SELECT ts, exec_secs FROM rounds ORDER BY round_id DESC LIMIT 1"
            ).fetchone()
            if row:
                ultimo_ts = row["ts"]
                if row["exec_secs"] is not None:
                    exec_secs    = float(row["exec_secs"])
                    duracao_str  = _fmt_exec(exec_secs)
                    duracao_icon = ("✅" if exec_secs < 1200
                                    else "⚠️" if exec_secs <= 1380
                                    else "🔴")
            conn.close()
    except Exception:
        pass

    # Ícone do último scan
    ultimo_str  = "—"
    ultimo_icon = ""
    if ultimo_ts:
        ultimo_str = _fmt_dt(ultimo_ts, "%d/%m %H:%M") + " BRT"
        try:
            dt_last = datetime.fromisoformat(ultimo_ts)
            if dt_last.tzinfo is None:
                dt_last = dt_last.replace(tzinfo=BRT)
            mins_since = (datetime.now(BRT) - dt_last).total_seconds() / 60
            ultimo_icon = ("✅" if mins_since < 35
                           else "⚠️" if mins_since <= 65
                           else "🔴")
        except Exception:
            pass

    # Próximo scan
    now = datetime.now(BRT)
    mins_to_add = SCAN_INTERVAL_MIN - (now.minute % SCAN_INTERVAL_MIN)
    if mins_to_add == 0:
        mins_to_add = SCAN_INTERVAL_MIN
    nxt       = (now + timedelta(minutes=mins_to_add)).replace(second=0, microsecond=0)
    delta_min = max(1, int((nxt - now).total_seconds() / 60))
    proximo_str = f"{nxt.strftime('%d/%m %H:%M')} BRT  (~{delta_min} min)"

    duracao_display = f"{duracao_str}  {duracao_icon}" if duracao_icon else duracao_str
    ultimo_display  = f"{ultimo_str}  {ultimo_icon}"   if ultimo_icon  else ultimo_str

    lines.append(f"🤖 <b>ATIRADOR v{BOT_VERSION}</b> | Status")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🕐 Último scan    {ultimo_display}")
    lines.append(f"⏭️  Próximo        {proximo_str}")
    lines.append(f"⏱️  Duração        {duracao_display}")

    # ── Seção 2: Logging ─────────────────────────────────────────────────────
    rodadas_str         = "—"
    ultima_escrita_str  = "—"
    ultima_escrita_icon = ""
    scan_db_size_str    = "—"
    journal_size_str    = "—"
    trades_abertos_str  = "—"

    try:
        conn = _scan_db_conn()
        if conn:
            row = conn.execute("SELECT COUNT(*) FROM rounds").fetchone()
            if row:
                rodadas_str = str(row[0])
            row2 = conn.execute(
                "SELECT ts FROM rounds ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            if row2 and row2[0]:
                ultima_escrita_str = _fmt_dt(row2[0], "%d/%m %H:%M")
                try:
                    dt_last = datetime.fromisoformat(row2[0])
                    if dt_last.tzinfo is None:
                        dt_last = dt_last.replace(tzinfo=BRT)
                    mins_since = (datetime.now(BRT) - dt_last).total_seconds() / 60
                    ultima_escrita_icon = ("✅" if mins_since <= 35
                                           else "⚠️" if mins_since <= 65
                                           else "🔴")
                except Exception:
                    pass
            conn.close()
    except Exception:
        pass

    try:
        if os.path.exists(_SCAN_DB):
            sz = os.path.getsize(_SCAN_DB)
            scan_db_size_str = (f"{sz/(1024*1024):.1f} MB"
                                if sz >= 1024*1024 else f"{sz//1024} KB")
    except Exception:
        pass

    try:
        if os.path.exists(_JOURNAL_DB):
            sz = os.path.getsize(_JOURNAL_DB)
            journal_size_str = (f"{sz/(1024*1024):.1f} MB"
                                if sz >= 1024*1024 else f"{sz//1024} KB")
    except Exception:
        pass

    try:
        conn = _journal_db_conn()
        if conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE status='OPEN'"
            ).fetchone()
            if row:
                trades_abertos_str = str(row[0])
            conn.close()
    except Exception:
        pass

    ue_display = (f"{ultima_escrita_str}  {ultima_escrita_icon}"
                  if ultima_escrita_icon else ultima_escrita_str)

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("📋 Logging")
    lines.append(f"  Rodadas gravadas   {rodadas_str}")
    lines.append(f"  Última escrita     {ue_display}")
    lines.append(f"  scan_log.db        {scan_db_size_str}")
    lines.append(f"  journal.db         {journal_size_str}")
    lines.append(f"  Trades abertos     {trades_abertos_str}")

    # ── Seção 3: VM ──────────────────────────────────────────────────────────
    disco_str  = "—"
    disco_icon = ""
    try:
        usage      = shutil.disk_usage(os.path.expanduser("~"))
        free_bytes = usage.free
        disco_str  = (f"{free_bytes/(1024**3):.1f} GB"
                      if free_bytes >= 1024**3
                      else f"{free_bytes//(1024**2)} MB")
        disco_icon = ("✅" if free_bytes > 5*1024**3
                      else "⚠️" if free_bytes >= 2*1024**3
                      else "🔴")
    except Exception:
        pass

    mem_str  = "—"
    mem_icon = ""
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    mem_kb   = int(line.split()[1])
                    mem_mb   = mem_kb // 1024
                    mem_str  = f"{mem_mb} MB"
                    mem_icon = ("✅" if mem_mb > 300
                                else "⚠️" if mem_mb >= 150
                                else "🔴")
                    break
    except Exception:
        pass

    load_str = "—"
    try:
        with open("/proc/loadavg") as fh:
            load_str = fh.read().split()[0]
    except Exception:
        pass

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("💾 VM")
    lines.append(f"  Disco livre        {disco_str}  {disco_icon}".rstrip())
    lines.append(f"  Memória livre      {mem_str}  {mem_icon}".rstrip())
    lines.append(f"  CPU (1min avg)     {load_str}")

    return "\n".join(lines)



def cmd_scan() -> str:
    """
    OBJETIVO:
        Disparar um scan imediato via GitHub Actions workflow_dispatch.
        Útil para forçar uma análise fora do ciclo de 30min do cron.

    FONTE DE DADOS:
        GitHub API — workflow_dispatch no arquivo scan.yml.

    LIMITAÇÕES CONHECIDAS:
        O scan roda numa máquina efêmera do GitHub Actions e NÃO alimenta
        os bancos SQLite da VM (scan_log.db, atirador_journal.db).
        O heartbeat que chega após o /scan não representa o estado real
        da VM — é apenas confirmação de execução no Actions.

    NÃO FAZER:
        Não interpretar o resultado como equivalente ao cron da VM.
        Não chamar subprocess nem bloquear o daemon.
    """
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
    OBJETIVO:
        Disparar análise individual de um token via GitHub Actions.
        Permite analisar tokens que não saíram nas rodadas automáticas
        ou validar manualmente um símbolo específico.

    FONTE DE DADOS:
        GitHub API — workflow_dispatch no arquivo analisar.yml.
        O resultado chega via mensagem Telegram pelo próprio workflow.

    LIMITAÇÕES CONHECIDAS:
        Roda numa máquina efêmera do GitHub Actions — NÃO alimenta os
        bancos SQLite da VM. Resultado não aparece em /log_last ou /perf.
        O workflow analisar.yml precisa estar atualizado para a v8.

    NÃO FAZER:
        Não bloquear o daemon aguardando resultado — o retorno é apenas
        confirmação de disparo, não o resultado da análise.
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


# ── Comandos de observabilidade ──────────────────────────────────────────────

SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


def cmd_log_last() -> str:
    """
    OBJETIVO:
        Mostrar o que aconteceu na última rodada completa: contexto
        de mercado, funil de filtragem (universo → gate 4H → 15m)
        e eventos emitidos (CALLs e QUASEs) com direção, símbolo
        e score do Check C.

    FONTE DE DADOS:
        scan_log.db — tabelas `rounds` e `round_events`.

    LIMITAÇÕES CONHECIDAS:
        Não mostra breakdown de checks por token.
        Para breakdown de QUASE use /log_quase.

    NÃO FAZER:
        Não exibir gate_1h — campo legado, sempre igual a gate_4h na v8.
        Não exibir thr_long/thr_short — thresholds do Check C são
        contextuais por zona, não um valor único global.
        Não exibir candle_locked — campo legado v6.
    """
    conn = _scan_db_conn()
    if not conn:
        return ("📋 <b>log_last</b>\n" + SEP +
                "\n⚠️ scan_log.db não encontrado.")
    try:
        row = conn.execute(
            "SELECT * FROM rounds ORDER BY round_id DESC LIMIT 1"
        ).fetchone()
        if not row:
            conn.close()
            return "📋 Nenhuma rodada registrada ainda."
        evs = conn.execute(
            """SELECT type, symbol, direction, check_c_total
               FROM round_events WHERE round_id=?
               ORDER BY type DESC""",
            (row["round_id"],)
        ).fetchall()
        conn.close()
    except Exception as e:
        return f"❌ Erro ao consultar scan_log.db: {e}"

    dt_str = _fmt_dt(row["ts"], "%d/%m %H:%M BRT")
    exch   = (row["exchange"] or "—").upper()

    lines = [
        f"🕐 <b>Última Rodada</b>  {dt_str}",
        SEP,
        f"📡 Exchange    {exch}  |  ⏱️ {_fmt_exec(row['exec_secs'])}",
        f"🌡️ FGI {row['fgi'] or '—'}  |  BTC 4H {row['btc_4h'] or '—'}",
        SEP,
        "📊 Funil",
        f"  Universo    {row['univ_count'] or '—'}",
        f"  Gate 4H  →  {row['gate_4h'] or '—'}",
        f"  Score 15m → {row['scored_15m'] or '—'}",
        SEP,
        "📣 Eventos",
    ]
    if evs:
        for ev in evs:
            ico = "🚀" if ev["direction"] == "LONG" else "📉"
            tag = "CALL" if ev["type"] == "CALL" else "⚠️ QUASE"
            sym = (ev["symbol"] or "").replace("USDT", "")
            lines.append(f"  {ico} {tag} {ev['direction']}  {sym}  C={ev['check_c_total']}")
    else:
        lines.append("  Nenhuma CALL ou QUASE nesta rodada")
    return "\n".join(lines)


def cmd_log_token(symbol: str | None) -> str:
    """
    OBJETIVO:
        Mostrar o histórico de aparições de um token nas últimas 48h,
        com score do Check C e status (CALL, QUASE, RADAR, DROP) por rodada.
        Permite acompanhar a evolução de um token ao longo do tempo.

    FONTE DE DADOS:
        scan_log.db — tabela token_scores, campos: ts, direction,
        score_total, threshold, status. Filtro: últimas 48h, limit 96.

    LIMITAÇÕES CONHECIDAS:
        score_total na v8 é o Check C (0–4), não score de pilares (0–25).
        Colunas p1..p9 existem na tabela mas são NULL na v8 — não ler.
        Só aparecem tokens que chegaram ao 15m (passaram pelo gate 4H
        e foram encontrados em zona).

    NÃO FAZER:
        Não ler colunas p1..p9 — são NULL na v8 (legado v6).
        Não bloquear o daemon.
    """
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
            """SELECT ts, direction, check_c_total, check_c_thr, status
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
        rounds[key][r["direction"]] = (r["check_c_total"], r["status"])

    lines = [f"📈 <b>{base} — Histórico 48h</b>", SEP]
    _status_ico = {"CALL": "✅CALL", "QUASE": "⚠️QUASE", "RADAR": "⬜", "DROP": "⬜"}
    for ts_key in sorted(rounds.keys(), reverse=True)[:24]:
        dt  = _fmt_dt(ts_key + ":00")
        ld  = rounds[ts_key].get("LONG",  (0, ""))
        sd  = rounds[ts_key].get("SHORT", (0, ""))
        l_ico = _status_ico.get(ld[1], "⬜")
        s_ico = _status_ico.get(sd[1], "⬜")
        lines.append(f"  {dt}  LONG C={ld[0]:>2} {l_ico}  SHORT C={sd[0]:>2} {s_ico}")
    lines += [SEP, f"  Rodadas registradas: {len(rounds)}"]
    return "\n".join(lines)


def cmd_log_quase() -> str:
    """
    OBJETIVO:
        Mostrar o breakdown detalhado dos QUASEs da última rodada —
        quais checks passaram e quais falharam, com motivo — para
        o operador entender por que o sinal não se qualificou como CALL.

    FONTE DE DADOS:
        scan_log.db — tabela round_events (lista de QUASEs)
        + tabela token_scores (check_a_ok, check_b_ok, check_c_det)
        para os mesmos symbol/direction/round_id.

    LIMITAÇÕES CONHECIDAS:
        check_c_det é JSON — parse pode falhar se corrompido.
        Só mostra QUASEs da última rodada registrada.

    NÃO FAZER:
        Não buscar no journal (atirador_journal.db) — dados de
        checks A/B/C estão em token_scores na v8.1.x.
        Não ler colunas p1..p9 — são NULL (legado v6).
    """
    conn = _scan_db_conn()
    if not conn:
        return "⚠️ scan_log.db não encontrado."

    try:
        last = conn.execute(
            "SELECT round_id, ts FROM rounds ORDER BY round_id DESC LIMIT 1"
        ).fetchone()
        if not last:
            conn.close()
            return "📋 Nenhuma rodada registrada."

        quases = conn.execute(
            """SELECT symbol, direction, check_c_total, gap
               FROM round_events
               WHERE round_id=? AND type='QUASE'
               ORDER BY check_c_total DESC""",
            (last["round_id"],)
        ).fetchall()
    except Exception as e:
        conn.close()
        return f"❌ Erro: {e}"

    if not quases:
        conn.close()
        return "✅ Nenhum QUASE na última rodada."

    dt_str = _fmt_dt(last["ts"], "%d/%m %H:%M")
    lines  = [f"⚠️ <b>QUASEs — {dt_str}</b>", SEP]

    for q in quases:
        sym = (q["symbol"] or "").replace("USDT", "")
        ico = "🚀" if q["direction"] == "LONG" else "📉"

        # Buscar breakdown em token_scores
        ts_row = None
        try:
            ts_row = conn.execute(
                """SELECT check_a_ok, check_a_reason,
                          check_b_ok, check_b_reason,
                          check_c_det, zona_qualidade
                   FROM token_scores
                   WHERE round_id=? AND symbol=? AND direction=?
                   LIMIT 1""",
                (last["round_id"], q["symbol"], q["direction"])
            ).fetchone()
        except Exception:
            pass

        zona = (ts_row["zona_qualidade"] if ts_row else None) or "—"
        lines.append(
            f"\n{ico} <b>{sym} {q['direction']}</b>"
            f"  C={q['check_c_total']}  zona={zona}"
        )

        if not ts_row:
            lines.append("  ⚠️ breakdown não disponível")
            continue

        def _ico(val):
            return "✅" if val else "❌"

        lines.append(
            f"  Check A  {_ico(ts_row['check_a_ok'])}"
            f"  {ts_row['check_a_reason'] or ''}"
        )
        lines.append(
            f"  Check B  {_ico(ts_row['check_b_ok'])}"
            f"  {ts_row['check_b_reason'] or ''}"
        )
        lines.append("  ── Check C ──")

        # Parse check_c_det
        det = {}
        try:
            det = json.loads(ts_row["check_c_det"] or "{}")
        except Exception:
            pass

        if det:
            lines.append(
                f"  C1 BB   {_ico(det.get('c1_bb'))}"
                f"  {det.get('c1_reason','')}"
            )
            lines.append(
                f"  C2 Vol  {_ico(det.get('c2_vol'))}"
                f"  {det.get('c2_reason','')}"
            )
            lines.append(
                f"  C3 CVD  {_ico(det.get('c3_cvd'))}"
                f"  {det.get('c3_reason','')}"
            )
            lines.append(
                f"  C4 OI   {_ico(det.get('c4_oi'))}"
                f"  {det.get('c4_reason','')}"
            )
        else:
            lines.append("  ⚠️ check_c_det ausente ou inválido")

    conn.close()
    lines += [SEP, f"Total: {len(quases)} QUASEs"]
    return "\n".join(lines)


def cmd_log_calls(arg: str | None = None) -> str:
    """
    OBJETIVO:
        Listar CALLs reais (não hipotéticas) dos últimos N dias com
        status de cada trade (WIN/LOSS/OPEN/EXPIRED). Dá uma visão
        rápida do histórico de sinais emitidos e seus desfechos.

    FONTE DE DADOS:
        atirador_journal.db — tabela trades, filtro is_hypothetical=0.
        Campos: timestamp, symbol, direction, score, status, type.

    LIMITAÇÕES CONHECIDAS:
        score na v8 é o Check C (0–4), não score de pilares.
        Trades OPEN ainda não têm desfecho — contam separados.
        Limite de exibição: 20 trades mais recentes na mensagem.

    NÃO FAZER:
        Não exibir QUASEs hipotéticos neste comando — use /perf_quase.
        Não bloquear o daemon.
    """
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
    """
    OBJETIVO:
        Exibir métricas de performance das CALLs reais dos últimos 30d:
        Win Rate, Profit Factor e Expectancy separados por LONG e SHORT.
        Principal indicador de qualidade do sistema de sinais.

    FONTE DE DADOS:
        atirador_journal.db — tabela trades, filtro is_hypothetical=0,
        status != 'OPEN', últimos 30d. Campos: direction, status, pnl_pct.

    LIMITAÇÕES CONHECIDAS:
        Requer mínimo de 10 trades fechados para métricas confiáveis.
        pnl_pct pode ser None para trades muito antigos.
        Não separa performance por zona ou por contexto de mercado.
        score na v8 é Check C (0–4) — não exibir como score de pilares.

    NÃO FAZER:
        Não incluir trades OPEN no cálculo — desfecho indefinido.
        Não incluir QUASEs hipotéticos — use /perf_quase para isso.
    """
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
    """
    OBJETIVO:
        Avaliar a qualidade dos sinais QUASE como ferramenta de
        calibração do threshold. Responde: "se eu tivesse executado
        os QUASEs como CALLs, qual seria o win rate comparado com
        as CALLs reais?" Permite decidir se o threshold está
        conservador ou adequado.

    FONTE DE DADOS:
        atirador_journal.db tabela trades.
        CALLs: is_hypothetical=0, status != 'OPEN', últimos 30d.
        QUASEs: is_hypothetical=1, status != 'OPEN', últimos 30d.

    LIMITAÇÕES CONHECIDAS:
        QUASEs têm SL/TP hipotéticos — WR pode ser otimista.
        Precisa de mínimo 5 QUASEs fechados para diagnóstico confiável.

    NÃO FAZER:
        Não ler campo 'threshold' dos rows — não existe no journal v8.
        Não interpretar 'score' como score de pilares — é Check C (0–4).
    """
    conn = _journal_db_conn()
    if not conn:
        return "⚠️ atirador_journal.db não encontrado."

    try:
        calls = conn.execute(
            """SELECT status FROM trades
               WHERE is_hypothetical=0
               AND timestamp >= datetime('now', '-30 days')
               AND status != 'OPEN'"""
        ).fetchall()
        quases = conn.execute(
            """SELECT status, direction FROM trades
               WHERE is_hypothetical=1
               AND timestamp >= datetime('now', '-30 days')
               AND status != 'OPEN'"""
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

    if n_q < 5:
        diag = f"⚠️ Dados insuficientes ({n_q} QUASEs fechados)"
    elif wr_q >= wr_c:
        diag = "→ Threshold pode estar conservador demais ⚠️"
    elif wr_q >= wr_c * 0.80:
        diag = "→ Threshold calibrado ✅"
    else:
        diag = "→ Threshold adequado — QUASEs são mais fracos ✅"

    longs  = [r for r in quases if r["direction"] == "LONG"]
    shorts = [r for r in quases if r["direction"] == "SHORT"]
    wr_ql, n_ql = _wr(longs)
    wr_qs, n_qs = _wr(shorts)

    lines = [
        "🔍 <b>Calibração — QUASEs (30d)</b>", SEP,
        f"  CALLs fechadas    {n_c:>4}   WR {wr_c}%",
        f"  QUASEs fechados   {n_q:>4}   WR {wr_q}%",
        SEP,
        f"  LONG  — QUASEs {n_ql}  WR {wr_ql}%",
        f"  SHORT — QUASEs {n_qs}  WR {wr_qs}%",
        SEP,
        f"  {diag}",
        SEP,
        "⚠️ WR hipotético — QUASEs não foram executados",
    ]
    return "\n".join(lines)


def cmd_trade(symbol: str | None) -> str:
    """
    OBJETIVO:
        Mostrar o status atual de um trade aberto para um símbolo:
        preço de entrada, SL, TPs, tempo decorrido, max runup/drawdown
        e tempo até expiração. Permite monitorar um trade específico.

    FONTE DE DADOS:
        atirador_journal.db — tabela trades, filtro symbol=?, status='OPEN',
        is_hypothetical=0.

    LIMITAÇÕES CONHECIDAS:
        Mostra apenas o trade mais recente aberto para o símbolo.
        Preços de SL/TP são calculados no momento do sinal — não são
        atualizados em tempo real.
        max_runup e max_drawdown são atualizados a cada rodada do cron,
        não em tempo real.

    NÃO FAZER:
        Não exibir trades hipotéticos (QUASEs) — apenas CALLs reais.
        Não fazer chamada à exchange para preço atual.
    """
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
    """
    OBJETIVO:
        Exportar o arquivo scan_log.jsonl completo via Telegram
        sendDocument. Permite análise offline da verdade bruta de
        todas as rodadas.

    FONTE DE DADOS:
        logs/scan_log.jsonl — leitura direta do arquivo.

    LIMITAÇÕES CONHECIDAS:
        Se o arquivo ultrapassar 50MB, envia apenas as últimas 1000
        linhas com nome scan_log_tail1000.jsonl.
        A leitura do arquivo inteiro é feita em memória — em arquivos
        grandes pode consumir RAM significativa na VM (1GB total).

    NÃO FAZER:
        Não bloquear o daemon por mais de 30s — o timeout do sendDocument
        já cobre casos normais.
    """
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


def cmd_health_export() -> str:
    """
    OBJETIVO:
        Disparar o health_report.py (relatório analítico completo)
        e enviar o arquivo gerado via Telegram, junto com logs e state.
        Usado para debug e análise de performance do sistema.

    FONTE DE DADOS:
        Chama externamente: python3 health_report.py --out {arquivo}
        Complementa com: log de rodada mais recente, log do bot do dia,
        states/atirador_state.json.

    LIMITAÇÕES CONHECIDAS:
        health_report.py pode demorar até 30s — roda em thread separada.
        Se health_report.py falhar, reporta o erro mas ainda envia os
        demais arquivos disponíveis.

    NÃO FAZER:
        Não usar subprocess.run() bloqueante — usar threading.Thread.
        Não travar o loop do daemon em hipótese alguma.
    """
    import glob

    now   = _now_brt()
    stamp = now.strftime("%Y%m%d_%H%M")
    saude_path = os.path.join(LOG_DIR, f"saude_{stamp}.txt")
    script     = os.path.expanduser("~/Setup_Atirador/health_report.py")

    result_holder = {"returncode": None, "stderr": ""}

    def _run():
        try:
            r = subprocess.run(
                ["python3", script, "--out", saude_path],
                capture_output=True, text=True, timeout=55,
            )
            result_holder["returncode"] = r.returncode
            result_holder["stderr"]     = r.stderr
        except Exception as e:
            result_holder["returncode"] = -1
            result_holder["stderr"]     = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=60)

    scan_logs = sorted(
        glob.glob(os.path.join(LOG_DIR, "atirador_LOG_*.log")),
        key=os.path.getmtime,
    )
    bot_logs = sorted(
        glob.glob(os.path.join(LOG_DIR, "bot_*.log")),
        key=os.path.getmtime,
    )

    files_to_send = []
    missing       = []

    def _add(path, label):
        if path and os.path.exists(path):
            files_to_send.append((path, label))
        else:
            missing.append(label)

    _add(saude_path,                           "health report")
    _add(scan_logs[-1] if scan_logs else None, "log rodada")
    _add(bot_logs[-1]  if bot_logs  else None, "log bot")
    _add(STATE_FILE,                           "state")

    status_report = ""
    if result_holder["returncode"] is None:
        status_report = "⚠️ health_report.py timeout (>60s)"
    elif result_holder["returncode"] != 0:
        status_report = f"⚠️ health_report.py erro: {result_holder['stderr'][-200:]}"

    header = (
        f"🔍 Health Export — {now.strftime('%Y-%m-%d %H:%M')}\n"
        f"Arquivos: {' | '.join(l for _, l in files_to_send)}"
    )
    if status_report:
        header += f"\n{status_report}"
    if missing:
        header += f"\n⚠️ Não encontrados: {', '.join(missing)}"
    _tg_send(header)

    errors = []
    for path, label in files_to_send:
        try:
            with open(path, "rb") as fh:
                content = fh.read()
            ok = _tg_send_document(os.path.basename(path), content, caption=label)
            if not ok:
                errors.append(label)
        except Exception as e:
            errors.append(f"{label} ({e})")

    if errors:
        return f"❌ Falha ao enviar: {', '.join(errors)}"
    return ""


# ── Polling principal ─────────────────────────────────────────────────────────

HANDLERS = {
    "/ajuda":       cmd_ajuda,
    "/help":        cmd_ajuda,
    "/status":      cmd_status,
    "/scan":        cmd_scan,
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
        if command == "/health_export":
            response = cmd_health_export()
            if response:    # vazio = documentos já enviados via sendDocument
                _tg_send(response)
            _log(f"{_ts()} | /health_export → {'enviado ✅' if not response else 'falhou'}")
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
        if command == "/health_export":
            response = cmd_health_export()
            if response:
                _tg_send(response)
            _log(f"{_ts()} | /health_export → {'enviado ✅' if not response else 'falhou'}")
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
