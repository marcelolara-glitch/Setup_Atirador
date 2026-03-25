#!/usr/bin/env python3
"""
diagnostics.py — Diagnóstico pós-rodada do Setup Atirador
===========================================================
Analisa o log da última execução em busca de problemas críticos que
comprometam o resultado do scan.

Critérios de CRÍTICO (apenas o que realmente impede ou compromete o resultado):
  - Scan encerrou com falha (exit code != 0)
  - Traceback Python não tratado
  - TODAS AS 3 FONTES FALHARAM (sem dados para analisar)
  - TV batch falhou após todos os retries (scoring comprometido)
  - Heartbeat Telegram não enviado (usuário não foi notificado)
  - Erro ao salvar estado diário (afeta próxima rodada: OI/trends)
  - Log não gerado (script crashou antes do logger init)
  - [ERROR] em excesso (> 10 linhas = problema sistêmico de API)

Critérios de AVISO (informativo, listado junto se houver crítico):
  - [ERROR] e [WARNING] individuais (falhas por-item tratadas graciosamente)
  - Coluna inválida / removida (TV API às vezes retorna dados ruins, é tratado)
  - trade_params=❌ (score válido, só parâmetros de trade não calculados)
  - Fear & Greed falhou (fallback funciona)
  - Timeout com retry (auto-recuperado)
  - OKX/Gate.io indisponíveis (fallback Bitget)
  - DADO AUSENTE (por token/pilar, tratado com score 0)

Envia Telegram APENAS se houver pelo menos 1 crítico.
Sem críticos → nenhuma mensagem (zero spam).
"""
import glob
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import requests

# ── Configuração ──────────────────────────────────────────────────────────────
BRT              = timezone(timedelta(hours=-3))
LOG_DIR          = "/tmp/atirador_logs"
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SCAN_OUTCOME     = os.getenv("SCAN_OUTCOME", "unknown")

# Número de linhas [ERROR] que indica problema sistêmico (não apenas por-item)
ERRORS_CRITICO_THRESHOLD = 10

# ── Padrões CRÍTICOS ──────────────────────────────────────────────────────────
#
# Somente o que realmente compromete o resultado final.
# LOG.error() neste codebase inclui erros por-item tratados (klines, JSON);
# por isso [ERROR] não entra como crítico unitário — só acima do threshold.
#
# Cada entrada: (regex, descrição, count_mode)
#   count_mode=False → reporta primeiras 3 ocorrências com contexto de linha
#   count_mode=True  → reporta como "label (×N)"
#
CRITICOS = [
    # Crash Python não tratado
    (r"Traceback \(most recent call last\)",
     "Traceback — exceção Python não tratada",
     False),

    # Todas as fontes de dados falharam — zero tokens analisados
    (r"TODAS AS 3 FONTES FALHARAM",
     "Todas as fontes de dados falharam — sem dados para analisar",
     False),

    # TV batch falhou após todos os retries — scoring comprometido para todos os tokens
    # Padrão exige ≥2 tentativas para excluir o teste individual do diagnóstico
    # de colunas inválidas (retries=1), que é parte do mecanismo de recuperação.
    (r"TV batch falhou após [2-9]\d* tentativas",
     "TV batch falhou — scoring comprometido (BB, Volume, ATR sem dados)",
     False),

    # Heartbeat Telegram não enviado — usuário não recebeu notificação do scan
    (r"📵\s+Telegram heartbeat: falha no envio",
     "Heartbeat Telegram não enviado — usuário não notificado",
     False),

    # Estado diário não salvo — afeta OI trend e setas de tendência na próxima rodada
    (r"Erro ao salvar estado",
     "Erro ao salvar estado diário — afeta OI/trends na próxima rodada",
     False),
]

# ── Padrões de AVISO ──────────────────────────────────────────────────────────
#
# Problemas reais mas tratados graciosamente pelo código.
# Listados na mensagem crítica quando há pelo menos 1 crítico.
# Nunca disparam alerta sozinhos.
#
AVISOS = [
    (r"\[ERROR\s*\]",
     "Linha [ERROR] no log (falha por-item ou API)"),
    (r"📵\s+Telegram",
     "Falha de envio Telegram"),
    (r"Coluna inválida detectada|Colunas removidas por falha de API",
     "Colunas TV inválidas/removidas (tratado)"),
    (r"trade_params=❌",
     "trade_params=❌ (score válido, trade inoperável)"),
    (r"Fear & Greed falhou",
     "Fear & Greed indisponível (fallback ativo)"),
    (r"OKX e Gate\.io indisponíveis",
     "OKX/Gate.io indisponíveis (fallback Bitget)"),
    (r"Timeout \(tentativa",
     "Timeout em API (com retry automático)"),
    (r"DADO AUSENTE",
     "Dado ausente (klines por token/pilar)"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tg_send(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠  Telegram não configurado — alerta não enviado")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as exc:
        print(f"⚠  Telegram send falhou: {exc}")
        return False


def _find_latest_log() -> tuple[str | None, str | None]:
    files = sorted(glob.glob(f"{LOG_DIR}/atirador_LOG_*.log"), reverse=True)
    if files:
        return files[0], os.path.basename(files[0])
    return None, None


def _analyze(path: str) -> dict:
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except Exception as exc:
        return {"read_error": str(exc), "criticos": [], "avisos": [],
                "n_warnings": 0, "n_errors": 0, "dado_ausente_count": 0}

    criticos_found: list[str] = []
    avisos_found:   list[str] = []

    # ── Padrões críticos diretos ──────────────────────────────────────────────
    for pattern, label, count_mode in CRITICOS:
        rx = re.compile(pattern)
        matches = [(i + 1, l.rstrip()) for i, l in enumerate(lines) if rx.search(l)]
        if not matches:
            continue
        if count_mode:
            criticos_found.append(f"{label} (×{len(matches)})")
        else:
            for lineno, line in matches[:3]:
                msg_part = re.sub(r"^\S+ BRT \[\w+\s*\]\s*", "", line).strip()
                msg_part = msg_part[:80] + "…" if len(msg_part) > 80 else msg_part
                criticos_found.append(f"L{lineno}: {label}" +
                                      (f" — {msg_part}" if msg_part else ""))

    # ── [ERROR] em excesso = problema sistêmico ───────────────────────────────
    n_errors = sum(1 for l in lines if re.search(r"\[ERROR\s*\]", l))
    if n_errors > ERRORS_CRITICO_THRESHOLD:
        criticos_found.append(
            f"[ERROR] em excesso ({n_errors} linhas) — possível problema sistêmico de API"
        )

    # ── DADO AUSENTE excessivo ────────────────────────────────────────────────
    dado_ausente_count = sum(1 for l in lines if "DADO AUSENTE" in l)

    # ── Padrões de aviso ──────────────────────────────────────────────────────
    for pattern, label in AVISOS:
        rx = re.compile(pattern)
        count = sum(1 for l in lines if rx.search(l))
        if count:
            avisos_found.append(f"{label} (×{count})")

    n_warnings = sum(1 for l in lines if re.search(r"\[WARNING", l))

    return {
        "read_error":         None,
        "criticos":           criticos_found,
        "avisos":             avisos_found,
        "n_warnings":         n_warnings,
        "n_errors":           n_errors,
        "dado_ausente_count": dado_ausente_count,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ts = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")

    log_path, log_name = _find_latest_log()
    scan_failed        = SCAN_OUTCOME not in ("success",)

    if log_path:
        result = _analyze(log_path)
    else:
        result = {
            "read_error": None,
            "criticos": ["Log não encontrado — script falhou antes de inicializar o logger"],
            "avisos": [], "n_warnings": 0, "n_errors": 0, "dado_ausente_count": 0,
        }

    # Scan com falha sempre é crítico
    if scan_failed:
        result["criticos"].insert(0, f"Scan encerrou com status: {SCAN_OUTCOME}")

    has_critical = bool(result["criticos"])

    # ── Saída no terminal do Actions (sempre) ─────────────────────────────────
    status_str = "✅ OK" if not has_critical else f"🚨 {len(result['criticos'])} CRÍTICO(S)"
    print(f"[diagnostics] {ts} | Scan: {SCAN_OUTCOME} | Status: {status_str} | "
          f"{result['n_warnings']} warnings | {result['n_errors']} errors | "
          f"{result['dado_ausente_count']} dado-ausente | "
          f"Log: {log_name or 'N/A'}")

    if not has_critical:
        print("[diagnostics] Sem problemas críticos — nenhum alerta enviado.")
        return 0

    # ── Monta e envia mensagem Telegram ───────────────────────────────────────
    scan_ico = "❌" if scan_failed else "⚠️"
    log_ref  = f"logs/{log_name}" if log_name else "— não gerado"

    msg  = f"🚨 <b>ATIRADOR — REVISÃO NECESSÁRIA</b> | {ts}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"{scan_ico} Scan: <b>{SCAN_OUTCOME}</b>\n"
    msg += f"📋 Log: <code>{log_ref}</code>\n"

    msg += f"\n🔴 <b>CRÍTICOS ({len(result['criticos'])}):</b>\n"
    for item in result["criticos"][:6]:
        msg += f"  · {item}\n"
    if len(result["criticos"]) > 6:
        msg += f"  · … +{len(result['criticos']) - 6} outros\n"

    if result["avisos"]:
        msg += f"\n⚠️ <b>AVISOS ({len(result['avisos'])}):</b>\n"
        for item in result["avisos"][:5]:
            msg += f"  · {item}\n"

    msg += f"\n→ Verifique o log: <code>{log_ref}</code>"

    if len(msg) > 4090:
        msg = msg[:4087] + "…"

    sent = _tg_send(msg)
    print(f"[diagnostics] Telegram: {'enviado ✅' if sent else 'falhou ❌'}")

    return 1  # sinaliza que houve crítico (não bloqueia o workflow)


if __name__ == "__main__":
    sys.exit(main())
