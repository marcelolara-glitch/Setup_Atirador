#!/bin/bash
# run-bot.sh — Lança o Telegram bot em modo daemon (long-polling contínuo)
# Chamado pelo systemd (atirador-bot.service). Não execute manualmente em background.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HOME/.env_atirador"

# Carrega variáveis de ambiente (tokens)
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "[run-bot] ERRO: $ENV_FILE não encontrado. Execute install.sh primeiro."
    exit 1
fi

cd "$REPO_DIR"

# Atualiza código ao iniciar (systemd reinicia o serviço após cada falha,
# então cada restart pega a versão mais recente do GitHub)
git pull origin main --quiet 2>&1 || true

exec python3 telegram_bot.py --daemon
