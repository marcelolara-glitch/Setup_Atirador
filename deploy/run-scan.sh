#!/bin/bash
# run-scan.sh — Wrapper do scan agendado para Oracle Cloud VM
# Chamado pelo cron a cada 30 minutos.
# Faz git pull, preserva estado, executa o scan e salva o estado de volta.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HOME/.env_atirador"
LOG_DIR="$REPO_DIR/logs"
STATE_SRC="$REPO_DIR/states/atirador_state.json"
STATE_TMP="/tmp/atirador_state.json"

# Carrega variáveis de ambiente (tokens)
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "[run-scan] ERRO: $ENV_FILE não encontrado. Execute install.sh primeiro."
    exit 1
fi

cd "$REPO_DIR"

# Atualiza código do GitHub (fonte da verdade)
git pull origin main --quiet 2>&1 || echo "[run-scan] AVISO: git pull falhou — usando versão local"

# Restaura estado persistente para /tmp (onde o script espera encontrá-lo)
if [ -f "$STATE_SRC" ]; then
    cp "$STATE_SRC" "$STATE_TMP"
fi

# Executa o scan (saída vai para log diário)
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/atirador_$(date +%Y%m%d).log"
echo "--- $(date '+%Y-%m-%d %H:%M:%S') BRT --- início da rodada ---" >> "$LOG_FILE"
python3 setup_atirador.py 2>&1 | tee -a "$LOG_FILE"
echo "--- $(date '+%Y-%m-%d %H:%M:%S') BRT --- fim da rodada ---" >> "$LOG_FILE"

# Persiste estado atualizado de volta para local permanente
if [ -f "$STATE_TMP" ]; then
    mkdir -p "$REPO_DIR/states"
    cp "$STATE_TMP" "$STATE_SRC"
fi

# Rotação de logs: mantém últimos 7 dias
find "$LOG_DIR" -name "atirador_*.log" -mtime +7 -delete 2>/dev/null || true
