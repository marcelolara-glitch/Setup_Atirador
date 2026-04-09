#!/bin/bash
# run-scan.sh — Wrapper do scan agendado para Oracle Cloud VM
# Chamado pelo cron a cada 30 minutos.
# Faz git pull (apenas código), preserva estado, executa o scan e salva o estado de volta.

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HOME/.env_atirador"
LOG_DIR="$REPO_DIR/logs"
STATE_SRC="$REPO_DIR/states/atirador_state.json"
STATE_TMP="/tmp/atirador_state.json"
LOCK_FILE="/tmp/atirador-scan.lock"

# Carrega variáveis de ambiente (tokens)
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "[run-scan] ERRO: $ENV_FILE não encontrado. Execute install.sh primeiro."
    exit 1
fi

# Previne execução concorrente: se outro scan estiver rodando, pula esta rodada.
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "[run-scan] $(date '+%Y-%m-%d %H:%M:%S') — scan anterior ainda em execução, saltando rodada."
    exit 0
fi

# Garante remoção do lock em qualquer saída (normal, erro ou kill).
cleanup() { rm -f "$LOCK_FILE"; }
trap cleanup EXIT

cd "$REPO_DIR"

# Atualiza APENAS código do GitHub, preservando arquivos de estado locais.
# Usa stash para evitar conflito no states/ que é atualizado localmente pela VM
# mas também commitado pelo GitHub Actions.
git stash push --quiet --include-untracked -- states/ 2>/dev/null || true
git pull origin main --quiet 2>&1 || echo "[run-scan] AVISO: git pull falhou — usando versão local"
git stash pop --quiet 2>/dev/null || true

# Restaura estado persistente para /tmp (onde o script espera encontrá-lo)
if [ -f "$STATE_SRC" ]; then
    cp "$STATE_SRC" "$STATE_TMP"
fi

# Executa o scan com timeout de 25 min para matar hangs de rede
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/atirador_$(date +%Y%m%d).log"
echo "--- $(date '+%Y-%m-%d %H:%M:%S') BRT --- início da rodada ---" >> "$LOG_FILE"
timeout 25m python3 main.py 2>&1 | tee -a "$LOG_FILE"
SCAN_EXIT=${PIPESTATUS[0]}
echo "--- $(date '+%Y-%m-%d %H:%M:%S') BRT --- fim da rodada (exit=$SCAN_EXIT) ---" >> "$LOG_FILE"

if [ "$SCAN_EXIT" -eq 124 ]; then
    echo "[run-scan] $(date '+%Y-%m-%d %H:%M:%S') AVISO: scan encerrado por timeout (25 min)" >> "$LOG_FILE"
elif [ "$SCAN_EXIT" -ne 0 ]; then
    echo "[run-scan] $(date '+%Y-%m-%d %H:%M:%S') AVISO: scan terminou com erro (exit=$SCAN_EXIT)" >> "$LOG_FILE"
fi

# Persiste estado atualizado de volta para local permanente (mesmo que o scan tenha falhado)
if [ -f "$STATE_TMP" ]; then
    mkdir -p "$REPO_DIR/states"
    cp "$STATE_TMP" "$STATE_SRC"
fi

# Rotação de logs: mantém últimos 7 dias
find "$LOG_DIR" -name "atirador_*.log" -mtime +7 -delete 2>/dev/null || true
