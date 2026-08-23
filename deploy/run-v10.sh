#!/bin/bash
# run-v10.sh — wrapper do ciclo v10. Espelha run-shadow.sh: carrega env, resolve
# REPO_DIR, roda a partir da raiz do repo (para `python -m v10.*` resolver o
# pacote). NAO toca no runtime v9 nem no shadow do DONCHIAN-A.
#
# Uso (via cron):
#   run-v10.sh               -> roda os setups ATIVOS + envia o DELTA da janela
#                               (python -m v10.runner)
#   run-v10.sh --relatorio   -> quadro completo, 1x/dia (python -m v10.relatorio)
# stdout/stderr vao p/ ~/cron-v10.log.
#
# Log PROPRIO: nada do v10 entra no log do DONCHIAN-A e vice-versa. Quando uma
# das duas coisas quebrar, o log diz qual sem ninguem ter que separar.

set -u

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HOME/.env_atirador"
LOG_FILE="$HOME/cron-v10.log"

# Carrega tokens (Telegram etc.), como run-scan.sh/run-shadow.sh. Ausencia nao e
# fatal: o motor grava no banco de qualquer jeito e so o envio falha, em
# silencio — o banco e a fonte de verdade, a mensagem e leitura dele.
if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
fi

# Ativa a venv se existir; caso contrario cai no python3 do sistema (como o v9).
PY="python3"
for VENV in "$REPO_DIR/.venv/bin/activate" "$HOME/.venv-atirador/bin/activate"; do
    if [ -f "$VENV" ]; then
        # shellcheck disable=SC1090
        source "$VENV"
        PY="python"
        break
    fi
done

cd "$REPO_DIR" || { echo "[run-v10] ERRO: cd $REPO_DIR falhou" >> "$LOG_FILE"; exit 1; }

TS="$(date -u '+%Y-%m-%d %H:%M:%S')"
if [ "${1:-}" = "--relatorio" ]; then
    echo "--- $TS UTC --- v10 quadro completo (diario) ---" >> "$LOG_FILE"
    "$PY" -m v10.relatorio >> "$LOG_FILE" 2>&1
else
    echo "--- $TS UTC --- v10 scan 4h + delta ---" >> "$LOG_FILE"
    "$PY" -m v10.runner "$@" >> "$LOG_FILE" 2>&1
fi
