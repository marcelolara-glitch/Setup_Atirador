#!/bin/bash
# run-shadow.sh — wrapper do shadow DONCHIAN-A (Estagio 2, ledger 18-19/07).
# Espelha run-scan.sh: carrega env, resolve REPO_DIR, roda a partir da raiz do
# repo (para `python -m shadow.*` resolver o pacote). NAO toca no runtime v9.
#
# Uso (via cron):
#   run-shadow.sh            -> scan pos-fechamento 4h  (python -m shadow.donchian_a)
#   run-shadow.sh --vigia    -> relatorio diario [VIGIA] (python -m shadow.vigia)
#   run-shadow.sh --kis      -> scan do COLETOR KIS+REGIME (shadow.kis_regime)
# stdout/stderr vao p/ ~/cron-shadow-donchian.log (~/cron-shadow-kis.log no --kis).

set -u

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HOME/.env_atirador"
LOG_FILE="$HOME/cron-shadow-donchian.log"
# O coletor KIS escreve em log PROPRIO: nada dele entra no log do DONCHIAN-A.
[ "${1:-}" = "--kis" ] && LOG_FILE="$HOME/cron-shadow-kis.log"

# Carrega tokens (Telegram etc.), como run-scan.sh. Ausencia nao e fatal aqui:
# o motor loga e o envio do [VIGIA] falha em silencio (observabilidade).
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

cd "$REPO_DIR" || { echo "[run-shadow] ERRO: cd $REPO_DIR falhou" >> "$LOG_FILE"; exit 1; }

TS="$(date -u '+%Y-%m-%d %H:%M:%S')"
if [ "${1:-}" = "--vigia" ]; then
    echo "--- $TS UTC --- [VIGIA] diario ---" >> "$LOG_FILE"
    "$PY" -m shadow.vigia >> "$LOG_FILE" 2>&1
elif [ "${1:-}" = "--kis" ]; then
    echo "--- $TS UTC --- shadow KIS+REGIME 4h ---" >> "$LOG_FILE"
    "$PY" -m shadow.kis_regime >> "$LOG_FILE" 2>&1
else
    echo "--- $TS UTC --- shadow scan 4h ---" >> "$LOG_FILE"
    "$PY" -m shadow.donchian_a >> "$LOG_FILE" 2>&1
fi
