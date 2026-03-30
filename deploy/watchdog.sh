#!/bin/bash
# =============================================================================
# ATIRADOR WATCHDOG — M14 (v6.6.3)
# =============================================================================
# Verifica se o scanner executou nos últimos 45 minutos.
# Se não, envia alerta via Telegram.
#
# Crontab recomendado (*/15 * * * *):
#   */15 * * * * /bin/bash ~/Setup_Atirador/deploy/watchdog.sh
#
# Dependências:
#   - ~/.env_atirador com TELEGRAM_TOKEN e TELEGRAM_CHAT_ID
#   - /tmp/atirador_last_run.json gravado pelo scanner ao final de cada rodada
#   - curl, python3 (para parsear o JSON e calcular delta)
# =============================================================================

set -euo pipefail

ENV_FILE="$HOME/.env_atirador"
TIMESTAMP_FILE="/tmp/atirador_last_run.json"
MAX_DELTA_SECONDS=2700   # 45 minutos

# Carrega variáveis de ambiente
if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

TELEGRAM_TOKEN="${TELEGRAM_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

# Sem credenciais, não há como alertar — exit silencioso
if [[ -z "$TELEGRAM_TOKEN" || -z "$TELEGRAM_CHAT_ID" ]]; then
    echo "[watchdog] TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID não configurados — saindo" >&2
    exit 0
fi

_tg_alert() {
    local msg="$1"
    curl -s -X POST \
        "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_CHAT_ID}" \
        -d "parse_mode=HTML" \
        --data-urlencode "text=${msg}" \
        > /dev/null
}

# Arquivo de timestamp não existe → scanner nunca rodou ou foi limpo
if [[ ! -f "$TIMESTAMP_FILE" ]]; then
    echo "[watchdog] $TIMESTAMP_FILE não encontrado — scanner pode não ter rodado ainda"
    exit 0
fi

# Extrai last_run via python3 (evita dependência de jq)
LAST_RUN=$(python3 -c "
import json, sys
try:
    with open('$TIMESTAMP_FILE') as f:
        d = json.load(f)
    print(d.get('last_run', ''))
except Exception as e:
    sys.exit(1)
" 2>/dev/null) || {
    echo "[watchdog] Erro ao ler $TIMESTAMP_FILE" >&2
    exit 0
}

if [[ -z "$LAST_RUN" ]]; then
    echo "[watchdog] Campo last_run vazio em $TIMESTAMP_FILE" >&2
    exit 0
fi

# Calcula delta em segundos
DELTA=$(python3 -c "
from datetime import datetime, timezone
try:
    last = datetime.fromisoformat('$LAST_RUN')
    now  = datetime.now(timezone.utc)
    if last.tzinfo is None:
        # Assume BRT (UTC-3) se sem timezone
        from datetime import timedelta
        last = last.replace(tzinfo=timezone(timedelta(hours=-3)))
    print(int((now - last).total_seconds()))
except Exception as e:
    print(-1)
" 2>/dev/null)

if [[ "$DELTA" -lt 0 ]]; then
    echo "[watchdog] Não foi possível calcular delta de tempo" >&2
    exit 0
fi

MINUTOS=$(( DELTA / 60 ))

echo "[watchdog] Último scan há ${MINUTOS} minuto(s) (delta=${DELTA}s, limite=${MAX_DELTA_SECONDS}s)"

if [[ "$DELTA" -gt "$MAX_DELTA_SECONDS" ]]; then
    MSG="⚠️ ATIRADOR OFFLINE — último scan há ${MINUTOS} minutos. Verificar VM."
    echo "[watchdog] ALERTA: $MSG"
    _tg_alert "$MSG"
else
    echo "[watchdog] OK — scanner ativo"
fi
