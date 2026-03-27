#!/bin/bash
# install.sh — Setup completo da Oracle Cloud VM para o Setup Atirador
# Execute UMA VEZ após criar a VM. Requer Ubuntu 22.04 ou superior.
#
# Uso:
#   chmod +x install.sh
#   ./install.sh
#
# Após o script, preencha ~/.env_atirador com os tokens e execute:
#   sudo systemctl start atirador-bot

set -euo pipefail

REPO_URL="https://github.com/marcelolara-glitch/setup_atirador.git"
REPO_DIR="$HOME/Setup_Atirador"
ENV_FILE="$HOME/.env_atirador"
SERVICE_NAME="atirador-bot"
CRON_FILE="/etc/cron.d/atirador-scan"

echo "============================================================"
echo " Setup Atirador — Instalação Oracle Cloud VM"
echo "============================================================"

# ── 1. Dependências do sistema ────────────────────────────────────────────────
echo "[1/6] Instalando dependências do sistema..."
sudo apt-get update -q
sudo apt-get install -y -q python3.11 python3-pip python3.11-venv git

# ── 2. Clone do repositório ───────────────────────────────────────────────────
echo "[2/6] Clonando repositório..."
if [ -d "$REPO_DIR" ]; then
    echo "  → Diretório já existe, atualizando via git pull"
    git -C "$REPO_DIR" pull origin main --quiet
else
    git clone "$REPO_URL" "$REPO_DIR"
fi

# ── 3. Dependências Python ────────────────────────────────────────────────────
echo "[3/6] Instalando dependências Python..."
pip3 install -q -r "$REPO_DIR/requirements.txt"

# ── 4. Arquivo de variáveis de ambiente ───────────────────────────────────────
echo "[4/6] Criando arquivo de ambiente..."
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" <<'EOF'
# Preencha os valores abaixo e salve o arquivo.
# Este arquivo é carregado pelos scripts run-scan.sh e run-bot.sh.

export TELEGRAM_TOKEN="SEU_TOKEN_AQUI"
export TELEGRAM_CHAT_ID="SEU_CHAT_ID_AQUI"

# GitHub PAT com permissão "workflow" — necessário para /scan e /analisar no bot
export GITHUB_TOKEN="SEU_GITHUB_PAT_AQUI"
export GITHUB_REPOSITORY="marcelolara-glitch/setup_atirador"
EOF
    chmod 600 "$ENV_FILE"
    echo "  → Criado: $ENV_FILE"
    echo "  ⚠  ATENÇÃO: Preencha os tokens em $ENV_FILE antes de continuar!"
else
    echo "  → Já existe: $ENV_FILE (mantido)"
fi

# ── 5. Cron do scan (a cada 30 minutos) ──────────────────────────────────────
echo "[5/6] Configurando cron do scan..."
chmod +x "$REPO_DIR/deploy/run-scan.sh"
chmod +x "$REPO_DIR/deploy/run-bot.sh"

sudo tee "$CRON_FILE" > /dev/null <<EOF
# Setup Atirador — scan de criptos a cada 30 minutos
SHELL=/bin/bash
HOME=$HOME
*/30 * * * * $USER $REPO_DIR/deploy/run-scan.sh >> $HOME/cron-scan.log 2>&1
EOF
sudo chmod 644 "$CRON_FILE"
echo "  → Cron instalado: $CRON_FILE"

# ── 6. Serviço systemd do bot ─────────────────────────────────────────────────
echo "[6/6] Instalando serviço systemd do bot..."

# Substitui o placeholder do usuário no arquivo de serviço
sed "s/ubuntu/$USER/g" "$REPO_DIR/deploy/atirador-bot.service" | \
    sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" > /dev/null

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"

echo ""
echo "============================================================"
echo " Instalação concluída!"
echo "============================================================"
echo ""
echo " Próximos passos:"
echo "   1. Preencha os tokens em: $ENV_FILE"
echo "      nano $ENV_FILE"
echo ""
echo "   2. Inicie o bot Telegram:"
echo "      sudo systemctl start $SERVICE_NAME"
echo "      sudo systemctl status $SERVICE_NAME"
echo ""
echo "   3. Verifique os logs do bot (tempo real):"
echo "      journalctl -u $SERVICE_NAME -f"
echo ""
echo "   4. O scan começa automaticamente no próximo múltiplo de 30min."
echo "      Para rodar agora: $REPO_DIR/deploy/run-scan.sh"
echo ""
echo "   5. Verifique os logs do scan:"
echo "      tail -f $REPO_DIR/logs/atirador_\$(date +%Y%m%d).log"
echo "============================================================"
