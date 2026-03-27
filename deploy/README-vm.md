# Deploy: Oracle Cloud VM

Guia de instalação do Setup Atirador na Oracle Cloud Free Tier.

## Pré-requisitos

- Conta Oracle Cloud ([cloud.oracle.com](https://cloud.oracle.com)) — gratuita forever
- VM: Ubuntu 22.04, shape `VM.Standard.A1.Flex` (ARM) ou `VM.Standard.E2.1.Micro` (AMD)
- Acesso SSH à VM

## Criando a VM no Oracle Cloud

1. Acesse **Compute → Instances → Create Instance**
2. **Image**: Ubuntu 22.04 (Minimal)
3. **Shape**: `VM.Standard.E2.1.Micro` (AMD, always free) ou `VM.Standard.A1.Flex` (ARM, 1 OCPU / 6 GB)
4. **Networking**: VCN padrão, subnet pública, IP público habilitado
5. **SSH keys**: adicione sua chave pública (ou gere uma nova)
6. Clique em **Create**

Aguarde ~2 min até a VM ficar `Running`. Anote o **IP público**.

## Instalação

```bash
# 1. Conecte na VM via SSH
ssh ubuntu@<IP_DA_VM>

# 2. Baixe e execute o script de instalação
curl -fsSL https://raw.githubusercontent.com/marcelolara-glitch/setup_atirador/main/deploy/install.sh -o install.sh
chmod +x install.sh
./install.sh

# 3. Preencha os tokens
nano ~/.env_atirador
```

No arquivo `~/.env_atirador`, substitua os valores:

```bash
export TELEGRAM_TOKEN="1234567890:AABBccDDeeFFggHH..."    # Token do @BotFather
export TELEGRAM_CHAT_ID="-100123456789"                    # ID do seu chat/grupo
export GITHUB_TOKEN="ghp_xxxxxxxxxxxxxxxxxxxx"             # PAT com permissão "workflow"
export GITHUB_REPOSITORY="marcelolara-glitch/setup_atirador"
```

> **Como criar o GitHub PAT**: GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens → New token → Repositório: `setup_atirador` → Permissão: `Actions: Read and write`

```bash
# 4. Inicie o bot
sudo systemctl start atirador-bot
sudo systemctl status atirador-bot   # deve aparecer: active (running)

# 5. (Opcional) Rode o scan agora para testar
~/Setup_Atirador/deploy/run-scan.sh
```

## Verificação

| O que verificar | Comando |
|---|---|
| Status do bot | `sudo systemctl status atirador-bot` |
| Logs do bot (tempo real) | `journalctl -u atirador-bot -f` |
| Logs do scan (hoje) | `tail -f ~/Setup_Atirador/logs/atirador_$(date +%Y%m%d).log` |
| Cron instalado | `cat /etc/cron.d/atirador-scan` |
| Próxima execução do cron | `grep atirador /etc/cron.d/atirador-scan` |

## Fluxo de atualização do código

O código é atualizado automaticamente pelo `run-scan.sh` via `git pull` antes de cada rodada. Se fizer uma mudança urgente e quiser aplicar imediatamente:

```bash
# Reinicia o bot (pega nova versão via git pull no startup do run-bot.sh)
sudo systemctl restart atirador-bot

# Ou rode o scan manualmente agora
~/Setup_Atirador/deploy/run-scan.sh
```

## Comandos úteis

```bash
# Ver últimas 50 linhas do log de hoje
tail -50 ~/Setup_Atirador/logs/atirador_$(date +%Y%m%d).log

# Ver log do cron (erros de execução)
tail -20 ~/cron-scan.log

# Parar o bot temporariamente
sudo systemctl stop atirador-bot

# Desabilitar o scan (remove do cron)
sudo rm /etc/cron.d/atirador-scan

# Reabilitar o scan
sudo cp ~/Setup_Atirador/deploy/atirador-scan /etc/cron.d/atirador-scan
```

## Arquitetura

```
GitHub (fonte da verdade do código)
  │
  └── Oracle Cloud VM (execução agendada)
       │
       ├── cron */30 * * * *
       │    └── run-scan.sh
       │         ├── git pull origin main   ← pega versão mais recente
       │         ├── cp states/ → /tmp/     ← restaura estado
       │         ├── python setup_atirador.py
       │         └── cp /tmp/ → states/     ← persiste estado
       │
       └── systemd: atirador-bot.service
            └── run-bot.sh
                 └── python telegram_bot.py --daemon
                      └── long-polling (getUpdates timeout=60)
                           └── resposta em <1s aos comandos
```

O `/scan` e `/analisar` do bot continuam disparando **GitHub Actions via workflow_dispatch** — o resultado do scan chega via Telegram (~2min).
