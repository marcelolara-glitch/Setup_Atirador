# Setup Atirador v6.6.2

Scanner profissional de criptomoedas para operações **LONG e SHORT alavancadas** de curto prazo.

Analisa perpetuals USDT em múltiplas exchanges (Bybit + Bitget + OKX) com sistema multi-timeframe
(4H macro + 1H estrutura + 15m gatilho), 15 pilares de scoring (máx. 28 pts), e gestão de risco
risk-first. Envia alertas e heartbeats via Telegram.

---

## Execução Rápida

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Configurar credenciais do Telegram
export TELEGRAM_TOKEN="seu_token_aqui"
export TELEGRAM_CHAT_ID="seu_chat_id_aqui"

# 3. Executar
python setup_atirador.py
```

---

## Execução Automática (GitHub Actions)

O workflow `.github/workflows/scan.yml` executa o scan automaticamente **3x por dia**
(06h, 12h e 18h BRT — horário de Brasília).

**Para ativar:**

1. Acesse o repositório no GitHub pelo celular ou computador
2. Vá em **Settings → Secrets and variables → Actions**
3. Clique em **New repository secret** e adicione:
   - `TELEGRAM_TOKEN` — token do seu bot Telegram (obtido via @BotFather)
   - `TELEGRAM_CHAT_ID` — ID do chat que receberá os alertas
4. Pronto. O workflow já está configurado e rodará automaticamente.

Para acionar manualmente: **Actions → Setup Atirador — Scan Programado → Run workflow**

---

## Estrutura do Projeto

```
Setup_Atirador/
├── setup_atirador.py           ← Entry point (executa versão ativa) ⭐
├── setup_atirador_v6_6_2.py    ← Versão ativa atual
├── requirements.txt
├── .env.example                ← Template de configuração (sem dados reais)
├── .github/
│   └── workflows/
│       └── scan.yml            ← Agendamento automático 3x/dia
├── docs/
│   ├── SKILL.md                ← Documentação completa do skill
│   ├── config.md               ← Parâmetros configuráveis
│   └── scoring-system.md       ← Sistema de 15 pilares (28 pts)
└── (versões anteriores na raiz — referência histórica)
```

---

## Saídas por Execução

| Destino | Conteúdo |
|---------|----------|
| **Telegram — Heartbeat** | Contexto de mercado + pipeline + radar + veredicto (toda rodada) |
| **Telegram — QUASE** | Alerta por token quando score está a ≤4 pts do threshold |
| **Telegram — Call** | Mensagem operacional completa: entrada, SL, TPs, alavancagem |
| `/tmp/atirador_SCAN_*.txt` | Relatório executivo local |
| `/tmp/atirador_logs/*.log` | Log técnico completo (DEBUG) |

---

## Atualizar Versão

Quando uma nova versão for desenvolvida (ex: v6.7.0), basta editar **uma linha** em `setup_atirador.py`:

```python
VERSION = "v6_7_0"   # era v6_6_2
```

Histórico completo de versões em [docs/SKILL.md](docs/SKILL.md).
