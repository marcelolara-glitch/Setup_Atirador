# Setup Atirador — Contexto para Claude

## ARQUITETURA ATUAL

### Execução (Oracle Cloud VM)
- **VM**: Ubuntu 22.04, VM.Standard.E2.1.Micro, IP: 137.131.132.190
- **Scan**: cron Linux `*/30 * * * *` → `deploy/run-scan.sh`
  - Faz `git pull origin main` antes de cada rodada
  - Preserva estado em `~/Setup_Atirador/states/atirador_state.json`
- **Bot Telegram**: systemd `atirador-bot.service` → `python3 telegram_bot.py --daemon`
  - Long-polling contínuo (`getUpdates timeout=60`), resposta em <2s

### GitHub (fonte da verdade do código)
- `scan.yml` e `telegram_bot.yml`: apenas `workflow_dispatch` (sem cron)
- `analisar.yml`: ativo para análises individuais on-demand
- `/scan` e `/analisar` no Telegram disparam `workflow_dispatch` via GitHub API

---

## SISTEMA DE ANÁLISE

**Script principal**: `setup_atirador_v6_6_2.py` (~4.390 linhas)

### Fontes de dados
- OKX (primário) → Gate.io → Bitget (fallback)
- TradingView Scanner API: indicadores 4H, 1H, 15m
- Filtros de universo: volume ≥ $2M/24h, OI ≥ $50M, pares USDT

### Pipeline
```
Tickers (OKX) + TV 4H + FGI
        ↓
  Gate 4H (direção macro)
        ↓
  TV 1H + Gate 1H (estrutura)
        ↓
  TV 15m (Tech + Candles) + Klines + Funding Rate
        ↓
  Score LONG + Score SHORT (15 pilares, máx 25 pts)
        ↓
  Telegram: Heartbeat / QUASE / Call
```

### 15 Pilares de score
| Pilar | Camada | Máx |
|-------|--------|-----|
| P4 Liquidez 4H | Estrutura 4H | 3 pts |
| P5 Figuras 4H | Estrutura 4H | 2 pts |
| P6 CHOCH/BOS 4H | Estrutura 4H | 3 pts |
| P-1H Res/OB 1H | Confirmação 1H | 4 pts |
| P1 Bollinger 15m | Gatilho 15m | 3 pts |
| P2 Candles 15m | Gatilho 15m | 4 pts |
| P3 Funding Rate | Contexto | 2 pts |
| P8 Volume 15m | Contexto | 2 pts |
| P9 OI Trend | Contexto | 2 pts |
| P7 Pump/Dump | Filtro (veto) | 0 pts |

### Thresholds adaptativos
- LONG: ≥ 20 pts (cauteloso) — ajustado por FGI + BTC 4H
- SHORT: ≥ 16 pts (moderado)

### Notificações Telegram (3 tipos)
1. **Heartbeat**: contexto de mercado + pipeline + radar top-5
2. **QUASE**: alerta quando score ≤ threshold - 4
3. **Call**: operacional completo com entrada, SL, TP1/TP2/TP3, leverage

---

## INFRAESTRUTURA

### VM `~/.env_atirador`
```bash
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
GITHUB_TOKEN=...        # PAT com Actions read/write
GITHUB_REPOSITORY=marcelolara-glitch/Setup_Atirador
```

### Dependências Python
```
aiohttp>=3.8.0
requests>=2.28.0
numpy>=1.23.0
tradingview-ta>=3.3.0
```

### Arquivos de estado (VM local, persistentes)
- `states/atirador_state.json` — OI history (48 candles), score history por token
- `states/bot_state.json` — last_update_id do Telegram
- `logs/atirador_YYYYMMDD.log` — log diário com rotação de 7 dias

---

## HISTÓRICO DE VERSÕES RELEVANTE
- **v6.6.2** (atual): fix endpoint FR, split TV batch 15m, arquitetura 3 mensagens
- **v6.6.0**: redesign Heartbeat/QUASE/Call
- **v6.5.0**: risk-first sizing, margem como warning
- **v6.4.1**: fix Bollinger SHORT, split COLS_15M_TECH + COLS_15M_CANDLES
- **v6.3.0**: 6 padrões de candle para SHORT
- **v6.2.0**: P9 OI trend, score history, radar no heartbeat
- **v6.0.0**: bidirecional LONG+SHORT com posição exclusiva

---

## ESTADO OPERACIONAL
- VM ativa desde 28/03/2026
- Scans a cada 30min no horário exato (cron Linux)
- Bot daemon respondendo em <2s
- GitHub Actions: apenas para `workflow_dispatch` manual

## REGRAS DE ENTREGA
- **Após criar ou fechar qualquer PR, sempre fazer merge para `main` e push.**
  A VM faz `git pull origin main` a cada rodada — código que não está no `main` não chega à produção.

## PRÓXIMAS PRIORIDADES
<!-- Atualize esta seção antes de cada sessão -->
- [ ] (defina aqui o objetivo da sessão atual)
