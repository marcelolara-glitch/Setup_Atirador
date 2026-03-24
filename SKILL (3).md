---
name: setup-atirador
description: >-
  Scanner profissional de criptomoedas para operações LONG e SHORT alavancadas de curto prazo.
  Analisa perpetuals USDT com sistema multi-timeframe (4H macro + 1H estrutura + 15m gatilho),
  scoring de 28 pilares reais (LONG e SHORT), padrões de candles, funding rate, Fear and Greed,
  análise de contexto de mercado, zonas de liquidez (suporte/resistência e order blocks),
  e figuras gráficas. Gestão de risco profissional risk-first com RR mínimo 1:2,
  alavancagem dinâmica até 50x, trailing stop e TP múltiplos.
  Fonte de dados tripla (Bybit primário + Bitget + OKX fallback) para máxima resiliência.
  Integração com Telegram para alertas e heartbeats.
  Use para varrer mercado crypto em busca de entradas alavancadas, analisar tokens para
  scalping/day trade, gerar relatórios de oportunidades com parâmetros de trade (SL/TP),
  monitorar mercado periodicamente, ou quando o usuário mencionar Setup Atirador,
  scanner crypto, oportunidades alavancadas, análise de tokens.
---

# Setup Atirador v6.4.0 (Bidirecional + Risk-First + Telegram)

Scanner profissional de criptomoedas para operações LONG e SHORT alavancadas de curto prazo com gestão de risco institucional risk-first, resiliência máxima de dados e notificações via Telegram.

**v6.4.0**: Atualização massiva introduzindo sizing risk-first, recalibração de score/thresholds/tabela de alavancagem, separação de data_quality do setup_score, operações bidirecionais (LONG/SHORT) e notificações avançadas via Telegram.

## Principais Melhorias v6.4.0 vs v5.2

### [v6.4.0] Gestão de Risco Risk-First
- **Problema Anterior**: A margem implícita por trade era a banca inteira, o que matematicamente era inconsistente para uso real.
- **Solução**: Fórmula risk-first onde a margem é calculada com base no risco fixo ($5) e na distância do Stop Loss (ATR).
- **Garantia**: A margem por trade nunca excede `MARGEM_MAX_POR_TRADE` ($35 para banca de $100).
- **Recalibração**: Tabela de alavancagem recalibrada para cobrir o range completo de thresholds (14-28 pts).

### [v6.0+] Operações Bidirecionais (LONG e SHORT)
- Scanner agora avalia oportunidades tanto para LONG quanto para SHORT simultaneamente.
- Pilares bearish espelhados para análise de SHORT.
- Exclusividade LONG/SHORT: o mesmo token não pode ter sinais conflitantes abertos simultaneamente.

### [v6.1+] Integração Telegram (Webhook)
- **Alerta de Call**: Enviado apenas quando há setup com score suficiente (≥ threshold). Contém dados completos para execução imediata (entrada, SL, TP, alavancagem).
- **Heartbeat**: Mensagem compacta enviada a cada execução para verificar consistência do sistema e acompanhar evolução dos scores.
- **Configuração**: Via variáveis de ambiente `TELEGRAM_TOKEN` e `TELEGRAM_CHAT_ID` ou arquivo `/tmp/atirador_telegram_config.json`.

### [v6.2+] Melhorias de Performance e Análise
- `KLINE_TOP_N` aumentado para 20 (captura mais candidatos LONG/SHORT).
- `SR_PROXIMITY_PCT` ajustado para 2.5% para cobrir altcoins com maior volatilidade.
- Score histórico por token armazenado no estado diário (48 rodadas/token).

## Workflow de Execução

1. **Limpar cache (opcional)**: `rm -rf /tmp/atirador_cache/`
2. **Instalar dependências**: `sudo pip3 install aiohttp requests numpy tradingview-ta -q`
3. **Configurar Telegram (opcional)**: 
   ```bash
   export TELEGRAM_TOKEN="seu_token"
   export TELEGRAM_CHAT_ID="seu_chat_id"
   ```
4. **Executar**: `python3 /home/ubuntu/skills/setup-atirador/scripts/setup_atirador.py`
5. **Ler relatório**: `/tmp/atirador_SCAN_YYYYMMDD_HHMM.txt`
6. **Ler log**: `/tmp/atirador_logs/atirador_LOG_YYYYMMDD_HHMM.log`

## Arquitetura de 3 Camadas (Max 28 pts)

| Camada | Propósito | Timeframe | Descrição |
|--------|-----------|-----------|-----------|
| **1** | Contexto Macro | 4H | "Qual é a direção do mercado?" (Tendência, Liquidez, Figuras) |
| **2** | Estrutura | 1H | "Estamos num bom ponto de entrada?" (Suporte/Resistência, OB) |
| **3** | Gatilho | 15m | "O timing de entrada está correto agora?" (Candles, RSI, MACD, Volume) |

## Threshold Adaptativo

| Contexto | Fear & Greed | BTC 4H | Threshold | Verdict |
|----------|--------------|--------|-----------|---------|
| Bull | <= 30 | BUY/STRONG_BUY | 14 | FAVORÁVEL |
| Neutro | 30-70 | NEUTRAL | 16 | MODERADO |
| Bear | >= 75 | SELL/STRONG_SELL | 20 | CAUTELOSO |
| Medo Extremo | <= 20 | Qualquer | 20 | CAUTELOSO |
| Desfavorável | >= 80 | STRONG_SELL | 99 | BOT OFF |

## Gestão de Risco — Sizing Risk-First [v6.4.0]

### Cálculo de Posição
```
Risco fixo: $5.00 por trade
SL: Entry ± 1.5×ATR (adapta à volatilidade)
TP: Entry ± 3% (RR 1:2)
Notional: RISCO_POR_TRADE_USD / stop_pct
Margem Max: $35.00 por trade (para banca de $100)
Alavancagem Necessária: notional / MARGEM_MAX_POR_TRADE
Alavancagem Final: min(alavancagem_necessaria, cap_por_score)
```

### Alavancagem por Score (Teto 28 pts)
```
Score 14–15 → até  5x  (threshold mínimo — setup marginal)
Score 16–17 → até 10x  (threshold moderado)
Score 18–19 → até 15x  (abaixo de cauteloso mas válido)
Score 20–21 → até 20x  (cauteloso — mercado exigente)
Score 22–23 → até 30x  (forte)
Score 24–25 → até 40x  (muito forte)
Score 26–28 → até 50x  (setup perfeito / excepcional com P9)
```

## Arquivos Gerados por Execução

| Arquivo | Localização | Conteúdo |
|---------|-------------|----------|
| **Relatório** | `/tmp/atirador_SCAN_YYYYMMDD_HHMM.txt` | Resumo executivo: contexto, alertas, oportunidades, gestão de risco |
| **Log Completo** | `/tmp/atirador_logs/atirador_LOG_YYYYMMDD_HHMM.log` | Detalhes técnicos: gates, scores, klines, cache, APIs (DEBUG level) |
| **Estado Diário** | `/tmp/atirador_state.json` | PnL do dia, trades abertos, histórico de scores |
| **Config Telegram** | `/tmp/atirador_telegram_config.json` | Credenciais salvas do Telegram |

## Histórico de Versões Recentes

- **v6.4.0**: Sizing risk-first, recalibrar score/thresholds/tabela alav, data_quality separado do setup_score.
- **v6.3.0**: Candles bearish 15m, candle lock, oi_estimado flag, obs_short.
- **v6.2.0**: KLINE_TOP_N→20, SR_PROXIMITY→2.5%, RSI<30 descarta SHORT no Gate 4H, P9 OI crescente no score.
- **v6.1.2**: Notificações Telegram (webhook) com dois modos de mensagem (Alerta e Heartbeat).
- **v6.0.0**: SHORT bidirecional, pilares bearish espelhados, exclusividade LONG/SHORT.
- **v5.2.0**: Fix CoinGecko parser (fallback 3-exchange) + Fix klines OKX fallback.
