# Configuração - Setup Atirador v6.4.0

## Parâmetros editáveis no script (topo do arquivo)

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `MIN_TURNOVER_24H` | 2,000,000 | Volume mínimo 24h em USDT para filtrar liquidez (reduzido de 5M para capturar mais altcoins) |
| `MIN_OI_USD` | 5,000,000 | Open Interest mínimo em USD para garantir execução sem slippage relevante |
| `KLINE_TOP_N` | 20 | Quantidade de tokens que avançam para análise completa com klines (LONG+SHORT) |
| `KLINE_TOP_N_LIGHT` | 30 | Quantidade de tokens para análise leve (sem klines) |
| `KLINE_LIMIT` | 60 | Candles para análise de S/R e figuras |
| `KLINE_CACHE_TTL_H` | 1 | Tempo de vida do cache de klines em horas (reduzido para capturar velas 4H novas) |
| `SR_PROXIMITY_PCT` | 2.5 | Proximidade percentual para considerar preço em zona de Suporte/Resistência |
| `OB_PROXIMITY_PCT` | 2.5 | Proximidade percentual para considerar preço em zona de Order Block |
| `PUMP_WARN_24H` | 20 | % de alta em 24h para aplicar penalidade no score (-2 pts) |
| `PUMP_WARN_24H_STRONG` | 30 | % de alta em 24h para aplicar penalidade forte no score (-3 pts) |
| `PUMP_BLOCK_24H` | 40 | % de alta em 24h para descartar o ativo (PUMP BLOCK) |

## Gestão de Risco (Risk-First)

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `BANKROLL` | 100.0 | Banca total em USD |
| `RISCO_POR_TRADE_USD` | 5.00 | Risco fixo em $ por trade (loss máximo) |
| `MAX_PERDA_DIARIA_USD` | 10.00 | Stop do dia (10% da banca, equivalente a 2 losses) |
| `MAX_TRADES_ABERTOS` | 2 | Máximo de trades simultâneos (LONG e SHORT combinados) |
| `MARGEM_MAX_POR_TRADE` | 35.0 | Máximo de margem alocada por trade ($) |
| `ALAVANCAGEM_MIN` | 2.0 | Alavancagem mínima permitida |
| `ALAVANCAGEM_MAX` | 50.0 | Teto absoluto de alavancagem |
| `RR_MINIMO` | 2.0 | Risk:Reward mínimo (1:2) |

## Tabela de Alavancagem por Score (Teto 28 pts)

| Range de Score | Alavancagem Máxima | Perfil do Setup |
|----------------|--------------------|-----------------|
| 14–15 | 5x | Threshold mínimo — setup marginal |
| 16–17 | 10x | Threshold moderado |
| 18–19 | 15x | Abaixo de cauteloso mas válido |
| 20–21 | 20x | Cauteloso — mercado exigente |
| 22–23 | 30x | Forte |
| 24–25 | 40x | Muito forte |
| 26–28 | 50x | Setup perfeito / excepcional com P9 |

## Integração Telegram

As credenciais do Telegram podem ser configuradas de duas formas:

1. **Variáveis de Ambiente**:
   ```bash
   export TELEGRAM_TOKEN="seu_token_aqui"
   export TELEGRAM_CHAT_ID="seu_chat_id_aqui"
   ```

2. **Arquivo de Configuração** (`/tmp/atirador_telegram_config.json`):
   ```json
   {
     "telegram_token": "seu_token_aqui",
     "telegram_chat_id": "seu_chat_id_aqui"
   }
   ```

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `TELEGRAM_HEARTBEAT` | True | Se True, envia resumo a cada rodada. Se False, só envia quando há alerta de call. |

## Fontes de Dados

| Dado | Fonte | Endpoint |
|------|-------|----------|
| Perpetuals USDT | Bybit / Bitget / OKX | Fallback em 3 níveis para máxima resiliência |
| Funding Rate | Bybit / Bitget / OKX | Extraído junto com os tickers |
| Klines | Bybit / Bitget / OKX | Fallback automático em caso de falha |
| Indicadores Técnicos | TradingView | Exchange BYBIT, timeframes 15m/1H/4H |
| Fear & Greed | Alternative.me | `/fng/?limit=1` |

## Dependências Python

- `aiohttp` (chamadas HTTP assíncronas)
- `requests` (chamadas HTTP síncronas)
- `numpy` (cálculos numéricos para S/R e figuras)
- `tradingview-ta` (análise técnica via TradingView)

Instalar: `sudo pip3 install aiohttp requests numpy tradingview-ta`
