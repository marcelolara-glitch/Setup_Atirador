# Configuração — Setup Atirador v6.6.2

## Parâmetros editáveis no script (topo do arquivo)

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `MIN_TURNOVER_24H` | 2,000,000 | Volume mínimo 24h em USDT |
| `MIN_OI_USD` | 5,000,000 | Open Interest mínimo em USD |
| `KLINE_TOP_N` | 20 | Tokens que avançam para análise completa com klines |
| `KLINE_TOP_N_LIGHT` | 30 | Tokens para análise leve (sem klines) |
| `KLINE_LIMIT` | 60 | Candles para análise de S/R e figuras |
| `KLINE_CACHE_TTL_H` | 1 | Cache de klines em horas |
| `SR_PROXIMITY_PCT` | 2.5 | Proximidade % para zona de Suporte/Resistência |
| `OB_PROXIMITY_PCT` | 2.5 | Proximidade % para zona de Order Block |
| `PUMP_WARN_24H` | 20 | % de alta 24h → penalidade -2 pts no score |
| `PUMP_WARN_24H_STRONG` | 30 | % de alta 24h → penalidade -3 pts no score |
| `PUMP_BLOCK_24H` | 40 | % de alta 24h → ativo descartado (PUMP BLOCK) |
| `QUASE_MARGEM` | 4 | threshold − N = gatilho de alerta QUASE no Telegram |

## Gestão de Risco (Risk-First)

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `BANKROLL` | 100.0 | Banca total em USD |
| `RISCO_POR_TRADE_USD` | 5.00 | Risco fixo em $ por trade (loss máximo) |
| `MAX_PERDA_DIARIA_USD` | 10.00 | Stop do dia (equivalente a 2 losses) |
| `MAX_TRADES_ABERTOS` | 2 | Máximo de trades simultâneos (LONG + SHORT) |
| `MARGEM_MAX_POR_TRADE` | 35.0 | Margem máxima alocada por trade ($) |
| `ALAVANCAGEM_MIN` | 2.0 | Alavancagem mínima permitida |
| `ALAVANCAGEM_MAX` | 50.0 | Teto absoluto de alavancagem |
| `RR_MINIMO` | 2.0 | Risk:Reward mínimo (1:2) |

## Tabela de Alavancagem por Score

| Score | Alavancagem Máx | Perfil do Setup |
|-------|-----------------|-----------------|
| 14–15 | 5x | Threshold mínimo — marginal |
| 16–17 | 10x | Moderado |
| 18–19 | 15x | Válido |
| 20–21 | 20x | Cauteloso |
| 22–23 | 30x | Forte |
| 24–25 | 40x | Muito forte |
| 26–28 | 50x | Setup perfeito |

## Integração Telegram

**Opção 1 — Variáveis de ambiente (recomendado para GitHub Actions):**
```bash
export TELEGRAM_TOKEN="seu_token_aqui"
export TELEGRAM_CHAT_ID="seu_chat_id_aqui"
```

**Opção 2 — Arquivo local (execução manual):**
```json
{
  "telegram_token": "seu_token_aqui",
  "telegram_chat_id": "seu_chat_id_aqui"
}
```
Localização: `/tmp/atirador_telegram_config.json`

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `TELEGRAM_HEARTBEAT` | True | Envia heartbeat a cada rodada |

## Fontes de Dados

| Dado | Fonte Primária | Fallback |
|------|----------------|----------|
| Tickers + perpetuais USDT | Bybit | Bitget → OKX |
| Funding Rate | Bybit / Bitget | OKX `/public/funding-rate` (batch assíncrono) |
| Klines 15m/1H/4H | Bybit | Bitget → OKX |
| Indicadores técnicos | TradingView (tradingview-ta) | — |
| Fear & Greed Index | Alternative.me `/fng/` | — |

## Dependências Python

```bash
pip install -r requirements.txt
# aiohttp, requests, numpy, tradingview-ta
```
