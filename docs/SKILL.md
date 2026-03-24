---
name: setup-atirador
description: >-
  Scanner profissional de criptomoedas para operações LONG e SHORT alavancadas de curto prazo.
  Analisa perpetuals USDT com sistema multi-timeframe (4H macro + 1H estrutura + 15m gatilho),
  scoring de 28 pilares reais (LONG e SHORT), padrões de candles, funding rate, Fear & Greed,
  análise de contexto de mercado, zonas de liquidez (suporte/resistência e order blocks),
  e figuras gráficas. Gestão de risco profissional risk-first com RR mínimo 1:2,
  alavancagem dinâmica até 50x, trailing stop e TP múltiplos.
  Fonte de dados tripla (Bybit primário + Bitget + OKX fallback) para máxima resiliência.
  Integração com Telegram para alertas e heartbeats.
---

# Setup Atirador v6.6.2

Scanner profissional de criptomoedas para operações LONG e SHORT alavancadas com gestão de risco
institucional risk-first, resiliência máxima de dados e notificações via Telegram.

## Novidades v6.6.2

**Arquitetura de comunicação Telegram redesenhada em 3 tipos de mensagem:**

| Tipo | Quando dispara | Conteúdo |
|------|----------------|----------|
| **Heartbeat** | Toda rodada | Contexto de mercado + pipeline completo + radar compacto + veredicto |
| **QUASE** | Score ≥ threshold − `QUASE_MARGEM` | 1 msg por token: todos os pilares (pts obtidos/máx) + o que falta |
| **Call** | Score ≥ threshold | 1 msg por token: entrada, SL, TPs, alavancagem, margem + breakdown pilares |

- Fix crítico: Funding Rate zerado — fetch dedicado via OKX `/public/funding-rate` em batch assíncrono.
- Fix crítico: Candles 15m — diagnóstico de colunas que causam TypeError no TradingView batch.
- `QUASE_MARGEM = 4` (threshold − 4 = gatilho de alerta QUASE)

## Workflow de Execução

```bash
pip install -r requirements.txt
export TELEGRAM_TOKEN="..." && export TELEGRAM_CHAT_ID="..."
python setup_atirador.py
```

Saídas:
- Relatório: `/tmp/atirador_SCAN_YYYYMMDD_HHMM.txt`
- Log completo: `/tmp/atirador_logs/atirador_LOG_YYYYMMDD_HHMM.log`

## Arquitetura de 3 Camadas (Max 28 pts)

| Camada | Propósito | Timeframe | Pergunta-chave |
|--------|-----------|-----------|----------------|
| **1** | Contexto Macro | 4H | "Qual é a direção do mercado?" |
| **2** | Estrutura | 1H | "Estamos num bom ponto de entrada?" |
| **3** | Gatilho | 15m | "O timing de entrada está correto agora?" |

## Threshold Adaptativo

| Contexto | Fear & Greed | BTC 4H | Threshold | Verdict |
|----------|--------------|--------|-----------|---------|
| Bull | ≤30 | BUY/STRONG_BUY | 14 | FAVORÁVEL |
| Neutro | 30–70 | NEUTRAL | 16 | MODERADO |
| Bear | ≥75 | SELL/STRONG_SELL | 20 | CAUTELOSO |
| Medo Extremo | ≤20 | Qualquer | 20 | CAUTELOSO |
| Desfavorável | ≥80 | STRONG_SELL | 99 | BOT OFF |

## Gestão de Risco — Sizing Risk-First

```
Risco fixo: $5.00 por trade
SL: Entry ± 1.5×ATR (adapta à volatilidade)
TP: Entry ± 3% (RR mínimo 1:2)
Notional: RISCO_POR_TRADE_USD / stop_pct
Margem Max: $35.00 por trade (para banca de $100)
Alavancagem: min(notional / margem_max, cap_por_score)
```

## Histórico de Versões

| Versão | Data | Mudanças principais |
|--------|------|---------------------|
| v6.6.2 | 24/03/2026 | Telegram 3 tipos (Heartbeat/QUASE/Call). Fix FR OKX. Fix candles 15m. |
| v6.6.1 | 23/03/2026 | Heartbeat reformulado como relatório decisivo. Breakdown real de pilares. |
| v6.6.0 | 23/03/2026 | Fix crítico FR OKX. Diagnóstico TradingView batch. |
| v6.5.0 | — | Melhorias de performance e análise avançada. |
| v6.4.1 | — | Sizing risk-first completo. Recalibração tabela alavancagem. |
| v6.4.0 | — | data_quality separado do setup_score. |
| v6.3.0 | — | Candles bearish 15m, candle lock, OI estimado flag. |
| v6.2.0 | — | KLINE_TOP_N→20, SR_PROXIMITY→2.5%, P9 OI no score. |
| v6.1.2 | 22/03/2026 | Notificações Telegram (Alerta + Heartbeat). |
| v6.1.1 | 22/03/2026 | Fix credenciais expostas → os.getenv(). |
| v6.0.0 | 21/03/2026 | SHORT bidirecional, pilares bearish espelhados. |
| v5.2.0 | 22/03/2026 | Fix CoinGecko parser + Fix klines OKX fallback. |
