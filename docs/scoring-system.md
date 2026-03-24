# Sistema de Scoring — Setup Atirador v6.6.2

## Pilares e Pontuação (Total: 28 pts)

Sistema bidirecional — avalia LONG e SHORT simultaneamente com pilares espelhados.

| # | Pilar | Max | Critério LONG | Critério SHORT |
|---|-------|-----|---------------|----------------|
| 1 | Tendência 4H | 4 | STRONG_BUY=4, BUY=2 | STRONG_SELL=4, SELL=2 |
| 2 | Confluência 1H | 3 | STRONG_BUY=3, BUY=2, SELL=-2 | STRONG_SELL=3, SELL=2, BUY=-2 |
| 3 | RSI 15m | 4 | <25=4, <30=3, <35=2, <42=1, >70=-2 | >75=4, >70=3, >65=2, >58=1, <30=-2 |
| 4 | Estocástico 15m | 3 | K<20 e K>D=3, K<10=2, K<20=1 | K>80 e K<D=3, K>90=2, K>80=1 |
| 5 | CCI 15m | 2 | <-200=2, <-100=1 | >200=2, >100=1 |
| 6 | Bollinger Bands | 2 | Posição <10%=2, <25%=1 | Posição >90%=2, >75%=1 |
| 7 | MACD 15m | 2 | MACD > Signal = 2 | MACD < Signal = 2 |
| 8 | Candles 15m | 4 | Bullish (Engulfing, Hammer, Doji) | Bearish (Engulfing, Shooting Star) |
| 9 | Funding Rate | 2 | <-0.1%=2, <0=1, >0.5%=-1 | >0.1%=2, >0=1, <-0.5%=-1 |
| 10 | ADX 15m | 1 | >30=1 | >30=1 |
| 11 | Zonas de Liquidez | 3 | S/R próximo=1, OB ativo=1, Confluência=1 | Resistência próxima=1, OB=1, Confluência=1 |
| 12 | Figuras Gráficas | 3 | Breakout LTB=1, Suporte LTA=1, Figura bullish=1 | Breakdown LTA=1, Resistência LTB=1, Figura bearish=1 |
| 13 | Filtro Pump/Dump | 0 | >40%=BLOCK, >30%=-3pts, >20%=-2pts | <-40%=BLOCK, <-30%=-3pts, <-20%=-2pts |
| 14 | CHOCH / BOS | 3 | BOS Bullish=1–2, CHOCH Bullish=1–2 | BOS Bearish=1–2, CHOCH Bearish=1–2 |
| 15 | Open Interest | 2 | OI Crescente: +1 a +2 pts | OI Crescente: +1 a +2 pts |

## Thresholds Adaptativos

| Contexto | Fear & Greed | BTC 4H | Thresh LONG | Thresh SHORT | Verdict |
|----------|--------------|--------|-------------|--------------|---------|
| Bull | ≤30 | BUY/STRONG_BUY | 14 | 20 | FAVORÁVEL (LONG) |
| Neutro | 30–70 | NEUTRAL | 16 | 16 | MODERADO |
| Bear | ≥75 | SELL/STRONG_SELL | 20 | 14 | FAVORÁVEL (SHORT) |
| Medo Extremo | ≤20 | Qualquer | 20 | 14 | CAUTELOSO (LONG) |
| Desfavorável | ≥80 | STRONG_SELL | 99 | 99 | BOT OFF |

## Sistema de Alertas Telegram (v6.6.2)

| Estado | Condição | Mensagem enviada |
|--------|----------|-----------------|
| **BOT OFF** | threshold = 99 | Heartbeat apenas |
| **AGUARDAR** | faltam ≥7 pts | Heartbeat apenas |
| **MONITORAR** | faltam 4–6 pts | Heartbeat apenas |
| **QUASE** | faltam ≤`QUASE_MARGEM` pts | Heartbeat + QUASE (por token) |
| **CALL ATIVA** | score ≥ threshold | Heartbeat + Call (por token) |

## Detalhamento dos Pilares Principais

### Pilar 11 — Zonas de Liquidez
- Swing points detectados com janela de 5 candles
- Clusters S/R agrupados por proximidade de 2.5%
- Order Blocks: último candle contrário antes de impulso ≥ 1.5%
- Pontuação: S/R próximo (<2.5%) = +1, OB ativo (<2.5%) = +1, Confluência = +1

### Pilar 12 — Figuras Gráficas
- Trendlines via regressão linear nos swing points (mín. R² > 0.5)
- Breakout/Breakdown: preço 0–3% além da linha = +1
- Figuras: Falling/Rising Wedge, Triângulos simétricos/ascendentes/descendentes

### Pilar 14 — CHOCH / BOS (Smart Money Concepts)
- **BOS (Break of Structure)**: Preço rompe o último swing na direção da tendência → continuação
- **CHOCH (Change of Character)**: Preço rompe o último swing na direção contrária → reversão
- Pontuação: CHOCH confirmado = +2, BOS confirmado = +1 a +2

### Pilar 15 — Open Interest
- Avalia variação do OI (contratos em aberto)
- OI crescente indica entrada de novo capital suportando o movimento
- +2 pts se OI crescente forte, +1 pts se OI crescente moderado

## Data Quality (desde v6.4.0)

Separado do score base — não reduz a pontuação, mas serve como alerta:
- Penaliza a confiança quando há dados faltantes (falha de API, timeout, etc.)
- Exibido como `⚠️DQ<100%` no relatório quando aplicável
