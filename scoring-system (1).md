# Sistema de Scoring - Setup Atirador v6.4.0

## Pilares e Pontuação Máxima (Total: 28 pts)

O sistema agora é bidirecional, avaliando oportunidades tanto para LONG quanto para SHORT. Os pilares são espelhados para cada direção.

| # | Pilar | Max | Critério LONG | Critério SHORT |
|---|-------|-----|---------------|----------------|
| 1 | Tendência 4H | 4 | STRONG_BUY=4, BUY=2 | STRONG_SELL=4, SELL=2 |
| 2 | Confluência 1H | 3 | STRONG_BUY=3, BUY=2, SELL=-2 | STRONG_SELL=3, SELL=2, BUY=-2 |
| 3 | RSI 15m | 4 | <25=4, <30=3, <35=2, <42=1, >70=-2 | >75=4, >70=3, >65=2, >58=1, <30=-2 |
| 4 | Estocástico 15m | 3 | K<20 e K>D=3, K<10=2, K<20=1 | K>80 e K<D=3, K>90=2, K>80=1 |
| 5 | CCI 15m | 2 | <-200=2, <-100=1 | >200=2, >100=1 |
| 6 | Bollinger Bands | 2 | Posição <10%=2, <25%=1 | Posição >90%=2, >75%=1 |
| 7 | MACD 15m | 2 | MACD > Signal = 2 | MACD < Signal = 2 |
| 8 | Candles 15m | 4 | Padrões bullish (Engulfing, Hammer) | Padrões bearish (Engulfing, Shooting Star) |
| 9 | Funding Rate | 2 | <-0.1%=2, <0=1, >0.5%=-1 | >0.1%=2, >0=1, <-0.5%=-1 |
| 10 | ADX 15m | 1 | >30=1 | >30=1 |
| 11 | Zonas de Liquidez | 3 | Suporte S/R próximo=1, OB ativo=1, Confluência=1 | Resistência S/R próxima=1, OB ativo=1, Confluência=1 |
| 12 | Figuras Gráficas | 3 | Breakout LTB=1, Suporte LTA=1, Figura bullish=1 | Breakdown LTA=1, Resistência LTB=1, Figura bearish=1 |
| 13 | Filtro de Pump/Dump | 0 | >40% 24h = DESCARTADO, >30% = -3 pts, >20% = -2 pts | <-40% 24h = DESCARTADO, <-30% = -3 pts, <-20% = -2 pts |
| 14 | CHOCH / BOS | 3 | BOS Bullish = 1 a 2 pts, CHOCH Bullish = 1 a 2 pts | BOS Bearish = 1 a 2 pts, CHOCH Bearish = 1 a 2 pts |
| 15 | Open Interest (P9) | 2 | OI Crescente (+1 a +2 pts) | OI Crescente (+1 a +2 pts) |

## Classificação e Thresholds

Os thresholds são adaptativos com base no contexto de mercado (Fear & Greed e BTC 4H).

| Contexto | Fear & Greed | BTC 4H | Threshold LONG | Threshold SHORT | Verdict |
|----------|--------------|--------|----------------|-----------------|---------|
| Bull | <= 30 | BUY/STRONG_BUY | 14 | 20 | FAVORÁVEL (LONG) / CAUTELOSO (SHORT) |
| Neutro | 30-70 | NEUTRAL | 16 | 16 | MODERADO |
| Bear | >= 75 | SELL/STRONG_SELL | 20 | 14 | CAUTELOSO (LONG) / FAVORÁVEL (SHORT) |
| Medo Extremo | <= 20 | Qualquer | 20 | 14 | CAUTELOSO (LONG) / FAVORÁVEL (SHORT) |
| Desfavorável | >= 80 | STRONG_SELL | 99 | 99 | BOT OFF |

## Detalhamento de Pilares Específicos

### Pilar 11: Zonas de Liquidez
Baseado em klines (60 candles de 15m e 1H):
- Swing points detectados com janela de 5 candles
- Clusters de S/R agrupados por proximidade de 2.5%
- Order Blocks: último candle contrário antes de impulso >= 1.5%
- Pontuação: Suporte/Resistência próximo (<2.5%) = +1, OB ativo/próximo (<2.5%) = +1, Confluência = +1

### Pilar 12: Figuras Gráficas
Baseado em klines de 15m:
- Trendlines via regressão linear nos swing points (mín. R² > 0.5)
- Breakout/Breakdown: preço 0-3% além da linha = +1
- Suporte/Resistência de LTA/LTB: preço a 0-1% da linha = +1
- Figuras: Falling/Rising Wedge, Triângulos = +1

### Pilar 13: Filtro de Pump/Dump
Baseado na variação de preço de 24h:
- Evita armadilhas de liquidez (entrar em pullback de ativo muito esticado)
- **>= 40% (LONG) ou <= -40% (SHORT)**: Ativo é descartado imediatamente (BLOCK)
- **>= 30% (LONG) ou <= -30% (SHORT)**: Penalidade de -3 pontos no score
- **>= 20% (LONG) ou <= -20% (SHORT)**: Penalidade de -2 pontos no score

### Pilar 14: CHOCH / BOS (Smart Money Concepts)
Baseado em klines de 15m:
- **BOS (Break of Structure)**: Preço rompe o último swing na direção da tendência, confirmando continuação.
- **CHOCH (Change of Character)**: Preço rompe o último swing na direção contrária à tendência, sinalizando reversão.
- Pontuação: CHOCH confirmado = +2, BOS confirmado = +1 a +2, Estrutura saudável = +1

### Pilar 15: Open Interest (P9)
- Avalia a variação do Open Interest (contratos em aberto)
- OI crescente indica entrada de novo capital suportando o movimento
- Adiciona até +2 pontos ao score final

## Data Quality (Qualidade de Dados)
Na versão 6.4.0, a qualidade dos dados (Data Quality) foi separada do Setup Score.
- Penaliza a confiança no sinal se houver dados faltantes (ex: falha na API do TradingView para algum timeframe).
- Exibido no relatório como `⚠️DQ<100%` quando aplicável.
- Não reduz o score base, mas serve como alerta de risco operacional.
