# Scratch — camada 2 (sandbox)
Uma linha por tarde. Formato: `AAAA-MM-DD — eixo — o que rodou — o que sobrou vivo (ou nada)`.
Nada aqui é validado. Promoção só via camada 3 (ledger).
2026-11-14 — saída KIS 34/89 e 8/21 + portão (etapa 1, briefing v3) — backtest/kis_saida.py entregue (E0, E1-k stop fixo, E2-k chandelier; teto T1 2026-08-26 via TEMP VIEW); rodada na VM pendente — nada lido ainda
2026-11-14 — filtro kNN (ADX/MFI, lastguru) sobre KIS 34/89 E0/E1-3 e 8/21 E0/E1-2 (etapa 2) — backtest/kis_knn.py entregue com teste de look-ahead + mutação e placebo 20 sementes; rodada na VM pendente — nada lido ainda
2026-11-14 — KIS 34/89 E0/E1-3 por tempo gráfico 1h/4h/8h/12h/1D, portão on/off (etapa 3) — backtest/kis_tf.py entregue (8h/12h/1D agregados em memória; 1h só se cobrir H1+H2); rodada na VM pendente — nada lido ainda
2026-11-15 — meta descritivo KIS 34/89 E0/E1-3: 9 indicadores no candle do sinal × vencedor/perdedor (etapa 4) — backtest/kis_meta.py entregue (quintis de H1 aplicados a H2, placebo 20 sementes, conciliação com kis_saida.resumo, teste de prefixo + mutação RSI); rodada na VM pendente — nada lido ainda
