# Investigação Lead-Lag C (abril 2026)

Coleta histórica de 60 dias × 50 tokens top-liquidez OKX + BTC, em
timeframe 15m, para backtest de hipótese lead-lag BTC → altcoins
(He et al. 2026; Liu et al. 2024).

## Status

EXPLORATÓRIO. Esta pasta NÃO faz parte do runtime de produção.
Pode ser deletada após investigação concluir.

## Como rodar

Na VM (137.131.132.190), com venv ativo:

    cd ~/Setup_Atirador
    python3 research/lead_lag_c_2026_04/coleta.py

Output em `~/research_output/lead_lag_c_2026_04/dataset.json`
e `~/research_output/lead_lag_c_2026_04/top50.json`.

Tempo esperado: 10-17 minutos respeitando rate-limit OKX.
Tamanho do dataset: ~14MB JSON cru (~2.5MB gzipped).

## O que NÃO fazer

- Não importar deste módulo a partir de código de produção
- Não rodar via cron
- Não confundir com `research/qualidade_entrada_2026_04/` (escopo diferente)
