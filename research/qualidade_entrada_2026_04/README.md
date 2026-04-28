# Investigação: Qualidade de entrada (abril 2026)

Análise exploratória one-shot sobre os 178 trades fechados do journal v9
em janela 24-27/04/2026, para testar hipóteses H1-H6 de qualidade de
entrada (ver briefing original).

## Status

EXPLORATÓRIO. Esta pasta NÃO faz parte do runtime de produção.
Pode ser deletada após a investigação concluir, ou promovida a
`analysis/` se virar análise recorrente.

## Como rodar

Na VM (137.131.132.190), com venv ativo:

    cd ~/Setup_Atirador
    python3 research/qualidade_entrada_2026_04/coleta.py

Output em `~/research_output/qualidade_entrada_2026_04.json`.

## O que NÃO fazer

- Não importar deste módulo a partir de código de produção
- Não rodar via cron
- Não confundir com `analysis/` (não existe ainda)
