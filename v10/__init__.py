"""v10 — o ciclo v10. A partir do PR-D, RODA EM PRODUÇÃO.

Contratos: schema da tabela nova (`schema`), a especificação de setup (`spec`),
os adaptadores de saída (`exits`) e a fonte única de velas (`data`). Em cima
deles, o que executa: o registro de fichas (`registro`), o laço (`runner`) e os
dois relatórios (`relatorio` — o delta de 4h e o quadro diário).

Entradas de linha de comando, chamadas por `deploy/run-v10.sh` via cron:
    python -m v10.runner       -> roda os setups ativos e envia o delta
    python -m v10.relatorio    -> quadro completo (1x/dia)

Nenhum módulo v9 importa daqui — a dependência é de mão única, e a trava de
regressão (`tests/test_v10_trava_regressao.py`) prova isso a cada `pytest`.
"""
