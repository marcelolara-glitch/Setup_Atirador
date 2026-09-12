# REGIME — regras vigentes do Setup Atirador

Dono único das regras de governança. O ledger referencia; não repete.
Histórico de versões: `git log` deste arquivo. Regras anteriores a 12/09/2026
permanecem nas entradas do ledger apenas como registro histórico.

Princípio: controle proporcional à camada. Exploração não tem cerimônia;
promoção tem toda.

## Camada 0 — Máquina íntegra (pré-condição da camada 3, não da 2)
- Guarda em `_abrir`: nunca abrir segunda posição no mesmo símbolo/direção com uma OPEN.
- Determinismo do sinal: mesma barra não pode ser `alvo=-1` na entrada e `alvo_saida=+1` na saída.
- Nenhuma promoção nova ao v10 enquanto os dois itens estiverem abertos.
  O sandbox (camada 2) opera independente disso — `stage1.py` não passa pelo runner.

## Camada 1 — Ideação
- Eixo + hipótese em uma frase. Pine no TradingView só para olhar o mecanismo.
- Controle: nenhum. Nada entra no ledger. TV não estima edge (lição 08/2026).

## Camada 2 — Sandbox
- Lugar: branch `sandbox` em worktree separado na VM (`~/Setup_Atirador_sandbox`),
  mesma venv, mesmo `candles_v9.db`. `main` e cron v10 intocados.
- Claude Code faz push direto em `sandbox`. Sem PR, sem revisão executável,
  sem regressão byte-idêntica, sem pré-registro, sem hash, sem `setup_id`.
- "Um eixo por vez" NÃO vale aqui.
- Três regras, só:
  1. Decompor antes de ler o agregado: direção, símbolo, episódio.
  2. Uma linha em `research/scratch.md` por tarde (não por rodada). Claude Code pode
     escrever nele. O ledger continua só do Marcelo.
  3. Rodada = comando único com log. Não disparar no minuto 16 de barra 4h (scan do cron).
- 1h/15m: janela única, sem duas metades, sem promoção até haver histórico.

## Camada 3 — Promoção ao forward (v10)
- Portão de entrada (regime 20/08): positivo nas duas metades, maioria dos trimestres,
  n ≥ 30, drawdown tolerável.
- Aqui entra tudo: PR revisado `sandbox → main`, registry, pré-registro de critérios
  operacionais, Telegram, "um eixo por vez".
- Janela definida por contagem, não por data: n trades mínimo e ≥ 2 episódios em
  direções opostas. Data-limite só como teto. (DONCHIAN-A: 84 dias = 1 episódio.)

## Camada 4 — Prontidão operacional
- Sizing, correlação no universo de 60, slippage real TIER2, posições simultâneas aceitas.
- Só para setups com veredito forward positivo.

## Camada 5 — OKX real
- Como está.

## Conduta do Claude por camada
- Camada 2: não pede pré-registro, ledger nem revisão. Continua pedindo decomposição.
- Camada 3 em diante: regime integral.

## Emendas
Alterar esta seção e o texto acima no mesmo commit; registrar uma linha no ledger
apontando o commit. Nunca reescrever o histórico de emendas.
- 2026-09-12 — criação. Consolida o regime de 20/08 (camada 3) e abre a camada 2.
