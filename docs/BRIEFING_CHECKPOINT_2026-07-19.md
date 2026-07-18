# BRIEFING — Setup Atirador (checkpoint 2026-07-19)
Substitui o briefing de 27/04 (obsoleto: descrevia o estado pré-campanha).
FONTE DE VERDADE da pesquisa: research/ledger.md (append-only, LER PRIMEIRO).

## O que este projeto é hoje
Não é mais (só) um scanner de sinais: é um laboratório quantitativo com
juiz calibrado que, em jun–jul/2026, julgou a família inteira "tendência
direcional por ativo sobre OHLCV" e a ENCERROU para novos testes
históricos — com UM sobrevivente aguardando validação forward.

## Estado (19/07/2026)
- SOBREVIVENTE: DONCHIAN-A — rompimento de canal N=120 (20 dias), 4h,
  TIER1 (20 majors), exit bracket S=1.5 ATR / T=6.0 ATR / H=48 barras.
  FORTE na Fase A (blockP2.5 +3.4, p_shift .004) — único duplo-passe
  primário em 10 da campanha; margem fina; vizinhança com sinal vivo e
  MONEY marginal. NÃO OPERÁVEL: promovido apenas a observação (Estágio 2).
- Todo o resto: MORTO com registro (5 setups SMC, classifier de regime,
  TSMOM em 2 universos, Donchian alts). Cláusula de fechamento VIGENTE:
  nenhum teste histórico novo desta família, sem exceção.
- v9 (runtime produção, cron 15min): INTOCADA e ligada como coletora;
  sinais dela comprovadamente perdem dinheiro; o journal v9 guarda ~2,5
  meses de forward CEGO aguardando leitura (moldura pré-comprometida no
  ledger, 19/07).
- Fila forward: DONCHIAN-A (titular) → H-42 (TSMOM L=42 alts; ativação
  após 4 semanas de shadow estável) → N=240 (anotado; nunca antes do
  titular).
- Decisão estratégica ABERTA, sem prazo (do Marcelo): portas pós-testes —
  funding/carry, cross-section, ou observatório.

## Pendências imediatas (espelho do PENDENTES do ledger)
1. Marcelo: TRAVAR o protocolo Estágio 2 emendado (relatório DIÁRIO
   [VIGIA] via Telegram; regras de veredito no ledger, 18–19/07).
2. Leitura do journal v9 (comando na conversa de 19/07; moldura
   pré-comprometida: descritiva; positiva NÃO ressuscita — vira
   candidata a julgamento formal).
3. PR-9: shadow do DONCHIAN-A (módulo próprio + cron 4h + relatório
   diário) — briefing após a trava. Runtime v9 intocado.
4. Depois: auditoria de indexação posicional (replay/sweep/null_model);
   hardening do juice (contiguidade por timestamp); higiene de logs v8.

## Infraestrutura de pesquisa
- VM ubuntu@atirador (~/Setup_Atirador, venv .venv). VM ≠ GitHub:
  comandos de VM = bloco único p/ colar no SSH; repo = briefings ao
  Claude Code, merges do Marcelo pela UI.
- backtest/stage1.py = soquete de detectores (classifier | tsmom |
  donchian) + juízes: block bootstrap temporal (bins 14d; P2.5 do
  líquido @6bps > 0 = MONEY) e nulo circular (p < 0.025 = SKILL).
  Calibrado com oracle e controle morto (07/07). Determinístico
  (seed 1337; reexecuções batem byte a byte).
- backtest/candles_v9.db: 4h TIER1 2024-05-22→2026-06-21 + TIER2
  congelada em 11/07 (40 alts; lista no ledger); 0 buracos verificados.
- Receita de coleta: todo instrumento termina no "caveat" —
  L=$(ls -t logs/PREFIXO_*.log | head -1); grep -c caveat "$L" → cat.
- Rodada longa: SEMPRE nohup + arquivo de log. Terminal consulta,
  nunca armazena.

## Regras de trabalho (emendadas, vigentes)
- Pré-registro ANTES do dado; cemitério: toda rodada entra no ledger
  ANTES do teste seguinte; exploratório NUNCA promove.
- Micro-PRs: ≤200 linhas ADICIONADAS de implementação (testes fora do
  orçamento, reportados); numstat EXATO + merge-base REAL no relatório.
- Claude Code: git push com credencial local; NUNCA abre PR, merge ou
  force-push. Merge só após revisão independente e EXECUTÁVEL do diff
  (gêmeo byte a byte, sintéticos com valores fechados à mão).
- Regressão byte-idêntica dos caminhos existentes antes de rodar o novo.
- Wrapper de shell é código: testa-se antes de emitir.
- Claude abre TODA mensagem com uma linha de localização em linguagem
  simples (compromisso de 15/07).

## Lições pagas (não reaprender)
- Estatística iid lisonjeia: o N efetivo são blocos de calendário.
- Indexação posicional é gap-frágil (caso 18/06↔01/07, no ledger).
- Melhor-de-N exploratório é ruído com cara de sinal; só primário
  pré-registrado promove.
- Em rompimento, o stop é metade do payload (bracket ≫ temporal, 8/8).
- A amostra histórica 2024–26 está EXAURIDA para esta família:
  validação só forward, ou outra classe de dado.