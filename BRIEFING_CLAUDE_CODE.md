# Instruções para o Claude Code — Setup Atirador

## Regras obrigatórias

### 1. Merge
- PRs de features: merge em branch de feature → revisão → merge em `main`
- NUNCA fazer merge em `main` sem instrução explícita do produto
- NUNCA fazer merge em `main` durante desenvolvimento de features

### 2. Escopo cirúrgico
- Cada PR toca apenas o módulo identificado no briefing
- Funções fora do escopo não são alteradas
- Constantes e parâmetros nunca são alterados incidentalmente

### 3. Constantes
- Todas as constantes vivem em `config.py`
- NUNCA usar strings literais inline para URLs, productType, thresholds
- `BITGET_PRODUCT_TYPE = "USDT-FUTURES"` (com hífen) — imutável

### 4. Versionamento
- VERSION vive em `config.py`
- Toda nova versão atualiza VERSION + heartbeat + changelog
- Histórico do changelog é preservado

### 5. Antes de qualquer PR de nova versão
Comparar com a versão anterior:
- Constantes e valores em `config.py`
- URLs e parâmetros de API em `exchanges.py`
- Lógica de fallback Bitget→OKX em `exchanges.py`
- Assinaturas de funções cross-módulo

### 6. Observabilidade
- Falhas de I/O em `logger.py` e `journal.py` são sempre silenciosas
- Warning no log, nunca raise, nunca interrompem o scan

### 7. Proteções — executadas pelo produto, nunca pelo Claude Code
- Criação de tags de versão
- Backups externos
- Force push em qualquer branch

## Estrutura de módulos

Ver `CONTRATO_V8.md` para assinaturas completas de todas as funções.

## Workflow padrão

1. Receber briefing completo do produto
2. Ler o módulo alvo antes de escrever qualquer código
3. Implementar apenas o escopo do briefing
4. Verificar checklist do briefing antes do commit
5. Reportar resultado com checklist preenchido
6. Aguardar aprovação antes do merge

## Histórico de bugs conhecidos resolvidos na v8.0.0

| # | Bug | Módulo | PR |
|---|---|---|---|
| 1 | `fetch_perpetuals` retornava `int` onde `run_scan_async` esperava `str` | `exchanges.py` | PR3 |
| 2 | `ALAV_POR_SCORE.get()` com chaves tuple — sempre retornava default | `config.py` | PR1 |
| 3 | `_get_nearest_resistance_zone` chamava `analyze_resistance_1h` sem `current_price` | `signals.py` | PR7 |
| 4 | `score_oi_trend` lia `state["score_history"][sym]["oi_history"]` mas `update_score_history` escrevia em `state["oi_history"][sym]` | `state.py` | PR2 |
