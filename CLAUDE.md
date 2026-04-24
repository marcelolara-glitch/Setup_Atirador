# CLAUDE.md — Guia de trabalho colaborativo no Setup Atirador

Este arquivo orienta assistentes (Claude.ai, Claude Code, outros) sobre como trabalhar neste repositório. Descreve a arquitetura, o workflow, as regras não-negociáveis e a fundação intelectual que embasa as decisões da v9.

---

## 1. Visão do sistema

Setup Atirador é um scanner automatizado de perpetual futures (crypto) com foco em scalping. Roda em VM na Oracle Cloud via cron a cada 15 minutos, avalia 70-90 tokens em múltiplos timeframes, emite alertas via Telegram quando detecta setups de alta probabilidade e acompanha cada CALL como forward test com saídas parciais.

**Arquitetura v9:**

- **Regime classifier** — cada token é classificado em um dos 4 regimes: `TREND_UP`, `TREND_DOWN`, `RANGE`, `SQUEEZE`. SQUEEZE tem prioridade quando detectado.
- **5 setups paralelos** — cada token é avaliado por todos os setups aplicáveis ao seu regime: `rev_zone`, `cont_pull`, `break_range`, `rev_exaust`, `breaker`. Múltiplos setups podem disparar simultaneamente (confluência).
- **Saídas parciais** — trade fechado em três níveis: TP1 (0.8× ATR, 50% da posição, SL→breakeven), TP2 (1.5× ATR, 30%, SL→TP1), TP3 (3.5× ATR, 20% runner). Saída total em SL ou timeout de 48h.
- **Decision contract** — pipeline produz um `SignalDecision` tipado: symbol, direction, action (CALL/SKIP), confluent_setups, confidence (0-100), regime, trade_plan, e todos os `SetupResult` avaliados (para observabilidade).
- **Duas camadas de persistência** — `scan_log_v9.db` (rodadas, avaliações por token, near-misses) e `atirador_journal_v9.db` (forward testing de CALLs com saídas parciais).

---

## 2. Fundação intelectual

A arquitetura v9 é resultado de um estudo comparativo entre cinco fontes de referência do mercado. Cada item da tabela foi avaliado com base em natureza (conceitual vs implementação) e prioridade de adoção. Esta é a tabela de decisões original, preservada para orientar futuras investigações e evoluções.

### Fontes de referência

| Sigla usada | Identidade | URL |
|-------------|-----------|-----|
| **NFIX7** | NostalgiaForInfinity — estratégia comunitária para Freqtrade (série NFIX no timeframe 5m) | https://github.com/iterativv/NostalgiaForInfinity |
| **LuxAlgo** | Smart Money Concepts [LuxAlgo] — Pine Script open-source no TradingView | https://www.tradingview.com/script/CnB3fSph-Smart-Money-Concepts-SMC-LuxAlgo/ |
| **Jesse** | Framework Python de algotrading com 300+ indicadores nativos | https://jesse.trade |
| **Python Trading Bot** | asier13/Python-Trading-Bot — scalper RSI multi-TF em Python | https://github.com/asier13/Python-Trading-Bot |
| **Flux Charts** | Price Action Toolkit — provedor premium de indicadores TradingView | https://fluxcharts.com |

### Tabela de decisões

| # | Fonte | Item | Natureza | Prioridade original | Status v9 |
|---|-------|------|----------|---------------------|-----------|
| 1 | NFIX7 | Arquitetura de sinais numerados paralelos | Conceitual | Alta | **Adotado** — 5 setups paralelos em `setups/` |
| 2 | NFIX7 | Camada de proteção separada do sinal | Conceitual | Alta | **Adotado** — `risk.py` é módulo independente de `signals.py` |
| 3 | NFIX7 | Enter tags em logs | Implementação | Média | A confirmar em implementação |
| 4 | NFIX7 | Derisking system (scaling out) | Conceitual | Baixa (depois) | **Adotado antecipadamente** — saídas parciais TP1/TP2/TP3 em `risk.update_trade_state` |
| 5 | LuxAlgo | Internal vs External structure | Implementação | Alta | A confirmar — `smc_lib.py` tem BOS/ChoCH, granularidade de sensibilidade a verificar |
| 6 | LuxAlgo | BoS/CHoCH classification | Implementação | Alta | **Adotado** — implementado em `smc_lib.py` |
| 7 | LuxAlgo | Zone state machine | Implementação | Média | A confirmar em implementação |
| 8 | Jesse | ADX + Choppiness para regime de mercado | Implementação direta | Alta | **Parcial** — `regime.py` usa ADX; Choppiness a confirmar |
| 9 | Jesse | Donchian para breakouts | Implementação direta | Alta | A confirmar em `setups/break_range.py` |
| 10 | Jesse | Elder's Force Index para validação de volume | Implementação direta | Média | A confirmar em implementação |
| 11 | Jesse | Sequential parameter nos indicadores | Refatoração | Média | A confirmar em implementação |
| 12 | Jesse | Correlation Cycle de Ehlers | Implementação direta | Baixa (experimental) | **Adiado** — marcado como experimental |
| 13 | Python Trading Bot | Setup RSI multi-TF | Novo setup | Alta | **Adotado** — implementado no Setup Atirador |
| 14 | Python Trading Bot | TP/SL recalibrado para scalp real | Configuração | Média | **Adotado** — TP1/TP2/TP3 em 0.8/1.5/3.5× ATR |
| 15 | Python Trading Bot | Backtester simples sobre histórico | Ferramenta | Alta | A confirmar — previsto em roadmap pós-v9 estável |
| 16 | Flux Charts | Volumetric OB (volume + strength ATR) | Implementação | Alta | **Adotado** — implementado no Setup Atirador |
| 17 | Flux Charts | Breaker Blocks como setup | Novo setup | Alta | **Adotado** — `setups/breaker.py` |
| 18 | Flux Charts | ATR como unidade universal | Refatoração | Alta | **Adotado** — TP/SL, regime e setups operam em unidades ATR |
| 19 | Flux Charts | Multi-TF bias score | Implementação | Média | A confirmar em implementação |

**Como usar esta tabela em futuras investigações:**

Antes de propor um novo setup ou modificação estrutural, consulte a tabela para verificar se a ideia já foi avaliada. Itens marcados como "Adotado" já têm implementação — evite reintroduzir. Itens "Adiado" foram considerados mas pospostos por prioridade — contêm histórico útil. Itens "A confirmar" precisam de auditoria no código antes de declarar completude. Quando implementar algum item pendente, atualize a coluna "Status v9" no mesmo commit.

---

## 3. Stack técnica

### Bibliotecas Python

**Análise técnica e dados:**
- **pandas_ta** — indicadores (ATR Wilder, ADX, RSI, BB, EMA, MACD). Wrapper unificado em `indicators.py` (MarketContext) e uso direto em `regime.py` e `risk.py`
- **pandas** / **numpy** — manipulação de DataFrames OHLCV, base de tudo

**SMC (Smart Money Concepts):**
- **smc_lib.py** (interno) — Pine Script do LuxAlgo SMC portado para Python. Structure detection: BOS/ChoCH, Order Blocks, Fair Value Gaps. Consumido por `rev_zone`, `breaker`, `cont_pull`

**Rede e exchanges:**
- **aiohttp** — fetch assíncrono de klines OKX/Bitget/Gate.io em paralelo (70-90 tokens simultâneos)
- **requests** — fetch síncrono no `journal.py` (isolado do fluxo async para reduzir blast radius de falhas)
- **tradingview-ta** — somente BTC 4H como contexto macro (não usado no filtro principal, que é o regime.py)

**Persistência:**
- **sqlite3** — nativo Python. `logger.py` grava `scan_log_v9.db`; `journal.py` grava `atirador_journal_v9.db`. Camada JSONL append-only em paralelo (`scan_log_v9.jsonl`) como verdade bruta reconstruível
- **json** — `atirador_state_v9.json`, evidências serializadas em colunas JSON do SQLite

**Telegram:**
- **requests** — chamadas diretas à Telegram Bot API (sem biblioteca wrapper especializada)

### Infraestrutura

- **VM:** Oracle Cloud Ubuntu 22.04 (IP 137.131.132.190)
- **Execução:** cron a cada 15 min → `deploy/run-scan.sh` → `python3 main.py`
- **Bot:** daemon systemd (`atirador-bot.service`) rodando `telegram_bot.py --daemon` (long-polling)
- **Exchanges:** OKX (primária) → Bitget → Gate.io (fallbacks automáticos)
- **Python:** 3.12+ (requisito do pandas_ta em versão estável)

---

## 4. Workflow de desenvolvimento

Arquitetura conceitual decidida no Claude.ai, implementação delegada ao Claude Code via briefing completo em mensagem única, revisão e merge controlados pelo proprietário.

### Divisão de responsabilidades

- **Marcelo** — decide arquitetura, aprova merges, executa tags/backups, comanda Claude Code. Nunca executa diretamente na VM: só comanda via briefing
- **Claude.ai** — análise, arquitetura, briefings auto-contidos, revisão de diffs antes de cada merge
- **Claude Code** — implementação cirúrgica no repositório GitHub via `curl` contra API GitHub (nunca `gh` CLI, nunca MCP GitHub)

### Regras absolutas

1. **Merge só com instrução explícita do proprietário.** Claude Code nunca mergeia sozinho
2. **Tag de proteção antes de cada marco.** `v9-persistence-stable`, `v8-final`, `v7-pre-modular` são checkpoints de recuperação
3. **Revisão de diff ANTES do merge.** Lição aprendida do PR #102, que foi mergeado sem revisão prévia. Proprietário sempre envia o diff ao Claude.ai para avaliar antes de apertar merge
4. **PRs nunca abertos programaticamente.** Claude Code apenas faz `git push` da branch. Proprietário abre PR via GitHub UI
5. **Briefings completos e auto-contidos.** Uma única mensagem ao Claude Code contém todo o contexto: base conceitual, código pré-escrito (opção B) quando possível, instrução explícita de reportar branch, PR link, diff raw e arquivos raw ao finalizar
6. **VM nunca tocada por comandos manuais do proprietário.** Toda interação com a VM é via briefing/script que Claude Code executa

### Micro-PRs

Briefings cujo output esperado do Code excede ~200 linhas quebram com idle timeout da API. Padrão estabelecido: dividir em micro-PRs de até 200 linhas cada. O `journal.py` foi entregue em 4 PRs (A: esqueleto + helpers; B: `open_trade`; C: `check_open_trades`; D: `get_performance`) exatamente por essa razão. Este padrão vale para qualquer módulo grande.

---

## 5. Convenções de código

### Pureza de módulos

- **Puros (zero I/O, zero estado global)** — `regime.py`, `risk.py`, `indicators.py`, `signals.py`, `setups/*`, `smc_lib.py`. Entrada: DataFrame OHLCV; saída: dataclass tipado. Testáveis sem mock de rede.
- **Persistência (I/O permitido, falhas silenciosas)** — `state.py`, `logger.py`, `journal.py`. Retornam `None`/`0`/`[]`/`False` em erro, nunca levantam. Falha de I/O jamais derruba um scan.
- **Rede (I/O com fallback)** — `exchanges.py`. OKX → Bitget → Gate.io, com cache em `/tmp/atirador_cache/`.
- **Apresentação/orquestração** — `telegram.py`, `main.py`, `telegram_bot.py`.

### Duck typing em fronteiras

`journal.py` não importa `SignalDecision` de `signals.py`. Usa `getattr()` para ler atributos. Isso evita ciclos de import e reduz o blast radius se um módulo for refatorado.

### Dataclasses para contratos

`MarketContext`, `RegimeClassification`, `SetupResult`, `SignalDecision`, `TradePlan`, `TradeState` são `@dataclass` tipados. Contratos explícitos e testáveis.

### Fetchers injetáveis

Toda função que depende de klines recebe `fetch_klines_fn` como parâmetro. Testes passam lambdas com candles fake. Produção usa `_fetch_klines_sync_v9` ou `fetch_klines_cached_async`.

### Bancos isolados v8/v9

Caminhos v9 têm sufixo `_v9` (`scan_log_v9.db`, `atirador_journal_v9.db`, `atirador_state_v9.json`). Bancos v8 ficam intactos em paralelo para histórico. Nenhuma migração forçada.

---

## 6. Limitações conhecidas do ambiente

**pandas_ta não instalável na sandbox do Claude Code harness** — a sandbox roda Python 3.11, e a versão estável no PyPI exige Python ≥ 3.12. Testes que dependem de `pandas_ta` (regime, indicators, signals, setups/rev_exaust, exchanges_v9) não coletam no harness. Validar manualmente na VM (Python 3.12+, `pandas_ta` instalado).

**raw.githubusercontent.com com rate limiting intermitente** — conteúdo de branches recém-criadas pode demorar até 60s para aparecer. Fallback: `git show origin/branch:arquivo.py` na VM e colar saída na conversa.

**web_fetch sem suporte a headers customizados** — Claude.ai não consegue autenticar em `api.github.com`. Para conteúdo de branches, proprietário roda comandos na VM e cola saída.

**Token PAT do proprietário tem escopo limitado** — `actions:write` para disparar workflows, mas sem `contents:write`. Tags e releases são publicadas via GitHub UI, não via push.

---

## 7. Referências rápidas

- **Repositório:** `github.com/marcelolara-glitch/Setup_Atirador`
- **Branch principal:** `main`
- **VM:** Oracle Cloud Ubuntu 22.04 (137.131.132.190), path `~/Setup_Atirador/`
- **Briefings por conversa** (referência histórica): `/mnt/user-data/outputs/v9_briefings/` no ambiente do Claude.ai
- **Tags de recuperação:**
  - `v9-persistence-stable` — estado pós state/logger/journal v9 completos
  - `v8-final` — último estado estável da v8
  - `v7-pre-modular` — monolito v7 antes da modularização v8
