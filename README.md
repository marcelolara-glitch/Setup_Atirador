# Setup Atirador

Scanner automatizado de perpetual futures (crypto) com foco em scalping. Avalia 70-90 tokens em múltiplos timeframes a cada 15 minutos e emite alertas via Telegram quando detecta setups de alta probabilidade. Cada alerta é acompanhado como forward test com saídas parciais até TP3, LOSS_SL ou timeout de 48h.

---

## Arquitetura

### Pipeline

```
Universo OKX
    ↓ (tokens qualificados: turnover ≥ 1M, OI ≥ 3M)
Classificação de regime por token
    ↓ (TREND_UP | TREND_DOWN | RANGE | SQUEEZE)
Avaliação paralela de 5 setups aplicáveis ao regime
    ↓ (rev_zone, cont_pull, break_range, rev_exaust, breaker)
Resolução de direção + confluências
    ↓ (SignalDecision: action, confidence, trade_plan)
Emissão de CALL via Telegram + persistência em journal
    ↓
Track de trades abertos (check a cada rodada)
    ↓ (saídas parciais: TP1/TP2/TP3 ponderados, SL dinâmico)
Fechamento automático em TP3, LOSS_SL ou timeout de 48h
```

### Setups

- **rev_zone** — reversão em zonas de estrutura (Order Block, S/R). Regimes: RANGE, SQUEEZE
- **cont_pull** — continuação após pullback em tendência. Regimes: TREND_UP, TREND_DOWN
- **break_range** — rompimento de range consolidado. Regimes: RANGE
- **rev_exaust** — reversão por exaustão. Regimes: TREND_UP, TREND_DOWN (na ponta oposta)
- **breaker** — breaker block confirmado. Regimes: TREND_UP, TREND_DOWN

Setups podem disparar simultaneamente no mesmo token — isso é confluência, e aumenta o score de confidence final.

### Regime classifier

Cada token é classificado em um dos quatro regimes a cada rodada, usando ATR, ADX e BB squeeze:

- **TREND_UP / TREND_DOWN** — tendência clara, ADX elevado
- **RANGE** — preço lateralizado entre níveis definidos
- **SQUEEZE** — compressão de volatilidade (BB dentro de Keltner), alta prioridade para setups de breakout

O regime filtra quais setups são avaliados. SQUEEZE tem prioridade quando presente.

### Saídas parciais

Cada CALL abre um trade virtual com três níveis:

| Nível | Distância | % Posição | Novo SL |
|-------|-----------|-----------|---------|
| TP1 | 0.8× ATR | 50% | breakeven (entry) |
| TP2 | 1.5× ATR | 30% | TP1 price |
| TP3 | 3.5× ATR | 20% (runner) | — |

Em `LOSS_SL`, a posição remanescente sai no SL atual (que pode ter migrado para breakeven após TP1 ou para TP1 após TP2). Em `EXPIRED` (48h sem resolução), o trade é fechado no preço de entrada com pnl = 0.

---

## Fundação intelectual

A arquitetura v9 é resultado de um estudo comparativo entre cinco fontes de referência reconhecidas no mercado. Cada item abaixo foi avaliado antes da implementação — a tabela é preservada no `CLAUDE.md` para orientar futuras investigações e evoluções.

### Fontes de referência

| Sigla | Identidade | URL |
|-------|-----------|-----|
| **NFIX7** | NostalgiaForInfinity — estratégia comunitária mais usada em Freqtrade (série NFIX, 5m) | https://github.com/iterativv/NostalgiaForInfinity |
| **LuxAlgo** | Smart Money Concepts [LuxAlgo] — Pine Script open-source | https://www.tradingview.com/script/CnB3fSph-Smart-Money-Concepts-SMC-LuxAlgo/ |
| **Jesse** | Framework Python de algotrading com 300+ indicadores | https://jesse.trade |
| **Python Trading Bot** | asier13/Python-Trading-Bot — scalper RSI multi-TF | https://github.com/asier13/Python-Trading-Bot |
| **Flux Charts** | Price Action Toolkit — provedor premium TradingView | https://fluxcharts.com |

### Tabela de decisões

| # | Fonte | Item | Natureza | Prioridade | Status |
|---|-------|------|----------|------------|--------|
| 1 | NFIX7 | Arquitetura de sinais numerados paralelos | Conceitual | Alta | Adotado |
| 2 | NFIX7 | Camada de proteção separada do sinal | Conceitual | Alta | Adotado |
| 3 | NFIX7 | Enter tags em logs | Implementação | Média | A confirmar |
| 4 | NFIX7 | Derisking system (scaling out) | Conceitual | Baixa | Adotado antecipadamente |
| 5 | LuxAlgo | Internal vs External structure | Implementação | Alta | A confirmar |
| 6 | LuxAlgo | BoS/CHoCH classification | Implementação | Alta | Adotado |
| 7 | LuxAlgo | Zone state machine | Implementação | Média | A confirmar |
| 8 | Jesse | ADX + Choppiness para regime de mercado | Implementação direta | Alta | Parcial (ADX ok) |
| 9 | Jesse | Donchian para breakouts | Implementação direta | Alta | A confirmar |
| 10 | Jesse | Elder's Force Index para volume | Implementação direta | Média | A confirmar |
| 11 | Jesse | Sequential parameter nos indicadores | Refatoração | Média | A confirmar |
| 12 | Jesse | Correlation Cycle de Ehlers | Implementação direta | Baixa | Adiado (experimental) |
| 13 | Python Trading Bot | Setup RSI multi-TF | Novo setup | Alta | Adotado |
| 14 | Python Trading Bot | TP/SL recalibrado para scalp real | Configuração | Média | Adotado |
| 15 | Python Trading Bot | Backtester simples sobre histórico | Ferramenta | Alta | A confirmar |
| 16 | Flux Charts | Volumetric OB (volume + strength ATR) | Implementação | Alta | Adotado |
| 17 | Flux Charts | Breaker Blocks como setup | Novo setup | Alta | Adotado |
| 18 | Flux Charts | ATR como unidade universal | Refatoração | Alta | Adotado |
| 19 | Flux Charts | Multi-TF bias score | Implementação | Média | A confirmar |

---

## Stack técnica

### Bibliotecas Python

**Análise técnica:**
- **pandas_ta** — indicadores (ATR Wilder, ADX, RSI, BB, EMA, MACD)
- **pandas / numpy** — manipulação de DataFrames OHLCV

**SMC:**
- **smc_lib.py** (módulo interno) — Pine Script do LuxAlgo SMC portado para Python. BOS/ChoCH, Order Blocks, Fair Value Gaps

**Rede e exchanges:**
- **aiohttp** — fetch assíncrono de klines em paralelo (dezenas de tokens simultâneos)
- **requests** — fetch síncrono isolado no journal (reduz blast radius de falhas)
- **tradingview-ta** — BTC 4H como contexto macro

**Persistência:**
- **sqlite3** — `scan_log_v9.db` (observabilidade) e `atirador_journal_v9.db` (trades). JSONL append-only em paralelo para verdade bruta
- **json** — state e evidências serializadas

**Telegram:**
- **requests** — chamadas diretas à Telegram Bot API

### Infraestrutura

- **VM:** Oracle Cloud Ubuntu 22.04 (137.131.132.190)
- **Execução:** cron a cada 15 min → `deploy/run-scan.sh` → `python3 main.py`
- **Bot:** daemon systemd (`atirador-bot.service`) rodando `telegram_bot.py --daemon`
- **Exchanges:** OKX (primária) → Bitget → Gate.io (fallbacks automáticos)
- **Python:** 3.12+

---

## Observabilidade

Duas camadas de persistência alimentadas em cada rodada:

**`scan_log_v9.db`** — verdade operacional
- `rounds` — uma linha por rodada (timestamp, FGI, BTC regime, universo, tempo de execução)
- `token_evaluations` — setups avaliados por token, com evidências completas
- `near_misses` — setups que quase dispararam (observabilidade pura, não geram CALL)
- `events` — CALLs emitidas

**`atirador_journal_v9.db`** — forward testing
- `trades` — CALLs com status OPEN/WIN_TP1/WIN_TP2/WIN_TP3/LOSS_SL/EXPIRED, pnl ponderado pelas saídas parciais, max_runup e max_drawdown

### Comandos Telegram

- `/status` — saúde do sistema, última rodada, espaço em disco/memória
- `/scan` — dispara scan manual via GitHub Actions
- `/log_last` — detalhes da última rodada (funil + eventos)
- `/log_calls [Nd]` — CALLs dos últimos N dias com status
- `/perf` — Win Rate, Profit Factor, Expectancy das CALLs fechadas
- `/perf_by_setup` — performance individual de cada setup (split de confluências)
- `/trade SYMBOL` — status de trade aberto
- `/ajuda` — lista completa

---

## Desenvolvimento

Projeto desenvolvido em workflow colaborativo humano + IA:

- Arquitetura e decisões conceituais: alinhamento iterativo entre proprietário e Claude.ai
- Implementação: Claude Code executa briefings auto-contidos contra a API GitHub
- Revisão: diff revisado no Claude.ai antes de cada merge em `main`
- Merge: sempre manual pelo proprietário via GitHub UI

Ver `CLAUDE.md` para detalhes completos do workflow, regras não-negociáveis, convenções de código e a tabela de decisões estendida.

### Estrutura do repositório

```
Setup_Atirador/
├── config.py              Constantes, paths, timezone
├── exchanges.py           Klines, universo, fallbacks (OKX/Bitget/Gate.io)
├── smc_lib.py             Structure detection (LuxAlgo SMC portado de Pine Script)
├── regime.py              Classify regime (TREND_UP/TREND_DOWN/RANGE/SQUEEZE)
├── indicators.py          MarketContext unificado (wrapper pandas_ta)
├── risk.py                TradePlan, TradeState, update_trade_state (saídas parciais)
├── signals.py             Orquestrador multi-setup → SignalDecision
├── setups/                5 setups paralelos
│   ├── base.py
│   ├── rev_zone.py
│   ├── cont_pull.py
│   ├── break_range.py
│   ├── rev_exaust.py
│   └── breaker.py
├── state.py               Persistência de setups_history
├── logger.py              RoundLoggerV9 (Camada 1 — observabilidade)
├── journal.py             TradeJournalV9 (Camada 2 — forward testing)
├── main.py                Orquestrador principal
├── telegram.py            Notificações (CALL, heartbeat)
├── telegram_bot.py        Bot bidirecional (comandos Telegram)
├── health_report.py       Relatório de saúde via /health_export
├── diagnostics.py         Utilitário de debug
├── deploy/                Scripts de VM (run-scan, run-bot, systemd)
├── .github/workflows/     Actions (scan, analisar, telegram_bot)
└── tests/                 Suite pytest
```

### Tags de recuperação

- `v9-persistence-stable` — state/logger/journal v9 completos
- `v8-final` — último estado estável da v8
- `v7-pre-modular` — monolito v7 antes da modularização v8

---

## Licença

Projeto privado, uso pessoal. Não distribuído publicamente.
