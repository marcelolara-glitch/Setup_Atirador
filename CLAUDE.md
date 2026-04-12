# Setup Atirador — Contexto para Claude Code

## ARQUITETURA ATUAL — v8.1.2 (produção)

### Execução (Oracle Cloud VM)
- **VM**: Ubuntu 22.04, IP 137.131.132.190
- **Scan**: cron `/etc/cron.d/atirador-scan` → `2-59/30 * * * *`
  → `deploy/run-scan.sh` → `python3 main.py`
  - Faz `git pull origin main` antes de cada rodada
  - Estado persistido em `states/atirador_state.json`
- **Bot Telegram**: systemd `atirador-bot.service`
  → `python3 telegram_bot.py --daemon` (long-polling)

### GitHub
- Repositório: `marcelolara-glitch/Setup_Atirador`, branch `main`
- `scan.yml` / `telegram_bot.yml`: apenas `workflow_dispatch`
- `/scan` e `/analisar` disparam `workflow_dispatch` — rodam em
  máquina efêmera, **não alimentam os bancos SQLite da VM**

---

## MÓDULOS (9 independentes)

| Módulo | Responsabilidade |
|---|---|
| `config.py` | Todas as constantes e VERSION (única fonte) |
| `exchanges.py` | Klines + universo (OKX primário, Bitget fallback) |
| `gates.py` | TradingView Scanner API (4H/1H/15m) |
| `indicators.py` | Análise técnica + `IndicatorParams` injetável |
| `scoring.py` | Checks A, B, C |
| `signals.py` | Pipeline por token + `calc_trade_params` |
| `telegram.py` | Formatação e envio de mensagens |
| `state.py` | Persistência entre rodadas |
| `main.py` | Orquestração + entry point do cron |

Preexistentes inalterados: `logger.py`, `journal.py`, `telegram_bot.py`

---

## LÓGICA DE DECISÃO
Universo OKX → Gate 4H (BUY/SELL) → Zona (OB/S&R) → Checks 15m
| Check | Critério |
|---|---|
| A — Rejeição | Wick inferior (LONG) ou superior (SHORT) ≥ 40% do range |
| B — Estrutura | ≥ 6/8 velas fechadas direcionais |
| C — Força | 4 sub-checks (BB, Volume, CVD proxy, OI trend), 0–4 pts |

**CALL**: A ✅ + B ✅ + C ≥ threshold (2 zona ALTA, 3 zona MEDIA/BASE)
**QUASE**: A ✅ mas B ❌ ou C abaixo do threshold

---

## OBSERVABILIDADE

**Camada 1 — `logger.py`**: grava cada rodada em `logs/scan_log.jsonl`
e `logs/scan_log.db` (SQLite)

**Camada 2 — `journal.py`**: registra CALLs e QUASEs como forward test.
- CALLs: `is_hypothetical=0` — métricas operacionais reais
- QUASEs: `is_hypothetical=1` — calibração de threshold
- `pillars_json` contém: `check_a_ok`, `check_b_ok`, `check_c_total`,
  `check_c_thr`, `zona_qualidade` + sub-checks de C

---

## INFRAESTRUTURA

- **Swap**: 1GB swapfile em `/etc/fstab` (necessário — VM tem 1GB RAM)
- **Env**: `~/.env_atirador` (TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
  GITHUB_TOKEN, GITHUB_REPOSITORY)
- **Bancos**: `logs/scan_log.db`, `journal/atirador_journal.db`
- **Estado**: `states/atirador_state.json`

---

## REGRAS DE DESENVOLVIMENTO — LEIA ANTES DE QUALQUER AÇÃO

### 1. Fluxo obrigatório
Implementação → commit na feature branch → PR aberto → PARAR
**Nunca fazer merge em `main` sem instrução explícita do produto.**
**Nunca fazer commit direto em `main`.**

### 2. Aprovação de merge
- Após abrir o PR, reportar o que foi feito e aguardar instrução
- Merge só ocorre quando o produto disser explicitamente:
  "pode fazer o merge" ou "aprovado para merge"
- Diff só é mostrado se explicitamente solicitado no briefing

### 3. Versionamento
- VERSION só é incrementada após aprovação explícita de merge
- Toda nova versão atualiza TODAS as ocorrências operacionais:
  header, constante `VERSION`, docstrings, scan logs, heartbeat,
  state JSON, changelog
- Versões nunca pulam — sempre sequencial

### 4. Escopo cirúrgico
- Implementar exatamente o que o briefing descreve
- Nunca alterar módulos fora do escopo definido
- Nunca "melhorar" código adjacente sem instrução explícita

### 5. Proteções — nunca delegadas ao Claude Code
- Tags de versão no GitHub
- Backups físicos
- Merges em `main` sem aprovação explícita do produto

---

## REFERÊNCIAS E HISTÓRICO

### Arquivos de referência
- `CONTRATO_V8.md` — assinaturas completas de todas as funções
  cross-módulo. Leia antes de qualquer alteração em interfaces públicas.

### Bugs críticos resolvidos na v8.0.0 — nunca regredir
| # | Bug | Módulo |
|---|---|---|
| 1 | `fetch_perpetuals` retornava `int` onde `run_scan_async` esperava `str` | `exchanges.py` |
| 2 | `ALAV_POR_SCORE.get()` com chaves tuple — sempre retornava default | `config.py` |
| 3 | `_get_nearest_resistance_zone` chamava `analyze_resistance_1h` sem `current_price` | `signals.py` |
| 4 | `score_oi_trend` lia `state["score_history"][sym]["oi_history"]` mas `update_score_history` escrevia em `state["oi_history"][sym]` | `state.py` |

### Constantes críticas — imutáveis
- `BITGET_PRODUCT_TYPE = "USDT-FUTURES"` (com hífen) — nunca alterar
- Todas as constantes vivem exclusivamente em `config.py`
- URLs e parâmetros de API nunca são strings literais inline

---

## CONTEXTO DE SESSÃO
O escopo de cada sessão é definido no briefing entregue pelo produto.
Roadmap e prioridades são mantidos externamente pelo produto.