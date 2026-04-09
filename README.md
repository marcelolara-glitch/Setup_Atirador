# Setup Atirador v8.0.0

Scanner automatizado de perpetual futures (crypto) com emissão de
alertas LONG/SHORT/QUASE via Telegram.

## Arquitetura

Sistema modular com 9 módulos independentes. Cada módulo tem
responsabilidade única — manutenção e evolução cirúrgicas.

| Módulo | Responsabilidade |
|---|---|
| `config.py` | Constantes, parâmetros, URLs — UM lugar só |
| `exchanges.py` | Fetch de klines e universo (Bitget→OKX fallback) |
| `gates.py` | TradingView Scanner API |
| `indicators.py` | Análise técnica: swing, OB, zonas (IndicatorParams injetável) |
| `scoring.py` | Checks A, B, C (rejeição, estrutura, força) |
| `signals.py` | Pipeline por token, cálculo de trade params |
| `telegram.py` | Formatação e envio de mensagens |
| `state.py` | Persistência de estado entre rodadas |
| `main.py` | Orquestração, entry point do cron |

## Infraestrutura

- **VM:** Oracle Cloud Ubuntu 22.04 (IP 137.131.132.190)
- **Execução:** cron `*/30 * * * *` → `deploy/run-scan.sh` → `python3 main.py`
- **Bot:** daemon systemd → `telegram_bot.py` (long-polling)
- **Dados:** OKX (universo) | Bitget→OKX (klines) | TradingView (indicadores)
- **Observabilidade:** `scan_log.jsonl` + `scan_log.db` + `atirador_journal.db`

## Pipeline de decisão

```
Universo OKX → Gate 4H (macro) → Zona (OB/S&R) → Check A + B + C (15m)
                                                         ↓
                                              CALL / QUASE / descarte
```

## Proteção de versões

- Tag `v7-pre-modular` — estado do monolito v7 antes da modularização
- Branch `modular-v8` — histórico completo da refatoração v8

## Dependências

```
aiohttp>=3.8.0
requests
numpy
tradingview-ta
```
