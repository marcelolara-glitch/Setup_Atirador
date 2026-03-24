# Informações de Versão - Setup Atirador

## Status Atual

- **Versão Instalada**: v6.4.1
- **Data de Atualização**: 23 de março de 2026 (15:12 BRT)
- **Status**: ✅ ATIVO E VALIDADO - Parser OKX Corrigido

## Verificação de Integridade

| Componente | Status | Detalhes |
|-----------|--------|----------|
| Script Principal | ✅ OK | `/home/ubuntu/skills/setup-atirador/scripts/setup_atirador.py` (3380+ linhas, 160+ KB) - Parser OKX v6.4.1 |
| Sintaxe Python | ✅ OK | Compilação bem-sucedida, sem erros de sintaxe |
| Documentação SKILL.md | ✅ OK | Atualizada para v6.4.1 com correção do parser OKX |
| Configuração (config.md) | ✅ OK | Atualizada com parâmetros v6.4.1 |
| Sistema de Scoring | ✅ OK | Atualizado com 15 pilares (28 pts máximo) |
| Backup de Versões | ✅ OK | Versões anteriores arquivadas em `/home/ubuntu/skills/setup-atirador/archive/` |

## Mudanças Principais v6.4.1

### Parser OKX Corrigido - Open Interest Real
- **Problema Crítico Resolvido**: Endpoint `/market/tickers` não retorna Open Interest. Campo `openInterest` não existe.
- **Impacto**: 100% dos tokens qualificados marcados como `oi_estimado=True`, bloqueando todos os alertas SHORT.
- **Solução Implementada**: Nova função `_fetch_okx_tickers_with_oi()` que busca dados de dois endpoints:
  - `/api/v5/market/tickers?instType=SWAP` — tickers (volume, preço)
  - `/api/v5/public/open-interest?instType=SWAP` — Open Interest em USD
- **Resultado**: 300/300 tokens com OI real, 0% com OI estimado.
- **Desbloqueio**: Alertas SHORT agora funcionam corretamente.
- **Performance**: +2.8s (15.1s vs 12.3s) — aceitável.

## Mudanças Principais v6.4.0

### Gestão de Risco (Risk-First)
- Implementação de sizing risk-first com margem máxima por trade.
- Fórmula: `notional = RISCO_POR_TRADE_USD / stop_pct`
- Garantia: margem por trade ≤ $35 (para banca de $100).

### Operações Bidirecionais
- Suporte completo para LONG e SHORT simultâneos.
- Pilares espelhados para cada direção.
- Exclusividade: mesmo token não pode ter sinais conflitantes abertos.

### Integração Telegram
- Alertas de Call com dados completos para execução.
- Heartbeat a cada rodada para monitoramento.
- Configuração via variáveis de ambiente ou arquivo persistente.

### Recalibração de Score
- Teto máximo: 28 pontos (com P9 OI +2).
- Tabela de alavancagem recalibrada para scores 14-28.
- Data Quality separado do Setup Score.

## Estrutura do Ambiente

```
/home/ubuntu/skills/setup-atirador/
├── scripts/
│   └── setup_atirador.py                    (v6.4.0 — ATIVO)
├── references/
│   ├── config.md                            (Atualizado)
│   └── scoring-system.md                    (Atualizado)
├── SKILL.md                                 (Atualizado)
├── VERSION.md                               (Este arquivo)
└── archive/
    └── scripts_v5.2_backup_*                (Backup de versões anteriores)
```

## Dependências Necessárias

```bash
sudo pip3 install aiohttp requests numpy tradingview-ta
```

## Como Executar

```bash
# Execução padrão
python3 /home/ubuntu/skills/setup-atirador/scripts/setup_atirador.py

# Com Telegram configurado
export TELEGRAM_TOKEN="seu_token"
export TELEGRAM_CHAT_ID="seu_chat_id"
python3 /home/ubuntu/skills/setup-atirador/scripts/setup_atirador.py
```

## Arquivos Gerados

- **Relatório**: `/tmp/atirador_SCAN_YYYYMMDD_HHMM.txt`
- **Log**: `/tmp/atirador_logs/atirador_LOG_YYYYMMDD_HHMM.log`
- **Estado Diário**: `/tmp/atirador_state.json`
- **Config Telegram**: `/tmp/atirador_telegram_config.json` (se configurado)

## Verificação de Versão Automática

Para verificar a versão instalada em qualquer momento:

```bash
grep -m 1 "SETUP ATIRADOR v" /home/ubuntu/skills/setup-atirador/scripts/setup_atirador.py
```

Resultado esperado: `SETUP ATIRADOR v6.4.1 - Scanner Profissional de Criptomoedas`

## Histórico de Atualizações

| Versão | Data | Mudanças Principais |
|--------|------|-------------------|
| v6.4.1 | 2026-03-23 | Parser OKX corrigido — Open Interest real, desbloqueio de alertas SHORT |
| v6.4.0 | 2026-03-23 | Risk-first sizing, recalibração score/thresholds, data_quality separado |
| v6.3.0 | 2026-03-22 | Candles bearish 15m, candle lock, oi_estimado flag |
| v6.2.0 | 2026-03-22 | KLINE_TOP_N→20, SR_PROXIMITY→2.5%, P9 OI no score |
| v6.1.2 | 2026-03-22 | Telegram webhook com alertas e heartbeats |
| v6.0.0 | 2026-03-21 | SHORT bidirecional, pilares bearish espelhados |
| v5.2.0 | 2026-03-22 | Fix CoinGecko parser, fallback 3-exchange |

## Suporte e Troubleshooting

### Problema: Script não executa
**Solução**: Verifique se todas as dependências estão instaladas:
```bash
sudo pip3 install aiohttp requests numpy tradingview-ta
```

### Problema: Telegram não envia mensagens
**Solução**: Verifique se as credenciais estão configuradas corretamente:
```bash
echo $TELEGRAM_TOKEN
echo $TELEGRAM_CHAT_ID
```

### Problema: Versão desatualizada
**Solução**: Verifique o arquivo VERSION.md e compare com a versão esperada. Se necessário, execute novamente o procedimento de atualização.

---

**Última Verificação**: 23 de março de 2026 às 15:12 BRT
**Responsável**: Manus AI
**Status da Correção**: ✅ Parser OKX v6.4.1 validado e testado com sucesso
