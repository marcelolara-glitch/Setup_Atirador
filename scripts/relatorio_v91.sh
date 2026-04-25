#!/bin/bash
# Relatório pós-merge v9.1 — risk estrutural + confidence binária.
#
# Schema real (inspecionado 24/04/2026):
#   scan_log_v9.db: rounds, round_events, near_misses, token_evaluations
#   atirador_journal_v9.db: trades
#
# Métricas derivadas (não existem como coluna; calculadas em SQL):
#   sl_distance_pct = abs(entry-sl)/entry*100
#   tp1_distance_pct = abs(tp1-entry)/entry*100
#   rr_tp1 = abs(tp1-entry)/abs(entry-sl)
#   rr_tp2 = abs(tp2-entry)/abs(entry-sl)
#   rr_tp3 = abs(tp3-entry)/abs(entry-sl)
#   sl_atr_mult = abs(entry-sl)/atr_value (detecta clamp ~0.8 sl_min)
#
# Uso:
#   ./scripts/relatorio_v91.sh                            # cutoff default
#   ./scripts/relatorio_v91.sh "2026-04-25 00:00:00"      # custom

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate 2>/dev/null || true
fi

CUTOFF="${1:-2026-04-24 18:30:00}"
SCAN_DB="logs/scan_log_v9.db"
JOURNAL_DB="journal/atirador_journal_v9.db"
LOG_TODAY="logs/atirador_v9_$(date +%Y%m%d).log"
OUT="/tmp/relatorio_v91_$(date +%Y%m%d_%H%M).txt"

exec > >(tee "$OUT") 2>&1

echo "========================================================="
echo "  RELATÓRIO PÓS-MERGE v9.1 — risk estrutural + confidence binária"
echo "  Gerado em: $(date '+%Y-%m-%d %H:%M:%S %Z') ($(date -u '+%H:%M UTC'))"
echo "  Cutoff de análise: $CUTOFF"
echo "  Project: $PROJECT_DIR"
echo "========================================================="
echo ""

sql() {
    local db="$1"
    local query="$2"
    if [ ! -f "$db" ]; then
        echo "(banco $db não encontrado)"
        return
    fi
    sqlite3 -header -column "$db" "$query" 2>&1
}

echo "## 1. HEAD do main e últimos commits"
git log --oneline -8 2>&1 || echo "(git falhou)"
echo ""

echo "## 2. Visão geral das rodadas (tabela rounds)"
sql "$SCAN_DB" "SELECT COUNT(*) AS n_rodadas, MIN(ts) AS primeira, MAX(ts) AS ultima, ROUND(AVG(exec_secs),1) AS avg_exec_s, ROUND(AVG(universe),1) AS avg_universe, ROUND(AVG(tokens_evaluated),1) AS avg_avaliados, SUM(setups_triggered_total) AS triggered_total, SUM(calls_emitted) AS calls_total, SUM(near_misses_count) AS nearmiss_total, SUM(btc_filter_blocked) AS btc_blocked FROM rounds WHERE ts >= '$CUTOFF';"
echo ""

echo "## 3. Contexto de cada rodada"
sql "$SCAN_DB" "SELECT substr(ts,1,19) AS ts, fgi, btc_regime, ROUND(btc_change_pct_15m,2) AS btc_chg, universe AS univ, tokens_evaluated AS aval, setups_triggered_total AS trig, calls_emitted AS calls, near_misses_count AS nm, ROUND(exec_secs,1) AS dur FROM rounds WHERE ts >= '$CUTOFF' ORDER BY ts DESC LIMIT 30;"
echo ""

echo "## 4. CALLs emitidos (round_events) — visão summary"
sql "$SCAN_DB" "SELECT substr(ts,1,19) AS ts, symbol, direction AS dir, signal_tag, ROUND(leverage,1) AS lev FROM round_events WHERE type='CALL' AND ts >= '$CUTOFF' ORDER BY ts DESC;"
echo ""

echo "## 5. CALLs com TODOS os detalhes (vindo de trades)"
sql "$JOURNAL_DB" "SELECT substr(timestamp,1,19) AS ts, symbol, direction AS dir, setup_name, ROUND(entry_price,8) AS entry, ROUND(sl_price,8) AS sl, ROUND(tp1_price,8) AS tp1, ROUND(tp2_price,8) AS tp2, ROUND(tp3_price,8) AS tp3, ROUND(ABS(entry_price-sl_price)/entry_price*100,2) AS sl_pct, ROUND(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price),2) AS rr1, ROUND(ABS(tp2_price-entry_price)/ABS(entry_price-sl_price),2) AS rr2, ROUND(ABS(tp3_price-entry_price)/ABS(entry_price-sl_price),2) AS rr3, ROUND(leverage,1) AS lev, regime_at_entry AS regime, status FROM trades WHERE timestamp >= '$CUTOFF' ORDER BY timestamp DESC;"
echo ""

echo "## 6. Distribuição R:R TP1 (faixas — diagnóstico fallback vs estrutural)"
sql "$JOURNAL_DB" "SELECT CASE WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 0.99 THEN 'A: < 0.99 (NAO DEVERIA)' WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 1.05 THEN 'B: ~1.00 (FALLBACK puro 1:1)' WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 1.5 THEN 'C: 1.05-1.49 (estrut moderado)' WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 2.0 THEN 'D: 1.50-1.99 (estrut bom)' ELSE 'E: >= 2.00 (estrut otimo)' END AS faixa_rr1, COUNT(*) AS n FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY faixa_rr1 ORDER BY faixa_rr1;"
echo ""

echo "## 7. Distribuição SL distance % vs ATR (faixas — detecta clamp em guard-rails)"
sql "$JOURNAL_DB" "SELECT CASE WHEN atr_value IS NULL OR atr_value=0 THEN 'X: sem atr_value' WHEN ABS(entry_price-sl_price)/atr_value < 0.85 THEN 'A: SL ~0.8 ATR (clamp sl_min)' WHEN ABS(entry_price-sl_price)/atr_value < 1.2 THEN 'B: SL 0.85-1.19 ATR' WHEN ABS(entry_price-sl_price)/atr_value < 1.6 THEN 'C: SL 1.20-1.59 ATR' ELSE 'D: SL >= 1.60 ATR (estrut distante)' END AS faixa_sl_atr, COUNT(*) AS n, ROUND(AVG(ABS(entry_price-sl_price)/entry_price*100),2) AS avg_sl_pct FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY faixa_sl_atr ORDER BY faixa_sl_atr;"
echo ""

echo "## 8. Distribuição SL distance % absoluto (clamp em sl_max=5%?)"
sql "$JOURNAL_DB" "SELECT CASE WHEN ABS(entry_price-sl_price)/entry_price*100 < 0.85 THEN 'A: < 0.85% (apertado)' WHEN ABS(entry_price-sl_price)/entry_price*100 < 1.5 THEN 'B: 0.85-1.49% (medio-)' WHEN ABS(entry_price-sl_price)/entry_price*100 < 2.5 THEN 'C: 1.50-2.49% (medio)' WHEN ABS(entry_price-sl_price)/entry_price*100 < 4.0 THEN 'D: 2.50-3.99% (largo)' WHEN ABS(entry_price-sl_price)/entry_price*100 < 5.05 THEN 'E: 4.00-5.04% (proximo do max)' ELSE 'F: >= 5.05% (CLAMP sl_max!)' END AS faixa_sl_pct, COUNT(*) AS n FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY faixa_sl_pct ORDER BY faixa_sl_pct;"
echo ""

echo "## 9. Distribuição CALLs por setup (com R:R médio)"
sql "$JOURNAL_DB" "SELECT setup_name, COUNT(*) AS n, ROUND(AVG(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS avg_rr1, ROUND(MIN(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS min_rr1, ROUND(MAX(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS max_rr1, ROUND(AVG(ABS(entry_price-sl_price)/entry_price*100),2) AS avg_sl_pct, ROUND(AVG(leverage),1) AS avg_lev FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY setup_name ORDER BY n DESC;"
echo ""

echo "## 10. Distribuição CALLs por regime + direção"
sql "$JOURNAL_DB" "SELECT regime_at_entry AS regime, direction AS dir, COUNT(*) AS n, ROUND(AVG(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS avg_rr1, ROUND(AVG(leverage),1) AS avg_lev FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY regime_at_entry, direction ORDER BY n DESC;"
echo ""

echo "## 11. Símbolos com múltiplos CALLs (concentração)"
sql "$JOURNAL_DB" "SELECT symbol, COUNT(*) AS n, GROUP_CONCAT(DISTINCT direction) AS dirs, GROUP_CONCAT(DISTINCT setup_name) AS setups FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY symbol HAVING COUNT(*) > 1 ORDER BY n DESC;"
echo ""

echo "## 12. Forward test — distribuição por status"
sql "$JOURNAL_DB" "SELECT status, COUNT(*) AS n, ROUND(AVG(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS avg_rr1_planned, ROUND(AVG(pnl_pct),2) AS avg_pnl_pct FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY status ORDER BY n DESC;"
echo ""

echo "## 13. Trades em andamento (OPEN / PARTIAL)"
sql "$JOURNAL_DB" "SELECT symbol, setup_name, direction AS dir, status, ROUND(entry_price,8) AS entry, ROUND(current_sl,8) AS curr_sl, tp1_hit, tp2_hit, tp3_hit, ROUND(position_remaining,2) AS pos_rem, ROUND(max_runup,2) AS max_up, ROUND(max_drawdown,2) AS max_dn, substr(timestamp,1,16) AS opened FROM trades WHERE status IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') AND timestamp >= '$CUTOFF' ORDER BY timestamp DESC;"
echo ""

echo "## 14. Trades fechados — resultados"
sql "$JOURNAL_DB" "SELECT symbol, setup_name, direction AS dir, status, ROUND(pnl_pct,2) AS pnl_pct, ROUND(max_runup,2) AS max_up, ROUND(max_drawdown,2) AS max_dn, substr(timestamp,1,16) AS opened, substr(exit_time,1,16) AS closed FROM trades WHERE status NOT IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') AND timestamp >= '$CUTOFF' ORDER BY exit_time DESC;"
echo ""

echo "## 15. SKIPs por motivo (token_evaluations)"
sql "$SCAN_DB" "SELECT skip_reason AS motivo, COUNT(*) AS n FROM token_evaluations WHERE action='SKIP' AND ts >= '$CUTOFF' AND skip_reason IS NOT NULL GROUP BY skip_reason ORDER BY n DESC;"
echo ""

echo "## 16. Near-misses recentes (último ranking de quem quase passou)"
sql "$SCAN_DB" "SELECT substr(ts,1,19) AS ts, symbol, closest_setup_name AS setup, closest_direction AS dir, closest_regime AS regime, closest_confidence AS conf FROM near_misses WHERE ts >= '$CUTOFF' ORDER BY ts DESC LIMIT 15;"
echo ""

echo "## 17. Heartbeats no log de hoje"
if [ -f "$LOG_TODAY" ]; then
    grep -E "(Rodada conclu|FGI|BTC regime|Avaliados|Universo|CALL emitido|CALLs emitidos)" "$LOG_TODAY" 2>/dev/null | tail -20
else
    echo "(log $LOG_TODAY não encontrado)"
fi
echo ""

echo "## 18. Erros recentes no log"
if [ -f "$LOG_TODAY" ]; then
    grep -iE "(ERROR|Traceback|Exception)" "$LOG_TODAY" 2>/dev/null | tail -10 || echo "(sem errors)"
else
    echo "(log não encontrado)"
fi
echo ""

echo "## 19. Status do cron"
ls -la /etc/cron.d/atirador-scan-v9 2>&1
echo ""
echo "-- últimas 5 linhas de ~/cron-scan-v9.log:"
tail -5 ~/cron-scan-v9.log 2>&1
echo ""

echo "========================================================="
echo "FIM DO RELATÓRIO"
echo "Saída persistida em: $OUT"
echo "========================================================="
