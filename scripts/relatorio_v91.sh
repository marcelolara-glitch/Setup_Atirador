#!/bin/bash
# Relatório pós-merge v9.1 — risk estrutural + confidence binária
#
# Extrai estatísticas das rodadas do cron v9 a partir de um cutoff
# configurável (default: 24/04/2026 18:30 UTC, primeiro merge da v9.1).
#
# Uso:
#   ./scripts/relatorio_v91.sh                 # cutoff default
#   ./scripts/relatorio_v91.sh "2026-04-25 00:00:00"   # cutoff custom
#
# Saída:
#   - stdout (para visualização imediata)
#   - /tmp/relatorio_v91_YYYYMMDD_HHMM.txt (cópia persistida)
#
# Pressuposto: rodando na VM Setup Atirador (~/Setup_Atirador)
# com .venv ativável e bancos em logs/ e journal/.

set -uo pipefail

# Diretório do projeto (script é chamado de qualquer lugar)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# Ativa venv se existir (não obrigatório; sqlite3 é binário do sistema)
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate 2>/dev/null || true
fi

# Cutoff: argumento posicional ou default da v9.1
CUTOFF="${1:-2026-04-24 18:30:00}"

# Caminhos dos bancos e log
SCAN_DB="logs/scan_log_v9.db"
JOURNAL_DB="journal/atirador_journal_v9.db"
LOG_TODAY="logs/atirador_v9_$(date +%Y%m%d).log"

# Saída persistida
OUT="/tmp/relatorio_v91_$(date +%Y%m%d_%H%M).txt"

# Tee duplo: stdout + arquivo
exec > >(tee "$OUT") 2>&1

echo "========================================================="
echo "  RELATÓRIO PÓS-MERGE v9.1 — risk estrutural + confidence binária"
echo "  Gerado em: $(date '+%Y-%m-%d %H:%M:%S %Z') ($(date -u '+%H:%M UTC'))"
echo "  Cutoff de análise: $CUTOFF UTC"
echo "  Project: $PROJECT_DIR"
echo "========================================================="
echo ""

# Função utilitária: roda query SQL e captura saída ou mostra erro
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

echo "## 2. Schemas relevantes (referência)"
echo "-- scan_rounds:"
sqlite3 "$SCAN_DB" ".schema scan_rounds" 2>&1 | head -20
echo "-- round_events:"
sqlite3 "$SCAN_DB" ".schema round_events" 2>&1 | head -20
echo "-- near_misses:"
sqlite3 "$SCAN_DB" ".schema near_misses" 2>&1 | head -20
echo "-- trades (journal):"
sqlite3 "$JOURNAL_DB" ".schema trades" 2>&1 | head -25
echo ""

echo "## 3. Rodadas executadas (visão geral)"
sql "$SCAN_DB" "SELECT COUNT(*) AS rodadas, MIN(datetime(round_started_at)) AS primeira, MAX(datetime(round_started_at)) AS ultima, ROUND(AVG(round_duration_s),1) AS avg_dur_s, ROUND(AVG(universe_size),1) AS avg_universe, SUM(setups_triggered_total) AS triggered, SUM(calls_total) AS calls, SUM(near_miss_total) AS nearmiss, SUM(skip_total) AS skips FROM scan_rounds WHERE datetime(round_started_at) >= datetime('$CUTOFF');"
echo ""

echo "## 4. CALLs emitidos — detalhes completos"
sql "$SCAN_DB" "SELECT substr(timestamp,1,19) AS ts, symbol, direction AS dir, signal_tag, ROUND(entry_price,6) AS entry, ROUND(sl_price,6) AS sl, ROUND(tp1_price,6) AS tp1, ROUND(rr_tp1,2) AS rr1, ROUND(rr_tp2,2) AS rr2, ROUND(leverage,1) AS lev, regime FROM round_events WHERE event_type='CALL' AND datetime(timestamp) >= datetime('$CUTOFF') ORDER BY timestamp DESC;"
echo ""

echo "## 5. Distribuição CALLs por setup_tag"
sql "$SCAN_DB" "SELECT signal_tag AS setup_tag, COUNT(*) AS n, ROUND(AVG(rr_tp1),2) AS avg_rr1, ROUND(MIN(rr_tp1),2) AS min_rr1, ROUND(MAX(rr_tp1),2) AS max_rr1, ROUND(AVG(sl_distance_pct),2) AS avg_sl_pct, ROUND(AVG(leverage),1) AS avg_lev FROM round_events WHERE event_type='CALL' AND datetime(timestamp) >= datetime('$CUTOFF') GROUP BY signal_tag ORDER BY n DESC;"
echo ""

echo "## 6. Distribuição CALLs por regime + direção"
sql "$SCAN_DB" "SELECT regime, direction AS dir, COUNT(*) AS n, ROUND(AVG(rr_tp1),2) AS avg_rr1, ROUND(AVG(leverage),1) AS avg_lev FROM round_events WHERE event_type='CALL' AND datetime(timestamp) >= datetime('$CUTOFF') GROUP BY regime, direction ORDER BY n DESC;"
echo ""

echo "## 7. Distribuição R:R TP1 (faixas — diagnóstico estrutural vs fallback)"
sql "$SCAN_DB" "SELECT CASE WHEN rr_tp1 < 1.0 THEN 'A: < 1.00 (NAO DEVERIA ocorrer)' WHEN rr_tp1 < 1.05 THEN 'B: 1.00 (fallback puro R:R 1:1)' WHEN rr_tp1 < 1.5 THEN 'C: 1.01-1.49 (estrut moderado)' WHEN rr_tp1 < 2.0 THEN 'D: 1.50-1.99 (estrut bom)' ELSE 'E: >= 2.00 (estrut otimo)' END AS faixa_rr1, COUNT(*) AS n FROM round_events WHERE event_type='CALL' AND datetime(timestamp) >= datetime('$CUTOFF') GROUP BY faixa_rr1 ORDER BY faixa_rr1;"
echo ""

echo "## 8. Distribuição SL distance % (faixas — detecta clamp nos guard-rails)"
sql "$SCAN_DB" "SELECT CASE WHEN sl_distance_pct < 0.85 THEN 'A: clampou no sl_min (~0.8 ATR)' WHEN sl_distance_pct < 1.5 THEN 'B: 0.85-1.49% (apertado)' WHEN sl_distance_pct < 2.5 THEN 'C: 1.50-2.49% (medio)' WHEN sl_distance_pct < 4.0 THEN 'D: 2.50-3.99% (largo)' WHEN sl_distance_pct < 5.05 THEN 'E: 4.00-5.04% (proximo do max)' ELSE 'F: >= 5.05% (clampou no sl_max)' END AS faixa_sl, COUNT(*) AS n FROM round_events WHERE event_type='CALL' AND datetime(timestamp) >= datetime('$CUTOFF') GROUP BY faixa_sl ORDER BY faixa_sl;"
echo ""

echo "## 9. Símbolos com múltiplos CALLs (concentração)"
sql "$SCAN_DB" "SELECT symbol, COUNT(*) AS n_calls, GROUP_CONCAT(DISTINCT direction) AS dirs, GROUP_CONCAT(DISTINCT signal_tag) AS setups FROM round_events WHERE event_type='CALL' AND datetime(timestamp) >= datetime('$CUTOFF') GROUP BY symbol HAVING COUNT(*) > 1 ORDER BY n_calls DESC;"
echo ""

echo "## 10. Razão SKIP geral"
sql "$SCAN_DB" "SELECT skip_reason AS motivo, COUNT(*) AS n FROM near_misses WHERE datetime(timestamp) >= datetime('$CUTOFF') GROUP BY skip_reason ORDER BY n DESC;"
echo ""

echo "## 11. Near-misses recentes (últimos 15)"
sql "$SCAN_DB" "SELECT substr(timestamp,1,19) AS ts, symbol, direction AS dir, signal_tag, skip_reason FROM near_misses WHERE datetime(timestamp) >= datetime('$CUTOFF') ORDER BY timestamp DESC LIMIT 15;"
echo ""

echo "## 12. Forward test — distribuição por status"
sql "$JOURNAL_DB" "SELECT status, COUNT(*) AS n FROM trades WHERE datetime(opened_at) >= datetime('$CUTOFF') GROUP BY status ORDER BY n DESC;"
echo ""

echo "## 13. Trades em andamento (OPEN / PARTIAL)"
sql "$JOURNAL_DB" "SELECT symbol, signal_tag, direction AS dir, status, ROUND(entry_price,6) AS entry, ROUND(sl_price,6) AS sl, ROUND(tp1_price,6) AS tp1, ROUND(rr_tp1,2) AS rr1, substr(opened_at,1,16) AS opened FROM trades WHERE status IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') AND datetime(opened_at) >= datetime('$CUTOFF') ORDER BY opened_at DESC;"
echo ""

echo "## 14. Trades fechados — resultados"
sql "$JOURNAL_DB" "SELECT symbol, signal_tag, direction AS dir, status, ROUND(rr_tp1,2) AS rr1_planned, substr(opened_at,1,16) AS opened, substr(closed_at,1,16) AS closed FROM trades WHERE status NOT IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') AND datetime(opened_at) >= datetime('$CUTOFF') ORDER BY closed_at DESC;"
echo ""

echo "## 15. Heartbeats no log de hoje (últimos 15)"
if [ -f "$LOG_TODAY" ]; then
    grep -E "(Rodada conclu|FGI|BTC regime|Avaliados|Universo)" "$LOG_TODAY" 2>/dev/null | tail -15
else
    echo "(log $LOG_TODAY não encontrado)"
fi
echo ""

echo "## 16. Erros recentes no log"
if [ -f "$LOG_TODAY" ]; then
    grep -iE "(ERROR|Traceback|Exception)" "$LOG_TODAY" 2>/dev/null | tail -10 || echo "(sem errors)"
else
    echo "(log não encontrado)"
fi
echo ""

echo "## 17. Status do cron"
ls -la /etc/cron.d/atirador-scan-v9 2>&1
echo ""
echo "-- últimas 5 linhas de ~/cron-scan-v9.log:"
tail -5 ~/cron-scan-v9.log 2>&1
echo ""

echo "========================================================="
echo "FIM DO RELATÓRIO"
echo "Saída persistida: $OUT"
echo "========================================================="
