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

# Cutoffs UTC dos merges dos PRs v9.1.1 (extraídos do git log)
PR113_CUTOFF="2026-04-25T02:37:33"   # tp1_source/sl_source
PR114_CUTOFF="2026-04-25T02:47:15"   # trailing SL timestamps
PR115_CUTOFF="2026-04-25T02:59:49"   # near_misses.conditions fix

exec > >(tee "$OUT") 2>&1

echo "========================================================="
echo "  RELATÓRIO PÓS-MERGE v9.1.1 — observabilidade SMC + trailing + near_miss fix"
echo "  Cobre: PRs #113 (tp1/sl_source), #114 (trailing), #115 (near_miss conds)"
echo "  Cutoffs UTC: PR113=$PR113_CUTOFF | PR114=$PR114_CUTOFF | PR115=$PR115_CUTOFF"
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

echo "## 5. CALLs com TODOS os detalhes (vindo de trades + tp1/sl_source)"
sql "$JOURNAL_DB" "SELECT substr(timestamp,1,19) AS ts, symbol, direction AS dir, setup_name, ROUND(entry_price,8) AS entry, ROUND(sl_price,8) AS sl, ROUND(tp1_price,8) AS tp1, ROUND(tp2_price,8) AS tp2, ROUND(tp3_price,8) AS tp3, ROUND(ABS(entry_price-sl_price)/entry_price*100,2) AS sl_pct, ROUND(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price),2) AS rr1, ROUND(ABS(tp2_price-entry_price)/ABS(entry_price-sl_price),2) AS rr2, ROUND(ABS(tp3_price-entry_price)/ABS(entry_price-sl_price),2) AS rr3, ROUND(leverage,1) AS lev, regime_at_entry AS regime, status, COALESCE(json_extract(evidence_json,'\$.tp1_source'),'pré-PR113') AS tp1_src, COALESCE(json_extract(evidence_json,'\$.sl_source'),'pré-PR113') AS sl_src FROM trades WHERE timestamp >= '$CUTOFF' ORDER BY timestamp DESC;"
echo ""

echo "## 6. Distribuição R:R TP1 (faixas — diagnóstico fallback vs estrutural)"
sql "$JOURNAL_DB" "SELECT CASE WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 0.99 THEN 'A: < 0.99 (NAO DEVERIA)' WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 1.05 THEN 'B: ~1.00 (FALLBACK puro 1:1)' WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 1.5 THEN 'C: 1.05-1.49 (estrut moderado)' WHEN ABS(tp1_price-entry_price)/ABS(entry_price-sl_price) < 2.0 THEN 'D: 1.50-1.99 (estrut bom)' ELSE 'E: >= 2.00 (estrut otimo)' END AS faixa_rr1, COUNT(*) AS n FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY faixa_rr1 ORDER BY faixa_rr1;"
echo ""

echo "## 6b. PR#113 — Distribuição R:R TP1 via token_evaluations.trade_plan (cobertura completa)"
sql "$SCAN_DB" "SELECT CASE WHEN CAST(json_extract(trade_plan,'\$.risk_reward_tp1') AS REAL) < 0.95 THEN 'A: < 0.95 (sub-fallback?)' WHEN CAST(json_extract(trade_plan,'\$.risk_reward_tp1') AS REAL) BETWEEN 0.95 AND 1.05 THEN 'B: ~1.00 (FALLBACK puro 1:1)' WHEN CAST(json_extract(trade_plan,'\$.risk_reward_tp1') AS REAL) < 1.5 THEN 'C: 1.05-1.49 (estrut moderado)' WHEN CAST(json_extract(trade_plan,'\$.risk_reward_tp1') AS REAL) < 2.0 THEN 'D: 1.50-1.99 (estrut bom)' ELSE 'E: >= 2.00 (estrut muito bom)' END AS faixa_rr1, COUNT(*) AS n FROM token_evaluations WHERE action='CALL' AND ts >= '$CUTOFF' AND trade_plan IS NOT NULL GROUP BY faixa_rr1 ORDER BY faixa_rr1;"
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

echo "## 9b.1 PR#113 — distribuição tp1_source × sl_source (CALLs)"
sql "$SCAN_DB" "SELECT json_extract(trade_plan,'\$.tp1_source') AS tp1_src, json_extract(trade_plan,'\$.sl_source') AS sl_src, COUNT(*) AS n FROM token_evaluations WHERE action='CALL' AND ts >= '$CUTOFF' AND json_extract(trade_plan,'\$.tp1_source') IS NOT NULL GROUP BY tp1_src, sl_src ORDER BY n DESC;"
echo ""

echo "## 9b.2 PR#113 — % TP1 estrutural por setup (rev_zone usa estrutura?)"
sql "$SCAN_DB" "SELECT signal_tag AS setup, COUNT(*) AS total, SUM(CASE WHEN json_extract(trade_plan,'\$.tp1_source') LIKE 'structural_%' THEN 1 ELSE 0 END) AS estrutural, SUM(CASE WHEN json_extract(trade_plan,'\$.tp1_source')='fallback_1r' THEN 1 ELSE 0 END) AS fallback_1r, ROUND(100.0*SUM(CASE WHEN json_extract(trade_plan,'\$.tp1_source') LIKE 'structural_%' THEN 1 ELSE 0 END)/COUNT(*),1) AS pct_estrutural FROM token_evaluations WHERE action='CALL' AND ts >= '$CUTOFF' AND json_extract(trade_plan,'\$.tp1_source') IS NOT NULL GROUP BY signal_tag ORDER BY total DESC;"
echo ""

echo "## 9b.3 PR#113 — % SL clampado por regime (guard-rails colando?)"
sql "$SCAN_DB" "SELECT regime, COUNT(*) AS total, SUM(CASE WHEN json_extract(trade_plan,'\$.sl_source')='clamp_atr_min' THEN 1 ELSE 0 END) AS clamp_atr_min, SUM(CASE WHEN json_extract(trade_plan,'\$.sl_source')='clamp_pct_max' THEN 1 ELSE 0 END) AS clamp_pct_max, ROUND(100.0*SUM(CASE WHEN json_extract(trade_plan,'\$.sl_source') LIKE 'clamp_%' THEN 1 ELSE 0 END)/COUNT(*),1) AS pct_clampado FROM token_evaluations WHERE action='CALL' AND ts >= '$CUTOFF' AND json_extract(trade_plan,'\$.sl_source') IS NOT NULL GROUP BY regime ORDER BY total DESC;"
echo ""

echo "## 9c.1 Saúde dos 5 setups via all_setup_results (avaliados x triggered, últimas 24h)"
sql "$SCAN_DB" "SELECT json_extract(s.value,'\$.setup_name') AS setup, COUNT(*) AS avaliados, SUM(CASE WHEN json_extract(s.value,'\$.triggered')=1 THEN 1 ELSE 0 END) AS triggered_total, SUM(CASE WHEN json_extract(s.value,'\$.evidence.skip_reason') IS NOT NULL THEN 1 ELSE 0 END) AS com_skip_reason FROM token_evaluations t, json_each(t.all_setup_results) s WHERE t.ts >= datetime('now','-24 hours') AND t.all_setup_results IS NOT NULL GROUP BY setup ORDER BY triggered_total DESC;"
echo ""

echo "## 9c.2 Top motivos de skip por setup (responde por que rev_exaust/break_range são raros)"
sql "$SCAN_DB" "SELECT json_extract(s.value,'\$.setup_name') AS setup, json_extract(s.value,'\$.evidence.skip_reason') AS motivo, COUNT(*) AS n FROM token_evaluations t, json_each(t.all_setup_results) s WHERE t.ts >= datetime('now','-24 hours') AND t.all_setup_results IS NOT NULL AND json_extract(s.value,'\$.evidence.skip_reason') IS NOT NULL GROUP BY setup, motivo ORDER BY setup, n DESC LIMIT 30;"
echo ""

echo "## 10. Distribuição CALLs por regime + direção"
sql "$JOURNAL_DB" "SELECT regime_at_entry AS regime, direction AS dir, COUNT(*) AS n, ROUND(AVG(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS avg_rr1, ROUND(AVG(leverage),1) AS avg_lev FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY regime_at_entry, direction ORDER BY n DESC;"
echo ""

echo "## 10b.1 ALERTAS — bug numérico R:R 1.00 < 1.0 (comparação de float)"
sql "$SCAN_DB" "SELECT skip_reason, COUNT(*) AS n, MIN(ts) AS primeiro, MAX(ts) AS ultimo FROM token_evaluations WHERE action='SKIP' AND ts >= '$CUTOFF' AND skip_reason LIKE 'invalid_trade_plan:R:R TP1 abaixo do mínimo (1.0% < 1.0)%' GROUP BY skip_reason;"
echo ""

echo "## 10b.2 ALERTAS — bug Preços incoerentes (SL/TP fora de ordem)"
sql "$SCAN_DB" "SELECT symbol, COUNT(*) AS n, MIN(ts) AS primeiro, MAX(ts) AS ultimo FROM token_evaluations WHERE action='SKIP' AND ts >= '$CUTOFF' AND skip_reason LIKE 'invalid_trade_plan:Preços incoerentes%' GROUP BY symbol ORDER BY n DESC;"
echo ""

echo "## 10b.3 ALERTAS — sample dos bugs acima (trade_plan que disparou)"
sql "$SCAN_DB" "SELECT substr(ts,1,19) AS ts, symbol, skip_reason, substr(trade_plan,1,250) AS trade_plan_truncated FROM token_evaluations WHERE action='SKIP' AND ts >= '$CUTOFF' AND (skip_reason LIKE 'invalid_trade_plan:Preços incoerentes%' OR skip_reason LIKE 'invalid_trade_plan:R:R TP1 abaixo do mínimo (1.0%') ORDER BY ts DESC LIMIT 6;"
echo ""

echo "## 11. Símbolos com múltiplos CALLs (concentração)"
sql "$JOURNAL_DB" "SELECT symbol, COUNT(*) AS n, GROUP_CONCAT(DISTINCT direction) AS dirs, GROUP_CONCAT(DISTINCT setup_name) AS setups FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY symbol HAVING COUNT(*) > 1 ORDER BY n DESC;"
echo ""

echo "## 12. Forward test — distribuição por status"
sql "$JOURNAL_DB" "SELECT status, COUNT(*) AS n, ROUND(AVG(ABS(tp1_price-entry_price)/ABS(entry_price-sl_price)),2) AS avg_rr1_planned, ROUND(AVG(pnl_pct),2) AS avg_pnl_pct FROM trades WHERE timestamp >= '$CUTOFF' GROUP BY status ORDER BY n DESC;"
echo ""

echo "## 12b.1 PR#114 — saúde global do trailing SL (sl_moved_to_be_at / tp1_at)"
sql "$JOURNAL_DB" "SELECT SUM(CASE WHEN sl_moved_to_be_at IS NOT NULL THEN 1 ELSE 0 END) AS sl_to_be, SUM(CASE WHEN sl_moved_to_tp1_at IS NOT NULL THEN 1 ELSE 0 END) AS sl_to_tp1, SUM(CASE WHEN tp1_hit=1 THEN 1 ELSE 0 END) AS tp1_hits_total, SUM(CASE WHEN tp2_hit=1 THEN 1 ELSE 0 END) AS tp2_hits_total, SUM(CASE WHEN tp1_hit=1 AND sl_moved_to_be_at IS NULL THEN 1 ELSE 0 END) AS tp1_orphans_pre_pr114, SUM(CASE WHEN tp2_hit=1 AND sl_moved_to_tp1_at IS NULL THEN 1 ELSE 0 END) AS tp2_orphans_pre_pr114 FROM trades;"
echo ""

echo "## 12b.2 PR#114 — trades órfãos (TP hit pré-PR114, sem timestamp)"
sql "$JOURNAL_DB" "SELECT symbol, status, substr(timestamp,1,19) AS opened, tp1_hit, tp2_hit, CASE WHEN sl_moved_to_be_at IS NULL THEN 'órfão BE' ELSE 'OK' END AS be_status, CASE WHEN tp2_hit=1 AND sl_moved_to_tp1_at IS NULL THEN 'órfão TP1' ELSE 'OK' END AS tp1_status FROM trades WHERE (tp1_hit=1 AND sl_moved_to_be_at IS NULL) OR (tp2_hit=1 AND sl_moved_to_tp1_at IS NULL) ORDER BY timestamp DESC LIMIT 15;"
echo ""

echo "## 12b.3 PR#114 — trailing acionado pós-merge (validação positiva)"
sql "$JOURNAL_DB" "SELECT symbol, status, substr(sl_moved_to_be_at,1,19) AS to_be_ts, substr(sl_moved_to_tp1_at,1,19) AS to_tp1_ts, ROUND(current_sl,8) AS current_sl, ROUND(entry_price,8) AS entry, ROUND(tp1_price,8) AS tp1 FROM trades WHERE sl_moved_to_be_at IS NOT NULL AND sl_moved_to_be_at >= '$PR114_CUTOFF' ORDER BY sl_moved_to_be_at DESC LIMIT 15;"
echo ""

echo "## 13. Trades em andamento (OPEN / PARTIAL)"
sql "$JOURNAL_DB" "SELECT symbol, setup_name, direction AS dir, status, ROUND(entry_price,8) AS entry, ROUND(current_sl,8) AS curr_sl, tp1_hit, tp2_hit, tp3_hit, ROUND(position_remaining,2) AS pos_rem, ROUND(max_runup,2) AS max_up, ROUND(max_drawdown,2) AS max_dn, substr(timestamp,1,16) AS opened FROM trades WHERE status IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') AND timestamp >= '$CUTOFF' ORDER BY timestamp DESC;"
echo ""

echo "## 13b.1 Saúde derivada dos trades em aberto (idade, % do SL/TP percorrido, trailing)"
# Nota: julianday(datetime('now','-3 hours')) compensa offset BRT (-03:00) dos timestamps em trades.
sql "$JOURNAL_DB" "SELECT symbol, setup_name AS setup, direction AS dir, status, ROUND((julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24, 1) AS idade_h, ROUND(timeout_hours - (julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24, 1) AS resta_h, ROUND(ABS(entry_price-sl_price)/entry_price*100, 2) AS sl_pct, ROUND(ABS(tp1_price-entry_price)/entry_price*100, 2) AS tp1_pct, ROUND(max_runup, 2) AS runup, ROUND(max_drawdown, 2) AS dd, CASE WHEN ABS(entry_price-sl_price) > 0 THEN ROUND(100.0 * max_drawdown / (ABS(entry_price-sl_price)/entry_price*100), 0) ELSE NULL END AS dd_vs_sl_pct, CASE WHEN ABS(tp1_price-entry_price) > 0 THEN ROUND(100.0 * max_runup / (ABS(tp1_price-entry_price)/entry_price*100), 0) ELSE NULL END AS runup_vs_tp1_pct, CASE WHEN tp2_hit=1 THEN 'SL→TP1' WHEN tp1_hit=1 THEN 'SL→BE' ELSE 'inicial' END AS trailing, ROUND(position_remaining, 2) AS pos_rem FROM trades WHERE status IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') ORDER BY idade_h DESC;"
echo ""

echo "## 13b.2 Trades em aberto em risco (drawdown alto vs SL ou perto do timeout)"
sql "$JOURNAL_DB" "SELECT symbol, setup_name AS setup, dir, status, idade_h, resta_h, dd_vs_sl_pct AS dd_pct_sl, alerta FROM (SELECT symbol, setup_name, direction AS dir, status, ROUND((julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24, 1) AS idade_h, ROUND(timeout_hours - (julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24, 1) AS resta_h, CASE WHEN ABS(entry_price-sl_price) > 0 THEN ROUND(100.0 * max_drawdown / (ABS(entry_price-sl_price)/entry_price*100), 0) ELSE 0 END AS dd_vs_sl_pct, CASE WHEN (julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24 > timeout_hours - 6 THEN '⚠ <6h timeout' WHEN max_drawdown >= 0.7 * ABS(entry_price-sl_price)/entry_price*100 AND tp1_hit = 0 THEN '⚠ DD ≥70% SL' WHEN max_drawdown >= 0.5 * ABS(entry_price-sl_price)/entry_price*100 AND tp1_hit = 0 THEN '⚡ DD ≥50% SL' ELSE '' END AS alerta FROM trades WHERE status IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2')) WHERE alerta != '' ORDER BY idade_h DESC;"
echo ""

echo "## 13b.3 Resumo agregado dos trades em aberto"
sql "$JOURNAL_DB" "SELECT COUNT(*) AS total_open, SUM(CASE WHEN tp1_hit=1 AND tp2_hit=0 THEN 1 ELSE 0 END) AS pos_tp1_only, SUM(CASE WHEN tp2_hit=1 THEN 1 ELSE 0 END) AS pos_tp2, SUM(CASE WHEN sl_moved_to_be_at IS NOT NULL THEN 1 ELSE 0 END) AS trailing_be, SUM(CASE WHEN sl_moved_to_tp1_at IS NOT NULL THEN 1 ELSE 0 END) AS trailing_tp1, ROUND(AVG((julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24), 1) AS avg_idade_h, ROUND(MAX((julianday(datetime('now','-3 hours')) - julianday(substr(timestamp,1,19)))*24), 1) AS max_idade_h, ROUND(AVG(max_runup), 2) AS avg_runup, ROUND(AVG(max_drawdown), 2) AS avg_dd FROM trades WHERE status IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2');"
echo ""

echo "## 14. Trades fechados — resultados"
sql "$JOURNAL_DB" "SELECT symbol, setup_name, direction AS dir, status, ROUND(pnl_pct,2) AS pnl_pct, ROUND(max_runup,2) AS max_up, ROUND(max_drawdown,2) AS max_dn, substr(timestamp,1,16) AS opened, substr(exit_time,1,16) AS closed FROM trades WHERE status NOT IN ('OPEN','PARTIAL_TP1','PARTIAL_TP2') AND timestamp >= '$CUTOFF' ORDER BY exit_time DESC;"
echo ""

echo "## 15. SKIPs por motivo (token_evaluations)"
sql "$SCAN_DB" "SELECT skip_reason AS motivo, COUNT(*) AS n FROM token_evaluations WHERE action='SKIP' AND ts >= '$CUTOFF' AND skip_reason IS NOT NULL GROUP BY skip_reason ORDER BY n DESC;"
echo ""

echo "## 15b.1 Distribuição protection_filters em SKIPs (chaves disparadas, últimas 24h)"
sql "$SCAN_DB" "SELECT 'already_open' AS filtro, SUM(CASE WHEN json_extract(protection_filters,'\$.already_open')=1 THEN 1 ELSE 0 END) AS bloqueados, SUM(CASE WHEN json_extract(protection_filters,'\$.already_open')=0 THEN 1 ELSE 0 END) AS passaram FROM token_evaluations WHERE action='SKIP' AND ts >= datetime('now','-24 hours') UNION ALL SELECT 'volatility_ok', SUM(CASE WHEN json_extract(protection_filters,'\$.volatility_ok')=0 THEN 1 ELSE 0 END), SUM(CASE WHEN json_extract(protection_filters,'\$.volatility_ok')=1 THEN 1 ELSE 0 END) FROM token_evaluations WHERE action='SKIP' AND ts >= datetime('now','-24 hours') UNION ALL SELECT 'btc_stable', SUM(CASE WHEN json_extract(protection_filters,'\$.btc_stable')=0 THEN 1 ELSE 0 END), SUM(CASE WHEN json_extract(protection_filters,'\$.btc_stable')=1 THEN 1 ELSE 0 END) FROM token_evaluations WHERE action='SKIP' AND ts >= datetime('now','-24 hours') UNION ALL SELECT 'confidence_threshold', SUM(CASE WHEN json_extract(protection_filters,'\$.confidence_threshold')=0 THEN 1 ELSE 0 END), SUM(CASE WHEN json_extract(protection_filters,'\$.confidence_threshold')=1 THEN 1 ELSE 0 END) FROM token_evaluations WHERE action='SKIP' AND ts >= datetime('now','-24 hours');"
echo ""

echo "## 15b.2 CALLs que passaram por todos os filtros (sanidade)"
sql "$SCAN_DB" "SELECT json_extract(protection_filters,'\$.already_open') AS open_chk, json_extract(protection_filters,'\$.volatility_ok') AS vol_chk, json_extract(protection_filters,'\$.btc_stable') AS btc_chk, json_extract(protection_filters,'\$.confidence_threshold') AS conf_chk, COUNT(*) AS n FROM token_evaluations WHERE action='CALL' AND ts >= '$CUTOFF' GROUP BY open_chk, vol_chk, btc_chk, conf_chk ORDER BY n DESC;"
echo ""

echo "## 16. Near-misses recentes (último ranking de quem quase passou)"
sql "$SCAN_DB" "SELECT substr(ts,1,19) AS ts, symbol, closest_setup_name AS setup, closest_direction AS dir, closest_regime AS regime, closest_confidence AS conf FROM near_misses WHERE ts >= '$CUTOFF' ORDER BY ts DESC LIMIT 15;"
echo ""

echo "## 16b.1 PR#115 — saúde do near_misses.conditions (% preenchido pré x pós merge)"
sql "$SCAN_DB" "SELECT CASE WHEN ts < '$PR115_CUTOFF' THEN 'pré-PR115' ELSE 'pós-PR115' END AS janela, CASE WHEN conditions IS NULL OR conditions='' OR conditions='[]' THEN 'vazio' ELSE 'preenchido' END AS estado, COUNT(*) AS n, MIN(ts) AS primeiro, MAX(ts) AS ultimo FROM near_misses GROUP BY janela, estado ORDER BY janela, estado;"
echo ""

echo "## 16b.2 PR#115 — top setups com mais near_misses pós-merge (calibração)"
sql "$SCAN_DB" "SELECT closest_setup_name AS setup, closest_direction AS dir, closest_regime AS regime, COUNT(*) AS n FROM near_misses WHERE ts >= '$PR115_CUTOFF' GROUP BY setup, dir, regime ORDER BY n DESC LIMIT 15;"
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
