#!/usr/bin/env python3
"""
health_report.py — Coleta de Dados de Saúde do Setup Atirador
==============================================================
Coleta e formata dados das últimas 48 rodadas (~24h) para análise
de saúde, consistência e calibração.

NÃO faz análise — apenas coleta e formata os dados para revisão externa.

Uso:
    python3 health_report.py              # últimas 24h (padrão)
    python3 health_report.py --hours 48   # últimas 48h
    python3 health_report.py --out report.txt  # saída para arquivo
"""

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Configuração ──────────────────────────────────────────────────────────────
BRT         = timezone(timedelta(hours=-3))
BASE_DIR    = Path.home() / "Setup_Atirador"
LOGS_DIR    = BASE_DIR / "logs"
JOURNAL_DIR = BASE_DIR / "journal"
STATES_DIR  = BASE_DIR / "states"

DB_PATH      = LOGS_DIR / "scan_log.db"
JSONL_PATH   = LOGS_DIR / "scan_log.jsonl"
JOURNAL_PATH = JOURNAL_DIR / "atirador_journal.db"
STATE_PATH   = STATES_DIR / "atirador_state.json"
WATCHDOG     = Path("/tmp/atirador_last_run.json")

SEP = "=" * 72


def _banner(title: str) -> str:
    return f"\n{SEP}\n=== {title}\n{SEP}"


def _connect(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_as_text(rows, headers=None) -> str:
    if not rows:
        return "(nenhum resultado)"
    rows = [dict(r) for r in rows]
    if headers is None:
        headers = list(rows[0].keys())
    col_widths = {h: max(len(str(h)), max(len(str(r.get(h, ""))) for r in rows))
                  for h in headers}
    fmt = "  ".join(f"{{:<{col_widths[h]}}}" for h in headers)
    sep = "  ".join("-" * col_widths[h] for h in headers)
    lines = [fmt.format(*headers), sep]
    for row in rows:
        lines.append(fmt.format(*[str(row.get(h, "")) for h in headers]))
    return "\n".join(lines)


def _since_iso(hours: int) -> str:
    return (datetime.now(BRT) - timedelta(hours=hours)).isoformat()


# ── SEÇÃO 1 — Integridade das Rodadas ────────────────────────────────────────

def section1(conn: sqlite3.Connection, hours: int) -> str:
    since = _since_iso(hours)
    out   = [_banner(f"SEÇÃO 1 — INTEGRIDADE DAS RODADAS (últimas {hours}h)")]

    # 1a — Total, primeira e última
    out.append("\n--- 1a) Total de rodadas, primeira e última ---")
    row = conn.execute(
        """SELECT COUNT(*) as total,
                  MIN(ts) as primeira,
                  MAX(ts) as ultima
           FROM rounds WHERE ts >= ?""",
        (since,)
    ).fetchone()
    if row:
        out.append(f"  total    : {row['total']}")
        out.append(f"  primeira : {row['primeira']}")
        out.append(f"  ultima   : {row['ultima']}")
    else:
        out.append("  (sem rodadas no período)")

    # 1b — Gaps > 40min
    out.append("\n--- 1b) Gaps entre rodadas > 40min ---")
    gaps = conn.execute(
        """SELECT r1.ts as rodada,
                  r2.ts as proxima,
                  round((julianday(r2.ts) - julianday(r1.ts)) * 1440, 1) as gap_min
           FROM rounds r1
           JOIN rounds r2 ON r2.round_id = (
               SELECT round_id FROM rounds
               WHERE ts > r1.ts
               ORDER BY ts ASC LIMIT 1
           )
           WHERE r1.ts >= ?
             AND round((julianday(r2.ts) - julianday(r1.ts)) * 1440, 1) > 40
           ORDER BY r1.ts""",
        (since,)
    ).fetchall()
    if gaps:
        out.append(_rows_as_text(gaps, ["rodada", "proxima", "gap_min"]))
    else:
        out.append("  (nenhum gap > 40min — ✓ cadência OK)")

    # 1c — Exchange por rodada
    out.append("\n--- 1c) Exchange primária usada (contagem) ---")
    rows = conn.execute(
        """SELECT exchange as exchange_used, COUNT(*) as total
           FROM rounds WHERE ts >= ?
           GROUP BY exchange
           ORDER BY total DESC""",
        (since,)
    ).fetchall()
    out.append(_rows_as_text(rows) if rows else "  (sem dados)")

    return "\n".join(out)


# ── SEÇÃO 2 — Funil de Tokens ─────────────────────────────────────────────────

def section2(conn: sqlite3.Connection, hours: int) -> str:
    since = _since_iso(hours)
    out   = [_banner(f"SEÇÃO 2 — FUNIL DE TOKENS POR RODADA (últimas {hours}h)")]

    # Calls/Quases emitidos por rodada a partir de round_events
    rows = conn.execute(
        """SELECT r.round_id,
                  r.ts      as timestamp,
                  r.exchange as exchange_used,
                  r.fgi     as fgi_value,
                  r.btc_4h  as btc_direction,
                  r.univ_count   as universe_size,
                  r.gate_4h      as gate4h_passed,
                  r.gate_1h      as gate1h_passed,
                  r.scored_15m,
                  r.thr_long,
                  r.thr_short,
                  r.exec_secs,
                  r.candle_locked,
                  COALESCE(ev.calls,  0) as calls_emitted,
                  COALESCE(ev.quases, 0) as quase_emitted
           FROM rounds r
           LEFT JOIN (
               SELECT round_id,
                      SUM(CASE WHEN type='CALL'  THEN 1 ELSE 0 END) as calls,
                      SUM(CASE WHEN type='QUASE' THEN 1 ELSE 0 END) as quases
               FROM round_events
               GROUP BY round_id
           ) ev ON ev.round_id = r.round_id
           WHERE r.ts >= ?
           ORDER BY r.ts""",
        (since,)
    ).fetchall()

    headers = ["timestamp", "exchange_used", "fgi_value", "btc_direction",
               "universe_size", "gate4h_passed", "gate1h_passed", "scored_15m",
               "thr_long", "thr_short", "exec_secs", "candle_locked",
               "calls_emitted", "quase_emitted"]
    out.append(_rows_as_text(rows, headers) if rows else "  (sem rodadas no período)")
    return "\n".join(out)


# ── SEÇÃO 3 — Tokens no Scoring 15m ──────────────────────────────────────────

def section3(conn: sqlite3.Connection, hours: int) -> str:
    since = _since_iso(hours)
    out   = [_banner(f"SEÇÃO 3 — TOKENS NO SCORING 15m (últimas {hours}h)")]

    rows = conn.execute(
        """SELECT r.ts   as timestamp,
                  t.symbol,
                  t.direction,
                  t.score_total as total_score,
                  t.threshold,
                  t.gap,
                  t.status,
                  t.p1,  t.p2,  t.p3,  t.p4,  t.p5,
                  t.p6,  t.p7,  t.p8,  t.p9,  t.p1h,
                  t.kline_venue,
                  t.tv_venue,
                  t.venue_quality
           FROM token_scores t
           JOIN rounds r ON r.round_id = t.round_id
           WHERE r.ts >= ?
           ORDER BY r.ts, t.score_total DESC""",
        (since,)
    ).fetchall()

    out.append(f"  Total de entradas: {len(rows)}")
    out.append("")
    headers = ["timestamp", "symbol", "direction", "total_score", "threshold",
               "gap", "status",
               "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p1h",
               "kline_venue", "tv_venue", "venue_quality"]
    out.append(_rows_as_text(rows, headers) if rows else "  (nenhum token no período)")
    return "\n".join(out)


# ── SEÇÃO 4 — Calls e Quases ──────────────────────────────────────────────────

def section4(conn: sqlite3.Connection, hours: int) -> str:
    since = _since_iso(hours)
    out   = [_banner(f"SEÇÃO 4 — CALLS E QUASES EMITIDOS (últimas {hours}h)")]

    rows = conn.execute(
        """SELECT r.ts   as timestamp,
                  e.type as event_type,
                  e.symbol,
                  e.direction,
                  e.score,
                  e.gap,
                  ts2.threshold as threshold_at_time
           FROM round_events e
           JOIN rounds r ON r.round_id = e.round_id
           LEFT JOIN token_scores ts2
                  ON ts2.round_id = e.round_id
                 AND ts2.symbol    = e.symbol
                 AND ts2.direction = e.direction
           WHERE r.ts >= ?
           ORDER BY r.ts""",
        (since,)
    ).fetchall()

    out.append(f"  Total de eventos: {len(rows)}")
    out.append("")
    headers = ["timestamp", "event_type", "symbol", "direction",
               "score", "gap", "threshold_at_time"]
    out.append(_rows_as_text(rows, headers) if rows else "  (nenhum evento no período)")
    return "\n".join(out)


# ── SEÇÃO 5 — Journal ─────────────────────────────────────────────────────────

def section5(jconn: sqlite3.Connection | None) -> str:
    out = [_banner("SEÇÃO 5 — JOURNAL — STATUS DOS TRADES")]

    if jconn is None:
        out.append("  ⚠ atirador_journal.db não encontrado")
        return "\n".join(out)

    # 5a — Schema
    out.append("\n--- 5a) Tabelas no journal ---")
    tables = jconn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    for t in tables:
        out.append(f"  · {t['name']}")
        cols = jconn.execute(f"PRAGMA table_info({t['name']})").fetchall()
        for c in cols:
            out.append(f"      {c['cid']:>2}  {c['name']:<22} {c['type']}")

    # 5b — Registros abertos ou fechados nas últimas 24h
    out.append("\n--- 5b) Trades (abertos ou fechados nas últimas 24h) ---")
    since = _since_iso(24)
    rows = jconn.execute(
        """SELECT id, timestamp, symbol, direction, type, is_hypothetical,
                  score, entry_price, sl_price, tp1_price, tp2_price, tp3_price,
                  status, exit_price, exit_time, pnl_pct, max_runup, max_drawdown,
                  kline_venue, tv_venue, venue_quality
           FROM trades
           WHERE timestamp >= ? OR (status = 'OPEN')
           ORDER BY timestamp""",
        (since,)
    ).fetchall()
    headers = ["id", "timestamp", "symbol", "direction", "type", "is_hypothetical",
               "score", "entry_price", "sl_price", "tp1_price", "tp2_price", "tp3_price",
               "status", "exit_price", "exit_time", "pnl_pct",
               "max_runup", "max_drawdown", "kline_venue", "tv_venue", "venue_quality"]
    out.append(_rows_as_text(rows, headers) if rows else "  (nenhum trade no período)")

    # 5c — Distribuição de status
    out.append("\n--- 5c) Distribuição de status (todos os tempos) ---")
    rows = jconn.execute(
        """SELECT status, is_hypothetical, COUNT(*) as total
           FROM trades
           GROUP BY status, is_hypothetical
           ORDER BY is_hypothetical, status"""
    ).fetchall()
    out.append(_rows_as_text(rows) if rows else "  (nenhum trade)")

    return "\n".join(out)


# ── SEÇÃO 6 — Consistência JSONL vs SQLite ────────────────────────────────────

def section6(conn: sqlite3.Connection, hours: int) -> str:
    out = [_banner("SEÇÃO 6 — CONSISTÊNCIA JSONL vs SQLite")]

    # Contar linhas JSONL
    out.append("\n--- 6a) Contagem de linhas no JSONL ---")
    if JSONL_PATH.exists():
        try:
            result = subprocess.run(
                ["wc", "-l", str(JSONL_PATH)],
                capture_output=True, text=True, timeout=10
            )
            out.append(f"  {result.stdout.strip()}")
        except Exception as e:
            out.append(f"  (erro ao contar: {e})")
    else:
        out.append(f"  ⚠ JSONL não encontrado: {JSONL_PATH}")

    # Rodadas no SQLite
    total_db = conn.execute("SELECT COUNT(*) as n FROM rounds").fetchone()["n"]
    out.append(f"  Rodadas no SQLite (total): {total_db}")

    # Últimas 3 entradas JSONL
    out.append("\n--- 6b) Últimas 3 entradas do JSONL ---")
    if JSONL_PATH.exists():
        try:
            result = subprocess.run(
                ["tail", "-3", str(JSONL_PATH)],
                capture_output=True, text=True, timeout=10
            )
            lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
            for line in lines:
                try:
                    d = json.loads(line)
                    # Resumo compacto sem os tokens completos para não explodir o output
                    summary = {
                        "ts"      : d.get("ts"),
                        "round_id": d.get("round_id"),
                        "version" : d.get("version"),
                        "meta"    : d.get("meta", {}),
                        "pipeline": d.get("pipeline", {}),
                        "events"  : d.get("events", []),
                        "n_tokens": len(d.get("tokens", [])),
                    }
                    out.append(json.dumps(summary, indent=2, ensure_ascii=False))
                    out.append("---")
                except json.JSONDecodeError:
                    out.append(f"  (linha inválida: {line[:80]})")
        except Exception as e:
            out.append(f"  (erro ao ler JSONL: {e})")
    else:
        out.append(f"  ⚠ JSONL não encontrado")

    return "\n".join(out)


# ── SEÇÃO 7 — Saúde do Sistema ────────────────────────────────────────────────

def section7() -> str:
    out = [_banner("SEÇÃO 7 — SAÚDE DO SISTEMA")]

    # 7a — Erros nos logs
    out.append("\n--- 7a) Últimas 20 linhas com error/exception/traceback/failed nos logs ---")
    log_pattern = str(LOGS_DIR / "*.log")
    try:
        grep = subprocess.run(
            f'grep -i "error\\|exception\\|traceback\\|failed" {log_pattern} 2>/dev/null | tail -20',
            shell=True, capture_output=True, text=True, timeout=15
        )
        if grep.stdout.strip():
            out.append(grep.stdout.rstrip())
        else:
            out.append("  (nenhuma ocorrência encontrada)")
    except Exception as e:
        out.append(f"  (erro ao executar grep: {e})")

    # 7b — Tamanhos dos arquivos críticos
    out.append("\n--- 7b) Tamanho dos arquivos críticos ---")
    files = [
        JSONL_PATH,
        DB_PATH,
        JOURNAL_PATH,
        STATE_PATH,
    ]
    for f in files:
        if f.exists():
            size = f.stat().st_size
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=BRT).isoformat()
            if size < 1024:
                sz_str = f"{size} B"
            elif size < 1024 * 1024:
                sz_str = f"{size/1024:.1f} KB"
            else:
                sz_str = f"{size/(1024*1024):.1f} MB"
            out.append(f"  {sz_str:>10}  {mtime}  {f}")
        else:
            out.append(f"  {'N/A':>10}  —                         ⚠ NÃO ENCONTRADO: {f}")

    # 7c — Conteúdo do state JSON
    out.append("\n--- 7c) Conteúdo do atirador_state.json ---")
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH, encoding="utf-8") as fh:
                state = json.load(fh)
            # Resumo: chaves de topo + tamanho de estruturas internas
            summary = {}
            for k, v in state.items():
                if isinstance(v, dict):
                    summary[k] = f"<dict com {len(v)} chaves>"
                elif isinstance(v, list):
                    summary[k] = f"<list com {len(v)} itens>"
                else:
                    summary[k] = v
            out.append(json.dumps(summary, indent=2, ensure_ascii=False))

            # Se tiver score_history, mostrar distribuição
            if "score_history" in state:
                sh = state["score_history"]
                out.append(f"\n  score_history: {len(sh)} símbolos")
                for sym, hist in list(sh.items())[:10]:
                    out.append(f"    {sym}: {hist}")
                if len(sh) > 10:
                    out.append(f"    ... ({len(sh) - 10} mais)")
        except Exception as e:
            out.append(f"  (erro ao ler state: {e})")
    else:
        out.append(f"  ⚠ State não encontrado: {STATE_PATH}")

    # 7d — Watchdog
    out.append("\n--- 7d) Watchdog (/tmp/atirador_last_run.json) ---")
    if WATCHDOG.exists():
        try:
            with open(WATCHDOG, encoding="utf-8") as fh:
                out.append(json.dumps(json.load(fh), indent=2, ensure_ascii=False))
        except Exception as e:
            out.append(f"  (erro ao ler watchdog: {e})")
    else:
        out.append("  watchdog file not found")

    return "\n".join(out)


# ── SEÇÃO 8 — Frequência de Ativação dos Pilares ─────────────────────────────

def section8(conn: sqlite3.Connection, hours: int) -> str:
    since = _since_iso(hours)
    out   = [_banner(f"SEÇÃO 8 — PILARES — FREQUÊNCIA DE ATIVAÇÃO (últimas {hours}h)")]

    pillar_cols = ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p1h"]
    pillar_names = {
        "p1" : "P1  Bollinger 15m",
        "p2" : "P2  Candles 15m",
        "p3" : "P3  Funding Rate",
        "p4" : "P4  Liquidez 4H",
        "p5" : "P5  Figuras 4H",
        "p6" : "P6  CHOCH/BOS 4H",
        "p7" : "P7  Pump/Dump (veto)",
        "p8" : "P8  Volume 15m",
        "p9" : "P9  OI Trend",
        "p1h": "P1H Res/OB 1H",
    }

    # Busca todos os tokens no período
    rows = conn.execute(
        f"""SELECT t.direction, {', '.join('t.' + p for p in pillar_cols)}, t.score_total
            FROM token_scores t
            JOIN rounds r ON r.round_id = t.round_id
            WHERE r.ts >= ?""",
        (since,)
    ).fetchall()

    if not rows:
        out.append("  (sem tokens no período)")
        return "\n".join(out)

    # Agregação por direção
    stats: dict = {
        "LONG" : {p: {"n_gt0": 0, "n_total": 0, "sum_score": 0} for p in pillar_cols},
        "SHORT": {p: {"n_gt0": 0, "n_total": 0, "sum_score": 0} for p in pillar_cols},
        "ALL"  : {p: {"n_gt0": 0, "n_total": 0, "sum_score": 0} for p in pillar_cols},
    }

    for row in rows:
        d = dict(row)
        direction = d.get("direction", "ALL")
        for p in pillar_cols:
            val = d.get(p)
            if val is None:
                continue
            val = int(val)
            for grp in [direction, "ALL"]:
                if grp not in stats:
                    stats[grp] = {pp: {"n_gt0": 0, "n_total": 0, "sum_score": 0}
                                  for pp in pillar_cols}
                stats[grp][p]["n_total"]  += 1
                stats[grp][p]["sum_score"] += val
                if val > 0:
                    stats[grp][p]["n_gt0"] += 1

    out.append(f"\n  Total de tokens analisados: {len(rows)}")
    out.append(f"  LONG : {sum(1 for r in rows if dict(r).get('direction') == 'LONG')}")
    out.append(f"  SHORT: {sum(1 for r in rows if dict(r).get('direction') == 'SHORT')}")

    for grp in ["ALL", "LONG", "SHORT"]:
        out.append(f"\n--- 8) Pilares — {grp} ---")
        header = f"  {'Pilar':<22}  {'Ativações':>10}  {'Total':>7}  {'Taxa%':>7}  {'Avg score':>10}"
        out.append(header)
        out.append("  " + "-" * 60)
        for p in pillar_cols:
            s = stats.get(grp, {}).get(p, {})
            n_total   = s.get("n_total", 0)
            n_gt0     = s.get("n_gt0", 0)
            sum_score = s.get("sum_score", 0)
            taxa      = round(n_gt0 / n_total * 100, 1) if n_total > 0 else 0.0
            avg       = round(sum_score / n_total, 2)   if n_total > 0 else 0.0
            name      = pillar_names.get(p, p)
            out.append(f"  {name:<22}  {n_gt0:>10}  {n_total:>7}  {taxa:>6.1f}%  {avg:>10.2f}")

    return "\n".join(out)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Setup Atirador — Health Report")
    parser.add_argument("--hours", type=int, default=24,
                        help="Janela de tempo em horas (padrão: 24)")
    parser.add_argument("--out", type=str, default=None,
                        help="Arquivo de saída (padrão: stdout)")
    args = parser.parse_args()

    now_str = datetime.now(BRT).strftime("%Y-%m-%d %H:%M BRT")
    lines   = []

    lines.append(SEP)
    lines.append(f"  SETUP ATIRADOR — HEALTH REPORT")
    lines.append(f"  Gerado em : {now_str}")
    lines.append(f"  Janela    : últimas {args.hours}h")
    lines.append(f"  scan_log.db      : {DB_PATH}")
    lines.append(f"  atirador_journal : {JOURNAL_PATH}")
    lines.append(f"  scan_log.jsonl   : {JSONL_PATH}")
    lines.append(SEP)

    # Conectar bancos
    conn  = _connect(DB_PATH)
    jconn = _connect(JOURNAL_PATH)

    if conn is None:
        lines.append(f"\n⚠  scan_log.db não encontrado em {DB_PATH}")
        lines.append("   O RoundLogger não foi ativado ainda, ou a VM nunca rodou o scan.")
        lines.append("   Execute pelo menos uma rodada do script para gerar os dados.")
    else:
        # Inspecionar schema real antes de tudo
        tables = {t["name"] for t in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        lines.append(f"\n  Tabelas em scan_log.db: {sorted(tables)}")

        lines.append(section1(conn, args.hours))
        lines.append(section2(conn, args.hours))
        lines.append(section3(conn, args.hours))
        lines.append(section4(conn, args.hours))
        lines.append(section8(conn, args.hours))

    lines.append(section5(jconn))
    if jconn:
        jconn.close()

    if conn is not None:
        lines.append(section6(conn, args.hours))
        conn.close()
    else:
        lines.append(
            f"\n{SEP}\n=== SEÇÃO 6 — CONSISTÊNCIA JSONL vs SQLite\n{SEP}\n"
            "  ⚠ scan_log.db não disponível"
        )
    lines.append(section7())

    output = "\n".join(lines)

    if args.out:
        Path(args.out).write_text(output, encoding="utf-8")
        print(f"[health_report] Relatório salvo em: {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
