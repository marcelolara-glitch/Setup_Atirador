#!/usr/bin/env python3
"""
health_report.py — Relatório de saúde do Setup Atirador v8
Uso: python3 health_report.py [--horas N] [--out arquivo.txt]
Default: últimas 24h, saída em stdout

Seções:
  1 — Integridade das rodadas (gaps, cadência)
  2 — Contexto de mercado (FGI, BTC bias)
  3 — Funil v8 (universo → gate 4H → score 15m)
  4 — Checks A/B/C (frequência de ativação via pillars_json)
  5 — Zonas de decisão (distribuição e taxa de conversão)
  6 — Top tokens (aparições e win rate)
  7 — Eventos emitidos (CALLs e QUASEs do período)
  8 — Saúde do sistema (erros de log, tamanho de arquivos, watchdog)
"""
import sqlite3, sys, os, argparse, glob, json
from datetime import datetime, timezone, timedelta

BRT        = timezone(timedelta(hours=-3))
DB_SCAN    = "/home/ubuntu/Setup_Atirador/logs/scan_log.db"
DB_JOURNAL = "/home/ubuntu/Setup_Atirador/journal/atirador_journal.db"
DIR_LOGS   = "/home/ubuntu/Setup_Atirador/logs"
STATE_FILE = "/home/ubuntu/Setup_Atirador/states/atirador_state.json"
WATCHDOG   = "/tmp/atirador_last_run.json"


def sep(titulo=""):
    line = "=" * 72
    if titulo:
        return f"\n{line}\n=== {titulo}\n{line}\n"
    return f"{line}\n"


def subsep(titulo):
    return f"\n--- {titulo} ---\n"


def _out(lines, path):
    text = "\n".join(lines)
    if path:
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(path, "w") as f:
            f.write(text)
    else:
        print(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--horas", type=int, default=24)
    parser.add_argument("--out",   type=str, default=None)
    args = parser.parse_args()

    lines = []
    w     = lines.append
    now   = datetime.now(BRT)
    desde = (now - timedelta(hours=args.horas)).isoformat()

    w(sep())
    w(f"  SETUP ATIRADOR — HEALTH REPORT v8")
    w(f"  Gerado em : {now.strftime('%Y-%m-%d %H:%M')} BRT")
    w(f"  Janela    : últimas {args.horas}h")
    w(f"  scan_log  : {DB_SCAN}")
    w(f"  journal   : {DB_JOURNAL}")
    w(sep())

    # ── Conectar bancos ───────────────────────────────────────────────────────
    if not os.path.exists(DB_SCAN):
        w(f"  ERRO: {DB_SCAN} não encontrado.")
        _out(lines, args.out)
        return

    conn_scan = sqlite3.connect(DB_SCAN)
    conn_scan.row_factory = sqlite3.Row

    conn_journal = None
    if os.path.exists(DB_JOURNAL):
        conn_journal = sqlite3.connect(DB_JOURNAL)
        conn_journal.row_factory = sqlite3.Row

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 1 — INTEGRIDADE DAS RODADAS"))
    # ══════════════════════════════════════════════════════════════════════════
    rounds = conn_scan.execute("""
        SELECT round_id, ts, univ_count, gate_4h, scored_15m,
               exec_secs, fgi, btc_4h
        FROM rounds WHERE ts >= ? ORDER BY ts
    """, (desde,)).fetchall()

    total_rounds = conn_scan.execute(
        "SELECT COUNT(*) FROM rounds"
    ).fetchone()[0]

    w(subsep("1a) Total de rodadas"))
    w(f"  Total no banco (all time) : {total_rounds}")
    w(f"  Na janela ({args.horas}h)          : {len(rounds)}")
    if rounds:
        w(f"  Primeira : {rounds[0]['ts'][:16]}")
        w(f"  Última   : {rounds[-1]['ts'][:16]}")
    else:
        w("  (sem rodadas no período)")
        conn_scan.close()
        if conn_journal:
            conn_journal.close()
        _out(lines, args.out)
        return

    w(subsep("1b) Gaps entre rodadas > 40min"))
    tss  = [datetime.fromisoformat(r["ts"]) for r in rounds]
    gaps = [
        (tss[i-1].isoformat()[:16], tss[i].isoformat()[:16],
         int((tss[i] - tss[i-1]).total_seconds() // 60))
        for i in range(1, len(tss))
        if (tss[i] - tss[i-1]).total_seconds() > 2400
    ]
    if gaps:
        for g in gaps:
            w(f"  ⚠️  {g[0]} → {g[1]} = {g[2]}min")
    else:
        w("  (nenhum gap > 40min — ✓ cadência OK)")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 2 — CONTEXTO DE MERCADO"))
    # ══════════════════════════════════════════════════════════════════════════
    fgis   = [r["fgi"] for r in rounds if r["fgi"] is not None]
    biases = {}
    for r in rounds:
        b = r["btc_4h"] or "?"
        biases[b] = biases.get(b, 0) + 1

    w(subsep("2a) Fear & Greed Index"))
    if fgis:
        w(f"  min={min(fgis)} | max={max(fgis)} | avg={sum(fgis)/len(fgis):.1f}")
    else:
        w("  (sem dados de FGI)")

    w(subsep("2b) BTC 4H (contagem de rodadas)"))
    for b, c in sorted(biases.items(), key=lambda x: -x[1]):
        w(f"  {b:<14}: {c} rodadas")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 3 — FUNIL v8"))
    # ══════════════════════════════════════════════════════════════════════════
    # gate_1h existe no schema mas = gate_4h (legado) — não exibir
    def _avg(key):
        vals = [r[key] for r in rounds if r[key] is not None]
        return sum(vals) / len(vals) if vals else 0.0

    u     = _avg("univ_count")
    g4    = _avg("gate_4h")
    s15   = _avg("scored_15m")
    execs = [r["exec_secs"] for r in rounds if r["exec_secs"]]

    w(subsep("3a) Médias por etapa"))
    w(f"  Universo              : {u:.1f}")
    w(f"  Gate 4H               : {g4:.1f}  ({g4/u*100:.0f}% do universo)" if u else "  Gate 4H               : —")
    w(f"  Score 15m             : {s15:.1f}  ({s15/u*100:.0f}% do universo)" if u else "  Score 15m             : —")
    if execs:
        w(f"  Exec: min={min(execs):.0f}s | avg={sum(execs)/len(execs):.0f}s | max={max(execs):.0f}s")

    w(subsep("3b) Detalhe por rodada (últimas 10)"))
    w(f"  {'Rodada':<18} {'Univ':>5} {'Gate4H':>7} {'15m':>5} {'Exec':>7}")
    for r in rounds[-10:]:
        w(f"  {r['ts'][:16]:<18} {(r['univ_count'] or 0):>5} "
          f"{(r['gate_4h'] or 0):>7} {(r['scored_15m'] or 0):>5} "
          f"{(r['exec_secs'] or 0):>6.0f}s")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 4 — CHECKS A/B/C"))
    # ══════════════════════════════════════════════════════════════════════════
    # Fonte: token_scores (check_a_ok, check_b_ok, check_c_det) — v8.1.x.
    # O journal registra CALLs/QUASEs mas checks A/B/C estão em token_scores.

    token_rows = conn_scan.execute("""
        SELECT direction, status,
               check_a_ok, check_b_ok, check_c_det, zona_qualidade
        FROM token_scores WHERE ts >= ?
        ORDER BY ts
    """, (desde,)).fetchall()

    parsed = []
    for row in token_rows:
        try:
            det = json.loads(row["check_c_det"] or "{}")
        except Exception:
            det = {}
        parsed.append({
            "direction": row["direction"],
            "status":    row["status"],
            "check_a":   bool(row["check_a_ok"]),
            "check_b":   bool(row["check_b_ok"]),
            "c1_bb":     det.get("c1_bb",  False),
            "c2_vol":    det.get("c2_vol", False),
            "c3_cvd":    det.get("c3_cvd", False),
            "c4_oi":     det.get("c4_oi",  False),
            "has_data":  bool(det),
        })

    valid = [x for x in parsed if x["has_data"]]
    n     = len(valid)

    w(subsep("4a) Frequência de ativação (todos os registros com check_c_det)"))
    if n:
        def _pct(lst, key):
            c = sum(1 for x in lst if x[key])
            return c, f"{c/len(lst)*100:.0f}%"

        w(f"  Total registros válidos   : {n}")
        ca, ca_p = _pct(valid, "check_a")
        cb, cb_p = _pct(valid, "check_b")
        c1, c1_p = _pct(valid, "c1_bb")
        c2, c2_p = _pct(valid, "c2_vol")
        c3, c3_p = _pct(valid, "c3_cvd")
        c4, c4_p = _pct(valid, "c4_oi")
        w(f"  Check A ativo             : {ca} ({ca_p})")
        w(f"  Check B ativo             : {cb} ({cb_p})")
        w(f"  C1 BB  ativo              : {c1} ({c1_p})")
        w(f"  C2 Vol ativo              : {c2} ({c2_p})")
        w(f"  C3 CVD ativo              : {c3} ({c3_p})")
        w(f"  C4 OI  ativo              : {c4} ({c4_p})")

        w(subsep("4b) Por direção"))
        for direcao in ["LONG", "SHORT"]:
            td = [x for x in valid if x["direction"] == direcao]
            if not td:
                continue
            w(f"  {direcao} (n={len(td)})")
            for key, label in [("check_a","A"),("check_b","B"),
                               ("c1_bb","C1"),("c2_vol","C2"),
                               ("c3_cvd","C3"),("c4_oi","C4")]:
                c = sum(1 for x in td if x[key])
                w(f"    {label}: {c} ({c/len(td)*100:.0f}%)")

        w(subsep("4c) Padrão de checks nos QUASEs (status=QUASE)"))
        qtoks = [x for x in valid if x["status"] == "QUASE"]
        if qtoks:
            patterns = {}
            for x in qtoks:
                pat = (f"A={int(x['check_a'])} B={int(x['check_b'])} "
                       f"C1={int(x['c1_bb'])} C2={int(x['c2_vol'])} "
                       f"C3={int(x['c3_cvd'])} C4={int(x['c4_oi'])}")
                patterns[pat] = patterns.get(pat, 0) + 1
            for pat, c in sorted(patterns.items(), key=lambda x: -x[1]):
                w(f"  {pat}: {c}x")
        else:
            w("  (sem QUASEs no período)")

        w(subsep("4d) Breakdown sub-checks do Check C por direção"))
        for direcao in ["LONG", "SHORT"]:
            td = [x for x in valid if x["direction"] == direcao]
            if not td:
                continue
            w(f"  {direcao}:")
            for key, label in [("c1_bb","C1 BB "),
                               ("c2_vol","C2 Vol"),
                               ("c3_cvd","C3 CVD"),
                               ("c4_oi", "C4 OI ")]:
                c = sum(1 for x in td if x[key])
                w(f"    {label}: {c}/{len(td)} ({c/len(td)*100:.0f}%)")
    else:
        w("  (sem registros com check_c_det no período)")

    # Fetch journal trades para seções 5 e 6 (venue_quality, is_hypothetical)
    jtrades = []
    if conn_journal:
        try:
            jtrades = conn_journal.execute("""
                SELECT symbol, direction, is_hypothetical, venue_quality, status
                FROM trades WHERE timestamp >= ? ORDER BY timestamp
            """, (desde,)).fetchall()
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 5 — ZONAS DE DECISÃO"))
    # ══════════════════════════════════════════════════════════════════════════
    # Nota: apenas tokens que geraram CALL ou QUASE aparecem aqui.
    # Tokens descartados antes do 15m não são registrados no journal.

    if not conn_journal:
        w("  ⚠️ atirador_journal.db não encontrado — seção indisponível")
    else:
        zonas     = {}
        zonas_c   = {}  # calls reais
        zonas_q   = {}  # quases hipotéticos
        for row in jtrades:
            z = row["venue_quality"] or "?"
            zonas[z] = zonas.get(z, 0) + 1
            if row["is_hypothetical"] == 0:
                zonas_c[z] = zonas_c.get(z, 0) + 1
            else:
                zonas_q[z] = zonas_q.get(z, 0) + 1

        w(subsep("5a) Distribuição e taxa de conversão"))
        w("  Nota: aparições = tokens que geraram CALL ou QUASE no período.")
        w(f"  {'Zona':<16} {'Total':>6} {'CALLs':>7} {'QUASEs':>8} {'Conv%':>7}")
        for z in sorted(zonas, key=lambda x: -zonas[x]):
            tz = zonas[z]
            nc = zonas_c.get(z, 0)
            nq = zonas_q.get(z, 0)
            pct = nc / tz * 100 if tz else 0
            w(f"  {z:<16} {tz:>6} {nc:>7} {nq:>8} {pct:>6.0f}%")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 6 — TOP TOKENS"))
    # ══════════════════════════════════════════════════════════════════════════
    # Nota: aparições = vezes que o token gerou CALL ou QUASE no período.
    # Tokens descartados antes do 15m não aparecem.

    if not conn_journal:
        w("  ⚠️ atirador_journal.db não encontrado — seção indisponível")
    else:
        tok_total = {}
        tok_calls = {}
        tok_quase = {}
        tok_wins  = {}
        for row in jtrades:
            s = row["symbol"] or "?"
            tok_total[s] = tok_total.get(s, 0) + 1
            if row["is_hypothetical"] == 0:
                tok_calls[s] = tok_calls.get(s, 0) + 1
                if (row["status"] or "").startswith("WIN"):
                    tok_wins[s] = tok_wins.get(s, 0) + 1
            else:
                tok_quase[s] = tok_quase.get(s, 0) + 1

        w(subsep("6a) Por aparições (top 15)"))
        w("  Nota: aparições = vezes que o token gerou CALL ou QUASE no período.")
        w(f"  {'Símbolo':<14} {'Apariç':>7} {'CALLs':>7} {'QUASEs':>8} {'WR%':>6}")
        for s in sorted(tok_total, key=lambda x: -tok_total[x])[:15]:
            nc  = tok_calls.get(s, 0)
            nq  = tok_quase.get(s, 0)
            nw  = tok_wins.get(s, 0)
            wr  = nw / nc * 100 if nc else 0
            w(f"  {s:<14} {tok_total[s]:>7} {nc:>7} {nq:>8} {wr:>5.0f}%")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 7 — EVENTOS EMITIDOS"))
    # ══════════════════════════════════════════════════════════════════════════
    events = conn_scan.execute("""
        SELECT ts, type, symbol, direction, score, gap
        FROM round_events WHERE ts >= ? ORDER BY ts DESC
    """, (desde,)).fetchall()

    calls  = [e for e in events if e["type"] == "CALL"]
    quases = [e for e in events if e["type"] == "QUASE"]

    w(subsep("7a) Resumo"))
    w(f"  CALLs  : {len(calls)}")
    w(f"  QUASEs : {len(quases)}")

    w(subsep("7b) CALLs (detalhado)"))
    if calls:
        for e in calls:
            w(f"  {e['ts'][:16]} | {e['direction']:<5} {e['symbol']:<14} C={e['score']}")
    else:
        w("  (nenhum CALL no período)")

    w(subsep("7c) QUASEs — top símbolos"))
    if quases:
        qsym = {}
        for e in quases:
            k = f"{e['direction']} {e['symbol']}"
            qsym[k] = qsym.get(k, 0) + 1
        for k, v in sorted(qsym.items(), key=lambda x: -x[1])[:10]:
            w(f"  {k}: {v}x")
    else:
        w("  (nenhum QUASE no período)")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 8 — SAÚDE DO SISTEMA"))
    # ══════════════════════════════════════════════════════════════════════════
    w(subsep("8a) Erros recentes nos logs de rodada"))
    log_files = sorted(
        glob.glob(f"{DIR_LOGS}/atirador_LOG_*.log"), reverse=True
    )[:10]
    erros = []
    for lf in log_files:
        try:
            for line in open(lf, errors="replace"):
                if any(k in line for k in ("ERROR", "Traceback", "TypeError",
                                           "Exception", "FATAL")):
                    erros.append(f"  {os.path.basename(lf)}: {line.rstrip()}")
        except Exception:
            pass
    if erros:
        for e in erros[-20:]:
            w(e)
    else:
        w("  (nenhum erro encontrado nos últimos 10 logs ✓)")

    w(subsep("8b) Tamanho dos arquivos críticos"))
    for arq in [DB_SCAN, DB_JOURNAL, STATE_FILE]:
        if os.path.exists(arq):
            sz_kb = os.path.getsize(arq) / 1024
            mtime = datetime.fromtimestamp(
                os.path.getmtime(arq), tz=BRT
            ).strftime("%Y-%m-%d %H:%M BRT")
            w(f"  {sz_kb:>8.1f} KB  {mtime}  {os.path.basename(arq)}")
        else:
            w(f"  (ausente) {arq}")

    w(subsep("8c) Watchdog (última execução)"))
    if os.path.exists(WATCHDOG):
        try:
            wd = json.load(open(WATCHDOG))
            w(f"  last_run : {wd.get('last_run', '?')}")
            w(f"  version  : {wd.get('version', '?')}")
        except Exception as ex:
            w(f"  (erro ao ler watchdog: {ex})")
    else:
        w(f"  (watchdog ausente — {WATCHDOG})")

    conn_scan.close()
    if conn_journal:
        conn_journal.close()

    w(sep())
    _out(lines, args.out)


if __name__ == "__main__":
    main()
