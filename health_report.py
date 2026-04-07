#!/usr/bin/env python3
"""
health_report.py — Relatório de saúde do Setup Atirador v7
Uso: python3 health_report.py [--horas N] [--out arquivo.txt]
Default: últimas 24h, saída em stdout
"""
import sqlite3, sys, os, argparse, glob, json
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))
DB_V7   = "/home/ubuntu/Setup_Atirador/logs/scan_log_v7.db"
DIR_LOGS = "/home/ubuntu/Setup_Atirador/logs"
WATCHDOG = "/tmp/atirador_last_run.json"


def sep(titulo=""):
    line = "=" * 72
    if titulo:
        return f"\n{line}\n=== {titulo}\n{line}\n"
    return f"{line}\n"


def subsep(titulo):
    return f"\n--- {titulo} ---\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--horas", type=int, default=24)
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    lines = []
    w = lines.append

    now = datetime.now(BRT)
    desde = (now - timedelta(hours=args.horas)).isoformat()

    w(sep())
    w(f"  SETUP ATIRADOR — HEALTH REPORT v7")
    w(f"  Gerado em : {now.strftime('%Y-%m-%d %H:%M')} BRT")
    w(f"  Janela    : últimas {args.horas}h")
    w(f"  scan_log_v7.db : {DB_V7}")
    w(sep())

    # ── conectar ──────────────────────────────────────────────────────────────
    if not os.path.exists(DB_V7):
        w(f"  ERRO: {DB_V7} não encontrado.")
        _out(lines, args.out)
        return

    conn = sqlite3.connect(DB_V7)
    conn.row_factory = sqlite3.Row

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 1 — INTEGRIDADE DAS RODADAS"))
    # ══════════════════════════════════════════════════════════════════════════

    rounds = conn.execute("""
        SELECT ts, univ_count, gate_4h_long, gate_4h_short,
               in_zona_long, in_zona_short, exec_secs, fgi, btc_bias
        FROM rounds WHERE ts >= ? ORDER BY ts
    """, (desde,)).fetchall()

    total_rounds = conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0]

    w(subsep("1a) Total de rodadas"))
    w(f"  Total no banco (all time) : {total_rounds}")
    w(f"  Na janela ({args.horas}h)          : {len(rounds)}")

    if rounds:
        w(f"  Primeira : {rounds[0]['ts'][:16]}")
        w(f"  Última   : {rounds[-1]['ts'][:16]}")
    else:
        w("  (sem rodadas no período)")
        _out(lines, args.out)
        return

    w(subsep("1b) Gaps entre rodadas > 40min"))
    tss = [datetime.fromisoformat(r['ts']) for r in rounds]
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

    fgis = [r['fgi'] for r in rounds if r['fgi'] is not None]
    biases = {}
    for r in rounds:
        b = r['btc_bias'] or '?'
        biases[b] = biases.get(b, 0) + 1

    w(subsep("2a) Fear & Greed Index"))
    if fgis:
        w(f"  min={min(fgis)} | max={max(fgis)} | avg={sum(fgis)/len(fgis):.1f}")
    else:
        w("  (sem dados de FGI)")

    w(subsep("2b) BTC 4H bias (contagem de rodadas)"))
    for b, c in sorted(biases.items(), key=lambda x: -x[1]):
        w(f"  {b:<12}: {c} rodadas")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 3 — FUNIL v7"))
    # ══════════════════════════════════════════════════════════════════════════

    avg = lambda key: sum(r[key] for r in rounds if r[key] is not None) / len(rounds)
    u  = avg('univ_count')
    l4 = avg('gate_4h_long');  s4 = avg('gate_4h_short')
    lz = avg('in_zona_long');  sz = avg('in_zona_short')
    execs = [r['exec_secs'] for r in rounds if r['exec_secs']]

    w(subsep("3a) Médias por etapa"))
    w(f"  Universo              : {u:.1f}")
    w(f"  Gate 4H LONG          : {l4:.1f}  ({l4/u*100:.0f}% do universo)")
    w(f"  Gate 4H SHORT         : {s4:.1f}  ({s4/u*100:.0f}% do universo)")
    w(f"  Em Zona LONG          : {lz:.1f}  ({lz/u*100:.0f}% do universo)")
    w(f"  Em Zona SHORT         : {sz:.1f}  ({sz/u*100:.0f}% do universo)")
    if execs:
        w(f"  Exec: min={min(execs):.0f}s | avg={sum(execs)/len(execs):.0f}s | max={max(execs):.0f}s")

    w(subsep("3b) Detalhe por rodada (últimas 10)"))
    w(f"  {'Rodada':<18} {'Univ':>5} {'4H L/S':>8} {'Zona L/S':>10} {'Exec':>7}")
    for r in rounds[-10:]:
        w(f"  {r['ts'][:16]:<18} {r['univ_count']:>5} "
          f"{r['gate_4h_long']:>3}/{r['gate_4h_short']:<3} "
          f"  {r['in_zona_long']:>3}/{r['in_zona_short']:<4} "
          f"  {r['exec_secs']:>6.0f}s")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 4 — CHECK A / B / C"))
    # ══════════════════════════════════════════════════════════════════════════

    tokens = conn.execute("""
        SELECT symbol, direction, zona_qualidade,
               check_a, check_b, check_c_total, status
        FROM token_scores WHERE ts >= ?
    """, (desde,)).fetchall()

    total_tok = len(tokens)

    w(subsep("4a) Frequência de ativação (todos os tokens em zona)"))
    if total_tok:
        a1 = sum(1 for t in tokens if t['check_a'] == 1)
        b1 = sum(1 for t in tokens if t['check_b'] == 1)
        c1 = sum(1 for t in tokens if t['check_c_total'] and t['check_c_total'] > 0)
        abc = sum(1 for t in tokens if t['check_a'] == 1 and t['check_b'] == 1
                  and t['check_c_total'] and t['check_c_total'] > 0)
        w(f"  Total de tokens avaliados : {total_tok}")
        w(f"  Check A ativo             : {a1} ({a1/total_tok*100:.0f}%)")
        w(f"  Check B ativo             : {b1} ({b1/total_tok*100:.0f}%)")
        w(f"  Check C ativo             : {c1} ({c1/total_tok*100:.0f}%)")
        w(f"  A + B + C todos ativos    : {abc} ({abc/total_tok*100:.1f}%)")

        w(subsep("4b) Por direção"))
        for direcao in ['LONG', 'SHORT']:
            td = [t for t in tokens if t['direction'] == direcao]
            if not td:
                continue
            a = sum(1 for t in td if t['check_a'] == 1)
            b = sum(1 for t in td if t['check_b'] == 1)
            c = sum(1 for t in td if t['check_c_total'] and t['check_c_total'] > 0)
            w(f"  {direcao}: n={len(td)} | "
              f"A={a}({a/len(td)*100:.0f}%) "
              f"B={b}({b/len(td)*100:.0f}%) "
              f"C={c}({c/len(td)*100:.0f}%)")

        w(subsep("4c) Padrão de checks nos QUASEs"))
        qtoks = [t for t in tokens if t['status'] == 'QUASE']
        if qtoks:
            patterns = {}
            for t in qtoks:
                p = f"A={t['check_a']} B={t['check_b']} C={t['check_c_total'] or 0}"
                patterns[p] = patterns.get(p, 0) + 1
            for p, c in sorted(patterns.items(), key=lambda x: -x[1]):
                w(f"  {p}: {c}x")
        else:
            w("  (sem QUASEs no período)")
    else:
        w("  (sem tokens no período)")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 5 — ZONAS DE DECISÃO"))
    # ══════════════════════════════════════════════════════════════════════════

    zonas = {}
    zonas_qc = {}
    for t in tokens:
        z = t['zona_qualidade'] or '?'
        zonas[z] = zonas.get(z, 0) + 1
        if t['status'] in ('QUASE', 'CALL'):
            zonas_qc[z] = zonas_qc.get(z, 0) + 1

    w(subsep("5a) Distribuição e taxa de conversão"))
    w(f"  {'Zona':<16} {'Total':>7} {'QUASE/CALL':>11} {'Conv%':>7}")
    for z in sorted(zonas, key=lambda x: -zonas[x]):
        tz = zonas[z]
        qc = zonas_qc.get(z, 0)
        pct = qc / tz * 100 if tz else 0
        w(f"  {z:<16} {tz:>7} {qc:>11} {pct:>6.0f}%")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 6 — TOP TOKENS"))
    # ══════════════════════════════════════════════════════════════════════════

    tok_freq = {}
    tok_qc = {}
    for t in tokens:
        s = t['symbol']
        tok_freq[s] = tok_freq.get(s, 0) + 1
        if t['status'] in ('QUASE', 'CALL'):
            tok_qc[s] = tok_qc.get(s, 0) + 1

    w(subsep("6a) Por aparições em zona (top 15)"))
    w(f"  {'Símbolo':<14} {'Aparições':>10} {'QUASE/CALL':>11} {'Conv%':>7}")
    for s in sorted(tok_freq, key=lambda x: -tok_freq[x])[:15]:
        ts_ = tok_freq[s]
        qc = tok_qc.get(s, 0)
        pct = qc / ts_ * 100 if ts_ else 0
        w(f"  {s:<14} {ts_:>10} {qc:>11} {pct:>6.0f}%")

    # ══════════════════════════════════════════════════════════════════════════
    w(sep("SEÇÃO 7 — EVENTOS EMITIDOS"))
    # ══════════════════════════════════════════════════════════════════════════

    events = conn.execute("""
        SELECT ts, type, symbol, direction, zona, check_c
        FROM round_events WHERE ts >= ? ORDER BY ts DESC
    """, (desde,)).fetchall()

    calls  = [e for e in events if e['type'] == 'CALL']
    quases = [e for e in events if e['type'] == 'QUASE']

    w(subsep("7a) Resumo"))
    w(f"  CALLs  : {len(calls)}")
    w(f"  QUASEs : {len(quases)}")

    w(subsep("7b) CALLs (detalhado)"))
    if calls:
        for e in calls:
            w(f"  {e['ts'][:16]} | {e['direction']} {e['symbol']} "
              f"zona={e['zona']} check_c={e['check_c']}")
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
    log_files = sorted(glob.glob(f"{DIR_LOGS}/atirador_LOG_*.log"), reverse=True)[:10]
    erros = []
    for lf in log_files:
        try:
            for line in open(lf):
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
    arquivos = [
        DB_V7,
        "/home/ubuntu/Setup_Atirador/journal/atirador_journal.db",
        "/home/ubuntu/Setup_Atirador/states/atirador_state.json",
    ]
    for arq in arquivos:
        if os.path.exists(arq):
            sz_kb = os.path.getsize(arq) / 1024
            mtime = datetime.fromtimestamp(os.path.getmtime(arq), tz=BRT).isoformat()
            w(f"  {sz_kb:>8.1f} KB  {mtime}  {arq}")
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

    conn.close()
    w(sep())
    _out(lines, args.out)


def _out(lines, path):
    text = "\n".join(lines)
    if path:
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(path, "w") as f:
            f.write(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
