# backtest/kis_saida.py — Setup Atirador (SANDBOX, camada 2) — KIS: SAIDA
# Briefing v3 de 14/11/2026, ETAPA 1. Exploratorio: nada aqui promove nada.
#
# PERGUNTA: o KIS devolve lucro (39,5% das perdas ja estiveram no lucro, ledger
# 21/08). Um stop de emergencia fixo ou um chandelier, ambos em ATR(14) da
# barra do sinal, recuperam essa devolucao sem quebrar a saida por inversao?
#
# MOTOR REUSADO, nao reescrito: entradas = `kis_horizonte.entradas` (par
# parametrizavel, warmup = EMA lenta), caminho = `excursion.measure_event`
# (path_cap=EXT_CAP=256), E0 = `kis_trail.trail_exit(arme=0)` com a MESMA conta
# de bps do kis_horizonte (para a conferencia bater exato), portao =
# `kis_regime.passa(0.02, 11)` sobre `inclinacoes` (EMA21 FIXA) e `_adx` 13.
#
# CELULAS por par (34/89 e 8/21, portao SEMPRE ligado):
#   E0     saida so na inversao do alvo (original)
#   E1-k   E0 + stop de emergencia FIXO em -k ATR do close do sinal, k = 2,3,4
#   E2-k   E0 + chandelier: stop = melhor extremo a favor desde a entrada - k ATR
#          (melhor extremo comeca no close do sinal => na 1a barra E2-k = E1-k)
# ATR = o ATR(14) de Wilder da barra do sinal que `measure_event` ja devolve; e
# a unidade de todo o caminho, entao o stop vive na mesma regua. NAO e ATR
# rolante (escolha documentada; o briefing diz "ATR(14) da barra do sinal" para
# E1 e nao especifica outro para E2).
# EXECUCAO: stop ativo desde a 1a barra apos o sinal; o nivel usado na barra j
# e o estabelecido ate j-1 (mesma ordem intrabarra do trail_exit). Preenchimento
# min(stop, open) orientado a favor (= min no LONG, max no SHORT): barra que ja
# abre alem do nivel paga a abertura (licao de agosto: +45 bps de ruido).
# Depois do stop a posicao fica ZERADA ate a proxima inversao — por construcao:
# cada trade vai de uma inversao a seguinte, e o stop so encurta o trade.
#
# TETO DE LEITURA: nenhum candle com ts >= --end (default 2026-08-26 00:00 UTC)
# e lido: a conexao ganha uma TEMP VIEW `candles` que esconde a tabela real (o
# SQLite resolve nome sem esquema primeiro em `temp`), entao NENHUMA
# leitura - inclusive o caminho forward de measure_event, que nao recebe
# `end` - enxerga o que vem depois; o main confere a view (vazamento de
# futuro ABORTA). A recusa de `--end` acima de 2026-08-26 (T1) foi retirada
# em 23/11/2026: qualquer `--end` e aceito (regra de estudo e do Marcelo, nao
# do instrumento).
#
# CUSTO: 6 bps/trade (convencao do motor e do ledger). A coluna a 10 bps
# (5 bps x 2 pernas, custo do forward) e subtracao linear: ret10 = ret6 - 4n.
#
# Uso (VM): python -m backtest.kis_saida --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP, _adx                   # noqa: E402
from backtest.kis_horizonte import (PORTAO_ADX_MIN, PORTAO_LIMIAR,  # noqa: E402
                                    entradas)
from backtest.kis_regime import inclinacoes, passa                # noqa: E402
from backtest.kis_trail import (COST_BPS, METADE_1, METADE_2,       # noqa: E402
                                _dentro, _quarter, _rss_mib,
                                add_teto_rss, define_teto_rss, teto_rss,
                                trail_exit)

TETO_ISO = "2026-08-26"            # T1: exclusivo (ts < 2026-08-26 00:00 UTC)
H1 = ("2024-08-26", "2025-08-25")  # metades por data de ENTRADA, inclusivas
H2 = ("2025-08-26", "2026-08-25")
COST_FWD_BPS = 10.0                # 5 bps x 2 pernas
PARES = ((34, 89), (8, 21))
KS = (2, 3, 4)
CELULAS = [("E0", 0, False)] + [(f"E1-{k}", k, False) for k in KS] + \
          [(f"E2-{k}", k, True) for k in KS]
NOMES = [c[0] for c in CELULAS]
BAR_4H_MS = 14_400_000
# Conferencia (briefing 2): E0 34/89 + portao, TIER1, janela do Eixo 1, metades
# do kis_trail. Ledger 26/08: 2a metade +54,2 bps/trade; por simbolo (19 tokens)
# 1a +1.435, 2a +2.300.
CONF_JANELA = ("2024-05-22", "2026-06-21")
CONF_REF = {"bpt_2a": 54.2, "sym_1a": 1435.0, "sym_2a": 2300.0}


def _ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
               .timestamp() * 1000)


def _iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime(
        "%Y-%m-%d %H:%M")


def conecta_com_teto(store: str, teto_ms: int):
    """Conexao em que a tabela `candles` so mostra ts < teto_ms. A TEMP VIEW
    tem o mesmo nome da tabela e vence a resolucao de nome sem esquema; todo
    `read_candles` (e portanto measure_event) passa por ela."""
    from backtest.candle_store import connect
    if not Path(store).is_file():
        raise SystemExit(f"[kis_saida] store inexistente: {store} (connect() "
                         f"criaria um .db vazio; abortando)")
    conn = connect(store)
    conn.execute("CREATE TEMP VIEW candles AS SELECT * FROM main.candles "
                 f"WHERE ts < {int(teto_ms)}")
    return conn


def stop_exit(fav, adv, abertura, fwd_bar, hold: int, k: float,
              movel: bool) -> tuple:
    """(retorno em ATR orientado a favor, parou_no_stop). Stop fixo em -k
    (movel=False) ou chandelier melhor_fav - k (movel=True; melhor comeca em 0
    = close do sinal). Nivel da barra j usa so barras < j. Sem stop atingido,
    sai igual ao E0: fechamento da barra hold-1 (a inversao)."""
    melhor = 0.0
    for j in range(hold):
        stop = (melhor - k) if movel else -k
        if -adv[j] <= stop:
            return min(stop, abertura[j]), True
        if movel and fav[j] > melhor:
            melhor = fav[j]
    return fwd_bar[hold - 1], False


def inicio_por_simbolo(conn, symbols: list, tf: str) -> dict:
    """{sym: (primeiro_ts, ultimo_ts, n)} lido do store em tempo de execucao
    (ja atras do teto). Simbolo sem candle -> None."""
    out = {}
    for s in symbols:
        r = conn.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM candles "
                         "WHERE symbol = ? AND timeframe = ?", (s, tf)).fetchone()
        out[s] = r if r and r[0] is not None else None
    return out


def run(conn, symbols: list, tf: str, start_ms, end_ms: int,
        pares=PARES) -> dict:
    """Por simbolo, le as velas UMA vez e roda os pares. Guarda por trade
    (bar_ts, sym, is_long, rets[7] liquidos a 6 bps, bits de stop, no_teto_256,
    marcado_no_fim). So entradas que PASSAM no portao viram trade; a inversao
    vetada continua fechando o trade anterior (o `hold` ja e ate a proxima
    inversao, passe ela ou nao)."""
    from backtest.candle_store import read_candles
    from backtest.excursion import measure_event
    por_par = {p: [] for p in pares}
    cont = {p: {"inversoes": 0, "vetadas_portao": 0, "excl_borda": 0,
                "excl_hold": 0} for p in pares}
    rss = _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=start_ms, end_ms=end_ms)
        if not cndl:
            continue
        closes = [c["close"] for c in cndl]
        incl, adx = inclinacoes(cndl, closes), _adx(cndl)
        pos = {c["ts"]: i for i, c in enumerate(cndl)}
        ult = len(cndl) - 1
        for par in pares:
            evs = entradas(cndl, par[0], par[1])
            for n_ev, ev in enumerate(evs):
                cont[par]["inversoes"] += 1
                i = pos[ev["bar_ts"]]
                if not passa(ev["direction"], incl[i], adx[i],
                             PORTAO_LIMIAR, PORTAO_ADX_MIN):
                    cont[par]["vetadas_portao"] += 1
                    continue
                m = measure_event(conn, sym, ev["bar_ts"], ev["direction"],
                                  tf=tf, path_cap=EXT_CAP)
                if m is None:
                    cont[par]["excl_borda"] += 1
                    continue
                hold = ev["hold"]
                if hold > len(m["fwd_bar"]):
                    cont[par]["excl_hold"] += 1
                    continue
                ap = m["atr"] / m["entry"]
                # E0: conta copiada do kis_horizonte (ordem de associacao inclusa)
                rets = [trail_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                                   m["fwd_bar"], hold, 0, 0)
                        * ap * 1e4 - COST_BPS]
                bits = 0
                for b, (_nome, k, movel) in enumerate(CELULAS[1:], start=1):
                    r, parou = stop_exit(m["fav_bar"], m["adv_bar"],
                                         m["open_bar"], m["fwd_bar"], hold,
                                         k, movel)
                    rets.append(r * ap * 1e4 - COST_BPS)
                    bits |= (1 << b) if parou else 0
                ultimo = n_ev == len(evs) - 1 and i + hold == ult
                por_par[par].append((ev["bar_ts"], sym,
                                     ev["direction"] == "LONG", rets, bits,
                                     hold >= EXT_CAP, ultimo))
        rss = _rss_mib()
        if rss > teto_rss():
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {teto_rss():.0f} "
                              f"apos {sym}; PARANDO antes de estourar a VM.")
    for p in pares:
        por_par[p].sort(key=lambda t: t[0])
    return {"por_par": por_par, "cont": cont, "rss": rss}


def resumo(trades: list, k: int, faixa: tuple) -> dict:
    """Metricas da celula k nos trades com ENTRADA dentro de `faixa`."""
    sel = [t for t in trades if _dentro(t[0], faixa)]
    rs = [t[3][k] for t in sel]
    n = len(rs)
    cum = pico = dd = 0.0
    tri, mes, sym = {}, {}, {}
    for t, r in zip(sel, rs):
        cum += r
        pico = max(pico, cum)
        dd = max(dd, pico - cum)
        q = _quarter(t[0])
        tri[q] = tri.get(q, 0.0) + r
        mm = _iso(t[0])[:7]
        mes[mm] = mes.get(mm, 0.0) + r
        sym[t[1]] = sym.get(t[1], 0.0) + r
    tot = sum(rs)
    melhor_mes = max(mes, key=mes.get) if mes else ""
    top3 = sorted(sym.items(), key=lambda x: -x[1])[:3]
    nl = sum(1 for t in sel if t[2])
    return {
        "n": n, "ret6": tot, "ret10": tot - (COST_FWD_BPS - COST_BPS) * n,
        "bpt6": tot / n if n else 0.0,
        "bpt10": (tot / n - (COST_FWD_BPS - COST_BPS)) if n else 0.0,
        "dd": dd,
        "tri_pos": sum(1 for v in tri.values() if v > 0), "tri_tot": len(tri),
        "n_long": nl, "ret_long": sum(r for t, r in zip(sel, rs) if t[2]),
        "n_short": n - nl,
        "ret_short": sum(r for t, r in zip(sel, rs) if not t[2]),
        "top3": " ".join(f"{s.replace('USDT', '')}:{v:+.0f}" for s, v in top3),
        "melhor_mes": melhor_mes,
        "ret_sem_melhor_mes": tot - (mes[melhor_mes] if mes else 0.0),
        "n_stop": sum(1 for t in sel if t[4] >> k & 1),
        "n_trunc256": sum(1 for t in sel if t[5]),
        "n_marcado_fim": sum(1 for t in sel if t[6]),
    }


CAMPOS = ["recorte", "par", "celula", "metade", "n", "ret6", "ret10", "bpt6",
          "bpt10", "dd", "tri_pos", "tri_tot", "n_long", "ret_long", "n_short",
          "ret_short", "top3", "melhor_mes", "ret_sem_melhor_mes", "n_stop",
          "n_trunc256", "n_marcado_fim"]


def linhas(por_par: dict, recortes: dict) -> list:
    out = []
    for nome_rec, syms in recortes.items():
        for par, trades in por_par.items():
            sub = [t for t in trades if t[1] in syms]
            for k, cel in enumerate(NOMES):
                for nome_h, faixa in (("H1", H1), ("H2", H2)):
                    out.append({"recorte": nome_rec,
                                "par": f"{par[0]}/{par[1]}", "celula": cel,
                                "metade": nome_h, **resumo(sub, k, faixa)})
    return out


def conferencia(conn, tf: str, symbols: list) -> None:
    """E0 34/89 + portao, TIER1, janela e metades do Eixo 1 (kis_trail)."""
    r = run(conn, symbols, tf, _ms(CONF_JANELA[0]), _ms(CONF_JANELA[1]),
            pares=((34, 89),))
    tr = r["por_par"][(34, 89)]
    a, b = resumo(tr, 0, METADE_1), resumo(tr, 0, METADE_2)
    n_sym = len({t[1] for t in tr}) or 1
    por_tok = [sum(1 for t in tr if t[1] == s) for s in {t[1] for t in tr}]
    print("\n===== CONFERENCIA: E0 34/89 + portao, TIER1, janela do Eixo 1 =====")
    print(f"janela {CONF_JANELA[0]} -> {CONF_JANELA[1]} | metades {METADE_1} / "
          f"{METADE_2} | simbolos com trade {n_sym} | n={len(tr)} | mediana "
          f"trades/token {median(por_tok) if por_tok else 0:.0f}")
    print(f"  2a metade bps/trade : {b['bpt6']:+.1f}  (ledger 26/08: "
          f"{CONF_REF['bpt_2a']:+.1f}) dif {b['bpt6'] - CONF_REF['bpt_2a']:+.1f}")
    print(f"  1a metade bps/trade : {a['bpt6']:+.1f}")
    print(f"  por simbolo 1a      : {a['ret6'] / n_sym:+.0f}  (ledger: "
          f"{CONF_REF['sym_1a']:+.0f}) | 2a: {b['ret6'] / n_sym:+.0f}  (ledger: "
          f"{CONF_REF['sym_2a']:+.0f})")
    print("  (diferenca esperada se o store mudou desde 26/08 — reconstrucao do "
          "store, TONUSDT delistada; anotar, nao ajustar)")


def _guarda_relogio() -> bool:
    """R3 como AVISO (23/11/2026; os crons estao desligados desde 14/11): perto
    do minuto 16 das barras 4h (scan do cron) imprime o aviso e SEGUE. Devolve
    True se avisou. `--sem-guarda-relogio` nos instrumentos nem chama."""
    now = datetime.now(timezone.utc)
    if now.hour % 4 == 0 and 10 <= now.minute <= 30:
        print(f"[aviso] {now:%H:%M} UTC: janela do scan 4h (minuto 16). Se o "
              f"cron estiver ligado, a rodada disputa CPU e memoria com ele. "
              f"Seguindo.", flush=True)
        return True
    return False


def _fmt(row: dict) -> str:
    return (f"{row['par']:>5} {row['celula']:>5} {row['metade']} | "
            f"{row['n']:>4} | {row['ret6']:>9.0f} | {row['ret10']:>9.0f} | "
            f"{row['bpt6']:>+7.1f} | {row['bpt10']:>+7.1f} | {row['dd']:>7.0f} | "
            f"{row['tri_pos']:>2}/{row['tri_tot']:<2} | "
            f"{row['n_long']:>3} {row['ret_long']:>+8.0f} | "
            f"{row['n_short']:>3} {row['ret_short']:>+8.0f} | "
            f"{row['ret_sem_melhor_mes']:>+9.0f} ({row['melhor_mes']}) | "
            f"{row['n_stop']:>4} | {row['n_trunc256']:>2} | {row['top3']}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="KIS: saida (etapa 1, sandbox)")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=TETO_ISO,
                    help=f"exclusivo; default {TETO_ISO} (qualquer data e "
                         "aceita)")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None,
                    help="default: SYMBOLS_KIS_3489 (65)")
    ap.add_argument("--out", default=None)
    ap.add_argument("--sem-conferencia", action="store_true")
    ap.add_argument("--sem-guarda-relogio", action="store_true",
                    help="so para teste local")
    add_teto_rss(ap)
    args = ap.parse_args(argv)
    define_teto_rss(args.teto_rss)

    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from backtest.sweep import TIER1
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    teto_ms = _ms(args.end)
    conn = conecta_com_teto(args.store, teto_ms)   # T1, camada 2
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        print(f"[kis_saida] teto {args.end} 00:00 UTC (exclusivo) | maior ts "
              f"visivel no store: {_iso(mx) if mx else '-'} | tf={args.tf}")
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[kis_saida] VAZAMENTO: view deixou passar "
                             "candle >= --end. ABORTADO.")

        ini = inicio_por_simbolo(conn, symbols, args.tf)
        print("\n===== INICIO POR SIMBOLO (lido do store, atras do teto) =====")
        for s, v in ini.items():
            print(f"  {s:>14}: " + (f"{_iso(v[0])} -> {_iso(v[1])} n={v[2]}"
                                    if v else "SEM CANDLES"))
        firsts = [v[0] for v in ini.values() if v]
        if firsts:
            print(f"  primeiro candle: min {_iso(min(firsts))} | mediana "
                  f"{_iso(int(median(firsts)))} | max {_iso(max(firsts))}")

        if not args.sem_conferencia:
            conferencia(conn, args.tf, [s for s in TIER1])

        r = run(conn, [s for s in symbols if ini.get(s)], args.tf, None,
                teto_ms - 1)
    finally:
        conn.close()

    tier1 = set(TIER1) & set(symbols)
    recortes = {"UNIVERSO": set(symbols), "TIER1": tier1}
    recortes.update({s: {s} for s in symbols})
    rows = linhas(r["por_par"], recortes)

    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"kis_saida_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for row in rows:
            w.writerow({c: (f"{row[c]:.1f}" if isinstance(row[c], float)
                            else row[c]) for c in CAMPOS})

    print("\n===== CONTAGENS =====")
    for par, c in r["cont"].items():
        tr = r["por_par"][par]
        fora = sum(1 for t in tr if not (_dentro(t[0], H1) or _dentro(t[0], H2)))
        print(f"  {par[0]}/{par[1]}: inversoes {c['inversoes']} | vetadas pelo "
              f"portao {c['vetadas_portao']} | excl_borda {c['excl_borda']} | "
              f"excl_hold {c['excl_hold']} | trades {len(tr)} | antes de H1 "
              f"(fora das metades) {fora} | hold no teto 256 "
              f"{sum(1 for t in tr if t[5])} | marcados no fim do dado "
              f"{sum(1 for t in tr if t[6])}")
    print(f"  RSS de pico {r['rss']:.0f} MiB (teto {teto_rss():.0f}) | "
          f"CSV: {out}")

    cab = (f"{'par':>5} {'cel':>5} H  | {'n':>4} | {'ret@6':>9} | {'ret@10':>9} | "
           f"{'bpt@6':>7} | {'bpt@10':>7} | {'dd':>7} | {'tri':>5} | "
           f"{'nL':>3} {'retL':>8} | {'nS':>3} {'retS':>8} | "
           f"{'sem melhor mes':>23} | {'stop':>4} | {'tr':>2} | top3")
    for rec in ("UNIVERSO", "TIER1"):
        print(f"\n===== {rec} ({len(recortes[rec])} simbolos) — por celula, "
              f"H1 {H1[0]}..{H1[1]} | H2 {H2[0]}..{H2[1]} =====")
        print(cab)
        for row in rows:
            if row["recorte"] == rec:
                print(_fmt(row))

    print("\n===== PLATO EM H1 — delta bps/trade vs E0, UNIVERSO =====")
    for par in r["por_par"]:
        p = f"{par[0]}/{par[1]}"
        base = {h: next(x for x in rows if x["recorte"] == "UNIVERSO" and
                        x["par"] == p and x["celula"] == "E0" and
                        x["metade"] == h)["bpt6"] for h in ("H1", "H2")}
        for cel in NOMES[1:]:
            d = {h: next(x for x in rows if x["recorte"] == "UNIVERSO" and
                         x["par"] == p and x["celula"] == cel and
                         x["metade"] == h)["bpt6"] - base[h] for h in ("H1", "H2")}
            print(f"  {p:>5} {cel:>5}: H1 {d['H1']:>+7.1f} | H2 {d['H2']:>+7.1f}")

    print("\n===== POR MES — UNIVERSO, ret@6 bps (soma por mes de ENTRADA) =====")
    for par, tr in r["por_par"].items():
        print(f"  par {par[0]}/{par[1]}")
        print("  " + f"{'mes':>7} {'n':>4} " + " ".join(f"{c:>8}" for c in NOMES))
        meses = sorted({_iso(t[0])[:7] for t in tr})
        for mm in meses:
            sel = [t for t in tr if _iso(t[0])[:7] == mm]
            print("  " + f"{mm:>7} {len(sel):>4} " + " ".join(
                f"{sum(t[3][k] for t in sel):>8.0f}" for k in range(len(NOMES))))

    print("\n===== POR SIMBOLO — E0 vs melhor E1/E2 em H1, ret@6 H1 | H2 =====")
    for par in r["por_par"]:
        p = f"{par[0]}/{par[1]}"
        print(f"  par {p}")
        for s in symbols:
            rs = {(x["celula"], x["metade"]): x for x in rows
                  if x["recorte"] == s and x["par"] == p}
            if not rs or rs[("E0", "H1")]["n"] + rs[("E0", "H2")]["n"] == 0:
                continue
            print(f"  {s:>14} n {rs[('E0','H1')]['n']:>3}/{rs[('E0','H2')]['n']:<3}"
                  + " ".join(f" {c}:{rs[(c,'H1')]['ret6']:>+6.0f}|"
                             f"{rs[(c,'H2')]['ret6']:>+6.0f}" for c in NOMES))

    print("\ncaveat: EXPLORATORIO (camada 2), nada promove. Teto de leitura "
          f"{args.end} (default: hold-out 31/08->14/11 fora). Metades por "
          "data de "
          "ENTRADA; trades antes de H1 nao entram em H1/H2. Mergulho = sobre a "
          "curva de trades ordenada por entrada (nao marca a mercado, nao "
          "considera trades simultaneos). Stop em ATR(14) da barra do sinal, "
          "fixo; chandelier parte do close do sinal. Fill min(stop, open). "
          "Hold teto 256 barras (coluna tr). Ultimo trade de cada simbolo e "
          "marcado no ultimo close antes do teto. @10 = @6 - 4 bps x n. "
          "Comparar com o trail 8/21 sem portao, 20 tokens (ledger 21/08): "
          "la nao houve plato e o ganho vivia so na 1a metade.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
