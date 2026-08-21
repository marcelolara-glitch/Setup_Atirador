# backtest/kis_trail.py — Setup Atirador v9 (BANCADA) — varredura EXPLORATORIA
# Trail armado sobre o detector kis_extremos. NAO toca runtime, NAO altera
# detector algum: reusa `extremos_entries` e `measure_event` como estao e troca
# SO a regra de saida. Nenhuma celula aqui e promovivel por si.
#
# Regra do trail (por trade, em unidades de ATR da barra de entrada):
#   sem posicao de stop ate o trade atingir MFE >= arme; dai em diante
#       stop = melhor_preco_ate_agora - recuo   (na orientacao a favor)
#   saida = o que vier primeiro entre stop armado, inversao do sinal e fim do
#   hold. arme = 0 significa SEM TRAIL (celula de controle).
# No kis_extremos inversao E fim do hold sao o MESMO evento (`hold` = barras
# ate a proxima entrada, que so existe invertendo), entao a celula arme=0
# reproduz exatamente o primario `extremos` do stage1 — e a ancora de leitura
# da grade: toda celula com trail e lida CONTRA ela.
#
# MEMORIA (restricao dura: a VM tem 956 MiB TOTAIS com o cron na mesma
# maquina). Guardar fav/adv de 256 barras por evento pediria ~600 MiB. Por isso
# as 17 celulas sao avaliadas DENTRO do laco do proprio evento e so os 17
# escalares sobrevivem; o caminho barra a barra morre na mesma iteracao. O teto
# de RSS e checado a cada simbolo e ABORTA antes de estourar.
#
# CAVEAT DE LEITURA — LEIA ANTES DE OLHAR QUALQUER NUMERO. A saida no stop e
# preenchida no NIVEL do stop, que e a convencao padrao de ordem stop e o que
# o briefing pediu. Ela e OTIMISTA: quando a barra atravessa o nivel de uma vez,
# o mercado imprimiu abaixo dele e o backtest cobra o nivel. Medido em bancada
# (scratchpad, 5 simbolos de passeio aleatorio SEM drift, custo descontado): o
# controle rende ~1 bps/trade e arme=1/recuo=1 rende ~45 bps/trade — em RUIDO
# PURO, onde o valor honesto e zero. O vies CRESCE quanto mais apertado o
# recuo, porque mais saidas viram atravessamento. Consequencia pratica: NAO
# leia a celula pelo numero absoluto nem prefira a de recuo menor por ela ser
# maior. A unica leitura defensavel e o desenho — se alguma regiao da grade se
# sustenta nas DUAS metades e nos trimestres, e onde ela fica — e sempre contra
# a celula de controle, que nao tem stop e portanto nao tem esse vies.
#
# CRITERIO (ledger de 20/08): 1a metade > 0 E 2a metade > 0 E maioria dos
# trimestres positivos E n >= 30. Sem blockP2.5, sem p_shift — nao sao o
# criterio e nao aparecem nesta varredura.
# Uso: .venv/bin/python -m backtest.kis_trail --start 2024-05-22 --end 2026-06-21
from __future__ import annotations
import argparse
import csv
import resource
import sys
from array import array
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP, extremos_entries   # noqa: E402

# GRADE FECHADA, definida no briefing e NAO ajustavel depois do fato: 1 celula
# de controle (arme=0, sem trail, recuo irrelevante) + 4 armes x 4 recuos.
ARMES, RECUOS = (1, 2, 3, 4), (1, 2, 3, 4)
GRADE = [(0, 0)] + [(a, r) for a in ARMES for r in RECUOS]   # 17 celulas
COST_BPS = 6.0                  # custo por trade, mesmo GATE_COST do stage1
RSS_CEILING_MIB = 250.0         # teto duro; acima disso ABORTA sem seguir
# Metades por data de ENTRADA do trade. Fronteira do ledger de 20/08: ajusta na
# primeira, confere na segunda sem mexer em nada.
METADE_1 = ("2024-05-22", "2025-06-06")
METADE_2 = ("2025-06-07", "2026-06-21")
MIN_TRADES = 30                 # (c) do criterio


def trail_exit(fav: list, adv: list, fwd_bar, hold: int,
               arme: float, recuo: float) -> float:
    """Retorno de saida do trade, em ATR, na orientacao A FAVOR da direcao.
    `fav[j]`/`adv[j]` = excursoes favoravel/adversa da barra j (sem piso, ja
    orientadas), `fwd_bar[j]` = fechamento dela; a barra percorre
    [-adv[j], +fav[j]]. arme=0 -> sem stop: fecha na barra `hold`-1, que E a
    inversao. ORDEM INTRABARRA: o stop e checado com o nivel estabelecido ate a
    barra ANTERIOR, antes de a maxima da barra j poder eleva-lo — nao olha o
    futuro e nao inventa uma sequencia que os OHLC nao contam (uma barra que
    arma e tocaria o proprio stop nao tem ordem resolvivel, entao nao resolve).
    Preenchimento no nivel do stop: ver o CAVEAT no topo do modulo."""
    if arme <= 0:
        return fwd_bar[hold - 1]
    mfe = float("-inf")         # melhor preco ate agora, orientado a favor
    for j in range(hold):
        if mfe >= arme:         # armado por alguma barra ANTERIOR a j
            stop = mfe - recuo
            if -adv[j] <= stop:
                return stop
        if fav[j] > mfe:
            mfe = fav[j]
    return fwd_bar[hold - 1]


def _rss_mib() -> float:
    """Pico de RSS do processo, em MiB (ru_maxrss vem em KiB no Linux)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc)


def _quarter(ts_ms: int) -> tuple:
    d = _dt(ts_ms)
    return (d.year, (d.month - 1) // 3 + 1)


def _dentro(ts_ms: int, faixa: tuple) -> bool:
    """Metade por data de ENTRADA, com as duas pontas INCLUSIVAS (as faixas do
    briefing sao contiguas e disjuntas: 06-06 fecha a 1a, 06-07 abre a 2a)."""
    d = _dt(ts_ms).strftime("%Y-%m-%d")
    return faixa[0] <= d <= faixa[1]


def metricas(trades: list, k: int) -> dict:
    """Metricas da celula `k` sobre `trades` = [(bar_ts, array de 17 bps
    liquidos)], ja ordenados por bar_ts. Formato de trader: contagem, acerto,
    as duas metades, o total, o pior mergulho e a contagem de trimestres."""
    rets = [c[k] for _ts, c in trades]
    n = len(rets)
    cum = pico = dd = 0.0
    por_tri: dict = {}
    for (ts, _c), r in zip(trades, rets):
        cum += r
        pico = max(pico, cum)
        dd = max(dd, pico - cum)          # queda desde o topo da curva acumulada
        q = _quarter(ts)
        por_tri[q] = por_tri.get(q, 0.0) + r
    return {
        "n_trades": n,
        "acerto_pct": (100.0 * sum(1 for r in rets if r > 0) / n) if n else 0.0,
        "ret_1a_metade_bps": sum(r for (ts, _c), r in zip(trades, rets)
                                 if _dentro(ts, METADE_1)),
        "ret_2a_metade_bps": sum(r for (ts, _c), r in zip(trades, rets)
                                 if _dentro(ts, METADE_2)),
        "ret_total_bps": sum(rets),
        "dd_max_bps": dd,
        "trimestres_pos": sum(1 for v in por_tri.values() if v > 0),
        "trimestres_total": len(por_tri),
    }


def aprovada(m: dict) -> bool:
    """Criterio do ledger de 20/08 (as quatro condicoes do RESUMO). O item (d),
    drawdown tolerado, e decisao do Marcelo: fica REPORTADO, nunca filtrado."""
    return (m["n_trades"] >= MIN_TRADES
            and m["ret_1a_metade_bps"] > 0 and m["ret_2a_metade_bps"] > 0
            and m["trimestres_total"] > 0
            and m["trimestres_pos"] / m["trimestres_total"] >= 0.5)


def run(store: str, symbols: list, start_iso: str, end_iso: str, tf: str) -> dict:
    """Por simbolo: entradas do kis_extremos -> mede -> avalia as 17 celulas na
    hora -> guarda 17 escalares. Populacao de trades IDENTICA a do stage1 em
    --detector kis_extremos (mesmas duas exclusoes); so a saida muda. Imports
    pesados sao lazy p/ os testes coletarem no sandbox."""
    from backtest.candle_store import connect, read_candles          # noqa: E402
    from backtest.excursion import _iso_to_ms, measure_event         # noqa: E402
    conn = connect(store)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)
    por_sym, excl_borda, excl_hold, rss = {}, 0, 0, _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=start_ms, end_ms=end_ms)
        trades = []
        for ev in extremos_entries(cndl):
            m = measure_event(conn, sym, ev["bar_ts"], ev["direction"],
                              tf=tf, path_cap=EXT_CAP)
            if m is None:               # borda direita / atr<=0 — regra travada
                excl_borda += 1
                continue
            if ev["hold"] > len(m["fwd_bar"]):
                excl_hold += 1          # hold nao cabe no futuro disponivel
                continue
            ap = m["atr"] / m["entry"]
            # AQUI mora a restricao de memoria: as 17 celulas saem de UMA
            # passada sobre o caminho deste evento, e o caminho e descartado.
            trades.append((ev["bar_ts"], array("d", [
                trail_exit(m["fav_bar"], m["adv_bar"], m["fwd_bar"],
                           ev["hold"], a, r) * ap * 1e4 - COST_BPS
                for a, r in GRADE])))
        por_sym[sym] = trades
        rss = _rss_mib()
        if rss > RSS_CEILING_MIB:
            conn.close()
            raise MemoryError(
                f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} MiB apos "
                f"{sym}; PARANDO antes de estourar a VM.")
    conn.close()
    return {"por_sym": por_sym, "excl_borda": excl_borda,
            "excl_hold": excl_hold, "rss_mib": rss, "tf": tf,
            "start": start_iso, "end": end_iso}


def linhas(por_sym: dict) -> list:
    """Uma linha por (symbol, arme, recuo), mais o agregado UNIVERSO (todos os
    trades num pote so, ordenados por data de entrada — a curva do universo e
    a da carteira, nao a media das curvas por simbolo)."""
    todos = sorted((t for ts in por_sym.values() for t in ts),
                   key=lambda t: t[0])
    out = []
    for sym, trades in list(por_sym.items()) + [("UNIVERSO", todos)]:
        for k, (a, r) in enumerate(GRADE):
            out.append(dict(symbol=sym, arme=a, recuo=r, **metricas(trades, k)))
    return out


CAMPOS = ["symbol", "arme", "recuo", "n_trades", "acerto_pct",
          "ret_1a_metade_bps", "ret_2a_metade_bps", "ret_total_bps",
          "dd_max_bps", "trimestres_pos", "trimestres_total"]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Varredura EXPLORATORIA de trail armado no kis_extremos")
    ap.add_argument("--tf", choices=["15m", "1h", "4h"], default="4h")
    ap.add_argument("--start", default="2024-05-22")
    ap.add_argument("--end", default="2026-06-21")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    ap.add_argument("--out", default=None, help="default: logs/sweep_kis_<data>.csv")
    args = ap.parse_args()
    from backtest.sweep import TIER1                                 # noqa: E402
    symbols = args.symbols if args.symbols else TIER1
    print(f"[kis_trail] tf={args.tf} janela {args.start}->{args.end} "
          f"symbols={len(symbols)} celulas={len(GRADE)}", file=sys.stderr)

    r = run(args.store, symbols, args.start, args.end, args.tf)
    rows = linhas(r["por_sym"])

    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"sweep_kis_{datetime.now(timezone.utc):%Y%m%d}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for row in rows:
            w.writerow({c: (f"{row[c]:.1f}" if isinstance(row[c], float)
                            else row[c]) for c in CAMPOS})

    n_tot = sum(len(t) for t in r["por_sym"].values())
    print(f"\n===== VARREDURA kis_extremos + TRAIL ARMADO ({r['tf']}) =====")
    print(f"janela {r['start']} -> {r['end']} | {len(r['por_sym'])} simbolos | "
          f"trades: {n_tot} | excluidas_borda: {r['excl_borda']} | "
          f"excluidas_hold: {r['excl_hold']}")
    print(f"grade: arme {list(ARMES)} x recuo {list(RECUOS)} + controle "
          f"arme=0 (sem trail) = {len(GRADE)} celulas | custo {COST_BPS:.0f} bps")
    print(f"RSS de pico: {r['rss_mib']:.0f} MiB (teto {RSS_CEILING_MIB:.0f})")
    print(f"CSV: {out}")

    print("\n===== RESUMO — celulas que passam no criterio de 20/08 =====")
    print(f"(1a metade > 0 E 2a metade > 0 E trimestres_pos/total >= 0.5 E "
          f"n >= {MIN_TRADES}; drawdown vai REPORTADO, e decisao do Marcelo)")
    print(f"{'symbol':>10} | {'arme':>4} | {'recuo':>5} | {'n':>5} | "
          f"{'acerto':>6} | {'1a(bps)':>9} | {'2a(bps)':>9} | {'tot(bps)':>9} | "
          f"{'ddmax':>8} | {'trim':>7}")
    passou = 0
    for row in rows:
        if not aprovada(row):
            continue
        passou += 1
        print(f"{row['symbol']:>10} | {row['arme']:>4} | {row['recuo']:>5} | "
              f"{row['n_trades']:>5} | {row['acerto_pct']:>5.1f}% | "
              f"{row['ret_1a_metade_bps']:>9.1f} | "
              f"{row['ret_2a_metade_bps']:>9.1f} | "
              f"{row['ret_total_bps']:>9.1f} | {row['dd_max_bps']:>8.1f} | "
              f"{row['trimestres_pos']:>3}/{row['trimestres_total']:<3}")
    if not passou:
        print("  (nenhuma celula passou — ver o CSV completo para a proxima "
              "variacao a testar)")
    print(f"\ncelulas aprovadas: {passou} de {len(rows)} avaliadas "
          f"(inclui a linha UNIVERSO). EXPLORATORIO: nada aqui promove nada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
