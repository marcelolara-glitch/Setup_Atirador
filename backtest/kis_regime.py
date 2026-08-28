# backtest/kis_regime.py — Setup Atirador v9 (BANCADA) — varredura EXPLORATORIA
# Filtro de REGIME na ENTRADA do detector kis_extremos. NAO toca runtime, NAO
# altera detector algum: reusa `extremos_entries`, `measure_event` e a saida do
# primario (`trail_exit` com arme=0) como estao, e so decide QUEM entra.
# Nenhuma celula aqui e promovivel por si.
#
# O filtro (duas portas, avaliadas na barra do sinal):
#   inclinacao[i] = (EMA21[i] - EMA21[i-5]) / 5 / ATR14[i]   # ATR por barra
#   adx[i]        = ADX(13) de Wilder — o MESMO `_adx` do keepitsimple
#   LONG  entra so se inclinacao[i] >= +limiar E adx[i] >= adx_min
#   SHORT entra so se inclinacao[i] <= -limiar E adx[i] >= adx_min
# Cada porta com limiar <= 0 fica DESLIGADA, entao (limiar=0, adx_min=0) e
# literalmente SEM FILTRO — a celula de controle cai da regra, nao de um caso
# especial escrito a mao.
#
# O FILTRO MUDA A POPULACAO, NAO SO O RESULTADO — e e por isso que ele nao se
# le pelo retorno absoluto:
#   * Vetar um sinal NAO antecipa o seguinte. O alvo de `_alvo_extremos` segue
#     evoluindo intocado: o filtro le o alvo, nunca o escreve. As entradas
#     candidatas sao EXATAMENTE as do controle; a celula e um SUBCONJUNTO delas.
#   * Logo, se um LONG e vetado, a proxima entrada possivel e a proxima MUDANCA
#     de alvo — que e, por construcao do detector, um SHORT. A carteira fica
#     FORA do mercado nesse intervalo inteiro, e nao "espera um LONG melhor".
#   * O `hold` de quem passa nao muda: continua sendo barras ate a proxima
#     mudanca de alvo, vetada ou nao. Vetar nao estica o trade anterior nem
#     encurta o seguinte — so abre um buraco de exposicao.
#   * Consequencia direta: cada celula tem n PROPRIO, menor que o do controle.
#     Comparar totais entre celulas de n diferente e comparar carteiras
#     diferentes; dai `pct_sinais_mantidos` sair no CSV COLADO no n.
#
# COMO LER O DELTA — e onde ele MENTE. Aqui o delta NAO e o do kis_trail. La as
# celulas tinham a MESMA populacao e so a saida mudava, entao o delta isolava o
# efeito da regra. Aqui a populacao E a variavel: uma celula que corta 95% dos
# sinais tambem deixa de pagar 95% dos 6 bps de custo, e isso sozinho ja empurra
# o delta para cima sem que o filtro tenha acertado NADA. Medido na bancada
# (passeio aleatorio SEM drift, onde o valor honesto e zero): o controle com
# 1033 trades marcou delta 0 por definicao e a celula limiar=0.20/adx=25, com 2
# trades (0.2% mantidos), marcou +38719 bps na 1a metade — puro custo nao pago.
# Portanto: delta ALTO com pct_sinais_mantidos BAIXO nao e sinal, e aritmetica.
# So delta positivo que SOBREVIVE com uma fracao mantida grande diz alguma
# coisa, e mesmo esse se le com as duas metades, nunca pelo total.
# Porta LIGADA com valor indisponivel (warmup de EMA/ATR/ADX) VETA: nao se
# confirma o que nao se consegue calcular.
#
# VALIDACAO: a celula de controle TEM que reproduzir exatamente o arme=0 do
# kis_trail. `--valida-controle` roda as duas varreduras no mesmo store e
# reporta max|dif|, que precisa dar 0.00e+00.
#
# MEMORIA: mesma disciplina do kis_trail (a VM tem 956 MiB TOTAIS com o cron na
# mesma maquina). As 16 celulas saem de UMA passada por evento e so escalares
# sobrevivem — aqui, 2 floats + 1 inteiro de 16 bits por trade, porque a SAIDA
# nao muda entre celulas: o retorno do trade e um so, muda a filiacao. O teto de
# RSS e checado a cada simbolo e ABORTA antes de estourar.
#
# CRITERIO (ledger de 20/08): 1a metade > 0 E 2a metade > 0 E maioria dos
# trimestres positivos E n >= 30. Sem blockP2.5, sem p_shift — nao sao o
# criterio e nao aparecem nesta varredura.
# Uso: .venv/bin/python -m backtest.kis_regime --start 2024-05-22 --end 2026-06-21
from __future__ import annotations
import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import (ADX_LEN, EMA_FAST, EMA_SLOW,  # noqa: E402
                                   EXT_CAP, EXT_CONF, _adx, _alvo_extremos,
                                   _ema, extremos_entries, states)
from backtest.kis_trail import (COST_BPS, MIN_TRADES,            # noqa: E402
                                RSS_CEILING_MIB, _rss_mib, aprovada,
                                metricas, trail_exit)

ATR_LEN = 14                    # ATR de Wilder por barra, denominador da rampa
INCL_LOOKBACK = 5               # barras entre as duas leituras da EMA21
# GRADE FECHADA, definida no briefing e NAO ajustavel depois do fato: 1 celula
# de controle (sem filtro) + 4 limiares x 4 pisos de ADX.
LIMIARES = (0.00, 0.01, 0.02, 0.03)   # 0.00 desliga a rampa: ADX PURO
ADX_MINS = (0, 11, 14, 17)   # 20 e penhasco: retencao 82% -> 50%
GRADE = [(0.0, 0)] + [(l, a) for l in LIMIARES for a in ADX_MINS
                      if not (l == 0.0 and a == 0)]   # 16: (0, 0) uma so vez


def _atr_series(candles: list, n: int = ATR_LEN) -> list:
    """ATR de Wilder POR BARRA. TR[k] = max(h-l, |h-cprev|, |l-cprev|); semente
    em i=n = media simples dos n primeiros TRs; dai ATR[i] = (ATR[i-1]*(n-1) +
    TR[i])/n. None enquanto i < n. A semente casa, por construcao, com o escalar
    de `excursion._wilder_atr` sobre a mesma janela de n+1 candles — o teste
    trava as duas contas uma contra a outra pra elas nao divergirem em silencio.
    """
    out = [None] * len(candles)
    if len(candles) <= n:
        return out
    tr = [max(c["high"] - c["low"], abs(c["high"] - p["close"]),
              abs(c["low"] - p["close"]))
          for p, c in zip(candles, candles[1:])]     # tr[k] <-> candles[k+1]
    a = out[n] = sum(tr[:n]) / n
    for k in range(n, len(tr)):
        a = out[k + 1] = (a * (n - 1) + tr[k]) / n
    return out


def inclinacoes(candles: list, closes: list, atr_len: int = ATR_LEN,
                ema_slow: int = EMA_SLOW,
                lookback: int = INCL_LOOKBACK) -> list:
    """inclinacao[i] = (EMA21[i] - EMA21[i-5]) / 5 / ATR14[i]: deslocamento da
    EMA longa POR BARRA, medido em ATRs da propria barra i. Adimensional, entao
    compara entre simbolos e entre epocas sem renormalizar nada. None enquanto
    EMA21[i-5] ou ATR14[i] nao existirem, ou se ATR14[i] <= 0.

    Os tres periodos sao OPCIONAIS com o default de hoje: a varredura da grade
    chama sem argumento e nada muda; quem passa explicito e a ficha do setup."""
    ema, atr = _ema(closes, ema_slow), _atr_series(candles, atr_len)
    out = [None] * len(closes)
    for i in range(lookback, len(closes)):
        ini, fim, a = ema[i - lookback], ema[i], atr[i]
        if ini is None or fim is None or a is None or a <= 0:
            continue
        out[i] = (fim - ini) / lookback / a
    return out


def passa(direction: str, incl, adx, limiar: float, adx_min: float) -> bool:
    """Porteiro da ENTRADA de UM sinal numa celula. Porta com limiar <= 0 esta
    DESLIGADA — e assim que (0, 0) vira SEM FILTRO sem caso especial. Porta
    LIGADA com valor None (warmup) VETA."""
    if limiar > 0:
        if incl is None:
            return False
        a_favor = incl >= limiar if direction == "LONG" else incl <= -limiar
        if not a_favor:
            return False
    if adx_min > 0 and (adx is None or adx < adx_min):
        return False
    return True


def mascara(direction: str, incl, adx) -> int:
    """Filiacao do sinal nas 16 celulas, num inteiro de 16 bits (bit k = passou
    na GRADE[k]). O bit 0 e o controle e vale SEMPRE 1."""
    return sum(1 << k for k, (lim, amin) in enumerate(GRADE)
               if passa(direction, incl, adx, lim, amin))


def alvos(closes: list, ema_fast: int = EMA_FAST, ema_slow: int = EMA_SLOW,
          confirmacao: int = EXT_CONF) -> list:
    """Serie do alvo vigente por barra — `_alvo_extremos` sobre `states`, com os
    tres periodos EXPLICITOS. Existe para que quem precisa do alvo (o modelo de
    saida `reverse` do v10) nao tenha que reproduzir a composicao a mao."""
    return _alvo_extremos(states(closes, ema_fast, ema_slow), confirmacao)


def avaliar(candles: list, ema_fast: int = EMA_FAST, ema_slow: int = EMA_SLOW,
            confirmacao: int = EXT_CONF, adx_len: int = ADX_LEN,
            atr_len: int = ATR_LEN,
            incl_lookback: int = INCL_LOOKBACK,
            incl_ema_slow: int = None) -> tuple:
    """ULTIMA barra fechada -> (alvo_vigente, direction, incl, adx). `alvo` sai
    de `alvos` (EMA 8/21, confirmacao >= 2 barras); `direction` so e nao-None
    quando o alvo MUDOU nesta barra — a inversao e a unica entrada do detector.
    O portao (`passa`) NAO e aplicado aqui: quem entra decide isso.

    MESMA conta de `shadow/kis_regime.avaliar`, agora na bancada e com os
    periodos como PARAMETRO. Motivo (adendo de 23/08): o cron do shadow foi
    desativado, e a maquina oficial (v10) nao pode importar logica de um
    apendice aposentado. O shadow fica intocado com a copia dele; a igualdade
    das duas contas e travada por teste, nao por confianca. O warmup segue a
    EMA longa (`WARMUP == EMA_SLOW` no keepitsimple), entao acompanha o
    parametro em vez de ficar preso em 21.

    `incl_ema_slow` DESACOPLA a EMA da RAMPA da EMA lenta do PAR. None (default)
    devolve o comportamento de hoje — rampa na propria `ema_slow` —, entao
    nenhuma chamada existente muda de resultado. Quem passa explicito e uma
    ficha de par longo: `backtest/kis_horizonte.py` chama `inclinacoes` com os
    DEFAULTS em toda celula da grade, inclusive nas de 34/89, porque o portao
    validado em dado virgem (limiar 0.02 / ADX 11) le a EMA21 SEMPRE. Ler a
    rampa na EMA89 num setup 34/89 mediria um portao que nunca foi validado, e
    o numero do eixo 1 deixaria de ser reproduzivel pela ficha."""
    closes = [c["close"] for c in candles]
    alvo = alvos(closes, ema_fast, ema_slow, confirmacao)
    i = len(closes) - 1
    if i < max(ema_slow, 1):
        return 0, None, None, None
    d = None
    if alvo[i] != alvo[i - 1] and alvo[i] != 0:
        d = "LONG" if alvo[i] > 0 else "SHORT"
    rampa = ema_slow if incl_ema_slow is None else int(incl_ema_slow)
    return (alvo[i], d,
            inclinacoes(candles, closes, atr_len, rampa, incl_lookback)[i],
            _adx(candles, adx_len)[i])


def celula(trades: list, k: int) -> list:
    """Trades da celula k no formato que `kis_trail.metricas` le: (bar_ts,
    sequencia indexavel de retornos), lida com indice 0. A SAIDA nao muda entre
    celulas — o filtro so decide quem entra —, entao o retorno de cada trade e o
    mesmo em toda celula e a celula e um subconjunto do controle."""
    return [(ts, (r,)) for ts, r, mask in trades if mask >> k & 1]


def run(store: str, symbols: list, start_iso: str, end_iso: str, tf: str) -> dict:
    """Por simbolo: entradas do kis_extremos -> mede -> guarda (bar_ts, retorno
    do primario, mascara de 17 bits). Entradas CANDIDATAS identicas as do
    kis_trail (mesmas duas exclusoes, mesma ordem); so a filiacao e nova.
    Imports pesados sao lazy p/ os testes coletarem no sandbox."""
    from backtest.candle_store import connect, read_candles          # noqa: E402
    from backtest.excursion import _iso_to_ms, measure_event         # noqa: E402
    conn = connect(store)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)
    por_sym, excl_borda, excl_hold, rss = {}, 0, 0, _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=start_ms, end_ms=end_ms)
        closes = [c["close"] for c in cndl]
        incl, adx = inclinacoes(cndl, closes), _adx(cndl)
        pos = {c["ts"]: i for i, c in enumerate(cndl)}
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
            i = pos[ev["bar_ts"]]
            ap = m["atr"] / m["entry"]
            # arme=0 e a saida do primario. MESMA chamada, MESMOS argumentos E
            # MESMA ORDEM DE ASSOCIACAO do kis_trail: `x * ap * 1e4` associa
            # diferente de `x * atr / entry * 1e4` e as duas versoes divergiram
            # em 4.5e-13 bps na bancada. A validacao pede max|dif| = 0 exato,
            # entao a conta e copiada, nao reescrita.
            ret = (trail_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                              m["fwd_bar"], ev["hold"], 0, 0)
                   * ap * 1e4 - COST_BPS)
            trades.append((ev["bar_ts"], ret,
                           mascara(ev["direction"], incl[i], adx[i])))
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
    """Uma linha por (symbol, limiar, adx_min), mais o agregado UNIVERSO (todos
    os trades num pote so, ordenados por data de entrada — a curva do universo e
    a da carteira, nao a media das curvas por simbolo)."""
    todos = sorted((t for ts in por_sym.values() for t in ts),
                   key=lambda t: t[0])
    out = []
    for sym, trades in list(por_sym.items()) + [("UNIVERSO", todos)]:
        ms = [metricas(celula(trades, k), 0) for k in range(len(GRADE))]
        ctl = ms[0]             # GRADE[0] e a celula de controle (sem filtro)
        for (lim, amin), m in zip(GRADE, ms):
            # Delta e fracao mantida saem JUNTOS e se leem JUNTOS: com n
            # diferente do controle o delta carrega custo nao pago junto com o
            # efeito do filtro, e os dois so se separam olhando quanto sobrou
            # (ver "COMO LER O DELTA" no topo). Controle contra si mesmo da 0
            # por definicao, e mantem 100% por definicao.
            out.append(dict(
                symbol=sym, limiar=lim, adx_min=amin, **m,
                pct_sinais_mantidos=(100.0 * m["n_trades"] / ctl["n_trades"]
                                     if ctl["n_trades"] else 0.0),
                delta_1a_vs_controle=(m["ret_1a_metade_bps"]
                                      - ctl["ret_1a_metade_bps"]),
                delta_2a_vs_controle=(m["ret_2a_metade_bps"]
                                      - ctl["ret_2a_metade_bps"])))
    return out


def valida_controle(store: str, symbols: list, start_iso: str, end_iso: str,
                    tf: str) -> tuple:
    """Prova dura de que a celula de controle E o arme=0 do kis_trail: roda as
    DUAS varreduras sobre o mesmo store e devolve (max|dif| em bps, n_controle,
    n_trail). Divergencia de populacao (n ou bar_ts) devolve inf — nao ha
    "quase igual" aqui."""
    from backtest.kis_trail import run as trail_run                  # noqa: E402
    meu = run(store, symbols, start_iso, end_iso, tf)
    dele = trail_run(store, symbols, start_iso, end_iso, tf)
    dif, n_a, n_b = 0.0, 0, 0
    for sym, trades in meu["por_sym"].items():
        a = celula(trades, 0)
        b = [(ts, c[0]) for ts, c in dele["por_sym"].get(sym, [])]
        n_a, n_b = n_a + len(a), n_b + len(b)
        if len(a) != len(b) or any(x[0] != y[0] for x, y in zip(a, b)):
            return float("inf"), n_a, n_b
        dif = max([dif] + [abs(x[1][0] - y[1]) for x, y in zip(a, b)])
    return dif, n_a, n_b


CAMPOS = ["symbol", "limiar", "adx_min", "n_trades", "pct_sinais_mantidos",
          "acerto_pct", "ret_1a_metade_bps", "ret_2a_metade_bps",
          "ret_total_bps", "delta_1a_vs_controle", "delta_2a_vs_controle",
          "dd_max_bps", "trimestres_pos", "trimestres_total"]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Varredura EXPLORATORIA de filtro de regime no kis_extremos")
    ap.add_argument("--tf", choices=["15m", "1h", "4h"], default="4h")
    ap.add_argument("--start", default="2024-05-22")
    ap.add_argument("--end", default="2026-06-21")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    ap.add_argument("--out", default=None,
                    help="default: logs/sweep_regime_<data>.csv")
    ap.add_argument("--valida-controle", action="store_true",
                    help="roda tambem o kis_trail e prova max|dif| = 0")
    args = ap.parse_args()
    from backtest.sweep import TIER1                                 # noqa: E402
    symbols = args.symbols if args.symbols else TIER1
    print(f"[kis_regime] tf={args.tf} janela {args.start}->{args.end} "
          f"symbols={len(symbols)} celulas={len(GRADE)}", file=sys.stderr)

    if args.valida_controle:
        d, n_a, n_b = valida_controle(args.store, symbols, args.start,
                                      args.end, args.tf)
        print(f"[valida-controle] n_controle={n_a} n_trail(arme=0)={n_b} "
              f"max|dif| = {d:.2e} bps", file=sys.stderr)
        if d != 0.0:
            print("[valida-controle] FALHOU: o controle NAO reproduz o arme=0.",
                  file=sys.stderr)
            return 1

    r = run(args.store, symbols, args.start, args.end, args.tf)
    rows = linhas(r["por_sym"])

    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"sweep_regime_{datetime.now(timezone.utc):%Y%m%d}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for row in rows:
            w.writerow({c: (f"{row[c]:.2f}" if c == "limiar" else
                            f"{row[c]:.1f}" if isinstance(row[c], float)
                            else row[c]) for c in CAMPOS})

    n_tot = sum(len(t) for t in r["por_sym"].values())
    print(f"\n===== VARREDURA kis_extremos + FILTRO DE REGIME ({r['tf']}) =====")
    print(f"janela {r['start']} -> {r['end']} | {len(r['por_sym'])} simbolos | "
          f"sinais candidatos: {n_tot} | excluidas_borda: {r['excl_borda']} | "
          f"excluidas_hold: {r['excl_hold']}")
    print(f"grade: limiar {list(LIMIARES)} x adx_min {list(ADX_MINS)} + controle "
          f"(sem filtro) = {len(GRADE)} celulas | custo {COST_BPS:.0f} bps")
    print(f"RSS de pico: {r['rss_mib']:.0f} MiB (teto {RSS_CEILING_MIB:.0f})")
    print(f"CSV: {out}")

    print("\n===== RESUMO — celulas que passam no criterio de 20/08 =====")
    print(f"(1a metade > 0 E 2a metade > 0 E trimestres_pos/total >= 0.5 E "
          f"n >= {MIN_TRADES}; drawdown vai REPORTADO, e decisao do Marcelo)")
    print(f"{'symbol':>10} | {'limiar':>6} | {'adx':>4} | {'n':>5} | "
          f"{'mantid':>6} | {'acerto':>6} | {'1a(bps)':>9} | {'2a(bps)':>9} | "
          f"{'tot(bps)':>9} | {'d1a_ctl':>9} | {'d2a_ctl':>9} | {'ddmax':>8} | "
          f"{'trim':>7}")
    passou = 0
    for row in rows:
        if not aprovada(row):
            continue
        passou += 1
        print(f"{row['symbol']:>10} | {row['limiar']:>6.2f} | "
              f"{row['adx_min']:>4} | {row['n_trades']:>5} | "
              f"{row['pct_sinais_mantidos']:>5.1f}% | "
              f"{row['acerto_pct']:>5.1f}% | "
              f"{row['ret_1a_metade_bps']:>9.1f} | "
              f"{row['ret_2a_metade_bps']:>9.1f} | "
              f"{row['ret_total_bps']:>9.1f} | "
              f"{row['delta_1a_vs_controle']:>+9.1f} | "
              f"{row['delta_2a_vs_controle']:>+9.1f} | "
              f"{row['dd_max_bps']:>8.1f} | "
              f"{row['trimestres_pos']:>3}/{row['trimestres_total']:<3}")
    if not passou:
        print("  (nenhuma celula passou — ver o CSV completo para a proxima "
              "variacao a testar)")
    print(f"\ncelulas aprovadas: {passou} de {len(rows)} avaliadas "
          f"(inclui a linha UNIVERSO). EXPLORATORIO: nada aqui promove nada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
