# backtest/kis_horizonte.py — Setup Atirador v9 (BANCADA) — EIXO 1: HORIZONTE
# Varredura EXPLORATORIA do PAR DE EMAs do detector kis_extremos. NAO toca
# runtime, NAO altera detector algum, NAO entra na lista branca da trava de
# superficie: nenhum consumidor de producao pode importar deste modulo, e
# tests/test_trava_superficie_kis.py reprova quem tentar. Nenhuma celula aqui e
# promovivel por si.
#
# HIPOTESE (briefing): o DONCHIAN-A (canal de 20 dias) capturou a alta de
# 19-22/08 com +62R no lado LONG; o KIS 8/21 (memoria ~3 dias) foi picotado
# DENTRO do mesmo movimento e perdeu no hold-out. Se a diferenca for ESCALA DE
# TEMPO e nao indicador, pares de EMA mais longos devem melhorar — e o lugar
# onde isso tem que aparecer e a 2a METADE, nao o total.
#
# O QUE MUDA ENTRE CELULAS — e por que isso NAO e o kis_regime. La as 16 celulas
# compartilhavam UMA populacao de entradas candidatas (8/21 fixo) e o filtro so
# decidia quem entrava; o retorno de cada trade era um so. Aqui o par de EMAs
# muda o DETECTOR: cada par tem alvo proprio, entradas proprias, `hold` proprio
# e retornos proprios. Duas consequencias que nao se pode esquecer ao ler o CSV:
#   * `pct_sinais_mantidos` NAO e mais "fracao do controle que sobreviveu ao
#     filtro". Entre pares DIFERENTES e a razao de dois censos independentes: um
#     par longo com 30% nao "cortou 70% dos sinais do controle", ele produziu um
#     terco do numero de sinais. Dentro do MESMO par, com portao ligado, ai sim
#     volta a ser fracao mantida no sentido do kis_regime.
#   * O aviso do kis_regime sobre custo nao pago continua valendo e piora: menos
#     trades = menos 6 bps pagos. Delta alto com fracao baixa segue sendo
#     aritmetica, nao sinal.
#
# PORTAO FIXO, NAO VARIAVEL — decisao do briefing, documentada aqui porque e o
# ponto mais facil de "corrigir" por engano depois: a inclinacao do portao le a
# EMA21 SEMPRE, em toda celula, inclusive nas de par 34/89, 55/144 e 89/233. Nao e
# a EMA lenta do par. Motivo: (limiar 0.02, adx_min 11) sobre a EMA21 e o
# componente validado em dado virgem (+3.393 bps) e entra EXATAMENTE como foi
# validado. Trocar a EMA de referencia junto com o par confundiria dois efeitos
# num numero so — e a varredura ficaria sem resposta para a pergunta que ela faz.
#
# WARMUP POR CELULA = EMA LENTA DO PAR (21, 89, 144, 233). As `slow` primeiras
# barras de cada simbolo sao descartadas, na mesma forma do `max(WARMUP, 1)` do
# keepitsimple. Pares longos perdem mais warmup E invertem menos: geram menos
# sinais por construcao. Isso e ESPERADO, nao e bug, e por isso o n de CADA
# celula sai no CSV e no resumo.
#
# ALERTA DE PODER (grade de 25/08 estendida): em 89/233 sao 233 barras de 4h —
# ~39 dias — descartadas POR SIMBOLO, e o n cai para a casa de 15-20 trades por
# token, ABAIXO do MIN_TRADES do criterio. Celula com mediana de trades por
# token abaixo do MIN_TRADES sai marcada `amos` = INCONCLUSIVA POR AMOSTRA, e
# NAO reprovada: o criterio de 20/08 nao se aplica onde a amostra nao existe.
# Confundir "nao ha evidencia" com "ha evidencia contraria" e o erro que essa
# marca existe para evitar.
#
# REUSO (e o que ele custa): o alvo por barra vem de `kis_regime.alvos`, que ja
# e `_alvo_extremos(states(closes, fast, slow), conf)` — a composicao com os
# periodos EXPLICITOS que a trava de 22/08 autoriza ("argumento OPCIONAL com
# default identico ao de hoje"). Reescrever EMA/estado/alvo aqui seria 40 linhas
# de aritmetica de float duplicada que pode divergir em silencio do original; a
# validacao do PASSO 4 pede max|dif| = 0.00e+00 EXATO, e a unica forma barata de
# garantir isso e nao ter uma segunda conta. O que e proprio deste modulo e a
# extracao de entradas: `extremos_entries` prende WARMUP em 21 e o par em 8/21,
# entao a versao parametrizada por (fast, slow) mora aqui.
#
# MEMORIA: mesma disciplina do kis_regime e do kis_trail (a VM tem 956 MiB
# TOTAIS com o cron na mesma maquina). So escalares sobrevivem — 2 floats + 1
# inteiro de 2 bits por trade, por par. O teto de RSS e checado a cada par de
# cada simbolo e ABORTA antes de estourar.
#
# CRITERIO (ledger de 20/08): 1a metade > 0 E 2a metade > 0 E maioria dos
# trimestres positivos E n >= 30. Sem blockP2.5, sem p_shift — nao sao o
# criterio e nao aparecem nesta varredura.
# Uso: .venv/bin/python -m backtest.kis_horizonte --start 2024-05-22 --end 2026-06-21
from __future__ import annotations
import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import EXT_CAP, EXT_CONF, _adx      # noqa: E402
from backtest.kis_regime import alvos, inclinacoes, passa      # noqa: E402
from backtest.kis_trail import (COST_BPS, MIN_TRADES,          # noqa: E402
                                RSS_CEILING_MIB, _rss_mib, aprovada,
                                metricas, trail_exit)

# GRADE FECHADA, definida no briefing e NAO ajustavel depois do fato: 4 pares x
# portao desligado/ligado = 8 celulas. (8, 21) e o par de hoje, entao
# ((8, 21), False) e o CONTROLE — mesma populacao e mesmos retornos do arme=0 do
# kis_trail, por construcao.
# ESTENDIDA (25/08): o melhor da primeira grade ficou na BORDA — 34/89 com
# portao deu 2a metade +54.2 bps/trade contra -12.5 do controle, 1a plana. Borda
# vencendo significa otimo possivelmente FORA da grade: a janela anda para
# 55/144 e 89/233, 34/89 fica de ancora, e 13/34 e 21/55 saem (ja medidos, e
# 13/34 foi PIOR que o controle na 2a metade).
PARES = ((8, 21), (34, 89), (55, 144), (89, 233))
PORTAO = (False, True)
GRADE = [(p, g) for p in PARES for g in PORTAO]
CONTROLE = (PARES[0], False)
# O portao validado em dado virgem, congelado (ver cabecalho): inclinacao da
# EMA21 em 5 barras >= 0.02 ATR/barra a favor E ADX(13) >= 11.
PORTAO_LIMIAR = 0.02
PORTAO_ADX_MIN = 11


def entradas(candles: list, ema_fast: int, ema_slow: int,
             confirmacao: int = EXT_CONF) -> list:
    """`extremos_entries` com o par de EMAs PARAMETRIZADO. Mesma regra, mesma
    ordem, mesmo `hold`: entrada em i <=> alvo[i] != alvo[i-1] e alvo[i] != 0;
    `hold` = barras ate a PROXIMA entrada (ate a ultima barra se nao houver),
    piso 1 e teto EXT_CAP. A UNICA diferenca e o warmup, que segue a EMA lenta
    do par em vez da constante WARMUP=21 do keepitsimple — para (8, 21) os dois
    dao 21 e a saida e identica, o que e o que a validacao do PASSO 4 exige.

    O payload descritivo (`bb_width_rel`, `forca_subindo`, `estado_origem`) NAO
    sai daqui: e autopsia, nao entra em decisao nenhuma, e calcula-lo por par
    multiplicaria por 4 o custo da varredura sem mudar uma linha do CSV."""
    tss = [c["ts"] for c in candles]
    alvo = alvos([c["close"] for c in candles], ema_fast, ema_slow, confirmacao)
    idx = [i for i in range(max(ema_slow, 1), len(alvo))
           if alvo[i] != alvo[i - 1] and alvo[i] != 0]
    out = []
    for k, i in enumerate(idx):
        bruto = (idx[k + 1] if k + 1 < len(idx) else len(alvo) - 1) - i
        out.append({"bar_ts": tss[i],
                    "direction": "LONG" if alvo[i] > 0 else "SHORT",
                    "hold": min(max(bruto, 1), EXT_CAP)})
    return out


def mascara(direction: str, incl, adx) -> int:
    """Filiacao do sinal nas DUAS celulas do seu par, em 2 bits: bit 0 = portao
    desligado (vale SEMPRE 1), bit 1 = portao ligado. O portao e o `passa` do
    kis_regime com o par de valores congelado — mesma funcao, nao uma copia,
    para que a celula ((8,21), True) reproduza a celula 0.02/11 de la sem
    "quase igual"."""
    return 1 | (2 if passa(direction, incl, adx,
                           PORTAO_LIMIAR, PORTAO_ADX_MIN) else 0)


def celula(trades: list, bit: int) -> list:
    """Trades da celula no formato que `kis_trail.metricas` le: (bar_ts,
    sequencia indexavel de retornos), lida com indice 0."""
    return [(ts, (r,)) for ts, r, mask in trades if mask >> bit & 1]


def run(store: str, symbols: list, start_iso: str, end_iso: str, tf: str) -> dict:
    """Por simbolo: le as velas UMA vez, calcula inclinacao/ADX UMA vez (o
    portao nao depende do par) e roda os 4 pares sobre elas, guardando por par
    (bar_ts, retorno, mascara de 2 bits). `measure_event` roda por par porque as
    entradas mudam com o par — e o custo real de varrer horizonte.
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
        por_par = {}
        for par in PARES:
            trades = []
            for ev in entradas(cndl, par[0], par[1]):
                m = measure_event(conn, sym, ev["bar_ts"], ev["direction"],
                                  tf=tf, path_cap=EXT_CAP)
                if m is None:           # borda direita / atr<=0 — regra travada
                    excl_borda += 1
                    continue
                if ev["hold"] > len(m["fwd_bar"]):
                    excl_hold += 1      # hold nao cabe no futuro disponivel
                    continue
                i = pos[ev["bar_ts"]]
                ap = m["atr"] / m["entry"]
                # arme=0 e a saida do primario. MESMA chamada, MESMOS argumentos
                # E MESMA ORDEM DE ASSOCIACAO do kis_trail/kis_regime: `x * ap *
                # 1e4` associa diferente de `x * atr / entry * 1e4` e as duas
                # versoes divergiram em 4.5e-13 bps na bancada. A validacao pede
                # max|dif| = 0 exato, entao a conta e copiada, nao reescrita.
                ret = (trail_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                                  m["fwd_bar"], ev["hold"], 0, 0)
                       * ap * 1e4 - COST_BPS)
                trades.append((ev["bar_ts"], ret,
                               mascara(ev["direction"], incl[i], adx[i])))
            por_par[par] = trades
            rss = _rss_mib()
            if rss > RSS_CEILING_MIB:
                conn.close()
                raise MemoryError(
                    f"RSS {rss:.0f} MiB > teto {RSS_CEILING_MIB:.0f} MiB apos "
                    f"{sym} par {par}; PARANDO antes de estourar a VM.")
        por_sym[sym] = por_par
    conn.close()
    return {"por_sym": por_sym, "excl_borda": excl_borda,
            "excl_hold": excl_hold, "rss_mib": rss, "tf": tf,
            "start": start_iso, "end": end_iso}


def linhas(por_sym: dict) -> list:
    """Uma linha por (symbol, par, portao), mais o agregado UNIVERSO (todos os
    trades do par num pote so, ordenados por data de entrada — a curva do
    universo e a da carteira, nao a media das curvas por simbolo). O controle de
    cada simbolo e o SEU proprio ((8,21), False), nunca o do universo."""
    universo = {p: sorted((t for d in por_sym.values() for t in d.get(p, [])),
                          key=lambda t: t[0]) for p in PARES}
    out = []
    for sym, por_par in list(por_sym.items()) + [("UNIVERSO", universo)]:
        ctl = metricas(celula(por_par[CONTROLE[0]], 0), 0)
        for par, portao in GRADE:
            m = metricas(celula(por_par[par], 1 if portao else 0), 0)
            # Delta e fracao mantida saem JUNTOS e se leem JUNTOS (ver "O QUE
            # MUDA ENTRE CELULAS" no topo): com n diferente do controle o delta
            # carrega custo nao pago junto com o efeito do par. Controle contra
            # si mesmo da 0 e mantem 100%, por definicao.
            out.append(dict(
                symbol=sym, ema_fast=par[0], ema_slow=par[1],
                portao=int(portao), **m,
                pct_sinais_mantidos=(100.0 * m["n_trades"] / ctl["n_trades"]
                                     if ctl["n_trades"] else 0.0),
                delta_1a_vs_controle=(m["ret_1a_metade_bps"]
                                      - ctl["ret_1a_metade_bps"]),
                delta_2a_vs_controle=(m["ret_2a_metade_bps"]
                                      - ctl["ret_2a_metade_bps"])))
    return out


def valida_portao(store: str, symbols: list, start_iso: str, end_iso: str,
                  tf: str) -> tuple:
    """PASSO 4 do briefing: prova dura de que a celula ((8,21), True) E a celula
    limiar=0.02 / adx_min=11 do kis_regime. Roda as DUAS varreduras sobre o
    mesmo store e devolve (max|dif| em bps, n_aqui, n_la). Divergencia de
    populacao (n ou bar_ts) devolve inf — nao ha "quase igual" aqui, e se der
    diferente o modulo NAO se ajusta pra bater: reporta-se a divergencia."""
    from backtest.kis_regime import GRADE as GRADE_REGIME          # noqa: E402
    from backtest.kis_regime import celula as celula_regime        # noqa: E402
    from backtest.kis_regime import run as regime_run              # noqa: E402
    k = GRADE_REGIME.index((PORTAO_LIMIAR, PORTAO_ADX_MIN))
    meu = run(store, symbols, start_iso, end_iso, tf)
    dele = regime_run(store, symbols, start_iso, end_iso, tf)
    dif, n_a, n_b = 0.0, 0, 0
    for sym, por_par in meu["por_sym"].items():
        a = celula(por_par[CONTROLE[0]], 1)
        b = celula_regime(dele["por_sym"].get(sym, []), k)
        n_a, n_b = n_a + len(a), n_b + len(b)
        if len(a) != len(b) or any(x[0] != y[0] for x, y in zip(a, b)):
            return float("inf"), n_a, n_b
        dif = max([dif] + [abs(x[1][0] - y[1][0]) for x, y in zip(a, b)])
    return dif, n_a, n_b


def inconclusivas(rows: list) -> set:
    """Celulas (ema_fast, ema_slow, portao) cuja MEDIANA de trades POR TOKEN
    fica abaixo do MIN_TRADES. Le as linhas por simbolo e NUNCA a do UNIVERSO:
    somar 40 tokens de 18 trades da 720 e esconde justamente o que isto mede.
    O n cai por CONSTRUCAO do warmup, nao por merito da celula — dai `amos` em
    vez do item (c) do criterio: sem amostra nao ha o que aprovar NEM o que
    reprovar (ver ALERTA DE PODER no cabecalho)."""
    por_celula: dict = {}
    for r in rows:
        if r["symbol"] != "UNIVERSO":
            por_celula.setdefault((r["ema_fast"], r["ema_slow"],
                                   r["portao"]), []).append(r["n_trades"])
    return {c for c, ns in por_celula.items() if ns and median(ns) < MIN_TRADES}


def _marca(row: dict, inconc: set) -> str:
    """`amos` vence `ok`: sem amostra por token nada e aprovado nem reprovado,
    seja qual for o sinal do que a celula mediu."""
    if (row["ema_fast"], row["ema_slow"], row["portao"]) in inconc:
        return "amos"
    return "ok" if aprovada(row) else "  "


CAMPOS = ["symbol", "ema_fast", "ema_slow", "portao", "n_trades",
          "pct_sinais_mantidos", "acerto_pct", "ret_1a_metade_bps",
          "ret_2a_metade_bps", "ret_total_bps", "delta_1a_vs_controle",
          "delta_2a_vs_controle", "dd_max_bps", "trimestres_pos",
          "trimestres_total"]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Varredura EXPLORATORIA de HORIZONTE (par de EMAs) do "
                    "detector kis_extremos")
    ap.add_argument("--tf", choices=["15m", "1h", "4h"], default="4h")
    ap.add_argument("--start", default="2024-05-22")
    ap.add_argument("--end", default="2026-06-21")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None, help="default: TIER1")
    ap.add_argument("--out", default=None,
                    help="default: logs/sweep_horizonte_<data>.csv")
    ap.add_argument("--valida-portao", action="store_true",
                    help="roda tambem o kis_regime e prova max|dif| = 0")
    args = ap.parse_args()
    from backtest.sweep import TIER1                                 # noqa: E402
    symbols = args.symbols if args.symbols else TIER1
    print(f"[kis_horizonte] tf={args.tf} janela {args.start}->{args.end} "
          f"symbols={len(symbols)} celulas={len(GRADE)}", file=sys.stderr)

    if args.valida_portao:
        d, n_a, n_b = valida_portao(args.store, symbols, args.start,
                                    args.end, args.tf)
        print(f"[valida-portao] n((8,21),True)={n_a} "
              f"n(kis_regime 0.02/11)={n_b} max|dif| = {d:.2e} bps",
              file=sys.stderr)
        if d != 0.0:
            print("[valida-portao] FALHOU: a celula ((8,21), True) NAO "
                  "reproduz a 0.02/11 do kis_regime. NAO ajustar este modulo "
                  "pra bater — reportar a divergencia.", file=sys.stderr)
            return 1

    r = run(args.store, symbols, args.start, args.end, args.tf)
    rows = linhas(r["por_sym"])

    out = Path(args.out) if args.out else (
        ROOT / "logs" / f"sweep_horizonte_{datetime.now(timezone.utc):%Y%m%d}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for row in rows:
            w.writerow({c: (f"{row[c]:.1f}" if isinstance(row[c], float)
                            else row[c]) for c in CAMPOS})

    n_tot = sum(len(t) for d in r["por_sym"].values() for t in d.values())
    print(f"\n===== VARREDURA kis_extremos + HORIZONTE (par de EMAs) "
          f"({r['tf']}) =====")
    print(f"janela {r['start']} -> {r['end']} | {len(r['por_sym'])} simbolos | "
          f"sinais candidatos (soma dos 4 pares): {n_tot} | "
          f"excluidas_borda: {r['excl_borda']} | "
          f"excluidas_hold: {r['excl_hold']}")
    print(f"grade: pares {list(PARES)} x portao {list(PORTAO)} = {len(GRADE)} "
          f"celulas | controle = {CONTROLE} | custo {COST_BPS:.0f} bps")
    print(f"portao FIXO (nao varia com o par): inclinacao da EMA21 em 5 barras "
          f">= {PORTAO_LIMIAR} ATR/barra a favor E ADX(13) >= {PORTAO_ADX_MIN}")
    print(f"warmup por celula = EMA lenta do par "
          f"{[p[1] for p in PARES]} — par longo gera menos sinal por construcao")
    print(f"RSS de pico: {r['rss_mib']:.0f} MiB (teto {RSS_CEILING_MIB:.0f})")
    print(f"CSV: {out}")

    print("\n===== RESUMO — TODAS as celulas, por simbolo e UNIVERSO =====")
    inconc = inconclusivas(rows)
    print(f"(ok = criterio de 20/08: 1a > 0 E 2a > 0 E trimestres_pos/total "
          f">= 0.5 E n >= {MIN_TRADES}; drawdown vai REPORTADO, e decisao do "
          f"Marcelo. A hipotese se le na coluna d2a_ctl, nao no total.)")
    print(f"(amos = INCONCLUSIVA POR AMOSTRA: mediana de trades POR TOKEN < "
          f"{MIN_TRADES}. NAO e reprovacao — e ausencia de amostra, pelo "
          f"warmup da EMA lenta. O n por token esta nas linhas por simbolo; a "
          f"do UNIVERSO soma tokens e NAO serve para essa leitura.)")
    print(f"{'symbol':>10} | {'par':>7} | {'ptao':>4} | {'n':>5} | "
          f"{'mantid':>6} | {'acerto':>6} | {'1a(bps)':>9} | {'2a(bps)':>9} | "
          f"{'tot(bps)':>9} | {'d1a_ctl':>9} | {'d2a_ctl':>9} | {'ddmax':>8} | "
          f"{'trim':>7} | ok")
    for row in rows:
        par = "{}/{}".format(row["ema_fast"], row["ema_slow"])
        print(f"{row['symbol']:>10} | {par:>7} | "
              f"{row['portao']:>4} | {row['n_trades']:>5} | "
              f"{row['pct_sinais_mantidos']:>5.1f}% | "
              f"{row['acerto_pct']:>5.1f}% | "
              f"{row['ret_1a_metade_bps']:>9.1f} | "
              f"{row['ret_2a_metade_bps']:>9.1f} | "
              f"{row['ret_total_bps']:>9.1f} | "
              f"{row['delta_1a_vs_controle']:>+9.1f} | "
              f"{row['delta_2a_vs_controle']:>+9.1f} | "
              f"{row['dd_max_bps']:>8.1f} | "
              f"{row['trimestres_pos']:>3}/{row['trimestres_total']:<3} | "
              f"{_marca(row, inconc)}")
    apr = sum(1 for r_ in rows if aprovada(r_) and _marca(r_, inconc) == 'ok')
    print(f"\ncelulas aprovadas: {apr} de {len(rows)} avaliadas (inclui as "
          f"linhas UNIVERSO); {len(inconc)} celulas INCONCLUSIVAS POR AMOSTRA: "
          f"{sorted(inconc) if inconc else '(nenhuma)'}. EXPLORATORIO: nada "
          f"aqui promove nada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
