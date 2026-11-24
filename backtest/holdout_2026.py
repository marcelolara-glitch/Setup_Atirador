# backtest/holdout_2026.py — Setup Atirador (SANDBOX, camada 2) — tres setups
# no periodo 31/08 -> 14/11/2026. Briefing v2 de 22/11/2026.
#
# O instrumento SO MEDE E DECOMPOE. Nao emite veredito, nao aplica criterio de
# aceite. Nenhum parametro muda em relacao ao sandbox.
#
# SETUPS (escolhidos por Marcelo), tf 4h, universo SYMBOLS_KIS_3489 (65):
#   KIS_E0    KIS 34/89 + portao (EMA21 fixa, 0,02, ADX 11), confirmacao 2,
#             saida so na inversao           = kis_saida.run, celula E0
#   KIS_E1-3  o mesmo + stop fixo 3 x ATR14  = kis_saida.run, celula E1-3
#   AIO_G1    AIO graduado G1 (volume >= 1,2x e ribbon 13/34), stop 2 x ATR14,
#             saida na virada contraria bruta = aio_graduado, G1, coluna S2
#
# REUSADO, nao reescrito: sinal KIS = `kis_horizonte.entradas`; portao =
# `kis_regime.inclinacoes` + `keepitsimple._adx` + `kis_regime.passa`; sinal AIO
# = `aio_graduado.serie_sinal` / `passa` / `holds`; saidas = `kis_trail.
# trail_exit` (sem stop) e `kis_saida.stop_exit` (stop fixo, fill min(stop,
# open)); mesma conta de bps (saida em ATR x atr/entry x 1e4 - 6).
#
# UNICA PECA NOVA: `medir_sem_borda`. `measure_event` / `kis_tf.medir` devolvem
# None quando ha menos de 48 barras a frente (borda), e o sandbox descarta o
# trade. Nesta rodada NAO ha exclusao por borda (briefing 2.4): o caminho e
# medido ate a ultima barra visivel. E a mesma conta do `kis_tf.medir` sem a
# linha da borda; o teste de paridade garante que, onde o sandbox mede, os
# numeros saem identicos.
#
# PERIODO: so trades com barra de ENTRADA de 2026-08-31 16:00 a 2026-11-14
# 12:00 UTC, inclusive nas duas pontas. Posicao aberta antes nao conta.
# AQUECIMENTO: passada continua desde o primeiro candle do store.
# FIM: `--end` (exclusivo, default 2026-11-14 16:00 UTC) vira TEMP VIEW via
# `kis_saida.conecta_com_teto`. Trade ainda aberto na ultima barra visivel e
# marcado no fechamento dela (`marcado_fim` = 1), com o stop valendo ate la; se
# o stop dispara antes, sai no stop (`marcado_fim` = 0). Entrada na propria
# ultima barra: hold 0, retorno bruto 0 (paga so o custo).
# HOLD > 256 barras: truncado em 256 como no sandbox (contado; nao e marcado
# no fim).
# CUSTO: duas colunas, 10 bps e 6 bps por trade (ret10 = ret6 - 4).
# VM 956 MiB: velas de UM simbolo por vez; teto de RSS do kis_trail; guarda de
# horario do kis_saida.
#
# Uso (VM): python -m backtest.holdout_2026 --store <candles_v9.db>
from __future__ import annotations
import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.aio_graduado import STOP_K, _Log, holds, serie_sinal   # noqa: E402
from backtest.aio_graduado import passa as passa_aio                  # noqa: E402
from backtest.keepitsimple import EXT_CAP, _adx                       # noqa: E402
from backtest.kis_horizonte import (PORTAO_ADX_MIN, PORTAO_LIMIAR,    # noqa: E402
                                    entradas)
from backtest.kis_regime import inclinacoes, passa                    # noqa: E402
from backtest.kis_saida import (BAR_4H_MS, COST_FWD_BPS,              # noqa: E402
                                _guarda_relogio, _iso, _ms,
                                conecta_com_teto, stop_exit)
from backtest.kis_tf import CTX                                       # noqa: E402
from backtest.kis_trail import (COST_BPS, _rss_mib, add_teto_rss,  # noqa: E402
                                define_teto_rss, teto_rss, trail_exit)
from backtest.vwma_knn import TOP_FRAC, concentracao                  # noqa: E402

INI_ISO = "2026-08-31 16:00"       # 1a barra de entrada (inclusiva)
FIM_ISO = "2026-11-14 12:00"       # ultima barra de entrada (inclusiva)
END_ISO = "2026-11-14 16:00"       # --end default (exclusivo)
JANELA = (_ms(INI_ISO), _ms(FIM_ISO))
PAR = (34, 89)
KIS_STOP_K = 3
AIO_CELULA = "G1"
SETUPS = ("KIS_E0", "KIS_E1-3", "AIO_G1")
DELTA_10 = COST_FWD_BPS - COST_BPS  # 4 bps: ret10 = ret6 - DELTA_10


def na_janela(ts: int, janela=JANELA) -> bool:
    return janela is None or janela[0] <= ts <= janela[1]


# ------------------------------------------------------------ medicao -------
def medir_sem_borda(cndl: list, i: int, direction: str,
                    bar_ms: int = BAR_4H_MS, cap: int = EXT_CAP,
                    periodo_atr: int = 14, mult_atr: float = 1.0):
    """`kis_tf.medir` SEM a exclusao de borda: caminho da barra i+1 ate a
    ultima visivel (teto `cap`). None so se ATR <= 0. fwd_bar pode ser [].
    `periodo_atr`/`mult_atr` (24/11/2026, `kis_refino.escala` com indicadores
    escalados): ATR = mult x ATR(periodo) com contexto de max(CTX, periodo+1)
    velas; o default (14, 1.0) e a conta de sempre."""
    from backtest.excursion import _wilder_atr      # tardio, como no kis_tf
    ctx = cndl[max(0, i - max(CTX, periodo_atr + 1) + 1):i + 1]
    entry, atr = ctx[-1]["close"], _wilder_atr(ctx, periodo_atr)
    if mult_atr != 1.0:
        atr *= mult_atr
    if atr <= 0:
        return None
    t0, lim = cndl[i]["ts"], cndl[i]["ts"] + cap * bar_ms
    fwd = []
    for c in cndl[i + 1:i + 1 + cap]:
        if c["ts"] > lim:
            break
        if c["ts"] >= t0 + bar_ms:
            fwd.append(c)
    sg = 1.0 if direction == "LONG" else -1.0
    if sg > 0:
        fav = [(c["high"] - entry) / atr for c in fwd]
        adv = [(entry - c["low"]) / atr for c in fwd]
    else:
        fav = [(entry - c["low"]) / atr for c in fwd]
        adv = [(c["high"] - entry) / atr for c in fwd]
    return {"atr": atr, "entry": entry,
            "fwd_bar": [sg * (c["close"] - entry) / atr for c in fwd],
            "open_bar": [sg * (c["open"] - entry) / atr for c in fwd],
            "fav_bar": fav, "adv_bar": adv}


def sair(m: dict, hold: int, k: float) -> tuple:
    """(saida em ATR, parou_no_stop, barras efetivas). k = 0 -> sem stop
    (`trail_exit` arme 0); k > 0 -> stop fixo (`stop_exit`). hold 0 = entrada
    na ultima barra visivel: marcada no proprio close, retorno 0."""
    if hold <= 0:
        return 0.0, False, 0
    if not k:
        return trail_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                          m["fwd_bar"], hold, 0, 0), False, hold
    r, parou = stop_exit(m["fav_bar"], m["adv_bar"], m["open_bar"],
                         m["fwd_bar"], hold, k, False)
    if parou:       # barra do stop: a 1a que toca o nivel fixo (so p/ a coluna)
        hold = next(j for j in range(hold) if -m["adv_bar"][j] <= -k) + 1
    return r, parou, hold


def _linha(setup: str, ts: int, sym: str, is_long: bool, m: dict, hold: int,
           k: float, fim: bool, trunc: bool) -> dict:
    r, parou, h = sair(m, hold, k)
    ret6 = r * (m["atr"] / m["entry"]) * 1e4 - COST_BPS
    return {"setup": setup, "ts": ts, "sym": sym, "long": is_long, "hold": h,
            "marcado_fim": bool(fim and not parou), "parou_stop": parou,
            "truncado": bool(trunc and not parou), "ret6": ret6,
            "ret10": ret6 - DELTA_10}


def _hold_visivel(m: dict, hold: int, fim: bool, cont: dict):
    """Hold que cabe no caminho. Trade aberto no fim: ate a ultima barra. Trade
    com saida conhecida e caminho mais curto (buraco de ts): contado, fora."""
    n = len(m["fwd_bar"])
    if hold <= n:
        return hold
    if fim:
        return n
    cont["excl_hold"] += 1
    return None


def novo_cont() -> dict:
    return {"kis_inversoes": 0, "kis_vetadas": 0, "aio_viradas": 0,
            "aio_fora_g1": 0, "atr0": 0, "excl_hold": 0}


# ------------------------------------------------------------ setups --------
def trades_kis(cndl: list, sym: str, cont: dict, janela=JANELA) -> list:
    """KIS_E0 e KIS_E1-3. Mesmo laco do `kis_saida.run` (entradas 34/89, portao
    na barra da inversao; inversao vetada nao abre trade mas fecha o anterior,
    porque o `hold` vai ate a proxima inversao, passe ela ou nao)."""
    if not cndl:
        return []
    closes = [c["close"] for c in cndl]
    incl, adx = inclinacoes(cndl, closes), _adx(cndl)
    pos = {c["ts"]: i for i, c in enumerate(cndl)}
    evs, ult, out = entradas(cndl, PAR[0], PAR[1]), len(cndl) - 1, []
    for n_ev, ev in enumerate(evs):
        if not na_janela(ev["bar_ts"], janela):
            continue
        cont["kis_inversoes"] += 1
        i = pos[ev["bar_ts"]]
        if not passa(ev["direction"], incl[i], adx[i], PORTAO_LIMIAR,
                     PORTAO_ADX_MIN):
            cont["kis_vetadas"] += 1
            continue
        m = medir_sem_borda(cndl, i, ev["direction"])
        if m is None:
            cont["atr0"] += 1
            continue
        bruto = ult - i                       # so vale para a ultima inversao
        fim = n_ev == len(evs) - 1 and bruto <= EXT_CAP
        trunc = ev["hold"] >= EXT_CAP and not fim
        hold = _hold_visivel(m, min(ev["hold"], bruto) if fim else ev["hold"],
                             fim, cont)
        if hold is None:
            continue
        lg = ev["direction"] == "LONG"
        out.append(_linha("KIS_E0", ev["bar_ts"], sym, lg, m, hold, 0, fim,
                          trunc))
        out.append(_linha("KIS_E1-3", ev["bar_ts"], sym, lg, m, hold,
                          KIS_STOP_K, fim, trunc))
    return out


def trades_aio(cndl: list, sym: str, cont: dict, janela=JANELA) -> list:
    """AIO_G1 com stop S2. Mesmo laco do `aio_graduado.trades_simbolo` (celula
    G1: entrada na virada, saida na proxima virada contraria BRUTA)."""
    if not cndl:
        return []
    vs = serie_sinal(cndl)["viradas"]
    out = []
    for v, (hb, fim) in zip(vs, holds(vs, len(cndl) - 1)):
        if not na_janela(v["ts"], janela):
            continue
        cont["aio_viradas"] += 1
        if not passa_aio(AIO_CELULA, v):
            cont["aio_fora_g1"] += 1
            continue
        m = medir_sem_borda(cndl, v["i"], "LONG" if v["long"] else "SHORT")
        if m is None:
            cont["atr0"] += 1
            continue
        trunc = hb > EXT_CAP
        fim = fim and not trunc
        hold = _hold_visivel(m, hb if fim else min(max(hb, 1), EXT_CAP), fim,
                             cont)
        if hold is None:
            continue
        out.append(_linha("AIO_G1", v["ts"], sym, v["long"], m, hold, STOP_K,
                          fim, trunc))
    return out


# ------------------------------------------------------------ dados ---------
def cobertura(conn, symbols: list, tf: str, ini_ms: int, teto_ms: int) -> dict:
    """Por SQL: {sym: n de velas com ini <= ts < teto}. Esperado por simbolo:
    (teto - ini) / barra."""
    return {s: conn.execute(
        "SELECT COUNT(*) FROM candles WHERE symbol=? AND timeframe=? AND "
        "ts >= ? AND ts < ?", (s, tf, ini_ms, teto_ms)).fetchone()[0]
        for s in symbols}


def coleta(conn, symbols: list, tf: str, teto_ms: int, janela=JANELA) -> dict:
    """Uma passada por simbolo (velas de um simbolo por vez em memoria)."""
    from backtest.candle_store import read_candles
    trades, cont, passivo, sem, maior = [], novo_cont(), {}, [], None
    rss = _rss_mib()
    for sym in symbols:
        cndl = read_candles(conn, sym, tf, start_ms=None, end_ms=teto_ms - 1)
        if not cndl:
            sem.append(sym)
            continue
        maior = max(maior or 0, cndl[-1]["ts"])
        trades.extend(trades_kis(cndl, sym, cont, janela))
        trades.extend(trades_aio(cndl, sym, cont, janela))
        px = {c["ts"]: c["close"] for c in cndl
              if c["ts"] in (janela[0], janela[1])}
        a, b = px.get(janela[0]), px.get(janela[1])
        passivo[sym] = 1e4 * (b / a - 1) if a and b else None
        del cndl
        rss = _rss_mib()
        if rss > teto_rss():
            raise MemoryError(f"RSS {rss:.0f} MiB > teto {teto_rss():.0f} "
                              f"apos {sym}; PARANDO.")
    trades.sort(key=lambda t: (t["ts"], t["sym"], t["setup"]))
    return {"trades": trades, "cont": cont, "passivo": passivo,
            "sem_candles": sem, "maior_ts": maior, "rss": rss}


# ------------------------------------------------------------ numeros -------
def curva(rs: list) -> tuple:
    """(pico, rebaixamento maximo) da soma acumulada, na ordem dada."""
    cum = pico = dd = 0.0
    for r in rs:
        cum += r
        pico = max(pico, cum)
        dd = max(dd, pico - cum)
    return pico, dd


def resumo(tr: list) -> dict:
    """Metricas descritivas de uma lista de trades (ja ordenada por entrada)."""
    n = len(tr)
    out = {"n": n, "marcados": sum(t["marcado_fim"] for t in tr),
           "stops": sum(t["parou_stop"] for t in tr),
           "truncados": sum(t["truncado"] for t in tr),
           "hold0": sum(1 for t in tr if t["hold"] == 0)}
    for c in ("ret10", "ret6"):
        rs = [t[c] for t in tr]
        pico, dd = curva(rs)
        out[c] = {"tot": sum(rs), "bpt": sum(rs) / n if n else 0.0,
                  "venc": 100.0 * sum(1 for r in rs if r > 0) / n if n else 0.0,
                  "pico": pico, "dd": dd}
    return out


def agrupa(tr: list, chave) -> dict:
    """{chave: [n, ret10, ret6]}"""
    out = {}
    for t in tr:
        x = out.setdefault(chave(t), [0, 0.0, 0.0])
        x[0] += 1
        x[1] += t["ret10"]
        x[2] += t["ret6"]
    return out


CAMPOS = ["setup", "bar_ts_iso", "mes", "sym", "direcao", "hold",
          "marcado_fim", "parou_stop", "ret6", "ret10"]


def linha_csv(t: dict) -> dict:
    iso = _iso(t["ts"])
    return {"setup": t["setup"], "bar_ts_iso": iso, "mes": iso[:7],
            "sym": t["sym"], "direcao": "LONG" if t["long"] else "SHORT",
            "hold": t["hold"], "marcado_fim": int(t["marcado_fim"]),
            "parou_stop": int(t["parou_stop"]), "ret6": f"{t['ret6']:.2f}",
            "ret10": f"{t['ret10']:.2f}"}


# ------------------------------------------------------------ log -----------
def _secao_resultado(log, por: dict) -> None:
    log("\n===== RESULTADO POR SETUP (bps somados por trade; curva na ordem "
        "de entrada) =====")
    for s in SETUPS:
        tr, r = por[s], resumo(por[s])
        log(f"\n  {s}: n {r['n']} (fechados {r['n'] - r['marcados']}, marcados "
            f"no fim {r['marcados']}) | saidas por stop {r['stops']} "
            f"({100.0 * r['stops'] / r['n'] if r['n'] else 0.0:.1f}%)")
        for c, rot in (("ret10", "@10 bps"), ("ret6", "@ 6 bps")):
            x = r[c]
            log(f"    {rot}: total {x['tot']:+.0f} | bps/trade {x['bpt']:+.1f} "
                f"| vencedores {x['venc']:.1f}% | pico da curva "
                f"{x['pico']:+.0f} | rebaixamento {x['dd']:.0f}")
        for nome, lado in (("LONG ", True), ("SHORT", False)):
            sub = resumo([t for t in tr if t["long"] is lado])
            log(f"    {nome}: n {sub['n']} | @10 {sub['ret10']['tot']:+.0f} "
                f"({sub['ret10']['bpt']:+.1f}/trade, vencedores "
                f"{sub['ret10']['venc']:.1f}%) | @6 {sub['ret6']['tot']:+.0f} "
                f"| stops {sub['stops']}")


def _secao_decomposicao(log, por: dict) -> None:
    log("\n===== DECOMPOSICAO DESCRITIVA (por mes de ENTRADA; n | @10 | @6) "
        "=====")
    for s in SETUPS:
        tr = por[s]
        tot10, tot6 = sum(t["ret10"] for t in tr), sum(t["ret6"] for t in tr)
        log(f"\n  {s}")
        mes = agrupa(tr, lambda t: _iso(t["ts"])[:7])
        for mm in sorted(mes):
            n, a, b = mes[mm]
            log(f"    {mm}: n {n:>4} | {a:>+9.0f} | {b:>+9.0f}")
        if mes:
            mm = max(mes, key=lambda k: mes[k][1])
            log(f"    sem o melhor mes ({mm}, pelo @10): @10 "
                f"{tot10 - mes[mm][1]:+.0f} | @6 {tot6 - mes[mm][2]:+.0f}")
        nt, _f10, sem10 = concentracao([t["ret10"] for t in tr])
        _nt, _f6, sem6 = concentracao([t["ret6"] for t in tr])
        log(f"    sem os {100 * TOP_FRAC:.0f}% melhores trades ({nt} de "
            f"{len(tr)}): @10 {sem10:+.0f} | @6 {sem6:+.0f}")
        tok = agrupa(tr, lambda t: t["sym"])
        ordem = sorted(tok.items(), key=lambda x: -x[1][1])
        fmt = lambda xs: " ".join(                               # noqa: E731
            f"{k.replace('USDT', '')}:{v[1]:+.0f}({v[0]})" for k, v in xs)
        log(f"    top 5 tokens @10 (n): {fmt(ordem[:5])}")
        log(f"    bottom 5 tokens @10 (n): {fmt(ordem[::-1][:5])}")
        log(f"    tokens positivos: @10 "
            f"{sum(1 for v in tok.values() if v[1] > 0)} de {len(tok)} com "
            f"trade | @6 {sum(1 for v in tok.values() if v[2] > 0)} de "
            f"{len(tok)}")


def _secao_passivo(log, passivo: dict, n_universo: int) -> None:
    ok = {s: v for s, v in passivo.items() if v is not None}
    fora = sorted(set(passivo) - set(ok))
    log(f"\n===== PASSIVO DESCRITIVO (cesta, pesos iguais, close da barra "
        f"{INI_ISO} -> close da barra {FIM_ISO}, sem custo) =====")
    if not ok:
        log("  sem token com as duas pontas")
        return
    soma = sum(ok.values())
    log(f"  tokens na cesta: {len(ok)} de {n_universo}"
        + (f" | sem uma das pontas: {' '.join(fora)}" if fora else ""))
    log(f"  comprar e segurar: media {soma / len(ok):+.0f} bps por token | "
        f"soma {soma:+.0f} bps | tokens em alta {sum(v > 0 for v in ok.values())}")
    log(f"  vender e segurar : media {-soma / len(ok):+.0f} bps por token | "
        f"soma {-soma:+.0f} bps")
    log("  (a soma e comparavel ao total dos setups: 1 unidade por token; os "
        "setups somam 1 unidade por trade)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="tres setups, 31/08 -> 14/11/2026")
    ap.add_argument("--tf", default="4h")
    ap.add_argument("--end", default=END_ISO,
                    help='exclusivo, UTC; default "%(default)s"')
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=None,
                    help="default: SYMBOLS_KIS_3489 (65)")
    ap.add_argument("--out", default=None, help="CSV; o log vai ao lado (.log)")
    ap.add_argument("--sem-guarda-relogio", action="store_true")
    add_teto_rss(ap)
    args = ap.parse_args(argv)
    define_teto_rss(args.teto_rss)
    aviso_tf = None
    if args.tf != "4h":                    # 23/11/2026: aviso, nao recusa
        aviso_tf = (
            f"[aviso] tf {args.tf}: este instrumento mede em barras de 4h e "
            f"NAO foi parametrizado pelo tf. Fica DISTORCIDO: (1) o caminho "
            f"do `medir_sem_borda` usa barra de 4h fixa e pula as barras do "
            f"tf nas primeiras 4h apos a entrada; (2) o diario do AIO "
            f"(`kis_tf.agregar(c, 6)`) espera 6 velas de 4h por dia, entao "
            f"todo dia some e o filtro diario veta tudo; (3) a cobertura "
            f"esperada conta barras de 4h; (4) teto do hold {EXT_CAP} e "
            f"aquecimento sao barras do tf, nao a mesma duracao. Numeros "
            f"desta rodada nao comparam com os de 4h.")
        print(aviso_tf, flush=True)
    teto_ms = _ms(args.end)
    if teto_ms <= JANELA[0]:
        raise SystemExit(f"[holdout_2026] --end {args.end} nao passa do inicio "
                         f"do periodo ({INI_ISO}). ABORTADO.")
    if not args.sem_guarda_relogio:
        _guarda_relogio()
    from v10.registro import SYMBOLS_KIS_3489
    symbols = args.symbols or list(SYMBOLS_KIS_3489)
    out = Path(args.out) if args.out else (
        ROOT / "logs" /
        f"holdout_2026_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    conn = conecta_com_teto(args.store, teto_ms)
    log = _Log(out.with_suffix(".log"))
    try:
        mx = conn.execute("SELECT MAX(ts) FROM candles").fetchone()[0]
        if mx is not None and mx >= teto_ms:
            raise SystemExit("[holdout_2026] view deixou passar candle >= "
                             "--end. ABORTADO.")
        # ---- 1. cabecalho ---------------------------------------------------
        log(f"[holdout_2026] periodo: entradas de {INI_ISO} a {FIM_ISO} UTC "
            f"(inclusive) | fim do dado --end {args.end} (exclusivo) | tf "
            f"{args.tf} | universo {len(symbols)} simbolos | custo 10 e 6 bps "
            f"por trade | setups: KIS_E0 (KIS {PAR[0]}/{PAR[1]} + portao "
            f"{PORTAO_LIMIAR}/ADX {PORTAO_ADX_MIN}, saida na inversao), "
            f"KIS_E1-3 (+ stop {KIS_STOP_K} x ATR14), AIO_G1 (stop "
            f"{STOP_K:g} x ATR14)")
        if aviso_tf:
            log(aviso_tf)
        # ---- 2. conferencias (cobertura ANTES de medir) ---------------------
        esp = (teto_ms - JANELA[0]) // BAR_4H_MS
        cob = cobertura(conn, symbols, args.tf, JANELA[0], teto_ms)
        falta = {s: n for s, n in cob.items() if n != esp}
        log(f"\n===== CONFERENCIAS =====\n  cobertura (SQL, velas {args.tf} "
            f"com {INI_ISO} <= ts < {args.end}): esperado {esp} por token | "
            f"completos {len(cob) - len(falta)} de {len(cob)}")
        for s, n in falta.items():
            log(f"    FALTA {s}: {n}/{esp}")
        A = coleta(conn, symbols, args.tf, teto_ms)
    finally:
        conn.close()

    por = {s: [t for t in A["trades"] if t["setup"] == s] for s in SETUPS}
    c = A["cont"]
    log(f"  maior ts visivel no store {_iso(mx) if mx else '-'} | maior ts "
        f"lido {_iso(A['maior_ts']) if A['maior_ts'] else '-'} | simbolos sem "
        f"candle: {' '.join(A['sem_candles']) or 'nenhum'}")
    log(f"  KIS no periodo: inversoes {c['kis_inversoes']} | vetadas pelo "
        f"portao {c['kis_vetadas']} | AIO no periodo: viradas "
        f"{c['aio_viradas']} | fora do G1 {c['aio_fora_g1']}")
    log(f"  fora da medicao (esperado 0 e 0): ATR14 <= 0 {c['atr0']} | "
        f"caminho mais curto que o hold por buraco de ts {c['excl_hold']}")
    for s in SETUPS:
        r = resumo(por[s])
        nl = sum(1 for t in por[s] if t["long"])
        log(f"  {s:>8}: entradas {r['n']} (LONG {nl}, SHORT {r['n'] - nl}) | "
            f"marcados no fim {r['marcados']} | entrada na ultima barra (hold "
            f"0) {r['hold0']} | truncados em {EXT_CAP} barras "
            f"{r['truncados']}")
    # ---- 3 a 5 ----------------------------------------------------------------
    _secao_resultado(log, por)
    _secao_decomposicao(log, por)
    _secao_passivo(log, A["passivo"], len(symbols))

    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        for t in sorted(A["trades"], key=lambda t: (SETUPS.index(t["setup"]),
                                                    t["ts"], t["sym"])):
            w.writerow(linha_csv(t))
    log(f"\n  CSV: {out} ({len(A['trades'])} linhas) | RSS de pico "
        f"{A['rss']:.0f} MiB (teto {teto_rss():.0f}) | "
        f"{time.monotonic() - t0:.0f} s")
    # ---- 6. fecho ---------------------------------------------------------------
    log("caveat: so medicao, sem veredito. Custo por trade nas duas colunas "
        "(@10 = @6 - 4 bps). Entrada no close da barra do sinal. Trade aberto "
        "na ultima barra visivel e marcado no close dela (sem exclusao de "
        "borda de 48 barras nesta rodada), com o stop valendo ate la; entrada "
        f"na propria ultima barra tem retorno bruto 0. Hold teto {EXT_CAP} "
        "barras. Aquecimento: passada continua desde o primeiro candle do "
        "store. Stop em ATR14 da barra do sinal, fill min(stop, open). Curva "
        "e rebaixamento sobre trades ordenados por entrada (nao marca a "
        "mercado, nao trata trades simultaneos). Mes = mes da ENTRADA (as "
        "duas barras de 31/08 caem em 2026-08).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
