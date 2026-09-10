# backtest/diag_invariancia_alvo.py — Setup Atirador v10 (DIAGNOSTICO)
# Mede, offline sobre o store, se o alvo do detector KIS depende do TAMANHO da
# janela entregue a ele. NAO corrige nada e nao toca em runtime: so prova e
# dimensiona.
#
# O QUE ESTA EM JOGO. `v10/runner.py:201` pede `warmup_barras + FOLGA_BARRAS`
# velas e entrega essa fatia ao detector E ao anotador (`kis_regime.alvos`).
# Para a ficha 34/89 isso e 89+80 = 169 barras. Duas coisas dentro de `alvos`
# tem memoria MAIS LONGA que 169 barras:
#   (a) SEMENTE DA EMA — `keepitsimple._ema` semeia com a SMA das n primeiras
#       barras DA JANELA. Com EMA89 (alpha = 2/90) o peso residual da semente na
#       ultima barra de uma janela de 169 e 0.97778^80 ~= 0.166: 17% do valor da
#       EMA89 vem de ONDE A JANELA COMECOU, nao do mercado.
#   (b) CARRY DO ALVO — `_alvo_extremos` comeca em alvo[0]=0 e CARREGA enquanto
#       o estado nao e extremo confirmado. Janela nova = alvo zerado: sem
#       VERDE/VERM confirmado depois do warmup, a janela curta devolve 0 onde a
#       serie inteira devolve +1/-1.
# Sao dois mecanismos independentes — (a) muda o ESTADO, (b) so o CARRY — e os
# dois aparecem como "o alvo mudou porque a janela deslizou", que e exatamente o
# que a saida `reverse` do v10 le pra fechar trade.
#
# LEITURA. Divergencia aqui NAO e erro de conta: as tres estao certas para a
# janela que receberam. O numero que importa e a FRACAO de barras em que a
# janela do runner discorda da serie inteira — o tamanho do vies de hoje.
# Uso: .venv/bin/python -m backtest.diag_invariancia_alvo --n-longa 500
from __future__ import annotations
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.kis_regime import alvos                              # noqa: E402


def alvo_em(candles: list, i: int, n, params: dict) -> int:
    """Alvo vigente na barra `i` calculado sobre a janela que TERMINA em `i` e
    tem no maximo `n` barras. `n=None` = todo o historico disponivel ate `i`.
    E a MESMA chamada que o anotador do v10 faz — `alvos` da bancada, com os
    periodos da ficha —, so muda de onde a janela comeca."""
    ini = 0 if n is None else max(0, i - int(n) + 1)
    closes = [c["close"] for c in candles[ini:i + 1]]
    return alvos(closes, params["ema_fast"], params["ema_slow"],
                 params["confirmacao"])[-1]


def divergencias(candles: list, params: dict, k: int, n_curta: int,
                 n_longa: int) -> list:
    """Uma linha por barra-alvo `t` (as ultimas `k` do store), com o alvo das
    tres janelas que terminam em `t`. Funcao PURA: recebe candles, devolve
    linhas. E por ela que o teste sintetico entra."""
    out = []
    for i in range(max(0, len(candles) - int(k)), len(candles)):
        out.append({"ts": candles[i]["ts"],
                    "curta": alvo_em(candles, i, n_curta, params),
                    "longa": alvo_em(candles, i, n_longa, params),
                    "tudo": alvo_em(candles, i, None, params),
                    "hist_longa_ok": (i + 1) >= int(n_longa)})
    return out


def _iso(ts_ms: int) -> str:
    return f"{datetime.fromtimestamp(ts_ms / 1000, timezone.utc):%Y-%m-%d %H:%M}"


def _schema(conn) -> str:
    """Schema REAL do store, lido do proprio banco — nenhum nome de coluna e
    assumido aqui nem no resto do arquivo (as velas saem de `read_candles`)."""
    linhas = [".tables + PRAGMA table_info (lido do store):"]
    tabelas = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    linhas.append("  tabelas: " + " ".join(tabelas))
    for t in tabelas:
        linhas.append(f"  PRAGMA table_info({t}):")
        for cid, nome, tipo, notnull, dflt, pk in conn.execute(
                f"PRAGMA table_info({t})"):
            linhas.append(f"    {cid} {nome:<10} {tipo:<8} notnull={notnull} "
                          f"pk={pk}")
    return "\n".join(linhas)


def roda_ficha(conn, nome: str, symbols: list, params: dict, n_curta: int,
               n_longa: int, k: int, tf: str) -> dict:
    """Percorre os simbolos da ficha e imprime a tabela por simbolo. Guarda so
    escalares e as 10 primeiras divergencias — nada de serie em memoria."""
    from backtest.candle_store import read_candles                 # noqa: E402
    print(f"\n===== {nome} | par {params['ema_fast']}/{params['ema_slow']} | "
          f"tf={tf} | k={k} barras-alvo | janelas: {n_curta} (runner), "
          f"{n_longa}, TUDO =====")
    print(f"{'symbol':>13} | {'barras':>7} | {'k':>4} | {'curta!=tudo':>11} | "
          f"{'longa!=tudo':>11}")
    tot = {"barras": 0, "d_curta": 0, "d_longa": 0}
    primeiras, curtos, vazios = [], [], []
    for sym in symbols:
        cndl = read_candles(conn, sym, tf)
        if not cndl:
            vazios.append(sym)
            continue
        if len(cndl) < n_longa:
            curtos.append(f"{sym}({len(cndl)})")
        linhas = divergencias(cndl, params, k, n_curta, n_longa)
        dc = sum(1 for r in linhas if r["curta"] != r["tudo"])
        dl = sum(1 for r in linhas if r["longa"] != r["tudo"])
        tot["barras"] += len(linhas)
        tot["d_curta"] += dc
        tot["d_longa"] += dl
        for r in linhas:
            if r["curta"] != r["tudo"] and len(primeiras) < 10:
                primeiras.append((sym, _iso(r["ts"]), r["curta"], r["longa"],
                                  r["tudo"]))
        print(f"{sym:>13} | {len(cndl):>7} | {len(linhas):>4} | {dc:>11} | "
              f"{dl:>11}")
    n = tot["barras"] or 1
    print(f"\nTOTAL {nome}: {tot['barras']} barras avaliadas | "
          f"curta({n_curta}) != TUDO: {tot['d_curta']} "
          f"({100.0 * tot['d_curta'] / n:.1f}%) | "
          f"longa({n_longa}) != TUDO: {tot['d_longa']} "
          f"({100.0 * tot['d_longa'] / n:.1f}%)")
    if primeiras:
        print(f"\n10 primeiras divergencias curta vs TUDO ({nome}):")
        print(f"{'symbol':>13} | {'ts (UTC)':>16} | {'curta':>5} | "
              f"{'longa':>5} | {'tudo':>4}")
        for s, ts, a, b, c in primeiras:
            print(f"{s:>13} | {ts:>16} | {a:>5} | {b:>5} | {c:>4}")
    else:
        print(f"\n(nenhuma divergencia curta vs TUDO em {nome})")
    return {"nome": nome, **tot, "curtos": curtos, "vazios": vazios}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Diagnostico: o alvo do KIS depende do tamanho da janela?")
    ap.add_argument("--tf", default="4h", help="rotulo do tf NO STORE")
    ap.add_argument("--k", type=int, default=60,
                    help="quantas barras-alvo (as ultimas do store)")
    ap.add_argument("--n-longa", type=int, default=500)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--fichas", nargs="+",
                    default=["kis_3489_60t_4h", "kis_regime_4h"])
    args = ap.parse_args()

    from backtest.candle_store import connect                      # noqa: E402
    from v10.registro import (KIS_3489_60T_4H, KIS_REGIME_4H,      # noqa: E402
                              PARAMS_KIS_3489, PARAMS_KIS_REGIME)
    from v10.runner import FOLGA_BARRAS                            # noqa: E402
    # n_curta NAO e constante deste arquivo: e derivada da ficha + do runner,
    # que sao os donos do numero. Mexer no warmup la muda o diagnostico aqui.
    disp = {"kis_3489_60t_4h": (KIS_3489_60T_4H, PARAMS_KIS_3489),
            "kis_regime_4h": (KIS_REGIME_4H, PARAMS_KIS_REGIME)}

    conn = connect(args.store)
    print(_schema(conn))
    resumos = []
    for nome in args.fichas:
        spec, params = disp[nome]
        resumos.append(roda_ficha(
            conn, nome, list(spec.symbols), params,
            int(spec.warmup_barras) + FOLGA_BARRAS, args.n_longa, args.k,
            args.tf))
    conn.close()

    print("\n===== RESUMO =====")
    for r in resumos:
        n = r["barras"] or 1
        print(f"{r['nome']:>18}: curta {r['d_curta']}/{r['barras']} "
              f"({100.0 * r['d_curta'] / n:.1f}%) | longa {r['d_longa']}/"
              f"{r['barras']} ({100.0 * r['d_longa'] / n:.1f}%)")
    curtos = sorted({s for r in resumos for s in r["curtos"]})
    vazios = sorted({s for r in resumos for s in r["vazios"]})
    print(f"\ncaveat: janela unica e uma so leitura por barra — isto DIMENSIONA "
          f"o vies, nao corrige nada e nao promove nada. 'TUDO' e todo o "
          f"historico NO STORE, que tambem tem inicio; e o piso da divergencia, "
          f"nao o valor verdadeiro. Historico < N_LONGA={args.n_longa} "
          f"(linha 'longa' nao comparavel): "
          f"{', '.join(curtos) if curtos else 'nenhum'}. "
          f"Sem candle nenhum no store: "
          f"{', '.join(vazios) if vazios else 'nenhum'}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
