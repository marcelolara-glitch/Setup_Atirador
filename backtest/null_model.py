# backtest/null_model.py — Setup Atirador v9
# O JUIZ. Converte a curva de excursao (excursion.py) num VEREDITO de edge,
# comparando a receita contra um NULO CASADO que isola o drift de regime/direcao
# — o que faz um setup parecer "positivo" sem ter skill.
#   edge[H] = mediana(MFE-MAE)_receita[H] - mediana(MFE-MAE)_nulo[H]
# O nulo sao entradas aleatorias na MESMA celula (simbolo x regime x direcao).
# TEETH-CHECK (--detector random): um detector aleatorio TEM que dar edge ~ 0;
# se nao der, o nulo esta furado e nenhum veredito vale.
# Independencia: apos colapsar, mantem um evento por (simbolo, direcao) so se a
# >= max(HORIZONS)=48 barras do ultimo mantido (janelas forward nao sobrepostas
# -> N independente). Substitui o cooldown de 8 do 2a NESTE modulo.
# Roda na VM (regime/exchanges dependem de pandas_ta). NAO toca runtime.
# Uso: .venv/bin/python -m backtest.null_model --start 2026-05-15 --end 2026-05-22

from __future__ import annotations

import argparse
import random
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from exchanges import klines_to_dataframe                              # noqa: E402
from regime import MIN_CANDLES, classify_regime                       # noqa: E402
from backtest.candle_store import connect as store_connect, read_candles  # noqa: E402
from backtest.excursion import HORIZONS, collapse_events, measure_event   # noqa: E402
from backtest.sweep import TIER1, sweep_symbol                         # noqa: E402

SEP_BARS = max(HORIZONS)        # 48 — separacao p/ janelas forward nao sobrepostas
_WINDOW = 150                   # velas p/ classificar regime (>= MIN_CANDLES)
_SEED = 1337                    # teeth-check/veredito reproduziveis entre rodadas
_DIR_OF_REGIME = {"TREND_UP": "LONG", "TREND_DOWN": "SHORT"}


def _iso_to_ms(iso: str) -> int:
    """ISO -> epoch-ms (UTC). Mesma convencao de excursion/sweep/replay."""
    return int(datetime.fromisoformat(iso)
               .astimezone(timezone.utc).timestamp() * 1000)


def regime_timeline(conn, symbol: str, start_ms: int, end_ms: int) -> dict:
    """bar_ts -> regime, para cada bar em [start_ms, end_ms]. Classifica sobre a
    janela de 150 velas terminando no bar (no-lookahead). Le os candles UMA vez e
    fatia por indice — caro (~13k chamadas no total), computado 1x por simbolo."""
    all_c = read_candles(conn, symbol, "15m", end_ms=end_ms)
    out: dict = {}
    for i, c in enumerate(all_c):
        ts = c["ts"]
        if ts < start_ms or ts > end_ms:
            continue
        window = all_c[max(0, i - _WINDOW + 1): i + 1]
        if len(window) < MIN_CANDLES:
            continue
        try:
            out[ts] = classify_regime(klines_to_dataframe(window)).regime
        except ValueError:
            continue
    return out


def get_events(conn, symbol: str, start_ms: int, end_ms: int,
               setup: str) -> list:
    """sweep_symbol -> filtra `setup in confluent_setups` -> colapsa por direcao
    com separacao >= SEP_BARS (nao-sobreposicao). Retorna [{bar_ts, direction}]."""
    sigs = [s for s in sweep_symbol(conn, symbol, start_ms, end_ms)
            if setup in s["confluent_setups"]]
    return [{"bar_ts": s["bar_ts"], "direction": s["direction"]}
            for s in collapse_events(sigs, SEP_BARS)]


def random_detector(conn, symbol: str, timeline: dict, start_ms: int,
                    end_ms: int, n: int) -> list:
    """Teeth-check: sorteia ate `n` bars do intervalo, direcao = a do regime no
    bar (TREND_UP->LONG, TREND_DOWN->SHORT; pula RANGE/SQUEEZE). Mesma separacao
    de SEP_BARS. Passa pelo mesmo pipeline -> edge esperado ~ 0."""
    cands = [{"bar_ts": ts, "direction": _DIR_OF_REGIME[reg]}
             for ts, reg in timeline.items() if reg in _DIR_OF_REGIME]
    if len(cands) > n:
        cands = random.sample(cands, n)
    return [{"bar_ts": s["bar_ts"], "direction": s["direction"]}
            for s in collapse_events(cands, SEP_BARS)]


def build_null_pool(conn, symbol: str, timeline: dict, regime: str,
                    direction: str, max_n: int = 500) -> dict:
    """Pool da celula (simbolo, regime, direcao): candidatos = bars cujo regime
    bate; amostra ate max_n, mede excursao em cada (descarta None) e acumula
    (mfe[H]-mae[H]) por horizonte. Pre-medido 1x; o bootstrap reamostra dele."""
    bars = [ts for ts, reg in timeline.items() if reg == regime]
    if len(bars) > max_n:
        bars = random.sample(bars, max_n)
    pool = {h: [] for h in HORIZONS}
    for ts in bars:
        m = measure_event(conn, symbol, ts, direction)
        if m is None:
            continue
        for h in HORIZONS:
            pool[h].append(m["mfe"][h] - m["mae"][h])
    return pool


def bootstrap_edge(events_vals: list, cell_pools: dict,
                   n_boot: int = 1000) -> dict:
    """Veredito por horizonte. `events_vals`=[{cell, vals{H}}]; `cell_pools`=
    cell->{H:[vals]}. recipe_med[H]=mediana de (mfe-mae) dos eventos reais. Nulo:
    n_boot vezes, p/ cada evento sorteia 1 valor do pool da SUA celula -> mediana
    por iteracao -> distribuicao nula da mediana. null_med=mediana das nulas;
    effect=recipe_med-null_med (ATR); p=fracao das nulas >= recipe_med (one-sided)."""
    nan = float("nan")
    out: dict = {}
    for h in HORIZONS:
        # so eventos cuja celula tem pool nao-vazio em h (receita e nulo casados).
        evs = [e for e in events_vals
               if h in e["vals"] and cell_pools.get(e["cell"], {}).get(h)]
        n = len(evs)
        if n == 0:
            out[h] = {"n_events": 0, "recipe_med": nan, "null_med": nan,
                      "effect": nan, "p": nan}
            continue
        recipe_med = statistics.median([e["vals"][h] for e in evs])
        pools = [cell_pools[e["cell"]][h] for e in evs]
        null_meds = [statistics.median([random.choice(p) for p in pools])
                     for _ in range(n_boot)]
        null_med = statistics.median(null_meds)
        p = sum(1 for m in null_meds if m >= recipe_med) / len(null_meds)
        out[h] = {"n_events": n, "recipe_med": recipe_med, "null_med": null_med,
                  "effect": recipe_med - null_med, "p": p}
    return out


def run(store_db: str, symbols: list, start_iso: str, end_iso: str, setup: str,
        detector: str, n_boot: int, max_null: int, n_random: int) -> dict:
    """Por simbolo: timeline -> eventos (real|random) -> casa cada evento a sua
    celula (simbolo, regime_no_bar, direcao) -> pools. Agrega entre simbolos e
    devolve o veredito do bootstrap por horizonte."""
    conn = store_connect(store_db)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)

    events_vals: list = []
    cell_pools: dict = {}
    for sym in symbols:
        timeline = regime_timeline(conn, sym, start_ms, end_ms)
        evs = (random_detector(conn, sym, timeline, start_ms, end_ms, n_random)
               if detector == "random"
               else get_events(conn, sym, start_ms, end_ms, setup))
        kept = 0
        for ev in evs:
            reg = timeline.get(ev["bar_ts"])
            if reg is None:
                continue
            m = measure_event(conn, sym, ev["bar_ts"], ev["direction"])
            if m is None:
                continue
            cell = (sym, reg, ev["direction"])
            events_vals.append({
                "cell": cell,
                "vals": {h: m["mfe"][h] - m["mae"][h] for h in HORIZONS}})
            if cell not in cell_pools:
                cell_pools[cell] = build_null_pool(
                    conn, sym, timeline, reg, ev["direction"], max_null)
            kept += 1
        print(f"[null_model] {sym}: timeline={len(timeline)} eventos={kept}",
              file=sys.stderr)

    conn.close()
    return {"n_total": len(events_vals),
            "verdict": bootstrap_edge(events_vals, cell_pools, n_boot)}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Veredito de edge: receita vs nulo casado (simbolo x regime x dir)")
    ap.add_argument("--setup", default="cont_pull", help="setup a julgar")
    ap.add_argument("--detector", choices=["real", "random"], default="real",
                    help="real=setup; random=teeth-check (edge esperado ~0)")
    ap.add_argument("--start", required=True, help="data ISO, ex. 2026-05-15")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-05-22")
    ap.add_argument("--n-boot", type=int, default=1000, help="iteracoes bootstrap")
    ap.add_argument("--max-null", type=int, default=500, help="amostras por pool")
    ap.add_argument("--n-random", type=int, default=200,
                    help="bars sorteados por simbolo no teeth-check")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=TIER1)
    args = ap.parse_args()

    random.seed(_SEED)
    print(f"[null_model] store={args.store} setup={args.setup} "
          f"detector={args.detector}", file=sys.stderr)
    print(f"[null_model] janela {args.start} -> {args.end} n_boot={args.n_boot} "
          f"max_null={args.max_null} symbols={len(args.symbols)}", file=sys.stderr)

    r = run(args.store, args.symbols, args.start, args.end, args.setup,
            args.detector, args.n_boot, args.max_null, args.n_random)

    tag = "TEETH-CHECK (random)" if args.detector == "random" else args.setup
    print(f"\n========== VEREDITO {tag} ==========")
    print(f"eventos totais : {r['n_total']}")
    print(f"\n--- edge por horizonte (em ATR) ---")
    print(f"{'H':>4} | {'n_ev':>5} | {'recipe':>8} | {'null':>8} | "
          f"{'effect':>8} | {'p':>6}")
    for h in HORIZONS:
        v = r["verdict"][h]
        print(f"{h:>4} | {v['n_events']:>5} | {v['recipe_med']:>8.3f} | "
              f"{v['null_med']:>8.3f} | {v['effect']:>8.3f} | {v['p']:>6.3f}")

    if args.detector == "random":
        print("\n[teeth-check] esperado: effect ~ 0 e p ~ 0.5 em todos os H.")
        print("[teeth-check] se o aleatorio mostrar edge, o NULO esta furado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
