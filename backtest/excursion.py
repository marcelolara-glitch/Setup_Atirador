# backtest/excursion.py — Setup Atirador v9
# Camada de MEDICAO sobre a varredura provada (sweep.py). Duas etapas:
#   1) Colapsa re-fires dos sinais varridos em eventos independentes (cooldown).
#      A varredura crua re-dispara ~6.3x; medir sobre disparos crus e pseudo-
#      replicacao. O colapso e estrutural, nao opcional.
#   2) Mede excursao forward (MFE/MAE) por evento, em unidades de ATR, numa
#      curva de horizontes. SEM TP/SL de producao — excursao pura.
# Roda na VM (sweep_symbol depende de pandas_ta). NAO toca runtime.
# Uso: .venv/bin/python -m backtest.excursion --start 2026-05-15 --end 2026-05-22

from __future__ import annotations

import argparse
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import connect as store_connect, read_candles  # noqa: E402
from backtest.replay import BAR_15M_MS                                    # noqa: E402
from backtest.sweep import TIER1, sweep_symbol                            # noqa: E402

HORIZONS = [4, 8, 16, 32, 48]   # barras 15m = 1h, 2h, 4h, 8h, 12h
_CTX_BARS = 30                  # historico p/ ancorar entry + ATR


def _iso_to_ms(iso: str) -> int:
    """ISO -> epoch-ms (UTC). Mesma convencao de sweep/replay."""
    return int(datetime.fromisoformat(iso)
               .astimezone(timezone.utc).timestamp() * 1000)


def _wilder_atr(candles: list, period: int = 14) -> float:
    """ATR de Wilder sobre os ultimos period+1 candles. TR de cada intervalo =
    max(h-l, |h-cprev|, |l-cprev|). Com exatamente `period` TRs, o ATR de Wilder
    e a media simples desses TRs (semente, sem suavizacao posterior). Determinista
    — sera reusada identica pelo nulo do PR-2b. Insuficiente/<0 -> 0.0."""
    if period <= 0 or len(candles) < period + 1:
        return 0.0
    window = candles[-(period + 1):]
    trs = []
    for i in range(1, len(window)):
        h, l = window[i]["high"], window[i]["low"]
        cprev = window[i - 1]["close"]
        trs.append(max(h - l, abs(h - cprev), abs(l - cprev)))
    atr = sum(trs) / period
    return atr if atr > 0 else 0.0


def collapse_events(signals: list, cooldown_bars: int) -> list:
    """Colapsa re-fires de UM simbolo (ordenados por bar_ts) por direction:
    mantem o PRIMEIRO disparo de cada cluster e suprime disparos da mesma
    direcao enquanto (bar_ts - ultimo_mantido) < cooldown_bars * BAR_15M_MS."""
    cooldown_ms = cooldown_bars * BAR_15M_MS
    last_kept: dict = {}   # direction -> bar_ts do ultimo mantido
    out = []
    for sig in sorted(signals, key=lambda s: s["bar_ts"]):
        d = sig["direction"]
        prev = last_kept.get(d)
        if prev is not None and (sig["bar_ts"] - prev) < cooldown_ms:
            continue
        last_kept[d] = sig["bar_ts"]
        out.append(sig)
    return out


def measure_event(conn, symbol: str, bar_ts: int, direction: str):
    """Mede excursao forward de UM evento, em ATR. Entry = close da barra do
    sinal (bar_ts); forward = barras bar_ts+1 ... bar_ts+48 (nunca a propria
    barra do sinal). atr<=0 -> None. Borda direita do store (< 48 barras
    forward) -> None. Retorna {symbol, bar_ts, direction, atr, mfe{H}, mae{H}}."""
    ctx = read_candles(conn, symbol, "15m", end_ms=bar_ts)
    if not ctx:
        return None
    ctx = ctx[-_CTX_BARS:]
    entry = ctx[-1]["close"]
    atr = _wilder_atr(ctx)
    if atr <= 0:
        return None

    fwd = read_candles(conn, symbol, "15m",
                       start_ms=bar_ts + BAR_15M_MS,
                       end_ms=bar_ts + max(HORIZONS) * BAR_15M_MS)
    if len(fwd) < max(HORIZONS):   # exclusao de borda direita — regra travada
        return None

    is_long = (direction or "").upper() == "LONG"
    mfe, mae = {}, {}
    for h in HORIZONS:
        win = fwd[:h]
        hi = max(c["high"] for c in win)
        lo = min(c["low"] for c in win)
        if is_long:
            mfe[h] = (hi - entry) / atr
            mae[h] = (entry - lo) / atr
        else:
            mfe[h] = (entry - lo) / atr
            mae[h] = (hi - entry) / atr
    return {"symbol": symbol, "bar_ts": bar_ts, "direction": direction,
            "atr": atr, "mfe": mfe, "mae": mae}


def run(store_db: str, symbols: list, start_iso: str, end_iso: str,
        cooldown_bars: int, setup: str) -> dict:
    """Por simbolo: sweep_symbol -> filtra `setup` -> collapse_events ->
    measure_event (descarta None). Agrega por horizonte e devolve contagens
    (raw, colapsados, medidos, excluidos_borda) + a curva de excursao."""
    conn = store_connect(store_db)
    start_ms, end_ms = _iso_to_ms(start_iso), _iso_to_ms(end_iso)

    raw = colapsados = medidos = excluidos_borda = 0
    acc_mfe = {h: [] for h in HORIZONS}
    acc_mae = {h: [] for h in HORIZONS}

    for sym in symbols:
        sigs = [s for s in sweep_symbol(conn, sym, start_ms, end_ms)
                if setup in s["confluent_setups"]]
        raw += len(sigs)
        events = collapse_events(sigs, cooldown_bars)
        colapsados += len(events)
        for ev in events:
            m = measure_event(conn, sym, ev["bar_ts"], ev["direction"])
            if m is None:
                excluidos_borda += 1
                continue
            medidos += 1
            for h in HORIZONS:
                acc_mfe[h].append(m["mfe"][h])
                acc_mae[h].append(m["mae"][h])

    conn.close()

    curva = []
    for h in HORIZONS:
        n = len(acc_mfe[h])
        if n:
            med_mfe = statistics.median(acc_mfe[h])
            med_mae = statistics.median(acc_mae[h])
            med_diff = statistics.median(
                [a - b for a, b in zip(acc_mfe[h], acc_mae[h])])
        else:
            med_mfe = med_mae = med_diff = float("nan")
        curva.append({"H": h, "n": n, "med_mfe": med_mfe,
                      "med_mae": med_mae, "med_diff": med_diff})

    return {"raw": raw, "colapsados": colapsados, "medidos": medidos,
            "excluidos_borda": excluidos_borda, "curva": curva}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Excursao MFE/MAE por evento (colapso de re-fires + ATR)")
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--symbols", nargs="+", default=TIER1)
    ap.add_argument("--start", required=True, help="data ISO, ex. 2026-05-15")
    ap.add_argument("--end", required=True, help="data ISO, ex. 2026-05-22")
    ap.add_argument("--cooldown", type=int, default=8, help="barras de cooldown")
    ap.add_argument("--setup", default="cont_pull", help="setup a medir")
    args = ap.parse_args()

    print(f"[excursion] store={args.store} setup={args.setup}", file=sys.stderr)
    print(f"[excursion] janela {args.start} -> {args.end} cooldown={args.cooldown} "
          f"symbols={len(args.symbols)}", file=sys.stderr)

    r = run(args.store, args.symbols, args.start, args.end,
            args.cooldown, args.setup)

    print(f"\n========== EXCURSAO {args.setup} ==========")
    print(f"raw            : {r['raw']}")
    print(f"colapsados     : {r['colapsados']}")
    print(f"medidos        : {r['medidos']}")
    print(f"excluidos_borda: {r['excluidos_borda']}")
    print(f"\n--- curva (excursao em ATR) ---")
    print(f"{'H':>4} | {'n':>5} | {'medMFE':>8} | {'medMAE':>8} | {'med(MFE-MAE)':>12}")
    for row in r["curva"]:
        print(f"{row['H']:>4} | {row['n']:>5} | {row['med_mfe']:>8.3f} | "
              f"{row['med_mae']:>8.3f} | {row['med_diff']:>12.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
