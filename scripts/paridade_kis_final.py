"""scripts/paridade_kis_final.py — o v10 roda o MESMO setup que o sandbox mediu?

Reprocessa o período NOVO barra a barra pelo runner do v10 (ficha
`kis_3489_final_4h`) e compara, trade a trade, com o CSV de trades do
`backtest/kis_grade.py` (célula `k3_p3-6_f50-25_t0`, período `NOVO`).

- Velas: `backtest/candles_v9.db` (4h), lidas UMA vez por símbolo, aberto só
  para leitura, guardadas em arrays e servidas ao runner por `velas_fn` —
  as últimas `janela_barras` fechadas antes de `agora_ms`.
- Journal: banco TEMPORÁRIO (default em /tmp). Recusa o de produção e
  recusa arquivo que já existe: começa sem posição aberta.
- Relógio: `agora_ms` = fechamento de cada barra de 4h, da primeira barra de
  entrada do NOVO (2026-08-31 16:00 UTC) até velas < 2026-11-14 16:00.
- Duas passadas no mesmo banco (config_hash distintos): `alav` desligado
  (paridade) e ligado, como na ficha (só conta liquidações e custo).

Uso (VM, raiz do repositório):
    python3 -m scripts.paridade_kis_final --ref logs/kis_grade_20261125_0305.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import resource
import sqlite3
import sys
import time
from array import array
from bisect import bisect_right
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.candle_store import read_candles               # noqa: E402
from config import JOURNAL_DB_V9                              # noqa: E402
from v10.registro import KIS_3489_FINAL_4H                    # noqa: E402
from v10.runner import _bar_ms, rodar                         # noqa: E402
from v10.schema import TABELA, connect                        # noqa: E402

CELULA, PERIODO = "k3_p3-6_f50-25_t0", "NOVO"
INI_ISO, END_ISO = "2026-08-31 16:00", "2026-11-14 16:00"
COST_REF_BPS = 6.0          # o `ret6` do sandbox: bruto - 6 bps por trade
TOL_BPS = 0.006             # o CSV grava ret6 com 2 casas
FINAL = {"stop": "stop", "flip": "inversao", "liquidacao": "liquidacao"}


def _ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
               .timestamp() * 1000)


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime(
        "%Y-%m-%d %H:%M")


def _rss() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


class Fonte:
    """velas_fn do runner com cache em memória: arrays por símbolo."""

    def __init__(self, store: str, symbols: list, end_ms: int):
        conn = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
        self.d, self.sem, self.agora_ms = {}, [], None
        try:
            for s in symbols:
                c = read_candles(conn, s, "4h", end_ms=end_ms - 1)
                if not c:
                    self.sem.append(s)
                    continue
                self.d[s] = tuple(array("q" if k == "ts" else "d",
                                        (x[k] for x in c))
                                  for k in ("ts", "open", "high", "low",
                                            "close"))
                del c
        finally:
            conn.close()

    def __call__(self, symbol: str, tf: str, n: int) -> list:
        if symbol not in self.d:
            return []
        ts, o, h, lo, c = self.d[symbol]
        b = bisect_right(ts, self.agora_ms - _bar_ms(tf))
        return [{"ts": ts[k], "open": o[k], "high": h[k], "low": lo[k],
                 "close": c[k], "volume": 0.0} for k in range(max(0, b - n), b)]


def passada(spec, conn, fonte, ini_ms, end_ms, bar, teto_rss, rot) -> dict:
    log = logging.getLogger("paridade")
    log.addHandler(logging.NullHandler())
    log.propagate = False
    falhas, t0, n = set(), time.monotonic(), 0
    for bar_ts in range(ini_ms, end_ms - bar + 1, bar):
        fonte.agora_ms = bar_ts + bar
        r = rodar(spec, conn, fonte.agora_ms, velas_fn=fonte, log=log)
        falhas.update(r["falha_symbols"])
        n += 1
        if _rss() > teto_rss:
            raise MemoryError(f"RSS {_rss():.0f} MiB > teto {teto_rss:.0f} "
                              f"em {_iso(bar_ts)}; PARANDO.")
        if n % 60 == 0:
            print(f"  [{rot}] {_iso(bar_ts)} | {time.monotonic() - t0:.0f} s"
                  f" | RSS {_rss():.0f} MiB", flush=True)
    return {"barras": n, "falhas": sorted(falhas),
            "seg": time.monotonic() - t0}


def trades_v10(conn, spec, bar: int) -> dict:
    import json
    out = {}
    for r in conn.execute(
            f"SELECT symbol, direction, entry_ts, status, exit_ts, pnl_pct, "
            f"exit_state_json FROM {TABELA} WHERE config_hash=?",
            (spec.config_hash,)):
        s = json.loads(r["exit_state_json"] or "{}")
        ent = int(r["entry_ts"])
        parc = "+".join(p["nome"] for p in s.get("parciais", []))
        if r["status"] == "OPEN":
            # Aberto na ÚLTIMA barra nunca passou por `avaliar`: sem marca.
            # O sandbox marca no próprio close da entrada (hold 0, bruto 0).
            fim, pnl = "fim", s.get("pnl_corrente_pct") or 0.0
            hold = (int(s.get("ultimo_ts_ms") or ent) - ent) // bar
        else:
            fim, pnl = FINAL[s["motivo"]], r["pnl_pct"]
            hold = (int(r["exit_ts"]) - ent) // bar
        out[(r["symbol"], ent, r["direction"])] = {
            "aberto": r["status"] == "OPEN", "hold": hold,
            "saida": "+".join(x for x in (parc, fim) if x),
            "ret6": None if pnl is None else float(pnl) * 100 - COST_REF_BPS}
    return out


def trades_ref(path: str, celula: str, periodo: str) -> dict:
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r["celula"] != celula or r["periodo"] != periodo:
                continue
            out[(r["sym"], _ms(r["bar_ts_iso"]), r["direcao"])] = {
                "aberto": r["saida"].split("+")[-1] == "fim",
                "hold": int(r["hold"]), "saida": r["saida"],
                "ret6": float(r["ret6"])}
    return out


def compara(meus: dict, ref: dict) -> None:
    for nome, aberto in (("fechados", False), ("abertos no fim", True)):
        a = {k: v for k, v in meus.items() if v["aberto"] is aberto}
        b = {k: v for k, v in ref.items() if v["aberto"] is aberto}
        com = sorted(set(a) & set(b))
        difs = [abs(a[k]["ret6"] - b[k]["ret6"]) for k in com]
        iguais = [k for k, d in zip(com, difs)
                  if d <= TOL_BPS and a[k]["saida"] == b[k]["saida"]
                  and a[k]["hold"] == b[k]["hold"]]
        print(f"  {nome}: iguais {len(iguais)} de {len(b)} | só no v10 "
              f"{len(set(a) - set(b))} | só no sandbox {len(set(b) - set(a))}"
              f" | max|dif| ret6 {max(difs, default=0.0):.4f} bps")
        dif = [k for k in com if k not in set(iguais)]
        for k in dif[:20]:
            print(f"    DIF {k[0]} {_iso(k[1])} {k[2]}: v10 {a[k]} | "
                  f"sandbox {b[k]}")
        for lado, ks in (("só no v10", set(a) - set(b)),
                         ("só no sandbox", set(b) - set(a))):
            for k in sorted(ks)[:20]:
                print(f"    {lado}: {k[0]} {_iso(k[1])} {k[2]} "
                      f"{(a if lado == 'só no v10' else b)[k]}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="paridade kis_final x kis_grade")
    ap.add_argument("--ref", required=True, help="CSV de trades do kis_grade")
    ap.add_argument("--celula", default=CELULA)
    ap.add_argument("--store", default=str(ROOT / "backtest" / "candles_v9.db"))
    ap.add_argument("--journal", default=None,
                    help="banco temporário (default /tmp/paridade_kis_final_"
                         "<hora>.db); nunca o de produção")
    ap.add_argument("--ini", default=INI_ISO)
    ap.add_argument("--end", default=END_ISO, help="exclusivo, UTC")
    ap.add_argument("--symbols", nargs="+", default=None)
    ap.add_argument("--teto-rss", type=float, default=250.0, help="MiB")
    a = ap.parse_args(argv)
    jornal = a.journal or (f"/tmp/paridade_kis_final_"
                           f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.db")
    if os.path.abspath(jornal) == os.path.abspath(JOURNAL_DB_V9):
        ap.error("--journal aponta para o journal de produção")
    if os.path.exists(jornal):
        ap.error(f"{jornal} já existe: a conferência começa sem posição aberta")
    spec_liq = KIS_3489_FINAL_4H
    spec = replace(spec_liq, exit_params={
        k: v for k, v in spec_liq.exit_params.items() if k not in ("alav",
                                                                   "mmr")})
    bar, ini, end = _bar_ms(spec.tf), _ms(a.ini), _ms(a.end)
    symbols = a.symbols or list(spec.symbols)
    t0 = time.monotonic()
    fonte = Fonte(a.store, symbols, end)
    spec = replace(spec, symbols=symbols)
    spec_liq = replace(spec_liq, symbols=symbols)
    print(f"[paridade] {spec.setup_id} | {len(symbols)} símbolos (sem velas: "
          f"{' '.join(fonte.sem) or 'nenhum'}) | agora de {_iso(ini + bar)} "
          f"a {_iso(end)} | journal {jornal} | leitura "
          f"{time.monotonic() - t0:.0f} s, RSS {_rss():.0f} MiB")
    conn = connect(jornal)
    try:
        p1 = passada(spec, conn, fonte, ini, end, bar, a.teto_rss, "paridade")
        p2 = passada(spec_liq, conn, fonte, ini, end, bar, a.teto_rss, "alav")
        meus, liq = trades_v10(conn, spec, bar), trades_v10(conn, spec_liq, bar)
    finally:
        conn.close()
    print(f"  passadas: {p1['barras']} barras cada | {p1['seg']:.0f} s + "
          f"{p2['seg']:.0f} s | símbolo com falha em alguma barra: "
          f"{' '.join(p1['falhas']) or 'nenhum'}")
    print(f"\n===== PARIDADE (alav desligado) x {a.celula} {PERIODO} =====")
    compara(meus, trades_ref(a.ref, a.celula, PERIODO))
    alav, mao = spec_liq.exit_params["alav"], spec_liq.exit_params["mao_usdt"]
    lq = {k: v for k, v in liq.items() if "liquidacao" in v["saida"]}
    custo = sum(v["ret6"] - meus[k]["ret6"] for k, v in lq.items()
                if k in meus and v["ret6"] is not None
                and meus[k]["ret6"] is not None)
    print(f"\n===== LIQUIDAÇÃO (alav {alav}, mmr "
          f"{spec_liq.exit_params['mmr']}) =====")
    print(f"  liquidações {len(lq)} de {len(liq)} trades | efeito no "
          f"resultado (com liquidação - sem) {custo:+.1f} bps somados = "
          f"{custo / 1e4 * mao:+.2f} USD com mão de {mao} USD (negativo = a "
          "liquidação custou; positivo = perdeu menos que o stop) — a conta "
          "do liq5_usd do kis_grade")
    for k in sorted(lq):
        print(f"    {k[0]} {_iso(k[1])} {k[2]}: {lq[k]['saida']} "
              f"ret6 {lq[k]['ret6']:+.1f} (sem alav "
              f"{meus.get(k, {}).get('ret6')})")
    print(f"\n  RSS de pico {_rss():.0f} MiB (teto {a.teto_rss:.0f}) | "
          f"{time.monotonic() - t0:.0f} s")
    print("caveat: conferência por reprocessamento, sem veredito. EMAs do "
          "v10 sobre janela de 1000 barras, as do sandbox sobre o histórico "
          "inteiro (diferença ~1e-9 relativa). ret6 do CSV tem 2 casas: igual "
          f"= mesma saída, mesmo hold e |dif| <= {TOL_BPS} bps. Abertos no fim:"
          " v10 marca pernas realizadas + resto no último close, como o "
          "sandbox. Saída no relógio de 4h sem run perdida (barras_atraso 0). "
          "Liquidação: margem isolada, perda da margem da fração restante; "
          "custo da perna de liquidação no preço de entrada (o kis_grade usa "
          "o de liquidação: < 1 bp por liquidação).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
