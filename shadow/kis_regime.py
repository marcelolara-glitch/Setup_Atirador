# shadow/kis_regime.py — COLETOR forward do kis_extremos + PORTAO DE REGIME.
# ISTO NAO E PROMOCAO: a celula congelada (limiar 0.02 / ADX 11, 6 tokens) foi
# REPROVADA no hold-out de 65 dias — -769 bps em 66 trades, 1 de 6 tokens
# positivo, contra criterio travado ANTES do numero (ledger 21/08). O SINAL esta
# morto pelo criterio; o que sobreviveu foi o PORTAO (-769 contra -4.162 bps do
# controle sem filtro, em dado virgem), e portao nao se mede sozinho. Este shadow
# existe para ACUMULAR TRADES REAIS, nao porque a estrategia foi aprovada.
# SEM STOP, SEM ALVO, SEM CAP DE TEMPO: posicao sempre-no-mercado, so inverte —
# a cauda de perda de um trade e ILIMITADA. COLETOR DE DADOS, NAO desenho
# operavel. Runtime v9 e DONCHIAN-A INTOCADOS; DB proprio no MESMO schema de
# shadow_trades (reusa _connect/_fetch_confirmed); detector e portao IMPORTADOS
# da bancada (stdlib puro) p/ nao divergirem da celula congelada em silencio.
from __future__ import annotations
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backtest.keepitsimple import WARMUP, _adx, _alvo_extremos, states  # noqa: E402
from backtest.kis_regime import inclinacoes, passa                      # noqa: E402
from shadow.donchian_a import (BAR_MS, LOG_DIR, _connect,               # noqa: E402
                               _fetch_confirmed)

LIMIAR, ADX_MIN = 0.02, 11      # celula congelada em 21/08 — NAO se ajusta aqui
SYMBOLS = ["ARBUSDT", "BNBUSDT", "BTCUSDT", "SUIUSDT", "TRXUSDT", "WLDUSDT"]
TF = "4h"
MIN_BARS = 60                   # ADX(13) fecha em 2n-1=25; folga p/ EMA21+ATR14
DB_PATH = str(ROOT / "journal" / "shadow_kis_regime.db")


def _setup_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)          # log proprio: nao suja o do 9A
    log = logging.getLogger("shadow_kis_regime")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(os.path.join(
            LOG_DIR, f"shadow_kis_{datetime.now(timezone.utc):%Y%m%d}.log"),
            encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(fh)
    return log


def avaliar(candles: list) -> tuple:
    """ULTIMA barra fechada -> (alvo_vigente, direction, incl, adx). `alvo` sai de
    `_alvo_extremos` (EMA 8/21, confirmacao >= 2 barras); `direction` so e nao-None
    quando o alvo MUDOU nesta barra — a inversao e a unica entrada do detector."""
    closes = [c["close"] for c in candles]
    alvo = _alvo_extremos(states(closes))
    i = len(closes) - 1
    if i < max(WARMUP, 1):
        return 0, None, None, None
    d = None
    if alvo[i] != alvo[i - 1] and alvo[i] != 0:
        d = "LONG" if alvo[i] > 0 else "SHORT"
    return alvo[i], d, inclinacoes(candles, closes)[i], _adx(candles)[i]


def _fechar(conn, symbol, alvo_now, bar, log):
    """Saida = inversao do sinal oposto, no FECHAMENTO da barra. Fecha todo OPEN
    que contraria o alvo VIGENTE, nao so na barra da inversao: run perdida atrasa
    a saida, nunca prende o trade. status WIN/LOSS pelo BRUTO (liquido no
    relatorio); sem stop/alvo/expiry essas colunas ficam NULL de proposito."""
    for r in conn.execute("SELECT * FROM shadow_trades WHERE symbol=? AND "
                          "status='OPEN'", (symbol,)).fetchall():
        if alvo_now == 0 or alvo_now == (1 if r["direction"] == "LONG" else -1):
            continue
        move = ((bar["close"] - r["entry_price"]) if r["direction"] == "LONG"
                else (r["entry_price"] - bar["close"]))
        pnl = move / r["entry_price"] * 100.0 if r["entry_price"] else 0.0
        conn.execute("UPDATE shadow_trades SET status=?, exit_ts=?, exit_price=?,"
                     " pnl_pct=? WHERE id=?",
                     ("WIN" if move > 0 else "LOSS", str(bar["ts"]),
                      bar["close"], pnl, r["id"]))
        conn.commit()
        log.info(f"  [{symbol}] {r['direction']} -> saida {pnl:+.3f}% bruto")


def _abrir(conn, symbol, direction, incl, adx, bar, log):
    """Portao na barra do sinal; warmup (None) VETA. Vetar NAO adia: a proxima
    entrada e a proxima inversao (oposta) e a carteira fica FORA o intervalo
    inteiro. Idempotente por (symbol, bar_ts_entry)."""
    if not passa(direction, incl, adx, LIMIAR, ADX_MIN):
        log.info(f"  [{symbol}] {direction} VETADO pelo portao "
                 f"(incl={incl} adx={adx})")
        return
    ev = json.dumps({"inclinacao": incl, "adx": adx, "limiar": LIMIAR,
                     "adx_min": ADX_MIN, "close": bar["close"], "tf": TF})
    try:
        conn.execute(
            "INSERT INTO shadow_trades (id, symbol, direction, bar_ts_entry, "
            "entry_price, status, evidence_json) VALUES (?,?,?,?,?,'OPEN',?)",
            (f"{symbol}-{bar['ts']}", symbol, direction, str(bar["ts"]),
             bar["close"], ev))
        conn.commit()
        log.info(f"  [{symbol}] OPEN {direction} @ {bar['close']} "
                 f"incl={incl:.4f} adx={adx:.1f}")
    except sqlite3.IntegrityError:
        pass


def run_once(db_path: str = DB_PATH, fetch_fn=None, symbols=None, log=None):
    """Por simbolo (try/except isolado): fetch -> fecha o que inverteu -> abre se
    houver sinal E o portao deixar. Falha e SILENCIOSA: warning, nunca interrompe.
    -> (ok, falhas)."""
    log = log or _setup_logger()
    fetch_fn = fetch_fn or _fetch_confirmed
    symbols = symbols or SYMBOLS
    run_ts = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    ok = falhas = 0
    falha_symbols = []
    for symbol in symbols:
        try:
            closed = fetch_fn(symbol)
            if len(closed) < MIN_BARS:
                raise ValueError(f"barras insuficientes: {len(closed)}<{MIN_BARS}")
            alvo_now, direction, incl, adx = avaliar(closed)
            _fechar(conn, symbol, alvo_now, closed[-1], log)
            if direction is not None:
                _abrir(conn, symbol, direction, incl, adx, closed[-1], log)
            ok += 1
        except Exception as e:
            falhas += 1
            falha_symbols.append(symbol)
            log.warning(f"  [{symbol}] falha: {type(e).__name__}: {e}")
    try:
        conn.execute("INSERT OR REPLACE INTO shadow_runs (run_ts, ok, falhas, "
                     "falha_symbols) VALUES (?,?,?,?)",
                     (run_ts, ok, falhas, json.dumps(falha_symbols)))
        conn.commit()
    except Exception as e:
        log.warning(f"  shadow_runs indisponivel: {type(e).__name__}: {e}")
    conn.close()
    log.info(f"[kis] run_once fim — ok={ok} falhas={falhas}")
    return ok, falhas


def build_block(conn, now: datetime) -> str:
    """SEGUNDO bloco do [VIGIA], mesmo formato do DONCHIAN-A. So REPORTA. Onde o
    DONCHIAN-A mostra R, aqui vai bps: SEM stop nao existe unidade de risco e
    imprimir R seria inventar denominador. liquido = bruto - 2x TAKER_FEE_BPS (uma
    perna por ponta); spread NAO medido aqui, entao o liquido e OTIMISTA — dito na
    linha, nao omitido."""
    from shadow.vigia import (EXPECTED_RUNS_PER_DAY, TAKER_FEE_BPS,  # noqa: E402
                              _fmt_bps, _utc_date_of_ms)
    now = now.astimezone(timezone.utc)
    now_ms, day = (int(now.timestamp() * 1000),
                   (now - timedelta(days=1)).date().isoformat())
    trades = conn.execute("SELECT symbol, direction, bar_ts_entry, status, "
                          "exit_ts, pnl_pct FROM shadow_trades").fetchall()
    runs = conn.execute("SELECT run_ts FROM shadow_runs").fetchall()

    def _liq(t):                                   # bruto(bps) - 2 pernas taker
        return (t["pnl_pct"] or 0.0) * 100.0 - 2.0 * TAKER_FEE_BPS
    res = [t for t in trades if t["status"] in ("WIN", "LOSS")]
    wins = sum(1 for t in res if t["status"] == "WIN")
    lg = [_liq(t) for t in res if t["direction"] == "LONG"]
    sh = [_liq(t) for t in res if t["direction"] == "SHORT"]
    starts = [r["run_ts"][:10] for r in runs] + [
        _utc_date_of_ms(t["bar_ts_entry"]) for t in trades if t["bar_ts_entry"]]
    obs = (max(1, (datetime.fromisoformat(day).date()
                   - datetime.fromisoformat(min(starts)).date()).days + 1)
           if starts else 0)
    opens = sorted((t for t in trades if t["status"] == "OPEN"),
                   key=lambda x: int(x["bar_ts_entry"]))
    fech = sorted((t for t in res if t["exit_ts"]),
                  key=lambda x: int(x["exit_ts"]), reverse=True)[:5]
    n_runs = sum(1 for r in runs if r["run_ts"][:10] == day)
    hoje = [t for t in res if t["exit_ts"] and _utc_date_of_ms(t["exit_ts"]) == day]

    def _linha(t):                                 # 1 trade fechado, mesma forma
        return (f"  {t['symbol']} {t['direction']} {t['status']} "
                f"{_fmt_bps(_liq(t))} bps")
    L = [f"🧪 <b>[VIGIA] KIS+REGIME</b> — {day} (UTC)", "",
         "  ⚠️ COLETOR: hold-out REPROVADO (-769 bps, 21/08) e SEM stop (cauda "
         "ilimitada). Observação, não promoção.",
         f"<b>ACUMULADO</b> · dia {obs} · fechados {len(res)}",
         f"  WIN {wins} · LOSS {len(res) - wins} · Σlíq "
         f"{_fmt_bps(sum(lg) + sum(sh))} bps (2×{TAKER_FEE_BPS:.0f} taker; "
         f"spread NÃO medido)",
         f"  LONG n={len(lg)} Σ {_fmt_bps(sum(lg))} · SHORT n={len(sh)} Σ "
         f"{_fmt_bps(sum(sh))} bps",
         "", f"<b>ABERTAS</b> ({len(opens)})"]
    L += [f"  {t['symbol']} {t['direction']} · "
          f"{(now_ms - int(t['bar_ts_entry'])) // BAR_MS}b" for t in opens] or \
        ["  — nenhuma"]
    L += ["", "<b>FECHADOS</b> (recentes)"]
    L += [f"{_linha(t)} · {_utc_date_of_ms(t['exit_ts'])}" for t in fech] or \
        ["  — nenhum"]
    L += ["", f"<b>HOJE</b> ({day})", f"  runs {n_runs}/{EXPECTED_RUNS_PER_DAY}"]
    if n_runs < EXPECTED_RUNS_PER_DAY:
        L.append(f"  ⚠️ cobertura incompleta: {EXPECTED_RUNS_PER_DAY - n_runs} "
                 f"barra(s) nao avaliada(s) — entrada(s) perdida(s) em silencio")
    L += [_linha(t) for t in hoje] or ["  resolucoes: nenhuma"]
    return "\n".join(L)


if __name__ == "__main__":
    run_once()
