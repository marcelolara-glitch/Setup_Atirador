# shadow/vigia.py — relatorio DIARIO [VIGIA] do shadow DONCHIAN-A (ledger
# 19/07 "EMENDA ao protocolo Estagio 2": diario via Telegram, tag [VIGIA],
# "acumulado no topo, abertas no meio, hoje no rodape"; regras de veredito
# INALTERADAS — este modulo so REPORTA, nunca decide/ajusta). Runtime v9
# INTOCADO. Le apenas o DB do shadow (shadow_trades + shadow_runs, PR-9A/9B).
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

# Reusa as constantes/infra do motor (PR-9A) — nao duplica DB_PATH/BAR_MS/DDL.
from shadow.donchian_a import BAR_MS, DB_PATH, _connect, _setup_logger

RESOLVED = ("WIN", "LOSS", "EXPIRED")
EXPECTED_RUNS_PER_DAY = 6      # cron 4h pos-fechamento: 0,4,8,12,16,20 UTC
WINDOW_DAYS, WINDOW_SIGNALS = 84, 80   # protocolo 18/07: min 12 semanas OU 80 fechados

# --- custo executavel (FASE 0, ledger research/ledger.md) --------------------
# FASE 0.1c — premissa de custo por perna (VERBATIM, ledger:323-325): "CUSTO:
# medido, nao assumido. Tres cenarios reportados — (i) EXECUTAVEL (cruza o
# spread + taxa taker nos dois lados; o cenario 'qualquer um consegue')". Logo,
# por trade: cruza o spread (medido) + taxa TAKER, nos DOIS lados (entrada+saida).
# spread round-trip vs mid = 1x o spread medido (metade por perna); + 2x taker.
# So o spread da ENTRADA e capturado -> usado como proxy do round-trip (saida ~=
# entrada); disclosure no relatorio.
TAKER_FEE_BPS = 5.0     # OKX perp taker padrao (nao-VIP; tier "qualquer um
#   consegue" da premissa 18/07). NAO fixado numericamente no ledger: constante
#   auditavel/vetavel — o ledger fixa a CONVENCAO (taker, spread inteiro nos
#   dois lados), nao a taxa. Alterar => recompoe a "media liquida".
# FASE 0.1b — referencia da pesquisa p/ o delta (ledger:300 "bracket: EV@6
# +88.6" da FASE A, entrada 18/07; reafirmado ledger:336 "expectativa da
# pesquisa (+88.6 bps @6)"). UNIDADE: bps por trade.
REF_EV_BPS = 88.6


def _utc_date_of_ms(ms_str) -> str:
    # exit_ts/bar_ts_entry sao epoch-ms gravados como TEXT (str(ts)).
    return datetime.fromtimestamp(int(ms_str) / 1000, timezone.utc).date().isoformat()


def _fmt_r(r) -> str:
    return f"{r:+.2f}R" if r is not None else "n/a"


def _fmt_bps(x, sign=True) -> str:
    if x is None:
        return "n/a"
    return f"{x:+.1f}" if sign else f"{x:.1f}"


def _median(xs):
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return None
    m = n // 2
    return s[m] if n % 2 else (s[m - 1] + s[m]) / 2.0     # par: media dos centrais


def _exec_of(row):
    # exec capturado (PR-9C) fica em evidence_json.exec; ausente/ilegivel -> None.
    try:
        return (json.loads(row["evidence_json"]) or {}).get("exec")
    except (TypeError, ValueError):
        return None


def build_report(conn, now: datetime) -> str:
    """Compoe a mensagem [VIGIA] (HTML) a partir do DB. Puro: sem I/O de rede.
    `now` = instante da execucao (UTC). O dia observado e o UTC FECHADO = D-1
    (o cron dispara 00:20 UTC, logo o dia anterior ja fechou por completo)."""
    now = now.astimezone(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    day = (now - timedelta(days=1)).date().isoformat()

    trades = conn.execute(
        "SELECT symbol, direction, bar_ts_entry, status, exit_ts, r_multiple, "
        "pnl_pct, evidence_json FROM shadow_trades").fetchall()
    runs = conn.execute("SELECT run_ts, ok, falhas FROM shadow_runs").fetchall()

    # --- acumulado (desde o inicio da observacao) ---------------------------
    resolved = [t for t in trades if t["status"] in RESOLVED]
    n = len(resolved)
    by_status = {s: sum(1 for t in resolved if t["status"] == s) for s in RESOLVED}
    sum_r = sum(t["r_multiple"] or 0.0 for t in resolved)

    # --- macro LONG/SHORT + R mediano (B3) ----------------------------------
    def _dir(d):
        rs = [t["r_multiple"] or 0.0 for t in resolved if t["direction"] == d]
        return len(rs), sum(rs)
    nl, rl = _dir("LONG")
    ns, rs_ = _dir("SHORT")
    r_med = _median([t["r_multiple"] or 0.0 for t in resolved])

    # --- execucao: spread mediano + media liquida executavel + delta (B1) ---
    # spread mediano: sobre TODOS os trades com exec (propriedade de entrada).
    spreads = [ex["spread_bps"] for t in trades
               if (ex := _exec_of(t)) is not None]
    spread_med = _median(spreads)
    # media liquida: so RESOLVIDOS com exec. liquido = bruto(bps) - spread(round-
    # trip, proxy=entrada) - 2x taker (premissa FASE 0.1c). Sem exec: nunca
    # imputado -> conta em k.
    nets, k_sem = [], 0
    for t in resolved:
        ex = _exec_of(t)
        if ex is None:
            k_sem += 1
            continue
        gross_bps = (t["pnl_pct"] or 0.0) * 100.0        # 1% = 100 bps
        nets.append(gross_bps - ex["spread_bps"] - 2.0 * TAKER_FEE_BPS)
    avg_net = (sum(nets) / len(nets)) if nets else None   # media aritmetica dos liquidos
    delta = (avg_net - REF_EV_BPS) if avg_net is not None else None
    # dias de observacao: do 1o run (fallback 1o trade) ate o dia observado.
    starts = [r["run_ts"][:10] for r in runs] + [
        _utc_date_of_ms(t["bar_ts_entry"]) for t in trades if t["bar_ts_entry"]]
    if starts:
        d0 = datetime.fromisoformat(min(starts)).date()
        d1 = datetime.fromisoformat(day).date()
        obs_days = max(1, (d1 - d0).days + 1)
    else:
        obs_days = 0

    # --- abertas (OPEN, no meio) --------------------------------------------
    opens = [t for t in trades if t["status"] == "OPEN"]

    # --- hoje (dia UTC fechado, no rodape) ----------------------------------
    runs_today = [r for r in runs if r["run_ts"][:10] == day]
    n_runs = len(runs_today)
    res_today = [t for t in resolved if t["exit_ts"] and _utc_date_of_ms(t["exit_ts"]) == day]

    # --- fechados recentes (B2): ultimos 5 por exit_ts desc ------------------
    fechados = sorted((t for t in resolved if t["exit_ts"]),
                      key=lambda x: int(x["exit_ts"]), reverse=True)[:5]

    # --- radar (B4): snapshot de watchlist do ULTIMO run --------------------
    last_snap = conn.execute(
        "SELECT symbol, dist_high_bps, dist_low_bps FROM shadow_watchlist WHERE "
        "run_ts=(SELECT MAX(run_ts) FROM shadow_watchlist)").fetchall()
    radar = []
    for w in last_snap:                                  # lado mais proximo por simbolo
        hi, lo = w["dist_high_bps"], w["dist_low_bps"]
        side, dist = ("topo", hi) if hi <= lo else ("fundo", lo)
        radar.append((w["symbol"], side, dist))
    radar = sorted(radar, key=lambda x: x[2])[:3]        # 3 mais proximos, cap fixo

    # --- montagem HTML -------------------------------------------------------
    L = [f"🛡️ <b>[VIGIA] DONCHIAN-A</b> — {day} (UTC)", ""]
    L.append(f"<b>ACUMULADO</b> · dia {obs_days}/{WINDOW_DAYS} · "
             f"fechados {n}/{WINDOW_SIGNALS}")
    L.append(f"  WIN {by_status['WIN']} · LOSS {by_status['LOSS']} · "
             f"EXPIRED {by_status['EXPIRED']} · ΣR {sum_r:+.2f}")
    L.append(f"  LONG n={nl} ΣR {rl:+.2f} · SHORT n={ns} ΣR {rs_:+.2f} · "
             f"R̃ {_fmt_r(r_med)}")                        # B3 macro
    L.append(f"  líq exec {_fmt_bps(avg_net)} bps · spread~ "
             f"{_fmt_bps(spread_med, sign=False)} bps · Δpesq {_fmt_bps(delta)} "
             f"bps · sem medição: {k_sem}")                # B1 (ref +88.6 bps @6)
    L.append("")
    L.append(f"<b>ABERTAS</b> ({len(opens)})")
    if opens:
        for t in sorted(opens, key=lambda x: int(x["bar_ts_entry"])):
            age = (now_ms - int(t["bar_ts_entry"])) // BAR_MS
            L.append(f"  {t['symbol']} {t['direction']} · {age}b")
    else:
        L.append("  — nenhuma")
    L.append("")
    L.append(f"<b>FECHADOS</b> (recentes)")               # B2, entre ABERTAS e HOJE
    if fechados:
        for t in fechados:
            L.append(f"  {t['symbol']} {t['direction']} {t['status']} "
                     f"{_fmt_r(t['r_multiple'])} · {_utc_date_of_ms(t['exit_ts'])}")
    else:
        L.append("  — nenhum")
    L.append("")
    L.append(f"<b>HOJE</b> ({day})")
    L.append(f"  runs {n_runs}/{EXPECTED_RUNS_PER_DAY}")
    if n_runs < EXPECTED_RUNS_PER_DAY:
        miss = EXPECTED_RUNS_PER_DAY - n_runs
        L.append(f"  ⚠️ cobertura incompleta: {miss} barra(s) nao avaliada(s) — "
                 f"entrada(s) potencialmente perdida(s) em silencio")
    if res_today:
        for t in res_today:
            L.append(f"  {t['symbol']} {t['direction']} {t['status']} "
                     f"{_fmt_r(t['r_multiple'])}")
    else:
        L.append("  resolucoes: nenhuma")
    if radar:                                            # B4: omite se snapshot ausente
        L.append("")
        L.append("<b>RADAR</b> (observação, sem ação)")   # nota na 1a ocorrencia do dia
        for sym, side, dist in radar:
            L.append(f"  {sym} {side} · {dist:.0f} bps")
    return "\n".join(L)


def _default_send(text: str) -> bool:
    # Reusa o transporte privado _tg_send do telegram.py (mesmo canal): notify_*
    # sao formatadores de mensagens estruturadas; o [VIGIA] e observabilidade
    # livre. Nao toca telegram.py. Import lazy: testes sem numpy/pandas_ta.
    from telegram import _tg_send
    return _tg_send(text)


def run(db_path: str = DB_PATH, now: datetime | None = None,
        send_fn=None, log: logging.Logger | None = None):
    """Gera e envia o [VIGIA] uma vez. Envio e I/O de observabilidade: falha
    vira warning, NUNCA excecao propagada (principio vigente)."""
    log = log or _setup_logger()
    now = now or datetime.now(timezone.utc)
    send_fn = send_fn or _default_send
    conn = _connect(db_path)
    try:
        msg = build_report(conn, now)
    finally:
        conn.close()
    try:
        ok = bool(send_fn(msg))
        if not ok:
            log.warning("[vigia] envio retornou False — relatorio nao entregue")
    except Exception as e:
        log.warning(f"[vigia] falha no envio: {type(e).__name__}: {e}")
        ok = False
    log.info(f"[vigia] relatorio gerado (enviado={ok})")
    return msg, ok


if __name__ == "__main__":
    run()
