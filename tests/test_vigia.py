"""Testes do relatorio diario [VIGIA] (shadow/vigia.py) e da emenda shadow_runs
em shadow/donchian_a.py (PR-9B).

Stdlib-only (topo de vigia/donchian_a e stdlib; telegram e import lazy dentro
de _default_send). Nao dependem de pandas_ta/numpy. Cobrem, conforme briefing:
  - composicao da mensagem com DB fixture: dia 6/6 runs (sem alerta), dia 4/6
    (linha de alerta presente), dia sem resolucoes, OPEN com idade correta;
  - shadow_runs gravada por run_once;
  - falha de envio nao propaga excecao (I/O de observabilidade silencioso).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from shadow import donchian_a as sd
from shadow import vigia

_NO_TICKER = lambda s: None                       # noqa: E731  offline: exec=null

BAR = sd.BAR_MS
QUIET = logging.getLogger("vigia_test")           # sem FileHandler: nao suja logs/
QUIET.addHandler(logging.NullHandler())

NOW = datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc)   # cron 00:20 UTC
DAY = "2026-07-18"                                          # dia UTC fechado (D-1)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _seed_conn(tmp_path):
    conn = sd._connect(str(tmp_path / "s.db"))
    return conn


def _add_run(conn, iso_ts, ok=20, falhas=0):
    conn.execute("INSERT OR REPLACE INTO shadow_runs (run_ts, ok, falhas) "
                 "VALUES (?,?,?)", (iso_ts, ok, falhas))
    conn.commit()


def _add_trade(conn, tid, symbol="BTCUSDT", direction="LONG", entry_ms=0,
               status="OPEN", exit_ms=None, r=None, pnl_pct=None, spread_bps=None):
    # spread_bps=None => sem exec (evidence.exec null); senao grava snapshot exec.
    ex = None if spread_bps is None else {
        "bid": 99.9, "ask": 100.1, "mid": 100.0, "spread_bps": spread_bps,
        "ts_capture": 1}
    ev = json.dumps({"exec": ex})
    conn.execute(
        "INSERT INTO shadow_trades (id, symbol, direction, bar_ts_entry, "
        "entry_price, atr_at_entry, sl_price, tp_price, expiry_bar_ts, status, "
        "exit_ts, r_multiple, pnl_pct, evidence_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (tid, symbol, direction, str(entry_ms), 100.0, 1.0, 98.5, 106.0,
         str(entry_ms + sd.H_BARS * BAR), status,
         (str(exit_ms) if exit_ms is not None else None), r, pnl_pct, ev))
    conn.commit()


def _six_runs(conn, day=DAY):
    for h in (0, 4, 8, 12, 16, 20):
        _add_run(conn, f"{day}T{h:02d}:08:00+00:00")


# --- cobertura de runs -------------------------------------------------------
def test_runs_6_6_sem_alerta(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "runs 6/6" in msg
    assert "cobertura incompleta" not in msg


def test_runs_4_6_alerta_presente(tmp_path):
    conn = _seed_conn(tmp_path)
    for h in (0, 4, 8, 12):                        # so 4 das 6 barras avaliadas
        _add_run(conn, f"{DAY}T{h:02d}:08:00+00:00")
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "runs 4/6" in msg
    assert "cobertura incompleta: 2 barra" in msg
    assert "perdida" in msg.lower()


# --- OPEN com idade em barras ------------------------------------------------
def test_open_idade_em_barras(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    entry_ms = _ms(NOW) - 5 * BAR                   # exatamente 5 barras atras
    _add_trade(conn, "BTCUSDT-1", direction="SHORT", entry_ms=entry_ms)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "ABERTAS</b> (1)" in msg
    assert "BTCUSDT SHORT · 5b" in msg


def test_sem_abertas(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "ABERTAS</b> (0)" in msg
    assert "— nenhuma" in msg


# --- resolucoes do dia -------------------------------------------------------
def test_resolucoes_do_dia_com_r(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    exit_ms = _ms(datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc))
    _add_trade(conn, "ETHUSDT-1", symbol="ETHUSDT", status="WIN",
               entry_ms=_ms(datetime(2026, 7, 16, tzinfo=timezone.utc)),
               exit_ms=exit_ms, r=4.0)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "ETHUSDT LONG WIN +4.00R" in msg


def test_dia_sem_resolucoes(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    # resolucao de OUTRO dia (17/07) nao conta como "hoje".
    exit_ms = _ms(datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc))
    _add_trade(conn, "ETHUSDT-1", symbol="ETHUSDT", status="LOSS",
               entry_ms=_ms(datetime(2026, 7, 15, tzinfo=timezone.utc)),
               exit_ms=exit_ms, r=-1.0)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "resolucoes: nenhuma" in msg


# --- acumulado ---------------------------------------------------------------
def test_acumulado_conta_todas_resolucoes(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    base = _ms(datetime(2026, 7, 16, tzinfo=timezone.utc))
    _add_trade(conn, "A", symbol="AAA", status="WIN",
               entry_ms=base, exit_ms=base + BAR, r=4.0)
    _add_trade(conn, "B", symbol="BBB", status="LOSS",
               entry_ms=base, exit_ms=base + BAR, r=-1.0)
    _add_trade(conn, "C", symbol="CCC", status="EXPIRED",
               entry_ms=base, exit_ms=base + BAR, r=0.3)
    _add_trade(conn, "D", symbol="DDD", status="OPEN",
               entry_ms=_ms(NOW) - BAR)                   # OPEN nao conta
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "fechados 3/80" in msg                   # 3 resolvidos, OPEN excluido
    assert "WIN 1 · LOSS 1 · EXPIRED 1" in msg
    assert "ΣR +3.30" in msg


# --- shadow_runs gravada por run_once ---------------------------------------
def _long_series(n_channel=sd.N + 1):
    bars = [{"ts": k * BAR, "open": 100.0, "high": 100.5, "low": 99.5,
             "close": 100.0, "volume": 1.0} for k in range(n_channel)]
    brk_ts = n_channel * BAR
    bars.append({"ts": brk_ts, "open": 100.0, "high": 101.0, "low": 100.0,
                 "close": 101.0, "volume": 1.0})
    return bars


def test_run_once_grava_shadow_runs(tmp_path):
    db = str(tmp_path / "s.db")
    bars = _long_series()
    ok, falhas = sd.run_once(db, fetch_fn=lambda s: bars,
                             symbols=["BTCUSDT"], log=QUIET, ticker_fn=_NO_TICKER)
    assert (ok, falhas) == (1, 0)
    conn = sd._connect(db)
    rows = conn.execute("SELECT ok, falhas FROM shadow_runs").fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0]["ok"] == 1 and rows[0]["falhas"] == 0


# --- falha de envio nao propaga ----------------------------------------------
def test_envio_falho_nao_propaga(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    conn.close()

    def _boom(text):
        raise RuntimeError("telegram down")

    # run() nao pode levantar; deve retornar ok=False.
    msg, ok = vigia.run(str(tmp_path / "s.db"), now=NOW, send_fn=_boom, log=QUIET)
    assert ok is False
    assert "[VIGIA]" in msg


def test_envio_false_nao_propaga(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    conn.close()
    msg, ok = vigia.run(str(tmp_path / "s.db"), now=NOW,
                        send_fn=lambda t: False, log=QUIET)
    assert ok is False


def test_run_envia_e_reporta_ok(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    conn.close()
    capturado = {}
    msg, ok = vigia.run(str(tmp_path / "s.db"), now=NOW,
                        send_fn=lambda t: capturado.setdefault("t", t) or True,
                        log=QUIET)
    assert ok is True
    assert capturado["t"] == msg


# --- PR-9C B1: execucao (spread mediano, media liquida, delta) ---------------
def _add_watch(conn, symbol, dist_high, dist_low, run_ts="2026-07-18T20:08:00+00:00"):
    conn.execute("INSERT OR REPLACE INTO shadow_watchlist (run_ts, symbol, "
                 "dist_high_bps, dist_low_bps) VALUES (?,?,?,?)",
                 (run_ts, symbol, dist_high, dist_low))
    conn.commit()


def test_b1_exec_presente_liquida_e_delta(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    base = _ms(datetime(2026, 7, 16, tzinfo=timezone.utc))
    # dois WIN com exec: pnl 1.0% => 100 bps bruto; spread 20 => liq = 100-20-10 = 70.
    _add_trade(conn, "A", symbol="AAA", status="WIN", entry_ms=base,
               exit_ms=base + BAR, r=4.0, pnl_pct=1.0, spread_bps=20.0)
    _add_trade(conn, "B", symbol="BBB", status="WIN", entry_ms=base,
               exit_ms=base + BAR, r=4.0, pnl_pct=1.0, spread_bps=20.0)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "spread~ 20.0 bps" in msg
    assert "líq exec +70.0 bps" in msg               # 100 - 20 - 2*5
    assert "Δpesq -18.6 bps" in msg                  # 70 - 88.6
    assert "sem medição: 0" in msg


def test_b1_sem_exec_conta_k_e_na(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    base = _ms(datetime(2026, 7, 16, tzinfo=timezone.utc))
    _add_trade(conn, "A", symbol="AAA", status="WIN", entry_ms=base,
               exit_ms=base + BAR, r=4.0, pnl_pct=1.0)          # sem exec
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "sem medição: 1" in msg
    assert "líq exec n/a bps" in msg
    assert "Δpesq n/a bps" in msg


def test_b1_spread_mediano_impar_e_par(tmp_path):
    conn = _seed_conn(tmp_path)
    base = _ms(datetime(2026, 7, 16, tzinfo=timezone.utc))
    # 3 trades (impar): spreads 4,8,20 => mediana 8.
    for i, s in enumerate((4.0, 8.0, 20.0)):
        _add_trade(conn, f"o{i}", symbol=f"S{i}", status="OPEN",
                   entry_ms=base, pnl_pct=None, spread_bps=s)
    assert "spread~ 8.0 bps" in vigia.build_report(conn, NOW)
    _add_trade(conn, "o3", symbol="S3", status="OPEN", entry_ms=base,
               spread_bps=12.0)                                 # 4 trades (par): (8+12)/2=10
    assert "spread~ 10.0 bps" in vigia.build_report(conn, NOW)
    conn.close()


# --- PR-9C B2: FECHADOS (recentes), cap 5 ------------------------------------
def test_b2_fechados_cap_5_e_ordem(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    base = _ms(datetime(2026, 7, 10, tzinfo=timezone.utc))
    for i in range(7):                                          # 7 fechados
        _add_trade(conn, f"f{i}", symbol=f"S{i}", status="WIN", entry_ms=base,
                   exit_ms=base + (i + 1) * BAR, r=1.0 + i)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "<b>FECHADOS</b> (recentes)" in msg
    assert msg.count("R · 20") == 5                             # 5 linhas de fechado (cap fixo)
    assert "S6 LONG WIN +7.00R" in msg                         # o mais recente aparece
    assert "S1 LONG WIN" not in msg                            # o 6o mais antigo cai fora


def test_b2_fechados_vazio(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "<b>FECHADOS</b> (recentes)" in msg
    assert "— nenhum" in msg


# --- PR-9C B3: macro split LONG/SHORT + R mediano ----------------------------
def test_b3_macro_split(tmp_path):
    conn = _seed_conn(tmp_path)
    base = _ms(datetime(2026, 7, 16, tzinfo=timezone.utc))
    _add_trade(conn, "L1", symbol="LLL", direction="LONG", status="WIN",
               entry_ms=base, exit_ms=base + BAR, r=4.0)
    _add_trade(conn, "S1", symbol="SSS", direction="SHORT", status="LOSS",
               entry_ms=base, exit_ms=base + BAR, r=-1.0)
    _add_trade(conn, "S2", symbol="TTT", direction="SHORT", status="EXPIRED",
               entry_ms=base, exit_ms=base + BAR, r=0.0)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "LONG n=1 ΣR +4.00 · SHORT n=2 ΣR -1.00" in msg
    assert "R̃ +0.00R" in msg                                   # mediana de {4,-1,0} = 0


# --- PR-9C B4: RADAR (snapshot presente / ausente) ---------------------------
def test_b4_radar_presente_top3(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    _add_watch(conn, "AAA", dist_high=30.0, dist_low=500.0)     # topo 30
    _add_watch(conn, "BBB", dist_high=800.0, dist_low=12.0)     # fundo 12
    _add_watch(conn, "CCC", dist_high=60.0, dist_low=400.0)     # topo 60
    _add_watch(conn, "DDD", dist_high=900.0, dist_low=900.0)    # longe (cai fora)
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "<b>RADAR</b> (observação, sem ação)" in msg
    assert "BBB fundo · 12 bps" in msg
    assert "AAA topo · 30 bps" in msg
    assert "CCC topo · 60 bps" in msg
    assert "DDD" not in msg                                     # cap fixo 3


def test_b4_radar_ausente_omite(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    msg = vigia.build_report(conn, NOW)                         # sem watchlist
    conn.close()
    assert "RADAR" not in msg


def test_b4_radar_usa_apenas_ultimo_run(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    _add_watch(conn, "OLD", 1.0, 1.0, run_ts="2026-07-17T20:00:00+00:00")   # run antigo
    _add_watch(conn, "NEW", 40.0, 40.0, run_ts="2026-07-18T20:00:00+00:00") # run recente
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert "NEW" in msg
    assert "OLD" not in msg                                     # so o snapshot do ultimo run


# --- PR-9C: tamanho da mensagem (folga em 4096) ------------------------------
def test_mensagem_cabe_com_folga(tmp_path):
    conn = _seed_conn(tmp_path)
    _six_runs(conn)
    base = _ms(datetime(2026, 7, 1, tzinfo=timezone.utc))
    for i in range(9):                                          # ABERTAS ~<10 (natural)
        _add_trade(conn, f"o{i}", symbol=f"OPN{i}", status="OPEN",
                   entry_ms=_ms(NOW) - (i + 1) * BAR, spread_bps=8.0 + i)
    for i in range(40):                                         # muitos fechados (cap 5 no bloco)
        st = ("WIN", "LOSS", "EXPIRED")[i % 3]
        _add_trade(conn, f"f{i}", symbol=f"CLS{i}", direction=("LONG", "SHORT")[i % 2],
                   status=st, entry_ms=base, exit_ms=base + (i + 1) * BAR,
                   r=(i % 5) - 2.0, pnl_pct=0.5, spread_bps=7.0)
    for i in range(20):                                         # watchlist cheia (RADAR cap 3)
        _add_watch(conn, f"W{i}", dist_high=float(i * 5), dist_low=float(i * 7))
    msg = vigia.build_report(conn, NOW)
    conn.close()
    assert len(msg) < 3500                                      # folga larga em 4096
