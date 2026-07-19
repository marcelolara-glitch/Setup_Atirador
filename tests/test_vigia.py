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

import logging
from datetime import datetime, timezone

from shadow import donchian_a as sd
from shadow import vigia

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
               status="OPEN", exit_ms=None, r=None):
    conn.execute(
        "INSERT INTO shadow_trades (id, symbol, direction, bar_ts_entry, "
        "entry_price, atr_at_entry, sl_price, tp_price, expiry_bar_ts, status, "
        "exit_ts, r_multiple) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (tid, symbol, direction, str(entry_ms), 100.0, 1.0, 98.5, 106.0,
         str(entry_ms + sd.H_BARS * BAR), status,
         (str(exit_ms) if exit_ms is not None else None), r))
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
                             symbols=["BTCUSDT"], log=QUIET)
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
