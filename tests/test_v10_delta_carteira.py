"""Delta por seções (fichas com `banca_usdt`) e a série `carteira_v10`.

Journal sintético com custo 0 e mão 450 (1 bps = 0,045 USDT), conferido à mão:

  janela (T0, T0+4h]
  FECHADAS na janela         bps      USDT
    S1 SHORT stop           -300    -13,50
    S2 SHORT flip           +500    +22,50   (parcial 25% @94 NA janela: já
                                               dentro do trade, fora de Parciais)
    S3 SHORT flip           +100     +4,50
    L1 LONG  stop            +50     +2,25   (parcial 50% @106 = +3% realizado;
                                               resto 0,5 no stop a -5% -> perna
                                               -250 bps = -11,25)
    L2 LONG  liquidacao    -2000    -90,00
    -> 5 trades -74,25 · Short 3 +13,50 · Long 2 -87,75
  fechada ANTES (exit = T0)  L0 LONG stop -100 -4,50  (fora: janela semiaberta)
  ABERTAS
    O1 LONG  parcial 50% @103 na janela (+150 bps -> +6,75); resto 0,5 @102 -> +4,50
    O2 SHORT parcial 50% @97 ANTES     (+150 bps -> +6,75); resto 0,5 @104 -> -9,00
    O3 LONG  sem parcial, -1%                                         -> -4,50
    -> 3 posições -9,00 · Short 1 -9,00 · Long 2 +0,00
  PARCIAIS na janela, só de quem segue aberto: O1 (50%) = 1 -> +6,75
  REALIZADO = fechadas (-74,25 - 4,50) + parciais das abertas (6,75 + 6,75) = -65,25
  BANCA = 10.000 - 65,25 - 9,00 = 9.925,75 (-0,7%)
  STOPS = só a perna do stop: S1 -13,50 + L1 -11,25 + L0 -4,50 = -29,25 (3)
          (L1 fechou POSITIVO e ainda assim entra negativo)
  NOCIONAL = 450 × (0,5 + 0,5 + 1) = 900 -> exposição 0,1× · margem 180 (5x)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from v10 import relatorio
from v10.runner import rodar
from v10.schema import connect
from v10.spec import SetupSpec

QUIET = logging.getLogger("v10_carteira_test")
QUIET.addHandler(logging.NullHandler())
H = 3_600_000
BAR = 4 * H
T0 = 1_700_000_000_000 - (1_700_000_000_000 % BAR)
EP = {"mao_usdt": 450, "banca_usdt": 10000, "alav": 5}


def _spec(**kw):
    base = dict(setup_id="kis_teste_4h", detector=lambda v: None, tf="4H",
                cadencia_barras=1, symbols=[f"S{i}USDT" for i in range(65)],
                warmup_barras=0, exit_model="reverse", exit_params=dict(EP),
                custo_bps_por_perna=0.0)
    base.update(kw)
    return SetupSpec(**base)


def _linha(conn, sp, sym, direction, *, status="OPEN", exit_ts=None, bps=None,
           estado=None, entry_ts=None):
    entry_ts = T0 - 10 * BAR if entry_ts is None else entry_ts
    conn.execute(
        "INSERT INTO trades_v10 (id, setup_id, config_hash, mode, symbol, tf, "
        "direction, entry_ts, entry_price, status, exit_ts, pnl_pct, pnl_bps_liq, "
        "exit_state_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{sym}-{entry_ts}", sp.setup_id, sp.config_hash, sp.mode, sym, sp.tf,
         direction, str(entry_ts), 100.0, status,
         None if exit_ts is None else str(exit_ts),
         None if bps is None else bps / 100.0, bps, json.dumps(estado or {})))


def _aberta(fr_rest, realizado_pct, ret_resto_pct, parciais=()):
    return {"fracao_restante": fr_rest, "pnl_realizado_pct": realizado_pct,
            "pnl_corrente_pct": realizado_pct + fr_rest * ret_resto_pct,
            "parciais": [dict(ts_ms=t, fracao=f, preco=p) for t, f, p in parciais]}


def _journal(conn, sp):
    dentro = T0 + 2 * H
    _linha(conn, sp, "S1", "SHORT", status="STOP", exit_ts=dentro, bps=-300)
    _linha(conn, sp, "S2", "SHORT", status="FLIP", exit_ts=dentro, bps=500,
           estado={"parciais": [{"ts_ms": T0 + H, "fracao": 0.25, "preco": 94.0}]})
    _linha(conn, sp, "S3", "SHORT", status="FLIP", exit_ts=T0 + BAR, bps=100)
    _linha(conn, sp, "L1", "LONG", status="STOP", exit_ts=dentro, bps=50,
           estado={"fracao_restante": 0.5, "pnl_realizado_pct": 3.0,
                   "parciais": [{"ts_ms": T0 + H, "fracao": 0.5, "preco": 106.0}]})
    _linha(conn, sp, "L2", "LONG", status="LIQUIDACAO", exit_ts=dentro, bps=-2000)
    _linha(conn, sp, "L0", "LONG", status="STOP", exit_ts=T0, bps=-100)
    _linha(conn, sp, "O1", "LONG", estado=_aberta(0.5, 1.5, 2.0,
                                                  [(T0 + H, 0.5, 103.0)]))
    _linha(conn, sp, "O2", "SHORT", estado=_aberta(0.5, 1.5, -4.0,
                                                   [(T0 - 1, 0.5, 97.0)]))
    _linha(conn, sp, "O3", "LONG", estado=_aberta(1.0, 0.0, -1.0))
    conn.commit()


def test_secoes_conferidas_a_mao(tmp_path):
    conn, sp = connect(str(tmp_path / "j.db")), _spec()
    _journal(conn, sp)
    txt = relatorio.bloco_carteira(conn, sp, T0, T0 + BAR, falhas=[],
                                   guarda=["XLMUSDT"])
    esperado = [
        "kis_teste_4h · paper · banca 10.000 · mão 450 · 5x",
        "━━━━━━ FECHADAS NA JANELA ━━━━━━",
        "5 trades → −74,25 USDT",
        "   Short 3 → +13,50 · stop 1 · flip 2",
        "   Long  2 → −87,75 · stop 1 · flip 0 · liquidacao 1",
        "Parciais: 1 → +6,75 USDT (1 × 50%)",
        "━━━━━━ ABERTAS ━━━━━━",
        "3 posições → −9,00 USDT (não realizado)",
        "   Short 1 → −9,00",
        "   Long  2 → +0,00",
        "━━━━━━ CARTEIRA ━━━━━━",
        "Banca: 9.925,75 USDT (−0,7%)",
        "Realizado acumulado: −65,25 USDT",
        "Stops acumulados: −29,25 USDT (3 stops)",
        "Exposição 0,1× · margem 180 USDT",
        "Rebaixamento: atual 0,7% · máximo 0,7%",
        "━━━━━━ SAÚDE ━━━━━━",
        "Coleta 65/65 · guarda 1 · liquidações 1",
    ]
    assert txt.splitlines() == esperado


def _so_um(tmp_path, sym, direction, **kw):
    conn, sp = connect(str(tmp_path / "j.db")), _spec()
    _linha(conn, sp, sym, direction, **kw)
    conn.commit()
    return relatorio.bloco_carteira(conn, sp, T0, T0 + BAR).splitlines()


def test_parcial_de_trade_fechado_na_janela_nao_aparece_em_parciais(tmp_path):
    est = {"parciais": [{"ts_ms": T0 + H, "fracao": 0.5, "preco": 103.0}]}
    L = _so_um(tmp_path, "X", "LONG", status="FLIP", exit_ts=T0 + 2 * H,
               bps=250, estado=est)
    assert "Parciais: 0" in L
    assert "1 trades → +11,25 USDT" in L            # a parcial está aqui dentro


def test_parcial_de_trade_que_fecha_depois_da_janela_aparece(tmp_path):
    # Fechou DEPOIS do fim da janela: no fim dela ainda estava aberto.
    est = {"parciais": [{"ts_ms": T0 + H, "fracao": 0.5, "preco": 103.0}]}
    L = _so_um(tmp_path, "X", "LONG", status="FLIP", exit_ts=T0 + 2 * BAR,
               bps=250, estado=est)
    assert "Parciais: 1 → +6,75 USDT (1 × 50%)" in L


def test_stop_depois_de_parcial_conta_so_a_perna_do_stop(tmp_path):
    # Parcial 50% a +3%, resto no stop a -5%: trade +0,5% (+2,25), perna -11,25.
    est = {"fracao_restante": 0.5, "pnl_realizado_pct": 3.0,
           "parciais": [{"ts_ms": T0 - 1, "fracao": 0.5, "preco": 106.0}]}
    L = _so_um(tmp_path, "X", "LONG", status="STOP", exit_ts=T0 + H, bps=50,
               estado=est)
    assert "Stops acumulados: −11,25 USDT (1 stops)" in L
    assert "1 trades → +2,25 USDT" in L


def test_liquidacao_fica_fora_dos_stops(tmp_path):
    L = _so_um(tmp_path, "X", "LONG", status="LIQUIDACAO", exit_ts=T0 + H,
               bps=-2000)
    assert "Stops acumulados: +0,00 USDT (0 stops)" in L
    assert L[-1].endswith("liquidações 1")


def test_nome_de_exibicao_fora_da_ficha(tmp_path):
    from v10.registro import KIS_3489_FINAL_4H as F
    conn = connect(str(tmp_path / "j.db"))
    sp = _spec(setup_id=F.setup_id)
    assert relatorio.bloco_carteira(conn, sp, T0, T0 + BAR).startswith(
        "KIS 34/89 4h · paper")
    # Sem entrada no dicionário: o setup_id.
    assert relatorio.bloco_carteira(conn, _spec(), T0, T0 + BAR).startswith(
        "kis_teste_4h · paper")


def test_parcial_paga_custo_so_da_sua_fracao(tmp_path):
    # custo 5 bps/perna: parcial 50% de +300 bps -> 0,5 × (300 - 10) = 145 bps
    conn, sp = connect(str(tmp_path / "j.db")), _spec(custo_bps_por_perna=5.0)
    _linha(conn, sp, "O1", "LONG", estado=_aberta(0.5, 1.5, 0.0,
                                                  [(T0 + H, 0.5, 103.0)]))
    conn.commit()
    c = relatorio.carteira(conn, sp)
    assert abs(c["realizado_usdt"] - 145 * 0.045) < 1e-9
    assert abs(c["aberto_usdt"] - (-0.5 * 10 * 0.045)) < 1e-9   # resto paga 0,5


def test_rebaixamento_com_dois_picos():
    # pico 10.500 -> 10.200 (2,86%); pico 10.800 -> 9.720 (10%); fim 10.000
    atual, maximo = relatorio.rebaixamento(
        [10000, 10500, 10200, 10800, 9720, 10000], 10000)
    assert round(maximo, 6) == 10.0
    assert round(atual, 4) == round(800 / 10800 * 100, 4)          # 7,4074%


def test_rebaixamento_sai_da_serie_carteira_v10(tmp_path):
    conn, sp = connect(str(tmp_path / "j.db")), _spec()
    for i, b in enumerate([10000, 10500, 10200, 10800, 9720, 10000]):
        conn.execute("INSERT INTO carteira_v10 VALUES (?,?,?,?,?,?,?,?)",
                     (T0 + i, sp.setup_id, sp.config_hash, 10000, b - 10000, 0, 0, 0))
    conn.commit()
    txt = relatorio.bloco_carteira(conn, sp, T0, T0 + BAR)
    assert "Rebaixamento: atual 7,4% · máximo 10,0%" in txt


def test_sem_evento_secoes_zeradas_e_heartbeat_completo(tmp_path):
    conn, sp = connect(str(tmp_path / "j.db")), _spec()
    txt = relatorio.bloco_carteira(conn, sp, T0, T0 + BAR)
    for linha in ("0 trades → +0,00 USDT", "Parciais: 0",
                  "0 posições → +0,00 USDT (não realizado)",
                  "Banca: 10.000,00 USDT (+0,0%)", "Stops acumulados: +0,00 USDT (0 stops)",
                  "Coleta n/d · guarda n/d · liquidações 0"):
        assert linha in txt.splitlines()


def test_formato_brasileiro():
    assert relatorio._br(1234.5) == "+1.234,50"
    assert relatorio._br(-1230.4) == "−1.230,40"
    assert relatorio._br(-0.001) == "+0,00"
    assert relatorio._br(10452.3, 2, False) == "10.452,30"


def test_build_delta_despacha_so_quem_tem_banca(tmp_path):
    conn = connect(str(tmp_path / "j.db"))
    velha = _spec(setup_id="velha_4h", exit_params={})
    txt = relatorio.build_delta(conn, [velha, _spec()], T0, T0 + BAR)
    assert "velha_4h</b> — sem eventos" in txt          # formato antigo
    assert "━━━━━━ CARTEIRA ━━━━━━" in txt               # formato novo
    assert txt.splitlines()[0].endswith(" UTC")


def test_ficha_desligada_fora_do_delta_padrao(tmp_path):
    from v10.registro import KIS_3489_FINAL_4H
    assert KIS_3489_FINAL_4H.executar is False
    txt = relatorio.build_delta(connect(str(tmp_path / "j.db")), None, T0, T0 + BAR)
    assert "kis_3489_final_4h" not in txt


def test_sem_envio_loga_nao_envia_nao_avanca_marca(tmp_path, caplog):
    db = str(tmp_path / "j.db")
    connect(db).close()

    def _explode(_):
        raise AssertionError("nao era para enviar")

    with caplog.at_level(logging.INFO, logger="v10_carteira_test"):
        msg, ok = relatorio.run_delta(
            now=datetime.fromtimestamp((T0 + BAR) / 1000, timezone.utc),
            send_fn=_explode, specs=[_spec()], db_path=db, log=QUIET,
            sem_envio=True)
    assert ok is False and "━━━━━━ SAÚDE ━━━━━━" in msg
    c = connect(db)
    assert relatorio.ler_marca(c) is None
    c.close()


def test_runner_grava_carteira_a_cada_passada(tmp_path):
    conn = connect(str(tmp_path / "j.db"))
    sp = _spec(symbols=["BTCUSDT"])
    velas = [{"ts": T0 + i * BAR, "open": 1, "high": 1, "low": 1, "close": 1}
             for i in range(3)]
    for agora in (T0 + 3 * BAR, T0 + 4 * BAR):
        rodar(sp, conn, agora, velas_fn=lambda *a, **k: velas, log=QUIET)
    rows = conn.execute("SELECT ts_ms, banca_usdt, n_abertas FROM carteira_v10 "
                        "ORDER BY ts_ms").fetchall()
    assert [tuple(r) for r in rows] == [(T0 + 3 * BAR, 10000.0, 0),
                                       (T0 + 4 * BAR, 10000.0, 0)]
    # Ficha sem banca não grava nada.
    rodar(_spec(setup_id="sem_banca", exit_params={}, symbols=["BTCUSDT"]),
          conn, T0 + 5 * BAR, velas_fn=lambda *a, **k: velas, log=QUIET)
    assert conn.execute("SELECT COUNT(*) FROM carteira_v10 WHERE "
                        "setup_id='sem_banca'").fetchone()[0] == 0
