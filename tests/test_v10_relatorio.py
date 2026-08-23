"""Relatorio generico do v10 (v10/relatorio.py) e a sua entrada no [VIGIA].

Tres coisas provadas aqui:
  1. o bloco: numeros fechados a mao (saldo, USDT/%, LONG/SHORT, pior mergulho,
     abertas com marcacao corrente, fechados recentes, rodape do nocional);
  2. o isolamento: um setup que estoura vira LINHA DE ERRO, os outros saem;
  3. a integracao: o bloco 9A (DONCHIAN-A) sai BYTE-IDENTICO com o bloco do v10
     na mesma mensagem, e o bloco do KIS shadow continua no lugar.

Stdlib-only no que interessa: `v10.relatorio` puxa `v10.runner` (que puxa
`risk`/numpy) — o mesmo que os outros testes de v10 ja puxam.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from shadow import donchian_a as sd
from shadow import vigia
from v10 import relatorio
from v10.schema import connect
from v10.spec import SetupSpec

QUIET = logging.getLogger("v10_relatorio_test")
QUIET.addHandler(logging.NullHandler())

NOW = datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc)
BAR_4H = 14_400_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % BAR_4H)      # ancorado na grade


def _spec(setup_id="alfa_4h", custo=0.0, mode="shadow",
          estado="proposto", aviso="") -> SetupSpec:
    return SetupSpec(setup_id=setup_id, detector=lambda v: None, tf="4H",
                     cadencia_barras=1, symbols=["BTCUSDT"], warmup_barras=0,
                     exit_model="reverse", custo_bps_por_perna=custo, mode=mode,
                     estado_ciclo=estado, aviso=aviso)


def _conn(tmp_path):
    return connect(str(tmp_path / "j.db"))


def _fechado(conn, spec, symbol, direction, entry_ts, exit_ts, bps,
             status="REVERTIDO", config_hash=None):
    conn.execute(
        "INSERT INTO trades_v10 (id, setup_id, config_hash, mode, symbol, tf, "
        "direction, entry_ts, entry_price, status, exit_ts, exit_price, "
        "pnl_pct, pnl_bps_liq) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{symbol}-{entry_ts}-{direction}", spec.setup_id,
         config_hash or spec.config_hash, spec.mode, symbol, spec.tf, direction,
         str(entry_ts), 100.0, status, str(exit_ts), 100.0, bps / 100.0, bps))
    conn.commit()


def _aberto(conn, spec, symbol, direction, entry_ts, pnl_corrente_pct=None):
    estado = {} if pnl_corrente_pct is None else {
        "pnl_corrente_pct": pnl_corrente_pct, "ultimo_close": 101.0}
    conn.execute(
        "INSERT INTO trades_v10 (id, setup_id, config_hash, mode, symbol, tf, "
        "direction, entry_ts, entry_price, status, exit_state_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,'OPEN',?)",
        (f"open-{symbol}-{entry_ts}", spec.setup_id, spec.config_hash, spec.mode,
         symbol, spec.tf, direction, str(entry_ts), 100.0, json.dumps(estado)))
    conn.commit()


# --- o bloco -----------------------------------------------------------------
def test_cabecalho_traz_identidade_completa_do_setup(tmp_path):
    sp = _spec(mode="shadow", estado="em_bancada", aviso="coletor, nao desenho")
    txt = relatorio.bloco(_conn(tmp_path), sp, NOW)
    cab = txt.splitlines()[0]
    assert "alfa_4h" in cab and sp.config_hash in cab
    assert "shadow" in cab and "em_bancada" in cab
    assert "⚠️ coletor, nao desenho" in txt


def test_acumulado_saldo_em_usdt_e_pct_sobre_o_nocional(tmp_path):
    # 3 fechados: +100, -300, +50 bps. Saldo = -150 bps.
    # USDT = 1000 * (-150/10000) = -15.00 ; % = -150/100 = -1.50
    sp, conn = _spec(), _conn(tmp_path)
    _fechado(conn, sp, "BTCUSDT", "LONG", T0, T0 + BAR_4H, 100.0)
    _fechado(conn, sp, "ETHUSDT", "SHORT", T0, T0 + 2 * BAR_4H, -300.0)
    _fechado(conn, sp, "SOLUSDT", "LONG", T0, T0 + 3 * BAR_4H, 50.0)
    txt = relatorio.bloco(conn, sp, NOW)
    assert "fechados 3 · abertas 0" in txt
    assert "WIN 2 · LOSS 1" in txt
    assert "saldo -15.00 USDT (-1.50%)" in txt
    assert "LONG n=2 +15.00 USDT (+1.50%)" in txt
    assert "SHORT n=1 -30.00 USDT (-3.00%)" in txt


def test_pior_mergulho_e_da_curva_acumulada_nao_do_pior_trade(tmp_path):
    # curva: +100 -> -200 -> -150. Topo 100, vale -200 => mergulho -300 bps.
    # O pior TRADE tambem e -300 aqui de proposito: o teste seguinte separa.
    sp, conn = _spec(), _conn(tmp_path)
    for i, bps in enumerate((100.0, -300.0, 50.0)):    # entry_ts distinto: UNIQUE
        _fechado(conn, sp, "BTCUSDT", "LONG", T0 + i * BAR_4H,
                 T0 + (i + 1) * BAR_4H, bps)
    assert "pior mergulho da curva -30.00 USDT (-3.00%)" in relatorio.bloco(
        conn, sp, NOW)


def test_pior_mergulho_soma_perdas_seguidas(tmp_path):
    # Nenhum trade perde mais que 100 bps, mas a curva devolve 250 desde o topo.
    sp, conn = _spec(), _conn(tmp_path)
    for i, bps in enumerate((200.0, -100.0, -100.0, -50.0, 300.0)):
        _fechado(conn, sp, "BTCUSDT", "LONG", T0 + i * BAR_4H,
                 T0 + (i + 1) * BAR_4H, bps)
    assert "pior mergulho da curva -25.00 USDT (-2.50%)" in relatorio.bloco(
        conn, sp, NOW)


def test_abertas_trazem_barras_e_resultado_corrente(tmp_path):
    sp, conn = _spec(), _conn(tmp_path)
    entrada = int(NOW.timestamp() * 1000) - 4 * BAR_4H
    _aberto(conn, sp, "BTCUSDT", "LONG", entrada, pnl_corrente_pct=1.5)
    txt = relatorio.bloco(conn, sp, NOW)
    assert "<b>ABERTAS</b> (1)" in txt
    assert "BTCUSDT LONG · 4b · +15.00 USDT (+1.50%)" in txt
    assert "em aberto +15.00 USDT (+1.50%)" in txt


def test_aberta_sem_marcacao_nao_e_imputada_como_zero(tmp_path):
    sp, conn = _spec(), _conn(tmp_path)
    _aberto(conn, sp, "BTCUSDT", "LONG", int(NOW.timestamp() * 1000) - BAR_4H)
    txt = relatorio.bloco(conn, sp, NOW)
    assert "BTCUSDT LONG · 1b · n/a" in txt
    assert "1 sem marcação" in txt


def test_total_soma_fechado_com_em_aberto(tmp_path):
    sp, conn = _spec(), _conn(tmp_path)
    _fechado(conn, sp, "BTCUSDT", "LONG", T0, T0 + BAR_4H, 200.0)
    _aberto(conn, sp, "ETHUSDT", "SHORT", int(NOW.timestamp() * 1000) - BAR_4H,
            pnl_corrente_pct=-0.5)
    txt = relatorio.bloco(conn, sp, NOW)
    assert "saldo +20.00 USDT (+2.00%)" in txt
    assert "TOTAL +15.00 USDT (+1.50%)" in txt          # 200 - 50 = 150 bps


def test_fechados_recentes_sao_os_ultimos_cinco_do_mais_novo_ao_mais_velho(tmp_path):
    sp, conn = _spec(), _conn(tmp_path)
    for i in range(7):
        _fechado(conn, sp, f"T{i}USDT", "LONG", T0 + i * BAR_4H,
                 T0 + (i + 1) * BAR_4H, 10.0)
    corpo = relatorio.bloco(conn, sp, NOW).split("<b>FECHADOS</b>")[1]
    assert [s for s in ("T6USDT", "T5USDT", "T4USDT", "T3USDT", "T2USDT")
            if s in corpo] == ["T6USDT", "T5USDT", "T4USDT", "T3USDT", "T2USDT"]
    assert "T1USDT" not in corpo and "T0USDT" not in corpo
    assert corpo.index("T6USDT") < corpo.index("T2USDT")    # mais novo primeiro


def test_custo_por_perna_desconta_dos_dois_lados(tmp_path):
    # Linha sem pnl_bps_liq gravado: o bloco recompoe bruto - 2x custo.
    sp, conn = _spec(custo=5.0), _conn(tmp_path)
    conn.execute(
        "INSERT INTO trades_v10 (id, setup_id, config_hash, mode, symbol, tf, "
        "direction, entry_ts, entry_price, status, exit_ts, pnl_pct) "
        "VALUES ('x',?,?,?,'BTCUSDT','4H','LONG',?,100.0,'WIN',?,1.0)",
        (sp.setup_id, sp.config_hash, sp.mode, str(T0), str(T0 + BAR_4H)))
    conn.commit()
    txt = relatorio.bloco(conn, sp, NOW)
    assert "saldo +9.00 USDT (+0.90%)" in txt            # 100 - 10 = 90 bps
    assert "líquido de 2×5 bps" in txt


def test_rodape_declara_o_nocional_de_referencia(tmp_path):
    txt = relatorio.bloco(_conn(tmp_path), _spec(), NOW)
    assert relatorio.NOCIONAL_USDT == 1000.0
    assert "nocional de referência 1.000 USDT/trade" in txt
    assert "sem alavancagem" in txt


def test_linhas_de_outra_configuracao_nao_entram_na_conta_mas_sao_avisadas(tmp_path):
    sp, conn = _spec(), _conn(tmp_path)
    _fechado(conn, sp, "BTCUSDT", "LONG", T0, T0 + BAR_4H, 100.0)
    _fechado(conn, sp, "ETHUSDT", "LONG", T0, T0 + BAR_4H, 900.0,
             config_hash="hashvelho999")
    txt = relatorio.bloco(conn, sp, NOW)
    assert "saldo +10.00 USDT (+1.00%)" in txt           # so a config corrente
    assert "1 linha(s) de outra configuração fora desta conta" in txt


def test_setup_sem_nenhum_trade_nao_quebra(tmp_path):
    txt = relatorio.bloco(_conn(tmp_path), _spec(), NOW)
    assert "— nenhuma" in txt and "— nenhum" in txt
    assert "saldo +0.00 USDT (+0.00%)" in txt


# --- isolamento --------------------------------------------------------------
def test_setup_que_estoura_vira_linha_de_erro_e_os_outros_saem(tmp_path):
    quebrado = _spec(setup_id="quebrado_9x")
    quebrado.tf = "3s"                                   # tf desconhecido -> KeyError
    txt = relatorio.build_report(_conn(tmp_path), NOW,
                                 specs=[quebrado, _spec(setup_id="bom_4h")])
    assert "bloco indisponível: KeyError" in txt
    assert "quebrado_9x" in txt
    assert "<b>ACUMULADO</b>" in txt                     # o bom saiu inteiro
    assert "bom_4h" in txt


def test_build_report_aceita_o_registro_como_dict(tmp_path):
    a, b = _spec(setup_id="a_4h"), _spec(setup_id="b_4h")
    txt = relatorio.build_report(_conn(tmp_path), NOW,
                                 specs={a.setup_id: a, b.setup_id: b})
    assert txt.count("<b>ACUMULADO</b>") == 2
    assert txt.index("a_4h") < txt.index("b_4h")         # ordem do registro


# --- integracao no [VIGIA] ---------------------------------------------------
def _vigia_dbs(tmp_path):
    """DBs vazios dos tres blocos + um trade no v10, p/ o bloco nao ficar mudo."""
    sd._connect(str(tmp_path / "donchian.db")).close()
    sd._connect(str(tmp_path / "kis.db")).close()
    conn = connect(str(tmp_path / "v10.db"))
    sp = _spec(setup_id="alfa_4h")
    _fechado(conn, sp, "BTCUSDT", "LONG", T0, T0 + BAR_4H, 100.0)
    conn.close()
    return sp


def test_bloco_9a_sai_byte_identico_com_o_v10_na_mesma_mensagem(tmp_path):
    sp = _vigia_dbs(tmp_path)
    conn = sd._connect(str(tmp_path / "donchian.db"))
    try:
        sozinho = vigia.build_report(conn, NOW)          # 9A isolado, a referencia
    finally:
        conn.close()
    msg, _ = vigia.run(db_path=str(tmp_path / "donchian.db"), now=NOW,
                       send_fn=lambda t: True, log=QUIET,
                       kis_db=str(tmp_path / "kis.db"),
                       v10_db=str(tmp_path / "v10.db"), v10_specs=[sp])
    assert msg.startswith(sozinho)                       # BYTE-IDENTICO, e primeiro
    assert msg.split("\n\n\n")[0].startswith("🛡️ <b>[VIGIA] DONCHIAN-A</b>")


def test_os_tres_blocos_convivem_na_mesma_mensagem(tmp_path):
    sp = _vigia_dbs(tmp_path)
    msg, ok = vigia.run(db_path=str(tmp_path / "donchian.db"), now=NOW,
                        send_fn=lambda t: True, log=QUIET,
                        kis_db=str(tmp_path / "kis.db"),
                        v10_db=str(tmp_path / "v10.db"), v10_specs=[sp])
    assert ok
    assert "[VIGIA] DONCHIAN-A" in msg                   # 9A legado
    assert "[VIGIA] KIS+REGIME" in msg                   # KIS shadow
    assert "[V10] alfa_4h" in msg                        # relatorio novo
    assert msg.index("DONCHIAN-A") < msg.index("KIS+REGIME") < msg.index("[V10]")


def test_v10_indisponivel_nao_derruba_o_vigia(tmp_path):
    sd._connect(str(tmp_path / "donchian.db")).close()
    sd._connect(str(tmp_path / "kis.db")).close()
    quebrado = _spec(setup_id="quebrado_9x")
    quebrado.tf = "3s"
    msg, ok = vigia.run(db_path=str(tmp_path / "donchian.db"), now=NOW,
                        send_fn=lambda t: True, log=QUIET,
                        kis_db=str(tmp_path / "kis.db"),
                        v10_db=str(tmp_path / "v10.db"), v10_specs=[quebrado])
    assert ok and "[VIGIA] DONCHIAN-A" in msg
    assert "bloco indisponível: KeyError" in msg         # erro visivel, nao silencio
