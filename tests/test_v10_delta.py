"""Delta de 4h do v10, a marca da janela, o setup desligado e o cron.

Quatro coisas provadas aqui:
  1. o delta: uma linha por ABRIU/FECHOU na janela, com bps E USDT, barras no
     trade e `barras_atraso`; sem evento, UMA linha de heartbeat por setup;
  2. a janela: semiaberta `(desde, ate]`, persistida, e que so avanca quando o
     envio confirma — envio que falha NAO perde evento e reenvio NAO duplica;
  3. o desligamento: `executar=False` faz o `rodar_todos` PULAR o setup, e o
     DONCHIAN-A esta desligado no registro (a instancia que conta e a legada);
  4. o deploy: o wrapper e o cron existem, com a sintaxe e os minutos certos.
"""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from v10 import relatorio
from v10.runner import rodar_todos
from v10.schema import connect
from v10.spec import SetupSpec

ROOT = Path(__file__).resolve().parents[1]
QUIET = logging.getLogger("v10_delta_test")
QUIET.addHandler(logging.NullHandler())

BAR_4H = 14_400_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % BAR_4H)     # ancorado na grade
NOW = datetime.fromtimestamp((T0 + 4 * BAR_4H) / 1000, timezone.utc)


def _spec(setup_id="alfa_4h", custo=0.0, **kw) -> SetupSpec:
    base = dict(setup_id=setup_id, detector=lambda v: None, tf="4H",
                cadencia_barras=1, symbols=["BTCUSDT"], warmup_barras=0,
                exit_model="reverse", custo_bps_por_perna=custo)
    base.update(kw)
    return SetupSpec(**base)


def _conn(tmp_path):
    return connect(str(tmp_path / "v10.db"))


def _linha(conn, spec, symbol, direction, entry_ts, *, entry=100.0, exit_ts=None,
           exit_price=None, bps=None, status="OPEN", estado=None):
    conn.execute(
        "INSERT INTO trades_v10 (id, setup_id, config_hash, mode, symbol, tf, "
        "direction, entry_ts, entry_price, status, exit_ts, exit_price, "
        "pnl_pct, pnl_bps_liq, exit_state_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{spec.setup_id}-{symbol}-{entry_ts}-{direction}", spec.setup_id,
         spec.config_hash, spec.mode, symbol, spec.tf, direction, str(entry_ts),
         entry, status, None if exit_ts is None else str(exit_ts), exit_price,
         None if bps is None else bps / 100.0, bps, json.dumps(estado or {})))
    conn.commit()


# --- o bloco do delta --------------------------------------------------------
def test_abriu_e_fechou_saem_com_preco_bps_usdt_barras_e_atraso(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", T0 + BAR_4H, entry=61234.5)     # ABRIU
    _linha(conn, sp, "ETHUSDT", "SHORT", T0 - 3 * BAR_4H, exit_ts=T0 + BAR_4H,
           exit_price=2500.0, bps=250.0, status="REVERTIDO",
           estado={"barras_atraso": 2})                                 # FECHOU
    txt = relatorio.bloco_delta(conn, sp, T0, T0 + 2 * BAR_4H)

    assert "▲ ABRIU BTCUSDT LONG · 61234.5" in txt
    # 250 bps sobre 1.000 USDT de nocional = +25.00 USDT. 4 barras no trade.
    assert ("▼ FECHOU ETHUSDT SHORT REVERTIDO · 2500 · "
            "+250.0 bps (+25.00 USDT) · 4b · atraso 2b") in txt
    assert "1 abriu · 1 fechou" in txt


def test_sem_atraso_medido_a_linha_nao_inventa_zero(tmp_path):
    # `bracket_simples` nao mede atraso. Imprimir "atraso 0b" seria AFIRMAR que
    # nao houve run perdida, quando o que houve foi ausencia de medicao.
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", T0, exit_ts=T0 + BAR_4H, exit_price=101.0,
           bps=90.0, status="WIN")
    txt = relatorio.bloco_delta(conn, sp, T0, T0 + 2 * BAR_4H)
    assert "▼ FECHOU BTCUSDT LONG WIN" in txt and "atraso" not in txt


def test_sem_eventos_sai_UMA_linha_de_heartbeat(tmp_path):
    # Ausencia de mensagem tem de ser incidente, nao silencio. Por isso o bloco
    # existe mesmo quando nada aconteceu — e traz o estado junto.
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", T0 - 9 * BAR_4H,
           estado={"pnl_corrente_pct": 1.0})                    # aberta antiga
    _linha(conn, sp, "ETHUSDT", "LONG", T0 - 9 * BAR_4H, exit_ts=T0 - 8 * BAR_4H,
           exit_price=99.0, bps=-40.0, status="LOSS")           # fechada antiga
    txt = relatorio.bloco_delta(conn, sp, T0, T0 + BAR_4H)

    assert txt.count("\n") == 0                                 # UMA linha
    assert "sem eventos" in txt
    assert "abertas 1" in txt
    assert "em aberto +100.0 bps (+10.00 USDT)" in txt           # 1% liquido
    assert "acumulado -40.0 bps (-4.00 USDT)" in txt


def test_aberta_sem_marcacao_e_declarada_nao_imputada(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", T0 - 9 * BAR_4H)        # sem marcacao
    txt = relatorio.bloco_delta(conn, sp, T0, T0 + BAR_4H)
    assert "abertas 1" in txt and "(1 sem marcação)" in txt
    assert "em aberto +0.0 bps (+0.00 USDT)" in txt             # soma de nada


def test_janela_e_semiaberta_desde_exclusivo_ate_inclusivo(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "AAAUSDT", "LONG", T0)                     # == desde: FORA
    _linha(conn, sp, "BBBUSDT", "LONG", T0 + BAR_4H)            # == ate: DENTRO
    txt = relatorio.bloco_delta(conn, sp, T0, T0 + BAR_4H)
    assert "BBBUSDT" in txt and "AAAUSDT" not in txt


def test_acumulado_conta_tudo_a_janela_so_filtra_os_eventos(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "AAAUSDT", "LONG", T0 - 9 * BAR_4H, exit_ts=T0 - 8 * BAR_4H,
           exit_price=101.0, bps=100.0, status="WIN")           # fora da janela
    _linha(conn, sp, "BBBUSDT", "LONG", T0, exit_ts=T0 + BAR_4H,
           exit_price=99.0, bps=-30.0, status="LOSS")           # dentro
    txt = relatorio.bloco_delta(conn, sp, T0, T0 + BAR_4H)
    assert "▼ FECHOU BBBUSDT" in txt and "AAAUSDT" not in txt
    assert "acumulado +70.0 bps (+7.00 USDT)" in txt            # 100 - 30


def test_custo_entra_no_liquido_do_delta(tmp_path):
    # Linha sem `pnl_bps_liq` gravado: o bloco recompoe do bruto descontando as
    # DUAS pernas, a mesma conta do runner.
    conn, sp = _conn(tmp_path), _spec(custo=5.0)
    conn.execute(
        "INSERT INTO trades_v10 (id, setup_id, config_hash, mode, symbol, tf, "
        "direction, entry_ts, entry_price, status, exit_ts, exit_price, pnl_pct) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("x", sp.setup_id, sp.config_hash, sp.mode, "BTCUSDT", "4H", "LONG",
         str(T0), 100.0, "WIN", str(T0 + BAR_4H), 101.0, 1.0))
    conn.commit()
    txt = relatorio.bloco_delta(conn, sp, T0 - BAR_4H, T0 + BAR_4H)
    assert "+90.0 bps (+9.00 USDT)" in txt                      # 100 - 2x5


# --- a montagem ---------------------------------------------------------------
def test_build_delta_poe_a_janela_no_cabecalho_e_isola_o_que_estoura(tmp_path):
    conn = _conn(tmp_path)
    ruim = _spec(setup_id="quebrado_9x")
    ruim.tf = "3s"                                              # tf desconhecido
    txt = relatorio.build_delta(conn, [ruim, _spec(setup_id="bom_4h")],
                                T0, T0 + BAR_4H)
    assert txt.splitlines()[0].startswith("⚡ <b>[V10Δ]</b> ")
    assert "→" in txt.splitlines()[0] and "(UTC)" in txt.splitlines()[0]
    assert "delta indisponível: KeyError" in txt                # erro VISIVEL
    assert "bom_4h" in txt and "sem eventos" in txt             # o bom saiu


def test_build_delta_usa_os_ATIVOS_por_padrao(tmp_path):
    # O default e ATIVOS, nao o REGISTRO: uma ficha desligada reportaria zeros a
    # cada 4h e pareceria estar sendo observada.
    from v10.registro import ATIVOS
    txt = relatorio.build_delta(_conn(tmp_path), None, T0, T0 + BAR_4H)
    assert [s.setup_id for s in ATIVOS] == ["kis_regime_4h"]
    assert "kis_regime_4h" in txt and "donchian_a_4h" not in txt


# --- a marca da janela --------------------------------------------------------
def test_primeira_run_nao_despeja_o_historico(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", T0 - 40 * BAR_4H)       # muito antigo
    conn.close()
    assert relatorio.ler_marca(_conn(tmp_path)) is None
    msg, ok = relatorio.run_delta(now=NOW, db_path=str(tmp_path / "v10.db"),
                                  specs=[sp], send_fn=lambda t: True, log=QUIET)
    assert ok and "sem eventos" in msg and "BTCUSDT" not in msg


def test_reenvio_da_mesma_janela_nao_duplica_linha(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", int(NOW.timestamp() * 1000) - BAR_4H // 2)
    conn.close()
    args = dict(db_path=str(tmp_path / "v10.db"), specs=[sp],
                send_fn=lambda t: True, log=QUIET)
    primeira, _ = relatorio.run_delta(now=NOW, **args)
    segunda, _ = relatorio.run_delta(now=NOW, **args)            # mesma janela
    assert "▲ ABRIU BTCUSDT" in primeira
    assert "▲ ABRIU" not in segunda and "sem eventos" in segunda


def test_envio_que_falha_nao_avanca_a_marca_e_nao_perde_o_evento(tmp_path):
    conn, sp = _conn(tmp_path), _spec()
    _linha(conn, sp, "BTCUSDT", "LONG", int(NOW.timestamp() * 1000) - BAR_4H // 2)
    conn.close()
    args = dict(db_path=str(tmp_path / "v10.db"), specs=[sp], log=QUIET)
    msg, ok = relatorio.run_delta(now=NOW, send_fn=lambda t: False, **args)
    assert ok is False and "▲ ABRIU BTCUSDT" in msg
    assert relatorio.ler_marca(_conn(tmp_path)) is None      # marca NAO avancou
    # a run seguinte reporta o MESMO evento: nada se perdeu no envio que falhou
    de_novo, ok2 = relatorio.run_delta(now=NOW, send_fn=lambda t: True, **args)
    assert ok2 and "▲ ABRIU BTCUSDT" in de_novo


def test_excecao_no_envio_nao_levanta_e_nao_avanca(tmp_path):
    def _boom(_):
        raise RuntimeError("telegram fora do ar")
    _conn(tmp_path).close()
    msg, ok = relatorio.run_delta(now=NOW, db_path=str(tmp_path / "v10.db"),
                                  specs=[_spec()], send_fn=_boom, log=QUIET)
    assert ok is False and "[V10Δ]" in msg
    assert relatorio.ler_marca(_conn(tmp_path)) is None


def test_marca_e_tabela_propria_e_nao_toca_trades_v10(tmp_path):
    conn = _conn(tmp_path)
    relatorio.gravar_marca(conn, T0)
    assert relatorio.ler_marca(conn) == T0
    relatorio.gravar_marca(conn, T0 + BAR_4H)                    # UPSERT
    assert relatorio.ler_marca(conn) == T0 + BAR_4H
    n = conn.execute(f"SELECT COUNT(*) FROM {relatorio.MARCA_TABELA}").fetchone()[0]
    assert n == 1                                                # uma linha so
    assert conn.execute("SELECT COUNT(*) FROM trades_v10").fetchone()[0] == 0


# --- o setup desligado --------------------------------------------------------
def test_rodar_todos_pula_quem_tem_executar_false(tmp_path):
    conn = _conn(tmp_path)
    chamou = []

    def _detector(velas):
        chamou.append(1)
        return None

    ligado = _spec(setup_id="ligado_4h", detector=_detector)
    desligado = _spec(setup_id="desligado_4h", detector=_detector, executar=False)
    r = rodar_todos([desligado, ligado], conn, int(NOW.timestamp() * 1000),
                    velas_fn=lambda *a: [{"ts": T0, "open": 1.0, "high": 1.0,
                                          "low": 1.0, "close": 1.0, "volume": 1.0}],
                    log=QUIET)
    assert r[0] == {"setup_id": "desligado_4h", "pulado": True}   # fato escrito
    assert r[1]["setup_id"] == "ligado_4h" and "pulado" not in r[1]
    assert len(chamou) == 1                                       # so o ligado


def test_donchian_a_esta_desligado_no_registro():
    # Rodar o DONCHIAN-A aqui abriria uma SEGUNDA instancia do mesmo desenho, e
    # o relogio de observacao do Estagio 2 nao pode reiniciar. A ficha fica —
    # documentacao, e o quadro diario a lista com o aviso dela.
    from v10.registro import ATIVOS, DONCHIAN_A_4H, KIS_REGIME_4H, REGISTRO
    assert DONCHIAN_A_4H.executar is False and KIS_REGIME_4H.executar is True
    assert set(REGISTRO) == {"kis_regime_4h", "donchian_a_4h"}     # nao sumiu
    assert ATIVOS == [KIS_REGIME_4H]


# --- deploy -------------------------------------------------------------------
def test_wrapper_existe_executavel_e_com_sintaxe_valida():
    sh = ROOT / "deploy" / "run-v10.sh"
    assert sh.exists() and sh.stat().st_mode & 0o111            # +x
    subprocess.run(["bash", "-n", str(sh)], check=True)
    texto = sh.read_text()
    # Invoca via "$PY" (venv quando existe, python3 do sistema quando nao) —
    # nunca um interpretador cravado, como o run-shadow.sh ja fazia.
    assert '"$PY" -m v10.runner' in texto
    assert '"$PY" -m v10.relatorio' in texto and '--relatorio' in texto
    assert "cron-v10.log" in texto                              # log PROPRIO


def test_cron_do_v10_no_minuto_16_das_seis_barras_4h():
    arq = ROOT / "deploy" / "cron-atirador-v10"
    assert arq.exists() and "." not in arq.name                 # cron ignora ponto
    linhas = [l for l in arq.read_text().splitlines()
              if l.strip() and not l.lstrip().startswith("#") and "=" not in l.split()[0]]
    assert len(linhas) == 2
    scan, quadro = linhas
    # minuto 16: deslocado do scan v9 (*/30), do DONCHIAN-A (8) e do KIS (12).
    assert scan.split()[:6] == ["16", "0,4,8,12,16,20", "*", "*", "*", "ubuntu"]
    assert scan.endswith("2>&1") and "run-v10.sh" in scan and "--relatorio" not in scan
    assert quadro.split()[:6] == ["20", "0", "*", "*", "*", "ubuntu"]
    assert "run-v10.sh --relatorio" in quadro


def test_cron_traz_o_desligamento_do_shadow_kis_escrito():
    # O desligamento e um passo de VM, e ele nao pode viver so na conversa: o
    # arquivo que instala o v10 carrega o comando que aposenta o que ele
    # substitui — com o ponto no fim, o mesmo mecanismo do v8/v9.
    texto = (ROOT / "deploy" / "cron-atirador-v10").read_text()
    assert "cron-atirador-shadow-kis." in texto and "sudo mv" in texto
    assert "shadow_kis_regime.db PERMANECE" in texto
    assert "cron-atirador-shadow-donchian NAO e tocado" in texto
