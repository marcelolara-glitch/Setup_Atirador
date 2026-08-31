"""Confiabilidade da coleta v10: backoff, falha nomeada, corte em 4096, --setup.

Nada aqui toca a rede. `v10.data._coletar` e o `velas_fn` do runner sao as duas
costuras injetaveis, e o teste passa por elas — e por isso este arquivo NAO usa
`importorskip`: o que nao coletar tem de reprovar com nome, nao sumir do relato.

O que se trava:

  - coleta que esgota as tentativas LEVANTA (`FalhaDeColeta`) com o simbolo no
    texto, e espera crescente entre as tentativas. Lista vazia vinda de uma
    corretora VIVA continua sendo resposta legitima, nao falha;
  - o simbolo que nao resolveu aparece NOMEADO em `falha_symbols` e CONTADO no
    rodape do delta — sinal perdido nao pode ler igual a sinal ausente;
  - mensagem acima do limite do Telegram e FATIADA em partes numeradas sem
    perder um caractere sequer;
  - `--setup` roda ficha com `executar=False` e RECUSA sem `--sem-envio`.
"""

from __future__ import annotations

import logging
import sqlite3

import pytest

from v10 import data, relatorio, runner
from v10.schema import TABELA, connect
from v10.spec import SetupSpec

BAR = 14_400_000                                   # 4h em ms
QUIET = logging.getLogger("v10_coleta_test")
QUIET.addHandler(logging.NullHandler())


def _b(ts, c=100.0):
    return dict(ts=ts, open=c, high=c, low=c, close=c, volume=1.0)


@pytest.fixture
def conn(tmp_path):
    c = connect(str(tmp_path / "t.db"))
    yield c
    c.close()


# --- 1. os tres config_hash seguem intactos ----------------------------------
def test_config_hash_dos_tres_setups_inalterados(capsys):
    """Este PR e sobre coleta e mensagem. Mudar um hash aqui partiria a serie de
    um setup em duas — e o do KIS-3489 acabou de ser mergeado no #163."""
    from v10.registro import REGISTRO

    esperado = {"kis_regime_4h": "e63ec120e131",
                "kis_3489_60t_4h": "82488baa3086",
                "donchian_a_4h": "250170cc8dc0"}
    obtido = {sid: s.config_hash for sid, s in REGISTRO.items()}
    with capsys.disabled():
        print("\n  config_hash apos o PR da coleta:")
        for sid, h in obtido.items():
            print(f"    {sid:<20} {h}  ({'OK' if esperado.get(sid) == h else 'MUDOU'})")
    assert obtido == esperado


# --- 2. backoff e falha nomeada ----------------------------------------------
def _sem_dormir(monkeypatch):
    esperas = []
    monkeypatch.setattr(data.time, "sleep", esperas.append)
    return esperas


def test_falha_das_duas_corretoras_levanta_com_o_simbolo_no_texto(monkeypatch):
    # `fetch_klines_async` engole a excecao e devolve ([], None): venue None e o
    # discriminador de "as duas cairam". O chamador NAO pode receber [].
    esperas = _sem_dormir(monkeypatch)
    monkeypatch.setattr(data, "_coletar", lambda s, tf, lim: ([], None))
    with pytest.raises(data.FalhaDeColeta) as e:
        data.velas("PENGUUSDT", "4H", 60)
    assert "PENGUUSDT" in str(e.value)
    assert esperas == [1.0, 2.0]                   # 3 tentativas, 2 esperas


def test_excecao_crua_da_rede_tambem_vira_falha_nomeada(monkeypatch):
    _sem_dormir(monkeypatch)

    def _explode(symbol, tf, limite):
        raise TimeoutError("connect timeout")

    monkeypatch.setattr(data, "_coletar", _explode)
    with pytest.raises(data.FalhaDeColeta) as e:
        data.velas("XRPUSDT", "4H", 60)
    assert "XRPUSDT" in str(e.value) and "TimeoutError" in str(e.value)


def test_tentativa_seguinte_recupera_e_nao_levanta(monkeypatch):
    esperas = _sem_dormir(monkeypatch)
    chamadas = []

    def _instavel(symbol, tf, limite):
        chamadas.append(symbol)
        if len(chamadas) < 2:
            return [], None
        return [_b(0), _b(BAR)], "okx"

    monkeypatch.setattr(data, "_coletar", _instavel)
    assert len(data.velas("BTCUSDT", "4H", 2)) == 2
    assert len(chamadas) == 2 and esperas == [1.0]


def test_vazio_de_corretora_viva_nao_e_falha(monkeypatch):
    # Janela sem vela e resposta legitima: nao ha o que repetir, e levantar aqui
    # transformaria "nao ha vela" em incidente de rede.
    _sem_dormir(monkeypatch)
    chamadas = []
    monkeypatch.setattr(data, "_coletar",
                        lambda s, tf, lim: (chamadas.append(s), ([], "okx"))[1])
    assert data.velas("BTCUSDT", "4H", 60) == []
    assert len(chamadas) == 1                      # nao repetiu


# --- 3. o simbolo que nao resolveu chega ao relatorio ------------------------
def _spec(**kw) -> SetupSpec:
    base = dict(setup_id="t", detector=lambda v: None, tf="4H", cadencia_barras=1,
                symbols=["BTCUSDT", "PENGUUSDT"], warmup_barras=2,
                exit_model="bracket_simples",
                exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4, "bar_ms": BAR},
                custo_bps_por_perna=5.0)
    base.update(kw)
    return SetupSpec(**base)


def test_runner_nomeia_o_simbolo_que_nao_coletou(conn):
    def _fonte(symbol, tf, n, ate=None):
        if symbol == "PENGUUSDT":
            raise data.FalhaDeColeta(f"{symbol} {tf}: sem coleta apos 3 tentativa(s)")
        return [_b(0), _b(BAR)]

    r = runner.rodar(_spec(), conn, 3 * BAR, velas_fn=_fonte, log=QUIET)
    assert r["ok"] == 1 and r["falhas"] == 1
    assert r["falha_symbols"] == ["PENGUUSDT"]


def test_rodape_do_delta_conta_as_falhas_e_o_silencio_nao_afirma_nada(conn):
    spec = _spec()
    sem_info = relatorio.bloco_delta(conn, spec, 0, 9 * BAR)
    com_falha = relatorio.bloco_delta(conn, spec, 0, 9 * BAR,
                                      falhas=["PENGUUSDT", "XRPUSDT"])
    assert "coleta" not in sem_info                # None != zero falhas
    assert "coleta 2 falhou" in com_falha and "PENGUUSDT" in com_falha


def test_muitas_falhas_saem_como_contagem_com_amostra(conn):
    falhas = [f"S{i}USDT" for i in range(30)]
    linha = relatorio.bloco_delta(conn, _spec(), 0, 9 * BAR, falhas=falhas)
    assert "coleta 30 falhou" in linha and "+27" in linha
    assert "S29USDT" not in linha                  # rodape nao vira despejo


def test_build_delta_roteia_as_falhas_por_setup_id(conn):
    a, b = _spec(setup_id="a"), _spec(setup_id="b")
    msg = relatorio.build_delta(conn, [a, b], 0, 9 * BAR,
                                coleta={"a": ["PENGUUSDT"]})
    linha_a = [x for x in msg.splitlines() if "<b>a</b>" in x][0]
    linha_b = [x for x in msg.splitlines() if "<b>b</b>" in x][0]
    assert "coleta 1 falhou" in linha_a and "coleta" not in linha_b


# --- 4. corte em 4096 --------------------------------------------------------
def _sem_cabecalho(partes):
    """Corpo de cada parte, sem o `(i/n)\\n` — o que tem de reconstruir a msg."""
    return [p.split("\n", 1)[1] for p in partes]


def test_mensagem_de_5000_chars_vira_duas_partes_sem_perder_caractere():
    msg = "\n".join("x" * 49 for _ in range(100))  # 100*49 + 99 = 4999
    msg += "y"                                     # 5000 exatos
    partes = relatorio.fatiar(msg)
    assert len(partes) == 2
    assert all(len(p) <= relatorio.LIMITE_TELEGRAM for p in partes)
    assert partes[0].startswith("(1/2)\n") and partes[1].startswith("(2/2)\n")
    assert "".join(_sem_cabecalho(partes)) == msg  # nada se perdeu


def test_linha_unica_maior_que_o_limite_e_cortada_sem_perda():
    # Sem fronteira de linha para respeitar: corta no meio, mas nao descarta.
    msg = "z" * 9000
    partes = relatorio.fatiar(msg)
    assert len(partes) == 3
    assert "".join(_sem_cabecalho(partes)) == msg


def test_mensagem_que_cabe_nao_ganha_cabecalho():
    msg = "⚡ delta curto\n  · t — sem eventos"
    assert relatorio.fatiar(msg) == [msg]


def test_barra_de_muitas_inversoes_chega_inteira():
    # O caso que este item existe para salvar: 120 eventos numa barra de virada.
    msg = "\n".join(f"  ▼ FECHOU S{i}USDT LONG WIN · 1.23 · +45.0 bps" for i in range(120))
    partes = relatorio.fatiar(msg)
    assert "".join(_sem_cabecalho(partes)) == msg
    assert all(len(p) <= relatorio.LIMITE_TELEGRAM for p in partes)


def test_enviar_manda_todas_as_partes_e_so_confirma_se_todas_forem():
    enviadas = []
    assert relatorio._enviar("w" * 9000, lambda t: (enviadas.append(t), True)[1],
                             QUIET) is True
    assert len(enviadas) == 3

    parciais = []

    def _cai_na_segunda(t):
        parciais.append(t)
        return len(parciais) != 2

    assert relatorio._enviar("w" * 9000, _cai_na_segunda, QUIET) is False
    assert len(parciais) == 3                      # tentou todas mesmo assim


# --- 5. --setup --------------------------------------------------------------
def _registro_falso(monkeypatch, tmp_path, executar=False):
    import v10.registro as reg
    import v10.schema as sch

    vistos = []

    def _det(velas):
        return {"direction": "LONG", "entry_price": 100.0, "atr_value": 1.0}

    spec = _spec(setup_id="desligado", detector=_det, symbols=["BTCUSDT"],
                 executar=executar)
    monkeypatch.setattr(reg, "REGISTRO", {"desligado": spec})
    # Conexao NOVA a cada chamada: `main` fecha a que abriu, e o teste abre a
    # dele depois para conferir o que ficou gravado.
    db = str(tmp_path / "setup.db")
    monkeypatch.setattr(sch, "connect", lambda *a, **k: connect(db))

    def _fonte(symbol, tf, n, ate=None):
        vistos.append(symbol)
        return [_b(0), _b(BAR)]

    monkeypatch.setattr(runner, "_velas_padrao", _fonte)
    return spec, db, vistos


def test_setup_roda_ficha_com_executar_false(monkeypatch, tmp_path):
    spec, db, vistos = _registro_falso(monkeypatch, tmp_path, executar=False)
    assert spec.executar is False
    assert runner.main(["--setup", "desligado", "--sem-envio"]) == 0
    assert vistos == ["BTCUSDT"]                   # avaliou, nao pulou
    c = connect(db)
    try:
        n = c.execute(f"SELECT COUNT(*) FROM {TABELA} WHERE setup_id='desligado'"
                      ).fetchone()[0]
    finally:
        c.close()
    assert n == 1


def test_sem_setup_a_ficha_desligada_continua_pulada(monkeypatch, tmp_path):
    _, _db, vistos = _registro_falso(monkeypatch, tmp_path, executar=False)
    assert runner.main(["--sem-envio"]) == 0
    assert vistos == []                            # `executar=False` respeitado


def test_setup_sem_sem_envio_recusa(monkeypatch, tmp_path, capsys):
    _registro_falso(monkeypatch, tmp_path)
    with pytest.raises(SystemExit) as e:
        runner.main(["--setup", "desligado"])
    assert e.value.code == 2
    assert "--sem-envio" in capsys.readouterr().err


def test_setup_desconhecido_recusa(monkeypatch, tmp_path, capsys):
    _registro_falso(monkeypatch, tmp_path)
    with pytest.raises(SystemExit) as e:
        runner.main(["--setup", "nao_existe", "--sem-envio"])
    assert e.value.code == 2
    assert "desconhecido" in capsys.readouterr().err
