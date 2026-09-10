"""GUARDA de posicao duplicada em `_abrir` — com o lado mutante junto.

O runner resolve o que esta aberto ANTES de detectar entrada nova, entao o que
continua OPEN quando `_abrir` roda e posicao que o modelo de saida NAO fechou
nesta barra. Abrir a segunda no MESMO lado dobraria a exposicao do mesmo desenho
no mesmo simbolo e somaria dois pnl onde a serie declara um.

POR DIRECAO, NAO POR SIMBOLO — e a diferenca importa. O briefing pedia a guarda
por `(setup_id, config_hash, symbol)`. Ela nao passa: o DONCHIAN-A mantem LONG e
SHORT abertos ao mesmo tempo no mesmo simbolo, e e o que a instancia legada faz.
Com a guarda por simbolo, `tests/test_v10_espelho_sintetico.py` para de bater
com o shadow (medido: 21 trades a menos no v10, com o log cheio de "posicao
duplicada evitada"), e esse espelho existe justamente para provar que o v10
reproduz o historico legado barra a barra. Nas fichas de saida `reverse` — as
duas que rodam — os dois lados nunca coexistem: quem inverte o alvo FECHA a
posicao na mesma passada, entao a guarda por direcao cobre o caso real.

O TESTE TEM OS DOIS LADOS, que e o que o faz provar alguma coisa:
  - com a guarda: 1 trade e 1 warning nomeado;
  - sem a guarda (`_ConnSemGuarda`, que responde "nada aberto" a consulta dela):
    2 trades. Se este segundo teste passar a devolver 1, ou a guarda mudou de
    forma que a mutacao nao a alcanca mais, ou ela sumiu — nos dois casos e o
    teste que tem de ser lido, nao consertado no automatico.

Mutacao no fonte, para conferir a mao (o resultado tem de ser 2 trades e o
teste do lado de cima tem de FALHAR):

    sed -i "s/^    if aberta is not None:$/    if False:/" v10/runner.py
    python3 -m pytest -q tests/test_v10_runner_guarda.py ; git checkout v10/runner.py

Medido em 10/09: com a mutacao, `test_com_a_guarda_o_segundo_short_nao_abre`
FALHA (2 trades onde a serie declara 1) e os outros tres seguem verdes — o
mutante e pego exatamente pelo teste que existe para pega-lo.
"""

from __future__ import annotations

import logging

import pytest

from v10.runner import rodar
from v10.schema import TABELA, connect
from v10.spec import SetupSpec

BAR = 14_400_000                                   # 4h em ms


def _b(ts):
    return {"ts": ts, "open": 100.0, "high": 100.0, "low": 100.0,
            "close": 100.0, "volume": 1.0}


SERIE = [_b(i * BAR) for i in range(4)]


def _detector_fixo(direction):
    """Emite o MESMO lado em toda barra — o detector patologico que a guarda
    existe para conter. Um detector real de `reverse` so emite na inversao."""
    return lambda velas: {"direction": direction, "entry_price": 100.0,
                          "atr_value": 1.0}


def _spec(direction="SHORT"):
    # h_bars alto e velas chatas: a posicao NAO fecha entre as duas barras, que
    # e a unica situacao em que a guarda tem o que barrar.
    return SetupSpec(setup_id="g", detector=_detector_fixo(direction), tf="4H",
                     cadencia_barras=1, symbols=["BTCUSDT"], warmup_barras=2,
                     exit_model="bracket_simples",
                     exit_params={"s_atr": 50.0, "t_atr": 50.0, "h_bars": 10,
                                  "bar_ms": BAR})


class _Vazio:
    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _ConnSemGuarda:
    """A conexao real, com UMA consulta neutralizada: a da guarda. Mutar a
    entrada da guarda prova o que mutar o fonte provaria — sem editar o fonte."""

    MARCA = "AND direction=? AND status='OPEN' LIMIT 1"

    def __init__(self, conn):
        self._c = conn

    def execute(self, sql, params=()):
        if self.MARCA in sql:
            return _Vazio()
        return self._c.execute(sql, params)

    def __getattr__(self, nome):
        return getattr(self._c, nome)


@pytest.fixture
def conn():
    c = connect(":memory:")
    yield c
    c.close()


def _rodar_duas_barras(conn, spec, log):
    for fim in (2, 3):                             # barras fechadas 1 e 2
        rodar(spec, conn, agora_ms=fim * BAR, velas_fn=lambda s, tf, n: SERIE[:fim],
              log=log)


def _linhas(conn):
    return conn.execute(
        f"SELECT symbol, direction, entry_ts, status FROM {TABELA} "
        "ORDER BY CAST(entry_ts AS INTEGER)").fetchall()


# --- com a guarda -------------------------------------------------------------
def test_com_a_guarda_o_segundo_short_nao_abre(conn, caplog):
    log = logging.getLogger("guarda.com")
    with caplog.at_level(logging.WARNING, logger=log.name):
        _rodar_duas_barras(conn, _spec("SHORT"), log)
    linhas = _linhas(conn)
    assert len(linhas) == 1
    assert (linhas[0]["direction"], linhas[0]["status"]) == ("SHORT", "OPEN")
    assert "posicao duplicada evitada" in caplog.text
    assert "BTCUSDT" in caplog.text and "SHORT" in caplog.text
    assert caplog.text.count("posicao duplicada evitada") == 1


def test_a_guarda_nao_barra_o_lado_oposto(conn):
    """O DONCHIAN-A depende disto: LONG e SHORT convivem no mesmo simbolo."""
    log = logging.getLogger("guarda.oposto")
    rodar(_spec("LONG"), conn, agora_ms=2 * BAR,
          velas_fn=lambda s, tf, n: SERIE[:2], log=log)
    rodar(_spec("SHORT"), conn, agora_ms=3 * BAR,
          velas_fn=lambda s, tf, n: SERIE[:3], log=log)
    assert {l["direction"] for l in _linhas(conn)} == {"LONG", "SHORT"}


def test_depois_de_fechada_o_mesmo_lado_reabre(conn):
    """A guarda e sobre posicao ABERTA. Fechada a primeira, a serie continua —
    barrar tambem a reentrada seria mudar o desenho, nao proteger a serie."""
    log = logging.getLogger("guarda.reabre")
    spec = _spec("SHORT")
    rodar(spec, conn, agora_ms=2 * BAR, velas_fn=lambda s, tf, n: SERIE[:2],
          log=log)
    conn.execute(f"UPDATE {TABELA} SET status='WIN' WHERE status='OPEN'")
    conn.commit()
    rodar(spec, conn, agora_ms=3 * BAR, velas_fn=lambda s, tf, n: SERIE[:3],
          log=log)
    assert [l["status"] for l in _linhas(conn)] == ["WIN", "OPEN"]


# --- sem a guarda: o lado mutante ---------------------------------------------
def test_sem_a_guarda_o_segundo_short_ABRE(conn):
    """Se este teste devolver 1, a mutacao nao alcanca mais a guarda (a consulta
    mudou de forma) — ou nao ha guarda nenhuma. Ler antes de consertar."""
    log = logging.getLogger("guarda.sem")
    _rodar_duas_barras(_ConnSemGuarda(conn), _spec("SHORT"), log)
    linhas = _linhas(conn)
    assert len(linhas) == 2                        # a duplicata que a guarda evita
    assert [l["entry_ts"] for l in linhas] == [str(BAR), str(2 * BAR)]
    assert {l["direction"] for l in linhas} == {"SHORT"}
