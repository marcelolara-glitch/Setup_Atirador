"""SetupSpec + config_hash: determinismo, estabilidade e sensibilidade."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import fields, replace
from pathlib import Path

from v10.schema import COLUNAS, DDL, TABELA, connect
from v10.spec import (FORA_DO_HASH, SetupSpec, config_dict,
                      config_hash)

ROOT = Path(__file__).resolve().parents[1]


def detector_a(velas):    # noqa: D103
    return None


def detector_b(velas):    # noqa: D103
    return None


def _spec(**kw) -> SetupSpec:
    base = dict(setup_id="kis_regime_4h", detector=detector_a, tf="4H",
                cadencia_barras=1, symbols=["BTCUSDT", "ETHUSDT"],
                warmup_barras=21, exit_model="reverse",
                exit_params={"alvo": "extremos", "conf": 2},
                detector_params={"limiar": 0.02, "adx_min": 11},
                direcoes=("LONG", "SHORT"), custo_bps_por_perna=6.0,
                mode="shadow", estado_ciclo="proposto", aviso="janela unica")
    base.update(kw)
    return SetupSpec(**base)


# --- determinismo ------------------------------------------------------------
def test_hash_tem_12_chars_hex():
    h = _spec().config_hash
    assert len(h) == 12 and all(c in "0123456789abcdef" for c in h)


def test_mesma_config_mesmo_hash():
    assert _spec().config_hash == _spec().config_hash


def test_reordenar_o_dict_nao_muda_o_hash():
    a = _spec(exit_params={"alvo": "extremos", "conf": 2})
    b = _spec(exit_params={"conf": 2, "alvo": "extremos"})
    assert a.exit_params != list(b.exit_params)      # a ordem de fato difere
    assert list(a.exit_params) != list(b.exit_params)
    assert a.config_hash == b.config_hash


def test_hash_estavel_entre_processos():
    # hash() do Python e randomizado por PYTHONHASHSEED — se alguem trocar
    # sha256 por hash(), dois processos discordam e este teste reprova.
    codigo = (
        "import sys; sys.path.insert(0, %r);"
        "from tests.test_v10_spec import _spec; print(_spec().config_hash)"
        % str(ROOT)
    )
    saidas = {
        subprocess.run([sys.executable, "-c", codigo], capture_output=True,
                       text=True, cwd=str(ROOT), check=True).stdout.strip()
        for _ in range(2)
    }
    assert saidas == {_spec().config_hash}


# --- sensibilidade: QUALQUER parametro muda o hash ---------------------------
_VARIACOES = {
    "setup_id": "outro", "detector": detector_b, "tf": "1H",
    "cadencia_barras": 2, "symbols": ["BTCUSDT"], "warmup_barras": 22,
    "exit_model": "bracket_multi", "exit_params": {"alvo": "extremos", "conf": 3},
    "detector_params": {"limiar": 0.03, "adx_min": 11},
    "direcoes": ("LONG",), "custo_bps_por_perna": 12.0, "mode": "live",
    "estado_ciclo": "em_bancada", "aviso": "",
}


def test_todo_campo_do_spec_esta_coberto_pelas_variacoes():
    # Cada campo esta OU nas variacoes (entra no hash) OU em FORA_DO_HASH (nao
    # entra). Campo novo sem decisao explicita reprova aqui, que e o ponto.
    assert set(_VARIACOES) | set(FORA_DO_HASH) == {f.name for f in fields(SetupSpec)}
    assert not set(_VARIACOES) & set(FORA_DO_HASH)


def test_qualquer_parametro_muda_o_hash():
    base = _spec()
    for campo, valor in _VARIACOES.items():
        assert replace(base, **{campo: valor}).config_hash != base.config_hash, campo


def test_hashes_das_variacoes_sao_todos_distintos():
    # Nao basta cada um diferir da base: dois campos diferentes nao podem
    # colidir entre si, senao o hash deixa de identificar a configuracao.
    hs = {replace(_spec(), **{c: v}).config_hash for c, v in _VARIACOES.items()}
    assert len(hs) == len(_VARIACOES)


def test_parametro_do_detector_muda_o_hash():
    # O motivo do campo existir. Antes do PR-E o limiar vivia DENTRO do
    # detector: trocar 0.02 por 0.03 deixava o hash igual, e as duas
    # configuracoes gravavam na mesma serie de `trades_v10`.
    a = _spec(detector_params={"limiar": 0.02, "adx_min": 11})
    b = _spec(detector_params={"limiar": 0.03, "adx_min": 11})
    assert a.config_hash != b.config_hash


def test_reordenar_detector_params_nao_muda_o_hash():
    a = _spec(detector_params={"limiar": 0.02, "adx_min": 11})
    b = _spec(detector_params={"adx_min": 11, "limiar": 0.02})
    assert list(a.detector_params) != list(b.detector_params)
    assert a.config_hash == b.config_hash


def test_reload_do_detector_nao_muda_o_hash():
    # O callable entra como modulo.qualname, nao como id() — recarregar o
    # modulo (endereco novo) nao pode invalidar o historico ja gravado.
    igual = _spec(detector=detector_a)
    assert config_dict(igual)["detector"].endswith(".detector_a")
    assert igual.config_hash == _spec().config_hash


def test_int_e_float_nao_colidem():
    assert (_spec(custo_bps_por_perna=6).config_hash
            != _spec(custo_bps_por_perna=6.0).config_hash)


# --- a excecao declarada: FORA_DO_HASH ---------------------------------------
def test_fora_do_hash_e_curta_e_declarada():
    # Lista fechada. Crescer aqui e decisao de projeto, e este teste obriga a
    # tomar essa decisao em vez de deixar o campo escorregar para fora do hash.
    assert FORA_DO_HASH == ("executar",)


def test_executar_nao_muda_o_hash():
    # `executar` diz se o ORQUESTRADOR chama o setup, nao o que o setup mede.
    # Se entrasse no hash, pausar e retomar partiria o historico em duas series
    # — exatamente o oposto do que o hash serve.
    assert _spec(executar=False).config_hash == _spec(executar=True).config_hash
    assert "executar" not in config_dict(_spec())


def test_config_hash_funcao_e_propriedade_concordam():
    s = _spec()
    assert config_hash(s) == s.config_hash


# --- schema ------------------------------------------------------------------
def test_tabela_e_indices_criados(tmp_path):
    conn = connect(tmp_path / "j.db")
    tabelas = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    indices = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")}
    assert tabelas == {TABELA}          # nao cria, nao toca, nao migra `trades`
    assert indices == {"idx_v10_setup_status", "idx_v10_symbol", "idx_v10_entry_ts"}


def test_colunas_batem_com_a_lista_canonica(tmp_path):
    conn = connect(tmp_path / "j.db")
    reais = tuple(d[1] for d in conn.execute(f"PRAGMA table_info({TABELA})"))
    assert reais == COLUNAS


def test_nenhuma_coluna_de_modelo_de_saida(tmp_path):
    # A regra do schema: TP/SL do bracket vao em exit_params_json; o alvo da
    # inversao em exit_state_json. Coluna dedicada aqui e regressao de projeto.
    proibidas = ("tp1", "tp2", "tp3", "sl_", "current_sl", "position_split",
                 "alvo", "leverage", "tp1_hit")
    assert not [c for c in COLUNAS if any(p in c for p in proibidas)]


def test_unique_da_idempotencia(tmp_path):
    import sqlite3
    conn = connect(tmp_path / "j.db")
    linha = ("id1", "s", "abc123", "shadow", "BTCUSDT", "4H", "LONG",
             "2026-08-01T00:00:00", 100.0)
    cols = "id,setup_id,config_hash,mode,symbol,tf,direction,entry_ts,entry_price"
    conn.execute(f"INSERT INTO {TABELA} ({cols}) VALUES (?,?,?,?,?,?,?,?,?)", linha)
    try:
        conn.execute(f"INSERT INTO {TABELA} ({cols}) VALUES (?,?,?,?,?,?,?,?,?)",
                     ("id2",) + linha[1:])
        raise AssertionError("UNIQUE nao barrou a reinsercao da mesma barra")
    except sqlite3.IntegrityError:
        pass
    # config_hash diferente e configuracao diferente: pode coexistir.
    conn.execute(f"INSERT INTO {TABELA} ({cols}) VALUES (?,?,?,?,?,?,?,?,?)",
                 ("id3", "s", "def456") + linha[3:])
    assert conn.execute(f"SELECT COUNT(*) FROM {TABELA}").fetchone()[0] == 2


def test_ddl_nao_menciona_a_tabela_do_v9():
    # `trades` fica READ-ONLY: nenhum ALTER, nenhum DROP, nenhum INSERT.
    assert "trades_v10" in DDL
    assert " trades " not in DDL.replace("trades_v10", "")
    assert "ALTER" not in DDL.upper() and "DROP" not in DDL.upper()
