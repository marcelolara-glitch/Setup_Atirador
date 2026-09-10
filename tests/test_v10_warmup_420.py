"""GUARDA — o PR do warmup 420, e a prova de que so isso mudou.

Tres coisas sao provadas aqui, nesta ordem de importancia:

1. OS TRES HASHES ANTIGOS NAO SE MEXERAM. `kis_3489_60t_4h` SAIU DO AR
   (`executar=False`) e continua em `82488baa3086`: `executar` esta em
   `spec.FORA_DO_HASH`, entao desliga-la nao PODE mover o hash dela — e nao
   mover e o ponto, porque o hash e o ENDERECO da serie ja gravada em
   `trades_v10`. A ficha nova entra com hash proprio, diferente dos tres.

   O QUE ESTE PR NAO FEZ, E POR QUE: o briefing pedia tambem
   `estado_ciclo="invalidado_coletor"` e um `aviso` novo na ficha antiga. Esses
   dois campos ENTRAM no `config_hash` (`FORA_DO_HASH` tem `executar` e mais
   nada), e reescreve-los trocaria `82488baa3086` por `1f9a9c2aed27` — as ~10
   dias de linhas ja gravadas deixariam de ser enderecaveis pela ficha, e o
   relatorio as veria como "linha(s) de outra configuracao fora desta conta".
   Como o proprio briefing poe a preservacao do hash como prova obrigatoria, o
   registro da invalidacao ficou em COMENTARIO no `v10/registro.py` (comentario
   nao e campo, nao entra no hash) e no `aviso` da ficha sucessora, que e o que
   chega ao leitor do quadro diario. Este teste trava as duas pontas.

2. `ATIVOS` TROCA A FICHA INVALIDADA PELA SUCESSORA. Segue com dois setups —
   `[kis_regime_4h, kis_3489_60t_4h_w420]` — e `ATIVOS` continua DERIVADO do
   `executar`, nunca uma segunda lista escrita a mao.

3. JANELA CURTA E FALHA NOMEADA, NUNCA SERIE CURTA. O runner pede
   `warmup_barras + FOLGA_BARRAS` (420 + 80 = 500) e confere o que voltou:
   abaixo do WARMUP declarado o simbolo entra em `falhas`/`falha_symbols` e o
   detector NAO e chamado; entre o warmup e o pedido, o detector roda e a
   diferenca sai nomeada no log (`n=<recebido>/<pedido>`) — e assim que um teto
   de `limit` da corretora aparece ANTES de cortar o warmup.

`pandas_ta` nao instala no sandbox (Python 3.11 x requisito >= 3.12) e nada
deste arquivo o alcanca: `v10.spec`, `v10.registro` e o laco do runner sao
stdlib puro.
"""

from __future__ import annotations

import logging
from dataclasses import replace

import v10.registro as reg
from v10.registro import (ATIVOS, DONCHIAN_A_4H, KIS_3489_60T_4H,
                          KIS_3489_60T_4H_W420, KIS_REGIME_4H, REGISTRO)
from v10.runner import FOLGA_BARRAS, rodar
from v10.schema import TABELA, connect
from v10.spec import FORA_DO_HASH, SetupSpec, config_dict

BAR = 14_400_000                                   # 4h em ms

# Os quatro hashes congelados, literais de proposito: rebaselinar exige EDITAR
# estas constantes, que e decisao visivel no diff. Se um destes testes falhar,
# e a mudanca que esta errada, nunca o hash.
HASHES = {"kis_regime_4h": "e63ec120e131",
          "kis_3489_60t_4h": "82488baa3086",
          "kis_3489_60t_4h_w420": "29752fcbad16",
          "donchian_a_4h": "250170cc8dc0"}


# --- 1. hashes ----------------------------------------------------------------
def test_os_quatro_config_hash_saem_impressos_e_conferidos(capsys):
    obtido = {sid: s.config_hash for sid, s in REGISTRO.items()}
    with capsys.disabled():
        print("\n  config_hash apos o PR do warmup 420:")
        for sid, h in obtido.items():
            marca = "OK" if HASHES.get(sid) == h else "MUDOU"
            print(f"    {sid:<22} {h}  ({marca})")
    assert obtido == HASHES


def test_desligar_a_ficha_invalidada_nao_moveu_o_hash_dela():
    """O ponto inteiro de `FORA_DO_HASH`, agora no sentido de desligar: a serie
    de `trades_v10` gravada sob `82488baa3086` continua enderecavel pela ficha
    que a gravou."""
    assert KIS_3489_60T_4H.executar is False
    assert KIS_3489_60T_4H.config_hash == "82488baa3086"
    assert replace(KIS_3489_60T_4H, executar=True).config_hash == "82488baa3086"


def test_executar_nao_entra_no_config_dict_de_nenhuma_ficha():
    assert FORA_DO_HASH == ("executar",)
    for sid, s in REGISTRO.items():
        assert "executar" not in config_dict(s), sid


def test_virar_executar_nos_dois_sentidos_nao_move_hash_nenhum():
    """Camada que nao passa por coincidencia: (a) provaria o mesmo se o campo
    tivesse entrado no hash e o numero fosse rebaselinado junto."""
    for sid, s in REGISTRO.items():
        assert replace(s, executar=False).config_hash == HASHES[sid], sid
        assert replace(s, executar=True).config_hash == HASHES[sid], sid


def test_o_hash_da_sucessora_e_diferente_dos_tres_antigos():
    """Warmup ENTRA no hash — e por isso que a serie do warmup 420 nao se
    mistura com a do warmup 89, que e o motivo de existir uma ficha nova em vez
    de uma edicao na antiga."""
    novo = KIS_3489_60T_4H_W420.config_hash
    assert novo == "29752fcbad16"
    assert novo not in {"e63ec120e131", "82488baa3086", "250170cc8dc0"}
    assert replace(KIS_3489_60T_4H, warmup_barras=420).config_hash != "82488baa3086"


def test_o_estado_ciclo_e_o_aviso_da_ficha_antiga_seguem_intactos():
    """A prova do que o item 1 do cabecalho explica: mexer neles TROCARIA o
    hash, e o endereco da serie vale mais que o rotulo. Quem carrega a
    invalidacao e o comentario do registro e o `aviso` da sucessora."""
    assert KIS_3489_60T_4H.estado_ciclo == "proposto"
    assert "EIXO 1" in KIS_3489_60T_4H.aviso
    trocado = replace(KIS_3489_60T_4H, estado_ciclo="invalidado_coletor")
    assert trocado.config_hash != "82488baa3086"        # e por isso que nao foi
    assert "INVALIDADA" in KIS_3489_60T_4H_W420.aviso   # o leitor recebe assim
    assert "kis_3489_60t_4h" in KIS_3489_60T_4H_W420.aviso
    assert "warmup 420" in KIS_3489_60T_4H_W420.aviso


# --- 2. a ficha nova e o ATIVOS -----------------------------------------------
def test_a_sucessora_copia_a_antiga_e_muda_so_a_janela():
    a, b = KIS_3489_60T_4H, KIS_3489_60T_4H_W420
    assert b.setup_id == "kis_3489_60t_4h_w420"
    assert b.warmup_barras == 420 and a.warmup_barras == 89
    assert b.detector is a.detector                    # nao ha segunda conta
    assert b.detector_params == a.detector_params == reg.PARAMS_KIS_3489
    assert b.symbols == a.symbols == reg.SYMBOLS_KIS_3489
    assert len(b.symbols) == 65 and "TONUSDT" not in b.symbols
    assert (b.tf, b.cadencia_barras, b.exit_model, b.exit_params, b.mode) == \
           (a.tf, a.cadencia_barras, a.exit_model, a.exit_params, a.mode)
    assert b.custo_bps_por_perna == a.custo_bps_por_perna
    assert b.estado_ciclo == "proposto" and b.executar is True


def test_a_lista_de_simbolos_e_importada_nao_copiada():
    """Duas listas literais divergiriam em silencio no dia em que uma mudasse."""
    assert KIS_3489_60T_4H_W420.symbols == reg.SYMBOLS_KIS_3489
    assert KIS_3489_60T_4H_W420.symbols is not reg.SYMBOLS_KIS_3489   # copia rasa


def test_ativos_troca_a_invalidada_pela_sucessora(capsys):
    ids = [s.setup_id for s in ATIVOS]
    with capsys.disabled():
        print(f"\n  ATIVOS ({len(ids)}): {ids}")
    assert ids == ["kis_regime_4h", "kis_3489_60t_4h_w420"]
    assert ATIVOS == [KIS_REGIME_4H, KIS_3489_60T_4H_W420]
    assert KIS_3489_60T_4H not in ATIVOS and DONCHIAN_A_4H not in ATIVOS


def test_ativos_continua_derivado_do_registro_pelo_proprio_executar():
    assert reg.ATIVOS == [s for s in REGISTRO.values() if s.executar]


def test_a_ficha_invalidada_nao_sumiu_do_registro():
    """Ela documenta a serie que gravou e continua no quadro diario."""
    assert REGISTRO["kis_3489_60t_4h"] is KIS_3489_60T_4H
    assert set(REGISTRO) == set(HASHES)


def test_o_kis_regime_4h_nao_foi_tocado():
    """O diag deu 0% de divergencia nele (par 8/21, janela 140). Nao mexer."""
    assert KIS_REGIME_4H.warmup_barras == 60 and KIS_REGIME_4H.executar is True
    assert KIS_REGIME_4H.config_hash == "e63ec120e131"


# --- 3. a janela que chega ao detector ----------------------------------------
def _b(ts):
    return {"ts": ts, "open": 100.0, "high": 100.0, "low": 100.0,
            "close": 100.0, "volume": 1.0}


def _spec_420(**kw):
    base = dict(setup_id="w420", detector=lambda v: {"direction": "LONG",
                                                     "entry_price": 100.0,
                                                     "atr_value": 1.0},
                tf="4H", cadencia_barras=1, symbols=["BTCUSDT"],
                warmup_barras=420, exit_model="bracket_simples",
                exit_params={"s_atr": 1.5, "t_atr": 6.0, "h_bars": 4,
                             "bar_ms": BAR})
    base.update(kw)
    return SetupSpec(**base)


def _fonte(quantas):
    """Devolve `quantas` velas, ignorando o `n` pedido — e o teto da corretora
    simulado: ela responde menos do que se pediu e nao avisa."""
    def velas_fn(symbol, tf, n):
        return [_b(i * BAR) for i in range(1, quantas + 1)]
    return velas_fn


def test_o_runner_pede_warmup_mais_folga(monkeypatch):
    pedidos = []

    def velas_fn(symbol, tf, n):
        pedidos.append(n)
        return [_b(i * BAR) for i in range(1, n + 1)]

    conn = connect(":memory:")
    rodar(_spec_420(), conn, agora_ms=10_000 * BAR, velas_fn=velas_fn,
          log=logging.getLogger("w420.pedido"))
    assert pedidos == [420 + FOLGA_BARRAS] == [500]


def test_abaixo_do_warmup_o_simbolo_falha_nomeado_e_o_detector_nao_roda(caplog):
    chamou = []

    def detector(velas):
        chamou.append(len(velas))
        return {"direction": "LONG", "entry_price": 100.0, "atr_value": 1.0}

    conn = connect(":memory:")
    log = logging.getLogger("w420.curta")
    with caplog.at_level(logging.WARNING, logger=log.name):
        r = rodar(_spec_420(detector=detector), conn, agora_ms=10_000 * BAR,
                  velas_fn=_fonte(419), log=log)
    assert (r["falhas"], r["abertos"], r["ok"]) == (1, 0, 0)
    assert r["falha_symbols"] == ["BTCUSDT"]
    assert chamou == []                                  # nao viu vela nenhuma
    assert "BTCUSDT" in caplog.text and "419 < 420" in caplog.text
    assert "n=419/500" in caplog.text                    # recebido/pedido
    assert conn.execute(f"SELECT COUNT(*) FROM {TABELA}").fetchone()[0] == 0


def test_coleta_curta_acima_do_warmup_roda_e_sai_nomeada_no_log(caplog):
    """499 de 500: o detector tem a janela que a ficha DECLARA precisar (420),
    entao roda — mas a diferenca nao some. Nomear a diferenca aqui e o que faz
    um teto de `limit` aparecer antes de virar warmup curto."""
    conn = connect(":memory:")
    log = logging.getLogger("w420.quase")
    with caplog.at_level(logging.WARNING, logger=log.name):
        r = rodar(_spec_420(), conn, agora_ms=10_000 * BAR,
                  velas_fn=_fonte(499), log=log)
    assert (r["falhas"], r["abertos"], r["ok"]) == (0, 1, 1)
    assert "coleta curta: n=499/500" in caplog.text and "BTCUSDT" in caplog.text


def test_janela_cheia_nao_reclama_de_nada(caplog):
    conn = connect(":memory:")
    log = logging.getLogger("w420.cheia")
    with caplog.at_level(logging.WARNING, logger=log.name):
        r = rodar(_spec_420(), conn, agora_ms=10_000 * BAR,
                  velas_fn=_fonte(500), log=log)
    assert (r["falhas"], r["abertos"]) == (0, 1)
    assert "coleta curta" not in caplog.text


def test_uma_falha_de_coleta_nao_derruba_os_outros_simbolos(caplog):
    """Isolamento por simbolo: com 65 tokens em serie, quem cai sao
    preferencialmente os do fim da lista, sempre os mesmos."""
    def velas_fn(symbol, tf, n):
        return [_b(i * BAR) for i in range(1, (300 if symbol == "AAA" else n) + 1)]

    conn = connect(":memory:")
    log = logging.getLogger("w420.isola")
    with caplog.at_level(logging.WARNING, logger=log.name):
        r = rodar(_spec_420(symbols=["AAA", "BTCUSDT"]), conn,
                  agora_ms=10_000 * BAR, velas_fn=velas_fn, log=log)
    assert (r["ok"], r["falhas"], r["abertos"]) == (1, 1, 1)
    assert r["falha_symbols"] == ["AAA"]
    assert "n=300/500" in caplog.text          # o teto de 300, se for esse
