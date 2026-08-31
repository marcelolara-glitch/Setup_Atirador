"""GUARDA — o PR que LIGA a `kis_3489_60t_4h`, e a prova de que so isso mudou.

O diff deste PR e UMA linha: `executar=False` -> `executar=True` na ficha
`kis_3489_60t_4h`. Nenhum outro campo, nenhuma outra ficha, nem o cron.

Duas coisas sao provadas aqui, nesta ordem de importancia:

1. NENHUM `config_hash` SE MEXEU. `executar` esta em `spec.FORA_DO_HASH`, entao
   liga-lo nao PODE mover hash nenhum — e essa e a razao de ele estar la: uma
   ficha pausada e religada devolve as linhas novas a MESMA serie de
   `trades_v10`, em vez de partir o historico em dois. A prova vem em tres
   camadas, da mais fraca para a mais forte:
     (a) os tres hashes literais continuam `e63ec120e131`, `82488baa3086` e
         `250170cc8dc0` — os mesmos numeros que `test_v10_coleta` e
         `test_v10_venue` ja congelaram;
     (b) `config_dict` de cada ficha nao contem a chave `executar`;
     (c) virar `executar` nos DOIS sentidos, em cada uma das tres fichas,
         devolve o hash identico. Esta e a camada que vale: (a) provaria a
         mesma coisa por coincidencia se o campo tivesse entrado no hash e o
         numero tivesse sido rebaselinado junto; (c) nao tem como passar por
         coincidencia.

2. `ATIVOS` PASSA A TER DOIS SETUPS. `ATIVOS` e derivado do REGISTRO pelo
   proprio campo `executar` — nunca uma segunda lista escrita a mao —, entao a
   linha virada e o unico lugar de onde a ficha nova pode ter entrado. Sai de
   `[kis_regime_4h]` e chega em `[kis_regime_4h, kis_3489_60t_4h]`, com o
   `donchian_a_4h` FORA: a instancia dele que conta e a legada, e o relogio da
   janela pre-registrada dele nao pode reiniciar.

O que este arquivo NAO reprova continua reprovado: a ficha entra ligada com
`estado_ciclo="proposto"` e com o `aviso` inteiro — celula do EIXO 1 varrida na
MESMA janela que a motiva, sem stop e sem cap de tempo. Ligar a coleta e ligar
a MEDICAO, nao promover o desenho.

`pandas_ta` nao instala no sandbox (Python 3.11 x requisito >= 3.12), e nada
deste arquivo o alcanca: `v10.spec` e `v10.registro` sao stdlib puro.
"""

from __future__ import annotations

from dataclasses import replace

import v10.registro as reg
from v10.registro import (ATIVOS, DONCHIAN_A_4H, KIS_3489_60T_4H,
                          KIS_REGIME_4H, REGISTRO)
from v10.spec import FORA_DO_HASH, config_dict

# Os hashes congelados, literais no arquivo de proposito: rebaselinar exige
# EDITAR estas constantes, que e uma decisao visivel no diff, e nao um numero
# que se conserta sozinho. Se um destes testes falhar, e a mudanca que esta
# errada, nunca o hash.
HASHES = {"kis_regime_4h": "e63ec120e131",
          "kis_3489_60t_4h": "82488baa3086",
          "donchian_a_4h": "250170cc8dc0"}


# --- 1. nenhum hash se mexeu --------------------------------------------------
def test_os_tres_config_hash_seguem_identicos(capsys):
    """Camada (a): os tres numeros congelados, impressos e conferidos."""
    obtido = {sid: s.config_hash for sid, s in REGISTRO.items()}
    with capsys.disabled():
        print("\n  config_hash apos LIGAR a kis_3489_60t_4h:")
        for sid, h in obtido.items():
            marca = "OK" if HASHES.get(sid) == h else "MUDOU"
            print(f"    {sid:<20} {h}  ({marca})")
    assert obtido == HASHES


def test_executar_nao_entra_no_dicionario_hasheado():
    """Camada (b): a chave nem chega ao dicionario que vira sha256."""
    assert "executar" in FORA_DO_HASH
    for sid, spec in REGISTRO.items():
        assert "executar" not in config_dict(spec), sid


def test_virar_executar_nos_dois_sentidos_nao_move_hash(capsys):
    """Camada (c): a prova mecanica — e a que nao passa por coincidencia.

    Se `executar` tivesse entrado no hash e os literais acima tivessem sido
    rebaselinados junto, (a) passaria e ESTE teste falharia."""
    with capsys.disabled():
        print("\n  hash com executar=False / executar=True:")
    for sid, spec in REGISTRO.items():
        desligada = replace(spec, executar=False).config_hash
        ligada = replace(spec, executar=True).config_hash
        with capsys.disabled():
            print(f"    {sid:<20} {desligada}  {ligada}")
        assert desligada == ligada == HASHES[sid], sid


def test_a_serie_de_trades_da_ficha_nova_nao_se_parte():
    """O motivo de (1) importar: o hash sob o qual a ficha foi revisada
    DESLIGADA e o mesmo sob o qual ela grava LIGADA. As linhas novas caem na
    mesma serie de `trades_v10` — que e o ponto inteiro de `FORA_DO_HASH`."""
    assert KIS_3489_60T_4H.executar is True
    assert KIS_3489_60T_4H.config_hash == "82488baa3086"
    assert replace(KIS_3489_60T_4H, executar=False).config_hash == "82488baa3086"


# --- 2. ATIVOS passa a ter dois setups ----------------------------------------
def test_ativos_passa_a_ter_dois_setups(capsys):
    ids = [s.setup_id for s in ATIVOS]
    with capsys.disabled():
        print(f"\n  ATIVOS ({len(ids)}): {ids}")
    assert len(ATIVOS) == 2
    assert ids == ["kis_regime_4h", "kis_3489_60t_4h"]
    assert ATIVOS == [KIS_REGIME_4H, KIS_3489_60T_4H]


def test_ativos_e_derivado_do_registro_pelo_proprio_executar():
    """Nao ha segunda lista a manter em sincronia: `ATIVOS` e filtro sobre o
    REGISTRO. A linha virada e o UNICO lugar de onde a ficha pode ter entrado."""
    assert reg.ATIVOS == [s for s in REGISTRO.values() if s.executar]


def test_o_registro_segue_com_as_tres_fichas():
    assert set(REGISTRO) == {"kis_regime_4h", "kis_3489_60t_4h", "donchian_a_4h"}


def test_donchian_continua_desligado_e_kis_regime_intocada():
    """As outras duas fichas nao foram tocadas. O DONCHIAN-A fica FORA: a
    instancia que conta e a legada (`shadow/donchian_a.py`, banco e cron
    proprios), e uma segunda instancia em paralelo divide a leitura da janela
    pre-registrada em vez de somar evidencia."""
    assert DONCHIAN_A_4H.executar is False
    assert DONCHIAN_A_4H not in ATIVOS
    assert KIS_REGIME_4H.executar is True          # ja era True antes do PR


# --- 3. ligar a coleta nao promove o desenho ----------------------------------
def test_a_ficha_entra_ligada_ainda_como_proposta_e_com_o_aviso_inteiro():
    s = KIS_3489_60T_4H
    assert s.estado_ciclo == "proposto"            # NAO virou promovido
    assert s.mode == "shadow"                      # NAO virou live
    assert "EIXO 1" in s.aviso and "hold-out" in s.aviso
    assert "SEM stop e SEM cap de tempo" in s.aviso
