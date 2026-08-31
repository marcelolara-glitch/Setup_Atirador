"""GUARDA — a ficha 34/89 em 65 tokens, e a prova de que ela nao tocou a curta.

Tres coisas sao provadas aqui, nesta ordem de importancia:

1. REGRESSAO DURA. `kis_regime_4h` continua com `config_hash` `e63ec120e131`.
   Se este teste falhar, e a mudanca que esta errada, nunca o hash. O valor esta
   literal no arquivo de proposito: rebaselinar exige EDITAR a constante, que e
   uma decisao visivel no diff, e nao um numero que se conserta sozinho.

2. HASH NOVO E DIFERENTE. Duas fichas com hash igual seriam duas configuracoes
   caindo na MESMA serie de `trades_v10` — o buraco exato que o campo existe
   para fechar.

3. O HASH DISCRIMINA O PAR. Um teste que passa sem discriminar e pior que teste
   ausente: ele afirma que a identidade da configuracao esta protegida quando
   nao esta. Aqui a prova e direta — trocar 34/89 por 8/21 em `detector_params`,
   sem mexer em mais nada, tem que devolver outro hash. E o mesmo para cada um
   dos SETE parametros, um a um.

`incl_ema_slow` ganha secao propria porque e o unico numero desta ficha que NAO
segue o par: a rampa do portao le a EMA21 em toda celula, inclusive nas de par
longo (ver `backtest/kis_horizonte.py`, cabecalho "PORTAO FIXO, NAO VARIAVEL").
Um campo que nao muda nada seria decoracao — entao ha teste mostrando que 21 e
89 produzem inclinacoes DIFERENTES na mesma serie.

`pandas_ta` nao instala no sandbox (Python 3.11 x requisito >= 3.12), mas nada
deste arquivo o alcanca: `v10.spec`, `v10.registro` e `backtest.kis_regime` sao
stdlib puro. Este arquivo COLETA e roda inteiro no sandbox.
"""

from __future__ import annotations

from dataclasses import replace

import backtest.kis_regime as bkr
import v10.registro as reg
from v10.registro import (KIS_3489_60T_4H, KIS_REGIME_4H, PARAMS_KIS_3489,
                          REGISTRO, SYMBOLS_KIS_3489)

# O hash congelado da ficha curta. Ver item 1 da docstring.
HASH_KIS_REGIME_4H = "e63ec120e131"
# Delistada em 07/2026 (ultimo candle 05/06/2026). A ausencia fica pelo NOME:
# sem isto, a lista de 65 pareceria uma escolha de merito.
DELISTADO = "TONUSDT"


def _serie(n: int = 400) -> list:
    """Passeio de LCG em centesimos — inteiro puro, identico entre maquinas.
    Mesma forma do gerador de `test_v10_detector_params`, mais longo porque uma
    EMA89 precisa de mais barra que uma EMA21 para sair do warmup."""
    x, preco, out = 20260827, 10_000, []
    for i in range(n):
        x = (1103515245 * x + 12345) % 2147483648
        preco = max(2000, preco + (x % 201) - 100)
        out.append({"ts": i * 14_400_000, "open": preco / 100.0,
                    "high": (preco + 60) / 100.0, "low": (preco - 60) / 100.0,
                    "close": preco / 100.0, "volume": 1.0})
    return out


# --- 1. regressao dura: a ficha curta nao foi tocada -------------------------
def test_kis_regime_4h_mantem_o_hash_congelado():
    h = KIS_REGIME_4H.config_hash
    print(f"\nconfig_hash kis_regime_4h   = {h}  (esperado {HASH_KIS_REGIME_4H})")
    assert h == HASH_KIS_REGIME_4H, (
        f"kis_regime_4h mudou de hash: {h} != {HASH_KIS_REGIME_4H}. E a "
        f"mudanca que esta errada, nao o hash.")


def test_a_ficha_curta_segue_com_universo_warmup_e_params_intactos():
    # O hash ja cobriria isto, mas um hash quebrado nao diz O QUE mudou.
    assert KIS_REGIME_4H.symbols == ["ARBUSDT", "BNBUSDT", "BTCUSDT", "SUIUSDT",
                                     "TRXUSDT", "WLDUSDT"]
    assert KIS_REGIME_4H.warmup_barras == 60
    assert KIS_REGIME_4H.detector_params == {
        "limiar": 0.02, "adx_min": 11, "ema_fast": 8, "ema_slow": 21,
        "atr_len": 14, "adx_len": 13, "confirmacao": 2}
    assert "incl_ema_slow" not in KIS_REGIME_4H.detector_params


def test_avaliar_da_bancada_nao_mudou_de_comportamento_padrao():
    # `incl_ema_slow` entrou como argumento OPCIONAL: sem passa-lo, a conta e a
    # de sempre. Se nao fosse, a ficha curta mudaria de resultado sem mudar de
    # hash — pior que mudar de hash.
    velas = _serie()
    for i in range(21, len(velas), 7):
        janela = velas[:i + 1]
        assert bkr.avaliar(janela) == bkr.avaliar(janela, incl_ema_slow=None)


# --- 2. a ficha nova, e o hash dela ------------------------------------------
def test_hash_novo_existe_e_e_diferente_do_da_ficha_curta():
    h = KIS_3489_60T_4H.config_hash
    print(f"config_hash kis_3489_60t_4h = {h}")
    assert len(h) == 12 and h != HASH_KIS_REGIME_4H


def test_todos_os_hashes_do_registro_sao_distintos():
    hs = {sid: s.config_hash for sid, s in REGISTRO.items()}
    assert len(set(hs.values())) == len(hs), f"hash repetido no registro: {hs}"


def test_o_registro_tem_as_tres_fichas_e_a_nova_executa():
    # A ficha entrou DESLIGADA no PR que a criou, para que o `config_hash` fosse
    # revisado antes de existir a primeira linha de trade sob ele, e foi ligada
    # num PR proprio de uma linha. O hash NAO se mexeu na virada — `executar`
    # esta em `FORA_DO_HASH` —, entao a serie de `trades_v10` e a mesma; a prova
    # esta em `tests/test_v10_liga_kis_3489.py`.
    assert set(REGISTRO) == {"kis_regime_4h", "kis_3489_60t_4h", "donchian_a_4h"}
    assert KIS_3489_60T_4H.executar is True
    assert [s.setup_id for s in reg.ATIVOS] == ["kis_regime_4h", "kis_3489_60t_4h"]


def test_a_ficha_nova_declara_o_que_o_briefing_pediu():
    s = KIS_3489_60T_4H
    assert s.setup_id == "kis_3489_60t_4h"
    assert s.tf == "4H" and s.cadencia_barras == 1
    assert s.exit_model == "reverse" and s.exit_params == {}
    assert s.custo_bps_por_perna == 5.0 and s.mode == "shadow"
    assert s.warmup_barras == 89                  # EMA lenta do par
    assert s.detector_params == {
        "limiar": 0.02, "adx_min": 11, "ema_fast": 34, "ema_slow": 89,
        "atr_len": 14, "adx_len": 13, "confirmacao": 2, "incl_ema_slow": 21}


def test_saida_reverse_nao_tem_stop_alvo_nem_cap():
    # `exit_params` vazio E o desenho: o `reverse` so fecha na inversao. Um
    # `h_bars` ou `s_atr` que aparecesse aqui seria outro setup.
    assert not KIS_3489_60T_4H.exit_params
    assert not {"s_atr", "t_atr", "h_bars"} & set(KIS_3489_60T_4H.exit_params)


# --- o universo, congelado e literal -----------------------------------------
def test_universo_tem_65_simbolos_unicos_e_ordenados():
    assert len(SYMBOLS_KIS_3489) == 65
    assert len(set(SYMBOLS_KIS_3489)) == 65
    assert SYMBOLS_KIS_3489 == sorted(SYMBOLS_KIS_3489)
    assert all(s.endswith("USDT") for s in SYMBOLS_KIS_3489)


def test_o_delistado_esta_fora_e_a_exclusao_esta_registrada_pelo_nome():
    assert DELISTADO not in SYMBOLS_KIS_3489
    fonte = (reg.__file__ and open(reg.__file__, encoding="utf-8").read()) or ""
    assert DELISTADO in fonte, (
        "a exclusao por delistagem tem que estar escrita: sem ela a lista de "
        "65 parece escolha de merito")


def test_a_ficha_carrega_uma_COPIA_do_universo():
    # `symbols=list(...)`: mutar a ficha nao pode reescrever a constante, senao
    # o universo mudaria em runtime sem mudar o hash.
    assert KIS_3489_60T_4H.symbols == SYMBOLS_KIS_3489
    assert KIS_3489_60T_4H.symbols is not SYMBOLS_KIS_3489
    assert KIS_3489_60T_4H.detector_params is not PARAMS_KIS_3489


def test_o_universo_e_literal_e_nao_lido_de_arquivo_nem_de_api():
    # Uma lista lida em runtime mudaria o universo sem mudar o `config_hash`.
    import ast
    from pathlib import Path
    arv = ast.parse(Path(reg.__file__).read_text(encoding="utf-8"))
    for no in ast.walk(arv):
        if (isinstance(no, ast.Assign) and no.targets
                and getattr(no.targets[0], "id", "") == "SYMBOLS_KIS_3489"):
            assert isinstance(no.value, ast.List)
            assert all(isinstance(e, ast.Constant) and isinstance(e.value, str)
                       for e in no.value.elts)
            return
    raise AssertionError("SYMBOLS_KIS_3489 nao e uma lista literal no modulo")


# --- 3. o hash DISCRIMINA -----------------------------------------------------
def test_trocar_3489_por_821_muda_o_hash():
    # O teste que o briefing pede por nome. Sem ele, os dois de cima passariam
    # mesmo que `detector_params` estivesse fora do `config_dict`.
    base = KIS_3489_60T_4H.config_hash
    curto = replace(KIS_3489_60T_4H,
                    detector_params=dict(PARAMS_KIS_3489, ema_fast=8, ema_slow=21))
    print(f"hash com 34/89 = {base} | com 8/21 = {curto.config_hash}")
    assert curto.config_hash != base


def test_cada_um_dos_sete_parametros_move_o_hash_sozinho():
    # Um a um: se algum nao mover, ele esta fora da identidade da configuracao,
    # e duas medicoes diferentes cairiam na mesma serie por causa dele.
    base = KIS_3489_60T_4H.config_hash
    for k, v in PARAMS_KIS_3489.items():
        mexido = replace(KIS_3489_60T_4H,
                         detector_params=dict(PARAMS_KIS_3489, **{k: v + 1}))
        assert mexido.config_hash != base, f"{k} nao entra no hash"


def test_universo_e_warmup_tambem_movem_o_hash():
    base = KIS_3489_60T_4H.config_hash
    assert replace(KIS_3489_60T_4H,
                   symbols=SYMBOLS_KIS_3489[:-1]).config_hash != base
    assert replace(KIS_3489_60T_4H, warmup_barras=21).config_hash != base


def test_executar_nao_move_hash_nenhum_dos_dois():
    # `executar` esta em FORA_DO_HASH: ligar a coleta depois tem que devolver as
    # linhas novas a MESMA serie. Se movesse, pausar e retomar partiria o
    # historico em dois — o oposto do que o hash serve.
    for spec in (KIS_REGIME_4H, KIS_3489_60T_4H):
        h = spec.config_hash
        assert replace(spec, executar=True).config_hash == h
        assert replace(spec, executar=False).config_hash == h
    assert KIS_REGIME_4H.config_hash == HASH_KIS_REGIME_4H   # e segue congelado


# --- a rampa do portao le a EMA21, e isso nao e decoracao --------------------
def test_incl_ema_slow_e_21_e_nao_segue_o_par():
    p = KIS_3489_60T_4H.detector_params
    assert p["incl_ema_slow"] == 21
    assert p["incl_ema_slow"] != p["ema_slow"], (
        "a rampa seguindo a EMA lenta do par mediria um portao que nunca foi "
        "validado — ver kis_horizonte, 'PORTAO FIXO, NAO VARIAVEL'")


def test_a_rampa_em_21_e_em_89_dao_inclinacoes_DIFERENTES():
    # Sem esta prova, `incl_ema_slow` poderia ser um campo inerte e todos os
    # testes acima passariam do mesmo jeito.
    velas = _serie()
    dif = sum(1 for i in range(90, len(velas))
              if bkr.avaliar(velas[:i + 1], 34, 89, incl_ema_slow=21)[2]
              != bkr.avaliar(velas[:i + 1], 34, 89, incl_ema_slow=89)[2])
    assert dif > 0, "incl_ema_slow nao mudou nenhuma inclinacao: campo inerte"


def test_o_detector_usa_a_rampa_da_ficha_e_nao_a_do_par():
    # O detector inteiro, ficha e tudo, contra a composicao explicita.
    velas, p = _serie(), PARAMS_KIS_3489
    for i in range(90, len(velas), 5):
        janela = velas[:i + 1]
        alvo, d, incl, adx = bkr.avaliar(
            janela, p["ema_fast"], p["ema_slow"], p["confirmacao"],
            p["adx_len"], p["atr_len"], incl_ema_slow=p["incl_ema_slow"])
        esperado = (None if d is None
                    or not bkr.passa(d, incl, adx, p["limiar"], p["adx_min"])
                    else d)
        got = reg.detector_kis_3489(janela, p)
        assert (got or {}).get("direction") == esperado, f"barra {i}"
        if got:
            assert got["evidence"]["incl_ema_slow"] == p["incl_ema_slow"]


def test_a_serie_da_comparacao_de_fato_inverte_nos_dois_lados():
    # Sem inversao, o teste acima seria "None == None" 60 vezes.
    velas, p = _serie(), PARAMS_KIS_3489
    dirs = {bkr.avaliar(velas[:i + 1], p["ema_fast"], p["ema_slow"])[1]
            for i in range(90, len(velas))}
    assert {"LONG", "SHORT"} <= dirs


# --- o anotador e o MESMO, e ele segue o par da ficha -------------------------
def test_o_anotador_e_compartilhado_e_carimba_o_par_da_ficha():
    assert reg.detector_kis_3489.anotar is reg.detector_kis_regime.anotar
    velas = _serie(200)
    a34 = reg.detector_kis_3489.anotar(velas, PARAMS_KIS_3489)
    a8 = reg.detector_kis_3489.anotar(velas, KIS_REGIME_4H.detector_params)
    assert [v["alvo"] for v in a34] != [v["alvo"] for v in a8]
    assert "alvo" not in velas[0]                 # nao mutou a lista do chamador
