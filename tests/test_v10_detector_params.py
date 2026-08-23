"""GUARDA — parametro de detector mora na ficha, nunca em constante de modulo.

O `config_hash` so identifica a configuracao se a configuracao INTEIRA estiver
na ficha. Um limiar escondido dentro do detector entra no hash apenas pelo NOME
da funcao: trocar 0.02 por 0.03 devolve o MESMO hash, e as linhas novas caem na
mesma serie de `trades_v10` que as antigas — duas configuracoes medidas como se
fossem uma. Esse era o estado do `kis_regime_4h` antes do PR-E.

Este arquivo e a guarda contra a reincidencia, e ela e ESTRUTURAL, nao uma
lista de nomes a manter a mao:

  * todo spec do REGISTRO tem `detector_params` NAO VAZIO;
  * o corpo de cada detector nao le NENHUM nome numerico de modulo — a checagem
    e por AST sobre `v10/registro.py`, cruzada com os valores que o modulo de
    fato expoe, entao um `LIMIAR` novo importado amanha ja nasce coberto;
  * cada detector declara o segundo parametro, senao o runner nao teria como
    entregar a ficha e o campo seria decorativo.

Um setup futuro que esqueca de declarar reprova aqui, e nao no dia em que
alguem for ler dois meses de trades misturados.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import backtest.keepitsimple as keep
import backtest.kis_regime as bkr
import shadow.donchian_a as sd
import shadow.kis_regime as skr
import v10.registro as reg
from v10.registro import (PARAMS_DONCHIAN_A, PARAMS_KIS_REGIME, REGISTRO,
                          SYMBOLS_KIS)

ROOT = Path(__file__).resolve().parents[1]
FONTE = ROOT / "v10" / "registro.py"


def _numericos_do_modulo() -> set:
    """Nomes de `v10.registro` cujo valor e numero — os proprios e os
    IMPORTADOS. E a lista de tudo que um detector poderia ler pelas costas da
    ficha; ela se atualiza sozinha quando o modulo ganha um nome novo."""
    return {n for n, v in vars(reg).items()
            if isinstance(v, (int, float)) and not isinstance(v, bool)}


def _corpos_dos_detectores() -> dict:
    """{nome: no da funcao} para cada `detector_*`/`_anotar_*` do registro."""
    arv = ast.parse(FONTE.read_text())
    return {no.name: no for no in ast.walk(arv)
            if isinstance(no, ast.FunctionDef)
            and (no.name.startswith("detector_") or no.name.startswith("_anotar_"))}


def _nomes_lidos(no: ast.FunctionDef) -> set:
    return {x.id for x in ast.walk(no)
            if isinstance(x, ast.Name) and isinstance(x.ctx, ast.Load)}


# --- a ficha esta preenchida -------------------------------------------------
def test_todo_spec_do_registro_declara_detector_params():
    vazios = [sid for sid, s in REGISTRO.items() if not s.detector_params]
    assert not vazios, f"detector_params vazio: {vazios}"


def test_detector_params_so_tem_numero():
    # Formato livre nao e vale-tudo: um callable ou um dict aninhado aqui
    # voltaria a esconder configuracao atras de `repr`, que nao e estavel.
    for sid, s in REGISTRO.items():
        for k, v in s.detector_params.items():
            assert isinstance(v, (int, float)) and not isinstance(v, bool), \
                f"{sid}.detector_params[{k!r}] = {v!r} nao e numero"


def test_todo_detector_aceita_os_params_da_ficha():
    # Sem o segundo parametro o runner nao entrega nada e o campo vira enfeite.
    for sid, s in REGISTRO.items():
        n = len(inspect.signature(s.detector).parameters)
        assert n >= 2, f"{sid}: detector com {n} parametro(s), nao recebe a ficha"
        anotar = getattr(s.detector, "anotar", None)
        if anotar is not None:
            assert len(inspect.signature(anotar).parameters) >= 2, \
                f"{sid}: anotar nao recebe a ficha"


# --- nada de constante de modulo dentro do detector --------------------------
def test_nenhum_detector_le_constante_numerica_de_modulo():
    numericos = _numericos_do_modulo()
    assert {"N", "S_ATR", "T_ATR", "ATR_PERIOD", "CTX_BARS"} <= numericos, \
        "a checagem so vale se ela de fato enxerga os numeros do modulo"
    for nome, no in _corpos_dos_detectores().items():
        vazou = sorted(_nomes_lidos(no) & numericos)
        assert not vazou, f"{nome} le constante de modulo: {vazou}"


def test_nenhum_detector_tem_numero_literal_no_corpo():
    # O outro caminho para o mesmo buraco: em vez de ler LIMIAR, escrever 0.02.
    for nome, no in _corpos_dos_detectores().items():
        lits = [x.value for x in ast.walk(no)
                if isinstance(x, ast.Constant)
                and isinstance(x.value, (int, float))
                and not isinstance(x.value, bool)
                and x.value not in (0, 1)]        # indice/offset, nao parametro
        assert not lits, f"{nome} tem numero literal no corpo: {lits}"


def test_nenhum_detector_tem_default_no_parametro_de_params():
    # Default seria a constante de modulo de volta, so que com outro nome.
    for sid, s in REGISTRO.items():
        p = list(inspect.signature(s.detector).parameters.values())[1]
        assert p.default is inspect.Parameter.empty, \
            f"{sid}: params com default — a ficha deixa de ser a unica fonte"


# --- a ficha diz a verdade sobre o que o detector usa -------------------------
def test_kis_declara_a_celula_congelada_e_os_periodos_da_bancada():
    assert PARAMS_KIS_REGIME["limiar"] == 0.02        # celula de 21/08
    assert PARAMS_KIS_REGIME["adx_min"] == 11
    assert PARAMS_KIS_REGIME["ema_fast"] == keep.EMA_FAST
    assert PARAMS_KIS_REGIME["ema_slow"] == keep.EMA_SLOW
    assert PARAMS_KIS_REGIME["adx_len"] == keep.ADX_LEN
    assert PARAMS_KIS_REGIME["confirmacao"] == keep.EXT_CONF
    assert PARAMS_KIS_REGIME["atr_len"] == bkr.ATR_LEN


def test_kis_nao_mudou_de_universo_nem_de_warmup_ao_sair_do_shadow():
    # Os dois valores sairam de shadow/kis_regime.py para a ficha. Sair de la
    # nao pode ter mexido neles — e o shadow segue existindo para comparar.
    assert SYMBOLS_KIS == skr.SYMBOLS
    assert REGISTRO["kis_regime_4h"].warmup_barras == skr.MIN_BARS


def test_donchian_declara_o_que_o_shadow_de_fato_usa():
    # `donchian_signal` le o N do shadow (congelado por sha256, nao recebe
    # parametro). Se a ficha dissesse outro N, o hash mudaria sem que o
    # comportamento mudasse — a mentira exata que o campo existe para impedir.
    assert PARAMS_DONCHIAN_A["n"] == sd.N
    assert PARAMS_DONCHIAN_A["atr_period"] == sd.ATR_PERIOD
    assert PARAMS_DONCHIAN_A["ctx_barras"] == sd.CTX_BARS


def test_saida_do_donchian_continua_em_exit_params():
    # A divisao e de fronteira, nao de gosto: o que o DETECTOR mede vai num
    # campo, o que a SAIDA faz vai no outro. Misturar apaga a distincao.
    ep = REGISTRO["donchian_a_4h"].exit_params
    assert {"s_atr", "t_atr", "h_bars"} <= set(ep)
    assert not {"s_atr", "t_atr", "h_bars"} & set(PARAMS_DONCHIAN_A)


# --- a conta que saiu do shadow e a MESMA conta -------------------------------
def _serie(n: int = 240) -> list:
    """Passeio de LCG em centesimos — inteiro puro, identico entre maquinas."""
    x, preco, out = 20260823, 10_000, []
    for i in range(n):
        x = (1103515245 * x + 12345) % 2147483648
        preco = max(2000, preco + (x % 201) - 100)
        out.append({"ts": i * 14_400_000, "open": preco / 100.0,
                    "high": (preco + 60) / 100.0, "low": (preco - 60) / 100.0,
                    "close": preco / 100.0, "volume": 1.0})
    return out


def test_avaliar_da_bancada_reproduz_o_do_shadow_barra_a_barra():
    # O v10 deixou de importar `avaliar` do shadow aposentado e passou a usar a
    # copia da bancada. "Copia" so vale enquanto for byte-identica: aqui as duas
    # rodam sobre a MESMA serie, barra a barra, com os defaults de hoje.
    velas = _serie()
    divergentes = [i for i in range(21, len(velas))
                   if bkr.avaliar(velas[:i + 1]) != skr.avaliar(velas[:i + 1])]
    assert not divergentes, f"bancada != shadow nas barras {divergentes[:10]}"


def test_a_serie_da_comparacao_de_fato_inverte_nos_dois_lados():
    # Sem inversao a comparacao acima seria "None == None" 200 vezes.
    velas = _serie()
    dirs = {skr.avaliar(velas[:i + 1])[1] for i in range(21, len(velas))}
    assert {"LONG", "SHORT"} <= dirs


def test_detector_com_a_ficha_reproduz_o_shadow_com_o_portao():
    # O detector inteiro, ficha e tudo, contra a composicao antiga
    # (avaliar do shadow + portao com as constantes do shadow).
    velas = _serie()
    p = PARAMS_KIS_REGIME
    for i in range(21, len(velas)):
        janela = velas[:i + 1]
        novo = reg.detector_kis_regime(janela, p)
        alvo, d, incl, adx = skr.avaliar(janela)
        antigo = (None if d is None
                  or not bkr.passa(d, incl, adx, skr.LIMIAR, skr.ADX_MIN) else d)
        assert (novo or {}).get("direction") == antigo, f"barra {i}"
