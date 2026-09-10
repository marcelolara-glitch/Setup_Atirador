# tests/test_diag_invariancia_alvo.py — cadeado do diagnostico de invariancia.
# O ponto deste arquivo NAO e "o script roda": e que ele SEPARA um caso que
# diverge de um que nao diverge. Um teste que passasse com `divergencias`
# devolvendo sempre lista vazia (ou sempre cheia) nao provaria nada sobre o
# store — e a licao de agosto/2026, teste que passa sem discriminar e pior que
# teste nenhum. Por isso as duas metades: um caso que TEM que divergir e um que
# TEM que concordar.
# stdlib puro: `alvos` vem do `keepitsimple` (sem pandas), entao coleta e roda
# no sandbox tambem.
from backtest.diag_invariancia_alvo import alvo_em, divergencias

PARAMS = {"ema_fast": 34, "ema_slow": 89, "confirmacao": 2}
N_CURTA = 169          # o que o runner entrega hoje na ficha 34/89 (89 + 80)
BAR_MS = 4 * 3600 * 1000


def _velas(closes: list) -> list:
    return [{"ts": 1_700_000_000_000 + i * BAR_MS, "close": c}
            for i, c in enumerate(closes)]


def _serie_que_diverge() -> list:
    """400 barras planas em 100 (historia funda), rampa de 140 barras a +1.5 e
    queda de 60 a -1.0. Na ultima barra o cruzamento EMA34xEMA89 JA aconteceu
    para a serie inteira, mas ainda NAO para a janela de 169 do runner: as duas
    contas apontam para lados OPOSTOS na mesma barra."""
    closes, v = [100.0] * 400, 100.0
    for _ in range(140):
        v += 1.5
        closes.append(v)
    for _ in range(60):
        v -= 1.0
        closes.append(v)
    return _velas(closes)


def test_lados_opostos_na_ultima_barra():
    c = _serie_que_diverge()
    i = len(c) - 1
    assert alvo_em(c, i, N_CURTA, PARAMS) == 1
    assert alvo_em(c, i, None, PARAMS) == -1


def test_divergencias_nao_reporta_zero():
    """Se `divergencias` disser ZERO nesta serie, o script nao esta medindo o
    que diz medir e este teste TEM que falhar."""
    linhas = divergencias(_serie_que_diverge(), PARAMS, k=10,
                          n_curta=N_CURTA, n_longa=500)
    assert len(linhas) == 10
    assert [r for r in linhas if r["curta"] != r["tudo"]], \
        "serie construida para divergir reportou ZERO divergencias"


def test_serie_monotona_concorda():
    """A outra metade do cadeado: sem cruzamento na margem, as tres janelas dao
    o MESMO alvo. Sem isto, um `divergencias` que so responde 'sim' passaria."""
    linhas = divergencias(_velas([100.0 + i * 0.5 for i in range(600)]),
                          PARAMS, k=10, n_curta=N_CURTA, n_longa=500)
    assert all(r["curta"] == r["tudo"] == r["longa"] for r in linhas)


def test_historico_curto_marcado():
    """`hist_longa_ok` False = a coluna `longa` nao e comparavel nessa barra."""
    linhas = divergencias(_velas([100.0 + i for i in range(300)]),
                          PARAMS, k=5, n_curta=N_CURTA, n_longa=500)
    assert all(r["hist_longa_ok"] is False for r in linhas)
