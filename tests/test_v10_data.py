"""v10.data.velas — assinatura e contrato. A rede em si nao e exercitada aqui.

O ponto do modulo e ser a UNICA porta: o detector nao chama exchange. Entao o
que se trava e (a) a assinatura, que a versao de store vai ter que respeitar,
(b) a ausencia de import de rede no topo — se `aiohttp`/`exchanges` subissem no
import, `v10.data` deixaria de ser importavel no sandbox e arrastaria a cadeia
pesada para dentro de qualquer teste puro que o tocasse.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from v10 import data

ROOT = Path(__file__).resolve().parents[1]


def test_assinatura_congelada():
    p = list(inspect.signature(data.velas).parameters.values())
    assert [x.name for x in p] == ["symbol", "tf", "n", "ate"]
    assert all(x.default is inspect.Parameter.empty for x in p[:3])
    assert p[3].default is None


def test_a_porta_e_uma_so():
    # `FalhaDeColeta` entrou junto com o backoff: e o tipo que distingue "as
    # corretoras cairam" de "nao ha vela", e por isso e publico. `Velas` e o TIPO
    # DE RETORNO da mesma porta (lista + `venue`) e `VENUE_PRIMARIA` e um nome
    # para comparar — nenhum dos dois e uma segunda entrada. Continua UMA porta:
    # a lista fechada esta aqui para que a segunda precise deste commit.
    assert data.__all__ == ["FalhaDeColeta", "VENUE_PRIMARIA", "Velas", "velas"]
    publicas = [n for n in vars(data) if not n.startswith("_") and callable(vars(data)[n])]
    assert publicas == ["Velas", "FalhaDeColeta", "velas"]
    assert issubclass(data.Velas, list) and data.Velas().venue is None


def test_import_de_rede_e_lazy():
    # Imports pesados so dentro da funcao (padrao de shadow/donchian_a.py).
    arv = ast.parse((ROOT / "v10" / "data.py").read_text())
    topo = {a.name.split(".")[0] for no in arv.body if isinstance(no, ast.Import)
            for a in no.names}
    topo |= {no.module.split(".")[0] for no in arv.body
             if isinstance(no, ast.ImportFrom) and no.module}
    assert not (topo & {"aiohttp", "exchanges", "requests", "pandas", "numpy"})


def test_nenhum_setup_ou_shadow_chama_exchange_direto_pelo_v10():
    # A regra que este modulo existe para impor. Vale para o que o v10 vier a
    # ter; hoje so ha o proprio data.py, e ele e a excecao autorizada.
    for arq in sorted((ROOT / "v10").glob("*.py")):
        if arq.name == "data.py":
            continue
        assert "exchanges" not in arq.read_text(), f"{arq.name} fala com a exchange"
