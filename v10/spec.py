"""v10/spec.py — SetupSpec: a ficha de um setup. Campos, não abstrações.

Um setup no v10 é um registro de dados, não uma classe a herdar. O detector é
um callable qualquer; tudo o mais é parâmetro declarado. Quem roda o setup lê
esta ficha — não pergunta ao setup como ele quer ser rodado.

``config_hash`` é a identidade da configuração: sha256 do dicionário de
configuração, truncado em 12 chars. Determinístico e ESTÁVEL entre execuções
(``json.dumps(sort_keys=True)``, nunca ``hash()`` do Python, que é
randomizado por processo via PYTHONHASHSEED). Entra na chave de idempotência
de ``trades_v10``: mudou parâmetro, mudou o hash, e as linhas novas não se
misturam com as da configuração anterior.

TODOS os campos entram no hash, inclusive ``estado_ciclo`` e ``aviso``. É de
propósito: um resultado gerado sob "proposto" não deve se confundir com o
mesmo setup depois de promovido, mesmo que os números sejam idênticos.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields
from typing import Any, Callable


@dataclass
class SetupSpec:
    """Especificação completa de um setup do ciclo v10.

    Campos:
        setup_id: identificador curto e estável (ex.: ``"kis_regime_4h"``).
        detector: callable que recebe velas e devolve sinal. Não faz I/O e
            NUNCA chama a API direto — recebe velas de :func:`v10.data.velas`.
        tf: timeframe das velas do detector (ex.: ``"4H"``).
        cadencia_barras: de quantas em quantas barras o detector é avaliado.
        symbols: universo do setup.
        warmup_barras: barras descartadas antes do primeiro sinal válido.
        exit_model: chave em :data:`v10.exits.ADAPTADORES`
            (``"bracket_multi"`` | ``"reverse"``).
        exit_params: parâmetros do adaptador de saída. Formato livre — é o
            adaptador que os interpreta, não o schema.
        direcoes: direções permitidas.
        custo_bps_por_perna: custo por perna em bps, para o pnl líquido.
        mode: ``"shadow"`` | ``"live"``.
        estado_ciclo: onde o setup está no ciclo (``"proposto"``,
            ``"em_bancada"``, ``"promovido"``, ``"arquivado"``).
        aviso: ressalva conhecida que acompanha qualquer leitura do resultado.
    """

    setup_id: str
    detector: Callable
    tf: str
    cadencia_barras: int
    symbols: list
    warmup_barras: int
    exit_model: str
    exit_params: dict = field(default_factory=dict)
    direcoes: tuple = ("LONG", "SHORT")
    custo_bps_por_perna: float = 0.0
    mode: str = "shadow"
    estado_ciclo: str = "proposto"
    aviso: str = ""

    @property
    def config_hash(self) -> str:
        """Atalho para :func:`config_hash` — sempre recalculado, nunca cacheado."""
        return config_hash(self)


def _canon(valor: Any) -> Any:
    """Forma canônica e serializável de um valor de configuração.

    - dict: chaves ordenadas por ``str`` (reordenar o dict NÃO muda o hash).
    - list/tuple: ordem PRESERVADA (ordem de uma lista é dado, não formatação).
    - float: ``repr`` — trava a representação e impede que 1 e 1.0 colidam.
    - callable: ``modulo.qualname`` — trocar o detector muda o hash, mas
      recarregar o mesmo módulo (endereço de memória novo) não muda.
    """
    if isinstance(valor, dict):
        return {str(k): _canon(valor[k]) for k in sorted(valor, key=str)}
    if isinstance(valor, (list, tuple)):
        return [_canon(v) for v in valor]
    if isinstance(valor, bool) or valor is None or isinstance(valor, (str, int)):
        return valor
    if isinstance(valor, float):
        return repr(valor)
    if callable(valor):
        nome = getattr(valor, "__qualname__", None) or getattr(valor, "__name__", "")
        return f"{getattr(valor, '__module__', '?')}.{nome}" if nome else repr(valor)
    return repr(valor)


def config_dict(spec: SetupSpec) -> dict:
    """Dicionário de configuração canônico — o que de fato é hasheado."""
    return {f.name: _canon(getattr(spec, f.name)) for f in fields(spec)}


def config_hash(spec: SetupSpec, tamanho: int = 12) -> str:
    """sha256 curto e determinístico da configuração completa do setup."""
    bruto = json.dumps(
        config_dict(spec), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(bruto.encode("utf-8")).hexdigest()[:tamanho]
