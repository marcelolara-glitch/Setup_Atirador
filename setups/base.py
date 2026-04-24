"""setups/base.py — interface comum dos setups v9.

Define o dataclass padrão :class:`SetupResult` que todo setup retorna,
o :class:`SetupProtocol` que todo setup deve implementar e os helpers
:func:`evaluate_condition` e :func:`calculate_setup_confidence` usados
para construir conditions e calcular confidence score ponderado.

Regras de pureza (absolutas):
    - Zero I/O (sem rede, arquivo, banco, logging persistente)
    - Zero estado global — helpers são funções puras
    - Dependências: pandas, dataclasses, typing
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd

if TYPE_CHECKING:
    from indicators import MarketContext
    from regime import RegimeClassification


__all__ = [
    "SetupProtocol",
    "SetupResult",
    "calculate_setup_confidence",
    "evaluate_condition",
]


# ---------------------------------------------------------------------------
# Dataclass padrão de retorno
# ---------------------------------------------------------------------------


@dataclass
class SetupResult:
    """Retorno padrão de todos os setups v9.

    Campos:
        setup_name: identificador curto do setup (ex.: ``"cont_pull"``).
        triggered: True quando todas as conditions exigidas passaram.
        direction: ``"LONG"`` | ``"SHORT"`` | None quando não dispara.
        confidence: score ponderado 0-100 das conditions.
        evidence: dicionário livre com detalhes para log/journal.
        triggered_at: timestamp do candle avaliado.
    """

    setup_name: str
    triggered: bool
    direction: str | None
    confidence: float
    evidence: dict
    triggered_at: pd.Timestamp


# ---------------------------------------------------------------------------
# Helpers de condition
# ---------------------------------------------------------------------------


_SUPPORTED_OPERATORS: tuple[str, ...] = (">", ">=", "<", "<=", "==")


def evaluate_condition(
    name: str,
    value: float,
    threshold: float,
    operator: str = ">",
    weight: float = 1.0,
) -> dict[str, Any]:
    """Avalia uma condition e retorna dict estruturado.

    Parâmetros:
        name: identificador legível da condition.
        value: valor observado.
        threshold: valor de referência.
        operator: um de ``">"``, ``">="``, ``"<"``, ``"<="``, ``"=="``.
        weight: peso da condition no confidence score (default 1.0).

    Retorno:
        Dicionário com as chaves ``name``, ``value``, ``threshold``,
        ``operator``, ``passed`` e ``weight``.

    Raises:
        ValueError: operador não suportado.
    """
    if operator == ">":
        passed = value > threshold
    elif operator == ">=":
        passed = value >= threshold
    elif operator == "<":
        passed = value < threshold
    elif operator == "<=":
        passed = value <= threshold
    elif operator == "==":
        passed = value == threshold
    else:
        raise ValueError(f"Unknown operator: {operator}")

    return {
        "name": name,
        "value": value,
        "threshold": threshold,
        "operator": operator,
        "passed": bool(passed),
        "weight": weight,
    }


def calculate_setup_confidence(conditions: list[dict[str, Any]]) -> float:
    """[DEPRECATED v9.1] Confidence binária 100/0.

    Mantida por compat com chamadores externos. Retorna 100.0 se TODAS
    as conditions passaram, 0.0 caso contrário. A semântica ponderada
    original (passed_weight/total_weight × 100) foi removida porque
    contradizia o trigger ``all(passed)`` — produzia confidence sempre
    igual a 100 quando triggered=True. Fundação intelectual original
    (CLAUDE.md §2) usa sinais binários puros (NFIX7, Jesse).

    Conditions individuais continuam disponíveis em ``SetupResult.evidence``
    para observabilidade e debug (value, threshold, passed, weight).
    """
    if not conditions:
        return 0.0
    return 100.0 if all(c["passed"] for c in conditions) else 0.0


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class SetupProtocol(Protocol):
    """Contract que todo setup v9 deve implementar."""

    def evaluate(
        self,
        ctx: "MarketContext",
        regime: "RegimeClassification",
    ) -> SetupResult:
        ...
