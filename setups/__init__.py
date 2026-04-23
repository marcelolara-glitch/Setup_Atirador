"""setups/ — biblioteca de setups v9.

Cada setup é um módulo Python independente que implementa o
:class:`~setups.base.SetupProtocol` e retorna um
:class:`~setups.base.SetupResult` padronizado.
"""

from setups.base import (
    SetupProtocol,
    SetupResult,
    calculate_setup_confidence,
    evaluate_condition,
)

__all__ = [
    "SetupProtocol",
    "SetupResult",
    "calculate_setup_confidence",
    "evaluate_condition",
]
