"""Conftest — stubs para harness sem pandas_ta_classic.

O harness do Claude Code não tem ``pandas_ta_classic`` (exige Python ≥ 3.12
e a sandbox roda 3.11). Instalamos um stub em ``sys.modules`` antes de
qualquer import que dispare a dependência transitiva. Testes cujos assertions
dependem de cálculos reais de ``pta.*`` são validados na VM.
"""

from __future__ import annotations

import sys
import types

if "pandas_ta_classic" not in sys.modules:
    sys.modules["pandas_ta_classic"] = types.ModuleType("pandas_ta_classic")
