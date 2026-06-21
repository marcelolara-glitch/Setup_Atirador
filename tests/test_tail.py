"""Testes de backtest/tail.py — tabela-verdade do exit convexo (bracket).

Aritmetica pura, sem pandas/pandas_ta: cobre `_bracket_outcome` (so-alvo,
so-stop, nenhum, ambos sob os dois bounds, fronteira inclusiva) e `_pct`.
O run completo passa por classify_regime e so valida na VM (Py3.12 + pandas_ta).
"""

from __future__ import annotations

import math

import pytest

from backtest.tail import _bracket_outcome, _pct

# (mfe, mae, ret, S, T, optimistic, esperado) — tabela-verdade.
_CASES = [
    (3.0, 0.4,  1.1, 0.5, 2.0, True,   2.0),  # so-alvo -> +T
    (3.0, 0.4,  1.1, 0.5, 2.0, False,  2.0),  # so-alvo -> +T (bound indiferente)
    (1.0, 0.9, -0.7, 0.5, 2.0, True,  -0.5),  # so-stop -> -S
    (1.0, 0.9, -0.7, 0.5, 2.0, False, -0.5),  # so-stop -> -S (bound indiferente)
    (1.2, 0.3,  0.8, 0.5, 2.0, True,   0.8),  # nenhum -> ret
    (1.2, 0.3,  0.8, 0.5, 2.0, False,  0.8),  # nenhum -> ret (bound indiferente)
    (3.0, 1.5,  0.2, 1.0, 2.0, True,   2.0),  # ambos -> otimista +T
    (3.0, 1.5,  0.2, 1.0, 2.0, False, -1.0),  # ambos -> pessimista -S
    (2.0, 0.5,  0.0, 0.5, 2.0, True,   2.0),  # fronteira inclusiva (>=)
    (2.0, 0.5,  0.0, 0.5, 2.0, False, -0.5),  # fronteira inclusiva (>=)
]


@pytest.mark.parametrize("mfe,mae,ret,S,T,opt,exp", _CASES)
def test_bracket_outcome(mfe, mae, ret, S, T, opt, exp):
    assert _bracket_outcome(mfe, mae, ret, S, T, opt) == exp


def test_pct_indices():
    vals = [float(i) for i in range(10)]   # 0..9, ja ordenado
    assert _pct(vals, 0.0) == 0.0
    assert _pct(vals, 0.5) == 4.0     # int(0.5*9)=4
    assert _pct(vals, 0.99) == 8.0    # int(0.99*9)=8
    assert math.isnan(_pct([], 0.5))
