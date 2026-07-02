"""Testes de resolve_bracket (backtest/bracket.py) — funcao pura, em ATR.

Importa APENAS resolve_bracket: o topo de backtest/bracket.py e stdlib-only
(imports pesados sao lazy), entao este teste coleta no sandbox sem pandas_ta.
"""

from __future__ import annotations

from backtest.bracket import resolve_bracket


def test_alvo_sem_stop():
    assert resolve_bracket(3.0, 0.5, 1.0, 1.0, 2.0) == (2.0, 2.0, "ALVO")


def test_stop_sem_alvo():
    assert resolve_bracket(1.0, 1.5, -0.8, 1.0, 2.0) == (-1.0, -1.0, "STOP")


def test_flat_sai_no_horizonte():
    assert resolve_bracket(1.0, 0.5, 0.4, 1.0, 2.0) == (0.4, 0.4, "FLAT")


def test_ambiguo_retorna_bounds():
    assert resolve_bracket(2.5, 1.2, 0.0, 1.0, 2.0) == (2.0, -1.0, "AMBIGUO")


def test_fronteira_mfe_igual_t_conta_como_toque():
    assert resolve_bracket(2.0, 0.0, 0.5, 1.0, 2.0) == (2.0, 2.0, "ALVO")


def test_flat_preserva_fwd_negativo():
    assert resolve_bracket(0.5, 0.7, -0.3, 1.0, 2.0) == (-0.3, -0.3, "FLAT")
