"""Testes de first_touch (backtest/bracket.py) — funcao pura, em ATR.

Importa APENAS first_touch: o topo de backtest/bracket.py e stdlib-only
(imports pesados sao lazy), entao este teste coleta no sandbox sem pandas_ta.
"""

from __future__ import annotations

from backtest.bracket import first_touch


def test_alvo_primeiro():
    fav = [0.5, 2.1, 0.0]
    adv = [0.2, 0.3, 5.0]
    assert first_touch(fav, adv, 0.9, 1.0, 2.0, 3) == (2.0, "ALVO")


def test_stop_primeiro():
    fav = [0.1, 0.2, 9.0]
    adv = [1.2, 0.0, 0.0]
    assert first_touch(fav, adv, 0.9, 1.0, 2.0, 3) == (-1.0, "STOP")


def test_mesma_barra_amb_sb_pessimista():
    fav = [2.5]
    adv = [1.1]
    assert first_touch(fav, adv, 0.0, 1.0, 2.0, 1) == (-1.0, "AMB_SB")


def test_flat_preserva_fwd_negativo():
    fav = [0.3, 0.4]
    adv = [0.5, 0.6]
    assert first_touch(fav, adv, -0.3, 1.0, 2.0, 2) == (-0.3, "FLAT")


def test_ordem_temporal_stop_barra1_vence_alvo_barra3():
    fav = [0.0, 0.0, 3.0]
    adv = [1.5, 0.0, 0.0]
    assert first_touch(fav, adv, 0.5, 1.0, 2.0, 3) == (-1.0, "STOP")


def test_cap_horizonte_toque_apos_h_e_flat():
    fav = [0.1, 0.2, 5.0]
    adv = [0.1, 0.1, 0.1]
    assert first_touch(fav, adv, 0.2, 1.0, 2.0, 2) == (0.2, "FLAT")
