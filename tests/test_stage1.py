"""Testes puros de stage1 — shift_idx e bin_of (topo stdlib -> coleta no sandbox)."""

from backtest.stage1 import bin_of, shift_idx


def test_shift_wrap():
    assert shift_idx(3, 5, 6) == 2


def test_shift_identidade():
    assert shift_idx(4, 0, 10) == 4


def test_bin_fronteira():
    assert bin_of(1000 + 500, 1000, 500) == 1


def test_bin_zero():
    assert bin_of(1000, 1000, 500) == 0
