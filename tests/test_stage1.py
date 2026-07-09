"""Testes puros de stage1 — shift_idx, bin_of e tsmom_entries (topo stdlib ->
coleta no sandbox)."""

from backtest.stage1 import bin_of, shift_idx, tsmom_entries


def test_shift_wrap():
    assert shift_idx(3, 5, 6) == 2


def test_shift_identidade():
    assert shift_idx(4, 0, 10) == 4


def test_bin_fronteira():
    assert bin_of(1000 + 500, 1000, 500) == 1


def test_bin_zero():
    assert bin_of(1000, 1000, 500) == 0


def _tss(n, bar_ms=1000):
    return [i * bar_ms for i in range(n)]


def test_tsmom_warmup():
    # nenhuma entrada com i < lookback (sinal so existe a partir de lookback)
    closes = [1, 2, 3, 4, 5, 6]
    out = tsmom_entries(closes, _tss(6), lookback=2, sep_bars=3, bar_ms=1000)
    assert out and all(e["bar_ts"] >= 2 * 1000 for e in out)


def test_tsmom_flip_dentro_do_gap():
    # LONG e depois SHORT dentro do gap da LONG: o flip entra (direcao oposta,
    # last[SHORT] ainda None)
    closes = [10, 20, 5]
    out = tsmom_entries(closes, _tss(3), lookback=1, sep_bars=3, bar_ms=1000)
    dirs = [(e["bar_ts"], e["direction"]) for e in out]
    assert (1000, "LONG") in dirs and (2000, "SHORT") in dirs


def test_tsmom_espacamento():
    # sinal LONG persistente reentra so apos sep_bars barras (em tempo)
    closes = [1, 2, 3, 4, 5, 6]
    out = tsmom_entries(closes, _tss(6), lookback=1, sep_bars=3, bar_ms=1000)
    ts = [e["bar_ts"] for e in out]
    assert ts == [1000, 4000] and ts[1] - ts[0] == 3 * 1000


def test_tsmom_ret_zero_sem_entrada():
    # ret == 0 (barra chata) nao gera entrada
    closes = [5, 5, 6]
    out = tsmom_entries(closes, _tss(3), lookback=1, sep_bars=3, bar_ms=1000)
    assert len(out) == 1 and out[0]["bar_ts"] == 2000


def test_tsmom_direcao_descendente():
    # serie descendente -> todas as entradas SHORT
    closes = [100, 90, 80, 70]
    out = tsmom_entries(closes, _tss(4), lookback=1, sep_bars=1, bar_ms=1000)
    assert out and all(e["direction"] == "SHORT" for e in out)
