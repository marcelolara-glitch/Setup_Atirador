"""ESPELHO SINTETICO — a mesma prova do espelho da VM, sem precisar da VM.

`tests/test_v10_espelho_donchian.py` compara o v10 contra o historico REAL do
shadow, e por isso so roda onde os dois bancos existem. Este arquivo faz a
comparacao que RODA EM QUALQUER LUGAR: gera uma serie 4h deterministica, roda
o modulo de producao `shadow/donchian_a.py` barra a barra sobre ela, roda o
v10 sobre exatamente as mesmas barras, e exige igualdade byte a byte.

A diferenca entre os dois arquivos e o que cada um prova. Este prova que o
CODIGO e equivalente. O da VM prova que o codigo equivalente, alimentado pelo
dado que a producao viu, chega no historico que a producao gravou. Um nao
substitui o outro: se este passar e o da VM falhar, a diferenca esta no dado.

A serie e um passeio aleatorio de LCG (semente fixa, so inteiro e divisao
exata): sem RNG de biblioteca e sem transcendental, entao e identica entre
maquinas e entre versoes de Python. A amplitude foi escolhida para o canal de
120 barras ser rompido varias vezes nos dois lados — uma serie sem rompimento
compararia "nenhum trade com nenhum trade" e passaria sem provar nada, e o
ultimo teste do arquivo existe para impedir exatamente isso.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import replace

import pytest

from shadow import donchian_a as sd
from v10.registro import DONCHIAN_A_4H as SPEC
from v10.runner import rodar
from v10.schema import connect

BAR = sd.BAR_MS
N_BARRAS = 420
SIMBOLOS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
            "DOGEUSDT"]
QUIET = logging.getLogger("espelho_sintetico")
QUIET.addHandler(logging.NullHandler())


def _serie(semente: int) -> list:
    """Passeio de LCG em centesimos de dolar. Aritmetica inteira ate o fim."""
    x, preco, saida = semente, 10_000, []
    for i in range(N_BARRAS):
        x = (1103515245 * x + 12345) % 2147483648
        preco = max(2000, preco + (x % 401) - 200)
        y = (1103515245 * x + 12345) % 2147483648
        meia = y % 150
        alto, baixo = preco + meia, max(1, preco - meia)
        abertura = saida[-1]["close"] if saida else preco / 100.0
        saida.append({"ts": i * BAR, "open": abertura, "high": alto / 100.0,
                      "low": baixo / 100.0, "close": preco / 100.0,
                      "volume": 1.0})
    return saida


SERIES = {s: _serie(1000 + i) for i, s in enumerate(SIMBOLOS)}
GRADE = [i * BAR for i in range(SPEC.warmup_barras - 1, N_BARRAS)]


@pytest.fixture(scope="module")
def confronto(tmp_path_factory):
    """Roda os dois motores sobre a MESMA grade. -> (shadow, v10)."""
    db = tmp_path_factory.mktemp("espelho") / "shadow.db"
    cursor = {"ms": 0}

    def fetch_fn(symbol):                       # espelha _fetch_confirmed
        return [c for c in SERIES[symbol] if c["ts"] <= cursor["ms"]]

    def velas_fn(symbol, tf, n):
        return [c for c in SERIES[symbol] if c["ts"] <= cursor["ms"] + BAR][-n:]

    spec = replace(SPEC, symbols=SIMBOLOS)
    conn = connect(":memory:")
    for barra in GRADE:
        cursor["ms"] = barra
        sd.run_once(db_path=str(db), fetch_fn=fetch_fn, symbols=SIMBOLOS,
                    log=QUIET, ticker_fn=lambda _s: None)
        rodar(spec, conn, agora_ms=barra + BAR, velas_fn=velas_fn, log=QUIET)

    sombra = sqlite3.connect(str(db))
    sombra.row_factory = sqlite3.Row
    a = {(r["symbol"], int(r["bar_ts_entry"])): (
            r["direction"], r["status"], r["entry_price"], r["atr_at_entry"],
            r["sl_price"], r["tp_price"], r["exit_price"], r["pnl_pct"],
            r["r_multiple"])
         for r in sombra.execute("SELECT * FROM shadow_trades")}
    sombra.close()

    b = {}
    for r in conn.execute("SELECT * FROM trades_v10"):
        p = json.loads(r["exit_params_json"] or "{}")
        s = json.loads(r["exit_state_json"] or "{}")
        b[(r["symbol"], int(r["entry_ts"]))] = (
            r["direction"], r["status"], r["entry_price"], p.get("atr_value"),
            p.get("sl_price"), p.get("tp_price"), r["exit_price"], r["pnl_pct"],
            s.get("r_multiple"))
    conn.close()
    return a, b


def test_o_mesmo_conjunto_de_trades(confronto):
    a, b = confronto
    assert sorted(a) == sorted(b), (
        f"shadow-only={sorted(set(a) - set(b))[:5]} v10-only={sorted(set(b) - set(a))[:5]}")


def test_direcao_status_e_precos_batem_byte_a_byte(confronto):
    a, b = confronto
    difs = [(k, a[k], b[k]) for k in a if k in b and a[k] != b[k]]
    assert not difs, f"{len(difs)} divergencia(s): {difs[:5]}"


def test_r_multiple_com_max_dif_zero(confronto):
    a, b = confronto
    difs = []
    for k in a:
        if k not in b or a[k][1] == "OPEN":
            continue
        x, y = a[k][-1], b[k][-1]
        if x is None or y is None or abs(float(x) - float(y)) != 0.0:
            difs.append((k, x, y))
    assert not difs, f"{len(difs)} r_multiple divergente(s): {difs[:5]}"


def test_a_amostra_exercita_os_tres_desfechos_e_os_dois_lados(confronto):
    # Sem isto o arquivo inteiro pode passar comparando conjunto vazio.
    a, _b = confronto
    status = {v[1] for v in a.values()}
    lados = {v[0] for v in a.values()}
    assert len(a) >= 20, f"amostra pequena demais: {len(a)} trades"
    assert {"WIN", "LOSS", "EXPIRED"} <= status, f"desfechos cobertos: {status}"
    assert lados == {"LONG", "SHORT"}, f"lados cobertos: {lados}"
    # A serie nao produz WIN no SHORT; esse ramo tem teste fechado a mao em
    # tests/test_v10_runner.py (test_short_r_multiple_tem_o_sinal_invertido).
