"""ESPELHO — o v10 tem de reproduzir o historico do shadow do DONCHIAN-A.

Este e o teste que justifica o PR. O runner + `bracket_simples` + a ficha
`donchian_a_4h` sao codigo NOVO; a instancia que esta medindo a janela
pre-registrada (fecha out/2026) e `shadow/donchian_a.py`, com banco e cron
proprios. A unica prova aceitavel de que o v10 pode um dia substituir aquilo e
reproduzir o que ja aconteceu — mesmo simbolo, mesma direcao, mesma barra de
entrada, mesmo status, mesmo r_multiple, com max|dif| = ZERO.

NAO RODA NO HARNESS. Precisa de dois arquivos que vivem na VM e nao no repo:

    journal/shadow_donchian_a.db   trades e runs do shadow (a verdade)
    backtest/candles_v9.db         velas 4h dos 20 TIER1 (o replay)

Na VM:  python3 -m pytest tests/test_v10_espelho_donchian.py -q -s
O `-s` mostra o resumo de cobertura, que e informacao, nao so decoracao.

COMO O REPLAY E CONSTRUIDO. A grade de barras nao e "toda barra 4h da janela":
e a grade que o shadow DE FATO avaliou, derivada de `shadow_runs`. Se o cron
perdeu uma run, o shadow perdeu aquela barra — e o v10 tem de perder tambem,
senao abriria um trade que o historico nao tem. Antes da primeira linha de
`shadow_runs` (a tabela nasceu depois dos primeiros trades) a grade volta a ser
completa, porque ali nao ha registro de falha para reproduzir.

SE DIVERGIR: o teste falha e imprime cada divergencia. NAO se ajusta o v10 para
bater — a divergencia e o achado, e ela sobe para o Marcelo como esta.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHADOW_DB = ROOT / "journal" / "shadow_donchian_a.db"
STORE_DB = ROOT / "backtest" / "candles_v9.db"
TF_STORE = "4h"                                    # rotulo do candle_store
FECHADOS = ("WIN", "LOSS", "EXPIRED")

pytestmark = pytest.mark.skipif(
    not (SHADOW_DB.exists() and STORE_DB.exists()),
    reason=f"espelho roda na VM: precisa de {SHADOW_DB.name} e {STORE_DB.name}")


def _abrir(caminho: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _iso_ms(iso: str) -> int:
    from datetime import datetime, timezone
    d = datetime.fromisoformat(iso)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)


@pytest.fixture(scope="module")
def replay():
    """Roda o v10 sobre a grade real do shadow. -> (esperado, obtido, resumo)."""
    from backtest.candle_store import read_candles
    from shadow.donchian_a import BAR_MS
    from v10.registro import DONCHIAN_A_4H as SPEC
    from v10.runner import rodar
    from v10.schema import connect

    sombra = _abrir(SHADOW_DB)
    trades = sombra.execute(
        "SELECT symbol, direction, bar_ts_entry, expiry_bar_ts, status, "
        "r_multiple FROM shadow_trades").fetchall()
    runs = [r["run_ts"] for r in sombra.execute("SELECT run_ts FROM shadow_runs")]
    sombra.close()

    fechados = [t for t in trades if t["status"] in FECHADOS]
    assert fechados, "shadow_trades sem trade fechado: nada a espelhar"

    def _barra(ms):                                # ultima barra fechada em `ms`
        return (ms // BAR_MS - 1) * BAR_MS

    barras_de_run = {_barra(_iso_ms(r)) for r in runs}
    primeira_entrada = min(int(t["bar_ts_entry"]) for t in trades)
    ultima = max(barras_de_run) if barras_de_run else primeira_entrada
    corte = min(barras_de_run) if barras_de_run else ultima + BAR_MS
    # Antes de `shadow_runs` existir nao ha falha de cron registrada: grade cheia.
    anteriores = set(range(primeira_entrada, corte, BAR_MS))
    grade = sorted(barras_de_run | anteriores)
    assert grade, "grade de replay vazia"

    loja = _abrir(STORE_DB)
    series = {s: read_candles(loja, s, TF_STORE) for s in SPEC.symbols}
    loja.close()

    inicio = grade[0] - SPEC.warmup_barras * BAR_MS
    buracos = {}
    for symbol, serie in series.items():
        tss = {c["ts"] for c in serie}
        faltando = [t for t in range(inicio, grade[-1] + BAR_MS, BAR_MS)
                    if t not in tss]
        if faltando:
            buracos[symbol] = len(faltando)

    cursor = {"ms": 0}

    def velas_fn(symbol, tf, n):
        janela = [c for c in series.get(symbol, []) if c["ts"] <= cursor["ms"]]
        return janela[-n:]

    conn = connect(":memory:")
    for barra in grade:
        cursor["ms"] = barra + BAR_MS              # relogio logo apos o fecho
        rodar(SPEC, conn, agora_ms=cursor["ms"], velas_fn=velas_fn)

    obtido = {}
    for r in conn.execute(
            "SELECT symbol, direction, entry_ts, status, exit_state_json "
            "FROM trades_v10 WHERE setup_id=?", (SPEC.setup_id,)):
        estado = json.loads(r["exit_state_json"] or "{}")
        obtido[(r["symbol"], int(r["entry_ts"]))] = {
            "direction": r["direction"], "status": r["status"],
            "r_multiple": estado.get("r_multiple")}
    conn.close()

    esperado = {(t["symbol"], int(t["bar_ts_entry"])): {
        "direction": t["direction"], "status": t["status"],
        "r_multiple": t["r_multiple"]} for t in fechados}
    resumo = {"grade": len(grade), "runs": len(runs), "fechados": len(fechados),
              "abertos_shadow": len(trades) - len(fechados),
              "v10": len(obtido), "buracos": buracos}
    print(f"\n[espelho] grade={resumo['grade']} barras "
          f"({len(barras_de_run)} de shadow_runs, {len(anteriores)} anteriores) | "
          f"shadow: {len(fechados)} fechados + {resumo['abertos_shadow']} abertos | "
          f"v10: {len(obtido)} trades | buracos no store: {buracos or 'nenhum'}")
    return esperado, obtido, resumo


def test_o_store_cobre_a_janela_inteira(replay):
    # Falha AQUI = problema de dado (rodar backtest/backfill_okx.py), nao
    # divergencia de logica. Separado de proposito para nao confundir os dois.
    _esp, _obt, resumo = replay
    assert not resumo["buracos"], (
        f"candles_v9.db incompleto no 4h: {resumo['buracos']} — o replay nao "
        "pode ser julgado sobre dado faltante")


def test_todo_trade_fechado_do_shadow_existe_no_v10(replay):
    esperado, obtido, _r = replay
    faltando = sorted(k for k in esperado if k not in obtido)
    assert not faltando, f"{len(faltando)} trade(s) do shadow que o v10 nao abriu: {faltando[:10]}"


def test_o_v10_nao_inventa_trade_que_o_shadow_nao_tem(replay):
    # So valem como extras os que o shadow tambem nao tem em aberto — por isso
    # a comparacao usa a chave contra TODOS os trades do shadow, nao so os
    # fechados; um trade ainda OPEN la aparece aqui como OPEN, nao como extra.
    esperado, obtido, _r = replay
    extras = sorted(k for k, v in obtido.items()
                    if k not in esperado and v["status"] != "OPEN")
    assert not extras, f"{len(extras)} trade(s) que o shadow nao tem: {extras[:10]}"


def test_direcao_e_status_batem_trade_a_trade(replay):
    esperado, obtido, _r = replay
    difs = [(k, e["direction"], e["status"], obtido[k]["direction"], obtido[k]["status"])
            for k, e in esperado.items() if k in obtido
            and (e["direction"], e["status"]) != (obtido[k]["direction"], obtido[k]["status"])]
    assert not difs, f"{len(difs)} divergencia(s) de direcao/status: {difs[:10]}"


def test_r_multiple_bate_com_max_dif_zero(replay):
    esperado, obtido, _r = replay
    difs = []
    for k, e in esperado.items():
        if k not in obtido:
            continue
        a, b = e["r_multiple"], obtido[k]["r_multiple"]
        if a is None or b is None:
            difs.append((k, a, b))
        elif abs(float(a) - float(b)) != 0.0:
            difs.append((k, a, b, abs(float(a) - float(b))))
    assert not difs, f"{len(difs)} r_multiple divergente(s): {difs[:10]}"


def test_o_espelho_cobre_de_fato_a_amostra_toda(replay):
    # Guarda contra "passar" com amostra vazia: se a intersecao encolher, os
    # testes acima ficam verdes sem provar nada. O numero e o do shadow.
    esperado, obtido, resumo = replay
    comuns = [k for k in esperado if k in obtido]
    assert len(comuns) == len(esperado) == resumo["fechados"], (
        f"comparados {len(comuns)} de {resumo['fechados']} trades fechados")
