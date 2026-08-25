"""Testes puros da varredura de HORIZONTE (backtest/kis_horizonte.py).

O topo de backtest/kis_horizonte.py e stdlib-only (candle_store/excursion sao
imports lazy dentro de `run`), entao este arquivo coleta no sandbox sem
pandas_ta. A prova ponta-a-ponta do PASSO 4 — celula ((8,21), True) contra a
celula 0.02/11 do kis_regime — fica atras de um importorskip e de stubs dos
modulos que arrastam rede/pandas.
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest

from backtest.keepitsimple import EMA_FAST, EMA_SLOW, extremos_entries
from backtest.kis_horizonte import (CAMPOS, CONTROLE, GRADE, PARES, PORTAO,
                                    PORTAO_ADX_MIN, PORTAO_LIMIAR, celula,
                                    entradas, linhas, mascara)
from backtest.kis_regime import passa


def _ts(dia: str) -> int:
    return int(datetime.fromisoformat(dia)
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def _serie(n: int, bar_ms: int = 14_400_000) -> list:
    """Passeio deterministico (LCG semeado) com range por barra — precisa
    inverter de estado varias vezes pro kis_extremos gerar populacao real."""
    out, p, seed = [], 100.0, 12345
    for i in range(n):
        seed = (1103515245 * seed + 12345) % (1 << 31)
        p = max(5.0, p * (1.0 + ((seed >> 16) % 2001 - 1000) / 20000.0))
        rng = p * 0.004
        out.append({"ts": i * bar_ms, "open": p - rng / 2, "high": p + rng,
                    "low": p - rng, "close": p, "volume": 1.0})
    return out


# ------------------------------------------------------------ grade fechada

def test_grade_tem_8_celulas_e_e_a_do_briefing():
    assert PARES == ((8, 21), (34, 89), (55, 144), (89, 233))
    assert PORTAO == (False, True)
    assert GRADE == [(p, g) for p in PARES for g in PORTAO]
    assert len(GRADE) == 8 and len(set(GRADE)) == 8


def test_controle_e_o_par_de_hoje_com_portao_desligado():
    assert CONTROLE == ((EMA_FAST, EMA_SLOW), False)
    assert GRADE[0] == CONTROLE


# ------------------------------------------------- entradas parametrizadas

def test_par_8_21_reproduz_extremos_entries_barra_a_barra():
    """O que a validacao do PASSO 4 exige na origem: com o par de hoje, a
    extracao parametrizada e a do keepitsimple — mesmos bar_ts, mesma direcao,
    mesmo hold, mesma ordem."""
    cndl = _serie(600)
    meu = entradas(cndl, EMA_FAST, EMA_SLOW)
    dele = extremos_entries(cndl)
    assert len(meu) > 0, "serie sintetica nao gerou entrada — teste vazio"
    assert [(e["bar_ts"], e["direction"], e["hold"]) for e in meu] == \
           [(e["bar_ts"], e["direction"], e["hold"]) for e in dele]


def test_warmup_de_cada_celula_e_a_ema_lenta_do_par():
    """Par longo descarta mais barras: nenhuma entrada antes do indice `slow`.
    Perder warmup e ESPERADO, e por isso o n de cada celula vai reportado."""
    cndl = _serie(600)
    for fast, slow in PARES:
        primeiras = {c["ts"] for c in cndl[:slow]}
        assert not [e for e in entradas(cndl, fast, slow)
                    if e["bar_ts"] in primeiras]


def test_par_longo_gera_menos_sinal_que_o_curto():
    cndl = _serie(1200)
    ns = [len(entradas(cndl, f, s)) for f, s in PARES]
    assert ns[0] > ns[-1], f"esperado menos sinal no par longo, veio {ns}"


def test_hold_tem_piso_1_e_e_a_distancia_ate_a_proxima_entrada():
    cndl = _serie(600)
    evs = entradas(cndl, 13, 34)
    assert all(e["hold"] >= 1 for e in evs)
    for a, b in zip(evs, evs[1:]):
        assert a["hold"] * 14_400_000 == b["bar_ts"] - a["bar_ts"]


def test_entradas_alternam_direcao_por_construcao():
    """O alvo e UMA posicao que so muda invertendo — vale para qualquer par."""
    for fast, slow in PARES:
        evs = entradas(_serie(600), fast, slow)
        assert all(a["direction"] != b["direction"]
                   for a, b in zip(evs, evs[1:]))


# ------------------------------------------------------------ portao fixo

def test_portao_e_o_par_congelado_validado_em_dado_virgem():
    assert (PORTAO_LIMIAR, PORTAO_ADX_MIN) == (0.02, 11)


def test_mascara_sempre_marca_o_controle():
    # bit 0 = portao desligado: vale 1 ate com valores ausentes (warmup).
    assert mascara("LONG", None, None) & 1
    assert mascara("SHORT", -9.0, 99.0) & 1


def test_mascara_marca_o_bit_do_portao_exatamente_quando_passa():
    for d, incl, adx in (("LONG", 0.05, 20.0), ("LONG", 0.01, 20.0),
                         ("LONG", 0.05, 5.0), ("SHORT", -0.05, 20.0),
                         ("SHORT", 0.05, 20.0), ("LONG", None, 20.0),
                         ("LONG", 0.05, None)):
        esperado = passa(d, incl, adx, PORTAO_LIMIAR, PORTAO_ADX_MIN)
        assert bool(mascara(d, incl, adx) >> 1 & 1) is esperado


def test_mascara_nao_recebe_o_par_do_detector():
    """O portao le a EMA21 SEMPRE, em toda celula — nao a EMA lenta do par. Se
    alguem tentar parametrizar isso, a assinatura muda e este teste cai."""
    import inspect
    assert list(inspect.signature(mascara).parameters) == ["direction", "incl",
                                                           "adx"]


def test_inclinacao_do_portao_usa_a_ema21_por_default():
    """A rampa do portao sai de `kis_regime.inclinacoes` sem argumento — e o
    default dela e EMA_SLOW=21, o mesmo componente validado em dado virgem."""
    import inspect

    from backtest.kis_regime import inclinacoes
    assert (inspect.signature(inclinacoes).parameters["ema_slow"].default
            == EMA_SLOW == 21)


# ----------------------------------------------------------------- celulas

def test_celula_le_pelo_bit_certo_e_preserva_o_retorno():
    trades = [(1, 10.0, 0b11), (2, -5.0, 0b01), (3, 7.0, 0b11)]
    assert celula(trades, 0) == [(1, (10.0,)), (2, (-5.0,)), (3, (7.0,))]
    assert celula(trades, 1) == [(1, (10.0,)), (3, (7.0,))]


def test_celula_com_portao_e_subconjunto_da_sem_portao():
    trades = [(1, 10.0, 0b11), (2, -5.0, 0b01), (3, 7.0, 0b11)]
    sem, com = celula(trades, 0), celula(trades, 1)
    assert set(t[0] for t in com) <= set(t[0] for t in sem)


# ------------------------------------------------------------------ linhas

def _por_sym(n_por_par: dict) -> dict:
    """{par: [(ts, ret, mask)]} para um simbolo so, com ts em datas reais."""
    return {"AAA": {p: [(_ts("2024-07-01"), 10.0, 0b11)] * n
                    for p, n in n_por_par.items()}}


def test_linhas_cobre_cada_celula_de_cada_simbolo_mais_o_universo():
    por_sym = _por_sym({p: 4 for p in PARES})
    rows = linhas(por_sym)
    assert len(rows) == len(GRADE) * 2                  # AAA + UNIVERSO
    assert {r["symbol"] for r in rows} == {"AAA", "UNIVERSO"}
    assert {(r["ema_fast"], r["ema_slow"], bool(r["portao"]))
            for r in rows} == {(p[0], p[1], g) for p, g in GRADE}


def test_pct_sinais_mantidos_e_n_da_celula_sobre_n_do_controle():
    por_sym = _por_sym({(8, 21): 10, (34, 89): 5, (55, 144): 2, (89, 233): 1})
    rows = {(r["ema_fast"], r["portao"]): r for r in linhas(por_sym)
            if r["symbol"] == "AAA"}
    assert rows[(8, 0)]["pct_sinais_mantidos"] == pytest.approx(100.0)
    assert rows[(34, 0)]["pct_sinais_mantidos"] == pytest.approx(50.0)
    assert rows[(89, 0)]["pct_sinais_mantidos"] == pytest.approx(10.0)


def test_simbolo_sem_trade_nao_divide_por_zero():
    rows = linhas(_por_sym({p: 0 for p in PARES}))
    assert all(r["pct_sinais_mantidos"] == 0.0 for r in rows)


def test_delta_e_a_distancia_ate_o_controle_do_PROPRIO_simbolo():
    ts1, ts2 = _ts("2024-07-01"), _ts("2025-10-01")     # 1a e 2a metades
    por_sym = {"AAA": {p: [(ts1, 10.0, 0b11), (ts2, 20.0, 0b11)]
                       for p in PARES},
               "BBB": {p: [(ts1, 1.0, 0b11), (ts2, 2.0, 0b11)] for p in PARES}}
    por_sym["AAA"][(34, 89)] = [(ts1, 30.0, 0b11), (ts2, 50.0, 0b11)]
    r = next(x for x in linhas(por_sym)
             if x["symbol"] == "AAA" and x["ema_fast"] == 34
             and x["portao"] == 0)
    assert r["delta_1a_vs_controle"] == pytest.approx(20.0)   # 30 - 10
    assert r["delta_2a_vs_controle"] == pytest.approx(30.0)   # 50 - 20


def test_controle_contra_si_mesmo_da_zero():
    for r in linhas(_por_sym({p: 3 for p in PARES})):
        if (r["ema_fast"], r["ema_slow"], r["portao"]) == (8, 21, 0):
            assert r["delta_1a_vs_controle"] == 0.0
            assert r["delta_2a_vs_controle"] == 0.0
            assert r["pct_sinais_mantidos"] == pytest.approx(100.0)


# ------------------------------------------ inconclusiva POR AMOSTRA

def test_celula_com_mediana_por_token_abaixo_do_minimo_e_inconclusiva():
    from backtest.kis_horizonte import MIN_TRADES, inconclusivas
    por_sym = {s: {p: [(_ts("2024-07-01"), 1.0, 0b11)] * n
                   for p, n in {(8, 21): 40, (34, 89): 40, (55, 144): 40,
                                (89, 233): 18}.items()}
               for s in ("AAA", "BBB", "CCC")}
    inconc = inconclusivas(linhas(por_sym))
    assert MIN_TRADES == 30
    assert {c[:2] for c in inconc} == {(89, 233)}      # os dois portoes


def test_universo_nao_mascara_a_falta_de_amostra_por_token():
    """3 tokens de 18 trades somam 54 no UNIVERSO — acima do MIN_TRADES. Se a
    conta lesse a linha do UNIVERSO, a celula passaria batido."""
    from backtest.kis_horizonte import inconclusivas
    por_sym = {s: {p: [(_ts("2024-07-01"), 1.0, 0b11)] * 18 for p in PARES}
               for s in ("AAA", "BBB", "CCC")}
    rows = linhas(por_sym)
    assert next(r for r in rows if r["symbol"] == "UNIVERSO"
                and r["ema_fast"] == 89)["n_trades"] == 54
    assert {c[:2] for c in inconclusivas(rows)} == {p for p in PARES}


def test_amos_vence_ok_mesmo_com_a_celula_aprovada():
    from backtest.kis_horizonte import _marca
    row = {"ema_fast": 89, "ema_slow": 233, "portao": 1, "n_trades": 999,
           "ret_1a_metade_bps": 10.0, "ret_2a_metade_bps": 10.0,
           "trimestres_pos": 4, "trimestres_total": 4}
    assert _marca(row, set()) == "ok"
    assert _marca(row, {(89, 233, 1)}) == "amos"


# -------------------------------------------------------------------- CSV

def test_campos_trocam_limiar_e_adx_min_pelo_par_e_pelo_portao():
    from backtest.kis_regime import CAMPOS as CAMPOS_REGIME
    assert "limiar" not in CAMPOS and "adx_min" not in CAMPOS
    assert CAMPOS[:4] == ["symbol", "ema_fast", "ema_slow", "portao"]
    assert "pct_sinais_mantidos" in CAMPOS
    # o resto do CSV e o mesmo do sweep_regime, na mesma ordem
    assert ([c for c in CAMPOS if c not in ("ema_fast", "ema_slow", "portao")]
            == [c for c in CAMPOS_REGIME if c not in ("limiar", "adx_min")])


def test_toda_linha_preenche_todos_os_campos():
    for r in linhas(_por_sym({p: 2 for p in PARES})):
        assert all(c in r for c in CAMPOS)


def test_custo_e_teto_sao_os_mesmos_do_kis_trail():
    from backtest import kis_horizonte, kis_trail
    assert kis_horizonte.COST_BPS == kis_trail.COST_BPS
    assert kis_horizonte.RSS_CEILING_MIB == kis_trail.RSS_CEILING_MIB


# --------------------------------------- PASSO 4: prova ponta-a-ponta dura

def _stub_pesados() -> None:
    """`excursion` puxa sweep/replay, que puxam pandas_ta e aiohttp. Stub em
    sys.modules na mesma forma do conftest — nada aqui usa sweep_symbol."""
    for nome, attrs in (("backtest.sweep", {"TIER1": ["AAA"],
                                            "sweep_symbol": lambda *a, **k: []}),
                        ("backtest.replay", {"BAR_15M_MS": 900_000})):
        if nome not in sys.modules:
            m = types.ModuleType(nome)
            m.__dict__.update(attrs)
            sys.modules[nome] = m


def test_celula_8_21_com_portao_reproduz_EXATAMENTE_a_0_02_11_do_regime(tmp_path):
    """PASSO 4 do briefing: mesma populacao, mesmos bar_ts, max|dif| = 0.00e+00.
    Roda as DUAS varreduras sobre o mesmo store."""
    _stub_pesados()
    pytest.importorskip("backtest.excursion")
    from backtest.candle_store import connect, upsert_candles
    from backtest.kis_horizonte import valida_portao

    bar = 14_400_000                                  # 4h
    db = tmp_path / "candles_teste.db"
    conn = connect(db)
    upsert_candles(conn, "AAA", "4h", _serie(1200, bar))
    conn.close()

    fim = datetime.fromtimestamp(1199 * bar / 1000.0, timezone.utc)
    dif, n_a, n_b = valida_portao(
        str(db), ["AAA"], "1970-01-01", fim.strftime("%Y-%m-%d"), "4h")
    assert n_a > 0, "serie sintetica nao gerou trade nenhum — teste vazio"
    assert n_a == n_b
    assert dif == 0.0
