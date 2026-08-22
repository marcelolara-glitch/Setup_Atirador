"""Testes puros do filtro de regime (backtest/kis_regime.py).

O topo de backtest/kis_regime.py e stdlib-only (candle_store/excursion sao
imports lazy dentro de `run`), entao este arquivo coleta no sandbox sem
pandas_ta. A prova ponta-a-ponta contra o kis_trail fica atras de um
importorskip e de stubs dos modulos que arrastam rede/pandas.
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest

from backtest.keepitsimple import _adx
from backtest.kis_regime import (ADX_MINS, ATR_LEN, CAMPOS, GRADE,
                                 INCL_LOOKBACK, LIMIARES, _atr_series, celula,
                                 inclinacoes, linhas, mascara, passa)


def _ts(dia: str) -> int:
    """'2024-06-01' -> epoch-ms UTC, mesma convencao de _iso_to_ms."""
    return int(datetime.fromisoformat(dia)
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def _c(h: float, l: float, c: float, o: float = None) -> dict:
    return {"ts": 0, "open": c if o is None else o, "high": h, "low": l,
            "close": c, "volume": 1.0}


# ------------------------------------------------------------ grade fechada

def test_grade_tem_16_celulas_com_um_unico_controle():
    assert len(GRADE) == 16
    assert GRADE[0] == (0.0, 0)                     # controle: sem filtro
    assert GRADE.count((0.0, 0)) == 1
    assert set(GRADE) == {(l, a) for l in LIMIARES for a in ADX_MINS}
    assert len(set(GRADE)) == len(GRADE)


def test_grade_e_a_do_briefing():
    assert LIMIARES == (0.00, 0.01, 0.02, 0.04)
    assert ADX_MINS == (0, 15, 20, 25)


# ------------------------------------------------------------------ ATR14

def test_atr_semente_e_media_simples_dos_n_primeiros_TRs():
    """Serie de barras com TR conhecido a mao: 15 barras de range 2.0 coladas
    (close = meio, sem gap) -> TR = 2.0 em toda barra -> ATR14[14] = 2.0."""
    cndl = [_c(101.0, 99.0, 100.0) for _ in range(15)]
    a = _atr_series(cndl)
    assert a[:ATR_LEN] == [None] * ATR_LEN          # warmup: None ate i < n
    assert a[14] == pytest.approx(2.0)


def test_atr_wilder_suaviza_a_partir_da_semente():
    """Semente 2.0 (14 barras de TR=2), depois UMA barra de TR=16:
    ATR = (2*13 + 16)/14 = 3.0. Conta fechada a mao."""
    cndl = [_c(101.0, 99.0, 100.0) for _ in range(15)]
    cndl.append(_c(108.0, 92.0, 100.0))             # h-l = 16 domina o TR
    a = _atr_series(cndl)
    assert a[14] == pytest.approx(2.0)
    assert a[15] == pytest.approx((2.0 * 13 + 16.0) / 14)


def test_atr_gap_usa_o_fechamento_anterior_no_TR():
    """Barra que abre em gap: TR = |high - close_anterior| = 10, nao h-l = 2."""
    cndl = [_c(101.0, 99.0, 100.0) for _ in range(15)]
    cndl.append(_c(110.0, 108.0, 109.0))
    a = _atr_series(cndl)
    assert a[15] == pytest.approx((2.0 * 13 + 10.0) / 14)


def test_atr_serie_curta_demais_e_toda_None():
    assert _atr_series([_c(1.0, 0.0, 0.5)] * ATR_LEN) == [None] * ATR_LEN


def test_atr_semente_bate_com_o_wilder_atr_do_excursion():
    """As duas contas de ATR do repo tem que concordar na semente — se alguem
    mexer numa, este teste derruba antes de a divergencia virar edge fabricado.
    """
    exc = pytest.importorskip("backtest.excursion")
    cndl = [_c(100.0 + i * 1.5 + (i % 3), 98.0 + i * 1.5 - (i % 2),
               99.0 + i * 1.5) for i in range(ATR_LEN + 1)]
    assert _atr_series(cndl)[ATR_LEN] == pytest.approx(exc._wilder_atr(cndl))


# ------------------------------------------------------------- inclinacao

def _rampa(n: int, meio_range: float) -> list:
    """Rampa de +1.0/barra com range FIXO de 2*meio_range por barra. Com
    meio_range >= 1.5 o termo h-l domina os dois termos de gap do TR, entao
    ATR14 = 2*meio_range exato (Wilder sobre entrada constante nao se move) e a
    conta a mao fecha. n grande o bastante pra EMA21 convergir (o erro da
    semente decai (20/22)^k)."""
    return [_c(100.0 + i + meio_range, 100.0 + i - meio_range, 100.0 + i)
            for i in range(n)]


def test_inclinacao_e_o_passo_da_EMA21_por_barra_em_ATR():
    """Rampa perfeita: close sobe 1.0/barra, entao a EMA21 madura tambem sobe
    1.0/barra e (EMA[i]-EMA[i-5])/5 = 1.0. Com ATR14 = 4.0 (range 4, gaps de 3
    e 1), inclinacao = 1.0/4.0 = 0.25."""
    cndl = _rampa(400, 2.0)
    assert _atr_series(cndl)[390] == pytest.approx(4.0)
    incl = inclinacoes(cndl, [c["close"] for c in cndl])
    assert incl[390] == pytest.approx(0.25, abs=1e-9)


def test_inclinacao_e_negativa_na_descida_e_simetrica():
    sobe = [_c(100.0 + i + 0.5, 100.0 + i - 0.5, 100.0 + i) for i in range(120)]
    desce = [_c(100.0 - i + 0.5, 100.0 - i - 0.5, 100.0 - i) for i in range(120)]
    a = inclinacoes(sobe, [c["close"] for c in sobe])[110]
    b = inclinacoes(desce, [c["close"] for c in desce])[110]
    assert b == pytest.approx(-a, abs=1e-6)


def test_inclinacao_e_None_no_warmup():
    cndl = [_c(100.5, 99.5, 100.0) for _ in range(120)]
    incl = inclinacoes(cndl, [c["close"] for c in cndl])
    assert all(v is None for v in incl[:INCL_LOOKBACK])
    assert incl[:ATR_LEN] == [None] * ATR_LEN       # ATR ainda nao existe


def test_inclinacao_de_lateral_perfeita_e_zero():
    cndl = [_c(100.5, 99.5, 100.0) for _ in range(120)]
    assert inclinacoes(cndl, [c["close"] for c in cndl])[110] == pytest.approx(0.0)


def test_inclinacao_normaliza_pela_volatilidade():
    """A MESMA rampa com o DOBRO do range por barra da METADE da inclinacao —
    e esse o ponto de dividir por ATR: rampa se mede em ATRs, nao em dolares.
    Um sinal so vale 0.10 de inclinacao se 0.10 significa a mesma coisa num
    token de range largo e num de range estreito."""
    fino, grosso = _rampa(400, 2.0), _rampa(400, 4.0)
    a = inclinacoes(fino, [c["close"] for c in fino])[390]
    b = inclinacoes(grosso, [c["close"] for c in grosso])[390]
    assert a == pytest.approx(0.25, abs=1e-9)
    assert b == pytest.approx(0.125, abs=1e-9)
    assert b == pytest.approx(a / 2.0, rel=1e-9)


# ---------------------------------------------------------------- porteiro

def test_controle_nao_veta_nada_nem_com_valores_ausentes():
    """A celula (0, 0) e SEM FILTRO de verdade: passa ate com incl/adx None,
    que e o unico jeito de a populacao bater com a do kis_trail."""
    for d in ("LONG", "SHORT"):
        assert passa(d, None, None, 0.0, 0)
        assert passa(d, -99.0, 0.0, 0.0, 0)


def test_long_precisa_de_rampa_a_favor():
    assert passa("LONG", 0.20, None, 0.10, 0)
    assert not passa("LONG", 0.05, None, 0.10, 0)
    assert not passa("LONG", -0.50, None, 0.10, 0)


def test_short_precisa_de_rampa_a_favor_no_sinal_oposto():
    assert passa("SHORT", -0.20, None, 0.10, 0)
    assert not passa("SHORT", -0.05, None, 0.10, 0)
    assert not passa("SHORT", 0.50, None, 0.10, 0)


def test_limiar_e_inclusivo_nos_dois_lados():
    assert passa("LONG", 0.10, None, 0.10, 0)
    assert passa("SHORT", -0.10, None, 0.10, 0)


def test_adx_min_e_inclusivo_e_veta_abaixo():
    assert passa("LONG", None, 20.0, 0.0, 20)
    assert not passa("LONG", None, 19.99, 0.0, 20)


def test_porta_ligada_com_valor_ausente_VETA():
    """Warmup nao e passe livre: nao se confirma o que nao se calcula."""
    assert not passa("LONG", None, 30.0, 0.10, 0)
    assert not passa("LONG", 0.50, None, 0.0, 20)


def test_as_duas_portas_sao_E_nao_OU():
    assert passa("LONG", 0.50, 30.0, 0.10, 20)
    assert not passa("LONG", 0.50, 10.0, 0.10, 20)   # rampa ok, ADX nao
    assert not passa("LONG", 0.01, 30.0, 0.10, 20)   # ADX ok, rampa nao


def test_adx_min_zero_nao_veta_por_adx_ausente():
    """adx_min=0 e porta DESLIGADA — nao pode cortar trade nenhum por warmup de
    ADX, senao a coluna pct_sinais_mantidos mediria a coisa errada."""
    assert passa("LONG", 0.50, None, 0.10, 0)


# ----------------------------------------------------------------- mascara

def test_mascara_sempre_marca_o_controle():
    assert mascara("LONG", None, None) & 1
    assert mascara("SHORT", -99.0, 0.0) & 1


def test_mascara_marca_exatamente_as_celulas_que_passam():
    incl, adx = 0.12, 22.0
    m = mascara("LONG", incl, adx)
    for k, (lim, amin) in enumerate(GRADE):
        assert bool(m >> k & 1) == passa("LONG", incl, adx, lim, amin)


def test_mascara_de_sinal_fraco_so_tem_o_controle():
    assert mascara("LONG", 0.0, 5.0) == 1


# ------------------------------------------------------- celula e subconjunto

def _tr(dia: str, ret: float, m: int) -> tuple:
    return (_ts(dia), ret, m)


def test_celula_e_subconjunto_do_controle_com_o_MESMO_retorno():
    trades = [_tr("2024-06-01", 10.0, 0b1), _tr("2024-07-01", -4.0, 0b11)]
    ctl, c1 = celula(trades, 0), celula(trades, 1)
    assert [t[0] for t in c1] == [_ts("2024-07-01")]
    assert set(t[0] for t in c1) <= set(t[0] for t in ctl)
    assert dict((t, r[0]) for t, r in c1).items() <= dict(
        (t, r[0]) for t, r in ctl).items()          # retorno nao muda


def test_celula_le_pelo_bit_certo():
    trades = [_tr("2024-06-01", 1.0, 1 | 1 << 5)]
    assert len(celula(trades, 5)) == 1
    assert celula(trades, 4) == []


# ------------------------------------------------------------------- linhas

def _pop(n_ctl: int, n_cel: int) -> list:
    """n_ctl trades, dos quais os n_cel primeiros tambem passam na celula 1."""
    return [_tr(f"2024-06-{i + 1:02d}", 1.0, 0b11 if i < n_cel else 0b1)
            for i in range(n_ctl)]


def test_pct_sinais_mantidos_e_n_da_celula_sobre_n_do_controle():
    rows = linhas({"AAA": _pop(10, 4)})
    ctl = next(r for r in rows if r["symbol"] == "AAA" and r["limiar"] == 0.0)
    cel = next(r for r in rows if r["symbol"] == "AAA"
               and (r["limiar"], r["adx_min"]) == GRADE[1])
    assert ctl["n_trades"] == 10 and ctl["pct_sinais_mantidos"] == 100.0
    assert cel["n_trades"] == 4 and cel["pct_sinais_mantidos"] == 40.0


def test_controle_mantem_100_pct_por_definicao():
    rows = linhas({"AAA": _pop(7, 2)})
    for r in rows:
        if r["limiar"] == 0.0 and r["adx_min"] == 0:
            assert r["pct_sinais_mantidos"] == 100.0


def test_pct_de_simbolo_sem_trade_nao_divide_por_zero():
    rows = linhas({"AAA": []})
    assert all(r["pct_sinais_mantidos"] == 0.0 for r in rows)


def test_linhas_cobre_cada_simbolo_e_o_universo():
    rows = linhas({"AAA": _pop(3, 1), "BBB": _pop(2, 2)})
    assert len(rows) == 3 * len(GRADE)
    assert {r["symbol"] for r in rows} == {"AAA", "BBB", "UNIVERSO"}
    uni = [r for r in rows if r["symbol"] == "UNIVERSO"]
    assert uni[0]["n_trades"] == 5


def test_delta_vs_controle_e_a_distancia_ate_a_celula_sem_filtro():
    """1a metade: controle soma 3 trades de +10; a celula 1 fica com 1 deles."""
    trades = [(_ts("2024-06-01"), 10.0, 0b11),
              (_ts("2024-06-02"), 10.0, 0b1),
              (_ts("2024-06-03"), 10.0, 0b1)]
    rows = linhas({"AAA": trades})
    ctl = next(r for r in rows if r["symbol"] == "AAA" and r["limiar"] == 0.0)
    cel = next(r for r in rows if r["symbol"] == "AAA"
               and (r["limiar"], r["adx_min"]) == GRADE[1])
    assert ctl["delta_1a_vs_controle"] == 0.0       # controle contra si mesmo
    assert ctl["ret_1a_metade_bps"] == pytest.approx(30.0)
    assert cel["ret_1a_metade_bps"] == pytest.approx(10.0)
    assert cel["delta_1a_vs_controle"] == pytest.approx(-20.0)


def test_delta_e_por_simbolo_nao_contra_o_controle_do_universo():
    rows = linhas({"AAA": _pop(4, 4), "BBB": _pop(4, 0)})
    aaa = next(r for r in rows if r["symbol"] == "AAA"
               and (r["limiar"], r["adx_min"]) == GRADE[1])
    assert aaa["delta_1a_vs_controle"] == 0.0       # AAA manteve tudo


def test_campos_do_csv_trocam_arme_recuo_e_acrescentam_mantidos():
    from backtest.kis_trail import CAMPOS as CAMPOS_TRAIL
    assert "arme" not in CAMPOS and "recuo" not in CAMPOS
    assert CAMPOS[:3] == ["symbol", "limiar", "adx_min"]
    assert "pct_sinais_mantidos" in CAMPOS
    assert set(CAMPOS_TRAIL) - set(CAMPOS) == {"arme", "recuo"}
    assert set(CAMPOS) - set(CAMPOS_TRAIL) == {"limiar", "adx_min",
                                               "pct_sinais_mantidos"}
    assert all(c in linhas({"AAA": _pop(2, 1)})[0] for c in CAMPOS)


def test_custo_e_o_mesmo_do_kis_trail():
    from backtest import kis_regime, kis_trail
    assert kis_regime.COST_BPS == kis_trail.COST_BPS
    assert kis_regime.RSS_CEILING_MIB == kis_trail.RSS_CEILING_MIB


# ------------------------------------------- prova ponta-a-ponta do controle

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


def _serie_sintetica(n: int, bar_ms: int) -> list:
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


def test_controle_reproduz_EXATAMENTE_o_arme_zero_do_kis_trail(tmp_path):
    """A validacao obrigatoria do briefing: mesma populacao, mesmos bar_ts,
    max|dif| = 0.00e+00. Roda as DUAS varreduras sobre o mesmo store."""
    _stub_pesados()
    pytest.importorskip("backtest.excursion")
    from backtest.candle_store import connect, upsert_candles
    from backtest.kis_regime import valida_controle

    bar = 14_400_000                                  # 4h
    db = tmp_path / "candles_teste.db"
    conn = connect(db)
    upsert_candles(conn, "AAA", "4h", _serie_sintetica(1200, bar))
    conn.close()

    fim = datetime.fromtimestamp(1199 * bar / 1000.0, timezone.utc)
    dif, n_a, n_b = valida_controle(
        str(db), ["AAA"], "1970-01-01", fim.strftime("%Y-%m-%d"), "4h")
    assert n_a > 0, "serie sintetica nao gerou trade nenhum — teste vazio"
    assert n_a == n_b
    assert dif == 0.0
