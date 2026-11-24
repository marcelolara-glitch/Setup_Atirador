# tests/test_kis_grade_escala.py — sandbox, briefing de 23/11/2026 (executado
# em 24/11): `kis_grade --escala-indicadores F`. F = 1 byte-identico ao de
# d6fabce em 4h (gabarito em test_sem_travas_de_estudo) e em 1h (aqui); F > 1
# multiplica os comprimentos dos indicadores e do sinal, ATR efetivo = r x
# ATR(14F), limiar do portao 0,02 / F. Teste de mutacao: com F = 4 os periodos
# que CHEGAM as funcoes sao 136/356/84/20/52/8/56, e trocar F muda a saida.
import hashlib
import random
from statistics import median

import pytest

from backtest import excursion
from backtest import kis_grade as g
from backtest import kis_refino as kr
from backtest import kis_saida as ks
from backtest.candle_store import connect, upsert_candles
from backtest.kis_regime import alvos
from tests.test_sem_travas_de_estudo import FIM, _serie_1h, _store_1h

H = 3_600_000
SYMS2 = ["BTCUSDT", "ETHUSDT"]
CEL_1H = ["REF_BASE", "REF_T5s", "k3_p0_t0", "k2_p3-6_f50-25_t512",
          "k4_p6-12_f33-33_t1536"]
# sha256 dos CSVs do kis_grade em 1h com o codigo de d6fabce (store de
# `_store_1h` com SYMS2 ate FIM, celulas CEL_1H)
GABARITO_1H = {
    "kis_grade_2":
        "da3f4582d7703f019ef190f36b540fc89c895b950a6b823c3b6dff956eb40076",
    "kis_grade_resumo":
        "32dcf3bc72a827645ae5283755cc2f284f7c5ef9fd99bd6991b4fdf8af8b7b74",
    "carteira_KIS_GRADE":
        "07e7b7cf7aecb408b1534eac76dcd9c04dd3590dc8cf6a3a8427d2a98be7469a"}


def _agrega_4h(cs: list) -> list:
    """Velas de 4h a partir das de 1h (blocos alinhados a 00/04/.../20 UTC)."""
    out, blocos = [], {}
    for c in cs:
        blocos.setdefault(c["ts"] - c["ts"] % ks.BAR_4H_MS, []).append(c)
    for ts in sorted(blocos):
        b = blocos[ts]
        if len(b) == 4:
            out.append({"ts": ts, "open": b[0]["open"], "close": b[-1]["close"],
                        "high": max(x["high"] for x in b),
                        "low": min(x["low"] for x in b), "volume": 1.0})
    return out


def _store_1h_4h(path, syms, ate_iso, choque_apos=None, seed=11):
    """1h aleatorio + 4h AGREGADO do mesmo 1h (razao de ATR real). Com
    `choque_apos`, a volatilidade x5 a partir dessa data."""
    conn = connect(path)
    rnd = random.Random(seed)
    for s in syms:
        p, ts, cs = 100.0, ks._ms("2024-06-17"), []
        fim = ks._ms(ate_iso)
        lim = ks._ms(choque_apos) if choque_apos else None
        while ts < fim:
            o, sd = p, 0.05 if lim and ts >= lim else 0.01
            p = max(1.0, p * (1 + rnd.gauss(0.0001, sd)))
            cs.append({"ts": ts, "open": o, "close": p,
                       "high": max(o, p) * (1 + abs(rnd.gauss(0, .003))),
                       "low": min(o, p) * (1 - abs(rnd.gauss(0, .003))),
                       "volume": 1.0})
            ts += H
        upsert_candles(conn, s, "1h", cs)
        upsert_candles(conn, s, "4h", _agrega_4h(cs))
    conn.close()


@pytest.fixture(scope="module")
def db1h(tmp_path_factory):
    p = tmp_path_factory.mktemp("esc") / "c.db"
    _store_1h(p, SYMS2, FIM)
    return str(p)


@pytest.fixture(scope="module")
def db14(tmp_path_factory):
    p = tmp_path_factory.mktemp("esc14") / "c.db"
    _store_1h_4h(p, SYMS2, FIM)
    return str(p)


def _roda(db, tmp_path, *extra) -> dict:
    assert g.main(["--store", db, "--symbols", *SYMS2, "--tf", "1h",
                   "--logs", str(tmp_path), "--sem-guarda-relogio",
                   "--celulas", *CEL_1H, *extra]) == 0
    return {pref: hashlib.sha256(next(tmp_path.glob(pref + "*.csv"))
                                 .read_bytes()).hexdigest()
            for pref in GABARITO_1H}


# ---- 1. F = 1 em 1h: byte-identico a d6fabce ----------------------------------
def test_f1_em_1h_byte_identico_a_d6fabce(db1h, tmp_path):
    assert _roda(db1h, tmp_path) == GABARITO_1H
    d2 = tmp_path / "explicito"
    d2.mkdir()
    assert _roda(db1h, d2, "--escala-indicadores", "1") == GABARITO_1H


# ---- 2. escala: valores e validacao -------------------------------------------
def test_indicadores_x1_sao_os_de_sempre_e_x4_multiplica():
    e1 = kr.escala("1h")
    assert (e1["ema"], e1["ema_ref"], e1["lb"], e1["adx"], e1["conf"],
            e1["atr"], e1["limiar"], e1["r"]) == \
        ((34, 89), 21, 5, 13, 2, 14, 0.02, 1.0)
    e4 = kr.escala("1h", 4, 2.0)
    assert (e4["ema"], e4["ema_ref"], e4["lb"], e4["adx"], e4["conf"],
            e4["atr"]) == ((136, 356), 84, 20, 52, 8, 56)
    assert e4["limiar"] == pytest.approx(0.005) and e4["r"] == 2.0
    assert (e4["borda"], e4["cap"]) == (192, 1024)     # duracoes nao mudam
    for ruim in ((0, 1.0), (2, 0.0), (1.5, 1.0)):
        with pytest.raises(ValueError):
            kr.escala("1h", *ruim)


def test_cli_recusa_escala_invalida():
    for ruim in ("0", "-2", "2.5", "x"):
        with pytest.raises(SystemExit):
            g.parser().parse_args(["--escala-indicadores", ruim])
    assert g.parser().parse_args([]).escala_indicadores == 1


# ---- 3. MUTACAO: periodos que chegam as funcoes com F = 4 ---------------------
def _espiao(monkeypatch, mod, nome, reg):
    orig = getattr(mod, nome)

    def f(*a, **kw):
        reg.setdefault(nome, []).append((a[1:], kw))
        return orig(*a, **kw)
    monkeypatch.setattr(mod, nome, f)


def test_mutacao_f4_periodos_efetivos(monkeypatch):
    reg = {}
    for nome in ("_ema", "_adx", "inclinacoes", "entradas", "gatilhos",
                 "medir_sem_borda", "limiares_portao", "passa", "states"):
        _espiao(monkeypatch, kr, nome, reg)
    wil = []
    orig_w = excursion._wilder_atr
    monkeypatch.setattr(excursion, "_wilder_atr",
                        lambda c, p=14: wil.append(p) or orig_w(c, p))
    cn = _serie_1h(4000)
    esc = kr.escala("1h", 4, 2.0)
    out = kr.trades_simbolo(cn, "X", "novo", kr.novo_cont(),
                            janela=(cn[0]["ts"], cn[-1]["ts"]), esc=esc)
    assert out["BASE"] and out["T5s"] and out["T5e"]
    x = kr._ind(esc)
    gL, gS, _ = kr.gatilhos(cn, None, *x["ema"], x["conf"])
    list(kr.eventos(cn, "sandbox", kr.novo_cont(), esc=esc))
    # EMA 136/356 (gatilhos/estados), EMA do portao 84 (T5e)
    assert {a[0] for a, _ in reg["_ema"]} == {136, 356, 84}
    assert {(a[0], a[1]) for a, _ in reg["states"]} == {(136, 356)}
    assert {a[1:] for a, _ in reg["gatilhos"]} == {(136, 356, 8)}
    assert {a for a, _ in reg["entradas"]} == {(136, 356, 8)}
    # inclinacao: ATR 56, EMA 84, lookback 20
    assert {a[1:] for a, _ in reg["inclinacoes"]} == {(56, 84, 20)}
    assert {a[0] for a, _ in reg["_adx"]} == {52}
    assert {(kw["periodo_atr"], kw["mult_atr"])
            for _, kw in reg["medir_sem_borda"]} == {(56, 2.0)}
    lp = {a[1:] for a, _ in reg["limiares_portao"]}
    assert lp == {(84, 20, 0.005, kr.PORTAO_ADX_MIN, 56, 52, 2.0)}
    assert {a[2] for a, _ in reg["passa"]} == {0.005}           # limiar/F
    assert {a[3] for a, _ in reg["passa"]} == {kr.PORTAO_ADX_MIN}  # 11 igual
    assert set(wil) == {56}                                     # ATR 14F


def test_atr_efetivo_e_r_vezes_atr14f_e_portao_divide_por_r():
    from backtest.excursion import _wilder_atr
    cn = _serie_1h(3000)
    e = kr.escala("1h", 4, 2.0)
    evs = list(kr.eventos(cn, "novo", kr.novo_cont(),
                          janela=(cn[0]["ts"], cn[-1]["ts"]), esc=e))
    assert evs
    for ev in evs:
        i = ev["i"]
        assert ev["atr"] == 2.0 * _wilder_atr(cn[i - 56:i + 1], 56)
        assert i >= 356                          # warm-up da EMA longa
    closes = [c["close"] for c in cn]
    inc, _ = kr.incl_adx(cn, closes, e)
    inc1, _ = kr.incl_adx(cn, closes, kr.escala("1h", 4, 1.0))
    assert all((a is None and b is None) or a == b / 2.0
               for a, b in zip(inc, inc1))
    # stop do motor a k x ATR efetivo
    ev = evs[0]
    sim = kr.simula(cn, ev["i"], ev["e_full"], ev["en"], ev["atr"], ev["sg"])
    assert all(x[4] != "stop" or x[2] == pytest.approx(
        ev["en"] - ev["sg"] * 3.0 * ev["atr"]) or x[2] == cn[x[0]]["open"]
        for x in sim[0])


def test_gatilho_com_confirmacao_generica_casa_com_o_alvo():
    """Toque do gatilho e fechamento alem dele <=> o alvo vira na barra, com
    confirmacao 8 (F = 4); com 2 = regra de sempre."""
    cn = _serie_1h(5000, seed=7)
    closes = [c["close"] for c in cn]
    for f, s, conf in ((136, 356, 8), (34, 89, 2)):
        gL, gS, _ = kr.gatilhos(cn, None, f, s, conf)
        al = alvos(closes, f, s, conf)
        viradas = 0
        for t in range(s + 1, len(cn)):
            if al[t] == 1 and al[t - 1] != 1:
                viradas += 1
                assert gL[t] is not None and closes[t] > gL[t] * (1 - 1e-12)
            if al[t] == -1 and al[t - 1] != -1:
                assert gS[t] is not None and closes[t] < gS[t] * (1 + 1e-12)
            if gL[t] is not None and closes[t] > gL[t] * (1 + 1e-9):
                assert al[t] == 1
            if gS[t] is not None and closes[t] < gS[t] * (1 - 1e-9):
                assert al[t] == -1
        assert viradas > 3
    assert kr.gatilhos(cn) == kr.gatilhos(cn, None, 34, 89, 2)


def test_trocar_f_muda_a_saida(db14, tmp_path):
    hs = {}
    for F in (1, 3, 4):
        d = tmp_path / f"f{F}"
        d.mkdir()
        hs[F] = _roda(db14, d, "--escala-indicadores", str(F))
    assert len({h["kis_grade_2"] for h in hs.values()}) == 3
    assert len({h["kis_grade_resumo"] for h in hs.values()}) == 3
    log = next((tmp_path / "f4").glob("kis_grade_2*.log")).read_text()
    primeira = log.splitlines()[0]
    assert "| tf 1h |" in primeira and "escala indicadores x4 | r = " in \
        primeira
    assert "iguais" in log and "so no kis_refino 0 | so aqui 0" in log
    for ln in log.splitlines():
        if " x BASE do kis_refino" in ln or " x T5s do kis_refino" in ln:
            n = ln.split("iguais ")[1].split(" de ")
            assert n[0] == n[1].split(" ")[0], ln       # conferencia fecha


# ---- 4. r: so H1, mediana no universo, mesma data ----------------------------
def test_r_mede_so_h1_e_bate_com_a_conta_direta(tmp_path):
    from backtest.candle_store import read_candles
    from backtest.excursion import _wilder_atr
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _store_1h_4h(a, SYMS2, "2025-10-01")
    _store_1h_4h(b, SYMS2, "2025-10-01", choque_apos="2025-08-26")
    ca, cb = connect(str(a)), connect(str(b))
    ra, rb = g.mede_r(ca, SYMS2, "1h", 4), g.mede_r(cb, SYMS2, "1h", 4)
    assert ra["r"] == rb["r"] and ra["n"] == rb["n"]   # depois de H1 nao entra
    assert 1.4 < ra["r"] < 2.8                          # ~ raiz(4) no passeio
    xs = []
    ini, fim = ks._ms(ks.H1[0]), ks._ms(ks.H2[0])
    for s in SYMS2:
        c4 = read_candles(ca, s, "4h", start_ms=None, end_ms=fim - 1)
        c1 = read_candles(ca, s, "1h", start_ms=None, end_ms=fim - 1)
        p1 = {c["ts"]: j for j, c in enumerate(c1)}
        for k in range(14, len(c4)):
            j = p1.get(c4[k]["ts"] + 3 * H)
            if c4[k]["ts"] >= ini and j is not None and j >= 56:
                xs.append(_wilder_atr(c4[k - 14:k + 1], 14) /
                          _wilder_atr(c1[j - 56:j + 1], 56))
    assert ra["r"] == median(xs) and ra["n"] == len(xs)
    assert ra["fora"] == [] and ra["n_sym"] == 2
    assert g.mede_r(ca, SYMS2 + ["ZZZUSDT"], "1h", 4)["fora"] == ["ZZZUSDT"]
    ca.close()
    cb.close()
