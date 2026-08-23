"""Testes do COLETOR KIS+REGIME (shadow/kis_regime.py) e do SEGUNDO bloco do
[VIGIA]. Stdlib-only (detector/portao da bancada sao stdlib; telegram e lazy).

Cobrem: sinal so na INVERSAO do alvo, portao vetando/deixando passar, saida na
inversao oposta (inclusive ATRASADA, quando uma run cai), colunas de bracket
NULL (sem stop/alvo/expiry), evidence com inclinacao+adx, idempotencia,
shadow_runs, falha isolada por simbolo, e a REGRESSAO do bloco do DONCHIAN-A
(byte-identico com e sem o bloco KIS).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from backtest.keepitsimple import _alvo_extremos, states
from shadow import donchian_a as sd
from shadow import kis_regime as kr
from shadow import vigia

BAR = sd.BAR_MS
QUIET = logging.getLogger("kis_test")             # sem FileHandler: nao suja logs/
QUIET.addHandler(logging.NullHandler())
NOW = datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc)
DAY = "2026-07-18"


def _bar(ts, c, h=None, l=None):
    return {"ts": ts, "open": c, "high": h if h is not None else c * 1.001,
            "low": l if l is not None else c * 0.999, "close": c, "volume": 1.0}


def _serie(closes):
    return [_bar(i * BAR, c) for i, c in enumerate(closes)]


def _rampa(n=80, base=100.0, passo=1.0):
    # Alta monotona: EMA8 > EMA21 e close > EMA8 => VERDE confirmado => alvo +1.
    return [base + passo * i for i in range(n)]


# --- detector ----------------------------------------------------------------
def test_avaliar_rampa_alta_alvo_long():
    alvo, d, incl, adx = kr.avaliar(_serie(_rampa()))
    assert alvo == 1 and incl > 0 and adx > 0


def test_avaliar_sinal_so_na_barra_da_inversao():
    c = _rampa()
    # Na rampa o alvo ja e +1 ha muitas barras: nao ha inversao NESTA barra.
    assert kr.avaliar(_serie(c))[1] is None
    # A barra em que o alvo muda de 0/-1 para +1 e a unica que emite direction.
    st = states(c)
    alvo = _alvo_extremos(st)
    i = next(k for k in range(1, len(alvo)) if alvo[k] != alvo[k - 1] and alvo[k])
    assert kr.avaliar(_serie(c)[:i + 1])[1] == "LONG"


def test_avaliar_warmup_sem_sinal():
    assert kr.avaliar(_serie([100.0] * 5)) == (0, None, None, None)


# --- portao ------------------------------------------------------------------
def test_portao_veta_incl_baixa(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    kr._abrir(conn, "BTCUSDT", "LONG", 0.001, 50.0, _bar(BAR, 100.0), QUIET)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_trades").fetchone()["c"] == 0


def test_portao_veta_adx_baixo(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    kr._abrir(conn, "BTCUSDT", "LONG", 0.50, 5.0, _bar(BAR, 100.0), QUIET)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_trades").fetchone()["c"] == 0


def test_portao_veta_warmup_none(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    kr._abrir(conn, "BTCUSDT", "LONG", None, None, _bar(BAR, 100.0), QUIET)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_trades").fetchone()["c"] == 0


def test_portao_passa_e_grava_evidencia(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    kr._abrir(conn, "BTCUSDT", "LONG", 0.05, 20.0, _bar(BAR, 100.0), QUIET)
    r = conn.execute("SELECT * FROM shadow_trades").fetchone()
    ev = json.loads(r["evidence_json"])
    assert r["status"] == "OPEN" and r["entry_price"] == 100.0
    assert ev["inclinacao"] == 0.05 and ev["adx"] == 20.0
    assert ev["limiar"] == kr.LIMIAR and ev["adx_min"] == kr.ADX_MIN
    # SEM stop, SEM alvo, SEM cap de tempo: colunas de bracket ficam NULL.
    assert r["sl_price"] is None and r["tp_price"] is None
    assert r["expiry_bar_ts"] is None


def test_short_exige_inclinacao_negativa(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    kr._abrir(conn, "BTCUSDT", "SHORT", +0.05, 20.0, _bar(BAR, 100.0), QUIET)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_trades").fetchone()["c"] == 0
    kr._abrir(conn, "BTCUSDT", "SHORT", -0.05, 20.0, _bar(BAR, 100.0), QUIET)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_trades").fetchone()["c"] == 1


def test_idempotencia_mesma_barra(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    for _ in range(3):
        kr._abrir(conn, "BTCUSDT", "LONG", 0.05, 20.0, _bar(BAR, 100.0), QUIET)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_trades").fetchone()["c"] == 1


# --- saida por inversao ------------------------------------------------------
def _open_long(tmp_path, price=100.0):
    conn = sd._connect(str(tmp_path / "k.db"))
    kr._abrir(conn, "BTCUSDT", "LONG", 0.05, 20.0, _bar(BAR, price), QUIET)
    return conn


def test_saida_so_na_inversao(tmp_path):
    conn = _open_long(tmp_path)
    kr._fechar(conn, "BTCUSDT", 1, _bar(2 * BAR, 80.0), QUIET)   # alvo ainda LONG
    assert conn.execute("SELECT status FROM shadow_trades").fetchone()[0] == "OPEN"


def test_saida_na_inversao_win(tmp_path):
    conn = _open_long(tmp_path)
    kr._fechar(conn, "BTCUSDT", -1, _bar(3 * BAR, 110.0), QUIET)
    r = conn.execute("SELECT * FROM shadow_trades").fetchone()
    assert r["status"] == "WIN" and r["exit_price"] == 110.0
    assert abs(r["pnl_pct"] - 10.0) < 1e-9 and r["exit_ts"] == str(3 * BAR)
    assert r["r_multiple"] is None                 # sem stop => R nao existe


def test_saida_na_inversao_loss(tmp_path):
    conn = _open_long(tmp_path)
    kr._fechar(conn, "BTCUSDT", -1, _bar(3 * BAR, 95.0), QUIET)
    r = conn.execute("SELECT status, pnl_pct FROM shadow_trades").fetchone()
    assert r["status"] == "LOSS" and abs(r["pnl_pct"] + 5.0) < 1e-9


def test_saida_atrasada_run_perdida(tmp_path):
    # Alvo VIGENTE contrario (nao so na barra da inversao): a saida sai atrasada,
    # mas sai — trade nenhum fica preso quando uma run cai.
    conn = _open_long(tmp_path)
    kr._fechar(conn, "BTCUSDT", -1, _bar(30 * BAR, 90.0), QUIET)
    assert conn.execute("SELECT status FROM shadow_trades").fetchone()[0] == "LOSS"


def test_alvo_zero_nao_fecha(tmp_path):
    conn = _open_long(tmp_path)
    kr._fechar(conn, "BTCUSDT", 0, _bar(2 * BAR, 90.0), QUIET)
    assert conn.execute("SELECT status FROM shadow_trades").fetchone()[0] == "OPEN"


# --- run_once ----------------------------------------------------------------
def test_run_once_ciclo_e_shadow_runs(tmp_path):
    db = str(tmp_path / "k.db")
    ok, falhas = kr.run_once(db, fetch_fn=lambda s: _serie(_rampa()),
                             symbols=["BTCUSDT"], log=QUIET)
    conn = sd._connect(db)
    assert (ok, falhas) == (1, 0)
    assert conn.execute("SELECT COUNT(*) c FROM shadow_runs").fetchone()["c"] == 1


def test_run_once_falha_isolada_nao_levanta(tmp_path):
    def _boom(symbol):
        raise RuntimeError("rede caiu")
    ok, falhas = kr.run_once(str(tmp_path / "k.db"), fetch_fn=_boom,
                             symbols=["BTCUSDT", "ARBUSDT"], log=QUIET)
    assert (ok, falhas) == (0, 2)


def test_run_once_barras_insuficientes(tmp_path):
    ok, falhas = kr.run_once(str(tmp_path / "k.db"),
                             fetch_fn=lambda s: _serie([100.0] * 10),
                             symbols=["BTCUSDT"], log=QUIET)
    assert (ok, falhas) == (0, 1)


def test_universo_congelado():
    assert kr.SYMBOLS == ["ARBUSDT", "BNBUSDT", "BTCUSDT", "SUIUSDT",
                          "TRXUSDT", "WLDUSDT"]
    assert (kr.LIMIAR, kr.ADX_MIN) == (0.02, 11)


# --- bloco do [VIGIA] --------------------------------------------------------
def _seed_bloco(tmp_path):
    conn = sd._connect(str(tmp_path / "k.db"))
    conn.execute("INSERT INTO shadow_trades (id, symbol, direction, "
                 "bar_ts_entry, entry_price, status, exit_ts, exit_price, "
                 "pnl_pct) VALUES ('a','BTCUSDT','LONG',?,100.0,'WIN',?,101.0,"
                 "1.0)", (str(_ms(DAY, 0)), str(_ms(DAY, 12))))
    conn.execute("INSERT INTO shadow_trades (id, symbol, direction, "
                 "bar_ts_entry, entry_price, status) VALUES "
                 "('b','ARBUSDT','SHORT',?,2.0,'OPEN')", (str(_ms(DAY, 4)),))
    conn.execute("INSERT INTO shadow_runs (run_ts, ok, falhas) VALUES (?,6,0)",
                 (f"{DAY}T00:12:00+00:00",))
    conn.commit()
    return conn


def _ms(day, hour):
    return int(datetime.fromisoformat(f"{day}T{hour:02d}:00:00+00:00")
               .timestamp() * 1000)


def test_bloco_cabecalho_e_disclaimer(tmp_path):
    msg = kr.build_block(_seed_bloco(tmp_path), NOW)
    assert "[VIGIA] KIS+REGIME" in msg and DAY in msg
    assert "COLETOR" in msg and "-769 bps" in msg and "SEM stop" in msg
    # O numero e PROVISORIO (ledger 22/08): a linha nao pode afirmar veredito.
    assert "PROVISÓRIO" in msg and "REPROVADO" not in msg


def test_bloco_liquido_desconta_dois_takers(tmp_path):
    # bruto 1.0% = 100 bps; liquido = 100 - 2x5 = +90.0 bps.
    msg = kr.build_block(_seed_bloco(tmp_path), NOW)
    assert "+90.0 bps" in msg and "WIN 1 · LOSS 0" in msg


def test_bloco_secoes_e_abertas(tmp_path):
    msg = kr.build_block(_seed_bloco(tmp_path), NOW)
    for sec in ("ACUMULADO", "ABERTAS", "FECHADOS", "HOJE"):
        assert f"<b>{sec}</b>" in msg
    assert "ARBUSDT SHORT ·" in msg and "runs 1/6" in msg
    assert "cobertura incompleta" in msg           # 1 de 6 runs no dia


def test_bloco_db_vazio_nao_quebra(tmp_path):
    msg = kr.build_block(sd._connect(str(tmp_path / "vazio.db")), NOW)
    assert "— nenhuma" in msg and "— nenhum" in msg and "runs 0/6" in msg


# --- REGRESSAO: o [VIGIA] volta a ser SO o bloco 9A --------------------------
def test_vigia_nao_carrega_mais_o_bloco_kis(tmp_path):
    """O shadow KIS foi DESLIGADO: o papel dele passou para o v10, que roda o
    mesmo detector no `trades_v10`. A mensagem diaria volta a ser o 9A do
    DONCHIAN-A e nada mais — byte a byte, sem sequer um separador sobrando."""
    d_db = str(tmp_path / "d.db")
    _seed_bloco(tmp_path)                          # k.db populado: e IGNORADO
    sent = []
    msg, ok = vigia.run(d_db, now=NOW, send_fn=lambda t: sent.append(t) or True,
                        log=QUIET)
    esperado = vigia.build_report(sd._connect(d_db), NOW)
    assert ok and msg == esperado                  # BYTE-IDENTICO e sozinho
    assert "KIS+REGIME" not in msg and sent == [msg]


def test_build_block_do_kis_continua_intacto(tmp_path):
    """Desligado nao e removido. `shadow/kis_regime.py` esta na trava sha256 e o
    banco `shadow_kis_regime.db` fica para historico: o que saiu foi a CHAMADA
    dentro do [VIGIA], nao o modulo. Quem quiser ler o coletor ainda le."""
    msg = kr.build_block(_seed_bloco(tmp_path), NOW)
    assert "[VIGIA] KIS+REGIME" in msg


def test_vigia_nao_importa_nem_chama_mais_o_bloco_kis():
    """Guarda contra religar por descuido. Olha o CODIGO, nao a prosa: o
    docstring do `run` explica por que o bloco saiu, e explicar nao e religar.
    O que nao pode voltar e o import e a chamada."""
    import ast
    import inspect
    arvore = ast.parse(inspect.getsource(vigia))
    importados = {a.name for n in ast.walk(arvore)
                  if isinstance(n, ast.ImportFrom) and "kis_regime" in (n.module or "")
                  for a in n.names}
    chamadas = {n.func.id for n in ast.walk(arvore)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert not importados and "build_block" not in chamadas
