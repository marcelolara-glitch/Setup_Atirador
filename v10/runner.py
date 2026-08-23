"""v10/runner.py — o laço que roda um setup. Uma função: :func:`rodar`.

O runner é genérico: ele lê a ficha (`SetupSpec`), não pergunta ao setup como
quer ser rodado. A ordem de cada símbolo é a mesma do shadow do DONCHIAN-A —
**resolve o que está aberto ANTES de detectar entrada nova** —, porque o
inverso deixaria a barra da saída abrir posição no mesmo instante em que fecha
a anterior, e o histórico do shadow não teria como ser reproduzido.

Três regras que este módulo impõe e nenhum detector pode driblar:

1. **Vela fechada.** Só entra no detector barra com ``ts + bar_ms <= agora_ms``.
   O `ate` da fonte não basta: uma corretora pode devolver a barra corrente.
2. **Cadência.** ``(bar_ts // bar_ms) % cadencia_barras == 0`` — ancorado na
   grade absoluta de tempo, não num contador. Reiniciar o processo não desloca
   a fase, e dois processos concorrentes avaliam as MESMAS barras.
3. **Isolamento.** Falha de um símbolo não derruba os outros (warning e segue);
   falha de um setup não derruba os outros (:func:`rodar_todos`).

`espacamento_barras` sai de ``spec.exit_params`` e é lido AQUI, não pelo
adaptador: espaçamento é regra de carteira (não reabrir a mesma direção do
mesmo símbolo antes de N barras) e depende do que já está gravado, que é do
runner. No DONCHIAN-A ele coincide com o horizonte `h_bars` de propósito.

Contrato do detector: recebe as velas fechadas (crescentes) e devolve ``None``
ou um sinal com ``direction`` e ``entry_price``; ``atr_value`` e ``evidence``
são opcionais. Se o detector expuser um atributo ``anotar``, o runner o usa
para carimbar as velas antes de entregá-las ao adaptador — é assim que o
modelo `reverse` enxerga o alvo vigente sem que o runner saiba o que é alvo.
"""

from __future__ import annotations

import json
import logging
import sqlite3

from v10.data import velas as _velas_padrao
from v10.exits import adaptador
from v10.schema import TABELA

__all__ = ["BAR_MS", "rodar", "rodar_todos"]

BAR_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000,
          "1h": 3_600_000, "4h": 14_400_000}
FOLGA_BARRAS = 80          # forward pedido além do warmup, p/ resolver abertas


def _bar_ms(tf: str) -> int:
    """ms de uma barra do `tf`. KeyError explícito: tf novo entra no mapa."""
    chave = str(tf).lower()
    if chave not in BAR_MS:
        raise KeyError(f"tf desconhecido: {tf!r} (conhecidos: {sorted(BAR_MS)})")
    return BAR_MS[chave]


def _pnl_pct(direction: str, entry: float, saida: float) -> float:
    """Bruto em %, sem alavancagem — mesma conta de shadow.donchian_a._pnl."""
    if not entry:
        return 0.0
    move = (saida - entry) if direction == "LONG" else (entry - saida)
    return move / float(entry) * 100.0


def _campo(sinal, nome, default=None):
    """Duck typing na fronteira: o sinal pode ser dict ou objeto (CLAUDE.md §5)."""
    if isinstance(sinal, dict):
        return sinal.get(nome, default)
    return getattr(sinal, nome, default)


def _atualizar(spec, adap, conn, symbol, velas, agora_ms) -> int:
    """Resolve as posições abertas do setup nesse símbolo. -> nº de fechadas."""
    abertas = conn.execute(
        f"SELECT id, entry_price, exit_params_json, exit_state_json FROM {TABELA} "
        "WHERE setup_id=? AND config_hash=? AND symbol=? AND status='OPEN'",
        (spec.setup_id, spec.config_hash, symbol)).fetchall()
    fechadas = 0
    for r in abertas:
        estado = {"exit_params": json.loads(r["exit_params_json"] or "{}"),
                  "exit_state": json.loads(r["exit_state_json"] or "{}")}
        novo, status, preco = adap.avaliar(spec, estado, velas, agora_ms=agora_ms)
        s = novo["exit_state"]
        ru, dd = float(s.get("max_runup") or 0.0), float(s.get("max_drawdown") or 0.0)
        if status is None:                       # segue aberta: só o rastro
            conn.execute(
                f"UPDATE {TABELA} SET exit_state_json=?, max_runup=?, "
                "max_drawdown=? WHERE id=?", (json.dumps(s), ru, dd, r["id"]))
            continue
        direction = novo["exit_params"].get("direction")
        pnl = _pnl_pct(direction, float(r["entry_price"]), float(preco))
        conn.execute(
            f"UPDATE {TABELA} SET status=?, exit_ts=?, exit_price=?, pnl_pct=?, "
            "pnl_bps_liq=?, max_runup=?, max_drawdown=?, exit_state_json=? "
            "WHERE id=?",
            (status, str(s.get("exit_ts_ms") or velas[-1]["ts"]), float(preco), pnl,
             pnl * 100.0 - 2.0 * float(spec.custo_bps_por_perna), ru, dd,
             json.dumps(s), r["id"]))
        fechadas += 1
    conn.commit()
    return fechadas


def _abrir(spec, adap, conn, symbol, velas, bar_ms) -> int:
    """Avalia o detector na última barra fechada e grava a entrada. -> 0 ou 1."""
    bar = velas[-1]
    bar_ts = int(bar["ts"])
    cadencia = max(int(spec.cadencia_barras or 1), 1)
    if (bar_ts // bar_ms) % cadencia:
        return 0
    sinal = spec.detector(velas)
    if not sinal:
        return 0
    direction = _campo(sinal, "direction")
    if direction not in tuple(spec.direcoes):
        return 0
    esp = int((spec.exit_params or {}).get("espacamento_barras", 0))
    if esp:
        ultimo = conn.execute(
            f"SELECT MAX(CAST(entry_ts AS INTEGER)) FROM {TABELA} WHERE "
            "setup_id=? AND config_hash=? AND symbol=? AND direction=?",
            (spec.setup_id, spec.config_hash, symbol, direction)).fetchone()[0]
        if ultimo is not None and bar_ts - int(ultimo) < esp * bar_ms:
            return 0
    estado = adap.abrir(spec, sinal, bar)
    try:
        conn.execute(
            f"INSERT INTO {TABELA} (id, setup_id, config_hash, mode, symbol, tf, "
            "direction, entry_ts, entry_price, status, evidence_json, "
            "exit_params_json, exit_state_json) VALUES (?,?,?,?,?,?,?,?,?,'OPEN',?,?,?)",
            (f"{spec.setup_id}-{spec.config_hash}-{symbol}-{bar_ts}", spec.setup_id,
             spec.config_hash, spec.mode, symbol, spec.tf, direction, str(bar_ts),
             float(_campo(sinal, "entry_price")),
             json.dumps(_campo(sinal, "evidence") or {}),
             json.dumps(estado["exit_params"]), json.dumps(estado["exit_state"])))
        conn.commit()
        return 1
    except sqlite3.IntegrityError:
        return 0                                 # (setup, config, symbol, barra)


def rodar(spec, conn, agora_ms, velas_fn=None, log=None) -> dict:
    """Uma passada do setup `spec` sobre o universo dele.

    Parâmetros:
        spec: a ficha (:class:`v10.spec.SetupSpec`).
        conn: conexão de :func:`v10.schema.connect` (row_factory de Row).
        agora_ms: relógio da passada, em epoch-ms. Explícito de propósito —
            é o que torna o replay de histórico idêntico à execução ao vivo.
        velas_fn: fonte de velas, injetável (CLAUDE.md §5). Default:
            :func:`v10.data.velas`. O detector NUNCA fala com a corretora.

    Retorno:
        ``{"setup_id", "config_hash", "ok", "falhas", "abertos", "fechados",
        "falha_symbols"}``. Nunca levanta por causa de um símbolo.
    """
    velas_fn = velas_fn or _velas_padrao
    log = log or logging.getLogger("v10.runner")
    adap = adaptador(spec.exit_model)
    bar_ms = _bar_ms(spec.tf)
    warmup = int(spec.warmup_barras)
    n = max(warmup + FOLGA_BARRAS, 2)
    anotar = getattr(spec.detector, "anotar", None)
    r = {"setup_id": spec.setup_id, "config_hash": spec.config_hash,
         "ok": 0, "falhas": 0, "abertos": 0, "fechados": 0, "falha_symbols": []}
    for symbol in spec.symbols:
        try:
            brutas = velas_fn(symbol, spec.tf, n) or []
            velas = [v for v in brutas if int(v["ts"]) + bar_ms <= int(agora_ms)]
            velas.sort(key=lambda v: int(v["ts"]))
            if len(velas) < warmup:
                raise ValueError(f"barras insuficientes: {len(velas)} < {warmup}")
            if anotar is not None:
                velas = anotar(velas)
            r["fechados"] += _atualizar(spec, adap, conn, symbol, velas, agora_ms)
            r["abertos"] += _abrir(spec, adap, conn, symbol, velas, bar_ms)
            r["ok"] += 1
        except Exception as e:
            r["falhas"] += 1
            r["falha_symbols"].append(symbol)
            log.warning(f"  [{spec.setup_id}/{symbol}] falha: {type(e).__name__}: {e}")
    log.info(f"[v10] {spec.setup_id} ok={r['ok']} falhas={r['falhas']} "
             f"abertos={r['abertos']} fechados={r['fechados']}")
    return r


def rodar_todos(specs, conn, agora_ms, velas_fn=None, log=None) -> list:
    """Roda vários setups. Um setup que estoure NÃO impede os seguintes: entra
    no resultado com ``erro`` preenchido e o laço continua."""
    log = log or logging.getLogger("v10.runner")
    saida = []
    for spec in specs:
        try:
            saida.append(rodar(spec, conn, agora_ms, velas_fn=velas_fn, log=log))
        except Exception as e:
            log.warning(f"[v10] setup {getattr(spec, 'setup_id', '?')} caiu: "
                        f"{type(e).__name__}: {e}")
            saida.append({"setup_id": getattr(spec, "setup_id", "?"),
                          "erro": f"{type(e).__name__}: {e}"})
    return saida
