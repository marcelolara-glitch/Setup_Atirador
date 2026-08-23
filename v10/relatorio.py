"""v10/relatorio.py — UM bloco de relatório por setup do REGISTRO, genérico.

Genérico quer dizer: não há nada aqui que saiba o nome de um setup. O módulo
itera o REGISTRO, lê `trades_v10` pela chave `(setup_id, config_hash)` e compõe
o mesmo bloco para qualquer ficha — inclusive uma que ainda não existe. Um
setup novo entra no relatório no mesmo commit em que entra no registro, sem
tocar neste arquivo. Foi essa a razão de ele nascer separado do [VIGIA]: os
blocos do DONCHIAN-A e do KIS shadow são um `build_*` por setup, e o terceiro
seria o terceiro de uma fila sem fim.

O formato visual é o dos blocos do [VIGIA] e a data vem do MESMO formatador
(`shadow.vigia._utc_date_of_ms`): duas réguas de data na mesma mensagem seriam
duas mensagens. O `_fmt_bps` de lá NÃO é usado de propósito — a unidade deste
bloco é USDT e %, e imprimir bps também só repetiria o mesmo número numa
terceira escala.

UNIDADE. O resultado é sempre LÍQUIDO — bruto menos duas pernas de
`custo_bps_por_perna`, a mesma conta que o runner grava em `pnl_bps_liq`. O
valor em USDT é uma CONVERSÃO de leitura sobre :data:`NOCIONAL_USDT`, não um
saldo de conta: não há alavancagem, não há composição, e cada trade entra com o
mesmo nocional. O rodapé de cada bloco diz isso — o número não circula sem a
premissa junto.

WIN/LOSS aqui é o SINAL DO LÍQUIDO, não o status. Os adaptadores nomeiam saída
de formas diferentes (`WIN`/`LOSS`, `WIN_TP1`..`LOSS_SL`, `REVERTIDO`), e um
relatório genérico não pode conhecer esses nomes. Quem paga a conta é o sinal.

ISOLAMENTO. Um setup que estoure vira uma LINHA DE ERRO no lugar do bloco dele.
Sumir em silêncio seria pior do que não ter relatório: a mensagem chegaria
completa e menor, e ninguém notaria.

DOIS relatórios, não um. :func:`build_report` é o QUADRO — tudo o que existe,
uma vez por dia. :func:`build_delta` é o que MUDOU desde a última janela
reportada, a cada 4h. O quadro responde "onde estamos"; o delta responde "o que
aconteceu". Um não substitui o outro, e por isso o delta usa bps E USDT (a
unidade em que um trade individual se lê) enquanto o quadro usa USDT e % (a
unidade em que uma série se lê).

HEARTBEAT. O delta sai MESMO sem evento: uma linha por setup, com abertas, em
aberto e acumulado. É a diferença entre "nada aconteceu" e "o cron não rodou" —
sem a linha, os dois casos chegariam como a mesma ausência de mensagem. Com
ela, ausência de mensagem é incidente.

A JANELA é persistida em :data:`MARCA_TABELA` e só avança quando o envio
CONFIRMA. Envio que falha não perde evento: a próxima run reporta a janela
maior. E reprocessar a mesma janela não duplica nada, porque a janela é
semiaberta ``(desde, ate]`` e o que já foi reportado ficou para trás da marca.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from shadow.vigia import _utc_date_of_ms
from v10.runner import _bar_ms
from v10.schema import TABELA

__all__ = ["JANELA_PADRAO_MS", "NOCIONAL_USDT", "RECENTES", "bloco",
           "bloco_delta", "build_delta", "build_report", "ler_marca", "run",
           "run_delta"]

# Nocional de referência por trade. Constante declarada, não parâmetro: o dia em
# que dois setups forem lidos com nocionais diferentes, a comparação entre eles
# deixa de existir. Alavancagem NÃO é assumida — 1.000 USDT é o tamanho da
# posição, não a margem.
NOCIONAL_USDT = 1000.0
RECENTES = 5              # quantos fechados recentes o bloco lista

# Estado do delta. Tabela PRÓPRIA, criada aqui e não em `v10/schema.py`: aquele
# arquivo é o schema dos TRADES, e marca de relatório não é trade. Criada sob
# demanda (`IF NOT EXISTS`) — quem só quer ler o quadro não paga por ela.
MARCA_TABELA = "relatorio_v10_marca"
MARCA_DELTA = "delta"
# Janela da PRIMEIRA run, quando ainda não há marca: uma barra de 4h. Não é o
# histórico inteiro de propósito — a primeira mensagem seria um despejo de tudo
# o que já existe, e o delta nasceria ilegível.
JANELA_PADRAO_MS = 14_400_000


def _usdt(bps) -> float:
    """bps de resultado -> USDT sobre :data:`NOCIONAL_USDT`."""
    return NOCIONAL_USDT * float(bps or 0.0) / 10_000.0


def _fmt_res(bps) -> str:
    """Resultado nas duas unidades pedidas: USDT e %. `None` -> 'n/a'."""
    if bps is None:
        return "n/a"
    return f"{_usdt(bps):+.2f} USDT ({float(bps) / 100.0:+.2f}%)"


def _liq(row, custo_bps: float) -> float:
    """Líquido em bps de uma linha fechada. Usa o que o runner gravou; recompõe
    a partir do bruto só se a coluna estiver vazia (linha de antes da coluna)."""
    if row["pnl_bps_liq"] is not None:
        return float(row["pnl_bps_liq"])
    return (row["pnl_pct"] or 0.0) * 100.0 - 2.0 * float(custo_bps)


def _corrente(row, custo_bps: float):
    """Líquido em bps de uma linha ABERTA, pela marcação que o runner carimbou
    em `exit_state_json`. `None` quando não há marcação — nunca imputado: um
    zero inventado aqui viraria 'no zero a zero' num trade que está sangrando."""
    try:
        estado = json.loads(row["exit_state_json"] or "{}") or {}
    except (TypeError, ValueError):
        return None
    pct = estado.get("pnl_corrente_pct")
    if pct is None:
        return None
    return float(pct) * 100.0 - 2.0 * float(custo_bps)


def _pior_mergulho(bps_em_ordem) -> float:
    """Pior queda pico-a-vale da curva ACUMULADA, em bps (<= 0).

    Não é a maior perda de um trade: é o quanto a curva chegou a devolver desde
    o topo dela. É o número que diz quanto de paciência o setup exigiu de quem
    o estivesse seguindo — e o único que aparece antes do saldo final.
    """
    pico = acum = pior = 0.0
    for x in bps_em_ordem:
        acum += float(x)
        pico = max(pico, acum)
        pior = min(pior, acum - pico)
    return pior


def _por_direcao(fechados, custo: float, direcao: str) -> tuple:
    linhas = [_liq(r, custo) for r in fechados if r["direction"] == direcao]
    return len(linhas), sum(linhas)


def bloco(conn, spec, now: datetime) -> str:
    """Compõe o bloco de UM setup. Puro: só lê o DB, não faz I/O de rede."""
    now = now.astimezone(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    cfg = spec.config_hash
    custo = float(getattr(spec, "custo_bps_por_perna", 0.0) or 0.0)
    barra_ms = _bar_ms(spec.tf)

    rows = conn.execute(
        "SELECT symbol, direction, entry_ts, status, exit_ts, pnl_pct, "
        f"pnl_bps_liq, exit_state_json FROM {TABELA} "
        "WHERE setup_id=? AND config_hash=?", (spec.setup_id, cfg)).fetchall()
    # Linhas do MESMO setup sob outra configuração não entram na conta (série
    # diferente), mas também não somem: viram uma linha de aviso.
    outras = conn.execute(
        f"SELECT COUNT(*) FROM {TABELA} WHERE setup_id=? AND config_hash<>?",
        (spec.setup_id, cfg)).fetchone()[0]

    abertas = sorted((r for r in rows if r["status"] == "OPEN"),
                     key=lambda r: int(r["entry_ts"]))
    fechados = [r for r in rows if r["status"] != "OPEN"]
    liq = [_liq(r, custo) for r in fechados]
    ganhos = sum(1 for x in liq if x > 0)
    perdas = sum(1 for x in liq if x < 0)
    zerados = len(liq) - ganhos - perdas
    saldo = sum(liq)
    em_aberto = [c for c in (_corrente(r, custo) for r in abertas) if c is not None]
    total = saldo + sum(em_aberto)
    nl, sl_ = _por_direcao(fechados, custo, "LONG")
    ns, ss_ = _por_direcao(fechados, custo, "SHORT")
    ordenados = sorted((r for r in fechados if r["exit_ts"]),
                       key=lambda r: int(r["exit_ts"]))
    mergulho = _pior_mergulho(_liq(r, custo) for r in ordenados)

    L = [f"🧭 <b>[V10] {spec.setup_id}</b> · <code>{cfg}</code> · {spec.mode} · "
         f"{spec.estado_ciclo}"]
    if spec.aviso:
        L.append(f"  ⚠️ {spec.aviso}")
    if outras:
        L.append(f"  ℹ️ {outras} linha(s) de outra configuração fora desta conta")
    L += ["", f"<b>ACUMULADO</b> · fechados {len(fechados)} · abertas {len(abertas)}",
          f"  WIN {ganhos} · LOSS {perdas}"
          + (f" · zero {zerados}" if zerados else "") + " (sinal do líquido)",
          f"  saldo {_fmt_res(saldo)}",
          f"  em aberto {_fmt_res(sum(em_aberto))}"
          + (f" · {len(abertas) - len(em_aberto)} sem marcação"
             if len(em_aberto) < len(abertas) else "")
          + f" · TOTAL {_fmt_res(total)}",
          f"  LONG n={nl} {_fmt_res(sl_)} · SHORT n={ns} {_fmt_res(ss_)}",
          f"  pior mergulho da curva {_fmt_res(mergulho)}",
          "", f"<b>ABERTAS</b> ({len(abertas)})"]
    L += [f"  {r['symbol']} {r['direction']} · "
          f"{(now_ms - int(r['entry_ts'])) // barra_ms}b · "
          f"{_fmt_res(_corrente(r, custo))}" for r in abertas] or ["  — nenhuma"]
    L += ["", "<b>FECHADOS</b> (recentes)"]
    L += [f"  {r['symbol']} {r['direction']} {r['status']} · "
          f"{_fmt_res(_liq(r, custo))} · {_utc_date_of_ms(r['exit_ts'])}"
          for r in reversed(ordenados[-RECENTES:])] or ["  — nenhum"]
    # Separador em ponto: o relatório é pt-BR e o `,` do format é en-US.
    nocional = f"{NOCIONAL_USDT:,.0f}".replace(",", ".")
    L += ["", f"  <i>nocional de referência {nocional} USDT/trade, sem "
              f"alavancagem e sem composição; líquido de 2×{custo:.0f} bps</i>"]
    return "\n".join(L)


def build_report(conn, now: datetime, specs=None) -> str:
    """Um bloco por setup, na ordem do REGISTRO. NÃO levanta.

    `specs` é injetável (lista ou o próprio dict do registro) para que o teste
    não precise do registro real; o default é o REGISTRO, importado tarde para
    que quem só quer o formatador não arraste os detectores junto.
    """
    if specs is None:
        from v10.registro import REGISTRO
        specs = REGISTRO
    if isinstance(specs, dict):
        specs = list(specs.values())
    partes = []
    for spec in specs:
        sid = getattr(spec, "setup_id", "?")
        try:
            partes.append(bloco(conn, spec, now))
        except Exception as e:                    # isolamento: erro visível
            partes.append(f"🧭 <b>[V10] {sid}</b>\n"
                          f"  ⚠️ bloco indisponível: {type(e).__name__}: {e}")
    return "\n\n".join(partes)


# --- DELTA: o que mudou desde a última janela reportada -----------------------
def _fmt_bu(bps) -> str:
    """Resultado em bps E em USDT — a unidade de um trade individual."""
    if bps is None:
        return "n/a"
    return f"{float(bps):+.1f} bps ({_usdt(bps):+.2f} USDT)"


def _preco(x) -> str:
    """Preço legível sem escolher casas por token: BTC e DOGE na mesma régua."""
    return "n/a" if x is None else f"{float(x):.6g}"


def _utc_min_of_ms(ms) -> str:
    """`YYYY-MM-DD HH:MM` em UTC — a régua da janela do delta."""
    d = datetime.fromtimestamp(int(ms) / 1000, timezone.utc)
    return d.strftime("%Y-%m-%d %H:%M")


def _na_janela(ts, desde_ms, ate_ms) -> bool:
    """Janela SEMIABERTA `(desde, ate]`. Semiaberta é o que impede a barra de
    fronteira de ser reportada duas vezes — uma no fim de uma janela e outra no
    início da seguinte."""
    if ts is None:
        return False
    try:
        t = int(ts)
    except (TypeError, ValueError):
        return False
    return int(desde_ms) < t <= int(ate_ms)


def _atraso(row):
    """`barras_atraso` gravado pelo adaptador `reverse` (PR-C): quantas barras a
    saída ficou atrás da inversão que a causou. `None` quando o modelo de saída
    não mede isso — nunca zero por omissão, que seria afirmar 'sem atraso'."""
    try:
        estado = json.loads(row["exit_state_json"] or "{}") or {}
    except (TypeError, ValueError):
        return None
    v = estado.get("barras_atraso")
    return None if v is None else int(v)


def _linha_saida(row, custo: float, barra_ms: int) -> str:
    atraso = _atraso(row)
    barras = (int(row["exit_ts"]) - int(row["entry_ts"])) // barra_ms
    return (f"  ▼ FECHOU {row['symbol']} {row['direction']} {row['status']} · "
            f"{_preco(row['exit_price'])} · {_fmt_bu(_liq(row, custo))} · "
            f"{barras}b" + ("" if atraso is None else f" · atraso {atraso}b"))


def bloco_delta(conn, spec, desde_ms, ate_ms) -> str:
    """O que mudou em UM setup na janela `(desde_ms, ate_ms]`. Puro: só lê o DB.

    COM evento: uma linha por ABRIU e por FECHOU, e um rodapé com a posição
    corrente. SEM evento: UMA linha só — o heartbeat. As duas formas trazem
    abertas, em aberto e acumulado, para que a leitura do estado não dependa de
    ter havido movimento.
    """
    cfg = spec.config_hash
    custo = float(getattr(spec, "custo_bps_por_perna", 0.0) or 0.0)
    barra_ms = _bar_ms(spec.tf)
    rows = conn.execute(
        "SELECT symbol, direction, entry_ts, entry_price, status, exit_ts, "
        f"exit_price, pnl_pct, pnl_bps_liq, exit_state_json FROM {TABELA} "
        "WHERE setup_id=? AND config_hash=?", (spec.setup_id, cfg)).fetchall()

    abertas = [r for r in rows if r["status"] == "OPEN"]
    fechados = [r for r in rows if r["status"] != "OPEN"]
    acum = sum(_liq(r, custo) for r in fechados)
    marcadas = [c for c in (_corrente(r, custo) for r in abertas) if c is not None]
    sem_marca = len(abertas) - len(marcadas)
    rodape = (f"abertas {len(abertas)} · em aberto {_fmt_bu(sum(marcadas))}"
              + (f" ({sem_marca} sem marcação)" if sem_marca else "")
              + f" · acumulado {_fmt_bu(acum)}")

    novas = sorted((r for r in rows if _na_janela(r["entry_ts"], desde_ms, ate_ms)),
                   key=lambda r: int(r["entry_ts"]))
    saidas = sorted((r for r in fechados
                     if _na_janela(r["exit_ts"], desde_ms, ate_ms)),
                    key=lambda r: int(r["exit_ts"]))
    if not novas and not saidas:
        return f"  · <b>{spec.setup_id}</b> — sem eventos · {rodape}"

    L = [f"🧭 <b>{spec.setup_id}</b> · <code>{cfg}</code> · "
         f"{len(novas)} abriu · {len(saidas)} fechou"]
    L += [f"  ▲ ABRIU {r['symbol']} {r['direction']} · {_preco(r['entry_price'])}"
          for r in novas]
    L += [_linha_saida(r, custo, barra_ms) for r in saidas]
    L.append(f"  {rodape}")
    return "\n".join(L)


def build_delta(conn, specs, desde_ms, ate_ms) -> str:
    """Um `bloco_delta` por setup, com a janela no cabeçalho. NÃO levanta.

    `specs` default é :data:`v10.registro.ATIVOS`, não o REGISTRO inteiro: uma
    ficha desligada reportaria zeros a cada 4h e daria a impressão de estar
    sendo observada. Ela aparece no quadro diário, onde o aviso dela sai junto.
    """
    if specs is None:
        from v10.registro import ATIVOS
        specs = ATIVOS
    if isinstance(specs, dict):
        specs = list(specs.values())
    L = [f"⚡ <b>[V10Δ]</b> {_utc_min_of_ms(desde_ms)} → "
         f"{_utc_min_of_ms(ate_ms)} (UTC)"]
    for spec in specs:
        sid = getattr(spec, "setup_id", "?")
        try:
            L.append(bloco_delta(conn, spec, desde_ms, ate_ms))
        except Exception as e:                    # isolamento: erro visível
            L.append(f"  · <b>{sid}</b> — ⚠️ delta indisponível: "
                     f"{type(e).__name__}: {e}")
    return "\n".join(L)


# --- marca da janela ---------------------------------------------------------
def _garantir_marca(conn) -> None:
    conn.execute(f"CREATE TABLE IF NOT EXISTS {MARCA_TABELA} ("
                 "chave TEXT PRIMARY KEY, ate_ms INTEGER NOT NULL, "
                 "atualizado_em TEXT)")


def ler_marca(conn, chave: str = MARCA_DELTA):
    """Fim da última janela reportada COM SUCESSO, em epoch-ms. `None` se nunca."""
    _garantir_marca(conn)
    r = conn.execute(f"SELECT ate_ms FROM {MARCA_TABELA} WHERE chave=?",
                     (chave,)).fetchone()
    return None if r is None else int(r[0])


def gravar_marca(conn, ate_ms, chave: str = MARCA_DELTA) -> None:
    """Avança a marca. Chamado SÓ depois de o envio confirmar."""
    _garantir_marca(conn)
    conn.execute(
        f"INSERT INTO {MARCA_TABELA} (chave, ate_ms, atualizado_em) VALUES (?,?,?) "
        "ON CONFLICT(chave) DO UPDATE SET ate_ms=excluded.ate_ms, "
        "atualizado_em=excluded.atualizado_em",
        (chave, int(ate_ms),
         datetime.now(timezone.utc).isoformat(timespec="seconds")))
    conn.commit()


# --- envio -------------------------------------------------------------------
def _default_send(text: str) -> bool:
    # Mesmo transporte do [VIGIA] (shadow/vigia.py:222): `_tg_send` do
    # telegram.py, que é o canal cru. Import lazy — quem só formata não arrasta
    # a dependência de rede junto.
    from telegram import _tg_send
    return _tg_send(text)


def _enviar(msg: str, send_fn, log) -> bool:
    """Envia e devolve se foi. Falha é SILENCIOSA: warning e segue. O banco é a
    fonte de verdade — mensagem é leitura dele, não o registro."""
    try:
        ok = bool((send_fn or _default_send)(msg))
    except Exception as e:
        log.warning(f"[v10] falha no envio: {type(e).__name__}: {e}")
        return False
    if not ok:
        log.warning("[v10] envio retornou False — relatorio nao entregue")
    return ok


def _abrir_conn(db_path):
    from v10.schema import connect
    return connect() if db_path is None else connect(db_path)


def run(now: datetime | None = None, send_fn=None, specs=None, db_path=None,
        log=None):
    """Quadro completo, uma vez por dia. -> `(mensagem, enviado)`."""
    log = log or logging.getLogger("v10.relatorio")
    now = now or datetime.now(timezone.utc)
    conn = _abrir_conn(db_path)
    try:
        msg = build_report(conn, now, specs=specs)
    finally:
        conn.close()
    ok = _enviar(msg, send_fn, log)
    log.info(f"[v10] quadro gerado (enviado={ok})")
    return msg, ok


def run_delta(now: datetime | None = None, send_fn=None, specs=None,
              db_path=None, log=None):
    """Delta da janela e avanço da marca. -> `(mensagem, enviado)`.

    A marca só avança se o envio confirmar. Envio que falha NÃO perde evento: a
    janela seguinte começa onde esta começou e reporta os dois trechos juntos.
    """
    log = log or logging.getLogger("v10.relatorio")
    now = now or datetime.now(timezone.utc)
    ate_ms = int(now.astimezone(timezone.utc).timestamp() * 1000)
    conn = _abrir_conn(db_path)
    try:
        desde_ms = ler_marca(conn)
        if desde_ms is None:
            desde_ms = ate_ms - JANELA_PADRAO_MS
            log.info("[v10] sem marca anterior: janela inicial de "
                     f"{JANELA_PADRAO_MS // 60_000} min")
        msg = build_delta(conn, specs, desde_ms, ate_ms)
        ok = _enviar(msg, send_fn, log)
        if ok:
            gravar_marca(conn, ate_ms)
        else:
            log.warning("[v10] marca NAO avancou: a proxima run reporta a "
                        "janela desde o mesmo ponto")
    finally:
        conn.close()
    log.info(f"[v10] delta gerado (enviado={ok})")
    return msg, ok


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    run()
