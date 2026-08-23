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
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from shadow.vigia import _utc_date_of_ms
from v10.runner import _bar_ms
from v10.schema import TABELA

__all__ = ["NOCIONAL_USDT", "RECENTES", "bloco", "build_report"]

# Nocional de referência por trade. Constante declarada, não parâmetro: o dia em
# que dois setups forem lidos com nocionais diferentes, a comparação entre eles
# deixa de existir. Alavancagem NÃO é assumida — 1.000 USDT é o tamanho da
# posição, não a margem.
NOCIONAL_USDT = 1000.0
RECENTES = 5              # quantos fechados recentes o bloco lista


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
