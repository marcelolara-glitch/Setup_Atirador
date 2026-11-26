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

__all__ = ["JANELA_PADRAO_MS", "LIMITE_TELEGRAM", "NOCIONAL_USDT", "RECENTES",
           "bloco", "bloco_carteira", "bloco_delta", "build_delta", "carteira", "build_report", "fatiar",
           "ler_marca", "run", "run_delta"]

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

# Teto de `sendMessage` da Bot API. Mensagem maior volta HTTP 400 e SOME — e a
# mensagem grande e justamente a que mais importa: barra com muitas inversoes e
# barra de virada de mercado. Por isso :func:`fatiar`, e nao um `msg[:4096]`.
LIMITE_TELEGRAM = 4096
# Reserva para o cabecalho de parte (``"(12/34)\n"``). Fixa: o tamanho do
# cabecalho depende do total de partes, que depende do corte — reservar o pior
# caso resolve a circularidade sem um segundo passe.
_RESERVA_CABECALHO = 12


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


def _fmt_coleta(falhas) -> str:
    """Trecho de rodapé com os símbolos que NÃO resolveram nesta run.

    `falhas` é a lista de símbolos que o runner não conseguiu coletar (ou cujas
    barras não bastaram). Vazia ou `None` -> string vazia: rodapé só cresce
    quando há o que dizer.

    A CONTAGEM é o que sai na mensagem, e os primeiros nomes vão junto só para
    orientar — a lista inteira está no log, por símbolo. Sinal perdido não pode
    chegar como sinal ausente: sem esta linha, um setup que perdeu 30 dos 65
    símbolos manda o MESMO delta de um setup que avaliou os 65 e não viu nada.
    """
    nomes = [str(x) for x in (falhas or [])]
    if not nomes:
        return ""
    amostra = ", ".join(nomes[:3])
    resto = len(nomes) - 3
    return (f" · ⚠️ coleta {len(nomes)} falhou ({amostra}"
            + (f" +{resto}" if resto > 0 else "") + ")")


def _fmt_venue(alt) -> str:
    """Trecho de rodapé com os símbolos servidos por corretora NÃO primária.

    `alt` são as entradas ``"SYMBOL@venue"`` do `venue_alt` de
    :func:`v10.runner.rodar`. Vazia ou `None` -> string vazia, e é aí que está o
    cuidado: silêncio aqui é AUSÊNCIA DE INFORMAÇÃO, nunca a afirmação de que
    tudo veio da primária. Quem é servido pelo fallback leu régua diferente do
    resto da série — outra liquidez, outra taxa, outro fecho de barra —, e sem
    esta linha esse símbolo chega ao relatório como qualquer outro.
    """
    nomes = [str(x) for x in (alt or [])]
    if not nomes:
        return ""
    amostra = ", ".join(nomes[:3])
    resto = len(nomes) - 3
    return (f" · 🔀 venue {len(nomes)} fora da primária ({amostra}"
            + (f" +{resto}" if resto > 0 else "") + ")")


def bloco_delta(conn, spec, desde_ms, ate_ms, falhas=None, venue_alt=None) -> str:
    """O que mudou em UM setup na janela `(desde_ms, ate_ms]`. Puro: só lê o DB.

    COM evento: uma linha por ABRIU e por FECHOU, e um rodapé com a posição
    corrente. SEM evento: UMA linha só — o heartbeat. As duas formas trazem
    abertas, em aberto e acumulado, para que a leitura do estado não dependa de
    ter havido movimento.

    `falhas` são os símbolos que a coleta desta run não resolveu (o
    `falha_symbols` de :func:`v10.runner.rodar`). Entram no rodapé como
    contagem. `None` = sem informação de coleta (o delta rodou fora da run) e
    NÃO é o mesmo que zero falhas — por isso a ausência não imprime nada.

    `venue_alt` (``["SYMBOL@venue", ...]``) é o mesmo contrato para os símbolos
    servidos por corretora diferente da primária.
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
              + f" · acumulado {_fmt_bu(acum)}" + _fmt_coleta(falhas)
              + _fmt_venue(venue_alt))

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


# --- DELTA POR SEÇÕES: fichas com `banca_usdt` (kis_3489_final_4h) -----------
# LEITURA DO ABERTO (a mesma em todas as seções): o "não realizado" de uma
# aberta é SÓ a fração restante marcada no último close; o que as parciais dela
# já executaram entra no REALIZADO. `pnl_corrente_pct` do runner traz as duas
# coisas somadas (realizado + resto), então aqui o resto é
# `pnl_corrente_pct - pnl_realizado_pct`. Somar as duas no aberto contaria a
# parcial duas vezes na banca. Custo: 2 × `custo_bps_por_perna` POR FRAÇÃO —
# parcial paga a fração dela, o resto paga a fração restante; somadas, 2 pernas
# cheias, a mesma conta do runner para o trade fechado.
_ROTULO_MODO = {"shadow": "paper"}
_MOTIVOS = ("stop", "flip")      # sempre impressos; outro motivo só se > 0


def _br(x, casas: int = 2, sinal: bool = True) -> str:
    """Número em formato brasileiro (milhar ponto, decimal vírgula). Com
    `sinal`, sempre explícito — e o negativo usa o menos tipográfico."""
    v = float(x)
    if round(v, casas) == 0:
        v = 0.0                                   # nada de "−0,00"
    txt = f"{abs(v):,.{casas}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    if not sinal:
        return ("−" if v < 0 else "") + txt
    return ("−" if v < 0 else "+") + txt


def _estado(row) -> dict:
    try:
        return json.loads(row["exit_state_json"] or "{}") or {}
    except (TypeError, ValueError):
        return {}


def _perna_bps(row, preco, fracao: float, custo: float) -> float:
    """Líquido em bps de UMA parcial, já ponderado pela fração dela."""
    entry = float(row["entry_price"])
    sg = 1.0 if row["direction"] == "LONG" else -1.0
    bruto = sg * (float(preco) - entry) / entry * 10_000.0 if entry else 0.0
    return float(fracao) * (bruto - 2.0 * custo)


def _resto_bps(row, custo: float):
    """Não realizado (bps) da fração restante de uma aberta. `None` sem marca."""
    s = _estado(row)
    pct = s.get("pnl_corrente_pct")
    if pct is None:
        return None
    fr = float(s.get("fracao_restante", 1.0))
    resto_pct = float(pct) - float(s.get("pnl_realizado_pct") or 0.0)
    return resto_pct * 100.0 - fr * 2.0 * custo


def _ler(conn, spec):
    return conn.execute(
        "SELECT symbol, direction, entry_ts, entry_price, status, exit_ts, "
        f"pnl_pct, pnl_bps_liq, exit_state_json FROM {TABELA} "
        "WHERE setup_id=? AND config_hash=?",
        (spec.setup_id, spec.config_hash)).fetchall()


def carteira(conn, spec, rows=None) -> dict:
    """Retrato da carteira AGORA, em USDT. Puro: só lê `trades_v10`.

    É a linha que o runner grava em `carteira_v10` a cada passada e a base das
    seções Carteira/Abertas do delta. Nocional das abertas = `mao_usdt` × fração
    restante (o tamanho que de fato segue na mão).
    """
    ep = spec.exit_params or {}
    mao = float(ep["mao_usdt"])
    custo = float(getattr(spec, "custo_bps_por_perna", 0.0) or 0.0)
    u = mao / 10_000.0
    rows = _ler(conn, spec) if rows is None else rows
    realizado = aberto = nocional = 0.0
    sem_marca = 0
    abertas = [r for r in rows if r["status"] == "OPEN"]
    for r in rows:
        if r["status"] != "OPEN":
            realizado += _liq(r, custo) * u       # trade inteiro, com parciais
    for r in abertas:
        s = _estado(r)
        realizado += sum(_perna_bps(r, p["preco"], p["fracao"], custo)
                         for p in s.get("parciais") or []) * u
        nocional += mao * float(s.get("fracao_restante", 1.0))
        b = _resto_bps(r, custo)
        if b is None:
            sem_marca += 1
        else:
            aberto += b * u
    return {"banca_usdt": float(ep["banca_usdt"]), "realizado_usdt": realizado,
            "aberto_usdt": aberto, "nocional_usdt": nocional,
            "n_abertas": len(abertas), "sem_marca": sem_marca}


def gravar_carteira(conn, spec, ts_ms) -> None:
    """Uma linha em `carteira_v10`. Idempotente no `(setup, config, ts_ms)`."""
    c = carteira(conn, spec)
    conn.execute(
        "INSERT OR REPLACE INTO carteira_v10 (ts_ms, setup_id, config_hash, "
        "banca_usdt, realizado_usdt, aberto_usdt, nocional_usdt, n_abertas) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (int(ts_ms), spec.setup_id, spec.config_hash, c["banca_usdt"],
         c["realizado_usdt"], c["aberto_usdt"], c["nocional_usdt"],
         c["n_abertas"]))
    conn.commit()


def rebaixamento(bancas, inicial) -> tuple:
    """(atual, máximo) em % do pico, sobre a série de banca total.

    O pico começa na banca inicial: a carteira nasce nela, e uma série que já
    começasse abaixo dela teria a queda inicial escondida."""
    pico, atual, maximo = float(inicial), 0.0, 0.0
    for b in bancas:
        pico = max(pico, float(b))
        atual = (pico - float(b)) / pico * 100.0 if pico > 0 else 0.0
        maximo = max(maximo, atual)
    return atual, maximo


def _data_janela(desde_ms, ate_ms) -> str:
    """`14/11 08:00 → 12:00 UTC`; a data do fim só sai se mudar de dia."""
    d = datetime.fromtimestamp(int(desde_ms) / 1000, timezone.utc)
    a = datetime.fromtimestamp(int(ate_ms) / 1000, timezone.utc)
    fim = a.strftime("%H:%M") if a.date() == d.date() else a.strftime("%d/%m %H:%M")
    return f"{d.strftime('%d/%m %H:%M')} → {fim} UTC"


def _secao(titulo: str) -> str:
    return f"━━━━━━ {titulo} ━━━━━━"


def _lado(rotulo: str, n: int, usdt: float, extra: str = "") -> str:
    return f"   {rotulo.ljust(5)} {n} → {_br(usdt)}{extra}"


def bloco_carteira(conn, spec, desde_ms, ate_ms, falhas=None, venue_alt=None,
                   guarda=None) -> str:
    """Delta por seções de uma ficha com `banca_usdt`. Puro: só lê o DB.

    Fechadas e Parciais são da JANELA `(desde, ate]`; Abertas, Carteira e Saúde
    saem SEMPRE — são o heartbeat. `falhas`/`guarda` `None` = sem informação da
    run (delta rodado sozinho), impresso como `n/d`, nunca como zero.
    """
    ep = spec.exit_params or {}
    mao, banca0 = float(ep["mao_usdt"]), float(ep["banca_usdt"])
    custo = float(getattr(spec, "custo_bps_por_perna", 0.0) or 0.0)
    u = mao / 10_000.0
    rows = _ler(conn, spec)
    fechados = [r for r in rows if r["status"] != "OPEN"]
    abertas = [r for r in rows if r["status"] == "OPEN"]
    c = carteira(conn, spec, rows)

    modo = _ROTULO_MODO.get(spec.mode, spec.mode)
    alav = ep.get("alav")
    L = [f"{spec.setup_id} · {modo} · banca {_br(banca0, 0, False)} · "
         f"mão {_br(mao, 0, False)}" + (f" · {_br(alav, 0, False)}x" if alav else "")]

    # FECHADAS NA JANELA — trade inteiro (inclui as parciais que ele teve)
    janela = [r for r in fechados if _na_janela(r["exit_ts"], desde_ms, ate_ms)]
    L += [_secao("FECHADAS NA JANELA"),
          f"{len(janela)} trades → {_br(sum(_liq(r, custo) for r in janela) * u)} USDT"]
    for direcao, rot in (("SHORT", "Short"), ("LONG", "Long")):
        lado = [r for r in janela if r["direction"] == direcao]
        if not lado:
            continue
        mot = [str(r["status"]).lower() for r in lado]
        nomes = list(_MOTIVOS) + sorted(set(mot) - set(_MOTIVOS))
        extra = "".join(f" · {m} {mot.count(m)}" for m in nomes
                        if m in _MOTIVOS or mot.count(m))
        L.append(_lado(rot, len(lado), sum(_liq(r, custo) for r in lado) * u, extra))

    # PARCIAIS — execuções com ts na janela, de abertas e de fechadas
    pernas = [(p, r) for r in rows for p in _estado(r).get("parciais") or []
              if _na_janela(p.get("ts_ms"), desde_ms, ate_ms)]
    if pernas:
        fr = [round(float(p["fracao"]) * 100) for p, _ in pernas]
        grupos = " · ".join(f"{fr.count(f)} × {f}%" for f in sorted(set(fr), reverse=True))
        tot = sum(_perna_bps(r, p["preco"], p["fracao"], custo) for p, r in pernas) * u
        L.append(f"Parciais: {len(pernas)} → {_br(tot)} USDT ({grupos})")
    else:
        L.append("Parciais: 0")

    # ABERTAS — só a fração restante (ver LEITURA DO ABERTO acima)
    L += [_secao("ABERTAS"),
          f"{len(abertas)} posições → {_br(c['aberto_usdt'])} USDT (não realizado)"
          + (f" · {c['sem_marca']} sem marcação" if c["sem_marca"] else "")]
    for direcao, rot in (("SHORT", "Short"), ("LONG", "Long")):
        lado = [r for r in abertas if r["direction"] == direcao]
        if lado:
            v = sum(b for b in (_resto_bps(r, custo) for r in lado) if b is not None)
            L.append(_lado(rot, len(lado), v * u))

    # CARTEIRA
    banca = banca0 + c["realizado_usdt"] + c["aberto_usdt"]
    stops = [r for r in fechados if str(r["status"]).lower() == "stop"]
    serie = [row[0] for row in conn.execute(
        "SELECT banca_usdt + realizado_usdt + aberto_usdt FROM carteira_v10 "
        "WHERE setup_id=? AND config_hash=? ORDER BY ts_ms",
        (spec.setup_id, spec.config_hash))] or [banca]
    dd_atual, dd_max = rebaixamento(serie, banca0)
    expo = c["nocional_usdt"] / banca if banca > 0 else 0.0
    margem = c["nocional_usdt"] / float(alav) if alav else None
    L += [_secao("CARTEIRA"),
          f"Banca: {_br(banca, 2, False)} USDT ({_br((banca - banca0) / banca0 * 100, 1)}%)",
          f"Realizado acumulado: {_br(c['realizado_usdt'])} USDT",
          f"Stops acumulados: {_br(sum(_liq(r, custo) for r in stops) * u)} USDT "
          f"({len(stops)} stops)",
          f"Exposição {_br(expo, 1, False)}×"
          + ("" if margem is None else f" · margem {_br(margem, 0, False)} USDT"),
          f"Rebaixamento: atual {_br(dd_atual, 1, False)}% · "
          f"máximo {_br(dd_max, 1, False)}%"]

    # SAÚDE
    n = len(spec.symbols)
    coleta = "n/d" if falhas is None else f"{n - len(falhas)}/{n}"
    liqs = sum(1 for r in fechados if str(r["status"]).lower() == "liquidacao")
    L += [_secao("SAÚDE"),
          f"Coleta {coleta} · guarda {'n/d' if guarda is None else len(guarda)}"
          f" · liquidações {liqs}" + _fmt_venue(venue_alt)]
    return "\n".join(L)


def build_delta(conn, specs, desde_ms, ate_ms, coleta=None, venue=None,
                guarda=None) -> str:
    """Um `bloco_delta` por setup, com a janela no cabeçalho. NÃO levanta.

    `specs` default é :data:`v10.registro.ATIVOS`, não o REGISTRO inteiro: uma
    ficha desligada reportaria zeros a cada 4h e daria a impressão de estar
    sendo observada. Ela aparece no quadro diário, onde o aviso dela sai junto.

    `coleta` é ``{setup_id: [símbolos que falharam]}``, montado por
    :func:`v10.runner.main` a partir do que `rodar_todos` devolveu. `None`
    (delta rodado sozinho) não imprime nada — ausência de informação não é
    afirmação de que a coleta foi limpa. `venue` é o par disso para a corretora
    que serviu (``{setup_id: ["SYMBOL@venue", ...]}``), com a mesma leitura da
    ausência: nada impresso não quer dizer "tudo OKX". `guarda` (mesma forma)
    traz os símbolos barrados pela guarda de :func:`v10.runner._abrir`.

    Ficha com ``exit_params["banca_usdt"]`` sai no formato por SEÇÕES
    (:func:`bloco_carteira`); as demais seguem no de uma linha por evento.
    """
    if specs is None:
        from v10.registro import ATIVOS
        specs = ATIVOS
    if isinstance(specs, dict):
        specs = list(specs.values())
    L = [f"⚡ <b>[V10Δ]</b> {_data_janela(desde_ms, ate_ms)}"]
    for spec in specs:
        sid = getattr(spec, "setup_id", "?")
        try:
            if "banca_usdt" in (getattr(spec, "exit_params", None) or {}):
                L.append(bloco_carteira(conn, spec, desde_ms, ate_ms,
                                        falhas=(coleta or {}).get(sid),
                                        venue_alt=(venue or {}).get(sid),
                                        guarda=(guarda or {}).get(sid)))
                continue
            L.append(bloco_delta(conn, spec, desde_ms, ate_ms,
                                 falhas=(coleta or {}).get(sid),
                                 venue_alt=(venue or {}).get(sid)))
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


def fatiar(msg: str, limite: int = LIMITE_TELEGRAM) -> list[str]:
    """Fatia `msg` em partes numeradas que cabem em `limite` caracteres.

    Fatiar, NÃO truncar. Concatenar os corpos das partes (tudo depois do
    cabeçalho ``(i/n)\\n``) reconstrói `msg` caractere por caractere — a
    mensagem grande é justamente a que mais importa, e um `msg[:4096]` cortaria
    exatamente a barra de virada de mercado.

    O corte prefere fronteira de linha (uma linha do delta é uma unidade de
    leitura, e o HTML das linhas fica fechado dentro delas). Linha maior que o
    limite é cortada no meio — perder um `<b>` é melhor que perder a linha.

    Mensagem que já cabe volta como lista de UM elemento, sem cabeçalho: o caso
    normal continua chegando exatamente como antes deste PR.
    """
    if len(msg) <= limite:
        return [msg]
    util = max(int(limite) - _RESERVA_CABECALHO, 1)
    # Mantém o "\n" colado na linha que o precede: a concatenação das partes
    # tem de devolver a string original, e um separador descartado no split
    # seria um caractere perdido.
    linhas = msg.splitlines(keepends=True)
    corpos, atual = [], ""
    for linha in linhas:
        while len(linha) > util:                  # linha sozinha maior que a parte
            if atual:
                corpos.append(atual)
                atual = ""
            corpos.append(linha[:util])
            linha = linha[util:]
        if len(atual) + len(linha) > util:
            corpos.append(atual)
            atual = ""
        atual += linha
    if atual:
        corpos.append(atual)
    total = len(corpos)
    return [f"({i}/{total})\n{c}" for i, c in enumerate(corpos, 1)]


def _enviar(msg: str, send_fn, log) -> bool:
    """Envia e devolve se foi. Falha é SILENCIOSA: warning e segue. O banco é a
    fonte de verdade — mensagem é leitura dele, não o registro.

    Mensagem acima de :data:`LIMITE_TELEGRAM` vai em partes numeradas. Só
    devolve `True` se TODAS as partes forem entregues: uma parte perdida é um
    delta incompleto, e a marca não pode avançar sobre ele.
    """
    envia = send_fn or _default_send
    partes = fatiar(msg)
    if len(partes) > 1:
        log.info(f"[v10] mensagem de {len(msg)} chars enviada em "
                 f"{len(partes)} partes (limite {LIMITE_TELEGRAM})")
    tudo = True
    for i, parte in enumerate(partes, 1):
        try:
            ok = bool(envia(parte))
        except Exception as e:
            log.warning(f"[v10] falha no envio da parte {i}/{len(partes)}: "
                        f"{type(e).__name__}: {e}")
            ok = False
        if not ok:
            log.warning(f"[v10] parte {i}/{len(partes)} nao entregue")
        tudo = tudo and ok
    return tudo


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
              db_path=None, log=None, coleta=None, venue=None, guarda=None,
              sem_envio=False):
    """Delta da janela e avanço da marca. -> `(mensagem, enviado)`.

    A marca só avança se o envio confirmar. Envio que falha NÃO perde evento: a
    janela seguinte começa onde esta começou e reporta os dois trechos juntos.

    `coleta` (``{setup_id: [símbolos que falharam]}``) vem da MESMA invocação
    que rodou os setups — é por isso que `main` chama as duas coisas juntas. Um
    delta rodado sozinho passa `None` e não afirma nada sobre a coleta. `venue`
    (mesma forma) traz os símbolos servidos fora da corretora primária, e
    `guarda` os barrados pela guarda.

    `sem_envio`: a mensagem vai para o LOG, nada é enviado e a marca NÃO
    avança — é a leitura prévia antes de ligar o Telegram.
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
        msg = build_delta(conn, specs, desde_ms, ate_ms, coleta=coleta,
                          venue=venue, guarda=guarda)
        if sem_envio:
            log.info("[v10] --sem-envio: delta NAO enviado, marca parada\n" + msg)
            return msg, False
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
