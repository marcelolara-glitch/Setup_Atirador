"""v10/data.py — a única porta de entrada de velas do v10.

Uma função: :func:`velas`. O detector NUNCA chama a exchange direto — se
chamasse, trocar API por store viraria uma edição em cada setup, e um setup
mediria com régua diferente do outro sem ninguém notar.

Hoje existe UMA implementação: a que lê da API (`exchanges.fetch_klines_async`,
OKX -> Bitget, mesmo caminho da produção). A versão de store NÃO está aqui de
propósito: ela entra no dia em que houver setup que precise de histórico mais
fundo que a janela da API — e nesse dia a assinatura já é esta.

FALHA NÃO É VAZIO. `fetch_klines_async` engole as próprias exceções e devolve
``([], None)`` quando OKX e Bitget falham — o `venue` é o discriminador: ``None``
significa que as DUAS corretoras caíram, e não que a corretora respondeu sem
velas. Coleta que não resolve levanta :class:`FalhaDeColeta` com o símbolo no
texto; nunca devolve ``[]``. Devolver lista vazia faria o chamador ler
"nenhum sinal" onde o certo é "não sei" — e com 65 símbolos em série quem cai
são preferencialmente os do fim da lista, sempre os mesmos, o que enviesaria a
série inteira em silêncio.

VENUE SOBE JUNTO. Qual corretora serviu não é detalhe de transporte: liquidez,
taxa e fecho de barra do fallback não são os da primária, e um símbolo servido
por ela leu uma régua diferente do resto da série. Por isso :func:`velas`
devolve :class:`Velas` — lista, com o `venue` pendurado — e o runner o carrega
até o rodapé do delta.
"""

from __future__ import annotations

import time

__all__ = ["FalhaDeColeta", "VENUE_PRIMARIA", "Velas", "velas"]

# Nome da corretora primária de `exchanges.fetch_klines_async` (OKX -> Bitget).
# É só um NOME para comparar: a ordem do fallback é de lá, não daqui.
VENUE_PRIMARIA = "okx"


class Velas(list):
    """As velas MAIS a corretora que as serviu, em :attr:`venue`.

    Subclasse de `list` de propósito: quem só quer as velas continua lendo uma
    lista e nenhum chamador precisou mudar para o `venue` passar a existir.
    Quem quer saber lê ``getattr(x, "venue", None)`` — fonte que devolva lista
    crua (um `velas_fn` de teste, a versão de store) dá ``None``, que significa
    SEM INFORMAÇÃO e nunca "veio da primária".
    """

    venue = None


# Tentativas por símbolo e espera entre elas (dobra a cada rodada: 1s, 2s).
# Constantes de módulo, não parâmetros: a assinatura de `velas` é o contrato que
# a versão de store vai ter de respeitar, e política de retry não é dado do
# pedido. Teste ajusta o módulo, não a chamada.
TENTATIVAS = 3
ESPERA_BASE_S = 1.0

# Teto de `limit` do endpoint recente (OKX /market/candles e Bitget). Pedido
# maior pagina para trás no histórico OKX, :data:`PAGINA_HIST` por chamada (cap
# do /history-candles, o mesmo de `backtest/backfill_okx.py`).
TETO_RECENTE = 300
OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
PAGINA_HIST = 100


class FalhaDeColeta(RuntimeError):
    """As tentativas acabaram sem resposta das corretoras para um símbolo."""


def _coletar(symbol: str, tf: str, limite: int) -> tuple[list[dict], str | None]:
    """UMA tentativa de rede. -> ``(candles, venue)``; `venue` ``None`` = caiu.

    Separada de :func:`velas` para ser a costura do teste: quem simula falha
    troca esta função e não precisa de rede, de `aiohttp` nem de `exchanges`.
    Imports pesados seguem lazy (o sandbox não os tem).
    """
    import asyncio

    import aiohttp
    from exchanges import fetch_klines_async

    async def _buscar():
        async with aiohttp.ClientSession() as session:
            return await fetch_klines_async(session, symbol, granularity=tf,
                                            limit=limite)

    return asyncio.run(_buscar())


def _com_retry(fn, symbol: str, tf: str):
    """Roda `fn` (uma tentativa de rede -> ``(candles, venue)``) até ter venue.
    Esgotou :data:`TENTATIVAS`: :class:`FalhaDeColeta` — nunca ``[]``."""
    motivo = "as duas corretoras falharam"
    for tentativa in range(1, max(int(TENTATIVAS), 1) + 1):
        try:
            candles, venue = fn()
        except Exception as e:                     # exceção crua da rede
            candles, venue, motivo = None, None, f"{type(e).__name__}: {e}"
        if venue is not None:
            return candles, venue
        if tentativa < max(int(TENTATIVAS), 1):
            time.sleep(ESPERA_BASE_S * 2 ** (tentativa - 1))
    raise FalhaDeColeta(f"{symbol} {tf}: sem coleta apos "
                        f"{max(int(TENTATIVAS), 1)} tentativa(s) — {motivo}")


def _coletar_antes(symbol: str, tf: str, after_ms: int) -> tuple[list[dict], str | None]:
    """UMA página do histórico OKX com ``ts < after_ms``. -> ``(crescente, venue)``;
    `venue` ``None`` = caiu; lista vazia com venue = o histórico acabou.
    Costura de teste, como :func:`_coletar`."""
    import asyncio

    import aiohttp
    from exchanges import api_get_async

    inst = f"{symbol.replace('USDT', '')}-USDT-SWAP"
    url = (f"{OKX_HISTORY_URL}?instId={inst}&bar={tf}&limit={PAGINA_HIST}"
           f"&after={int(after_ms)}")

    async def _buscar():
        async with aiohttp.ClientSession() as session:
            return await api_get_async(session, url)

    data = asyncio.run(_buscar())
    if not data or "data" not in data:
        return [], None
    out = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
            "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
           for c in data["data"] if len(c) <= 8 or c[8] != "0"]
    out.reverse()                                  # OKX: decrescente
    return out, VENUE_PRIMARIA


def _paginar(symbol: str, tf: str, n: int, ate, candles: list) -> list:
    """Estende `candles` para trás até sobrarem `n` com ``ts <= ate``. Dedup por
    ts; para quando o histórico acaba ou o cursor não anda (nunca laço eterno)."""
    por_ts = {int(c["ts"]): c for c in candles}

    def _uteis():
        return sum(1 for t in por_ts if ate is None or t <= int(ate))

    for _ in range(n // max(PAGINA_HIST, 1) + 10):
        if _uteis() >= n:
            break
        cursor = min(por_ts)
        pagina, _v = _com_retry(lambda: _coletar_antes(symbol, tf, cursor),
                                symbol, tf)
        novos = [c for c in (pagina or []) if int(c["ts"]) < cursor]
        if not novos:
            break
        por_ts.update((int(c["ts"]), c) for c in novos)
    return [por_ts[t] for t in sorted(por_ts)]


def velas(symbol: str, tf: str, n: int, ate=None) -> list[dict]:
    """`n` velas de `symbol` no timeframe `tf`, crescentes (antiga -> recente).

    Parâmetros:
        symbol: par no formato da produção (ex.: ``"BTCUSDT"``).
        tf: barra OKX/Bitget (``"1m"``, ``"5m"``, ``"15m"``, ``"1H"``, ``"4H"``).
        n: quantas velas devolver — as `n` MAIS RECENTES da janela.
        ate: epoch-ms opcional; descarta velas com ``ts > ate``.

    Retorno:
        :class:`Velas` (uma `list`) de
        ``{"ts": int_ms, "open", "high", "low", "close", "volume"}`` em ordem
        crescente, com o `venue` que respondeu no atributo homônimo.

    Levanta:
        :class:`FalhaDeColeta` se as :data:`TENTATIVAS` se esgotarem sem
        corretora. Lista vazia é resposta LEGÍTIMA da corretora (janela sem
        vela, ou `ate` anterior à janela) — nunca falha de rede.

    LIMITE CONHECIDO do `ate`: a API só devolve a janela recente, então `ate`
    filtra DENTRO dela. Pedir um `ate` mais antigo que a janela devolve menos
    de `n` velas (ou nenhuma) — não devolve dado errado, devolve menos. Passado
    fundo é trabalho da versão de store, que ainda não existe.

    JANELA LONGA (n > :data:`TETO_RECENTE`): a 1ª página sai do caminho de
    sempre (endpoint recente, com fallback); as anteriores, do histórico da OKX
    (:data:`OKX_HISTORY_URL`, :data:`PAGINA_HIST` por página, cursor `after`),
    o mesmo padrão de `backtest/backfill_okx.py`. Só pagina se a 1ª página veio
    da PRIMÁRIA: emendar Bitget com OKX daria uma série de duas réguas, então
    nesse caso levanta :class:`FalhaDeColeta` em vez de devolver menos.
    """
    # Com `ate`, parte da janela vai ser descartada — pede folga para ainda
    # sobrar `n`. Teto de 300 é o limite prático de limit das duas exchanges.
    n = max(int(n), 1)
    limite = min(TETO_RECENTE, n * 2) if ate is not None else min(TETO_RECENTE, n)
    candles, venue = _com_retry(lambda: _coletar(symbol, tf, limite), symbol, tf)
    candles = list(candles or [])
    if n > TETO_RECENTE and candles:
        if venue != VENUE_PRIMARIA:
            raise FalhaDeColeta(f"{symbol} {tf}: janela de {n} exige historico "
                                f"da primaria, 1a pagina veio de {venue}")
        candles = _paginar(symbol, tf, n, ate, candles)
    if ate is not None:
        candles = [c for c in candles if int(c["ts"]) <= int(ate)]
    candles.sort(key=lambda c: int(c["ts"]))
    saida = Velas(candles[-int(n):] if n and len(candles) > int(n) else candles)
    saida.venue = venue
    return saida
