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
    """
    # Com `ate`, parte da janela vai ser descartada — pede folga para ainda
    # sobrar `n`. Teto de 300 é o limite prático de limit das duas exchanges.
    limite = min(300, max(int(n), 1) * 2) if ate is not None else max(int(n), 1)

    candles = None
    motivo = "as duas corretoras falharam"
    for tentativa in range(1, max(int(TENTATIVAS), 1) + 1):
        try:
            candles, venue = _coletar(symbol, tf, limite)
        except Exception as e:                     # exceção crua da rede
            candles, venue, motivo = None, None, f"{type(e).__name__}: {e}"
        if venue is not None:
            break
        if tentativa < max(int(TENTATIVAS), 1):
            time.sleep(ESPERA_BASE_S * 2 ** (tentativa - 1))
    else:
        raise FalhaDeColeta(f"{symbol} {tf}: sem coleta apos "
                            f"{max(int(TENTATIVAS), 1)} tentativa(s) — {motivo}")

    candles = list(candles or [])
    if ate is not None:
        candles = [c for c in candles if int(c["ts"]) <= int(ate)]
    candles.sort(key=lambda c: int(c["ts"]))
    saida = Velas(candles[-int(n):] if n and len(candles) > int(n) else candles)
    saida.venue = venue
    return saida
