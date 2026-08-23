"""v10/data.py — a única porta de entrada de velas do v10.

Uma função: :func:`velas`. O detector NUNCA chama a exchange direto — se
chamasse, trocar API por store viraria uma edição em cada setup, e um setup
mediria com régua diferente do outro sem ninguém notar.

Hoje existe UMA implementação: a que lê da API (`exchanges.fetch_klines_async`,
OKX -> Bitget, mesmo caminho da produção). A versão de store NÃO está aqui de
propósito: ela entra no dia em que houver setup que precise de histórico mais
fundo que a janela da API — e nesse dia a assinatura já é esta.
"""

from __future__ import annotations

__all__ = ["velas"]


def velas(symbol: str, tf: str, n: int, ate=None) -> list[dict]:
    """`n` velas de `symbol` no timeframe `tf`, crescentes (antiga -> recente).

    Parâmetros:
        symbol: par no formato da produção (ex.: ``"BTCUSDT"``).
        tf: barra OKX/Bitget (``"1m"``, ``"5m"``, ``"15m"``, ``"1H"``, ``"4H"``).
        n: quantas velas devolver — as `n` MAIS RECENTES da janela.
        ate: epoch-ms opcional; descarta velas com ``ts > ate``.

    Retorno:
        ``[{"ts": int_ms, "open", "high", "low", "close", "volume"}, ...]``
        em ordem crescente. Lista VAZIA se as duas exchanges falharem — falha
        de rede não levanta (CLAUDE.md §5, camada de rede).

    LIMITE CONHECIDO do `ate`: a API só devolve a janela recente, então `ate`
    filtra DENTRO dela. Pedir um `ate` mais antigo que a janela devolve menos
    de `n` velas (ou nenhuma) — não devolve dado errado, devolve menos. Passado
    fundo é trabalho da versão de store, que ainda não existe.
    """
    import asyncio

    import aiohttp                                   # lazy: pesado, e o sandbox
    from exchanges import fetch_klines_async         # não precisa para testar

    # Com `ate`, parte da janela vai ser descartada — pede folga para ainda
    # sobrar `n`. Teto de 300 é o limite prático de limit das duas exchanges.
    limite = min(300, max(int(n), 1) * 2) if ate is not None else max(int(n), 1)

    async def _buscar():
        async with aiohttp.ClientSession() as session:
            return await fetch_klines_async(session, symbol, granularity=tf,
                                            limit=limite)

    try:
        candles, _venue = asyncio.run(_buscar())
    except Exception:
        return []

    if ate is not None:
        candles = [c for c in candles if int(c["ts"]) <= int(ate)]
    candles.sort(key=lambda c: int(c["ts"]))
    return candles[-int(n):] if n and len(candles) > int(n) else candles
