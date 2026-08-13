# backtest/keepitsimple.py — Setup Atirador v9 (BANCADA) — detector keepitsimple
# Estado por par de EMAs (8/21), plugado no soquete do stage1 com a MESMA
# convencao dos irmaos (tsmom/donchian): avalia na barra FECHADA i, emite o
# evento com bar_ts=tss[i], espacamento por direcao em TEMPO, direcao "LONG"/
# "SHORT". Zero parametro configuravel: 8/21/13/21x1.3 sao pre-registro.
# stdlib puro (sem pandas/pandas_ta) -> coleta e roda no sandbox.
from __future__ import annotations

EMA_FAST, EMA_SLOW = 8, 21
WARMUP = EMA_SLOW              # barras descartadas por simbolo (EMA longa/BB)
NAT_CAP = 48                   # cap do primario nativo, em barras do tf
ADX_LEN = 13
SMA_FAST, SMA_SLOW = 5, 13
BB_LEN, BB_MULT = 21, 1.3


def _ema(vals: list, n: int) -> list:
    """EMA canonica (alpha=2/(n+1)) semeada pela SMA das n primeiras barras.
    out[i] is None enquanto i < n-1 (semente ainda nao formada)."""
    out = [None] * len(vals)
    if len(vals) < n:
        return out
    a = 2.0 / (n + 1.0)
    e = out[n - 1] = sum(vals[:n]) / n
    for i in range(n, len(vals)):
        e = out[i] = a * vals[i] + (1.0 - a) * e
    return out


def _sma(vals: list, n: int) -> list:
    """SMA simples; out[i] is None enquanto i < n-1."""
    return [None if i < n - 1 else sum(vals[i - n + 1:i + 1]) / n
            for i in range(len(vals))]


def _adx(candles: list, n: int = ADX_LEN) -> list:
    """ADX de Wilder: suavizacao de Wilder em TR/+DM/-DM, DX = 100*|+DI - -DI|/
    (+DI + -DI) e ADX = media de Wilder de DX. None ate i = 2n-1."""
    out = [None] * len(candles)
    tr, pdm, ndm = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        tr.append(max(c["high"] - c["low"], abs(c["high"] - p["close"]),
                      abs(c["low"] - p["close"])))
        up, dn = c["high"] - p["high"], p["low"] - c["low"]
        pdm.append(up if up > dn and up > 0 else 0.0)
        ndm.append(dn if dn > up and dn > 0 else 0.0)
    if len(tr) < 2 * n - 1:
        return out
    str_, spdm, sndm, dx = sum(tr[:n]), sum(pdm[:n]), sum(ndm[:n]), []
    for k in range(n - 1, len(tr)):
        if k >= n:                       # Wilder: S += x - S/n
            str_ += tr[k] - str_ / n
            spdm += pdm[k] - spdm / n
            sndm += ndm[k] - sndm / n
        pdi = 100.0 * spdm / str_ if str_ > 0 else 0.0
        ndi = 100.0 * sndm / str_ if str_ > 0 else 0.0
        dx.append(100.0 * abs(pdi - ndi) / (pdi + ndi) if pdi + ndi else 0.0)
    adx = out[2 * n - 1] = sum(dx[:n]) / n   # dx[m] <-> candle n+m (offset TR)
    for k in range(n, len(dx)):
        adx = out[n + k] = (adx * (n - 1) + dx[k]) / n
    return out


def _descritivos(candles: list, closes: list) -> tuple:
    """Payload de autopsia por barra (None enquanto o lookback nao fecha):
    forca = ADX(13) + 100*|SMA5-SMA13|/SMA13; bb_width_rel = (bbSup-bbInf)/
    mediaBB, com BB 21 x 1.3 sobre EMA(21) e desvio POPULACIONAL das 21 closes.
    Nenhum dos dois entra em decisao — sao descritivos."""
    adx, f, s = _adx(candles), _sma(closes, SMA_FAST), _sma(closes, SMA_SLOW)
    forca = [None if (adx[i] is None or f[i] is None or not s[i])
             else adx[i] + 100.0 * abs(f[i] - s[i]) / s[i]
             for i in range(len(closes))]
    mid, mu = _ema(closes, BB_LEN), _sma(closes, BB_LEN)
    bbw = [None] * len(closes)
    for i in range(BB_LEN - 1, len(closes)):
        if mid[i]:
            sd = (sum((x - mu[i]) ** 2
                      for x in closes[i - BB_LEN + 1:i + 1]) / BB_LEN) ** 0.5
            bbw[i] = 2.0 * BB_MULT * sd / mid[i]
    return forca, bbw


def states(closes: list) -> list:
    """Estado por barra a partir de EMA(8)=curta e EMA(21)=longa:
    VERDE close>curta e curta>longa | AZUL close<curta e curta>longa |
    ROXO close>curta e curta<longa  | VERM close<curta e curta<longa |
    CINZA caso contrario (empate exato de float, ou warmup da EMA)."""
    fast, slow = _ema(closes, EMA_FAST), _ema(closes, EMA_SLOW)
    out = []
    for i, c in enumerate(closes):
        f, s = fast[i], slow[i]
        if f is None or s is None or c == f or f == s:
            out.append("CINZA")
        elif c > f:
            out.append("VERDE" if f > s else "ROXO")
        else:
            out.append("AZUL" if f > s else "VERM")
    return out


def keepitsimple_entries(candles: list, sep_bars: int, bar_ms: int) -> list:
    """Entradas keepitsimple (pre-registro desta PR): na barra FECHADA i, LONG
    se estado[i]=='VERDE' e estado[i-1]!='VERDE'; SHORT se estado[i]=='VERM' e
    estado[i-1]!='VERM'. As WARMUP primeiras barras do simbolo nunca geram
    entrada. Espacamento por direcao em TEMPO, na mesma forma dos irmaos
    (`last` persiste entre clusters); com sep_bars=0 — como o stage1 chama, por
    KIS_SEP — a guarda vira no-op e nada e suprimido. Cada entrada carrega
    `hold` = barras ate o estado DEIXAR o de origem (cap/fallback NAT_CAP), que
    e a saida do primario nativo, e o payload descritivo de autopsia. Saida
    indexada sempre por bar_ts."""
    closes = [c["close"] for c in candles]
    tss = [c["ts"] for c in candles]
    st = states(closes)
    forca, bbw = _descritivos(candles, closes)
    last = {"LONG": None, "SHORT": None}
    out = []
    for i in range(WARMUP, len(closes)):
        if st[i] == "VERDE" and st[i - 1] != "VERDE":
            d = "LONG"
        elif st[i] == "VERM" and st[i - 1] != "VERM":
            d = "SHORT"
        else:
            continue
        ts = tss[i]
        if last[d] is not None and ts - last[d] < sep_bars * bar_ms:
            continue
        last[d] = ts
        hold = NAT_CAP
        for j in range(i + 1, min(i + NAT_CAP, len(st) - 1) + 1):
            if st[j] != st[i]:
                hold = j - i
                break
        out.append({"bar_ts": ts, "direction": d, "hold": hold,
                    "estado_origem": st[i - 1], "bb_width_rel": bbw[i],
                    "forca_subindo": (None if forca[i] is None
                                      or forca[i - 1] is None
                                      else forca[i] > forca[i - 1])})
    return out
