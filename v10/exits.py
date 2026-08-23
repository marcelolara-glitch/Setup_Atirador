"""v10/exits.py — adaptadores de saída. Interface única, pequena, dois campos.

    abrir(spec, sinal, vela)            -> {"exit_params": {...}, "exit_state": {...}}
    avaliar(spec, estado, velas_novas)  -> (novo_estado, status_final|None, preco|None)

`estado` é sempre o dict devolvido por `abrir` (params + state juntos), para
que `avaliar` não precise de nenhum outro contexto. `status_final=None`
significa "segue aberto"; nesse caso `preco` também é None.

O detector diz ONDE entrar; o adaptador diz QUANDO e POR QUANTO sair. Trocar
um não mexe no outro — é a razão de o schema não ter coluna de TP/SL.
"""

from __future__ import annotations

from typing import Any

from config import TRADE_TIMEOUT_HOURS
from risk import TradePlan, TradeState, update_trade_state

__all__ = ["ADAPTADORES", "BracketMulti", "BracketSimples", "Reverse",
           "adaptador"]

_MS_POR_HORA = 3_600_000.0


def _campo(obj: Any, nome: str, default: Any = None) -> Any:
    """Lê `nome` de dict OU de objeto. Duck typing na fronteira (CLAUDE.md §5)."""
    if isinstance(obj, dict):
        return obj.get(nome, default)
    return getattr(obj, nome, default)


def _ts(vela: Any) -> Any:
    """Timestamp de uma vela em ms, ou None se ausente. NUNCA 0 como sentinela:
    0 e um epoch legitimo, e um `if ts:` sobre ele pula silenciosamente a
    checagem de timeout — foi o que este helper existe para impedir."""
    bruto = _campo(vela, "ts", None)
    return None if bruto is None else int(bruto)


def _novas(velas: list, entry_ts_ms: Any) -> list:
    """Velas POSTERIORES a entrada, em ordem crescente — o filtro `ts > open_ts_ms`
    do v9. Sem entry_ts conhecido nao ha o que filtrar: passa tudo, ordenado."""
    if entry_ts_ms is not None:
        velas = [v for v in velas if (_ts(v) or 0) > int(entry_ts_ms)]
    return sorted(velas, key=lambda v: _ts(v) or 0)


def _plano(sinal: Any) -> Any:
    """Aceita um SignalDecision (usa .trade_plan) ou o próprio plano/dict."""
    return _campo(sinal, "trade_plan", None) or sinal


def _extremos(direction: str, entry: float, hi: float, lo: float) -> tuple:
    """runup/drawdown em % — a mesma conta de journal._check_one_trade passo 5."""
    if entry <= 0:
        return 0.0, 0.0
    if direction == "LONG":
        return (hi - entry) / entry * 100.0, (entry - lo) / entry * 100.0
    return (entry - lo) / entry * 100.0, (hi - entry) / entry * 100.0


def _acumular(s: dict, direction: str, entry: float, vela: Any) -> bool:
    """Soma os extremos de `vela` em `s`. False se a vela for malformada (pulada,
    como no v9). Existe para que a mesma acumulação sirva ao laço e à cauda de
    barras posteriores à inversão."""
    try:
        hi, lo = float(_campo(vela, "high")), float(_campo(vela, "low"))
    except (TypeError, ValueError):
        return False
    ru, dd = _extremos(direction, entry, hi, lo)
    s["max_runup"] = max(s["max_runup"], ru)
    s["max_drawdown"] = max(s["max_drawdown"], dd)
    return True


class BracketMulti:
    """Porta de journal._check_one_trade: TP1/TP2/TP3/SL/EXPIRED com parciais.

    A máquina de estado por vela NÃO é reimplementada — é `risk.update_trade_state`,
    a mesma função que o v9 chama. Reimplementá-la abriria espaço para divergir
    em detalhe (ela decrementa a posição com o POSITION_SPLIT do MÓDULO, não com
    o plan.position_split — risk.py:713). Aqui se porta só a orquestração:
    timeout antes das velas, filtro de velas novas, laço cronológico com break na
    primeira saída, acumulação de extremos INCLUINDO a vela da saída, e o mapa
    status -> preço de saída de journal._exit_price_for_status.
    """

    nome = "bracket_multi"

    @staticmethod
    def abrir(spec: Any, sinal: Any, vela: Any) -> dict:
        p = _plano(sinal)
        entry = float(_campo(p, "entry_price"))
        params = {
            "direction": _campo(p, "direction"),
            "entry_price": entry,
            "sl_price": float(_campo(p, "sl_price")),
            "tp1_price": float(_campo(p, "tp1_price")),
            "tp2_price": float(_campo(p, "tp2_price")),
            "tp3_price": float(_campo(p, "tp3_price")),
            "atr_value": float(_campo(p, "atr_value", 0.0) or 0.0),
            "position_split": list(_campo(p, "position_split", (0.5, 0.3, 0.2))),
            "leverage": float(_campo(p, "leverage", 1.0) or 1.0),
            "timeout_hours": float(
                (spec.exit_params or {}).get("timeout_hours", TRADE_TIMEOUT_HOURS)
            ),
            "entry_ts_ms": _ts(vela),
        }
        estado = {
            "current_sl": params["sl_price"],
            "tp1_hit": False, "tp2_hit": False, "tp3_hit": False,
            "position_remaining": 1.0, "status": "OPEN",
            "max_runup": 0.0, "max_drawdown": 0.0,
        }
        return {"exit_params": params, "exit_state": estado}

    @staticmethod
    def avaliar(spec: Any, estado: dict, velas_novas: list, agora_ms=None) -> tuple:
        p, s = estado["exit_params"], dict(estado["exit_state"])
        entry, direction = float(p["entry_price"]), p["direction"]
        novo = {"exit_params": p, "exit_state": s}
        if s["status"] != "OPEN":
            return novo, s["status"], None

        velas = _novas(velas_novas, p["entry_ts_ms"])

        # 1. Timeout ANTES das velas — mesma ordem do v9 (lá evita I/O
        #    desnecessário; aqui preserva o resultado: EXPIRED sai no entry,
        #    pnl 0, mesmo que uma vela da janela batesse TP ou SL).
        #    Sem `agora_ms` o relógio é a última vela: puro, sem wall-clock.
        #    Sem entry_ts não há relógio nenhum — não expira em vez de expirar
        #    tudo (um entry_ts ausente lido como 0 venceria todo trade na hora).
        ref = agora_ms if agora_ms is not None else (
            _ts(velas[-1]) if velas else None)
        if p["entry_ts_ms"] is not None and ref is not None:
            if (int(ref) - int(p["entry_ts_ms"])) / _MS_POR_HORA >= float(
                    p["timeout_hours"]):
                s["status"] = "EXPIRED"
                return novo, "EXPIRED", entry

        # 2. Laço cronológico. Extremos primeiro, estado depois, break na saída —
        #    a vela que fecha o trade ENTRA no runup/drawdown (decisão do v9).
        plan = TradePlan(
            direction=direction, entry_price=entry, sl_price=float(p["sl_price"]),
            sl_distance_pct=0.0, tp1_price=float(p["tp1_price"]),
            tp2_price=float(p["tp2_price"]), tp3_price=float(p["tp3_price"]),
            tp1_distance_pct=0.0, tp2_distance_pct=0.0, tp3_distance_pct=0.0,
            risk_reward_tp1=0.0, risk_reward_tp2=0.0, risk_reward_tp3=0.0,
            atr_value=float(p["atr_value"]), position_split=tuple(p["position_split"]),
            leverage=float(p["leverage"]),
        )
        st = TradeState(
            trade_plan=plan, current_sl=float(s["current_sl"]),
            tp1_hit=bool(s["tp1_hit"]), tp2_hit=bool(s["tp2_hit"]),
            tp3_hit=bool(s["tp3_hit"]),
            position_remaining=float(s["position_remaining"]), status=s["status"],
        )
        for v in velas:
            try:
                hi, lo = float(_campo(v, "high")), float(_campo(v, "low"))
            except (TypeError, ValueError):
                continue          # vela malformada é pulada, como no v9
            ru, dd = _extremos(direction, entry, hi, lo)
            s["max_runup"] = max(s["max_runup"], ru)
            s["max_drawdown"] = max(s["max_drawdown"], dd)
            st = update_trade_state(st, {"high": hi, "low": lo})
            if st.status != "OPEN":
                break

        s.update(current_sl=st.current_sl, tp1_hit=st.tp1_hit, tp2_hit=st.tp2_hit,
                 tp3_hit=st.tp3_hit, position_remaining=st.position_remaining,
                 status=st.status)
        if st.status == "OPEN":
            return novo, None, None
        return novo, st.status, BracketMulti.preco_saida(st.status, p, st.current_sl)

    @staticmethod
    def preco_saida(status: str, params: dict, current_sl: float) -> float:
        """Mapa de journal._exit_price_for_status. EXPIRED e desconhecido -> entry."""
        return {
            "WIN_TP3": float(params["tp3_price"]),
            "WIN_TP2": float(params["tp2_price"]),
            "WIN_TP1": float(params["tp1_price"]),
            "LOSS_SL": float(current_sl),
        }.get(status, float(params["entry_price"]))


class Reverse:
    """Sem stop, sem alvo. Fecha quando o alvo do detector INVERTE.

    "Inverte" é troca de lado: +1 -> -1 ou -1 -> +1. Alvo 0 (neutro) NÃO fecha —
    o detector deixou de opinar, não mudou de opinião. Cada vela carrega o alvo
    vigente na chave `alvo`; vela sem `alvo` não decide nada. O novo alvo fica
    registrado em `alvo_saida` para auditoria.

    PREÇO DE SAÍDA — a inversão pode ser detectada numa barra ANTERIOR à última
    processada (uma run perdida pelo cron faz a passada seguinte varrer várias
    barras de uma vez). Nesse caso existem DOIS preços, e os dois ficam
    gravados em ``exit_state``:

    - ``preco_ideal``: o ``close`` da barra em que o alvo inverteu. É o desenho.
    - ``preco_real``: o ``close`` da barra CORRENTE (a última processada). É o
      único preço em que ainda dá para vender — o outro já passou.

    Quem sai é o ``mode`` da ficha: ``backtest`` usa o ideal (lá o objeto de
    estudo é o desenho, e não há execução a simular); ``shadow`` e ``live``
    usam o real, porque sair num preço que já passou é olhar para trás — o
    número ficaria melhor do que a operação conseguiria ser.

    A diferença ``preco_real - preco_ideal`` é a MEDIDA do custo de uma run
    perdida, em ``barras_atraso`` barras. Sem gravar os dois esse custo não
    existe em lugar nenhum.
    """

    nome = "reverse"

    @staticmethod
    def abrir(spec: Any, sinal: Any, vela: Any) -> dict:
        direction = _campo(_plano(sinal), "direction")
        params = {
            "direction": direction,
            "entry_price": float(_campo(_plano(sinal), "entry_price")),
            "entry_ts_ms": _ts(vela),
        }
        estado = {
            "alvo": 1 if direction == "LONG" else -1,
            "alvo_saida": None, "status": "OPEN",
            "max_runup": 0.0, "max_drawdown": 0.0,
            "preco_ideal": None, "preco_real": None, "barras_atraso": 0,
            "exit_ts_ms": None,
        }
        return {"exit_params": params, "exit_state": estado}

    @staticmethod
    def avaliar(spec: Any, estado: dict, velas_novas: list, agora_ms=None) -> tuple:
        p, s = estado["exit_params"], dict(estado["exit_state"])
        entry, direction, alvo = float(p["entry_price"]), p["direction"], int(s["alvo"])
        novo = {"exit_params": p, "exit_state": s}
        if s["status"] != "OPEN":
            return novo, s["status"], None

        velas = _novas(velas_novas, p["entry_ts_ms"])
        backtest = str(_campo(spec, "mode", "shadow")) == "backtest"
        for i, v in enumerate(velas):
            if not _acumular(s, direction, entry, v):
                continue
            novo_alvo = _campo(v, "alvo", None)
            if novo_alvo is None:
                continue
            novo_alvo = int(novo_alvo)
            if novo_alvo == 0 or novo_alvo != -alvo:
                continue
            # Fora do backtest a posição SEGUE nas mãos até a barra corrente —
            # os extremos dessas barras aconteceram de verdade e entram. Omiti-
            # los daria um max_drawdown menor do que o que o trade sofreu.
            if not backtest:
                for w in velas[i + 1:]:
                    _acumular(s, direction, entry, w)
            return novo, "REVERTIDO", Reverse._fechar(
                s, novo_alvo, v, velas[-1], len(velas) - 1 - i, backtest)
        return novo, None, None

    @staticmethod
    def _fechar(s: dict, novo_alvo: int, vela_inv, vela_corrente, atraso: int,
                backtest: bool) -> float:
        """Grava ideal e real em `s` e devolve o preço em que este `mode` sai.

        `atraso` é quantas barras a inversão ficou para trás. Zero — o caso das
        runs em dia — faz ideal e real coincidirem, e a correção some do número:
        ela só aparece onde de fato houve buraco de cobertura.
        """
        ideal = float(_campo(vela_inv, "close"))
        real = float(_campo(vela_corrente, "close"))
        saida, vela_saida = (ideal, vela_inv) if backtest else (real, vela_corrente)
        s.update(status="REVERTIDO", alvo_saida=novo_alvo, preco_ideal=ideal,
                 preco_real=real, barras_atraso=int(atraso),
                 exit_ts_ms=_ts(vela_saida))
        return saida


class BracketSimples:
    """Stop e alvo fixos em ATR, com horizonte em barras. Sem parciais.

    Porta de ``shadow.donchian_a.resolve_bracket`` + ``_pnl`` — a regra de saida
    do DONCHIAN-A, que o v10 tem de reproduzir barra a barra. As tres decisoes
    que a definem, todas preservadas aqui:

    1. empate alvo+stop na MESMA vela -> STOP (pessimista);
    2. sem toque ate a barra de expiracao -> EXPIRED no ``close`` dessa barra;
    3. barra de expiracao ausente (buraco de dado) -> segue OPEN, nao expira.

    A unidade de risco e ``s_atr * atr`` — o mesmo denominador do shadow —, e o
    ``r_multiple`` fica em ``exit_state``, nao em coluna: o schema do v10 nao
    tem campo de modelo de saida.

    Le de ``spec.exit_params``: ``s_atr``, ``t_atr``, ``h_bars`` e ``bar_ms``.
    ``bar_ms`` vem declarado em vez de deduzido do ``tf`` para que a expiracao
    seja um parametro congelado da ficha, e nao um mapa escondido aqui dentro.
    """

    nome = "bracket_simples"

    @staticmethod
    def abrir(spec: Any, sinal: Any, vela: Any) -> dict:
        cfg = spec.exit_params or {}
        plano = _plano(sinal)
        direction = _campo(plano, "direction")
        entry = float(_campo(plano, "entry_price"))
        atr = float(_campo(plano, "atr_value", 0.0) or 0.0)
        s_atr, t_atr = float(cfg["s_atr"]), float(cfg["t_atr"])
        lado = 1.0 if direction == "LONG" else -1.0
        entry_ts = _ts(vela)
        params = {
            "direction": direction, "entry_price": entry, "atr_value": atr,
            "s_atr": s_atr, "t_atr": t_atr,
            "sl_price": entry - lado * s_atr * atr,
            "tp_price": entry + lado * t_atr * atr,
            "entry_ts_ms": entry_ts,
            "expiry_ts_ms": None if entry_ts is None else (
                entry_ts + int(cfg["h_bars"]) * int(cfg["bar_ms"])),
        }
        estado = {"status": "OPEN", "max_runup": 0.0, "max_drawdown": 0.0,
                  "r_multiple": None, "exit_ts_ms": None}
        return {"exit_params": params, "exit_state": estado}

    @staticmethod
    def avaliar(spec: Any, estado: dict, velas_novas: list, agora_ms=None) -> tuple:
        p, s = estado["exit_params"], dict(estado["exit_state"])
        novo = {"exit_params": p, "exit_state": s}
        if s["status"] != "OPEN":
            return novo, s["status"], None
        entry, direction = float(p["entry_price"]), p["direction"]
        sl, tp = float(p["sl_price"]), float(p["tp_price"])
        expiry = None if p["expiry_ts_ms"] is None else int(p["expiry_ts_ms"])
        e_long = direction == "LONG"
        for v in _novas(velas_novas, p["entry_ts_ms"]):
            ts = _ts(v)
            if expiry is not None and ts is not None and ts > expiry:
                break                     # janela do horizonte acabou
            try:
                hi, lo = float(_campo(v, "high")), float(_campo(v, "low"))
            except (TypeError, ValueError):
                continue                  # vela malformada e pulada, como no v9
            ru, dd = _extremos(direction, entry, hi, lo)
            s["max_runup"] = max(s["max_runup"], ru)
            s["max_drawdown"] = max(s["max_drawdown"], dd)
            se_stop = lo <= sl if e_long else hi >= sl
            se_alvo = hi >= tp if e_long else lo <= tp
            if se_stop:                   # cobre o empate alvo+stop: pessimista
                fim = ("LOSS", sl)
            elif se_alvo:
                fim = ("WIN", tp)
            elif expiry is not None and ts == expiry:
                fim = ("EXPIRED", float(_campo(v, "close")))
            else:
                continue
            s.update(status=fim[0], exit_ts_ms=ts,
                     r_multiple=BracketSimples.r_multiple(p, fim[1]))
            return novo, fim[0], fim[1]
        return novo, None, None

    @staticmethod
    def r_multiple(params: dict, exit_price: float) -> float:
        """Retorno em unidades de risco. Denominador = ``s_atr * atr`` (shadow
        ``_pnl``); risco nao-positivo devolve 0.0 em vez de dividir por zero."""
        risco = float(params["s_atr"]) * float(params["atr_value"])
        entry = float(params["entry_price"])
        move = ((exit_price - entry) if params["direction"] == "LONG"
                else (entry - exit_price))
        return move / risco if risco > 0 else 0.0


ADAPTADORES = {a.nome: a for a in (BracketMulti, Reverse, BracketSimples)}


def adaptador(exit_model: str):
    """Resolve o adaptador pelo nome. KeyError explícito em modelo desconhecido."""
    if exit_model not in ADAPTADORES:
        raise KeyError(
            f"exit_model desconhecido: {exit_model!r} "
            f"(conhecidos: {sorted(ADAPTADORES)})"
        )
    return ADAPTADORES[exit_model]
