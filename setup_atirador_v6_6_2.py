#!/usr/bin/env python3
"""
=============================================================================
SETUP ATIRADOR v6.6.2 - Scanner Profissional de Criptomoedas
=============================================================================
Arquitetura Multi-Timeframe com 3 Camadas Independentes:

  CAMADA 1 — 4H "Qual é a direção do mercado?" (contexto macro)
  CAMADA 2 — 1H "Estamos num bom ponto de entrada?" (estrutura)
  CAMADA 3 — 15m "O timing de entrada está correto agora?" (gatilho)

Score máximo: 28 pts (LONG e SHORT, incluindo P9 OI +2)
Thresholds adaptativos: Favorável ≥14 | Moderado ≥16 | Cauteloso ≥20 | Bot OFF = 99

=============================================================================
HISTÓRICO DE VERSÕES
=============================================================================

v6.6.2 (24/03/2026):
  - Arquitetura de comunicação Telegram redesenhada em 3 tipos de mensagem:
    1. Heartbeat (toda rodada): contexto de mercado + pipeline completo
       (universo→gates→análise) + radar compacto com setas direcionais +
       veredicto simples (AGUARDAR / aviso de calls disparadas).
    2. QUASE (separado, 1 msg por token): disparado quando score está a
       menos de 5 pts do threshold. Mostra todos os pilares com pts obtidos
       e pts máximos, e lista explicitamente as fontes de pts disponíveis.
       Múltiplos tokens QUASE = múltiplas mensagens independentes.
    3. Call (separado, 1 msg por token): score ≥ threshold. Mensagem
       operacional completa: entrada, SL, TPs, alavancagem, margem, risco,
       e breakdown completo de todos os pilares. Heartbeat apenas indica
       quantas calls foram disparadas, sem repetir parâmetros.
  - _tg_relatorio_rodada(): reescrita para heartbeat limpo (sem breakdown).
  - _tg_quase(): nova função — mensagem QUASE por token.
  - _tg_call_long() / _tg_call_short(): adicionado breakdown de pilares.
  - tg_notify(): orquestra os 3 tipos na ordem correta.
  - QUASE_MARGEM = 4 (threshold − 4 = gatilho de alerta QUASE).

v6.6.2 (23/03/2026):
  - Heartbeat Telegram reformulado como relatório decisivo de rodada.
    Novo formato único e autocontido — elimina necessidade de abrir arquivos
    paralelos. Estrutura: Contexto → Veredicto go/no-go → Breakdown do top
    LONG e SHORT (todos os pilares, o que pontuou e o que falta) → Radar
    compacto. Mantém limite de 1 mensagem Telegram (≤4096 chars).
  - _tg_relatorio_rodada() reescrita: usa breakdown real dos tokens top
    (pilares do calculate_score) para exibir cada pilar com pts obtidos,
    pts máximos e descrição. A linha "→ Falta:" lista explicitamente quais
    pilares precisam pontuar para a próxima call.
  - Veredicto adaptativo: CALL ATIVA / QUASE (faltam ≤3pts) / MONITORAR
    (faltam 4-6) / AGUARDAR (faltam 7+) / BOT OFF, com mensagem contextual
    diferente para cada estado.
  - Radar compacto (linha única por direção) preservado no final para
    visão do universo sem consumir chars em excesso.

v6.6.2 (23/03/2026):
  - FIX CRÍTICO — Funding Rate zerado: endpoint OKX /market/tickers não inclui
    fundingRate. Adicionado fetch dedicado de FR via /api/v5/public/funding-rate
    em batch assíncrono. P3 agora recebe dados reais em todas as rodadas.
  - FIX CRÍTICO — Candles 15m falha persistente: adicionado diagnóstico explícito
    das colunas que causam TypeError no TradingView batch. Se o request de candles
    falha 3x, o script tenta requests individuais por coluna para isolar o
    nome inválido e logar qual coluna está quebrando a API.
  - FIX — Bollinger SHORT: pos<0 (abaixo da banda inferior) não é mais descartado
    para operações SHORT. Preço abaixo da BB_lower em contexto bearish recebe
    pontuação BB SHORT (momentum descendente confirmado pela banda).
  - FIX — Aviso de margem filtrado: LOG.warning de margem excedida só é emitido
    quando o score do token está acima do threshold da rodada. Abaixo do
    threshold, o cálculo é feito mas o aviso não polui o log.
  - Versão bumpeada em todas as referências internas.

v6.5.0 (23/03/2026):
  - Telegram persistente: config em ~/.atirador_telegram_config.json, sobrevive
    a reinícios do ambiente. Migração automática de /tmp/ para ~/.
  - Margem como aviso: trades nunca descartados por margem. Aviso explícito
    no log e Telegram quando margem excede MARGEM_MAX_POR_TRADE.
  - Threshold SHORT assimétrico corrigido: FGI≤20 + BTC SELL → thr=14 (FAVORÁVEL),
    FGI≤20 + BTC NEUTRAL → thr=16 (MODERADO), FGI≤20 + BTC BUY → thr=20 (CAUTELOSO).
  - Diagnóstico P2 candles: distingue "sem padrão" de "dado ausente da API".

v6.4.1 (23/03/2026):
  - FIX crítico: BB, ATR e candles zerados para todos os tokens. Causa: colunas
    de candle bearish inválidas contaminavam todo o request TV 15m. Solução:
    COLS_15M dividido em dois requests independentes (COLS_15M_TECH + COLS_15M_CANDLES).
  - FIX OKX OI: campo openInterest inexistente substituído por endpoint dedicado
    /public/open-interest com campo oiUsd real. 0% de OI estimado (era 100%).

v6.4.0 (23/03/2026):
  - A1: Sizing risk-first — fórmula reescrita. Notional = RISCO/stop_pct.
    Margem limitada a MARGEM_MAX_POR_TRADE=$35 (banca $100, 2 trades = $70 máx).
  - A2: Tabela de alavancagem recalibrada sobre teto real de 28 pts.
    Scores 14–28 cobertos (era só 20–26). ALAVANCAGEM_MIN = 2x.
  - A9: data_quality separado do setup_score. Pilares sem klines marcados
    como "DADO AUSENTE" no breakdown, não silenciosamente zerados.

v6.3.0 (22/03/2026):
  - A3: 6 colunas de candle bearish adicionadas ao COLS_15M (paridade SHORT).
  - A4: Trava de candle fechado. get_candle_lock_status() + apply_candle_lock()
    aplicados nos loops LONG e SHORT. Visível no log e Telegram.
  - A6: Flag oi_estimado nas 3 parsers. Alertas bloqueados se OI não verificado.
    Tokens com OI estimado vão para Observação, não geram call.
  - A8: Análise leve SHORT (top_light_short) com paridade ao LONG.

v6.2.0 (22/03/2026):
  - KLINE_TOP_N: 10→20 (LONG+SHORT). KLINE_TOP_N_LIGHT: 20→30.
  - SR_PROXIMITY_PCT e OB_PROXIMITY_PCT: 1.0%→2.5% (altcoins voláteis).
  - RSI<30 descarta candidatos SHORT no Gate 4H (evita shortar fundos exaustos).
  - P9 OI crescente: novo pilar +2 pts. Compara OI atual com média histórica.
  - Score histórico por token no state.json (48 rodadas, TTL 25h, limpeza auto).
  - Heartbeat Telegram expandido: radar top-7, evolução de score (↑↑↑→↓↓),
    candle lock, obs SHORT, counts de pipeline completos.
  - KLINE_CACHE_TTL_H: 3h→1h.
  - Threshold SHORT espelha LONG invertido (corrigido em v6.5.0).

v6.1.2 (22/03/2026):
  - Telegram: relatório completo de rodada substitui heartbeat compacto.
    Radar top-7 LONG e SHORT com score, OI, FR e tendência por token.
  - Mensagens de call expandidas: até 5 razões técnicas, margem, notional.
  - Credenciais via os.getenv() (fix de segurança do v6.1.1).

v6.1.1 (22/03/2026):
  - Fix credenciais Telegram expostas → os.getenv().
  - Remoção de dead code e duplicações.
  - Heartbeat sem duplicatas.

v6.0 (21/03/2026):
  - Arquitetura bidirecional LONG/SHORT completa.
  - Pilares bearish espelhados para todos os pilares (P1–P8).
  - Exclusividade LONG/SHORT: mesmo token não abre posições conflitantes.
  - Threshold SHORT adaptativo (espelho invertido do LONG).

v5.3.1: OKX como Fonte 1, Gate.io como Fonte 2 (reversão empírica).
v5.3:   Gate.io como Fonte 1 (revertido — universo insuficiente).
v5.2:   Fix CoinGecko parser + klines OKX fallback.
v5.1:   Hierarquia 3 fontes, timeout 8s, pump bloqueados, RSI>80.
v5.0:   Fonte dual Bybit/Bitget, filtros relaxados ($2M/$5M), TOP_N removido.
v4.9:   Perpétuos puros + Estratégia de Recuperação de Banca.

Autor: Manus AI | v4.1→v6.6.2 (revisão Claude/Anthropic)
=============================================================================

=============================================================================
ROADMAP EVOLUTIVO — MELHORIAS IDENTIFICADAS E PENDENTES
=============================================================================
Este bloco documenta evoluções técnicas avaliadas e aprovadas conceitualmente,
aguardando implementação. Preservado aqui para não perder o contexto técnico
acumulado durante o desenvolvimento.

--- P9: PILAR DE LIQUIDAÇÕES / HEATMAP [PENDENTE] ---
  Conceito: Clusters de liquidação são magnetos reais de preço em scalp
  alavancado. Quando há massa de posições abertas que liquidarão se o
  preço mover em determinada direção, o mercado tende a "caçar" essas zonas
  gerando squeezes explosivos de 5–15 minutos.

  Integração no scoring:
    LONG: cluster de SHORTs acima do preço → +1 a +3 pts (preço sobe para liquidá-los)
    SHORT: cluster de LONGs abaixo do preço → +1 a +3 pts (preço cai para liquidá-los)

  Fontes avaliadas:
    CoinGlass API: MELHOR opção (OI agregado de 30+ exchanges). Requer plano
      pago (~$35/mês Hobby). Endpoint: /api/futures/liquidation/heatmap
    OKX /liquidation-orders: GRATUITO mas retorna liquidações JÁ EXECUTADAS
      (históricas), não posições abertas em risco futuro. É proxy fraca —
      detectar clusters sobre dados passados pode gerar sinais invertidos.
      NÃO usar até validar empiricamente com pelo menos 30 rodadas.
    Hyblock Capital: free tier limitado, cobertura menor que CoinGlass.

  Decisão atual: AGUARDAR fonte gratuita confiável ou orçar CoinGlass Hobby.
  Não implementar com dados passados do /liquidation-orders sem validação.

--- MICRO-SCAN 5m/1m PÓS-ALERTA [PENDENTE] ---
  Conceito: após gerar um alerta de call (score ≥ threshold), fazer um
  mini-scan adicional nos 3-5 tokens que passaram, usando klines de 5m/1m
  para refinar o timing de entrada.

  Vantagem: a decisão macro já está tomada em 4H/1H/15m. O 5m/1m só
  confirma se o timing imediato é favorável (ex: vela de reversão no 5m
  exatamente sobre o OB do 1H).

  Custo de API: zero adicional — klines 5m/1m da Bitget/OKX são públicos.
  Impacto esperado: reduz entradas prematuras ("entrou cedo demais").

  Implementação: novo parâmetro --micro no main(), ativa fetch de klines
  de 5m para os tokens em alertas, calcula Bollinger e candle no 5m.
  Não altera o pipeline principal — é etapa pós-alerta opcional.

--- INTEGRAÇÃO BINANCE (via VPS exterior) [PENDENTE] ---
  Conceito: Binance fapi tem geo-block do Brasil em endpoints de futuros.
  Solução: rodar o script em VPS com IP de Singapura/Hong Kong.

  Vantagem: +50–70 tokens líquidos no universo (Binance domina ~40–50%
  do volume global de futuros). Elimina distorção de dados locais (OKX).

  Custo: ~$5–8/mês (DigitalOcean, Hetzner, Vultr).

  Implementação no código: substituir o parser OKX pelo endpoint Binance
  fapi/v1/ticker/24hr (volume) + fapi/v1/openInterest por lote (OI ainda
  requer 1 chamada por símbolo — avaliar viabilidade de cache estendido).
  NÃO usar ccxt para isso — o script é async nativo (aiohttp) e ccxt usa
  threads síncronas que causam contenção.

  Decisão atual: implementar quando/se o VPS for configurado.
  O pipeline atual (OKX) é suficiente para a fase de validação.

--- WEBHOOK WHATSAPP [ALTERNATIVA AO TELEGRAM] [PENDENTE] ---
  Opção 1 — WhatsApp Cloud API (Meta oficial):
    Gratuito até 1.000 msgs/mês. Requer conta Meta Business + aprovação
    de templates (1–3 dias de setup). Zero risco de ban.
    Endpoint: graph.facebook.com/v18.0/{phone_id}/messages

  Opção 2 — Evolution API (não-oficial, mais prática):
    Instalar no VPS (Docker). Conecta WhatsApp pessoal via QR code.
    POST local para http://localhost:8080/send. Zero custo.
    Risco de ban baixo se < 5 msgs por execução. Favorita dos devs BR.

  Decisão atual: Telegram implementado (v6.1.2). WhatsApp como alternativa
  quando/se o VPS for configurado (Evolution API é a opção recomendada).

--- CVD / DELTA DE VOLUME [PESQUISAR] ---
  Conceito: Cumulative Volume Delta (CVD) mede se o volume está sendo
  dominado por compras ou vendas agressivas (takers). Divergência entre
  preço e CVD é sinal de reversão iminente.
  Fontes: OKX /api/v5/market/trades (takers) — cálculo local a partir dos
  trades brutos. Requer acumulação por janela temporal (últimas 100 trades).

--- ALERTAS DE RISCO OPERACIONAL [PENDENTE] ---
  Heartbeat de ausência: se o script não rodar por mais de 45 minutos
  (esperado: 30min), enviar alerta de "scanner offline" via Telegram.
  Implementação: arquivo /tmp/atirador_last_run.json com timestamp.
  Um script secundário (cron a cada 15min) verifica e alerta se > 45min.

=============================================================================
"""
import json
import requests
import time
import os
import sys
import logging
import numpy as np
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

# Fuso horário BRT (Brasília, UTC-3)
BRT = timezone(timedelta(hours=-3))

# ===========================================================================
# SISTEMA DE LOG CENTRALIZADO [v4.7]
# ===========================================================================
# Grava em arquivo E terminal simultaneamente.
# Um arquivo de log por execução: /tmp/atirador_YYYYMMDD_HHMM.log
# Níveis: DEBUG (detalhes internos) | INFO (fluxo normal) | WARNING | ERROR

LOG_DIR = "/tmp/atirador_logs"

def setup_logger():
    """
    Configura logger com saída dupla: arquivo + terminal.
    [v4.8 FIX 1] Nome do arquivo: atirador_LOG_YYYYMMDD_HHMM.log
    [v4.8 FIX 3] Timestamps do log em BRT (não UTC) via converter customizado
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    ts_brt  = datetime.now(BRT)
    ts_str  = ts_brt.strftime("%Y%m%d_%H%M")
    logfile = f"{LOG_DIR}/atirador_LOG_{ts_str}.log"

    logger = logging.getLogger("atirador")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # [v4.8 FIX 3] Converter customizado para que %(asctime)s use BRT, não UTC
    def brt_converter(timestamp, *args):
        return datetime.fromtimestamp(timestamp, BRT).timetuple()

    # Formato completo para arquivo (com timestamp BRT e nível)
    fmt_file = logging.Formatter(
        "%(asctime)s BRT [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fmt_file.converter = brt_converter

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt_file)

    # Formato compacto para terminal (só mensagem)
    fmt_term = logging.Formatter("%(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt_term)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"📋 Log iniciado: {logfile}")
    return logger, logfile, ts_str   # retorna ts_str para usar no nome do relatório

# Logger global — inicializado em run_scan_async
LOG      = None
LOG_FILE = None
TS_SCAN  = None   # timestamp da execução — usado no nome do relatório

def log_section(title):
    """Separador visual de seção no log."""
    LOG.info(f"\n{'─'*55}")
    LOG.info(f"  {title}")
    LOG.info(f"{'─'*55}")


# ===========================================================================
# MÓDULO TELEGRAM [v6.1]
# ===========================================================================
# Dois tipos de mensagem:
#   1. ALERTA DE CALL — score ≥ threshold. Conteúdo completo operacional.
#   2. HEARTBEAT      — toda rodada. Resumo compacto para monitoramento.
#
# Arquitetura:
#   _tg_send()       → chamada HTTP base com tratamento de erro silencioso
#   _tg_call_long()  → formata alerta LONG
#   _tg_call_short() → formata alerta SHORT
#   _tg_heartbeat()  → formata resumo da rodada
#   tg_notify()      → orquestra tudo: chamado uma vez no final do scan
#
# Falha de envio é sempre silenciosa — WARNING no log, nunca interrompe o scan.
# ===========================================================================

def _tg_send(text: str) -> bool:
    """
    Envia mensagem para o Telegram via API HTTP.
    Retorna True se enviou com sucesso, False se falhou.
    Silencioso em falha — apenas loga WARNING.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False  # Telegram não configurado — silencioso
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id"    : TELEGRAM_CHAT_ID,
            "text"       : text,
            "parse_mode" : "HTML",
        }, timeout=8)
        if resp.status_code != 200:
            LOG.warning(f"  ⚠️  Telegram: HTTP {resp.status_code} — {resp.text[:80]}")
            return False
        return True
    except Exception as e:
        LOG.warning(f"  ⚠️  Telegram: falha ao enviar — {type(e).__name__}: {e}")
        return False


def _score_trend_line(r: dict, state: dict, direction: str) -> str:
    """Retorna linha de tendência de score para exibição no radar do Telegram."""
    sym   = r.get("symbol", "")
    trend = get_score_trend(state, sym, direction)
    score = r["score"] if direction == "LONG" else r.get("score_short", 0)
    oi_sc = r.get("oi_score", 0)
    oi_str = f" | OI{'+'if oi_sc>0 else ''}{oi_sc}" if oi_sc != 0 else ""
    s4h   = r.get("summary_4h", "?")
    sym_s = r.get("base_coin", "?")
    return f"  🟡 {sym_s:<6} {score}/25 {trend}  {s4h}{oi_str}"


def _tg_breakdown_pilares(bd: list, direction: str) -> str:
    """
    [v6.6.2] Formata o breakdown de pilares para mensagens QUASE e Call.
    Exibe todos os pilares relevantes (max_pts > 0) com:
      ✅/⬜  NOME  +pts/max  descrição curta
    Retorna string pronta para concatenar na mensagem.
    """
    linhas = []
    for pilar, pts, max_pts, detalhe in bd:
        if max_pts == 0:
            continue   # P7 Pump/Dump — skip se neutro
        ico  = "✅" if pts > 0 else ("🔻" if pts < 0 else "⬜")
        desc = detalhe.split("|")[0].strip()
        if len(desc) > 40:
            desc = desc[:38] + "…"
        linhas.append(f"  {ico} {pilar:<16} {pts:>+2}/{max_pts}  {desc}")
    return "\n".join(linhas)


def _tg_call_long(r: dict, ctx: dict, state: dict = None) -> str:
    """
    [v6.6.2] Mensagem exclusiva de CALL LONG — operacional completa.
    Inclui entrada/SL/TPs, sizing risk-first e breakdown completo de pilares.
    """
    t     = r["trade"]
    score = r["score"]
    sym   = r["base_coin"]
    thr   = ctx["threshold"]
    trend = get_score_trend(state, r.get("symbol",""), "LONG") if state else "🆕"

    bd_str = _tg_breakdown_pilares(r.get("breakdown", []), "LONG")

    msg = (
        f"🚀 <b>LONG {sym}</b>  {trend}  {score}/25\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entrada  <b>${t['entry']:.4f}</b>\n"
        f"🎯 TP1      ${t['tp1']:.4f}  (+{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
        f"🎯 TP2      ${t['tp2']:.4f}  (+{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
        f"🎯 TP3      ${t['tp3']:.4f}  (+{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
        f"🛑 SL       ${t['sl']:.4f}  (−{t['sl_distance_pct']:.2f}%)\n"
        f"⚡ <b>{t['alavancagem']}x</b>  |  Margem ${t.get('margem_usd',0):.0f}"
        f"{'⚠️' if t.get('margem_excedida') else ''}  |  "
        f"Risco ${t['risco_usd']:.2f}  |  Ganho ${t['ganho_rr2_usd']:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧱 <b>PILARES ({score}/25)</b>\n"
        f"{bd_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol ${r['turnover_24h']/1e6:.1f}M  |  "
        f"OI ${r.get('oi_usd',0)/1e6:.1f}M  |  "
        f"FR {r.get('funding_rate',0):.4%}\n"
        f"⏰ Mover SL para breakeven +0.5% após TP1"
    )
    return msg



def _tg_call_short(r: dict, ctx: dict, state: dict = None) -> str:
    """
    [v6.6.2] Mensagem exclusiva de CALL SHORT — operacional completa.
    Inclui entrada/SL/TPs, sizing risk-first e breakdown completo de pilares.
    """
    t     = r["trade_short"]
    score = r.get("score_short", 0)
    sym   = r["base_coin"]
    thr   = ctx["threshold_short"]
    trend = get_score_trend(state, r.get("symbol",""), "SHORT") if state else "🆕"

    bd_str = _tg_breakdown_pilares(r.get("breakdown_short", []), "SHORT")

    msg = (
        f"📉 <b>SHORT {sym}</b>  {trend}  {score}/25\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entrada  <b>${t['entry']:.4f}</b>\n"
        f"🎯 TP1      ${t['tp1']:.4f}  (−{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
        f"🎯 TP2      ${t['tp2']:.4f}  (−{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
        f"🎯 TP3      ${t['tp3']:.4f}  (−{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
        f"🛑 SL       ${t['sl']:.4f}  (+{t['sl_distance_pct']:.2f}%) ← ACIMA\n"
        f"⚡ <b>{t['alavancagem']}x</b>  |  Margem ${t.get('margem_usd',0):.0f}"
        f"{'⚠️' if t.get('margem_excedida') else ''}  |  "
        f"Risco ${t['risco_usd']:.2f}  |  Ganho ${t['ganho_rr2_usd']:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧱 <b>PILARES ({score}/25)</b>\n"
        f"{bd_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol ${r['turnover_24h']/1e6:.1f}M  |  "
        f"OI ${r.get('oi_usd',0)/1e6:.1f}M  |  "
        f"FR {r.get('funding_rate',0):.4%}\n"
        f"⏰ Mover SL para breakeven −0.5% após TP1"
    )
    return msg


def _tg_quase(r: dict, direction: str, ctx: dict, state: dict) -> str:
    """
    [v6.6.2] Mensagem de alerta QUASE — token a menos de QUASE_MARGEM pts do threshold.
    Mostra todos os pilares (ativos e inativos) e lista as fontes de pts disponíveis.
    Disparada uma mensagem por token elegível, separada do heartbeat.
    """
    if direction == "LONG":
        score  = r["score"]
        thr    = ctx["threshold"]
        bd     = r.get("breakdown", [])
        s4h    = r.get("summary_4h", "?")
        s1h    = r.get("summary_1h", "?")
        ico    = "⚠️"
        diric  = "LONG"
        sym    = r["base_coin"]
    else:
        score  = r.get("score_short", 0)
        thr    = ctx["threshold_short"]
        bd     = r.get("breakdown_short", [])
        s4h    = r.get("summary_4h", "?")
        s1h    = r.get("summary_1h", "?")
        ico    = "⚠️"
        diric  = "SHORT"
        sym    = r["base_coin"]

    falta = thr - score
    trend = get_score_trend(state, r.get("symbol",""), direction)

    # Pilares formatados
    bd_str = _tg_breakdown_pilares(bd, direction)

    # Fontes de pts ainda disponíveis (pilares zerados com max > 0)
    fontes = []
    for pilar, pts, max_pts, detalhe in bd:
        if pts == 0 and max_pts > 0 and "AUSENTE" not in detalhe:
            fontes.append(f"{pilar.strip()} (+{max_pts})")
    fontes_str = ", ".join(fontes[:5]) if fontes else "—"

    msg = (
        f"⚠️ <b>QUASE — {diric} {sym}</b>  {trend}  {score}/25\n"
        f"  {s4h} 4H  |  {s1h} 1H  |  thr ≥{thr}  |  faltam <b>{falta} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧱 PILARES\n"
        f"{bd_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"→ Fontes disponíveis: {fontes_str}"
    )
    return msg


def _tg_relatorio_rodada(ctx: dict, total_items: int, qualificados: int,
                          n_long_gate1: int, n_long_gate2: int,
                          n_short_gate1: int, n_short_gate2: int,
                          results: list, results_short: list,
                          fonte: str, elapsed: float,
                          state: dict, tokens_sem_dados: list,
                          candle_lock: dict = None,
                          obs_long: list = None, obs_short: list = None,
                          n_calls: int = 0) -> str:
    """
    [v6.6.2] Heartbeat limpo — status do mercado e preparação para calls.

    Estrutura (top→bottom = mais crítico primeiro):
      1. Cabeçalho    — versão, timestamp, FGI, BTC, thresholds, P&L, slots
      2. Risco        — banca, risco/trade, perda máx/dia
      3. Pipeline     — universo→gates→análise, fonte, tempo, candle lock
      4. Radar LONG   — top-5 tokens com score e seta direcional, máx e gap
      5. Radar SHORT  — idem
      6. Veredicto    — AGUARDAR / QUASE (msgs separadas) / CALL (ver msg)

    SEM breakdown de pilares — isso fica nas mensagens QUASE e Call.
    Limite: ≤ 4096 chars (1 mensagem Telegram).
    """
    ts      = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
    fgi_val = ctx["fg"]
    fgi_ico = ("🔴" if fgi_val <= 20 else "🟠" if fgi_val <= 35 else
               "🟡" if fgi_val <= 55 else "🟢" if fgi_val >= 70 else "⚪")
    fgi_txt = ("Medo Extremo" if fgi_val <= 20 else "Medo" if fgi_val <= 40 else
               "Neutro" if fgi_val <= 60 else "Ganância" if fgi_val <= 80 else "Ganância Extrema")
    btc_ico = "📈" if "BUY" in ctx["btc"] else ("📉" if "SELL" in ctx["btc"] else "➡️")
    thr_l   = ctx["threshold"]
    thr_s   = ctx["threshold_short"]

    # ── 1. CABEÇALHO ────────────────────────────────────────────────────────
    msg  = f"🤖 <b>ATIRADOR v6.6.2</b> | {ts}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🌡️ MERCADO\n"
    msg += f"  {fgi_ico} FGI {fgi_val} — {fgi_txt}  |  {btc_ico} BTC 4H: {ctx['btc']}\n"
    msg += f"  📋 LONG ≥{thr_l} {ctx['verdict'].split('(')[0].strip()}  |  SHORT ≥{thr_s} {ctx['verdict_short'].split('(')[0].strip()}\n"

    # ── 2. PIPELINE ─────────────────────────────────────────────────────────
    if candle_lock and candle_lock.get("use_prev"):
        cl = f"⚠️ Candle em formação — penúltimo usado"
    elif candle_lock:
        cl = f"✅ Candle ok (último: {candle_lock['ts_last_close']})"
    else:
        cl = ""
    sem_tv = f"  ⚠️ Sem TV: {', '.join(tokens_sem_dados[:3])}\n" if tokens_sem_dados else ""

    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔍 PIPELINE\n"
    msg += f"  Universo: {total_items} tokens  →  {qualificados} qualificados\n"
    msg += f"  Gate 4H:  LONG {n_long_gate1:<3}  |  SHORT {n_short_gate1}\n"
    msg += f"  Gate 1H:  LONG {n_long_gate2:<3}  |  SHORT {n_short_gate2}\n"
    msg += f"  Análise:  LONG {len(results):<3}  |  SHORT {len(results_short)}\n"
    msg += f"  📡 {fonte}  |  ⏱️ {elapsed:.1f}s\n"
    if cl:
        msg += f"  {cl}\n"
    if sem_tv:
        msg += sem_tv

    # ── 4 + 5. RADAR LONG e SHORT ───────────────────────────────────────────
    def _radar(lista, direction, thr):
        key = "score" if direction == "LONG" else "score_short"
        ico = "📈" if direction == "LONG" else "📉"
        max_sc = max((r[key] if direction == "LONG" else r.get(key, 0)
                      for r in lista), default=0)
        falta  = thr - max_sc
        lines = []
        for r in lista[:5]:
            sc  = r[key] if direction == "LONG" else r.get(key, 0)
            tr  = get_score_trend(state, r.get("symbol",""), direction)
            sym = r.get("base_coin","?")
            lines.append(f"  · {sym:<6} {sc:>2} {tr}")
        tok_str = "\n".join(lines) if lines else "  —"
        gap_str = f"faltam {falta} pts" if falta > 0 else "✅ threshold atingido"
        return (f"{ico} {direction}  máx {max_sc}/{thr} — {gap_str}\n"
                f"{tok_str}")

    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += _radar(results,       "LONG",  thr_l) + "\n\n"
    msg += _radar(results_short, "SHORT", thr_s) + "\n"

    # ── 6. VEREDICTO ────────────────────────────────────────────────────────
    alertas_long  = [r for r in results       if r["score"]             >= thr_l]
    alertas_short = [r for r in results_short if r.get("score_short",0) >= thr_s]
    max_l = max((r["score"]             for r in results),       default=0)
    max_s = max((r.get("score_short",0) for r in results_short), default=0)
    quase_long  = [r for r in results       if thr_l - r["score"]             <= QUASE_MARGEM
                   and r["score"] < thr_l]
    quase_short = [r for r in results_short if thr_s - r.get("score_short",0) <= QUASE_MARGEM
                   and r.get("score_short",0) < thr_s]

    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    if alertas_long or alertas_short:
        n_c = len(alertas_long) + len(alertas_short)
        msg += f"🔥 <b>{n_c} CALL(S) DISPARADA(S) — ver mensagem(ns) exclusiva(s) abaixo</b>\n"
    elif quase_long or quase_short:
        n_q = len(quase_long) + len(quase_short)
        nomes = ([f"LONG {r['base_coin']}" for r in quase_long] +
                 [f"SHORT {r['base_coin']}" for r in quase_short])
        msg += f"⚠️ {n_q} QUASE — {', '.join(nomes)} — ver msg(s) abaixo\n"
    else:
        msg += f"⚪ AGUARDAR — sem candidatos próximos do threshold\n"

    # Guardar no limite
    if len(msg) > 4090:
        linhas = msg.split("\n")
        while len("\n".join(linhas)) > 4090 and len(linhas) > 8:
            linhas.pop(-2)
        msg = "\n".join(linhas)

    return msg


def tg_notify(ctx: dict, results: list, results_short: list,
              total_items: int, qualificados: int,
              n_long_gate1: int, n_long_gate2: int,
              n_short_gate1: int, n_short_gate2: int,
              fonte: str, elapsed: float,
              state: dict, tokens_sem_dados: list,
              candle_lock: dict = None,
              obs_long: list = None, obs_short: list = None):
    """
    [v6.6.2] Orquestra as 3 camadas de comunicação Telegram por rodada:

      1. Heartbeat (sempre)  — status do mercado, pipeline, radar, veredicto
      2. QUASE (por token)   — score a menos de QUASE_MARGEM pts do threshold
                               Uma mensagem separada por token elegível
      3. Call (por token)    — score ≥ threshold
                               Uma mensagem separada por token com dados completos

    Ordem de envio: heartbeat → QUASEs → calls.
    Falha de envio é sempre silenciosa — WARNING no log, scan continua.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        LOG.debug("  📵  Telegram não configurado — notificações desativadas")
        return

    thr_l = ctx["threshold"]
    thr_s = ctx["threshold_short"]

    alertas_long  = [r for r in results       if r["score"]             >= thr_l]
    alertas_short = [r for r in results_short if r.get("score_short",0) >= thr_s]
    quase_long    = [r for r in results
                     if thr_l - r["score"] <= QUASE_MARGEM and r["score"] < thr_l]
    quase_short   = [r for r in results_short
                     if thr_s - r.get("score_short",0) <= QUASE_MARGEM
                     and r.get("score_short",0) < thr_s]

    n_calls = len(alertas_long) + len(alertas_short)
    n_enviados = 0

    # ── 1. Heartbeat ─────────────────────────────────────────────────────────
    if TELEGRAM_HEARTBEAT:
        msg_hb = _tg_relatorio_rodada(
            ctx, total_items, qualificados,
            n_long_gate1, n_long_gate2, n_short_gate1, n_short_gate2,
            results, results_short, fonte, elapsed,
            state, tokens_sem_dados,
            candle_lock=candle_lock,
            obs_long=obs_long or [], obs_short=obs_short or [],
            n_calls=n_calls,
        )
        if _tg_send(msg_hb):
            n_enviados += 1
            LOG.info(f"  📲  Telegram heartbeat: enviado ✅")
        else:
            LOG.warning(f"  📵  Telegram heartbeat: falha no envio")

    # ── 2. Mensagens QUASE — uma por token (LONG depois SHORT) ───────────────
    for r in quase_long:
        msg = _tg_quase(r, "LONG", ctx, state)
        if _tg_send(msg):
            n_enviados += 1
            LOG.info(f"  📲  Telegram QUASE LONG {r['base_coin']}: enviado ✅")
        else:
            LOG.warning(f"  📵  Telegram QUASE LONG {r['base_coin']}: falha")

    for r in quase_short:
        msg = _tg_quase(r, "SHORT", ctx, state)
        if _tg_send(msg):
            n_enviados += 1
            LOG.info(f"  📲  Telegram QUASE SHORT {r['base_coin']}: enviado ✅")
        else:
            LOG.warning(f"  📵  Telegram QUASE SHORT {r['base_coin']}: falha")

    # ── 3. Mensagens de Call — uma por token ─────────────────────────────────
    for r in alertas_long:
        msg = _tg_call_long(r, ctx, state=state)
        if _tg_send(msg):
            n_enviados += 1
            LOG.info(f"  📲  Telegram CALL LONG {r['base_coin']}: enviado ✅")
        else:
            LOG.warning(f"  📵  Telegram CALL LONG {r['base_coin']}: falha")

    for r in alertas_short:
        msg = _tg_call_short(r, ctx, state=state)
        if _tg_send(msg):
            n_enviados += 1
            LOG.info(f"  📲  Telegram CALL SHORT {r['base_coin']}: enviado ✅")
        else:
            LOG.warning(f"  📵  Telegram CALL SHORT {r['base_coin']}: falha")

    total_tent = ((1 if TELEGRAM_HEARTBEAT else 0) +
                  len(quase_long) + len(quase_short) +
                  len(alertas_long) + len(alertas_short))
    LOG.info(f"  📲  Telegram: {n_enviados}/{total_tent} mensagens enviadas "
             f"(1 heartbeat + {len(quase_long)+len(quase_short)} QUASE + {n_calls} call)")





# ===========================================================================
# CONFIGURAÇÃO
# ===========================================================================

# Filtros Institucionais [v5.0]
# MIN_TURNOVER_24H: $5M → $2M  (captura mais altcoins com liquidez real em perpetuals)
# MIN_OI_USD:      $10M → $5M  (OI de $5M garante execução sem slippage relevante)
# TOP_N removido: todos os qualificados entram no pipeline.
#   Gargalo real = KLINE_TOP_N (busca de klines), não o universo de entrada.
MIN_TURNOVER_24H = 2_000_000
MIN_OI_USD       = 5_000_000

# ===========================================================================
# GESTÃO DE RISCO — SIZING RISK-FIRST [v6.4.0 — A1+A2]
# ===========================================================================
#
# PROBLEMA ANTERIOR (v4.9–v6.3):
#   posicao_usd = BANKROLL * alavancagem
#   → Margem implícita por trade = banca inteira. Com MAX_TRADES_ABERTOS=2
#     e alavancagem 20x, o modelo alocava $2000 de notional por trade sobre
#     $100 de banca — margem de $100 por trade × 2 = $200 em conta de $100.
#     Matematicamente inconsistente para uso real.
#
# SOLUÇÃO v6.4.0 — FÓRMULA RISK-FIRST (recomendada pela auditoria):
#   1. stop_pct  = distância % entre entrada e SL  (vem do ATR)
#   2. notional  = RISCO_POR_TRADE_USD / stop_pct  (quanto preciso para arriscar $5)
#   3. margem    = notional / alavancagem_alvo      (capital real alocado)
#   4. alavancagem_necessaria = notional / MARGEM_MAX_POR_TRADE
#   5. alavancagem_final = min(alavancagem_necessaria, cap_por_score)
#
#   Garantia: margem_por_trade ≤ MARGEM_MAX_POR_TRADE em qualquer cenário.
#   Com 2 trades simultâneos: exposição máxima = 2 × MARGEM_MAX_POR_TRADE.
#
# CALIBRAÇÃO com banca $100, 2 trades simultâneos:
#   MARGEM_MAX_POR_TRADE = $35 → máx exposição simultânea = $70 (70% banca)
#   Reserva de $30 cobre funding, taxas e flutuação de margem.
#
# RECALIBRAÇÃO DO SCORE MÁXIMO E TABELA DE ALAVANCAGEM [A2]:
#   Score real máximo = 28 pts (com P9 OI +2).
#   Thresholds de alerta: 14 / 16 / 20 pts.
#   Problema anterior: tabela de alav só iniciava em score 20 → calls com
#   score 14-19 recebiam 1x (inútil para scalp). Corrigido: tabela agora
#   cobre o range completo de scores que geram alertas.
#
#   Faixas recalibradas sobre teto real de 28:
#     Score 14–15 → até  5x  (threshold mínimo — setup marginal)
#     Score 16–17 → até 10x  (threshold moderado)
#     Score 18–19 → até 15x  (abaixo de cauteloso mas válido)
#     Score 20–21 → até 20x  (cauteloso — mercado exigente)
#     Score 22–23 → até 30x  (forte)
#     Score 24–25 → até 40x  (muito forte)
#     Score 26–28 → até 50x  (setup perfeito / excepcional com P9)
#
BANKROLL              = 100.0
RISCO_POR_TRADE_USD   = 5.00    # Risco fixo em $ por trade (loss máximo)
MARGEM_MAX_POR_TRADE  = 35.0    # [v6.4.0 A1] Máximo de margem alocada por trade ($)
                                 # Com 2 trades: máx $70 expostos de $100 de banca
ALAVANCAGEM_MIN       = 2.0     # [v6.4.0 A2] ↑1x→2x — 1x é inútil para scalp
ALAVANCAGEM_MAX       = 50.0    # Teto absoluto
RR_MINIMO             = 2.0     # Risk:Reward mínimo 1:2

# Tabela de alavancagem máxima por score [v6.4.0 A2]
# Recalibrada sobre teto real de 28 pts e range completo de thresholds (14–28).
ALAV_POR_SCORE = {
    (14, 15): 5.0,
    (16, 17): 10.0,
    (18, 19): 15.0,
    (20, 21): 20.0,
    (22, 23): 30.0,
    (24, 25): 40.0,
    (26, 28): 50.0,
}

def get_alav_max_por_score(score: int) -> float:
    """
    [v6.4.0 A2] Retorna a alavancagem máxima para o score dado.
    Cobre range completo 14–28 (todos os thresholds que geram alertas).
    Score abaixo de 14 não deveria gerar alerta — retorna mínimo por segurança.
    """
    for (sc_min, sc_max), alav_max in ALAV_POR_SCORE.items():
        if sc_min <= score <= sc_max:
            return alav_max
    return ALAVANCAGEM_MIN

# Performance
KLINE_TOP_N        = 20       # [v6.2.0] ↑10→20 — captura mais candidatos SHORT/LONG
KLINE_TOP_N_LIGHT  = 30       # [v6.2.0] ↑20→30 — análise leve (sem klines)
KLINE_LIMIT        = 60
KLINE_CACHE_TTL_H  = 1        # [v6.2.0] ↓3h→1h — captura velas 4H novas a cada hora

# Análise Técnica
SWING_WINDOW     = 5
SR_PROXIMITY_PCT = 2.5        # [v6.2.0] ↑1.0→2.5% — cobre altcoins com ATR 3-5%/vela 1H
OB_IMPULSE_N     = 3          # Candles para medir impulso após OB
OB_IMPULSE_PCT   = 1.5        # Impulso mínimo para qualificar OB (%)
OB_PROXIMITY_PCT = 2.5        # [v6.2.0] ↑1.5→2.5% — alinhado com SR_PROXIMITY_PCT

# Score histórico por token [v6.2.0]
SCORE_HISTORY_MAX_ROUNDS = 48  # Máx rodadas armazenadas por token (~24h a cada 30min)
SCORE_HISTORY_TTL_H      = 25  # Remove tokens sem aparição há mais de 25h

# Filtro de Pump
PUMP_WARN_24H        = 20     # Penalidade -2 pts
PUMP_WARN_24H_STRONG = 30     # Penalidade -3 pts
PUMP_BLOCK_24H       = 40     # Descarte total

# Estado Diário
STATE_FILE = "/tmp/atirador_state.json"

# ===========================================================================
# CONFIGURAÇÃO TELEGRAM [v6.1] — credenciais via variáveis de ambiente
# ===========================================================================
# NÃO coloque token ou chat_id diretamente no código — use variáveis de ambiente.
#
# Como configurar (Linux/Mac):
#   export TELEGRAM_TOKEN="8322261249:AAH..."
#   export TELEGRAM_CHAT_ID="1021264693"
#
# Como configurar permanentemente (adicione ao ~/.bashrc ou ~/.zshrc):
#   echo 'export TELEGRAM_TOKEN="SEU_TOKEN"' >> ~/.bashrc
#   echo 'export TELEGRAM_CHAT_ID="SEU_CHAT_ID"' >> ~/.bashrc
#   source ~/.bashrc
#
# Como obter o token:
#   BotFather no Telegram → /newbot → copie o token
#
# Como obter o chat_id:
#   1. Envie qualquer mensagem para o bot
#   2. Acesse: https://api.telegram.org/bot{TOKEN}/getUpdates
#   3. Copie o número em "chat" → "id"
#
# Para desativar: não defina as variáveis (deixe sem export)
#
# Exclusividade LONG/SHORT: o mesmo token não pode ter LONG e SHORT abertos
# simultaneamente. Se HYPE tem LONG aberto, qualquer sinal SHORT de HYPE é
# bloqueado naquela execução. MAX_TRADES_ABERTOS=2 cobre ambas as direções.
#
# Carregar credenciais do Telegram (com fallback para variáveis de ambiente)
# [v6.5.0] Caminhos persistentes — sobrevivem a reinícios do ambiente
# /home/ubuntu/ é persistente no Manus; /tmp/ é limpo a cada reinício.
TELEGRAM_CONFIG_FILE = os.path.join(
    os.path.expanduser("~"), ".atirador_telegram_config.json"
)
# Fallback para /tmp/ (compatibilidade com instalações antigas)
TELEGRAM_CONFIG_FILE_LEGACY = "/tmp/atirador_telegram_config.json"


def _load_telegram_config():
    """
    [v6.5.0] Carrega credenciais Telegram com hierarquia de 3 fontes:
      1. ~/.atirador_telegram_config.json  (persistente — sobrevive a reinícios)
      2. /tmp/atirador_telegram_config.json (legado — pode sumir em reinícios)
      3. Variáveis de ambiente TELEGRAM_TOKEN / TELEGRAM_CHAT_ID

    Ao encontrar credenciais válidas no arquivo legado (/tmp/), migra
    automaticamente para o arquivo persistente (~/).
    """
    # Fonte 1: arquivo persistente em ~/
    for cfg_path, is_legacy in [
        (TELEGRAM_CONFIG_FILE, False),
        (TELEGRAM_CONFIG_FILE_LEGACY, True),
    ]:
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, 'r') as f:
                    cfg = json.load(f)
                token   = cfg.get("telegram_token", "")
                chat_id = cfg.get("telegram_chat_id", "")
                if token and chat_id:
                    if is_legacy:
                        # Migrar automaticamente para local persistente
                        _migrate_telegram_config(token, chat_id, cfg_path)
                    return token, chat_id
            except Exception:
                pass

    # Fonte 3: variáveis de ambiente
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    return token, chat_id


def _migrate_telegram_config(token: str, chat_id: str, source_path: str):
    """Migra config do /tmp/ para ~/.atirador_telegram_config.json."""
    try:
        cfg = {
            "telegram_token"  : token,
            "telegram_chat_id": chat_id,
            "migrated_from"   : source_path,
            "migrated_at"     : datetime.now(timezone.utc).isoformat(),
        }
        with open(TELEGRAM_CONFIG_FILE, 'w') as f:
            json.dump(cfg, f, indent=2)
        # Log só pode acontecer se LOG já foi inicializado — use print como fallback
        try:
            LOG.info(f"  📦  Telegram: config migrada de {source_path} → {TELEGRAM_CONFIG_FILE}")
        except Exception:
            print(f"  📦  Telegram: config migrada de {source_path} → {TELEGRAM_CONFIG_FILE}")
    except Exception as e:
        try:
            LOG.warning(f"  ⚠️  Telegram: falha ao migrar config: {e}")
        except Exception:
            pass

TELEGRAM_TOKEN, TELEGRAM_CHAT_ID = _load_telegram_config()

# Heartbeat: True = envia resumo a cada rodada (recomendado para monitoramento)
#            False = só envia quando há alerta de call
TELEGRAM_HEARTBEAT = True

# [v6.6.2] Margem de pts abaixo do threshold que dispara mensagem QUASE.
# Ex: threshold=16, QUASE_MARGEM=4 → tokens com score ≥12 recebem msg QUASE.
QUASE_MARGEM = 4

# Função auxiliar para salvar credenciais do Telegram no arquivo de configuração
def save_telegram_config(token, chat_id):
    """
    [v6.5.0] Salva credenciais do Telegram em ~/.atirador_telegram_config.json
    (persistente) E em /tmp/ (legado, para compatibilidade).
    """
    config = {
        "telegram_token"  : token,
        "telegram_chat_id": chat_id,
        "telegram_enabled": bool(token and chat_id),
        "created_at"      : datetime.now(timezone.utc).isoformat(),
        "last_updated"    : datetime.now(timezone.utc).isoformat(),
    }
    saved = False
    for path in [TELEGRAM_CONFIG_FILE, TELEGRAM_CONFIG_FILE_LEGACY]:
        try:
            os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
            with open(path, 'w') as f:
                json.dump(config, f, indent=2)
            saved = True
        except Exception as e:
            LOG.warning(f"  ⚠️  Telegram: falha ao salvar em {path}: {e}")
    return saved

# ===========================================================================
# GERENCIAMENTO DE ESTADO DIÁRIO
# ===========================================================================

def load_daily_state():
    """
    Carrega estado persistente. Preserva score_history e oi_history entre rodadas.
    """
    default = {
        "score_history": {},   # [v6.2.0] {symbol: [{ts, score_long, score_short},...]}
        "oi_history": {},      # [v6.2.0] {symbol: [{ts, oi},...]}
    }
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            # Garante campos em states antigos (migração transparente)
            if "score_history" not in state: state["score_history"] = {}
            if "oi_history"    not in state: state["oi_history"]    = {}
            return state
    except Exception:
        pass
    save_daily_state(default)
    return default


def save_daily_state(state):
    # [v6.6.2] Persiste apenas campos canônicos — descarta vestigiais de versões anteriores
    # (trades_abertos, pnl_dia, bloqueado, historico, etc.) que não são usados pela v6.6.2.
    canonical = {
        "date":          state.get("date", datetime.now(BRT).strftime("%Y-%m-%d")),
        "score_history": state.get("score_history", {}),
        "oi_history":    state.get("oi_history", {}),
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(canonical, f, indent=2)
    except Exception as e:
        LOG.warning(f"⚠️  Erro ao salvar estado diário: {e}")


def update_score_history(state: dict, results: list, results_short: list, ts: str):
    """
    [v6.2.0] Registra scores desta rodada no histórico por token.

    Estrutura armazenada por token:
      state["score_history"][symbol] = [
          {"ts": "2026-03-22T23:32", "long": 6, "short": 0}, ...  # últimas 48 rodadas
      ]

    Apenas tokens que passaram pelo score completo (top_full_long/short) são registrados.
    Tokens com score -1 ou -99 (descartados antes) não entram.
    """
    sh = state.setdefault("score_history", {})
    oh = state.setdefault("oi_history", {})

    seen = {}  # symbol → {long, short, oi}

    for r in results:
        sym = r.get("symbol", "")
        if sym:
            seen.setdefault(sym, {"long": 0, "short": 0, "oi": r.get("oi_usd", 0)})
            seen[sym]["long"] = r.get("score", 0)

    for r in results_short:
        sym = r.get("symbol", "")
        if sym:
            seen.setdefault(sym, {"long": 0, "short": 0, "oi": r.get("oi_usd", 0)})
            seen[sym]["short"] = r.get("score_short", 0)

    for sym, vals in seen.items():
        # Score history
        entry = {"ts": ts, "long": vals["long"], "short": vals["short"]}
        hist  = sh.get(sym, [])
        hist.append(entry)
        sh[sym] = hist[-SCORE_HISTORY_MAX_ROUNDS:]   # cap em 48 rodadas

        # OI history (mesma lógica, guarda raw USD)
        oi_entry = {"ts": ts, "oi": vals["oi"]}
        oi_hist  = oh.get(sym, [])
        oi_hist.append(oi_entry)
        oh[sym] = oi_hist[-SCORE_HISTORY_MAX_ROUNDS:]


def cleanup_score_history(state: dict):
    """
    [v6.2.0] Remove tokens que não aparecem há mais de SCORE_HISTORY_TTL_H horas.

    Critério: última entrada no score_history mais antiga que o TTL.
    Executado uma vez por rodada, depois de update_score_history.
    Custo: O(n_tokens) — negligenciável.
    """
    sh    = state.get("score_history", {})
    oh    = state.get("oi_history", {})
    agora = datetime.now(BRT)
    ttl_h = SCORE_HISTORY_TTL_H
    removidos = []

    for sym in list(sh.keys()):
        hist = sh[sym]
        if not hist:
            del sh[sym]; oh.pop(sym, None); removidos.append(sym); continue
        try:
            last_ts = datetime.fromisoformat(hist[-1]["ts"])
            # Garante timezone BRT se ausente
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=BRT)
            age_h = (agora - last_ts).total_seconds() / 3600
            if age_h > ttl_h:
                del sh[sym]; oh.pop(sym, None); removidos.append(sym)
        except Exception:
            del sh[sym]; oh.pop(sym, None); removidos.append(sym)

    if removidos:
        LOG.debug(f"  🧹  Score history: {len(removidos)} tokens removidos por TTL ({ttl_h}h): {removidos[:10]}")


def get_score_trend(state: dict, symbol: str, direction: str = "LONG") -> str:
    """
    [v6.2.0] Retorna indicador visual de tendência de score para um token.

    Compara score da penúltima rodada com o da rodada anterior a ela.
    (A rodada atual ainda não foi gravada no momento em que isso é chamado.)

      ↑↑  subiu 3+ pts
      ↑   subiu 1-2 pts
      →   estável (±0)
      ↓   caiu 1-2 pts
      ↓↓  caiu 3+ pts
      🔄  flip de direção (era dominante na direção oposta na rodada anterior)
      🆕  aparece pela primeira vez (apenas 1 entrada no histórico)
    """
    field     = "long" if direction == "LONG" else "short"
    opp_field = "short" if direction == "LONG" else "long"
    hist = state.get("score_history", {}).get(symbol, [])
    if len(hist) <= 1:
        return "🆕"
    # [v6.6.2] Detecta flip de direção: token estava dominante na direção oposta
    last = hist[-1]
    if last.get(opp_field, 0) > last.get(field, 0):
        return "🔄"
    delta = hist[-1][field] - hist[-2][field]
    if delta >= 3:    return "↑↑"
    elif delta >= 1:  return "↑"
    elif delta <= -3: return "↓↓"
    elif delta <= -1: return "↓"
    else:             return "→"


# ===========================================================================
# TRADINGVIEW SCANNER API
# ===========================================================================

TV_URL     = "https://scanner.tradingview.com/crypto/scan"
TV_HEADERS = {
    "User-Agent"   : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type" : "application/json",
    "Origin"       : "https://www.tradingview.com",
    "Referer"      : "https://www.tradingview.com/",
}

# ---------------------------------------------------------------------------
# Convenção de símbolo — IMPORTANTE
# ---------------------------------------------------------------------------
# TradingView Scanner exige o símbolo COMPLETO do par: "BYBIT:BTCUSDT"
# O script passa d["symbol"] (ex: "BTCUSDT") e fetch_tv_batch_async monta
# "BYBIT:BTCUSDT" internamente. O resultado é devolvido com chave "BTCUSDT"
# (sem prefixo), permitindo tv_4h.get("BTCUSDT", {}) funcionar corretamente.
#
# ATENÇÃO: base_coin ("BTC") NÃO é um símbolo válido para o TradingView.
# Usar base_coin causava retorno vazio silencioso em todos os gates — era o
# bug original que fazia zero tokens passarem. Corrigido nesta versão.
#
# Bitget klines também usam d["symbol"] (ex: "BTCUSDT") — mesmo campo.
# ---------------------------------------------------------------------------

# CAMADA 1: Gate 4H — direção macro
COLS_4H = [
    "Recommend.All|240",   # Gate: descarta SELL/STRONG_SELL
    "RSI|240",             # Contexto de força macro (não pontuado)
]

# CAMADA 2: Gate 1H — velocidade de entrada
# [v4.3 FIX] Coluna duplicada removida. A API TV retorna valores por posição;
# colunas duplicadas desalinham o zip e causam retorno silenciosamente errado.
COLS_1H = [
    "Recommend.All|60",   # Gate: exige BUY ou STRONG_BUY
]

# CAMADA 3: Gatilho 15m — dividido em dois grupos de colunas [v6.4.1 FIX]
#
# PROBLEMA IDENTIFICADO: quando BB, ATR e candles são enviados num único
# request ao TradingView Scanner, qualquer coluna inválida (ex: nome errado
# de candle bearish) faz a API retornar dados vazios para TODAS as colunas,
# zerando BB, ATR e funding rate silenciosamente.
#
# SOLUÇÃO: dois requests independentes.
#   COLS_15M_TECH: BB + ATR — request 1. Se falhar, perde P1 e SL.
#   COLS_15M_CANDLES: todos os padrões — request 2. Se falhar, perde só P2.
# Assim um request com coluna inválida não contamina o outro.
#
# [v6.3.0 A3] Candles BEARISH adicionados para paridade SHORT.

# Request 1 — indicadores técnicos (colunas validadas pelo TradingView)
COLS_15M_TECH = [
    "BB.upper|15", "BB.lower|15",        # P1 Bollinger — posição no canal
    "ATR|15",                             # Cálculo do SL dinâmico
]

# Request 2 — padrões de candle (bullish + bearish)
# Se a API rejeitar algum nome, apenas P2 é afetado; BB e ATR ficam intactos
COLS_15M_CANDLES = [
    # Bullish (LONG)
    "Candle.Engulfing.Bullish|15",
    "Candle.Hammer|15",
    "Candle.MorningStar|15",
    "Candle.3WhiteSoldiers|15",
    "Candle.Harami.Bullish|15",
    "Candle.Doji.Dragonfly|15",
    # Bearish (SHORT) — [v6.3.0 A3]
    "Candle.Engulfing.Bearish|15",
    "Candle.ShootingStar|15",
    "Candle.EveningStar|15",
    "Candle.3BlackCrows|15",
    "Candle.Harami.Bearish|15",
    # "Candle.Doji.GraveStone|15",  # removida: API retorna data=null → TypeError (confirmado 25/03/2026)
]

# Mantido para compatibilidade com partes do código que referenciam COLS_15M
COLS_15M = COLS_15M_TECH + COLS_15M_CANDLES

def recommendation_from_value(val):
    if val is None:    return "NEUTRAL"
    if val >= 0.5:     return "STRONG_BUY"
    elif val >= 0.1:   return "BUY"
    elif val >= -0.1:  return "NEUTRAL"
    elif val >= -0.5:  return "SELL"
    else:              return "STRONG_SELL"

async def fetch_tv_batch_async(session, symbols, columns, retries=3):
    """
    Busca indicadores do TradingView — SOMENTE contratos perpétuos.
    [v4.9 FIX 1] Sufixo .P força contrato perpétuo: BYBIT:BTCUSDT.P
    [v5.1 FIX]   Fallback de prefixo: tenta BITGET:XUSDT.P para tokens
                 sem retorno no BYBIT: — reduz tokens sem dados TV quando
                 a fonte de tickers é Bitget ou token só existe na Bitget.
    """
    if not symbols: return {}

    # Tentativa 1: prefixo BYBIT (padrão)
    tickers_bybit = [f"BYBIT:{s}.P" for s in symbols]
    payload = {"symbols": {"tickers": tickers_bybit, "query": {"types": []}},
               "columns": columns}

    LOG.debug(f"  TV batch: {len(symbols)} tokens (.P perpétuo) | cols: {columns}")

    result = {}
    for attempt in range(retries):
        try:
            t0 = time.time()
            async with session.post(TV_URL, json=payload,
                                    headers=TV_HEADERS, timeout=25) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                data    = json.loads(raw.decode("utf-8"))
                for item in (data.get("data") or []):  # guard: API retorna "data":null para colunas inválidas
                    sym  = item["s"].replace("BYBIT:", "").replace(".P", "")
                    vals = item["d"]
                    none_cols = [c for c, v in zip(columns, vals) if v is None]
                    if none_cols:
                        LOG.debug(f"    ⚠️  {sym}: valores None em: {none_cols}")
                    result[sym] = dict(zip(columns, vals))

                missing = [s for s in symbols if s not in result]
                LOG.debug(f"  ✅  TV batch BYBIT: {len(result)}/{len(symbols)} retornados | {elapsed:.2f}s")

                # [v5.1 FIX] Fallback BITGET: para tokens sem retorno no BYBIT:
                if missing:
                    LOG.warning(f"  ⚠️  TV BYBIT: {len(missing)} sem retorno: {missing}")
                    LOG.info(f"  🔄  [v5.1] Tentando prefixo BITGET: para {len(missing)} tokens sem dados...")
                    tickers_bitget = [f"BITGET:{s}.P" for s in missing]
                    payload_fb = {"symbols": {"tickers": tickers_bitget, "query": {"types": []}},
                                  "columns": columns}
                    try:
                        t1 = time.time()
                        async with session.post(TV_URL, json=payload_fb,
                                                headers=TV_HEADERS, timeout=15) as resp_fb:
                            elapsed_fb = time.time() - t1
                            raw_fb     = await resp_fb.read()
                            data_fb    = json.loads(raw_fb.decode("utf-8"))
                            recovered  = []
                            for item in data_fb.get("data", []):
                                sym_fb = item["s"].replace("BITGET:", "").replace(".P", "")
                                if sym_fb in missing:
                                    result[sym_fb] = dict(zip(columns, item["d"]))
                                    recovered.append(sym_fb)
                            still_missing = [s for s in missing if s not in result]
                            LOG.info(f"  ✅  TV BITGET: recuperados {len(recovered)}: {recovered} | {elapsed_fb:.2f}s")
                            if still_missing:
                                LOG.warning(f"  ⚠️  TV sem dados (ambos prefixos): {still_missing}")
                    except Exception as e_fb:
                        LOG.warning(f"  ⚠️  TV BITGET: fallback falhou: {type(e_fb).__name__}: {e_fb}")

                return result

        except Exception as e:
            LOG.warning(f"  ⚠️  TV batch tentativa {attempt+1}/{retries}: {type(e).__name__}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))

    LOG.error(f"  ❌  TV batch falhou após {retries} tentativas")
    return {}

# ===========================================================================
# HELPERS
# ===========================================================================

def sf(val, default=0.0):
    try: return float(val) if val is not None and val != "" else default
    except: return default

# Headers para Bitget — desabilita Brotli explicitamente.
# O aiohttp negocia "br" por padrão; sem pacote brotli instalado,
# a decodificação falha silenciosamente. Forçar gzip/deflate resolve.
BITGET_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Accept":          "application/json",
    "User-Agent":      "Mozilla/5.0",
}

async def api_get_async(session, url, retries=3, headers=None):
    """
    GET assíncrono com decodificação explícita e log completo.
    Loga: URL, tentativa, status HTTP, tamanho da resposta, erros.
    """
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            t0 = time.time()
            async with session.get(url, timeout=20, headers=headers) as resp:
                elapsed = time.time() - t0
                raw     = await resp.read()
                status  = resp.status
                size_kb = len(raw) / 1024
                encoding = resp.headers.get("Content-Encoding", "none")

                LOG.debug(f"  GET {short_url}")
                LOG.debug(f"      → HTTP {status} | {size_kb:.1f}KB | enc:{encoding} | {elapsed:.2f}s")

                if status != 200:
                    LOG.warning(f"  ⚠️  HTTP {status} para {short_url}")
                    if i < retries - 1:
                        await asyncio.sleep(2)
                        continue
                    return None

                data = json.loads(raw.decode("utf-8"))
                return data

        except asyncio.TimeoutError:
            LOG.warning(f"  ⏱️  Timeout (tentativa {i+1}/{retries}): {short_url}")
        except json.JSONDecodeError as e:
            LOG.error(f"  ❌  JSON inválido: {e} | URL: {short_url}")
            return None
        except Exception as e:
            LOG.warning(f"  ⚠️  Erro tentativa {i+1}/{retries}: {type(e).__name__}: {e}")

        if i < retries - 1:
            wait = 2 ** (i + 1)
            LOG.debug(f"  ↻  Aguardando {wait}s antes de retry...")
            await asyncio.sleep(wait)

    LOG.error(f"  ❌  Falha após {retries} tentativas: {short_url}")
    return None

def api_get(url, retries=3):
    """GET síncrono (usado para fetch_perpetuals na inicialização)."""
    short_url = url[:80] + ("..." if len(url) > 80 else "")
    for i in range(retries):
        try:
            t0   = time.time()
            resp = requests.get(url, timeout=20,
                                headers={"Accept-Encoding": "gzip, deflate"})
            elapsed = time.time() - t0
            LOG.debug(f"  GET(sync) {short_url} → HTTP {resp.status_code} | {elapsed:.2f}s")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            LOG.warning(f"  ⚠️  api_get tentativa {i+1}/{retries}: {e}")
            if i < retries - 1: time.sleep(2)
            else:
                LOG.error(f"  ❌  api_get falhou: {short_url}")
                raise

async def fetch_klines_async(session, symbol, granularity="15m", limit=60):
    """
    [v5.2] Busca klines com fallback OKX quando Bitget retorna 400.

    Fluxo:
      1. Tenta Bitget (comportamento original)
      2. Se Bitget retorna HTTP 400 (símbolo desconhecido), tenta OKX
         ex: BEATUSDT → BEAT-USDT-SWAP na OKX
      3. Loga claramente qual fonte forneceu os klines

    Mapeamento de granularidade Bitget → OKX:
      15m → 15m | 1H → 1H | 4H → 4H (formato idêntico)

    HTTP 400 da Bitget = símbolo existe na OKX mas não na Bitget.
    Isso ocorre com tokens listados exclusivamente na OKX (ex: BEAT, LIGHT).
    """
    # --- Tentativa 1: Bitget ---
    url_bitget = (f"https://api.bitget.com/api/v2/mix/market/candles"
                  f"?productType=USDT-FUTURES&symbol={symbol}"
                  f"&granularity={granularity}&limit={limit}")
    try:
        data = await api_get_async(session, url_bitget, headers=BITGET_HEADERS)

        if data is None:
            # api_get_async retorna None em HTTP != 200 (inclui 400)
            # Verificar se é 400 pelo log já feito — tentar OKX
            LOG.debug(f"  🔄  [{symbol} {granularity}] Bitget sem resposta → tentando OKX")
        elif "data" not in data:
            LOG.error(f"  ❌  Klines {symbol} {granularity}: campo 'data' ausente na Bitget")
            LOG.debug(f"      Resposta recebida: {str(data)[:120]}")
        else:
            raw_candles = data["data"]
            if raw_candles:
                result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                           "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                          for c in raw_candles]
                result.reverse()
                ts_ini = datetime.fromtimestamp(result[0]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
                ts_fim = datetime.fromtimestamp(result[-1]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
                LOG.debug(f"  ✅  Klines {symbol} {granularity}: {len(result)} candles | {ts_ini} → {ts_fim}")
                return result
            else:
                LOG.warning(f"  ⚠️  Klines {symbol} {granularity}: 'data' vazio na Bitget")

    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async Bitget {symbol} {granularity}: {type(e).__name__}: {e}")

    # --- Tentativa 2: OKX fallback [v5.2 FIX] ---
    # Converte símbolo: BEATUSDT → BEAT-USDT-SWAP
    # Converte granularidade: 15m→15m | 1H→1H | 4H→4H
    base_coin  = symbol.replace("USDT", "")
    okx_instid = f"{base_coin}-USDT-SWAP"
    okx_bar    = granularity  # OKX usa mesmo formato: 15m, 1H, 4H
    url_okx    = (f"https://www.okx.com/api/v5/market/candles"
                  f"?instId={okx_instid}&bar={okx_bar}&limit={limit}")

    LOG.info(f"  🔄  [v5.2] Klines {symbol} {granularity}: Bitget falhou → tentando OKX ({okx_instid})")
    try:
        data_okx = await api_get_async(session, url_okx)
        if data_okx and "data" in data_okx and data_okx["data"]:
            raw = data_okx["data"]
            # OKX candles: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
            result = [{"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                       "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
                      for c in raw]
            result.reverse()
            ts_ini = datetime.fromtimestamp(result[0]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
            ts_fim = datetime.fromtimestamp(result[-1]["ts"]/1000, BRT).strftime("%m/%d %H:%M")
            LOG.info(f"  ✅  Klines {symbol} {granularity} via OKX: {len(result)} candles | {ts_ini} → {ts_fim}")
            return result
        else:
            LOG.warning(f"  ⚠️  Klines {symbol} {granularity}: OKX também sem dados — token descartado")
            return []

    except Exception as e:
        LOG.error(f"  ❌  fetch_klines_async OKX {symbol} {granularity}: {type(e).__name__}: {e}")
        return []

async def fetch_klines_cached_async(session, symbol, granularity="4H", limit=60):
    """
    Klines com cache local + log completo de diagnóstico de cache.
    [v4.7 FIX] Não grava cache com lista vazia.
    [v4.7 FIX] Invalida cache que contenha lista vazia (cache corrompido).
    [v4.7 LOG] Loga: HIT/MISS/CORROMPIDO, idade do cache, candles encontrados.
    """
    cache_dir  = "/tmp/atirador_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = f"{cache_dir}/klines_{symbol}_{granularity}.json"

    if os.path.exists(cache_file):
        age_h   = (time.time() - os.path.getmtime(cache_file)) / 3600
        age_min = age_h * 60
        try:
            with open(cache_file) as f:
                cached = json.load(f)

            # [v4.7 FIX] Cache corrompido: lista vazia gravada por versão anterior
            if not cached:
                LOG.warning(f"  🗑️  Cache CORROMPIDO {symbol} {granularity}: "
                            f"arquivo contém lista vazia — descartando e rebuscando")
                os.remove(cache_file)
                # Cai para o fetch abaixo

            elif age_h >= KLINE_CACHE_TTL_H:
                LOG.debug(f"  ⏰  Cache EXPIRADO {symbol} {granularity}: "
                          f"{age_min:.0f}min > {KLINE_CACHE_TTL_H*60:.0f}min — rebuscando")
                # Cai para o fetch abaixo

            else:
                LOG.debug(f"  💾  Cache HIT {symbol} {granularity}: "
                          f"{len(cached)} candles | idade {age_min:.0f}min")
                return cached

        except (json.JSONDecodeError, Exception) as e:
            LOG.warning(f"  ⚠️  Cache INVÁLIDO {symbol} {granularity}: {e} — rebuscando")

    else:
        LOG.debug(f"  📡  Cache MISS {symbol} {granularity}: arquivo não existe — buscando")

    klines = await fetch_klines_async(session, symbol, granularity, limit)

    if klines:
        try:
            with open(cache_file, "w") as f:
                json.dump(klines, f)
            LOG.debug(f"  💾  Cache GRAVADO {symbol} {granularity}: {len(klines)} candles")
        except Exception as e:
            LOG.warning(f"  ⚠️  Falha ao gravar cache {symbol} {granularity}: {e}")
    else:
        # [v4.7 FIX] NÃO grava cache vazio — evita corromper para próximas execuções
        LOG.warning(f"  🚫  Cache NÃO gravado {symbol} {granularity}: "
                    f"klines vazios — próxima execução rebuscará da API")

    return klines

# ===========================================================================
# DADOS DE MERCADO — Hierarquia de Fontes [v5.3]
# ===========================================================================
# Histórico de problemas com Fonte 1:
#   v5.0 Bybit:       geo-block Brasil (HTTP 403 / timeout 92s)
#   v5.1 CoinGecko:   parser incompatível (21.094 itens, todos rejeitados)
#   v5.2 CoinGecko/exchange-specific: HTTP 404 (IDs errados + endpoint pago)
#   v5.3 Gate.io:     confirmada acessível do Brasil ✅ (teste 22/03/2026)
#
# Pesquisa exaustiva de fontes (22/03/2026):
#   Binance fapi: geo-block Brasil (igual à Bybit)
#   CoinGlass:    pago (sem free tier com API)
#   Sem fonte gratuita de dados AGREGADOS acessível do Brasil.
#   Decisão: melhor fonte LOCAL = Gate.io.
#
# Hierarquia final: Gate.io → OKX → Bitget
#
# Gate.io requer 2 chamadas:
#   /futures/usdt/tickers  → volume, OI (contratos), preço, funding
#   /futures/usdt/contracts → quanto_multiplier para converter OI a USD
# O /contracts é cacheado por 24h (muda raramente).
#
# Normalização crítica: Gate usa BTC_USDT → normalizar para BTCUSDT
# (TV Scanner e klines Bitget/OKX usam BTCUSDT sem separador)
# ===========================================================================

TICKER_TIMEOUT = 8   # segundos por fonte — cai imediatamente se não responder

DATA_SOURCE          = "desconhecida"
DATA_SOURCE_ATTEMPTS = []

# Cache de quanto_multiplier da Gate.io (válido por 24h)
_GATE_MULTIPLIERS      = {}
_GATE_MULTIPLIERS_TS   = 0.0
_GATE_MULTIPLIERS_TTL  = 86400   # 24 horas em segundos

def _log_source_attempt(fonte, url, status, elapsed, tokens_brutos,
                        qualificados, motivo_falha=None):
    """
    Registra diagnóstico completo de cada tentativa de fonte.
    Fundamental para debug de bloqueios e monitoramento de estabilidade.
    """
    entrada = {
        "fonte"        : fonte,
        "url"          : url[:80],
        "status"       : status,
        "elapsed_s"    : round(elapsed, 2),
        "tokens_brutos": tokens_brutos,
        "qualificados" : qualificados,
        "falha"        : motivo_falha,
    }
    DATA_SOURCE_ATTEMPTS.append(entrada)
    if motivo_falha:
        LOG.warning(f"  ⛔  [{fonte}] FALHOU | HTTP {status} | "
                    f"{elapsed:.2f}s | motivo: {motivo_falha}")
        LOG.warning(f"      URL: {url[:100]}")
    else:
        LOG.info(f"  ✅  [{fonte}] OK | HTTP {status} | {elapsed:.2f}s | "
                 f"{tokens_brutos} brutos → {qualificados} qualificados")


def _fetch_gate_multipliers():
    """
    [v5.3] Busca e cacheia o quanto_multiplier de cada contrato Gate.io.

    quanto_multiplier converte OI de contratos para USD:
      oi_usd = total_size × mark_price × quanto_multiplier

    Valores variam por ativo (ex: BTC pode ser 0.0001, USDT-settled pode ser 1).
    Cache de 24h evita chamada extra a cada execução (muda raramente).
    Retorna dict: {"BTCUSDT": 0.0001, "ETHUSDT": 0.01, ...}
    """
    global _GATE_MULTIPLIERS, _GATE_MULTIPLIERS_TS

    agora = time.time()
    if _GATE_MULTIPLIERS and (agora - _GATE_MULTIPLIERS_TS) < _GATE_MULTIPLIERS_TTL:
        age_h = (agora - _GATE_MULTIPLIERS_TS) / 3600
        LOG.debug(f"  💾  [Gate.io/contracts] Cache HIT | {len(_GATE_MULTIPLIERS)} contratos | "
                  f"idade {age_h:.1f}h")
        return _GATE_MULTIPLIERS

    LOG.debug("  📡  [Gate.io/contracts] Buscando quanto_multiplier...")
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    t0  = time.time()
    try:
        resp    = requests.get(url, timeout=TICKER_TIMEOUT,
                               headers={"Accept-Encoding": "gzip, deflate",
                                        "User-Agent": "Mozilla/5.0 (compatible; scanner/6.1.2)"})
        elapsed = time.time() - t0
        LOG.debug(f"       → HTTP {resp.status_code} | {len(resp.content)/1024:.1f}KB | {elapsed:.2f}s")

        if resp.status_code == 200:
            contratos = resp.json()
            mults = {}
            for c in contratos:
                nome = c.get("name", "")
                # Normaliza BTC_USDT → BTCUSDT
                sym = nome.replace("_", "")
                mult = sf(c.get("quanto_multiplier", 1.0))
                if mult <= 0:
                    mult = 1.0
                if sym.endswith("USDT"):
                    mults[sym] = mult
            _GATE_MULTIPLIERS    = mults
            _GATE_MULTIPLIERS_TS = agora
            LOG.debug(f"  💾  [Gate.io/contracts] Cache GRAVADO | {len(mults)} contratos")
            return mults
        else:
            LOG.warning(f"  ⚠️  [Gate.io/contracts] HTTP {resp.status_code} — usando multiplier=1 como fallback")
            return {}
    except Exception as e:
        elapsed = time.time() - t0
        LOG.warning(f"  ⚠️  [Gate.io/contracts] Erro após {elapsed:.1f}s: {type(e).__name__}: {e}")
        return {}


def _parse_gateio_tickers(items, multipliers):
    """
    [v5.3] Normaliza tickers Gate.io para estrutura interna padrão.

    Gate.io campos:
      contract           → "BTC_USDT" (normalizar → "BTCUSDT")
      volume_24h_quote   → volume 24h em USDT direto
      last               → preço atual
      mark_price         → mark price (usado para cálculo de OI)
      total_size         → OI em contratos (× mark_price × quanto_multiplier = USD)
      funding_rate       → taxa de financiamento decimal
      change_percentage  → variação % 24h (ex: -7.76 = -7.76%)

    OI em USD = total_size × mark_price × quanto_multiplier
    Para contratos onde quanto_multiplier não foi obtido, usa 1.0 como fallback.
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0

    for t in items:
        contrato = t.get("contract", "")
        # Normaliza BTC_USDT → BTCUSDT (crítico para TV Scanner e klines)
        sym = contrato.replace("_", "")
        if not sym.endswith("USDT"):
            rej_symbol += 1; continue

        turnover = sf(t.get("volume_24h_quote", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue

        price      = sf(t.get("last", 0) or t.get("mark_price", 0))
        mark_price = sf(t.get("mark_price", 0) or price)
        if price <= 0:
            rej_symbol += 1; continue

        # OI: total_size (contratos) × mark_price × quanto_multiplier
        total_size = sf(t.get("total_size", 0))
        mult       = multipliers.get(sym, 1.0)
        oi_usd     = total_size * mark_price * mult

        # [v6.3.0 A6] Separar OI real de OI estimado
        oi_estimado = False
        if oi_usd <= 0:
            oi_usd      = turnover * 0.1
            oi_estimado = True   # dado inferido — não deve gerar alerta acionável

        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue

        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "oi_estimado"     : oi_estimado,   # [v6.3.0 A6]
            "volume_24h"      : turnover,
            "funding_rate"    : sf(t.get("funding_rate", 0)),
            "price_change_24h": sf(t.get("change_percentage", 0)),
        })
    return qualified, rej_vol, rej_oi


def _fetch_okx_tickers_with_oi():
    """
    [v6.4.1] Busca tickers + OI da OKX em duas requisições paralelas.
    Mescla os dados antes de passar ao parser.
    
    Endpoints:
      1. /api/v5/market/tickers?instType=SWAP — tickers (volume, preço)
      2. /api/v5/public/open-interest?instType=SWAP — OI em USD
    
    Retorna lista de tickers enriquecida com oiUsd real (não estimado).
    """
    try:
        t0 = time.time()
        
        # Requisição 1: Tickers
        LOG.debug("  📡  [OKX] Buscando tickers...")
        tickers_resp = requests.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=TICKER_TIMEOUT,
            headers={"Accept-Encoding": "gzip, deflate",
                     "User-Agent": "Mozilla/5.0 (compatible; scanner/6.1.2)"}
        )
        tickers_resp.raise_for_status()
        tickers_data = tickers_resp.json().get("data", [])
        LOG.debug(f"     ✅ Tickers: {len(tickers_data)} itens em {time.time()-t0:.2f}s")
        
        # Requisição 2: Open Interest
        LOG.debug("  📡  [OKX] Buscando Open Interest...")
        oi_resp = requests.get(
            "https://www.okx.com/api/v5/public/open-interest?instType=SWAP",
            timeout=TICKER_TIMEOUT,
            headers={"Accept-Encoding": "gzip, deflate",
                     "User-Agent": "Mozilla/5.0 (compatible; scanner/6.1.2)"}
        )
        oi_resp.raise_for_status()
        oi_data = oi_resp.json().get("data", [])
        LOG.debug(f"     ✅ OI: {len(oi_data)} itens em {time.time()-t0:.2f}s")
        
        # Mesclar: criar dicionário OI por instId
        oi_dict = {item["instId"]: item for item in oi_data}
        
        # Adicionar OI aos tickers
        merged_count = 0
        for ticker in tickers_data:
            inst_id = ticker.get("instId")
            if inst_id in oi_dict:
                ticker["oiUsd"] = float(oi_dict[inst_id]["oiUsd"])
                ticker["oi_real"] = True  # Flag para indicar OI real
                merged_count += 1
            else:
                # Fallback apenas se OI não encontrado (raro)
                ticker["oiUsd"] = 0
                ticker["oi_real"] = False
        
        # Requisição 3: Funding Rates reais [v6.6.2]
        # OKX /market/tickers não inclui fundingRate — endpoint dedicado necessário.
        # Busca apenas os instIds que passarão nos filtros (todo o universo SWAP).
        LOG.debug("  📡  [OKX] Buscando Funding Rates reais...")
        # Filtro rápido: só busca FR para tokens USDT-SWAP com volume mínimo
        # (evita 300 chamadas desnecessárias para tokens que serão filtrados)
        # Usa os instIds diretamente da lista de tickers já obtida
        swap_instids = [t.get("instId") for t in tickers_data
                        if t.get("instId", "").endswith("-USDT-SWAP")]
        fr_map = _fetch_okx_funding_rates(swap_instids)
        LOG.debug(f"     ✅ FR: {len(fr_map)} tokens com funding rate em {time.time()-t0:.2f}s")

        # Injetar FR nos tickers antes de retornar
        for ticker in tickers_data:
            inst_id = ticker.get("instId", "")
            sym_internal = inst_id.replace("-USDT-SWAP", "") + "USDT"
            if sym_internal in fr_map:
                ticker["fundingRate"] = fr_map[sym_internal]
            # Se não encontrado, deixa como 0 (comportamento anterior)

        LOG.debug(f"     ✅ Mesclados: {merged_count}/{len(tickers_data)} com OI real em {time.time()-t0:.2f}s")
        return tickers_data
        
    except requests.exceptions.Timeout as e:
        LOG.error(f"  ❌ [OKX] Timeout ao buscar tickers+OI: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        LOG.error(f"  ❌ [OKX] Erro de conexão: {e}")
        return None
    except Exception as e:
        LOG.error(f"  ❌ [OKX] Erro ao buscar tickers+OI: {type(e).__name__}: {e}")
        return None


def _fetch_okx_funding_rates(symbols_okx: list) -> dict:
    """
    [v6.6.2] Busca Funding Rates reais da OKX via endpoint dedicado.

    Problema identificado: /api/v5/market/tickers?instType=SWAP NÃO inclui
    o campo fundingRate no payload de retorno — campo sempre ausente/zero.
    Solução: endpoint específico /api/v5/public/funding-rate retorna FR real,
    mas aceita apenas 1 instId por chamada → busca em batch assíncrono.

    Para manter o script síncrono nesta etapa (fetch_perpetuals é síncrono),
    usamos requests em sequência com timeout curto (2s por símbolo).
    O overhead é aceitável: ~300 tokens × 0.3s = 90s no pior caso,
    mas na prática a maioria é filtrada antes (43 qualificados nesta rodada).
    Para escalabilidade futura, mover para async junto ao pipeline principal.

    Alternativa: endpoint /api/v5/public/funding-rate-summary (não documentado).
    Usar endpoint oficial por segurança.

    Retorna: {"BTCUSDT": 0.0001, "ETHUSDT": -0.0002, ...}
    """
    fr_map = {}
    base_url = "https://www.okx.com/api/v5/public/funding-rate"
    headers = {"Accept-Encoding": "gzip, deflate",
               "User-Agent": "Mozilla/5.0 (compatible; scanner/6.6.2)"}
    n_ok = 0; n_err = 0
    for sym_okx in symbols_okx:
        # sym_okx já está no formato "BTC-USDT-SWAP"
        try:
            resp = requests.get(
                f"{base_url}?instId={sym_okx}",
                timeout=3,
                headers=headers
            )
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    fr = sf(data[0].get("fundingRate", 0))
                    # Normalizar instId → símbolo interno: BTC-USDT-SWAP → BTCUSDT
                    sym_internal = sym_okx.replace("-USDT-SWAP", "") + "USDT"
                    fr_map[sym_internal] = fr
                    n_ok += 1
                else:
                    n_err += 1
            else:
                n_err += 1
        except Exception:
            n_err += 1
    if LOG:
        LOG.debug(f"  📡  [OKX FR] Funding rates obtidos: {n_ok}/{len(symbols_okx)} | erros: {n_err}")
    return fr_map


    """
    Normaliza tickers da OKX /v5/market/tickers?instType=SWAP.
    instId: "BTC-USDT-SWAP" → "BTCUSDT"
    volCcy24h: volume em USDT | oiUsd: Open Interest em USD (real, não estimado) [v6.4.1]
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0
    for t in items:
        inst = t.get("instId", "")
        if not inst.endswith("-USDT-SWAP"):
            rej_symbol += 1; continue
        sym      = inst.replace("-USDT-SWAP", "") + "USDT"
        base     = sym.replace("USDT", "")
        turnover = sf(t.get("volCcy24h", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue
        price  = sf(t.get("last", 0))
        
        # [v6.4.1] Usar oiUsd real do endpoint /public/open-interest
        oi_usd      = sf(t.get("oiUsd", 0))
        oi_real     = t.get("oi_real", False)  # Flag indicando se é real
        oi_estimado = not oi_real  # Estimado apenas se não for real
        
        if oi_usd == 0:
            oi_usd      = turnover * 0.1
            oi_estimado = True   # [v6.3.0 A6] — fallback apenas se OI for zero
        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "oi_estimado"     : oi_estimado,   # [v6.3.0 A6]
            "volume_24h"      : turnover,
            "funding_rate"    : sf(t.get("fundingRate", 0)),
            "price_change_24h": sf(t.get("chg24h", 0)) * 100,
        })
    return qualified, rej_vol, rej_oi


def _parse_bitget_tickers(items):
    """
    Normaliza tickers Bitget (comportamento original v4.9).
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0
    for t in items:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            rej_symbol += 1; continue
        turnover = sf(t.get("usdtVolume"))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue
        price   = sf(t.get("lastPr"))
        holding = sf(t.get("holdingAmount"))
        oi_usd  = holding * price
        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue
        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "oi_estimado"     : False,   # [v6.3.0 A6] Bitget OI é sempre real (holdingAmount)
            "volume_24h"      : sf(t.get("baseVolume")),
            "funding_rate"    : sf(t.get("fundingRate")),
            "price_change_24h": sf(t.get("change24h")) * 100,
        })
    return qualified, rej_vol, rej_oi


def _parse_okx_tickers(items):
    """
    [v6.6.2] Normaliza tickers OKX (já mesclados com OI real por _fetch_okx_tickers_with_oi).

    Campos OKX relevantes (após mesclagem):
      instId      → "BTC-USDT-SWAP" → normaliza para "BTCUSDT"
      last        → preço atual
      open24h     → preço de abertura 24h atrás (para calcular variação %)
      volCcy24h   → volume 24h em USDT (turnover)
      oiUsd       → OI em USD (injetado pelo fetch; real ou 0 como fallback)
      oi_real     → bool indicando se o OI é real ou estimado
      fundingRate → taxa de financiamento (injetada pelo fetch via endpoint dedicado)
    """
    qualified  = []
    rej_symbol = rej_vol = rej_oi = 0

    for t in items:
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"):
            rej_symbol += 1; continue

        # Normaliza "BTC-USDT-SWAP" → "BTCUSDT"
        sym = inst_id.replace("-USDT-SWAP", "") + "USDT"

        turnover = sf(t.get("volCcy24h", 0))
        if turnover < MIN_TURNOVER_24H:
            rej_vol += 1; continue

        price = sf(t.get("last", 0))
        if price <= 0:
            rej_symbol += 1; continue

        oi_usd  = sf(t.get("oiUsd", 0))
        oi_real = t.get("oi_real", False)

        # Fallback estimado se OI não veio ou é zero
        oi_estimado = False
        if oi_usd <= 0:
            oi_usd      = turnover * 0.1
            oi_estimado = True

        if oi_usd < MIN_OI_USD:
            rej_oi += 1; continue

        # Variação % 24h: (last - open24h) / open24h * 100
        open24h = sf(t.get("open24h", 0))
        price_change = ((price - open24h) / open24h * 100) if open24h > 0 else 0.0

        base = sym.replace("USDT", "")
        qualified.append({
            "symbol"          : sym,
            "base_coin"       : base,
            "price"           : price,
            "turnover_24h"    : turnover,
            "oi_usd"          : oi_usd,
            "oi_estimado"     : oi_estimado and not oi_real,
            "volume_24h"      : sf(t.get("vol24h", 0)),
            "funding_rate"    : sf(t.get("fundingRate", 0)),
            "price_change_24h": price_change,
        })
    return qualified, rej_vol, rej_oi


def _try_source(nome, url, parse_fn, extract_fn, timeout=None, parse_kwargs=None):
    """
    Tenta buscar tickers de uma fonte com diagnóstico completo.
    Retorna (qualified, total_brutos) em caso de sucesso, ou None em falha.
    parse_kwargs: argumentos extras para parse_fn (ex: multipliers da Gate.io)
    """
    t_used = timeout or TICKER_TIMEOUT
    LOG.info(f"  📡  [{nome}] Tentando: {url[:80]}{'...' if len(url) > 80 else ''}")
    LOG.debug(f"       timeout={t_used}s | filtros: vol≥${MIN_TURNOVER_24H/1e6:.1f}M OI≥${MIN_OI_USD/1e6:.0f}M")
    t0 = time.time()
    try:
        resp    = requests.get(url, timeout=t_used,
                               headers={"Accept-Encoding": "gzip, deflate",
                                        "User-Agent": "Mozilla/5.0 (compatible; scanner/6.1.2)"})
        elapsed = time.time() - t0
        status  = resp.status_code
        size_kb = len(resp.content) / 1024
        LOG.debug(f"       → HTTP {status} | {size_kb:.1f}KB | {elapsed:.2f}s")

        if status != 200:
            motivo = (f"HTTP {status} — "
                      f"{'Geo-block/Forbidden' if status == 403 else 'Not Found' if status == 404 else 'Erro servidor' if status >= 500 else 'Erro cliente'}")
            _log_source_attempt(nome, url, status, elapsed, 0, 0, motivo)
            return None

        data  = resp.json()
        items = extract_fn(data)
        if not items:
            motivo = "Resposta JSON vazia ou sem campo esperado"
            _log_source_attempt(nome, url, status, elapsed, 0, 0, motivo)
            return None

        kwargs    = parse_kwargs or {}
        qualified, rej_vol, rej_oi = parse_fn(items, **kwargs)
        LOG.debug(f"       Rejeitados: {rej_vol} vol<${MIN_TURNOVER_24H/1e6:.1f}M | "
                  f"{rej_oi} OI<${MIN_OI_USD/1e6:.0f}M")
        top5 = [d['base_coin'] for d in sorted(qualified, key=lambda x: x['turnover_24h'], reverse=True)[:5]]
        LOG.debug(f"       TOP 5 por volume: {top5}")

        if not qualified:
            motivo = f"Nenhum token passou os filtros ({len(items)} brutos, todos rejeitados)"
            _log_source_attempt(nome, url, status, elapsed, len(items), 0, motivo)
            return None

        _log_source_attempt(nome, url, status, elapsed, len(items), len(qualified))
        return qualified, len(items)

    except requests.exceptions.Timeout:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"Timeout após {elapsed:.1f}s (limite={t_used}s)")
        return None
    except requests.exceptions.ConnectionError as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"Erro de conexão: {str(e)[:80]}")
        return None
    except (json.JSONDecodeError, ValueError) as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"JSON inválido: {str(e)[:80]}")
        return None
    except Exception as e:
        elapsed = time.time() - t0
        _log_source_attempt(nome, url, 0, elapsed, 0, 0, f"{type(e).__name__}: {str(e)[:80]}")
        return None



# ===========================================================================
# TRAVA DE CANDLE FECHADO [v6.3.0 — A4]
# ===========================================================================
# O gatilho de entrada usa timeframe 15m. Se o scanner rodar quando a vela
# atual ainda está em formação, BB, volume e padrões de candle são lidos
# com dados parciais — distorcendo o sinal.
#
# Lógica: o candle 15m fecha nos minutos 0, 15, 30, 45 de cada hora.
# Uma vela está "fechada e resfriada" se o scanner roda DEPOIS de
# CANDLE_CLOSED_GRACE_S segundos do fechamento. Isso garante que a API
# já propagou os dados finais da vela.
#
# Comportamento:
#   • Candle fechado (>= grace) → prossegue normalmente
#   • Candle muito novo (< grace) → usa o candle anterior (penúltimo)
#     para cálculo de BB e padrões; ATR usa média normal
#   • O log sempre indica qual candle foi usado e quanto tempo decorreu
#
# Em produção com cron a cada 30min alinhado em :00/:30, o grace de 60s
# é suficiente. Com cron não alinhado, pode precisar de 90–120s.

CANDLE_15M_SECONDS   = 900    # 15 minutos em segundos
CANDLE_CLOSED_GRACE_S = 60    # segundos após fechamento para garantir propagação

def get_candle_lock_status() -> dict:
    """
    [v6.3.0 A4] Verifica se o candle 15m atual está fechado e propagado.

    Retorna dict com:
      closed      : bool  — True se pode usar último candle
      use_prev    : bool  — True se deve usar penúltimo candle (vela em formação)
      seconds_open: float — segundos desde que o candle atual abriu
      seconds_ago : float — segundos desde o último fechamento
      next_close  : float — segundos até o próximo fechamento
      ts_last_close: str  — timestamp BRT do último fechamento
    """
    now_ts    = time.time()
    # Segundos decorridos desde o último múltiplo de 15min (em UTC)
    seconds_in_period = now_ts % CANDLE_15M_SECONDS
    seconds_since_close = seconds_in_period  # == 0 exatamente no fechamento

    closed     = seconds_since_close >= CANDLE_CLOSED_GRACE_S
    use_prev   = not closed
    next_close = CANDLE_15M_SECONDS - seconds_since_close

    ts_last = datetime.fromtimestamp(
        now_ts - seconds_since_close, BRT
    ).strftime("%H:%M:%S BRT")

    return {
        "closed"       : closed,
        "use_prev"     : use_prev,
        "seconds_open" : seconds_since_close,
        "seconds_ago"  : seconds_since_close,
        "next_close"   : next_close,
        "ts_last_close": ts_last,
    }


def apply_candle_lock(candles_15m: list, lock: dict) -> list:
    """
    [v6.3.0 A4] Aplica a trava de candle fechado à lista de klines 15m.

    Se lock["use_prev"] == True, remove o último candle (em formação) antes
    de passar para os pilares de score. Isso garante que BB position,
    padrões de candle e volume usem apenas velas completamente fechadas.

    Retorna a lista ajustada (nunca modifica a original).
    """
    if not candles_15m or len(candles_15m) < 2:
        return candles_15m
    if lock["use_prev"]:
        return candles_15m[:-1]   # descarta última vela (em formação)
    return candles_15m

def fetch_perpetuals():
    """
    [v6.4.0] Busca perpetuals USDT — hierarquia OKX → Gate.io → Bitget.

    ╔══════════════════════════════════════════════════════════════════╗
    ║  NOTA TÉCNICA — LIMITAÇÃO RECONHECIDA                           ║
    ║  A solução ideal seria a API da CoinGlass (coinglass.com),      ║
    ║  que retorna OI AGREGADO de 30+ exchanges em 1 chamada.         ║
    ║  Não implementada: sem plano gratuito com acesso à API.         ║
    ║  Planos a partir de ~$35/mês (Hobby). Ver docstring do módulo.  ║
    ╚══════════════════════════════════════════════════════════════════╝

    OKX (Fonte 1): melhor universo disponível gratuitamente no Brasil.
      100+ tokens qualificados. Gates técnicos filtram meme coins.
    Gate.io (Fonte 2): fallback com boa qualidade de TOP 5, mas universo
      reduzido (~22 qualificados vs ~100 OKX). Confirmada acessível do BR.
    Bitget (Fonte 3): último recurso, estável desde v4.x.

    CoinGecko: REMOVIDA (nunca funcionou em produção — v5.1 e v5.2).
    Bybit/Binance: geo-block Brasil confirmado — não implementadas.
    """
    global DATA_SOURCE, DATA_SOURCE_ATTEMPTS
    DATA_SOURCE_ATTEMPTS = []

    LOG.info("📡 [v6.6.2] Iniciando busca de tickers — hierarquia OKX → Gate.io → Bitget")
    LOG.info(f"   Filtros: vol≥${MIN_TURNOVER_24H/1e6:.1f}M | OI≥${MIN_OI_USD/1e6:.0f}M | timeout={TICKER_TIMEOUT}s/fonte")
    LOG.info("   [Nota] Fonte ideal seria CoinGlass API (pago ~$35/mês) — ver docstring")

    # ------------------------------------------------------------------
    # FONTE 1: OKX [v6.4.1]
    # Melhor universo gratuito disponível no Brasil (~100 qualificados).
    # Meme coins no top são filtrados pelos gates técnicos (4H/1H).
    # [v6.4.1] Busca tickers + OI em dois endpoints e mescla os dados.
    # ------------------------------------------------------------------
    LOG.info("  📡  [OKX] Tentando com tickers + OI real...")
    tickers_with_oi = _fetch_okx_tickers_with_oi()
    if tickers_with_oi:
        qualified, rej_vol, rej_oi = _parse_okx_tickers(tickers_with_oi)
        if qualified:
            DATA_SOURCE = "OKX"
            qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
            LOG.info(f"  ✅  [OKX] Fonte ativa (com OI real) | {len(tickers_with_oi)} brutos → {len(qualified)} qualificados")
            return qualified, len(tickers_with_oi)

    # ------------------------------------------------------------------
    # FONTE 2: Gate.io
    # Boa qualidade de TOP 5 (ETH/BTC/SOL no topo, sem meme coins).
    # Universo menor (~22 qualificados). Latência ~7s por chamada.
    # Requer 2 chamadas: tickers + contracts (contracts cacheado 24h).
    # ------------------------------------------------------------------
    multipliers = _fetch_gate_multipliers()
    resultado   = _try_source(
        nome         = "Gate.io",
        url          = "https://api.gateio.ws/api/v4/futures/usdt/tickers",
        parse_fn     = _parse_gateio_tickers,
        extract_fn   = lambda d: d if isinstance(d, list) else [],
        parse_kwargs = {"multipliers": multipliers},
    )
    if resultado:
        DATA_SOURCE = "Gate.io"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [Gate.io] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        LOG.warning("  ⚠️  [Gate.io] Universo reduzido (~22 qualificados vs ~100 OKX) — cobertura limitada")
        return qualified, total

    # ------------------------------------------------------------------
    # FONTE 3: Bitget — último recurso, estável desde v4.x
    # ------------------------------------------------------------------
    LOG.warning("  ⚠️  OKX e Gate.io indisponíveis — usando Bitget (último recurso)")
    resultado = _try_source(
        nome       = "Bitget",
        url        = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
        parse_fn   = _parse_bitget_tickers,
        extract_fn = lambda d: d.get("data", []),
        timeout    = 20,
    )
    if resultado:
        DATA_SOURCE = "Bitget"
        qualified, total = resultado
        qualified.sort(key=lambda x: x["turnover_24h"], reverse=True)
        LOG.info(f"  ✅  [Bitget] Fonte ativa | {total} brutos → {len(qualified)} qualificados")
        return qualified, total

    # ------------------------------------------------------------------
    # TODAS AS FONTES FALHARAM
    # ------------------------------------------------------------------
    DATA_SOURCE = "NENHUMA"
    LOG.error("  ❌  TODAS AS 3 FONTES FALHARAM — scan abortado")
    LOG.error("  Resumo de tentativas:")
    for a in DATA_SOURCE_ATTEMPTS:
        LOG.error(f"    [{a['fonte']}] HTTP {a['status']} | {a['elapsed_s']}s | {a['falha']}")
    LOG.error("  Ações sugeridas:")
    LOG.error("    1. Verificar conectividade de rede")
    LOG.error("    2. Testar: curl https://www.okx.com/api/v5/market/tickers?instType=SWAP")
    LOG.error("    3. Aguardar 15-30 min (possível rate-limit temporário)")
    raise RuntimeError("Todas as fontes de tickers falharam. Scan abortado.")

async def fetch_fear_greed_async(session):
    """Fear & Greed Index global."""
    LOG.debug("  Buscando Fear & Greed Index...")
    try:
        data = await api_get_async(session, "https://api.alternative.me/fng/?limit=1")
        if data and "data" in data:
            v = data["data"][0]
            fg = {"value": int(v["value"]), "classification": v["value_classification"]}
            LOG.info(f"  📊 Fear & Greed: {fg['value']} ({fg['classification']})")
            return fg
    except Exception as e:
        LOG.warning(f"  ⚠️  Fear & Greed falhou: {e}")
    LOG.warning("  ⚠️  Fear & Greed: usando fallback 50 (Neutral)")
    return {"value": 50, "classification": "Neutral"}

# ===========================================================================
# ANÁLISE TÉCNICA — UTILITÁRIOS
# ===========================================================================

def find_swing_points(candles, window=None):
    """
    Detecta swing highs e swing lows.
    [v4.9 FIX 2] Fallback automático: tenta window=5 primeiro.
    Se retornar < 3 pontos em qualquer lista, tenta window=3.
    Resolve P5 "dados insuficientes" e P-1H "longe de suportes"
    que ocorriam mesmo com 60 candles disponíveis.
    """
    if window is None: window = SWING_WINDOW

    def _detect(candles, w):
        if len(candles) < w * 2 + 1: return [], []
        highs = np.array([c["high"] for c in candles])
        lows  = np.array([c["low"]  for c in candles])
        sh, sl = [], []
        for i in range(w, len(candles) - w):
            if highs[i] == np.max(highs[i - w:i + w + 1]):
                sh.append({"index": i, "price": highs[i]})
            if lows[i]  == np.min(lows[i  - w:i + w + 1]):
                sl.append({"index": i, "price": lows[i]})
        return sh, sl

    sh, sl = _detect(candles, window)

    # Fallback: se qualquer lista tiver < 3 pontos e window > 3, tenta menor
    if (len(sh) < 3 or len(sl) < 3) and window > 3:
        sh_fb, sl_fb = _detect(candles, 3)
        # Usa o fallback apenas se trouxer mais pontos
        if len(sh_fb) >= len(sh): sh = sh_fb
        if len(sl_fb) >= len(sl): sl = sl_fb
        if len(sh_fb) >= 3 and len(sl_fb) >= 3:
            LOG.debug(f"    find_swing_points: fallback window=3 aplicado "
                      f"(window={window} insuficiente: {len(sh)}H/{len(sl)}L → "
                      f"{len(sh_fb)}H/{len(sl_fb)}L)")

    return sh, sl

def detect_order_blocks(candles):
    """
    Order Blocks bullish: último candle bearish antes de impulso ≥ OB_IMPULSE_PCT.
    Retorna lista de {'high', 'low', 'index'}.
    """
    obs = []
    n = OB_IMPULSE_N
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] >= c["open"]: continue          # precisa ser bearish
        ref = c["close"]
        if ref <= 0: continue
        max_close   = max(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (max_close - ref) / ref * 100
        if impulse_pct >= OB_IMPULSE_PCT:
            obs.append({
                "high" : max(c["open"], c["close"]),
                "low"  : min(c["open"], c["close"]),
                "index": i,
            })
    return obs


def detect_order_blocks_bearish(candles):
    """
    [v6.0/v6.1.2] Order Blocks bearish: último candle bullish antes de impulso de QUEDA
    ≥ OB_IMPULSE_PCT. Espelho de detect_order_blocks() para operações SHORT.
    Retorna lista de {'high', 'low', 'index'}.
    """
    obs = []
    n = OB_IMPULSE_N
    for i in range(len(candles) - n - 1):
        c = candles[i]
        if c["close"] <= c["open"]: continue          # precisa ser bullish
        ref = c["close"]
        if ref <= 0: continue
        min_close   = min(candles[j]["close"] for j in range(i + 1, i + n + 1))
        impulse_pct = (ref - min_close) / ref * 100   # queda em %
        if impulse_pct >= OB_IMPULSE_PCT:
            obs.append({
                "high" : max(c["open"], c["close"]),
                "low"  : min(c["open"], c["close"]),
                "index": i,
            })
    return obs

# ===========================================================================
# CAMADA 2 — PILAR 1H: SUPORTE / ORDER BLOCK em klines 1H  (LONG)
# ===========================================================================

def analyze_support_1h(candles_1h, current_price):
    """
    LONG — P-1H: preço perto de suporte (swing low ou OB bullish) no 1H.
    Pontuação máxima: 4 pts.
    """
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"

    sh, sl = find_swing_points(candles_1h)
    score   = 0
    details = []

    if sl:
        for s in reversed(sl):
            dist_pct = (current_price - s["price"]) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 2
                details.append(f"Suporte 1H em {s['price']:.4f} ({dist_pct:.2f}% abaixo)")
                break

    obs = detect_order_blocks(candles_1h)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = (current_price - ob_mid) / current_price * 100
            if -OB_PROXIMITY_PCT <= dist_pct <= OB_PROXIMITY_PCT:
                score += 2
                details.append(f"Order Block 1H ({ob['low']:.4f}–{ob['high']:.4f})")
                break

    if not details:
        return 0, "Preço longe de suportes no 1H"
    return min(score, 4), " | ".join(details)


def analyze_resistance_1h(candles_1h, current_price):
    """
    [v6.0/v6.1.2] SHORT — P-1H: preço perto de resistência (swing high ou OB bearish) no 1H.
    Espelho de analyze_support_1h() para operações SHORT.
    Pontuação máxima: 4 pts.
      +2  Preço abaixo de swing high recente (≤1% acima)
      +2  Preço perto de Order Block bearish (≤1.5% do meio)
    """
    if not candles_1h:
        return 0, "Klines 1H indisponíveis"

    sh, sl = find_swing_points(candles_1h)
    score   = 0
    details = []

    # Resistência por Swing High 1H — preço subiu até perto do topo recente
    if sh:
        for s in reversed(sh):
            dist_pct = (s["price"] - current_price) / current_price * 100
            if 0 < dist_pct <= SR_PROXIMITY_PCT:
                score += 2
                details.append(f"Resistência 1H em {s['price']:.4f} ({dist_pct:.2f}% acima)")
                break

    # Order Block bearish 1H
    obs = detect_order_blocks_bearish(candles_1h)
    if obs:
        for ob in reversed(obs[-10:]):
            ob_mid   = (ob["high"] + ob["low"]) / 2
            dist_pct = abs(current_price - ob_mid) / current_price * 100
            if dist_pct <= OB_PROXIMITY_PCT:
                score += 2
                details.append(f"OB Bearish 1H ({ob['low']:.4f}–{ob['high']:.4f})")
                break

    if not details:
        return 0, "Preço longe de resistências no 1H"
    return min(score, 4), " | ".join(details)

# ===========================================================================
# CAMADA 3 — PILARES 15m
# ===========================================================================

# P1 — Bollinger Bands 15m  (LONG: banda inferior | SHORT: banda superior)
def score_bollinger(d, direction="LONG"):
    """
    Posição do preço no canal de Bollinger 15m.
    LONG:  pontuação quando preço perto da banda INFERIOR (sobrevenda)
    SHORT: pontuação quando preço perto da banda SUPERIOR (sobrecompra)
    Max: 3 pts.
    """
    price = d.get("price", 0)
    bbl   = d.get("bb_lower_15m", 0)
    bbu   = d.get("bb_upper_15m", 0)
    sym   = d.get("base_coin", "?")

    if not price or price <= 0:
        return 0, "BB N/A (preço inválido)"
    if not bbl or bbl <= 0:
        return 0, "BB N/A (BB_lower ausente)"
    if not bbu or bbu <= 0:
        return 0, "BB N/A (BB_upper ausente)"

    banda = bbu - bbl
    banda_min = price * 0.001
    if banda <= banda_min:
        return 0, f"BB N/A (banda estreita: {banda:.4f})"

    pos = (price - bbl) / banda

    # [v6.6.2] Lógica de descarte assimétrica por direção:
    #   LONG:  pos > 1.5 (acima da banda superior = sobrecompra anômala) → descarta
    #          pos < 0   (abaixo da inferior) → LONG não é ponto de entrada (descarta)
    #   SHORT: pos > 1.5 → descarta (anômalo)
    #          pos < 0   → preço abaixo da banda inferior em contexto SELL =
    #                       momentum bearish forte. Não descarta; pontua como
    #                       "BB extremo inferior SHORT" (preço rompeu a banda pra baixo).
    if direction == "SHORT":
        if pos > 1.5:
            LOG.warning(f"    BB {sym}: pos={pos:.0%} fora do range esperado — descartando")
            return 0, f"BB N/A (pos anômala: {pos:.0%})"
        # pos < 0: preço abaixo da banda inferior — momentum bearish confirmado
        if pos < 0:
            return 3, f"BB rompeu inferior ({pos:.0%}) — momentum bearish"
        # pos normal: pontuação cresce quanto mais próximo da banda superior
        if pos > 0.95:   return 3, f"BB extremo superior ({pos:.0%})"
        elif pos > 0.85: return 2, f"BB superior ({pos:.0%})"
        elif pos > 0.75: return 1, f"BB alta ({pos:.0%})"
        else:            return 0, f"BB neutro ({pos:.0%})"
    else:
        if pos < 0 or pos > 1.5:
            LOG.warning(f"    BB {sym}: pos={pos:.0%} fora do range esperado — descartando")
            return 0, f"BB N/A (pos anômala: {pos:.0%})"
        # LONG: pontuação cresce quanto mais próximo da banda inferior
        if pos < 0.05:   return 3, f"BB extremo inferior ({pos:.0%})"
        elif pos < 0.15: return 2, f"BB inferior ({pos:.0%})"
        elif pos < 0.25: return 1, f"BB baixa ({pos:.0%})"
        else:            return 0, f"BB neutro ({pos:.0%})"


# P2 — Padrões de Candle 15m  (LONG: bullish | SHORT: bearish)
def score_candles(ind, direction="LONG"):
    """
    Price action puro no 15m.
    LONG:  padrões bullish de reversão/continuação
    SHORT: padrões bearish de reversão/continuação
    Max: 4 pts (cap).

    [v6.5.0] Diagnóstico de cobertura: distingue "nenhum padrão presente"
    de "dados ausentes (API não retornou)". Se TODOS os campos estão ausentes,
    o dict retorna vazio — isso indica falha no request de candles, não ausência
    de padrões. O log discrimina os dois casos para facilitar debug.
    """
    if not ind: return [], 0

    if direction == "SHORT":
        checks = {
            "Candle.Engulfing.Bearish|15" : ("Engulfing Bearish",  2),
            "Candle.ShootingStar|15"      : ("Shooting Star",      2),
            "Candle.EveningStar|15"       : ("Evening Star",       2),
            "Candle.3BlackCrows|15"       : ("3 Black Crows",      2),
            "Candle.Harami.Bearish|15"    : ("Harami Bearish",     1),
            # "Candle.Doji.GraveStone|15" removida de COLS_15M_CANDLES (API retorna data=null)
            # e removida aqui também para evitar dead code (confirmado 25/03/2026).
        }
    else:
        checks = {
            "Candle.Engulfing.Bullish|15": ("Engulfing Bullish", 2),
            "Candle.Hammer|15"           : ("Hammer",            2),
            "Candle.MorningStar|15"      : ("Morning Star",      2),
            "Candle.3WhiteSoldiers|15"   : ("3 White Soldiers",  2),
            "Candle.Harami.Bullish|15"   : ("Harami Bullish",    1),
            "Candle.Doji.Dragonfly|15"   : ("Dragonfly Doji",    1),
        }

    patterns, score = [], 0
    n_present = 0   # quantos campos existem no dict (mesmo que None/0)
    for key, (name, pts) in checks.items():
        v = ind.get(key)
        if key in ind:
            n_present += 1   # campo existe (pode ser None ou 0 = sem padrão)
        if v and v != 0:
            patterns.append(name)
            score += pts

    # [v6.5.0] Se nenhum campo foi retornado pela API, marcar como sem dado
    if n_present == 0:
        return [], 0   # sem dados — tratado como "Nenhum" no breakdown

    return patterns, min(score, 4)


# P3 — Funding Rate  (LONG: negativo bom | SHORT: positivo alto bom)
def score_funding_rate(fr, direction="LONG"):
    """
    LONG:  funding negativo = shorts dominando = squeeze potencial de alta
    SHORT: funding positivo alto = longs excessivos = squeeze potencial de baixa
    Max: 2 pts.
    """
    if direction == "SHORT":
        if fr > 0.0005:  return 2, f"{fr:.4%} (longs excessivos — squeeze short potencial)"
        elif fr > 0:     return 1, f"{fr:.4%} (leve positivo)"
        elif fr < -0.0005: return -1, f"{fr:.4%} (shorts dominando — desfavorável para short)"
        else:            return 0, f"{fr:.4%} (neutro)"
    else:
        if fr < -0.0005: return 2, f"{fr:.4%} (squeeze potencial)"
        elif fr < 0:     return 1, f"{fr:.4%} (leve negativo)"
        elif fr > 0.0005: return -1, f"{fr:.4%} (longs excessivos)"
        else:            return 0, f"{fr:.4%} (neutro)"

# P8 — Volume 15m adaptativo
def score_volume_15m(candles_15m, fg_value=50):
    """
    Confirma se o volume da vela atual sustenta o movimento.
    Threshold adaptativo ao regime de mercado (Fear & Greed).
    Max: 2 pts.
    """
    if len(candles_15m) < 21: return 1, "Volume N/A (fallback)"
    current_vol = candles_15m[-1]["volume"]
    avg_vol     = np.mean([c["volume"] for c in candles_15m[-21:-1]])
    if avg_vol == 0: return 1, "Volume N/A (avg zero)"

    # Threshold adaptativo
    if fg_value <= 30:   threshold = 1.2  # Bull: exige volume crescente
    elif fg_value <= 70: threshold = 1.0  # Neutro: volume = média é suficiente
    else:                threshold = 0.8  # Bear: volume baixo é normal

    ratio = current_vol / avg_vol
    if ratio >= threshold * 1.5: return 2, f"Volume forte ({ratio:.1f}x média)"
    elif ratio >= threshold:     return 1, f"Volume adequado ({ratio:.1f}x média)"
    else:                        return 0, f"Volume fraco ({ratio:.1f}x < {threshold:.1f}x)"

# ===========================================================================
# CAMADA 1 — PILARES 4H  (LONG e SHORT)
# ===========================================================================

# P4 — Zonas de Liquidez 4H
def analyze_liquidity_zones_4h(candles_4h, current_price, direction="LONG"):
    """
    LONG:  suporte 4H + OB bullish abaixo do preço
    SHORT: resistência 4H + OB bearish acima do preço
    Max: 3 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    score, details = 0, []

    if direction == "SHORT":
        # Resistência por Swing High 4H
        sr_hit = False
        if sh:
            for s in reversed(sh):
                dist_pct = (s["price"] - current_price) / current_price * 100
                if 0 < dist_pct <= SR_PROXIMITY_PCT:
                    score  += 1
                    sr_hit  = True
                    details.append(f"Resistência 4H {s['price']:.4f} ({dist_pct:.2f}% acima)")
                    break

        # OB bearish 4H
        ob_hit = False
        obs = detect_order_blocks_bearish(candles_4h)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = abs(current_price - ob_mid) / current_price * 100
                if dist_pct <= OB_PROXIMITY_PCT:
                    score  += 1
                    ob_hit  = True
                    details.append(f"OB Bearish 4H ({ob['low']:.4f}–{ob['high']:.4f})")
                    break

        if sr_hit and ob_hit:
            score += 1
            details.append("Confluência Res+OB Bearish")

    else:
        # LONG (comportamento original)
        sr_hit = False
        if sl:
            for s in reversed(sl):
                dist_pct = (current_price - s["price"]) / current_price * 100
                if 0 < dist_pct <= SR_PROXIMITY_PCT:
                    score  += 1
                    sr_hit  = True
                    details.append(f"Suporte 4H {s['price']:.4f} ({dist_pct:.2f}%)")
                    break

        ob_hit = False
        obs = detect_order_blocks(candles_4h)
        if obs:
            for ob in reversed(obs[-10:]):
                ob_mid   = (ob["high"] + ob["low"]) / 2
                dist_pct = (current_price - ob_mid) / current_price * 100
                if -OB_PROXIMITY_PCT <= dist_pct <= OB_PROXIMITY_PCT:
                    score  += 1
                    ob_hit  = True
                    details.append(f"OB 4H ({ob['low']:.4f}–{ob['high']:.4f})")
                    break

        if sr_hit and ob_hit:
            score += 1
            details.append("Confluência S/R+OB")

    if not details:
        label = "resistências" if direction == "SHORT" else "zonas de liquidez"
        return 0, f"Longe de {label} 4H"
    return min(score, 3), " | ".join(details)


# P5 — Figuras Gráficas 4H
def analyze_chart_patterns_4h(candles_4h, direction="LONG"):
    """
    LONG:  Falling Wedge, Triângulo Simétrico, Triângulo Ascendente, Cunha Desc.
    SHORT: Rising Wedge, H&S (approx), Triângulo Descendente, Cunha Ascendente
    Max: 2 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    if len(sh) < 3 or len(sl) < 3:
        return 0, "Dados insuficientes para figuras"

    sh_p = [s["price"] for s in sh[-3:]]
    sl_p = [s["price"] for s in sl[-3:]]

    highs_lower  = sh_p[0] > sh_p[1] > sh_p[2]
    highs_higher = sh_p[0] < sh_p[1] < sh_p[2]
    highs_flat   = abs(sh_p[0] - sh_p[2]) / sh_p[0] < 0.015
    lows_higher  = sl_p[0] < sl_p[1] < sl_p[2]
    lows_lower   = sl_p[0] > sl_p[1] > sl_p[2]
    lows_flat    = abs(sl_p[0] - sl_p[2]) / sl_p[0] < 0.015

    if direction == "SHORT":
        # Rising Wedge — reversão bearish (topos sobem menos que fundos)
        if highs_higher and lows_higher:
            high_rise = (sh_p[2] - sh_p[0]) / sh_p[0]
            low_rise  = (sl_p[2] - sl_p[0]) / sl_p[0]
            if high_rise < low_rise * 0.8:
                return 2, "Rising Wedge (reversão bearish)"

        # Triângulo Simétrico — compressão (bearish se contexto SELL)
        if highs_lower and lows_higher:
            return 2, "Triângulo Simétrico (compressão bearish)"

        # Triângulo Descendente — suporte cedendo
        if lows_flat and highs_lower:
            return 2, "Triângulo Descendente (distribuição bearish)"

        # Cunha Ascendente — pullback bearish em tendência de baixa
        if highs_higher and not lows_lower:
            return 1, "Cunha Ascendente (pullback bearish)"

        return 0, "Sem figuras bearish claras no 4H"

    else:
        # LONG (comportamento original)
        if highs_lower and lows_lower:
            high_drop = (sh_p[0] - sh_p[2]) / sh_p[0]
            low_drop  = (sl_p[0] - sl_p[2]) / sl_p[0]
            if low_drop < high_drop * 0.8:
                return 2, "Falling Wedge (reversão bullish)"

        if highs_lower and lows_higher:
            return 2, "Triângulo Simétrico (compressão)"

        if highs_flat and lows_higher:
            return 2, "Triângulo Ascendente (acumulação bullish)"

        if highs_lower and not lows_higher:
            return 1, "Cunha Descendente (pullback)"

        return 0, "Sem figuras claras no 4H"


# P6 — CHOCH / BOS 4H
def analyze_choch_bos_4h(candles_4h, current_price, direction="LONG"):
    """
    Smart Money Concepts no 4H.
    LONG:  CHOCH Bullish, BOS Bullish, Higher Lows
    SHORT: CHOCH Bearish, BOS Bearish, Lower Highs
    Max: 3 pts.
    """
    sh, sl = find_swing_points(candles_4h)
    if len(sh) < 2 or len(sl) < 2:
        return 0, "Dados insuficientes para estrutura 4H"

    last_sh = sh[-1]["price"]
    prev_sh = sh[-2]["price"]
    last_sl = sl[-1]["price"]
    prev_sl = sl[-2]["price"]

    if direction == "SHORT":
        # CHOCH Bearish: uptrend confirmado + rompimento de swing low
        in_uptrend = (prev_sl > sl[-3]["price"]) if len(sl) >= 3 else False
        if in_uptrend and current_price < last_sl:
            return 3, "CHOCH Bearish 4H (reversão confirmada)"

        # BOS Bearish: Lower Lows + Lower Highs + rompimento para baixo
        if last_sl < prev_sl and last_sh < prev_sh and current_price < last_sl:
            return 2, "BOS Bearish 4H (continuação de baixa)"

        # Estrutura Bearish: Lower Highs consecutivos (distribuição)
        if last_sh < prev_sh and len(sh) >= 3 and prev_sh < sh[-3]["price"]:
            return 1, "Estrutura 4H bearish (Lower Highs)"

        return 0, "Sem estrutura bearish no 4H"

    else:
        # LONG (comportamento original)
        in_downtrend = (prev_sh < sh[-3]["price"]) if len(sh) >= 3 else False
        if in_downtrend and current_price > last_sh:
            return 3, "CHOCH Bullish 4H (reversão confirmada)"

        if last_sh > prev_sh and last_sl > prev_sl and current_price > last_sh:
            return 2, "BOS Bullish 4H (continuação de alta)"

        if last_sl > prev_sl and len(sl) >= 3 and prev_sl > sl[-3]["price"]:
            return 1, "Estrutura 4H saudável (Higher Lows)"

        return 0, "Sem estrutura bullish no 4H"


# P7 — Filtro de Dump/Pump
def score_pump_filter(price_change_24h, direction="LONG"):
    """
    LONG:  pump excessivo bloqueia ou penaliza (ativo muito estendido pra cima)
    SHORT: dump excessivo bloqueia ou penaliza (ativo muito estendido pra baixo)
    Bloqueio total: ±40% | Penalidade gradual: ±20% a ±39%
    """
    if direction == "SHORT":
        change = -price_change_24h   # inverte: queda é "pump" para o short
        if change >= PUMP_BLOCK_24H:
            return None, f"DUMP BLOCK: {price_change_24h:.1f}% em 24h (ativo exausto)"
        elif change >= PUMP_WARN_24H_STRONG:
            return -3, f"Dump forte ({price_change_24h:.1f}% > -30%)"
        elif change >= PUMP_WARN_24H:
            return -2, f"Dump moderado ({price_change_24h:.1f}% > -20%)"
        else:
            return 0, f"OK ({price_change_24h:.1f}%)"
    else:
        if price_change_24h >= PUMP_BLOCK_24H:
            return None, f"PUMP BLOCK: +{price_change_24h:.1f}% em 24h"
        elif price_change_24h >= PUMP_WARN_24H_STRONG:
            return -3, f"Pump forte ({price_change_24h:.1f}% > 30%)"
        elif price_change_24h >= PUMP_WARN_24H:
            return -2, f"Pump moderado ({price_change_24h:.1f}% > 20%)"
        else:
            return 0, f"OK ({price_change_24h:.1f}%)"

# ===========================================================================
# GESTÃO DE RISCO — TRADE PARAMS
# ===========================================================================

def calc_trade_params(price, atr, score=0, threshold=0):
    """
    [v6.4.0 A1] LONG — sizing risk-first. SL abaixo, TPs acima.

    Fórmula:
      stop_pct  = 1.5 × ATR / preço            (distância ao SL em %)
      notional  = RISCO_POR_TRADE_USD / stop_pct (posição necessária para arriscar $5)
      alav_nec  = notional / MARGEM_MAX_POR_TRADE (alavancagem que cabe na margem)
      alav_final= min(alav_nec, cap_por_score)    (nunca excede o cap do score)
      margem    = notional / alav_final           (margem real alocada ≤ MARGEM_MAX)
      risco_real= margem × alav_final × stop_pct  (deve ≈ RISCO_POR_TRADE_USD)

    Garantia: margem_por_trade ≤ MARGEM_MAX_POR_TRADE em qualquer cenário.
    [v6.6.2] threshold: aviso de margem só é logado se score >= threshold.
    """
    if not price or not atr or atr <= 0: return None
    stop_pct = (1.5 * atr) / price           # decimal (ex: 0.05 = 5%)
    if stop_pct < 0.0005: return None        # SL < 0.05% — inválido para scalp
    sl = price * (1 - stop_pct)

    # Notional necessário para que o risco seja exatamente RISCO_POR_TRADE_USD
    notional = RISCO_POR_TRADE_USD / stop_pct

    # Alavancagem necessária para caber na margem máxima por trade
    alav_por_margem = notional / MARGEM_MAX_POR_TRADE
    alav_max_score  = get_alav_max_por_score(score)
    alav_final      = max(ALAVANCAGEM_MIN, min(alav_por_margem, alav_max_score))

    # Recalcular com alavancagem final
    margem_real  = notional / alav_final
    risco_real   = margem_real * alav_final * stop_pct   # ≈ RISCO_POR_TRADE_USD
    ganho_rr2    = risco_real * RR_MINIMO

    # [v6.5.0] Margem como aviso, nunca como bloqueio.
    # O risco ($5) está sempre garantido. A margem pode exceder MARGEM_MAX_POR_TRADE
    # quando o cap do score é menor que a alavancagem necessária — isso é informado
    # ao trader que decide se entra ou não. Nenhuma oportunidade é descartada.
    margem_excedida = margem_real > MARGEM_MAX_POR_TRADE

    stop_pct_pct = stop_pct * 100
    aviso_margem = f" ⚠️MARGEM ${margem_real:.0f}>{MARGEM_MAX_POR_TRADE:.0f}" if margem_excedida else ""
    LOG.debug(f"    TradeParams LONG: SL={stop_pct_pct:.2f}% | "
              f"notional=${notional:.0f} | alav_margem={alav_por_margem:.1f}x "
              f"→ cap={alav_max_score:.0f}x → alav={alav_final:.1f}x | "
              f"margem=${margem_real:.2f}{aviso_margem} | risco=${risco_real:.2f} | ganho=${ganho_rr2:.2f}")
    if margem_excedida and score >= threshold:
        LOG.warning(f"    ⚠️  LONG: margem ${margem_real:.0f} excede limite ${MARGEM_MAX_POR_TRADE:.0f} "
                    f"(score {score} → cap {alav_max_score:.0f}x < necessário {alav_por_margem:.1f}x). "
                    f"Trade válido — gerencie a margem manualmente.")
    return {
        "direction"       : "LONG",
        "entry"           : price,
        "sl"              : sl,
        "sl_distance_pct" : stop_pct_pct,
        "tp1"             : price * (1 + stop_pct),
        "tp2"             : price * (1 + stop_pct * 2),
        "tp3"             : price * (1 + stop_pct * 3),
        "rr"              : RR_MINIMO,
        "alavancagem"     : round(alav_final, 1),
        "alav_max_score"  : alav_max_score,
        "margem_usd"      : round(margem_real, 2),
        "margem_excedida" : margem_excedida,       # [v6.5.0] aviso, não bloqueio
        "notional_usd"    : round(notional, 2),
        "risco_usd"       : round(risco_real, 2),
        "ganho_rr2_usd"   : round(ganho_rr2, 2),
        "atr"             : atr,
    }


def calc_trade_params_short(price, atr, score=0, threshold=0):
    """
    [v6.4.0 A1] SHORT — sizing risk-first. SL ACIMA da entrada, TPs ABAIXO.

    Mesma lógica risk-first do LONG, direção invertida:
      sl  = price × (1 + stop_pct)   ← ACIMA
      tp1 = price × (1 - stop_pct)   ← abaixo (RR 1:1)
      tp2 = price × (1 - stop_pct×2) ← abaixo (RR 1:2)
      tp3 = price × (1 - stop_pct×3) ← abaixo (RR 1:3)
    [v6.6.2] threshold: aviso de margem só é logado se score >= threshold.
    """
    if not price or not atr or atr <= 0: return None
    stop_pct = (1.5 * atr) / price           # decimal
    if stop_pct < 0.0005: return None
    sl = price * (1 + stop_pct)              # SL ACIMA

    notional        = RISCO_POR_TRADE_USD / stop_pct
    alav_por_margem = notional / MARGEM_MAX_POR_TRADE
    alav_max_score  = get_alav_max_por_score(score)
    alav_final      = max(ALAVANCAGEM_MIN, min(alav_por_margem, alav_max_score))

    margem_real  = notional / alav_final
    risco_real   = margem_real * alav_final * stop_pct
    ganho_rr2    = risco_real * RR_MINIMO

    # [v6.5.0] Margem como aviso, nunca como bloqueio.
    margem_excedida = margem_real > MARGEM_MAX_POR_TRADE

    stop_pct_pct = stop_pct * 100
    aviso_margem = f" ⚠️MARGEM ${margem_real:.0f}>{MARGEM_MAX_POR_TRADE:.0f}" if margem_excedida else ""
    LOG.debug(f"    TradeParams SHORT: SL={stop_pct_pct:.2f}% | "
              f"notional=${notional:.0f} | alav_margem={alav_por_margem:.1f}x "
              f"→ cap={alav_max_score:.0f}x → alav={alav_final:.1f}x | "
              f"margem=${margem_real:.2f}{aviso_margem} | risco=${risco_real:.2f} | ganho=${ganho_rr2:.2f}")
    if margem_excedida and score >= threshold:
        LOG.warning(f"    ⚠️  SHORT: margem ${margem_real:.0f} excede limite ${MARGEM_MAX_POR_TRADE:.0f} "
                    f"(score {score} → cap {alav_max_score:.0f}x < necessário {alav_por_margem:.1f}x). "
                    f"Trade válido — gerencie a margem manualmente.")
    return {
        "direction"       : "SHORT",
        "entry"           : price,
        "sl"              : sl,
        "sl_distance_pct" : stop_pct_pct,
        "tp1"             : price * (1 - stop_pct),
        "tp2"             : price * (1 - stop_pct * 2),
        "tp3"             : price * (1 - stop_pct * 3),
        "rr"              : RR_MINIMO,
        "alavancagem"     : round(alav_final, 1),
        "alav_max_score"  : alav_max_score,
        "margem_usd"      : round(margem_real, 2),
        "margem_excedida" : margem_excedida,       # [v6.5.0] aviso, não bloqueio
        "notional_usd"    : round(notional, 2),
        "risco_usd"       : round(risco_real, 2),
        "ganho_rr2_usd"   : round(ganho_rr2, 2),
        "atr"             : atr,
    }

# ===========================================================================
# SISTEMA DE SCORE v4.3
# ===========================================================================


# P9 — Open Interest crescente na direção do trade [v6.2.0]
def score_oi_trend(current_oi_usd: float, symbol: str, state: dict, direction: str = "LONG") -> tuple:
    """
    OI crescente na direção do trade confirma que novo dinheiro está entrando,
    não apenas cobertura de posições. É uma das melhores confirmações de força.

    Lógica:
      LONG:  OI crescendo (mais dinheiro abrindo posições) = confirmação bullish
      SHORT: OI crescendo enquanto preço cai = confirmação bearish (distribuição real)

    Implementação: compara OI atual com a média dos últimos scans (estado diário).
    Armazena o histórico de OI no state["oi_history"][symbol] — max 10 rodadas.

    Pontuação:
      OI crescendo >15% vs média recente → +2 pts (fluxo forte na direção)
      OI crescendo >5%  vs média recente → +1 pt  (fluxo moderado)
      OI estável ou caindo              → 0 pts
      OI caindo >15%                    → -1 pt (posições fechando — sinal de exaustão)

    Max: 2 pts | Min: -1 pt
    """
    if current_oi_usd <= 0:
        return 0, "OI N/A (zero)"

    oi_history = state.get("oi_history", {})
    hist = oi_history.get(symbol, [])

    if len(hist) < 2:
        return 0, f"OI sem histórico ainda ({current_oi_usd/1e6:.1f}M USD)"

    avg_oi = sum(h["oi"] for h in hist[-5:]) / len(hist[-5:])  # média últimas 5 rodadas
    if avg_oi <= 0:
        return 0, "OI histórico inválido"

    pct_change = (current_oi_usd - avg_oi) / avg_oi * 100

    if pct_change > 15:
        return 2, f"OI +{pct_change:.1f}% vs média ({current_oi_usd/1e6:.1f}M) — fluxo forte"
    elif pct_change > 5:
        return 1, f"OI +{pct_change:.1f}% vs média ({current_oi_usd/1e6:.1f}M) — fluxo moderado"
    elif pct_change < -15:
        return -1, f"OI {pct_change:.1f}% vs média — posições fechando (exaustão)"
    else:
        return 0, f"OI estável ({pct_change:+.1f}% vs média, {current_oi_usd/1e6:.1f}M)"

def calculate_score(d, candles_15m=None, candles_1h=None, candles_4h=None,
                    fg_value=50, log_breakdown=False, direction="LONG", state=None):
    """
    Score com 3 camadas independentes. Max: 25 pts (P9 OI acrescenta até +2, base=23).
    [v6.0/v6.1.2] Parâmetro direction="LONG"|"SHORT" inverte a lógica dos pilares.
    [v6.2.0] state passado para P9 (score_oi_trend), opcional — sem state P9=0.
    [v6.4.0 A9] data_missing rastreado separadamente do score. Dado ausente
                retorna 0 pts no pilar MAS é marcado como "AUSENTE" no breakdown,
                não como "setup ruim". O campo data_quality no retorno indica
                quantos pilares que dependem de klines ficaram sem dado.

    LONG:  gates 4H/1H exigem BUY | pilares medem suporte/bullish
    SHORT: gates 4H/1H exigem SELL | pilares medem resistência/bearish

    Retornos especiais:
      -1  = Token descartado pelo gate de direção
      -99 = Token descartado por PUMP/DUMP BLOCK
      ≥0  = Score válido (verificar data_quality para confiabilidade)
    """
    sc           = 0
    reasons      = []
    breakdown    = []
    data_missing = 0   # [v6.4.0 A9] pilares sem dado (klines ausentes)

    # -----------------------------------------------------------------------
    # GATE CAMADA 1: 4H — direção macro
    # LONG:  SELL/STRONG_SELL descarta
    # SHORT: BUY/STRONG_BUY descarta
    # -----------------------------------------------------------------------
    s4h = d.get("summary_4h", "NEUTRAL")
    if direction == "SHORT":
        if "BUY" in s4h:
            return -1, [f"4H {s4h} — descartado (tendência bullish, não short)"], [], 1.0
        breakdown.append(("Contexto 4H", 0, 0, f"{s4h} (contexto SHORT, não pontuado)"))
    else:
        if "STRONG_SELL" in s4h or s4h == "SELL":
            return -1, [f"4H {s4h} — descartado pelo gate macro"], [], 1.0
        breakdown.append(("Contexto 4H", 0, 0, f"{s4h} (contexto, não pontuado)"))

    # -----------------------------------------------------------------------
    # CAMADA 1 — Pilares 4H (estrutura de preço, klines)
    # -----------------------------------------------------------------------
    price = d.get("price", 0)

    # P4 — Zonas de Liquidez 4H
    if candles_4h:
        lz_sc, lz_det = analyze_liquidity_zones_4h(candles_4h, price, direction)
    else:
        lz_sc, lz_det = 0, "⚠️ DADO AUSENTE — klines 4H"
        data_missing += 1
    sc += lz_sc
    breakdown.append(("P4 Liquidez 4H", lz_sc, 3, lz_det))
    if lz_sc >= 2: reasons.append("Zona liquidez 4H")

    # P5 — Figuras Gráficas 4H
    if candles_4h:
        cp_sc, cp_det = analyze_chart_patterns_4h(candles_4h, direction)
    else:
        cp_sc, cp_det = 0, "⚠️ DADO AUSENTE — klines 4H"
        data_missing += 1
    sc += cp_sc
    breakdown.append(("P5 Figuras 4H", cp_sc, 2, cp_det))
    if cp_sc > 0: reasons.append(cp_det.split(" (")[0])

    # P6 — CHOCH / BOS 4H
    if candles_4h:
        cb_sc, cb_det = analyze_choch_bos_4h(candles_4h, price, direction)
    else:
        cb_sc, cb_det = 0, "⚠️ DADO AUSENTE — klines 4H"
        data_missing += 1
    sc += cb_sc
    breakdown.append(("P6 CHOCH/BOS 4H", cb_sc, 3, cb_det))
    label_4h = "Estrutura 4H bearish" if direction == "SHORT" else "Estrutura 4H bullish"
    if cb_sc > 0: reasons.append(label_4h)

    # -----------------------------------------------------------------------
    # CAMADA 2 — Pilar 1H (posição de preço, klines)
    # -----------------------------------------------------------------------
    if candles_1h:
        if direction == "SHORT":
            s1h_sc, s1h_det = analyze_resistance_1h(candles_1h, price)
        else:
            s1h_sc, s1h_det = analyze_support_1h(candles_1h, price)
    else:
        s1h_sc, s1h_det = 0, "⚠️ DADO AUSENTE — klines 1H"
        data_missing += 1
    sc += s1h_sc
    label_1h = "P-1H Resistência 1H" if direction == "SHORT" else "P-1H Suporte 1H"
    breakdown.append((label_1h, s1h_sc, 4, s1h_det))
    label_1h_reason = "Resistência/OB 1H confirmado" if direction == "SHORT" else "Suporte/OB 1H confirmado"
    if s1h_sc >= 2: reasons.append(label_1h_reason)

    # -----------------------------------------------------------------------
    # CAMADA 3 — Pilares 15m (gatilho de entrada)
    # -----------------------------------------------------------------------

    # P1 — Bollinger Bands 15m
    bb_sc, bb_det = score_bollinger(d, direction)
    sc += bb_sc
    breakdown.append(("P1 Bollinger 15m", bb_sc, 3, bb_det))
    bb_label = "BB superior" if direction == "SHORT" else "BB inferior"
    if bb_sc >= 2: reasons.append(bb_label)

    # P2 — Padrões de Candle 15m
    ind_15m = d.get("_ind_15m", {})
    cp_list, ca_sc = score_candles(ind_15m, direction)
    sc += ca_sc
    breakdown.append(("P2 Candles 15m", ca_sc, 4,
                       f"Padrões: {', '.join(cp_list)}" if cp_list else "Nenhum"))
    if cp_list: reasons.append(f"Candle: {cp_list[0]}")

    # P3 — Funding Rate
    fr = d.get("funding_rate", 0)
    fr_sc, fr_det = score_funding_rate(fr, direction)
    sc += fr_sc
    breakdown.append(("P3 Funding Rate", fr_sc, 2, fr_det))
    fr_label = "FR squeeze short" if direction == "SHORT" else "FR squeeze"
    if fr_sc >= 2: reasons.append(fr_label)

    # P7 — Filtro de Pump/Dump
    pump_sc, pump_det = score_pump_filter(d.get("price_change_24h", 0), direction)
    if pump_sc is None:
        return -99, ["PUMP/DUMP BLOCK"], [], 1.0
    sc += pump_sc
    breakdown.append(("P7 Filtro Pump/Dump", pump_sc, 0, pump_det))

    # P8 — Volume 15m
    if candles_15m:
        vol_sc, vol_det = score_volume_15m(candles_15m, fg_value)
    else:
        vol_sc, vol_det = 0, "⚠️ DADO AUSENTE — klines 15m"
        data_missing += 1
    sc += vol_sc
    breakdown.append(("P8 Volume 15m", vol_sc, 2, vol_det))
    if vol_sc >= 2: reasons.append("Volume forte")

    # P9 — Open Interest crescente na direção do trade [v6.2.0]
    if state is not None:
        oi_sc, oi_det = score_oi_trend(d.get("oi_usd", 0), d.get("symbol", ""), state, direction)
    else:
        oi_sc, oi_det = 0, "OI sem histórico (primeiro scan)"
    sc += oi_sc
    breakdown.append(("P9 OI Crescente", oi_sc, 2, oi_det))
    if oi_sc >= 2: reasons.append("OI crescendo forte")
    elif oi_sc == 1: reasons.append("OI crescendo")

    final_sc = max(sc, 0)
    # [v6.4.0 A9] data_quality: fração de pilares com dado real
    total_kline_pilares = 5  # P4, P5, P6, P-1H, P8
    data_quality = round(1.0 - data_missing / total_kline_pilares, 2)  # 1.0=perfeito, 0.0=sem klines
    if data_missing > 0:
        reasons.append(f"⚠️ {data_missing}/{total_kline_pilares} pilares sem dado (klines ausentes)")
    if not reasons: reasons.append(f"Score {final_sc}/25 (sem sinal dominante)")

    return final_sc, reasons, breakdown, data_quality


# Pontuação mínima para logar o breakdown detalhado de pilares no arquivo de log.
# Tokens com score abaixo deste valor são resumidos em 1 linha (DEBUG), evitando
# que o log de ~100KB/rodada seja dominado por tokens que nunca geraram sinal.
# Abrange: Radar (≥5), Oportunidades (≥10) e Alertas (≥threshold).
LOG_BREAKDOWN_MIN_SCORE = 5


def log_score_breakdown(sym: str, direction: str, score: int,
                        breakdown: list, data_quality: float,
                        candles_15m, candles_1h, candles_4h) -> None:
    """[v6.6.2] Loga breakdown de pilares em INFO para tokens relevantes (score≥LOG_BREAKDOWN_MIN_SCORE)."""
    LOG.info(f"  SCORE {sym} [{direction}]: {score}/25 | DQ={data_quality:.0%} | klines: "
             f"15m={'✅' if candles_15m else '❌'} "
             f"1H={'✅' if candles_1h else '❌'} "
             f"4H={'✅' if candles_4h else '❌'}")
    for pilar, pts, max_pts, detail in breakdown:
        bar = "█" * pts if pts > 0 else ("▒" * abs(pts) if pts < 0 else "·")
        LOG.info(f"    {pilar:<24} {pts:>+3}/{max_pts} {bar} {detail}")


# ===========================================================================
# CONTEXTO DE MERCADO E THRESHOLD ADAPTATIVO
# ===========================================================================

def analyze_market_context(fg, btc_4h_str):
    """
    [v6.0/v6.1.2] Threshold adaptativo para LONG e SHORT.
    LONG:  Bear/Medo Extremo eleva threshold (mais seletivo)
    SHORT: Bear/Medo Extremo reduz threshold SHORT (mercado favorece short)
           Bull eleva threshold SHORT (mais difícil operar contra a tendência)
    """
    fg_val     = fg.get("value", 50)
    risk_score = 0

    if fg_val <= 20:   risk_score += 0
    elif fg_val <= 25: risk_score += 1
    elif fg_val <= 50: risk_score += 2
    elif fg_val >= 75: risk_score -= 1

    if "STRONG_BUY" in btc_4h_str: risk_score += 2
    elif "BUY" in btc_4h_str:      risk_score += 1
    elif "SELL" in btc_4h_str:     risk_score -= 2

    # Mercado desfavorável para LONG — bot desligado para LONG
    if fg_val >= 80 and "SELL" in btc_4h_str:
        verdict_long  = "DESFAVORÁVEL (Bot Desligado)"
        threshold_long = 99
    elif fg_val <= 20:
        threshold_long = 20; verdict_long = "CAUTELOSO (Medo Extremo)"
    elif fg_val <= 30 and "BUY" in btc_4h_str:
        threshold_long = 14; verdict_long = "FAVORÁVEL (Bull)"
    elif fg_val >= 75 or "SELL" in btc_4h_str:
        threshold_long = 20; verdict_long = "CAUTELOSO (Bear)"
    else:
        threshold_long = 16; verdict_long = "MODERADO (Neutro)"

    # [v6.5.0] Threshold SHORT: lógica assimétrica corrigida.
    #
    # Raciocínio: o threshold deve refletir o FAVORECIMENTO do mercado para aquela
    # direção. Medo Extremo (FGI≤20) é bearish por natureza — favorece SHORT.
    # Portanto o threshold SHORT deve ser REDUZIDO (mais fácil), não igual ao LONG.
    # O risco de squeeze existe, mas é coberto pelo próprio mecanismo de score
    # (pilares de estrutura exigem confirmação técnica real).
    #
    # Tabela corrigida:
    #   Bull Extremo (FGI≥80 + BTC BUY)     → SHORT desligado (99)   — contra tendência forte
    #   Medo Extremo (FGI≤20) + BTC SELL    → SHORT FAVORÁVEL (14)   — pânico confirmado no BTC
    #   Medo Extremo (FGI≤20) + BTC NEUTRAL → SHORT MODERADO (16)    — bearish mas sem conf. BTC
    #   Medo Extremo (FGI≤20) + BTC BUY     → SHORT CAUTELOSO (20)   — contra tendência do BTC
    #   Bear claro (FGI≤30 + BTC SELL)      → SHORT FAVORÁVEL (14)   — melhor janela para short
    #   SELL presente ou FGI≥75             → SHORT MODERADO (16)    — bear confirmado
    #   Neutro                              → SHORT CAUTELOSO (20)   — sem direcional claro
    if fg_val >= 80 and "BUY" in btc_4h_str:
        threshold_short = 99;  verdict_short = "DESLIGADO (Mercado Bull Extremo — short proibido)"
    elif fg_val <= 20 and "SELL" in btc_4h_str:
        threshold_short = 14;  verdict_short = "FAVORÁVEL (Pânico + BTC SELL)"
    elif fg_val <= 20 and "BUY" in btc_4h_str:
        threshold_short = 20;  verdict_short = "CAUTELOSO (Medo Extremo + BTC contra)"
    elif fg_val <= 20:
        threshold_short = 16;  verdict_short = "MODERADO (Medo Extremo — mercado bearish)"
    elif fg_val <= 30 and "SELL" in btc_4h_str:
        threshold_short = 14;  verdict_short = "FAVORÁVEL (Bear/Medo + BTC SELL)"
    elif fg_val >= 75 or "SELL" in btc_4h_str:
        threshold_short = 16;  verdict_short = "MODERADO (Bear claro)"
    else:
        threshold_short = 20;  verdict_short = "CAUTELOSO (Neutro — sem direcional claro)"

    LOG.debug(f"  Contexto: FGI={fg_val} | BTC={btc_4h_str} | "
              f"LONG={verdict_long}(thr={threshold_long}) | "
              f"SHORT={verdict_short}(thr={threshold_short}) | risk_score={risk_score}")

    return {
        "verdict"        : verdict_long,
        "threshold"      : threshold_long,
        "verdict_short"  : verdict_short,
        "threshold_short": threshold_short,
        "risk_score"     : risk_score,
        "fg"             : fg_val,
        "btc"            : btc_4h_str,
    }

# ===========================================================================
# EXECUÇÃO PRINCIPAL
# ===========================================================================

async def run_scan_async():
    global LOG, LOG_FILE, TS_SCAN
    LOG, LOG_FILE, TS_SCAN = setup_logger()

    LOG.info("🚀 Setup Atirador v6.6.2 | Arquitetura 3 Camadas (LONG+SHORT) | Iniciando scan...")
    t_start = time.time()

    state = load_daily_state()
    async with aiohttp.ClientSession() as session:

        # -------------------------------------------------------------------
        # ETAPA 1: Tickers + Fear & Greed (paralelo)
        # -------------------------------------------------------------------
        log_section("ETAPA 1 — Tickers (Bybit/Bitget) + Fear & Greed")
        perpetuals, total_items = fetch_perpetuals()
        # [v5.0] TOP_N removido — todos os qualificados entram no pipeline.
        # O gargalo de performance é KLINE_TOP_N (etapa 4), não o universo de entrada.
        symbols = [d["symbol"] for d in perpetuals]
        LOG.info(f"  Fonte de dados: {DATA_SOURCE} | "
                 f"Universo: {total_items} brutos → {len(perpetuals)} qualificados")
        LOG.info(f"  Analisando todos os {len(perpetuals)} tokens qualificados: "
                 f"{[d['base_coin'] for d in perpetuals[:10]]}{'...' if len(perpetuals) > 10 else ''}")

        tv_4h_task = fetch_tv_batch_async(session, symbols, COLS_4H)
        fg_task    = fetch_fear_greed_async(session)
        tv_4h, fg  = await asyncio.gather(tv_4h_task, fg_task)

        # -------------------------------------------------------------------
        # GATE 1 — Camada 4H
        # -------------------------------------------------------------------
        log_section("GATE 1 — Direção 4H (LONG: não SELL | SHORT: não BUY)")
        gate1_passed    = []   # LONG candidates
        gate1_short     = []   # SHORT candidates
        gate1_rejected  = 0
        tokens_sem_dados = []

        for d in perpetuals:
            sym    = d["symbol"]
            ind_4h = tv_4h.get(sym, {})
            raw_val = ind_4h.get("Recommend.All|240")
            rsi_4h  = sf(ind_4h.get("RSI|240"), default=50.0)

            if raw_val is None:
                tokens_sem_dados.append(d["base_coin"])
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 4H=SEM_DADOS (val=None) — excluído")
                continue

            s4h = recommendation_from_value(raw_val)
            d["summary_4h"] = s4h
            d["rsi_4h"]     = rsi_4h

            # LONG gate: passa se não SELL
            if "SELL" not in s4h:
                gate1_passed.append(d)
                if rsi_4h > 80:
                    LOG.warning(f"  ✅⚠️  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — LONG PASSOU (RSI EXTREMO)")
                    d["rsi_extremo"] = True
                else:
                    LOG.debug(f"  ✅  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — LONG PASSOU")
                    d["rsi_extremo"] = False
            else:
                gate1_rejected += 1
                LOG.debug(f"  ❌  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — LONG REJEITADO")

            # SHORT gate: passa se SELL/STRONG_SELL e RSI >= 30
            # [v6.2.0] RSI 4H < 30 = ativo exausto/sobrevenda — risco de squeeze curto,
            # não é ponto de entrada SHORT de qualidade. Descarta para evitar shortar fundos.
            if "SELL" in s4h:
                if rsi_4h < 30:
                    LOG.debug(f"  ⛔  {d['base_coin']:<8} 4H={s4h} RSI={rsi_4h:.1f} — SHORT REJEITADO (RSI<30, exausto)")
                else:
                    gate1_short.append(d)
                    LOG.debug(f"  📉  {d['base_coin']:<8} 4H={s4h} (val={raw_val:.4f}) RSI={rsi_4h:.1f} — SHORT CANDIDATO")

        LOG.info(f"  Gate 4H: LONG={len(gate1_passed)} | SHORT={len(gate1_short)} | "
                 f"sem dados TV={len(tokens_sem_dados)} | universo={len(perpetuals)}")
        if tokens_sem_dados:
            LOG.info(f"  Sem dados TV: {tokens_sem_dados}")

        # -------------------------------------------------------------------
        # GATE 2 — Camada 1H
        # -------------------------------------------------------------------
        log_section("GATE 2 — Estrutura 1H (LONG: BUY+ | SHORT: SELL+)")
        # Busca TV 1H para todos os candidatos (LONG + SHORT, sem duplicatas)
        all_gate1 = list({d["symbol"]: d for d in gate1_passed + gate1_short}.values())
        symbols_1h = [d["symbol"] for d in all_gate1]
        tv_1h      = await fetch_tv_batch_async(session, symbols_1h, COLS_1H)

        gate2_passed   = []   # LONG
        gate2_short    = []   # SHORT
        gate2_rejected = 0

        for d in gate1_passed:
            sym    = d["symbol"]
            ind_1h = tv_1h.get(sym, {})
            raw_1h = ind_1h.get("Recommend.All|60")
            if raw_1h is None:
                if d["base_coin"] not in tokens_sem_dados:
                    tokens_sem_dados.append(d["base_coin"])
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 1H=SEM_DADOS — excluído LONG")
                gate2_rejected += 1; continue
            s1h = recommendation_from_value(raw_1h)
            d["summary_1h"] = s1h
            if "BUY" in s1h:
                gate2_passed.append(d)
                LOG.debug(f"  ✅  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — LONG PASSOU")
            else:
                gate2_rejected += 1
                LOG.debug(f"  ❌  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — LONG REJEITADO")

        for d in gate1_short:
            sym    = d["symbol"]
            ind_1h = tv_1h.get(sym, {})
            raw_1h = ind_1h.get("Recommend.All|60")
            if raw_1h is None:
                # [v6.1.2] Loga explicitamente — não descarta silenciosamente
                LOG.warning(f"  ⚠️  {d['base_coin']:<8} 1H=SEM_DADOS — excluído SHORT (sem dados TV no 1H)")
                continue
            s1h = recommendation_from_value(raw_1h)
            d["summary_1h"] = s1h
            if "SELL" in s1h:
                gate2_short.append(d)
                LOG.debug(f"  📉  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — SHORT PASSOU")
            else:
                LOG.debug(f"  ❌  {d['base_coin']:<8} 1H={s1h} (val={raw_1h:.4f}) — SHORT REJEITADO (não SELL no 1H)")

        LOG.info(f"  Gate 1H: LONG={len(gate2_passed)} | SHORT={len(gate2_short)} | rejeitados={gate2_rejected}")

        if not gate2_passed and not gate2_short:
            LOG.warning("  ⚠️  Nenhum token passou os 2 gates (LONG ou SHORT) — encerrando scan")
            ts_full = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
            btc     = recommendation_from_value(tv_4h.get("BTCUSDT", {}).get("Recommend.All|240"))
            ctx     = analyze_market_context(fg, btc)
            report  = f"{'='*58}\n🎯 SETUP ATIRADOR v6.6.2\n📅 {ts_full}\n📋 Log: {os.path.basename(LOG_FILE)}\n{'='*58}\n"
            report += f"📊 Contexto: {ctx['verdict']} | FGI: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
            report += f"⏱️  Execução: {time.time()-t_start:.1f}s | Analisados: {total_items}\n"
            report += f"\n⚠️  Nenhum token passou os dois gates (LONG ou SHORT).\n"
            report += f"   Gate 4H: LONG={len(gate1_passed)} | SHORT={len(gate1_short)}\n"
            report += f"   Gate 1H: LONG=0/{len(gate1_passed)} | SHORT=0/{len(gate1_short)}\n"
            if tokens_sem_dados:
                report += f"   Sem dados TV ({len(tokens_sem_dados)}): {', '.join(tokens_sem_dados)}\n"
            report += f"\n   Aguarde próximo scan ou verifique o TradingView manualmente.\n"
            report += f"\n📋 Log completo: {LOG_FILE}\n"
            LOG.info(report)
            return report

        # -------------------------------------------------------------------
        # ETAPA 3: Indicadores 15m TradingView — dois requests independentes [v6.4.1 FIX]
        # -------------------------------------------------------------------
        # CORREÇÃO v6.4.1: BB/ATR e candles enviados em requests separados.
        # Um request com coluna inválida não zera BB/ATR do outro request.
        # -------------------------------------------------------------------
        log_section("ETAPA 3 — Indicadores 15m (TradingView) — LONG + SHORT [v6.4.1]")
        all_gate2 = list({d["symbol"]: d for d in gate2_passed + gate2_short}.values())
        symbols_15m = [d["symbol"] for d in all_gate2]

        # Request 1: BB + ATR (colunas técnicas — críticas para P1 e SL)
        tv_15m_tech = await fetch_tv_batch_async(session, symbols_15m, COLS_15M_TECH)

        # Request 2: padrões de candle (P2 — se falhar, perde só P2)
        tv_15m_candles = await fetch_tv_batch_async(session, symbols_15m, COLS_15M_CANDLES)

        # [v6.6.2] Se o batch de candles falhou completamente (retornou {}),
        # tenta requests individuais por coluna para isolar qual nome está quebrando.
        if not tv_15m_candles:
            LOG.warning("  ⚠️  [v6.6.2] Batch candles 15m falhou — tentando colunas individualmente para diagnóstico")
            cols_ok = []
            cols_fail = []
            for col in COLS_15M_CANDLES:
                test_result = await fetch_tv_batch_async(
                    session, symbols_15m[:3], [col], retries=1   # só 3 tokens para teste rápido
                )
                if test_result:
                    cols_ok.append(col)
                else:
                    cols_fail.append(col)
                    LOG.error(f"  ❌  [v6.6.2] Coluna inválida detectada: {col}")
            if cols_ok:
                LOG.info(f"  🔄  [v6.6.2] Rebuscando candles com {len(cols_ok)} colunas válidas: {cols_ok}")
                tv_15m_candles = await fetch_tv_batch_async(session, symbols_15m, cols_ok)
            if cols_fail:
                LOG.warning(f"  ⚠️  [v6.6.2] Colunas removidas por falha de API: {cols_fail}")
                LOG.warning(f"      → Atualizar COLS_15M_CANDLES no código para remover as inválidas")

        # Mesclar os dois resultados por símbolo
        tv_15m = {}
        all_syms = set(list(tv_15m_tech.keys()) + list(tv_15m_candles.keys()))
        for s in all_syms:
            merged = {}
            merged.update(tv_15m_tech.get(s, {}))
            merged.update(tv_15m_candles.get(s, {}))
            tv_15m[s] = merged

        n_tech    = sum(1 for s in symbols_15m if s in tv_15m_tech)
        n_candles = sum(1 for s in symbols_15m if s in tv_15m_candles)
        LOG.info(f"  TV 15m: tech={n_tech}/{len(symbols_15m)} | candles={n_candles}/{len(symbols_15m)}")

        for d in all_gate2:
            sym               = d["symbol"]
            ind_15m           = tv_15m.get(sym, {})
            d["_ind_15m"]     = ind_15m
            d["bb_upper_15m"] = sf(ind_15m.get("BB.upper|15"))
            d["bb_lower_15m"] = sf(ind_15m.get("BB.lower|15"))
            d["atr_15m"]      = sf(ind_15m.get("ATR|15"))
            LOG.debug(f"  {d['base_coin']:<8} ATR={d['atr_15m']:.4f} | "
                      f"BB_lower={d['bb_lower_15m']:.4f} | BB_upper={d['bb_upper_15m']:.4f} | "
                      f"FR={d['funding_rate']:.5f}")

        # Score parcial LONG para ordenação
        log_section("ETAPA 3b — Score parcial (sem klines, para ordenação)")
        pump_bloqueados      = []
        pump_bloqueados_short = []

        for d in gate2_passed:
            sc_p, _, _, _dq = calculate_score(d, fg_value=fg.get("value", 50),
                                         log_breakdown=False, direction="LONG", state=state)
            d["_partial_score"] = sc_p
            if sc_p == -99:
                pump_bloqueados.append(d)
                LOG.warning(f"  🚫  {d['base_coin']:<8} PUMP BLOCK LONG | {d.get('price_change_24h',0):.1f}%")
            else:
                LOG.debug(f"  {d['base_coin']:<8} score parcial LONG: {sc_p}/25 "
                          f"[FR={d.get('funding_rate',0):.4%} "
                          f"BB={d.get('bb_lower_15m',0):.4f}–{d.get('bb_upper_15m',0):.4f}]")

        for d in gate2_short:
            sc_p, _, _, _dq = calculate_score(d, fg_value=fg.get("value", 50),
                                         log_breakdown=False, direction="SHORT", state=state)
            d["_partial_score_short"] = sc_p
            if sc_p == -99:
                pump_bloqueados_short.append(d)
                LOG.warning(f"  🚫  {d['base_coin']:<8} DUMP BLOCK SHORT | {d.get('price_change_24h',0):.1f}%")
            else:
                LOG.debug(f"  {d['base_coin']:<8} score parcial SHORT: {sc_p}/25 "
                          f"[FR={d.get('funding_rate',0):.4%} "
                          f"BB={d.get('bb_lower_15m',0):.4f}–{d.get('bb_upper_15m',0):.4f}]")

        gate2_passed.sort(key=lambda x: x["_partial_score"], reverse=True)
        gate2_passed = [d for d in gate2_passed if d["_partial_score"] >= 0]
        gate2_short.sort(key=lambda x: x.get("_partial_score_short", 0), reverse=True)
        gate2_short = [d for d in gate2_short if d.get("_partial_score_short", 0) >= 0]

        LOG.info(f"  Ordem LONG: {[d['base_coin'] for d in gate2_passed]}")
        LOG.info(f"  Ordem SHORT: {[d['base_coin'] for d in gate2_short]}")
        if pump_bloqueados:
            LOG.info(f"  Bloqueados pump LONG: {[d['base_coin'] for d in pump_bloqueados]}")
        if pump_bloqueados_short:
            LOG.info(f"  Bloqueados dump SHORT: {[d['base_coin'] for d in pump_bloqueados_short]}")

        # -------------------------------------------------------------------
        # ETAPA 4: Klines — TOP N LONG + TOP N SHORT
        # Regra de exclusividade: mesmo token não pode ter LONG e SHORT juntos
        # -------------------------------------------------------------------
        log_section(f"ETAPA 4 — Klines + Score completo (TOP {KLINE_TOP_N} LONG + SHORT) [v6.2.0]")

        # Contexto de mercado — necessário antes do loop de score (ctx["threshold"])
        btc_4h_val = tv_4h.get("BTCUSDT", {}).get("Recommend.All|240")
        btc_4h_str = recommendation_from_value(btc_4h_val)
        ctx        = analyze_market_context(fg, btc_4h_str)

        top_full_long  = gate2_passed[:KLINE_TOP_N]
        top_light_long = gate2_passed[KLINE_TOP_N:KLINE_TOP_N_LIGHT]
        top_full_short = gate2_short[:KLINE_TOP_N]

        # Busca klines para todos de uma vez (sem duplicatas)
        all_top = list({d["symbol"]: d for d in top_full_long + top_full_short}.values())
        results       = []
        results_short = []
        observacoes   = []
        obs_short     = []   # [v6.3.0 A8] análise leve SHORT — declarado cedo para escopo completo

        # [v6.3.0 A4] Verificar status do candle 15m ANTES de buscar klines
        candle_lock = get_candle_lock_status()
        if candle_lock["use_prev"]:
            LOG.warning(
                f"  ⚠️  [A4 CANDLE LOCK] Vela 15m em formação "
                f"({candle_lock['seconds_open']:.0f}s desde abertura, "
                f"grace={CANDLE_CLOSED_GRACE_S}s). "
                f"Último fechamento: {candle_lock['ts_last_close']}. "
                f"Usando penúltima vela para BB, candles e volume."
            )
        else:
            LOG.info(
                f"  ✅  [A4 CANDLE LOCK] Vela 15m fechada "
                f"({candle_lock['seconds_open']:.0f}s após fechamento). "
                f"Último fechamento: {candle_lock['ts_last_close']}."
            )

        if all_top:
            LOG.info(f"  Buscando klines para: {[d['base_coin'] for d in all_top]}")
            tasks_15m = [fetch_klines_async(session, d["symbol"], "15m") for d in all_top]
            tasks_1h  = [fetch_klines_cached_async(session, d["symbol"], "1H") for d in all_top]
            tasks_4h  = [fetch_klines_cached_async(session, d["symbol"], "4H") for d in all_top]
            k15m_all, k1h_all, k4h_all = await asyncio.gather(
                asyncio.gather(*tasks_15m),
                asyncio.gather(*tasks_1h),
                asyncio.gather(*tasks_4h),
            )
            klines_map = {d["symbol"]: (k15m_all[i], k1h_all[i], k4h_all[i])
                          for i, d in enumerate(all_top)}

            # Score LONG
            for d in top_full_long:
                k15m_raw, k1h, k4h = klines_map.get(d["symbol"], ([], [], []))
                sym = d["base_coin"]
                # [v6.3.0 A4] Aplicar trava de candle fechado
                k15m = apply_candle_lock(k15m_raw, candle_lock)
                lock_tag = " [penúltimo candle]" if candle_lock["use_prev"] else ""
                LOG.info(f"  ─ LONG {sym}: 15m={len(k15m)}{lock_tag} | 1H={len(k1h)} | 4H={len(k4h)}")
                if not k15m:
                    LOG.warning(f"  ⚠️  {sym}: klines 15m vazios após candle lock — pulando LONG")
                    continue
                sc, reasons, bd, dq = calculate_score(
                    d, candles_15m=k15m, candles_1h=k1h, candles_4h=k4h, state=state,
                    fg_value=fg.get("value", 50), direction="LONG")
                d["score"] = sc; d["reasons"] = reasons; d["breakdown"] = bd
                d["data_quality"] = dq   # [v6.4.0 A9]
                if sc >= LOG_BREAKDOWN_MIN_SCORE:
                    log_score_breakdown(sym, "LONG", sc, bd, dq, k15m, k1h, k4h)
                else:
                    LOG.debug(f"  SCORE {sym} [LONG]: {sc}/25 | DQ={dq:.0%} (abaixo do radar)")
                # [v6.3.0 A6] Alerta bloqueado se OI é estimado (não verificado)
                if d.get("oi_estimado"):
                    LOG.warning(f"  ⚠️  {sym}: OI ESTIMADO (fallback vol*0.1) — alerta LONG bloqueado, vai para Observação")
                    d["trade"] = calc_trade_params(d["price"], d.get("atr_15m", 0), score=sc, threshold=ctx["threshold"])
                    if d["trade"]: observacoes.append(d)
                    continue
                trade = calc_trade_params(d["price"], d.get("atr_15m", 0), score=sc, threshold=ctx["threshold"])
                if trade:
                    LOG.info(f"  📈 LONG {sym}: score={sc}/25 | entry={trade['entry']:.4f} | "
                             f"SL={trade['sl_distance_pct']:.2f}% | alav={trade['alavancagem']}x ✅")
                    d["trade"] = trade; d["direction"] = "LONG"
                    results.append(d)
                else:
                    LOG.warning(f"  📈 LONG {sym}: score={sc}/25 | trade_params=❌ "
                                f"(ATR={d.get('atr_15m',0):.4f} — inválido para SL dinâmico)")

            # Score SHORT
            for d in top_full_short:
                k15m_raw, k1h, k4h = klines_map.get(d["symbol"], ([], [], []))
                sym = d["base_coin"]
                # [v6.3.0 A4] Aplicar trava de candle fechado
                k15m = apply_candle_lock(k15m_raw, candle_lock)
                lock_tag = " [penúltimo candle]" if candle_lock["use_prev"] else ""
                LOG.info(f"  ─ SHORT {sym}: 15m={len(k15m)}{lock_tag} | 1H={len(k1h)} | 4H={len(k4h)}")
                if not k15m:
                    LOG.warning(f"  ⚠️  {sym}: klines 15m vazios após candle lock — pulando SHORT")
                    continue
                sc, reasons, bd, dq = calculate_score(
                    d, candles_15m=k15m, candles_1h=k1h, candles_4h=k4h,
                    fg_value=fg.get("value", 50), direction="SHORT", state=state)
                d["score_short"] = sc; d["reasons_short"] = reasons; d["breakdown_short"] = bd
                d["data_quality_short"] = dq   # [v6.4.0 A9]
                if sc >= LOG_BREAKDOWN_MIN_SCORE:
                    log_score_breakdown(sym, "SHORT", sc, bd, dq, k15m, k1h, k4h)
                else:
                    LOG.debug(f"  SCORE {sym} [SHORT]: {sc}/25 | DQ={dq:.0%} (abaixo do radar)")
                # [v6.3.0 A6] Alerta bloqueado se OI é estimado (não verificado)
                if d.get("oi_estimado"):
                    LOG.warning(f"  ⚠️  {sym}: OI ESTIMADO (fallback vol*0.1) — alerta SHORT bloqueado")
                    continue
                trade = calc_trade_params_short(d["price"], d.get("atr_15m", 0), score=sc, threshold=ctx["threshold_short"])
                if trade:
                    LOG.info(f"  📉 SHORT {sym}: score={sc}/25 | entry={trade['entry']:.4f} | "
                             f"SL={trade['sl_distance_pct']:.2f}% | alav={trade['alavancagem']}x ✅")
                    d["trade_short"] = trade; d["direction"] = "SHORT"
                    results_short.append(d)
                else:
                    LOG.warning(f"  📉 SHORT {sym}: score={sc}/25 | trade_params=❌ "
                                f"(ATR={d.get('atr_15m',0):.4f} — inválido para SL dinâmico)")

        # Análise leve LONG (sem klines)
        log_section("ETAPA 4b — Análise leve LONG + SHORT (sem klines) [v6.3.0 A8]")
        for d in top_light_long:
            sc, reasons, bd, dq = calculate_score(d, fg_value=fg.get("value", 50),
                                              log_breakdown=False, direction="LONG", state=state)
            d["score"] = sc; d["reasons"] = reasons; d["breakdown"] = bd
            trade = calc_trade_params(d["price"], d.get("atr_15m", 0), score=sc, threshold=ctx["threshold"])
            if trade:
                d["trade"] = trade; d["direction"] = "LONG"
                observacoes.append(d)
                LOG.debug(f"  {d['base_coin']:<8} score parcial LONG={sc}/25 → Em Observação")

        # [v6.3.0 A8] Análise leve SHORT — paridade com LONG
        # Tokens SHORT posições 21-30 (após o top_full_short de 20) também entram
        # em observação leve para completar o market state map do lado vendedor.
        top_light_short = gate2_short[KLINE_TOP_N:KLINE_TOP_N_LIGHT]
        for d in top_light_short:
            sc, reasons, bd, dq = calculate_score(d, fg_value=fg.get("value", 50),
                                              log_breakdown=False, direction="SHORT", state=state)
            d["score_short"] = sc; d["reasons_short"] = reasons; d["breakdown_short"] = bd
            if sc >= 0:
                obs_short.append(d)
                LOG.debug(f"  {d['base_coin']:<8} score parcial SHORT={sc}/25 → Em Observação SHORT")

        # -------------------------------------------------------------------
        # ETAPA 4c — Armazena OI score nos resultados (para exibição no radar)
        # -------------------------------------------------------------------
        # Guarda o OI score calculado em calculate_score dentro do dict do token
        # para que o Telegram possa exibir sem recalcular. O score_oi_trend
        # já foi executado dentro de calculate_score — aqui só copiamos o valor
        # do breakdown para o dict principal de forma conveniente.
        for r in results:
            for pilar, pts, _, _ in r.get("breakdown", []):
                if pilar == "P9 OI Crescente":
                    r["oi_score"] = pts; break
        for r in results_short:
            for pilar, pts, _, _ in r.get("breakdown_short", []):
                if pilar == "P9 OI Crescente":
                    r["oi_score_short"] = pts; break

        # -------------------------------------------------------------------
        # ETAPA 5: Contexto e Relatório bidirecional
        # -------------------------------------------------------------------
        log_section("ETAPA 5 — Contexto de Mercado e Relatório Bidirecional")
        results.sort(key=lambda x: x["score"], reverse=True)
        results_short.sort(key=lambda x: x.get("score_short", 0), reverse=True)
        observacoes.sort(key=lambda x: x["score"], reverse=True)
        obs_short.sort(key=lambda x: x.get("score_short", 0), reverse=True)  # [v6.3.0 A8]

        # [v6.2.0] Atualiza histórico de scores/OI e faz limpeza de tokens antigos
        ts_iso = datetime.now(BRT).strftime("%Y-%m-%dT%H:%M")
        update_score_history(state, results, results_short, ts_iso)
        cleanup_score_history(state)
        save_daily_state(state)
        LOG.info(f"  📚  Score history: {len(state.get('score_history', {}))} tokens rastreados")

        btc_4h_val = tv_4h.get("BTCUSDT", {}).get("Recommend.All|240")
        btc_4h_str = recommendation_from_value(btc_4h_val)
        ctx        = analyze_market_context(fg, btc_4h_str)

        LOG.info(f"  BTC 4H: {btc_4h_str} | Thr LONG: {ctx['threshold']} ({ctx['verdict']}) | "
                 f"Thr SHORT: {ctx['threshold_short']} ({ctx['verdict_short']})")
        LOG.info(f"  Results LONG: {len(results)} | SHORT: {len(results_short)} | "
                 f"Obs LONG: {len(observacoes)} | Obs SHORT: {len(obs_short)}")  # [v6.3.0 A8]

        ts_full   = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
        risco_usd = RISCO_POR_TRADE_USD

        report  = f"{'='*58}\n"
        report += f"🎯 SETUP ATIRADOR v6.6.2\n"
        report += f"📅 {ts_full}\n"
        report += f"📋 Log: {os.path.basename(LOG_FILE)}\n"
        report += f"{'='*58}\n"
        report += f"📊 CONTEXTO DE MERCADO\n"
        report += f"   Fear & Greed: {ctx['fg']} | BTC 4H: {ctx['btc']}\n"
        report += f"   LONG:  {ctx['verdict']} | Threshold: {ctx['threshold']} pts\n"
        report += f"   SHORT: {ctx['verdict_short']} | Threshold: {ctx['threshold_short']} pts\n"
        report += f"   Risk Score: {ctx['risk_score']}\n"
        report += f"{'='*58}\n\n"

        report += f"💼 SIZING Risk-First [v6.6.2]\n"
        report += f"   Banca: ${BANKROLL:.2f} | Risco fixo/trade: ${risco_usd:.2f} | Margem máx/trade: ${MARGEM_MAX_POR_TRADE:.0f}\n"
        report += "\n"

        # [v6.3.0 A6] Contar tokens com OI estimado nos qualificados
        n_oi_estimado = sum(1 for d in perpetuals if d.get("oi_estimado"))

        report += f"🔍 PIPELINE\n"
        report += f"   Fonte de dados: {DATA_SOURCE} (perpetuals USDT)\n"
        report += f"   Universo: {total_items} tokens | Qualificados: {len(perpetuals)}"
        if n_oi_estimado:
            report += f" (⚠️ {n_oi_estimado} com OI estimado)"
        report += f"\n"
        report += f"   Gate 4H: LONG={len(gate1_passed)} | SHORT={len(gate1_short)}\n"
        report += f"   Gate 1H: LONG={len(gate2_passed)} | SHORT={len(gate2_short)}\n"
        report += f"   Análise completa: LONG={len(top_full_long)} | SHORT={len(top_full_short)}"
        report += f" | Obs: LONG={len(observacoes)} SHORT={len(obs_short)}\n"
        # [v6.3.0 A4] Status do candle lock
        cl = candle_lock
        if cl["use_prev"]:
            report += (f"   ⚠️  Candle 15m em formação ({cl['seconds_open']:.0f}s) — "
                       f"usando penúltima vela. Próximo fechamento em {cl['next_close']:.0f}s\n")
        else:
            report += (f"   ✅  Candle 15m fechado ({cl['seconds_open']:.0f}s após fechamento, "
                       f"último: {cl['ts_last_close']})\n")

        if len(DATA_SOURCE_ATTEMPTS) > 1:
            report += f"   📡 Fontes tentadas:\n"
            for a in DATA_SOURCE_ATTEMPTS:
                status_str = f"HTTP {a['status']}" if a['status'] else "sem resposta"
                if a['falha']:
                    report += f"      ⛔ {a['fonte']}: {status_str} em {a['elapsed_s']}s — {a['falha']}\n"
                else:
                    report += f"      ✅ {a['fonte']}: {status_str} em {a['elapsed_s']}s — {a['qualificados']} qualificados\n"
        elif DATA_SOURCE_ATTEMPTS:
            a = DATA_SOURCE_ATTEMPTS[0]
            report += f"   📡 Fonte ativa: {a['fonte']} | HTTP {a['status']} | {a['elapsed_s']}s\n"

        if tokens_sem_dados:
            report += f"   ⚠️  Sem dados TradingView ({len(tokens_sem_dados)}): {', '.join(tokens_sem_dados)}\n"

        all_pump = pump_bloqueados + pump_bloqueados_short
        if all_pump:
            pump_str = ", ".join(f"{d['base_coin']}({d.get('price_change_24h',0):.0f}%)" for d in all_pump)
            report += f"   🚫 Bloqueados pump/dump: {pump_str}\n"

        rsi_extremos = [d for d in gate1_passed if d.get("rsi_extremo")]
        if rsi_extremos:
            rsi_str = ", ".join(f"{d['base_coin']}(RSI={d['rsi_4h']:.0f})" for d in rsi_extremos)
            report += f"   ⚠️  RSI 4H extremo (>80): {rsi_str}\n"

        report += "\n"

        # ─── SEÇÃO LONG ───────────────────────────────────────────────────
        report += f"{'─'*58}\n📈 OPERAÇÕES LONG\n{'─'*58}\n"
        alertas_long = [r for r in results if r["score"] >= ctx["threshold"]]

        if ctx["threshold"] == 99:
            report += "🛑 LONG DESLIGADO — Mercado desfavorável.\n"
        elif not alertas_long:
            max_sc = max((r["score"] for r in results), default=0)
            report += f"ℹ️  Nenhum alerta LONG forte (score ≥ {ctx['threshold']}) no momento.\n"
            if results:
                report += f"   Score máximo: {max_sc}/25 (faltam {ctx['threshold'] - max_sc} pts)\n"
        else:
            report += f"🔥 {len(alertas_long)} ALERTA(S) LONG — Score ≥ {ctx['threshold']}/25:\n\n"
            for r in alertas_long:
                t    = r["trade"]
                report += f"🚀 LONG {r['base_coin']}\n"
                report += f"   Score: {r['score']}/25 | 4H: {r['summary_4h']} | 1H: {r['summary_1h']}\n"
                report += f"   Razões: {', '.join(r['reasons'][:4])}\n"
                report += f"   Preço: ${r['price']:.4f} | Vol 24h: ${r['turnover_24h']/1e6:.1f}M\n"
                report += f"   Alavancagem: {t['alavancagem']}x | Risco: ${t['risco_usd']:.2f} | Ganho: ${t['ganho_rr2_usd']:.2f}\n"
                report += f"   SL:  ${t['sl']:.4f}  (-{t['sl_distance_pct']:.2f}%)\n"
                report += f"   TP1: ${t['tp1']:.4f} (+{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
                report += f"   TP2: ${t['tp2']:.4f} (+{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
                report += f"   TP3: ${t['tp3']:.4f} (+{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
                report += f"   Trailing: ativar no TP1 → SL breakeven +0.5%\n\n"

        oport_long = [r for r in results if ctx["threshold"] > r["score"] >= 10]
        radar_long = [r for r in results if 5 <= r["score"] < 10]
        if oport_long:
            report += f"\n📈 OPORTUNIDADES LONG em Formação — Score 10–{ctx['threshold']-1}:\n"
            for r in oport_long[:5]:
                report += f"   ▶ {r['base_coin']} | {r['score']}/25 | {', '.join(r['reasons'][:2])}\n"
        if radar_long:
            report += f"\n🔎 RADAR LONG — Score 5–13:\n"
            report += f"   ⚠️  Não operar — insuficiente. Monitorar.\n"
            for r in radar_long[:5]:
                report += f"   · {r['base_coin']} | {r['score']}/25 | {r['summary_4h']} 4H\n"

        # ─── SEÇÃO SHORT ──────────────────────────────────────────────────
        report += f"\n{'─'*58}\n📉 OPERAÇÕES SHORT\n{'─'*58}\n"
        alertas_short = [r for r in results_short
                         if r.get("score_short", 0) >= ctx["threshold_short"]]

        if ctx["threshold_short"] == 99:
            report += "🛑 SHORT DESLIGADO — Mercado muito bullish, risco extremo de operar contra.\n"
        elif not alertas_short:
            max_sc_s = max((r.get("score_short", 0) for r in results_short), default=0)
            report += f"ℹ️  Nenhum alerta SHORT forte (score ≥ {ctx['threshold_short']}) no momento.\n"
            if results_short:
                report += f"   Score máximo SHORT: {max_sc_s}/25 (faltam {ctx['threshold_short'] - max_sc_s} pts)\n"
        else:
            report += f"🔥 {len(alertas_short)} ALERTA(S) SHORT — Score ≥ {ctx['threshold_short']}/25:\n\n"
            for r in alertas_short:
                t    = r["trade_short"]
                report += f"📉 SHORT {r['base_coin']}\n"
                report += f"   Score: {r['score_short']}/25 | 4H: {r['summary_4h']} | 1H: {r['summary_1h']}\n"
                report += f"   Razões: {', '.join(r.get('reasons_short', [])[:4])}\n"
                report += f"   Preço: ${r['price']:.4f} | Vol 24h: ${r['turnover_24h']/1e6:.1f}M\n"
                report += f"   Alavancagem: {t['alavancagem']}x | Risco: ${t['risco_usd']:.2f} | Ganho: ${t['ganho_rr2_usd']:.2f}\n"
                report += f"   SL:  ${t['sl']:.4f}  (+{t['sl_distance_pct']:.2f}%) ← ACIMA\n"
                report += f"   TP1: ${t['tp1']:.4f} (-{t['sl_distance_pct']:.2f}%) → fechar 50%\n"
                report += f"   TP2: ${t['tp2']:.4f} (-{t['sl_distance_pct']*2:.2f}%) → fechar 30%\n"
                report += f"   TP3: ${t['tp3']:.4f} (-{t['sl_distance_pct']*3:.2f}%) → fechar 20%\n"
                report += f"   Trailing: ativar no TP1 → SL breakeven -0.5%\n\n"

        radar_short = [r for r in results_short
                       if 5 <= r.get("score_short", 0) < ctx["threshold_short"]]
        if radar_short:
            report += f"\n🔎 RADAR SHORT — Score 5–{ctx['threshold_short']-1}:\n"
            report += f"   ⚠️  Não operar — insuficiente. Monitorar.\n"
            for r in radar_short[:5]:
                report += f"   · {r['base_coin']} | {r.get('score_short',0)}/25 | {r['summary_4h']} 4H\n"

        # Em Observação — LONG leve
        obs_relevantes = [o for o in observacoes if o["score"] >= 8]
        if obs_relevantes:
            report += f"\n👁️  EM OBSERVAÇÃO LONG — análise leve ({len(obs_relevantes)} tokens):\n"
            for o in obs_relevantes[:5]:
                oi_tag = " ⚠️OI?" if o.get("oi_estimado") else ""
                report += f"   · {o['base_coin']} | Score parcial: {o['score']}/25 | {o['summary_4h']} 4H{oi_tag}\n"

        # [v6.3.0 A8] Em Observação — SHORT leve (paridade)
        obs_short_rel = [o for o in obs_short if o.get("score_short", 0) >= 8]
        if obs_short_rel:
            report += f"\n👁️  EM OBSERVAÇÃO SHORT — análise leve ({len(obs_short_rel)} tokens):\n"
            for o in obs_short_rel[:5]:
                oi_tag = " ⚠️OI?" if o.get("oi_estimado") else ""
                report += f"   · {o['base_coin']} | Score parcial: {o.get('score_short',0)}/25 | {o['summary_4h']} 4H{oi_tag}\n"

        elapsed = time.time() - t_start
        report += f"\n{'-'*58}\n"
        report += f"⏱️  Execução: {elapsed:.1f}s | Analisados: {total_items} tokens\n"
        report += f"📁 Estado diário: {STATE_FILE}\n"
        report += f"📋 Log completo: {LOG_FILE}\n"

        # [v6.1.2] Notificações Telegram — calls + heartbeat
        log_section("ETAPA 6 — Notificações Telegram")
        tg_notify(
            ctx              = ctx,
            results          = results,
            results_short    = results_short,
            total_items      = total_items,
            qualificados     = len(perpetuals),
            n_long_gate1     = len(gate1_passed),
            n_long_gate2     = len(gate2_passed),
            n_short_gate1    = len(gate1_short),
            n_short_gate2    = len(gate2_short),
            fonte            = DATA_SOURCE,
            elapsed          = elapsed,
            state            = state,
            tokens_sem_dados = tokens_sem_dados,
            candle_lock      = candle_lock,       # [v6.3.0 A4]
            obs_long         = observacoes,        # [v6.3.0 A8]
            obs_short        = obs_short,          # [v6.3.0 A8]
        )

        LOG.info(report)
        LOG.info(f"✅ Scan v6.6.2 concluído em {elapsed:.1f}s | Fonte: {DATA_SOURCE} | Log: {LOG_FILE}")
        return report


def main():
    # Logger inicializado dentro de run_scan_async (precisa do timestamp de execução)
    asyncio.run(run_scan_async())

if __name__ == "__main__":
    main()
