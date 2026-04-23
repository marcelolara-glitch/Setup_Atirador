# config.py — Constantes centralizadas do Setup Atirador v9.0.0-beta
# Este arquivo é a fonte da verdade para todas as constantes do sistema.
# NÃO contém lógica de negócio — apenas valores configuráveis.
#
# v9: portado do v8 com adaptações cirúrgicas — multi-setup, sem TradingView
# Scanner, saídas parciais TP1/TP2/TP3, regimes de mercado.

# ---------------------------------------------------------------------------
# Bloco 1 — Imports e timezone
# ---------------------------------------------------------------------------
from datetime import timezone, timedelta

BRT = timezone(timedelta(hours=-3))

# ---------------------------------------------------------------------------
# Bloco 2 — Versão
# ---------------------------------------------------------------------------
VERSION = "9.0.0-beta"

# ---------------------------------------------------------------------------
# Bloco 3 — Paths
# ---------------------------------------------------------------------------
import os

BASE_DIR       = os.path.expanduser("~/Setup_Atirador")
LOG_DIR        = os.path.join(BASE_DIR, "logs")
STATE_FILE     = os.path.join(BASE_DIR, "states", "atirador_state.json")
STATE_FILE_V9  = os.path.join(BASE_DIR, "states", "atirador_state_v9.json")
JOURNAL_DIR    = os.path.join(BASE_DIR, "journal")

TELEGRAM_CONFIG_FILE        = os.path.expanduser("~/.atirador_telegram_config.json")
TELEGRAM_CONFIG_FILE_LEGACY = "/tmp/atirador_telegram_config.json"

# v9 — bancos novos, isolados dos v8 (descartados no deploy)
SCAN_LOG_DB_V9    = os.path.join(LOG_DIR, "scan_log_v9.db")
JOURNAL_DB_V9     = os.path.join(JOURNAL_DIR, "atirador_journal_v9.db")
STATE_FILE_V9     = os.path.join(BASE_DIR, "states", "atirador_state_v9.json")
SCAN_LOG_JSONL_V9 = os.path.join(LOG_DIR, "scan_log_v9.jsonl")

# ---------------------------------------------------------------------------
# Bloco 4 — Parâmetros de universo e klines
# ---------------------------------------------------------------------------
MIN_TURNOVER_24H    = 1_000_000   # v8 era 2_000_000 — ampliando universo em v9
MIN_OI_USD          = 3_000_000   # v8 era 5_000_000
# v9: None = sem cap, processa universo todo. Se rodada ficar lenta demais, voltamos a um int.
KLINE_TOP_N         = None
KLINE_TOP_N_LIGHT   = None
KLINE_LIMIT         = 200         # v8 era 60 — v9 precisa mais história para indicadores
KLINE_CACHE_TTL_H   = 1
TICKER_TIMEOUT      = 8
_GATE_MULTIPLIERS_TTL = 86400
CANDLE_15M_SECONDS  = 900
CANDLE_CLOSED_GRACE_S = 60

# ---------------------------------------------------------------------------
# Bloco 5 — Parâmetros de indicadores técnicos
# ---------------------------------------------------------------------------
SWING_WINDOW        = 5
SR_PROXIMITY_PCT    = 2.5
OB_IMPULSE_N        = 3
OB_IMPULSE_PCT      = 1.5
OB_PROXIMITY_PCT    = 2.5
ZONE_PROXIMITY_PCT  = 1.5

# ---------------------------------------------------------------------------
# Bloco 6 — Parâmetros de estado
# ---------------------------------------------------------------------------
# Legado v8 — manter enquanto módulos v8 referenciam; remover ao descomissionar.
SCORE_HISTORY_MAX_ROUNDS = 48
SCORE_HISTORY_TTL_H      = 25

# v9 — nomes explícitos para o estado multi-setup.
SETUPS_HISTORY_MAX_ROUNDS = 96   # 96 rodadas = 24h em cron 15min
OI_HISTORY_MAX_ROUNDS     = 96
STATE_TTL_HOURS           = 26   # 24h + folga

# ---------------------------------------------------------------------------
# Bloco 7 — Parâmetros de trade e risco
# ---------------------------------------------------------------------------
BANKROLL             = 100.0
RISCO_POR_TRADE_USD  = 5.00
MARGEM_MAX_POR_TRADE = 35.0
ALAVANCAGEM_MIN      = 2.0
ALAVANCAGEM_MAX      = 50.0
RR_MINIMO            = 2.0

# ---------------------------------------------------------------------------
# Bloco 8 — Cron e timing
# ---------------------------------------------------------------------------
SCAN_INTERVAL_MIN = 15   # v8 era 30 — v9 alinha com candle 15m
SCAN_OFFSET_MIN   = 1    # offset em minutos para dar tempo de fechar candle

# ---------------------------------------------------------------------------
# Bloco 9 — Setups (kill-switches)
# ---------------------------------------------------------------------------
SETUPS_ENABLED: dict[str, bool] = {
    "cont_pull":   True,
    "rev_exaust":  True,
    "break_range": True,
    "rev_zone":    True,
    "breaker":     True,
}

# ---------------------------------------------------------------------------
# Bloco 10 — BTC contexto
# ---------------------------------------------------------------------------
BTC_SYMBOL = "BTCUSDT"   # formato interno (sem -USDT-SWAP)
BTC_ABRUPT_MOVE_THRESHOLD_PCT = 2.0   # bloqueia CALLs se BTC mover mais que isso em 15m

# ---------------------------------------------------------------------------
# Bloco 11 — URLs de API
# ---------------------------------------------------------------------------
URLS = {
    "okx_tickers":   "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    "okx_oi":        "https://www.okx.com/api/v5/public/open-interest?instType=SWAP",
    "okx_funding":   "https://www.okx.com/api/v5/public/funding-rate",
    "okx_klines":    "https://www.okx.com/api/v5/market/candles",
    "gate_tickers":  "https://api.gateio.ws/api/v4/futures/usdt/tickers",
    "gate_contracts":"https://api.gateio.ws/api/v4/futures/usdt/contracts",
    "bitget_tickers":"https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
    "bitget_klines": "https://api.bitget.com/api/v2/mix/market/candles",
    "fear_greed":    "https://api.alternative.me/fng/?limit=1",
}

# ---------------------------------------------------------------------------
# Bloco 12 — Parâmetros Bitget
# CRÍTICO: sem esta constante a Bitget V2 retorna vazio. Não tocar.
# ---------------------------------------------------------------------------
BITGET_PRODUCT_TYPE = "USDT-FUTURES"

# ---------------------------------------------------------------------------
# Bloco 13 — Telegram
# ---------------------------------------------------------------------------
TELEGRAM_HEARTBEAT = True
TELEGRAM_BETA_TAG  = "v9-beta"

# ---------------------------------------------------------------------------
# Bloco 14 — Confidence mínimo
# ---------------------------------------------------------------------------
MIN_CONFIDENCE_FOR_CALL = 50.0   # signals.py descarta CALL abaixo disso

# ---------------------------------------------------------------------------
# Bloco 15 — Near-miss observability
# ---------------------------------------------------------------------------
NEAR_MISS_CONFIDENCE_MIN = 40.0   # confidence mínima para considerar near-miss

# ---------------------------------------------------------------------------
# Changelog
# ---------------------------------------------------------------------------
# v9.0.0-beta (23/04/2026): Reescrita arquitetural — multi-setup, sem TV Scanner,
#                            saídas parciais TP1/TP2/TP3, regimes de mercado.
#                            Universo ampliado (MIN_TURNOVER_24H 2M→1M,
#                            MIN_OI_USD 5M→3M). KLINE_LIMIT 60→200, sem cap em
#                            KLINE_TOP_N. Bancos SQLite/JSONL isolados em *_v9.
# v8.4.0 (13/04/2026): c1_bb reescrito — lógica toque+retorno na banda BB
#                       (high≥BB.upper+close<BB.upper para SHORT;
#                        low≤BB.lower+close>BB.lower para LONG).
#                       Adicionado high|15 e low|15 em COLS_15M_TECH.
