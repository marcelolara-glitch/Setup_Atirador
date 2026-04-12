# config.py — Constantes centralizadas do Setup Atirador v8.1.2
# Este arquivo é a fonte da verdade para todas as constantes do sistema.
# NÃO contém lógica de negócio — apenas valores configuráveis.

# ---------------------------------------------------------------------------
# Bloco 1 — Imports e timezone
# ---------------------------------------------------------------------------
from datetime import timezone, timedelta

BRT = timezone(timedelta(hours=-3))

# ---------------------------------------------------------------------------
# Bloco 2 — Versão
# ---------------------------------------------------------------------------
VERSION = "8.2.0"

# ---------------------------------------------------------------------------
# Bloco 3 — Paths
# ---------------------------------------------------------------------------
import os

BASE_DIR    = os.path.expanduser("~/Setup_Atirador")
LOG_DIR     = os.path.join(BASE_DIR, "logs")
STATE_FILE  = os.path.join(BASE_DIR, "states", "atirador_state.json")
JOURNAL_DIR = os.path.join(BASE_DIR, "journal")

TELEGRAM_CONFIG_FILE        = os.path.expanduser("~/.atirador_telegram_config.json")
TELEGRAM_CONFIG_FILE_LEGACY = "/tmp/atirador_telegram_config.json"

# ---------------------------------------------------------------------------
# Bloco 4 — Parâmetros de universo e klines
# ---------------------------------------------------------------------------
MIN_TURNOVER_24H    = 2_000_000
MIN_OI_USD          = 5_000_000
KLINE_TOP_N         = 20
KLINE_TOP_N_LIGHT   = 30
KLINE_LIMIT         = 60
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
SCORE_HISTORY_MAX_ROUNDS = 48
SCORE_HISTORY_TTL_H      = 25

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
# Bloco 8 — Alavancagem por score
# Correção do Bug #2: chaves int simples (não tuplas) para evitar falha
# silenciosa no .get(int_key).
# ---------------------------------------------------------------------------
ALAV_POR_SCORE: dict = {
    14: 5.0,
    15: 5.0,
    16: 10.0,
    17: 10.0,
    18: 15.0,
    19: 15.0,
    20: 20.0,
    21: 20.0,
    22: 30.0,
    23: 30.0,
    24: 40.0,
    25: 40.0,
}


def get_alav_max_por_score(score: int) -> float:
    """Retorna alavancagem máxima para o score dado.
    Fallback para ALAVANCAGEM_MIN se score não mapeado."""
    return ALAV_POR_SCORE.get(score, ALAVANCAGEM_MIN)


# ---------------------------------------------------------------------------
# Bloco 9 — Colunas TradingView
# ---------------------------------------------------------------------------
COLS_4H     = ["Recommend.All|240", "RSI|240"]
COLS_1H     = ["Recommend.All|60"]
COLS_15M_TECH = ["BB.upper|15", "BB.lower|15", "ATR|15"]

# ---------------------------------------------------------------------------
# Bloco 10 — URLs de API
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
    "tradingview":   "https://scanner.tradingview.com/crypto/scan",
    "fear_greed":    "https://api.alternative.me/fng/?limit=1",
}

# ---------------------------------------------------------------------------
# Bloco 11 — Parâmetros Bitget
# ---------------------------------------------------------------------------
BITGET_PRODUCT_TYPE = "USDT-FUTURES"

# ---------------------------------------------------------------------------
# Bloco 12 — Heartbeat
# ---------------------------------------------------------------------------
TELEGRAM_HEARTBEAT = True

# ---------------------------------------------------------------------------
# Bloco 13 — Zona order
# ---------------------------------------------------------------------------
ZONA_ORDER = ["MAXIMA", "ALTA_OB4H", "ALTA_OB1H", "MEDIA", "BASE"]
