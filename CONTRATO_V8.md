# CONTRATO_V8.md — Contrato de Extração para Modularização v8.0.0

> **Gerado em:** 2026-04-08  
> **Origem:** `setup_atirador_v7_0_0.py` (2157 linhas)  
> **Commit base:** `1f793a1` (main)  
> **Regra:** Nenhum PR de modularização pode alterar nomes, assinaturas ou valores listados aqui sem aprovação explícita do produto.

---

## Seção 1 — Constantes críticas

| Constante | Valor atual | Linha | Módulo v8 |
|-----------|-------------|-------|-----------|
| `VERSION` | `"7.0.0"` | 61 | `config.py` |
| `BRT` | `timezone(timedelta(hours=-3))` | 60 | `config.py` |
| `LOG_DIR` | `~/Setup_Atirador/logs` | 82 | `config.py` |
| `MIN_TURNOVER_24H` | `2_000_000` | 337 | `exchanges.py` |
| `MIN_OI_USD` | `5_000_000` | 338 | `exchanges.py` |
| `BANKROLL` | `100.0` | 340 | `signals.py` |
| `RISCO_POR_TRADE_USD` | `5.00` | 341 | `signals.py` |
| `MARGEM_MAX_POR_TRADE` | `35.0` | 342 | `signals.py` |
| `ALAVANCAGEM_MIN` | `2.0` | 343 | `signals.py` |
| `ALAVANCAGEM_MAX` | `50.0` | 344 | `signals.py` |
| `RR_MINIMO` | `2.0` | 345 | `signals.py` |
| `ALAV_POR_SCORE` | `{(14,15):5.0, (16,17):10.0, (18,19):15.0, (20,21):20.0, (22,23):30.0, (24,25):40.0, (26,28):50.0}` | 347-355 | `signals.py` |
| `KLINE_TOP_N` | `20` | 363 | `exchanges.py` |
| `KLINE_TOP_N_LIGHT` | `30` | 364 | `exchanges.py` |
| `KLINE_LIMIT` | `60` | 365 | `exchanges.py` |
| `KLINE_CACHE_TTL_H` | `1` | 366 | `exchanges.py` |
| `SWING_WINDOW` | `5` | 368 | `indicators.py` |
| `SR_PROXIMITY_PCT` | `2.5` | 369 | `indicators.py` |
| `OB_IMPULSE_N` | `3` | 370 | `indicators.py` |
| `OB_IMPULSE_PCT` | `1.5` | 371 | `indicators.py` |
| `OB_PROXIMITY_PCT` | `2.5` | 372 | `indicators.py` |
| `ZONE_PROXIMITY_PCT` | `1.5` | 373 | `indicators.py` |
| `SCORE_HISTORY_MAX_ROUNDS` | `48` | 375 | `state.py` |
| `SCORE_HISTORY_TTL_H` | `25` | 376 | `state.py` |
| `STATE_FILE` | `~/Setup_Atirador/states/atirador_state.json` | 378 | `state.py` |
| `TELEGRAM_CONFIG_FILE` | `~/.atirador_telegram_config.json` | 383 | `config.py` |
| `TELEGRAM_CONFIG_FILE_LEGACY` | `/tmp/atirador_telegram_config.json` | 384 | `config.py` |
| `TELEGRAM_HEARTBEAT` | `True` | 424 | `config.py` |
| `TICKER_TIMEOUT` | `8` | 764 | `exchanges.py` |
| `_GATE_MULTIPLIERS_TTL` | `86400` | 770 | `exchanges.py` |
| `CANDLE_15M_SECONDS` | `900` | 1090 | `indicators.py` |
| `CANDLE_CLOSED_GRACE_S` | `60` | 1091 | `indicators.py` |
| `productType` (Bitget) | `USDT-FUTURES` (inline nas URLs) | 700, 1015 | `exchanges.py` |

### Thresholds adaptativos v7 (substituem score por pilares da v6)

| Threshold | Valor | Condição |
|-----------|-------|----------|
| Gate 4H LONG | `BUY` ou `STRONG_BUY` | NEUTRAL → DROP silencioso |
| Gate 4H SHORT | `SELL` ou `STRONG_SELL` | NEUTRAL → DROP silencioso |
| Gate 1H | qualquer (incluindo NEUTRAL) | Contexto apenas, não filtra |
| Check C threshold ALTA | `>= 2` pts | zona in (MAXIMA, ALTA_OB4H, ALTA_OB1H) |
| Check C threshold MEDIA/BASE | `>= 3` pts | zona in (MEDIA, BASE) |

### URLs de API

| Exchange | Endpoint | URL |
|----------|----------|-----|
| OKX | Tickers SWAP | `https://www.okx.com/api/v5/market/tickers?instType=SWAP` |
| OKX | Open Interest | `https://www.okx.com/api/v5/public/open-interest?instType=SWAP` |
| OKX | Funding Rate | `https://www.okx.com/api/v5/public/funding-rate` |
| OKX | Klines | `https://www.okx.com/api/v5/market/candles` |
| Gate.io | Tickers | `https://api.gateio.ws/api/v4/futures/usdt/tickers` |
| Gate.io | Contracts | `https://api.gateio.ws/api/v4/futures/usdt/contracts` |
| Bitget | Tickers | `https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES` |
| Bitget | Klines | `https://api.bitget.com/api/v2/mix/market/candles` |
| TradingView | Scanner | `https://scanner.tradingview.com/crypto/scan` |
| Fear & Greed | Index | `https://api.alternative.me/fng/?limit=1` |
| Telegram | Send | `https://api.telegram.org/bot{TOKEN}/sendMessage` |

### Fallback hierarchy de klines

`Bitget → OKX` (função `fetch_klines_async`, linha 697)

### Fallback hierarchy de tickers (universo)

`OKX → Gate.io → Bitget` (função `fetch_perpetuals`, linha 985)

### Colunas TradingView por timeframe

| Variável | Colunas | Timeframe |
|----------|---------|-----------|
| `COLS_4H` | `Recommend.All\|240`, `RSI\|240` | 4H |
| `COLS_1H` | `Recommend.All\|60` | 1H |
| `COLS_15M_TECH` | `BB.upper\|15`, `BB.lower\|15`, `ATR\|15` | 15m |


---

## Seção 2 — Assinaturas de funções

> Cada função listada será extraída para o módulo indicado. Nomes, parâmetros e retornos são contratos imutáveis.

### exchanges.py — Busca de dados de mercado

| Função | Parâmetros | Retorno | Linha |
|--------|------------|---------|-------|
| `api_get_async` | `session: aiohttp.ClientSession, url: str, retries: int = 3, headers: dict\|None = None` | `dict\|None` | 651 |
| `api_get` | `url: str, retries: int = 3` | `dict` (raises on failure) | 681 |
| `fetch_klines_async` | `session: aiohttp.ClientSession, symbol: str, granularity: str = "15m", limit: int = 60` | `tuple[list[dict], str\|None]` | 697 |
| `fetch_klines_cached_async` | `session: aiohttp.ClientSession, symbol: str, granularity: str = "4H", limit: int = 60` | `list[dict]` | 734 |
| `_fetch_token_okx_async` | `session: aiohttp.ClientSession, symbol: str` | `dict\|None` | 1044 |
| `_fetch_okx_tickers_with_oi` | _(nenhum)_ | `list[dict]\|None` | 856 |
| `_parse_okx_tickers` | `items: list[dict]` | `tuple[list[dict], int, int]` | 897 |
| `_fetch_okx_funding_rates` | `symbols_okx: list[str]` | `dict[str, float]` | 838 |
| `_fetch_gate_multipliers` | _(nenhum)_ | `dict[str, float]` | 784 |
| `_parse_gateio_tickers` | `items: list[dict], multipliers: dict` | `tuple[list[dict], int, int]` | 810 |
| `_parse_bitget_tickers` | `items: list[dict]` | `tuple[list[dict], int, int]` | 928 |
| `_try_source` | `nome: str, url: str, parse_fn: callable, extract_fn: callable, timeout: int\|None = None, parse_kwargs: dict\|None = None` | `tuple[list[dict], int]\|None` | 951 |
| `_log_source_attempt` | `fonte: str, url: str, status: int, elapsed: float, tokens_brutos: int, qualificados: int, motivo_falha: str\|None = None` | `None` | 773 |
| `fetch_perpetuals` | _(nenhum)_ | `tuple[list[dict], int]` ⚠️ | 985 |
| `fetch_fear_greed_async` | `session: aiohttp.ClientSession` | `dict` | 1031 |
| `_build_venue_info` | `kline_venue: str\|None, tv_venue: str\|None` | `dict` | 633 |
| `sf` | `val: Any, default: float = 0.0` | `float` | 628 |

> ⚠️ `fetch_perpetuals` retorna `(list[dict], int)` onde o segundo elemento é o **total raw de tickers** (não o nome da exchange). O nome da exchange fica em `DATA_SOURCE` global. Em `run_scan_async` (linha 1922), a variável recebe o nome `exchange` mas contém um int — **bug conhecido v7**.

### gates.py — TradingView e gate direcional

| Função | Parâmetros | Retorno | Linha |
|--------|------------|---------|-------|
| `fetch_tv_batch_async` | `session: aiohttp.ClientSession, symbols: list[str], columns: list[str], retries: int = 3` | `tuple[dict, dict]` | 568 |
| `recommendation_from_value` | `val: float\|None` | `str` | 559 |

### indicators.py — Análise técnica

| Função | Parâmetros | Retorno | Linha |
|--------|------------|---------|-------|
| `find_swing_points` | `candles: list[dict], window: int\|None = None` | `tuple[list[dict], list[dict]]` | 1124 |
| `detect_order_blocks` | `candles: list[dict]` | `list[dict]` | 1147 |
| `detect_order_blocks_bearish` | `candles: list[dict]` | `list[dict]` | 1167 |
| `analyze_support_1h` | `candles_1h: list[dict], current_price: float` | `tuple[int, str]` | 1187 |
| `analyze_resistance_1h` | `candles_1h: list[dict], current_price: float` | `tuple[int, str]` | 1214 |
| `analyze_liquidity_zones_4h` | `candles_4h: list[dict], current_price: float, direction: str = "LONG"` | `tuple[int, str]` | 1241 |
| `identify_zona` | `candles_4h: list[dict], candles_1h: list[dict], current_price: float, direction: str` | `tuple[str, str]` | 1297 |
| `get_candle_lock_status` | _(nenhum)_ | `dict` | 1094 |
| `apply_candle_lock` | `candles_15m: list[dict], lock: dict` | `list[dict]` | 1112 |

### scoring.py — Checks A, B, C e score

| Função | Parâmetros | Retorno | Linha |
|--------|------------|---------|-------|
| `check_rejeicao_presente` | `candles_15m: list[dict], direction: str` | `tuple[bool, str]` | 1438 |
| `check_estrutura_direcional` | `candles_15m: list[dict], direction: str, janela: int = 8` | `tuple[bool, str]` | 1471 |
| `score_oi_trend` | `symbol: str, direction: str, state: dict` | `tuple[int, str]` | 1493 |
| `check_forca_movimento` | `candles_15m: list[dict], d: dict, state: dict, direction: str` | `tuple[int, dict]` | 1514 |
| `_zone_to_score` | `zona_qualidade: str, check_c_total: int` | `int` | 1612 |

### signals.py — Trade params e pipeline por token

| Função | Parâmetros | Retorno | Linha |
|--------|------------|---------|-------|
| `get_alav_max_por_score` | `score: int` | `float` | 357 |
| `_get_nearest_resistance_zone` | `candles_4h: list[dict], candles_1h: list[dict], current_price: float` | `float\|None` | 1633 |
| `_get_nearest_support_zone` | `candles_4h: list[dict], candles_1h: list[dict], current_price: float` | `float\|None` | 1654 |
| `calc_trade_params` | `symbol: str, current_price: float, zona_qualidade: str, check_c_total: int, candles_4h: list[dict], candles_1h: list[dict]` | `dict\|None` | 1677 |
| `calc_trade_params_short` | `symbol: str, current_price: float, zona_qualidade: str, check_c_total: int, candles_4h: list[dict], candles_1h: list[dict]` | `dict\|None` | 1729 |
| `analisar_token_async` | `session: aiohttp.ClientSession, symbol: str, d_4h: dict, d_1h: dict, current_price: float, state: dict, exchange: str` | `dict\|None` | 1782 |
| `run_scan_async` | _(nenhum)_ | `None` | 1895 |

> ⚠️ **Bug v7 em `calc_trade_params` / `calc_trade_params_short` (linha 1700/1748):** usa `ALAV_POR_SCORE.get(score_alav, 3)` onde `score_alav` é int 1–5, mas as chaves de `ALAV_POR_SCORE` são tuplas `(int, int)`. O `.get()` sempre retorna o default `3`. Deveria usar `get_alav_max_por_score(score_alav)`.

> ⚠️ **Bug v7 em `_get_nearest_resistance_zone` (linha 1647) e `_get_nearest_support_zone` (linha 1668):** chamam `analyze_resistance_1h(candles_1h)` sem `current_price` e tentam iterar o retorno como lista de dicts de preço. `analyze_resistance_1h` retorna `tuple[int, str]`, não lista. Código morto/com erro.

### state.py — Persistência de estado

| Função | Parâmetros | Retorno | Linha |
|--------|------------|---------|-------|
| `load_daily_state` | _(nenhum)_ | `dict` | 448 |
| `save_daily_state` | `state: dict` | `None` | 464 |
| `update_score_history` | `state: dict, results: list[dict], ts: str` | `None` | 478 |
| `cleanup_score_history` | `state: dict` | `None` | 494 |
| `get_score_trend` | `state: dict, symbol: str, direction: str = "LONG"` | `str` | 513 |

### config.py / telegram.py — Configuração e notificações

| Função | Parâmetros | Retorno | Módulo v8 | Linha |
|--------|------------|---------|-----------|-------|
| `setup_logger` | _(nenhum)_ | `tuple[Logger, str, str]` | `config.py` | 84 |
| `log_section` | `title: str` | `None` | `config.py` | 121 |
| `_load_telegram_config` | _(nenhum)_ | `tuple[str, str]` | `config.py` | 387 |
| `_migrate_telegram_config` | `token: str, chat_id: str, source_path: str` | `None` | `config.py` | 409 |
| `save_telegram_config` | `token: str, chat_id: str` | `None` | `config.py` | 427 |
| `_tg_send` | `text: str` | `bool` | `telegram.py` | 131 |
| `_fmt_price` | `p: float` | `str` | `telegram.py` | 151 |
| `_tv_links` | `symbol: str` | `tuple[str, str]` | `telegram.py` | 159 |
| `_chk` | `passed: bool` | `str` | `telegram.py` | 167 |
| `_tg_call_v7` | `r: dict, direction: str, fg_val: int` | `str` | `telegram.py` | 171 |
| `_tg_quase_v7` | `r: dict, direction: str, fg_val: int` | `str` | `telegram.py` | 232 |
| `_tg_heartbeat_v7` | `n_univ: int, n_gate_short: int, n_gate_long: int, n_zona_short: int, n_zona_long: int, n_calls: int, n_quase: int, fg_val: int, btc_4h: str, elapsed: float, exchange: str` | `str` | `telegram.py` | 273 |
| `tg_notify_v7` | `results: list[dict], fg_val: int, n_univ: int, n_gate_short: int, n_gate_long: int, n_zona_short: int, n_zona_long: int, elapsed: float, exchange: str, btc_4h: str` | `None` | `telegram.py` | 291 |
| `main` | _(nenhum)_ | `None` | `main.py` | 2129 |


---

## Seção 3 — Dependências entre funções (fronteiras de módulo)

> Formato: `chamador (módulo origem)` → `chamado (módulo destino)` [parâmetros passados]

### signals.py → exchanges.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `analisar_token_async` | `fetch_klines_cached_async` | `[session, symbol, "4H", 50]` |
| `analisar_token_async` | `fetch_klines_cached_async` | `[session, symbol, "1H", 50]` |
| `analisar_token_async` | `fetch_klines_cached_async` | `[session, symbol, "15m", 20]` |
| `run_scan_async` | `fetch_perpetuals` | `[]` |
| `run_scan_async` | `fetch_fear_greed_async` | `[session]` |
| `run_scan_async` _(injeção de 15m)_ | `fetch_klines_cached_async` | `[session, sym, "15m", 20]` |
| `run_scan_async` _(injeção de 15m re-CALL)_ | `fetch_klines_cached_async` | `[session, sym, "4H", 50]` e `[session, sym, "1H", 50]` |

### signals.py → gates.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `analisar_token_async` | `recommendation_from_value` | `[d_4h.get("Recommend.All\|240")]` |
| `run_scan_async` | `fetch_tv_batch_async` | `[session, symbols, COLS_4H]` |
| `run_scan_async` | `fetch_tv_batch_async` | `[session, gate_syms, COLS_1H]` |
| `run_scan_async` | `fetch_tv_batch_async` | `[session, gate_syms, COLS_15M_TECH]` |
| `run_scan_async` | `fetch_tv_batch_async` | `[session, ["BTCUSDT"], COLS_4H]` (BTC trend) |
| `run_scan_async` | `recommendation_from_value` | `[d4.get("Recommend.All\|240")]` (gate loop) |
| `run_scan_async` | `recommendation_from_value` | `[btc_d4.get("Recommend.All\|240")]` (BTC) |

### signals.py → indicators.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `analisar_token_async` | `identify_zona` | `[candles_4h, candles_1h, current_price, direction]` |
| `run_scan_async` | `get_candle_lock_status` | `[]` |
| `run_scan_async` | `apply_candle_lock` | `[gate_syms, candle_lock]` |
| `_get_nearest_resistance_zone` | `detect_order_blocks_bearish` | `[candles_4h]` |
| `_get_nearest_resistance_zone` | `find_swing_points` | `[candles_4h]` |
| `_get_nearest_resistance_zone` | `analyze_resistance_1h` | `[candles_1h]` ⚠️ falta `current_price` |
| `_get_nearest_support_zone` | `detect_order_blocks` | `[candles_4h]` |
| `_get_nearest_support_zone` | `find_swing_points` | `[candles_4h]` |
| `_get_nearest_support_zone` | `analyze_support_1h` | `[candles_1h, current_price]` |

### signals.py → scoring.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `analisar_token_async` | `check_rejeicao_presente` | `[candles_15m, direction]` |
| `analisar_token_async` | `check_estrutura_direcional` | `[candles_15m, direction]` |
| `analisar_token_async` | `check_forca_movimento` | `[candles_15m, d_15m, state, direction]` |
| `run_scan_async` _(injeção 15m)_ | `check_forca_movimento` | `[candles_15m_recheck, d15m, state, r["direction"]]` |
| `calc_trade_params` | `_zone_to_score` | `[zona_qualidade, check_c_total]` |
| `calc_trade_params_short` | `_zone_to_score` | `[zona_qualidade, check_c_total]` |

### signals.py → state.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `run_scan_async` | `load_daily_state` | `[]` |
| `run_scan_async` | `update_score_history` | `[state, perps, ts_now]` |
| `run_scan_async` | `cleanup_score_history` | `[state]` |
| `run_scan_async` | `save_daily_state` | `[state]` |

### signals.py → telegram.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `run_scan_async` | `tg_notify_v7` | `[results, fg_val, n_univ, n_gate_short, n_gate_long, n_zona_short, n_zona_long, elapsed, exchange, btc_4h]` |

### scoring.py → state.py

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `score_oi_trend` | lê diretamente `state["score_history"][symbol]["oi_history"]` | acesso direto ao dict |
| `check_forca_movimento` | `score_oi_trend` | `[symbol, direction, state]` |

### indicators.py (intra-módulo, documentado para v8)

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `identify_zona` | `find_swing_points` | `[candles_4h]`, `[candles_1h]` |
| `identify_zona` | `detect_order_blocks` | `[candles_4h]`, `[candles_1h]` |
| `identify_zona` | `detect_order_blocks_bearish` | `[candles_4h]`, `[candles_1h]` |
| `analyze_support_1h` | `find_swing_points` | `[candles_1h]` |
| `analyze_support_1h` | `detect_order_blocks` | `[candles_1h]` |
| `analyze_resistance_1h` | `find_swing_points` | `[candles_1h]` |
| `analyze_resistance_1h` | `detect_order_blocks_bearish` | `[candles_1h]` |
| `analyze_liquidity_zones_4h` | `find_swing_points` | `[candles_4h]` |
| `analyze_liquidity_zones_4h` | `detect_order_blocks` | `[candles_4h]` |
| `analyze_liquidity_zones_4h` | `detect_order_blocks_bearish` | `[candles_4h]` |

### exchanges.py (intra-módulo)

| Chamador | Chamado | Parâmetros |
|----------|---------|------------|
| `fetch_perpetuals` | `_fetch_okx_tickers_with_oi` | `[]` |
| `fetch_perpetuals` | `_parse_okx_tickers` | `[tickers_with_oi]` |
| `fetch_perpetuals` | `_fetch_gate_multipliers` | `[]` |
| `fetch_perpetuals` | `_try_source` | `[nome, url, parse_fn, extract_fn, ...]` |
| `_try_source` | `_log_source_attempt` | `[nome, url, status, elapsed, ...]` |
| `_fetch_okx_tickers_with_oi` | `_fetch_okx_funding_rates` | `[swap_instids]` |
| `fetch_klines_cached_async` | `fetch_klines_async` | `[session, symbol, granularity, limit]` |
| `fetch_klines_async` | `api_get_async` | `[session, url_bitget, headers=BITGET_HEADERS]` |
| `fetch_klines_async` | `api_get_async` | `[session, url_okx]` (fallback OKX) |
| `fetch_tv_batch_async` | faz chamadas HTTP diretamente via `session.post` | sem helper |


---

## Seção 4 — Estado compartilhado

> Variáveis globais ou de estado lidas/escritas por mais de uma função.

### Globais de módulo (nível de script)

| Variável | Tipo | Funções que escrevem | Funções que leem | Decisão v8 |
|----------|------|----------------------|-----------------|------------|
| `DATA_SOURCE` | `str` | `fetch_perpetuals` | `run_scan_async` (via retorno com bug) | Vira parâmetro explícito ou propriedade de `exchanges.py` |
| `DATA_SOURCE_ATTEMPTS` | `list[dict]` | `fetch_perpetuals` (reset), `_log_source_attempt` (append), `_try_source` (via `_log_source_attempt`) | `fetch_perpetuals` | Vira retorno explícito de `fetch_perpetuals` |
| `_GATE_MULTIPLIERS` | `dict[str, float]` | `_fetch_gate_multipliers` | `_fetch_gate_multipliers` | Encapsular em `exchanges.py` como cache de módulo |
| `_GATE_MULTIPLIERS_TS` | `float` | `_fetch_gate_multipliers` | `_fetch_gate_multipliers` | Idem |
| `LOG` | `logging.Logger` | `setup_logger`, `main` | Todas as funções com `LOG.info/warning/error/debug` | Permanece global em `config.py`; injetado via `logging.getLogger("atirador")` |
| `LOG_FILE` | `str\|None` | `main` (via `setup_logger`) | Nenhuma (apenas logging interno) | Permanece em `config.py` |
| `TS_SCAN` | `str\|None` | `main` (via `setup_logger`) | Nenhuma no código atual | Remover ou mover para `config.py` |
| `TELEGRAM_TOKEN` | `str` | `_load_telegram_config` (init), `save_telegram_config` | `_tg_send`, `tg_notify_v7` | Vira parâmetro explícito ou objeto `TelegramConfig` em `config.py` |
| `TELEGRAM_CHAT_ID` | `str` | `_load_telegram_config` (init), `save_telegram_config` | `_tg_send`, `tg_notify_v7` | Idem |
| `TELEGRAM_HEARTBEAT` | `bool` | Inicializado na linha 424 | `tg_notify_v7` | Vira parâmetro de `tg_notify_v7` ou campo de `TelegramConfig` |
| `_round_logger_v7` | `RoundLoggerV7\|None` | Linha 76 (init) | `run_scan_async` (usa instância local `log`, não este global) | Remover global; instanciar localmente em `run_scan_async` |
| `_trade_journal` | `TradeJournal\|None` | Linha 77 (init) | Não usado no scan principal | Remover ou mover para `state.py` |
| `_OBSERVABILITY_V7` | `bool` | Bloco try/import (linha 64) | Não verificado em `run_scan_async` (usa `RoundLoggerV7` incondicionalmente) | Remover; `run_scan_async` deve ter try/except próprio |
| `_OBSERVABILITY_JOURNAL` | `bool` | Bloco try/import (linha 71) | Não verificado | Remover |

### State dict (passado explicitamente)

O dict `state` é carregado por `load_daily_state()` e passado como argumento. Suas chaves são:

| Chave | Tipo | Funções que escrevem | Funções que leem | Decisão v8 |
|-------|------|----------------------|-----------------|------------|
| `state["score_history"]` | `dict[str, list]` | `update_score_history` | `get_score_trend`, `score_oi_trend` (via `state["score_history"][symbol]`) | Permanece em `state.py`; passado explicitamente |
| `state["oi_history"]` | `dict[str, list[dict]]` | `update_score_history` | `cleanup_score_history`, `score_oi_trend` (via `state["score_history"][symbol]["oi_history"]`) | Permanece em `state.py` |
| `state["date"]` | `str` | `save_daily_state` | `save_daily_state` | Permanece em `state.py` |

> **Atenção:** `score_oi_trend` (linha 1498) lê `state["score_history"][symbol]["oi_history"]`, mas `update_score_history` (linha 489) escreve em `state["oi_history"][symbol]` — **são chaves diferentes**. Há inconsistência de schema entre escritor e leitor. Bug conhecido v7.

### Hierarquia de zona (constante de ordenação usada em run_scan_async)

```python
ZONA_ORDER = ["MAXIMA", "ALTA_OB4H", "ALTA_OB1H", "MEDIA", "BASE"]
```
Definida localmente na linha 2080 dentro de `run_scan_async`. Em v8 deve ser constante de módulo em `indicators.py`.

---

## Notas de integridade para extração v8

| # | Tipo | Descrição | Localização |
|---|------|-----------|-------------|
| 1 | Bug | `fetch_perpetuals` retorna `(list, int)` mas `run_scan_async` usa a variável como `exchange: str` | L985 / L1922 |
| 2 | Bug | `calc_trade_params` usa `ALAV_POR_SCORE.get(int_key)` em vez de `get_alav_max_por_score(int_key)` | L1700, L1748 |
| 3 | Bug | `_get_nearest_resistance_zone` chama `analyze_resistance_1h(candles_1h)` sem `current_price` e itera retorno como lista | L1647 |
| 4 | Bug | `score_oi_trend` lê `state["score_history"][sym]["oi_history"]` mas `update_score_history` escreve em `state["oi_history"][sym]` | L1498 / L489 |
| 5 | Aviso | `ZONA_ORDER` definida localmente em `run_scan_async` — deve virar constante de módulo | L2080 |
| 6 | Aviso | `_OBSERVABILITY_V7` não é verificado antes de usar `RoundLoggerV7` em `run_scan_async` | L64 / L1909 |
| 7 | Aviso | `ALAV_POR_SCORE` usa chaves tupla `(int,int)` — padrão incomum; extrair dict simples `int→float` em v8 | L347 |
