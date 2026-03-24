# Prompt de Pesquisa: Fontes de Dados para Scanner de Futuros Perpétuos

## Contexto do Projeto

Estou desenvolvendo um scanner automatizado de criptomoedas chamado **Setup Atirador** (atualmente na versão 5.2), cujo objetivo é identificar oportunidades de entrada em trades alavancados em futuros perpétuos USDT. O script roda a cada 30 minutos de forma autônoma e precisa coletar dados de mercado em tempo real sem intervenção humana.

---

## O Problema Central

### Objetivo conceitual da coleta de dados

O scanner precisa, a cada execução, responder a uma pergunta simples:

> **"Quais tokens do mercado de futuros perpétuos têm liquidez suficiente para eu operar com alavancagem agora, e qual é o estado técnico atual de cada um?"**

Para responder isso, o script precisa de dois tipos de dados:

**Tipo 1 — Dados de liquidez (filtro de universo)**
Usados para descartar tokens ilíquidos antes de qualquer análise técnica. São aplicados logo na Etapa 1 do pipeline:
- **Volume 24h em USDT** — mínimo $2M para garantir execução sem slippage
- **Open Interest (OI) em USD** — mínimo $5M para garantir que há posições abertas relevantes
- **Preço atual** — necessário para calcular OI em USD quando a exchange retorna OI em contratos
- **Funding Rate atual** — usado mais adiante como pilar de score (P3), mas coletado junto com os tickers

**Tipo 2 — Dados técnicos (análise)**
Coletados depois que o universo foi filtrado:
- Klines OHLCV (15m, 1H, 4H) — para cálculo de indicadores técnicos, swing points, Order Blocks, CHOCH/BOS
- Indicadores do TradingView (Recommend.All) — gates de direção 4H e 1H
- Fear & Greed Index — contexto macro de mercado

**Esta pesquisa foca exclusivamente no Tipo 1** — a coleta de volume, OI e funding rate para definir o universo de tokens.

---

## Onde esses dados se encaixam no algoritmo

```
EXECUÇÃO DO SCANNER (a cada 30 min)
│
├── ETAPA 1: Coleta de tickers ← PROBLEMA AQUI
│   ├── Busca lista de todos os futuros perpétuos USDT ativos
│   ├── Filtra por: volume 24h ≥ $2M E OI ≥ $5M
│   └── Resultado: ~60-100 tokens qualificados (de ~300-650 brutos)
│
├── ETAPA 2: Gate 4H (TradingView — sem problema)
├── ETAPA 3: Gate 1H (TradingView — sem problema)
├── ETAPA 4: Score técnico com klines (Bitget — sem problema)
└── ETAPA 5: Relatório
```

O filtro de liquidez da Etapa 1 é o **gargalo de qualidade** de todo o sistema. Se os dados de volume e OI usados no filtro não forem representativos do mercado real, tokens ilíquidos entram no pipeline e tokens líquidos são excluídos — comprometendo todos os resultados subsequentes.

---

## O Requisito Técnico da Coleta

Para ser viável no scanner, uma fonte de dados precisa atender **todos** os critérios abaixo:

1. **Cobertura completa em uma chamada** — retornar volume + OI de todos os tokens disponíveis em uma única requisição HTTP (ou no máximo 2-3 chamadas paginadas). Fontes que exigem uma chamada por símbolo são inviáveis.

2. **Sem autenticação obrigatória** — ou com API key gratuita sem cartão de crédito

3. **Sem geo-block para IPs brasileiros** — o script roda no Brasil

4. **Tempo de resposta ≤ 10 segundos** — execução total do scanner deve ser ≤ 30s

5. **Dados representativos do mercado global** — não apenas de uma exchange local

6. **Disponibilidade ≥ 95%** — para um scanner autônomo 24/7

---

## Histórico de Tentativas e Falhas por Versão

### v4.x (Bitget como fonte única)
**Fonte:** `GET https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES`
- Retorna volume (`usdtVolume`), OI (`holdingAmount × lastPr`), funding rate em uma chamada
- **Funcionou bem** como prova de conceito
- **Limitação descoberta:** universo de 539 tokens, menor que outras exchanges. Alguns tokens relevantes do mercado não estão listados na Bitget.

### v5.0 (Bybit primário + Bitget fallback)
**Fonte primária:** `GET https://api.bybit.com/v5/market/tickers?category=linear`
- Retorna volume (`turnover24h`), OI (`openInterest × lastPrice`), funding rate em uma chamada
- Universo de 650 tokens — mais completo
- **Falha 1 (21/03/2026 15h):** Timeout de 42s nas 2 primeiras tentativas. Respondeu na 3ª após ~92s total. Causa: provável throttling de IP brasileiro pelo CDN da Bybit.
- **Falha 2 (22/03/2026 07h):** HTTP 403 Forbidden nas 3 tentativas. Causa: geo-block deliberado de IP brasileiro. A Bybit restringe acesso de algumas regiões ao endpoint público.
- **Conclusão:** Bybit é instável/inacessível para IPs brasileiros. Removida nas versões seguintes.

### v5.1 (CoinGecko primário + OKX + Bitget)
**Fonte 1 tentada:** `GET https://api.coingecko.com/api/v3/derivatives?include_tickers=unexpired`
- Retornou HTTP 200 com 21.094 itens
- **Falha:** Todos os 21.094 itens rejeitados pelo parser. Causa: o endpoint agrega contratos de centenas de exchanges com formatos de símbolo inconsistentes (`BTC_USDT`, `BTC/USDT`, `BTC-USDT`), volumes em unidades heterogêneas (base coin, contratos, USD), e sem distinção confiável entre perpétuos e futuros com vencimento. Parser não conseguiu normalizar.

**Fonte 2 (OKX):** `GET https://www.okx.com/api/v5/market/tickers?instType=SWAP`
- Retorna volume (`volCcy24h`) e OI (`openInterest × lastPrice`) em uma chamada
- HTTP 200, resposta em ~0.5s, sem geo-block
- **Funcionou** — passou a ser a fonte ativa
- **Limitação descoberta:** Universo de apenas 300 tokens. TOP 5 por volume: SATS, PEPE, SHIB, BONK, FLOKI — meme coins com volume especulativo concentrado na OKX. Isso distorce o filtro: tokens com liquidez real no mercado mas baixo volume na OKX são excluídos, enquanto meme coins OKX-exclusivos entram no pipeline.

### v5.2 (CoinGecko exchange-specific + OKX + Bitget)
**Fonte 1 tentada:** `GET https://api.coingecko.com/api/v3/derivatives/exchanges/{id}/tickers`
- IDs tentados: `bybit_futures`, `okex_swap`, `binance_futures`
- **Falha:** HTTP 404 para todos os três. Causa: IDs incorretos (supostos, não verificados). Adicionalmente, suspeita de que este endpoint específico requer plano pago (Analyst+) no CoinGecko.

**Fonte 2 (OKX):** continua funcionando, mas com o problema de distorção por meme coins identificado acima.

---

## O Problema Estrutural Identificado

Além das falhas pontuais de cada fonte, há um **problema conceitual mais profundo**:

Todas as fontes tentadas até agora são **dados locais de uma única exchange**. Volume e OI de uma exchange refletem apenas a atividade dos usuários daquela plataforma — não o mercado global de futuros.

**Exemplo concreto:**
- SATS tem volume alto na OKX mas liquidez global baixa → entra no pipeline erroneamente
- Um token com mercado forte na Bybit mas fraco na OKX → é excluído erroneamente

O ideal é usar **dados agregados de múltiplas exchanges** para o filtro de liquidez — o mesmo dado que traders profissionais consultam antes de tomar posição.

---

## O que a Binance oferece (e por que não é suficiente sozinha)

A Binance é a maior exchange de futuros do mundo e seria a melhor referência. Porém:
- `GET https://fapi.binance.com/fapi/v1/ticker/24hr` — retorna volume de TODOS os símbolos em 1 chamada ✅
- `GET https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT` — retorna OI de **UM símbolo por vez** ❌

Para 300 tokens = 300 chamadas separadas de OI, com rate limit de 20/min = 15 minutos de espera. Inviável para um scanner em tempo real.

A Binance pode ser usada como **validação de volume** (1 chamada) mas não como fonte primária completa por falta de endpoint de OI em massa.

---

## O que esta pesquisa precisa encontrar

Preciso que você pesquise e avalie fontes de dados que atendam ao seguinte:

### Pergunta principal
> Existe alguma fonte de dados **gratuita** (sem pagamento ou com API key gratuita) que retorne, em **no máximo 3 chamadas HTTP**, o seguinte para todos os futuros perpétuos USDT ativos no mercado:
> - Volume 24h em USD
> - Open Interest total em USD
> - Preço atual
> - (Opcional) Funding rate
>
> E que seja acessível de IPs brasileiros sem geo-block?

### Fontes a pesquisar e avaliar

**Agregadores especializados em derivativos:**
- **CoinGlass** (`https://coinglass.com/api`) — especialista em OI agregado. Verificar: endpoint público gratuito, cobertura de tokens, formato de resposta, rate limits
- **Laevitas** — dados de derivativos. Verificar acesso gratuito
- **The Block** — dados de mercado. Verificar API pública
- **Velo Data** — dados de derivativos cripto

**Exchanges com API mais completa:**
- **Bitfinex** — verificar se tem endpoint de tickers de futuros perpétuos com OI em massa
- **Gate.io** (`/api/v4/futures/usdt/tickers`) — verificar se retorna OI junto com volume
- **MEXC** — verificar endpoint de futuros perpétuos
- **Hyperliquid** — exchange descentralizada com API pública, boa cobertura de perps

**Possibilidade combinada:**
- Binance volume (1 chamada) + OKX OI (1 chamada) como fonte híbrida
- Avaliar se essa combinação produz dados mais confiáveis que qualquer fonte individual

### Critérios de avaliação para cada fonte encontrada

Para cada fonte candidata, forneça:

1. **URL exata do endpoint** que retorna os dados necessários
2. **Formato de resposta** — campos disponíveis e seus nomes exatos
3. **Autenticação** — sem auth / API key gratuita / pago
4. **Cobertura** — quantos tokens perpétuos USDT retorna aproximadamente
5. **Chamadas necessárias** — 1 chamada para tudo ou múltiplas?
6. **Rate limit** — requisições por minuto/hora
7. **Geo-block Brasil** — acessível ou bloqueado?
8. **Dados agregados ou locais** — representa mercado global ou exchange individual?
9. **Exemplo de resposta** — trecho do JSON com os campos relevantes

### Resultado esperado

Um ranking das 3 melhores opções com justificativa, e uma proposta de hierarquia de fontes para substituir a arquitetura atual do scanner, que hoje usa:

```
Fonte 1: CoinGecko/exchange-specific (HTTP 404 — não funciona)
Fonte 2: OKX (funciona mas distorcida por meme coins)
Fonte 3: Bitget (funciona, universo menor)
```

---

## Contexto adicional

- O script é em Python, usa `requests` para chamadas síncronas e `aiohttp` para assíncronas
- A exchange de execução é a **WEEX** — mas os dados de mercado não precisam vir dela (ela provavelmente não tem API pública robusta)
- Os klines (OHLCV para análise técnica) já têm solução: Bitget primário + OKX fallback — **não precisa ser resolvido nesta pesquisa**
- O TradingView Scanner também já tem solução — **não precisa ser resolvido nesta pesquisa**
- O foco é exclusivamente: **qual fonte fornece volume + OI + funding rate de todos os perpetuals USDT em poucas chamadas, de forma confiável e acessível do Brasil**
