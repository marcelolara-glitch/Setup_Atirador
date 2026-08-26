# LEDGER — resultados de pesquisa (append-only)

Regras: toda rodada concluída gera entrada ANTES de qualquer decisão sobre ela.
Entradas antigas nunca são editadas — correções entram como novas entradas
referenciando a antiga. Formato: data | experimento | config | resultado-chave
| veredito | log/fonte.

---

## 2026-06-18 — juice 4h @12bps, agregado ~2a — INVALIDADO em 2026-07-03
Config: tf=4h, TIER1 (20), --cost-bps 12, janela ~2a até 18/06 (comando não
registrado no chat de origem). Fonte: chat "Correção de ancoragem", 18/06.

     H | trades | bruto% | liquido% | win% | p(<=0)
     4 |  1356  |  0.103 |  -0.017  | 47.1 | 0.572
     8 |  1353  | -0.038 |  -0.158  | 48.1 | 0.856
    16 |  1348  | -0.136 |  -0.256  | 47.0 | 0.897
    32 |  1344  | -0.076 |  -0.196  | 49.4 | 0.735
    48 |  1337  | -0.229 |  -0.349  | 49.2 | 0.839

Veredito na época: "drift 4h lava através do ciclo".
INVALIDAÇÃO (03/07): store 4h comprovadamente incompleto na data (TONUSDT=8
trades vs ~70 esperados); juice indexa horizonte por POSIÇÃO (i+h) e regime
por janela posicional de 150 barras — buracos corrompem retornos e entradas.
Código idêntico entre 18/06 e 01/07 (git: nenhum merge no intervalo).
A conclusão "4h não paga através do ciclo" SAI do registro. Pergunta reaberta.
[Ver CORREÇÃO de 2026-07-04 abaixo.]

## 2026-07-01 — juice 4h @12bps, agregado 2a — VÁLIDO (reproduzido em 03/07)
Config: tf=4h, TIER1 (20), 2024-05-22→2026-06-21, --cost-bps 12, seed 1337.
Log: logs/juice_sweep_20260703_1049.log (passada 1).

     H | trades | bruto% | liquido% | win% | p(<=0)
     4 |  1380  |  0.332 |   0.212  | 48.0 | 0.041
     8 |  1379  |  0.173 |   0.053  | 49.0 | 0.362
    16 |  1370  |  0.049 |  -0.071  | 48.0 | 0.632
    32 |  1366  |  0.205 |   0.085  | 50.4 | 0.374
    48 |  1365  |  0.131 |   0.011  | 50.2 | 0.486

Leitura vigente: melhor-de-5 ⇒ p_corr≈0.20 no taker (NÃO significante);
win% de moeda; estrutura serrilhada; agregado sem fatiar. Reprodução byte a
byte em 03/07 confirma determinismo. Gate: OOS (ver PENDENTES).

## 2026-07-03 — gap-scan do candle store — LIMPO
4h: 0 buracos; 15m: 0 buracos (contiguidade min→max por símbolo).
Sustenta a invalidação de 18/06 e valida a tabela de 01/07.

## 2026-07-02/03 — bracket Estágio 0 (bounds MFE/MAE 15m) — CINZA
Config: 15m, TIER1 (20), 2026-03-07→2026-06-06, grid 3×4×3, custos 12/6,
n=2835 entradas TREND. Log: logs/bracket_stage0_20260702_2336.log.
Melhor otimista: S=0.75/T=3.0/H=48 → +44.6 bps @12 / +50.6 @6.
Melhor pessimista: S=1.5/T=6.0/H=48 → −2.2 bps @12 / +3.8 @6 (IC cruza 0).
Sinais: cauda direita existe (14.1% correm +6 ATR sem tocar 1.5 ATR);
stop rates 34–72% confirmam excursão adversa imediata das entradas do
classificador. Veredito: CINZA — autorizou PR-2 (primeiro toque exato, #138).

## 2026-07-04 — CORREÇÃO da entrada de invalidação (03/07)
TONUSDT=9 trades na rodada atual com store 0-buracos ⇒ TON tem história
curta, não buracos; a "prova TON=8" da invalidação de 18/06 estava ERRADA.
A invalidação permanece por fundamento distinto: código idêntico entre as
rodadas (git), determinismo confirmado, tabelas incompatíveis ⇒ conteúdo do
store mudou entre 18/06 e 01/07; estado de 18/06 inauditável (rework do
backfill #136), estado atual limpo e reproduzível (3 reproduções, custo-
linearidade exata). Grau de confiança da invalidação: rebaixado de Certo
para Provável-forte.

## 2026-07-04 — juice 4h sweep 12/6/0 bps — COMPLETO (3/3)
Janela 2024-05-22→2026-06-21, TIER1(20), N≈1380. Log: juice_sweep_20260703_1049.
H=4 (hold 16h): bruto +0.332% p=0.007 | líq@6 +0.272 p=0.019 | líq@12 +0.212 p=0.041.
Demais H: fracos/ruído. Correção melhor-de-5: @0 p=0.035 (PASSA 5%),
@6 p≈0.095, @12 p≈0.205. Linearidade de custo exata (régua íntegra).
Leitura: sinal concentrado no hold curto; gate = OOS 4 janelas.

## 2026-07-04 — bracket exato 15m (Estágio 0.5, #138) — CANDIDATO-MAKER
Janela 2026-03-07→2026-06-06, n=2835. Log: bracket_exato_20260703_1135.
Resolução exata colapsou o bound otimista (ex-melhor +44.6 → −6.0 @12);
q≈0.19–0.25 conforme matemática de barreiras; AMB_SB≈0.1%.
@12: 1 célula EV>0, zero lo95>0. @6: 18 EV>0; lo95>0 em S=1.5/T=4.0/H=48
(EV 5.8, p .015) e S=1.5/T=6.0/H=48 (EV 8.2, lo95 2.6, p .002).
Caveats: melhor-de-36 ⇒ p_corr≈.07; janela única (regime atual); símbolos
correlacionados. Gradiente monotônico até a borda ⇒ ótimo fora da caixa.
Convergência com juice: dois instrumentos independentes apontam hold 12–16h.

## 2026-07-04 — OOS juice 4h, 4 janelas @0bps — GATE PASSOU (3/4)
Janelas iguais 2024-05-22→2026-06-21. Log: juice_oos4_20260704_1335.
H=4 bruto: W1 +0.902 (p.000) | W2 +0.122 (p.373) | W3 −0.051 (p.612) |
W4 +0.303 (p.050). Critério pré-registrado (sinal ≥3/4): PASSA na margem.
Estrutura: ~69% do agregado vem de W1; W2+W3 (um ano) ≈ ruído; perfil de
horizonte inverte entre janelas (W4: H=32 +2.16 p.000, H=48 +1.73 p.001;
W2/W3: longos fortemente negativos). Leitura: beta de tendência condicional
a regime; H=4 é o menos regime-sensível, não o mais forte. TON 0/0/0/9
confirma história curta (fecha a correção de 04/07).
Exploratório anotado (não primário): holds longos fortes no regime atual.

## 2026-07-05/06 — PR-4 (#139) validado — regressão 15m byte-idêntica
Pipeline pr4_pipeline_20260705_1634: rerun 15m pós-parametrização reproduziu
bracket_exato_20260703_1135 byte a byte (da zona de custo em diante) antes
de liberar o 4h. Instrumento multi-tf validado em produção.

## 2026-07-05 — PRÉ-REGISTRO Estágio 1 (travado ANTES da leitura do 4h)
Nulo por deslocamento circular (R=1000; preserva contagem, espaçamento,
sequência de direções/tilt e clustering por símbolo; destrói só o
alinhamento com preço). Block bootstrap temporal (bins de 14 dias-
calendário, 2000 réplicas). Gates @6bps, Bonferroni ×2 embutido:
MONEY = percentil 2,5 do block bootstrap do líquido > 0;
SKILL = p do nulo circular < 0,025.
Vereditos: FORTE (algum primário passa em ambos) / BETA (algum passa só
MONEY; avança ao Estágio 2 com anotação "valor no tilt/regime, não no
timing") / MORTO (nenhum passa MONEY).
Primários imutáveis: temporal H=4 | bracket S=1.5/T=6.0/H=4 (tf 4h).

## 2026-07-06 — bracket exato 4h (janela cheia) — CANDIDATO-TAKER*
Config: tf=4h, TIER1(20), 2024-05-22→2026-06-21, n=1365 (borda: 15).
Log: pr4_pipeline_20260705_1634 (VM).
36/36 células EV>0 nos dois custos (contraste 15m: 1/36 @12). lo95(iid)>0:
12 células @6, 3 @12. Sweet spot H=4–8 (16–32h); H=16 enfraquece.
CÉLULA PRIMÁRIA S=1.5/T=6.0/H=4: @6 EV +24.6, lo95 +3.6, p=.010 (iid);
@12 EV +18.6, lo95 −2.4, p=.047. Anatomia 77/21/1.7 (flat/stop/alvo) =
temporal H=4 com stop-desastre; custo do seguro ≈2.6 bps vs juice @6.
*As 3 células do veredito automático (1.0/4.0/8; 1.0/6.0/4; 1.5/4.0/8) são
melhor-de-36 → EXPLORATÓRIAS, não promovíveis. Gate final = Estágio 1
(block + shift); iid é referência otimista por construção.

## 2026-07-08 — ESTÁGIO 1 (stage1.py, 4h, janela cheia) — VEREDITO: MORTO
Config: tf=4h, TIER1(20), 2024-05-22→2026-06-21, n=1365 (borda 15).
Log: mais recente logs/stage1_*.log (VM). Gates pré-registrados @6bps, ×2:
  temporal H=4:      EV@6 +27.7 | blockP2.5 −13.3 → MONEY NÃO | p_shift 0.004 → SKILL SIM
  bracket 1.5/6/4:   EV@6 +24.6 | blockP2.5 −17.7 → MONEY NÃO | p_shift 0.004 → SKILL SIM
MORTO: nenhum primário com MONEY. Primários NÃO avançam ao Estágio 2.
Quadrante inédito SKILL-sim/MONEY-não: timing do classificador bate colocação
cega com mesmo tilt (1º positivo Bonferroni-robusto da campanha), mas a média
líquida through-cycle não é distinguível de zero sob bootstrap por bloco —
lucro concentrado em janelas trending; iid p=0.009 era lisonja (N efetivo
≈ blocos, não trades). Reconciliações: temporal 27.7 ↔ juice 27.2 (15 bordas);
bracket 24.6 ↔ célula primária do bracket4h. Efeito real, episódico, pequeno
demais para promover. MORTO ≠ provado-zero.
Nits do PR-5 (não-bloqueantes): n=0 degrada p/ MORTO sem abort; frase BETA
imprecisa no caso cruzado SKILL-só/MONEY-só; p_shift sem correção +1.

## 2026-07-08 — PRÉ-REGISTRO: programa TSMOM canônico (rota pós-MORTO)
Travado antes de qualquer código ou rodada. Juiz: stage1 (calibrado 07/07,
oracle + controle morto), INALTERADO — PR-6 troca só o gerador de entradas.

SINAL (canônico, sem classificador): a cada barra 4h,
ret_L = close_t/close_{t−L} − 1; LONG se >0, SHORT se <0; sem skip.
Entradas: primeira barra de cada troca de sinal + reentrada a cada 48
barras enquanto o sinal persiste (semântica idêntica a _trend_entries —
premissas de espaçamento/clustering do juiz preservadas).
Elegibilidade: L barras de história + 48 forward (measure_event).

PRIMÁRIOS (Bonferroni ×2 por fase, imutáveis):
  P1 temporal: L=84 (2 semanas), H=48 (8 dias)
  P2 bracket:  L=84, S=1.5/T=6.0, H=48
EXPLORATÓRIOS (reportados, jamais promovíveis nesta rodada):
  L ∈ {42, 168} × H ∈ {16, 32} × ambos os exits.
Limite de instrumento registrado: holds > 48 barras (8d) exigem cirurgia
em HORIZONS/excursion — fora deste programa; se vier, é novo pré-registro.

FASES (famílias separadas; B roda independente do veredito de A):
  A: TIER1 (20 majors) — dados existentes, roda imediato pós-PR-6.
  B: TIER2 = 40 perps USDT seguintes por volume 24h (snapshot congelado
     no ledger no dia do backfill), história 4h ≥ 400 dias, ex-TIER1.
     Caveat registrado: snapshot atual carrega viés de sobrevivência
     (flatteia a perna LONG) — a leitura desconta isso explicitamente.

GATES por fase (custo 6 bps, mesmos limiares de 05/07):
  MONEY = blockP2.5 (bins 14d, 2000 réplicas) do líquido > 0
  SKILL = p do nulo circular < 0.025
  Vereditos FORTE / BETA / MORTO com semântica inalterada.
OOS 4 janelas @0bps: DIAGNÓSTICO reportado, NÃO gate — o block bootstrap
é o teste through-cycle. Fixado agora para não re-litigar após os números.

MORTE DO PROGRAMA: A MORTO e B MORTO ⇒ TSMOM canônico morre; o candidato
seguinte do menu de 01/07 (Donchian/Turtle) exige novo pré-registro.
PROMOÇÃO: FORTE ou BETA em qualquer fase ⇒ Estágio 2 (shadow execution).

REGRA DO CEMITÉRIO (nova, permanente): toda rodada do stage1 —
exploratória ou não — ganha entrada no ledger ANTES do teste seguinte.
A família de testes cresce ⇒ a correção cresce junto.

## 2026-07-09 — CORREÇÃO de instrumentação: wrapper do pipeline PR-6
`bash -c '...' _ "$OLD"` entrega $OLD em $1, e o script lia "$2" → o diff
comparou VAZIO × rerun ("0a1,19") e o guard, corretamente fail-closed,
bloqueou o TSMOM. A regressão em si PASSOU: o rerun reproduziu o log MORTO
número a número (PR-6/#141 validado em produção). Bug do wrapper — autoria
do revisor, não do Code. Lição permanente: wrapper de shell é código;
testa-se antes de emitir.

## 2026-07-09 — FASE A: TSMOM canônico L=84/H=48, TIER1 — VEREDITO: MORTO
Config: 4h, 2024-05-22→2026-06-21, n=2374 (borda 27), gates de 08/07.
Log: faseA_tsmom_20260709_2013 (VM).
  temporal H=48:    EV@6 +32.9 | blockP2.5 −58.4 → MONEY NÃO | p_shift 0.075 → SKILL NÃO
  bracket 1.5/6/48: EV@6 +16.0 | blockP2.5 −40.4 → MONEY NÃO | p_shift 0.318 → SKILL NÃO
MORTO nos dois gates — mais morto que o classifier (SKILL 0.004 lá).
Achado comparativo: em majors, o timing do classificador de regime BATE o
TSMOM canônico (0.004 vs 0.075). EV pontual maior (32.9) mas variância de
hold 8d ~3× engole o CI; bins 14d vs hold 8d tendem a lisonjear o gate ⇒
falha a fortiori. Stop 1.5 ATR em 8d custa ~17 bps (16.0 vs 32.9).
Fase B (alts) segue por pré-registro — independe de A.

## 2026-07-11 — FASE A exploratórios: família TSMOM L×H, TIER1 — CONSOLIDADO
8 rodadas (batch único, ruling de família declarada): L∈{42,84,168} ×
H∈{16,32,48} menos a primária. Log: faseA_explor_20260711_0145 (VM).
MONEY: 0 de 16 células (blockP2.5 −16 a −81) — somado às 2 primárias,
18/18 negativo ⇒ o MORTO da Fase A vale para a FAMÍLIA, não para a célula.
Estrutura: L=42 morto; L=168 fraco/negativo (bracket <0 em todos os holds);
L=84 domina (EV@6 27–33 nos três holds) ⇒ pré-registro escolheu o L certo.
Único SKILL: L=84/H=16 p_shift 0.020 — melhor-de-16, não sobrevive a
correção, MONEY negativo ⇒ observação, não candidato. Custo do stop cresce
com hold (gap bracket−temporal: 1.8→6.9→16.9 bps em H 16→32→48).
Nota de família ampliada: programa paralelo (SMC_Monitor) soma à contagem
de hipóteses do pesquisador — p marginais de ambos leem-se com isso.

## 2026-07-11 — TIER2 CONGELADA (snapshot 21:00 UTC) — FASE B LIBERADA
Critérios: 40 perps USDT por notional 24h, ex-TIER1, ex-TradFi (emenda
XAU/XAG/XAUT/PAXG), listTime ≥ 400d, live. 155 elegíveis. UNIVERSO
CONGELADO (nenhum símbolo entra ou sai):
HYPE VIRTUAL PEPE UNI AAVE HMSTR PARTI TRUMP KAITO XLM ONDO FIL HBAR
GRASS THETA MOODENG TAO LDO IOTA INJ JTO ETHFI CHZ MORPHO ORDI SHIB
AGLD PENGU TIA ICP FARTCOIN CRV PYTH PLUME WIF BONK BLUR EIGEN ZRO ETC
(sufixo USDT). Deriva registrada: snapshot contaminado de 01:53 (XAU)
difere deste em ~8 nomes — ranking por volume deriva em horas; o
congelamento existe para isso.
Backfill 740d/4h: 40 jobs, 0 falhas, 233s; 429s 100% recuperados na 1ª
retry. Piloto pré-existente estendido sem emenda (AAVE/FIL/INJ/TIA/ICP
779d). GAP-SCAN: 0 buracos em 40/40; mínimo PLUME 429d ≥ 400 ⇒ regras
mecânicas de exclusão dispararam ZERO vezes.
JANELA Fase B: 2024-05-22→2026-06-21 (idêntica à Fase A; ~20d pós-21/06
deliberadamente fora — sem segunda olhada em janela estendida). Proibido
refresh de TIER1 antes da Fase B. Ruído "FALTAM 143" no coverage = cauda
defasada do store + eras-piloto, sem ação.
Asteriscos pré-declarados: sobrevivência flatteia perna LONG; custo
executável de memes > premissa 6bps ⇒ FORTE/BETA nasce condicionado ao
Estágio 2 com força extra.

## 2026-07-12 — FASE B: TSMOM L=84/H=48, TIER2(40) — VEREDITO: MORTO
Config: 4h, 2024-05-22→2026-06-21, n=4585 (borda 0 — store TIER2 até 11/07
deu forward real às entradas tardias; assimetria inofensiva vs Fase A,
registrada). Log: faseB_tsmom_20260711_2252 (VM).
  temporal: EV@6 +9.0  | blockP2.5 −88.7 → MONEY NÃO | p_shift 0.386 → SKILL NÃO
  bracket:  EV@6 +11.4 | blockP2.5 −41.9 → MONEY NÃO | p_shift 0.153 → SKILL NÃO
MORTO nos dois gates. A-FORTIORI: resultado veio COM o viés de sobrevivência
pré-declarado a favor da perna LONG. EV/trade em alts = 1/3 do de majors —
o território da alegação acadêmica falhou mais forte que o controle.
Observação: bracket > temporal pela 1ª vez (stop paga em cauda esquerda de
memes). Pendente: batch exploratório 8 combos (pré-registrado, informativo).

## 2026-07-12 — PRÉ-COMPROMISSO: fechamento de família (travado antes
## do pré-registro Donchian)
Donchian/Turtle herda o menu de 01/07 com PRIOR REBAIXADO (mesma família
de sinal que TSMOM, morto em 2 universos). CLÁUSULA: se Donchian morrer
nos gates vigentes (A e B), a família "tendência direcional por ativo
sobre OHLCV" FECHA INTEIRA — sem variantes de gatilho (Keltner, NR4/7,
VCP, squeeze breakout etc.) — e a decisão sobe para CLASSE DE DADO:
funding/carry, cross-section, ou plataforma-observatório. Cerca
anti-strategy-shopping plantada antes da tentação.

## 2026-07-14 — FASE B exploratórios (8 combos, TIER2) — CONSOLIDADO + H-42
Log: faseB_explor_20260714_1211 (VM). Estrutura INVERTIDA vs majors: mel em
L curto. L=42: SKILL 5/6 gates; temporal H=32 EV@6 61.1 (p .000, block −4.2);
BRACKET H=16: EV@6 43.2, blockP2.5 +0.9, p_shift .000 — ÚNICO duplo-passe
em 36 pares de gates do programa. NÃO-PROMOVÍVEL: exploratório por registro,
melhor-de-36, margem +0.9 contra os DOIS asteriscos pré-declarados
(sobrevivência LONG; custo de meme > 6bps). L=84 fraco; L=168 morto.
Bracket > temporal em H=16 (stop paga em cauda gorda de alts).
REGISTRO DE HIPÓTESE H-42: TSMOM L=42/H=16, bracket 1.5/6.0, TIER2
congelada — promovível EXCLUSIVAMENTE via validação forward (shadow);
amostra histórica exausta pela exploração (validação in-sample = circular).
Protocolo forward a pré-registrar ANTES do primeiro dado.

## 2026-07-14 — PROGRAMA TSMOM CANÔNICO: ENCERRADO (termos de 08 e 12/07)
Primários MORTO em A (TIER1) e B (TIER2). Placar: 36 pares de gates,
MONEY 1/36 (exploratório, ver acima), SKILL 6/36. Legado: timing de
tendência existe, episódico — L≈84 em majors (classificador > canônico,
p .004), L≈42 em alts — e não converte em dinheiro through-cycle a custo
maker em nenhuma colheita testada. Cemitério íntegro: toda célula julgada
está neste ledger.

## 2026-07-14 — PRÉ-REGISTRO: programa DONCHIAN (herança de 12/07,
## prior rebaixado, cláusula de fechamento incorporada)
SINAL (canônico, closes): na barra i, LONG se close_i > max(closes das N
barras anteriores); SHORT se < min; senão sem sinal. Entradas espaçadas
48 barras por direção (semântica do soquete). Elegibilidade: N barras de
história + 48 forward.
PRIMÁRIOS (×2, imutáveis, NÃO-contaminados pelo brilho do TSMOM-42):
N=120 (20 dias — Turtle S1) | temporal H=48 | bracket S=1.5/T=6.0/H=48.
EXPLORATÓRIOS (família declarada, jamais promovíveis): N ∈ {42, 240} ×
H ∈ {16, 32, 48} × ambos os exits.
FASES: A = TIER1(20); B = TIER2 congelada de 11/07 (mesma janela
2024-05-22→2026-06-21, mesmos asteriscos). Gates e vereditos de 08/07
INALTERADOS. Brilho exploratório (se houver) → fila forward junto ao H-42.
CLÁUSULA (de 12/07, reafirmada): primários MORTO em A e B ⇒ família
"tendência direcional por ativo sobre OHLCV" FECHA INTEIRA; decisão sobe
para classe de dado (funding/carry, cross-section, ou observatório).

## 2026-07-18 — DONCHIAN N=120: FASE A (TIER1) = FORTE | FASE B = MORTO
Pipeline donchian_pipeline_20260718_0218; regressão tsmom byte-idêntica OK.
FASE A (n=933, borda 5):
  temporal: EV@6 +90.4 | block −82.5 → MONEY NÃO | p_shift .034 → SKILL NÃO
  bracket:  EV@6 +88.6 | block +3.4  → MONEY SIM | p_shift .004 → SKILL SIM
  VEREDITO FORTE — 1º e único duplo-passe PRIMÁRIO em 10 pares da campanha.
FASE B (n=1850): bracket EV@6 +77.2, SKILL .001, block −12.8 → MORTO
  (asteriscos pré-declarados cortando contra; morte limpa).
Anatomia: mesma média do temporal; o stop 1.5 ATR é seguro de variância
(block +3.4 vs −82.5) — entradas raras em ignição de rompimento
(~23/símbolo/2a). Honestidade: margem MONEY fina; 1 FORTE em 10 pares ⇒
sorte agregada residual [Provável ~1/5] ⇒ Estágio 2 obrigatório, capital
proibido. CLÁUSULA de fechamento NÃO dispara (exigia A e B mortos).
Família tendência-direcional: ENCERRADA para novos testes históricos;
sobrevivente único DONCHIAN-A → fila forward (à frente do H-42).

## 2026-07-18 — PRÉ-REGISTRO: ESTÁGIO 2 — observação ao vivo DONCHIAN-A
(travado antes do primeiro dado forward; sem dinheiro em nenhuma hipótese)
CANDIDATO (imutável durante toda a observação): DONCHIAN N=120, 4h,
TIER1 (20 majors, congelados nesta data), bracket S=1.5 ATR / T=6.0 ATR
/ H=48 barras, entradas espaçadas 48 barras por direção. Qualquer
mudança de parâmetro ⇒ o relógio reinicia do zero.
MECÂNICA: detecção no fechamento de cada barra 4h; cada sinal registra
(a) preço teórico (close) e (b) realidade executável no momento da
detecção — melhor bid/ask e spread. Posição simulada pelo bracket nas
barras seguintes; saídas registradas do mesmo jeito. Nenhuma ordem
enviada, nenhum capital.
CUSTO: medido, não assumido. Três cenários reportados — (i) EXECUTÁVEL
(cruza o spread + taxa taker nos dois lados; o cenário "qualquer um
consegue"), (ii) 6 bps da pesquisa, (iii) 0 bps. O VEREDITO usa (i).
JANELA: mínimo 12 semanas OU 80 sinais fechados — o que vier POR ÚLTIMO.
VEREDITOS (travados agora):
  CONFIRMA: média líquida > 0 no executável E spread mediano ≤ 10 bps
    ⇒ vivo; conversa de capital mínimo vira decisão dedicada, à parte.
  VIVO-COM-ASTERISCO: média > 0 mas spread mediano > 10 bps ⇒ +6
    semanas; o problema é execução, não sinal.
  ENTERRA: média ≤ 0 no executável ao fim da janela ⇒ candidato morre;
    a cláusula de 13/07 termina de disparar; família fechada em
    definitivo; decisão sobe para classe de dado.
SEMPRE REPORTADO: slippage teórico→executável por sinal e comparação
com a expectativa da pesquisa (+88.6 bps @6) — informativa, não gate.
H-42 (TSMOM L=42 alts): segundo passageiro do MESMO shadow após 4
semanas estáveis, por ativação registrada — sem novo protocolo.
INFRA: PR-9 (módulo shadow + cron 4h + relatório semanal); briefing
após a trava. Runtime v9 de produção INTOCADO.

## 2026-07-18 — Exploratórios Donchian Fase A (6 combos) — VIZINHANÇA VIVA;
## MONEY marginal em toda a família
Log: donchA_explor_20260718_1817. Bracket (exit do vencedor): EV@6
positivo nos 6 vizinhos (55.5→116.2 bps, crescendo com N) e SKILL 6/6
(p .003–.006); block P2.5 negativo nos 6 (−8.9 a −18.0) ⇒ o +3.4 do
primário segue o ÚNICO passe de MONEY da campanha. Temporal: morto em
toda parte (block −84 a −219).
Leitura: vencedor em crista elevada e coerente — probabilidade de "sinal
por sorte" CAI; a dúvida concentra-se na monetização through-cycle,
exatamente o que o Estágio 2 mede. N=240 (EV 116.2) anotado e ARQUIVADO:
exploratório não promove; eventual 2º passageiro do shadow, nunca antes.
Achado estrutural: em rompimento, o stop é metade do payload (bracket ≫
temporal em 6/6; sem stop a família morre). Nada muda no protocolo.

## 2026-07-19 — EMENDA ao protocolo Estágio 2 (antes da trava): relatório
## DIÁRIO + moldura de leitura do journal v9
Relatório do shadow passa de semanal para DIÁRIO via Telegram, tag
[VIGIA]: acumulado-que-decide no topo (dia N/84, sinais X/80, média
líquida no executável, spread mediano, delta vs pesquisa), abertas no
meio, "hoje" no rodapé. Regras de veredito INALTERADAS; ruído diário não
autoriza ajuste (cláusula anti-tinkering vigente). v9 permanece intocada
e LIGADA (coletora); canal antigo silenciado no cliente, zero código.
MOLDURA PRÉ-COMPROMETIDA da leitura do journal v9 (~2,5 meses forward,
cego, resolve o asterisco das conclusões 15m pré-reparo): leitura
DESCRITIVA; negativa/morna ⇒ veredito de junho confirmado com dado
forward; positiva ⇒ NÃO ressuscita — candidata a julgamento formal
pelos juízes calibrados antes de qualquer mudança de status.

## 2026-07-19 — JOURNAL v9 (FORWARD CEGO 24/04→18/07): CONFIRMA MORTO | ESTÁGIO 2 TRAVADO | DESATIVAÇÃO v9 DECIDIDA
Moldura pré-comprometida (19/07): leitura descritiva; positiva não
ressuscita. Janela 85d: 5427 trades, 5381 fechados/expirados, 46 OPEN.
Status: LOSS_SL 2484 | WIN_TP1 1790 | WIN_TP2 351 | WIN_TP3 23 |
EXPIRED 733. TP1-or-better (decididos): 46.6% (2164/4648) vs ~71%
necessário p/ breakeven na estrutura 50/30/20. PnL −3890% agregado;
4/4 meses negativos (abr −288 | mai −1516.7 | jun −1270.2 | jul −815.1);
todos os setups principais negativos. TP3: 23/4648 = 0.49%.
Setups inertes: rev_exaust 1 disparo em 85d; break_range 15.
VEREDITO: forward cego CONFIRMA os vereditos históricos MORTO da
família SMC v9. Cláusula de fechamento vigente, sem emenda.
DECISÃO 1: protocolo Estágio 2 emendado TRAVADO (regras de 18–19/07).
DECISÃO 2: runtime v9 será DESATIVADO — papel de coletora encerrado
com esta leitura. Execução condicionada, pré-registrada:
  (a) PR-9A + PR-9B merged, shadow DONCHIAN-A rodando;
  (b) [VIGIA] diário estável 3+ dias consecutivos sem erro;
  (c) leitura final do journal v9: OPEN remanescentes marcados como
      TRUNCADOS no ledger (não contam como EXPIRED);
  (d) cronfile v9 → .DISABLED (padrão v8); DBs, logs e código
      preservados; nenhuma exclusão.
Até (a)-(b), cron v9 permanece ativo como pulso da VM.
Nota de precisão: DONCHIAN-A NÃO foi validado como operável — passou
Fase A com margem fina e foi promovido a observação; o que roda no
shadow é candidato em julgamento forward, não sistema aprovado.

## 2026-07-19 — PR-9A/9B/9C MERGED: SHADOW DONCHIAN-A NO AR | RELÓGIO ESTÁGIO 2 INICIADO
PR-9A (motor): exceção registrada ao teto de 200 linhas (226 brutas /
188 SLOC; branco+traçabilidade); revisão executável: gêmeo sintético
20 walks, 765 entradas e 765 vereditos idênticos vs bancada
(donchian_entries+first_touch), zero divergências.
PR-9B ([VIGIA]+deploy): 199 linhas; layout conforme moldura da emenda
19/07; cobertura de runs 6/dia com alerta (requisito da revisão 9A).
PR-9C (execução): captura bid/ask no instante da detecção
(evidence_json.exec; falha nunca bloqueia abertura); topo do [VIGIA]
completo (líq executável, spread mediano, Δpesq); FECHADOS cap 5,
macro LONG/SHORT, RADAR cap 3 (observação, sem ação). Auditoria
aritmética independente do build_report: PASS.
CONSTANTE DE IMPLEMENTAÇÃO (não é número do protocolo): TAKER_FEE_BPS
= 5.0 (OKX perp taker não-VIP, tier "qualquer um consegue" da premissa
:323-325). Troca exige registro aqui.
CAVEAT DE LEITURA (fixado): Δpesq = líq@EXECUTÁVEL − 88.6@6bps —
mistura bases de custo por construção; Δ ≈ −(spread+10−6) bps é
esperado com edge idêntico ao da pesquisa. Δ é informativo, não gate;
Δ negativo pequeno NÃO é decaimento de edge.
LIMITES CONHECIDOS (revisão 9A/9B): sinal avaliado só na última barra
fechada de cada run — cron perdido = barra nunca avaliada (alerta no
[VIGIA]); outage >~25d degrada resolução de OPENs (janela fetch 200
barras); [VIGIA] importa _tg_send privado — refactor de telegram.py
mata o relatório em silêncio. REGRA OPERACIONAL: [VIGIA] ausente às
21:20 BRT é INCIDENTE. Spread de saída = proxy pela entrada
(disclosure no código).
RELÓGIO ESTÁGIO 2: início na instalação do cron (19/07/2026); janela
mín. 12 semanas OU 80 fechados, o que vier POR ÚLTIMO; veredito pelas
regras travadas; anti-tinkering vigente — RADAR não autoriza nada.

## 2026-07-19 — TONUSDT INDISPONÍVEL (DELISTING OKX): TIER1 20 CONGELADO, OBSERVÁVEIS 19
Probe direto: OKX 51001 (instId inexistente; lista SWAP sem TON*),
Bitget 40034. Estrutural, não transitório. TIER1 NÃO é alterado —
substituição adulteraria o experimento; TON permanece como membro de
0 sinais. Veredito inalterado (conta sinais/80); cobertura passa a
ser sobre 19 observáveis. Atrito de universo = informação do forward.
RELÓGIO ESTÁGIO 2: válido desde 19/07 (instalação 14:39 UTC); [VIGIA]
de 19/07 mostrará runs 2/6 (esperado, não incidente); dia 1 LIMPO da
janela = 20/07.
PR-9D promovido a prioridade: [VIGIA] deve expor falhas POR SÍMBOLO
(hoje só conta agregada — símbolo pode falhar 6/6 por 12 semanas
invisível atrás de "runs 6/6").

PR-9D merged (19/07): falhas por símbolo em shadow_runs + linha no
[VIGIA] com alerta de dia-inteiro. Revisão: 51/51 + auditoria
independente da agregação (dedupe, NULL, JSON ilegível) PASS.

## 2026-07-25 — RUNTIME v9 DESATIVADO: CONDIÇÕES (a)-(d) DE 19/07 EXECUTADAS
(a) PR-9A/9B/9C/9D merged; shadow DONCHIAN-A no ar desde 19/07 14:39 UTC.
(b) [VIGIA] estável 5 dias consecutivos (20-24/07): runs 6/6 todos os
dias, falha só TONUSDT (delisting conhecido), enviado=True 7/7 às 00:20.
(c) Leitura FINAL do journal v9 (24/04→25/07, 92d): 5760 trades —
LOSS_SL 2639 | WIN_TP1 1881 | WIN_TP2 373 | WIN_TP3 24 | EXPIRED 798 |
OPEN 45. TP1-or-better decididos: 46.3% (2278/4917) — consistente com
a leitura de 19/07; veredito MORTO inalterado. Os 45 OPEN ficam
TRUNCADOS nesta data (não contam como EXPIRED; congelados no DB).
(d) Cronfile → atirador-scan-v9.DISABLED (padrão v8); DBs, logs e
código preservados; telegram_bot mantido para consultas SQL.
Heartbeat da VM passa a ser o shadow (6 runs/dia + [VIGIA] diário).
NOTA (não é gate, não autoriza ação): semana 1 do shadow = 9 fechados,
9 LOSS (−9.0R), 6 OPEN (bloco SHORT 23-24/07). Whipsaw em bloco é o
modo de falha esperado do detector; breakeven do bracket ≈ 20% WR;
anti-tinkering vigente; veredito só ao fim da janela (80 fechados /
12 semanas, o que vier por último).

## 2026-08-09 — EMENDA: reabertura da família com pedágio + reenquadramento do juiz

Motivo. A cláusula de 13/07 ("família encerrada para novos testes históricos,
sem exceção") era mais larga que o argumento que a sustentava. O argumento real
é orçamento de multiplicidade sobre a amostra 2024-05-22→2026-06-21, não
"tendência direcional não existe". Redação corrigida abaixo. Isto NÃO revoga
nenhum veredito anterior; todos permanecem MORTO.

Contabilidade da campanha (auditada por grep no próprio ledger, 09/08):
10 pares primários, alpha idêntico em todos (MONEY = blockP2.5 > 0;
SKILL = p_shift < 0.025; correção ×2 apenas INTRA-rodada, nunca entre rodadas).
  MONEY: 1 passe em 10  → P(≥1 passe por acaso em 10 @2.5%) = 22%
  SKILL: 5 passes em 10 → esperado 0.25. NÃO é acaso.
Leitura: o achado robusto da campanha é que estes detectores acertam QUANDO
melhor que o acaso; o que não sobrevive é o custo de 5 bps taker. O único
passe de MONEY (DONCHIAN-A, blockP2.5 +3.4) tinha ~1 em 4,5 de ser sorte pura.

REGRA 1 — Reabertura com pedágio.
A família "tendência direcional por ativo sobre OHLCV" está REABERTA para novos
testes históricos na janela 2024-05-22→2026-06-21, sob limiares agravados:
  MONEY = blockP2.5 do líquido @6bps > +5 bps   (era > 0)
  SKILL = p_shift < 0.005                        (era < 0.025)
O agravamento existe porque 10 rodadas ao alpha original já consumiram o
orçamento a 22% de falso-positivo. Sob esta régua, o DONCHIAN-A NÃO teria
passado — e não deveria ter passado.
Permanece intocado: pré-registro ANTES do dado; UM primário por ideia;
exploratório NUNCA promove; nenhuma varredura de parâmetro após falha do
primário; toda rodada entra no ledger antes do teste seguinte.

REGRA 2 — O juiz é fila, não certificado.
Passar no juiz histórico NÃO é aprovação de edge. É entrada na fila de shadow
forward. O recurso escasso é o slot de observação (~12 semanas cada), e a
função do juiz é ordenar candidatos para esse slot. O único gate que certifica
é o forward, porque encontra dados que não existiam quando a estratégia foi
desenhada. O juiz histórico já provou empiricamente que não prevê lucro:
aprovou o DONCHIAN-A, que está em 6.9% de acerto contra 20% de breakeven.
Hold-out pós-2026-06-21: DESEJÁVEL, não obrigatório (~3,5 blocos, poder baixo
demais para reprovar; serve para detectar desastre óbvio, não para certificar).

REGISTRO PRÉ-VEREDITO — DONCHIAN-A (Estágio 2).
Estado em 08/08: dia 21/84, fechados 29/80, WIN 2 · LOSS 24 · EXPIRED 3,
ΣR -14.89. Decomposição exata: 24 LOSS são stop cheio (-1.00R cada); 3 EXPIRED
levemente positivos (+0.24/+0.49/+0.38); 2 WIN a +4.00R. 83% dos fechados
batem stop. Breakeven do bracket 4:1 = 20% de acerto; observado 6.9%.
Binomial P(X<=2 | n=29, p=0.20) = 0.052 — NÃO cruzou o limiar de 0.025.
A janela segue até o fechamento pré-registrado (80 fechados OU 84 dias, o que
for MAIS TARDE). Sem veredito interino.
HIPÓTESE DE TRABALHO REGISTRADA AGORA, antes do veredito: o DONCHIAN-A foi o
falso-positivo esperado dos 22%. Se ele virar, vira contra expectativa escrita.

PENDÊNCIA TÉCNICA ABERTA (não bloqueante do veredito): Δpesq -186.9 bps no
VIGIA de 08/08 excede em ordem de magnitude o caveat registrado ("delta
levemente negativo por bases de custo mistas"). Verificar se `líq exec` é
acumulado ou média por trade. Candidato a bug de medição, não a decaimento.
NIT: falha `TONUSDT ×6` diária é estrutural (delistada da OKX, 07/2026);
marcar como permanente no VIGIA para não treinar o olho a ignorar o campo.

## 2026-08-09 — PRÉ-REGISTRO: KEEP IT SIMPLE (FabioMaistro) — antes de qualquer dado

Fonte: tradingview.com/script/nP5fBSfa/ (Pine v6, open-source, publicado 08/2026).
Prior externo NULO: script novo, sem track record, sem crítica pública. Registrado
como fraqueza, não como impedimento.

AUDITORIA DO PINE (feita antes do pré-registro):
- corEstado tem 4 estados alcançáveis. O 5º (CINZAGRID) exige empate exato de
  float (close==mmeCurta ou mmeCurta==mmeLonga) — inalcançável na prática.
  A "neutral/transition" da descrição é decorativa.
- Bollinger (21, 1.3σ, base EMA) alimenta APENAS fill(). Nenhuma decisão depende
  dela. EXCLUÍDA dos primários por decisão explícita (09/08).
- forca = adxV + variacao soma unidades incompatíveis (ADX 0-100 limitado +
  distância % |SMA5-SMA13| ilimitada); cortes 15/20/25/30 calibrados para ações.
  variacao é cega à direção (valor absoluto). EXCLUÍDA dos primários.
- Conteúdo negociável real: estado (4 valores) + forca>forca[1]. O resto é pintura.

DETECTOR (a ser implementado, sem grau de liberdade):
  mmeCurta = EMA(close,8); mmeLonga = EMA(close,21); tf 4h; universo TIER1(20).
  estado = VERDE  se close>mmeCurta e mmeCurta>mmeLonga
         | AZUL   se close<mmeCurta e mmeCurta>mmeLonga
         | ROXO   se close>mmeCurta e mmeCurta<mmeLonga
         | VERM   se close<mmeCurta e mmeCurta<mmeLonga
  Entrada LONG  = barra fechada em que estado passa a VERDE (de qualquer outro).
  Entrada SHORT = barra fechada em que estado passa a VERM.
  Execução no open da barra seguinte. Warmup: descartar 21 primeiras barras/símbolo.

PRIMÁRIO 1 — NATIVO (saída da própria máquina de estados)
  Sai quando estado deixa VERDE (LONG) / VERM (SHORT). Cap duro H=48 barras.
  SEM stop. NÃO OPERÁVEL (cauda descoberta) — é instrumento de medição, mede o
  indicador como escrito. Se passar, NÃO vai a shadow sem redesenho de saída.

PRIMÁRIO 2 — BRACKET
  S=1.5 ATR14 · T=3.0 ATR14 · H=24 barras. Puro S/T/H, sem override por estado.
  Derivação (nenhuma do dado): S=1.5 idêntico à campanha (comparabilidade);
  T=3.0 porque 4:1 foi calibrado para sinal de 20 dias e EMA8/21 em 4h tem
  memória ~3-4 dias — mesmo erro do TP3 a 3.5xATR diagnosticado no v9 em 04/2026;
  H=24 ≈ uma memória completa da EMA longa (centro de massa ~21 barras).

CONFUNDIMENTO CONHECIDO (registrado ANTES do resultado):
  Os 10 pares anteriores usaram bracket T=6.0/H=48. Aqui T=3.0/H=24. Se o
  Primário 2 passar e o DONCHIAN não, "T=3.0 é melhor em cripto 4h" é explicação
  concorrente e NÃO separável nesta rodada. Não pode ser descoberto depois.

SECUNDÁRIOS (descritivos, NÃO promovíveis, nunca reciclados como primário):
  forca>forca[1] no momento da entrada; largura BB relativa; estado de origem
  da transição. Reportados para autópsia apenas.

GATES (pedágio da emenda 09/08):
  MONEY = blockP2.5 do líquido @6bps > +5 bps
  SKILL = p_shift < 0.005
  Correção ×2 intra-rodada já embutida (2 primários), como nas 10 anteriores.

CRITÉRIOS DE MORTE:
  Nenhum primário com MONEY ⇒ MORTO. Sem varredura de parâmetro após falha.
  n < 400 entradas na janela ⇒ INCONCLUSIVO (não é licença para afrouxar gate).
  Janela: 2024-05-22 → 2026-06-21, candles_v9.db, seed 1337, determinístico.

EXPECTATIVA REGISTRADA ANTES DO FATO:
  [Provável] morre no custo, não no sinal — EV@0 positivo com EV@6 negativo.
  Estado de EMA8/21 em 4h gira muito; a 5 bps/perna precisa de ~10 bps brutos
  por trade só para empatar. Se o padrão observado for outro, é informação.

### EMENDA ao pré-registro KEEP IT SIMPLE (10/08, ANTES de qualquer rodada)

1. EXECUÇÃO: entrada no CLOSE da barra do sinal, não no open da seguinte.
   É a convenção de measure_event usada nos 10 pares anteriores. O
   pré-registro estava errado; a convenção existente prevalece.

2. ESPAÇAMENTO: SEP = 0 para o keepitsimple (irmãos mantêm SEP_BARS=48).
   Razão: o primário NATIVO já se auto-espaça — o estado é único, só se
   reentra depois de sair. A carência de 48 barras não protegia de nada e
   descartava a maioria das transições, testando "primeira virada após 8
   dias de cooldown" em vez do indicador. Reversão de decisão anterior
   (Claude havia recomendado manter 48; argumento de comparabilidade era
   fraco e foi retirado).
   PREMISSA DE CAPITAL REGISTRADA: no primário BRACKET (H=24) o estado pode
   voltar antes do fechamento, empilhando ~2-3 posições simultâneas no mesmo
   símbolo. É perfil de risco distinto dos 10 pares anteriores. A correlação
   estatística da sobreposição é tratada pelos bins de 14 dias do block
   bootstrap; a premissa de capital NÃO é tratada e fica anotada.
CORREÇÃO (10/08, antes da rodada): a estimativa "~2-3 posições simultâneas"
   estava subdimensionada. Smoke sintético com SEP=0 deu hold mediano 3 barras
   e 4,1x mais entradas; com bracket H=24 o empilhamento provável é 5-8
   posições por símbolo. Premissa de capital MAIS pesada que a registrada.
   Medição permanece válida (correlação tratada pelos bins de 14d); a distância
   entre medido e operável é maior do que o texto original sugeria.

3. GATES: reafirmados MONEY > +5.0 bps e SKILL < 0.005. A implementação
   inicial do PR usava os antigos (0 / 0.025); corrigida antes do merge.

4. REGRA DE DECISÃO — travada ANTES de ver qualquer número:
   Custo fixo do gate = 6 bps/trade. Lê-se EV@0 da tabela informativa.
     EV@0 < 6 bps  ⇒ o sinal não paga o pedágio. Churn CONFIRMADO por
       mecanismo. Habilita a RODADA 2 (abaixo).
     EV@0 > 6 bps e MONEY reprova ⇒ o problema NÃO é custo, é variância/
       episodicidade. RODADA 2 fica PROIBIDA — amortecedor seria o filtro
       errado. MORTO, cemitério.
     Qualquer primário com MONEY e SKILL ⇒ entra na fila de forward.
   Diagnóstico auxiliar (não decisional): hold mediano do nativo. 1-3 barras
   = chicote puro; 8-15 = outro mecanismo.

5. RODADA 2 — PRÉ-REGISTRADA E CONDICIONADA (só dispara se EV@0 < 6 bps):
   Banda morta em ATR, com histerese:
     LONG  entra se close > EMA8 + k*ATR14 e EMA8 > EMA21
     SHORT entra se close < EMA8 - k*ATR14 e EMA8 < EMA21
     nativo sai do LONG quando close < EMA8 - k*ATR14 (banda invertida)
   k = 0.5, DERIVADO: exatamente 1/3 do stop pré-registrado (S=1.5 ATR).
   Leitura: o ruído que o preço precisa vencer para entrar é um terço fixo
   do risco assumido. Não é garimpo; é razão fixa contra parâmetro já travado.
   Escolhido sobre BB (exigiria lookback de percentil = parâmetro livre novo)
   e sobre ADX/forca (soma unidades incompatíveis e é cego à direção).
   CUSTO ACEITO: [Provável] a banda atrasa a entrada; troca-se menos trades
   por entradas piores. Não existe amortecedor de graça.
   Mesmos gates, mesmos dois primários, mesma janela. UMA rodada. Se falhar,
   MORTO sem varredura de k.

## 2026-08-10 — KEEP IT SIMPLE (EMA 8/21, TIER1 4h) — VEREDITO: MORTO
n=10532 entradas (LONG 4942 / SHORT 5590), excluidas_borda 138, SEP=0.
  nativo  (saida-por-estado, H<=48): EV@0 +14.1 | EV@6 +8.1 (iid_p .041)
          | blockP2.5 -10.6 → MONEY NAO | p_shift 0.024 → SKILL NAO (toll .005)
  bracket (S=1.5 T=3.0 H=24):        EV@0 +11.2 | EV@6 +5.2 (iid_p .163)
          | blockP2.5 -25.8 → MONEY NAO | p_shift 0.330 → SKILL NAO
MORTO: nenhum primario com MONEY. Rodada 2 (banda morta k=0.5) PROIBIDA pela
regra de decisao pre-registrada (EV@0 > 6 bps ⇒ o mecanismo NAO e custo).

PREVISAO FALSIFICADA: o pre-registro apostava "[Provavel] morre no custo,
EV@0 positivo com EV@6 negativo". EV@6 e POSITIVO (+8.1). A previsao estava
errada; a regra escrita antes do dado impediu que o erro virasse racionalizacao.

ROBUSTEZ DO VEREDITO: sob os gates ANTIGOS (>0 / <0.025) o nativo daria
MONEY nao / SKILL SIM (0.024). Continua MORTO. O pedagio de 09/08 NAO foi o
executor — quem matou foi o block bootstrap. A emenda nao esta calibrada
para reprovar.

CONTABILIDADE: 12 primarias na campanha, 1 passe de MONEY.
P(>=1 passe por acaso @2.5%) = 26% (era 22% com 10). O DONCHIAN-A fica
progressivamente mais compativel com sorte.

LICAO NOVA — corrige generalizacao anterior:
O ledger registrava "em rompimento o bracket domina o temporal (8/8)". AQUI E
O INVERSO: nativo 14.1/p .024 vs bracket 11.2/p .330. O bracket destroi a
skill. Leitura corrigida: bracket domina em ROMPIMENTO; em sinal de horizonte
curto com saida ENDOGENA, o stop fixo corta a operacao no meio da informacao
e e subtracao. Nao generalizar "bracket sempre".

AUTOPSIA (descritiva, NAO promovivel):
hold_mediano 3 barras (12h); forca_subindo em apenas 29% das entradas;
bb_width_rel_med 0.0502; origem ROXO 5275 / AZUL 4611 / VERDE 340 / VERM 306
(94% vindo dos estados adjacentes de pullback, como esperado).
PROIBIDO derivar primaria de qualquer um destes numeros. Se "forca crescente"
virar hipotese algum dia, vem de raciocinio a priori, com outro nome, sem
citar esta rodada como motivacao.

### 2026-08-10 — EXPLORATÓRIO: keepitsimple símbolo único (SOLUSDT)
Motivação: inspeção VISUAL do gráfico após o veredito MORTO do agregado.
Isto é melhor-de-20 pós-hoc. P(>=1 dos 20 passar por acaso @2.5%) = 40%.
NÃO PROMOVÍVEL sob nenhuma circunstância, qualquer que seja o resultado.
Não gera rodada 2, não entra na fila forward, não vira primária depois com
outro nome. Serve APENAS para diagnosticar dispersão entre símbolos.
Poder esperado: n ~500 sobre ~52 bins de 14d ⇒ blockP2.5 muito largo.
[Provável] o resultado será INCONCLUSIVO, não exculpatório.

### 2026-08-16 — EXPLORATÓRIO POR SÍMBOLO (keepitsimple, 20 TIER1)

RESSALVA DE PROCEDÊNCIA — ler antes do resto. O exploratório de 10/08 acima
previa SOLUSDT isolado. Foi ampliado para os 20 símbolos da TIER1 por decisão
de 10/08, e a regra de leitura abaixo foi fixada por escrito na mesma data,
ANTES do lançamento da rodada (14/08). Mas o bloco da ampliação NÃO foi
commitado na época: está sendo registrado agora, DEPOIS do resultado. A regra
é legítima por ter antecedido o dado; a lacuna de registro fica anotada como
falha de processo, não se repete.

REGRA DE LEITURA (fixada 10/08, antes do dado): >=15 de 20 símbolos com
EV@6 > 0 mereceria atenção. O melhor símbolo isolado NÃO conta como evidência
(máximo de 20 sorteios é alto por construção). Dispersão só é informativa se
ESTRUTURADA e nomeável a priori.

RESULTADO: 12 de 19 com EV@6 > 0 (nativo). P(>=12 de 19) = 0.18. É ACASO.
TON excluído das contagens: n=69 contra ~520-575 dos demais (amostra quebrada
por delistagem). n total 10532, bate com o agregado de 10/08.
  positivos: XRP +30.0 · DOGE +34.7 · ARB +27.4 · WLD +25.3 · ADA +20.7 ·
             DOT +20.7 · SUI +16.5 · TRX +13.4 · AVAX +12.6 · NEAR +7.8 ·
             BNB +2.4 · OP +2.1
  negativos: LTC -23.8 · BCH -18.0 · ETH -7.4 · SOL -6.7 · LINK -6.3 ·
             BTC -0.3 · APT -0.3

SOL REFUTADO. A hipótese visual que motivou este ramo ("SOL é visualmente e
seguramente rentável") está ERRADA. SOL nativo EV@6 -6.7, blockP2.5 -38.2;
bracket EV@6 -14.9, blockP2.5 -76.3. TERCEIRO PIOR dos 20. Registrado como
medida direta do valor probatório da leitura visual de gráfico nesta bancada.

ACHADO ESTRUTURAL — o mais importante desta rodada.
blockP2.5 negativo em 19 de 20 símbolos, mediana -27.8 bps, INCLUSIVE nos de
maior EV: XRP EV@6 +30.0 com block -10.0; ARB +27.4 com block -3.4. Com ~550
trades sobre ~52 bins de 14 dias (~10 trades/bin), a variância entre janelas
engole a média.

O PEDÁGIO DE 09/08 SE PAGOU. Único blockP2.5 > 0 em toda a varredura: TRX
bracket +1.4 bps com p_shift 0.024. Sob os gates ANTIGOS (>0 / <0.025) seria
DUPLO-PASSE e viraria "o segundo DONCHIAN-A". Sob o pedágio morre nos dois.
É exatamente o melhor-de-20 previsto (P=40%) aparecendo uma vez. A emenda foi
calibrada para acertar, não para reprovar.

PREVISÃO CONFIRMADA: o pré-registro de 10/08 dizia "[Provável] o resultado
será INCONCLUSIVO, não exculpatório". Foi.

===== O QUE ISTO ENCERRA E O QUE NÃO ENCERRA =====

ENCERRADO — por FALTA DE PODER, não por regra:
Símbolo isolado como caminho de PROMOÇÃO. Com ~550 trades sobre ~52 bins, o
blockP2.5 é estruturalmente negativo. Qualquer variante futura rodada símbolo
a símbolo terá o mesmo destino, por aritmética, independentemente do mérito.
Rodar por símbolo continua PERMITIDO como DIAGNÓSTICO de dispersão; NÃO como
candidatura a primária.

NÃO ENCERRADO — frente ABERTA e SEM TETO DE TENTATIVAS:
Variantes da estratégia sobre o UNIVERSO INTEIRO. A frente "Keep It Simple"
NÃO está fechada. O veredito MORTO de 10/08 se aplica à CONFIGURAÇÃO testada
(regra Estado + saída por mudança de estado + bracket 1.5/3.0/24), não à
família de variantes. Marcelo está em fase de DESENHO LIVRE no TradingView.
Variantes já em exploração visual, todas legítimas como primárias futuras:
  - regra "Extremos": posição = último estado EXTREMO visto; AZUL/ROXO/CINZA
    não agem; só o extremo oposto inverte (stop-and-reverse);
  - saída por cruzamento do preço com a EMA LONGA (21);
  - confirmação de N barras no estado antes de liberar entrada;
  - separação mínima EMA curta-longa em ATR (filtro de lateralidade);
  - ADX mínimo como double-check.
Cada configuração CONGELADA vira primária nova (#13, #14, ...), com
pré-registro antes do dado e os gates de pedágio de 09/08. Não há limite de
quantas podem ser submetidas ao juiz.
NOTA TÉCNICA registrada: "sair quando o preço cruza de volta a EMA CURTA" é
matematicamente idêntico a sair do estado VERDE/VERM — é a regra já morta em
10/08, sem as reentradas. A EMA LONGA é que constitui saída genuinamente nova.

TERCEIRO CAMINHO, TAMBÉM ABERTO:
Universo em SUBCONJUNTO definido A PRIORI (ex.: "somente alts", "somente
top-5 por liquidez"). Legítimo como primária desde que o critério do recorte
seja escrito ANTES de olhar resultado — o n volta a ser grande e o poder
volta. PROIBIDO: escolher o subconjunto olhando a tabela por símbolo acima.
Isso seria melhor-de-20 com outro nome.

FASE DE DESENHO vs. FASE DE JULGAMENTO — distinção registrada:
Exploração visual no TradingView é GERAÇÃO DE HIPÓTESE, não evidência. Não há
limite de configurações que Marcelo possa avaliar visualmente. Ao submeter uma
ao juiz, informar quantas combinações foram avaliadas a sério — entra na
leitura do p-valor, não como julgamento moral.

PERMANECE FACTUAL, NÃO SE REABRE: a refutação do SOL e o caso TRX. São
medições, não vereditos.

CONTABILIDADE DA CAMPANHA (inalterada por este exploratório): 12 primárias,
1 passe de MONEY. P(>=1 por acaso @2.5%) = 26%.

## 2026-08-17 — PRÉ-REGISTRO: KEEP IT SIMPLE EXTREMOS (primária #13)

Procedência honesta: formulada por Marcelo em 14/08, DEPOIS do veredito MORTO
da regra "Estado" (10/08). Não é ideia virgem. Desenho fechado em bancada
visual no TradingView; nenhum parâmetro contínuo garimpado (todos nos valores
canônicos: EMA 8/21, ATR14, confirmação = 2 barras, filtros desligados).
Combinações avaliadas a sério na bancada: ~8 (regra Estado/Extremos × saída
extremo-oposto/EMA-longa × stop nenhum/fixo/BE-trail/trail-EMA).

DETECTOR (sem grau de liberdade):
  mmeCurta=EMA(close,8); mmeLonga=EMA(close,21); tf 4h; TIER1(20).
  estado ∈ {VERDE, AZUL, ROXO, VERM} como no pré-registro de 09/08.
  barrasNoEstado >= 2 exigido (confirmação; estado deve sobreviver ao
  fechamento seguinte).
  alvoPos := +1 quando estado==VERDE e confirmado
           := -1 quando estado==VERM  e confirmado
           AZUL/ROXO/CINZA NÃO alteram alvoPos (atravessa o pullback).
  Entrada quando alvoPos muda de valor. SEMPRE NO MERCADO (stop-and-reverse).
  Execução no CLOSE da barra do sinal (convenção do soquete).
  SEP irrelevante: sobreposição impossível por construção.

PRIMÁRIO ÚNICO — sem bracket, portanto SEM correção ×2 intra-rodada.
  Saída = próxima inversão de alvoPos. Sem stop, sem alvo, sem cap de tempo.
  Justificativa de haver um só: o bracket foi medido como destrutivo nesta
  família (10/08: nativo p_shift .024 vs bracket .330). Testá-lo de novo
  gastaria multiplicidade para reconfirmar um cadáver.

GATES (pedágio de 09/08): blockP2.5 líquido @6bps > +5.0 bps E p_shift < 0.005.

NÃO OPERÁVEL — registrado ANTES do resultado:
  Bancada TradingView, 6 pares, 2024-01 a 2026-08, mediu DD máximo de
  perdedores em 32.05 ATR (TAO) e 13.99 ATR (XRP). Um evento adverso de 32 ATR
  vale 1.6x TODO o ganho da estratégia naquele par (SOMA 19.7 ATR em 213
  trades). Em perpétuo alavancado isso é liquidação; o backtest nunca é
  liquidado. Se este primário PASSAR, NÃO vai a Estágio 2 sem uma regra de
  cauda pré-registrada em rodada própria.

HOLD (medido na bancada): mediano 17-18 barras; MÁXIMO 117 a 225 barras.
  Exige estender o horizonte de measure_event de 48 para 256 barras. Truncar
  em 48 cortaria a cauda longa, que num seguidor de tendência É o payoff, e
  testaria outra estratégia.

CRITÉRIOS DE MORTE: sem MONEY ⇒ MORTO, sem varredura de parâmetro depois.
  n < 400 ⇒ INCONCLUSIVO.
  Janela 2024-05-22 → 2026-06-21, candles_v9.db, seed 1337.

EXPECTATIVA REGISTRADA ANTES DO FATO:
  [Provável] SKILL passa com folga; MONEY REPROVA por concentração episódica.
  Base: as curvas de L&P de XRP e DOGE mostram um degrau vertical único
  seguido de ~2 anos de deriva lateral — poucos trades carregam o resultado.
  Se MONEY passar, a leitura de concentração estava errada e isso deve ser
  registrado como acerto do desenho, não como sorte.
  Marcador de calibração: na rodada de 10/08 eu previ morte por CUSTO e
  errei (morreu por variância). Esta previsão é do mesmo tipo — trate como
  hipótese testável, não como oráculo.

CONTABILIDADE: se passar, campanha vai a 13 primárias com 2 passes de MONEY.

### ADENDO ao pré-registro KIS_EXTREMOS (17/08, ANTES da rodada)

CORREÇÃO DE DIAGNÓSTICO: o briefing de revisão afirmava que o clamp de
`fwd_bar` mordia na borda direita do store. FALSO, e demonstrado pelo Claude
Code: `hold` conta velas até a inversão (limitado pelo fim da série) e
`fwd_bar` lê do store sem limite de janela, logo hold <= len(fwd_bar) em série
CONTÍGUA. O gatilho real da exclusão é BURACO no store — `hold` conta velas,
`fwd_bar` cobre 256 larguras de barra em TEMPO; atravessando um buraco as
contagens divergem. Perfil TON/delistagem. A exclusão (`excluida_hold`) fica,
com a justificativa corrigida.

RESÍDUO DO NULO — aceito com regra de decisão, não corrigido.
No nulo circular o sinal é deslocado para barra arbitrária; perto da borda o
clamp ainda morde. Direção do viés: PERMISSIVA (retorno truncado encolhe a
variância nula, p_shift sai MENOR que o real). Exposição superior medida em
`cache_curto` (~5% das barras elegíveis); fração efetiva estimada em 1-2%.
NÃO corrigir estruturalmente: excluir do cache custaria 5,6% do universo e
encolheria o n do nulo — troca ruim.

REGRA TRAVADA ANTES DO RESULTADO:
  MONEY é imune (block bootstrap não usa o nulo circular). Se MONEY reprovar,
  o resíduo é irrelevante e o veredito é MORTO, ponto.
  Se MONEY PASSAR e p_shift cair em [0.001, 0.010] — faixa marginal em torno
  do gate de 0.005 — o SKILL é declarado SUSPEITO e o primário NÃO é promovido
  sem re-rodada com o nulo estruturalmente limpo.
  p_shift < 0.001 ou > 0.010: o resíduo não pode ter virado o resultado.

MEMÓRIA: VM com 956 MiB totais, 272 disponíveis, cron v9 na mesma máquina.
Cache com lista Python pedia 928 MiB (déficit 656). array('d') resolve:
206 MiB medidos, abaixo do baseline de 374 MiB do keepitsimple. Precisão
provada bit a bit via float.hex(), não por aproximação.

## 2026-08-18 — KIS_EXTREMOS (primária #13) — VEREDITO: MORTO NOS DOIS GATES
n=3261 (LONG 1627 / SHORT 1634), excluidas_borda 39, excluidas_hold 0.
  extremos: EV@0 +26.8 | EV@6 +20.8 (iid_p .112)
            | blockP2.5 -56.5 → MONEY NAO | p_shift 0.117 → SKILL NAO
MORTO. Nenhum gate. Sem rodada 2, sem varredura, cemitério.

PREVISÃO FALSIFICADA (2a seguida): o pré-registro dizia "[Provável] SKILL passa
com folga; MONEY reprova por concentração". MONEY reprovou por concentração —
acertei o mecanismo. SKILL NÃO passou: 0.117, vinte e três vezes o gate.
Registro: erro do mesmo tipo do de 10/08 (previ morte por custo, morreu por
variância). O padrão é que eu subestimo sistematicamente quanto do resultado
aparente vem de ruído. Calibrar para baixo nas próximas.

O ACHADO CENTRAL — a variante "melhorada" tem MENOS skill que o cadáver:
             EV@6   blockP2.5   p_shift    n
  Estado nativo  +8.1     -10.6      0.024   10532
  Extremos      +20.8     -56.5      0.117    3261
Retorno por trade subiu 2,5x; n caiu 3,2x; líquido AGREGADO ficou 20% MENOR
(67.829 vs 85.309 bps-trade). O gap média-para-blockP2.5 explodiu de 18.7 para
77.3 bps. Segurar o pullback não criou dinheiro: concentrou o mesmo dinheiro em
menos eventos independentes, destruindo o pouco de timing que existia.
Sem viés direcional para explicar (1627/1634) e com o nulo preservando hold e
direção, o +20.8 bps está inteiramente dentro da banda de ruído.

CALIBRAÇÃO DA BANCADA VISUAL — registrar e não esquecer:
No TradingView esta configuração deu resultado médio POSITIVO em 6 de 6 pares
(+0.093 a +0.993 ATR/trade) e curvas de equity de aparência excelente. O juiz
reprovou nos dois gates. Divergência explicada, sem bug: (i) seleção — 6
símbolos escolhidos a dedo vs 20 congelados; (ii) janela — 2024-01→2026-08 na
bancada vs 2024-05→2026-06 no juiz, 5 meses a mais incluindo hold-out;
(iii) régua — ATR normaliza por volatilidade, bps por nocional.
NÃO explica: custo. O juiz usou 6 bps/trade contra 10 bps ida-e-volta da
bancada — foi MAIS generoso e mesmo assim reprovou.
CONCLUSÃO OPERACIONAL: a bancada visual serve para GERAR hipótese e para
depurar mecanismo. Não serve para estimar edge, nem em 6 pares, nem com
tamanho de ordem fixo. Esta rodada é a medida do tamanho do engano: de
"+0.38 ATR médio em 6 pares" para "indistinguível de ruído em 20".

RESÍDUO DO NULO: regra travada em 17/08 resolvida sem ambiguidade. p_shift
0.117 está fora da faixa suspeita [0.001, 0.010] por uma ordem de magnitude;
cache_curto=4160 não pode ter alterado o veredito.

INFRAESTRUTURA — permanece válida e reutilizável:
Horizonte estendido (FWD_BAR_CAP=256) era necessário: 452 entradas (14% da
amostra) têm hold>48 e só existem por causa dele, distribuídas uniformemente
pelos 20 símbolos. hold_max=197, truncados_no_teto=0, excluidas_hold=0 —
confirma a demonstração de que série contígua nunca dispara a exclusão (só
buraco no store dispararia). array('d') no cache: 928→206 MiB, viabilizou a
rodada numa VM de 956 MiB. Qualquer detector futuro de hold longo já tem
onde rodar.

CONTABILIDADE: 13 primárias, 1 passe de MONEY.
P(>=1 por acaso @2.5%) = 28.0% (era 26% com 12).

FRENTE "Keep It Simple": continua ABERTA conforme a emenda de 16/08. O que
morreu foi esta configuração, não a família. Mas duas configurações desta
família já morreram, e a segunda com timing PIOR que a primeira.

AUTÓPSIA EM ANDAMENTO (18/08): investigação do formato das saídas em prejuízo
na bancada visual — contagem de saídas ganho/perda, razão de payoff, fração de
perdedores "natimortos" (runup < 0.1 ATR) e hold mediano por resultado.
É diagnóstico descritivo, NÃO varredura de parâmetro. Qualquer configuração
que emergir desta autópsia é primária NOVA (#14), com pré-registro próprio
antes do dado. Não herda nada desta rodada.

## 2026-08-20 — MUDANÇA DE ABORDAGEM

DIAGNÓSTICO: 13 primárias, todas da família "prever direção em OHLCV".
Todas mortas. A família está exaurida — não por rigor do juiz, mas porque
o fenômeno não paga o custo de execução neste horizonte.

CORREÇÃO DE OPERAÇÃO (erro do Claude): desde 09/08 a REGRA 2 diz que o juiz
histórico é FILA, não certificado. Ele vinha sendo operado como portão de
aprovação. A partir de agora:
  - blockP2.5 e p_shift são REPORTADOS, não são veto.
  - Servem para ORDENAR candidatos na fila do shadow.
  - Quem aprova ou reprova é o forward. Só ele.
  - Nenhuma frente fecha por resultado de juiz. Fecha quando Marcelo decide.

PRIORIDADE NOVA: sair da classe "prever direção".
  1. FUNDING/CARRY (perpétuos): colher pagamento mecânico, delta-neutro.
     Primeiro passo: baixar histórico de funding dos 20 e somar. Contabilidade,
     não pesquisa. Sem gate, sem pré-registro.
  2. FORÇA RELATIVA (cross-section): long nos 5 mais fortes, short nos 5 mais
     fracos. Remove beta de mercado. Reusa candles_v9.db.
  3. KIS: permanece aberta enquanto Marcelo quiser.

REGRA DE CONDUTA DO CLAUDE (nova): quando um resultado vier ruim, apresentar
o que fazer para melhorar OU qual caminho alternativo tomar. Nunca terminar
em "MORTO" sem uma proposta de próximo passo.

## 2026-08-20 — SIMPLIFICAÇÃO DO CRITÉRIO (decisão do Marcelo)

Os gates blockP2.5 e p_shift DEIXAM DE VETAR. Permanecem calculados e
registrados para o histórico, mas não reprovam nada e não fecham frente.
Motivo: viraram muro intransponível e não respondem à pergunta prática.
A REGRA 2 de 09/08 já dizia que o juiz histórico é FILA, não certificado —
estava sendo operada ao contrário. Corrigido.

CRITÉRIO NOVO, em uma frase:
  Ajusta na primeira metade dos dados. Confere na segunda, sem mexer em nada.

CANDIDATA A FORWARD quando:
  (a) resultado positivo na 1ª metade E na 2ª metade;
  (b) maioria dos trimestres positivos;
  (c) pelo menos 30 trades;
  (d) drawdown máximo tolerável para o Marcelo.
Sem p-valor, sem percentil, sem multiplicidade.

FOCO POR TOKEN é permitido e legítimo. Não é preciso funcionar em 20.
Exigência: o encaixe tem que aparecer nas DUAS metades.

NENHUMA FRENTE FECHA por resultado de teste. Fecha quando o Marcelo decide.
Funding e cross-section ficam na fila, não abrem enquanto o KIS não se
esgotar por decisão dele.

CONDUTA DO CLAUDE: resultado ruim vem sempre acompanhado da próxima variação
a testar. Nunca terminar em "MORTO" e esperar comando.

## 2026-08-21 — VARREDURA DO TRAIL (exploratória) — nada promove

17 células x 20 tokens, kis_extremos, critério das duas metades.
TRAIL NÃO FUNCIONA: melhor célula bate o controle em 9 de 20 tokens; com 16
células testadas, P(alguma chegar a 9 por acaso) = 49%. Sem platô.
Toda melhora aparente vive na 1ª metade (d_1a +30k a +40k) e some na 2ª
(d_2a -10k a +3k). Assinatura de ajuste ao passado.

ACHADO PRINCIPAL, independente do trail — a base DECAIU entre as metades:
  1ª metade (mai/24→jun/25):  +109.181 bps
  2ª metade (jun/25→jun/26):   -41.245 bps
10 de 20 tokens negativos na 2ª metade. XRP e DOGE, os melhores da 1ª,
desabaram na 2ª — demonstração direta do valor do teste das duas metades.

8 tokens positivos NAS DUAS metades: WLD, ADA, ARB, NEAR, SUI, TRX, BNB, BTC.
ARB é o mais equilibrado. TRX e OP melhoraram na 2ª metade.

PRÓXIMO: o problema não é saída, é ENTRAR sem tendência (58% das perdas foram
trades que nunca andaram a favor). Varredura A = filtro de regime na entrada.

## 2026-08-21 — CONGELAMENTO KIS + FILTRO DE REGIME (antes do hold-out)

CÉLULA CONGELADA: kis_extremos com filtro de regime, limiar = 0.02, adx_min = 11.
  entrada só se: inclinacao(EMA21, 5 barras, em ATR/barra) >= +0.02 para LONG
                 e <= -0.02 para SHORT, E ADX(13) >= 11.
  Escolhida como CENTRO do platô, não como pico. A linha limiar=0.02 tem três
  células vizinhas com as duas deltas positivas (adx 0, 11, 14); as bordas
  degradam dos dois lados (limiar 0.03 piora; adx 17 destrói a 1a metade).
  0.02/14 tinha a melhor 2a metade (+13.390) mas é BORDA, vizinha de 0.02/17
  que colapsa. Pegar o pico foi rejeitado deliberadamente.

TOKENS CONGELADOS (6): ARB, BNB, BTC, SUI, TRX, WLD.
  Critério: positivos nas DUAS metades em TODAS as quatro configurações
  testadas (controle sem filtro, 0.02/15 da grade antiga, 0.02/14 e 0.02/11
  da grade fina). Estabilidade, não magnitude.
  1a metade +33.046 | 2a metade +22.820 | total +55.866 bps.
  FORA: XRP — no controle sem filtro a 2a metade era -1.895; só vira positivo
  COM o filtro, o que é o filtro salvando o token, não edge do token.
  FORA: LINK — 2a metade -2.268 nesta célula contra +22 na vizinha. A virada
  de +10.488 bps observada na grade antiga não se sustentou. Ruído confirmado.

PROCEDÊNCIA HONESTA: célula e lista de tokens foram escolhidas OLHANDO as duas
metades de 2024-05-22 a 2026-06-17. Essa janela deixou de ser teste. O único
dado limpo restante é o hold-out abaixo.

HOLD-OUT: entradas de 2026-06-18 a 2026-08-21 (65 dias, ~390 barras 4h por
símbolo, ~106 trades estimados nos 6 tokens). Coletado via backfill_okx em
21/08 ANTES de qualquer análise. GASTA-SE UMA VEZ SÓ.
Ressalva registrada: eventos com entrada até 17/06 e saída depois disso já
consomem uma fatia fina do hold-out — inevitável pela regra de borda, ~1-2%.

CRITÉRIO DE LEITURA — TRAVADO ANTES DO NÚMERO:
  (a) soma dos 6 tokens POSITIVA no hold-out  E
  (b) pelo menos 4 dos 6 positivos individualmente
  ⇒ CANDIDATO A FORWARD (Estágio 2), entra na fila atrás do DONCHIAN-A.
  Qualquer resultado abaixo disso ⇒ a frente KIS se esgota NESTA FORMA e o
  próximo passo é a varredura B (RSI como teto + extensão do preço).
  NÃO se ajusta célula nem lista de tokens depois de ver o hold-out. Se o
  resultado for ruim e vier a tentação de "testar só mais uma célula", isso
  seria queimar a última amostra limpa da família inteira.

  ## 2026-08-21 — HOLD-OUT DO KIS+REGIME — REPROVADO (critério travado em 21/08)

Célula 0.02/11, 6 tokens congelados, entradas de 18/06 a 21/08/2026.
  ARB -409 | BNB -278 | BTC -1.294 | SUI -939 | TRX -137 | WLD +2.288
  SOMA -769 bps em 66 trades (-11,6 bps/trade). 1 de 6 positivos.
CRITÉRIO EXIGIA: soma > 0 E >= 4 de 6. NÃO PASSOU.
A frente KIS se esgota NESTA FORMA. Nenhuma célula nova, nenhuma lista nova.
O hold-out foi gasto. Não há mais amostra limpa nesta família.

O QUE SOBREVIVEU — registrar separado do veredito:
  controle sem filtro no mesmo hold-out: -4.162 bps
  com o filtro de regime:                  -769 bps
  diferença: +3.393 bps EM DADO VIRGEM.
O portão de regime (inclinação EMA21 >= 0.02 ATR/barra + ADX(13) >= 11) é o
ÚNICO componente da campanha que melhorou fora da amostra onde foi calibrado.
Fica disponível como COMPONENTE REUTILIZÁVEL para qualquer sinal futuro.
Conclusão que se separa: o SINAL do KIS está morto; o PORTÃO não está.

RESSALVA DE PODER: 66 trades é amostra pequena; isto não é rejeição forte do
sinal, é o cumprimento de um critério pré-comprometido. Registrado para que
ninguém releia daqui a seis meses como "provado que não funciona".

## 2026-08-22 — DONCHIAN-A: leitura honesta do forward (registrar ANTES do fechamento)

ΣR +36,08 acumulado. MAS a decomposição desmonta a leitura otimista:
  20/07 a 18/08 (30 dias): 37 trades,  2 wins, ΣR -19,92
  19/08 a 22/08 (4 dias):  24 trades, 16 wins, ΣR +56,00
16 das 18 vitórias em QUATRO dias consecutivos = UM episódio de mercado.
Apenas 3 episódios de ganho em toda a amostra (29/07, 06/08, 19-22/08).

É BETA, NÃO HABILIDADE — os 16 vencedores do episódio foram TODOS LONG.
  no episódio:  LONG 16W/2L (+62R) | SHORT 0W/5L (-5R)
  fora dele:    LONG 1W/12L (-8R)  | SHORT 1W/20L/4E (-12,92R)
Amostra inteira: LONG 17W/14L (55%, +54R) | SHORT 1W/25L (3,8%, -17,92R).
O lado SHORT tem 1 vitória em 26 fechados. Não é variância.

FALHA DE DESENHO DA JANELA: 80 trades / 84 dias conta TRADES. O N efetivo de
um seguidor de tendência são EPISÓDIOS. Em 34 dias houve UM, unidirecional.
Fechar em outubro com 2 episódios NÃO produzirá veredito — produzirá esta
mesma discussão. Registrado agora para não virar racionalização depois.
VIGIA segue rodando (custo zero). O veredito de outubro deve ser declarado
INCONCLUSIVO POR CONTAGEM DE EPISÓDIOS, salvo aparecer uma queda forte que
teste o lado short.

CLAUDE ERROU DUAS VEZES NESTA ANÁLISE: em 09/08 registrou "foi o falso-positivo
dos 22%"; em 22/08 leu a virada como "a estratégia funcionou". Marcelo apontou
a concentração no cisne e estava certo nas duas vezes. Padrão: Claude reage ao
número mais recente em vez de decompor primeiro.

## 2026-08-22 — ROADMAP DE EXPLORAÇÃO (registro de caminhos abertos)

CORREÇÃO: o hold-out do KIS (-769 bps) é PROVISÓRIO, não veredito. A regra de
borda (48 barras) e a exclusão por hold removeram da medição as entradas a
partir de ~13/08 — o episódio de 19-22/08 está majoritariamente AUSENTE.
Rerodar em ~05/09 com o store estendido. O critério travado em 21/08 continua
valendo; só será aplicado sobre dados completos.

EIXOS ABERTOS, em ordem de prioridade e com justificativa:

1. HORIZONTE DO SINAL em 4h — EMA (8,21) controle | (13,34) | (21,55) | (34,89)
   Por quê primeiro: dados prontos, uma constante, e testa diretamente a
   hipótese levantada pelo contraste Donchian (canal 20d, capturou a alta) vs
   KIS 8/21 (girou a cada 3 dias e foi picotado dentro dela).

2. TIMEFRAME DIÁRIO — mesma hipótese por outro caminho. Exige verificar
   cobertura no store; se não houver, coletar. Menos trades, custo relativo
   menor por trade.

3. LONG-ONLY — no shadow do DONCHIAN-A o lado SHORT tem 1 vitória em 26
   fechados (3,8%). Se o mesmo padrão aparecer no KIS, desligar SHORT é uma
   linha de código. ATENÇÃO: seria escolha PÓS-HOC; precisa de pré-registro
   próprio e do hold-out para valer.

4. TIMEFRAMES MENORES (1h, 15m) — [Provável] piores: mais trades, e a 5 bps
   por perna o custo domina. Testar por último, e mais para fechar a questão
   do que por expectativa.

5. VARREDURA B (RSI como teto + extensão do preço) — pré-declarada em 20/08,
   segue disponível. Rebaixada de prioridade: é mais um filtro de ENTRADA
   sobre um sinal cuja escala de tempo pode ser o problema real.

6. REVERSÃO À MÉDIA — única classe complementar nunca testada em 13 primárias.
   Reaproveita o portão de regime invertido (entrar quando NÃO há tendência).

COMPONENTE VALIDADO E REUTILIZÁVEL: o portão de regime (inclinação EMA21 >=
0.02 ATR/barra + ADX(13) >= 11) melhorou +3.393 bps em dado virgem. É o único
componente da campanha que sobreviveu fora da amostra de calibração. Aplicável
a qualquer sinal futuro, não pertence ao KIS.

REGRA DE CONDUTA: um eixo por vez. Nunca dois simultâneos. Cada rodada com
grade fechada antes, célula de controle explícita, e leitura por platô — nunca
por pico.

## 2026-08-22 — TRAVA: superfície de importação de keepitsimple congelada

`shadow/kis_regime.py` (coletor forward do KIS+REGIME) roda em produção
importando de `backtest/keepitsimple.py`: **WARMUP, states, _alvo_extremos,
_adx**. Esses quatro passam a ser superfície pública: assinatura, valor e
comportamento padrão não mudam.

Variação de par de EMA vai em **módulo novo**, ou como **argumento opcional com
default idêntico ao de hoje (8, 21)** — nunca alterando o que a chamada sem
argumentos extras devolve.

Cadeado: `tests/test_trava_superficie_kis.py`. Golden byte-idêntico (string +
sha256) para `states` e `_alvo_extremos`, tolerância 1e-12 nos floats do `_adx`,
e guarda de assinatura que reprova parâmetro novo sem default.

Nota de método, porque a primeira versão do cadeado era falsa: a série trending
óbvia NÃO discrimina ±1 no período — `states(closes)` com EMA_FAST=9 devolvia o
mesmo golden, e o teste passava. A série final tem 78 barras construídas para
cair dentro das frestas entre as EMAs (close entre EMA8 e EMA9; EMA8 entre EMA21
e EMA22), e o próprio arquivo prova a discriminação: trocar (8,21) por
(7,21)/(9,21)/(8,20)/(8,22) muda 8/55/12/10 barras. Um cadeado que não foi
testado contra a mutação que ele deveria pegar não é cadeado.

## TRAVA (adendo) — fechar a lacuna registrada em 22/08
A checagem por AST hoje assere o CONJUNTO de nomes importados de
backtest.keepitsimple por shadow/kis_regime.py. Ela NÃO impede que o coletor
passe a importar o detector de um módulo NOVO.
Estender a checagem para uma LISTA BRANCA de módulos-fonte: shadow/kis_regime.py
só pode importar detector/portão de {backtest.keepitsimple, backtest.kis_regime}.
Qualquer outra origem reprova o teste e exige reapontar a trava de propósito.
Vale para o módulo novo de horizonte: ele NÃO entra na lista branca.

FECHADO em 22/08 (mesmo PR da trava). `_origens_de` extrai por AST todo módulo
de primeira parte importado por `shadow/kis_regime.py` — inclusive os lazy
dentro de função — usando `sys.stdlib_module_names` para separar stdlib (lista
do interpretador, não uma lista minha, que envelheceria). Lista branca de sinal:
{backtest.keepitsimple, backtest.kis_regime}; infra do shadow
({shadow.donchian_a, shadow.vigia}) é permitida à parte porque não é detector.
Import relativo reprova, para a origem nunca ficar ambígua. Alargar a lista
exige editar a asserção — que é o "reapontar de propósito".
Mutação que confirma: trocar a origem do detector para `backtest.kis_horizonte`
MANTENDO os quatro nomes reprova 3 testes; origem nova de primeira parte
reprova 3; import relativo reprova 2.

CORREÇÃO APLICADA no mesmo PR: o docstring do coletor e a linha do [VIGIA]
diziam "hold-out REPROVADO". Com o registro de 22/08 isso passou a afirmar
veredito onde há número provisório. Passaram a dizer "-769 bps (PROVISÓRIO,
remedição ~05/09)", sem afrouxar o "não promoção".

LIMITAÇÃO CONHECIDA do config_hash (PR-A, 22/08): o detector entra no hash por
NOME (modulo.qualname), não por conteúdo. Mudar o CORPO de um detector sem
mudar o nome NÃO muda o hash, e os trades se misturam no journal.
REGRA: alterou comportamento de detector, incrementa o setup_id
(ex.: kis_regime_4h -> kis_regime_4h_v2). Não há checagem automática.
Para keepitsimple isso já é coberto por tests/test_trava_superficie_kis.py;
para detectores futuros, é disciplina.

## 2026-08-25 — PR-E: parâmetros do detector dentro do config_hash

Os sete parâmetros do KIS (limiar 0.02, adx_min 11, EMA 8/21, ATR 14, ADX 13,
confirmação 2) viviam DENTRO de detector_kis_regime e não entravam no hash —
mudar qualquer um deles não mudava a identidade da série. Agora vão em
SetupSpec.detector_params e entram. Nenhum valor mudou: pipeline rodado antes
e depois sobre a mesma série, 49 trades, zero divergências.
  kis_regime_4h   53f05ad99e8a -> e63ec120e131
  donchian_a_4h   40497232d65b -> 250170cc8dc0
trades_v10 estava VAZIA no merge — nenhuma linha órfã, sem migração.

DEPENDÊNCIA CORTADA: v10/registro.py não importa mais de shadow/kis_regime.py
(apêndice aposentado em 23/08). `avaliar` foi para backtest/kis_regime.py — a
bancada, que já era dona de `passa` e `inclinacoes`. A lista branca da trava
de superfície passou a cobrir os DOIS consumidores (shadow/kis_regime.py e
v10/registro.py), com teste reprovando reintrodução do import.

DÍVIDA ANOTADA PARA OUTUBRO: v10/registro.py ainda importa de
shadow/donchian_a.py. Hoje é legítimo — aquele shadow está VIVO e a ficha do
v10 é espelho dele. Quando a janela do Estágio 2 fechar e o shadow for
desativado, esse import vira a mesma dívida que o PR-E acabou de pagar.

## 2026-08-26 — EIXO 1 (HORIZONTE): hipótese de escala de tempo CONFIRMADA

Grade 8 células, pares (8,21) controle / (34,89) / (55,144) / (89,233) x portão.
Normalizado por trade (única leitura válida entre pares com n diferente):
  2a metade, bps/trade: 8/21 -12,5 | 34/89 +54,2 | 55/144 +92,7 | 89/233 +296,4
  1a metade fica plana (~+33) até 55/144 e DESABA em 89/233 (+13,9).
55/144 e 89/233 saíram INCONCLUSIVAS POR AMOSTRA (mediana de 27 e 14 trades por
token). 34/89 é a última célula com amostra decente (42/token) e status ok.

NÃO É BETA — a quebra por direção derruba a suspeita:
  34/89 c/ portão, 2a metade: LONG -27.781 | SHORT +73.614
  O ganho da 2a metade é 100% do lado SHORT; o long é NEGATIVO ali.
  Padrão: 1a metade long ganha/short perde; 2a metade long perde/short ganha.
  O par longo melhora OS DOIS lados na 2a: long de -71.864 para -27.781 e
  short de +30.570 para +73.614. É seguidor de tendência acompanhando a
  direção, não viés comprado. Oposto do DONCHIAN-A (short 1 vitória em 26).

CONTRA COMPRAR E SEGURAR (média por símbolo, 19 tokens):
                    1a metade   2a metade    total
  comprar e segurar    +2.675      -4.993   -2.318
  KIS 34/89 portão     +1.435      +2.300   +3.735
  diferença            -1.240      +7.292   +6.052
Bate o B&H em 14 de 19 tokens no período inteiro e 17 de 19 na 2a metade.

RESSALVA QUE NÃO PODE SUMIR: vender-e-segurar na 2a metade renderia +4.993/
símbolo; a estratégia rendeu +2.300. Ela NÃO vence a aposta direcional correta
de nenhuma das duas metades — o que ela faz é ESCOLHER O LADO sem saber de
antemão qual metade é de alta e qual é de queda. É acerto de direção em
horizonte longo, não timing dentro da tendência.
CUSTO: dd/trade 90,5 contra 35,6 do controle — 2,5x mais mergulho por trade.

EXPLORATÓRIO: nada aqui promove nada. O passo seguinte é o hold-out.

## PENDENTES (pré-registrados)
- Estágio 2 em curso: [VIGIA] diário; veredito só ao fim da janela.
- H-42 (TSMOM L=42 alts): elegível a shadow próprio após 4 semanas de
  DONCHIAN-A estável (a partir de ~17/08); N=240 depois, nunca antes.
- Auditoria de indexação posicional (replay/sweep/null_model).
- Hardening do juice (contiguidade por timestamp).
- Higiene de logs v8.
