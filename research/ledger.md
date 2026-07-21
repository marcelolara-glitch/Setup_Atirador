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

## PENDENTES (pré-registrados)
- Estágio 2 em curso: [VIGIA] diário; veredito só ao fim da janela.
- Desativação v9 conforme condições (a)-(d) — após 3+ dias de [VIGIA]
  estável (leitura final + OPEN truncados + cronfile .DISABLED).
- H-42: shadow só após 4 semanas de DONCHIAN-A estável; N=240 depois.
- Auditoria de indexação posicional; hardening do juice; higiene v8.