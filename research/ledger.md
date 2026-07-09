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

## PENDENTES (pré-registrados)
- Exploratórios Fase A: batch único, 8 combos L×H da família registrada.
  RULING cemitério: família declarada em bloco ⇒ 1 entrada consolidada
  (não há seleção sequencial entre elas).
- Snapshot TIER2 (40 perps USDT por volume 24h, ex-TIER1, ≥400d 4h)
  congelado no ledger → backfill 4h → FASE B (mesmos gates/primários).
- Se Fase B MORTO ⇒ programa TSMOM morre; próximo do menu 01/07
  (Donchian/Turtle) exige novo pré-registro.
- Auditoria de indexação posicional (replay/sweep/null_model).
- Hardening: guarda de contiguidade por timestamp no juice.
- Higiene (baixa): ~45 logs v8 + whitelist do .gitignore.