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

## PENDENTES (pré-registrados)
- PR-4: parametrizar tf em excursion.measure_event + bracket (--tf).
  Pré-registro 4h: GRID_H=(4,8,16); célula PRIMÁRIA = S=1.5/T=6.0/H=4;
  resto exploratório. Regressão obrigatória: 15m byte-idêntico ao log
  bracket_exato_20260703_1135 antes de rodar 4h.
- Rodada bracket 4h (2024-05-22→2026-06-21, custos 12/6) após PR-4.
- PR-5 — Estágio 1: nulo casado + bootstrap por bloco temporal +
  correção de multiplicidade sobre os primários. Spec após inspeção
  de null_model.py.
- Estágio 2 (condicional): shadow execution p/ premissa maker.
- Auditoria de indexação posicional (replay/sweep/null_model).
- Hardening: guarda de contiguidade por timestamp no juice.
- Backfill 15m profundo: só se Estágio 1 passar.