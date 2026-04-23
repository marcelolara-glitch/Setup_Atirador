#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_TOKEN:?GITHUB_TOKEN não definido}"

API="https://api.github.com/repos/marcelolara-glitch/Setup_Atirador/contents"
REF="v9-foundation"

STEPS=(
  "1|smc_lib.py"
  "2|regime.py"
  "3|risk.py"
  "4|indicators.py"
  "5|setups/base.py,setups/cont_pull.py"
  "6|setups/rev_exaust.py"
)

exists() {
  [ "$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "${API}/$1?ref=${REF}")" = "200" ]
}

ok=0; missing=""
for step in "${STEPS[@]}"; do
  n="${step%%|*}"; paths="${step#*|}"; all=1
  IFS=',' read -ra items <<< "$paths"
  for p in "${items[@]}"; do exists "$p" || all=0; done
  label="${paths//,/ + }"
  if [ "$all" = "1" ]; then
    echo "✅ ${n}. ${label}"; ok=$((ok + 1))
  else
    echo "❌ ${n}. ${label} NÃO ENCONTRADO"
    [ -z "$missing" ] && missing="$n"
  fi
done

echo
if [ "$ok" = "${#STEPS[@]}" ]; then
  echo "Status: ${ok}/${#STEPS[@]} — segue para o passo 7"
else
  echo "Status: ${ok}/${#STEPS[@]} — investigar passo ${missing} antes de continuar"
fi
