"""TRAVA de regressao do PR-A: o alicerce do v10 nao encosta no v9.

O PR-A e so schema + SetupSpec + adaptadores. Nenhum dos arquivos abaixo pode
mudar um byte. Comparar com `git diff` prova isso no momento do merge e nunca
mais; um sha256 congelado prova a cada `pytest`, inclusive num rebase que
arrastasse alteracao de outra branch para dentro destes arquivos.

Se um teste daqui falhar num PR que NAO era para mexer nesses arquivos: e a
mudanca que esta errada, nao o hash. Rebaselinar so quando o PR for
declaradamente sobre o arquivo — e ai o hash novo entra no mesmo commit da
alteracao, de proposito, nunca para "fazer o teste passar".
"""

from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# sha256 em 0d3565f (merge-base do PR-A com main).
CONGELADOS = {
    "journal.py":
        "0362962718b69feb8dfb195a66e2ec4eb00455ae3f281189e39c4fb91b7c20c0",
    "main.py":
        "6448536013bde0180b53fd8dd5d2f2d103c91399628e9e5beef9cddc737eb491",
    "signals.py":
        "54cb72fb6ef9980c866584b2d4b9fa3a3d889c6863f435c058cde94353de866a",
    "telegram.py":
        "68d4046f8823a6c53583064ccc2a109454e32f0aaa96c67e65e05f1ec1815cf9",
    "setups/rev_zone.py":
        "56a2fb9c91aea528b773d1c0f1778d7141ccc3968e31833cf53c9e40a9ec6da1",
    "setups/cont_pull.py":
        "eb328e3ee9178bfb32c191718025a655645be417efc9b1c7f625e15afefbcc09",
    "setups/break_range.py":
        "aaac76229b92d46afe8023a3473894f14219c09d4130b9e78c04f7e5cfd84837",
    "setups/rev_exaust.py":
        "f4485b11df579a0618216cbb8bae375ca75d99c5af1aca6a652b4877adaa740b",
    "setups/breaker.py":
        "b87f174e39bec72727f74cf3fbbc8edb900b104771d3cdc9760bc9fde8f5713c",
    "shadow/donchian_a.py":
        "3a2ce076862593baa4622d967250ba9c546930ec59b0cd6dc1ccb0baace1c624",
    "shadow/kis_regime.py":
        "a3a30d996dd6cdb5d61778d73f27522e8fbd75e81e7e5d6fc3ae845afd5cd5cc",
}


def test_arquivos_do_v9_byte_identicos():
    mudados = [
        rel for rel, esperado in CONGELADOS.items()
        if hashlib.sha256((ROOT / rel).read_bytes()).hexdigest() != esperado
    ]
    assert not mudados, f"PR-A nao pode tocar: {mudados}"


def test_a_lista_cobre_os_cinco_setups_e_os_dois_shadows():
    # Guarda contra "passar" apagando linha da lista em vez de desfazer a
    # alteracao. Se um setup sumir daqui, este teste reprova antes.
    setups = {r for r in CONGELADOS if r.startswith("setups/")}
    shadows = {r for r in CONGELADOS if r.startswith("shadow/")}
    assert len(setups) == 5 and len(shadows) == 2
    assert {"journal.py", "main.py", "signals.py"} <= set(CONGELADOS)


def test_v10_nao_e_importado_por_nenhum_modulo_do_v9():
    # O alicerce e mudo: nada do v9 depende dele. No dia em que um consumidor
    # existir, ele nasce em modulo NOVO e este teste ganha a excecao explicita.
    for rel in CONGELADOS:
        texto = (ROOT / rel).read_text()
        assert "v10" not in texto, f"{rel} referencia v10"
