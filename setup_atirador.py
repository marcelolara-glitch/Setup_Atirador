#!/usr/bin/env python3
"""
Setup Atirador — Entry Point

Este é o único arquivo que você precisa executar.
Para atualizar a versão ativa, altere a variável VERSION abaixo.

Versão ativa atual: v6.6.2
"""
import subprocess
import sys
import os

# ─── Versão ativa ────────────────────────────────────────────────────────────
# Altere aqui ao fazer upgrade para uma nova versão (ex: "v6_7_0")
VERSION = "v6_6_2"
# ─────────────────────────────────────────────────────────────────────────────

script_dir = os.path.dirname(os.path.abspath(__file__))
script_path = os.path.join(script_dir, f"setup_atirador_{VERSION}.py")

if not os.path.exists(script_path):
    print(f"[ERRO] Script não encontrado: {script_path}")
    print(f"Verifique se o arquivo setup_atirador_{VERSION}.py existe no repositório.")
    sys.exit(1)

if __name__ == "__main__":
    sys.exit(subprocess.call([sys.executable, script_path] + sys.argv[1:]))
