#!/usr/bin/env python3
"""
Setup Atirador — Entry Point

Descobre e executa automaticamente a versão mais recente do scanner.
Nenhuma variável VERSION manual — ao fazer git pull, o script mais novo
presente no diretório é selecionado sem nenhuma intervenção.
"""
import glob
import re
import sys
import os
import subprocess


def _latest_script(script_dir: str):
    """Retorna o path do setup_atirador_vX_Y_Z.py de maior versão."""
    candidates = glob.glob(os.path.join(script_dir, "setup_atirador_v*.py"))

    def version_key(path):
        m = re.search(r'v(\d+)_(\d+)_(\d+)', os.path.basename(path))
        return tuple(int(x) for x in m.groups()) if m else (0, 0, 0)

    candidates.sort(key=version_key)
    return candidates[-1] if candidates else None


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = _latest_script(script_dir)

    if not script_path:
        print(f"[ERRO] Nenhum script setup_atirador_v*.py encontrado em {script_dir}")
        sys.exit(1)

    version_str = re.search(r'v\d+_\d+_\d+', os.path.basename(script_path))
    label = version_str.group().replace("_", ".") if version_str else "?"
    print(f"[entry] {os.path.basename(script_path)} ({label})", flush=True)

    sys.exit(subprocess.call([sys.executable, script_path] + sys.argv[1:]))
