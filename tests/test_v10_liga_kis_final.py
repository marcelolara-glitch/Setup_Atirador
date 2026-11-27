"""GUARDA — briefing 4 (25/11/2026): LIGA a `kis_3489_final_4h` e desliga as
fichas antigas. Só o campo `executar` mudou; `executar` está em
`spec.FORA_DO_HASH`, então nenhum `config_hash` pode ter se mexido.

Hashes literais de propósito: são os do merge do briefing 3 (`ed93ba5`).
Rebaselinar exige editar esta tabela, visível no diff.
"""

from __future__ import annotations

from dataclasses import replace

from v10.registro import ATIVOS, KIS_3489_FINAL_4H, REGISTRO
from v10.spec import FORA_DO_HASH, config_dict

HASHES = {"kis_regime_4h": "e63ec120e131",
          "kis_3489_60t_4h": "82488baa3086",
          "donchian_a_4h": "250170cc8dc0",
          "kis_3489_final_4h": "79bbf2bcc366"}


def test_ativos_contem_so_a_final():
    assert [s.setup_id for s in ATIVOS] == ["kis_3489_final_4h"]
    assert ATIVOS == [KIS_3489_FINAL_4H]


def test_fichas_antigas_desligadas_e_seguem_no_registro():
    assert set(REGISTRO) == set(HASHES)
    for sid in ("kis_regime_4h", "kis_3489_60t_4h", "donchian_a_4h"):
        assert REGISTRO[sid].executar is False, sid


def test_nenhum_config_hash_mudou(capsys):
    obtido = {sid: s.config_hash for sid, s in REGISTRO.items()}
    with capsys.disabled():
        print("\n  config_hash após o briefing 4:")
        for sid, h in obtido.items():
            print(f"    {sid:<20} {h}  executar={REGISTRO[sid].executar}")
    assert obtido == HASHES


def test_executar_fora_do_hash_nos_dois_sentidos():
    assert "executar" in FORA_DO_HASH
    for sid, spec in REGISTRO.items():
        assert "executar" not in config_dict(spec), sid
        assert (replace(spec, executar=False).config_hash
                == replace(spec, executar=True).config_hash
                == HASHES[sid]), sid


def test_ligar_a_final_nao_a_promove():
    s = KIS_3489_FINAL_4H
    assert (s.mode, s.estado_ciclo) == ("shadow", "proposto")
