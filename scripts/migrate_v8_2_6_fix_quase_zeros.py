#!/usr/bin/env python3
# scripts/migrate_v8_2_6_fix_quase_zeros.py
#
# Migration v8.2.6 — encerra trades QUASE gravados com entry_price=0.
#
# Contexto:
#   Antes de v8.2.6, calc_trade_params não era chamado para QUASE,
#   resultando em params=None e, via `p.get("entry", 0)`, em entry_price=0.
#   Esses trades ficam indefinidamente OPEN sem possibilidade de tracking.
#
# Ação:
#   Marca todos os trades com entry_price=0.0 AND status='OPEN'
#   como status='EXPIRED', exit_time=datetime('now').
#
# Idempotência:
#   A query só afeta linhas com entry_price=0.0 AND status='OPEN'.
#   Rodadas subsequentes não encontrarão novas linhas para alterar.

import os
import sqlite3
from datetime import datetime, timezone

BASE_DIR    = os.path.expanduser("~/Setup_Atirador")
JOURNAL_DB  = os.path.join(BASE_DIR, "journal", "atirador_journal.db")


def run_migration() -> None:
    if not os.path.exists(JOURNAL_DB):
        print(f"[migrate] Banco não encontrado: {JOURNAL_DB}")
        print("[migrate] Nada a fazer.")
        return

    conn = sqlite3.connect(JOURNAL_DB)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()

        # Listar os trades afetados antes de alterar (para o relatório)
        cur.execute("""
            SELECT id, symbol, direction, type, exit_time
            FROM trades
            WHERE entry_price = 0.0
              AND status = 'OPEN'
            ORDER BY symbol
        """)
        affected = cur.fetchall()

        if not affected:
            print("[migrate] Nenhum trade com entry_price=0.0 e status='OPEN' encontrado.")
            print("[migrate] Banco já está limpo — nada a fazer.")
            return

        # Executar a migration
        cur.execute("""
            UPDATE trades
            SET status    = 'EXPIRED',
                exit_time = datetime('now')
            WHERE entry_price = 0.0
              AND status = 'OPEN'
        """)
        conn.commit()
        n_updated = cur.rowcount

        # Relatório
        print(f"[migrate] Migration v8.2.6 concluída.")
        print(f"[migrate] Trades encerrados como EXPIRED: {n_updated}")
        print(f"[migrate] Símbolos afetados:")
        for row in affected:
            print(
                f"  id={row['id']:>5}  {row['symbol']:<18}  "
                f"{row['direction']:<5}  {row['type']:<5}  "
                f"exit_time={row['exit_time']}"
            )

    finally:
        conn.close()


if __name__ == "__main__":
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[migrate] Iniciando às {ts}")
    run_migration()
