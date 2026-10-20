from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from policy_search.harness import paths

_SCHEMA = """
CREATE TABLE IF NOT EXISTS EXPERIMENTS (
    run_id TEXT PRIMARY KEY,
    policy_name TEXT NOT NULL,
    policy_spec_sha256 TEXT NOT NULL,
    split TEXT NOT NULL,
    db_path TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    notes TEXT,
    created_at REAL NOT NULL
);
"""


def _connect() -> sqlite3.Connection:
    paths.EXPERIMENTS_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(paths.EXPERIMENTS_DB))
    con.execute(_SCHEMA)
    return con


def record_run(run_id: str, policy_name: str, policy_spec_sha256: str, split: str, db_path: Path, metrics: dict,
                notes: str = "") -> None:
    with _connect() as con:
        con.execute(
            "INSERT OR REPLACE INTO EXPERIMENTS "
            "(run_id, policy_name, policy_spec_sha256, split, db_path, metrics_json, notes, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, policy_name, policy_spec_sha256, split, str(db_path), json.dumps(metrics), notes, time.time()),
        )
        con.commit()


def list_runs(policy_name: str | None = None, split: str | None = None) -> list[dict]:
    with _connect() as con:
        con.row_factory = sqlite3.Row
        query = "SELECT * FROM EXPERIMENTS WHERE 1=1"
        args: list = []
        if policy_name:
            query += " AND policy_name = ?"
            args.append(policy_name)
        if split:
            query += " AND split = ?"
            args.append(split)
        query += " ORDER BY created_at DESC"
        cur = con.execute(query, args)
        return [dict(row) for row in cur.fetchall()]


def update_notes(run_id: str, notes: str) -> bool:
    with _connect() as con:
        cur = con.execute("UPDATE EXPERIMENTS SET notes = ? WHERE run_id = ?", (notes, run_id))
        con.commit()
        return cur.rowcount > 0


def get_run(run_id: str) -> dict | None:
    with _connect() as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("SELECT * FROM EXPERIMENTS WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        return dict(row) if row else None
