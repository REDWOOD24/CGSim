from __future__ import annotations

import sqlite3
from pathlib import Path


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def read_job_results(db_path: Path) -> list[dict]:
    with _connect(db_path) as con:
        cur = con.execute("SELECT * FROM JOB_RESULTS")
        return [dict(row) for row in cur.fetchall()]


def read_run_metadata(db_path: Path) -> dict:
    with _connect(db_path) as con:
        cur = con.execute("SELECT KEY, VALUE FROM RUN_METADATA")
        return {row["KEY"]: row["VALUE"] for row in cur.fetchall()}


def write_run_metadata(db_path: Path, extra: dict) -> None:
    """Appends/overwrites RUN_METADATA rows the harness knows but the C++
    plugin doesn't (e.g. which split was used, wallclock timing) -- the
    plugin only records what it can see from inside the simulation.
    """
    with _connect(db_path) as con:
        con.executemany(
            "INSERT OR REPLACE INTO RUN_METADATA (KEY, VALUE) VALUES (?, ?)",
            [(k, str(v)) for k, v in extra.items()],
        )
        con.commit()


def has_events_table(db_path: Path) -> bool:
    with _connect(db_path) as con:
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='EVENTS'")
        return cur.fetchone() is not None


def read_events(db_path: Path) -> list[dict]:
    with _connect(db_path) as con:
        cur = con.execute("SELECT * FROM EVENTS ORDER BY TIMESTAMP")
        return [dict(row) for row in cur.fetchall()]
