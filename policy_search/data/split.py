"""Chronological calibration/dev/test split for the PanDA job trace.

The split is a plain percentile cut on `creationtime`, with no shuffling:
shuffling would destroy the no-future-leakage property that chronological
splitting exists to guarantee. `seed` is accepted only for interface
symmetry with a possible future stratified-split variant and is unused here.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from pathlib import Path

DEFAULT_CALIB_FRAC = 0.6
DEFAULT_DEV_FRAC = 0.2


def load_and_sort(csv_path: Path) -> list[dict]:
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    rows.sort(key=lambda r: float(r["creationtime"]))
    return rows


def compute_cutoffs(
    rows: list[dict],
    calib_frac: float = DEFAULT_CALIB_FRAC,
    dev_frac: float = DEFAULT_DEV_FRAC,
    seed: int | None = None,  # unused in v1; kept for interface symmetry
) -> tuple[int, int]:
    n = len(rows)
    calib_end = int(n * calib_frac)
    dev_end = int(n * (calib_frac + dev_frac))
    return calib_end, dev_end


def _site_counts(rows: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["computingsite"]] = counts.get(r["computingsite"], 0) + 1
    return counts


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent
        ).decode().strip()
    except Exception:
        return "unknown"


def write_splits(rows: list[dict], cutoffs: tuple[int, int], out_dir: Path, source_csv: Path) -> dict:
    calib_end, dev_end = cutoffs
    splits = {
        "calib": rows[:calib_end],
        "dev": rows[calib_end:dev_end],
        "test": rows[dev_end:],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    for name, split_rows in splits.items():
        out_path = out_dir / f"{name}.csv"
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(split_rows)

    manifest = {
        "source_csv": str(source_csv),
        "source_sha256": _sha256(source_csv),
        "git_commit": _git_commit(),
        "total_rows": len(rows),
        "calib_end_index": calib_end,
        "dev_end_index": dev_end,
        "t_calib_end": float(rows[calib_end - 1]["creationtime"]) if calib_end > 0 else None,
        "t_dev_end": float(rows[dev_end - 1]["creationtime"]) if dev_end > 0 else None,
        "splits": {
            name: {
                "row_count": len(split_rows),
                "site_counts": _site_counts(split_rows),
                "t_min": float(split_rows[0]["creationtime"]) if split_rows else None,
                "t_max": float(split_rows[-1]["creationtime"]) if split_rows else None,
            }
            for name, split_rows in splits.items()
        },
    }
    with open(out_dir / "split_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("config/job_data.csv"))
    parser.add_argument("--out", type=Path, default=Path("policy_search/data/splits"))
    parser.add_argument("--calib-frac", type=float, default=DEFAULT_CALIB_FRAC)
    parser.add_argument("--dev-frac", type=float, default=DEFAULT_DEV_FRAC)
    args = parser.parse_args()

    rows = load_and_sort(args.source)
    cutoffs = compute_cutoffs(rows, args.calib_frac, args.dev_frac)
    manifest = write_splits(rows, cutoffs, args.out, args.source)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
