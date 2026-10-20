from __future__ import annotations

import statistics
from pathlib import Path

from policy_search.harness import db


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values) - 1) * pct
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def _summary(values: list[float]) -> dict:
    if not values:
        return {"mean": float("nan"), "p50": float("nan"), "p95": float("nan"), "n": 0}
    return {
        "mean": statistics.mean(values),
        "p50": _percentile(values, 0.5),
        "p95": _percentile(values, 0.95),
        "n": len(values),
    }


def compute_aggregate_metrics(db_path: Path) -> dict:
    rows = db.read_job_results(db_path)
    n_total = len(rows)
    finished = [r for r in rows if r["STATUS"] == "finished"]
    failed = [r for r in rows if r["STATUS"] != "finished"]

    turnaround = [r["COMPLETION_TIME"] - r["CREATION_TIME"] for r in finished]
    resource_wait = [r["RESOURCE_WAITING_QUEUE_TIME"] for r in finished]
    transfer_wait = [r["FILE_TRANSFER_QUEUE_TIME"] for r in finished]

    per_site_counts: dict[str, int] = {}
    for r in finished:
        per_site_counts[r["ASSIGNED_SITE"]] = per_site_counts.get(r["ASSIGNED_SITE"], 0) + 1

    moved_from_historical = sum(1 for r in finished if r["ASSIGNED_SITE"] != r["HISTORICAL_SITE"])

    return {
        "job_count": n_total,
        "finished_count": len(finished),
        "failed_count": len(failed),
        "failure_rate": (len(failed) / n_total) if n_total else float("nan"),
        "turnaround": _summary(turnaround),
        "resource_waiting_queue_time": _summary(resource_wait),
        "file_transfer_queue_time": _summary(transfer_wait),
        "per_site_finished_counts": per_site_counts,
        "moved_from_historical_site": moved_from_historical,
        "moved_from_historical_fraction": (moved_from_historical / len(finished)) if finished else float("nan"),
    }
