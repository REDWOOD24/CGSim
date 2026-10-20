from __future__ import annotations

import statistics
from pathlib import Path

from policy_search.harness import db, metrics
from policy_search.harness.schemas import ComparisonResult


class ComparisonError(RuntimeError):
    pass


def compare_runs(baseline_run_id: str, baseline_db_path: Path, candidate_run_id: str, candidate_db_path: Path) -> ComparisonResult:
    baseline_meta = db.read_run_metadata(baseline_db_path)
    candidate_meta = db.read_run_metadata(candidate_db_path)

    baseline_split = baseline_meta.get("split")
    candidate_split = candidate_meta.get("split")
    if baseline_split and candidate_split and baseline_split != candidate_split:
        raise ComparisonError(
            f"Cannot compare runs on different splits: baseline={baseline_split!r} candidate={candidate_split!r}"
        )

    baseline_dataset = baseline_meta.get("dataset")
    candidate_dataset = candidate_meta.get("dataset")
    if baseline_dataset and candidate_dataset and baseline_dataset != candidate_dataset:
        raise ComparisonError(
            f"Cannot compare runs from different datasets: baseline={baseline_dataset!r} "
            f"candidate={candidate_dataset!r} -- e.g. one from the 3-site trace, one from the 31-site trace"
        )

    baseline_rows = {r["JOB_ID"]: r for r in db.read_job_results(baseline_db_path) if r["STATUS"] == "finished"}
    candidate_rows = {r["JOB_ID"]: r for r in db.read_job_results(candidate_db_path) if r["STATUS"] == "finished"}
    common_ids = sorted(set(baseline_rows) & set(candidate_rows))

    deltas = []
    same_site = 0
    for job_id in common_ids:
        b, c = baseline_rows[job_id], candidate_rows[job_id]
        b_turnaround = b["COMPLETION_TIME"] - b["CREATION_TIME"]
        c_turnaround = c["COMPLETION_TIME"] - c["CREATION_TIME"]
        deltas.append(c_turnaround - b_turnaround)
        if b["ASSIGNED_SITE"] == c["ASSIGNED_SITE"]:
            same_site += 1

    return ComparisonResult(
        baseline_run_id=baseline_run_id,
        candidate_run_id=candidate_run_id,
        paired_job_count=len(common_ids),
        mean_turnaround_delta=statistics.mean(deltas) if deltas else float("nan"),
        median_turnaround_delta=statistics.median(deltas) if deltas else float("nan"),
        baseline_metrics=metrics.compute_aggregate_metrics(baseline_db_path),
        candidate_metrics=metrics.compute_aggregate_metrics(candidate_db_path),
        assignment_agreement_fraction=(same_site / len(common_ids)) if common_ids else float("nan"),
    )
