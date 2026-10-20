from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunSpec:
    policy_name: str
    split: str  # "calib" | "dev" | "test"
    run_id: str
    num_jobs: int = -1


@dataclass
class RunResult:
    run_id: str
    db_path: str
    config_path: str
    job_count: int
    metrics: dict = field(default_factory=dict)


@dataclass
class ComparisonResult:
    baseline_run_id: str
    candidate_run_id: str
    paired_job_count: int
    mean_turnaround_delta: float  # candidate - baseline; negative = candidate is faster
    median_turnaround_delta: float
    baseline_metrics: dict
    candidate_metrics: dict
    assignment_agreement_fraction: float  # fraction of jobs assigned to the same site in both runs
