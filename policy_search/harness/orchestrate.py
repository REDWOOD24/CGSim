from __future__ import annotations

import json
import time
import uuid

from policy_search.harness import config_gen, db, experiment_log, metrics, paths, policy_validation, runner
from policy_search.harness.schemas import RunResult


class SplitGuardError(RuntimeError):
    pass


def _new_run_id(policy_name: str, split: str) -> str:
    return f"{policy_name}__{split}__{int(time.time())}__{uuid.uuid4().hex[:8]}"


def run_policy(policy_name: str, split: str, num_jobs: int = -1, notes: str = "",
                allow_test_split: bool = False, run_id: str | None = None,
                workspace: str | None = None, dataset: str = "default") -> RunResult:
    """Runs one policy on one split end to end: generates the cg-sim config,
    runs the simulation, computes aggregate metrics, records everything in
    the experiment ledger. This is what the MCP `run_simulation` tool calls.

    `workspace` resolves policy_name from that session's private directory
    first, falling back to the shared curated policies/ directory -- see
    paths.resolve_policy_spec_path. `dataset` selects which trace/split set
    to use (see paths.DATASETS).
    """
    if split == "test" and not allow_test_split:
        raise SplitGuardError(
            "Refusing to run on the locked 'test' split without allow_test_split=True. "
            "Iterate on 'calib'/'dev' during policy search; only evaluate frozen finalists on 'test'."
        )

    spec_path = paths.resolve_policy_spec_path(policy_name, workspace)  # raises FileNotFoundError if missing

    run_id = run_id or _new_run_id(policy_name, split)
    run_dir = paths.RUNS_DIR / run_id

    config_path = config_gen.generate_config(policy_name, split, run_dir, num_jobs=num_jobs, workspace=workspace,
                                              dataset=dataset)
    timing = runner.run_simulation(config_path, run_dir)

    db_path = run_dir / "results.db"
    db.write_run_metadata(db_path, {"split": split, "dataset": dataset, "run_id": run_id,
                                     "wallclock_s": timing["wallclock_s"]})

    run_metrics = metrics.compute_aggregate_metrics(db_path)
    spec_sha256 = policy_validation.policy_spec_sha256(policy_name, workspace)
    experiment_log.record_run(run_id, policy_name, spec_sha256, split, db_path, run_metrics, notes=notes)

    return RunResult(run_id=run_id, db_path=str(db_path), config_path=str(config_path),
                      job_count=run_metrics["job_count"], metrics=run_metrics)


def validate_policy(policy_name: str, workspace: str | None = None, dataset: str = "default") -> dict:
    """Zero-cost-ish dry run: runs the policy against a single job from the
    calib split so any error the C++ loader would raise (unknown feature,
    malformed JSON, ...) surfaces immediately, without spending a full run.
    """
    try:
        spec_path = paths.resolve_policy_spec_path(policy_name, workspace)
    except FileNotFoundError as e:
        return {"valid": False, "error": str(e)}

    try:
        spec = json.loads(spec_path.read_text())
        warnings = policy_validation.validate_spec_shape(spec)
    except (json.JSONDecodeError, policy_validation.PolicySpecError) as e:
        return {"valid": False, "error": str(e)}

    try:
        result = run_policy(policy_name, "calib", num_jobs=1, notes="validate_policy dry run",
                             run_id=f"_validate_{policy_name}_{uuid.uuid4().hex[:8]}", workspace=workspace,
                             dataset=dataset)
    except Exception as e:
        return {"valid": False, "error": str(e), "warnings": warnings}

    return {"valid": True, "warnings": warnings, "dry_run_job_count": result.job_count}
