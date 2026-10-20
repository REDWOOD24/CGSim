"""MCP tool server for the CGSim policy-search harness.

Run under a Python >= 3.10 environment (the `mcp` package's requirement);
the harness modules it wraps are stdlib-only and also import fine under the
repo's default python3. See policy_search/requirements.txt.
"""
from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mcp.server.mcpserver import MCPServer

from policy_search.harness import bottlenecks, compare, describe, experiment_log, orchestrate, policy_validation

server = MCPServer(name="cgsim-policy-search")

# One MCP server process = one agent session (loop.py launches a fresh
# server subprocess per run). Policies this session registers are written
# under this private workspace rather than the shared curated policies/
# directory, so concurrent sessions can never collide with each other's
# policy names, and a session can never silently overwrite a curated
# baseline (both happened in practice before this existed -- see
# policy_search/README.md). Reads still fall back to the shared directory,
# so baseline names like "historical_replay" resolve normally.
SESSION_WORKSPACE = uuid.uuid4().hex[:12]

# Which dataset this whole session defaults to (see paths.DATASETS). Set once
# at server startup (loop.py passes --dataset on the command line it launches
# this process with) rather than trusted to arrive as a per-call tool
# argument -- a model that forgets to pass dataset="large" on some call would
# otherwise silently query the default 3-site trace instead. Every tool below
# still accepts an explicit `dataset` override for a rare deliberate
# cross-dataset comparison.
_arg_parser = argparse.ArgumentParser()
_arg_parser.add_argument("--dataset", default="default", choices=["default", "large"])
SESSION_DATASET = _arg_parser.parse_known_args()[0].dataset


@server.tool()
def describe_workload(split: str, dataset: str = SESSION_DATASET) -> dict:
    """Describe a data split (calib/dev/test): row count, columns, per-site
    counts, time range, and known modeling caveats (e.g. no historical
    failure signal, non-uniform time density). `dataset` is "default" (the
    original 15,000-row / 3-site trace) or "large" (849,070 rows / 31 real
    sites, from a real PanDA export -- see policy_search/README.md; note its
    per-simulation runtime scales to several minutes at full split size, so
    prefer a bounded num_jobs while iterating)."""
    return describe.describe_workload(split, dataset)


@server.tool()
def register_policy(policy_name: str, spec: dict) -> dict:
    """Validate and save a JSON policy spec under this session's private
    workspace (never the shared curated policies/ directory, and never
    visible to other concurrent sessions). Returns a content hash and any
    non-fatal warnings (e.g. leakage-feature usage)."""
    try:
        sha256, warnings = policy_validation.register_policy(policy_name, spec, workspace=SESSION_WORKSPACE)
        return {"ok": True, "sha256": sha256, "warnings": warnings}
    except policy_validation.PolicySpecError as e:
        return {"ok": False, "error": str(e)}


@server.tool()
def validate_policy(policy_name: str, dataset: str = SESSION_DATASET) -> dict:
    """Dry-run a registered policy against a single job so spec errors the
    C++ plugin would raise (unknown feature, malformed JSON, ...) surface
    immediately, without spending a full simulation."""
    return orchestrate.validate_policy(policy_name, workspace=SESSION_WORKSPACE, dataset=dataset)


@server.tool()
def run_simulation(policy_name: str, split: str, num_jobs: int = -1, notes: str = "",
                    allow_test_split: bool = False, dataset: str = SESSION_DATASET) -> dict:
    """Run a registered policy on a split via cg-sim and record aggregate
    metrics in the experiment ledger. Refuses split="test" unless
    allow_test_split=True, to keep the locked test split honest during
    iteration. `dataset`: "default" (15,000 rows, 3 sites, fast) or "large"
    (849,070 rows, 31 sites, real PanDA data -- ~4.6ms/job, so a full split
    takes minutes; pass a bounded num_jobs while exploring, e.g. 5000-20000,
    and note that a small num_jobs takes the chronologically FIRST rows of
    the split, which can be dominated by a single bursty site -- prefer at
    least ~20000 for a representative sample)."""
    try:
        result = orchestrate.run_policy(policy_name, split, num_jobs=num_jobs, notes=notes,
                                         allow_test_split=allow_test_split, workspace=SESSION_WORKSPACE,
                                         dataset=dataset)
        return {"ok": True, **asdict(result)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@server.tool()
def compare_runs(baseline_run_id: str, candidate_run_id: str) -> dict:
    """Paired comparison between two prior run_ids from run_simulation (they
    must have used the same split)."""
    baseline = experiment_log.get_run(baseline_run_id)
    candidate = experiment_log.get_run(candidate_run_id)
    if baseline is None:
        return {"ok": False, "error": f"Unknown run_id: {baseline_run_id}"}
    if candidate is None:
        return {"ok": False, "error": f"Unknown run_id: {candidate_run_id}"}
    try:
        result = compare.compare_runs(baseline_run_id, Path(baseline["db_path"]),
                                       candidate_run_id, Path(candidate["db_path"]))
        return {"ok": True, **asdict(result)}
    except compare.ComparisonError as e:
        return {"ok": False, "error": str(e)}


@server.tool()
def inspect_bottlenecks(run_id: str) -> dict:
    """Time-series diagnostics (queue depth, utilization) for a prior run.
    Only available if that run's policy spec set "emit_debug_events": true."""
    run = experiment_log.get_run(run_id)
    if run is None:
        return {"ok": False, "error": f"Unknown run_id: {run_id}"}
    try:
        return {"ok": True, **bottlenecks.inspect_bottlenecks(Path(run["db_path"]))}
    except bottlenecks.BottleneckDataUnavailable as e:
        return {"ok": False, "error": str(e)}


@server.tool()
def record_experiment(run_id: str, notes: str) -> dict:
    """Attach free-text rationale to a prior run in the experiment ledger
    (e.g. why this policy was tried, what was learned)."""
    updated = experiment_log.update_notes(run_id, notes)
    if not updated:
        return {"ok": False, "error": f"Unknown run_id: {run_id}"}
    return {"ok": True}


if __name__ == "__main__":
    server.run(transport="stdio")
