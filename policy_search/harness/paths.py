from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_SEARCH_ROOT = REPO_ROOT / "policy_search"

CG_SIM_BINARY = REPO_ROOT / "build" / "cg-sim"
PLUGIN_SO = POLICY_SEARCH_ROOT / "plugin" / "build" / "libScoringPolicyPlugin.so"

SITE_INFO = REPO_ROOT / "config" / "site_info.json"
SITE_CONN_INFO = REPO_ROOT / "config" / "site_conn_info.json"

POLICIES_DIR = POLICY_SEARCH_ROOT / "policies"
AGENT_WORKSPACES_DIR = POLICIES_DIR / "_agent_runs"
RUNS_DIR = POLICY_SEARCH_ROOT / "runs"
EXPERIMENTS_DIR = POLICY_SEARCH_ROOT / "experiments"
EXPERIMENTS_DB = EXPERIMENTS_DIR / "experiments.db"

# A "dataset" is a (source CSV, splits directory) pair. "default" is the
# original 15,000-row / 3-site trace; "large" is the 849,070-row / 31-site
# trace prepared by data/prepare_all_jobs.py from the real PanDA export at
# data/all_jobs.csv (see that script's docstring for the filtering rules).
# Every dataset's split CSVs share the same column contract, so nothing
# downstream of split_csv_path needs to know which dataset it's looking at.
DATASETS = {
    "default": {
        "source_csv": REPO_ROOT / "config" / "job_data.csv",
        "splits_dir": POLICY_SEARCH_ROOT / "data" / "splits",
    },
    "large": {
        "source_csv": REPO_ROOT / "config" / "panda_jobs_large.csv",
        "splits_dir": POLICY_SEARCH_ROOT / "data" / "splits_large",
    },
}


def _dataset(name: str) -> dict:
    if name not in DATASETS:
        raise ValueError(f"Unknown dataset {name!r}; expected one of {sorted(DATASETS)}")
    return DATASETS[name]


def source_csv_path(dataset: str = "default") -> Path:
    return _dataset(dataset)["source_csv"]


def splits_dir(dataset: str = "default") -> Path:
    return _dataset(dataset)["splits_dir"]


def split_manifest_path(dataset: str = "default") -> Path:
    return splits_dir(dataset) / "split_manifest.json"


def split_csv_path(split: str, dataset: str = "default") -> Path:
    if split not in ("calib", "dev", "test"):
        raise ValueError(f"Unknown split '{split}', expected calib/dev/test")
    return splits_dir(dataset) / f"{split}.csv"


def policy_spec_path(policy_name: str, workspace: str | None = None) -> Path:
    """Where to WRITE a policy spec. Curated baselines (workspace=None) live
    directly under policies/. An agent session's own policies live under a
    private per-session subdirectory instead, so two concurrent agent runs
    (each its own MCP server process, see mcp_server/server.py's
    SESSION_WORKSPACE) can never collide with each other's policy names, and
    an agent can never silently overwrite a curated baseline -- both of
    which happened in practice before this existed (see policy_search/README.md).
    """
    if workspace:
        return AGENT_WORKSPACES_DIR / workspace / f"{policy_name}.json"
    return POLICIES_DIR / f"{policy_name}.json"


def resolve_policy_spec_path(policy_name: str, workspace: str | None = None) -> Path:
    """Where to READ a policy spec from. Prefers the session's own
    workspace -- so if an agent reuses a name (e.g. re-registers
    "locality_first" as its own experiment), that shadows the shared
    version for its own subsequent run_simulation/validate_policy calls
    without ever touching the shared file -- falling back to the shared
    curated policies/ directory (where historical_replay, least_loaded,
    etc. live). Raises FileNotFoundError if neither exists.
    """
    if workspace:
        candidate = AGENT_WORKSPACES_DIR / workspace / f"{policy_name}.json"
        if candidate.exists():
            return candidate
    shared = POLICIES_DIR / f"{policy_name}.json"
    if shared.exists():
        return shared
    where = f"workspace {workspace!r} or the shared policies/ directory" if workspace else "the shared policies/ directory"
    raise FileNotFoundError(f"Policy '{policy_name}' not found in {where}")


def sites_in_split(split: str, dataset: str = "default") -> list[str]:
    """The distinct `computingsite` values present in a split, sorted for
    determinism. This -- not a hardcoded constant -- is what config_gen uses
    as Limited_Sites, so adding more sites to the trace/config just works:
    the grid CGSim simulates always matches whatever sites the workload
    actually references, for any number of sites.
    """
    with open(split_csv_path(split, dataset), newline="") as f:
        reader = csv.DictReader(f)
        sites = {row["computingsite"] for row in reader if row.get("computingsite")}
    if not sites:
        raise ValueError(f"No computingsite values found in split '{split}' of dataset '{dataset}'")

    import json

    known_sites = set(json.loads(SITE_INFO.read_text()).keys())
    unknown = sites - known_sites
    if unknown:
        raise ValueError(
            f"Split '{split}' (dataset {dataset!r}) references sites not defined in {SITE_INFO.name}: "
            f"{sorted(unknown)}"
        )
    return sorted(sites)
