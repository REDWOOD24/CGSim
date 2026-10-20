from __future__ import annotations

import json
from pathlib import Path

from policy_search.harness import paths


def generate_config(policy_name: str, split: str, run_dir: Path, num_jobs: int = -1,
                     workspace: str | None = None, dataset: str = "default") -> Path:
    """Writes a cg-sim config.json for `policy_name` on `split` into run_dir.

    Every path is written absolute, since cg-sim resolves the paths inside
    the config (and the plugin's jobs_file/output_file custom parameters)
    relative to the process's current working directory, not the config
    file's location. `workspace` (see paths.resolve_policy_spec_path)
    resolves an agent session's own policy first, falling back to the
    shared curated policies/ directory. `dataset` selects which trace/split
    set to use (see paths.DATASETS) -- "default" (15K rows, 3 sites) or
    "large" (849K rows, 31 sites, see data/prepare_all_jobs.py).
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(exist_ok=True)  # cg-sim always writes logs/CGSim.log relative to its cwd

    config = {
        "Grid_Name": "ATLAS-GRID",
        "Sites_Information": str(paths.SITE_INFO),
        "Sites_Connection_Information": str(paths.SITE_CONN_INFO),
        "Plugin": str(paths.PLUGIN_SO),
        "Limited_Sites": paths.sites_in_split(split, dataset),
        "Custom_Parameters": {
            "Num_of_Jobs": str(num_jobs),
            "jobs_file": str(paths.split_csv_path(split, dataset)),
            "output_file": str(run_dir / "results.db"),
            "policy_spec_file": str(paths.resolve_policy_spec_path(policy_name, workspace)),
        },
    }

    config_path = run_dir / "config.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    return config_path
