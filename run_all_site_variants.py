#!/usr/bin/env python3
"""
Run cg-sim for each ``toy_data/toy_new_site_info_<n>.json`` (numeric ``n`` order), collect logs and plots under a
timestamped run folder under build/logs/, then run the same plotting suite as generate_policy_variants.py.

Layout:

  build/logs/runs_sites_<YYYYMMDD_HHMMSS>/
    01_toy_new_site_info_1_p0r0/toy_config.json
    01_toy_new_site_info_1_p0r0/atlas_grid_simulation.log
    01_toy_new_site_info_1_p0r0/toy_job.db
    ... PNGs after plotting ...
    cross_config_transfer_stacked.png

Each subdir must contain toy_config.json + atlas_grid_simulation.log for the plotter.
Each ``toy_config.json`` is the base config with:

- ``Sites_Information`` → absolute path of the matching ``toy_data/toy_new_site_info_*.json``
- **Varied data-management policy** so cross-configuration plots mix proactive templates
  (``storage_rebalance``, ``network_aware_rebalance``, ``hotset_replication``), reactive
  remote-source templates (first five strategies), file-pick / path-metric sub-options, and
  alternating proactive ``MOVE`` / ``COPY``.

At least ``MIN_SITE_SWEEP_RUNS`` simulations are scheduled by default (override with
``--min-runs N``), cycling site files as needed so template combinations are well represented.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CONFIG_DIR = ROOT / "config-files"
TOY_DATA_DIR = ROOT / "toy_data"
BASE_CONFIG = CONFIG_DIR / "toy_config.json"
BUILD_DIR = ROOT / "build"
SITE_RUNS_PARENT = BUILD_DIR / "logs"
CG_SIM = BUILD_DIR / "cg-sim"
DEFAULT_LOG_REL = Path("logs") / "atlas_grid_simulation.log"

from plotting.batch import run_plot_suite_for_run_root  # noqa: E402
from plotting.dm_log_parser import resolve_site_info_json  # noqa: E402

# Minimum sweep length so proactive template types (3) × policy offsets appear often enough
# in cross-configuration stacked plots (repeat site JSONs as needed when fewer files exist).
MIN_SITE_SWEEP_RUNS = 45

PROACTIVE_TRANSFER_SEQUENCE = [
    "storage_rebalance",
    "network_aware_rebalance",
    "hotset_replication",
    "custom_policy_agent",
]

REACTIVE_TEMPLATE_SEQUENCE = [
    "first_replica",
    "least_utilized_source",
    "most_utilized_source",
    "random_replica",
    "hash_filename_job",
    "custom_policy_agent",
]


def _iter_site_variant_json_files(toy_data_dir: Path) -> list[Path]:
    """``toy_new_site_info_1.json`` … ``toy_new_site_info_30.json`` in numeric order."""

    def sort_key(p: Path) -> tuple[int, str]:
        m = re.fullmatch(r"toy_new_site_info_(\d+)\.json", p.name)
        if m:
            return (int(m.group(1)), p.name)
        return (10**12, p.name)

    return sorted(toy_data_dir.glob("toy_new_site_info_*.json"), key=sort_key)


def _expand_site_run_plan(site_files: list[Path], *, min_runs: int) -> list[tuple[Path, int]]:
    """(site_json_path, variant_id) with ``variant_id`` driving policy rotation; length >= min runs."""
    if not site_files:
        return []
    n = len(site_files)
    count = max(min_runs, n)
    return [(site_files[i % n], i) for i in range(count)]


def _load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def _config_for_site_policy_variant(
    base: dict, site_info_path: Path, variant_id: int, output_db_path: Path
) -> dict:
    """
    Clone base config, set ``Sites_Information``, and rotate DM policy for sweep diversity.

    Proactive template cycles storage / network-aware / hotset (indices 0–2; never
    ``custom_policy_agent``). Reactive uses the first five remote-source strategies only.
    """
    cfg = json.loads(json.dumps(base))
    cfg["Sites_Information"] = str(site_info_path.resolve())
    dmp = cfg.setdefault("Data_Management_Policy", {})
    reactive = dmp.setdefault("reactive", {})
    proactive = dmp.setdefault("proactive", {})

    proactive_index = variant_id % 3
    reactive_index = variant_id % 5
    reactive["remote_source_template"] = [
        reactive_index,
        list(REACTIVE_TEMPLATE_SEQUENCE),
    ]
    proactive["transfer_template"] = [
        proactive_index,
        list(PROACTIVE_TRANSFER_SEQUENCE),
    ]
    proactive["data_transfer_mode"] = "MOVE" if (variant_id % 2 == 0) else "COPY"

    tpl = proactive.setdefault("template_params", {})
    fp_i = (variant_id // 3) % 4
    pm_i = (variant_id // 5) % 3
    hot_thr = [0.05, 0.08, 0.12][variant_id % 3]

    sb = tpl.get("storage_rebalance")
    if isinstance(sb, dict):
        sb["file_pick"] = [
            fp_i,
            ["first_fit", "largest_fit", "smallest_fit", "random_fit"],
        ]
    na = tpl.get("network_aware_rebalance")
    if isinstance(na, dict):
        na["path_metric"] = [
            pm_i,
            ["estimated_transfer_time", "link_load", "bandwidth_only"],
        ]
    hs = tpl.get("hotset_replication")
    if isinstance(hs, dict):
        hs["hotness_threshold"] = hot_thr

    cfg.setdefault("Custom_Parameters", {})["output_file"] = str(output_db_path.resolve())

    return cfg


def _run_cg_sim(config_path: Path) -> None:
    if not CG_SIM.is_file():
        raise FileNotFoundError(f"Missing {CG_SIM} — build the project first.")
    subprocess.run(
        [str(CG_SIM), "-c", str(config_path)],
        cwd=BUILD_DIR,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-root",
        type=Path,
        default=None,
        help="Existing build/logs/runs_sites_* folder to re-run plots only (skip sims)",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip matplotlib plots",
    )
    parser.add_argument(
        "--site-info",
        type=Path,
        default=None,
        help="Fallback site-list JSON for heatmaps if a run's toy_config Sites_Information is missing (default: mimic, else base toy_config, else toy_new_site_info_1.json). Per-run plots prefer each toy_config's Sites_Information.",
    )
    parser.add_argument(
        "--min-runs",
        type=int,
        default=MIN_SITE_SWEEP_RUNS,
        metavar="N",
        help=(
            f"Minimum number of simulations in the sweep (default {MIN_SITE_SWEEP_RUNS}); "
            "site JSONs are cycled in numeric order when N exceeds how many exist."
        ),
    )
    args = parser.parse_args()

    site_info = resolve_site_info_json(args.site_info, repo_root=ROOT, base_config_path=BASE_CONFIG)
    print(
        f"Heatmap fallback site-info: {site_info} "
        "(each subdir uses toy_config Sites_Information when set)"
    )

    if args.run_root is not None:
        run_root = args.run_root.resolve()
        if not run_root.is_dir():
            sys.exit(f"Not a directory: {run_root}")
        run_plot_suite_for_run_root(run_root, site_info, ROOT, no_plots=args.no_plots)
        print(f"Done (plots only). Run root: {run_root}")
        return

    if not BASE_CONFIG.is_file():
        sys.exit(f"Missing base config: {BASE_CONFIG}")

    site_files = _iter_site_variant_json_files(TOY_DATA_DIR)
    if not site_files:
        sys.exit(f"No toy_new_site_info_*.json files under {TOY_DATA_DIR}")

    plan = _expand_site_run_plan(site_files, min_runs=max(1, args.min_runs))
    print(
        f"Site sweep: {len(plan)} runs (min_runs={args.min_runs}, "
        f"{len(site_files)} site JSONs) — proactive cycles str/net/hot, reactive 0–4, MOVE/COPY."
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_root = (SITE_RUNS_PARENT / f"runs_sites_{stamp}").resolve()
    run_root.mkdir(parents=True, exist_ok=False)

    base_cfg = _load_json(BASE_CONFIG)

    for idx, (site_path, variant_id) in enumerate(plan, start=1):
        stem = site_path.stem
        proactive_index = variant_id % 3
        reactive_index = variant_id % 5
        sub_name = f"{idx:02d}_{stem}_p{proactive_index}r{reactive_index}"
        sub = run_root / sub_name
        sub.mkdir(parents=True, exist_ok=False)
        cfg = _config_for_site_policy_variant(base_cfg, site_path, variant_id, sub / "toy_job.db")
        cfg_path = sub / "toy_config.json"
        _write_json(cfg_path, cfg)
        pkind = ("storage", "netaware", "hotset")[proactive_index]
        print(
            f"[{idx}/{len(plan)}] {site_path.name} variant={variant_id} "
            f"proactive={pkind} reactive_i={reactive_index} -> {sub_name}"
        )
        _run_cg_sim(cfg_path)
        built_log = (BUILD_DIR / DEFAULT_LOG_REL).resolve()
        if not built_log.is_file():
            raise FileNotFoundError(f"Expected log at {built_log} after cg-sim")
        shutil.move(str(built_log), str(sub / "atlas_grid_simulation.log"))

    run_plot_suite_for_run_root(run_root, site_info, ROOT, no_plots=args.no_plots)
    print(f"Done. Run root: {run_root}")


if __name__ == "__main__":
    main()
