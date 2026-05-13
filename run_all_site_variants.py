#!/usr/bin/env python3
"""
Run cg-sim for each ``toy_data/toy_new_site_info_<n>.json`` (numeric ``n`` order), collect logs and plots under a
timestamped run folder under build/logs/, then run the same plotting suite as generate_policy_variants.py.

Layout:

  build/logs/runs_sites_<YYYYMMDD_HHMMSS>/
    01_toy_new_site_info_1/toy_config.json
    01_toy_new_site_info_1/atlas_grid_simulation.log
    ... PNGs after plotting ...
    cross_config_transfer_stacked.png

Each subdir must contain toy_config.json + atlas_grid_simulation.log for the plotter.
Each toy_config.json is the base toy_config with ``Sites_Information`` set to the absolute
path of the matching ``toy_data/toy_new_site_info_*.json`` (cg-sim does not accept inline site objects).
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


def _iter_site_variant_json_files(toy_data_dir: Path) -> list[Path]:
    """``toy_new_site_info_1.json`` … ``toy_new_site_info_30.json`` in numeric order."""

    def sort_key(p: Path) -> tuple[int, str]:
        m = re.fullmatch(r"toy_new_site_info_(\d+)\.json", p.name)
        if m:
            return (int(m.group(1)), p.name)
        return (10**12, p.name)

    return sorted(toy_data_dir.glob("toy_new_site_info_*.json"), key=sort_key)


def _load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def _config_for_site_variant(base: dict, site_info_path: Path) -> dict:
    """cg-sim expects Sites_Information to be a path string, not an inline JSON object."""
    cfg = json.loads(json.dumps(base))
    cfg["Sites_Information"] = str(site_info_path.resolve())
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

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_root = (SITE_RUNS_PARENT / f"runs_sites_{stamp}").resolve()
    run_root.mkdir(parents=True, exist_ok=False)

    base_cfg = _load_json(BASE_CONFIG)

    for idx, site_path in enumerate(site_files, start=1):
        stem = site_path.stem
        sub = run_root / f"{idx:02d}_{stem}"
        sub.mkdir(parents=True, exist_ok=False)
        cfg = _config_for_site_variant(base_cfg, site_path)
        cfg_path = sub / "toy_config.json"
        _write_json(cfg_path, cfg)
        print(f"[{idx}/{len(site_files)}] Running cg-sim for {site_path.name} -> {sub.name}")
        _run_cg_sim(cfg_path)
        built_log = (BUILD_DIR / DEFAULT_LOG_REL).resolve()
        if not built_log.is_file():
            raise FileNotFoundError(f"Expected log at {built_log} after cg-sim")
        shutil.move(str(built_log), str(sub / "atlas_grid_simulation.log"))

    run_plot_suite_for_run_root(run_root, site_info, ROOT, no_plots=args.no_plots)
    print(f"Done. Run root: {run_root}")


if __name__ == "__main__":
    main()
