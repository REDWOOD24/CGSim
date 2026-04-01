#!/usr/bin/env python3
"""
Run CGSim for all 30 toy_new_site_info_[1..30].json variants, one by one,
and save each run's log as atlas_grid_simulation_[n].log under build/logs/.

This script does NOT modify the main toy_config.json; instead, it creates
temporary config files toy_config_[n].json that differ only in the
Sites_Information path.
"""

import json
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config-files"
TOY_DATA_DIR = ROOT / "toy_data"
BUILD_DIR = ROOT / "build"
LOGS_DIR = BUILD_DIR / "logs"


def run_variant(idx: int) -> None:
    base_config = CONFIG_DIR / "toy_config.json"
    variant_site_info = TOY_DATA_DIR / f"toy_new_site_info_{idx}.json"

    if not base_config.exists():
        raise FileNotFoundError(f"Base config not found: {base_config}")
    if not variant_site_info.exists():
        raise FileNotFoundError(f"Variant site info not found: {variant_site_info}")

    # Load base config and override Sites_Information
    with base_config.open("r") as f:
        cfg = json.load(f)

    cfg["Sites_Information"] = str(variant_site_info)

    # Write temporary config for this run
    tmp_config = CONFIG_DIR / f"toy_config_{idx}.json"
    with tmp_config.open("w") as f:
        json.dump(cfg, f, indent=2)

    # Ensure logs directory exists
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Remove any previous default log to avoid mixing runs
    default_log = LOGS_DIR / "atlas_grid_simulation.log"
    if default_log.exists():
        default_log.unlink()

    # Run cg-sim with this variant config
    cmd = ["./cg-sim", "-c", str(tmp_config)]
    print(f"=== Running variant {idx}: {cmd}")
    result = subprocess.run(cmd, cwd=BUILD_DIR)
    if result.returncode != 0:
        print(f"Variant {idx} FAILED with exit code {result.returncode}")
    else:
        # If a log was produced, rename it with suffix _idx
        if default_log.exists():
            dest = LOGS_DIR / f"atlas_grid_simulation_{idx}.log"
            shutil.move(str(default_log), str(dest))
            print(f"Saved log to {dest}")
        else:
            print(f"Variant {idx} completed but no default log found at {default_log}")


def main() -> None:
    for idx in range(1, 31):
        try:
            run_variant(idx)
        except Exception as e:
            print(f"Error while running variant {idx}: {e}")


if __name__ == "__main__":
    main()

