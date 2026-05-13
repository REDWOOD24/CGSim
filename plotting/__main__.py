"""CLI: python -m plotting --plot-only config-files/runs_* [--site-info PATH]"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BASE_CONFIG = ROOT / "config-files" / "toy_config.json"

from plotting.batch import run_plot_suite_for_run_root  # noqa: E402
from plotting.dm_log_parser import resolve_site_info_json  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--plot-only",
        type=Path,
        required=True,
        help="Path to a runs_* or runs_sites_* folder (policy runs: config-files/; site sweep: build/logs/)",
    )
    p.add_argument(
        "--site-info",
        type=Path,
        default=None,
        help="Fallback site axes JSON if a subdir's toy_config Sites_Information is missing (default: mimic, else base toy_config, else toy_new_site_info_1.json)",
    )
    args = p.parse_args()
    run_root = args.plot_only.resolve()
    if not run_root.is_dir():
        sys.exit(f"Not a directory: {run_root}")
    site_info = resolve_site_info_json(args.site_info, repo_root=ROOT, base_config_path=BASE_CONFIG)
    print(f"Heatmap fallback site-info: {site_info} (each subdir uses its toy_config Sites_Information when set)")
    run_plot_suite_for_run_root(run_root, site_info, ROOT, no_plots=False)
    print(f"Plots updated under {run_root}")


if __name__ == "__main__":
    main()
