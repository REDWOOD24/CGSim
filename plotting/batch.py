"""Run the full plot suite for a timestamped run directory (policy or site sweeps)."""

from __future__ import annotations

from pathlib import Path

from plotting.dm_log_parser import NegativeTransferBytesError, iter_config_run_directories
from plotting.dm_log_plots import (
    plot_cross_configuration_summary,
    plot_per_configuration_transfers,
    preflight_matplotlib,
)


def run_plot_suite_for_run_root(
    run_root: Path,
    site_info: Path,
    repo_root: Path,
    *,
    no_plots: bool,
) -> None:
    if no_plots:
        return
    preflight_matplotlib(repo_root)
    for d in iter_config_run_directories(run_root):
        try:
            plot_per_configuration_transfers(d, site_info, stem_title=d.name)
            print(f"Plots -> {d}")
        except NegativeTransferBytesError:
            raise
        except Exception as e:
            print(f"Plot error for {d.name}: {e}")
    try:
        plot_cross_configuration_summary(run_root, site_info)
        print(f"Cross-config plot -> {run_root / 'cross_config_transfer_stacked.png'}")
    except NegativeTransferBytesError:
        raise
    except Exception as e:
        print(f"Cross-config plot error: {e}")
