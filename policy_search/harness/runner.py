from __future__ import annotations

import subprocess
import time
from pathlib import Path

from policy_search.harness import paths


class SimulationError(RuntimeError):
    pass


def run_simulation(config_path: Path, run_dir: Path, timeout_s: float = 600.0) -> dict:
    """Runs cg-sim against config_path with cwd=run_dir.

    cwd is set to run_dir (not the repo root) so cg-sim's fixed relative log
    path (logs/CGSim.log) lands inside this run's own directory instead of a
    single shared location that a later run could clobber.
    """
    if not paths.CG_SIM_BINARY.exists():
        raise SimulationError(f"cg-sim binary not found at {paths.CG_SIM_BINARY}; build it first")
    if not paths.PLUGIN_SO.exists():
        raise SimulationError(f"plugin not found at {paths.PLUGIN_SO}; build policy_search/plugin first")

    start = time.time()
    proc = subprocess.run(
        [str(paths.CG_SIM_BINARY), "-c", str(config_path)],
        cwd=run_dir,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    wallclock_s = time.time() - start

    (run_dir / "stdout.log").write_text(proc.stdout)
    (run_dir / "stderr.log").write_text(proc.stderr)

    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.splitlines()[-40:])
        raise SimulationError(f"cg-sim exited with code {proc.returncode} for {config_path}:\n{tail}")

    return {"returncode": proc.returncode, "wallclock_s": wallclock_s}
