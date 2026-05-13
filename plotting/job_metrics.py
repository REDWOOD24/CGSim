"""Per-configuration job CSV metrics (creation / start / end times) for plotting."""

from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

_DT_FORMATS = ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S")


def _parse_dt(value: str) -> datetime | None:
    s = (value or "").strip()
    if not s:
        return None
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def jobs_csv_path_for_config_dir(config_dir: Path) -> Path | None:
    """Resolve ``Custom_Parameters.jobs_file`` from ``config_dir/toy_config.json``."""
    cfg_path = config_dir / "toy_config.json"
    if not cfg_path.is_file():
        return None
    try:
        with cfg_path.open(encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    cp = cfg.get("Custom_Parameters")
    if not isinstance(cp, dict):
        return None
    raw = cp.get("jobs_file")
    if not isinstance(raw, str) or not raw.strip():
        return None
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (config_dir / p).resolve()
    else:
        p = p.resolve()
    return p if p.is_file() else None


@dataclass(frozen=True)
class SeriesStats:
    """Per-job sample statistics for one derived series (seconds)."""

    n: int
    min_s: float
    max_s: float
    mean_s: float
    median_s: float
    stdev_s: float | None  # sample stdev; ``None`` if ``n < 2``


def load_e2e_queue_series(jobs_csv: Path) -> tuple[list[float], list[float]] | None:
    """
    Load per-job end-to-end (``end - creation``) and queue (``start - creation``) in seconds.

    Returns ``None`` if the file is missing, headers are wrong, or there are no valid rows.
    """
    if not jobs_csv.is_file():
        return None
    e2e: list[float] = []
    queue: list[float] = []
    try:
        with jobs_csv.open(newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return None
            lower = {h.lower(): h for h in reader.fieldnames if h}
            for c_need in ("creationtime", "starttime", "endtime"):
                if c_need not in lower:
                    return None
            c_col = lower["creationtime"]
            s_col = lower["starttime"]
            e_col = lower["endtime"]
            for row in reader:
                if not row:
                    continue
                t0 = _parse_dt(str(row.get(c_col, "")))
                t1 = _parse_dt(str(row.get(s_col, "")))
                t2 = _parse_dt(str(row.get(e_col, "")))
                if t0 is None or t1 is None or t2 is None:
                    continue
                q_sec = (t1 - t0).total_seconds()
                life_sec = (t2 - t0).total_seconds()
                if math.isfinite(q_sec) and math.isfinite(life_sec):
                    queue.append(q_sec)
                    e2e.append(life_sec)
    except OSError:
        return None

    if not e2e:
        return None
    return e2e, queue


def series_stats(values: list[float]) -> SeriesStats:
    n = len(values)
    mn = min(values)
    mx = max(values)
    mean = statistics.fmean(values)
    med = float(statistics.median(values))
    if n < 2:
        sd: float | None = None
    else:
        sd = float(statistics.stdev(values))
    return SeriesStats(n=n, min_s=mn, max_s=mx, mean_s=mean, median_s=med, stdev_s=sd)


def job_timing_stats_for_csv(
    jobs_csv: Path,
) -> tuple[SeriesStats | None, SeriesStats | None]:
    """``(end-to-end stats, queue stats)`` from one jobs CSV; both ``None`` if unusable."""
    data = load_e2e_queue_series(jobs_csv)
    if data is None:
        return None, None
    e2e, queue = data
    return series_stats(e2e), series_stats(queue)


def mean_job_lifetime_and_queue_seconds(
    jobs_csv: Path,
) -> tuple[float | None, float | None]:
    """
    From a jobs CSV with ``creationtime``, ``starttime``, ``endtime`` columns:

    - Mean ``endtime - creationtime`` (seconds), all rows with valid times.
    - Mean ``starttime - creationtime`` (seconds), queuing interval.

    Returns ``(None, None)`` if the file is missing or no valid rows.
    """
    data = load_e2e_queue_series(jobs_csv)
    if data is None:
        return None, None
    e2e, queue = data
    return statistics.fmean(e2e), statistics.fmean(queue)


def job_timing_stats_bundle_for_config_dir(
    config_dir: Path,
) -> tuple[Path | None, SeriesStats | None, SeriesStats | None]:
    """
    Resolve ``jobs_file`` and return ``(csv_path, e2e_stats, queue_stats)``.

    Stats are from the **per-job** distributions in that CSV (not across configurations).
    """
    jp = jobs_csv_path_for_config_dir(config_dir)
    if jp is None:
        return None, None, None
    e2e_st, q_st = job_timing_stats_for_csv(jp)
    return jp, e2e_st, q_st


def mean_job_times_for_config_dir(
    config_dir: Path,
) -> tuple[float | None, float | None]:
    """``(mean end-to-end seconds, mean queue seconds)`` using this run's ``jobs_file``."""
    _jp, e2e_st, q_st = job_timing_stats_bundle_for_config_dir(config_dir)
    if e2e_st is None or q_st is None:
        return None, None
    return e2e_st.mean_s, q_st.mean_s


def format_series_stats(label: str, st: SeriesStats) -> str:
    sd = "n/a" if st.stdev_s is None else f"{st.stdev_s:.4g}"
    return (
        f"  {label}: n={st.n}  min={st.min_s:.4g}s  max={st.max_s:.4g}s  "
        f"mean={st.mean_s:.4g}s  median={st.median_s:.4g}s  stdev={sd}s"
    )


def format_cross_config_timing_report(
    run_root: Path,
    entries: list[tuple[str, Path | None, SeriesStats | None, SeriesStats | None]],
) -> str:
    """
    Human-readable report: per-configuration per-job stats, then aggregate stats on
    the per-configuration means used for the overlay lines.
    """
    lines: list[str] = [
        "Cross-configuration job timing report",
        f"run_root: {run_root.resolve()}",
        "",
    ]

    valid = [(n, jp, e, q) for n, jp, e, q in entries if jp is not None and e is not None and q is not None]

    for name, jp, e_st, q_st in entries:
        lines.append(f"=== {name} ===")
        if jp is None:
            lines.append("  (no jobs_file or missing toy_config.json)")
            lines.append("")
            continue
        lines.append(f"  jobs_file: {jp}")
        if e_st is None or q_st is None:
            lines.append("  (could not parse job times from CSV)")
            lines.append("")
            continue
        lines.append(format_series_stats("End-to-end (end - creation), per job", e_st))
        lines.append(format_series_stats("Queue (start - creation), per job", q_st))
        lines.append(
            f"  Plotted overlay (per-job mean for this config): "
            f"e2e_mean={e_st.mean_s:.6g}s  queue_mean={q_st.mean_s:.6g}s"
        )
        lines.append("")

    lines.append("--- Across configurations: stats on per-config overlay means (one value per bar column) ---")
    if not valid:
        lines.append("  (no timing data)")
    else:
        e2e_means = [e.mean_s for _n, _jp, e, _q in valid]
        queue_means = [q.mean_s for _n, _jp, _e, q in valid]
        lines.append(format_series_stats("End-to-end mean (across configs)", series_stats(e2e_means)))
        lines.append(format_series_stats("Queue mean (across configs)", series_stats(queue_means)))
        ue = {round(m, 9) for m in e2e_means}
        uq = {round(m, 9) for m in queue_means}
        if len(ue) == 1 and len(uq) == 1:
            lines.append(
                "Note: every configuration has the same mean end-to-end and the same mean queue time. "
                "That happens when all runs use the same jobs CSV with identical timestamps per job — "
                "the overlay lines are flat and not visually separable by configuration."
            )
        elif len(ue) == 1:
            lines.append(
                "Note: mean end-to-end is identical for every configuration (flat red/orange line)."
            )
        elif len(uq) == 1:
            lines.append("Note: mean queue time is identical for every configuration (flat green line).")
        lines.append("")

    lines.append("--- jobs_file path → how many configurations reference it ---")
    if not valid:
        lines.append("  (none)")
    else:
        by_csv: dict[Path, list[str]] = defaultdict(list)
        for n, jp, _e, _q in valid:
            by_csv[jp].append(n)
        for p in sorted(by_csv, key=lambda x: str(x)):
            names = by_csv[p]
            lines.append(f"  {p}")
            lines.append(f"    count={len(names)}")
    return "\n".join(lines) + "\n"
