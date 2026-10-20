from __future__ import annotations

from pathlib import Path

from policy_search.harness import db


class BottleneckDataUnavailable(RuntimeError):
    pass


def _summarize(values: list[float]) -> dict:
    if not values:
        return {"min": 0, "max": 0, "mean": 0}
    return {"min": min(values), "max": max(values), "mean": sum(values) / len(values)}


def _downsample(points: list[dict], max_points: int) -> list[dict]:
    n = len(points)
    if n <= max_points:
        return points
    step = n / max_points
    return [points[int(i * step)] for i in range(max_points)]


def inspect_bottlenecks(db_path: Path, max_sample_points_per_site: int = 12) -> dict:
    """Diagnostics from the optional debug EVENTS table, only present when
    the policy spec set "emit_debug_events": true.

    Returns bounded summary statistics (min/max/mean of queue depth,
    running jobs, available cores per site) plus a small downsampled
    time-series sample -- NOT the full raw EVENTS table. A run can produce
    thousands of events; returning them verbatim gets echoed back into an
    LLM agent's conversation history on every subsequent turn (each of our
    chat clients resends the full history each call), which has in practice
    blown a smaller-context model's context window mid-session. Callers
    that genuinely need the raw series can query the run's own results.db
    EVENTS table directly.
    """
    if not db.has_events_table(db_path):
        raise BottleneckDataUnavailable(
            f"{db_path} has no EVENTS table; re-run with \"emit_debug_events\": true in the policy spec"
        )

    events = db.read_events(db_path)
    if not events:
        raise BottleneckDataUnavailable(f"{db_path}'s EVENTS table is empty")

    per_site_points: dict[str, list[dict]] = {}
    for e in events:
        per_site_points.setdefault(e["SITE"], []).append({
            "timestamp": e["TIMESTAMP"],
            "state": e["STATE"],
            "pending_jobs": e["SITE_PENDING_JOBS"],
            "running_jobs": e["SITE_RUNNING_JOBS"],
            "finished_jobs": e["SITE_FINISHED_JOBS"],
            "failed_jobs": e["SITE_FAILED_JOBS"],
            "available_cores": e["AVAILABLE_SITE_CORES"],
        })

    summary_by_site = {}
    sample_series_by_site = {}
    for site, points in per_site_points.items():
        summary_by_site[site] = {
            "event_count": len(points),
            "pending_jobs": _summarize([p["pending_jobs"] for p in points]),
            "running_jobs": _summarize([p["running_jobs"] for p in points]),
            "available_cores": _summarize([p["available_cores"] for p in points]),
            "final_finished_jobs": points[-1]["finished_jobs"],
            "final_failed_jobs": points[-1]["failed_jobs"],
        }
        sample_series_by_site[site] = _downsample(points, max_sample_points_per_site)

    return {
        "event_count": len(events),
        "sites": sorted(per_site_points.keys()),
        "summary_by_site": summary_by_site,
        "sample_series_by_site": sample_series_by_site,
        "note": (
            f"summary_by_site covers all {len(events)} events; sample_series_by_site is downsampled to "
            f"at most {max_sample_points_per_site} points per site for readability, not the full series."
        ),
    }
