"""One-time prep of the larger real PanDA trace (data/all_jobs.csv, 2.35M rows,
97 sites) into a workload-manager-compatible CSV, analogous to config/job_data.csv
but ~56x larger.

One filter is applied, documented in policy_search/README.md:

jobstatus == "finished" only. CGSim has no model for a job that partially
executes then fails/is cancelled -- a job either runs its declared work to
completion or is marked FAILED by dispatch/retry-exhaustion logic before ever
running. Replaying a historical "failed" row's observed cpuconsumptiontime as
if it were a successful completion would misrepresent it. (finished=1,690,890
/ closed=407,236 / failed=227,486 / cancelled=26,780 in the raw 2,352,392-row
export.)

computingsite is restricted only to sites that exist in config/site_info.json
at all (79 of the 97 sites present in finished jobs; the other 18, e.g. every
ANALY_* queue, simply have no CGSim site definition to simulate). Missing
*network connectivity* between some of those 79 sites (site_conn_info.json is
a sparse graph, not a complete one -- confirmed: ~1700 total entries, vs
C(150,2)=11,175 needed for a complete graph over all of site_info.json's 150
sites) is NOT treated as a reason to exclude a site: real grids are not
always fully interconnected, and that's a legitimate constraint for the
dispatcher to respect (ScoringDispatcher::is_feasible() in
plugin/src/scoring_dispatcher.cpp only offers a candidate site when a route
actually exists to move the job's data there), not something this script
should curate around. An earlier version of this script instead restricted to
a fully-connected clique of sites -- that was the wrong fix (see git history
/ README): it reduced real site coverage for no reason once the dispatcher
itself was made to handle a missing route gracefully instead of crashing on
it (SimGrid's "Bad gateways for route from ... to ..." if a transfer is ever
actually attempted across a non-existent route).

creationtime is also converted from a "YYYY-MM-DD HH:MM:SS" string to a float
number of seconds since the filtered dataset's earliest job -- the numeric,
sortable format ScoringWorkloadManager and split.py expect (matching
config/job_data.csv's convention).
"""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "all_jobs.csv"
DEFAULT_OUTPUT = REPO_ROOT / "config" / "panda_jobs_large.csv"
SITE_INFO = REPO_ROOT / "config" / "site_info.json"
SITE_CONN_INFO = REPO_ROOT / "config" / "site_conn_info.json"

# Columns ScoringWorkloadManager actually reads (see plugin/src/scoring_workload_manager.cpp);
# every other column in the raw export is dropped rather than carried through unused.
OUTPUT_COLUMNS = [
    "pandaid", "creationtime", "jobstatus", "cpuconsumptiontime", "computingsite",
    "corecount", "ninputdatafiles", "inputfilebytes", "noutputdatafiles", "outputfilebytes",
]


def _connectivity_report(sites: set[str]) -> dict:
    """Informational only -- NOT used to filter sites (see module docstring).
    Reports how sparse the network data is among the sites actually kept, so
    that's visible without re-deriving it, without that sparsity being a
    reason to drop coverage.
    """
    conn = json.loads(SITE_CONN_INFO.read_text())
    conn_pairs = {frozenset(key.split(":")) for key in conn}
    pairs = list(itertools.combinations(sorted(sites), 2))
    present = sum(1 for a, b in pairs if frozenset((a, b)) in conn_pairs)
    return {
        "pairs_total": len(pairs),
        "pairs_with_a_direct_route": present,
        "pairs_missing_a_direct_route": len(pairs) - present,
    }


def prepare(input_csv: Path, output_csv: Path) -> dict:
    df = pd.read_csv(input_csv, index_col=0)
    n_raw = len(df)

    finished = df[df["jobstatus"] == "finished"].copy()
    n_finished = len(finished)

    known_sites = set(json.loads(SITE_INFO.read_text()).keys())
    sites = set(finished["computingsite"].unique()) & known_sites

    kept = finished[finished["computingsite"].isin(sites)].copy()

    creation_ts = pd.to_datetime(kept["creationtime"])
    origin = creation_ts.min()
    kept["creationtime"] = (creation_ts - origin).dt.total_seconds()
    kept = kept.sort_values("creationtime")

    for col in OUTPUT_COLUMNS:
        if col not in kept.columns:
            kept[col] = ""
    kept = kept[OUTPUT_COLUMNS]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    kept.to_csv(output_csv, index=False)

    manifest = {
        "source_csv": str(input_csv),
        "output_csv": str(output_csv),
        "raw_rows": n_raw,
        "finished_rows": n_finished,
        "sites": sorted(sites),
        "num_sites": len(sites),
        "connectivity": _connectivity_report(sites),
        "kept_rows": len(kept),
        "kept_fraction_of_finished": len(kept) / n_finished,
        "creationtime_origin_utc": str(origin),
        "site_row_counts": kept["computingsite"].value_counts().to_dict(),
    }
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    manifest = prepare(args.input, args.output)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
