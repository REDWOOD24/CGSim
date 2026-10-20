from __future__ import annotations

import csv
import json

from policy_search.harness import paths

DEFAULT_CAVEATS = [
    "All 15,000 source jobs have jobstatus=finished -- there is no historical failure "
    "signal in this trace. Any policy's observed failure_rate reflects only failures "
    "this simulator's own dispatch/feasibility logic manufactures (e.g. an overly "
    "strict storage_margin_bytes), not anything learned from real ATLAS failures.",
    "creationtime is not uniformly dense: a sparse trickle of jobs precedes a dense "
    "~35-day block holding ~90% of all rows. A policy tuned only on the dense region "
    "may see very few examples from the sparse region.",
    "Each job's entire input dataset is placed at exactly one site (its historical "
    "computingsite). The 'locality_first' baseline is therefore mathematically close "
    "to historical_replay by construction: no other site can ever match its locality.",
]

LARGE_CAVEATS = [
    "Sourced from a real 2.35M-row PanDA export (data/all_jobs.csv, Jan-Jun 2024) filtered "
    "to jobstatus=finished (1,690,890 of 2,352,392 raw rows -- the rest are "
    "closed/failed/cancelled, excluded because CGSim has no model for a job that partially "
    "executes then doesn't complete) and to the 79 of 97 finished-job sites that exist in "
    "config/site_info.json at all (1,533,243 of 1,690,890 rows, 90.7%).",
    "Sites are NOT fully pairwise interconnected -- only 447 of the 3,081 possible pairs "
    "(14.5%) among these 79 sites have a measured direct route in site_conn_info.json. This "
    "is NOT a bug or a reason to distrust the data: real grids are not fully interconnected "
    "either. ScoringDispatcher::is_feasible() only offers a candidate site when a route "
    "genuinely exists to move a job's input data there, so a policy will never be offered a "
    "site it can't actually reach -- it will simply never see certain sites as candidates for "
    "jobs whose data lives somewhere disconnected from them. Replay mode is unaffected (it "
    "only ever uses a job's own historical site).",
    "Site volume is long-tailed: BNL alone is ~15% of kept rows; many included sites have a "
    "few hundred to a few thousand. A policy that looks good in aggregate may be dominated by "
    "behavior at the largest handful of sites.",
    "corecount is no longer always 1 -- this trace has real multi-core jobs. Verify "
    "site_info.json's CPU clusters actually have hosts with enough cores per unit to run the "
    "largest jobs, or they will never find a feasible site and will retry indefinitely.",
    "Unlike the default dataset, real core-capacity-driven failures CAN occur here even under "
    "historical_replay (observed directly: 87 failures in a 20,000-job replay sample) -- some "
    "small sites' capacity is genuinely exhausted at times in this trace. A policy's "
    "failure_rate is therefore meaningful here, not just a feasibility-margin artifact, though "
    "it still only reflects CGSim's own retry/feasibility logic, not real historical failure "
    "causes (jobstatus=failed/cancelled rows were filtered out before this split existed).",
]


def describe_workload(split: str, dataset: str = "default") -> dict:
    csv_path = paths.split_csv_path(split, dataset)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Split '{split}' of dataset '{dataset}' not found at {csv_path}; generate it first "
            "(policy_search/data/split.py, or prepare_all_jobs.py + split.py for 'large')"
        )

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        rows = list(reader)

    manifest = {}
    manifest_path = paths.split_manifest_path(dataset)
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text()).get("splits", {}).get(split, {})

    return {
        "dataset": dataset,
        "split": split,
        "row_count": len(rows),
        "columns": columns,
        "site_counts": manifest.get("site_counts"),
        "t_min": manifest.get("t_min"),
        "t_max": manifest.get("t_max"),
        "caveats": LARGE_CAVEATS if dataset == "large" else DEFAULT_CAVEATS,
    }
