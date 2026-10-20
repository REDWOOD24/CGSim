# CGSim Policy-Search Harness (Milestone 1)

Infrastructure for LLM-agent-driven scheduling-policy discovery on top of
CGSim, evaluated against a chronologically-split real ATLAS PanDA job trace
(`config/job_data.csv`). See `/home/ssrivatsa/.claude/plans/cheerful-mixing-badger.md`
for the design rationale.

## Layout

- `plugin/` — `ScoringPolicyPlugin`, a single C++ CGSim plugin (built once)
  that reads a JSON policy spec at startup and scores candidate sites for
  every job. All baselines and future agent-authored policies are JSON
  files read by this one `.so` — no recompilation per candidate. Supports
  both plain single-strategy specs and **adaptive, multi-phase** specs that
  genuinely change strategy mid-simulation based on live grid state (see
  "Adaptive policies" below).
- `data/split.py` — chronological calibration/dev/test split of
  `config/job_data.csv`. Regenerate with `python3 policy_search/data/split.py`.
- `policies/*.json` — policy specs (`historical_replay`, `locality_first`,
  `least_loaded`, `earliest_predicted_completion`, `adaptive_locality_then_load`,
  plus agent-authored ones).
- `harness/` — pure-stdlib Python: config generation, cg-sim subprocess
  orchestration, SQLite result parsing, metrics, paired comparison, and a
  SQLite experiment ledger (`experiments/experiments.db`). The grid's site
  list is never hardcoded: `harness/paths.sites_in_split()` derives
  `Limited_Sites` from whichever `computingsite` values actually appear in
  the split being run, so adding sites to the trace/config needs no code
  change.
- `mcp_server/server.py` — MCP tool server wrapping the harness
  (`describe_workload`, `register_policy`, `validate_policy`,
  `run_simulation`, `compare_runs`, `inspect_bottlenecks`,
  `record_experiment`). Requires Python >= 3.10; run it from the
  `policy-search` conda env (`conda create -n policy-search python=3.11 && conda run -n policy-search pip install -r policy_search/requirements.txt`).
  The harness package itself is stdlib-only and also runs under the repo's
  default `python3` (3.9).
- `agent/` — the search loop itself: a provider-agnostic chat+tool-use
  client (`llm_clients.py`, supports Anthropic's API and any
  OpenAI-compatible endpoint, including self-hosted/open-weight models),
  the system prompt (`prompts.py`), and the driver (`loop.py`) that connects
  to the MCP server as a client and runs an iterative propose/register/run/
  compare/reflect cycle.

## Datasets

Two datasets, selected via `dataset="default"` (default) or `dataset="large"`
on `describe_workload`/`run_simulation`/`validate_policy` (or `--dataset` on
`agent/loop.py`, which passes it to the MCP server at launch so every tool
call in that session defaults to it without the agent needing to repeat it):

| | `default` | `large` |
|---|---|---|
| Rows | 15,000 | 1,533,243 |
| Sites | 3 (BNL, AGLT2, MWT2) | 79 (real; sparsely but not fully interconnected) |
| Source | `config/job_data.csv` | `config/panda_jobs_large.csv`, derived from a real `data/all_jobs.csv` PanDA export (2.35M rows, Jan-Jun 2024) by `policy_search/data/prepare_all_jobs.py` |
| Splits | `policy_search/data/splits/` | `policy_search/data/splits_large/` |
| Runtime | full `dev` split (3,000 jobs) in seconds | roughly linear in job count; use a bounded `num_jobs` (5,000-20,000+) while iterating, full splits for final comparisons |

`prepare_all_jobs.py` filters to `jobstatus=finished` only (excludes
closed/failed/cancelled — 28% of the 2.35M raw rows are failed jobs with
real wasted compute, which CGSim has no model for since a job here either
runs to completion or never starts) and to the 79 of 97 finished-job sites
that exist in `config/site_info.json` at all (the other 18, e.g. every
`ANALY_*` queue, have no CGSim site definition to simulate).

**It does *not* filter sites by network connectivity**, and getting there
took a wrong turn worth recording. `site_conn_info.json` is a sparse graph
over those 79 sites (only 447 of the 3,081 possible pairs — 14.5% — have a
measured direct route), and CGSim has no multi-hop routing: a transfer
attempted across a missing route crashes SimGrid (`Bad gateways for route
from ... to ...`). The first fix was to shrink the dataset to a fully
pairwise-connected subset (first a 31-site connected component, which
turned out to still be missing 18 of 465 needed pairs and crashed mid-search;
then a corrected 30-site clique with zero missing pairs). That shipped and
ran cleanly — but it was the **wrong** fix: real grids are not, and don't
need to be, fully interconnected, and discarding 45% of the real sites to
force a complete graph was curating the data to fit a limitation that
belonged in the code. The actual fix is in
`ScoringDispatcher::is_feasible()` (`plugin/src/scoring_dispatcher.cpp`):
a candidate site is only offered for a job if a route genuinely exists to
move that job's input data there (checked via
`ResourceManager::get_connection_bandwidth`, which throws for a missing
route, caught and treated as "infeasible," not propagated); replay mode is
unaffected since it only ever uses a job's own historical site. With that
in place, all 79 sites coexist safely regardless of pairwise connectivity,
and `prepare_all_jobs.py` reports connectivity density purely as
information, not as a filter.

**The headline result, verified on the corrected full 79-site dataset**:
`mct_balanced` (`estimated_completion_time` weight -1.0 +
`available_cores_fraction` weight +1.0 — MCT blended with a load-balancing
term, nothing more exotic) beats real historical PanDA placement
substantially and reproducibly. Found by an agent search (`gpt-oss-120b`,
confirmed via its own `inspect_bottlenecks` call), then independently
re-verified end-to-end on a separate, larger 50,000-job sample of `dev`:

| | `historical_replay` | `mct_balanced` |
|---|---|---|
| Mean turnaround | 109.0s | 28.0s (-74.3%) |
| p95 turnaround | 177.6s | 58.2s (-67.2%) |
| Failures | 333 | 0 |
| Jobs reassigned | 0% | 45.5% |

The failure elimination is the most important part: `historical_replay`'s
333 failures at this sample size are *real* core-capacity exhaustion (see
caveat above — this dataset is the first one where that can happen at all),
not a feasibility-margin artifact. `mct_balanced` avoids all of them simply
by never piling jobs onto an already-saturated site, which is exactly what
adding a load term to MCT is supposed to do (Shivaratri et al. 1992 +
Maheswaran et al. 1999 above — this is their combination, not a new idea).
Earlier, smaller-sample tests found **pure** MCT (`mct_pure`/`mct_simple`,
no load term) was inconsistent at this scale — sometimes losing to replay on
p95 — reinforcing that the load-balancing term is doing real work here, not
just adding noise. `mct_balanced` is now a shipped baseline
(`policy_search/policies/mct_balanced.json`); a locked-`test`-split
evaluation is still outstanding (see "Not yet built" below).

**A second, much larger data-quality bug was found and fixed while
onboarding `large`**: `config/site_info.json`'s 150 site entries turned out
to follow two incompatible JSON schemas for CPU cluster info — a "correct"
one (top-level `name` and `ram` keys) used by only 3 sites (AGLT2, BNL,
MWT2, i.e. exactly the sites the harness had ever actually exercised before
`large` existed), and a broken one (no `name`, `ram` nested inside a
`properties` list instead) used by the other **146 of 150 sites**. This
caused two bugs, one loud and one silent: `Platform::create_platform`
crashes (`Invalid Size Units`) reading the empty top-level `ram` before ever
reaching the correctly-nested value in `properties`; separately, any site
with 2+ CPU clusters and no cluster `name` gets colliding host names
(`SITE_cpu_-0`, `SITE_cpu_-1`, ...) that silently overwrite each other in
SimGrid's host map, with no error, quietly losing most of that site's
capacity regardless of the `ram` crash. Fixed by normalizing the JSON data
(hoisting `ram` out of `properties`, synthesizing unique cluster names)
rather than making the C++ loader tolerate two schemas — same reasoning as
the BNL/BNL-ATLAS fix below: a defensive parser change would have silently
tolerated both the crash-causing bug and the silent host-collision bug
across 146 sites, rather than fixing the data corruption itself. Original
preserved at `config/site_info.json.bak_before_schema_normalize`.

**A real, publication-relevant finding from `large`-scale testing**: the
shipped `earliest_predicted_completion` baseline (MCT +
`bandwidth_from_nearest_input_site` weight 0.2) — which beat
`historical_replay` by ~88% on the small `default` trace — is essentially
indistinguishable from real historical PanDA placement on `large` (mean
delta ≈ 0, 99% site agreement, confirmed twice independently at 20,000
jobs). But a **pure** MCT policy with no bandwidth term at all
(`estimated_completion_time` weight -1.0 only, no locality/bandwidth
tie-breaker) found by an agent search *does* win meaningfully: mean
turnaround 284.7 vs. replay's 320.1 (-11%), p95 799.8 vs. 1490.2 (-46%),
99.8% of jobs reassigned. The gap between these two near-identical-looking
policies is the finding: a small, "sensible-looking" bandwidth/locality
tie-breaker term is enough to erase MCT's entire advantage at this scale —
unlike the small 3-site trace, where the same term barely mattered. Real
historical PanDA placement across 30 real sites and a real workload mix is
a much stronger baseline than the toy trace made it look, and both getting
a real win *and* understanding which feature combinations silently cancel
each other out required testing at this scale, not just the small one.

## Building the plugin

```
cmake -S policy_search/plugin -B policy_search/plugin/build -Dremote=ON -DBOOST_ROOT=/home/ssrivatsa/local/boost
cmake --build policy_search/plugin/build
```

## Running

```
python3 policy_search/data/split.py                       # writes data/splits/{calib,dev,test}.csv
python3 -c "from policy_search.harness import orchestrate; print(orchestrate.run_policy('least_loaded', 'dev'))"
conda run -n policy-search python policy_search/tests/test_mcp_tools.py   # end-to-end MCP smoke test
conda run -n policy-search python -m policy_search.agent.loop --provider anthropic --model claude-sonnet-5 --max-iterations 12
```

## Literature grounding

Every feature and baseline maps to a specific, named heuristic family from
the scheduling literature, not an ad hoc invention:

| Feature / baseline | Literature | What it approximates |
|---|---|---|
| `available_cores_fraction`, `available_cores_abs` (→ `least_loaded`) | Shivaratri, Krueger & Singhal 1992, "Load Distributing for Locally Distributed Systems", *IEEE Computer* 25(12):33-44 | A load index used to prefer less-loaded receivers (their survey's central primitive; they discuss queue-length and utilization indices specifically) |
| `input_locality_bytes_fraction` (→ `locality_first`) | Zaharia, Borthakur, Sen Sarma, Elmeleegy, Shenker & Stoica 2010, "Delay Scheduling: A Simple Technique for Achieving Locality and Fairness in Cluster Scheduling", *EuroSys 2010*, pp.265-278 | Preferring the site already holding a task's input data |
| `execution_time_estimate` | Braun, Siegel, Beck, Bölöni, Maheswaran, Reuther, Robertson, Theys, Yao, Hensgen & Freund 2001, "A Comparison of Eleven Static Heuristics for Mapping a Class of Independent Tasks onto Heterogeneous Distributed Computing Systems", *J. Parallel Distrib. Comput.* 61(6):810-837 | Minimum Execution Time (MET): job size / site compute rate |
| `estimated_completion_time` (→ `earliest_predicted_completion`) | Maheswaran, Ali, Siegel, Hensgen & Freund 1999, "Dynamic Matching and Scheduling of a Class of Independent Tasks onto Heterogeneous Computing Systems", *HCW'99*, pp.30-44 (also compared in Braun et al. 2001 above) | Minimum Completion Time (MCT): execution time + site ready time |
| `historical_replay` | Maeno et al. 2014, "Evolution of the ATLAS PanDA workload management system for exascale computational science", *J. Phys.: Conf. Ser.* 513(3):032062 (CHEP2013) | The real PanDA/JEDI brokerage this trace was produced by |
| adaptive multi-phase policies (`adaptive_locality_then_load`) | The "adaptive algorithms" category in Shivaratri et al. 1992 above | A policy that changes its own dispatch rule based on observed system state, rather than running one fixed rule for the whole system lifetime |

`estimated_completion_time`'s formal MCT ready-time term (sum of remaining
runtime of everything already queued at a site) isn't observable through
CGSim's `Site` API (only queue *depth* is exposed, not queued jobs'
remaining runtimes); it's approximated as
`pending_queue_depth(site) * this_job's_own_execution_time_estimate`. This
is a documented deviation from the textbook definition, not the definition
itself — see the code comment in `policy_spec.cpp`.

## Adaptive policies (mid-simulation hot-reload)

A policy spec can be a single flat strategy (as above) or a **sequence of
phases**, each a full strategy, where later phases activate automatically
based on live grid state observed *during* the same simulation run — no
restart, no external intervention:

```jsonc
{
  "policy_name": "adaptive_locality_then_load",
  "check_interval_seconds": 500,
  "phases": [
    { "phase_name": "locality_first",
      "spec": { "features": [{"name": "input_locality_bytes_fraction", "weight": 5.0}, ...] } },
    { "phase_name": "load_balance_fallback",
      "activate_when": {"feature": "available_cores_fraction", "aggregate": "min_over_sites", "op": "<", "value": 0.99999},
      "spec": { "features": [{"name": "available_cores_fraction", "weight": 1.0}, ...] } }
  ]
}
```

Mechanics: `check_interval_seconds` schedules a repeating callback through
CGSim's own timer-driven scheduler (`CGSim::GlobalManagers::PolicyManager`,
which already exists for exactly this purpose — no new C++ facility was
built for it). At each tick, every phase after phase 0 re-evaluates its
`activate_when` condition (a state-only feature — `available_cores_fraction`,
`available_cores_abs`, `remaining_storage_fraction`, `pending_queue_depth`,
`running_jobs`, or `gflops` — aggregated across every site in the grid and
compared to a threshold); the *last-declared* phase whose condition holds
wins, so a later phase can override an earlier one whose condition is also
true, and the system falls back toward phase 0 automatically once no later
condition holds. Every job's `JOB_RESULTS` row records `ACTIVE_PHASE`, the
phase that actually made *its* dispatch decision (not whatever phase is
active when it later finishes), and every switch is logged to a
`POLICY_SWITCHES` table with the simulated timestamp.

**Threshold sensitivity, learned the hard way**: sites in this grid have
36,000–97,500 cores each, so `available_cores_fraction` barely moves below
1.0 even with several jobs running (a single job nudges it by ~0.00003).
An `activate_when` threshold like `< 0.98` will effectively never fire;
`< 0.99999` (or comparing `available_cores_abs`/`pending_queue_depth`
directly) is what actually detects "something is running." Pick threshold
scales relative to a site's actual capacity, not round numbers.

## Modeling caveats (read before trusting a result)

1. **No historical failure signal.** Every row in `config/job_data.csv` has
   `jobstatus=finished`. Any non-zero `failure_rate` a policy produces comes
   entirely from this simulator's own feasibility/retry logic (e.g. an
   overly strict `storage_margin_bytes`), not from anything observed in
   real ATLAS operations.

2. **`locality_first` is close to `historical_replay` by construction.**
   Each job's entire input dataset is placed at exactly one site — its
   historical `computingsite` (see `ScoringWorkloadManager`: input files are
   always created at `historical_site`, regardless of dispatch mode, since
   the data really was staged there independent of which site later
   executes the job). That means `input_locality_bytes_fraction` is `1.0`
   at the historical site and `0.0` everywhere else for every job, so a
   policy dominated by that feature can never prefer another site. Any
   meaningful improvement over replay has to come from load- or
   queue-aware signals, not locality alone, in this dataset.

3. **`execution_time_estimate`/`estimated_completion_time` are estimates
   backed out of the job's own historical observation, not a second ground
   truth.** `ScoringDispatcher::set_flops()` computes
   `flops = site_GFLOPS * cpu_consumption_time * cores`; a site-independent
   "reference work" figure is recovered as
   `historical_site_GFLOPS * cpu_consumption_time`, then divided by a
   *candidate* site's GFLOPS to estimate how long the job would take there.
   The real number is whatever ends up in `JOB_RESULTS.COMPLETION_TIME`;
   these features are decision-time predictions, and empirically (see
   Milestone-1 verification below) a policy built on them produces the
   largest turnaround improvement of any baseline shipped here.

4. **Ties are common in this trace, and tie-breaking matters.** Grid CPU
   utilization is near 0% throughout (3,000 jobs spread over ~35 dense
   days across sites with tens of thousands of cores each), so features like
   `pending_queue_depth` are frequently tied at 0 across every candidate.
   `ScoringDispatcher::pick_best_site()` breaks near-ties (within `1e-9`)
   by preferring the job's own historical site, specifically so an
   indifferent policy doesn't reassign jobs "for no reason" based on
   candidate-list alphabetical order. Without this, `least_loaded`-style
   policies moved ~72% of jobs essentially at random (alphabetically-first
   feasible site) whenever load didn't actually differ.

5. **`bandwidth_from_nearest_input_site` at the job's current site is
   treated as effectively infinite bandwidth** (no network hop needed), so
   after min-max normalization across candidates it becomes a near-binary
   "local vs. not" signal rather than a continuous one. A location with no
   route at all to a candidate site (see caveat 6) is skipped rather than
   contributing a value.

6. **Known data gap (fixed): `config/site_conn_info.json` used
   `"BNL-ATLAS"` as the network-topology name while `site_info.json`,
   `Limited_Sites`, and the job trace's `computingsite` column all use
   `"BNL"`.** This left BNL with zero interzonal routes and crashed SimGrid
   the first time any policy tried to move a job's data to/from BNL
   (`historical_replay` never surfaces this, since it never needs a
   cross-site transfer). Fixed by renaming the 61 `"BNL-ATLAS:*"` /
   `"*:BNL-ATLAS"` keys to `"BNL"` in place; the original file is preserved
   at `config/site_conn_info.json.bak_before_bnl_rename`. If more sites are
   added later, check for similar naming mismatches before trusting
   cross-site dispatch results.

7. **Determinism.** No RNG is used anywhere in the dispatch path, so a
   simulation is fully determined by (policy spec, split, config) — no
   need to average over multiple seeds when comparing two runs.

## Milestone-1 verification (passed)

- All shipped baselines run to completion on the full `dev` split (3,000
  jobs) with zero crashes and zero unfinished jobs.
- `historical_replay`'s `ASSIGNED_SITE` matches `dev.csv`'s `computingsite`
  for all 3,000 jobs (ground-truth correctness check on the replay path).
- `least_loaded` vs. `historical_replay`: mean turnaround drops from
  ~4,575 to ~1,916 (simulated seconds), 64% of jobs reassigned.
  `earliest_predicted_completion` (MCT-style) drops it further, to ~627 on
  a 300-job sample — the largest improvement of any shipped baseline,
  consistent with MCT's literature reputation as a strong, cheap heuristic
  (Braun et al. 2001 found plain Min-Min competitive with much more
  expensive search methods).
- The adaptive `adaptive_locality_then_load` policy genuinely switches
  phases mid-run based on live state (confirmed via `POLICY_SWITCHES` and
  per-job `ACTIVE_PHASE`), landing close to `least_loaded`'s improvement
  once it falls back to load-balancing.
- `inspect_bottlenecks` returns non-empty per-site time series when a
  policy sets `"emit_debug_events": true`.
- The full MCP tool surface (all 7 tools) was driven end-to-end through a
  real stdio subprocess using the `mcp` SDK's own client
  (`policy_search/tests/test_mcp_tools.py`), including the guard that
  refuses `split="test"` without `allow_test_split=True`.

## Not yet built (future milestones)

- The second policy-representation track (LLM-generated raw C++,
  recompiled per candidate) — explicitly deferred per the approved plan.
- Baseline/finalist evaluation on the locked `test` split.
- Bayesian/random search comparison to establish whether agent-guided
  search beats simpler search under an equal simulation budget.
