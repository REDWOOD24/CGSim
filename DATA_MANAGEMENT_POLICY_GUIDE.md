## Data Management Policy Implementation Guide

This guide documents the **current data management subsystem** in CGSim:

- How proactive and reactive policies are wired into the simulation.
- What configuration parameters exist.
- How decisions are made (when and why transfers are triggered).
- How to extend the framework with your own policy.

The goal is that **this single file** is enough to understand and extend the data‑management code, without reading the C++ first.

---

## High‑level architecture

- **Facade**: `CGSim::DataManagementPolicy`
  - Header: `include/data_management_policy.h`
  - Implementation: `data_management_plugins/data_management_policy.cpp`
  - Exposes a small set of static APIs used by the core:
    - `configure(const nlohmann::json& cfg)`
    - `onSimulationStart()`
    - `onSimulationEnd()`
    - `onFileRequest(const FileRequestContext&) -> FileRequestDecision`
    - `onTimerTick(double current_time)` (currently not used; timers are internal)
    - `isEnabled()`, `getExecutionCount()`

- **Plugin interface**: `CGSim::DataManagementPlugin`
  - Header: `include/DataManagementPlugin.h`
  - Defines the abstract interface that any data‑management plugin must implement:
    - `configure(const nlohmann::json&)`
    - `onSimulationStart()`
    - `onSimulationEnd()`
    - `onFileRequest(const FileRequestContext&)`
    - `onTimerTick(double current_time)`

- **Concrete policy implementation**:
  - `ProactiveDataManagementPlugin` in `data_management_plugins/data_management_policy.cpp`.
  - Implements **both**:
    - Proactive, timer‑driven balancing logic.
    - Reactive, per‑request source/transfer‑mode decisions.

- **Core integration points**:
  - `src/main.cpp`:
    - Reads JSON config and calls `CGSim::DataManagementPolicy::configure(j["Data_Management_Policy"])`.
  - `util/job_executor.cpp`:
    - `attach_callbacks()`:
      - On simulation start: calls `DataManagementPolicy::onSimulationStart()` if enabled.
      - On simulation end: logs final statistics via `getExecutionCount()`.
    - `execute_job(Job* j)`:
      - For each input file:
        - Builds a `FileRequestContext` (file name + all current replicas from `FileManager`).
        - Calls `DataManagementPolicy::onFileRequest(ctx)` to get:
          - `chosen_src_site`.
          - `decision_mode` (`COPY` or `MOVE`).
        - Uses that to choose the source site and `FileTransferMode` when scheduling `comm_file_async`.

Internally, `DataManagementPolicy` holds a single `std::unique_ptr<DataManagementPlugin>` and delegates all work to it. You can swap in a completely different plugin implementation by changing only `data_management_policy.cpp`.

---

## Configuration: JSON schema

In your simulation config (for example `config-files/toy_config.json`), the data‑management block looks like:

```json
"Data_Management_Policy": {
  "enabled": true,
  "proactive": {
    "enabled": true,
    "interval": 10,
    "data_transfer_mode": "MOVE",
    "random_seed": 1337,
    "transfer_template": [
      0,
      [
        "storage_rebalance",
        "network_aware_rebalance",
        "hotset_replication",
        "custom_policy_agent"
      ]
    ],
    "template_params": {
      "storage_rebalance": { "..." : "..." },
      "network_aware_rebalance": { "..." : "..." },
      "hotset_replication": { "..." : "..." },
      "custom_policy_agent": { "..." : "..." }
    }
  },
  "reactive": {
    "enabled": true,
    "prefer_local_replica": true,
    "remote_source_template": [
      0,
      [
        "first_replica",
        "least_utilized_source",
        "most_utilized_source",
        "random_replica",
        "hash_filename_job",
        "custom_policy_agent"
      ]
    ],
    "random_seed": 1337,
    "copy_to_move_threshold": 3
  }
}
```

### Top level

- **`enabled`** (bool):
  - If `false`, no proactive or reactive logic runs; the core uses its default behavior (first file location, always `COPY`).

### `proactive` section

- **`enabled`** (bool):
  - Turn proactive balancing on or off.

- **`interval`** (double, simulation time units):
  - Period between consecutive policy executions.
  - First execution is scheduled at `current_time + interval` when the simulation starts.

- **`data_transfer_mode`** (`"COPY"` or `"MOVE"`):
  - How proactive transfers change the file layout:
    - `"COPY"`: replicate files (source keeps its copy).
    - `"MOVE"`: migrate files (source copy removed after successful transfer).

- **`random_seed`** (unsigned integer):
  - Seeds the proactive RNG (used for `file_pick` = `random_fit` inside storage / network templates).

- **`transfer_template`** (2-element array):
  - Format: `[selected_index, fixed_template_list]` (same pattern as reactive `remote_source_template`).
  - Fixed list (in order):
    - **`storage_rebalance`**: move data from sites with `util ≥ high_utilization_threshold` toward sites with `util ≤ low_utilization_threshold`, using `file_pick` and up to `max_transfers_per_tick` actions per tick (no-op if no valid pair/file).
    - **`network_aware_rebalance`**: same storage gating, plus require a SimGrid link `link_<src>:<dst>` (or reversed name), `link_load ≤ max_path_load`, and pick the best `(src, dst, file)` triple by `path_metric` (`estimated_transfer_time`, `link_load`, or `bandwidth_only`, each as `[index, [...]]` tuples or plain strings).
    - **`hotset_replication`**: file-centric heuristic using **replica prevalence** `replicas(file) / num_sites`; if prevalence `≥ hotness_threshold` and replicas `< target_replica_count`, copy from the **most utilized** replica host to the **least utilized** site missing the file that has space (`hotness_window` / `prediction_horizon` reserved for future telemetry; `candidate_destination_policy` reserved).
    - **`custom_policy_agent`**: not implemented; selecting it throws at the first proactive tick (same pattern as reactive stub).

- **`template_params`**:
  - Object with optional sub-objects named `storage_rebalance`, `network_aware_rebalance`, `hotset_replication`, `custom_policy_agent`; see `config-files/toy_config.json` for a full example.

Backward compatibility: if **`transfer_template` is absent**, the plugin behaves like **`storage_rebalance`** using legacy fields only:
- **`high_utilization_threshold`**: source-site trigger (no `low_utilization_threshold`; destinations are any site with globally lowest utilization among those that can accept a file).

### `reactive` section

- **`enabled`** (bool):
  - Turn reactive logic on or off.

- **`prefer_local_replica`** (bool):
  - If `true`, and a replica already exists on the job’s compute site, that local site is selected.
  - If `false`, source selection always uses `remote_source_template` across all replicas.

- **`copy_to_move_threshold`** (integer):
  - If the number of replicas of a file **exceeds** this value when a job requests it:
    - The reactive policy suggests `MOVE`.
  - Otherwise:
    - The reactive policy suggests `COPY`.

- **`remote_source_template`** (2-element array):
  - Format: `[selected_index, fixed_template_list]`
  - `selected_index` chooses one entry from the list.
  - The expected fixed template list is:
    - `"first_replica"`
    - `"least_utilized_source"`
    - `"most_utilized_source"`
    - `"random_replica"`
    - `"hash_filename_job"`
    - `"custom_policy_agent"`
  - Backward compatibility: a plain string value is still accepted by the current parser.

- **`random_seed`** (unsigned integer):
  - Seed used only when the selected template is `"random_replica"`.

- **`custom_policy_agent`**:
  - Current implementation is a stub.
  - If selected, it logs `"not implemented yet."` and stops execution.

---

## Reactive behavior: per‑request decisions

When a job needs an input file, `JOB_EXECUTOR::execute_job` does:

1. Build `FileRequestContext`:
   - `job`: pointer to the `Job`.
   - `filename`: file name (e.g. `"17"`).
   - `replicas`: vector of `ReplicaInfo { sitename, hostname, size }` for **all sites** where the file currently exists, as reported by `FileManager`.

2. Call:
   - `auto decision = CGSim::DataManagementPolicy::onFileRequest(ctx);`

3. Interpret `decision`:
   - `decision.chosen_site`:
     - If empty → use the core default (first file location).
     - If non‑empty → use this as `src_site`.
   - `decision.mode`:
     - `COPY` → use `FileTransferMode::COPY` in `comm_file_async`.
     - `MOVE` → use `FileTransferMode::MOVE` (after transfer, source file is removed).

### Reactive decision logic (current plugin)

Given the context `ctx`:

1. If `reactive_enabled_` is false:
   - Return an empty decision (core default).

2. Count **replicas**:
   - `replica_count = ctx.replicas.size()`.

3. Choose source site:
   - Let `job_site = ctx.job->comp_site`.
   - If `prefer_local_replica` is `true` and any replica has `replica.sitename == job_site`:
     - `decision.chosen_site = job_site` (serve locally).
   - Else if `ctx.replicas` is non‑empty:
     - `decision.chosen_site` is selected by `reactive.remote_source_template`:
       - `"first_replica"`: first replica in list.
       - `"least_utilized_source"`: minimum storage utilization site among replicas.
       - `"most_utilized_source"`: maximum storage utilization site among replicas.
       - `"random_replica"`: random replica (seeded by `reactive.random_seed`).
       - `"hash_filename_job"`: deterministic hash pick using `filename#jobid`.
       - `"custom_policy_agent"`: logs `"not implemented yet."` and throws runtime error.
   - Else:
     - Leave `chosen_site` empty (no known replicas; core’s default is used).

4. Choose transfer mode:
   - If `replica_count > copy_to_move_threshold_`:
     - `decision.mode = MOVE` (thin out overly replicated files).
   - Else:
     - `decision.mode = COPY`.

5. Log the decision:
   - Example log line:

```text
Reactive Data Management: job 2765833703 requesting file '15'; replicas=3, chosen_src_site='AGLT2_site_11',
dst_site='AGLT2_site_15', decision_mode=COPY [replica_sites=AGLT2_site_3,AGLT2_site_11,AGLT2_site_18]
```

This tells you:

- **`replicas=3`**: the file is on 3 sites.
- **`[replica_sites=...]`**: which sites.
- **`chosen_src_site`**: source site selected by the reactive policy.
- **`dst_site`**: job’s compute site (where the job runs).
- **`decision_mode`**: `MOVE` or `COPY`.

---

## Proactive behavior: timer‑driven balancing

The proactive plugin uses SimGrid **kernel timers** to run **`performDataManagementOperations(t)`** on every interval tick. Each tick selects a template via `transfer_template` and runs at most **`max_transfers_per_tick`** transfers (possibly zero).

### Scheduling (unchanged)

- At `onSimulationStart()`: schedules `current_time + interval` when enabled and `interval > 0`.
- Callback increments `execution_count_`, invokes `performDataManagementOperations`, then schedules the next tick.
- `onSimulationEnd()` removes the timer if active.

### Core dispatch (`performDataManagementOperations`)

Depending on **`transfer_template`**:

1. **`storage_rebalance`**: greedy search over `(src_site, dst_site)` with `src.util ≥ high` and `dst.util ≤ low`, using `file_pick` (`first_fit`, `largest_fit`, `smallest_fit`, or `random_fit` as `[index, list]` tuples or strings). Skips `(filename, src_site)` in `in_flight_transfers_`.
2. **`network_aware_rebalance`**: same storage filters, additionally requires named inter-site **`link_<a>:<b>`** (same convention as dispatcher output), rejects when `link_load > max_path_load`, then minimizes a lexicographic key derived from `path_metric`.
3. **`hotset_replication`**: prevalence-based copy of “hot” files (see **`template_params`** above).
4. **`custom_policy_agent`**: stub that logs and **`throw`s** (“not implemented yet”).

### Transfer mechanics (shared)

Transfers use communication hosts **`{site}_communication`**, detached `Comm::sendto_init()`, completion callback `FileManager::create` (+ `remove` when `MOVE`), and **`in_flight_transfers_`** book-keeping identical to older builds.

### Logging

Each initiation line includes the template tag in brackets, for example **`[storage_rebalance]`** or **`[network_aware_rebalance]`**.


### Combined reactive + proactive flow diagram

```mermaid
flowchart LR
  %% Layout hints
  classDef phase fill:#f5f5f5,stroke:#999,stroke-width:1px;
  classDef op fill:#e6f7ff,stroke:#1890ff,stroke-width:1px;
  classDef decision fill:#fff7e6,stroke:#fa8c16,stroke-width:1px;
  classDef log fill:#f9f0ff,stroke:#722ed1,stroke-width:1px;

  %% Columns (left to right): Config & lifecycle | Proactive | Reactive | Core transfers & end

  %% Config & lifecycle
  M[main(): read config<br/>+ Dispatcher plugin]:::phase --> MC[configure(Data_Management_Policy)<br/>(JSON)]:::op
  MC -->|enabled=false| M0[No data management<br/>core uses defaults]:::log
  MC -->|enabled=true| S[Simulation start]:::phase

  S --> SS[JOB_EXECUTOR::attach_callbacks()]:::op
  SS --> S1[on_simulation_start_cb<br/>dispatcher->onSimulationStart()]:::op
  S1 --> S2{DataManagementPolicy::isEnabled()?}:::decision
  S2 -- no --> R0[No data-management hooks]:::log
  S2 -- yes --> P0[DataManagementPolicy::onSimulationStart()]:::op

  %% Proactive column
  P0 --> P1{proactive_enabled<br/>&& interval>0?}:::decision
  P1 -- no --> PZ[No proactive timers]:::log
  P1 -- yes --> PT0[scheduleNext(t+interval)<br/>(kernel Timer)]:::op

  PT0 --> PT1[Timer fires at t]:::op
  PT1 --> PT2[execution_count++]:::op
  PT2 --> PT3[performDataManagementOperations(t)]:::op
  PT3 --> PT4{∃ src_site with<br/>util ≥ threshold and<br/>suitable dst_site/file?}:::decision
  PT4 -- no --> PT5[Skip this tick<br/>(no proactive transfer)]:::log
  PT4 -- yes --> PT7[Create detached Comm<br/>src_site → dst_site]:::op
  PT7 --> PT8[On completion:<br/>FileManager::create(dst)<br/>+ remove(src) if MOVE]:::op
  PT8 --> PT9[Log proactive completion]:::log
  PT5 --> PT6[scheduleNext(t+interval)]:::op
  PT9 --> PT6
  PT6 --> PT1

  %% Reactive column
  S --> J0[JOB_EXECUTOR::start_job_execution()]:::phase
  J0 --> J1[dispatcher->getWorkload()]:::op
  J1 --> J2[Jobs assigned to compute hosts/sites]:::op
  J2 --> JR[For each job input file]:::phase

  JR --> R1[Build FileRequestContext:<br/>filename + all replicas]:::op
  R1 --> R2{reactive_enabled?}:::decision
  R2 -- no --> R3[Use default source<br/>(first location), mode=COPY]:::op
  R2 -- yes --> R4[onFileRequest(ctx):<br/>pick chosen_src_site]:::op
  R4 --> R6[Compute replica_count]:::op
  R6 --> R7{replica_count ><br/>copy_to_move_threshold?}:::decision
  R7 -- yes --> R8[decision.mode = MOVE]:::op
  R7 -- no --> R9[decision.mode = COPY]:::op
  R8 --> R10
  R9 --> R10
  R10 --> R11[Log reactive decision:<br/>job, file, replicas,<br/>src_site, dst_site, mode,<br/>replica_sites=[...]]:::log

  %% Core transfers & end
  R3 --> C0
  R11 --> C0[Choose src_site + FileTransferMode<br/>for comm_file_async()]:::op
  C0 --> C1[Start Comm + IO + Exec<br/>(pending_activities)]:::op

  C1 --> SE[All jobs & activities done]:::phase
  SE --> ES[on_simulation_end_cb:<br/>dispatcher->onSimulationEnd()]:::op
  ES --> EP[DataManagementPolicy::onSimulationEnd()]:::op
  EP --> E0[Cancel proactive timers,<br/>log execution count]:::log
```

---

## Where simulation time advances and how policy hooks in

- **Simulation time** advances inside SimGrid’s internal engine (`sg4::Engine::run()`).
- CGSim hooks into it in two ways:
  - Through **SimGrid callbacks**:
    - `sg4::Engine::on_simulation_start_cb` / `on_simulation_end_cb` in `JOB_EXECUTOR::attach_callbacks()`.
    - Used to start/stop the data‑management plugin.
  - Through **kernel timers**:
    - `simgrid::kernel::timer::Timer::set` in `ProactiveDataManagementPlugin::scheduleNext`.
    - Used to run proactive logic at fixed intervals, independent of job execution.

The data‑management subsystem is therefore **job‑independent**: proactive timers can run even when no jobs are currently active, as long as the simulation is still alive.

---

## Extensibility: writing your own data‑management plugin

To define a new policy:

1. Implement a class deriving from `CGSim::DataManagementPlugin`:

   - File example: `data_management_plugins/my_policy/my_data_management_plugin.cpp`.

   - Implement:
     - `void configure(const nlohmann::json& cfg) override;`
     - `void onSimulationStart() override;`
     - `void onSimulationEnd() override;`
     - `FileRequestDecision onFileRequest(const FileRequestContext& ctx) override;`
     - `void onTimerTick(double current_time) override;` (optional if you manage timers internally).

2. Decide where to manage timers:

   - Easiest: follow the current `ProactiveDataManagementPlugin` pattern:
     - Use `simgrid::kernel::timer::Timer::set` and a private `scheduleNext` helper.
     - Store your own `Timer* current_timer_` and `execution_count_`.

3. Wire your plugin into the facade:

   - In `data_management_plugins/data_management_policy.cpp`, change:

   ```cpp
   if (!plugin_) {
       plugin_ = std::make_unique<ProactiveDataManagementPlugin>();
   }
   ```

   to construct your plugin instead:

   ```cpp
   if (!plugin_) {
       plugin_ = std::make_unique<MyDataManagementPlugin>();
   }
   ```

   or dispatch based on a config key in `cfg` (e.g. `"policy_type": "my_policy"`).

4. Extend the JSON schema:

   - Add any new fields needed by your policy under `"proactive"` and/or `"reactive"`.
   - Parse them in your `configure()` implementation.

5. Reuse utilities:

   - `CGSim::FileManager`:
     - `get_site_names()`, `get_site_capacity()`, `request_remaining_site_storage()`.
     - `get_files_on_site()`, `exists(file, site)`, `create()`, `remove()`, `request_file_size()`.
   - Logging:
     - Use `CG_SIM_LOG_INFO` / `CG_SIM_LOG_WARN` to make behavior visible in `atlas_grid_simulation*.log`.

With this structure, you can experiment with:

- Different proactive heuristics (e.g. access‑frequency based replication).
- Different reactive strategies (e.g. latency‑aware source selection).
- Hybrid policies that turn proactive/reactive on/off independently based on configuration.

All while keeping the core CGSim job‑execution and file‑management code unchanged. 

---

## How many policy configurations are possible?

Even with the current, relatively small schema, there is a large (and practically infinite) configuration space. It is useful to think about it in two layers:

- **Discrete choices** – on/off switches, COPY vs MOVE, strategy selection, integer thresholds.
- **Continuous choices** – timer intervals and utilization thresholds.

### Discrete configuration space

Ignoring the exact numeric values of thresholds/intervals and focusing only on structural choices:

- Top level:
  - `enabled` ∈ {true, false}

- Proactive:
  - `proactive.enabled` ∈ {true, false}
  - `data_transfer_mode` ∈ {COPY, MOVE}

- Reactive:
  - `reactive.enabled` ∈ {true, false}
  - `prefer_local_replica` ∈ {true, false}
  - `remote_source_template` index over 6 templates.
  - `copy_to_move_threshold` – an integer threshold in some finite range \[0, R] (R being a “max reasonable replica count”).

If we bound ourselves to:

- 4 booleans (`enabled`, `proactive.enabled`, `reactive.enabled`, `prefer_local_replica`) → \(2^4 = 16\) combinations.
- 2 values for `data_transfer_mode`.
- 6 template choices.
- T distinct values for the integer threshold.

then the number of structurally distinct policies is on the order of:

\[
N_\text{discrete} = 2^4 \times 2 \times 6 \times T = 192 \times T.
\]

For example, with T = 10 threshold options, you already have
\(192 × 10 = 1{,}920\) different “shapes” of policy, before considering timing/threshold parameters.

Qualitatively, these discrete switches let you control:

- Whether the system is **proactive only**, **reactive only**, **both**, or **disabled**.
- Whether transfers are biased toward **replication (COPY)** or **migration (MOVE)**.
- How aggressively the reactive layer converts “many replicas” into MOVE operations via `copy_to_move_threshold`.

### Continuous configuration space

On top of the discrete structure, you have continuous parameters:

- `interval` (double ≥ 0):
  - How often proactive checks run.
  - Very small → frequent balancing; very large → few proactive actions.

- `high_utilization_threshold` (double in \[0,1]):
  - Which sites are candidates for proactive movement.
  - Low values (e.g. 0.1) → almost all sites look “full enough”.
  - High values (e.g. 0.9) → only truly saturated sites trigger moves.

Mathematically, each of these yields infinitely many possibilities. In practice, you will restrict yourself to a manageable grid, e.g.:

- 10 candidate values for `interval`.
- 10 candidate values for `high_utilization_threshold`.

This multiplies the discrete space by another factor of \(T_\text{interval} × T_\text{threshold}\). Using the earlier example (K = 3, T = 10) and 10×10 continuous grid:

\[
N_\text{practical} \approx 16 \times K \times T \times 10 \times 10
                    = 16 \times 3 \times 10 \times 100
                    = 48{,}000
\]

practically distinct configurations, before even changing workload or platform.

### Qualitative axes

Rather than enumerating all combinations, it’s more intuitive to think in terms of a few axes you can move along:

- **Proactive activity axis**:
  - Controlled by `enabled`, `proactive.enabled`, `interval`, and `high_utilization_threshold`.
  - Ranges from “never runs” → “occasionally rebalances full sites” → “constant background shuffling”.

- **Reactive aggressiveness axis**:
  - Controlled by `reactive.enabled`, `prefer_local_replica`, `remote_source_template`, and `copy_to_move_threshold`.
  - Ranges from “no influence (off)” → “locality-first or template-driven source selection” → “actively converting surplus replicas into MOVE operations”.

- **Replication vs migration axis**:
  - Controlled by `data_transfer_mode` and the reactive MOVE/COPY decisions.
  - Ranges from “mostly COPY (cache‑like replication)” to “mostly MOVE (strict migration)”.

By picking a small set of representative points along these axes, you can:

- Emulate classical policies (pure replication, pure migration, threshold‑based rebalance).
- Explore **corner cases** (extremely low/high utilization thresholds, very short/long intervals).
- Design **stress tests** (e.g. high job load on very full platforms with aggressive reactive MOVE).

The plugin design is intentionally modular so that adding more knobs (new strategies, extra thresholds, hysteresis, per‑file or per‑site parameters) simply extends the same discrete×continuous structure, without requiring changes to the core CGSim engine. 

---

## Policy sweep outputs and visualization

The helper script `generate_policy_variants.py` writes each run under a **timestamped** directory:

- `config-files/runs_<YYYYMMDD_HHMMSS>/<config_name>/toy_config.json`
- `config-files/runs_<YYYYMMDD_HHMMSS>/<config_name>/atlas_grid_simulation.log` (moved from `build/logs/` after each `cg-sim` run)
- `config-files/runs_<YYYYMMDD_HHMMSS>/<config_name>/transfer_heatmap_bytes.png` — site×site bytes (log1p color), axes ordered from `toy_data/mimic_new_site_info.json` top-level keys
- `config-files/runs_<YYYYMMDD_HHMMSS>/<config_name>/transfer_stacked_by_edge.png` — stacked bytes per directed site edge, segments `reactive|<template>` vs `proactive|<template>`
- `config-files/runs_<YYYYMMDD_HHMMSS>/cross_config_transfer_stacked.png` — one stacked bar per configuration (same segment keys), sorted by total bytes

**Dependencies:** `pip install -r requirements-viz.txt` (matplotlib).

**CLI shortcuts:**

- `python3 generate_policy_variants.py --plot-only config-files/runs_<ts>` — regenerate figures from existing logs.
- `python3 -m plotting --plot-only config-files/runs_<ts> [--site-info toy_data/mimic_new_site_info.json]`

Reactive transfer sizes in logs omit byte counts; the parser **imputes** sizes from proactive initiation lines for the same filename when possible (scan proactive first, then reactive).

