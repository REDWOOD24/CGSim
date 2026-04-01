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
    "interval": 10.0,
    "high_utilization_threshold": 0.1,
    "data_transfer_mode": "MOVE"
  },
  "reactive": {
    "enabled": true,
    "strategy": "prefer_local_then_first",
    "move_when_replica_count_exceeds": 3
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

- **`high_utilization_threshold`** (double in \[0,1]):
  - Site utilization is defined as:
    - `util = 1.0 - remaining_bytes / capacity_bytes`
  - A site is considered “highly utilized” if `util ≥ threshold`.

- **`data_transfer_mode`** (`"COPY"` or `"MOVE"`):
  - How proactive transfers change the file layout:
    - `"COPY"`: replicate files (source keeps its copy).
    - `"MOVE"`: migrate files (source copy removed after successful transfer).

### `reactive` section

- **`enabled`** (bool):
  - Turn reactive logic on or off.

- **`strategy`** (string, currently informational):
  - Default: `"prefer_local_then_first"`.
  - Describes the selection rule:
    - Prefer a replica on the job’s compute site.
    - If none, pick the first available replica.

- **`move_when_replica_count_exceeds`** (integer):
  - If the number of replicas of a file **exceeds** this value when a job requests it:
    - The reactive policy suggests `MOVE`.
  - Otherwise:
    - The reactive policy suggests `COPY`.

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
   - If any replica has `replica.sitename == job_site`:
     - `decision.chosen_site = job_site` (serve locally).
   - Else if `ctx.replicas` is non‑empty:
     - `decision.chosen_site = ctx.replicas.front().sitename` (fallback: first replica).
   - Else:
     - Leave `chosen_site` empty (no known replicas; core’s default is used).

4. Choose transfer mode:
   - If `replica_count > move_when_replica_count_exceeds_`:
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

The proactive plugin uses SimGrid’s **kernel timers** to run periodically and rebalance storage across sites.

### Scheduling

- At `onSimulationStart()`:
  - If `proactive_enabled_` and `interval > 0`:
    - Compute `next_time = current_time + interval`.
    - Call `timer::Timer::set(next_time, callback)`.

- The callback:
  - Increments `execution_count_`.
  - Calls `performDataManagementOperations(sg4::Engine::get_clock())`.
  - Schedules the **next** timer at `current_time + interval`.

- At `onSimulationEnd()`:
  - If a timer exists:
    - `current_timer_->remove(); current_timer_ = nullptr;`

### Core algorithm (`performDataManagementOperations`)

At each execution time \(t\):

1. Query all sites:
   - `sites = FileManager::get_site_names()`.
   - For each `sitename`:
     - `capacity = FileManager::get_site_capacity(sitename)`.
     - `remaining = FileManager::request_remaining_site_storage(sitename)`.
     - `util = 1.0 - remaining / capacity`.

2. Build `site_utilization`:
   - `std::vector<std::pair<std::string, double>>` of `(sitename, util)`.

3. For each `src_site` in `site_utilization`:
   - If `util < high_utilization_threshold_`:
     - Skip (source not considered “full”).

   - Find candidate `dst_site`:
     - The site with **lowest utilization**, `u_dst`, such that:
       - `dst_site != src_site`.
       - Has enough remaining space for at least one file from `src_site`.

   - If no such `dst_site` exists:
     - Continue to next `src_site`.

   - Query files on `src_site`:
     - `files_on_site = FileManager::get_files_on_site(src_site)`.
     - For each `filename`:
       - Skip if `FileManager::exists(filename, dst_site)` (already has a replica).
       - Skip if the pair `(filename, src_site)` is currently in `in_flight_transfers_`.
       - Query `size = FileManager::request_file_size(filename)`.
       - If `size ≤ remaining_dst`:
         - Select this `filename_to_copy`, `file_size` and break.

   - If no candidate file found:
     - Continue to next `src_site`.

   - Initiate transfer:
     - Resolve communication hosts:
       - `src_host = Engine::host_by_name_or_null(src_site + "_communication")`.
       - `dst_host = Engine::host_by_name_or_null(dst_site + "_communication")`.
     - Create a detached `Comm`:
       - `Comm::sendto_init()->set_source(src_host)->set_destination(dst_host)->set_payload_size(file_size)`.
     - Name the activity:
       - `"DataMgmt_move_<file>_from_<src>_to_<dst>"` or `"DataMgmt_copy_..."`.
     - Register completion callback:
       - On completion:
         - `FileManager::create(file, size, dst_site)`.
         - If `data_transfer_mode_ == MOVE`:
           - `FileManager::remove(file, src_site)` (free space and update maps).
         - Remove `(file, src_site)` from `in_flight_transfers_`.

   - Log **initiation** and **completion**:

     - Initiation (reason included):

     ```text
     Proactive Data Management: initiating MOVE of '17' from AGLT2_site_3 (util 85.00%) to AGLT2_site_5
     (util 20.00%) at time 100 because src_util >= threshold (85.00% >= 80.00%) and dst has enough free storage
     ```

     - Completion:

     ```text
     Proactive Data Management: completed MOVE of '17' (524288000 bytes) from AGLT2_site_3 to AGLT2_site_5
     ```

4. Only **one** transfer is initiated per policy execution:
   - After the first successful initiation, the loop `break`s.


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
  R6 --> R7{replica_count ><br/>move_when_replica_count_exceeds?}:::decision
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
  - `strategy` – currently effectively `"prefer_local_then_first"` but could be extended to a small enum of K strategies.
  - `move_when_replica_count_exceeds` – an integer threshold in some finite range \[0, R] (R being a “max reasonable replica count”).

If we bound ourselves to:

- 3 booleans (`enabled`, `proactive.enabled`, `reactive.enabled`) → \(2^3 = 8\) combinations.
- 2 values for `data_transfer_mode`.
- K possible strategies.
- T distinct values for the integer threshold.

then the number of structurally distinct policies is on the order of:

\[
N_\text{discrete} = 2^3 \times 2 \times K \times T = 16 \times K \times T.
\]

For example, with K = 3 strategy options and T = 10 threshold options, you already have
\(16 × 3 × 10 = 480\) different “shapes” of policy, before considering timing/threshold parameters.

Qualitatively, these discrete switches let you control:

- Whether the system is **proactive only**, **reactive only**, **both**, or **disabled**.
- Whether transfers are biased toward **replication (COPY)** or **migration (MOVE)**.
- How aggressively the reactive layer converts “many replicas” into MOVE operations via `move_when_replica_count_exceeds`.

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
  - Controlled by `reactive.enabled`, `strategy`, and `move_when_replica_count_exceeds`.
  - Ranges from “no influence (off)” → “just choose best source” → “actively converting surplus replicas into MOVE operations”.

- **Replication vs migration axis**:
  - Controlled by `data_transfer_mode` and the reactive MOVE/COPY decisions.
  - Ranges from “mostly COPY (cache‑like replication)” to “mostly MOVE (strict migration)”.

By picking a small set of representative points along these axes, you can:

- Emulate classical policies (pure replication, pure migration, threshold‑based rebalance).
- Explore **corner cases** (extremely low/high utilization thresholds, very short/long intervals).
- Design **stress tests** (e.g. high job load on very full platforms with aggressive reactive MOVE).

The plugin design is intentionally modular so that adding more knobs (new strategies, extra thresholds, hysteresis, per‑file or per‑site parameters) simply extends the same discrete×continuous structure, without requiring changes to the core CGSim engine. 

