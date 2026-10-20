#ifndef POLICY_SPEC_H
#define POLICY_SPEC_H

#include <functional>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "CGSim.h"

// Note: named ScoringPolicySpec (not "Policy"/"PolicyManager") to avoid
// colliding with CGSim::GlobalManagers::PolicyManager, an unrelated
// timer/callback-scheduling facility (include/global_managers/policy_manager.h).

namespace ScoringPolicy {

enum class DispatchMode { Replay, Score };
enum class Normalization { MinMax, None };

struct FeatureWeight {
  std::string name;
  double weight;
};

// "when job_property(job_property) op value: override feature's weight to override_weight"
struct ConditionalRule {
  std::string job_property;
  std::string op;  // one of: >=, <=, >, <, ==, !=
  std::string value;
  std::string override_feature;
  double override_weight;
};

struct FeasibilityConfig {
  long long storage_margin_bytes = 0;
};

// CGSim::Site has no public get_name(); the name is only known externally
// (ResourceManager's site map keys / get_list_of_sites()), so every feature
// takes the candidate site's name explicitly alongside the Site* and Job*.
using FeatureFn = std::function<double(const std::string&, CGSim::Site*, CGSim::Job*)>;

// Feature names that encode the historical answer directly. Legitimate to
// *read* (e.g. for a replay baseline or for measuring similarity-to-history),
// but using one to *drive* a "score" dispatch policy is a leakage trap:
// register_policy()/validate_policy() should warn (not block) on this.
bool is_leakage_feature(const std::string& name);

// Must be called once, before any input/output files are created (i.e. from
// Plugin::beforeSimulationStart(), which CGSim invokes before setWorkload()),
// so remaining_storage_fraction has a true full-capacity denominator instead
// of whatever happens to remain after workload setup consumes some storage.
void snapshot_storage_capacities();

// One dispatch strategy: a set of weighted, per-(job,site) features scored
// and combined into a single number per candidate site. This is the whole
// policy for a non-adaptive spec, or one phase of an AdaptivePolicySpec.
class PolicySpec {
public:
  std::string policy_name;
  DispatchMode dispatch_mode = DispatchMode::Score;
  FeasibilityConfig feasibility;
  std::unordered_set<std::string> excluded_sites;
  std::vector<FeatureWeight> features;
  std::vector<ConditionalRule> conditional_rules;
  Normalization normalization = Normalization::MinMax;
  bool emit_debug_events = false;

  // Parses and validates a policy spec from a JSON file. Throws
  // std::runtime_error (with a descriptive message) on any malformed or
  // unknown-feature-name input, so a bad spec fails fast in
  // beforeSimulationStart() rather than mid-simulation.
  void load(const std::string& path);

  // Same validation/parsing, from an already-parsed JSON object -- used
  // both by load() (whole-file case) and by AdaptivePolicySpec (one phase's
  // "spec" sub-object).
  static PolicySpec from_json(const nlohmann::json& j);

  // Feature weights for `job`, after applying any matching conditional_rules
  // overrides. Base `features` order is preserved; an override replaces the
  // weight of the named feature (the feature must already be present in
  // `features` for an override to apply).
  std::vector<FeatureWeight> effective_features_for(CGSim::Job* job) const;

  static double feature_value(const std::string& name, const std::string& site_name, CGSim::Site* site, CGSim::Job* job);
  static bool is_known_feature(const std::string& name);

  // Features usable in an AdaptivePolicySpec's activate_when condition: they
  // must depend only on (site_name, site), not on a specific job, since a
  // switch check has no "current job" to evaluate against. A subset of
  // is_known_feature(); see is_state_only_feature().
  static bool is_state_only_feature(const std::string& name);
  static double state_feature_value(const std::string& name, const std::string& site_name, CGSim::Site* site);

private:
  static const std::unordered_map<std::string, FeatureFn>& registry();
};

// A condition over CURRENT live grid state (evaluated at every adaptive
// switch check, not just once at simulation start), used to decide whether
// an AdaptivePolicySpec should switch to a later phase. Aggregates one
// state-only feature across every site in the grid, then compares the
// aggregate to a threshold -- e.g. {feature: pending_queue_depth,
// aggregate: max_over_sites, op: ">", value: 5} means "some site's queue
// has grown past 5 jobs deep."
struct StateCondition {
  std::string feature;
  std::string aggregate;  // one of: max_over_sites, min_over_sites, avg_over_sites, sum_over_sites
  std::string op;         // one of: >=, <=, >, <, ==, !=
  double value = 0.0;

  bool evaluate() const;
};

struct AdaptivePhase {
  std::string phase_name;
  PolicySpec spec;
  bool has_condition = false;  // false only for phase 0, the default/initial phase
  StateCondition condition;
};

// A policy that can genuinely change mid-simulation: an ordered list of
// phases, each a full PolicySpec, where phases[0] is active from t=0 and
// each later phase declares a StateCondition under which it takes over.
// Modeled after the "adaptive algorithms" category in Shivaratri, Krueger &
// Singhal 1992 ("Load Distributing for Locally Distributed Systems", IEEE
// Computer 25(12):33-44), which surveys load-distributing policies that
// change their own behavior based on observed system state rather than
// running one fixed rule for the whole system lifetime.
//
// Every policy spec -- even a plain, non-adaptive one -- is loaded as an
// AdaptivePolicySpec with exactly one phase (phase_name "default", no
// condition), so ScoringDispatcher/ScoringWorkloadManager/ScoringOutput only
// ever deal with this one type.
class AdaptivePolicySpec {
public:
  std::string policy_name;
  double check_interval_seconds = 0.0;  // 0 => single-phase, no periodic check registered
  std::vector<AdaptivePhase> phases;

  void load(const std::string& path);

  const PolicySpec& active_spec() const { return phases.at(active_phase_index_).spec; }
  const std::string& active_phase_name() const { return phases.at(active_phase_index_).phase_name; }
  bool is_adaptive() const { return phases.size() > 1; }

  // Re-evaluates every phase's condition against current grid state (in
  // reverse declaration order, so a later phase wins over an earlier one
  // whose condition is also currently true) and switches phases if the
  // winner differs from the active one. Calls on_switch(from_phase,
  // to_phase, sim_time) exactly when a switch happens, so the caller can
  // log it (see ScoringOutput::insertPolicySwitch).
  void evaluate_and_maybe_switch(const std::function<void(const std::string&, const std::string&, double)>& on_switch);

private:
  int active_phase_index_ = 0;
};

}  // namespace ScoringPolicy

#endif  // POLICY_SPEC_H
