#include "policy_spec.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <numeric>
#include <stdexcept>

using json = nlohmann::json;

namespace ScoringPolicy {

namespace {

std::unordered_map<std::string, unsigned long long> g_initial_storage_capacity;

bool compare(double lhs, const std::string& op, double rhs) {
  if (op == ">=") return lhs >= rhs;
  if (op == "<=") return lhs <= rhs;
  if (op == ">") return lhs > rhs;
  if (op == "<") return lhs < rhs;
  if (op == "==") return lhs == rhs;
  if (op == "!=") return lhs != rhs;
  throw std::runtime_error("Unknown comparison operator: " + op);
}

double bytes_at_site(
    const std::unordered_map<std::string, std::pair<long long, std::unordered_set<std::string>>>& locations,
    const std::string& site_name) {
  double total = 0;
  for (const auto& [filename, info] : locations) {
    const auto& [size, sites] = info;
    if (sites.count(site_name)) total += static_cast<double>(size);
  }
  return total;
}

double total_input_bytes(
    const std::unordered_map<std::string, std::pair<long long, std::unordered_set<std::string>>>& locations) {
  double total = 0;
  for (const auto& [filename, info] : locations) total += static_cast<double>(info.first);
  return total;
}

// Best connection from any of the job's input-file locations to `site_name`:
// highest bandwidth, or lowest latency. Returns 0.0 if the job has no input
// files. A location equal to `site_name` needs no network hop at all (CGSim
// never creates a self-loop link, so looking one up would throw) and always
// dominates any real link. A location with no route at all to `site_name`
// (ResourceManager throws for a missing link -- some site pairs genuinely
// lack route data) is skipped rather than treated as a crash.
double best_connection(CGSim::Job* job, const std::string& site_name, bool bandwidth) {
  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  double best = 0.0;
  bool found = false;
  for (const auto& [filename, info] : job->get_input_files_sizes_locations()) {
    for (const auto& src_site : info.second) {
      double v;
      if (src_site == site_name) {
        v = bandwidth ? std::numeric_limits<double>::max() : 0.0;
      } else {
        try {
          v = bandwidth ? rm->get_connection_bandwidth(src_site, site_name)
                        : rm->get_connection_latency(src_site, site_name);
        } catch (const std::exception&) {
          continue;
        }
      }
      bool better = bandwidth ? (v > best) : (v < best);
      if (!found || better) {
        best = v;
        found = true;
      }
    }
  }
  return best;
}

// Minimum Execution Time (MET)-style estimate (Braun et al. 2001, "A
// Comparison of Eleven Static Heuristics for Mapping a Class of Independent
// Tasks onto Heterogeneous Distributed Computing Systems", J. Parallel
// Distrib. Comput. 61(6):810-837): execution_time(job, site) = job's
// site-independent work / site's compute rate.
//
// The site-independent "work" figure is backed out from the job's own
// historical observation: ScoringDispatcher::set_flops() defines
// flops = site_GFLOPS * cpu_consumption_time * cores, so
// reference_work := historical_site_GFLOPS * cpu_consumption_time is a
// GFLOP-seconds figure for this job that does NOT depend on which candidate
// site we're scoring -- exactly the "ETC matrix" entry the Min-Min/MCT/MET
// literature assumes is available per (job, machine) pair. Dividing by a
// candidate site's own GFLOPS then gives a genuine, site-varying execution
// time estimate (fixing the earlier version of this feature, which computed
// the same value for every candidate and so could never discriminate
// between sites -- see git history / PR discussion).
double execution_time_estimate(CGSim::Site* site, CGSim::Job* job) {
  double site_gflops = 0.0;
  try {
    site_gflops = std::stod(site->get_property("GFLOPS"));
  } catch (const std::exception&) {
    return 0.0;
  }
  if (site_gflops <= 0.0) return 0.0;

  try {
    auto* rm = CGSim::GlobalManagers::get_resource_manager();
    std::string historical_site = job->get_property("historical_site");
    double historical_gflops = std::stod(rm->get_site(historical_site)->get_property("GFLOPS"));
    double cpu_consumption_time = std::stod(job->get_property("cpu_consumption_time"));
    return (historical_gflops * cpu_consumption_time) / site_gflops;
  } catch (const std::exception&) {
    return 0.0;
  }
}

}  // namespace

bool is_leakage_feature(const std::string& name) { return name == "matches_historical_site"; }

void snapshot_storage_capacities() {
  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  for (const auto& site_name : rm->get_list_of_sites()) {
    g_initial_storage_capacity[site_name] = rm->get_site(site_name)->get_remaining_storage();
  }
}

const std::unordered_map<std::string, FeatureFn>& PolicySpec::registry() {
  static const std::unordered_map<std::string, FeatureFn> reg = {
      // Load index in the sense of Shivaratri, Krueger & Singhal 1992,
      // "Load Distributing for Locally Distributed Systems", IEEE Computer
      // 25(12):33-44 -- their survey's central primitive is a scalar load
      // index per node (they discuss queue-length and CPU-utilization
      // indices specifically) used to prefer less-loaded receivers.
      {"available_cores_fraction",
       [](const std::string&, CGSim::Site* site, CGSim::Job*) -> double {
         if (site->get_number_of_total_cores() == 0) return 0.0;
         return static_cast<double>(site->get_number_of_total_cores() - site->get_number_of_used_cores()) /
                static_cast<double>(site->get_number_of_total_cores());
       }},
      {"available_cores_abs",
       [](const std::string&, CGSim::Site* site, CGSim::Job*) -> double {
         return static_cast<double>(site->get_number_of_total_cores() - site->get_number_of_used_cores());
       }},
      {"remaining_storage_fraction",
       [](const std::string& site_name, CGSim::Site* site, CGSim::Job*) -> double {
         auto it = g_initial_storage_capacity.find(site_name);
         unsigned long long capacity =
             it != g_initial_storage_capacity.end() ? it->second : site->get_remaining_storage();
         if (capacity == 0) return 0.0;
         return static_cast<double>(site->get_remaining_storage()) / static_cast<double>(capacity);
       }},
      {"pending_queue_depth",
       [](const std::string&, CGSim::Site* site, CGSim::Job*) -> double {
         return static_cast<double>(site->get_pending_jobs().size());
       }},
      {"running_jobs",
       [](const std::string&, CGSim::Site* site, CGSim::Job*) -> double {
         return static_cast<double>(site->get_running_jobs().size());
       }},
      {"gflops",
       [](const std::string&, CGSim::Site* site, CGSim::Job*) -> double {
         try {
           return std::stod(site->get_property("GFLOPS"));
         } catch (const std::exception&) {
           return 0.0;
         }
       }},
      {"execution_time_estimate",
       [](const std::string&, CGSim::Site* site, CGSim::Job* job) -> double {
         return execution_time_estimate(site, job);
       }},
      {"estimated_completion_time",
       [](const std::string&, CGSim::Site* site, CGSim::Job* job) -> double {
         // Minimum Completion Time (MCT)-style estimate (Maheswaran, Ali,
         // Siegel, Hensgen & Freund 1999, "Dynamic Matching and Scheduling
         // of a Class of Independent Tasks onto Heterogeneous Computing
         // Systems", HCW'99, pp.30-44; also compared in Braun et al. 2001
         // above): completion_time(job, site) = execution_time(job, site) +
         // ready_time(site), where ready_time is when the site becomes free
         // to start this job.
         //
         // DOCUMENTED SIMPLIFICATION: the formal definition's ready_time is
         // the sum of remaining execution time of everything already queued
         // at the site, which this feature registry has no way to observe
         // (CGSim's Site only exposes queue *depth*, not queued jobs'
         // remaining runtimes). We approximate ready_time as
         // pending_queue_depth(site) * this_job's_own_execution_time_estimate
         // -- i.e. "assume everything ahead of me takes about as long as I
         // will." This is a real deviation from the textbook definition;
         // treat this feature as MCT-flavored, not literally MCT.
         double exec_time = execution_time_estimate(site, job);
         double ready_time = static_cast<double>(site->get_pending_jobs().size()) * exec_time;
         return exec_time + ready_time;
       }},
      // Data-locality preference in the sense of Zaharia, Borthakur, Sen
      // Sarma, Elmeleegy, Shenker & Stoica 2010, "Delay Scheduling: A Simple
      // Technique for Achieving Locality and Fairness in Cluster
      // Scheduling", EuroSys 2010, pp.265-278: prefer the site already
      // holding a task's input data, since Hadoop/Dryad-style clusters show
      // this can matter more for throughput than instantaneous load
      // balance. Their paper's actual mechanism (wait a bounded delay for a
      // locally-schedulable slot before falling back to a remote one) isn't
      // implemented here -- this feature only supplies the raw locality
      // signal; the delay/fallback tradeoff is left to policy weights (or
      // to an adaptive multi-phase policy, see plugin/include/policy_spec.h
      // AdaptivePolicy).
      {"input_locality_bytes_fraction",
       [](const std::string& site_name, CGSim::Site*, CGSim::Job* job) -> double {
         const auto& locations = job->get_input_files_sizes_locations();
         double total = total_input_bytes(locations);
         if (total <= 0.0) return 1.0;  // no input data to move; locality is a non-issue
         return bytes_at_site(locations, site_name) / total;
       }},
      {"bandwidth_from_nearest_input_site",
       [](const std::string& site_name, CGSim::Site*, CGSim::Job* job) -> double {
         return best_connection(job, site_name, /*bandwidth=*/true);
       }},
      {"latency_from_nearest_input_site",
       [](const std::string& site_name, CGSim::Site*, CGSim::Job* job) -> double {
         return best_connection(job, site_name, /*bandwidth=*/false);
       }},
      {"matches_historical_site",
       [](const std::string& site_name, CGSim::Site*, CGSim::Job* job) -> double {
         try {
           return site_name == job->get_property("historical_site") ? 1.0 : 0.0;
         } catch (const std::exception&) {
           return 0.0;
         }
       }},
  };
  return reg;
}

bool PolicySpec::is_known_feature(const std::string& name) { return registry().count(name) > 0; }

double PolicySpec::feature_value(const std::string& name, const std::string& site_name, CGSim::Site* site,
                                  CGSim::Job* job) {
  auto it = registry().find(name);
  if (it == registry().end()) throw std::runtime_error("Unknown policy feature: " + name);
  return it->second(site_name, site, job);
}

namespace {
// Features whose lambda in registry() never touches its Job* argument --
// the only ones safe to evaluate with no specific job in mind, i.e. usable
// in an AdaptivePolicySpec's activate_when condition (see StateCondition).
const std::unordered_set<std::string>& state_only_features() {
  static const std::unordered_set<std::string> s = {
      "available_cores_fraction", "available_cores_abs", "remaining_storage_fraction",
      "pending_queue_depth", "running_jobs", "gflops",
  };
  return s;
}
}  // namespace

bool PolicySpec::is_state_only_feature(const std::string& name) { return state_only_features().count(name) > 0; }

double PolicySpec::state_feature_value(const std::string& name, const std::string& site_name, CGSim::Site* site) {
  if (!is_state_only_feature(name)) {
    throw std::runtime_error("Feature '" + name + "' depends on a specific job and cannot be used in an "
                              "activate_when condition (allowed: available_cores_fraction, available_cores_abs, "
                              "remaining_storage_fraction, pending_queue_depth, running_jobs, gflops)");
  }
  return feature_value(name, site_name, site, nullptr);
}

namespace {

// Looks up a job property by name, checking built-in job attributes first
// (fields that live on CGSim::Job directly rather than in its string
// metadata map) before falling back to job->get_property(key).
bool lookup_job_property(CGSim::Job* job, const std::string& key, std::string& out) {
  if (key == "cores") {
    out = std::to_string(job->get_cores());
    return true;
  }
  try {
    out = job->get_property(key);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

}  // namespace

std::vector<FeatureWeight> PolicySpec::effective_features_for(CGSim::Job* job) const {
  std::vector<FeatureWeight> result = features;

  for (const auto& rule : conditional_rules) {
    std::string actual;
    if (!lookup_job_property(job, rule.job_property, actual)) continue;

    bool matched;
    try {
      matched = compare(std::stod(actual), rule.op, std::stod(rule.value));
    } catch (const std::exception&) {
      matched = (rule.op == "==") ? (actual == rule.value) : (rule.op == "!=") ? (actual != rule.value) : false;
    }
    if (!matched) continue;

    for (auto& fw : result) {
      if (fw.name == rule.override_feature) fw.weight = rule.override_weight;
    }
  }
  return result;
}

PolicySpec PolicySpec::from_json(const json& j) {
  PolicySpec spec;

  // Left empty ("") when parsing one phase of an AdaptivePolicySpec, whose
  // "spec" sub-object doesn't carry its own policy_name -- the caller
  // (PolicySpec::load, or AdaptivePolicySpec::load) fills it in.
  spec.policy_name = j.value("policy_name", std::string(""));

  std::string mode = j.value("dispatch_mode", std::string("score"));
  if (mode == "replay") {
    spec.dispatch_mode = DispatchMode::Replay;
  } else if (mode == "score") {
    spec.dispatch_mode = DispatchMode::Score;
  } else {
    throw std::runtime_error("PolicySpec: unknown dispatch_mode '" + mode + "'");
  }

  if (j.contains("feasibility")) {
    spec.feasibility.storage_margin_bytes = j["feasibility"].value("storage_margin_bytes", 0LL);
  }

  if (j.contains("excluded_sites")) {
    for (const auto& s : j["excluded_sites"]) spec.excluded_sites.insert(s.get<std::string>());
  }

  if (j.contains("features")) {
    for (const auto& f : j["features"]) {
      std::string name = f.at("name").get<std::string>();
      if (!is_known_feature(name)) throw std::runtime_error("PolicySpec: unknown feature '" + name + "'");
      spec.features.push_back({name, f.at("weight").get<double>()});
    }
  }

  if (j.contains("conditional_rules")) {
    for (const auto& r : j["conditional_rules"]) {
      ConditionalRule rule;
      const auto& when = r.at("when");
      rule.job_property = when.at("job_property").get<std::string>();
      rule.op = when.at("op").get<std::string>();
      rule.value = when.at("value").get<std::string>();
      const auto& ov = r.at("override_weight");
      rule.override_feature = ov.at("feature").get<std::string>();
      if (!is_known_feature(rule.override_feature))
        throw std::runtime_error("PolicySpec: unknown override feature '" + rule.override_feature + "'");
      rule.override_weight = ov.at("weight").get<double>();
      spec.conditional_rules.push_back(std::move(rule));
    }
  }

  std::string norm = j.value("normalization", std::string("minmax_across_candidates"));
  if (norm == "minmax_across_candidates") {
    spec.normalization = Normalization::MinMax;
  } else if (norm == "none") {
    spec.normalization = Normalization::None;
  } else {
    throw std::runtime_error("PolicySpec: unknown normalization '" + norm + "'");
  }

  spec.emit_debug_events = j.value("emit_debug_events", false);
  return spec;
}

void PolicySpec::load(const std::string& path) {
  std::ifstream in(path);
  if (!in.is_open()) throw std::runtime_error("PolicySpec: could not open " + path);
  json j;
  in >> j;
  *this = from_json(j);
  if (policy_name.empty()) throw std::runtime_error("PolicySpec: 'policy_name' is required");
}

bool StateCondition::evaluate() const {
  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  std::vector<double> values;
  for (const auto& site_name : rm->get_list_of_sites()) {
    values.push_back(PolicySpec::state_feature_value(feature, site_name, rm->get_site(site_name)));
  }
  if (values.empty()) return false;

  double agg;
  if (aggregate == "max_over_sites") {
    agg = *std::max_element(values.begin(), values.end());
  } else if (aggregate == "min_over_sites") {
    agg = *std::min_element(values.begin(), values.end());
  } else if (aggregate == "sum_over_sites") {
    agg = std::accumulate(values.begin(), values.end(), 0.0);
  } else if (aggregate == "avg_over_sites") {
    agg = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
  } else {
    throw std::runtime_error("StateCondition: unknown aggregate '" + aggregate + "'");
  }
  return compare(agg, op, value);
}

void AdaptivePolicySpec::load(const std::string& path) {
  std::ifstream in(path);
  if (!in.is_open()) throw std::runtime_error("AdaptivePolicySpec: could not open " + path);
  json j;
  in >> j;

  if (!j.contains("phases")) {
    // Plain, non-adaptive spec: every policy -- even a simple one -- is
    // represented internally as a single-phase AdaptivePolicySpec, so
    // ScoringDispatcher/ScoringWorkloadManager/ScoringOutput only ever deal
    // with one type.
    PolicySpec spec = PolicySpec::from_json(j);
    if (spec.policy_name.empty()) throw std::runtime_error("PolicySpec: 'policy_name' is required");
    policy_name = spec.policy_name;
    check_interval_seconds = 0.0;
    AdaptivePhase phase;
    phase.phase_name = "default";
    phase.spec = std::move(spec);
    phase.has_condition = false;
    phases.push_back(std::move(phase));
    return;
  }

  policy_name = j.at("policy_name").get<std::string>();
  check_interval_seconds = j.value("check_interval_seconds", 3600.0);
  if (check_interval_seconds <= 0.0) {
    throw std::runtime_error("AdaptivePolicySpec: check_interval_seconds must be positive when 'phases' is used");
  }

  const auto& phases_json = j.at("phases");
  if (phases_json.empty()) throw std::runtime_error("AdaptivePolicySpec: 'phases' must not be empty");

  for (std::size_t i = 0; i < phases_json.size(); ++i) {
    const auto& pj = phases_json[i];
    AdaptivePhase phase;
    phase.phase_name = pj.at("phase_name").get<std::string>();
    phase.spec = PolicySpec::from_json(pj.at("spec"));
    phase.spec.policy_name = policy_name + "/" + phase.phase_name;

    if (phase.spec.dispatch_mode != DispatchMode::Score) {
      throw std::runtime_error("AdaptivePolicySpec: phase '" + phase.phase_name +
                                "' must use dispatch_mode 'score' -- 'replay' doesn't make sense inside an "
                                "adaptive policy");
    }

    if (i == 0) {
      if (pj.contains("activate_when")) {
        throw std::runtime_error("AdaptivePolicySpec: phase 0 ('" + phase.phase_name +
                                  "') is the default phase, active from t=0, and must not have activate_when");
      }
      phase.has_condition = false;
    } else {
      const auto& cond = pj.at("activate_when");
      phase.has_condition = true;
      phase.condition.feature = cond.at("feature").get<std::string>();
      if (!PolicySpec::is_state_only_feature(phase.condition.feature)) {
        throw std::runtime_error("AdaptivePolicySpec: activate_when feature '" + phase.condition.feature +
                                  "' is job-dependent and cannot be used in a state condition");
      }
      phase.condition.aggregate = cond.at("aggregate").get<std::string>();
      phase.condition.op = cond.at("op").get<std::string>();
      phase.condition.value = cond.at("value").get<double>();
    }
    phases.push_back(std::move(phase));
  }
}

void AdaptivePolicySpec::evaluate_and_maybe_switch(
    const std::function<void(const std::string&, const std::string&, double)>& on_switch) {
  int winner = 0;
  // Reverse order: a later-declared phase takes priority over an earlier
  // one whose condition also currently holds (e.g. a "severe congestion"
  // phase declared after a "mild congestion" phase wins when both match).
  for (int i = static_cast<int>(phases.size()) - 1; i >= 1; --i) {
    if (phases[i].condition.evaluate()) {
      winner = i;
      break;
    }
  }
  if (winner != active_phase_index_) {
    on_switch(phases[active_phase_index_].phase_name, phases[winner].phase_name, sg4::Engine::get_clock());
    active_phase_index_ = winner;
  }
}

}  // namespace ScoringPolicy
