#include "scoring_dispatcher.h"

#include <algorithm>
#include <limits>
#include <map>

double ScoringDispatcher::storage_needed(const std::unordered_map<std::string, std::string>& files) {
  long long sum = 0;
  for (const auto& [name, size] : files) sum += CGSim::Utilities::parse_units_size(size);
  return static_cast<double>(sum);
}

void ScoringDispatcher::find_available_cpu(CGSim::Job* j) {
  if (j->get_site().empty()) return;
  auto* site = CGSim::GlobalManagers::get_resource_manager()->get_site(j->get_site());
  if (site->get_pending_jobs().size() > 0) return;
  for (const auto& cpu : site->get_cpus()) {
    if (cpu->get_cores_available() < static_cast<unsigned int>(j->get_cores())) continue;
    if (site->get_remaining_storage() < storage_needed(j->get_output_files())) continue;
    auto d = cpu->get_disks()[0];
    j->set_disk(d->get_name());
    j->set_cpu(cpu->get_name());
    return;
  }
}

void ScoringDispatcher::set_flops(CGSim::Job* j) {
  auto* site = CGSim::GlobalManagers::get_resource_manager()->get_site(j->get_site());
  double cpu_consumption_time = 0.0;
  try {
    cpu_consumption_time = std::stod(j->get_property("cpu_consumption_time"));
  } catch (const std::exception&) {
    cpu_consumption_time = 0.0;
  }
  j->set_flops(static_cast<long long>(std::stol(site->get_property("GFLOPS")) * cpu_consumption_time * j->get_cores()));
}

bool ScoringDispatcher::is_feasible(const std::string& site_name, CGSim::Job* j) {
  const auto& active = spec_.active_spec();
  if (active.excluded_sites.count(site_name)) return false;

  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  auto* site = rm->get_site(site_name);

  bool cores_ok = false;
  for (const auto& cpu : site->get_cpus()) {
    if (cpu->get_cores_available() >= static_cast<unsigned int>(j->get_cores())) {
      cores_ok = true;
      break;
    }
  }
  if (!cores_ok) return false;

  double needed = storage_needed(j->get_output_files()) + static_cast<double>(active.feasibility.storage_margin_bytes);
  if (static_cast<double>(site->get_remaining_storage()) < needed) return false;

  // A candidate site is only feasible if every one of the job's input files
  // either already lives there, or has a working route from at least one of
  // its current locations. Real grids are not always fully interconnected --
  // missing connectivity between two sites is an expected, legitimate
  // constraint of the data, not a bug to engineer around by excluding sites
  // from the simulation. But CGSim has no multi-hop routing, so attempting a
  // transfer across a genuinely missing route crashes SimGrid outright
  // ("Bad gateways for route from ... to ..."); this check is what prevents
  // a site lacking a direct route from ever being offered as a candidate in
  // the first place, so that crash can't happen downstream.
  for (const auto& [filename, info] : j->get_input_files_sizes_locations()) {
    const auto& locations = info.second;
    if (locations.count(site_name)) continue;  // already local, no route needed
    bool reachable = false;
    for (const auto& src_site : locations) {
      try {
        rm->get_connection_bandwidth(src_site, site_name);
        reachable = true;
        break;
      } catch (const std::exception&) {
        continue;
      }
    }
    if (!reachable) return false;
  }

  return true;
}

std::vector<std::string> ScoringDispatcher::feasible_sites(CGSim::Job* j) {
  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  std::vector<std::string> sites(rm->get_list_of_sites().begin(), rm->get_list_of_sites().end());
  std::sort(sites.begin(), sites.end());  // deterministic iteration order for tie-breaking

  std::vector<std::string> result;
  for (const auto& site_name : sites) {
    if (is_feasible(site_name, j)) result.push_back(site_name);
  }
  return result;
}

std::string ScoringDispatcher::pick_best_site(const std::vector<std::string>& candidates, CGSim::Job* j) {
  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  const auto& active = spec_.active_spec();
  auto effective_features = active.effective_features_for(j);

  // raw_values[feature_name][site_name] = raw feature value
  std::map<std::string, std::map<std::string, double>> raw_values;
  for (const auto& fw : effective_features) {
    for (const auto& site_name : candidates) {
      raw_values[fw.name][site_name] = ScoringPolicy::PolicySpec::feature_value(fw.name, site_name, rm->get_site(site_name), j);
    }
  }

  // normalized[feature_name][site_name]
  std::map<std::string, std::map<std::string, double>> normalized;
  for (const auto& fw : effective_features) {
    const auto& values = raw_values[fw.name];
    if (active.normalization == ScoringPolicy::Normalization::None) {
      normalized[fw.name] = values;
      continue;
    }
    double min_v = std::numeric_limits<double>::max();
    double max_v = std::numeric_limits<double>::lowest();
    for (const auto& [site_name, v] : values) {
      min_v = std::min(min_v, v);
      max_v = std::max(max_v, v);
    }
    for (const auto& [site_name, v] : values) {
      normalized[fw.name][site_name] = (max_v > min_v) ? (v - min_v) / (max_v - min_v) : 1.0;
    }
  }

  std::unordered_map<std::string, double> scores;
  double best_score = std::numeric_limits<double>::lowest();
  for (const auto& site_name : candidates) {
    double score = 0.0;
    for (const auto& fw : effective_features) score += fw.weight * normalized[fw.name][site_name];
    scores[site_name] = score;
    best_score = std::max(best_score, score);
  }

  // Ties are common (e.g. every candidate's pending_queue_depth is 0 in a
  // lightly loaded grid, which normalizes to an identical value for all of
  // them). Breaking such ties by candidate-list order (alphabetical, since
  // feasible_sites() sorts it) would silently reassign most jobs "for no
  // reason." Prefer the job's own historical site among near-ties instead,
  // so a policy that is genuinely indifferent leaves jobs where they were;
  // only fall back to lexicographic order if the historical site isn't one
  // of the near-optimal candidates.
  constexpr double kTieEpsilon = 1e-9;
  std::string historical_site;
  try {
    historical_site = j->get_property("historical_site");
  } catch (const std::exception&) {
  }
  if (!historical_site.empty() && scores.count(historical_site) &&
      scores.at(historical_site) >= best_score - kTieEpsilon) {
    return historical_site;
  }

  for (const auto& site_name : candidates) {  // lexicographically sorted by feasible_sites()
    if (scores[site_name] >= best_score - kTieEpsilon) return site_name;
  }
  return candidates.front();  // unreachable: every candidate has a score
}

void ScoringDispatcher::assignJob(CGSim::Job* j) {
  if (!j->get_site().empty()) {
    // Site already chosen (replay-pinned, or chosen on a prior assignJob call
    // and now sitting in that site's local pending queue).
    set_flops(j);
    find_available_cpu(j);
    return;
  }

  bool is_replay = spec_.active_spec().dispatch_mode == ScoringPolicy::DispatchMode::Replay;

  std::vector<std::string> candidates;
  if (is_replay) {
    std::string historical_site;
    try {
      historical_site = j->get_property("historical_site");
    } catch (const std::exception&) {
      return;  // no historical site recorded; job stays pending
    }
    candidates = {historical_site};
  } else {
    candidates = feasible_sites(j);
  }

  if (candidates.empty()) return;  // stays global-pending, retried on the next dispatch tick

  std::string best = is_replay ? candidates.front() : pick_best_site(candidates, j);
  j->set_site(best);
  // Tag the job with whichever phase actually made this decision, since for
  // an adaptive (multi-phase) policy the active phase can change later in
  // the run -- JOB_RESULTS should reflect the phase in force at assignment
  // time, not whatever happens to be active when the job later finishes.
  j->set_property("assigned_under_phase", spec_.active_phase_name());
  set_flops(j);
  find_available_cpu(j);
}
