#ifndef SCORING_DISPATCHER_H
#define SCORING_DISPATCHER_H

#include <string>
#include <vector>
#include "CGSim.h"
#include "policy_spec.h"

class ScoringDispatcher {
public:
  explicit ScoringDispatcher(const ScoringPolicy::AdaptivePolicySpec& spec) : spec_(spec) {}

  void assignJob(CGSim::Job* j);

private:
  const ScoringPolicy::AdaptivePolicySpec& spec_;

  double storage_needed(const std::unordered_map<std::string, std::string>& files);
  void find_available_cpu(CGSim::Job* j);
  void set_flops(CGSim::Job* j);

  bool is_feasible(const std::string& site_name, CGSim::Job* j);
  std::vector<std::string> feasible_sites(CGSim::Job* j);
  std::string pick_best_site(const std::vector<std::string>& candidates, CGSim::Job* j);
};

#endif  // SCORING_DISPATCHER_H
