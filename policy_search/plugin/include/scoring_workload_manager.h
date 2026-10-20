#ifndef SCORING_WORKLOAD_MANAGER_H
#define SCORING_WORKLOAD_MANAGER_H

#include "CGSim.h"
#include "policy_spec.h"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>

class ScoringWorkloadManager {
public:
  explicit ScoringWorkloadManager(const ScoringPolicy::AdaptivePolicySpec& spec) : spec_(spec) {}
  void setWorkload(CGSim::JobQueue& jobs);

private:
  const ScoringPolicy::AdaptivePolicySpec& spec_;

  std::vector<std::string> parseCSVLine(const std::string& line);
  std::string getColumn(const std::vector<std::string>& row, const std::unordered_map<std::string, int>& column_map,
                         const std::string& key, const std::string& default_val = "");
};

#endif  // SCORING_WORKLOAD_MANAGER_H
