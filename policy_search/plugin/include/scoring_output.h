#ifndef SCORING_OUTPUT_H
#define SCORING_OUTPUT_H

#include <sqlite3.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include "CGSim.h"
#include "policy_spec.h"

class ScoringOutput {
public:
  explicit ScoringOutput(const ScoringPolicy::AdaptivePolicySpec& spec) : spec_(spec) { initialize(); }
  ~ScoringOutput() { sqlite3_close_v2(db); }

  void onJobFinish(CGSim::Job* job);
  void onJobFailure(CGSim::Job* job);
  void onJobStatusChange(CGSim::Job* job);  // optional debug EVENTS row, only if the active phase's emit_debug_events is set
  void insertPolicySwitch(const std::string& from_phase, const std::string& to_phase, double sim_time);

private:
  const ScoringPolicy::AdaptivePolicySpec& spec_;
  sqlite3* db = nullptr;
  bool initialized = false;

  void initialize();
  void createTables();
  void writeRunMetadata();
  void insertJobResult(CGSim::Job* job, const std::string& status);
  void insertDebugEvent(CGSim::Job* job);

  long long input_files_bytes(CGSim::Job* job);
  long long output_files_bytes(CGSim::Job* job);
};

#endif  // SCORING_OUTPUT_H
