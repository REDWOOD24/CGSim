#include "plugin.h"
#include "policy_spec.h"
#include "scoring_dispatcher.h"
#include "scoring_output.h"
#include "scoring_workload_manager.h"
#include <memory>

class ScoringPolicyPlugin : public CGSim::Plugin {
public:
  ScoringPolicyPlugin() = default;

  void beforeSimulationStart() final override;
  void setWorkload(CGSim::JobQueue& jobs) final override;
  void assignJob(CGSim::Job* job) final override;

  void onJobSubmission(CGSim::Job* job) final override;
  void onJobSitePending(CGSim::Job* job) final override;
  void onJobAssignment(CGSim::Job* job) final override;
  void onJobExecutionStart(CGSim::Job* job) final override;
  void onJobExecutionEnd(CGSim::Job* job) final override;
  void onJobFinish(CGSim::Job* job) final override;
  void onJobFailure(CGSim::Job* job) final override;

private:
  ScoringPolicy::AdaptivePolicySpec spec_;
  std::unique_ptr<ScoringDispatcher> dispatcher_;
  std::unique_ptr<ScoringWorkloadManager> workload_manager_;
  std::unique_ptr<ScoringOutput> output_;

  void registerAdaptiveSwitchCheck();
};

void ScoringPolicyPlugin::beforeSimulationStart() {
  std::string spec_path = CGSim::GlobalManagers::get_resource_manager()->get_custom_parameter("policy_spec_file");
  spec_.load(spec_path);

  // Must run before setWorkload() creates any input files, so remaining
  // storage capacity is snapshotted at its true full value.
  ScoringPolicy::snapshot_storage_capacities();

  dispatcher_ = std::make_unique<ScoringDispatcher>(spec_);
  workload_manager_ = std::make_unique<ScoringWorkloadManager>(spec_);
  output_ = std::make_unique<ScoringOutput>(spec_);

  if (spec_.is_adaptive()) registerAdaptiveSwitchCheck();
}

// Wires the adaptive policy's periodic state check into CGSim's own
// timer-driven policy scheduler (CGSim::GlobalManagers::PolicyManager,
// include/global_managers/policy_manager.h) rather than building a separate
// mechanism -- this IS what that facility is for. `policy` is intentionally
// never deleted: it must outlive the whole simulation, matching how every
// other CGSim::Policy in this codebase is registered.
void ScoringPolicyPlugin::registerAdaptiveSwitchCheck() {
  auto* policy = new CGSim::Policy();
  policy->name = "adaptive_policy_switch_check";
  policy->start_time = spec_.check_interval_seconds;  // first check after one interval, not at t=0
  policy->end_time = 0.0;                             // unbounded
  policy->repeat_interval = spec_.check_interval_seconds;
  policy->callback = [this]() {
    spec_.evaluate_and_maybe_switch(
        [this](const std::string& from, const std::string& to, double sim_time) {
          output_->insertPolicySwitch(from, to, sim_time);
        });
  };
  CGSim::GlobalManagers::get_policy_manager()->addPolicy(policy);
}

void ScoringPolicyPlugin::setWorkload(CGSim::JobQueue& jobs) { workload_manager_->setWorkload(jobs); }

void ScoringPolicyPlugin::assignJob(CGSim::Job* job) { dispatcher_->assignJob(job); }

void ScoringPolicyPlugin::onJobSubmission(CGSim::Job* job) { output_->onJobStatusChange(job); }

void ScoringPolicyPlugin::onJobSitePending(CGSim::Job* job) { output_->onJobStatusChange(job); }

void ScoringPolicyPlugin::onJobAssignment(CGSim::Job* job) { output_->onJobStatusChange(job); }

void ScoringPolicyPlugin::onJobExecutionStart(CGSim::Job* job) { output_->onJobStatusChange(job); }

void ScoringPolicyPlugin::onJobExecutionEnd(CGSim::Job* job) { output_->onJobStatusChange(job); }

void ScoringPolicyPlugin::onJobFinish(CGSim::Job* job) { output_->onJobFinish(job); }

void ScoringPolicyPlugin::onJobFailure(CGSim::Job* job) { output_->onJobFailure(job); }

extern "C" ScoringPolicyPlugin* createScoringPolicyPlugin() { return new ScoringPolicyPlugin; }
