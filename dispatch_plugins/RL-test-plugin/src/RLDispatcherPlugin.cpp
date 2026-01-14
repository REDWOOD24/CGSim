#include "DispatcherPlugin.h"
#include "rl_dispatcher.h"

class RLDispatcherPlugin : public DispatcherPlugin {
public:
  RLDispatcherPlugin() = default;

  JobQueue getWorkload(long num_of_jobs) override {
    return sd_->getJobs(num_of_jobs);
  }

  void getResourceInformation(simgrid::s4u::NetZone* platform) final override {
    sd_->setPlatform(platform);
    sd_->connectPython("127.0.0.1", 5555);
  }

  Job* assignJob(Job* job) final override {
    return sd_->assignJobToResource(job);
  }

  void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const&) final override {
    sd_->free(job);
  }

  void onSimulationEnd() final override {
    sd_->disconnectPython();
    sd_->cleanup();
  }

private:
  std::unique_ptr<RL_DISPATCHER> sd_ = std::make_unique<RL_DISPATCHER>();
};

// IMPORTANT: CGSim is looking for this symbol:
extern "C" DispatcherPlugin* createRL_DISPATCHER() {
  return new RLDispatcherPlugin;
}

// Optional: keep the old name too (harmless)
extern "C" DispatcherPlugin* createRLDispatcherPlugin() {
  return createRL_DISPATCHER();
}
