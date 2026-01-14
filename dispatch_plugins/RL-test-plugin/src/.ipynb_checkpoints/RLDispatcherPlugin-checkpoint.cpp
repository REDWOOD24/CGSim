#include "DispatcherPlugin.h"
#include "rl_dispatcher.h"

class RLDispatcherPlugin : public DispatcherPlugin {

public:
    RLDispatcherPlugin();
    virtual Job* assignJob(Job* job) final override;
    virtual sg4::NetZone* getPlatform() final override;
    virtual void getResourceInformation(simgrid::s4u::NetZone* platform) final override;
    virtual void onJobEnd(Job* job) final override;
    virtual void onSimulationEnd() final override;

private:
    std::unique_ptr<RL_DISPATCHER> sd = std::make_unique<RL_DISPATCHER>();
};

RLDispatcherPlugin::RLDispatcherPlugin()
{
  LOG_INFO("Loading the Job Dispatcher from RL Dispatcher Plugin ....");
}

void RLDispatcherPlugin::getResourceInformation(simgrid::s4u::NetZone* platform)
{
  LOG_INFO("Inside the Resource information");
  sd->setPlatform(platform);
  LOG_INFO("Finished getting the Resource information");

  // Connect once to Python (Python is the server in your script).
  // If Python isn't running, you'll fail fast here instead of at the first job.
  try {
    sd->connectPython("127.0.0.1", 5555);
    LOG_INFO("Connected to Python RL server.");
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to connect to Python RL server (127.0.0.1:5555): {}", e.what());
    // You can choose: throw to stop simulation, or keep going and let jobs pend.
    // Throwing is usually better because the dispatcher won't function.
    throw;
  }
}

sg4::NetZone* RLDispatcherPlugin::getPlatform()
{
    return sd->getPlatform();
}

Job* RLDispatcherPlugin::assignJob(Job* job)
{
  if (!job) return job;

  LOG_DEBUG("Inside assignJob: jobid={} comp_site={}", job->jobid, job->comp_site);

  try {
    return sd->assignJobToResource(job);
  } catch (const std::exception& e) {
    LOG_ERROR("assignJobToResource exception for job {}: {}", job->jobid, e.what());
    job->status = "pending";
    return job;
  }
}

void RLDispatcherPlugin::onJobEnd(Job* job)
{
  if (!job) return;

  try {
    sd->free(job);
  } catch (const std::exception& e) {
    LOG_ERROR("free() exception for job {}: {}", job->jobid, e.what());
  }
}

void RLDispatcherPlugin::onSimulationEnd()
{
  // Close Python socket before cleanup (optional but clean)
  try {
    sd->disconnectPython();
  } catch (...) {
    // ignore
  }

  sd->cleanup();
}

extern "C" RLDispatcherPlugin* createRLDispatcherPlugin()
{
    return new RLDispatcherPlugin;
}