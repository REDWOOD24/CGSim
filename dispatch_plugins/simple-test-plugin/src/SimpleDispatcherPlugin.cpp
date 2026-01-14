#include "DispatcherPlugin.h"
#include "simple_dispatcher.h"
#include "workload_manager.h"
#include "output.h"

class SimpleDispatcherPlugin : public DispatcherPlugin {

public:
    SimpleDispatcherPlugin();
    virtual JobQueue getWorkload() override;
    virtual Job* assignJob(Job* job) final override;
    virtual void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onSimulationEnd() final override;

private:
    std::unique_ptr<SIMPLE_DISPATCHER> sd = std::make_unique<SIMPLE_DISPATCHER>();
    std::unique_ptr<WORKLOAD_MANAGER>  wm = std::make_unique<WORKLOAD_MANAGER>();
    std::unique_ptr<OUTPUT>            ou = std::make_unique<OUTPUT>();

};

SimpleDispatcherPlugin::SimpleDispatcherPlugin()
{
}

JobQueue SimpleDispatcherPlugin::getWorkload()
{
  return wm->getWorkload();
}

Job* SimpleDispatcherPlugin::assignJob(Job* job)
{
  return sd->assignJob(job);
}

void SimpleDispatcherPlugin::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
}

void SimpleDispatcherPlugin::onSimulationEnd()
{
}

extern "C" SimpleDispatcherPlugin* createSimpleDispatcherPlugin()
{
    return new SimpleDispatcherPlugin;
}
