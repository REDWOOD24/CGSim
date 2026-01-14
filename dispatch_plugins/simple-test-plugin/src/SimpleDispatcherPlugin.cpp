#include "DispatcherPlugin.h"
#include "simple_dispatcher.h"
#include "workload_manager.h"
#include "output.h"

class SimpleDispatcherPlugin : public DispatcherPlugin {

public:
    SimpleDispatcherPlugin();
    virtual JobQueue getWorkload() override;
    virtual Job* assignJob(Job* job) final override;

    virtual void onSimulationStart() final override;
    virtual void onSimulationEnd() final override;
    virtual void onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobTransferStart(Job* job, simgrid::s4u::Mess const& me) final override;
    virtual void onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me) final override;
    virtual void onFileTransferStart(Job* job, const std::string& filename, simgrid::s4u::Comm const& co) final override;
    virtual void onFileTransferEnd(Job* job, const std::string& filename, simgrid::s4u::Comm const& co) final override;
    virtual void onFileReadStart(Job* job, const std::string& filename, simgrid::s4u::Io const& io) final override;
    virtual void onFileReadEnd(Job* job, const std::string& filename, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteStart(Job* job, const std::string& filename, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteEnd(Job* job, const std::string& filename, simgrid::s4u::Io const& io) final override;

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

// Virtual function can be implemented to execute code on simulation start
void SimpleDispatcherPlugin::onSimulationStart(){};

// Virtual function can be implemented to execute code on simulation end
void SimpleDispatcherPlugin::onSimulationEnd(){};

// Virtual function can be implemented when a job execution starts
void SimpleDispatcherPlugin::onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex){};

// Virtual function can be implemented when a job execution finishes
void SimpleDispatcherPlugin::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex){};

// Virtual function can be implemented when a job transfer starts
void SimpleDispatcherPlugin::onJobTransferStart(Job* job, simgrid::s4u::Mess const& me){};

// Virtual function can be implemented when a job transfer ends
void SimpleDispatcherPlugin::onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me){};

// Virtual function can be implemented when a file transfer starts
void SimpleDispatcherPlugin::onFileTransferStart(Job* job, const std::string& filename, simgrid::s4u::Comm const& co){};

// Virtual function can be implemented when a file transfer ends
void SimpleDispatcherPlugin::onFileTransferEnd(Job* job, const std::string& filename, simgrid::s4u::Comm const& co){};

// Virtual function can be implemented when a file read starts
void SimpleDispatcherPlugin::onFileReadStart(Job* job, const std::string& filename, simgrid::s4u::Io const& io){};

// Virtual function can be implemented when a file read ends
void SimpleDispatcherPlugin::onFileReadEnd(Job* job, const std::string& filename, simgrid::s4u::Io const& io){};

// Virtual function can be implemented when a file write starts
void SimpleDispatcherPlugin::onFileWriteStart(Job* job, const std::string& filename, simgrid::s4u::Io const& io){};

// Virtual function can be implemented when a file write ends
void SimpleDispatcherPlugin::onFileWriteEnd(Job* job, const std::string& filename, simgrid::s4u::Io const& io){};

extern "C" SimpleDispatcherPlugin* createSimpleDispatcherPlugin()
{
    return new SimpleDispatcherPlugin;
}
