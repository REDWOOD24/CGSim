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
    virtual void onFileTransferStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site) final override;
    virtual void onFileTransferEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site) final override;
    virtual void onFileReadStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileReadEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;

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

void SimpleDispatcherPlugin::onSimulationStart()
{
  ou->onSimulationStart();
}

void SimpleDispatcherPlugin::onSimulationEnd()
{
   ou->onSimulationEnd();
}

void SimpleDispatcherPlugin::onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex)
{
   ou->onJobExecutionStart(job,ex);
}

void SimpleDispatcherPlugin::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
   ou->onJobExecutionEnd(job,ex);
}

void SimpleDispatcherPlugin::onJobTransferStart(Job* job, simgrid::s4u::Mess const& me)
{
   ou->onJobTransferStart(job,me);
}

void SimpleDispatcherPlugin::onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me)
{
   ou->onJobTransferEnd(job,me);
}

void SimpleDispatcherPlugin::onFileTransferStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site)
{
   ou->onFileTransferStart(job,filename, filesize, co,src_site,dst_site);
}

void SimpleDispatcherPlugin::onFileTransferEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site)
{
   ou->onFileTransferEnd(job,filename, filesize, co,src_site,dst_site);
}

void SimpleDispatcherPlugin::onFileReadStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileReadStart(job,filename, filesize, io);
}

void SimpleDispatcherPlugin::onFileReadEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileReadEnd(job,filename, filesize, io);
}

void SimpleDispatcherPlugin::onFileWriteStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileWriteStart(job,filename, filesize, io);
}

void SimpleDispatcherPlugin::onFileWriteEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileWriteEnd(job,filename, filesize, io);
}

extern "C" SimpleDispatcherPlugin* createSimpleDispatcherPlugin()
{
    return new SimpleDispatcherPlugin;
}
