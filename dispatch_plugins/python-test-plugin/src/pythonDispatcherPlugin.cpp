#include "DispatcherPlugin.h"
#include "python_dispatcher.h"
#include "workload_manager.h"
#include "output.h"
#include "local_scheduler.h"

class pythonDispatcherPlugin : public DispatcherPlugin {

public:
    pythonDispatcherPlugin();
    virtual JobQueue getWorkload() override;
    virtual Job* assignJob(Job* job) final override;

    virtual void onSimulationStart() final override;
    virtual void onSimulationEnd() final override;
    virtual void onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobTransferStart(Job* job, simgrid::s4u::Mess const& me) final override;
    virtual void onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me) final override;
    virtual void onFileTransferStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site) final override;
    virtual void onFileTransferEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site) final override;
    virtual void onFileReadStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileReadEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io) final override;

private:
    // IMPORTANT: use pythonDispatcher (it owns assignJob), not PYTHON_DISPATCHER
    std::unique_ptr<pythonDispatcher>  pd = std::make_unique<pythonDispatcher>();
    std::unique_ptr<WORKLOAD_MANAGER>  wm = std::make_unique<WORKLOAD_MANAGER>();
    std::unique_ptr<OUTPUT>            ou = std::make_unique<OUTPUT>();
    std::unique_ptr<LOCAL_SCHEDULER>   ls = std::make_unique<LOCAL_SCHEDULER>();
};

pythonDispatcherPlugin::pythonDispatcherPlugin()
{
}

JobQueue pythonDispatcherPlugin::getWorkload()
{
  return wm->getWorkload();
}

Job* pythonDispatcherPlugin::assignJob(Job* job)
{
  if (!job) return nullptr;

  // 1) Site assignment via Python (single-job wrapper)
  Job* sitedJob = pd->assignJob(job);
  if (!sitedJob) return nullptr;

  // 2) CPU assignment via local scheduler (mutates in place; returns void)
  ls->assignJob(sitedJob, "random");

  return sitedJob;
}

void pythonDispatcherPlugin::onSimulationStart()
{
  ou->onSimulationStart();
}

void pythonDispatcherPlugin::onSimulationEnd()
{
   ou->onSimulationEnd();
}

void pythonDispatcherPlugin::onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex)
{
   ou->onJobExecutionStart(job,ex);
}

void pythonDispatcherPlugin::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
   ou->onJobExecutionEnd(job,ex);
}

void pythonDispatcherPlugin::onJobTransferStart(Job* job, simgrid::s4u::Mess const& me)
{
   ou->onJobTransferStart(job,me);
}

void pythonDispatcherPlugin::onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me)
{
   ou->onJobTransferEnd(job,me);
}

void pythonDispatcherPlugin::onFileTransferStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site)
{
   ou->onFileTransferStart(job,filename, filesize, co,src_site,dst_site);
}

void pythonDispatcherPlugin::onFileTransferEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site)
{
   ou->onFileTransferEnd(job,filename, filesize, co,src_site,dst_site);
}

void pythonDispatcherPlugin::onFileReadStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileReadStart(job,filename, filesize, io);
}

void pythonDispatcherPlugin::onFileReadEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileReadEnd(job,filename, filesize, io);
}

void pythonDispatcherPlugin::onFileWriteStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileWriteStart(job,filename, filesize, io);
}

void pythonDispatcherPlugin::onFileWriteEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileWriteEnd(job,filename, filesize, io);
}

extern "C" DispatcherPlugin* createPYTHON_DISPATCHER()
{
    return new pythonDispatcherPlugin();
}
