#include "plugin.h"
#include "track4_dispatcher.h"
#include "track4_workload_manager.h"
#include "track4_output.h"
#include "track4_output_calibration.h"

class Track4Plugin : public CGSim::Plugin {

public:
    Track4Plugin();
    virtual JobQueue getWorkload() final override;
    virtual Job* assignJob(Job* job) final override;

    virtual void onJobSubmission(Job* job) final override;
    virtual void onJobAssignment(Job* job) final override;
    virtual void onJobSitePending(Job* job) final override;
    virtual void onJobFailure(Job* job) final override;
    virtual void onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex) final override;
 

private:
    std::unique_ptr<TRACK4_DISPATCHER>        t4d = std::make_unique<TRACK4_DISPATCHER>();
    std::unique_ptr<TRACK4_WORKLOAD_MANAGER>  t4wm = std::make_unique<TRACK4_WORKLOAD_MANAGER>();
    std::unique_ptr<TRACK4_OUTPUT>            t4ou = std::make_unique<TRACK4_OUTPUT>();
    //std::unique_ptr<TRACK4_OUTPUT_CALIBRATION> t4ouc = std::make_unique<TRACK4_OUTPUT_CALIBRATION>();

};

Track4Plugin::Track4Plugin()
{
}

JobQueue Track4Plugin::getWorkload()
{
   return t4wm->getWorkload();
}

Job* Track4Plugin::assignJob(Job* job)
{
   return t4d->assignJob(job);
}

void Track4Plugin::onJobSubmission(Job* job)
{
   t4ou->onJobStatusChange(job);
}

void Track4Plugin::onJobSitePending(Job* job)
{
   t4ou->onJobStatusChange(job);
}

void Track4Plugin::onJobFailure(Job* job)
{
   t4ou->onJobStatusChange(job);
}

void Track4Plugin::onJobAssignment(Job* job)
{
   t4ou->onJobStatusChange(job);
}

void Track4Plugin::onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex)
{
   t4ou->onJobStatusChange(job);
}

void Track4Plugin::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
   //t4ouc->onJobExecutionEnd(job,ex);
   t4ou->onJobStatusChange(job);
}

extern "C" Track4Plugin* createTrack4Plugin()
{
    return new Track4Plugin;
}
