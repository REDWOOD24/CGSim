#include "statistics.h"
#include "job_executor.h"

namespace CGSim {

namespace Utilities {

unsigned long Statistics::get_pending_activities_size() {return CGSim::Core::JOB_EXECUTOR::pending_activities.size();}

double Statistics::get_current_system_time() 
{
    double current_time = std::chrono::duration<double>(Clock::now() - start_).count();
    previous_time_ = current_time;
    return current_time;
}

double Statistics::get_previous_recorded_system_time() 
{
    return previous_time_;
}

}

}
