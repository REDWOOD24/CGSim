#include "site_policy_manager.h"
#include "job_executor.h"

std::unordered_map<std::string, SitePolicy*> CGSim::SitePolicyManager::active_policies;

namespace CGSim {

void SitePolicyManager::addSitePolicy(SitePolicy* p)
{
    if (!p->active) throw std::runtime_error("Policy: " + p->name + " must be active on addition");
    active_policies[p->name] = p;
    simgrid::kernel::timer::Timer::set(p->start_time, [p]() {
      p->callback(JOB_EXECUTOR::pending_activities);
      if(p->repeat_interval > 0) schedule_policy_repeat(p);
      else{active_policies.erase(p->name); delete p;}
    });
}

void SitePolicyManager::schedule_policy_repeat(SitePolicy* p)
{
    if(sg4::Engine::get_clock() > p->end_time || !p->active) return;
    if(sg4::Engine::get_instance()->host_by_name_or_null("JOB-SERVER_cpu-0")->is_on())
      {
        simgrid::kernel::timer::Timer::set(sg4::Engine::get_clock()+p->repeat_interval,
        [p]() {p->callback(JOB_EXECUTOR::pending_activities); schedule_policy_repeat(p);});
      }
}






}