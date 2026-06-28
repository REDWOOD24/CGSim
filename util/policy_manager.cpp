#include "policy_manager.h"
#include <stdexcept>

std::unordered_map<std::string, CGSim::Policy*> CGSim::PolicyManager::policies;

namespace CGSim {

void PolicyManager::addPolicy(CGSim::Policy* policy)
{
  if (policy == nullptr) throw std::invalid_argument("Cannot add a null policy");
  if (policies.find(policy->name) != policies.end()) throw std::runtime_error( "Policy already exists: " + policy->name);
  policies.emplace(policy->name, policy);
  simgrid::kernel::timer::Timer::set(policy->start_time,[policy]() {PolicyManager::run_policy(policy);});
}

void PolicyManager::run_policy(CGSim::Policy* policy)
{
  const double now = sg4::Engine::get_clock();

  //Can reactivate policy if we continue on !policy->active 
  if (!policy->active || (now >= policy->end_time && policy->end_time > 0.0)) {deactivate_policy(policy); return;}
  auto* main_server = sg4::Engine::get_instance()->host_by_name_or_null("JOB-SERVER_cpu-0");

  if (!RUNNING) {deactivate_policy(policy); return;}
  policy->callback();

  if (policy->repeat_interval == 0.0) {deactivate_policy(policy); return;}
  const double next_time = now + policy->repeat_interval;

  if (next_time >= policy->end_time  && policy->end_time > 0.0) {deactivate_policy(policy); return;}

  simgrid::kernel::timer::Timer::set(next_time,[policy]() {PolicyManager::run_policy(policy);});
}

void PolicyManager::deactivate_policy(CGSim::Policy* policy)
{
    const auto it = policies.find(policy->name);
    if (it == policies.end() || it->second != policy) return;
    policy->active = false;
}

}