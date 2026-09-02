#include "track4_dispatcher.h"
#include <random>

double TRACK4_DISPATCHER::storage_needed(const std::unordered_map<std::string, std::string>& files) 
{
    long long sum = 0;
    for (const auto& [name, size] : files)
        sum += CGSim::Utilities::parse_units_size(size);
    return sum;
}

void TRACK4_DISPATCHER::findAvailableCPU(CGSim::Job* j)
{
  auto* sm = CGSim::GlobalManagers::get_site_manager();
  auto* fm = CGSim::GlobalManagers::get_file_manager();
  auto cpus = sm->get_site(j->get_comp_site())->cpus;
  if(sm->get_site(j->get_comp_site())->pending_jobs.size() > 0 && j->get_comp_site().empty()) return;

  for(const auto& cpu: cpus)
  {
    if(sm->get_cores_available(cpu) < j->get_cores()) continue;
    if(fm->request_remaining_site_storage(j->get_comp_site()) < storage_needed(j->get_output_files())) continue;

    auto d = cpu->get_disks()[0]; //Change later
    j->set_disk(d->get_name());
    j->set_comp_host(cpu->get_name());
    return;
  }
}

void TRACK4_DISPATCHER::assignJob(CGSim::Job* job)
{
  auto* sm = CGSim::GlobalManagers::get_site_manager();
  job->set_flops(std::stol(sm->get_site(job->get_comp_site())->properties.at("GFLOPS"))*std::stod(job->get_property("cpu_consumption_time"))*job->get_cores());
  findAvailableCPU(job);
}
