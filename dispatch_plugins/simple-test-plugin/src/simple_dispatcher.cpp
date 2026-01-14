#include "simple_dispatcher.h"

double SIMPLE_DISPATCHER::storage_needed(std::unordered_map<std::string, long long>& files) {
    long long sum = 0;
    for (const auto& [_, value] : files)
        sum += value;
    return sum;
  }

sg4::Host* SIMPLE_DISPATCHER::findAvailableCPU(const std::vector<sg4::Host*>& cpus, Job* j)
{
    for(const auto& cpu: cpus)
    {
        if(cpu->get_name().find("_communication") != std::string::npos) continue;
        if(cpu->extension<HostExtensions>()->get_cores_available() < j->cores) continue;
        if(CGSim::FileManager::request_remaining_site_storage(cpu->get_englobing_zone()->get_name()) < storage_needed(j->output_files)) continue;
        j->disk       =  (cpu->get_disks()[0])->get_name();
        j->comp_host  =  cpu->get_name();

        //std::cout << "Grid CPU UTIL: " << calculate_grid_cpu_util() << std::endl;
        //std::cout << "Grid Storage UTIL: " << calculate_grid_storage_util() << std::endl;
        //std::cout << "Site CPU UTIL: " << calculate_site_cpu_util(j->comp_site) << std::endl;
        //std::cout << "Site Storage UTIL: " << calculate_site_storage_util(j->comp_site) << std::endl;

        return cpu;
    }
    return nullptr;
}

Job* SIMPLE_DISPATCHER::assignJob(Job* job)
{
  job->comp_site = "AGLT2_site_0";
  sg4::Host* cpu = nullptr;
  auto site = sg4::Engine::get_instance()->netzone_by_name_or_null(job->comp_site);

  job->flops = std::stol(site->get_property("gflops"))*job->cpu_consumption_time*job->cores;
  cpu   = findAvailableCPU(site->get_all_hosts(), job);

  if(cpu) {job->status = "assigned";}
  else{job->status = "pending";}

  return job;
}

