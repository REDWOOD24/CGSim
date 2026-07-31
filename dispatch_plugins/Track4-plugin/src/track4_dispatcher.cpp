#include "track4_dispatcher.h"
#include <random>

double TRACK4_DISPATCHER::storage_needed(std::unordered_map<std::string, long long>& files) {
    long long sum = 0;
    for (const auto& [_, value] : files)
        sum += value;
    return sum;
  }

/*sg4::Host* TRACK4_DISPATCHER::findAvailableCPU(const std::vector<sg4::Host*>& cpus, Job* j){
    std::vector<sg4::Host*> available_cpus;

    for (const auto& cpu : cpus)
    {
        if (cpu->get_name().find("JOB-SERVER_cpu") != std::string::npos)
            continue;

        if (cpu->get_name().find("_communication_server") != std::string::npos)
            continue;

        auto* ext = cpu->extension<HostExtensions>();
        if (!ext)
            continue;

        if (ext->get_cores_available() < j->cores)
            continue;

        if (cpu->get_disks().empty())
            continue;

        available_cpus.push_back(cpu);
    }

    if (available_cpus.empty())
        return nullptr;

    // Random selection
    static std::random_device rd;
    static std::mt19937 gen(rd());

    std::uniform_int_distribution<> dist(0, available_cpus.size() - 1);

    sg4::Host* cpu = available_cpus[dist(gen)];

    auto d = cpu->get_disks()[0];

    j->disk              = d->get_name();
    j->disk_read_bw      = d->get_read_bandwidth();
    j->disk_write_bw     = d->get_write_bandwidth();
    j->comp_host         = cpu->get_name();
    j->comp_host_speed   = cpu->get_speed();

    return cpu;
}*/


sg4::Host* TRACK4_DISPATCHER::findAvailableCPU(const std::vector<sg4::Host*>& cpus, Job* j)
{
  if(CGSim::get_site_manager()->get_site(j->comp_site)->pending_jobs.size() > 0 && j->comp_site.empty()) return nullptr;
    for(const auto& cpu: cpus)
    {
        if(cpu->get_name().find("JOB-SERVER_cpu") != std::string::npos) continue;
        if(cpu->get_name().find("_communication_server") != std::string::npos) continue;
        if(cpu->extension<HostExtensions>()->get_cores_available() < j->cores) continue;
        //if(CGSim::get_file_manager()->request_remaining_site_storage(cpu->get_englobing_zone()->get_name()) < storage_needed(j->output_files)) continue;

        auto d = cpu->get_disks()[0]; //Change later

        j->disk               =  d->get_name();
        j->disk_read_bw       =  d->get_read_bandwidth();
        j->disk_write_bw      =  d->get_write_bandwidth();
        j->comp_host          =  cpu->get_name();
        j->comp_host_speed    =  cpu->get_speed();

        return cpu;
    }
    return nullptr;
}

Job* TRACK4_DISPATCHER::assignJob(Job* job)
{
  sg4::Host* cpu = nullptr;
  auto site = sg4::Engine::get_instance()->netzone_by_name_or_null(job->comp_site);

  job->flops = std::stol(site->get_property("GFLOPS"))*job->cpu_consumption_time*job->cores;
  cpu   = findAvailableCPU(CGSim::get_site_manager()->get_site(job->comp_site)->cpus, job);

  return job;
}
