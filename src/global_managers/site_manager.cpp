#include "site_manager.h"
#include <iostream>
#include "host_extensions.h"
#include "print.h"

namespace CGSim {

namespace GlobalManagers {


SiteManager& SiteManager::instance()
{
    static SiteManager sm;
    return sm;
}


bool SiteManager::exists(const std::string& site_name)
{
    return Sites.count(site_name) > 0;
}
    

std::unordered_set<std::string> SiteManager::get_all_sites()
{
    return list_of_sites;
}


Site* SiteManager::get_site(const std::string& site_name)
{
    if(!exists(site_name)) throw std::runtime_error("Site does not Exist");
    return Sites.at(site_name);
}


void SiteManager::register_site(sg4::NetZone* site, std::vector<sg4::Host*> compute_cpus, std::unordered_map<std::string, std::string> properties){

    Site* cgsim_site = new Site; 
    cgsim_site->name = site->get_name();
    CGSim::Utilities::print_site(cgsim_site->name);
    cgsim_site->total_cores = std::stoul(site->get_property("total_cores"));
    cgsim_site->total_memory = std::stoull(site->get_property("total_memory"));
    cgsim_site->cpus = compute_cpus;
    cgsim_site->total_cpus = compute_cpus.size();
    cgsim_site->properties = properties;
    Sites[cgsim_site->name] = cgsim_site;
    list_of_sites.insert(cgsim_site->name);

    TOTAL_GRID_CORES  += cgsim_site->total_cores;
    TOTAL_GRID_MEMORY += cgsim_site->total_memory;
}


double SiteManager::get_site_cpu_utilization(const std::string& site_name)
{
    auto* s = get_site(site_name);
    return (1.0*s->used_cores)/(1.0*s->total_cores);
}

double SiteManager::get_site_memory_utilization(const std::string& site_name)
{
    auto* s = get_site(site_name);
    return (1.0*s->used_memory)/(1.0*s->total_memory);
}

double SiteManager::get_grid_cpu_utilization()
{
    return (1.0*USED_GRID_CORES)/(1.0*TOTAL_GRID_CORES);
}

double SiteManager::get_grid_memory_utilization()
{
    return (1.0*USED_GRID_MEMORY)/(1.0*TOTAL_GRID_MEMORY);
}

unsigned long SiteManager::get_total_grid_cores()
{
    return TOTAL_GRID_CORES;
}


unsigned long SiteManager::get_used_grid_cores()
{
    return USED_GRID_CORES;
}

unsigned long long SiteManager::get_total_grid_memory()
{
    return TOTAL_GRID_MEMORY;
}


unsigned long long SiteManager::get_used_grid_memory()
{
    return USED_GRID_MEMORY;
}


std::unordered_map<std::string, Job*> SiteManager::get_global_pending_jobs() 
{
    return GlobalPendingJobs;
}


std::unordered_map<std::string, Job*> SiteManager::get_global_failed_jobs() 
{
    return GlobalFailedJobs;
}


std::string SiteManager::get_custom_parameter(const std::string& param_name)
{
    return Custom_Parameters.at(param_name);
}

unsigned int SiteManager::get_cores_available(sg4::Host* cpu)
{
    return cpu->extension<CGSim::Core::HostExtensions>()->get_cores_available();
}

unsigned int SiteManager::get_cores_used(sg4::Host* cpu)
{
    return cpu->extension<CGSim::Core::HostExtensions>()->get_cores_used();
}

double SiteManager::get_cpu_utilization(sg4::Host* cpu)
{
    double total_cores = 1.0*cpu->get_core_count();
    double used_cores = 1.0*cpu->extension<CGSim::Core::HostExtensions>()->get_cores_used();
    return used_cores / total_cores;
}

unsigned long long SiteManager::get_memory_available(sg4::Host* cpu)
{
    return cpu->extension<CGSim::Core::HostExtensions>()->get_memory_available();
}

unsigned long long SiteManager::get_memory_used(sg4::Host* cpu)
{
    return cpu->extension<CGSim::Core::HostExtensions>()->get_memory_used();
}

double SiteManager::get_memory_utilization(sg4::Host* cpu)
{
    double total_memory = 1.0*CGSim::Utilities::parse_units_size(cpu->get_property("ram"));
    double used_memory = 1.0*cpu->extension<CGSim::Core::HostExtensions>()->get_memory_used();
    return used_memory / total_memory;
}

}

}
