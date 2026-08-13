#include "site_manager.h"
#include <iostream>
#include "host_extensions.h"

namespace CGSim {


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


void SiteManager::register_site(sg4::NetZone* site, std::vector<sg4::Host*> compute_cpus, std::unordered_map<std::string, std::string> custom_parameters){

    Site* cgsim_site = new Site; 
    cgsim_site->name = site->get_name();
    std::cout << "Registering site: " << cgsim_site->name << std::endl;
    cgsim_site->total_cores = std::stol(site->get_property("total_cores"));
    cgsim_site->cpus = compute_cpus;
    cgsim_site->total_cpus = compute_cpus.size();
    cgsim_site->custom_parameters = custom_parameters;
    Sites[cgsim_site->name] = cgsim_site;
    list_of_sites.insert(cgsim_site->name);

    TOTAL_GRID_CORES += cgsim_site->total_cores;
}


double SiteManager::get_site_cpu_utilization(const std::string& site_name)
{
    auto* s = get_site(site_name);
    return (1.0*s->used_cores)/(1.0*s->total_cores);
}


double SiteManager::get_grid_cpu_utilization()
{
    return (1.0*USED_GRID_CORES)/(1.0*TOTAL_GRID_CORES);
}

long SiteManager::get_total_grid_cores()
{
    return TOTAL_GRID_CORES;
}


long SiteManager::get_used_grid_cores()
{
    return USED_GRID_CORES;
}


std::unordered_map<long long, Job*> SiteManager::get_global_pending_jobs() 
{
    return GlobalPendingJobs;
}


std::unordered_map<long long, Job*> SiteManager::get_global_failed_jobs() 
{
    return GlobalFailedJobs;
}


std::string SiteManager::get_custom_parameter(const std::string& param_name)
{
    return Custom_Parameters.at(param_name);
}

std::string SiteManager::get_status_string(CGSim::STATUS status)
{
    return status_string.at(status);
}

unsigned int SiteManager::get_cores_available(sg4::Host* cpu)
{
    return cpu->extension<HostExtensions>()->get_cores_available();
}

unsigned int SiteManager::get_cores_used(sg4::Host* cpu)
{
    return cpu->extension<HostExtensions>()->get_cores_used();
}

}

