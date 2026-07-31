#include "site_manager.h"
#include <iostream>

namespace CGSim {

    
SiteManager& SiteManager::instance()
{
    static SiteManager sm;
    return sm;
}



Site* SiteManager::get_site(const std::string& site_name)
{
    if(!exists(site_name)) throw std::runtime_error("Site does not Exist");
    return Sites.at(site_name);
}



void SiteManager::register_site(sg4::NetZone* site, std::vector<sg4::Host*> compute_cpus){

    Site* cgsim_site = new Site; 
    cgsim_site->name = site->get_name();
    std::cout << "Registering site: " << cgsim_site->name << std::endl;
    cgsim_site->total_cores = std::stol(site->get_property("total_cores"));
    cgsim_site->cpus = compute_cpus;
    cgsim_site->total_cpus = compute_cpus.size();
    Sites[cgsim_site->name] = cgsim_site;
    list_of_sites.insert(cgsim_site->name);

    TOTAL_GRID_CORES += cgsim_site->total_cores;
}

double SiteManager::get_site_cpu_utilization(const std::string& site_name)
{
    auto* s = get_site(site_name);
    return (1.0*s->used_cores)/(1.0*s->total_cores);
}

}

