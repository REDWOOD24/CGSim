#include "resource_manager.h"
#include "print.h"

namespace CGSim {

namespace GlobalManagers {

ResourceManager& ResourceManager::instance(){static ResourceManager rm; return rm;}

Site* ResourceManager::get_site(const std::string& site_name) 
{
    if(!site_exists(site_name)) throw std::runtime_error("Site does not Exist"); 
    return global_site_map.at(site_name);
}

void ResourceManager::print_site_info(const std::string& site_name)
{
    CGSim::Utilities::print_site(site_name);
}

CGSim::Site* ResourceManager::create_site(const std::string& site_name, sg4::NetZone* _simgrid_site)
{
    auto* site = new CGSim::Site; 
    site->name = site_name; 
    site->simgrid_site = _simgrid_site;
    list_of_sites.insert(site_name); 
    global_site_map[site_name] = site; 
    return site;
}

CGSim::CPU*  ResourceManager::create_cpu(const std::string& cpu_name, sg4::Host* _simgrid_host)
{
    auto* cpu = new CGSim::CPU; 
    cpu->name = cpu_name;
    cpu->simgrid_host =  _simgrid_host;
    global_cpu_map[cpu_name] = cpu; 
    return cpu;
}

CGSim::Disk* ResourceManager::create_disk(const std::string& disk_name, sg4::Disk* _simgrid_disk)
{
    auto* disk = new CGSim::Disk;
    disk->name = disk_name;
    disk->simgrid_disk = _simgrid_disk;
    return disk;
}

}

}
