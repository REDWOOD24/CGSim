#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <unordered_set>
#include <vector>
#include <deque>
#include <simgrid/s4u.hpp>
#include "job.h"
#include "units_parser.h"
#include "cgsim_site.h"
#include "cgsim_cpu.h"

namespace sg4 = simgrid::s4u;
int main(int argc, char** argv);

namespace CGSim::Core
{
class Platform;
class JOB_EXECUTOR;
class HostExtensions;
}

namespace CGSim::GlobalManagers 
{
class ResourceManager;
}

namespace CGSim {

namespace GlobalManagers {

class ResourceManager {
public:
    ResourceManager(const ResourceManager&) = delete;
    ResourceManager& operator=(const ResourceManager&) = delete;
    static ResourceManager& instance();

    inline unsigned long   get_total_grid_cores(){return TOTAL_GRID_CORES;}
    inline unsigned long   get_used_grid_cores() {return USED_GRID_CORES;}

    inline unsigned long long  get_total_grid_memory_capacity(){return TOTAL_GRID_MEMORY;}
    inline unsigned long long  get_used_grid_memory(){return USED_GRID_MEMORY;}

    inline double get_grid_cpu_utilization(){return (1.0*USED_GRID_CORES)/(1.0*TOTAL_GRID_CORES);}
    inline double get_grid_memory_utilization(){return (1.0*USED_GRID_MEMORY)/(1.0*TOTAL_GRID_MEMORY);}

    inline std::unordered_map<std::string, Job*> get_global_pending_jobs(){return global_pending_jobs;}
    inline std::unordered_map<std::string, Job*> get_global_failed_jobs(){return global_failed_jobs;}

    inline void set_custom_parameter(const std::string& key, const std::string& value){Custom_Parameters[key] = value;}
    inline std::string get_custom_parameter(const std::string& key){return Custom_Parameters.at(key);}

    inline std::unordered_set<std::string> get_list_of_sites(){return list_of_sites;}
    inline std::unordered_map<std::string, Site*> get_all_sites(){return global_site_map;}
    inline std::unordered_map<std::string, CPU*>  get_all_cpus(){return global_cpu_map;}

    Site* get_site(const std::string& site_name);
    inline CGSim::CPU* get_cpu(const std::string& cpu_name){return global_cpu_map.at(cpu_name);} //Check existence first

    inline bool site_exists(const std::string& site_name){return global_site_map.count(site_name) > 0;}
    inline bool cpu_exists(const std::string& cpu_name){return global_cpu_map.count(cpu_name) > 0;}

private:
    ResourceManager() = default;
    CGSim::Site* create_site(const std::string& site_name, sg4::NetZone* _simgrid_site);
    CGSim::CPU*  create_cpu(const std::string& cpu_name, sg4::Host* _simgrid_host);
    CGSim::Disk* create_disk(const std::string& disk_name, sg4::Disk* _simgrid_disk);
    
    void print_site_info(const std::string& site_name);
    std::unordered_set<std::string> list_of_sites;

    std::unordered_map<std::string, Site*> global_site_map = {};
    std::unordered_map<std::string, CPU*>  global_cpu_map = {};
    std::unordered_map<std::string, Job*>  global_pending_jobs = {}; //Make Deque Object
    std::unordered_map<std::string, Job*>  global_failed_jobs = {};

    unsigned long TOTAL_GRID_CORES = 0;
    unsigned long USED_GRID_CORES = 0;
    unsigned long long TOTAL_GRID_MEMORY = 0;
    unsigned long long USED_GRID_MEMORY = 0;


    std::unordered_map<std::string, std::string>  Custom_Parameters = {};

    friend int   ::main(int argc, char** argv);
    friend class ::CGSim::Core::Platform;
    friend class ::CGSim::Core::JOB_EXECUTOR;
    friend class ::CGSim::Core::HostExtensions;
};

inline ResourceManager* get_resource_manager(){return &ResourceManager::instance();}

} 

}