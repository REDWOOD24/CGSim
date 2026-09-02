#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <unordered_set>
#include <simgrid/s4u.hpp>
#include "job.h"
#include "units_parser.h"

namespace sg4 = simgrid::s4u;
int main(int argc, char** argv);

namespace CGSim::Core
{
class Platform;
class JOB_EXECUTOR;
class HostExtensions;
}

namespace CGSim {

struct Site
{
    std::string name                                                       = "";
    std::vector<sg4::Host*> cpus                                           = {};
    unsigned long total_cores                                              = 0;
    unsigned long used_cores                                               = 0;
    unsigned long total_cpus                                               = 0;
    std::unordered_set<std::string> used_cpus                              = {};
    unsigned long long total_memory                                        = 0;
    unsigned long long used_memory                                         = 0;
    std::deque<Job*> pending_jobs                                          = {};
    std::unordered_map<std::string, Job*>   assigned_jobs                  = {};
    std::unordered_map<std::string, Job*>   running_jobs                   = {};
    std::unordered_map<std::string, Job*>   finished_jobs                  = {};
    std::unordered_map<std::string, Job*>   failed_jobs                    = {};
    std::unordered_map<std::string, std::string>   properties       = {};
    bool job_assignment_enabled                             = true; //@ToDo Risky to turn this off as site pending jobs wont get submitted at all even if turned on if no site jobs are currently executing.
    unsigned long MAX_RETRIES                                        = 100000; //DEFAULT
    std::unordered_map<std::string, std::string> incoming_file_transfers = {};
};

namespace GlobalManagers {

class SiteManager {
public:
    SiteManager(const SiteManager&) = delete;
    SiteManager& operator=(const SiteManager&) = delete;
    static SiteManager& instance();

    //Grid Information
    double get_grid_cpu_utilization();
    double get_grid_memory_utilization();
    unsigned long   get_total_grid_cores();
    unsigned long   get_used_grid_cores();
    unsigned long long  get_total_grid_memory();
    unsigned long long  get_used_grid_memory();
    std::unordered_map<std::string, Job*> get_global_pending_jobs(); //@Todo Change job_id to string
    std::unordered_map<std::string, Job*> get_global_failed_jobs();
    void set_custom_parameter(const std::string& key, const std::string& value){Custom_Parameters[key] = value;}
    std::string get_custom_parameter(const std::string& param_name);
    std::unordered_set<std::string> get_all_sites();


    //Site Information
    bool exists(const std::string& site_name);
    Site* get_site(const std::string& site_name);
    double get_site_cpu_utilization(const std::string& site_name);
    double get_site_memory_utilization(const std::string& site_name);

    //CPU Information
    unsigned int get_cores_available(sg4::Host* cpu);
    unsigned int get_cores_used(sg4::Host* cpu);
    double get_cpu_utilization(sg4::Host* cpu);

    unsigned long long get_memory_available(sg4::Host* cpu);
    unsigned long long get_memory_used(sg4::Host* cpu);
    double get_memory_utilization(sg4::Host* cpu);


private:
    SiteManager() = default;
    std::unordered_map<std::string, Site*> Sites;
    std::unordered_set<std::string> list_of_sites;
    void register_site(sg4::NetZone* site, std::vector<sg4::Host*> compute_cpus, std::unordered_map<std::string, std::string> properties);

    std::unordered_map<std::string, Job*>  GlobalPendingJobs = {};
    std::unordered_map<std::string, Job*>  GlobalFailedJobs = {};
    unsigned long long TOTAL_GRID_CORES = 0;
    unsigned long long USED_GRID_CORES = 0;
    unsigned long long TOTAL_GRID_MEMORY = 0;
    unsigned long long USED_GRID_MEMORY = 0;

    std::unordered_map<std::string, std::string>  Custom_Parameters = {};

    friend int   ::main(int argc, char** argv);
    friend class ::CGSim::Core::Platform;
    friend class ::CGSim::Core::JOB_EXECUTOR;
    friend class ::CGSim::Core::HostExtensions;

};

inline SiteManager* get_site_manager(){return &SiteManager::instance();}

} 

}