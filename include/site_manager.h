#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <unordered_set>
#include <simgrid/s4u.hpp>
#include "job.h"

namespace sg4 = simgrid::s4u;
int main(int argc, char** argv);
class Platform;
class JOB_EXECUTOR;
class HostExtensions;

namespace CGSim {

struct Site
{
    std::string name                                                       = "";
    std::vector<sg4::Host*> cpus                                           = {};
    long total_cores                                                       = 0;
    long used_cores                                                        = 0;
    long total_cpus                                                        = 0;
    long used_cpus                                                         = 0;
    std::deque<Job*> pending_jobs                                          = {};
    std::unordered_map<long long, Job*>   assigned_jobs                    = {};
    std::unordered_map<long long, Job*>   running_jobs                     = {};
    std::unordered_map<long long, Job*>   finished_jobs                    = {};
    std::unordered_map<long long, Job*>   failed_jobs                      = {};
    std::unordered_map<std::string, std::string>   custom_parameters       = {};
    bool job_assignment_enabled                             = true; //@ToDo Risky to turn this off as site pending jobs wont get submitted at all even if turned on if no site jobs are currently executing.
    long MAX_RETRIES                                        = 100000; //DEFAULT
    std::unordered_map<std::string, std::string> incoming_file_transfers = {};
};

class SiteManager {
public:
    SiteManager(const SiteManager&) = delete;
    SiteManager& operator=(const SiteManager&) = delete;
    static SiteManager& instance();

    //Grid Information
    double get_grid_cpu_utilization();
    long   get_total_grid_cores();
    long   get_used_grid_cores();
    std::unordered_map<long long, Job*> get_global_pending_jobs();
    std::unordered_map<long long, Job*> get_global_failed_jobs();
    std::string get_custom_parameter(const std::string& param_name);
    std::unordered_set<std::string> get_all_sites();


    //Site Information
    bool exists(const std::string& site_name);
    Site* get_site(const std::string& site_name);
    double get_site_cpu_utilization(const std::string& site_name);
    unsigned int get_cores_available(sg4::Host* cpu);
    unsigned int get_cores_used(sg4::Host* cpu); 

    //Job Status String
    std::string get_status_string(CGSim::STATUS status); 


private:
    SiteManager() = default;
    std::unordered_map<std::string, Site*> Sites;
    std::unordered_set<std::string> list_of_sites;
    void register_site(sg4::NetZone* site, std::vector<sg4::Host*> compute_cpus, std::unordered_map<std::string, std::string> custom_parameters);

    std::unordered_map<long long, Job*>  GlobalPendingJobs = {};
    std::unordered_map<long long, Job*>  GlobalFailedJobs = {};
    long long TOTAL_GRID_CORES = 0;
    long long USED_GRID_CORES = 0;
    std::unordered_map<std::string, std::string>  Custom_Parameters = {};

    std::unordered_map<STATUS,std::string> status_string = {
    {CGSim::STATUS::GlOBAL_PENDING,"global_pending"},
    {CGSim::STATUS::SITE_PENDING,"site_pending"},
    {CGSim::STATUS::ASSIGNED,"assigned"},
    {CGSim::STATUS::RUNNING,"running"},
    {CGSim::STATUS::FINISHED,"finished"},
    {CGSim::STATUS::FAILED,"failed"},
    {CGSim::STATUS::NONE,"none"}};

    friend int   ::main(int argc, char** argv);
    friend class ::Platform;
    friend class ::JOB_EXECUTOR;
    friend class ::HostExtensions;

};

inline SiteManager* get_site_manager(){return &SiteManager::instance();}

} 