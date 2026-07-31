#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <set>
#include <simgrid/s4u.hpp>
#include "job.h"

namespace sg4 = simgrid::s4u;

namespace CGSim {

struct Site
{
    std::string name                                        = "";
    std::vector<sg4::Host*> cpus                            = {};
    long total_cores                                        = 0;
    long used_cores                                         = 0;
    long total_cpus                                         = 0;
    long used_cpus                                          = 0;
    std::deque<Job*> pending_jobs                           = {};
    std::unordered_map<long long, Job*>   assigned_jobs     = {};
    std::unordered_map<long long, Job*>   running_jobs      = {};
    std::unordered_map<long long, Job*>   finished_jobs     = {};
    std::unordered_map<long long, Job*>   failed_jobs       = {};
    bool job_assignment_enabled                             = true; //@ToDo Risky to turn this off as site pending jobs wont get submitted at all even if turned on if no site jobs are currently executing.
    long MAX_RETRIES                                        = 100000; //DEFAULT
};

class SiteManager {
public:
    SiteManager(const SiteManager&) = delete;
    SiteManager& operator=(const SiteManager&) = delete;
    static SiteManager& instance();

    //Grid Information
    std::unordered_map<long long, Job*>  GlobalPendingJobs = {};
    std::unordered_map<long long, Job*>  GlobalFailedJobs = {};
    long long TOTAL_GRID_CORES = 0;
    long long USED_GRID_CORES = 0;
    double get_grid_cpu_utilization(){return (1.0*USED_GRID_CORES)/(1.0*TOTAL_GRID_CORES);}

    //Site Information
    void register_site(sg4::NetZone* site, std::vector<sg4::Host*> compute_cpus);
    bool exists(const std::string& site_name){return Sites.count(site_name) > 0;};
    std::set<std::string> get_all_sites(){return list_of_sites;}
    Site* get_site(const std::string& site_name);
    double get_site_cpu_utilization(const std::string& site_name);

    std::unordered_map<STATUS,std::string> status_string = {
    {CGSim::STATUS::GlOBAL_PENDING,"global_pending"},
    {CGSim::STATUS::SITE_PENDING,"site_pending"},
    {CGSim::STATUS::ASSIGNED,"assigned"},
    {CGSim::STATUS::RUNNING,"running"},
    {CGSim::STATUS::FINISHED,"finished"},
    {CGSim::STATUS::FAILED,"failed"},
    {CGSim::STATUS::NONE,"none"}
};
    

private:
    SiteManager() = default;
    std::unordered_map<std::string, Site*> Sites;
    std::set<std::string> list_of_sites;
    
};

inline SiteManager* get_site_manager(){return &SiteManager::instance();}

} 