#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <set>
#include <simgrid/s4u.hpp>

namespace sg4 = simgrid::s4u;

namespace CGSim {

class SiteManager {
public:
    SiteManager(const SiteManager&) = delete;
    SiteManager& operator=(const SiteManager&) = delete;

    static SiteManager& instance();


    void register_site(sg4::NetZone* site);
    bool exists(const std::string& site);

    void occupy_cores(const std::string& site, int cores);
    void free_cores(const std::string& site, int cores);

    void occupy_cpu(const std::string& site);
    void free_cpu(const std::string& site);

    void addSystemPendingJob();
    void addPendingJob(const std::string& site);
    void addRunningJob(const std::string& site);
    void addFinishedJob(const std::string& site);
    void addFailedJob(const std::string& site);

    void removeSystemPendingJob();
    void removePendingJob(const std::string& site);
    void removeRunningJob(const std::string& site);


    long long               getSystemPendingJobs();
    long                    getPendingJobs(const std::string& site);
    long                    getRunningJobs(const std::string& site);
    long                    getFinishedJobs(const std::string& site);
    long                    getFailedJobs(const std::string& site);
    long                    getActiveJobs(const std::string& site);
    std::set<std::string>   get_all_sites(){return Sites;}

    long   getCPUsAvailable(const std::string& site);
    long   getCPUsUsed(const std::string& site);
    long   getCoresAvailable(const std::string& site);
    long   getCoresUsed(const std::string& site);
    double getCPUUtilization(const std::string& site);
    double getGridCPUUtilization();


    void moveSystemPendingtoPendingJob(const std::string& site);
    void movePendingtoRunningJob(const std::string& site);
    void moveRunningtoFinishedJob(const std::string& site);
    void moveRunningtoFailedJob(const std::string& site);

private:
    SiteManager() = default;

    std::set<std::string> Sites;

    std::unordered_map<std::string, long> SiteTotalCores;
    std::unordered_map<std::string, long> SiteAvailableCores;

    std::unordered_map<std::string, long> SiteTotalCPUs;
    std::unordered_map<std::string, long> SiteAvailableCPUs;

    long long SystemPendingJobs = 0;
    std::unordered_map<std::string, long> SitePendingJobs;
    std::unordered_map<std::string, long> SiteRunningJobs;
    std::unordered_map<std::string, long> SiteFinishedJobs;
    std::unordered_map<std::string, long> SiteFailedJobs;

    inline static long TOTAL_GRID_CORES = 0;
    inline static long AVAILABLE_GRID_CORES = 0;
};

inline SiteManager* get_site_manager() {
    return &SiteManager::instance();
}

} 