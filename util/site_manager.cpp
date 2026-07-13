#include "site_manager.h"
#include <iostream>

namespace CGSim {

    
SiteManager& SiteManager::instance()
{
    static SiteManager sm;
    return sm;
}

void SiteManager::register_site(sg4::NetZone* site){
    std::string name = site->get_name();
    std::cout << "Registering site: " << name << std::endl;
    Sites.insert(name);
    SiteTotalCores[name] = std::stol(site->get_property("total_cores"));
    SiteAvailableCores[name] = std::stol(site->get_property("total_cores"));
    SiteTotalCPUs[name] = site->get_host_count() - 1;
    SiteAvailableCPUs[name] = site->get_host_count() - 1;
    SitePendingJobs[name] = 0;
    SiteRunningJobs[name] = 0;
    SiteFinishedJobs[name] = 0;
    SiteFailedJobs[name] = 0;

    TOTAL_GRID_CORES += std::stol(site->get_property("total_cores"));
    AVAILABLE_GRID_CORES += std::stol(site->get_property("total_cores"));
}

bool SiteManager::exists(const std::string& site) {
    return Sites.count(site) > 0;
}

void SiteManager::occupy_cores(const std::string& site, int cores){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteAvailableCores.at(site) -= cores;
    AVAILABLE_GRID_CORES -= cores;
}

void SiteManager::free_cores(const std::string& site, int cores){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteAvailableCores.at(site) += cores;
    AVAILABLE_GRID_CORES += cores;
}

void SiteManager::occupy_cpu(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteAvailableCPUs.at(site) -= 1;
}

void SiteManager::free_cpu(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteAvailableCPUs.at(site) += 1;
}

void SiteManager::addSystemPendingJob(){
    SystemPendingJobs++;
}

void SiteManager::removeSystemPendingJob(){
    SystemPendingJobs--;
}

void SiteManager::addPendingJob(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SitePendingJobs.at(site) += 1;
}

void SiteManager::removePendingJob(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SitePendingJobs.at(site) -= 1;
}

void SiteManager::addRunningJob(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteRunningJobs.at(site) += 1;
}

void SiteManager::removeRunningJob(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteRunningJobs.at(site) -= 1;
}

void SiteManager::addFinishedJob(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteFinishedJobs.at(site) += 1;
}

void SiteManager::addFailedJob(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    SiteFailedJobs.at(site) += 1;
}

void SiteManager::moveSystemPendingtoPendingJob(const std::string& site){
removeSystemPendingJob();
addPendingJob(site);
}

void SiteManager::movePendingtoRunningJob(const std::string& site){
removePendingJob(site);
addRunningJob(site);
}

void SiteManager::moveRunningtoFinishedJob(const std::string& site){
removeRunningJob(site);
addFinishedJob(site);
}

void SiteManager::moveRunningtoFailedJob(const std::string& site){
removeRunningJob(site);
addFailedJob(site);
}

long long SiteManager::getSystemPendingJobs(){
    return SystemPendingJobs;
}

long SiteManager::getPendingJobs(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SitePendingJobs.at(site);
}

long SiteManager::getRunningJobs(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteRunningJobs.at(site);
}

long SiteManager::getFinishedJobs(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteFinishedJobs.at(site);
}

long SiteManager::getFailedJobs(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteFailedJobs.at(site);
}

long SiteManager::getActiveJobs(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SitePendingJobs.at(site) + SiteRunningJobs.at(site);
}

double SiteManager::getCPUUtilization(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return 1.0 - ((1.0*SiteAvailableCores.at(site))/(1.0*SiteTotalCores.at(site)));
}

double SiteManager::getGridCPUUtilization(){
    return 1.0 - ((1.0*AVAILABLE_GRID_CORES)/(1.0*TOTAL_GRID_CORES));
}

long SiteManager::getCPUsAvailable(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteAvailableCPUs.at(site);
}

long SiteManager::getCPUsUsed(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteTotalCPUs.at(site) - SiteAvailableCPUs.at(site);
}

long SiteManager::getCoresAvailable(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteAvailableCores.at(site);
}

long SiteManager::getCoresUsed(const std::string& site){
    if(!exists(site)) throw std::runtime_error("SiteManager error: site not registered -> " + site);
    return SiteTotalCores.at(site) - SiteAvailableCores.at(site);
}

}

