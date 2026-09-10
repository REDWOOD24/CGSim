#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <simgrid/s4u.hpp>
#include <simgrid/s4u/Io.hpp>
#include <simgrid/s4u/Disk.hpp>
#include <simgrid/s4u/Comm.hpp>
#include <simgrid/s4u/Exec.hpp>
#include <simgrid/s4u/Mess.hpp>
#include <stdexcept>
#include "job.h"
#include "resource_manager.h"
#include "units_parser.h"

namespace sg4 = simgrid::s4u;
int main(int argc, char** argv);

namespace CGSim::Core
{
class Actions;
class JOB_EXECUTOR;
class Platform;
}


namespace CGSim {

class Plugin;
enum class FileTransferDecisionMode {COPY,MOVE};

namespace GlobalManagers {

class FileManager {
public:
    FileManager(const FileManager&) = delete;
    FileManager& operator=(const FileManager&) = delete;

    static FileManager& instance();
    const bool exists(const std::string& filename) const;
    const bool exists(const std::string& filename, const std::string& sitename) const;

    const std::unordered_set<std::string>& request_site_files (const std::string& sitename) const;
    const std::unordered_set<std::string>& request_file_sites(const std::string& filename) const; //File Locations
    unsigned long long request_file_size(const std::string& filename) const;
    unsigned long long request_remaining_site_storage(const std::string& sitename) const;
    unsigned long long request_remaining_grid_storage() const;
    double  request_grid_storage_utilization() const;
    double request_site_storage_utilization(const std::string& sitename) const;

    void create(const std::string& filename, const unsigned long long& size, const std::string& sitename);
    void create(const std::string& filename, const std::string& size, const std::string& sitename);
    bool remove(const std::string& filename, const std::string& sitename);
    void transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, CGSim::FileTransferDecisionMode mode, const std::string& metadata = "");
    void write(const std::string& filename, const unsigned long long& size, const std::string& site, const std::string& cpu, const std::string& disk);
    void write(const std::string& filename, const std::string& size, const std::string& site, const std::string& cpu, const std::string& disk);
    void read(const std::string& filename, const std::string& site, const std::string& cpu, const std::string& disk);

    const bool is_in_flight(const std::string& filename, const std::string& src_site, const std::string& dst_site) const;
    const std::string generate_transfer_key(const std::string& filename,const std::string& src_site,const std::string& dst_site) const;

    inline static std::unordered_set<std::string> in_flight_transfers = {};

    

private:
    FileManager() = default; 
    std::unordered_map<std::string, std::unordered_set<std::string>> SiteFiles;
    std::unordered_map<std::string, std::unordered_set<std::string>> FileSites;
    std::unordered_map<std::string, unsigned long long> FileSizes;
    std::unordered_map<std::string, unsigned long long> TotalSiteStorages;
    std::unordered_map<std::string, unsigned long long> SiteStorages;
    inline static std::shared_ptr<CGSim::Plugin>        plugin;

    std::unordered_set<std::string> internal_transfers; //Hack to avoid double start comm callback
    std::unordered_set<std::string> user_initiated_transfers; //Hack to avoid double start comm callback

    void register_site(sg4::NetZone* site, const std::unordered_map<std::string, unsigned long long>& files);
    static void set_plugin(std::shared_ptr<CGSim::Plugin>& p){plugin = p;}

    sg4::IoPtr   internal_write(const std::string& filename, const unsigned long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    sg4::IoPtr   internal_read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    sg4::CommPtr internal_transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, FileTransferDecisionMode mode = CGSim::FileTransferDecisionMode::COPY);


    std::unordered_map<std::string, sg4::CommPtr> ongoing_transfers;
    Job* request_file_location(Job* j);

    unsigned long long TOTAL_GRID_STORAGE = 0;
    unsigned long long USED_GRID_STORAGE = 0;
  
    friend int   ::main(int argc, char** argv);
    friend class ::CGSim::Core::Actions;
    friend class ::CGSim::Core::JOB_EXECUTOR;
    friend class ::CGSim::Core::Platform;
};

inline FileManager* get_file_manager() {
    return &FileManager::instance();
} 

}

}
