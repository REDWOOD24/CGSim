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
#include "site_manager.h"

namespace sg4 = simgrid::s4u;
class DispatcherPlugin;

namespace CGSim {

enum class FileTransferDecisionMode {
    COPY,
    MOVE
};

class FileManager {
public:
    FileManager(const FileManager&) = delete;
    FileManager& operator=(const FileManager&) = delete;
    static void set_dispatcher(std::shared_ptr<DispatcherPlugin>& d){dispatcher = d;}

    static FileManager& instance();
    bool exists(const std::string& filename);
    bool exists(const std::string& filename, const std::string& sitename);
    void register_site(sg4::NetZone* site, const std::unordered_map<std::string, long long>& files);
    Job* request_file_location(Job* j);
    std::unordered_set<std::string> request_site_files(const std::string& sitename);
    std::unordered_set<std::string> request_file_sites(const std::string& filename);
    unsigned long long request_file_size(const std::string& filename);
    unsigned long long request_remaining_site_storage(const std::string& sitename);
    unsigned long long request_remaining_grid_storage();

    void create(const std::string& filename, const unsigned long long& size, const std::string& sitename);
    sg4::IoPtr write(const std::string& filename, const unsigned long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    sg4::IoPtr read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    sg4::CommPtr transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, FileTransferDecisionMode mode = CGSim::FileTransferDecisionMode::COPY);
    bool remove(const std::string& filename, const std::string& sitename);

    void make_background_transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, CGSim::FileTransferDecisionMode mode, const std::string& policy_name = "");
    bool is_in_flight(const std::string& filename, const std::string& src_site, const std::string& dst_site);
    std::string generate_transfer_key(const std::string& filename,const std::string& src_site,const std::string& dst_site);

    inline static std::unordered_set<std::string> in_flight_transfers = {};
    std::unordered_map<std::string, sg4::CommPtr> ongoing_transfers;

    

private:
    FileManager() = default; 
    std::unordered_map<std::string, std::unordered_set<std::string>> SiteFiles;
    std::unordered_map<std::string, std::unordered_set<std::string>> FileSites;
    std::unordered_map<std::string, unsigned long long> FileSizes;
    std::unordered_map<std::string, unsigned long long> SiteStorages;
    inline static std::shared_ptr<DispatcherPlugin>     dispatcher;

    //Needed for Background transfers
    std::unordered_set<std::string> started_transfers; //Hack to avoid double start comm callback
    std::unordered_set<std::string> started_background_transfers; //Hack to avoid double start comm callback


};

inline FileManager* get_file_manager() {
    return &FileManager::instance();
} 

}