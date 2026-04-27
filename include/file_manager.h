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
namespace sg4 = simgrid::s4u;

namespace CGSim {

class FileManager {
public:
    FileManager(const FileManager&) = delete;
    FileManager& operator=(const FileManager&) = delete;

    static FileManager& instance();

    void register_site(sg4::NetZone* site, const std::unordered_map<std::string, long long>& files);
    Job* request_file_location(Job* j);
    unsigned long long request_file_size(const std::string& filename);
    unsigned long long request_remaining_site_storage(const std::string& sitename);
    unsigned long long request_remaining_grid_storage();
    
    void create(const std::string& filename, const unsigned long long& size, const std::string& sitename);
    sg4::IoPtr write(const std::string& filename, const unsigned long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    sg4::IoPtr read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    sg4::CommPtr transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site);
    bool exists(const std::string& filename);
    bool exists(const std::string& filename, const std::string& sitename);
    bool remove(const std::string& filename, const std::string& sitename);
    

private:
    FileManager() = default; 
    std::unordered_map<std::string, std::unordered_set<std::string>> SiteFiles;
    std::unordered_map<std::string, std::unordered_set<std::string>> FileSites;
    std::unordered_map<std::string, unsigned long long> FileSizes;
    std::unordered_map<std::string, unsigned long long> SiteStorages;


};

inline FileManager* get_file_manager() {
    return &FileManager::instance();
} 

}