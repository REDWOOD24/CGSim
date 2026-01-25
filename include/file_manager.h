#pragma once
#include <string>
#include <unordered_map>
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

    static void register_site(sg4::NetZone* site, const std::unordered_map<std::string, long long>& files);
    static Job* request_file_location(Job* j);
    static unsigned long long request_file_size(const std::string& filename);
    static unsigned long long request_remaining_site_storage(const std::string& sitename);
    static unsigned long long request_remaining_grid_storage();
    
    static void create(const std::string& filename, const unsigned long long& size, const std::string& sitename);
    static sg4::IoPtr write(const std::string& filename, const unsigned long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    static sg4::IoPtr read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk);
    static sg4::CommPtr transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site);
    static bool exists(const std::string& filename);
    static bool exists(const std::string& filename, const std::string& sitename);
    static bool remove(const std::string& filename, const std::string& sitename);
    
    
    static FileManager* instance();


private:
    FileManager() = default; 
    static std::unordered_map<std::string, std::unordered_set<std::string>> SiteFiles;
    static std::unordered_map<std::string, std::unordered_set<std::string>> FileSites;
    static std::unordered_map<std::string, unsigned long long> FileSizes;
    static std::unordered_map<std::string, long long> SiteStorages;


};

static inline FileManager* get_file_manager() {
    return FileManager::instance();
}

} 

