#include "file_manager.h"

namespace CGSim {

std::unordered_map<std::string,std::unordered_set<std::string>>  FileManager::SiteFiles;
std::unordered_map<std::string, std::unordered_set<std::string>> FileManager::FileSites;
std::unordered_map<std::string, unsigned long long>              FileManager::FileSizes;
std::unordered_map<std::string,  long long>              FileManager::SiteStorages;
std::unordered_map<std::string, long long>              FileManager::SiteCapacities;


FileManager* FileManager::instance() {
    static FileManager instance; 
    return &instance;
}

bool FileManager::exists(const std::string& filename)
{
return FileSizes.count(filename) > 0;
}

bool FileManager::exists(const std::string& filename, const std::string& sitename)
{
    auto it = SiteFiles.find(sitename);
    if (it == SiteFiles.end()) {
        return false;
    }
    return it->second.count(filename) > 0;
}

bool FileManager::remove(const std::string& filename, const std::string& sitename)
{
    // Check if file exists on this site
    if (SiteFiles.count(sitename) == 0 || SiteFiles.at(sitename).count(filename) == 0) {
        return false;
    }
    
    // Get file size before removing from tracking
    unsigned long long size = FileSizes.at(filename);
    
    // Remove file from site's file list
    SiteFiles.at(sitename).erase(filename);
    
    // Remove site from file's site list
    bool site_removed = false;
    if (FileSites.count(filename) > 0) {
        site_removed = FileSites.at(filename).erase(sitename) > 0;
        
        // If file no longer exists on any site, remove it from FileSizes
        if (FileSites.at(filename).empty()) {
            FileSites.erase(filename);
            FileSizes.erase(filename);
        }
    }
    
    // Restore storage capacity on the source site
    SiteStorages[sitename] += size;
    
    return site_removed;
}

void FileManager::register_site(sg4::NetZone* site, const std::unordered_map<std::string, long long>& files){

    const std::string& site_name = site->get_name();
    long long capacity = std::stoll(site->get_property("storage_capacity_bytes"));
    SiteCapacities[site_name] = capacity;
    SiteStorages[site_name] = capacity;

    for (const auto& [file, size] : files) {
        SiteFiles[site_name].insert(file);
        FileSizes[file] = size;
        FileSites[file].insert(site_name);
        SiteStorages[site_name] -= size;
        if (SiteStorages[site_name] < 0) throw std::runtime_error("Site "+site_name+" is out of storage");
    }
}

Job* FileManager::request_file_location(Job* j){
    for(auto& [file,file_info]: j->input_files)
    {
        if (!exists(file)) throw std::runtime_error("File: " +file+ " does not exist");
        file_info.first  = FileSizes.at(file);
        file_info.second = FileSites.at(file);
    }
    return j;
}

unsigned long long FileManager::request_file_size(const std::string& filename)
{
    if (!exists(filename)) throw std::runtime_error("File: " +filename+ " does not exist");
    return FileSizes.at(filename);

}

unsigned long long FileManager::request_remaining_grid_storage() {
    long long total = 0;
    for (const auto& [key, value] : SiteStorages) {
        total += value;
    }
    return total;
}

unsigned long long FileManager::request_remaining_site_storage(const std::string& sitename) {
    auto it = SiteStorages.find(sitename);
    if (it == SiteStorages.end()) {
        // If the site was not registered in storage maps, treat remaining storage as 0
        // instead of aborting the simulation.
        return 0;
    }
    return it->second;
}

long long FileManager::get_site_capacity(const std::string& sitename) {
    auto it = SiteCapacities.find(sitename);
    if (it == SiteCapacities.end()) {
        // Unknown site capacity: report 0 instead of throwing
        return 0;
    }
    return it->second;
}

std::vector<std::string> FileManager::get_site_names() {
    std::vector<std::string> names;
    names.reserve(SiteStorages.size());
    for (const auto& [name, _] : SiteStorages) {
        names.push_back(name);
    }
    return names;
}

std::vector<std::string> FileManager::get_files_on_site(const std::string& sitename) {
    if (SiteFiles.count(sitename) == 0) throw std::runtime_error("Site " + sitename + " does not exist");
    std::vector<std::string> files(SiteFiles.at(sitename).begin(), SiteFiles.at(sitename).end());
    return files;
}

void FileManager::create(const std::string& filename, const unsigned long long& size, const std::string& sitename){

    // Ensure the site entry exists in our maps instead of aborting if it had no files initially.
    // register_site() always populates SiteStorages and SiteCapacities for valid sites, but
    // SiteFiles may be empty if the site started with zero files. In that case we just
    // create an empty set here.
    if (SiteFiles.count(sitename) == 0) {
        SiteFiles[sitename] = {};
    }

    SiteFiles[sitename].insert(filename);
    FileSites[filename].insert(sitename);
    FileSizes[filename] = size;
    SiteStorages[sitename] -= size;

    if (SiteStorages[sitename] < 0) throw std::runtime_error("Site "+sitename+" is out of storage");
}

sg4::IoPtr FileManager::write(const std::string& filename, const unsigned long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk){

    // Ensure the site exists in our bookkeeping even if it started with zero files.
    // register_site() guarantees that a valid site will have entries in SiteStorages
    // and SiteCapacities; if SiteFiles doesn't yet have an entry, initialize it.
    if (SiteFiles.count(comp_sitename) == 0) {
        SiteFiles[comp_sitename] = {};
    }
    auto disk = sg4::Host::by_name(comp_host)->get_disk_by_name(comp_disk);
    auto write_activity = sg4::Io::init()->set_disk(disk)->set_size(size)->set_op_type(sg4::Io::OpType::WRITE);
    write_activity->on_this_completion_cb([filename,size,comp_sitename](simgrid::s4u::Io const& io) {
        create(filename,size,comp_sitename);
      });
    return write_activity;
}

sg4::IoPtr FileManager::read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk){

    if (!exists(filename)) throw std::runtime_error("File: " +filename+ " does not exist");
    auto disk = sg4::Host::by_name(comp_host)->get_disk_by_name(comp_disk);
    auto size_in_bytes = FileSizes.at(filename);
    auto read_activity = sg4::Io::init()->set_disk(disk)->set_size(size_in_bytes)->set_op_type(sg4::Io::OpType::READ);

    read_activity->on_this_start_cb([filename,comp_sitename](simgrid::s4u::Io const& io) {
        if (!exists(filename,comp_sitename)) throw std::runtime_error("File: " +filename+
            " does not exist on Site: "+comp_sitename+" does not exist");});
    return read_activity;
}

} 

