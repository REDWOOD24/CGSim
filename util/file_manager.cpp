#include "file_manager.h"

namespace CGSim {

std::unordered_map<std::string,std::unordered_set<std::string>> FileManager::SiteFiles;
std::unordered_map<std::string, long long> FileManager::FileSizes;

FileManager* FileManager::instance() {
    static FileManager instance; 
    return &instance;
}

void FileManager::register_site(sg4::NetZone* site, const std::unordered_map<std::string, long long>& files){
  for(const auto& [file,size] : files){
    SiteFiles[site->get_name()].insert(file);
    FileSizes[site->get_name()]= size;
    }

}

Job* FileManager::request_file_location(Job* j){
    for(auto& [file,file_info]: j->input_files)
    {
        file_info.first  = FileSizes.at(file);
        file_info.second = FileSites.at(file);
    }
    return j;
}

void FileManager::create(const std::string& filename, const long long& size, const std::string& sitename){

    //auto site = simgrid::s4u::Engine::get_instance()->netzone_by_name_or_null(sitename);
    //auto hosts = site->get_all_hosts();
    
    //Check if Site exists
    if (!SiteFiles.count(sitename)) throw std::runtime_error("Site does not exist");
    SiteFiles.at(sitename).insert(filename);
    FileSites.at(filename).insert(sitename);
    FileSizes[filename] = size;
    
}

sg4::IoPtr FileManager::write(const std::string& filename, const long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& file_host, const std::string& comp_disk){
    //disk->extension<sg4::FileSystemDiskExt>()->decrease_used_size(size);
    create(filename,size,comp_sitename);
    auto disk = simgrid::s4u::Engine::get_instance()->host_by_name_or_null(comp_host)->get_disk_by_name(comp_disk);

    return simgrid::s4u::Io::init()->set_disk(disk)->set_size(size)->set_op_type(sg4::Io::OpType::WRITE);
}


sg4::IoPtr FileManager::read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& file_host, const std::string& comp_disk){
    
    if(!exists(filename,comp_sitename)) return nullptr;
    //auto disk_name = 
    
    //auto site = 
    auto disk = nullptr;
    auto size_in_bytes = 10;

    return simgrid::s4u::Io::init()->set_disk(disk)->set_size(size_in_bytes)->set_op_type(sg4::Io::OpType::READ);
}


bool FileManager::exists(const std::string& filename, const std::string& sitename) {
    return FileSizes.find(filename) != FileSizes.end();
}

bool FileManager::remove(const std::string& filename, const std::string& sitename) {
    return (FileSizes.erase(filename) > 0 && SiteFiles.at(sitename).erase(filename) > 0);
}

} 

