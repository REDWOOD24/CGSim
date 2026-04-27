#include "file_manager.h"

namespace CGSim {

FileManager& FileManager::instance()
{
    static FileManager fm;
    return fm;
}

bool FileManager::exists(const std::string& filename)
{
return FileSizes.count(filename) > 0;
}

bool FileManager::exists(const std::string& filename, const std::string& sitename)
{
if (SiteStorages.count(sitename) == 0) throw std::runtime_error("Site: "+sitename+" does not exist");
return SiteFiles.at(sitename).count(filename) > 0;
}

//Fix Later, Dont remove FileSizes as that erases grid wide
bool FileManager::remove(const std::string& filename, const std::string& sitename)
{
return (FileSizes.erase(filename) > 0 && SiteFiles.at(sitename).erase(filename) > 0 && FileSites.at(filename).erase(sitename) > 0);
}

void FileManager::register_site(sg4::NetZone* site, const std::unordered_map<std::string, long long>& files){

    const std::string& site_name = site->get_name();
    SiteStorages[site_name] = std::stoull(site->get_property("storage_capacity_bytes"));
    SiteFiles[site_name];

    for (const auto& [file, size] : files) {
        if (SiteStorages[site_name] < size) throw std::runtime_error("Site: "+site_name+" is out of storage");
        SiteFiles[site_name].insert(file);
        FileSizes[file] = size;
        FileSites[file].insert(site_name);
        SiteStorages[site_name] -= size;
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
    unsigned long long total = 0;
    for (const auto& [key, value] : SiteStorages) {
        total += value;
    }
    return total;
}

unsigned long long FileManager::request_remaining_site_storage(const std::string& sitename) {
    if (SiteStorages.count(sitename) == 0) throw std::runtime_error("Site: "+sitename+" does not exist");
    return SiteStorages.at(sitename);
}

void FileManager::create(const std::string& filename, const unsigned long long& size, const std::string& sitename){

    //Check if File already exists on the site
    if (exists(filename, sitename)) return;

     //Check if Site has enough storage for the file
    if (SiteStorages[sitename] < size) throw std::runtime_error("Site: "+sitename+" is out of storage");


    SiteFiles[sitename].insert(filename);
    FileSites[filename].insert(sitename);
    FileSizes[filename] = size;
    SiteStorages[sitename] -= size;
}

sg4::IoPtr FileManager::write(const std::string& filename, const unsigned long long& size, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk){

    if (SiteFiles.count(comp_sitename) == 0) throw std::runtime_error("Site: "+comp_sitename+" does not exist");
    if (exists(filename)) throw std::runtime_error("File: "+filename+" already exists on the grid");
    auto disk = sg4::Host::by_name(comp_host)->get_disk_by_name(comp_disk);
    auto write_activity = sg4::Io::init()->set_disk(disk)->set_size(size)->set_op_type(sg4::Io::OpType::WRITE);
    write_activity->on_this_completion_cb([filename,size,comp_sitename](simgrid::s4u::Io const& io) {
        FileManager::instance().create(filename,size,comp_sitename);
      });
    return write_activity;
}

sg4::IoPtr FileManager::read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk){

    if (!FileManager::instance().exists(filename)) throw std::runtime_error("File: " +filename+ " does not exist");
    auto disk = sg4::Host::by_name(comp_host)->get_disk_by_name(comp_disk);
    auto size_in_bytes = FileSizes.at(filename);
    auto read_activity = sg4::Io::init()->set_disk(disk)->set_size(size_in_bytes)->set_op_type(sg4::Io::OpType::READ);

    read_activity->on_this_start_cb([filename,comp_sitename](simgrid::s4u::Io const& io) {
        if (!FileManager::instance().exists(filename,comp_sitename)) throw std::runtime_error("File: " +filename+
            " does not exist on Site: "+comp_sitename);});
    return read_activity;
}

sg4::CommPtr FileManager::transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site){

    if(!exists(filename,src_site)) throw std::runtime_error("File: "+filename+" does not exist at Site: "+src_site+" so no transfer");
    if(exists(filename,dst_site)) throw std::runtime_error("File: "+filename+" already exists at Site: "+dst_site+" so no transfer");
    auto src_host = sg4::Engine::get_instance()->host_by_name_or_null(src_site+"_communication_server");
    auto dst_host = sg4::Engine::get_instance()->host_by_name_or_null(dst_site+"_communication_server");
    auto size     = FileSizes.at(filename);
    auto transfer_activity = sg4::Comm::sendto_init()->set_source(src_host)->set_destination(dst_host)->set_payload_size(size);
    transfer_activity->set_name("Transfer_File_" + filename + "_from_" + src_site + "_to_" + dst_site);
    return transfer_activity;
  }

} 

