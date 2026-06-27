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

bool FileManager::remove(const std::string& filename, const std::string& sitename)
{
    if (!exists(filename, sitename))
        return false;

    auto size = FileSizes.at(filename);

    SiteFiles.at(sitename).erase(filename);
    FileSites.at(filename).erase(sitename);
    SiteStorages.at(sitename) += size;
    if(FileSites.at(filename).empty()) {FileSites.erase(filename); FileSizes.erase(filename);}

    return true;
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
    for(auto& file: j->input_files)
    {
        if (!exists(file)) throw std::runtime_error("File: " +file+ " does not exist");
        j->input_files_sizes_locations[file] = {FileSizes.at(file), FileSites.at(file)};
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
    write_activity->on_this_completion_cb([this,filename,size,comp_sitename](simgrid::s4u::Io const& io) 
    {create(filename,size,comp_sitename);});
    return write_activity;
}

sg4::IoPtr FileManager::read(const std::string& filename, const std::string& comp_sitename, const std::string& comp_host, const std::string& comp_disk){

    if (!FileManager::instance().exists(filename)) throw std::runtime_error("File: " +filename+ " does not exist");
    auto disk = sg4::Host::by_name(comp_host)->get_disk_by_name(comp_disk);
    auto size_in_bytes = FileSizes.at(filename);
    auto read_activity = sg4::Io::init()->set_disk(disk)->set_size(size_in_bytes)->set_op_type(sg4::Io::OpType::READ);

    read_activity->on_this_start_cb([this,filename,comp_sitename](simgrid::s4u::Io const& io) {
        if (!exists(filename,comp_sitename)) throw std::runtime_error("File: " +filename+
            " does not exist on Site: "+comp_sitename);});
    return read_activity;
}

sg4::CommPtr FileManager::transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site){

    if(!exists(filename,src_site)) throw std::runtime_error("File: "+filename+" does not exist at Site: "+src_site+" so no transfer");
    if(exists(filename,dst_site))  throw std::runtime_error("File: "+filename+" already exists at Site: "+dst_site+" so no transfer");
    auto src_host = sg4::Engine::get_instance()->host_by_name_or_null(src_site+"_communication_server");
    auto dst_host = sg4::Engine::get_instance()->host_by_name_or_null(dst_site+"_communication_server");
    auto size     = FileSizes.at(filename);
    auto transfer_activity = sg4::Comm::sendto_init()->set_source(src_host)->set_destination(dst_host)->set_payload_size(size);
    transfer_activity->set_name("Transfer_File_" + filename + "_from_" + src_site + "_to_" + dst_site);
    transfer_activity->on_this_completion_cb([this,filename,size,src_site,dst_site]
        (simgrid::s4u::Comm const& co) {create(filename,size,dst_site);});
    return transfer_activity;
  }

} 

