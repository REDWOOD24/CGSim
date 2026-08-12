#include "file_manager.h"
#include "plugin.h"

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

bool FileManager::is_in_flight(const std::string& filename, const std::string& src_site, const std::string& dst_site){
    return in_flight_transfers.find(generate_transfer_key(filename, src_site, dst_site)) != in_flight_transfers.end();
}

std::string FileManager::generate_transfer_key(const std::string& filename, const std::string& src_site, const std::string& dst_site){
    return filename + "|" + src_site + "|" + dst_site;
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

std::unordered_set<std::string> FileManager::request_site_files(const std::string& sitename)
{
    if(SiteStorages.count(sitename) == 0) throw std::runtime_error("Site: " +sitename+ " does not exist");
    return SiteFiles.at(sitename);
}

std::unordered_set<std::string> FileManager::request_file_sites(const std::string& filename)
{
    if (!exists(filename)) throw std::runtime_error("File: " +filename+ " does not exist");
    return FileSites.at(filename);
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

    if (!exists(filename)) throw std::runtime_error("File: " +filename+ " does not exist");
    auto disk = sg4::Host::by_name(comp_host)->get_disk_by_name(comp_disk);
    auto size_in_bytes = FileSizes.at(filename);
    auto read_activity = sg4::Io::init()->set_disk(disk)->set_size(size_in_bytes)->set_op_type(sg4::Io::OpType::READ);

    read_activity->on_this_start_cb([this,filename,comp_sitename](simgrid::s4u::Io const& io) {
        if (!exists(filename,comp_sitename)) throw std::runtime_error("File: " +filename+
            " does not exist on Site: "+comp_sitename);});
    return read_activity;
}

sg4::CommPtr FileManager::transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, FileTransferDecisionMode mode){

    if(!exists(filename,src_site)) throw std::runtime_error("File: "+filename+" does not exist at Site: "+src_site+" so no transfer");
    if(exists(filename,dst_site))  throw std::runtime_error("File: "+filename+" already exists at Site: "+dst_site+" so no transfer");

    const std::string key = generate_transfer_key(filename, src_site, dst_site);
    if (!in_flight_transfers.insert(key).second) throw std::runtime_error("File transfer: " + key + " is already in progress");
    if(!(CGSim::get_site_manager()->get_site(dst_site)->incoming_file_transfers.insert({filename,src_site}).second)) 
    throw std::runtime_error("File: " + filename + " already being transferred to site:  " + dst_site);

    auto src_host = sg4::Engine::get_instance()->host_by_name_or_null(src_site+"_communication_server");
    auto dst_host = sg4::Engine::get_instance()->host_by_name_or_null(dst_site+"_communication_server");
    auto size     = FileSizes.at(filename);
    auto transfer_activity = sg4::Comm::sendto_init()->set_source(src_host)->set_destination(dst_host)->set_payload_size(size);
    transfer_activity->set_name("Transfer_File_" + filename + "_from_" + src_site + "_to_" + dst_site);
    ongoing_transfers[key] = transfer_activity;

    //@ToDo Remove Redundant block or maybe keep?
    transfer_activity->on_this_start_cb([this,key,mode,filename,size,src_site,dst_site]
        (simgrid::s4u::Comm const& co) 
        {
            if (!started_transfers.insert(co.get_name()).second) return;
        });

    transfer_activity->on_this_completion_cb([this,transfer_activity,key,mode,filename,size,src_site,dst_site]
        (simgrid::s4u::Comm const& co) 
        {
            started_transfers.erase(co.get_name());
            ongoing_transfers.erase(key);
            CGSim::get_site_manager()->get_site(dst_site)->incoming_file_transfers.erase(filename);
            create(filename,size,dst_site);
            if(mode == CGSim::FileTransferDecisionMode::MOVE) remove(filename, src_site);
            in_flight_transfers.erase(key);
        });
    return transfer_activity;
  }

  void FileManager::make_background_transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, CGSim::FileTransferDecisionMode mode, const std::string& policy_name){

    auto t = transfer(filename, src_site, dst_site, mode);
    const auto size = FileSizes.at(filename);

    t->on_this_start_cb([t, this, policy_name, filename, size, src_site, dst_site](simgrid::s4u::Comm const& co) {
        if (!started_background_transfers.insert(co.get_name()).second) return;
        dispatcher->onBackGroundFileTransferStart(filename,size,co,src_site,dst_site,policy_name);
    });

    t->on_this_completion_cb([this, policy_name, filename, size, src_site, dst_site](simgrid::s4u::Comm const& co){
        started_background_transfers.erase(co.get_name());
        dispatcher->onBackGroundFileTransferEnd(filename,size,co,src_site,dst_site,policy_name);
    });
    
    t->start();
}

} 

