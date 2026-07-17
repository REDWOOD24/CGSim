#include "file_manager.h"
#include "DispatcherPlugin.h"

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

    // Refuse removal while a local-read pin is held (MOVE completion must not steal).
    if (is_pinned(filename, sitename)) {
        return false;
    }

    auto size = FileSizes.at(filename);

    SiteFiles.at(sitename).erase(filename);
    FileSites.at(filename).erase(sitename);
    SiteStorages.at(sitename) += size;
    if(FileSites.at(filename).empty()) {FileSites.erase(filename); FileSizes.erase(filename);}

    return true;
}

bool FileManager::is_in_flight(const std::string& filename, const std::string& src_site, const std::string& dst_site){
    return in_flight_transfers.find(transfer_key(filename, src_site, dst_site)) != in_flight_transfers.end();
}

std::string FileManager::transfer_key(const std::string& filename, const std::string& src_site, const std::string& dst_site) const {
    return filename + "|" + src_site + "|" + dst_site;
}

std::optional<sg4::CommPtr> FileManager::find_in_flight_comm(
    const std::string& filename,
    const std::string& src_site,
    const std::string& dst_site) const
{
    const std::string key = transfer_key(filename, src_site, dst_site);
    auto it = active_transfers.find(key);
    if (it == active_transfers.end() || it->second == nullptr) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<FileManager::InFlightToDestination> FileManager::find_in_flight_to_destination(
    const std::string& filename,
    const std::string& dst_site) const
{
    const std::string prefix = filename + "|";
    const std::string suffix = "|" + dst_site;
    for (const auto& [key, comm] : active_transfers) {
        if (comm == nullptr) {
            continue;
        }
        if (key.size() < prefix.size() + suffix.size()) {
            continue;
        }
        if (key.compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        if (key.compare(key.size() - suffix.size(), suffix.size(), suffix) != 0) {
            continue;
        }
        const std::string src = key.substr(prefix.size(), key.size() - prefix.size() - suffix.size());
        if (src.empty()) {
            continue;
        }
        return InFlightToDestination{src, comm};
    }
    return std::nullopt;
}

std::optional<FileManager::InFlightToDestination> FileManager::find_any_in_flight(
    const std::string& filename) const
{
    const std::string prefix = filename + "|";
    for (const auto& [key, comm] : active_transfers) {
        if (comm == nullptr) {
            continue;
        }
        if (key.compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        // key = filename|src|dst
        const std::string rest = key.substr(prefix.size());
        const auto sep = rest.find('|');
        if (sep == std::string::npos || sep == 0 || sep + 1 >= rest.size()) {
            continue;
        }
        const std::string src = rest.substr(0, sep);
        return InFlightToDestination{src, comm};
    }
    return std::nullopt;
}

std::string FileManager::replica_pin_key(const std::string& filename, const std::string& sitename) const
{
    return filename + "|" + sitename;
}

void FileManager::pin_replica(const std::string& filename, const std::string& sitename)
{
    ReplicaPinCounts[replica_pin_key(filename, sitename)] += 1;
}

void FileManager::unpin_replica(const std::string& filename, const std::string& sitename)
{
    const std::string key = replica_pin_key(filename, sitename);
    auto it = ReplicaPinCounts.find(key);
    if (it == ReplicaPinCounts.end()) {
        return; // idempotent: unpin with count already zero
    }
    if (it->second <= 1) {
        ReplicaPinCounts.erase(it);
    } else {
        it->second -= 1;
    }
}

bool FileManager::is_pinned(const std::string& filename, const std::string& sitename) const
{
    auto it = ReplicaPinCounts.find(replica_pin_key(filename, sitename));
    return it != ReplicaPinCounts.end() && it->second > 0;
}

unsigned int FileManager::pin_count(const std::string& filename, const std::string& sitename) const
{
    auto it = ReplicaPinCounts.find(replica_pin_key(filename, sitename));
    return it == ReplicaPinCounts.end() ? 0u : it->second;
}

std::size_t FileManager::active_background_transfer_count() const
{
    std::size_t count = 0;
    for (const auto& entry : active_background_transfers) {
        if (entry.second.second) {
            count++;
        }
    }
    return count;
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

    if (exists(filename, sitename)) return;

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
    // Existence at start is validated by Actions::read_file_async (which also releases the local-read pin).
    (void)comp_sitename;
    return read_activity;
}

sg4::CommPtr FileManager::transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, FileTransferDecisionMode mode){

    if(!exists(filename,src_site)) throw std::runtime_error("File: "+filename+" does not exist at Site: "+src_site+" so no transfer");
    if(exists(filename,dst_site))  throw std::runtime_error("File: "+filename+" already exists at Site: "+dst_site+" so no transfer");

    // Job MOVE with a pinned source: keep source (COPY semantics) so local readers are safe.
    if (mode == FileTransferDecisionMode::MOVE && is_pinned(filename, src_site)) {
        mode = FileTransferDecisionMode::COPY;
    }

    const std::string key = transfer_key(filename, src_site, dst_site);
    if (!in_flight_transfers.insert(key).second) {
        throw std::runtime_error("File transfer: " + key + " is already in progress");
    }

    auto src_host = sg4::Engine::get_instance()->host_by_name_or_null(src_site + "_communication_server");
    auto dst_host = sg4::Engine::get_instance()->host_by_name_or_null(dst_site + "_communication_server");
    auto size     = FileSizes.at(filename);
    auto transfer_activity = sg4::Comm::sendto_init()->set_source(src_host)->set_destination(dst_host)->set_payload_size(size);
    transfer_activity->set_name("Transfer_File_" + filename + "_from_" + src_site + "_to_" + dst_site);
    active_transfers[key] = transfer_activity;
    transfer_activity->on_this_completion_cb([this,key,mode,filename,size,src_site,dst_site]
        (simgrid::s4u::Comm const& co)
        {
            create(filename,size,dst_site);
            // remove() itself refuses while pinned (covers MOVE that started before the pin).
            if(mode == CGSim::FileTransferDecisionMode::MOVE) remove(filename, src_site);
            in_flight_transfers.erase(key);
            active_transfers.erase(key);
            background_callback_comm_names.erase(co.get_name());
        });
    return transfer_activity;
}

void FileManager::attach_background_transfer_callbacks(
    const sg4::CommPtr& comm,
    const std::string& policy_name,
    const std::string& filename,
    unsigned long long size,
    const std::string& src_site,
    const std::string& dst_site)
{
    comm->on_this_start_cb([this, policy_name, filename, size, src_site, dst_site](simgrid::s4u::Comm const& co) {
        if (!started_transfers.insert(co.get_name()).second) return;
        dispatcher->onBackGroundFileTransferStart(filename,size,co,src_site,dst_site,policy_name);
    });

    comm->on_this_completion_cb([this, policy_name, filename, size, src_site, dst_site](simgrid::s4u::Comm const& co){
        started_transfers.erase(co.get_name());
        dispatcher->onBackGroundFileTransferEnd(filename,size,co,src_site,dst_site,policy_name);
        active_background_transfers[co.get_name()].second = false;
        });
}

void FileManager::make_background_transfer(const std::string& filename, const std::string& src_site, const std::string& dst_site, CGSim::FileTransferDecisionMode mode, const std::string& policy_name){

    if (exists(filename, dst_site)) {
        return;
    }
    if (is_in_flight(filename, src_site, dst_site)) {
        return;
    }
    // Proactive/drop-in MOVE: skip entirely while source has a local-read pin. COPY is allowed.
    if (mode == FileTransferDecisionMode::MOVE && is_pinned(filename, src_site)) {
        return;
    }

    auto comm = transfer(filename, src_site, dst_site, mode);
    const auto size = FileSizes.at(filename);
    const auto comm_name = comm->get_name();

    if (background_callback_comm_names.insert(comm_name).second) {
        attach_background_transfer_callbacks(
            comm, policy_name, filename, size, src_site, dst_site);
    }

    comm->start();
    active_background_transfers[comm_name] = {comm, true};

    for (auto it = active_background_transfers.begin(); it != active_background_transfers.end();)
    {if (!it->second.second) it = active_background_transfers.erase(it); else ++it;}
}

} 
