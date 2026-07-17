#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <optional>
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
    std::string transfer_key(const std::string& filename,const std::string& src_site,const std::string& dst_site) const;
    std::size_t active_background_transfer_count() const;

    /** Return the live Comm for an exact (file, src, dst) route, if any. */
    std::optional<sg4::CommPtr> find_in_flight_comm(
        const std::string& filename,
        const std::string& src_site,
        const std::string& dst_site) const;

    struct InFlightToDestination {
        std::string src_site;
        sg4::CommPtr comm;
    };
    /** Return any in-flight Comm delivering filename to dst_site. */
    std::optional<InFlightToDestination> find_in_flight_to_destination(
        const std::string& filename,
        const std::string& dst_site) const;

    /**
     * Return any in-flight Comm for filename (any destination).
     * Used for remote-bound observation when the resting catalog is empty.
     */
    std::optional<InFlightToDestination> find_any_in_flight(
        const std::string& filename) const;

    /** Reference-counted local-read pin on (filename, site). */
    std::string replica_pin_key(const std::string& filename, const std::string& sitename) const;
    void pin_replica(const std::string& filename, const std::string& sitename);
    void unpin_replica(const std::string& filename, const std::string& sitename);
    bool is_pinned(const std::string& filename, const std::string& sitename) const;
    unsigned int pin_count(const std::string& filename, const std::string& sitename) const;

    inline static std::unordered_set<std::string> in_flight_transfers = {};
    

private:
    FileManager() = default; 
    std::unordered_map<std::string, std::unordered_set<std::string>> SiteFiles;
    std::unordered_map<std::string, std::unordered_set<std::string>> FileSites;
    std::unordered_map<std::string, unsigned long long> FileSizes;
    std::unordered_map<std::string, unsigned long long> SiteStorages;
    /** Active local-read pins: key = filename|site, value = reference count. */
    std::unordered_map<std::string, unsigned int> ReplicaPinCounts;
    inline static std::shared_ptr<DispatcherPlugin>     dispatcher;

    std::unordered_map<std::string, std::pair<sg4::CommPtr, bool>> active_background_transfers;
    std::unordered_set<std::string> started_transfers;
    std::unordered_set<std::string> background_callback_comm_names;
    std::unordered_map<std::string, sg4::CommPtr> active_transfers;

    void attach_background_transfer_callbacks(
        const sg4::CommPtr& comm,
        const std::string& policy_name,
        const std::string& filename,
        unsigned long long size,
        const std::string& src_site,
        const std::string& dst_site);
};

inline FileManager* get_file_manager() {
    return &FileManager::instance();
} 

}
