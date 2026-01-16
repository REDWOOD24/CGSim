#include "output.h"
#include <filesystem>

void OUTPUT::initialize()
{
    if (initialized) return;
    std::string file_name = platform->get_property("output_file");
    if (std::filesystem::exists(file_name)) std::filesystem::remove(file_name);

    if (sqlite3_open(file_name.c_str(), &db) != SQLITE_OK) {
        throw std::invalid_argument("SQLite file " + file_name + " cannot be opened.");
    }

    if (SQLITE_OK != sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, 0, nullptr)) {
        throw std::runtime_error("Failed to set database connection in WAL mode.");
    }
    initialized = true;
    createEventsTable();
}

void OUTPUT::createEventsTable()
{
    const char* create_stmt =
        "CREATE TABLE EVENTS ("
        "_ID INTEGER PRIMARY KEY AUTOINCREMENT, "
        "EVENT TEXT NOT NULL, "
        "STATE TEXT NOT NULL, "
        "STATUS TEXT NOT NULL, "
        "JOB_ID TEXT NOT NULL, "
        "TIME FLOAT NOT NULL, "
        "METADATA TEXT"
        ");";


    char* errmsg = nullptr;
    int ret = sqlite3_exec(db, create_stmt, nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
        sqlite3_free(errmsg);
        throw std::runtime_error("Database table creation failed");
    }
}

void OUTPUT::insert_event(
                  const std::string& event,
                  const std::string& state,
                  const std::string& job_id,
                  const std::string& status,
                  double time,
                  const std::string& payload)
{
    sqlite3_stmt* stmt;
    std::string sql_insert =
        "INSERT INTO EVENTS (EVENT, STATE, JOB_ID, STATUS, TIME, METADATA) VALUES (?, ?, ?, ?, ?, ?)";

    // Prepare the statement
    int rc = sqlite3_prepare_v2(db, sql_insert.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string("SQLite prepare failed: ") + sqlite3_errmsg(db));

    // Bind parameters (1-based indexing)
    sqlite3_bind_text(stmt, 1, event.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, state.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, job_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, status.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(stmt, 5, time);
    sqlite3_bind_text(stmt, 6, payload.c_str(), -1, SQLITE_TRANSIENT);

    // Execute the statement
    if (sqlite3_step(stmt) != SQLITE_DONE)
    {
        sqlite3_finalize(stmt);  // Ensure cleanup before throwing
        throw std::runtime_error(std::string("SQLite step failed: ") + sqlite3_errmsg(db));
    }

    // Finalize the statement
    sqlite3_finalize(stmt);
}


void OUTPUT::onSimulationStart()
{

}

void OUTPUT::onSimulationEnd()
{

}

void OUTPUT::onJobTransferStart(Job* job, simgrid::s4u::Mess const& me)
{
    std::string payload =
        "{"
        "\"status\":\"" + job->status + "\","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\""
        "}";
    insert_event("JobAllocation", "Start", std::to_string(job->jobid),
                 job->status, sg4::Engine::get_clock(), payload);
}

void OUTPUT::onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me)
{
    std::string payload =
        "{"
        "\"status\":\"" + job->status + "\","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"site_storage_util\":" + std::to_string(calculate_site_storage_util(job->comp_site)) + ","
        "\"grid_storage_util\":" + std::to_string(calculate_grid_storage_util()) + ","
        "\"site_cpu_util\":" + std::to_string(calculate_site_cpu_util(job->comp_site)) + ","
        "\"grid_cpu_util\":" + std::to_string(calculate_grid_cpu_util()) +
        "}";
    insert_event("JobAllocation", "Finished", std::to_string(job->jobid),
                 job->status, sg4::Engine::get_clock(), payload);
}

void OUTPUT::onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex)
{
    std::string payload =
        "{"
        "\"flops\":" + std::to_string(job->flops) + ","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"cores\":" + std::to_string(job->cores) + ","
        "\"speed\":" + std::to_string(job->comp_host_speed) + ","
        "\"start_time\":" + std::to_string(ex.get_start_time()) + ","
        "\"site_cpu_util\":" + std::to_string(calculate_site_cpu_util(job->comp_site)) + ","
        "\"grid_cpu_util\":" + std::to_string(calculate_grid_cpu_util()) +
        "}";
    insert_event("JobExecution", "Start", std::to_string(job->jobid),
                 job->status, ex.get_start_time(), payload);
}

void OUTPUT::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
    std::string payload =
        "{"
        "\"flops\":" + std::to_string(job->flops) + ","
        "\"cores\":" + std::to_string(job->cores) + ","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"speed\":" + std::to_string(job->comp_host_speed) + ","
        "\"cost\":" + std::to_string(ex.get_cost()) + ","
        "\"site_cpu_util\":" + std::to_string(calculate_site_cpu_util(job->comp_site)) + ","
        "\"grid_cpu_util\":" + std::to_string(calculate_grid_cpu_util()) + ","
        "\"duration\":" + std::to_string(ex.get_finish_time() - ex.get_start_time()) + ","
        "\"retries\":" + std::to_string(job->retries) + ","
        "\"queue_time\":" + std::to_string(ex.get_start_time()) +
        "}";
    insert_event("JobExecution", "Finished", std::to_string(job->jobid),
                 job->status, ex.get_finish_time(), payload);
}

void OUTPUT::onFileTransferStart(Job* job, const std::string& filename,
                                 const long long filesize,
                                 simgrid::s4u::Comm const& co,
                                 const std::string& src_site,
                                 const std::string& dst_site)
{
    auto link = get_link(src_site, dst_site);
    std::string payload =
        "{"
        "\"file\":\"" + filename + "\","
        "\"size\":" + std::to_string(filesize) + ","
        "\"source_site\":\"" + src_site + "\","
        "\"destination_site\":\"" + dst_site + "\","
        "\"bandwidth\":" + std::to_string(link->get_bandwidth()) + ","
        "\"latency\":" + std::to_string(link->get_latency()) + ","
        "\"link_load\":" + std::to_string(link->get_load()) + ","
        "\"site_storage_util\":" + std::to_string(calculate_site_storage_util(job->comp_site)) + ","
        "\"grid_storage_util\":" + std::to_string(calculate_grid_storage_util()) +
        "}";
    insert_event("FileTransfer", "Start", std::to_string(job->jobid),
                 job->status, co.get_start_time(), payload);
}

void OUTPUT::onFileTransferEnd(Job* job, const std::string& filename,
                               const long long filesize,
                               simgrid::s4u::Comm const& co,
                               const std::string& src_site,
                               const std::string& dst_site)
{
    auto link = get_link(src_site, dst_site);
    std::string payload =
        "{"
        "\"file\":\"" + filename + "\","
        "\"size\":" + std::to_string(filesize) + ","
        "\"source_site\":\"" + src_site + "\","
        "\"destination_site\":\"" + dst_site + "\","
        "\"duration\":" + std::to_string(co.get_finish_time() - co.get_start_time()) + ","
        "\"bandwidth\":" + std::to_string(link->get_bandwidth()) + ","
        "\"latency\":" + std::to_string(link->get_latency()) + ","
        "\"link_load\":" + std::to_string(link->get_load()) + ","
        "\"site_storage_util\":" + std::to_string(calculate_site_storage_util(job->comp_site)) + ","
        "\"grid_storage_util\":" + std::to_string(calculate_grid_storage_util()) +
        "}";
    insert_event("FileTransfer", "Finished", std::to_string(job->jobid),
                 job->status, co.get_finish_time(), payload);
}

void OUTPUT::onFileReadStart(Job* job, const std::string& filename,
                            const long long filesize, simgrid::s4u::Io const& io)
{
    std::string payload =
        "{"
        "\"file\":\"" + filename + "\","
        "\"size\":" + std::to_string(filesize) + ","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"disk\":\"" + job->disk + "\","
        "\"disk_read_bw\":" + std::to_string(job->disk_read_bw) +
        "}";
    insert_event("FileRead", "Start", std::to_string(job->jobid),
                 job->status, io.get_start_time(), payload);
}

void OUTPUT::onFileReadEnd(Job* job, const std::string& filename,
                           const long long filesize, simgrid::s4u::Io const& io)
{
    std::string payload =
        "{"
        "\"file\":\"" + filename + "\","
        "\"size\":" + std::to_string(filesize) + ","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"disk\":\"" + job->disk + "\","
        "\"disk_read_bw\":\"" + std::to_string(job->disk_read_bw) + "\","
        "\"duration\":" + std::to_string(io.get_finish_time() - io.get_start_time()) +
        "}";
    insert_event("FileRead", "Finished", std::to_string(job->jobid),
                 job->status, io.get_finish_time(), payload);
}

void OUTPUT::onFileWriteStart(Job* job, const std::string& filename,
                             const long long filesize, simgrid::s4u::Io const& io)
{
    std::string payload =
        "{"
        "\"file\":\"" + filename + "\","
        "\"size\":" + std::to_string(filesize) + ","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"disk\":\"" + job->disk + "\","
        "\"disk_write_bw\":\"" + std::to_string(job->disk_write_bw) + "\","
        "\"site_storage_util\":" + std::to_string(calculate_site_storage_util(job->comp_site)) + ","
        "\"grid_storage_util\":" + std::to_string(calculate_grid_storage_util()) +
        "}";
    insert_event("FileWrite", "Start", std::to_string(job->jobid),
                 job->status, io.get_start_time(), payload);
}

void OUTPUT::onFileWriteEnd(Job* job, const std::string& filename,
                           const long long filesize, simgrid::s4u::Io const& io)
{
    std::string payload =
        "{"
        "\"file\":\"" + filename + "\","
        "\"size\":" + std::to_string(filesize) + ","
        "\"site\":\"" + job->comp_site + "\","
        "\"host\":\"" + job->comp_host + "\","
        "\"duration\":" + std::to_string(io.get_finish_time() - io.get_start_time()) + ","
        "\"disk\":\"" + job->disk + "\","
        "\"disk_write_bw\":\"" + std::to_string(job->disk_write_bw) + "\","
        "\"site_storage_util\":" + std::to_string(calculate_site_storage_util(job->comp_site)) + ","
        "\"grid_storage_util\":" + std::to_string(calculate_grid_storage_util()) +
        "}";
    insert_event("FileWrite", "Finished", std::to_string(job->jobid),
                 job->status, io.get_finish_time(), payload);
}

sg4::Link* OUTPUT::get_link(const std::string& src_site, const std::string& dst_site)
{

    sg4::Link* link = sg4::Link::by_name_or_null("link_" + src_site + ":" + dst_site);
    if (!link) link = sg4::Link::by_name_or_null("link_" + dst_site + ":" + src_site);
    if (!link) throw std::runtime_error("Link not found");
    return link;
}

double OUTPUT::calculate_grid_cpu_util()
{
    double cores_used = 0;
    double total_cores = std::stoul(platform->get_property("grid_storage"));
    for (const auto& host : sg4::Engine::get_instance()->get_all_hosts()) {
        cores_used += host->extension<HostExtensions>()->get_cores_used();
    }
    return cores_used/total_cores;
}

double OUTPUT::calculate_site_cpu_util(std::string& site_name)
{
    auto site = sg4::Engine::get_instance()->netzone_by_name_or_null(site_name);
    double total_cores = std::stoul(site->get_property("total_cores"));
    double cores_used = 0;
    for (const auto& host : site->get_all_hosts()) {
        cores_used += host->extension<HostExtensions>()->get_cores_used();
    }
    return cores_used/total_cores;
}

double OUTPUT::calculate_grid_storage_util()
{
    double total_storage = std::stoull(platform->get_property("grid_storage"));
    double remaining_storage = CGSim::FileManager::request_remaining_grid_storage();
    return (1.0-remaining_storage/total_storage);
}

double OUTPUT::calculate_site_storage_util(std::string& site_name)
{
    auto   site = sg4::Engine::get_instance()->netzone_by_name_or_null(site_name);
    double total_storage = std::stoull(site->get_property("storage_capacity_bytes"));
    double remaining_storage = CGSim::FileManager::request_remaining_site_storage(site_name);
    return (1.0-remaining_storage/total_storage);
}

