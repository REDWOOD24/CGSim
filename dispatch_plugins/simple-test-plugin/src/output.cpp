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
        "JOB_ID TEXT NOT NULL, "
        "TIME FLOAT NOT NULL, "
        "PAYLOAD TEXT"
        ");";


    char* errmsg = nullptr;
    int ret = sqlite3_exec(db, create_stmt, nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
        sqlite3_free(errmsg);
        throw std::runtime_error("Database table creation failed");
    }
}

void OUTPUT::insert_event(sqlite3_stmt* stmt,
                  const std::string& event,
                  const std::string& state,
                  const std::string& job_id,
                  double time,
                  const std::string& payload)
{
    sqlite3_bind_text(stmt, 1, event.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, state.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, job_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(stmt, 4, time);

    if (!payload.empty())
        sqlite3_bind_text(stmt, 5, payload.c_str(), -1, SQLITE_TRANSIENT);
    else
        sqlite3_bind_null(stmt, 5);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
}

double OUTPUT::calculate_grid_cpu_util() {
    double cores_used = 0;
    double total_cores = std::stoul(platform->get_property("grid_storage"));
    for (const auto& host : sg4::Engine::get_instance()->get_all_hosts()) {
        cores_used += host->extension<HostExtensions>()->get_cores_used();
    }
    return cores_used/total_cores;
}

double OUTPUT::calculate_site_cpu_util(std::string& site_name) {
    auto site = sg4::Engine::get_instance()->netzone_by_name_or_null(site_name);
    double total_cores = std::stoul(site->get_property("total_cores"));
    double cores_used = 0;
    for (const auto& host : site->get_all_hosts()) {
        cores_used += host->extension<HostExtensions>()->get_cores_used();
    }
    return cores_used/total_cores;
}

double OUTPUT::calculate_grid_storage_util() {
    double total_storage = std::stoull(platform->get_property("grid_storage"));
    double remaining_storage = CGSim::FileManager::request_remaining_grid_storage();
    return (1.0-remaining_storage/total_storage);
}

double OUTPUT::calculate_site_storage_util(std::string& site_name) {
    auto   site = sg4::Engine::get_instance()->netzone_by_name_or_null(site_name);
    double total_storage = std::stoull(site->get_property("storage_capacity_bytes"));
    double remaining_storage = CGSim::FileManager::request_remaining_site_storage(site_name);
    return (1.0-remaining_storage/total_storage);
}

