#include "track4_output_calibration.h"

void TRACK4_OUTPUT_CALIBRATION::initialize()
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

void TRACK4_OUTPUT_CALIBRATION::createEventsTable()
{
    const char* create_stmt =
        "CREATE TABLE IF NOT EXISTS JOBS ("
        "_ID INTEGER PRIMARY KEY AUTOINCREMENT, "
        "JOB_ID INTEGER NOT NULL, "
        "simulated_time REAL NOT NULL, "
        "historical_time REAL NOT NULL"
        ");";

    char* errmsg = nullptr;
    int ret = sqlite3_exec(db, create_stmt, nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
        std::string msg = errmsg ? errmsg : sqlite3_errmsg(db);
        sqlite3_free(errmsg);
        throw std::runtime_error("Database table creation failed: " + msg);
    }
}

void TRACK4_OUTPUT_CALIBRATION::insert_event(
                    long long job_id,
                    double simulated_time,
                    double historical_time
                    )
{
    sqlite3_stmt* stmt = nullptr;

    const char* sql_insert =
        "INSERT INTO JOBS ("
        "JOB_ID, simulated_time, historical_time"
        ") VALUES (?, ?, ?);";

    int rc = sqlite3_prepare_v2(db, sql_insert, -1, &stmt, nullptr);
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string("SQLite prepare failed: ") + sqlite3_errmsg(db));

    sqlite3_bind_int64(stmt, 1, job_id);
    sqlite3_bind_double(stmt, 2, simulated_time);
    sqlite3_bind_double(stmt, 3, historical_time);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE)
        throw std::runtime_error(std::string("SQLite step failed: ") + sqlite3_errmsg(db));
}


void TRACK4_OUTPUT_CALIBRATION::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
    double simulated_time = ex.get_finish_time() - ex.get_start_time();
    double historical_time = job->cpu_consumption_time;

    insert_event
    (
        job->jobid,
        simulated_time,
        historical_time
    );
}

