#include "output.h"


void Output::setFilePath(const std::string& file)
{
    file_name = file;
    initialize();
}

void Output::initialize()
{
    if (initialized) return;

    if (sqlite3_open(file_name.c_str(), &db) != SQLITE_OK) {
        throw std::invalid_argument("SQLite file " + file_name + " cannot be opened.");
    }

    if (SQLITE_OK != sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, 0, nullptr)) {
        throw std::runtime_error("Failed to set database connection in WAL mode.");
    }

    initialized = true;
}

void Output::createEventsTable()
{
    const char* drop_stmt = "DROP TABLE IF EXISTS JOBS;";
    char* errmsg = nullptr;
    int ret = sqlite3_exec(db, drop_stmt, nullptr, nullptr, &errmsg);

    if (ret != SQLITE_OK) {
        sqlite3_free(errmsg);
        throw std::runtime_error("Failed to drop JOBS table.");
    }

    const char* create_stmt =
        "CREATE TABLE JOBS ("
        "JOB_ID TEXT PRIMARY KEY, "
        "SITE TEXT NOT NULL, "
        "CPU TEXT NOT NULL, "
        "STATUS TEXT NOT NULL, "
        "MEMORY REAL NOT NULL, "
        "CORES INTEGER NOT NULL, "
        "FLOPS REAL NOT NULL, "
        "EXECUTION_TIME REAL NOT NULL, "
        "IO_SIZE REAL NOT NULL, "
        "IO_TIME REAL NOT NULL, "
        "CPU_CONSUMPTION_TIME NOT NULL "
        ");";

    ret = sqlite3_exec(db, create_stmt, nullptr, nullptr, &errmsg);
    if (ret != SQLITE_OK) {
        sqlite3_free(errmsg);
        throw std::runtime_error("Database table creation failed");
    }


}


