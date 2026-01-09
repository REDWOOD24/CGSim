#include "sqliteSaver.h"
#include "logger.h"  
#include <iomanip>
#include <set>
#include <fstream>
#include <sstream>

void sqliteSaver::setFilePath(const std::string& file)
{
    file_name = file;
    initialize();
}

void sqliteSaver::initialize()
{
    if (initialized) return;

    if (sqlite3_open(file_name.c_str(), &db) != SQLITE_OK) {
        throw std::invalid_argument("SQLite file " + file_name + " cannot be opened.");
    }

    if (SQLITE_OK != sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, 0, nullptr)) {
        LOG_WARN("Failed to set database connection in WAL mode.");
    }

    initialized = true;
    LOG_INFO("Initialized SQLite database: {}", file_name);
}

void sqliteSaver::createJobsTable()
{
    const char* drop_stmt = "DROP TABLE IF EXISTS JOBS;";
    char* errmsg = nullptr;
    int ret = sqlite3_exec(db, drop_stmt, nullptr, nullptr, &errmsg);

    if (ret != SQLITE_OK) {
        LOG_ERROR("Failed to drop JOBS table.\nStatement: {}\nError: {}", drop_stmt, errmsg);
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
        LOG_ERROR("Failed to create JOBS table.\nStatement: {}\nError: {}", create_stmt, errmsg);
        sqlite3_free(errmsg);
        throw std::runtime_error("Database table creation failed");
    }

    LOG_INFO("JOBS table created successfully.");
}

void sqliteSaver::saveJob(Job* j)
{
    if (!j) {
        LOG_ERROR("Null job pointer provided to saveJob.");
        return;
    }

    sqlite3_stmt* stmt;
    std::string sql_insert =
        "INSERT INTO JOBS (JOB_ID, SITE, CPU, STATUS, MEMORY, CORES, FLOPS, EXECUTION_TIME, IO_SIZE, IO_TIME, CPU_CONSUMPTION_TIME) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    if (sqlite3_prepare_v2(db, sql_insert.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, std::to_string(j->jobid).c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, j->comp_site.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, (j->comp_host.substr(j->comp_host.rfind('_') + 1)).c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, j->status.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 5, j->memory_usage);
        sqlite3_bind_int(stmt, 6, j->cores);
        sqlite3_bind_double(stmt, 7, j->flops);
        sqlite3_bind_double(stmt, 8, j->EXEC_time_taken);
        sqlite3_bind_double(stmt, 9, j->IO_size_performed);
        sqlite3_bind_double(stmt, 10, j->IO_time_taken);
        sqlite3_bind_double(stmt, 11, j->cpu_consumption_time);

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            LOG_ERROR("Error inserting job {}: {}", j->jobid, sqlite3_errmsg(db));
        } else {
            LOG_DEBUG("Inserted job {} into JOBS table", j->jobid);
        }

        sqlite3_finalize(stmt);
    } else {
        LOG_ERROR("Error preparing insert statement: {}", sqlite3_errmsg(db));
    }
}

void sqliteSaver::updateJob(Job* j)
{
    if (!j) {
        LOG_ERROR("Null job pointer provided to updateJob.");
        return;
    }

    const char* sql =
        "UPDATE JOBS SET "
        "SITE = ?, "
        "CPU = ?, "
        "STATUS = ?, "
        "MEMORY = ?, "
        "CORES = ?, "
        "FLOPS = ?, "
        "EXECUTION_TIME = ?, "
        "IO_SIZE = ?, "
        "IO_TIME = ? "
        "WHERE JOB_ID = ?;";

    sqlite3_stmt* stmt = nullptr;
    int ret = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (ret != SQLITE_OK) {
        LOG_ERROR("Error preparing update statement: {}", sqlite3_errmsg(db));
        return;
    }

    if (j->status == "finished") {
        LOG_INFO("Updating finished job ID: {}", j->jobid);
        LOG_DEBUG("Status: {}, Memory: {}, Cores: {}, FLOPS: {}, Exec Time: {}, IO Size: {}, IO Time: {}",
            j->status, j->memory_usage, j->cores, j->flops, j->EXEC_time_taken, j->IO_size_performed, j->IO_time_taken);
    }

    std::string jobIdStr = std::to_string(j->jobid);
    sqlite3_bind_text(stmt, 1, j->comp_site.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, (j->comp_host.substr(j->comp_host.rfind('_') + 1)).c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, j->status.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(stmt, 4, j->memory_usage);
    sqlite3_bind_int(stmt, 5, j->cores);
    sqlite3_bind_double(stmt, 6, j->flops);
    sqlite3_bind_double(stmt, 7, j->EXEC_time_taken);
    sqlite3_bind_double(stmt, 8, j->IO_size_performed);
    sqlite3_bind_double(stmt, 9, j->IO_time_taken);
    sqlite3_bind_text(stmt, 10, jobIdStr.c_str(), -1, SQLITE_TRANSIENT);

    ret = sqlite3_step(stmt);
    if (ret != SQLITE_DONE) {
        LOG_ERROR("Error updating job {}: {}", j->jobid, sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
}

void sqliteSaver::exportJobsToCSV()
{
    std::string csvFilePath;
    size_t pos = file_name.rfind('.');
    csvFilePath = (pos != std::string::npos) ? file_name.substr(0, pos) + ".csv" : file_name + ".csv";

    std::ofstream csvFile(csvFilePath);
    if (!csvFile.is_open()) {
        LOG_ERROR("Failed to open CSV file for writing: {}", csvFilePath);
        return;
    }

    const char* sql = "SELECT * FROM JOBS;";
    sqlite3_stmt* stmt = nullptr;
    int ret = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (ret != SQLITE_OK) {
        LOG_ERROR("Error preparing select statement: {}", sqlite3_errmsg(db));
        csvFile.close();
        return;
    }

    auto escapeCSV = [](const std::string& s) -> std::string {
        std::string escaped = s;
        bool needQuotes = s.find(',') != std::string::npos || s.find('"') != std::string::npos;
        if (needQuotes) {
            size_t pos = 0;
            while ((pos = escaped.find('"', pos)) != std::string::npos) {
                escaped.insert(pos, "\"");
                pos += 2;
            }
            escaped = "\"" + escaped + "\"";
        }
        return escaped;
    };

    int numCols = sqlite3_column_count(stmt);
    std::vector<std::string> lines;
    std::stringstream header;
    // Build header line
    for (int i = 0; i < numCols; ++i) {
        if (i > 0) {
            header << ",";
        }
        const char* colName = sqlite3_column_name(stmt, i);
        header << escapeCSV(colName ? colName : "");
    }
    lines.push_back(header.str());

    // Build each row and add it to our vector
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        std::stringstream row;
        for (int i = 0; i < numCols; ++i) {
            if (i > 0) {
                row << ",";
            }
            const unsigned char* colText = sqlite3_column_text(stmt, i);
            std::string value = colText ? reinterpret_cast<const char*>(colText) : "";
            row << escapeCSV(value);
        }
        lines.push_back(row.str());
    }
    sqlite3_finalize(stmt);

    // Write the vector lines to the file without a trailing newline at the end
    for (size_t i = 0; i < lines.size(); ++i) {
        csvFile << lines[i];
        if (i != lines.size() - 1) {
            csvFile << "\n";
        }
    }
    csvFile.close();
    LOG_INFO("Exported JOBS table to CSV: {}", csvFilePath);
}

