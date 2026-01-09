#ifndef SQLITESAVER_H
#define SQLITESAVER_H

#include <iostream>
#include <string>
#include <stdexcept>
#include <sqlite3.h>
#include <vector>
#include <sstream>
#include "job.h"


class sqliteSaver {
    bool initialized = false;
    std::string file_name;
    sqlite3 *db;
    void initialize();

public:
     sqliteSaver(){};
    ~sqliteSaver() { sqlite3_close_v2(db); }

    void setFilePath(const std::string& file);
    void createJobsTable();
    void saveJob(Job* j);
    void updateJob(Job* j);
    void exportJobsToCSV();

};

#endif
//SQLITESAVER_H
