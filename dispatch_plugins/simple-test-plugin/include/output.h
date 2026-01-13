#ifndef OUTPUT_H
#define OUTPUT_H

#include <iostream>
#include <string>
#include <stdexcept>
#include <sqlite3.h>
#include <vector>
#include <sstream>
#include "CGSim.h"


class Output {
    bool initialized = false;
    std::string file_name;
    sqlite3 *db;
    void initialize();

public:
     Output(){};
    ~Output() { sqlite3_close_v2(db); }

    void setFilePath(const std::string& file);
    void createEventsTable();
    void exportJobsToCSV();

};

#endif
//OUTPUT_H
