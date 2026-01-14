#ifndef OUTPUT_H
#define OUTPUT_H

#include <iostream>
#include <string>
#include <stdexcept>
#include <sqlite3.h>
#include <vector>
#include <sstream>
#include "CGSim.h"


class OUTPUT {

public:
     OUTPUT(){initialize();};
    ~OUTPUT() {sqlite3_close_v2(db);}

    void initialize();
    void createEventsTable();
    void insert_event(sqlite3_stmt* stmt,
                  const std::string& event,
                  const std::string& state,
                  const std::string& job_id,
                  double time,
                  const std::string& payload);
    double calculate_grid_cpu_util();
    double calculate_site_cpu_util(std::string& site_name);
    double calculate_grid_storage_util();
    double calculate_site_storage_util(std::string& site_name);


private:
    bool initialized = false;
    sqlite3 *db;
    sg4::NetZone* platform = sg4::Engine::get_instance()->get_netzone_root();
};

#endif
//OUTPUT_H
