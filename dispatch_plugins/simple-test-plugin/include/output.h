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


    void onSimulationStart();
    void onSimulationEnd();
    void onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex);
    void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex);
    void onJobTransferStart(Job* job, simgrid::s4u::Mess const& me);
    void onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me);
    void onFileTransferStart(Job* job, const std::string& filename, simgrid::s4u::Comm const& co);
    void onFileTransferEnd(Job* job, const std::string& filename, simgrid::s4u::Comm const& co);
    void onFileReadStart(Job* job, const std::string& filename, simgrid::s4u::Io const& io);
    void onFileReadEnd(Job* job, const std::string& filename, simgrid::s4u::Io const& io);
    void onFileWriteStart(Job* job, const std::string& filename, simgrid::s4u::Io const& io);
    void onFileWriteEnd(Job* job, const std::string& filename, simgrid::s4u::Io const& io);


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
