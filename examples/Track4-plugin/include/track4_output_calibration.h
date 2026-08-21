#ifndef TRACK4_OUTPUT_CALIBRATION_H
#define TRACK4_OUTPUT_CALIBRATION_H

#include <iostream>
#include <string>
#include <stdexcept>
#include <sqlite3.h>
#include <vector>
#include <sstream>
#include "CGSim.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

class TRACK4_OUTPUT_CALIBRATION {

public:
     TRACK4_OUTPUT_CALIBRATION(){initialize();};
    ~TRACK4_OUTPUT_CALIBRATION() {sqlite3_close_v2(db);}

    void initialize();
    void createEventsTable();
    void insert_event(
                    long long job_id,
                    double simulated_time,
                    double historical_time
                    );
    void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex);


private:
    bool initialized = false;
    sqlite3 *db;
    sg4::NetZone* platform = sg4::Engine::get_instance()->get_netzone_root();
};

#endif
//TRACK4_OUTPUT_CALIBRATION_H