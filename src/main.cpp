#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <fstream>
#include <string>
#include <memory>
#include <cmath>
#include <chrono>
#include <list>
#include <simgrid/s4u.hpp>
#include "parser.h"
#include "platform.h"
#include "version.h"
#include "logger.h"
#include "job_executor.h"

int main(int argc, char** argv)
{
    logger::init();  // Must be called before any logging
    
    const std::string usage = std::string("usage: ") + argv[0] + " -c config.json";
    
    if (argc != 3) {
     //   LOG_ERROR("Invalid number of arguments.\n{}", usage);
        return -1;
    }

    if (std::string(argv[1]) != "-c") {
     //   LOG_ERROR("Missing -c option.\n{}", usage);
        return -1;
    }
    
    // Parse Configuration File
    const std::string configFile = argv[2];
    LOG_INFO("Reading in configuration from: {}", configFile);

    std::ifstream in(configFile);
    if (!in.is_open()) {
     //   LOG_CRITICAL("Failed to open config file: {}", configFile);
        return -1;
    }

    auto j = json::parse(in);

    const std::string gridName                     = j["Grid_Name"];
    const std::string siteInfoFile                 = j["Sites_Information"];
    const std::string siteConnInfoFile             = j["Sites_Connection_Information"];
    const std::string dispatcherPath               = j["Dispatcher_Plugin"];
    const std::string outputFile                   = j["Output_DB"];
    const long        num_of_jobs                 = j["Num_of_Jobs"];
    const std::string jobFile                      = j["Input_Job_CSV"];
    const std::list<std::string> filteredSiteList  = j["Sites"].get<std::list<std::string>>();

    // Calibration Parameter these will be moved to site info later
    // const int cpu_min = j["cpu_min_max"][0];
    // const int cpu_max = j["cpu_min_max"][1];
    // const int speed_precision = j["cpu_speed_precision"]; 
    // const std::vector<double> cpuSpeeds = j["cpu_speeds"].get<std::vector<double>>();

    std::unique_ptr<Parser> parser = std::make_unique<Parser>(siteConnInfoFile, siteInfoFile, jobFile, filteredSiteList);
    // auto siteNameCPUInfo = parser->getSiteNameCPUInfo(cpu_min,cpu_max,speed_precision);
    auto siteNameCPUInfo = parser->getSiteNameCPUInfo();
    auto siteConnInfo    = parser->getSiteConnInfo();
    auto siteNameGLOPS   = parser->getSiteNameGFLOPS();

    // Initialize SimGrid
    sg4::Engine e(&argc, argv);

    // Create the platform
    std::unique_ptr<Platform> pf = std::make_unique<Platform>();
    auto* platform = pf->create_platform(gridName);
    pf->initialize_simgrid_plugins();
   
    // Create sites and connections
    auto sites = pf->create_sites(platform, filteredSiteList, siteNameCPUInfo, siteNameGLOPS);
    pf->initialize_site_connections(platform, siteConnInfo, sites);
    pf->initialize_job_server(platform, siteNameCPUInfo, sites);


    PluginLoader<DispatcherPlugin> plugin_loader;
    auto dispatcher = plugin_loader.load(dispatcherPath);
    dispatcher->getResourceInformation(platform);

    // Create and set up executor
    std::unique_ptr<JOB_EXECUTOR> executor = std::make_unique<JOB_EXECUTOR>();
    executor->set_output(outputFile);
    executor->set_dispatcher(dispatcher);
    executor->start_receivers();
    executor->start_job_execution(num_of_jobs);

    // Print version
    LOG_INFO("SimATLAS version: {}.{}.{}", MAJOR_VERSION, MINOR_VERSION, BUILD_NUMBER);

    return 0;
}
