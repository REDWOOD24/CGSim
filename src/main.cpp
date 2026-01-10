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
    //Initialize Logging
    CGSim::logger::init();
    const std::string usage = std::string("usage: ") + argv[0] + " -c config.json";

    //Read in Configuration
    if (argc != 3 || std::string(argv[1]) != "-c") {throw std::runtime_error(usage);}
    const std::string configFile = argv[2];
    CG_SIM_LOG_INFO("Reading in configuration from: {}", configFile);

    std::ifstream in(configFile);
    if (!in.is_open()) { throw std::runtime_error("could not open configuration file");}
    auto j = json::parse(in);

    //Configuration Elements
    const std::string gridName                     = j["Grid_Name"];
    const std::string siteInfoFile                 = j["Sites_Information"];
    const std::string siteConnInfoFile             = j["Sites_Connection_Information"];
    const std::string dispatcherPath               = j["Dispatcher_Plugin"];
    const std::string outputFile                   = j["Output_DB"];
    const long        num_of_jobs                  = j["Num_of_Jobs"];
    const std::set<std::string> filteredSiteList   = j["Sites"].get<std::set<std::string>>();

    //Parse Input
    std::unique_ptr<Parser> parser = std::make_unique<Parser>(siteConnInfoFile, siteInfoFile, filteredSiteList);
    auto sitesInfo     = parser->getSiteInfo();
    auto siteConnInfo  = parser->getSiteConnInfo();

    // Initialize SimGrid
    sg4::Engine e(&argc, argv);

    // Create the platform
    std::unique_ptr<Platform> pf = std::make_unique<Platform>(gridName, sitesInfo, siteConnInfo);
    auto* platform = pf->get_simgrid_platform();

    PluginLoader<DispatcherPlugin> plugin_loader;
    auto dispatcher = plugin_loader.load(dispatcherPath);
    dispatcher->getResourceInformation(platform);

    // Create and set up executor
    JOB_EXECUTOR::set_dispatcher(dispatcher);
    JOB_EXECUTOR::start_receivers();
    JOB_EXECUTOR::start_job_execution(num_of_jobs);

    // Print version
    CG_SIM_LOG_INFO("SimATLAS version: {}.{}.{}", MAJOR_VERSION, MINOR_VERSION, BUILD_NUMBER);

    return 0;
}
