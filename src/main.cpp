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
#include "file_manager.h"
#include "site_manager.h"

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
    const std::string pluginPath                   = j["Plugin"];
    const std::set<std::string> filteredSiteList   = j["Limited_Sites"].get<std::set<std::string>>();

    //Parse Input
    std::unique_ptr<Parser> parser = std::make_unique<Parser>(siteConnInfoFile, siteInfoFile, filteredSiteList);
    auto sitesInfo     = parser->getSiteInfo();
    auto siteConnInfo  = parser->getSiteConnInfo();

    // Initialize SimGrid
    sg4::Engine e(&argc, argv);
    //simgrid::s4u::Engine::set_config("precision/timing", 1e-3);

    // Create the platform
    std::unique_ptr<Platform> pf = std::make_unique<Platform>(gridName, sitesInfo, siteConnInfo);
    auto* platform = pf->get_simgrid_platform();
    for (auto& [key, value] : j["Custom_Parameters"].items()) 
    {
        platform->set_property(key,value.get<std::string>()); 
        CGSim::get_site_manager()->Custom_Parameters[key] = value;
    }

    PluginLoader<CGSim::Plugin> plugin_loader;
    auto unique_dispatcher = plugin_loader.load(pluginPath);
    std::shared_ptr<CGSim::Plugin> dispatcher = std::move(unique_dispatcher);

    // Create and set up executor
    JOB_EXECUTOR::set_dispatcher(dispatcher);
    CGSim::FileManager::set_dispatcher(dispatcher);
    dispatcher->beforeSimulationStart();
    JOB_EXECUTOR::start_job_execution();

    // Print version
    CG_SIM_LOG_INFO("CGSim version: {}.{}.{}", MAJOR_VERSION, MINOR_VERSION, BUILD_NUMBER);

    return 0;
}
