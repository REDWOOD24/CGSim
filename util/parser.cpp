#include "parser.h"
#include <fstream>
#include <list>
#include <random>
#include <iomanip>
#include <algorithm>
#include <nlohmann/json.hpp>
#include <iostream>

using json = nlohmann::json;

static std::string joinStrings(const std::vector<std::string>& vec, const std::string& delimiter = ", ")
{
    std::ostringstream oss;
    for (size_t i = 0; i < vec.size(); ++i) {
        if (i != 0)
            oss << delimiter;
        oss << vec[i];
    }
    return oss.str();
}

Parser::Parser(const std::string& _siteConnInfoFile, const std::string& _siteInfoFile)
{
    siteConnInfoFile = _siteConnInfoFile;
    siteInfoFile     = _siteInfoFile;
    this->setSiteCPUCount(); // Set before site-names because if a site has 0 CPUs (no info), I don't include it.
    this->setSiteNames();
}

Parser::Parser(const std::string& _siteConnInfoFile, const std::string& _siteInfoFile, const std::string& _jobInfoFile)
{
    siteConnInfoFile = _siteConnInfoFile;
    siteInfoFile     = _siteInfoFile;
    jobFile          = _jobInfoFile;
    this->setSiteCPUCount(); // Set before site-names because if a site has 0 CPUs (no info), I don't include it.
    this->setSiteNames();
    this->setSiteGFLOPS();
}

Parser::Parser(const std::string& _siteConnInfoFile, const std::string& _siteInfoFile, const std::string& _jobInfoFile, const std::list<std::string>& filteredSiteList)
{
    siteConnInfoFile = _siteConnInfoFile;
    siteInfoFile     = _siteInfoFile;
    jobFile          = _jobInfoFile;
    this->setSiteCPUCount(); // Set before site-names because if a site has 0 CPUs (no info), I don't include it.
    this->setSiteNames(filteredSiteList);
    this->setSiteGFLOPS();
}

int Parser::genRandNum(int lower, int upper)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(lower, upper);
    return distrib(gen);
}

double Parser::GaussianDistribution(double mean, double stddev)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> d(mean, stddev);
    return d(gen);
}

void Parser::setSiteNames()
{
    std::ifstream in(siteInfoFile);
    if (!in.is_open()) {
        //LOG_ERROR("Error: Could not open file {}", siteConnInfoFile);
        return;
    }
    json j;
    try {
        j = json::parse(in);
    } catch (const json::parse_error& e) {
        //LOG_ERROR("Error parsing JSON: {}", e.what());
        return;
    }
    // Get all site names
    for (auto it = j.begin(); it != j.end(); ++it) {
        std::string site = it.key();
        if (siteCPUCount[site] > 0) {
            site_names.insert(site);
        } else {
            //LOG_INFO("Site Name (invalid or no CPUs or info not available): {}", site);
        }
    }
}

void Parser::setSiteNames(const std::list<std::string>& filteredSiteList)
{
    std::ifstream in(siteInfoFile);
    if (!in.is_open()) {
        //LOG_ERROR("Error: Could not open file {}", siteInfoFile);
        return;
    }
    
    json j;
    try {
        j = json::parse(in);
    } catch (const json::parse_error& e) {
        //LOG_ERROR("Error parsing JSON: {}", e.what());
        return;
    }
    
    // If filteredSiteList is empty, use all sites from the JSON file.
    if (filteredSiteList.empty()) {
        for (auto it = j.begin(); it != j.end(); ++it) {
            std::string site = it.key();
            if (siteCPUCount[site] > 0) {
                site_names.insert(site);
            } else {
                //LOG_INFO("Site Name (invalid or no CPUs): {}", site);
            }
        }
    } else {
        // Otherwise, add only the sites provided in filteredSiteList.
        for (const auto& site : filteredSiteList) {
            if (siteCPUCount[site] > 0) {
                site_names.insert(site);
            } else {
                //LOG_INFO("Site Name (invalid or no CPUs): {}", site);
            }
        }
    }
}

void Parser::setSiteCPUCount()
{
    std::ifstream in(siteInfoFile);
    auto j = json::parse(in);
    for (auto it = j.begin(); it != j.end(); ++it) {
        // moved the computation of CPU counts to the siteInfo.json file
        // 500 flops per core and 32 cores per host (assumption)
        int cpus   = (j[it.key()]["CPUCount"]).get<int>();
        std::vector<double> cpuSpeeds = (j[it.key()]["CPUSpeed"]).get<std::vector<double>>();
        // int gflops = (j[it.key()]["GFLOPS"]).get<int>(); 
        // int cpus   = std::round(gflops / (32 * 500.0));
        siteCPUCount[it.key()] = cpus;
        siteCPUSpeeds[it.key()] = cpuSpeeds;
    }
}





void Parser::setSiteGFLOPS()
{
    std::ifstream in(siteInfoFile);
    auto j = json::parse(in);
    for (auto it = j.begin(); it != j.end(); ++it) {
        // 500 flops per core and 32 cores per host (assumption)
        int gflops = (j[it.key()]["GFLOPS"]).get<int>(); 
        siteNameGFLOPS[it.key()] = gflops;
    }
}

std::unordered_map<std::string,int> Parser::getSiteNameGFLOPS()
{
    return siteNameGFLOPS;
}

std::vector<DiskInfo> Parser::getDisksInfo(const std::string site_name, int num_of_cpus)
{
    std::ifstream in(siteInfoFile);
    auto j = json::parse(in);
    std::vector<DiskInfo> disks;
    auto info = j[site_name]["RSE"];
    
    for (auto it = info.begin(); it != info.end(); ++it)
    {
        if (j[site_name]["RSE"][it.key()] == 0) continue;
        if (it.key().find("DISK") == std::string::npos) continue;

        DiskInfo disk;
        disk.name = it.key();
        disk.read_bw = this->genRandNum(150, 250) * 1e8; // Assumption
        disk.write_bw = this->genRandNum(40, 120) * 1e8;  // Assumption
        disk.size = std::to_string(std::round(1048576.0 * 1000 * 1000 * std::stod(j[site_name]["RSE"][it.key()].get<std::string>()) / num_of_cpus * 1.0)) + "kB";

        if (it.key().find("SCRATCH") != std::string::npos)
            disk.mount = "/scratch/";
        else if (it.key().find("LOCAL") != std::string::npos)
            disk.mount = "/local/";
        else if (it.key().find("DATA") != std::string::npos)
            disk.mount = "/data/";
        else if (it.key().find("CALIB") != std::string::npos)
            disk.mount = "/calib/";
        else
            disk.mount = "/other/";

        disks.push_back(disk);
    }
  
    return disks;
}

std::unordered_map<std::string, std::unordered_map<std::string, CPUInfo>> Parser::getSiteNameCPUInfo()
{
    std::unordered_map<std::string, std::unordered_map<std::string, CPUInfo>> siteNameCPUInfo;
  
    for (const auto& site : site_names) {
        // int num_of_cpus = siteCPUCount[site] + this->genRandNum(-3, 3); // Little random offset added.
        int num_of_cpus = siteCPUCount[site]; // Little random offset added.
        std::vector<DiskInfo> disks = this->getDisksInfo(site, num_of_cpus);

        //LOG_INFO("Adding Site: {} with CPUs: {}", site, num_of_cpus);
        std::vector<double> cpuSpeeds = siteCPUSpeeds[site];
        // std::vector<double> cpuSpeeds = {
        //     1.6e6, 200000, 600000, 1.5e6, 1.4e6, 600000, 2e6, 1.8e6, 1e6, 900000,
        // 200000, 1.5e6, 200000, 1.9e6, 1.3e6, 1.6e6, 2e6, 800000, 600000, 600000,
        // 1.5e6, 100000, 100000, 2e6, 1.5e6, 300000, 1.3e6, 1.3e6, 400000, 700000,
        // 800000, 400000, 500000, 600000, 1.5e6, 100000, 1.5e6, 800000, 1.8e6, 1.5e6,
        // 1.4e6, 1.3e6, 1.5e6, 1.7e6, 1.3e6, 1.3e6, 400000, 100000, 1.2e6, 1.1e6,
        // 1.3e6, 1.4e6, 1e6, 1.1e6, 2e6, 1.7e6, 1e6, 600000, 1.3e6, 300000,
        // 700000, 600000, 900000, 400000, 600000, 1.8e6, 1e6, 1.7e6, 500000, 400000,
        // 1.8e6, 1.1e6, 1.1e6, 200000, 500000, 1.8e6, 1.3e6, 1.6e6, 1.5e6, 900000,
        // 1.9e6, 1.7e6, 700000, 1.9e6, 1.6e6, 500000, 1.6e6, 300000, 100000, 1.6e6,
        // 800000, 400000, 1.4e6, 300000, 900000, 300000, 1.6e6, 1.7e6, 400000, 1.9e6,
        // 800000, 300000, 100000, 900000, 1.8e6, 600000, 700000, 500000, 100000, 900000,
        // 1.6e6, 900000
        // // };


        // std::cout << "Site CPUS: " << siteCPUCount[site] << std::endl;
        // std::cout << "CPUS count: " << num_of_cpus << std::endl;
        // std::cout << "CPU Speeds: " << cpuSpeeds.size() << std::endl;
        for (int cpu_num = 0; cpu_num < num_of_cpus; cpu_num++) {
            CPUInfo cpu;
            cpu.cores = 32;                                          // Assumption.
            // cpu.speed = this->genRandNum(2, 4) * 1e7;   
            cpu.speed =  cpuSpeeds.at(cpu_num);            // Assumption.
            // std::cout << "CPU Speed: " << cpu.speed << std::endl;
            cpu.BW_CPU = this->genRandNum(10, 20) * 1e10;              // Assumption.
            cpu.LAT_CPU = 0;                                          // Assumption.
            cpu.ram = std::to_string(this->genRandNum(8, 16)) + "GiB";  // Assumption.
            cpu.disk_info = disks;
            siteNameCPUInfo[site][site + "_cpu-" + std::to_string(cpu_num)] = cpu; 
        }
    }
    return siteNameCPUInfo;
}
// this method is just for calibration will be removed after calibrating the cpuspeeds
std::unordered_map<std::string, std::unordered_map<std::string, CPUInfo>> Parser::getSiteNameCPUInfo(int cpuMin, int cpuMax, int speedPrecision)
{
    std::unordered_map<std::string, std::unordered_map<std::string, CPUInfo>> siteNameCPUInfo;
  
    for (const auto& site : site_names) {
        // int num_of_cpus = siteCPUCount[site] + this->genRandNum(-3, 3); // Little random offset added.
        int num_of_cpus = siteCPUCount[site] + 3; // Little random offset added.
        std::vector<DiskInfo> disks = this->getDisksInfo(site, num_of_cpus);

        //LOG_INFO("Adding Site: {} with CPUs: {}", site, num_of_cpus);
        
        // std::cout << "Site CPUS: " << siteCPUCount[site] << std::endl;
        // std::cout << "CPUS count: " << num_of_cpus << std::endl;
        // std::cout << "CPU Speeds: " << cpuSpeeds.size() << std::endl;
        for (int cpu_num = 0; cpu_num < num_of_cpus; cpu_num++) {
            CPUInfo cpu;
            cpu.cores = 32;                                          // Assumption.
            cpu.speed = this->genRandNum(cpuMin, cpuMax) * std::pow(10,speedPrecision);   
            //cpu.speed =  cpuSpeeds.at(cpu_num);            // Assumption.
            // std::cout << "CPU Speed: " << cpu.speed << std::endl;
            cpu.BW_CPU = this->genRandNum(10, 20) * 1e10;              // Assumption.
            cpu.LAT_CPU = 0;                                          // Assumption.
            cpu.ram = std::to_string(this->genRandNum(8, 16)) + "GiB";  // Assumption.
            cpu.disk_info = disks;
            siteNameCPUInfo[site][site + "_cpu-" + std::to_string(cpu_num)] = cpu; 
        }
    }
    return siteNameCPUInfo;
}



std::unordered_map<std::string, std::pair<double, double>> Parser::getSiteConnInfo()
{
    std::ifstream in(siteConnInfoFile);
    auto j = json::parse(in);

    // Defining Latency in ms based on 'closeness' value in JSON file
    std::unordered_map<int, double> closeness_latency_map = {
        {0, 0.0}, {1, 10.0}, {2, 20.0}, {3, 30.0}, {4, 40.0},
        {5, 50.0}, {6, 60.0}, {7, 70.0}, {8, 80.0}, {9, 90.0},
        {10, 100.0}, {11, 110.0}, {12, 120.0}
    };

    std::unordered_map<std::string, std::pair<double, double>> siteConnInfo;
    for (auto it1 = site_names.begin(); it1 != site_names.end(); ++it1) {
        for (auto it2 = std::next(it1); it2 != site_names.end(); ++it2) {
            std::string site1 = *it1;
            std::string site2 = *it2;
            std::string connection = site1 + ":" + site2;
      
            // Check if connection information exists.
            bool info_exists = j[connection].contains("mbps") &&
                               j[connection]["closeness"].contains("latest") &&
                               j[connection]["mbps"]["dashb"].contains("1w");

            if (info_exists) {
                double latency = closeness_latency_map[j[connection]["closeness"]["latest"]];
                double bandwidth = j[connection]["mbps"]["dashb"]["1w"];
                siteConnInfo[connection] = std::make_pair(latency, bandwidth);
            }
        }
    }
 
    return siteConnInfo;
}
