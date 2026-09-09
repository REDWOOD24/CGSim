#pragma once
#include <simgrid/s4u.hpp>
#include "cgsim_disk.h"
#include "job.h"
namespace sg4 = simgrid::s4u;

namespace CGSim::Core 
{
    class Platform;
}

namespace CGSim {

class Site;

class CPU
{
public:

    std::string  get_name();
    unsigned int get_cores_available();
    unsigned int get_cores_used();
    unsigned int get_total_cores();

    double get_cpu_utilization();
    double get_speed();


    unsigned long long get_memory_available();
    unsigned long long get_memory_used();
    double             get_memory_utilization();

    std::unordered_map<std::string,Job*> get_assigned_jobs();
    std::unordered_map<std::string,Job*> get_running_jobs();
    std::unordered_map<std::string,Job*> get_finished_jobs();
    std::unordered_map<std::string,Job*> get_failed_jobs();

    inline void        set_property(const std::string& key, const std::string& value) {properties[key] = value;}
    inline std::string get_property(std::string& key) {return properties.at(key);}

    std::vector<CGSim::Disk*> get_disks();



private:
    std::string name{};
    sg4::Host* simgrid_host = nullptr;
    std::unordered_map<std::string,Job*> assigned_jobs{}, running_jobs{}, finished_jobs{}, failed_jobs{};
    std::unordered_map<std::string,std::string> properties{};
    std::vector<CGSim::Disk*> disks{};

    void add_assigned_job(CGSim::Job* j);
    void remove_assigned_job(CGSim::Job* j);

    void add_running_job(CGSim::Job* j);
    void remove_running_job(CGSim::Job* j);

    void add_finished_job(CGSim::Job* j);
    void add_failed_job(CGSim::Job* j);

    void add_disk(CGSim::Disk* disk);

    friend class ::CGSim::Core::Platform;
    friend class ::CGSim::GlobalManagers::ResourceManager;
    friend class Site;
};

}