#pragma once
#include <simgrid/s4u.hpp>
#include "job.h"
namespace sg4 = simgrid::s4u;

namespace CGSim {

namespace Core
{
class Platform;
class Actions;
class HostExtensions; 
class JOB_EXECUTOR; 
}

namespace GlobalManagers
{
class ResourceManager;
class FileManager;
}

class CPU;

class Site 
{
public:
    Site() = default;
    Site(const Site&) = delete;
    Site& operator=(const Site&) = delete;

    bool cpu_exists_at_site(const std::string& cpu_name);
    inline std::vector<CGSim::CPU*> get_cpus() {return cpus;}
    inline std::unordered_set<std::string> get_used_cpus_list() {return used_cpus_list;}

    inline double get_cpu_utilization(){return ((1.0)*used_cpus_list.size())/((1.0)*total_cpus);}
    inline double get_memory_utilization(){return ((1.0)*used_memory)/((1.0)*total_memory);}

    inline std::deque<Job*> get_pending_jobs() {return pending_jobs;}
    inline std::unordered_map<std::string,Job*> get_assigned_jobs(){return assigned_jobs;}
    inline std::unordered_map<std::string,Job*> get_running_jobs(){return running_jobs;}
    inline std::unordered_map<std::string,Job*> get_finished_jobs(){return finished_jobs;}
    inline std::unordered_map<std::string,Job*> get_failed_jobs(){return failed_jobs;}

    inline unsigned long get_number_of_total_cores(){return total_cores;}
    inline unsigned long get_number_of_used_cores(){return used_cores;}
    inline unsigned long get_number_of_total_cpus(){return total_cpus;}
    inline unsigned long get_number_of_used_cpus(){return used_cpus_list.size();}

    inline unsigned long long get_memory_capacity() {return total_memory;};
    inline unsigned long long get_used_memory() {return used_memory;}

    inline void enable_job_assignment() {job_assignment_enabled = true;}
    inline void disable_job_assignment() {job_assignment_enabled = false;}
    inline bool is_job_assignment_enabled() {return job_assignment_enabled;}

    inline void        set_property(const std::string& key, const std::string& value) {properties[key] = value;}
    inline std::string get_property(const std::string& key) {return properties.at(key);}

    inline void          set_max_retries(unsigned long _MAX_RETRIES) {MAX_RETRIES = _MAX_RETRIES;}
    inline unsigned long get_max_retries(){return MAX_RETRIES;}

    inline std::unordered_map<std::string,std::string>& get_incoming_file_transfers() {return incoming_file_transfers;}

    std::unordered_set<std::string> get_files();
    unsigned long long get_remaining_storage();

private:
    std::string name{};
    unsigned long total_cores=0, used_cores=0, total_cpus=0, MAX_RETRIES=100000;
    unsigned long long total_memory=0, used_memory=0;
    
    std::vector<CGSim::CPU*> cpus{};
    std::unordered_map<std::string, sg4::Host*> simgrid_hosts{};
    std::unordered_set<std::string> used_cpus_list{};

    std::deque<Job*> pending_jobs{};
    std::unordered_map<std::string,Job*> assigned_jobs{}, running_jobs{}, finished_jobs{}, failed_jobs{};
    std::unordered_map<std::string,std::string> properties{}, incoming_file_transfers{};
    bool job_assignment_enabled=true;

    sg4::NetZone* simgrid_site = nullptr;
    
    void add_assigned_job(CGSim::Job* j);
    void remove_assigned_job(CGSim::Job* j);

    void add_running_job(CGSim::Job* j);
    void remove_running_job(CGSim::Job* j);

    void add_finished_job(CGSim::Job* j);
    void add_failed_job(CGSim::Job* j);

    void add_cpu(CGSim::CPU* cpu);

    friend class CPU;
    friend class ::CGSim::Core::Platform;
    friend class ::CGSim::Core::Actions;
    friend class ::CGSim::Core::HostExtensions;
    friend class ::CGSim::Core::JOB_EXECUTOR;
    friend class GlobalManagers::ResourceManager;
    friend class GlobalManagers::FileManager;

};

}