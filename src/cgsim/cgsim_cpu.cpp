#include "cgsim_cpu.h"
#include "resource_manager.h"
#include "host_extensions.h"

namespace CGSim {

void CPU::add_assigned_job(Job* j) 
{
    assigned_jobs[j->id] = j;
}

void CPU::remove_assigned_job(Job* j) 
{
    assigned_jobs.erase(j->id);
}

void CPU::add_running_job(Job* j) 
{
    running_jobs[j->id] = j;
}

void CPU::remove_running_job(Job* j) 
{
    running_jobs.erase(j->id);
}

void CPU::add_finished_job(Job* j) 
{
    finished_jobs[j->id] = j;
}

void CPU::add_failed_job(Job* j) 
{
    failed_jobs[j->id] = j;
}

void CPU::add_disk(CGSim::Disk* disk)
{
    disks.push_back(disk);
}

std::string CPU::get_name()
{
    return name;
}

std::vector<CGSim::Disk*> CPU::get_disks()
{
    return disks;
}

unsigned int CPU::get_cores_available()
{
    return simgrid_host->extension<CGSim::Core::HostExtensions>()->get_cores_available();
}

unsigned int CPU::get_cores_used()
{
    return simgrid_host->extension<CGSim::Core::HostExtensions>()->get_cores_used();
}

double CPU::get_cpu_utilization()
{
    double total_cores = 1.0*simgrid_host->get_core_count();
    double used_cores = 1.0*simgrid_host->extension<CGSim::Core::HostExtensions>()->get_cores_used();
    return used_cores / total_cores;
}

unsigned long long CPU::get_memory_available()
{
    return simgrid_host->extension<CGSim::Core::HostExtensions>()->get_memory_available();
}

unsigned long long CPU::get_memory_used()
{
    return simgrid_host->extension<CGSim::Core::HostExtensions>()->get_memory_used();
}

double CPU::get_memory_utilization()
{
    double total_memory = 1.0*CGSim::Utilities::parse_units_size(simgrid_host->get_property("ram"));
    double used_memory = 1.0*simgrid_host->extension<CGSim::Core::HostExtensions>()->get_memory_used();
    return used_memory / total_memory;
}

}