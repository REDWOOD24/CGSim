#include "cgsim_site.h"
#include "resource_manager.h"
#include "file_manager.h"

namespace CGSim {

bool Site::cpu_exists_at_site(const std::string& cpu_name)
{
    auto* cpu = CGSim::GlobalManagers::get_resource_manager()->get_cpu(cpu_name);
    return name == cpu->simgrid_host->get_englobing_zone()->get_name();
}

std::unordered_set<std::string> Site::get_files()
{
    return CGSim::GlobalManagers::get_file_manager()->request_site_files(name);
}

unsigned long long Site::get_remaining_storage()
{
    return CGSim::GlobalManagers::get_file_manager()->request_remaining_site_storage(name);
}

void Site::add_assigned_job(Job* j) 
{
    assigned_jobs[j->id] = j;
    CGSim::GlobalManagers::get_resource_manager()->get_cpu(j->cpu)->add_assigned_job(j);
}

void Site::remove_assigned_job(Job* j) 
{
    assigned_jobs.erase(j->id);
    CGSim::GlobalManagers::get_resource_manager()->get_cpu(j->cpu)->remove_assigned_job(j);
}

void Site::add_running_job(Job* j) 
{
    running_jobs[j->id] = j;
    CGSim::GlobalManagers::get_resource_manager()->get_cpu(j->cpu)->add_running_job(j);
}

void Site::remove_running_job(Job* j) 
{
    running_jobs.erase(j->id);
    CGSim::GlobalManagers::get_resource_manager()->get_cpu(j->cpu)->remove_running_job(j);
}

void Site::add_finished_job(Job* j) 
{
    finished_jobs[j->id] = j;
    CGSim::GlobalManagers::get_resource_manager()->get_cpu(j->cpu)->remove_running_job(j);
}

void Site::add_failed_job(Job* j) 
{
    failed_jobs[j->id] = j;
    CGSim::GlobalManagers::get_resource_manager()->get_cpu(j->cpu)->add_failed_job(j);
}


void Site::add_cpu(CGSim::CPU* cpu){cpus.push_back(cpu);}

}