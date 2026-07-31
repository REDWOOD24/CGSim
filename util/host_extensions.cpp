#include "host_extensions.h"
#include <iostream>
simgrid::xbt::Extension<simgrid::s4u::Host, HostExtensions> HostExtensions::EXTENSION_ID;

void HostExtensions::registerJob(Job* j) {
    simgrid::kernel::actor::simcall_answered([this, j] {

        if(host->get_name().find("JOB-SERVER_cpu") != std::string::npos) throw std::runtime_error("Can't register job on main server");
        if(host->get_name().find("_communication_server") != std::string::npos) throw std::runtime_error("Can't register job on communication server: " + host->get_name());

        job_ids.insert(std::to_string(j->jobid));
        cores_used      += j->cores;
        cores_available -= j->cores;
        CGSim::get_site_manager()->get_site(j->comp_site)->used_cores += j->cores;
        CGSim::get_site_manager()->USED_GRID_CORES += j->cores;
        if(cores_available == 0 && available == true) {CGSim::get_site_manager()->get_site(j->comp_site)->used_cpus++; available = false;}
    });
}

void HostExtensions::onJobFinish(Job* j) {
    simgrid::kernel::actor::simcall_answered([this, j] {
        job_ids.erase(std::to_string(j->jobid));
        cores_used      -= j->cores;
        cores_available += j->cores;
        CGSim::get_site_manager()->get_site(j->comp_site)->used_cores -= j->cores;
        CGSim::get_site_manager()->USED_GRID_CORES -= j->cores;
        if(cores_available > 0 && available == false) {CGSim::get_site_manager()->get_site(j->comp_site)->used_cpus--; available = true;}
    });
}

unsigned int HostExtensions::get_cores_used() const { return cores_used; }
unsigned int HostExtensions::get_cores_available() const { return cores_available; }

static void on_host_creation(simgrid::s4u::Host& h) {
    h.extension_set<HostExtensions>(new HostExtensions(&h));
}

void host_extension_init() {
    if (not HostExtensions::EXTENSION_ID.valid()) {
        HostExtensions::EXTENSION_ID = simgrid::s4u::Host::extension_create<HostExtensions>();
        simgrid::s4u::Host::on_creation_cb(&on_host_creation);
    }
}
