#include "host_extensions.h"
#include <iostream>
simgrid::xbt::Extension<simgrid::s4u::Host, HostExtensions> HostExtensions::EXTENSION_ID;

void HostExtensions::registerJob(Job* j) {
    simgrid::kernel::actor::simcall_answered([this, j] {
        job_ids.insert(std::to_string(j->jobid));
        cores_used      += j->cores;
        cores_available -= j->cores;
    });
}

void HostExtensions::onJobFinish(Job* j) {
    simgrid::kernel::actor::simcall_answered([this, j] {
        job_ids.erase(std::to_string(j->jobid));
        cores_used      -= j->cores;
        cores_available += j->cores;
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
