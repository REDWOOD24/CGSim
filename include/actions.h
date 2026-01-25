#ifndef ACTIONS_H
#define ACTIONS_H

#include <map>
#include <set>
#include <fstream>
#include <string>
#include <math.h>
#include "job.h"
#include <simgrid/s4u.hpp>
#include <simgrid/s4u/Exec.hpp>
#include "DispatcherPlugin.h"
#include "host_extensions.h"
#include "file_manager.h"
#include "job_executor.h"

namespace sg4 = simgrid::s4u;
static std::unordered_set<std::string> started_transfers; //Hack to avoid double start of comms in callback

class Actions
{
public:
     Actions() = default;
    ~Actions() = default;

    static sg4::ExecPtr  exec_task_multi_thread_async(Job* j, std::unique_ptr<DispatcherPlugin>& dispatcher);
    static sg4::CommPtr  transfer_file_async(Job* j, const std::string& filename, const std::string& src_site, const std::string& dst_site, std::unique_ptr<DispatcherPlugin>& dispatcher);
    static sg4::IoPtr    read_file_async(Job* j, const std::string& filename, std::unique_ptr<DispatcherPlugin>& dispatcher);
    static sg4::IoPtr    write_file_async(Job* j, const std::string& filename, const unsigned long long& size, std::unique_ptr<DispatcherPlugin>& dispatcher);
};

#endif

