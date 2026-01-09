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
#include "fsmod.hpp"
#include "DispatcherPlugin.h"
#include "host_extensions.h"

namespace sg4 = simgrid::s4u;

class Actions
{
public:
     Actions() = default;
    ~Actions() = default;

    static void  exec_task_multi_thread_async(Job* j, sg4::ActivitySet& pending_activities, sg4::ActivitySet& exec_activities, std::unique_ptr<DispatcherPlugin>& dispatcher);
    static void  read_file_async(const std::shared_ptr<simgrid::fsmod::FileSystem>& fs, Job* j, sg4::ActivitySet& pending_activities, std::unique_ptr<DispatcherPlugin>& dispatcher);
    static void  write_file_async(const std::shared_ptr<simgrid::fsmod::FileSystem>& fs, Job* j, sg4::ActivitySet& pending_activities, std::unique_ptr<DispatcherPlugin>& dispatcher);

};

#endif

