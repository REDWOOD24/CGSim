#ifndef JOB_EXECUTOR_H
#define JOB_EXECUTOR_H

#include "DispatcherPlugin.h"
#include "PluginLoader.h"
#include "actions.h"
#include "policy_manager.h"
#include "host_extensions.h"
#include <chrono>
#include <simgrid/kernel/Timer.hpp>

class Actions;

class JOB_EXECUTOR
{


public:
     JOB_EXECUTOR()= default;
    ~JOB_EXECUTOR()= default;
    friend class Actions;

    static void                set_dispatcher(std::unique_ptr<DispatcherPlugin>& d){dispatcher = std::move(d);}
    static void                start_job_execution();


private:
    static void                start_receivers();
    static void                start_server();
    static void                execute_job(Job* j);
    static void                onJobAssignment(Job* job);
    static void                get_jobs();
    static void                advance_to_time(double time);
    [[noreturn]] static void   receiver(const std::string& MQ_name);
    static void                attach_callbacks();
    static unsigned long       totalJobs(JobQueue jobs);


    static   sg4::ActivitySet                     pending_activities;
    static   unsigned long                        USED_CORES;
    static   unsigned long                        TOTAL_CORES;
    static   unsigned long                        DISPATCHED_JOBS;
    static   unsigned long                        FINISHED_JOBS;
    static   unsigned long                        ACTIVATED_JOBS;
    static   unsigned long                        TOTAL_JOBS;
    static   unsigned long                        MAX_RETRIES;
    static   JobQueue                             jobs;
    static   std::unordered_map<long long, Job*>  all_jobs;
    static   std::vector<Job*>                    pending_jobs;
    static   std::unique_ptr<DispatcherPlugin>    dispatcher;


};

#endif //JOB_EXECUTOR_H
