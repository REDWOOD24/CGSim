#ifndef JOB_EXECUTOR_H
#define JOB_EXECUTOR_H

#include "DispatcherPlugin.h"
#include "PluginLoader.h"
#include "actions.h"
#include "host_extensions.h"
#include <chrono>

class JOB_EXECUTOR
{


public:
     JOB_EXECUTOR()= default;
    ~JOB_EXECUTOR()= default;


    static void set_dispatcher(std::unique_ptr<DispatcherPlugin>& d){dispatcher = std::move(d);}
    static void start_server(JobQueue jobs);
    static void execute_job(Job* j);
    static void start_job_execution(long num_of_jobs_to_run);
    static void receiver(const std::string& MQ_name);
    static void start_receivers();
    static void saveJobs();
    static void attach_callbacks();


private:
    static   std::unique_ptr<DispatcherPlugin>      dispatcher;
    static   constexpr int MAX_RETRIES            = 300;
    static   constexpr int RETRY_INTERVAL         = 1000000000; //seconds
    static   sg4::ActivitySet pending_activities;
    static   sg4::ActivitySet exec_activities;
    static   JobQueue jobs;
    static   std::vector<Job*> pending_jobs;
    static   std::unordered_map<Job*, int> retry_counts;

};

#endif //JOB_EXECUTOR_H
