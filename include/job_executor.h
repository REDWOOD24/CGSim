#ifndef JOB_EXECUTOR_H
#define JOB_EXECUTOR_H

#include "DispatcherPlugin.h"
#include "PluginLoader.h"
#include "job_manager.h"
#include "actions.h"
#include "sqliteSaver.h"
#include "logger.h"
#include "host_extensions.h"
#include "job_stats.h"

class JOB_EXECUTOR
{
public:
     JOB_EXECUTOR(){LOG_INFO("Initalizing Job Executor .....");};
    ~JOB_EXECUTOR()= default;



    static void set_dispatcher(const std::string& dispatcherPath, sg4::NetZone* platform);
    static void update_disk_content(const std::shared_ptr<simgrid::fsmod::FileSystem>& fs, const std::unordered_map<std::string, size_t>&  input_files, Job* j);
	static void start_server(JobQueue jobs);
    static int  coreReleased(std::string &activityName);
	// static void suspend_server();
    // static bool is_suspended();
    static void execute_job(Job* j);
    void        start_job_execution(JobQueue jobs);
    static void receiver(const std::string& MQ_name);
    static void start_receivers();
    static void set_output(const std::string& outputFile);
    static void saveJobs(JobQueue jobs);
    static void attach_callbacks();
    static void on_job_finished(Job* j);
    static std::string get_job_time_stamp(std::string jobCreationTime, double simgrid_clock);
    static double get_job_queue_time(std::string jobCreationTime, std::string jobStartTime);
    static void set_fixed_creation_time(const std::string& creation_time); 
    static std::string& get_fixed_creation_time();
private:
    static   std::unique_ptr<DispatcherPlugin>    dispatcher;
    static   std::unique_ptr<sqliteSaver>         saver;
    static   constexpr int MAX_RETRIES            = 300;
    static   constexpr int RETRY_INTERVAL         = 1000000000; //seconds
    // static   constexpr bool suspended             = false;
    // static   std::vector<Job*> pending_jobs;
    static   sg4::ActivitySet pending_activities;
    static   sg4::ActivitySet exec_activities;
    static sg4::NetZone*      platform;
    static std::unordered_map<std::string, JobSiteStats> site_statistics;
    static std::string fixed_creation_time;
};

#endif //JOB_EXECUTOR_H