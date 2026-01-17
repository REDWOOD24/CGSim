#ifndef JOB_H
#define JOB_H
#include <string>
#include <unordered_map>
#include <queue>
#include <unordered_set>
using Files    = std::unordered_map<std::string, std::pair<long long, std::unordered_set<std::string>>>;


//Information needed to a specify a Job
struct Job {
    long long                                   jobid{};
    std::string                                 status{};
    long long                                   flops{};
    int                                         cores{};
    int                                         priority{};
    std::string                                 disk{};
    std::string                                 comp_site{};
    std::string                                 comp_host{};
    double                                      memory_usage{};
    std::string                                 id{};
    int                                         retries{};
    double                                      disk_read_bw{};
    double                                      disk_write_bw{};
    double                                      comp_host_speed{};
    double                                      cpu_consumption_time;
    double                                      total_io_read_time{};
    double                                      total_io_write_time{};
    double                                      file_transfer_queue_time{};
    double                                      resource_waiting_queue_time{};
    Files                                       input_files{};
    std::unordered_map<std::string, long long>  output_files{};
    bool operator<(const Job& other) const {if(priority == other.priority){return jobid > other.jobid;} return priority < other.priority;}
  };

using JobQueue = std::priority_queue<Job*>;


#endif //JOB_H
