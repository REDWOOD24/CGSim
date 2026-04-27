#ifndef JOB_H
#define JOB_H
#include <string>
#include <unordered_map>
#include <queue>
#include <unordered_set>
using Files    = std::unordered_map<std::string, std::pair<unsigned long long, std::unordered_set<std::string>>>;

//Information needed to a specify a Job
struct Job 
{
    long long                                   jobid{};
    long long                                   creation_time{};
    long long                                   submission_time{};
    std::string                                 status{};
    long long                                   flops{};
    int                                         cores{};
    std::string                                 disk{};
    std::string                                 comp_site{};
    std::string                                 comp_host{};
    double                                      memory_usage{};
    int                                         retries{};
    double                                      disk_read_bw{};
    double                                      disk_write_bw{};
    double                                      comp_host_speed{};
    double                                      cpu_consumption_time{};
    double                                      total_io_read_time{};
    double                                      total_io_write_time{};
    double                                      file_transfer_queue_time{};
    double                                      resource_waiting_queue_time{};
    Files                                       input_files{};
    std::unordered_map<std::string, long long>  output_files{};
    bool operator<(const Job& other) const {return creation_time >= other.creation_time;}
};
struct JobPtrCompare {bool operator()(const Job* a, const Job* b) const {return a->creation_time >= b->creation_time;}};

using JobQueue = std::priority_queue<Job*, std::vector<Job*>, JobPtrCompare>;


#endif //JOB_H
