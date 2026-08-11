#ifndef JOB_H
#define JOB_H
#include <string>
#include <unordered_map>
#include <queue>
#include <unordered_set>
using Files = std::unordered_map<std::string, std::pair<long long, std::unordered_set<std::string>>>;

namespace CGSim
{
    enum struct STATUS {
    GlOBAL_PENDING,
    SITE_PENDING,
    ASSIGNED,
    RUNNING,
    FINISHED,
    FAILED,
    NONE
};
}

//Information needed to a specify a Job
struct Job 
{
    long long                                      jobid{};
    double                                         creation_time{-1.0};
    double                                         submission_time{};
    CGSim::STATUS                                  status;
    long long                                      flops{};
    int                                            cores{};
    std::string                                    disk{};
    std::string                                    comp_site{};
    std::string                                    comp_host{};
    double                                         memory_usage{};
    int                                            retries{};
    double                                         disk_read_bw{};
    double                                         disk_write_bw{};
    double                                         comp_host_speed{};
    double                                         cpu_consumption_time{};
    double                                         total_io_read_time{};
    double                                         total_io_write_time{};
    double                                         file_transfer_queue_time{};
    double                                         resource_waiting_queue_time{};
    std::unordered_set<std::string>                input_files{};
    Files                                          input_files_sizes_locations{}; //Change this later
    std::unordered_map<std::string, long long>     output_files{};
    long                                           files_written{};
    std::unordered_map<std::string,std::string>    metadata{};
    std::unordered_set<long long>                  parents{};               
    std::unordered_map<long long, double>          children{};
    void add_parent(long long parent)              {this->parents.insert(parent);}               
    void add_child (long long child, double time)  {this->children[child] = time;}               
    bool operator< (const Job& other) const        {return creation_time > other.creation_time;}
};
struct JobPtrCompare 
{
    bool operator()(const Job* a, const Job* b) const 
    {
        const bool a_negative = a->creation_time < 0;
        const bool b_negative = b->creation_time < 0;
        if (a_negative != b_negative) return a_negative;
        return a->creation_time > b->creation_time;
    }
};
using JobQueue = std::priority_queue<Job*, std::vector<Job*>, JobPtrCompare>;


#endif //JOB_H
