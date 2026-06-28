#include "workload_manager.h"
#include <random>

int WORKLOAD_MANAGER::JOB_ID = 1;

long long WORKLOAD_MANAGER::random_number(long long min, long long max) 
{
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    std::uniform_int_distribution<long long> dist(min, max);
    return dist(gen);
}

Job* WORKLOAD_MANAGER::createJob()
{
    Job* job = new Job();
    job->jobid = JOB_ID;
    job->cores = random_number(1,8);
    job->flops = random_number(1000000,2000000);

    int number_of_input_files  = random_number(1,5);
    int number_of_output_files = 1; //random_number(1,3);


    for(int j = 0; j <= number_of_input_files; j++)
        {
            auto file = std::to_string(random_number(0,29999));
            job->input_files.insert(file);
        }

    for(int j = 0; j <= number_of_output_files; j++)
    {
        auto output_file_name = "output_" + std::to_string(job->jobid) + "_" +std::to_string(j) + ".root";
        auto output_file_size = random_number(200000,300000);
        job->output_files[output_file_name] = output_file_size;
    }

    JOB_ID++;

    return job;
}

JobQueue WORKLOAD_MANAGER::getWorkload() 
{

    sg4::NetZone* platform = sg4::Engine::get_instance()->get_netzone_root();
    long max_jobs = std::stol(platform->get_property("Num_of_Jobs"));
    JobQueue jobs;

    for(int i = 1; i <= max_jobs; i++)
    {
        Job* parent_job_1 = createJob();
        parent_job_1->creation_time = random_number(0,3000);

        Job* parent_job_2 = createJob();
        parent_job_2->creation_time = random_number(5000,300000);

        Job* child_job_1 = createJob();

        Job* child_job_2 = createJob();

        parent_job_1->add_child(child_job_1->jobid, 2.0);
        child_job_1->add_parent(parent_job_1->jobid);

        parent_job_2->add_child(child_job_1->jobid, 2.0);
        child_job_1->add_parent(parent_job_2->jobid);

        parent_job_1->add_child(child_job_2->jobid, 2.0);
        child_job_2->add_parent(parent_job_1->jobid);


        jobs.push(parent_job_1);
        jobs.push(parent_job_2);
        jobs.push(child_job_1);
        jobs.push(child_job_2);


    }

    return jobs;
}
