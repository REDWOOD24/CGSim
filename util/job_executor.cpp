#include "job_executor.h"

std::unique_ptr<DispatcherPlugin>   JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                    JOB_EXECUTOR::pending_activities;
sg4::ActivitySet                    JOB_EXECUTOR::exec_activities;
std::vector<Job*>                   JOB_EXECUTOR::pending_jobs;
JobQueue                            JOB_EXECUTOR::jobs;
std::unordered_map<Job*, int>       JOB_EXECUTOR::retry_counts;


void JOB_EXECUTOR::start_job_execution(long num_of_jobs_to_run)
{
  attach_callbacks();
  const auto* e = sg4::Engine::get_instance();
  sg4::Host* job_server = sg4::Engine::get_instance()->host_by_name("JOB-SERVER_cpu-0");
  if (!job_server) { throw std::runtime_error("JOB-SERVER not initialized properly");}
  JobQueue jobs = dispatcher->getWorkload(num_of_jobs_to_run);
  sg4::Actor::create("JOB-EXECUTOR-actor", job_server,start_server,jobs);
  e->run();
}

void JOB_EXECUTOR::start_server(JobQueue jobs) {

  while (!jobs.empty()) {
    Job* job = jobs.top(); jobs.pop();

    CGSim::FileManager::request_file_location(job);
    dispatcher->assignJob(job);

    if (job->status == "assigned") {
      sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
      pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + job->id + "_on_" + job->comp_host));
    }
    else {
      if(job->status != "failed"){job->status = "pending"; pending_jobs.push_back(job);}
    }

  }

  for (Job* job : pending_jobs) {retry_counts[job] = 0;}

  // Poll the pending jobs list until none remain.
  while (!pending_jobs.empty()) {

    for (auto it = pending_jobs.begin(); it != pending_jobs.end(); ) {
      Job* job = *it;
      dispatcher->assignJob(job);
      retry_counts[job]++;

      if (job->status == "assigned") {
        sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
        pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + job->id + "_on_" + job->comp_host));
        it = pending_jobs.erase(it);
      }
      else ++it;
    }

    if (!exec_activities.empty()) {
      auto activityPtr = exec_activities.wait_any();
    }
  }


  while (!exec_activities.empty())
    {
    exec_activities.wait_any();
    pending_activities.wait_all();
    }

}

void JOB_EXECUTOR::execute_job(Job* j)
{
  j->status = "running";
  Actions::read_file_async(j, pending_activities, dispatcher);
  Actions::exec_task_multi_thread_async(j, pending_activities, exec_activities, dispatcher);
  Actions::write_file_async(j, pending_activities, dispatcher);

}

void JOB_EXECUTOR::receiver(const std::string& MQ_name)
{
  sg4::Actor::self()->daemonize();
  sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(MQ_name);

  while (sg4::this_actor::get_host()->is_on())
    {
    sg4::MessPtr mess = mqueue->get_async();
    mess->wait();
    auto* job = static_cast<Job*>(mess->get_payload());
    execute_job(job);
    }
}

void JOB_EXECUTOR::start_receivers()
{
  const auto*  eng    = sg4::Engine::get_instance();
  auto         hosts  = eng->get_all_hosts();
  for (const auto& host : hosts) {
    if (host->get_name() == "JOB-SERVER_cpu-0") continue;
    sg4::Actor::create(host->get_name() + "-actor", host, receiver, host->get_name() + "-MQ");
  }
}


void JOB_EXECUTOR::attach_callbacks()
{
  sg4::Engine::on_simulation_start_cb([]() {

  });

  sg4::Engine::on_simulation_end_cb([]() {
    dispatcher->onSimulationEnd();
  });
}
