#include "job_executor.h"

std::unique_ptr<DispatcherPlugin>   JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                    JOB_EXECUTOR::pending_activities;
std::vector<Job*>                   JOB_EXECUTOR::pending_jobs;
JobQueue                            JOB_EXECUTOR::jobs;
std::unordered_map<Job*, int>       JOB_EXECUTOR::retry_counts;
unsigned long                       JOB_EXECUTOR::MAX_RETRIES;


void JOB_EXECUTOR::start_job_execution(long num_of_jobs_to_run)
{
  attach_callbacks();
  sg4::Host* job_server = sg4::Host::by_name("JOB-SERVER_cpu-0");
  if (!job_server) {throw std::runtime_error("JOB-SERVER not initialized properly");}
  JobQueue jobs = dispatcher->getWorkload(num_of_jobs_to_run);
  sg4::Actor::create("JOB-EXECUTOR-actor",job_server,start_server,jobs);
  sg4::Engine::get_instance()->run();
}

void JOB_EXECUTOR::start_server(JobQueue jobs) {

  while (!jobs.empty()) {
    std::cout << "Jobs left: " << jobs.size() << std::endl;
    Job* job = jobs.top();
    jobs.pop();

    std::cout << "Processing job " << job->jobid << " with status " << job->status << std::endl;

    CGSim::FileManager::request_file_location(job);
    std::cout << "File requested for job " << job->jobid << std::endl;

    dispatcher->assignJob(job);
    std::cout << "Job " << job->jobid << " status: " << job->status << std::endl;

    if (job->status == "assigned") {
      sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
      std::cout << "Putting job " << job->jobid << " on MQ " << job->comp_host << std::endl;
      //auto send_activity = mqueue->put_async(job)->set_name("Comm_send_Job_" + std::to_string(job->jobid) + "_on_" + job->comp_host);
      //send_activity->on_this_completion_cb([](simgrid::s4u::Mess const& me) {
       // std::cout << me.get_cname() << ", Finished on:  " << me.get_finish_time() << std::endl;
         //  });
      sg4::MessPtr act = mqueue->put_async(job)->set_name("Comm_send_Job_" + std::to_string(job->jobid) + "_on_" + job->comp_host);
      act->on_this_completion_cb([](simgrid::s4u::Mess const& me) {std::cout << "Sending Job: " << me.get_name() << std::endl;});
      pending_activities.push(act);
      std::cout << "Job " << job->jobid << " async put done" << std::endl;
    } else {
      if (job->status != "failed") {
        job->status = "pending";
        pending_jobs.push_back(job);
        std::cout << "Job " << job->jobid << " pushed to pending_jobs" << std::endl;
      } else {
        std::cout << "Job " << job->jobid << " failed" << std::endl;
      }
    }
  }
  MAX_RETRIES = 2*pending_jobs.size();
  while (true) {if (pending_activities.wait_any()->get_name().find("Exec") != std::string::npos) break;}

  for (Job* job : pending_jobs) {retry_counts[job] = 0;}

  // Poll the pending jobs list until none remain.
  while (!pending_jobs.empty()) {

    for (auto it = pending_jobs.begin(); it != pending_jobs.end(); ) {
      Job* job = *it;
      dispatcher->assignJob(job);
      retry_counts[job]++;

      if (job->status == "assigned") {
        sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
        sg4::MessPtr act = mqueue->put_async(job)->set_name("Comm_send_Job_" + job->id + "_on_" + job->comp_host);
        act->on_this_completion_cb([](simgrid::s4u::Mess const& me) {std::cout << "Sending Job: " << me.get_name() << std::endl;});
        pending_activities.push(act);
        it = pending_jobs.erase(it);
      }
      else ++it;
    }
    while (true) {if (pending_activities.wait_any()->get_name().find("Exec") != std::string::npos) break;}
  }

  while (!pending_activities.empty())
  {
    pending_activities.wait_any();
    std::cout << sg4::Engine::get_clock() <<  std::endl;
  }


}

void JOB_EXECUTOR::execute_job(Job* j)
{
  j->status = "running";
  auto exec_activity = Actions::exec_task_multi_thread_async(j,dispatcher);
  std::vector<sg4::IoPtr>   read_activities;
  std::vector<sg4::CommPtr> comm_activities;
  std::vector<sg4::IoPtr>   write_activities;

  for (const auto& [filename,fileinfo] : j->input_files) {
    auto size = fileinfo.first;
    auto filelocation = *(fileinfo.second.begin());

    if (filelocation != j->comp_site) {

      auto comm_activity = Actions::comm_file_async(j,filename,filelocation,j->comp_site,size,dispatcher);
      auto read_activity = Actions::read_file_async(j,filename,dispatcher);

      comm_activity->add_successor(read_activity);
      read_activity->add_successor(exec_activity);

      comm_activities.push_back(comm_activity);
      read_activities.push_back(read_activity);
    }
    else{
      auto read_activity = Actions::read_file_async(j,filename,dispatcher);
      read_activity->add_successor(exec_activity);
      read_activities.push_back(read_activity);
    }
  }

  for (const auto& [filename,size] : j->output_files) {
    auto write_activity = Actions::write_file_async(j,filename,size,dispatcher);
    exec_activity->add_successor(write_activity);
    write_activities.push_back(write_activity);
  }

  for (const auto& comm_activity : comm_activities) {pending_activities.push(comm_activity);}
  for (const auto& read_activity : read_activities) {pending_activities.push(read_activity);}
  pending_activities.push(exec_activity);
  for (const auto& write_activity : write_activities) {pending_activities.push(write_activity);}


  for (const auto& comm_activity : comm_activities) {comm_activity->start();}
  for (const auto& read_activity : read_activities) {read_activity->start();}
  exec_activity->start();
  for (const auto& write_activity : write_activities) {write_activity->start();}

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
