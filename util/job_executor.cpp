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
  sg4::Host* job_server = sg4::Host::by_name("JOB-SERVER_cpu-0");
  if (!job_server) {throw std::runtime_error("JOB-SERVER not initialized properly");}
  JobQueue jobs = dispatcher->getWorkload(num_of_jobs_to_run);
  sg4::Actor::create("JOB-EXECUTOR-actor",job_server,start_server,jobs);
  sg4::Engine::get_instance()->run();
}

void JOB_EXECUTOR::start_server(JobQueue jobs) {

  while (!jobs.empty()) {
    Job* job = jobs.top();
    jobs.pop();

    CGSim::FileManager::request_file_location(job);
    dispatcher->assignJob(job);
    job->status = "assigned";
    job->comp_host = "AGLT2_site_0_cpu-4";
    job->comp_site = "AGLT2_site_0";
    job->disk= "AGLT2_CALIBDISK";

    if (job->status == "assigned") {
      sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
      pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + std::to_string(job->jobid) + "_on_" + job->comp_host));}
    else {if(job->status != "failed"){job->status = "pending"; pending_jobs.push_back(job);}}
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

  while (!pending_activities.empty())
  {
    std::cout << sg4::Engine::get_clock() <<  std::endl;
    pending_activities.wait_any();
    //pending_activities.wait_any();
  }

  /*while (!exec_activities.empty())
    {
    std::cout << sg4::Engine::get_clock() <<  std::endl;
    exec_activities.wait_any();
    //pending_activities.wait_any();
    }*/

  //pending_activities.wait_all();


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

  for (const auto& [filename,size] : j->output_file) {
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
