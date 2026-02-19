#include "job_executor.h"

std::unique_ptr<DispatcherPlugin>   JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                    JOB_EXECUTOR::pending_activities;
std::vector<Job*>                   JOB_EXECUTOR::pending_jobs;
JobQueue                            JOB_EXECUTOR::jobs;
unsigned long                       JOB_EXECUTOR::MAX_RETRIES = 20;
unsigned long                       JOB_EXECUTOR::USED_CORES = 0;
unsigned long                       JOB_EXECUTOR::TOTAL_CORES;
unsigned long                       JOB_EXECUTOR::DISPATCHED_JOBS;
unsigned long                       JOB_EXECUTOR::TOTAL_JOBS;



void JOB_EXECUTOR::start_job_execution()
{
  TOTAL_CORES = 48;/*std::stoul((sg4::Engine::get_instance()->get_netzone_root())->get_property("grid_cores"));
  attach_callbacks();*/
  sg4::Host* job_server = sg4::Host::by_name("JOB-SERVER_cpu-0");
  if (!job_server) throw std::runtime_error("JOB-SERVER not initialized properly");
  jobs = std::move(dispatcher->getWorkload());
  TOTAL_JOBS = jobs.size();
  DISPATCHED_JOBS = 0;
  sg4::Actor::create("JOB-EXECUTOR-actor",job_server,start_server);
  sg4::Engine::get_instance()->run();
}

void JOB_EXECUTOR::get_jobs()
{
  while(!jobs.empty())
  {
    Job* job = jobs.top();
    if (sg4::Engine::get_clock() >= job->creation_time) 
    {
      CGSim::FileManager::request_file_location(job);
      pending_jobs.push_back(job);
      job->submission_time = sg4::Engine::get_clock();
      std::cout << job->jobid << ", submission time " << job->submission_time << ", creation time " << job->creation_time << std::endl;
      dispatcher->onJobSubmission(job);
      jobs.pop();
    }
    else break;
  }
}

void JOB_EXECUTOR::advance_to_time(double time)
{
  while (sg4::Engine::get_clock() < time) 
  {
    if (pending_activities.empty()) 
    {
      sg4::this_actor::sleep_until(time);
      return;
    }

    try 
    {
      pending_activities.wait_any_for(time - sg4::Engine::get_clock());
      while (true) {if(!pending_activities.test_any()) break;}
    }
    catch (const simgrid::TimeoutException&) {return;}
  }  
}


void JOB_EXECUTOR::start_server()
{
  while (DISPATCHED_JOBS != TOTAL_JOBS)
  {
    if(!jobs.empty())
    {
    std::cout << sg4::Engine::get_clock() << std::endl;
    std::cout << jobs.top()->creation_time << std::endl;
    if(sg4::Engine::get_clock() < jobs.top()->creation_time) advance_to_time(jobs.top()->creation_time);
    get_jobs();
    }

    for (auto it = pending_jobs.begin(); it != pending_jobs.end();)
    {
      if((1.0*USED_CORES)/(1.0*TOTAL_CORES) > 0.8) break;
      Job* job = *it;
      dispatcher->assignJob(job);
      if (job->status == "assigned"){onJobAssignment(job); it = pending_jobs.erase(it);}
      else if (job->status == "pending"){job->retries++; ++it;}
      else ++it;
    }

    while ((1.0*USED_CORES)/(1.0*TOTAL_CORES) >= 0.6) {pending_activities.wait_any();}
  }
  while (!pending_activities.empty()){pending_activities.wait_any();}
}

void JOB_EXECUTOR::onJobAssignment(Job* job)
{
  DISPATCHED_JOBS++;
  USED_CORES += job->cores; 
  std::cout << "Job: " << job->jobid << ", Status: " << job->status << " after " << job->retries << " tries" <<std::endl;
  sg4::Host::by_name(job->comp_host)->extension<HostExtensions>()->registerJob(job);
  sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
  sg4::MessPtr job_transfer = mqueue->put_async(job)->set_name("Transfer_Job_" + std::to_string(job->jobid) + "_to_" + job->comp_host+"_from_JOB-SERVER");
  job_transfer->on_this_start_cb([job](simgrid::s4u::Mess const& me) {dispatcher->onJobTransferStart(job, me);});
  job_transfer->on_this_completion_cb([job](simgrid::s4u::Mess const& me)
    {job->resource_waiting_queue_time = sg4::Engine::get_clock() - job->creation_time; dispatcher->onJobTransferEnd(job, me);});
  pending_activities.push(job_transfer);
}

void JOB_EXECUTOR::execute_job(Job* j)
{
  auto exec_activity = Actions::exec_task_multi_thread_async(j);
  std::vector<sg4::IoPtr>   read_activities;
  std::vector<sg4::CommPtr> comm_activities;
  std::vector<sg4::IoPtr>   write_activities;

  for (const auto& [filename,fileinfo] : j->input_files) {
    
    //Take the first location where the file is located, may want to change this behavior later
    auto filelocation = *(fileinfo.second.begin());

    if (filelocation != j->comp_site) { //Check if in list, not first element

      auto comm_activity = Actions::transfer_file_async(j,filename,filelocation,j->comp_site);
      auto read_activity = Actions::read_file_async(j,filename);

      comm_activity->add_successor(read_activity);
      read_activity->add_successor(exec_activity);

      comm_activities.push_back(comm_activity);
      read_activities.push_back(read_activity);
    }
    else{
      auto read_activity = Actions::read_file_async(j,filename);
      read_activity->add_successor(exec_activity);
      read_activities.push_back(read_activity);
    }
  }

  for (const auto& [filename,size] : j->output_files) {
    auto write_activity = Actions::write_file_async(j,filename,size);
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

[[noreturn]] void JOB_EXECUTOR::receiver(const std::string& MQ_name)
{
  sg4::Actor::self()->daemonize();
  sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(MQ_name);

  while (true)
    {
    sg4::MessPtr mess = mqueue->get_async();
    mess->wait();
    auto* job = static_cast<Job*>(mess->get_payload());
    execute_job(job);
    }
}

void JOB_EXECUTOR::start_receivers()
{
  for (const auto& host : sg4::Engine::get_instance()->get_all_hosts()) 
  {
    if (host->get_name().find("JOB-SERVER_cpu") != std::string::npos) continue;
    if (host->get_name().find("_communication_server") != std::string::npos) continue;
    sg4::Actor::create(host->get_name() + "-actor", host, receiver, host->get_name() + "-MQ");
  }
}

void JOB_EXECUTOR::attach_callbacks()
{
  sg4::Engine::on_simulation_start_cb([](){dispatcher->onSimulationStart();});
  sg4::Engine::on_simulation_end_cb([]() {dispatcher->onSimulationEnd();});
}

