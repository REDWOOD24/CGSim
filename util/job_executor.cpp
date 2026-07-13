#include "job_executor.h"

std::shared_ptr<DispatcherPlugin>    JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                     JOB_EXECUTOR::pending_activities;
std::unordered_map<long long, Job*>  JOB_EXECUTOR::all_jobs;
std::vector<Job*>                    JOB_EXECUTOR::pending_jobs;
JobQueue                             JOB_EXECUTOR::jobs;
unsigned long                        JOB_EXECUTOR::MAX_RETRIES = 20;
unsigned long                        JOB_EXECUTOR::DISPATCHED_JOBS = 0;
unsigned long                        JOB_EXECUTOR::ACTIVATED_JOBS = 0;
unsigned long                        JOB_EXECUTOR::FINISHED_JOBS = 0;
unsigned long                        JOB_EXECUTOR::TOTAL_JOBS;

unsigned long JOB_EXECUTOR::totalJobs(JobQueue jobs)
{
  while (!jobs.empty()) 
  {
      auto* job = jobs.top();
      all_jobs[job->jobid] = job;
      jobs.pop();
  }
  return all_jobs.size();
}

void JOB_EXECUTOR::start_job_execution()
{
  start_receivers();
  attach_callbacks();
  sg4::Host* job_server = sg4::Host::by_name("JOB-SERVER_cpu-0");
  if (!job_server) throw std::runtime_error("JOB-SERVER not initialized properly");
  jobs = std::move(dispatcher->getWorkload());
  TOTAL_JOBS = totalJobs(jobs);
  std::cout << "TOTAL_JOBS: " << TOTAL_JOBS << std::endl;
  sg4::Actor::create("JOB-EXECUTOR-actor",job_server,start_server);
  sg4::Engine::get_instance()->run();
}

void JOB_EXECUTOR::get_jobs()
{
  while(DISPATCHED_JOBS != TOTAL_JOBS  && !jobs.empty())
  {
    Job* job = jobs.top();
    if(job->creation_time < 0) break;
    if (sg4::Engine::get_clock() >= job->creation_time) 
    {
      CGSim::get_file_manager()->request_file_location(job);
      pending_jobs.push_back(job);
      job->submission_time = sg4::Engine::get_clock();
      std::cout << job->jobid << ", submission time " << job->submission_time << ", creation time " << job->creation_time << std::endl;
      job->status = "submitted";
      CGSim::get_site_manager()->addSystemPendingJob();
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
      if (ACTIVATED_JOBS < DISPATCHED_JOBS){sg4::this_actor::yield(); continue;}
      sg4::this_actor::sleep_until(time); 
      return;
    }

    try 
    {
      auto act = pending_activities.wait_any_for(time - sg4::Engine::get_clock());
      while (pending_activities.test_any()){}
      if(act && time > sg4::Engine::get_clock()){if(act->get_name().find("Exec") != std::string::npos){dispatch_system_pending_jobs();}}    
    }
    catch (const simgrid::TimeoutException&) {return;}
  }  
}


void JOB_EXECUTOR::dispatch_system_pending_jobs()
{
  for (auto it = pending_jobs.begin(); it != pending_jobs.end();)
  {
  if(dispatcher->stopJobAssignment()) break;
  Job* job = *it;
  dispatcher->assignJob(job);
  if(job->comp_host != ""){job->status = "assigned"; CGSim::get_site_manager()->moveSystemPendingtoPendingJob(job->comp_site); onJobAssignment(job); it = pending_jobs.erase(it);}
  else {job->status = "pending"; job->retries++; ++it;}
  }
}


void JOB_EXECUTOR::start_server()
{
  while (DISPATCHED_JOBS != TOTAL_JOBS)
  {
    std::cout << DISPATCHED_JOBS << " / " << TOTAL_JOBS << " jobs dispatched" << std::endl;
    std::cout << "Pending Jobs: " << pending_jobs.size() << std::endl;
    std::cout << "Pending Activities: " <<  pending_activities.size() << std::endl;
    std::cout << "Current Simulated Time: " << sg4::Engine::get_clock() << std::endl;
    std::cout << "CORE USAGE: " << CGSim::get_site_manager()->getGridCPUUtilization() << std::endl;

    if(pending_jobs.size() + DISPATCHED_JOBS != TOTAL_JOBS) //PENDING_JOBS.size() + DISPATCHED JOBS <= TOTAL JOBS
    {
    if(sg4::Engine::get_clock() < jobs.top()->creation_time) advance_to_time(jobs.top()->creation_time);
    get_jobs();
    }

    else
    {
      while(!pending_activities.empty())
      {
        auto act = pending_activities.wait_any();
        if(act->get_name().find("Exec") != std::string::npos) break;
      }
    }

    dispatch_system_pending_jobs();
  }

  while (ACTIVATED_JOBS != TOTAL_JOBS || !pending_activities.empty())
  {
    if (!pending_activities.empty()) pending_activities.wait_any();
    else sg4::this_actor::yield();
  }

  CGSim::PolicyManager::RUNNING = false;
}

void JOB_EXECUTOR::onJobAssignment(Job* job)
{
  DISPATCHED_JOBS++;
  std::cout << "Job: " << job->jobid << ", Cores: " << job->cores  << ", Status: " << job->status << " after " << job->retries << " tries" <<std::endl;
  sg4::Host::by_name(job->comp_host)->extension<HostExtensions>()->registerJob(job);
  dispatcher->onJobAssignment(job);
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

  for (const auto& [filename,fileinfo] : j->input_files_sizes_locations) 
  {
    std::string filelocation = "";
    CGSim::FileTransferDecisionMode mode = CGSim::FileTransferDecisionMode::COPY;

    dispatcher->onFileRequest(j, filename, fileinfo.first, fileinfo.second, filelocation, mode);
    if(filelocation.empty()) throw std::runtime_error("File location not specified for file: "+filename);

    if (filelocation != j->comp_site) 
    { 

      auto comm_activity = Actions::transfer_file_async(j,filename,filelocation,j->comp_site,mode);
      auto read_activity = Actions::read_file_async(j,filename);

      comm_activity->add_successor(read_activity);
      read_activity->add_successor(exec_activity);

      comm_activities.push_back(comm_activity);
      read_activities.push_back(read_activity);
    }
    else
    {
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

  ACTIVATED_JOBS++;
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

