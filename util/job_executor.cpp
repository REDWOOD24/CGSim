#include "job_executor.h"
#include "data_management_policy.h"
#include "logger.h"

std::unique_ptr<DispatcherPlugin>   JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                    JOB_EXECUTOR::pending_activities;
std::vector<Job*>                   JOB_EXECUTOR::pending_jobs;
JobQueue                            JOB_EXECUTOR::jobs;
std::unordered_map<Job*, int>       JOB_EXECUTOR::retry_counts;
unsigned long                       JOB_EXECUTOR::MAX_RETRIES;
std::atomic<bool>                   JOB_EXECUTOR::simulation_done{false};

void JOB_EXECUTOR::start_job_execution()
{
  simulation_done.store(false);
  attach_callbacks();
  sg4::Host* job_server = sg4::Host::by_name("JOB-SERVER_cpu-0");
  if (!job_server) {throw std::runtime_error("JOB-SERVER not initialized properly");}
  JobQueue jobs = dispatcher->getWorkload();
  sg4::Actor::create("JOB-EXECUTOR-actor",job_server,start_server,jobs);
  sg4::Engine::get_instance()->run();
}

void JOB_EXECUTOR::start_server(JobQueue jobs)
{
  while (!jobs.empty())
  {
    Job* job = jobs.top();
    jobs.pop();

    CGSim::FileManager::request_file_location(job);
    dispatcher->assignJob(job);

    if (job->status == "assigned")
    {
      std::cout << "Job: " << job->jobid << ", Status: " << job->status << " after " << retry_counts[job] << " tries" <<std::endl;
      sg4::Host::by_name(job->comp_host)->extension<HostExtensions>()->registerJob(job);
      sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
      sg4::MessPtr transfer = mqueue->put_async(job)->set_name("Comm_send_Job_" + std::to_string(job->jobid) + "_to_" + job->comp_host+"_from_JOB-SERVER");
      transfer->on_this_start_cb([job](simgrid::s4u::Mess const& me) {dispatcher->onJobTransferStart(job, me);});
      transfer->on_this_completion_cb([job](simgrid::s4u::Mess const& me)
        {job->resource_waiting_queue_time = sg4::Engine::get_clock(); dispatcher->onJobTransferEnd(job, me);});
      pending_activities.push(transfer);
    }
    else if (job->status == "pending") pending_jobs.push_back(job);
  }

  // Initialize retry counters for all jobs that were not assigned in the first pass.
  MAX_RETRIES = 2*pending_jobs.size();
  for (Job* job : pending_jobs) {
    retry_counts[job] = 0;
  }

  // Poll the pending jobs list until none remain.
  while (!pending_jobs.empty())
  {
    for (auto it = pending_jobs.begin(); it != pending_jobs.end(); )
    {
      Job* job = *it;
      dispatcher->assignJob(job);
      retry_counts[job]++;
      job->retries++;

      if (job->status == "assigned")
      {
        std::cout << "Job: " << job->jobid << ", Status: " << job->status << " after " << retry_counts[job] << " tries" <<std::endl;
        sg4::Host::by_name(job->comp_host)->extension<HostExtensions>()->registerJob(job);
        sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
        sg4::MessPtr transfer = mqueue->put_async(job)->set_name("Comm_send_Job_" + job->id + "_to_" + job->comp_host+"_from_JOB-SERVER");
        transfer->on_this_start_cb([job](simgrid::s4u::Mess const& me) {dispatcher->onJobTransferStart(job, me);});
        transfer->on_this_completion_cb([job](simgrid::s4u::Mess const& me)
          {job->resource_waiting_queue_time = sg4::Engine::get_clock(); dispatcher->onJobTransferEnd(job, me);});
        pending_activities.push(transfer);
        it = pending_jobs.erase(it);
      }
      else ++it;
    }
    while (true) {if(pending_activities.wait_any()->get_name().find("Exec") != std::string::npos) break;}
  }

  while (!pending_activities.empty()){pending_activities.wait_any();}

  // All jobs are finished: notify the data management policy so it can stop any timers
  // and avoid rescheduling further operations that would keep the simulation alive.
  if (CGSim::DataManagementPolicy::isEnabled()) {
      CGSim::DataManagementPolicy::onSimulationEnd();
  }

  // Mark simulation as done so receivers can exit if their queues are drained.
  simulation_done.store(true);

  // Send a shutdown sentinel job message to each receiver so CPU actors can exit cleanly.
  for (const auto& host : sg4::Engine::get_instance()->get_all_hosts()) {
    if (host->get_name() == "JOB-SERVER_cpu-0") continue;
    if (host->get_name().find("_communication") != std::string::npos) continue;
    sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(host->get_name() + "-MQ");
    // Allocate a sentinel Job with jobid == -1 to signal shutdown.
    Job* shutdown_job = new Job();
    shutdown_job->jobid = -1;
    mqueue->put(shutdown_job, 0);
  }
}

void JOB_EXECUTOR::execute_job(Job* j)
{
  auto exec_activity = Actions::exec_task_multi_thread_async(j,dispatcher);
  std::vector<sg4::IoPtr>   read_activities;
  std::vector<sg4::CommPtr> comm_activities;
  std::vector<sg4::IoPtr>   write_activities;

  for (const auto& [filename,fileinfo] : j->input_files) {
    auto size = fileinfo.first;
    // Take the first location where the file is located as a default
    auto filelocation = *(fileinfo.second.begin());

    // Build reactive context for data management policy
    CGSim::FileRequestContext ctx;
    ctx.job = j;
    ctx.filename = filename;
    for (const auto& site : CGSim::FileManager::get_site_names()) {
      if (CGSim::FileManager::exists(filename, site)) {
        CGSim::ReplicaInfo r;
        r.sitename = site;
        r.hostname = site + "_communication";
        r.size = CGSim::FileManager::request_file_size(filename);
        ctx.replicas.push_back(std::move(r));
      }
    }

    // Ask the data management policy (if enabled) how to serve this request
    std::string src_site = filelocation;
    FileTransferMode mode = FileTransferMode::COPY;
    if (CGSim::DataManagementPolicy::isEnabled()) {
      auto decision = CGSim::DataManagementPolicy::onFileRequest(ctx);
      if (!decision.chosen_site.empty()) {
        src_site = decision.chosen_site;
      }
      if (decision.mode == CGSim::FileTransferDecisionMode::MOVE) {
        mode = FileTransferMode::MOVE;
      }
    }

    if (src_site != j->comp_site) {
      auto comm_activity = Actions::comm_file_async(j,filename,src_site,j->comp_site,size,dispatcher, mode);
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
  sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(MQ_name);

  while (true) {
    sg4::MessPtr mess = mqueue->get_async();
    mess->wait();
    auto* job = static_cast<Job*>(mess->get_payload());

    // Sentinel jobid -1 means "shutdown" for this receiver
    if (job == nullptr || job->jobid == -1) {
      break;
    }

    execute_job(job);

    // If the global simulation is done, and we just handled the last job
    // that was in this queue, we can safely exit instead of waiting forever
    // for new messages that will never arrive.
    if (simulation_done.load()) {
      // We rely on the contract that once simulation_done is true,
      // no new real jobs will be enqueued for this receiver.
      break;
    }
  }
}

void JOB_EXECUTOR::start_receivers()
{
  for (const auto& host : sg4::Engine::get_instance()->get_all_hosts()) {
    if (host->get_name() == "JOB-SERVER_cpu-0") continue;
    if ((host->get_name()).find("_communication") != std::string::npos) continue;
    sg4::Actor::create(host->get_name() + "-actor", host, receiver, host->get_name() + "-MQ");
  }
}

void JOB_EXECUTOR::attach_callbacks()
{
  sg4::Engine::on_simulation_start_cb([](){
      dispatcher->onSimulationStart();
      
      // Notify the data management policy plugin
      if (CGSim::DataManagementPolicy::isEnabled()) {
          CGSim::DataManagementPolicy::onSimulationStart();
      }
  });
  
  sg4::Engine::on_simulation_end_cb([]() {
      dispatcher->onSimulationEnd();
  });
}

