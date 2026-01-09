#include "job_executor.h"

std::unique_ptr<DispatcherPlugin>   JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                    JOB_EXECUTOR::pending_activities;
sg4::ActivitySet                    JOB_EXECUTOR::exec_activities;


void JOB_EXECUTOR::set_dispatcher(std::unique_ptr<DispatcherPlugin>& d)
{
 dispatcher = std::move(d);
}

void JOB_EXECUTOR::set_output(const std::string& outputFile)
{
  //LOG_INFO("Output path set to: {}", outputFile);
  //saver->setFilePath(outputFile);
  //saver->createJobsTable();
}

void JOB_EXECUTOR::start_job_execution(long num_of_jobs_to_run)
{
  attach_callbacks();
  //LOG_INFO("Callbacks attached. Starting job execution...");
  const auto* e = sg4::Engine::get_instance();
  sg4::Host* job_server = nullptr;
  for (auto& h : e->get_all_hosts()) {
    if (h->get_name() == "JOB-SERVER_cpu-0") {
      job_server = h;
      break;
    }
  }

  if (!job_server) {
    //LOG_CRITICAL("Job Server not initialized properly.");
    exit(-1);
  }

  JobQueue jobs = dispatcher->getWorkload(num_of_jobs_to_run);
  sg4::Actor::create("JOB-EXECUTOR-actor", job_server, JOB_EXECUTOR::start_server,jobs);
   
  e->run();
}

void JOB_EXECUTOR::start_server(JobQueue jobs)
{
    const auto* e = sg4::Engine::get_instance();
    //sg4::ActivitySet job_activities;

    //LOG_INFO("Server started. Initial jobs count: {}", jobs.size());


    // Transfer all jobs from the queue into a vector for central polling.
    std::vector<Job*> pending_jobs;
    while (!jobs.empty()) {
        Job* job = jobs.top();
        jobs.pop();
        //saver->saveJob(job);
        // LOG_INFO("Job saved to DB: {}", job->id);

        // Attempt a one‑time assignment.
        Job* result = dispatcher->assignJob(job);
        //saver->updateJob(result);
        if (result->status == "assigned") {
            // If assigned immediately, dispatch it.
            auto fs = simgrid::fsmod::FileSystem::get_file_systems_by_netzone(
                          e->netzone_by_name_or_null(job->comp_site))
                          .at(job->comp_site + job->comp_host + job->disk + "filesystem");
            update_disk_content(fs, job->input_files, job);
            sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
            pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + job->id + "_on_" + job->comp_host));
            //LOG_DEBUG("Job {} dispatched immediately to host {}", job->id, job->comp_host);
        } else {
            // Set status and add to pending list.
            if(result->status != "failed"){job->status = "pending"; pending_jobs.push_back(job);}
        }
    }

    //std::cout << "Simulator time Before retrying pending jobs: " << sg4::Engine::get_clock() << std::endl;
    // Use a retry counter map for each pending job.
    std::unordered_map<Job*, int> retry_counts;
    for (Job* job : pending_jobs) {
        retry_counts[job] = 0;
    }

    // Poll the pending jobs list until none remain.
    while (!pending_jobs.empty()) {
        for (auto it = pending_jobs.begin(); it != pending_jobs.end(); ) {
            Job* job = *it;
            dispatcher->assignJob(job);
            //saver->updateJob(job);
            retry_counts[job]++;
            if (job->status == "assigned") {
                auto fs = simgrid::fsmod::FileSystem::get_file_systems_by_netzone(
                              e->netzone_by_name_or_null(job->comp_site))
                              .at(job->comp_site + job->comp_host + job->disk + "filesystem");
                update_disk_content(fs, job->input_files, job);
                sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
                pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + job->id + "_on_" + job->comp_host));
                //LOG_DEBUG("Job {} dispatched after {} retries to host {}", job->id, retry_counts[job], job->comp_host);
                it = pending_jobs.erase(it);
            }

            else {
                ++it;
            }
        }
        if (!exec_activities.empty()) {
          auto activityPtr = exec_activities.wait_any();
          //LOG_INFO("Updated Pending Activities count: {}", JOB_EXECUTOR::pending_activities.size());
          //LOG_INFO("Activity completed: {}", activityPtr->get_name());
        }
        //LOG_INFO("Pending jobs count: {}", pending_jobs.size());
    }


  while (!exec_activities.empty())
  {
    exec_activities.wait_any();
    //std::cout << pending_activities.size() << std::endl;
    //std::cout << pending_activities.wait_any()->get_name() << std::endl;
    //std::cout << pending_activities.size() << std::endl;
  }

    /*while (!pending_activities.empty())
      {
      pending_activities.wait_any();
      //std::cout << pending_activities.size() << std::endl;
      //std::cout << pending_activities.wait_any()->get_name() << std::endl;
      //std::cout << pending_activities.size() << std::endl;
      }*/

  //exec_activities.wait_all();
  pending_activities.wait_all();
  //LOG_DEBUG("Finished All Pending Activities");

}

void JOB_EXECUTOR::execute_job(Job* j)
{
  //LOG_DEBUG("Executing job: {}", j->id);
  j->status = "running";
  //saver->updateJob(j);

  const auto* e = sg4::Engine::get_instance();
  auto fs = simgrid::fsmod::FileSystem::get_file_systems_by_netzone(
    e->netzone_by_name_or_null(j->comp_site)).at(j->comp_site + j->comp_host + j->disk + "filesystem");
  // sg4::this_actor::get_host()->extension<HostExtensions>()->registerJob(j);  
  Actions::read_file_async(fs, j, pending_activities, dispatcher);
  Actions::exec_task_multi_thread_async(j, pending_activities, exec_activities, dispatcher);
  Actions::write_file_async(fs, j, pending_activities, dispatcher);
  //LOG_DEBUG("Activities added for job: {}", j->job_name);

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
    //LOG_DEBUG("Received job on queue {}: {}", MQ_name, job->id);
    execute_job(job);
    }
}

void JOB_EXECUTOR::start_receivers()
{
  auto         start  = std::chrono::high_resolution_clock::now();
  const auto*  eng    = sg4::Engine::get_instance();
  auto         hosts  = eng->get_all_hosts();

  auto host_fetch_time = std::chrono::high_resolution_clock::now();
  //LOG_INFO("Time to fetch hosts: {} ms",
       //    std::chrono::duration_cast<std::chrono::milliseconds>(host_fetch_time - start).count());

  for (const auto& host : hosts) {
    if (host->get_name() == "JOB-SERVER_cpu-0") continue;
    sg4::Actor::create(host->get_name() + "-actor", host, receiver, host->get_name() + "-MQ");
  }

  auto end = std::chrono::high_resolution_clock::now();
  //LOG_INFO("Finished creating receivers in: {} ms",
        //   std::chrono::duration_cast<std::chrono::milliseconds>(end - host_fetch_time).count());
  //LOG_INFO("Total receiver setup time: {} ms",
         //  std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
}

void JOB_EXECUTOR::update_disk_content(const std::shared_ptr<simgrid::fsmod::FileSystem>& fs, const std::unordered_map<std::string, size_t>& input_files, Job* j)
{
  const auto* e = sg4::Engine::get_instance();
  for (const auto& d : e->host_by_name(j->comp_host)->get_disks()) {
    if (std::string(d->get_cname()) == j->disk) {
      j->mount = d->get_property("mount");
      break;
    }
  }
  if (j->mount.empty()) {
    throw std::runtime_error("Read disk mount point not found for job: " + j->id);
  }

  for (const auto& inputfile : input_files) {
    fs->create_file(j->mount + inputfile.first, std::to_string(inputfile.second) + "kB");
  }
}

/*void JOB_EXECUTOR::saveJobs()
{
  while (!jobs.empty()) {
    Job* j = jobs.top();
    //saver->updateJob(j);
    jobs.pop();
    delete j;
  }
}*/

void JOB_EXECUTOR::attach_callbacks()
{
  sg4::Engine::on_simulation_start_cb([]() {
    //LOG_INFO("Simulation starting...");
  });

  sg4::Engine::on_simulation_end_cb([]() {
    //LOG_INFO("Simulation finished, SIMULATED TIME: {}", sg4::Engine::get_clock());
    dispatcher->onSimulationEnd();
    //saver->exportJobsToCSV();
  });
}
