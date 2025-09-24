#include "job_executor.h"
#include "logger.h"  
#include <chrono>
#include <ctime>
#include <regex>
#include <iomanip>

// Recursive function to print a NetZone and its children.
void printNetZone(const simgrid::s4u::NetZone* zone, int indent = 0) {
  if (!zone) return;

  std::string indentStr(indent, ' ');
  LOG_DEBUG("{}Zone Name: {}", indentStr, zone->get_name());

  const std::vector<simgrid::s4u::NetZone*>& children = zone->get_children();
  if (!children.empty()) {
    LOG_DEBUG("{}Children:", indentStr);
    for (const auto child : children) {
      printNetZone(child, indent + 2);
    }
  } else {
    LOG_DEBUG("{}No children.", indentStr);
  }
}

std::unique_ptr<DispatcherPlugin> JOB_EXECUTOR::dispatcher;
std::unique_ptr<sqliteSaver> JOB_EXECUTOR::saver = std::make_unique<sqliteSaver>();
// std::vector<Job*> JOB_EXECUTOR::pending_jobs;
sg4::ActivitySet JOB_EXECUTOR::pending_activities;
sg4::ActivitySet JOB_EXECUTOR::exec_activities;
std::unordered_map<std::string, JobSiteStats> JOB_EXECUTOR::site_statistics;
std::string JOB_EXECUTOR::fixed_creation_time; // Added static member for fixed creation time


void JOB_EXECUTOR::set_dispatcher(const std::string& dispatcherPath, sg4::NetZone* platform)
{
  PluginLoader<DispatcherPlugin> plugin_loader;
  dispatcher = plugin_loader.load(dispatcherPath);
  platform = platform;
  for (const auto& site : platform->get_children()) {
      site_statistics[site->get_name()] = JobSiteStats();
  }
  dispatcher->getResourceInformation(platform);
}

void JOB_EXECUTOR::set_output(const std::string& outputFile)
{
  LOG_INFO("Output path set to: {}", outputFile);
  saver->setFilePath(outputFile);
  saver->createJobsTable();
  saver->createStateTable();
}

void JOB_EXECUTOR::set_fixed_creation_time(const std::string& creation_time)
{
  fixed_creation_time = creation_time;
  LOG_INFO("Fixed creation time set to: {}", fixed_creation_time);
}

std::string& JOB_EXECUTOR::get_fixed_creation_time()
{
  return fixed_creation_time;
}

void JOB_EXECUTOR::start_job_execution(JobQueue jobs)
{
  // Set the fixed creation time before starting job execution
  if (fixed_creation_time.empty()) {
    fixed_creation_time = "1/22/2024 12:47"; // Default creation time
    LOG_INFO("Using default fixed creation time: {}", fixed_creation_time);
  }
  
  attach_callbacks();
  LOG_INFO("Callbacks attached. Starting job execution...");
  const auto* e = sg4::Engine::get_instance();
  sg4::Host* job_server = nullptr;
  for (auto& h : e->get_all_hosts()) {
    if (h->get_name() == "JOB-SERVER_cpu-0") {
      job_server = h;
      break;
    }
  }

  if (!job_server) {
    LOG_CRITICAL("Job Server not initialized properly.");
    exit(-1);
  }

  sg4::Actor::create("JOB-EXECUTOR-actor", job_server, this->start_server, jobs);
   
  e->run();
}

void JOB_EXECUTOR::start_server(JobQueue jobs)
{
    const auto* e = sg4::Engine::get_instance();
    //sg4::ActivitySet job_activities;

    LOG_INFO("Server started. Initial jobs count: {}", jobs.size());

    // Transfer all jobs from the queue into a vector for central polling.
    std::vector<Job*> pending_jobs;
    while (!jobs.empty()) {
        Job* job = jobs.top();
        jobs.pop();
        saver->saveJob(job);
        
        // LOG_INFO("Job saved to DB: {}", job->id);

        // Attempt a one‑time assignment.
        Job* result = dispatcher->assignJob(job);
        job->available_site_cores = result->available_site_cores;
        job->available_site_cpus = result->available_site_cpus;
        saver->updateJob(result);

        if (result->status == "assigned") {
            job->site_cores = result->site_cores;
            job->site_cpus = result->site_cpus;
            if (!result->comp_site.empty()) {
                site_statistics[result->comp_site].assigned++;
            }
            // saving the state of the job using fixed creation time
            job->lastUpdatedTimeStamp = get_job_time_stamp(fixed_creation_time, sg4::Engine::get_clock());
            saver->saveState(job,site_statistics[result->comp_site]);
            // If assigned immediately, dispatch it.
            auto fs = simgrid::fsmod::FileSystem::get_file_systems_by_netzone(
                          e->netzone_by_name_or_null(job->comp_site))
                          .at(job->comp_site + job->comp_host + job->disk + "filesystem");
            update_disk_content(fs, job->input_files, job);
            sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
            pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + job->id + "_on_" + job->comp_host));
            LOG_DEBUG("Job {} dispatched immediately to host {}", job->id, job->comp_host);
        } else {
            // Set status and add to pending list.
            if (result->status == "failed") {
                // -- Increment failed count --
                if (!result->comp_site.empty()) { // Assuming submission_site is where it failed
                    site_statistics[result->comp_site].failed++;
                }
                saver->saveState(job,site_statistics[result->comp_site]);
            } else {
                job->status = "pending";
                // -- Increment pending count --
                if (!job->comp_site.empty()) {
                     site_statistics[job->comp_site].pending++;
                }
                // Use fixed creation time instead of job->creation_time
                job->lastUpdatedTimeStamp = get_job_time_stamp(fixed_creation_time, sg4::Engine::get_clock());
                saver->saveState(job,site_statistics[result->comp_site]);
                pending_jobs.push_back(job);
            }
        }
    }

    //std::cout << "Simulator time Before retrying pending jobs: " << sg4::Engine::get_clock() << std::endl;
    // Use a retry counter map for each pending job.
    std::unordered_map<Job*, int> retry_counts;
    for (Job* job : pending_jobs) {
        retry_counts[job] = 0;
    }
     // create a map of sitename and current free cores and initalize all of them to zeros
    std::unordered_map<std::string, int> site_free_cores;
    // Poll the pending jobs list until none remain.
    while (!pending_jobs.empty()) {

        for (auto it = pending_jobs.begin(); it != pending_jobs.end(); ) {
            if (site_free_cores[(*it)->comp_site] < (*it)->cores) {
                ++it;
                continue; // Skip this job if not enough free cores at the site
            } 

            Job* job = *it;
            Job* result = dispatcher->assignJob(job);
            job->available_site_cores = result->available_site_cores;
            job->available_site_cpus = result->available_site_cpus;
            saver->updateJob(job);
            retry_counts[job]++; 
           
            if (job->status == "assigned") { 
                 
                site_statistics[job->comp_site].assigned++;
                site_statistics[job->comp_site].pending--;
                // Update the free cores for that site in the site_free_cores map
                site_free_cores[job->comp_site] -= job->cores;
                // If assigned, dispatch it using fixed creation time
                job->lastUpdatedTimeStamp = get_job_time_stamp(fixed_creation_time, sg4::Engine::get_clock());
                saver->saveState(job,site_statistics[job->comp_site]);
                auto fs = simgrid::fsmod::FileSystem::get_file_systems_by_netzone(
                              e->netzone_by_name_or_null(job->comp_site))
                              .at(job->comp_site + job->comp_host + job->disk + "filesystem");
                update_disk_content(fs, job->input_files, job);
                sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
                pending_activities.push(mqueue->put_async(job)->set_name("Comm_Job_" + job->id + "_on_" + job->comp_host));
                // LOG_CRITICAL("Job {} dispatched after {} retries to host {}", job->id, retry_counts[job], job->comp_host);
                it = pending_jobs.erase(it);
            }

            else {
                ++it;
            }
        }
        if (!exec_activities.empty()) {
          auto activityPtr = exec_activities.wait_any();
          std::cout << "Activity completed: " << activityPtr->get_name() << std::endl;

          // Extracting cores released by this activity, using the cores used information embedded in the activity name as a workaround
          std::string activityName = activityPtr->get_name();
          int cores_free = JOB_EXECUTOR::coreReleased(activityName);

          // LOG_CRITICAL("Updated Pending Activities count: {}", JOB_EXECUTOR::exec_activities.size());
          // LOG_CRITICAL("Activity completed: {}", activityPtr->get_name());
          // std::cout << "Activity completed: " << activityPtr->get_name() << std::endl;
          while (auto concurrentActivityPtr = exec_activities.test_any()) {
            std::string concurrentActivityName = activityPtr->get_name();
            cores_free += JOB_EXECUTOR::coreReleased(concurrentActivityName);
            // no time advancement, just removes finished activities
            // wait_any() returns only one finished activity even if multiple are finished at the time stamp
            // test_any() returns one finished activity at the time stamp but does not advance time
            // std::cout << "Activity completed: " << concurrentActivityPtr->get_name() << std::endl;
          }
          // update the free cores for that site info into the site_free_cores map
          // need to know the site name from the activity name, here is format of activity name: Exec_Job_<job_name>_on_<host_name>_at_site_<site_name>_UsingCores_<num_of_cores>
          std::regex site_regex(R"_(_at_site_(\w+)_UsingCores_)_");
          std::smatch match;
          if (std::regex_search(activityName, match, site_regex)) {
              std::string site_name = match[1];
              // std::cout << "Site name extracted from activity: " << site_name << std::endl;
              site_free_cores[site_name] += cores_free;
              // std::cout << "Total free cores for site " << site_name << ": " << site_free_cores[site_name] << std::endl;
          } else {
              std::cout << "Failed to extract site name from activity: " << activityName << std::endl;
          }
        }         
        std::cout << "Pending Jobs Count " << pending_jobs.size() << std::endl;
        LOG_CRITICAL("Pending jobs count: {}", pending_jobs.size());
    }

    while (!exec_activities.empty())
      {
      std::cout << "Execution Activity Size"<< exec_activities.size() << std::endl;
      exec_activities.wait_any();
      // std::cout << exec_activities.wait_any()->get_name() << std::endl;
      // std::cout << exec_activities.size() << std::endl;
      }
    // while (!pending_activities.empty())
    //   {
    //   std::cout << "Pending Activity Size" << pending_activities.size() << std::endl;
    //   pending_activities.wait_all();
    //   // std::cout << pending_activities.wait_any()->get_name() << std::endl;
    //   // std::cout << pending_activities.size() << std::endl;
    //   }

  pending_activities.wait_all();
  LOG_DEBUG("Finished All Pending Activities");
}

int JOB_EXECUTOR::coreReleased(std::string &activityName)
{
  std::regex cores_regex(R"(_UsingCores_(\d+))");
  std::smatch match;
  int cores_released = 0;
  if (std::regex_search(activityName, match, cores_regex))
  {
    cores_released = std::stoi(match[1]);
  }
  std::cout << "Cores released by activity: " << cores_released << std::endl;
  return cores_released;
}



void JOB_EXECUTOR::execute_job(Job* j)
{
  LOG_DEBUG("Executing job: {}", j->id);
  j->status = "running";
  // Use fixed creation time instead of j->creation_time
  j->lastUpdatedTimeStamp = get_job_time_stamp(fixed_creation_time, sg4::Engine::get_clock());
  saver->updateJob(j);
  saver->saveState(j,site_statistics[j->comp_site]);

  const auto* e = sg4::Engine::get_instance();
  auto fs = simgrid::fsmod::FileSystem::get_file_systems_by_netzone(
    e->netzone_by_name_or_null(j->comp_site)).at(j->comp_site + j->comp_host + j->disk + "filesystem");
  // sg4::this_actor::get_host()->extension<HostExtensions>()->registerJob(j);  
  Actions::read_file_async(fs, j, pending_activities, dispatcher);
  Actions::exec_task_multi_thread_async(j, pending_activities, exec_activities, saver, dispatcher);
  Actions::write_file_async(fs, j, pending_activities, dispatcher);
  LOG_DEBUG("Activities added for job: {}", j->job_name);

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
    LOG_DEBUG("Received job on queue {}: {}", MQ_name, job->id);
    execute_job(job);
    }
}

void JOB_EXECUTOR::start_receivers()
{
  auto         start  = std::chrono::high_resolution_clock::now();
  const auto*  eng    = sg4::Engine::get_instance();
  auto         hosts  = eng->get_all_hosts();

  auto host_fetch_time = std::chrono::high_resolution_clock::now();
  LOG_INFO("Time to fetch hosts: {} ms",
           std::chrono::duration_cast<std::chrono::milliseconds>(host_fetch_time - start).count());

  for (const auto& host : hosts) {
    if (host->get_name() == "JOB-SERVER_cpu-0") continue;
    sg4::Actor::create(host->get_name() + "-actor", host, receiver, host->get_name() + "-MQ");
  }

  auto end = std::chrono::high_resolution_clock::now();
  LOG_INFO("Finished creating receivers in: {} ms",
           std::chrono::duration_cast<std::chrono::milliseconds>(end - host_fetch_time).count());
  LOG_INFO("Total receiver setup time: {} ms",
           std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
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

void JOB_EXECUTOR::on_job_finished(Job* j)
{
    if (j == nullptr || j->comp_site.empty()) {
        return; 
    }

    site_statistics[j->comp_site].assigned--;
    site_statistics[j->comp_site].finished++;
    saver->saveState(j, site_statistics[j->comp_site]);
    LOG_DEBUG("Site '{}' stats updated for finished job {}: assigned(-1), finished(+1)", j->comp_site, j->id);
}

std::string JOB_EXECUTOR::get_job_time_stamp(std::string jobCreationTime, double simgrid_clock)
{
    // Convert job creation time to a timestamp
    std::regex timestamp_regex1(R"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})");  // Original format
    std::regex timestamp_regex2(R"(\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2})");   // New format
    std::smatch match;
    
    std::time_t base_time;
    
    if (std::regex_search(jobCreationTime, match, timestamp_regex1)) {
        // Handle original format: YYYY-MM-DD HH:MM:SS
        std::string timestamp_str = match.str();
        
        std::tm tm = {};
        std::istringstream ss(timestamp_str);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        base_time = std::mktime(&tm);
        
    } else if (std::regex_search(jobCreationTime, match, timestamp_regex2)) {
        // Handle new format: MM/DD/YYYY H:MM
        std::string timestamp_str = match.str();
        
        std::tm tm = {};
        std::istringstream ss(timestamp_str);
        ss >> std::get_time(&tm, "%m/%d/%Y %H:%M");
        base_time = std::mktime(&tm);
        
    } else {
        LOG_ERROR("Job creation time format is incorrect: {}", jobCreationTime);
        return "";
    }
    
    // Calculate job timestamp
    std::time_t job_timestamp = base_time + static_cast<std::time_t>(simgrid_clock);
    
    // Format timestamp
    auto format_time = [](std::time_t t) {
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
        return oss.str();
    };
    
    return format_time(job_timestamp);
}

double JOB_EXECUTOR::get_job_queue_time(std::string jobCreationTime, std::string jobStartTime)
{
    double queue_time = 0.0;

    // If either job creation time or job start time is empty, return 0.0
    if (jobCreationTime.empty() || jobStartTime.empty()) {
        LOG_ERROR("Job creation time or start time is empty: {} or {}", jobCreationTime, jobStartTime);
        return queue_time;
    }

    // Helper function to parse timestamp from any supported format
    auto parse_timestamp = [](const std::string& timeStr) -> std::time_t {
        std::regex timestamp_regex1(R"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})");  // Original format: YYYY-MM-DD HH:MM:SS
        std::regex timestamp_regex2(R"(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)");   // New format with AM/PM: M/D/YYYY  H:MM:SS AM/PM
        std::regex timestamp_regex3(R"(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})");   // Alternative format: M/D/YYYY H:MM
        std::smatch match;
        
        if (std::regex_search(timeStr, match, timestamp_regex1)) {
            // Handle original format: YYYY-MM-DD HH:MM:SS
            std::string timestamp_str = match.str();
            std::tm tm = {};
            std::istringstream ss(timestamp_str);
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
            return std::mktime(&tm);
            
        } else if (std::regex_search(timeStr, match, timestamp_regex2)) {
            // Handle new format with AM/PM: M/D/YYYY  H:MM:SS AM/PM
            std::string timestamp_str = match.str();
            std::tm tm = {};
            std::istringstream ss(timestamp_str);
            ss >> std::get_time(&tm, "%m/%d/%Y %I:%M:%S %p");  // %I for 12-hour format, %p for AM/PM
            return std::mktime(&tm);
            
        } else if (std::regex_search(timeStr, match, timestamp_regex3)) {
            // Handle alternative format: M/D/YYYY H:MM
            std::string timestamp_str = match.str();
            std::tm tm = {};
            std::istringstream ss(timestamp_str);
            ss >> std::get_time(&tm, "%m/%d/%Y %H:%M");
            return std::mktime(&tm);
            
        } else {
            return -1; // Invalid format
        }
    };

    // Parse both timestamps
    std::time_t creation_time = parse_timestamp(jobCreationTime);
    std::time_t start_time = parse_timestamp(jobStartTime);
    
    // Check if both timestamps were parsed successfully
    if (creation_time == -1 || start_time == -1) {
        LOG_ERROR("Job creation or start time format is incorrect: {} or {}", jobCreationTime, jobStartTime);
        return 0.0;
    }
    
    // Calculate queue time (difference in seconds)
    queue_time = static_cast<double>(start_time - creation_time);
    
    return queue_time;
}

void JOB_EXECUTOR::saveJobs(JobQueue jobs)
{
  while (!jobs.empty()) {
    Job* j = jobs.top();
    saver->updateJob(j);
    jobs.pop();
    delete j;
  }
}

void JOB_EXECUTOR::attach_callbacks()
{
  sg4::Engine::on_simulation_start_cb([]() {
    LOG_INFO("Simulation starting...");
  });

  sg4::Engine::on_simulation_end_cb([]() {
    LOG_INFO("Simulation finished, SIMULATED TIME: {}", sg4::Engine::get_clock());
    dispatcher->onSimulationEnd();
    saver->exportDBTableToCSV("JOBS");
    saver->exportDBTableToCSV("STATE");
  });
}