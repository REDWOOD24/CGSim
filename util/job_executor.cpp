#include "job_executor.h"

#include "file_manager.h"
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {

std::unordered_set<std::string> live_replica_sites(const std::string& filename)
{
  auto* fm = CGSim::get_file_manager();
  if (!fm->exists(filename)) {
    return {};
  }
  return fm->request_file_sites(filename);
}

void write_overwrite_lines(int line_count, bool& active, const std::vector<std::string>& lines)
{
  if (active) {
    std::cout << "\033[" << line_count << "A";
  }

  for (const auto& line : lines) {
    std::cout << "\r" << line << "\033[K\n";
  }
  std::cout << std::flush;
  active = true;
}

void finish_overwrite_lines(bool& active)
{
  if (active) {
    std::cout << std::endl;
    active = false;
  }
}

class DispatchStatusDisplay {
public:
  static constexpr int kLineCount = 8;

  void record_submission(long jobid, double submission_time, double creation_time)
  {
    std::ostringstream line;
    line << "Last submission: job " << jobid
         << " at " << std::fixed << std::setprecision(1) << submission_time
         << " (created " << creation_time << ")";
    last_submission_ = line.str();
  }

  void record_assignment(const Job* job)
  {
    std::ostringstream line;
    line << "Last assignment: job " << job->jobid
         << ", cores " << job->cores
         << ", status " << job->status
         << " after " << job->retries << " tries";
    last_assignment_ = line.str();
  }

  void refresh(unsigned long dispatched_jobs,
               unsigned long total_jobs,
               std::size_t pending_job_count,
               std::size_t staged_job_count,
               unsigned long activated_jobs,
               unsigned long finished_jobs,
               std::size_t pending_activity_count,
               double sim_time,
               double core_usage)
  {
    std::ostringstream dispatched;
    dispatched << "Dispatched: " << dispatched_jobs << " / " << total_jobs;

    std::ostringstream progress;
    progress << "Finished: " << finished_jobs << " / " << total_jobs
             << "  |  Activated: " << activated_jobs << " / " << total_jobs
             << "  |  Staged: " << staged_job_count;

    std::ostringstream pending_jobs;
    pending_jobs << "Pending jobs: " << pending_job_count;

    std::ostringstream pending_activities;
    pending_activities << "Pending activities: " << pending_activity_count;

    std::ostringstream sim_time_line;
    sim_time_line << std::fixed << std::setprecision(1)
                  << "Sim time: " << sim_time;

    std::ostringstream cores;
    cores << std::fixed << std::setprecision(2)
          << "Core usage: " << (core_usage * 100.0) << "%";

    write_overwrite_lines(
        kLineCount,
        active_,
        {
            dispatched.str(),
            progress.str(),
            pending_jobs.str(),
            pending_activities.str(),
            sim_time_line.str(),
            cores.str(),
            last_submission_.empty() ? "Last submission: (none)" : last_submission_,
            last_assignment_.empty() ? "Last assignment: (none)" : last_assignment_,
        });
  }

  void finish() { finish_overwrite_lines(active_); }

private:
  bool active_ = false;
  std::string last_submission_;
  std::string last_assignment_;
};

DispatchStatusDisplay g_dispatch_status;

} // namespace

void JOB_EXECUTOR::track_pending_activity(const sg4::ActivityPtr& activity)
{
  // No dedup: every owned activity is a fresh object, and a raw-pointer dedup
  // set can dangle after allocator address reuse, silently skipping the push.
  // A skipped push leaves a started activity unreferenced by the ActivitySet;
  // it is destroyed when the job graph goes out of scope and its FileRead
  // never finishes (root cause in execute_job_implementation.md section 3.14).
  if (!activity) {
    return;
  }
  pending_activities.push(activity);
}

void JOB_EXECUTOR::drain_completed_pending_activities()
{
  while (pending_activities.test_any() != nullptr) {
    // test_any() already removed the completed activity from the set.
  }
}

void JOB_EXECUTOR::wait_for_pending_progress()
{
  drain_completed_pending_activities();
  if (pending_activities.empty()) {
    return;
  }

  pending_activities.wait_any();
  drain_completed_pending_activities();
}

std::shared_ptr<DispatcherPlugin>    JOB_EXECUTOR::dispatcher;
sg4::ActivitySet                     JOB_EXECUTOR::pending_activities;
std::unordered_map<long long, Job*>  JOB_EXECUTOR::all_jobs;
std::vector<Job*>                    JOB_EXECUTOR::pending_jobs;
std::vector<Job*>                    JOB_EXECUTOR::staging_jobs;
JobQueue                             JOB_EXECUTOR::jobs;
unsigned long                        JOB_EXECUTOR::MAX_RETRIES = 20;
unsigned long                        JOB_EXECUTOR::USED_CORES = 0;
unsigned long                        JOB_EXECUTOR::TOTAL_CORES;
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
  TOTAL_CORES = std::stoul((sg4::Engine::get_instance()->get_netzone_root())->get_property("grid_cores"));
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
      g_dispatch_status.record_submission(job->jobid, job->submission_time, job->creation_time);
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

      if (ACTIVATED_JOBS < DISPATCHED_JOBS)
        {
          sg4::this_actor::yield();
          continue;
        }

      sg4::this_actor::sleep_until(time);
      return;
    }

    try 
    {
      // wait_any_for removes the completed activity from the set itself;
      // drain the other same-timestamp completions.
      pending_activities.wait_any_for(time - sg4::Engine::get_clock());
      drain_completed_pending_activities();
    }
    catch (const simgrid::TimeoutException&) {return;}
  }  
}


void JOB_EXECUTOR::start_server()
{
  const auto refresh_dispatch_status = []() {
    const double core_usage =
        TOTAL_CORES > 0 ? (1.0 * USED_CORES) / (1.0 * TOTAL_CORES) : 0.0;
    g_dispatch_status.refresh(
        DISPATCHED_JOBS,
        TOTAL_JOBS,
        pending_jobs.size(),
        staging_jobs.size(),
        ACTIVATED_JOBS,
        FINISHED_JOBS,
        pending_activities.size(),
        sg4::Engine::get_clock(),
        core_usage);
  };

  while (DISPATCHED_JOBS != TOTAL_JOBS || !staging_jobs.empty())
  {
    try_release_staged_jobs();

    if(pending_jobs.size() + DISPATCHED_JOBS + staging_jobs.size() != TOTAL_JOBS)
    {
    if(sg4::Engine::get_clock() < jobs.top()->creation_time) advance_to_time(jobs.top()->creation_time);
    get_jobs();
    }
    
    bool nothing_assigned = true;
    bool activities_finished = false;
    for (auto it = pending_jobs.begin(); it != pending_jobs.end();)
    {
      if((1.0*USED_CORES)/(1.0*TOTAL_CORES) > 0.8) break;
      Job* job = *it;
      dispatcher->assignJob(job);
      if(job->comp_host != ""){
        job->status = "assigned";
        nothing_assigned = false;
        CGSim::get_site_manager()->moveSystemPendingtoPendingJob(job->comp_site);
        if (job_needs_transfer_staging(job)) {
          reserve_job_assignment(job);
          job->status = "staged";
          staging_jobs.push_back(job);
        } else {
          reserve_job_assignment(job);
          dispatch_job_to_host(job);
        }
        it = pending_jobs.erase(it);
      }
      else {job->status = "pending"; job->retries++; ++it;}
      //else ++it;
    }

    if(pending_activities.empty() && DISPATCHED_JOBS != TOTAL_JOBS)
    {
      refresh_dispatch_status();
      sg4::this_actor::yield();
      continue;
    }

    while (!pending_activities.empty() && (1.0 * USED_CORES) / (1.0 * TOTAL_CORES) >= 0.6){
      activities_finished = true;
      wait_for_pending_progress();
      try_release_staged_jobs();
    }

    if(!pending_activities.empty() && nothing_assigned && !activities_finished)
    {
      while(!pending_activities.empty())
      {
        drain_completed_pending_activities();
        if (pending_activities.empty()) {
          break;
        }
        auto act = pending_activities.wait_any();
        if (act) {
          drain_completed_pending_activities();
          if (act->get_name().find("Exec") != std::string::npos) {
            break;
          }
        }
      }
      try_release_staged_jobs();
    }

    refresh_dispatch_status();
  }

  try_release_staged_jobs();
  g_dispatch_status.finish();

  // Stop proactive policy callbacks during tail drain.
  CGSim::PolicyManager::RUNNING = false;

  while (ACTIVATED_JOBS != TOTAL_JOBS || !pending_activities.empty())
    {
      if (!pending_activities.empty()) {
        wait_for_pending_progress();
      } else {
        sg4::this_actor::yield();
      }
    }
}

void JOB_EXECUTOR::onJobAssignment(Job* job)
{
  reserve_job_assignment(job);
  dispatch_job_to_host(job);
}

namespace {

class NoGlobalReplicaError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

class SoftRestageSignal : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

} // namespace

bool JOB_EXECUTOR::job_needs_transfer_staging(Job* job)
{
  // Stage until observation is safe for execute_job without foreign-Comm join:
  // I (inbound): hold until catalog shows local (L) — do not join in-flight Comms.
  // B (remote-bound): hold until some resting replica exists again.
  // U is dispatchable when this job can own a new transfer to comp_site.
  for (const auto& [filename, fileinfo] : job->input_files_sizes_locations)
  {
    try {
      const InputAccessPlan plan = plan_one_input(job, filename, fileinfo);
      if (plan.kind == InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ) {
        return true; // I: stage until L
      }
      if (plan.kind == InputAccessKind::REMOTE_BOUND_WAIT) {
        return true; // B: stage until resting replica appears
      }
      if (plan.kind == InputAccessKind::TRANSFER_THEN_READ) {
        auto* fm = CGSim::get_file_manager();
        // Inbound appeared for this destination → treat as I, stage until L.
        if (fm->find_in_flight_to_destination(filename, job->comp_site) ||
            fm->is_in_flight(filename, plan.src_site, job->comp_site)) {
          return true;
        }
      }
    } catch (const SoftRestageSignal&) {
      return true;
    } catch (const NoGlobalReplicaError&) {
      // Illegal under accountability axiom if truly empty; keep staged for retry
      // until catalog/in-flight catches up, rather than dispatching a doomed job.
      return true;
    } catch (const std::runtime_error&) {
      return true;
    }
  }
  return false;
}

void JOB_EXECUTOR::resolve_input_file_source(
    Job* job,
    const std::string& filename,
    const std::pair<long long, std::unordered_set<std::string>>& fileinfo,
    std::string& filelocation,
    CGSim::FileTransferDecisionMode& mode)
{
  // Called only on the U path: at least one resting replica exists somewhere.
  auto* fm = CGSim::get_file_manager();
  const auto live_sites = live_replica_sites(filename);
  if (live_sites.empty()) {
    throw NoGlobalReplicaError(
        "resolve_input_file_source called with empty catalog for file: " + filename);
  }

  dispatcher->onFileRequest(job, filename, fileinfo.first, live_sites, filelocation, mode);
  if (filelocation.empty()) {
    throw NoGlobalReplicaError("File location not specified for file: " + filename);
  }

  if (fm->exists(filename, job->comp_site)) {
    filelocation = job->comp_site;
    return;
  }

  if (filelocation != job->comp_site && fm->exists(filename, filelocation)) {
    return;
  }

  // Stale policy source: pick any live remote resting replica (table: stale source string).
  filelocation.clear();
  for (const auto& site : live_sites) {
    if (site == job->comp_site) {
      continue;
    }
    if (fm->exists(filename, site)) {
      filelocation = site;
      return;
    }
  }

  throw NoGlobalReplicaError(
      "File: " + filename + " has no available resting replica for job "
      + std::to_string(job->jobid));
}

JOB_EXECUTOR::InputAccessPlan JOB_EXECUTOR::plan_one_input(
    Job* job,
    const std::string& filename,
    const std::pair<long long, std::unordered_set<std::string>>& fileinfo)
{
  // Observation order L ≻ I ≻ U ≻ B (execute_job_first_principles.md §3.1 / §3.5).
  auto* fm = CGSim::get_file_manager();
  InputAccessPlan plan;
  plan.filename = filename;
  plan.mode = CGSim::FileTransferDecisionMode::COPY;

  // L: local resting replica
  if (fm->exists(filename, job->comp_site)) {
    plan.kind = InputAccessKind::LOCAL_READ;
    return plan;
  }

  // I: inbound in-flight to compute site (destination uniqueness ⇒ at most one)
  if (auto inbound = fm->find_in_flight_to_destination(filename, job->comp_site)) {
    plan.kind = InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ;
    plan.src_site = inbound->src_site;
    plan.in_flight_comm = inbound->comm;
    return plan;
  }

  // U: remote resting replica exists → policy chooses source and COPY/MOVE
  const auto live_sites = live_replica_sites(filename);
  if (!live_sites.empty()) {
    std::string filelocation;
    resolve_input_file_source(job, filename, fileinfo, filelocation, plan.mode);

    if (fm->exists(filename, job->comp_site) || filelocation == job->comp_site) {
      plan.kind = InputAccessKind::LOCAL_READ;
      return plan;
    }

    // Race: inbound appeared while resolving — prefer I over starting a duplicate.
    if (auto inbound = fm->find_in_flight_to_destination(filename, job->comp_site)) {
      plan.kind = InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ;
      plan.src_site = inbound->src_site;
      plan.in_flight_comm = inbound->comm;
      return plan;
    }

    plan.kind = InputAccessKind::TRANSFER_THEN_READ;
    plan.src_site = filelocation;
    return plan;
  }

  // B: catalog empty, but some in-flight transfer still accounts for the file
  if (fm->find_any_in_flight(filename)) {
    plan.kind = InputAccessKind::REMOTE_BOUND_WAIT;
    return plan;
  }

  // Illegal under accountability axiom (R=∅ and T=∅)
  throw NoGlobalReplicaError(
      "File: " + filename + " has no resting replica and no in-flight transfer (job "
      + std::to_string(job->jobid) + ")");
}

std::vector<JOB_EXECUTOR::InputAccessPlan> JOB_EXECUTOR::plan_all_inputs(Job* job)
{
  std::vector<InputAccessPlan> plans;
  plans.reserve(job->input_files_sizes_locations.size());
  for (const auto& [filename, fileinfo] : job->input_files_sizes_locations) {
    plans.push_back(plan_one_input(job, filename, fileinfo));
  }
  return plans;
}

void JOB_EXECUTOR::revalidate_plan(Job* job, InputAccessPlan& plan)
{
  // Re-run observation order on a previously planned input (live R, T may have moved).
  auto* fm = CGSim::get_file_manager();
  if (fm->exists(plan.filename, job->comp_site)) {
    plan.kind = InputAccessKind::LOCAL_READ;
    plan.src_site.clear();
    plan.in_flight_comm = nullptr;
    return;
  }

  if (auto inbound = fm->find_in_flight_to_destination(plan.filename, job->comp_site)) {
    plan.kind = InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ;
    plan.src_site = inbound->src_site;
    plan.in_flight_comm = inbound->comm;
    return;
  }

  if (plan.kind == InputAccessKind::REMOTE_BOUND_WAIT) {
    // Still no local / inbound. If a resting replica appeared elsewhere → U; else stay B.
    if (!live_replica_sites(plan.filename).empty()) {
      const auto fileinfo = job->input_files_sizes_locations.at(plan.filename);
      plan = plan_one_input(job, plan.filename, fileinfo);
      return;
    }
    if (!fm->find_any_in_flight(plan.filename)) {
      throw NoGlobalReplicaError(
          "remote-bound wait lost all accounts of file " + plan.filename);
    }
    throw SoftRestageSignal(
        "remote-bound wait for file " + plan.filename
        + " job " + std::to_string(job->jobid));
  }

  if (plan.kind == InputAccessKind::TRANSFER_THEN_READ) {
    if (!fm->exists(plan.filename, plan.src_site)) {
      const auto fileinfo = job->input_files_sizes_locations.at(plan.filename);
      plan = plan_one_input(job, plan.filename, fileinfo);
    }
    return;
  }

  if (plan.kind == InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ) {
    if (plan.in_flight_comm == nullptr) {
      if (auto any = fm->find_in_flight_to_destination(plan.filename, job->comp_site)) {
        plan.src_site = any->src_site;
        plan.in_flight_comm = any->comm;
      } else {
        throw SoftRestageSignal(
            "in-flight join unavailable for file " + plan.filename
            + " job " + std::to_string(job->jobid));
      }
    }
  }
}

void JOB_EXECUTOR::build_local_read_branch(
    Job* job, const std::string& filename, JobActivityGraph& graph)
{
  auto read = Actions::read_file_async(job, filename);
  read->add_successor(graph.exec);
  graph.reads.push_back(read);
}

void JOB_EXECUTOR::build_transfer_read_branch(
    Job* job, const InputAccessPlan& plan, JobActivityGraph& graph)
{
  auto* fm = CGSim::get_file_manager();
  // Race: delivery to compute site already in flight — stage until L, never join.
  if (fm->find_in_flight_comm(plan.filename, plan.src_site, job->comp_site) ||
      fm->find_in_flight_to_destination(plan.filename, job->comp_site)) {
    throw SoftRestageSignal(
        "inbound in flight for file " + plan.filename
        + " job " + std::to_string(job->jobid) + "; stage until local");
  }

  auto comm = Actions::transfer_file_async(
      job, plan.filename, plan.src_site, job->comp_site, plan.mode);
  auto read = Actions::read_file_async(job, plan.filename);
  comm->add_successor(read);
  read->add_successor(graph.exec);
  graph.transfers.push_back(comm);
  graph.reads.push_back(read);
}

void JOB_EXECUTOR::build_join_in_flight_branch(
    Job* job, const InputAccessPlan& plan, JobActivityGraph& /*graph*/)
{
  // Inbound join via add_successor on a foreign Comm is disabled (unsafe if Comm
  // already running/finished). I means stage until L instead.
  throw SoftRestageSignal(
      "inbound wait for file " + plan.filename
      + " job " + std::to_string(job->jobid) + "; stage until local (no join)");
}

JOB_EXECUTOR::JobActivityGraph JOB_EXECUTOR::build_job_graph(
    Job* job, std::vector<InputAccessPlan> plans)
{
  JobActivityGraph graph;
  graph.exec = Actions::exec_task_multi_thread_async(job);

  for (auto& plan : plans) {
    revalidate_plan(job, plan);
    switch (plan.kind) {
      case InputAccessKind::LOCAL_READ:
        build_local_read_branch(job, plan.filename, graph);
        break;
      case InputAccessKind::TRANSFER_THEN_READ:
        build_transfer_read_branch(job, plan, graph);
        break;
      case InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ:
        // I: stage until L — do not join foreign in-flight Comm.
        build_join_in_flight_branch(job, plan, graph);
        break;
      case InputAccessKind::REMOTE_BOUND_WAIT:
        // B: do not build a partial graph; soft-restage until a resting replica exists.
        throw SoftRestageSignal(
            "remote-bound wait for file " + plan.filename
            + " job " + std::to_string(job->jobid));
    }
  }

  for (const auto& [filename, size] : job->output_files) {
    auto write = Actions::write_file_async(job, filename, size);
    graph.exec->add_successor(write);
    graph.writes.push_back(write);
  }
  return graph;
}

void JOB_EXECUTOR::register_job_graph(JobActivityGraph& graph)
{
  // Track only activities this job owns. Joined inbound Comms stay owned by their
  // originator (destination uniqueness); we depend on them via add_successor only.
  for (auto& comm : graph.transfers) {
    track_pending_activity(comm);
  }
  for (auto& read : graph.reads) {
    track_pending_activity(read);
  }
  track_pending_activity(graph.exec);
  for (auto& write : graph.writes) {
    track_pending_activity(write);
  }
  graph.registered = true;
}

void JOB_EXECUTOR::start_job_graph(JobActivityGraph& graph)
{
  for (auto& comm : graph.transfers) {
    comm->start();
  }
  // joined Comms are already started by their owners — do not start again
  for (auto& read : graph.reads) {
    read->start();
  }
  graph.exec->start();
  for (auto& write : graph.writes) {
    write->start();
  }
}

void JOB_EXECUTOR::return_job_to_staging(Job* job)
{
  // Soft deferral: job was dispatched to a host but cannot activate yet.
  if (DISPATCHED_JOBS > 0) {
    DISPATCHED_JOBS--;
  }
  job->status = "staged";
  staging_jobs.push_back(job);
}

void JOB_EXECUTOR::fail_job_clean(Job* job)
{
  job->status = "failed";
  if (job->comp_host != "" && job->cores > 0) {
    auto* host = sg4::Host::by_name_or_null(job->comp_host);
    if (host != nullptr && host->extension<HostExtensions>() != nullptr) {
      host->extension<HostExtensions>()->onJobFinish(job);
    }
    if (USED_CORES >= static_cast<unsigned long>(job->cores)) {
      USED_CORES -= job->cores;
    }
  }
  std::cerr << "[job_executor] job " << job->jobid
            << " failed cleanly (no global replica / unrecoverable access plan)\n";
}

void JOB_EXECUTOR::reserve_job_assignment(Job* job)
{
  USED_CORES += job->cores;
  g_dispatch_status.record_assignment(job);
  sg4::Host::by_name(job->comp_host)->extension<HostExtensions>()->registerJob(job);
  dispatcher->onJobAssignment(job);
}

void JOB_EXECUTOR::dispatch_job_to_host(Job* job)
{
  DISPATCHED_JOBS++;
  sg4::MessageQueue* mqueue = sg4::MessageQueue::by_name(job->comp_host + "-MQ");
  sg4::MessPtr job_transfer = mqueue->put_async(job)->set_name(
      "Transfer_Job_" + std::to_string(job->jobid) + "_to_" + job->comp_host + "_from_JOB-SERVER");
  job_transfer->on_this_start_cb([job](simgrid::s4u::Mess const& me) {
    dispatcher->onJobTransferStart(job, me);
  });
  job_transfer->on_this_completion_cb([job](simgrid::s4u::Mess const& me) {
    job->resource_waiting_queue_time = sg4::Engine::get_clock() - job->creation_time;
    dispatcher->onJobTransferEnd(job, me);
  });
  track_pending_activity(job_transfer);
}

void JOB_EXECUTOR::try_release_staged_jobs()
{
  for (auto it = staging_jobs.begin(); it != staging_jobs.end(); )
  {
    Job* job = *it;
    if (job_needs_transfer_staging(job)) {
      ++it;
      continue;
    }

    job->status = "assigned";
    dispatch_job_to_host(job);
    it = staging_jobs.erase(it);
  }
}

void JOB_EXECUTOR::execute_job(Job* j)
{
  // Plan (obs L/I/U/B) → Build → Register → Start (HEAD activation order).
  // I and B soft-restage until L / resting replica; never join foreign Comms.
  JobActivityGraph graph;
  try {
    auto plans = plan_all_inputs(j);
    for (const auto& plan : plans) {
      if (plan.kind == InputAccessKind::WAIT_IN_FLIGHT_THEN_LOCAL_READ) {
        throw SoftRestageSignal(
            "inbound wait for file " + plan.filename
            + " job " + std::to_string(j->jobid) + "; stage until local");
      }
      if (plan.kind == InputAccessKind::REMOTE_BOUND_WAIT) {
        throw SoftRestageSignal(
            "remote-bound wait for file " + plan.filename
            + " job " + std::to_string(j->jobid));
      }
    }
    graph = build_job_graph(j, std::move(plans));
    register_job_graph(graph);
    start_job_graph(graph);
    ACTIVATED_JOBS++;
  } catch (const SoftRestageSignal&) {
    // I or B: defer without failing the job; release when plan becomes L or U.
    return_job_to_staging(j);
  } catch (const NoGlobalReplicaError&) {
    // Accountability violation (R empty and T empty) or unrecoverable plan error.
    fail_job_clean(j);
  }
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

