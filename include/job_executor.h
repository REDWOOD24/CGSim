#ifndef JOB_EXECUTOR_H
#define JOB_EXECUTOR_H

#include "DispatcherPlugin.h"
#include "PluginLoader.h"
#include "actions.h"
#include "file_manager.h"
#include "policy_manager.h"
#include "host_extensions.h"
#include <chrono>
#include <unordered_set>
#include <vector>
#include <simgrid/kernel/Timer.hpp>

class Actions;

class JOB_EXECUTOR
{


public:
     JOB_EXECUTOR()= default;
    ~JOB_EXECUTOR()= default;
    friend class Actions;

    static void                set_dispatcher(std::shared_ptr<DispatcherPlugin>& d){dispatcher = d;}
    static void                start_job_execution();

    /** Add an owned activity to pending_activities (unconditional push; each owned
     *  activity is registered exactly once by construction, see register_job_graph). */
    static void                track_pending_activity(const sg4::ActivityPtr& activity);
    /** Remove already-finished activities from pending_activities. */
    static void                drain_completed_pending_activities();
    /** Drain finished activities, then block until one more completes. */
    static void                wait_for_pending_progress();


private:
    // Observation labels (Section 3.5): L, I, U, B
    // I/B actions: stage until L (or resting replica); do not join foreign Comms.
    enum class InputAccessKind {
        LOCAL_READ,                      // L: resting replica at compute site
        WAIT_IN_FLIGHT_THEN_LOCAL_READ,  // I: inbound — stage until L
        TRANSFER_THEN_READ,              // U: remote resting replica; start new transfer
        REMOTE_BOUND_WAIT                // B: remote-bound in-flight — stage until resting
    };

    struct InputAccessPlan {
        std::string filename;
        InputAccessKind kind = InputAccessKind::LOCAL_READ;
        std::string src_site;
        CGSim::FileTransferDecisionMode mode = CGSim::FileTransferDecisionMode::COPY;
        sg4::CommPtr in_flight_comm;
    };

    struct JobActivityGraph {
        sg4::ExecPtr exec;
        std::vector<sg4::CommPtr> transfers;
        std::vector<sg4::CommPtr> joined;
        std::vector<sg4::IoPtr> reads;
        std::vector<sg4::IoPtr> writes;
        bool registered = false;
    };

    static void                start_receivers();
    static void                start_server();
    static void                execute_job(Job* j);
    static void                onJobAssignment(Job* job);
    static void                reserve_job_assignment(Job* job);
    static void                dispatch_job_to_host(Job* job);
    static bool                job_needs_transfer_staging(Job* job);
    static void                try_release_staged_jobs();
    static void                resolve_input_file_source(
        Job* job,
        const std::string& filename,
        const std::pair<long long, std::unordered_set<std::string>>& fileinfo,
        std::string& filelocation,
        CGSim::FileTransferDecisionMode& mode);
    static InputAccessPlan     plan_one_input(
        Job* job,
        const std::string& filename,
        const std::pair<long long, std::unordered_set<std::string>>& fileinfo);
    static std::vector<InputAccessPlan> plan_all_inputs(Job* job);
    static void                revalidate_plan(Job* job, InputAccessPlan& plan);
    static JobActivityGraph    build_job_graph(Job* job, std::vector<InputAccessPlan> plans);
    static void                build_local_read_branch(Job* job, const std::string& filename, JobActivityGraph& graph);
    static void                build_transfer_read_branch(Job* job, const InputAccessPlan& plan, JobActivityGraph& graph);
    static void                build_join_in_flight_branch(Job* job, const InputAccessPlan& plan, JobActivityGraph& graph);
    static void                register_job_graph(JobActivityGraph& graph);
    static void                start_job_graph(JobActivityGraph& graph);
    static void                return_job_to_staging(Job* job);
    static void                fail_job_clean(Job* job);
    static void                get_jobs();
    static void                advance_to_time(double time);
    [[noreturn]] static void   receiver(const std::string& MQ_name);
    static void                attach_callbacks();
    static unsigned long       totalJobs(JobQueue jobs);


    static   sg4::ActivitySet                     pending_activities;
    static   unsigned long                        USED_CORES;
    static   unsigned long                        TOTAL_CORES;
    static   unsigned long                        DISPATCHED_JOBS;
    static   unsigned long                        FINISHED_JOBS;
    static   unsigned long                        ACTIVATED_JOBS;
    static   unsigned long                        TOTAL_JOBS;
    static   unsigned long                        MAX_RETRIES;
    static   JobQueue                             jobs;
    static   std::unordered_map<long long, Job*>  all_jobs;
    static   std::vector<Job*>                    pending_jobs;
    static   std::vector<Job*>                    staging_jobs;
    static   std::shared_ptr<DispatcherPlugin>    dispatcher;

};

#endif //JOB_EXECUTOR_H
