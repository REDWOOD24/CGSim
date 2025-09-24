#include "actions.h"
#include "logger.h"  // Add this include for logging

#include <ctime>
#include <regex>
#include <iomanip>
#include <job_executor.h>

void Actions::exec_task_multi_thread_async(Job* j, sg4::ActivitySet& pending_activities, sg4::ActivitySet& exec_activities, std::unique_ptr<sqliteSaver>& saver, std::unique_ptr<DispatcherPlugin>& dispatcher)
{   // @askFred
    auto host = sg4::this_actor::get_host();
    LOG_DEBUG("In exec_task_multi_thread_async on host: {}", host->get_name());
    sg4::ExecPtr exec_activity = sg4::Exec::init()
        ->set_flops_amount(j->flops)
        ->set_host(host)
        ->set_name("Exec_Job_" + j->job_name + "_on_" + host->get_name() +"_at_site_"+j->comp_site+ "_UsingCores_" + std::to_string(j->cores));

    exec_activity->start();
    
    // pending_activities.push(exec_activity);
    exec_activities.push(exec_activity);

    exec_activity->on_this_completion_cb([j, &saver, &dispatcher, host](simgrid::s4u::Exec const& ex) {
        j->EXEC_time_taken += ex.get_finish_time() - ex.get_start_time();
        j->status = "finished";
        j->start_time = JOB_EXECUTOR::get_job_time_stamp(JOB_EXECUTOR::get_fixed_creation_time(), ex.get_start_time());
        j->end_time = JOB_EXECUTOR::get_job_time_stamp(JOB_EXECUTOR::get_fixed_creation_time(), ex.get_finish_time());
        j->queue_time = JOB_EXECUTOR::get_job_queue_time(JOB_EXECUTOR::get_fixed_creation_time(), j->start_time);
        j->lastUpdatedTimeStamp = j->end_time; // Update last updated timestamp to end time

        std::cout<< "Call back of exec activity for job id: " << j->jobid << std::endl;
        std::cout<< "Job End Time" << j->end_time<< std::endl; 
        saver->updateJob(j);
    

         if (j->status == "finished" ) {
            // LOG_DEBUG("All files read and written for job {}.", j->id);
            // LOG_CRITICAL("JOB ID {} Finished on host: {}", j->jobid ,host->get_name());
            JOB_EXECUTOR::on_job_finished(j);

            // sg4::this_actor::get_host()->extension<HostExtensions>()->onJobFinish(j);
             host->extension<HostExtensions>()->onJobFinish(j);
            // dispatcher->onJobEnd(j);
        }
      
}   
    );
}

void Actions::read_file_async(const std::shared_ptr<simgrid::fsmod::FileSystem>& fs, Job* j, sg4::ActivitySet& pending_activities, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    for (const auto& input_file : j->input_files)
    {
        const std::string filename = j->mount + input_file.first;
        auto file = fs->open(filename, "r");
        const sg_size_t filesize = fs->file_size(filename);
        file->seek(0);

        auto read_activity = file->read_async(filesize);
        read_activity->set_name("Read_" + filename + "_for_Job_" +j->job_name);
        
        
        pending_activities.push(read_activity);

        LOG_DEBUG("Reading file asynchronously: {} ({} bytes)", filename, filesize);

        read_activity->on_this_completion_cb([file, j, &dispatcher](simgrid::s4u::Io const& io) {
            file->close();
            j->IO_time_taken += io.get_finish_time() - io.get_start_time();
            j->IO_size_performed += io.get_performed_ioops();
            j->files_read += 1;

            LOG_DEBUG("Read complete: {} | Time: {} | IO Ops: {}", file->get_path(), io.get_finish_time() - io.get_start_time(), io.get_performed_ioops());

            // if (j->status == "finished" &&
            //     j->files_read == j->input_files.size() &&
            //     j->files_written == j->output_files.size()) {
            //     sg4::this_actor::get_host()->extension<HostExtensions>()->onJobFinish(j);
            //     // dispatcher->onJobEnd(j);
            // }
        });
    }
}

void Actions::write_file_async(const std::shared_ptr<simgrid::fsmod::FileSystem>& fs, Job* j, sg4::ActivitySet& pending_activities, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    auto host = sg4::this_actor::get_host();
    for (const auto& output_file : j->output_files)
    {
        const std::string filename = j->mount + output_file.first;
        const std::string filesize = std::to_string(output_file.second) + "kB";

        auto file = fs->open(filename, "w");
        auto write_activity = file->write_async(filesize);
        write_activity->set_name("Write_" + filename + "_for_Job_" + j->job_name);
        pending_activities.push(write_activity);

        LOG_DEBUG("Writing file asynchronously: {} ({} kB)", filename, output_file.second);

        write_activity->on_this_completion_cb([file, j, &dispatcher, host](simgrid::s4u::Io const& io) {
	  file->close();
            j->IO_time_taken += io.get_finish_time() - io.get_start_time();
            j->IO_size_performed += io.get_performed_ioops();
            j->files_written += 1;

            LOG_DEBUG("Job ID: {} | Write complete: {} | Time: {} | IO Ops: {}", std::to_string(j->jobid) ,file->get_path(), io.get_finish_time() - io.get_start_time(), io.get_performed_ioops());

            if (j->status == "finished" ) {
                LOG_DEBUG("Files Read equal to input file size",(j->files_read == j->input_files.size()));
                LOG_DEBUG("Files write equal to write file size",(j->files_written == j->output_files.size()));
            //     j->files_written == j->output_files.size()
                // sg4::this_actor::get_host()->extension<HostExtensions>()->onJobFinish(j);
                //host->extension<HostExtensions>()->onJobFinish(j);
                // dispatcher->onJobEnd(j);
            }
        });
    }
}