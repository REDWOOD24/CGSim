#include "actions.h"

sg4::ExecPtr Actions::exec_task_multi_thread_async(Job* j, sg4::ActivitySet& pending_activities, sg4::ActivitySet& exec_activities, std::unique_ptr<DispatcherPlugin>& dispatcher)
{   // @askFred
    auto host = sg4::this_actor::get_host();
    sg4::ExecPtr exec_activity = sg4::Exec::init()
        ->set_flops_amount(j->flops)
        ->set_host(host)
        ->set_name("Exec_Job_" + j->job_name + "_on_" + host->get_name());

    exec_activities.push(exec_activity);
    exec_activity->start();


    exec_activity->on_this_completion_cb([j, &dispatcher, host](simgrid::s4u::Exec const& ex) {
        j->EXEC_time_taken += ex.get_finish_time() - ex.get_start_time();
        j->status = "finished";
         if (j->status == "finished" ) {
             host->extension<HostExtensions>()->onJobFinish(j);
             dispatcher->onJobEnd(j);
        }
      
}   
    );
}

void Actions::read_file_async(Job* j, sg4::ActivitySet& pending_activities, std::unique_ptr<DispatcherPlugin>& dispatcher)
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


        read_activity->on_this_completion_cb([file, j, &dispatcher](simgrid::s4u::Io const& io) {
            file->close();
            j->IO_time_taken += io.get_finish_time() - io.get_start_time();
            j->IO_size_performed += io.get_performed_ioops();
            j->files_read += 1;

        });
    }
}

void Actions::write_file_async(Job* j, sg4::ActivitySet& pending_activities, std::unique_ptr<DispatcherPlugin>& dispatcher)
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


        write_activity->on_this_completion_cb([file, j, &dispatcher, host](simgrid::s4u::Io const& io) {
	        file->close();
            j->IO_time_taken += io.get_finish_time() - io.get_start_time();
            j->IO_size_performed += io.get_performed_ioops();
            j->files_written += 1;
        });
    }
}
