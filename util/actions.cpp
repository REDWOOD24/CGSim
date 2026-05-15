#include "actions.h"

sg4::ExecPtr Actions::exec_task_multi_thread_async(Job* j)
{
    auto host = sg4::Host::by_name(j->comp_host);
    sg4::ExecPtr exec_activity = sg4::Exec::init()
        ->set_flops_amount((1.0*j->flops)/(1.0*j->cores))
        ->set_host(host)
        ->set_name("Exec_Job_" + std::to_string(j->jobid) + "_on_" + host->get_name());

    exec_activity->on_this_start_cb([j](simgrid::s4u::Exec const& ex) {
        j->status = "running";
        CGSim::get_site_manager()->movePendingtoRunningJob(j->comp_site);
        //std::cout << "Site: " << j->comp_site << ", Jobs in Pending: " << CGSim::get_site_manager()->getPendingJobs(j->comp_site) << std::endl;
        //std::cout << "Site: " << j->comp_site << ", Jobs in Running: " << CGSim::get_site_manager()->getRunningJobs(j->comp_site) << std::endl;
        //std::cout << "Site: " << j->comp_site << ", Jobs in Finished: " << CGSim::get_site_manager()->getFinishedJobs(j->comp_site) << std::endl;
        //std::cout << "Site: " << j->comp_site << ", CPUs Available: " << CGSim::get_site_manager()->getCPUsAvailable(j->comp_site) << std::endl;
        //std::cout << "Site: " << j->comp_site << ", CPUs Used: " << CGSim::get_site_manager()->getCPUsUsed(j->comp_site) << std::endl;
        //std::cout << "Site: " << j->comp_site << ", Cores Available: " << CGSim::get_site_manager()->getCoresAvailable(j->comp_site) << std::endl;
        //std::cout << "Site: " << j->comp_site << ", Cores Used: " << CGSim::get_site_manager()->getCoresUsed(j->comp_site) << std::endl;
        j->file_transfer_queue_time = sg4::Engine::get_clock() - j->resource_waiting_queue_time - j->total_io_read_time;
        JOB_EXECUTOR::dispatcher->onJobExecutionStart(j,ex);
    });

    exec_activity->on_this_completion_cb([j, host](simgrid::s4u::Exec const& ex) {
        j->status = "finished";
        CGSim::get_site_manager()->moveRunningtoFinishedJob(j->comp_site);
        host->extension<HostExtensions>()->onJobFinish(j);
        JOB_EXECUTOR::USED_CORES -= j->cores;
        JOB_EXECUTOR::dispatcher->onJobExecutionEnd(j,ex);
        });

    return exec_activity;
}

sg4::IoPtr Actions::read_file_async(Job* j, const std::string& filename)
{

    auto read_activity = CGSim::get_file_manager()->read(filename, j->comp_site,j->comp_host,j->disk);
    read_activity->set_name("Read_File_"+ filename + "_for_Job_" + std::to_string(j->jobid) + "_on_" + j->comp_host);
    auto size = CGSim::get_file_manager()->request_file_size(filename);
    read_activity->on_this_start_cb([j,filename,size](simgrid::s4u::Io const& io) {
        JOB_EXECUTOR::dispatcher->onFileReadStart(j,filename,size,io);
        });

    read_activity->on_this_completion_cb([j,filename,size](simgrid::s4u::Io const& io) {
            j->total_io_read_time += (io.get_finish_time() - io.get_start_time());
            JOB_EXECUTOR::dispatcher->onFileReadEnd(j,filename,size,io);
            });

  return read_activity;
}

sg4::IoPtr Actions::write_file_async(Job* j, const std::string& filename, const unsigned long long& size)
{
    auto write_activity = CGSim::get_file_manager()->write(filename, size, j->comp_site,j->comp_host,j->disk);
    write_activity->set_name("Write_File_"+ filename + "_for_Job_" +std::to_string(j->jobid) + "_on_" + j->comp_host);

    write_activity->on_this_start_cb([j,filename,size](simgrid::s4u::Io const& io) {
        JOB_EXECUTOR::dispatcher->onFileWriteStart(j,filename,size,io);
        });

    write_activity->on_this_completion_cb([j,filename,size](simgrid::s4u::Io const& io) {
            j->total_io_write_time += (io.get_finish_time() - io.get_start_time());
            JOB_EXECUTOR::dispatcher->onFileWriteEnd(j,filename,size,io);
       });

    return write_activity;
}

sg4::CommPtr Actions::transfer_file_async(Job* j, const std::string& filename, const std::string& src_site, const std::string& dst_site)
{

    auto transfer_activity = CGSim::get_file_manager()->transfer(filename,src_site,dst_site);
    const auto size = static_cast<unsigned long long>(transfer_activity->get_remaining());

    transfer_activity->on_this_start_cb([j,filename,size,src_site,dst_site](simgrid::s4u::Comm const& co) {
        if (!started_transfers.insert(co.get_name()).second) return;
        JOB_EXECUTOR::dispatcher->onFileTransferStart(j,filename,size,co,src_site,dst_site);
        });

    transfer_activity->on_this_completion_cb([filename,size,src_site,dst_site,j](simgrid::s4u::Comm const& co) {
        CGSim::get_file_manager()->create(filename,size,dst_site);
        started_transfers.erase(co.get_name());
        JOB_EXECUTOR::dispatcher->onFileTransferEnd(j,filename,size,co,src_site,dst_site);
        });

    return transfer_activity;
}
