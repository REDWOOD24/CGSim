#include "actions.h"

sg4::ExecPtr Actions::exec_task_multi_thread_async(Job* j, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    auto host = sg4::Host::by_name(j->comp_host);
    sg4::ExecPtr exec_activity = sg4::Exec::init()
        ->set_flops_amount((1.0*j->flops)/(1.0*j->cores))
        ->set_host(host)
        ->set_name("Exec_Job_" + std::to_string(j->jobid) + "_on_" + host->get_name());

    exec_activity->on_this_start_cb([j, &dispatcher](simgrid::s4u::Exec const& ex) {
        j->file_transfer_queue_time = sg4::Engine::get_clock() - j->resource_waiting_queue_time - j->total_io_read_time;
        j->status = "running";
        dispatcher->onJobExecutionStart(j,ex);
    });

    exec_activity->on_this_completion_cb([j, &dispatcher, host](simgrid::s4u::Exec const& ex) {
        j->status = "finished";
        host->extension<HostExtensions>()->onJobFinish(j);
        dispatcher->onJobExecutionEnd(j,ex);
        });

    return exec_activity;
}

sg4::IoPtr Actions::read_file_async(Job* j, const std::string& filename, std::unique_ptr<DispatcherPlugin>& dispatcher)
{

    auto read_activity = CGSim::FileManager::read(filename, j->comp_site,j->comp_host,j->disk);
    read_activity->set_name("Read_File_"+ filename + "_for_Job_" + std::to_string(j->jobid) + "_on_" + j->comp_host);
    auto size = CGSim::FileManager::request_file_size(filename);
    read_activity->on_this_start_cb([j,filename,size, &dispatcher](simgrid::s4u::Io const& io) {
        dispatcher->onFileReadStart(j,filename,size,io);
        });

    read_activity->on_this_completion_cb([j,filename,size,&dispatcher](simgrid::s4u::Io const& io) {
            j->total_io_read_time += (io.get_finish_time() - io.get_start_time());
            dispatcher->onFileReadEnd(j,filename,size,io);
            });

  return read_activity;
}

sg4::IoPtr Actions::write_file_async(Job* j, const std::string& filename, const unsigned long long& size, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    auto write_activity = CGSim::FileManager::write(filename, size, j->comp_site,j->comp_host,j->disk);
    write_activity->set_name("Write_File_"+ filename + "_for_Job_" +std::to_string(j->jobid) + "_on_" + j->comp_host);

    write_activity->on_this_start_cb([j,filename,size,&dispatcher](simgrid::s4u::Io const& io) {
        dispatcher->onFileWriteStart(j,filename,size,io);
        });

    write_activity->on_this_completion_cb([j,filename,size,&dispatcher](simgrid::s4u::Io const& io) {
            j->total_io_write_time += (io.get_finish_time() - io.get_start_time());
            dispatcher->onFileWriteEnd(j,filename,size,io);
       });

    return write_activity;
}

sg4::CommPtr Actions::transfer_file_async(Job* j, const std::string& filename, const std::string& src_site, const std::string& dst_site, std::unique_ptr<DispatcherPlugin>& dispatcher)
{

    auto transfer_activity = CGSim::FileManager::transfer(filename,src_site,dst_site);
    const auto size = static_cast<unsigned long long>(transfer_activity->get_remaining());

    transfer_activity->on_this_start_cb([j,filename,size,src_site,dst_site, &dispatcher](simgrid::s4u::Comm const& co) {
        if (!started_transfers.insert(co.get_name()).second) return;
        dispatcher->onFileTransferStart(j,filename,size,co,src_site,dst_site);
        });

    transfer_activity->on_this_completion_cb([filename,size,src_site,dst_site,j, &dispatcher](simgrid::s4u::Comm const& co) {
        CGSim::FileManager::create(filename,size,dst_site);
        started_transfers.erase(co.get_name());
        dispatcher->onFileTransferEnd(j,filename,size,co,src_site,dst_site);
        });

    return transfer_activity;
}
