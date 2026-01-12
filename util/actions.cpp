#include "actions.h"

sg4::ExecPtr Actions::exec_task_multi_thread_async(Job* j, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    auto host = sg4::Host::by_name(j->comp_host);
    sg4::ExecPtr exec_activity = sg4::Exec::init()
        ->set_flops_amount(j->flops)
        ->set_host(host)
        ->set_name("Exec_Job_" + std::to_string(j->jobid) + "_on_" + host->get_name());


    exec_activity->on_this_start_cb([j, &dispatcher](simgrid::s4u::Exec const& ex) {
        //std::cout << "Starting Exec: " << ex.get_name() << std::endl;
        });

    exec_activity->on_this_completion_cb([j, &dispatcher, host](simgrid::s4u::Exec const& ex) {
        std::cout << ex.get_cname() << ":  " << ex.get_finish_time() - ex.get_start_time() << std::endl;
        j->EXEC_time_taken += ex.get_finish_time() - ex.get_start_time();
        j->status = "finished";
         if (j->status == "finished" ) {
             host->extension<HostExtensions>()->onJobFinish(j);
             dispatcher->onJobEnd(j);
        }});
    return exec_activity;
}

sg4::IoPtr Actions::read_file_async(Job* j, const std::string& filename, std::unique_ptr<DispatcherPlugin>& dispatcher)
{

    auto read_activity = CGSim::FileManager::read(filename, j->comp_site,j->comp_host,j->disk);
    read_activity->set_name("Read_" + filename + "_for_Job_" + std::to_string(j->jobid));

    read_activity->on_this_start_cb([j,filename, &dispatcher](simgrid::s4u::Io const& io) {
        //std::cout << "Starting Read: " << io.get_name() << std::endl;
        if (!CGSim::FileManager::exists(filename,j->comp_site)) throw std::runtime_error("File does not exist");
        });

    read_activity->on_this_completion_cb([j, &dispatcher](simgrid::s4u::Io const& io) {
            j->IO_READ_time_taken += io.get_finish_time() - io.get_start_time();
            j->IO_READ_size_performed += io.get_performed_ioops();
            j->files_read += 1;
            std::cout << io.get_cname() << ":  " << io.get_finish_time() - io.get_start_time() << std::endl;

        });

  return read_activity;
}

sg4::IoPtr Actions::write_file_async(Job* j, const std::string& filename, const long long& size, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    auto write_activity = CGSim::FileManager::write(filename, size, j->comp_site,j->comp_host,j->disk);
    write_activity->set_name("Write_" + filename + "_for_Job_" + std::to_string(j->jobid));
    write_activity->on_this_start_cb([j,filename, &dispatcher](simgrid::s4u::Io const& io) {
        //std::cout << "Starting write: " << io.get_name() << std::endl;
    });
    write_activity->on_this_completion_cb([j, &dispatcher](simgrid::s4u::Io const& io) {
           j->IO_WRITE_time_taken += io.get_finish_time() - io.get_start_time();
           j->IO_WRITE_size_performed += io.get_performed_ioops();
           j->files_written += 1;
           std::cout << io.get_cname() << ":  " << io.get_finish_time() - io.get_start_time() << std::endl;

       });
    return write_activity;
}

sg4::CommPtr Actions::comm_file_async(Job* j, const std::string& filename, const std::string& src_site, const std::string& dst_site, const long long& size, std::unique_ptr<DispatcherPlugin>& dispatcher)
{
    auto src_host = sg4::Engine::get_instance()->host_by_name_or_null(src_site+"_cpu-0");
    auto dst_host = sg4::Engine::get_instance()->host_by_name_or_null(dst_site+"_cpu-0");
    auto comm_activity = sg4::Comm::sendto_init()->set_source(src_host)->set_destination(dst_host)->set_payload_size(size);
    comm_activity->set_name("Comm_" + filename + "_for_Job_" + std::to_string(j->jobid));
    comm_activity->on_this_start_cb([filename,size,dst_site,j, &dispatcher](simgrid::s4u::Comm const& co) {
        //std::cout << "Starting comm: " << co.get_name()<< std::endl;
           });
    comm_activity->on_this_completion_cb([filename,size,dst_site,j, &dispatcher](simgrid::s4u::Comm const& co) {
        CGSim::FileManager::create(filename,size,dst_site);
        std::cout << co.get_cname() << ":  " << co.get_finish_time() - co.get_start_time() << std::endl;
           });
    return comm_activity;
}