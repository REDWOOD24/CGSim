#include "platform.h"

Platform::Platform(const std::string&  platform_name, std::vector<SiteInfo>& all_site_info,
    std::vector<SiteConnInfo>& site_conn_info)
{
    this->create_platform(platform_name,all_site_info);
    this->initialize_site_connections(site_conn_info);
    this->initialize_job_server();
}

void Platform::create_platform(const std::string& platform_name, const std::vector<SiteInfo>& all_site_info)
{
    platform = sg4::create_full_zone(platform_name);
    Platform::initialize_simgrid_plugins();
    for (auto& site_info : all_site_info){
        auto* site = sg4::create_star_zone(site_info.name);
        site->set_parent(platform);
        for (const auto& [key,value] : site_info.properties){site->set_property(key,value);}
        int cpu_counter = 0;
        for (const auto& cpu_clusters : site_info.cpu_info) {
            for (int cpu = 0; cpu < cpu_clusters.units; ++cpu) {
                std::string cpu_name = site_info.name + "_cpu-" + std::to_string(cpu_counter);
                sg4::Host* host = site->create_host(cpu_name, cpu_clusters.speed);
                host->set_core_count(cpu_clusters.cores);
                for (const auto& [key,value] : cpu_clusters.properties){host->set_property(key,value);}
                const sg4::Link* link = site->create_split_duplex_link("link_" + cpu_name,
                    cpu_clusters.BW_CPU)->set_latency(cpu_clusters.LAT_CPU)->seal();
                site->add_route(host, nullptr, {{link, sg4::LinkInRoute::Direction::UP}}, true);
                if (cpu_name == std::string(site_info.name + "_cpu-0")) {site->set_gateway(host->get_netpoint());}
                for (const auto& d : cpu_clusters.disk_info) {
                    host->create_disk(d.name, d.read_bw, d.write_bw);
                }
                cpu_counter++;
                host->seal();
            }
        }
        CGSim::FileManager::register_site(site,site_info.files);
        sites[site_info.name] = site;
    }
}

void Platform::initialize_site_connections(std::vector<SiteConnInfo>& site_conn_info)
{
    for (const auto& siteConn : site_conn_info) {
        const auto src_name = siteConn.site_A;
        const auto dst_name = siteConn.site_B;
        const auto linkname = "link_" + siteConn.site_A+":"+siteConn.site_B;

        const sg4::NetZone* src = sites.at(src_name);
        const sg4::NetZone* dst = sites.at(dst_name);

        const sg4::Link* interzonal_link = platform->create_link(linkname, siteConn.bandwidth)->set_latency(siteConn.latency)->seal();
        platform->add_route(src, dst, { sg4::LinkInRoute(interzonal_link) });

        //LOG_DEBUG("Connected {} <--> {} with latency {} and bandwidth {}", src_name, dst_name, latency, bandwidth);
    }
}

void Platform::initialize_simgrid_plugins()
{
    simatlas_host_extension_init();
}

void Platform::initialize_job_server()
{
    auto* JOB_SERVER_site = sg4::create_star_zone("JOB-SERVER");
    JOB_SERVER_site->set_parent(platform);

    const double JOB_SERVER_CPU_SPEED = 1e9;
    const double JOB_SERVER_BW_CPU = 1e11;
    const double JOB_SERVER_LAT_CPU = 0;
    const int JOB_SERVER_cores = 32;
    const std::string JOB_SERVER_RAM = "16GiB";
    const sg4::Link* JOB_SERVER_link = JOB_SERVER_site->create_link(
        "link_JOB-SERVER_cpu-0", JOB_SERVER_BW_CPU)->set_latency(JOB_SERVER_LAT_CPU)->seal();
    sg4::Host* JOB_SERVER_host = JOB_SERVER_site->create_host("JOB-SERVER_cpu-0", JOB_SERVER_CPU_SPEED);

    JOB_SERVER_host->set_core_count(JOB_SERVER_cores);
    JOB_SERVER_host->set_property("ram", JOB_SERVER_RAM);
    JOB_SERVER_site->add_route(JOB_SERVER_host, nullptr, { { JOB_SERVER_link, sg4::LinkInRoute::Direction::UP}}, true);
    JOB_SERVER_site->set_gateway(JOB_SERVER_host->get_netpoint());
    JOB_SERVER_host->seal();


    for (auto& [site_name, site] : sites) {
        const auto linkname = "link_JOB_SERVER:" + site_name;
        const std::string latency = "0ms";
        const double bandwidth = 1000 * 1.25e+7;
        const sg4::Link* server_site_link = platform->create_link(linkname, bandwidth)->set_latency(latency)->seal();
        platform->add_route(JOB_SERVER_site, site, {sg4::LinkInRoute(server_site_link)});
    }
}
