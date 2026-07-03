#include "policy.h"

void POLICY::addPolicies() 
{
    auto p = test_policy();
    CGSim::PolicyManager::addPolicy(p);
}

CGSim::Policy* POLICY::test_policy()
{
    CGSim::Policy* p = new CGSim::Policy();
    p->start_time = 0.0;
    p->end_time = 8000000.0;
    p->repeat_interval = 10000.0;
    p->name = "Data Movement Policy";

    const std::string policy_name = p->name;
    p->callback = [this,policy_name]()
    {
    auto sites = CGSim::get_site_manager()->get_all_sites();
    std::string src_site;
    std::string dst_site;
    std::string filename;

    do
    {
    auto src_it = sites.begin();
    std::advance(src_it, std::rand() % sites.size());
    src_site = *src_it;

    auto dst_it = sites.begin();
    std::advance(dst_it, std::rand() % sites.size());
    dst_site = *dst_it;
    }
    while (src_site == dst_site);

    auto files = CGSim::get_file_manager()->request_site_files(src_site);
    auto file_it = files.begin();
    std::advance(file_it, std::rand() % files.size());
    filename = *file_it;

    CGSim::FileTransferDecisionMode mode = CGSim::FileTransferDecisionMode::COPY;
    make_background_transfer(policy_name,filename,src_site,dst_site,mode);
    };
    return p;
}

void POLICY::make_background_transfer(const std::string& policy_name, const std::string& filename, 
    const std::string& src_site, const std::string& dst_site, CGSim::FileTransferDecisionMode& mode)
{
    auto t = CGSim::get_file_manager()->transfer(filename,src_site,dst_site,mode);
    auto size = CGSim::get_file_manager()->request_file_size(filename);

    t->on_this_start_cb([this,policy_name,filename,size,src_site,dst_site,mode](simgrid::s4u::Comm const& co) {
    if (!started_transfers.insert(co.get_name()).second) return;
    ou->onPolicyFileTransferStart(policy_name,filename, size, co, src_site,dst_site);
    });

    t->on_this_completion_cb([this,policy_name,filename,size,src_site,dst_site,mode](simgrid::s4u::Comm const& co) {
    started_transfers.erase(co.get_name());
    ou->onPolicyFileTransferEnd(policy_name,filename, size, co, src_site,dst_site);
    active_background_transfers[co.get_name()].second = false;
    });

    t->start();
    active_background_transfers[t->get_name()] = {t,true};

    //cleanup
    for (auto it = active_background_transfers.begin(); it != active_background_transfers.end(); ) 
    {
    const std::string& id = it->first;
    sg4::CommPtr& comm = it->second.first;
    bool& active = it->second.second;
    if (!active) it = active_background_transfers.erase(it);else ++it;
    }
    std::cout << "active_background_transfers: " << active_background_transfers.size() << std::endl;
}