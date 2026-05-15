#pragma once
#include <string>
#include <simgrid/kernel/Timer.hpp>
#include <functional>
#include <unordered_map>
#include <memory>
#include <simgrid/s4u.hpp>
namespace sg4 = simgrid::s4u;

struct SitePolicy
{
    std::string name{};
    double start_time{};        // seconds
    double end_time{};          // seconds
    double repeat_interval{};   // seconds
    std::function<void(sg4::ActivitySet&)> callback;
    bool active = true;
};

namespace CGSim {

class SitePolicyManager {
    public:
        SitePolicyManager(const SitePolicyManager&) = delete;
        SitePolicyManager& operator=(const SitePolicyManager&) = delete;
        static void addSitePolicy(SitePolicy* p);


    private:
      static std::unordered_map<std::string, SitePolicy*> active_policies;
      static void schedule_policy_repeat(SitePolicy* p);

};

}