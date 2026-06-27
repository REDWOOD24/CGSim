#pragma once
#include <functional>
#include <string>
#include <unordered_map>
#include <simgrid/kernel/Timer.hpp>
#include <simgrid/s4u.hpp>

namespace sg4 = simgrid::s4u;

struct Policy
{
    bool active = true;
    double start_time       = 0.0;
    double end_time         = 0.0;
    double repeat_interval  = 0.0;
    std::function<void()> callback;
    std::string name;
};

namespace CGSim {

class PolicyManager
{
public:
    PolicyManager() = delete;
    PolicyManager(const PolicyManager&) = delete;
    PolicyManager& operator=(const PolicyManager&) = delete;

    //PolicyManager takes ownership of the pointer.
    static void addPolicy(Policy* policy);

private:
    static void run_policy(Policy* policy);
    static void deactivate_policy(Policy* policy);

    static std::unordered_map<std::string, Policy*> policies;
};

} // namespace CGSim