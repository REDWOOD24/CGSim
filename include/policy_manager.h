#pragma once
#include <functional>
#include <string>
#include <unordered_map>
#include <simgrid/kernel/Timer.hpp>
#include <simgrid/s4u.hpp>

namespace sg4 = simgrid::s4u;

class JOB_EXECUTOR;

namespace CGSim {

struct Policy
{
    bool active = true;
    double start_time       = 0.0;
    double end_time         = 0.0;
    double repeat_interval  = 0.0;
    std::function<void()> callback;
    std::string name;
};

class PolicyManager
{
public:
    PolicyManager() = delete;
    PolicyManager(const PolicyManager&) = delete;
    PolicyManager& operator=(const PolicyManager&) = delete;

    static void addPolicy(CGSim::Policy* policy);

private:
    static void run_policy(CGSim::Policy* policy);
    static void deactivate_policy(CGSim::Policy* policy);
    static std::unordered_map<std::string, CGSim::Policy*> policies;
    friend class ::JOB_EXECUTOR;
    inline static bool RUNNING = true;
};

} // namespace CGSim