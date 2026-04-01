#ifndef DATA_MANAGEMENT_POLICY_H
#define DATA_MANAGEMENT_POLICY_H

#include <memory>
#include <nlohmann/json.hpp>
#include "DataManagementPlugin.h"

namespace CGSim {

class DataManagementPolicy {
public:
    // Configure and enable/disable the active data management plugin.
    static void configure(const nlohmann::json& cfg);

    // Lifecycle controls used by the core simulation.
    static void onSimulationStart();
    static void onSimulationEnd();

    static bool isEnabled();
    static void setEnabled(bool enabled);

    // Reactive hook used by core when a job requests a file.
    static FileRequestDecision onFileRequest(const FileRequestContext& ctx);

    // Proactive hook (optional): called periodically by whoever manages timers.
    static void onTimerTick(double current_time);

    // Optional statistics
    static unsigned long getExecutionCount();

private:
    static bool enabled_;
    static unsigned long execution_count_;
    static std::unique_ptr<DataManagementPlugin> plugin_;
};

} // namespace CGSim

#endif // DATA_MANAGEMENT_POLICY_H
