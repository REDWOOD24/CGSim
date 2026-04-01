#ifndef DATA_MANAGEMENT_PLUGIN_H
#define DATA_MANAGEMENT_PLUGIN_H

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include "job.h"

namespace CGSim {

enum class FileTransferDecisionMode {
    DEFAULT,  // let core use default behavior
    COPY,
    MOVE
};

struct ReplicaInfo {
    std::string sitename;
    std::string hostname;
    unsigned long long size;
};

struct FileRequestContext {
    Job* job;
    std::string filename;
    std::vector<ReplicaInfo> replicas;
};

struct FileRequestDecision {
    // empty sitename => let core pick default source
    std::string chosen_site;
    FileTransferDecisionMode mode = FileTransferDecisionMode::DEFAULT;
};

class DataManagementPlugin {
public:
    virtual ~DataManagementPlugin() = default;

    // Pass the policy-specific JSON subtree
    virtual void configure(const nlohmann::json& cfg) = 0;

    // Simulation lifecycle
    virtual void onSimulationStart() {}
    virtual void onSimulationEnd() {}

    // Reactive hook: decide from which site and with which mode to serve a file request
    virtual FileRequestDecision onFileRequest(const FileRequestContext& ctx) = 0;

    // Proactive hook: optional periodic tick
    virtual void onTimerTick(double /*current_time*/) {}
};

} // namespace CGSim

#endif // DATA_MANAGEMENT_PLUGIN_H

