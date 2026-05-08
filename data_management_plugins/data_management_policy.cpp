#include "data_management_policy.h"
#include "file_manager.h"
#include "logger.h"
#include <simgrid/s4u/Comm.hpp>
#include <simgrid/s4u/Link.hpp>
#include <simgrid/kernel/Timer.hpp>
#include <algorithm>
#include <vector>
#include <set>
#include <unordered_set>
#include <tuple>
#include <random>
#include <limits>
#include <stdexcept>

namespace sg4 = simgrid::s4u;
namespace timer = simgrid::kernel::timer;

namespace {

enum class FilePickMode { FIRST_FIT, LARGEST_FIT, SMALLEST_FIT, RANDOM_FIT };

enum class PathMetricMode { ESTIMATED_TRANSFER_TIME, LINK_LOAD, BANDWIDTH_ONLY };

enum class HotDestinationPolicyMode {
    REQUESTING_SITES_FIRST,
    LEAST_UTILIZED_AMONG_REQUESTING
};

enum class ProactiveTransferKind {
    STORAGE_REBALANCE,
    NETWORK_AWARE_REBALANCE,
    HOTSET_REPLICATION,
    CUSTOM_POLICY_AGENT
};

enum class ReactiveRemoteTemplate {
    FIRST_REPLICA,
    LEAST_UTILIZED_SOURCE,
    MOST_UTILIZED_SOURCE,
    RANDOM_REPLICA,
    HASH_FILENAME_JOB,
    CUSTOM_POLICY_AGENT
};

[[nodiscard]] static sg4::Link* linkBetweenSites(const std::string& src_site,
                                                 const std::string& dst_site) {
    sg4::Link* l = sg4::Link::by_name_or_null("link_" + src_site + ":" + dst_site);
    if (!l) {
        l = sg4::Link::by_name_or_null("link_" + dst_site + ":" + src_site);
    }
    return l;
}

[[nodiscard]] static std::string indexedListPickString(const nlohmann::json& value,
                                                       const std::string& fallback) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    if (value.is_array() && value.size() == 2 && value[1].is_array() && value[0].is_number()) {
        const long long raw = value[0].get<long long>();
        const auto& arr = value[1];
        if (!arr.empty() && raw >= 0) {
            const std::size_t idx = static_cast<std::size_t>(raw);
            if (idx < arr.size() && arr[idx].is_string()) {
                return arr[idx].get<std::string>();
            }
        }
    }
    return fallback;
}

[[nodiscard]] static FilePickMode parseFilePickMode(const nlohmann::json& value) {
    const std::string s = indexedListPickString(value, "first_fit");
    if (s == "largest_fit") {
        return FilePickMode::LARGEST_FIT;
    }
    if (s == "smallest_fit") {
        return FilePickMode::SMALLEST_FIT;
    }
    if (s == "random_fit") {
        return FilePickMode::RANDOM_FIT;
    }
    return FilePickMode::FIRST_FIT;
}

[[nodiscard]] static PathMetricMode parsePathMetricMode(const nlohmann::json& value) {
    const std::string s = indexedListPickString(value, "estimated_transfer_time");
    if (s == "link_load") {
        return PathMetricMode::LINK_LOAD;
    }
    if (s == "bandwidth_only") {
        return PathMetricMode::BANDWIDTH_ONLY;
    }
    return PathMetricMode::ESTIMATED_TRANSFER_TIME;
}

[[nodiscard]] static HotDestinationPolicyMode parseHotDestinationPolicy(const nlohmann::json& value) {
    const std::string s = indexedListPickString(value, "requesting_sites_first");
    if (s == "least_utilized_among_requesting") {
        return HotDestinationPolicyMode::LEAST_UTILIZED_AMONG_REQUESTING;
    }
    return HotDestinationPolicyMode::REQUESTING_SITES_FIRST;
}

[[nodiscard]] static ProactiveTransferKind parseProactiveTransferKind(const nlohmann::json& value) {
    std::string s;
    if (value.is_string()) {
        s = value.get<std::string>();
    } else if (value.is_array() && value.size() == 2 && value[0].is_number() &&
               value[1].is_array()) {
        const long long raw = value[0].get<long long>();
        const auto& arr = value[1];
        if (!arr.empty() && raw >= 0) {
            const std::size_t idx = static_cast<std::size_t>(raw);
            if (idx < arr.size() && arr[idx].is_string()) {
                s = arr[idx].get<std::string>();
            }
        }
    }
    if (s == "network_aware_rebalance") {
        return ProactiveTransferKind::NETWORK_AWARE_REBALANCE;
    }
    if (s == "hotset_replication") {
        return ProactiveTransferKind::HOTSET_REPLICATION;
    }
    if (s == "custom_policy_agent") {
        return ProactiveTransferKind::CUSTOM_POLICY_AGENT;
    }
    return ProactiveTransferKind::STORAGE_REBALANCE;
}

struct StorageRebalanceParams {
    double high_utilization_threshold = 0.8;
    double low_utilization_threshold = 1.0;
    FilePickMode file_pick = FilePickMode::FIRST_FIT;
    int max_transfers_per_tick = 1;
    bool skip_if_already_replica_on_destination = true;
};

struct NetworkAwareParams {
    double high_utilization_threshold = 0.8;
    double low_utilization_threshold = 1.0;
    PathMetricMode path_metric = PathMetricMode::ESTIMATED_TRANSFER_TIME;
    double max_path_load = 1.0;
    FilePickMode file_pick = FilePickMode::FIRST_FIT;
    int max_transfers_per_tick = 1;
};

struct HotsetReplicationParams {
    int hotness_window = 100;
    double hotness_threshold = 0.7;
    int prediction_horizon = 50;
    int target_replica_count = 3;
    HotDestinationPolicyMode dest_policy = HotDestinationPolicyMode::REQUESTING_SITES_FIRST;
    int max_transfers_per_tick = 1;
};

[[nodiscard]] static const char* proactiveTransferKindLogName(ProactiveTransferKind kind) {
    switch (kind) {
    case ProactiveTransferKind::STORAGE_REBALANCE:
        return "storage_rebalance";
    case ProactiveTransferKind::NETWORK_AWARE_REBALANCE:
        return "network_aware_rebalance";
    case ProactiveTransferKind::HOTSET_REPLICATION:
        return "hotset_replication";
    case ProactiveTransferKind::CUSTOM_POLICY_AGENT:
        return "custom_policy_agent";
    }
    return "storage_rebalance";
}

class ProactiveDataManagementPlugin : public CGSim::DataManagementPlugin {
public:
    void configure(const nlohmann::json& cfg) override {
        // Proactive section
        if (cfg.contains("proactive") && cfg["proactive"].is_object()) {
            const auto& p = cfg["proactive"];
            proactive_enabled_ = p.value("enabled", true);
            if (p.contains("interval")) {
                interval_ = p["interval"].get<double>();
            }
            if (p.contains("data_transfer_mode")) {
                std::string mode_str = p["data_transfer_mode"].get<std::string>();
                data_transfer_mode_ =
                    (mode_str == "MOVE") ? FileTransferMode::MOVE : FileTransferMode::COPY;
            }
            proactive_rng_.seed(
                static_cast<std::mt19937::result_type>(p.value("random_seed", 1337)));

            proactive_transfer_kind_ = ProactiveTransferKind::STORAGE_REBALANCE;

            const bool has_new_schema = p.contains("transfer_template");

            storage_rebalance_params_ = {};
            network_aware_params_ = {};
            hotset_params_ = {};

            if (has_new_schema) {
                proactive_transfer_kind_ = parseProactiveTransferKind(p["transfer_template"]);
                const auto& tpl = p.contains("template_params") && p["template_params"].is_object()
                                      ? p["template_params"]
                                      : nlohmann::json::object();

                if (tpl.contains("storage_rebalance") && tpl["storage_rebalance"].is_object()) {
                    const auto& s = tpl["storage_rebalance"];
                    storage_rebalance_params_.high_utilization_threshold =
                        std::clamp(s.value("high_utilization_threshold", 0.85), 0.0, 1.0);
                    storage_rebalance_params_.low_utilization_threshold =
                        std::clamp(s.value("low_utilization_threshold", 0.60), 0.0, 1.0);
                    storage_rebalance_params_.file_pick = parseFilePickMode(s.value(
                        "file_pick", nlohmann::json("first_fit")));
                    storage_rebalance_params_.max_transfers_per_tick =
                        std::max(1, s.value("max_transfers_per_tick", 1));
                    storage_rebalance_params_.skip_if_already_replica_on_destination =
                        s.value("skip_if_already_replica_on_destination", true);
                }
                if (tpl.contains("network_aware_rebalance") &&
                    tpl["network_aware_rebalance"].is_object()) {
                    const auto& n = tpl["network_aware_rebalance"];
                    network_aware_params_.high_utilization_threshold =
                        std::clamp(n.value("high_utilization_threshold", 0.85), 0.0, 1.0);
                    network_aware_params_.low_utilization_threshold =
                        std::clamp(n.value("low_utilization_threshold", 0.60), 0.0, 1.0);
                    network_aware_params_.path_metric = parsePathMetricMode(n.value(
                        "path_metric", nlohmann::json("estimated_transfer_time")));
                    network_aware_params_.max_path_load = n.value("max_path_load", 0.80);
                    network_aware_params_.file_pick = parseFilePickMode(n.value(
                        "file_pick", nlohmann::json("first_fit")));
                    network_aware_params_.max_transfers_per_tick =
                        std::max(1, n.value("max_transfers_per_tick", 1));
                }
                if (tpl.contains("hotset_replication") && tpl["hotset_replication"].is_object()) {
                    const auto& h = tpl["hotset_replication"];
                    hotset_params_.hotness_window = h.value("hotness_window", 100);
                    hotset_params_.hotness_threshold =
                        std::clamp(h.value("hotness_threshold", 0.70), 0.0, 1.0);
                    hotset_params_.prediction_horizon = h.value("prediction_horizon", 50);
                    hotset_params_.target_replica_count =
                        std::max(1, h.value("target_replica_count", 3));
                    hotset_params_.dest_policy =
                        parseHotDestinationPolicy(h.value("candidate_destination_policy",
                                                          nlohmann::json(
                                                              "requesting_sites_first")));
                    hotset_params_.max_transfers_per_tick =
                        std::max(1, h.value("max_transfers_per_tick", 1));
                }
            } else {
                double legacy_high = 0.8;
                if (p.contains("high_utilization_threshold")) {
                    legacy_high = std::clamp(p["high_utilization_threshold"].get<double>(), 0.0, 1.0);
                }

                proactive_transfer_kind_ = ProactiveTransferKind::STORAGE_REBALANCE;
                storage_rebalance_params_.high_utilization_threshold = legacy_high;
                storage_rebalance_params_.low_utilization_threshold = 1.0;
                storage_rebalance_params_.file_pick = FilePickMode::FIRST_FIT;
                storage_rebalance_params_.max_transfers_per_tick = 1;
                storage_rebalance_params_.skip_if_already_replica_on_destination = true;
            }
        } else {
            proactive_enabled_ = false;
        }

        // Reactive section
        if (cfg.contains("reactive") && cfg["reactive"].is_object()) {
            const auto& r = cfg["reactive"];
            reactive_enabled_ = r.value("enabled", true);
            prefer_local_replica_ = r.value("prefer_local_replica", prefer_local_replica_);
            copy_to_move_threshold_ =
                r.value("copy_to_move_threshold", copy_to_move_threshold_);
            remote_source_template_ = parseRemoteSourceTemplateFromConfig(
                r.value("remote_source_template", nlohmann::json("first_replica")));
            if (remote_source_template_ == ReactiveRemoteTemplate::RANDOM_REPLICA) {
                random_seed_ = r.value("random_seed", random_seed_);
                rng_.seed(static_cast<std::mt19937::result_type>(random_seed_));
            }
        } else {
            reactive_enabled_ = false;
        }
    }

    void onSimulationStart() override {
        if (!proactive_enabled_ || interval_ <= 0.0) {
            return;
        }
        double current_time = sg4::Engine::get_clock();
        const char* mode_str =
            (data_transfer_mode_ == FileTransferMode::MOVE) ? "MOVE" : "COPY";
        CG_SIM_LOG_INFO(
            "Proactive Data Management Plugin started. Transfer template={}, data movement "
            "mode={}. "
            "Immediate tick at t={}, then periodic every {:.6g} sim time units",
            proactiveTransferKindLogName(proactive_transfer_kind_), mode_str,
            current_time, interval_);

        // One synchronous tick at bootstrap: JOB_EXECUTOR may call onSimulationEnd as soon as
        // job activities drain — often before the first kernel Timer fires — so timers alone
        // can miss proactive work on short toy runs.
        execution_count_++;
        performDataManagementOperations(current_time);
        scheduleNext(current_time);
    }

    void onSimulationEnd() override {
        if (current_timer_) {
            current_timer_->remove();
            current_timer_ = nullptr;
            CG_SIM_LOG_INFO("Proactive Data Management Plugin stopped");
        }
    }

    void onTimerTick(double current_time) override {
        // For now, the timer is managed internally via scheduleNext; no external ticks used.
        (void)current_time;
    }

    CGSim::FileRequestDecision onFileRequest(const CGSim::FileRequestContext& ctx) override {
        if (!reactive_enabled_) {
            return {};
        }
        CGSim::FileRequestDecision decision;
        const std::string& job_site = ctx.job->comp_site;

        // Count replicas per site
        std::size_t replica_count = ctx.replicas.size();

        // Prefer local replica if available
        const CGSim::ReplicaInfo* local_replica = nullptr;
        for (const auto& replica : ctx.replicas) {
            if (replica.sitename == job_site) {
                local_replica = &replica;
                break;
            }
        }

        if (prefer_local_replica_ && local_replica != nullptr) {
            decision.chosen_site = local_replica->sitename;
        } else if (!ctx.replicas.empty()) {
            // No local replica: use the configured remote source selection template.
            decision.chosen_site = chooseRemoteSourceSite(ctx);
        }

        // Decide mode: if there are "too many" replicas, suggest MOVE to thin them out.
        // Otherwise, explicitly use COPY (never log DEFAULT to keep notation clear).
        if (replica_count > copy_to_move_threshold_) {
            decision.mode = CGSim::FileTransferDecisionMode::MOVE;
        } else {
            decision.mode = CGSim::FileTransferDecisionMode::COPY;
        }

        const char* mode_str =
            (decision.mode == CGSim::FileTransferDecisionMode::MOVE) ? "MOVE" : "COPY";
        const std::string& dst_site = job_site; // destination is always the job's compute site

        // Build a comma-separated list of all replica site names for logging.
        std::vector<std::string> replica_sites;
        replica_sites.reserve(ctx.replicas.size());
        for (const auto& replica : ctx.replicas) {
            replica_sites.push_back(replica.sitename);
        }
        std::string replica_sites_str;
        for (std::size_t i = 0; i < replica_sites.size(); ++i) {
            if (i > 0) {
                replica_sites_str += ",";
            }
            replica_sites_str += replica_sites[i];
        }

        CG_SIM_LOG_INFO(
            "Reactive Data Management: job {} requesting file '{}'; replicas={}, chosen_src_site='{}', "
            "dst_site='{}', decision_mode={}, remote_source_template={} [replica_sites={}]", 
            ctx.job->jobid,
            ctx.filename,
            replica_count,
            decision.chosen_site.empty() ? "<default>" : decision.chosen_site,
            dst_site,
            mode_str,
            remoteSourceTemplateToString(remote_source_template_),
            replica_sites_str);

        return decision;
    }

    void performDataManagementOperations(double current_time) {
        switch (proactive_transfer_kind_) {
        case ProactiveTransferKind::STORAGE_REBALANCE:
            performStorageRebalanceTicks(current_time);
            break;
        case ProactiveTransferKind::NETWORK_AWARE_REBALANCE:
            performNetworkAwareRebalanceTicks(current_time);
            break;
        case ProactiveTransferKind::HOTSET_REPLICATION:
            performHotsetReplicationTicks(current_time);
            break;
        case ProactiveTransferKind::CUSTOM_POLICY_AGENT:
            CG_SIM_LOG_ERROR(
                "Proactive Data Management: template 'custom_policy_agent' not implemented yet.");
            throw std::runtime_error("custom_policy_agent not implemented yet.");
        }
    }

private:
    static ReactiveRemoteTemplate parseRemoteSourceTemplate(const std::string& value) {
        if (value == "least_utilized_source") {
            return ReactiveRemoteTemplate::LEAST_UTILIZED_SOURCE;
        }
        if (value == "most_utilized_source") {
            return ReactiveRemoteTemplate::MOST_UTILIZED_SOURCE;
        }
        if (value == "random_replica") {
            return ReactiveRemoteTemplate::RANDOM_REPLICA;
        }
        if (value == "hash_filename_job") {
            return ReactiveRemoteTemplate::HASH_FILENAME_JOB;
        }
        if (value == "custom_policy_agent") {
            return ReactiveRemoteTemplate::CUSTOM_POLICY_AGENT;
        }
        return ReactiveRemoteTemplate::FIRST_REPLICA;
    }

    static ReactiveRemoteTemplate parseRemoteSourceTemplateFromConfig(const nlohmann::json& value) {
        if (value.is_string()) {
            return parseRemoteSourceTemplate(value.get<std::string>());
        }
        if (value.is_array() && value.size() == 2 && value[0].is_number() &&
            value[1].is_array()) {
            const long long raw = value[0].get<long long>();
            const auto& templates = value[1];
            if (!templates.empty() && raw >= 0) {
                const std::size_t idx = static_cast<std::size_t>(raw);
                if (idx < templates.size() && templates[idx].is_string()) {
                    return parseRemoteSourceTemplate(templates[idx].get<std::string>());
                }
            }
        }
        return ReactiveRemoteTemplate::FIRST_REPLICA;
    }

    static const char* remoteSourceTemplateToString(ReactiveRemoteTemplate t) {
        switch (t) {
        case ReactiveRemoteTemplate::FIRST_REPLICA:
            return "first_replica";
        case ReactiveRemoteTemplate::LEAST_UTILIZED_SOURCE:
            return "least_utilized_source";
        case ReactiveRemoteTemplate::MOST_UTILIZED_SOURCE:
            return "most_utilized_source";
        case ReactiveRemoteTemplate::RANDOM_REPLICA:
            return "random_replica";
        case ReactiveRemoteTemplate::HASH_FILENAME_JOB:
            return "hash_filename_job";
        case ReactiveRemoteTemplate::CUSTOM_POLICY_AGENT:
            return "custom_policy_agent";
        }
        return "first_replica";
    }

    double siteUtilization(const std::string& sitename) const {
        long long capacity = CGSim::FileManager::get_site_capacity(sitename);
        long long remaining =
            static_cast<long long>(CGSim::FileManager::request_remaining_site_storage(sitename));
        if (capacity <= 0) {
            return 1.0;
        }
        return 1.0 - (static_cast<double>(remaining) / static_cast<double>(capacity));
    }

    std::string chooseRemoteSourceSite(const CGSim::FileRequestContext& ctx) {
        if (ctx.replicas.empty()) {
            return "";
        }

        switch (remote_source_template_) {
        case ReactiveRemoteTemplate::FIRST_REPLICA:
            return ctx.replicas.front().sitename;
        case ReactiveRemoteTemplate::LEAST_UTILIZED_SOURCE: {
            double best_util = std::numeric_limits<double>::infinity();
            std::string best_site = ctx.replicas.front().sitename;
            for (const auto& replica : ctx.replicas) {
                const double util = siteUtilization(replica.sitename);
                if (util < best_util) {
                    best_util = util;
                    best_site = replica.sitename;
                }
            }
            return best_site;
        }
        case ReactiveRemoteTemplate::MOST_UTILIZED_SOURCE: {
            double best_util = -1.0;
            std::string best_site = ctx.replicas.front().sitename;
            for (const auto& replica : ctx.replicas) {
                const double util = siteUtilization(replica.sitename);
                if (util > best_util) {
                    best_util = util;
                    best_site = replica.sitename;
                }
            }
            return best_site;
        }
        case ReactiveRemoteTemplate::RANDOM_REPLICA: {
            std::uniform_int_distribution<std::size_t> dist(0, ctx.replicas.size() - 1);
            return ctx.replicas[dist(rng_)].sitename;
        }
        case ReactiveRemoteTemplate::HASH_FILENAME_JOB: {
            const std::string key = ctx.filename + "#" + std::to_string(ctx.job->jobid);
            const std::size_t idx = std::hash<std::string>{}(key) % ctx.replicas.size();
            return ctx.replicas[idx].sitename;
        }
        case ReactiveRemoteTemplate::CUSTOM_POLICY_AGENT:
            CG_SIM_LOG_ERROR(
                "Reactive Data Management: template 'custom_policy_agent' not implemented yet.");
            throw std::runtime_error("custom_policy_agent not implemented yet.");
        }
        return ctx.replicas.front().sitename;
    }

    [[nodiscard]] std::vector<std::pair<std::string, double>> buildSiteUtilization() const {
        std::vector<std::pair<std::string, double>> site_utilization;
        const std::vector<std::string> sites = CGSim::FileManager::get_site_names();
        for (const std::string& sitename : sites) {
            const long long capacity = CGSim::FileManager::get_site_capacity(sitename);
            if (capacity <= 0)
                continue;
            const long long remaining =
                static_cast<long long>(CGSim::FileManager::request_remaining_site_storage(sitename));
            const double utilization =
                1.0 - (static_cast<double>(remaining) / static_cast<double>(capacity));
            site_utilization.emplace_back(sitename, utilization);
        }
        return site_utilization;
    }

    [[nodiscard]] static double estimateTransferSeconds(sg4::Link* link,
                                                        unsigned long long payload_bytes) {
        if (link == nullptr) {
            return std::numeric_limits<double>::infinity();
        }
        const double bw = link->get_bandwidth();
        const double lat = link->get_latency();
        if (bw <= 0.0) {
            return std::numeric_limits<double>::infinity();
        }
        return lat + static_cast<double>(payload_bytes) / bw;
    }

    [[nodiscard]] static std::array<double, 3> networkMetricSortKey(PathMetricMode metric,
                                                                    sg4::Link* link,
                                                                    unsigned long long payload) {
        const double t_est = estimateTransferSeconds(link, payload);
        const double load = (link != nullptr) ? link->get_load() : 1.0;
        const double bw = (link != nullptr) ? link->get_bandwidth() : 0.0;
        switch (metric) {
        case PathMetricMode::ESTIMATED_TRANSFER_TIME:
            return {t_est, load, -bw};
        case PathMetricMode::LINK_LOAD:
            return {load, t_est, -bw};
        case PathMetricMode::BANDWIDTH_ONLY:
            return {-bw, t_est, load};
        }
        return {t_est, load, -bw};
    }

    [[nodiscard]] static bool isSortKeyBetter(const std::array<double, 3>& cand,
                                             const std::array<double, 3>& best) {
        return std::tie(cand[0], cand[1], cand[2]) < std::tie(best[0], best[1], best[2]);
    }

    [[nodiscard]] std::optional<std::pair<std::string, unsigned long long>> pickFileForProactiveTransfer(
        const std::string& src_site,
        const std::string& dst_site,
        FilePickMode mode,
        bool skip_if_dst_has_replica) {

        std::vector<std::string> files_on_site =
            CGSim::FileManager::get_files_on_site(src_site);
        const long long dst_remaining_ll =
            static_cast<long long>(CGSim::FileManager::request_remaining_site_storage(dst_site));
        if (dst_remaining_ll <= 0) {
            return std::nullopt;
        }
        const auto dst_remaining = static_cast<unsigned long long>(dst_remaining_ll);

        if (mode == FilePickMode::FIRST_FIT) {
            for (const std::string& filename : files_on_site) {
                if (skip_if_dst_has_replica && CGSim::FileManager::exists(filename, dst_site))
                    continue;
                if (in_flight_transfers_.count(std::make_pair(filename, src_site)) > 0)
                    continue;
                const unsigned long long size = CGSim::FileManager::request_file_size(filename);
                if (size <= dst_remaining) {
                    return std::make_pair(filename, size);
                }
            }
            return std::nullopt;
        }

        struct Fit {
            std::string name;
            unsigned long long size;
        };
        std::vector<Fit> fitting;
        fitting.reserve(files_on_site.size());
        for (const std::string& filename : files_on_site) {
            if (skip_if_dst_has_replica && CGSim::FileManager::exists(filename, dst_site))
                continue;
            if (in_flight_transfers_.count(std::make_pair(filename, src_site)) > 0)
                continue;
            const unsigned long long size = CGSim::FileManager::request_file_size(filename);
            if (size <= dst_remaining) {
                fitting.push_back({filename, size});
            }
        }
        if (fitting.empty()) {
            return std::nullopt;
        }
        if (mode == FilePickMode::LARGEST_FIT) {
            auto it = std::max_element(
                fitting.begin(), fitting.end(),
                [](const Fit& a, const Fit& b) { return a.size < b.size; });
            return std::make_pair(it->name, it->size);
        }
        if (mode == FilePickMode::SMALLEST_FIT) {
            auto it = std::min_element(
                fitting.begin(), fitting.end(),
                [](const Fit& a, const Fit& b) { return a.size < b.size; });
            return std::make_pair(it->name, it->size);
        }
        std::uniform_int_distribution<std::size_t> dist(0, fitting.size() - 1);
        const std::size_t idx = dist(proactive_rng_);
        return std::make_pair(fitting[idx].name, fitting[idx].size);
    }

    bool initiateProactiveTransfer(const std::string& src_site,
                                   const std::string& dst_site,
                                   const std::string& filename_to_copy,
                                   unsigned long long file_size,
                                   double current_time,
                                   double src_util,
                                   double dst_util,
                                   const char* reason_tag) {
        sg4::Host* src_host = sg4::Engine::get_instance()->host_by_name_or_null(
            src_site + "_communication");
        sg4::Host* dst_host = sg4::Engine::get_instance()->host_by_name_or_null(
            dst_site + "_communication");
        if (!src_host || !dst_host) {
            return false;
        }

        const FileTransferMode mode = data_transfer_mode_;
        const std::string src_site_capture = src_site;
        const std::string filename_capture = filename_to_copy;

        in_flight_transfers_.insert(std::make_pair(filename_to_copy, src_site));

        sg4::CommPtr comm = sg4::Comm::sendto_init()
                                ->set_source(src_host)
                                ->set_destination(dst_host)
                                ->set_payload_size(file_size);
        const bool is_move = (mode == FileTransferMode::MOVE);
        comm->set_name(std::string("DataMgmt_") + (is_move ? "move_" : "copy_") + filename_to_copy +
                       "_from_" + src_site + "_to_" + dst_site);
        comm->on_this_completion_cb(
            [this, filename_capture, file_size, dst_site, src_site_capture, mode](
                simgrid::s4u::Comm const&) {
                CGSim::FileManager::create(filename_capture, file_size, dst_site);
                if (mode == FileTransferMode::MOVE) {
                    CGSim::FileManager::remove(filename_capture, src_site_capture);
                    CG_SIM_LOG_INFO(
                        "Proactive Data Management: completed MOVE of '{}' ({} bytes) from {} to {}",
                        filename_capture, file_size, src_site_capture, dst_site);
                } else {
                    CG_SIM_LOG_INFO(
                        "Proactive Data Management: completed COPY of '{}' ({} bytes) to site {}",
                        filename_capture, file_size, dst_site);
                }
                in_flight_transfers_.erase(std::make_pair(filename_capture, src_site_capture));
            });
        comm->detach();
        CG_SIM_LOG_INFO(
            "Proactive Data Management: initiating {} of '{}' ({}) [{}] from {} (util {:.2f}%) to {} "
            "(util {:.2f}%) at time {}",
            is_move ? "MOVE" : "COPY", filename_to_copy, file_size, reason_tag, src_site,
            src_util * 100.0, dst_site, dst_util * 100.0, current_time);
        return true;
    }

    void performStorageRebalanceTicks(double current_time) {
        const StorageRebalanceParams& p = storage_rebalance_params_;
        int transfers_done = 0;
        while (transfers_done < p.max_transfers_per_tick) {
            const auto site_util = buildSiteUtilization();
            if (site_util.empty()) {
                return;
            }

            std::vector<std::pair<std::string, double>> src_candidates;
            for (const auto& [sitename, u] : site_util) {
                if (u >= p.high_utilization_threshold) {
                    src_candidates.emplace_back(sitename, u);
                }
            }
            std::sort(src_candidates.begin(), src_candidates.end(),
                      [](const auto& a, const auto& b) { return a.second > b.second; });

            bool progressed = false;
            for (const auto& [src_site, src_u] : src_candidates) {
                std::vector<std::pair<std::string, double>> dst_candidates;
                for (const auto& [sitename, u] : site_util) {
                    if (sitename != src_site && u <= p.low_utilization_threshold) {
                        dst_candidates.emplace_back(sitename, u);
                    }
                }
                std::sort(dst_candidates.begin(), dst_candidates.end(),
                          [](const auto& a, const auto& b) { return a.second < b.second; });

                for (const auto& [dst_site, dst_u] : dst_candidates) {
                    const auto picked =
                        pickFileForProactiveTransfer(src_site, dst_site, p.file_pick,
                                                     p.skip_if_already_replica_on_destination);
                    if (!picked.has_value()) {
                        continue;
                    }
                    if (!initiateProactiveTransfer(src_site, dst_site, picked->first, picked->second,
                                                   current_time, src_u, dst_u, "storage_rebalance")) {
                        continue;
                    }
                    transfers_done++;
                    progressed = true;
                    break;
                }
                if (progressed) {
                    break;
                }
            }
            if (!progressed) {
                return;
            }
        }
    }

    void performNetworkAwareRebalanceTicks(double current_time) {
        const NetworkAwareParams& p = network_aware_params_;
        int transfers_done = 0;

        while (transfers_done < p.max_transfers_per_tick) {
            const auto site_util = buildSiteUtilization();
            if (site_util.empty()) {
                return;
            }

            bool found = false;
            std::array<double, 3> best_key{std::numeric_limits<double>::infinity(),
                                           std::numeric_limits<double>::infinity(),
                                           std::numeric_limits<double>::infinity()};
            std::string best_src;
            std::string best_dst;
            std::string best_file;
            unsigned long long best_size = 0;
            double best_src_util = 0.0;
            double best_dst_util = 0.0;

            for (const auto& [src_site, src_u] : site_util) {
                if (src_u < p.high_utilization_threshold) {
                    continue;
                }
                for (const auto& [dst_site, dst_u] : site_util) {
                    if (dst_site == src_site || dst_u > p.low_utilization_threshold) {
                        continue;
                    }
                    sg4::Link* link = linkBetweenSites(src_site, dst_site);
                    if (link == nullptr) {
                        continue;
                    }
                    if (link->get_load() > p.max_path_load) {
                        continue;
                    }
                    const auto picked =
                        pickFileForProactiveTransfer(src_site, dst_site, p.file_pick,
                                                     true);
                    if (!picked.has_value()) {
                        continue;
                    }
                    const std::array<double, 3> key =
                        networkMetricSortKey(p.path_metric, link, picked->second);
                    const bool better = !found || isSortKeyBetter(key, best_key);
                    if (better) {
                        found = true;
                        best_key = key;
                        best_src = src_site;
                        best_dst = dst_site;
                        best_file = picked->first;
                        best_size = picked->second;
                        best_src_util = src_u;
                        best_dst_util = dst_u;
                    }
                }
            }

            if (!found)
                return;
            if (!initiateProactiveTransfer(best_src, best_dst, best_file, best_size, current_time,
                                           best_src_util, best_dst_util,
                                           "network_aware_rebalance")) {
                return;
            }
            transfers_done++;
        }
    }

    void performHotsetReplicationTicks(double current_time) {
        const HotsetReplicationParams& hp = hotset_params_;
        static_cast<void>(hp.hotness_window);
        static_cast<void>(hp.prediction_horizon);
        static_cast<void>(hp.dest_policy);

        int transfers_done = 0;

        while (transfers_done < hp.max_transfers_per_tick) {
            const auto site_util = buildSiteUtilization();
            std::unordered_map<std::string, double> util_map;
            for (const auto& entry : site_util) {
                util_map[entry.first] = entry.second;
            }
            const std::vector<std::string> sites = CGSim::FileManager::get_site_names();
            if (sites.empty()) {
                return;
            }
            const std::size_t n_sites = sites.size();

            std::unordered_set<std::string> uniq_files;
            for (const std::string& s : sites) {
                for (const std::string& f : CGSim::FileManager::get_files_on_site(s)) {
                    uniq_files.insert(f);
                }
            }

            struct HotCand {
                std::string filename;
                int replica_count = 0;
                double prevalence = 0.0;
            };
            std::vector<HotCand> hot_candidates;
            for (const std::string& fname : uniq_files) {
                int replicas = 0;
                for (const std::string& s : sites) {
                    if (CGSim::FileManager::exists(fname, s)) {
                        replicas++;
                    }
                }
                if (replicas >= hp.target_replica_count) {
                    continue;
                }
                const double prevalence = static_cast<double>(replicas) / static_cast<double>(n_sites);
                if (prevalence < hp.hotness_threshold) {
                    continue;
                }
                hot_candidates.push_back(HotCand{fname, replicas, prevalence});
            }
            std::sort(hot_candidates.begin(), hot_candidates.end(),
                      [](const HotCand& a, const HotCand& b) { return a.prevalence > b.prevalence; });

            auto util_lookup = [&](const std::string& site) -> double {
                const auto found = util_map.find(site);
                if (found != util_map.end()) {
                    return found->second;
                }
                const double computed = siteUtilization(site);
                util_map[site] = computed;
                return computed;
            };

            bool progressed = false;
            for (const HotCand& cand : hot_candidates) {
                std::string src_site;
                double src_best_util = -1.0;
                for (const std::string& s : sites) {
                    if (!CGSim::FileManager::exists(cand.filename, s)) {
                        continue;
                    }
                    const double util = util_lookup(s);
                    if (!src_site.empty() && !(util > src_best_util)) {
                        continue;
                    }
                    src_site = s;
                    src_best_util = util;
                }
                if (src_site.empty()) {
                    continue;
                }

                std::vector<std::string> candidate_dst;
                candidate_dst.reserve(sites.size());
                for (const std::string& s : sites) {
                    if (s != src_site && !CGSim::FileManager::exists(cand.filename, s)) {
                        candidate_dst.push_back(s);
                    }
                }
                std::sort(candidate_dst.begin(), candidate_dst.end(),
                          [&](const std::string& a, const std::string& b) {
                              return util_lookup(a) < util_lookup(b);
                          });

                std::string dst_site;
                double dst_best_util = 0.0;
                const unsigned long long fsize =
                    CGSim::FileManager::request_file_size(cand.filename);
                for (const std::string& s : candidate_dst) {
                    const long long remaining =
                        static_cast<long long>(CGSim::FileManager::request_remaining_site_storage(s));
                    if (remaining < static_cast<long long>(fsize)) {
                        continue;
                    }
                    dst_site = s;
                    dst_best_util = util_lookup(s);
                    break;
                }
                if (dst_site.empty()) {
                    continue;
                }

                if (in_flight_transfers_.count(std::make_pair(cand.filename, src_site)) > 0) {
                    continue;
                }

                if (!initiateProactiveTransfer(src_site, dst_site, cand.filename, fsize, current_time,
                                               src_best_util, dst_best_util, "hotset_replication")) {
                    continue;
                }
                transfers_done++;
                progressed = true;
                break;
            }
            if (!progressed) {
                return;
            }
        }
    }

    void scheduleNext(double current_time) {
        if (!proactive_enabled_ || interval_ <= 0.0) {
            return;
        }
        double next_time = current_time + interval_;
        current_timer_ = timer::Timer::set(next_time, [this]() {
            execution_count_++;
            performDataManagementOperations(sg4::Engine::get_clock());
            scheduleNext(sg4::Engine::get_clock());
        });
    }

    bool proactive_enabled_ = false;
    bool reactive_enabled_ = false;
    bool prefer_local_replica_ = true;
    std::size_t copy_to_move_threshold_ = 3;
    ReactiveRemoteTemplate remote_source_template_ = ReactiveRemoteTemplate::FIRST_REPLICA;
    unsigned int random_seed_ = 1337;
    std::mt19937 rng_{1337};
    double interval_ = -1.0;
    ProactiveTransferKind proactive_transfer_kind_ = ProactiveTransferKind::STORAGE_REBALANCE;
    StorageRebalanceParams storage_rebalance_params_{};
    NetworkAwareParams network_aware_params_{};
    HotsetReplicationParams hotset_params_{};
    std::mt19937 proactive_rng_{1337};
    FileTransferMode data_transfer_mode_ = FileTransferMode::COPY;
    timer::Timer* current_timer_ = nullptr;
    unsigned long execution_count_ = 0;
    std::set<std::pair<std::string, std::string>> in_flight_transfers_;
};

} // anonymous namespace

namespace CGSim {

bool DataManagementPolicy::enabled_ = false;
unsigned long DataManagementPolicy::execution_count_ = 0;
std::unique_ptr<DataManagementPlugin> DataManagementPolicy::plugin_;

void DataManagementPolicy::configure(const nlohmann::json& cfg) {
    bool enabled = false;
    if (cfg.contains("enabled")) {
        enabled = cfg["enabled"].get<bool>();
    }

    if (!plugin_) {
        plugin_ = std::make_unique<ProactiveDataManagementPlugin>();
    }
    if (plugin_) {
        plugin_->configure(cfg);
    }
    enabled_ = enabled;
}

void DataManagementPolicy::onSimulationStart() {
    if (!enabled_ || !plugin_) {
        return;
    }
    plugin_->onSimulationStart();
}

void DataManagementPolicy::onSimulationEnd() {
    if (!plugin_) {
        return;
    }
    plugin_->onSimulationEnd();
}

bool DataManagementPolicy::isEnabled() {
    return enabled_ && static_cast<bool>(plugin_);
}

void DataManagementPolicy::setEnabled(bool enabled) {
    enabled_ = enabled;
}

FileRequestDecision DataManagementPolicy::onFileRequest(const FileRequestContext& ctx) {
    if (!plugin_) {
        return {};
    }
    return plugin_->onFileRequest(ctx);
}

void DataManagementPolicy::onTimerTick(double current_time) {
    if (!plugin_) {
        return;
    }
    execution_count_++;
    plugin_->onTimerTick(current_time);
}

unsigned long DataManagementPolicy::getExecutionCount() {
    return execution_count_;
}

} // namespace CGSim
