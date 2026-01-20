// logger.h
#pragma once
#include <memory>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace CGSim::logger {

    inline std::shared_ptr<spdlog::logger> logger_instance;

    // Initializes the logger. Must be called at the beginning of main().
    inline void init() {

        std::filesystem::create_directories("logs");
        logger_instance = spdlog::basic_logger_mt("atlas_grid_simulation", "logs/atlas_grid_simulation.log", true);
        spdlog::set_default_logger(logger_instance);
        spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e [%l] %v");
        spdlog::set_level(spdlog::level::debug);
        spdlog::flush_on(spdlog::level::debug);
    }

    // Returns a reference to the global logger instance.
    inline std::shared_ptr<spdlog::logger>& getLogger() {
        return logger_instance;
    }
}

#define CG_SIM_LOG_TRACE(fmt, ...)    CGSim::logger::getLogger()->trace("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define CG_SIM_LOG_DEBUG(fmt, ...)    CGSim::logger::getLogger()->debug("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define CG_SIM_LOG_INFO(fmt, ...)     CGSim::logger::getLogger()->info("" fmt, ##__VA_ARGS__)
#define CG_SIM_LOG_WARN(fmt, ...)     CGSim::logger::getLogger()->warn("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define CG_SIM_LOG_ERROR(fmt, ...)    CGSim::logger::getLogger()->error("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define CG_SIM_LOG_CRITICAL(fmt, ...) CGSim::logger::getLogger()->critical("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
