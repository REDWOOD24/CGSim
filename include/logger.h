// logger.h
#pragma once

#include <memory>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace logger {

    inline std::shared_ptr<spdlog::logger> logger_instance;

    // Initializes the logger. Must be called at the beginning of main().
    inline void init() {

        std::filesystem::create_directories("logs");
        logger_instance = spdlog::basic_logger_mt("atlas_grid_simulation", "logs/atlas_grid_simulation.log", true);
        spdlog::set_default_logger(logger_instance);
        spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e [%l] %v");  
        spdlog::set_level(spdlog::level::info); // Set default log level to debug        
        spdlog::flush_on(spdlog::level::info); // Flush on critical level
    }

    // Returns a reference to the global logger instance.
    inline std::shared_ptr<spdlog::logger>& getLogger() {
        return logger_instance;
    }
}


#define LOG_TRACE(fmt, ...)    logger::getLogger()->trace("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...)    logger::getLogger()->debug("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)     logger::getLogger()->info("" fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)     logger::getLogger()->warn("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...)    logger::getLogger()->error("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOG_CRITICAL(fmt, ...) logger::getLogger()->critical("[{}:{} {}] " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)