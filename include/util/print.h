#pragma once
#include <iostream>
#include <iomanip>
#include <string>
#include "site_manager.h"
#include "version.h"
#include <sstream>
#include <algorithm>

namespace CGSim {

namespace Utilities {

inline std::string get_grid_name()
{
    return CGSim::GlobalManagers::get_site_manager()->get_custom_parameter("Grid Name");
}

inline void print_CGSim_Logo()
{
    constexpr int WIDTH = 62;

    std::ostringstream title;
    title << "Computing Grid Simulator  [v" << MAJOR_VERSION << "." << MINOR_VERSION << "]";

    const int titlePad = std::max(0, (WIDTH - static_cast<int>(title.str().size())) / 2);

    std::cout
        << "\033[1;36m"
        << R"(
           ██████╗  ██████╗ ███████╗██╗███╗   ███╗
          ██╔════╝ ██╔════╝ ██╔════╝██║████╗ ████║
          ██║      ██║  ███╗███████╗██║██╔████╔██║
          ██║      ██║   ██║╚════██║██║██║╚██╔╝██║
          ╚██████╗ ╚██████╔╝███████║██║██║ ╚═╝ ██║
           ╚═════╝  ╚═════╝ ╚══════╝╚═╝╚═╝     ╚═╝
)"
        << "\033[0m\n"
        << "\033[90m"
        << "══════════════════════════════════════════════════════════════\n"
        << "\033[0m"
        << std::string(titlePad, ' ')
        << "\033[1;37mComputing Grid Simulator\033[0m  "
        << "\033[90m[v\033[1;33m"
        << MAJOR_VERSION << "." << MINOR_VERSION
        << "\033[90m]\033[0m\n"
        << "\033[90m"
        << "══════════════════════════════════════════════════════════════\n"
        << "\033[0m\n";
}

inline void print_site(const std::string& siteName)
{
    static bool headerPrinted = false;
    constexpr int INNER_WIDTH = 60, SITE_WIDTH = 37;

    if (!headerPrinted)
    {
        headerPrinted = true;

        const std::string gridName = get_grid_name();
        const int left  = std::max(0, (INNER_WIDTH - static_cast<int>(gridName.size())) / 2);
        const int right = std::max(0, INNER_WIDTH - static_cast<int>(gridName.size()) - left);

        std::cout
            << "\n\033[1;36m"
            << "╔════════════════════════════════════════════════════════════╗\n"
            << "║" << std::string(left, ' ')
            << "\033[1;37m" << gridName << "\033[1;36m"
            << std::string(right, ' ') << "║\n"
            << "╠═══════════════════════════════════════╦════════════════════╣\n"
            << "\033[0m";
    }

    std::cout
        << "\033[1;36m║\033[0m "
        << "\033[1;37m" << std::left << std::setw(SITE_WIDTH) << siteName
        << "\033[0m "
        << "\033[1;36m║\033[0m "
        << "\033[1;32m● REGISTERED\033[0m"
        << std::string(7, ' ')
        << "\033[1;36m║\033[0m\n"
        << std::right;
}


inline void printSimulationDashBoard(std::size_t dispatchedJobs, std::size_t totalJobs,
                                     std::size_t activatedJobs, std::size_t finishedJobs,
                                     std::size_t pendingGlobalJobs, std::size_t pendingSiteJobs,
                                     std::size_t pendingActivities, double simulatedTime,
                                     double gridCpuUsage)
{
    static bool first = true;
    constexpr int BAR = 27, LINES = 16, LABEL_WIDTH = 16, BAR_LABEL_WIDTH = 16;

    auto s = static_cast<unsigned long long>(simulatedTime);
    auto d = s / 86400; s %= 86400;
    auto h = s / 3600;  s %= 3600;
    auto m = s / 60;    auto sec = s % 60;

    std::ostringstream t;
    if (d) t << d << "d ";
    t << std::setfill('0')
    << std::setw(2) << h << "h "
    << std::setw(2) << m << "m "
    << std::setw(2) << sec << "s";

    double p = totalJobs ? static_cast<double>(dispatchedJobs) / totalJobs : 0.0;
    p = std::clamp(p, 0.0, 1.0);

    const double cpu = std::clamp(gridCpuUsage * 100.0, 0.0, 100.0);
    const int jobFilled = static_cast<int>(p * BAR);
    const int cpuFilled = static_cast<int>((cpu / 100.0) * BAR);

    if (!first) std::cout << "\033[" << LINES << "A";
    first = false;

    auto clear = [] { std::cout << "\033[2K\r"; };
    auto border = [] { std::cout << "\033[62G\033[1;36m│\033[0m\n"; };

    clear(); std::cout << "\033[1;36m╭────────────────────────────────────────────────────────────╮\033[0m\n";
    clear(); std::cout << "\033[1;36m│                                                            │\033[0m\n";

    clear();
    std::cout << "\033[1;36m│\033[0m  \033[90m"
            << std::left << std::setw(BAR_LABEL_WIDTH) << "JOB PROGRESS"
            << "\033[0m[";

    for (int i = 0; i < BAR; ++i)
        std::cout << (i < jobFilled ? "\033[1;32m█\033[0m" : "\033[90m░\033[0m");

    std::cout << "] \033[1;37m"
            << std::right << std::fixed << std::setprecision(1)
            << std::setw(5) << p * 100.0
            << "%\033[0m";
    border();

    clear();
    std::cout << "\033[1;36m│\033[0m  "
            << std::left << std::setw(BAR_LABEL_WIDTH) << ""
            << "\033[1;37m"
            << dispatchedJobs
            << "\033[90m / \033[1;37m"
            << totalJobs
            << "\033[0m dispatched";
    border();

    clear(); std::cout << "\033[1;36m│                                                            │\033[0m\n";

    clear(); std::cout << "\033[1;36m│\033[0m  \033[1;32m●\033[0m  " << std::left << std::setw(LABEL_WIDTH) << "Running"       << "\033[1;37m" << activatedJobs     << "\033[0m"; border();
    clear(); std::cout << "\033[1;36m│\033[0m  \033[1;32m✓\033[0m  " << std::left << std::setw(LABEL_WIDTH) << "Finished"      << "\033[1;37m" << finishedJobs      << "\033[0m"; border();
    clear(); std::cout << "\033[1;36m│\033[0m  \033[1;33m◇\033[0m  " << std::left << std::setw(LABEL_WIDTH) << "Global Queue"  << "\033[1;37m" << pendingGlobalJobs << "\033[0m"; border();
    clear(); std::cout << "\033[1;36m│\033[0m  \033[1;33m◇\033[0m  " << std::left << std::setw(LABEL_WIDTH) << "Site Queues"    << "\033[1;37m" << pendingSiteJobs   << "\033[0m"; border();
    clear(); std::cout << "\033[1;36m│\033[0m  \033[1;36m◆\033[0m  " << std::left << std::setw(LABEL_WIDTH) << "Activities"    << "\033[1;37m" << pendingActivities << "\033[0m"; border();

    clear(); std::cout << "\033[1;36m│                                                            │\033[0m\n";

    clear();
    std::cout << "\033[1;36m│\033[0m  \033[1;36m◆\033[0m  "
            << std::left << std::setw(LABEL_WIDTH) << "Simulated Time"
            << "\033[1;37m" << t.str()
            << "\033[0m";
    border();

    clear(); std::cout << "\033[1;36m│                                                            │\033[0m\n";

    clear();
    std::cout << "\033[1;36m│\033[0m  \033[90m"
            << std::left << std::setw(BAR_LABEL_WIDTH) << "GRID CPU UTIL"
            << "\033[0m[";

    for (int i = 0; i < BAR; ++i)
        std::cout << (i < cpuFilled ? "\033[1;35m█\033[0m" : "\033[90m░\033[0m");

    std::cout << "] \033[1;35m"
            << std::right << std::fixed << std::setprecision(1)
            << std::setw(5) << cpu
            << "%\033[0m";
    border();

    clear(); std::cout << "\033[1;36m│                                                            │\033[0m\n";
    clear(); std::cout << "\033[1;36m╰────────────────────────────────────────────────────────────╯\033[0m\n"
                    << std::right << std::flush;
}

}

}