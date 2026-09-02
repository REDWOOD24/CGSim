#pragma once
#include <chrono>

int main(int argc, char** argv);

namespace CGSim {

namespace Utilities {

class Statistics {
public:
    using Clock = std::chrono::steady_clock;
    static unsigned long get_pending_activities_size();
    static double get_current_system_time();
    static double get_previous_recorded_system_time();

    
private:
    inline static Clock::time_point start_ = Clock::now();
    inline static double previous_time_ = 0.0;
    static void start() {start_ = Clock::now();}

    friend int ::main(int argc, char** argv);

};

}

}
