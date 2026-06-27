#ifndef WORKLOAD_MANAGER_H
#define WORKLOAD_MANAGER_H

#include "CGSim.h"
#include <fstream>
#include <sstream>

class WORKLOAD_MANAGER {

public:
    WORKLOAD_MANAGER(){};
   ~WORKLOAD_MANAGER(){};
    JobQueue getWorkload();

private:
   long long random_number(long long min, long long max);
   
};


#endif //WORKLOAD_MANAGER_H
