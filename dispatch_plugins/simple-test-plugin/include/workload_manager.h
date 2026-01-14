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

   std::vector<std::string> parseCSVLine(const std::string& line);
   std::string getColumn(const std::vector<std::string>& row,
                       const std::unordered_map<std::string,int>& column_map,
                       const std::string& key,
                       const std::string& default_val = "");
};


#endif //WORKLOAD_MANAGER_H
