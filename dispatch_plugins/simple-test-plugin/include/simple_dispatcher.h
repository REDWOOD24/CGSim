#ifndef SIMPLE_DISPATCHER_H
#define SIMPLE_DISPATCHER_H

#include <map>
#include <iostream>
#include <string>
#include "CGSim.h"

class SIMPLE_DISPATCHER
{

public:
  SIMPLE_DISPATCHER(){};
 ~SIMPLE_DISPATCHER(){};

  double      storage_needed(std::unordered_map<std::string, long long>& files);
  sg4::Host*  findAvailableCPU(const std::vector<sg4::Host*>& cpus, Job* j);
  Job*        assignJob(Job* job);

private:
  sg4::NetZone* platform = sg4::Engine::get_instance()->get_netzone_root();
};

#endif
