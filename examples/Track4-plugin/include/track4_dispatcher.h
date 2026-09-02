#ifndef TRACK4_DISPATCHER_H
#define TRACK4_DISPATCHER_H

#include <map>
#include <iostream>
#include <string>
#include "CGSim.h"

class TRACK4_DISPATCHER
{

public:
  TRACK4_DISPATCHER(){};
 ~TRACK4_DISPATCHER(){};

  double      storage_needed(std::unordered_map<std::string, long long>& files);
  sg4::Host*  findAvailableCPU(const std::vector<sg4::Host*>& cpus, Job* j);
  void        assignJob(Job* job);

private:
  sg4::NetZone* platform = sg4::Engine::get_instance()->get_netzone_root();
};

#endif