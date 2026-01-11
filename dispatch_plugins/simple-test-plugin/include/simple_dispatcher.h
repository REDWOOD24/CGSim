#ifndef SIMPLE_DISPATCHER_H
#define SIMPLE_DISPATCHER_H

#include <map>
#include <set>
#include <iostream>
#include <fstream>
#include <string>
#include <math.h>
#include <simgrid/s4u.hpp>
#include <iomanip>
#include "job.h"
#include "host_extensions.h"

namespace sg4 = simgrid::s4u;


//Need basic characterization of sites, hosts and disks to find the optimal one for each job.
struct Disk {
  std::string                                    name{};
  std::string                                    mount{};
  double                                         read_bw{};
  double                                         write_bw{};
};

struct Host {
  std::string                                    name{};
  double                                         speed{};
  int                                            cores{};
  int                                            cores_available{};
  std::vector<Disk*>                             disks{};
  std::unordered_map<std::string, Disk*>         disks_map{};
  std::unordered_set<long long>                  jobs{};
  bool operator<(const Host& other) const {return cores_available <= other.cores_available;}
};

struct Site {
  std::string                                    name{};
  int                                            priority{};
  std::vector<Host*>                             cpus{};
  std::unordered_map<std::string, Host*>         cpus_map{};
  int                                            cpus_in_use{};
  long long                                      gflops{};
  long long                                      storage{};
  bool operator<(const Site& other) const {return priority <= other.priority;}
};



class SIMPLE_DISPATCHER
{

public:
  SIMPLE_DISPATCHER(){};
 ~SIMPLE_DISPATCHER(){};

  //Assign Workload
  JobQueue getJobs(long max_jobs);

  //Resource Management
  void setPlatform(sg4::NetZone* platform);

  //Functions needed to specify hosts for jobs
  double calculateWeightedScore(Host* cpu, Job* j, std::string& best_disk_name);
  double getTotalSize(const std::unordered_map<std::string, size_t>& files);
  Host*  findBestAvailableCPU(std::vector<Host*>& cpus, Job* j);
  Job*   assignJobToResource(Job* job);
  void   free(Job* job);
  void   printJobInfo(Job* job);
  void   cleanup();
  Site*  findSiteByName(std::vector<Site*>& sites, const std::string& site_name);


private:
  int                                            current_site_index{0};
  bool                                           use_round_robin{false};
  std::vector<Site*>                             _sites{};
  std::unordered_map<std::string, Site*>         _sites_map{};
  sg4::NetZone*                                  platform;

  const std::unordered_map<std::string, double>  weights =
      {
        {"speed", 1.0},
        {"cores", 1.0},
        {"disk", 1.0},
        {"disk_storage", 1.0},
        {"disk_read_bw", 1.0},
        {"disk_write_bw", 1.0}
      };
};

#endif
