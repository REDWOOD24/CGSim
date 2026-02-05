#ifndef LOCAL_SCHEDULER_H
#define LOCAL_SCHEDULER_H

#include <string>
#include <random>
#include <vector>

#include <simgrid/s4u.hpp>

#include "job.h"
#include "host_extensions.h"
#include "file_manager.h"
#include "output.h"

namespace sg4 = simgrid::s4u;

class LOCAL_SCHEDULER {
public:
  explicit LOCAL_SCHEDULER(std::string defaultAlgorithm = "random");

  void assignJob(Job* job, const std::string& schedulerAlgorithm = "random");

private:
  std::string defaultAlgorithm_;
  std::mt19937_64 rng_;

  sg4::NetZone* getRoot_() const;
  sg4::NetZone* findSite_(sg4::NetZone* root, const std::string& siteName) const;

  bool isComputeHost_(sg4::Host* h) const;
  bool hasResources_(sg4::Host* cpu, const Job* j) const;

  sg4::Disk* pickDisk_(sg4::Host* cpu) const;

  sg4::Host* pickRandomCpuInSite_(sg4::NetZone* site, const Job* j);
  void applyAssignment_(Job* job, sg4::NetZone* site, sg4::Host* cpu);
};

#endif // LOCAL_SCHEDULER_H
