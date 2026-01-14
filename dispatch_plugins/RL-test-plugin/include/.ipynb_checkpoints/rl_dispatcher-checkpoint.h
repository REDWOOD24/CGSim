// rl_dispatcher.h
#ifndef RL_DISPATCHER_H
#define RL_DISPATCHER_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <simgrid/s4u.hpp>

#include "cnpy.h"
#include "fsmod.hpp"
#include "host_extensions.h"
#include "job.h"
#include "logger.h"

namespace sg4 = simgrid::s4u;

// Need basic characterization of sites, hosts and disks to find the optimal one for each job.
struct Disk {
  std::string name{};
  std::string mount{};
  size_t      storage{};
  double      read_bw{};
  double      write_bw{};
};

struct Host {
  std::string                            name{};
  double                                 speed{};
  int                                    cores{};
  int                                    cores_available{};
  std::vector<Disk*>                     disks{};
  std::unordered_map<std::string, Disk*> disks_map{};
  std::unordered_set<long long>          jobs{};
  bool operator<(const Host& other) const { return cores_available <= other.cores_available; }
};

struct Site {
  std::string                            name{};
  int                                    priority{};
  std::vector<Host*>                     cpus{};
  std::unordered_map<std::string, Host*> cpus_map{};
  int                                    cpus_in_use{};
  long long                              gflops{};
  bool operator<(const Site& other) const { return priority <= other.priority; }
};

class RL_DISPATCHER
{
public:
  RL_DISPATCHER() = default;
  ~RL_DISPATCHER() = default;

  // Resource Management
  void setPlatform(sg4::NetZone* platform);
  virtual sg4::NetZone* getPlatform();

  // Functions needed to specify hosts for jobs
  double calculateWeightedScore(Host* cpu, Job* j, std::string& best_disk_name);
  double getTotalSize(const std::unordered_map<std::string, size_t>& files);
  Host*  findBestAvailableCPU(std::vector<Host*>& cpus, Job* j);

  // This is the one we will make Python-driven
  Job*   assignJobToResource(Job* job);

  void   free(Job* job);
  void   printJobInfo(Job* job);
  void   cleanup();
  Site*  findSiteByName(std::vector<Site*>& sites, const std::string& site_name);

  // ---------------- Feature extraction (site) ----------------
  // Arrays: shape [num_sites][max_num_cpus], padded with 0
  std::vector<std::vector<double>> collectCoreSpeeds() const;
  std::vector<std::vector<int>>    collectTotalCores() const;
  std::vector<std::vector<int>>    collectAvailableCores() const;

  // ---------------- Feature extraction (jobs) ----------------
  // Matrix: shape [num_jobs][4], columns:
  // [core_count, no_of_inp_files, flops, inp_file_bytes]
  std::vector<std::vector<double>> collectJobFeatures(const std::vector<Job*>& jobs) const;

  // Optional: ordering helpers
  std::vector<std::string>              collectSiteNames() const;
  std::vector<std::vector<std::string>> collectCpuNamesPadded() const;

  // ---------------- Python communication (client) ----------------
  // Python is the server (your script binds and accepts), C++ connects.
  void connectPython(const std::string& host = "127.0.0.1", int port = 5555);
  void disconnectPython();
  bool isPythonConnected() const { return py_sock_ >= 0; }

  // One-job RPC: sends current grid + job features, receives decision matrix
  bool chooseWithPython(Job* job, Site*& out_site, Host*& out_cpu);

  // String messages
  std::string receiveMessage(int sock) const;
  void        sendMessage(int sock, const std::string& msg) const;

  // NPY arrays
  template <typename T>
  void sendData(int sock, const T* data, const std::vector<size_t>& shape) const;

  cnpy::NpyArray receiveData(int sock, const std::string& tmp_path = "/tmp/incoming.npy") const;

private:
  // deterministic ordering + max cpu helpers
  std::vector<Site*>        getSitesOrderedByName_() const;
  static std::vector<Host*> getCpusOrderedByName_(const Site* site);
  std::size_t               getMaxCpuCount_() const;

  // low-level I/O helpers
  static void     recv_all_(int sock, void* out, size_t n);
  static void     send_all_(int sock, const void* data, size_t n);
  static uint64_t recv_u64_be_(int sock);
  static void     send_u64_be_(int sock, uint64_t x);

  // flatten helper for sending contiguous memory
  template <typename T>
  static std::vector<T> flatten2D_(const std::vector<std::vector<T>>& a);

  // decode returned matrix -> (site_i, cpu_i)
  static bool decodeDecision_(const cnpy::NpyArray& decision,
                             size_t S, size_t maxC,
                             size_t& out_site_i, size_t& out_cpu_i);

private:
  int                                     current_site_index{0};
  bool                                    use_round_robin{false};
  std::vector<Site*>                      _sites{};
  std::unordered_map<std::string, Site*>  _sites_map{};
  sg4::NetZone*                           platform{nullptr};

  // Used in setPlatform() (you already reference this->weights)
  std::unordered_map<std::string, double> weights{{"speed", 1.0}, {"cores", 1.0}};

  // Python socket (client side)
  int py_sock_{-1};
};

// ---- template implementation must be in header ----
template <typename T>
inline void RL_DISPATCHER::sendData(int sock, const T* data, const std::vector<size_t>& shape) const
{
  std::vector<char> header = cnpy::create_npy_header<T>(shape);

  size_t nels = 1;
  for (auto s : shape) nels *= s;

  uint64_t total = (uint64_t)header.size() + (uint64_t)(nels * sizeof(T));
  send_u64_be_(sock, total);

  send_all_(sock, header.data(), header.size());
  if (nels) send_all_(sock, data, nels * sizeof(T));
}

template <typename T>
inline std::vector<T> RL_DISPATCHER::flatten2D_(const std::vector<std::vector<T>>& a)
{
  if (a.empty()) return {};
  std::vector<T> out;
  out.reserve(a.size() * a[0].size());
  for (const auto& row : a) {
    out.insert(out.end(), row.begin(), row.end());
  }
  return out;
}

#endif // RL_DISPATCHER_H
