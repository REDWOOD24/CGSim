// rl_dispatcher.h  (CGSim-Refactor compatible, Python chooses SITE only)
#ifndef RL_DISPATCHER_H
#define RL_DISPATCHER_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <queue>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <simgrid/s4u.hpp>

#include "cnpy.h"
#include "job.h"

namespace sg4 = simgrid::s4u;

// ---------------- Resource structs (match refactor toy model) ----------------
struct Disk {
  std::string name{};
  std::string mount{};
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
  long long                              storage{};
  bool operator<(const Site& other) const { return priority <= other.priority; }
};

// ---------------- RL Dispatcher core ----------------
class RL_DISPATCHER
{
public:
  RL_DISPATCHER()  = default;
  ~RL_DISPATCHER() { cleanup(); disconnectPython(); }

  // ---------------- Workload loader ----------------
  // Same pattern as SIMPLE_DISPATCHER::getJobs(), but without hardcoded /Users/... paths.
  //
  // By default we try:
  //   1) $CGSIM_JOB_CSV (if set)
  //   2) ./new_data/mimic_job.csv
  //   3) ./new_data/jobs.csv
  JobQueue getJobs(long max_jobs);

  // Platform
  void setPlatform(sg4::NetZone* platform);
  sg4::NetZone* getPlatform() const { return platform_; }

  // Assignment
  Job* assignJobToResource(Job* job);
  void free(Job* job);
  void cleanup();

  // -------- feature collectors used by Python protocol --------
  // shape [S][maxC], padded with 0
  std::vector<std::vector<int>>    collectTotalCores() const;
  std::vector<std::vector<int>>    collectAvailableCores() const;
  std::vector<std::vector<double>> collectCoreSpeeds() const;

  // shape [1][4] for one job: [core_count, no_of_inp_files, flops, inp_file_bytes]
  std::array<double, 4> collectOneJobFeatures(const Job* job, const Site* site_for_flops) const;

  // -------- Python comms (client) --------
  void connectPython(const std::string& host = "127.0.0.1", int port = 5555);
  void disconnectPython();
  bool isPythonConnected() const { return py_sock_ >= 0; }

  // Python chooses SITE ONLY
  // Protocol (matches your Python):
  //   send "SBMT" -> recv "WAIT"
  //   send arrays, recv "CNFM" after each
  //   send "WAIT"
  //   recv decision (onehot site)
  bool chooseSiteWithPython(Job* job, Site*& out_site);

private:
  // deterministic ordering helpers
  std::vector<Site*>        getSitesOrderedByName_() const;
  static std::vector<Host*> getCpusOrderedByName_(const Site* site);
  std::size_t               getMaxCpuCount_() const;

  // choose random feasible cpu within site
  Host* pickRandomCpuInSite_(Site* site, int required_cores);

  // low-level socket I/O
  static void     recv_all_(int sock, void* out, size_t n);
  static void     send_all_(int sock, const void* data, size_t n);
  static uint64_t recv_u64_be_(int sock);
  static void     send_u64_be_(int sock, uint64_t x);

  // messages (framed)
  std::string receiveMessage_(int sock) const;
  void        sendMessage_(int sock, const std::string& msg) const;

  // arrays: u64 len + npy bytes (header+raw)
  template <typename T>
  void sendData_(int sock, const T* data, const std::vector<size_t>& shape) const;

  cnpy::NpyArray receiveData_(int sock, const std::string& tmp_path = "/tmp/incoming.npy") const;

  template <typename T>
  static std::vector<T> flatten2D_(const std::vector<std::vector<T>>& a);

  // decode site-only onehot
  static bool decodeSiteDecision_(const cnpy::NpyArray& decision, size_t S, size_t& out_site_i);

private:
  sg4::NetZone*                          platform_{nullptr};
  std::vector<Site*>                     sites_{};
  std::unordered_map<std::string, Site*> sites_map_{};

  const std::unordered_map<std::string, double> weights_ = {
      {"speed", 1.0},
      {"cores", 1.0},
  };

  int py_sock_{-1};

  mutable std::mt19937 rng_{std::random_device{}()};
};

// ---------------- template impl ----------------
template <typename T>
inline void RL_DISPATCHER::sendData_(int sock, const T* data, const std::vector<size_t>& shape) const
{
  std::vector<char> header = cnpy::create_npy_header<T>(shape);

  size_t nels = 1;
  for (auto s : shape) nels *= s;

  const uint64_t total = (uint64_t)header.size() + (uint64_t)(nels * sizeof(T));
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
  for (const auto& row : a) out.insert(out.end(), row.begin(), row.end());
  return out;
}

#endif // RL_DISPATCHER_H
