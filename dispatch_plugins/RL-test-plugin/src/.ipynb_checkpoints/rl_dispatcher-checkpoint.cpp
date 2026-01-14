#include "rl_dispatcher.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

// socket client (C++ connects to Python server)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(__linux__)
  #include <sys/syscall.h>
  #ifndef MFD_CLOEXEC
    #define MFD_CLOEXEC 0x0001U
  #endif
#endif

// ---------------- Platform ----------------

sg4::NetZone* RL_DISPATCHER::getPlatform()
{
  return this->platform;
}

void RL_DISPATCHER::setPlatform(sg4::NetZone* platform)
{
  std::priority_queue<Site*> site_queue;
  this->platform = platform;
  auto all_sites = platform->get_children();
  for(const auto& site: all_sites)
  {
    if(site->get_name() == std::string("JOB-SERVER")) continue; //No computation on Job server
    Site* _site = new Site;
    _site->name = site->get_cname();

    const char* gflops_str = site->get_property("gflops");
    if (gflops_str) {
      try {
        _site->gflops = std::stol(gflops_str);
      } catch (const std::exception& e) {
        LOG_ERROR("Error: Failed to convert 'gflops' to integer. Exception: {}", e.what());
      }
    }

    for(const auto& host: site->get_all_hosts())
    {
      Host* cpu = new Host;
      cpu->name            = host->get_cname();
      cpu->cores           = host->get_core_count();
      cpu->speed           = host->get_speed();
      cpu->cores_available = host->get_core_count();

      for(const auto& disk: host->get_disks())
      {
        Disk* d     = new Disk;
        d->name     = disk->get_cname();
        d->mount    = disk->get_property("mount");
        d->storage  = (simgrid::fsmod::FileSystem::get_file_systems_by_netzone(site)
                         .at(_site->name + cpu->name + d->name + "filesystem")
                         ->get_free_space_at_path(d->mount)) / 1000;
        d->read_bw  = disk->get_read_bandwidth();
        d->write_bw = disk->get_write_bandwidth();

        cpu->disks.push_back(d);
        cpu->disks_map[d->name] = d;
      }

      _site->cpus.push_back(cpu);
      _site->cpus_map[cpu->name] = cpu;

      // Site priority is determined by quality of cpus available
      _site->priority += (int)std::round(cpu->speed/1e8 * this->weights.at("speed") +
                                         cpu->cores * this->weights.at("cores"));
    }

    _site->priority    = (int)std::round((double)_site->priority / (double)_site->cpus.size()); //Normalize
    _site->cpus_in_use = 0;

    site_queue.push(_site);
    _sites_map[_site->name] = _site;
  }

  while (!site_queue.empty()) { _sites.push_back(site_queue.top()); site_queue.pop(); }
}

double RL_DISPATCHER::getTotalSize(const std::unordered_map<std::string, size_t>& files)
{
  size_t total_size = 0;
  for (const auto& file : files) total_size += file.second;
  return (double)total_size;
}

// ---------------- Resource free/cleanup ----------------

void RL_DISPATCHER::free(Job* job)
{
  Host* cpu = _sites_map.at(job->comp_site)->cpus_map.at(job->comp_host);
  if(cpu->jobs.count(job->jobid) > 0)
  {
    Disk* disk            = cpu->disks_map.at(job->disk);
    cpu->cores_available += job->cores;
    disk->storage        += (this->getTotalSize(job->input_files) + this->getTotalSize(job->output_files));
    cpu->jobs.erase(job->jobid);
    LOG_DEBUG("Job {} freed from CPU {}", job->jobid, cpu->name);
  }
}

Site* RL_DISPATCHER::findSiteByName(std::vector<Site*>& sites, const std::string& site_name)
{
  auto it = std::find_if(sites.begin(), sites.end(),
                         [&site_name](Site* site) { return site->name == site_name; });
  return it != sites.end() ? *it : nullptr;
}

void RL_DISPATCHER::printJobInfo(Job* job)
{
  LOG_DEBUG("----------------------------------------------------------------------");
  LOG_INFO("Submitting .. {}", job->jobid);
  LOG_DEBUG("FLOPs to be executed: {}", job->flops);
  LOG_DEBUG("Files to be read:");
  for (const auto& file : job->input_files)
    LOG_DEBUG("File: {:<40} Size: {:>10}", file.first, file.second);
  LOG_DEBUG("Files to be written:");
  for (const auto& file : job->output_files)
    LOG_DEBUG("File: {:<40} Size: {:>10}", file.first, file.second);
  LOG_DEBUG("Cores Used: {}", job->cores);
  LOG_DEBUG("Disk Used: {}", job->disk);
  LOG_DEBUG("Host: {}", job->comp_host);
  LOG_DEBUG("Site: {}", job->comp_site);
  LOG_DEBUG("----------------------------------------------------------------------");
}

void RL_DISPATCHER::cleanup()
{
  for(auto& s : _sites){
    for(auto& h : s->cpus){
      for(auto& d : h->disks){ delete d; }
      h->disks.clear();
      delete h;
    }
    s->cpus.clear();
    delete s;
  }
  _sites.clear();
  _sites_map.clear();
}

// -------------------- Ordering helpers --------------------

std::vector<Site*> RL_DISPATCHER::getSitesOrderedByName_() const
{
  std::vector<Site*> sites = _sites;
  std::sort(sites.begin(), sites.end(),
            [](const Site* a, const Site* b) {
              if (a == nullptr) return false;
              if (b == nullptr) return true;
              return a->name < b->name;
            });
  return sites;
}

std::vector<Host*> RL_DISPATCHER::getCpusOrderedByName_(const Site* site)
{
  std::vector<Host*> cpus;
  if (!site) return cpus;
  cpus = site->cpus;
  std::sort(cpus.begin(), cpus.end(),
            [](const Host* a, const Host* b) {
              if (a == nullptr) return false;
              if (b == nullptr) return true;
              return a->name < b->name;
            });
  return cpus;
}

std::size_t RL_DISPATCHER::getMaxCpuCount_() const
{
  std::size_t maxC = 0;
  for (const Site* s : _sites) {
    if (!s) continue;
    maxC = std::max<std::size_t>(maxC, s->cpus.size());
  }
  return maxC;
}

// -------------------- Feature collectors --------------------

std::vector<std::vector<double>> RL_DISPATCHER::collectCoreSpeeds() const
{
  const auto sites = getSitesOrderedByName_();
  const std::size_t S = sites.size();
  const std::size_t maxC = getMaxCpuCount_();

  std::vector<std::vector<double>> speeds(S, std::vector<double>(maxC, 0.0));
  for (std::size_t si = 0; si < S; ++si) {
    const Site* site = sites[si];
    const auto cpus = getCpusOrderedByName_(site);
    const std::size_t C = std::min<std::size_t>(cpus.size(), maxC);
    for (std::size_t ci = 0; ci < C; ++ci) {
      const Host* cpu = cpus[ci];
      if (!cpu) continue;
      speeds[si][ci] = cpu->speed;
    }
  }
  return speeds;
}

std::vector<std::vector<int>> RL_DISPATCHER::collectTotalCores() const
{
  const auto sites = getSitesOrderedByName_();
  const std::size_t S = sites.size();
  const std::size_t maxC = getMaxCpuCount_();

  std::vector<std::vector<int>> cores(S, std::vector<int>(maxC, 0));
  for (std::size_t si = 0; si < S; ++si) {
    const Site* site = sites[si];
    const auto cpus = getCpusOrderedByName_(site);
    const std::size_t C = std::min<std::size_t>(cpus.size(), maxC);
    for (std::size_t ci = 0; ci < C; ++ci) {
      const Host* cpu = cpus[ci];
      if (!cpu) continue;
      cores[si][ci] = cpu->cores;
    }
  }
  return cores;
}

std::vector<std::vector<int>> RL_DISPATCHER::collectAvailableCores() const
{
  const auto sites = getSitesOrderedByName_();
  const std::size_t S = sites.size();
  const std::size_t maxC = getMaxCpuCount_();

  std::vector<std::vector<int>> avail(S, std::vector<int>(maxC, 0));
  for (std::size_t si = 0; si < S; ++si) {
    const Site* site = sites[si];
    const auto cpus = getCpusOrderedByName_(site);
    const std::size_t C = std::min<std::size_t>(cpus.size(), maxC);
    for (std::size_t ci = 0; ci < C; ++ci) {
      const Host* cpu = cpus[ci];
      if (!cpu) continue;
      avail[si][ci] = cpu->cores_available;
    }
  }
  return avail;
}

std::vector<std::string> RL_DISPATCHER::collectSiteNames() const
{
  const auto sites = getSitesOrderedByName_();
  std::vector<std::string> names;
  names.reserve(sites.size());
  for (const Site* s : sites) names.push_back(s ? s->name : "");
  return names;
}

std::vector<std::vector<std::string>> RL_DISPATCHER::collectCpuNamesPadded() const
{
  const auto sites = getSitesOrderedByName_();
  const std::size_t S = sites.size();
  const std::size_t maxC = getMaxCpuCount_();

  std::vector<std::vector<std::string>> cpu_names(S, std::vector<std::string>(maxC, ""));
  for (std::size_t si = 0; si < S; ++si) {
    const Site* site = sites[si];
    const auto cpus = getCpusOrderedByName_(site);
    const std::size_t C = std::min<std::size_t>(cpus.size(), maxC);
    for (std::size_t ci = 0; ci < C; ++ci) {
      const Host* cpu = cpus[ci];
      if (!cpu) continue;
      cpu_names[si][ci] = cpu->name;
    }
  }
  return cpu_names;
}

// Job features: [core_count, no_of_inp_files, flops, inp_file_bytes]
std::vector<std::vector<double>>
RL_DISPATCHER::collectJobFeatures(const std::vector<Job*>& jobs) const
{
  const std::size_t J = jobs.size();
  std::vector<std::vector<double>> X(J, std::vector<double>(4, 0.0));

  for (std::size_t j = 0; j < J; ++j) {
    const Job* job = jobs[j];
    if (!job) continue;
    X[j][0] = (double)job->core_count;
    X[j][1] = (double)job->no_of_inp_files;
    X[j][2] = (double)job->flops;
    X[j][3] = (double)job->inp_file_bytes;
  }
  return X;
}

// ---------------- low-level I/O ----------------

void RL_DISPATCHER::recv_all_(int sock, void* out, size_t n)
{
  uint8_t* p = static_cast<uint8_t*>(out);
  while (n) {
    ssize_t r = ::recv(sock, p, n, 0);
    if (r <= 0) throw std::runtime_error("recv failed / socket closed");
    p += (size_t)r;
    n -= (size_t)r;
  }
}

void RL_DISPATCHER::send_all_(int sock, const void* data, size_t n)
{
  const uint8_t* p = static_cast<const uint8_t*>(data);
  while (n) {
    ssize_t s = ::send(sock, p, n, 0);
    if (s <= 0) throw std::runtime_error("send failed / socket closed");
    p += (size_t)s;
    n -= (size_t)s;
  }
}

uint64_t RL_DISPATCHER::recv_u64_be_(int sock)
{
  uint8_t b[8];
  recv_all_(sock, b, 8);
  uint64_t x = 0;
  for (int i = 0; i < 8; i++) x = (x << 8) | b[i];
  return x;
}

void RL_DISPATCHER::send_u64_be_(int sock, uint64_t x)
{
  uint8_t b[8];
  for (int i = 7; i >= 0; --i) { b[i] = (uint8_t)(x & 0xFF); x >>= 8; }
  send_all_(sock, b, 8);
}

// ---------------- messages ----------------

std::string RL_DISPATCHER::receiveMessage(int sock) const
{
  uint64_t n = recv_u64_be_(sock);
  std::string s;
  s.resize((size_t)n);
  if (n) recv_all_(sock, s.data(), (size_t)n);
  return s;
}

void RL_DISPATCHER::sendMessage(int sock, const std::string& msg) const
{
  send_u64_be_(sock, (uint64_t)msg.size());
  if (!msg.empty()) send_all_(sock, msg.data(), msg.size());
}

// ---------------- arrays (.npy receive) ----------------

#if defined(__linux__)
static int memfd_create_compat_(const char* name, unsigned int flags) {
#ifdef SYS_memfd_create
  return (int)syscall(SYS_memfd_create, name, flags);
#else
  (void)name; (void)flags;
  return -1;
#endif
}
#endif

cnpy::NpyArray RL_DISPATCHER::receiveData(int sock, const std::string& tmp_path) const
{
  uint64_t n = recv_u64_be_(sock);
  std::vector<uint8_t> buf((size_t)n);
  if (n) recv_all_(sock, buf.data(), buf.size());

#if defined(__linux__)
  int fd = memfd_create_compat_("incoming_npy", MFD_CLOEXEC);
  if (fd >= 0) {
    ssize_t w = ::write(fd, buf.data(), (size_t)buf.size());
    if (w != (ssize_t)buf.size()) {
      ::close(fd);
      throw std::runtime_error("failed to write full npy payload to memfd");
    }
    std::string path = "/proc/self/fd/" + std::to_string(fd);
    cnpy::NpyArray arr = cnpy::npy_load(path);
    ::close(fd);
    return arr;
  }
#endif

  std::ofstream out(tmp_path, std::ios::binary);
  out.write(reinterpret_cast<const char*>(buf.data()), (std::streamsize)buf.size());
  out.close();
  if (!out) throw std::runtime_error("failed to write temp npy");

  return cnpy::npy_load(tmp_path);
}

// ---------------- Python client connection ----------------

void RL_DISPATCHER::connectPython(const std::string& host, int port)
{
  if (py_sock_ >= 0) return;

  py_sock_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (py_sock_ < 0) throw std::runtime_error("socket() failed");

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);

  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    ::close(py_sock_);
    py_sock_ = -1;
    throw std::runtime_error("inet_pton() failed for host=" + host);
  }

  if (::connect(py_sock_, (sockaddr*)&addr, sizeof(addr)) != 0) {
    std::string err = std::strerror(errno);
    ::close(py_sock_);
    py_sock_ = -1;
    throw std::runtime_error("connect() failed: " + err);
  }

  // Python sends this immediately after accept()
  std::string hello = receiveMessage(py_sock_);
  LOG_INFO("Python server says: {}", hello);
}

void RL_DISPATCHER::disconnectPython()
{
  if (py_sock_ >= 0) {
    ::close(py_sock_);
    py_sock_ = -1;
  }
}

// ---------------- Decision decode ----------------

bool RL_DISPATCHER::decodeDecision_(const cnpy::NpyArray& decision,
                                   size_t S, size_t maxC,
                                   size_t& out_site_i, size_t& out_cpu_i)
{
  if (decision.shape.size() != 2) return false;
  if (decision.shape[0] != S || decision.shape[1] != maxC) return false;

  const size_t N = S * maxC;

  // common cases: uint8, int64, float64
  if (decision.word_size == 1) {
    const uint8_t* p = decision.data<uint8_t>();
    for (size_t idx = 0; idx < N; ++idx) {
      if (p[idx] != 0) { out_site_i = idx / maxC; out_cpu_i = idx % maxC; return true; }
    }
    return false;
  }

  if (decision.word_size == 8) {
    // Treat as double; nonzero => selected
    const double* p = decision.data<double>();
    for (size_t idx = 0; idx < N; ++idx) {
      if (p[idx] != 0.0) { out_site_i = idx / maxC; out_cpu_i = idx % maxC; return true; }
    }
    return false;
  }

  return false;
}

// ---------------- One-job RPC (matches your Python loop) ----------------

bool RL_DISPATCHER::chooseWithPython(Job* job, Site*& out_site, Host*& out_cpu)
{
  out_site = nullptr;
  out_cpu  = nullptr;

  if (!job) return false;
  if (py_sock_ < 0) connectPython("127.0.0.1", 5555);

  // Build current grid status arrays
  auto totalCores     = collectTotalCores();       // [S][maxC]
  auto availableCores = collectAvailableCores();   // [S][maxC]
  auto coreSpeeds     = collectCoreSpeeds();       // [S][maxC]

  const auto sitesOrdered = getSitesOrderedByName_();
  const size_t S = sitesOrdered.size();
  const size_t maxC = getMaxCpuCount_();

  // Flatten for contiguous send
  auto totalFlat = flatten2D_(totalCores);
  auto availFlat = flatten2D_(availableCores);
  auto speedFlat = flatten2D_(coreSpeeds);

  // Job features: Python expects receiveData() => jobFeatures
  // We'll send [1,4] for THIS job only.
  double jobFeat[4] = {
    (double)job->core_count,
    (double)job->no_of_inp_files,
    (double)job->flops,
    (double)job->inp_file_bytes
  };

  // ----- Protocol (exact strings from your Python code) -----
  sendMessage(py_sock_, "Job submission.");

  {
    std::string msg = receiveMessage(py_sock_);
    if (msg != "Waiting data.") {
      LOG_ERROR("Python protocol mismatch: expected 'Waiting data.' got '{}'", msg);
      return false;
    }
  }

  // total cores
  sendData<int>(py_sock_, totalFlat.data(), {S, maxC});
  {
    std::string msg = receiveMessage(py_sock_);
    if (msg != "Cores received.") {
      LOG_ERROR("Python protocol mismatch after totalCores: got '{}'", msg);
      return false;
    }
  }

  // available cores
  sendData<int>(py_sock_, availFlat.data(), {S, maxC});
  {
    std::string msg = receiveMessage(py_sock_);
    if (msg != "Available cores received.") {
      LOG_ERROR("Python protocol mismatch after availableCores: got '{}'", msg);
      return false;
    }
  }

  // speeds
  sendData<double>(py_sock_, speedFlat.data(), {S, maxC});
  {
    std::string msg = receiveMessage(py_sock_);
    if (msg != "Core speeds received.") {
      LOG_ERROR("Python protocol mismatch after coreSpeeds: got '{}'", msg);
      return false;
    }
  }

  // job features
  sendData<double>(py_sock_, jobFeat, {1, (size_t)4});
  {
    std::string msg = receiveMessage(py_sock_);
    if (msg != "Features received.") {
      LOG_ERROR("Python protocol mismatch after jobFeatures: got '{}'", msg);
      return false;
    }
  }

  // Python sends bestCPU matrix [S, maxC]
  cnpy::NpyArray decision = receiveData(py_sock_);

  size_t bestSi = 0, bestCi = 0;
  if (!decodeDecision_(decision, S, maxC, bestSi, bestCi)) {
    LOG_ERROR("Python returned invalid decision matrix shape/type.");
    return false;
  }

  if (bestSi >= sitesOrdered.size()) return false;
  Site* chosenSite = sitesOrdered[bestSi];
  if (!chosenSite) return false;

  auto cpusOrdered = getCpusOrderedByName_(chosenSite);
  if (bestCi >= cpusOrdered.size()) {
    LOG_ERROR("Python selected padded CPU column (ci={}, site has {}).", bestCi, cpusOrdered.size());
    return false;
  }

  out_site = chosenSite;
  out_cpu  = cpusOrdered[bestCi];
  return (out_cpu != nullptr);
}

// ---------------- assignJobToResource: now Python-driven ----------------

Job* RL_DISPATCHER::assignJobToResource(Job* job)
{
  if (job == nullptr) {
    LOG_DEBUG("JOB pointer null");
    return job;
  }

  // If flops isn't set yet in your pipeline, Python will receive 0.
  // That's OK for now; you can compute flops earlier if you want.
  LOG_DEBUG("Waiting to assign job resources : {}", job->jobid);

  Site* chosenSite = nullptr;
  Host* chosenCpu  = nullptr;

  bool ok = false;
  try {
    ok = chooseWithPython(job, chosenSite, chosenCpu);
  } catch (const std::exception& e) {
    LOG_ERROR("Python communication failed: {}", e.what());
    ok = false;
  }

  if (!ok || !chosenSite || !chosenCpu) {
    job->status = "pending";
    LOG_DEBUG("No CPU selected by Python; job {} pending.", job->jobid);
    return job;
  }

  // Basic feasibility
  if (chosenCpu->cores_available < job->cores) {
    job->status = "pending";
    LOG_DEBUG("Python chose {}, but not enough available cores (need {}, have {}).",
              chosenCpu->name, job->cores, chosenCpu->cores_available);
    return job;
  }

  // Choose disk using your existing scoring logic
  std::string best_disk_name;
  double score = calculateWeightedScore(chosenCpu, job, best_disk_name);
  (void)score;

  if (best_disk_name.empty() || chosenCpu->disks_map.count(best_disk_name) == 0) {
    job->status = "pending";
    LOG_DEBUG("No suitable disk found on chosen CPU {}; job pending.", chosenCpu->name);
    return job;
  }

  Disk* disk = chosenCpu->disks_map.at(best_disk_name);
  const double needed_storage = this->getTotalSize(job->input_files) + this->getTotalSize(job->output_files);
  if (disk->storage < (size_t)needed_storage) {
    job->status = "pending";
    LOG_DEBUG("Chosen disk {} lacks storage (need {}, have {}).", best_disk_name, needed_storage, disk->storage);
    return job;
  }

  // Reserve resources
  chosenCpu->cores_available -= job->cores;
  disk->storage -= (size_t)needed_storage;
  chosenCpu->jobs.insert(job->jobid);

  chosenSite->cpus_in_use++;

  // Fill job assignment fields
  job->comp_site = chosenSite->name;
  job->comp_host = chosenCpu->name;
  job->disk      = best_disk_name;
  job->status    = "assigned";

  LOG_DEBUG("Job {} assigned to site {} cpu {} disk {}", job->jobid, job->comp_site, job->comp_host, job->disk);

  printJobInfo(job);
  return job;
}
