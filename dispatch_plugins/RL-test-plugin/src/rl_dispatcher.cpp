// rl_dispatcher.cpp
#include "rl_dispatcher.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <queue>
#include <cstdlib>

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

// ---------------- Workload loader (CSV) ----------------
// Same idea as SIMPLE_DISPATCHER::getJobs(), but with:
// - safer path selection (env var + relative defaults)
// - no hardcoded /Users/... path

static std::vector<std::string> parseCSVLine_(const std::string& line)
{
  std::vector<std::string> row;
  std::string cell;
  bool in_quotes = false;

  for (char c : line) {
    if (c == '"') {
      in_quotes = !in_quotes;
    } else if (c == ',' && !in_quotes) {
      row.push_back(cell);
      cell.clear();
    } else {
      cell += c;
    }
  }
  row.push_back(cell);

  for (auto& field : row) {
    if (!field.empty() && field.front() == '"' && field.back() == '"') {
      field = field.substr(1, field.size() - 2);
    }
    field.erase(std::remove_if(field.begin(), field.end(),
                               [](unsigned char ch) { return !std::isprint(ch); }),
                field.end());
  }
  return row;
}

static std::string getColumn_(const std::vector<std::string>& row,
                              const std::unordered_map<std::string, int>& column_map,
                              const std::string& key,
                              const std::string& def = "")
{
  auto it = column_map.find(key);
  if (it == column_map.end()) return def;
  const int idx = it->second;
  if (idx < 0 || idx >= (int)row.size()) return def;
  if (row[idx].empty()) return def;
  return row[idx];
}

JobQueue RL_DISPATCHER::getJobs(long max_jobs)
{
  std::vector<std::string> candidates;
  if (const char* env = std::getenv("CGSIM_JOB_CSV")) {
    if (*env) candidates.emplace_back(env);
  }
  candidates.emplace_back("new_data/mimic_job.csv");
  candidates.emplace_back("new_data/jobs.csv");

  std::ifstream file;
  std::string chosen;
  for (const auto& p : candidates) {
    file.open(p);
    if (file.is_open()) { chosen = p; break; }
    file.clear();
  }
  if (!file.is_open()) {
    throw std::runtime_error(
        "RL_DISPATCHER::getJobs: Could not open any job CSV. Set CGSIM_JOB_CSV or provide new_data/mimic_job.csv");
  }

  JobQueue jobs;
  std::string line;
  std::unordered_map<std::string, int> column_map;
  bool header_parsed = false;

  while (std::getline(file, line)) {
    auto row = parseCSVLine_(line);

    if (!header_parsed) {
      header_parsed = true;
      for (int i = 0; i < (int)row.size(); ++i) {
        std::string col = row[i];
        std::transform(col.begin(), col.end(), col.begin(), ::tolower);
        column_map[col] = i;
      }
      continue;
    }

    if (max_jobs != -1 && (long)jobs.size() >= max_jobs) break;

    try {
      Job* job = new Job();

      job->jobid                 = std::stoll(getColumn_(row, column_map, "pandaid", "0"));
      job->creation_time         = getColumn_(row, column_map, "creationtime", "");
      job->job_status            = getColumn_(row, column_map, "jobstatus", "");
      job->job_name              = getColumn_(row, column_map, "jobname", "");
      job->cpu_consumption_time  = std::stod(getColumn_(row, column_map, "cpuconsumptiontime", "0"));
      job->comp_site             = "AGLT2_site_" + getColumn_(row, column_map, "computingsite", "0");
      job->destination_dataset_name = getColumn_(row, column_map, "destinationdblock", "");
      job->destination_SE        = getColumn_(row, column_map, "destinationse", "");
      job->source_site           = getColumn_(row, column_map, "sourcesite", "");
      job->transfer_type         = getColumn_(row, column_map, "transfertype", "");
      job->core_count            = std::stoi(getColumn_(row, column_map, "corecount", "0"));
      job->cores                 = job->core_count;
      job->no_of_inp_files       = std::stoi(getColumn_(row, column_map, "ninputdatafiles", "0"));
      job->inp_file_bytes        = std::stod(getColumn_(row, column_map, "inputfilebytes", "0"));
      job->no_of_out_files       = std::stoi(getColumn_(row, column_map, "noutputdatafiles", "0"));
      job->out_file_bytes        = std::stod(getColumn_(row, column_map, "outputfilebytes", "0"));
      job->pilot_error_code      = getColumn_(row, column_map, "piloterrorcode", "");
      job->exe_error_code        = getColumn_(row, column_map, "exeerrorcode", "");
      job->ddm_error_code        = getColumn_(row, column_map, "ddmerrorcode", "");
      job->dispatcher_error_code = getColumn_(row, column_map, "jobdispatchererrorcode", "");
      job->taskbuffer_error_code = getColumn_(row, column_map, "taskbuffererrorcode", "");
      job->status                = "created";

      // Parse input files JSON-ish column "files_info"
      std::string json_str = getColumn_(row, column_map, "files_info", "");
      if (!json_str.empty() && json_str.front() == '"' && json_str.back() == '"')
        json_str = json_str.substr(1, json_str.size() - 2);

      json_str.erase(std::remove(json_str.begin(), json_str.end(), '{'), json_str.end());
      json_str.erase(std::remove(json_str.begin(), json_str.end(), '}'), json_str.end());

      std::stringstream ss(json_str);
      std::string token;
      while (std::getline(ss, token, ',')) {
        auto colon_pos = token.find(':');
        if (colon_pos == std::string::npos) continue;
        std::string key = token.substr(0, colon_pos);
        key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
        key.erase(std::remove(key.begin(), key.end(), '"'), key.end());
        if (!key.empty()) job->input_files[key] = {0LL, {}};
      }

      // Generate output files
      long long size_per_out_file =
          job->no_of_out_files > 0 ? (long long)(job->out_file_bytes / job->no_of_out_files) : 0;
      for (int f = 1; f <= job->no_of_out_files; ++f) {
        std::string filename =
            "/output/user.output." + std::to_string(job->jobid) + ".0000" + std::to_string(f) + ".root";
        job->output_files[filename] = size_per_out_file;
      }

      jobs.push(job);
    } catch (const std::exception& e) {
      std::cerr << "Skipping invalid row in " << chosen << " : " << e.what() << "\n";
    }
  }

  return jobs;
}

// ---------------- Platform build ----------------
void RL_DISPATCHER::setPlatform(sg4::NetZone* platform)
{
  if (!platform) throw std::invalid_argument("setPlatform: platform is null");
  cleanup();

  platform_ = platform;

  std::priority_queue<Site*> site_queue;

  auto all_sites = platform->get_children();
  for (const auto& site : all_sites) {
    if (!site) continue;

    const char* site_cname = site->get_cname();
    if (!site_cname) continue;

    std::string site_name(site_cname);
    if (site_name == "JOB-SERVER") continue;

    Site* s = new Site;
    s->name = site_name;

    const char* storage_str = site->get_property("storage_capacity_bytes");
    s->storage = storage_str ? std::stoll(storage_str) : 0;

    const char* gflops_str = site->get_property("gflops");
    s->gflops = gflops_str ? std::stoll(gflops_str) : 0;

    s->priority    = 0;
    s->cpus_in_use = 0;

    for (const auto& host : site->get_all_hosts()) {
      if (!host) continue;
      if (host->get_name().find("_communication") != std::string::npos) continue;

      Host* h = new Host;
      const char* host_cname = host->get_cname();
      h->name            = host_cname ? host_cname : "UNKNOWN_HOST";
      h->cores           = host->get_core_count();
      h->cores_available = h->cores;
      h->speed           = host->get_speed();

      for (const auto& disk : host->get_disks()) {
        if (!disk) continue;
        Disk* d = new Disk;

        const char* disk_cname = disk->get_cname();
        d->name = disk_cname ? disk_cname : "UNKNOWN_DISK";

        const char* mount_prop = disk->get_property("mount");
        d->mount = mount_prop ? mount_prop : "/";

        d->read_bw  = disk->get_read_bandwidth();
        d->write_bw = disk->get_write_bandwidth();

        h->disks.push_back(d);
        h->disks_map[d->name] = d;
      }

      s->cpus.push_back(h);
      s->cpus_map[h->name] = h;

      s->priority += (int)std::round(h->speed / 1e8 * weights_.at("speed") + h->cores * weights_.at("cores"));
    }

    if (!s->cpus.empty()) s->priority = (int)std::round((double)s->priority / (double)s->cpus.size());

    sites_map_[s->name] = s;
    site_queue.push(s);
  }

  while (!site_queue.empty()) {
    sites_.push_back(site_queue.top());
    site_queue.pop();
  }
}

void RL_DISPATCHER::cleanup()
{
  for (auto* s : sites_) {
    if (!s) continue;
    for (auto* h : s->cpus) {
      if (!h) continue;
      for (auto* d : h->disks) delete d;
      h->disks.clear();
      delete h;
    }
    s->cpus.clear();
    delete s;
  }
  sites_.clear();
  sites_map_.clear();
  platform_ = nullptr;
}

// ---------------- Ordering helpers ----------------
std::vector<Site*> RL_DISPATCHER::getSitesOrderedByName_() const
{
  std::vector<Site*> out = sites_;
  std::sort(out.begin(), out.end(),
            [](const Site* a, const Site* b) {
              if (!a) return false;
              if (!b) return true;
              return a->name < b->name;
            });
  return out;
}

std::vector<Host*> RL_DISPATCHER::getCpusOrderedByName_(const Site* site)
{
  std::vector<Host*> out;
  if (!site) return out;
  out = site->cpus;
  std::sort(out.begin(), out.end(),
            [](const Host* a, const Host* b) {
              if (!a) return false;
              if (!b) return true;
              return a->name < b->name;
            });
  return out;
}

std::size_t RL_DISPATCHER::getMaxCpuCount_() const
{
  std::size_t maxC = 0;
  for (const Site* s : sites_) {
    if (!s) continue;
    maxC = std::max<std::size_t>(maxC, s->cpus.size());
  }
  return maxC;
}

// ---------------- Feature collectors ----------------
std::vector<std::vector<int>> RL_DISPATCHER::collectTotalCores() const
{
  const auto sites = getSitesOrderedByName_();
  const std::size_t S = sites.size();
  const std::size_t maxC = getMaxCpuCount_();
  std::vector<std::vector<int>> cores(S, std::vector<int>(maxC, 0));

  for (std::size_t si = 0; si < S; ++si) {
    const auto cpus = getCpusOrderedByName_(sites[si]);
    for (std::size_t ci = 0; ci < std::min<std::size_t>(cpus.size(), maxC); ++ci) {
      if (cpus[ci]) cores[si][ci] = cpus[ci]->cores;
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
    const auto cpus = getCpusOrderedByName_(sites[si]);
    for (std::size_t ci = 0; ci < std::min<std::size_t>(cpus.size(), maxC); ++ci) {
      if (cpus[ci]) avail[si][ci] = cpus[ci]->cores_available;
    }
  }
  return avail;
}

std::vector<std::vector<double>> RL_DISPATCHER::collectCoreSpeeds() const
{
  const auto sites = getSitesOrderedByName_();
  const std::size_t S = sites.size();
  const std::size_t maxC = getMaxCpuCount_();
  std::vector<std::vector<double>> speeds(S, std::vector<double>(maxC, 0.0));

  for (std::size_t si = 0; si < S; ++si) {
    const auto cpus = getCpusOrderedByName_(sites[si]);
    for (std::size_t ci = 0; ci < std::min<std::size_t>(cpus.size(), maxC); ++ci) {
      if (cpus[ci]) speeds[si][ci] = cpus[ci]->speed;
    }
  }
  return speeds;
}

std::array<double, 4> RL_DISPATCHER::collectOneJobFeatures(const Job* job, const Site* site_for_flops) const
{
  std::array<double, 4> f{0, 0, 0, 0};
  if (!job) return f;

  const double site_gflops = site_for_flops ? (double)site_for_flops->gflops : 0.0;
  const double flops = site_gflops * (double)job->cpu_consumption_time * (double)job->cores;

  f[0] = (double)job->core_count;
  f[1] = (double)job->no_of_inp_files;
  f[2] = flops;
  f[3] = (double)job->inp_file_bytes;
  return f;
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
std::string RL_DISPATCHER::receiveMessage_(int sock) const
{
  uint64_t n = recv_u64_be_(sock);
  std::string s;
  s.resize((size_t)n);
  if (n) recv_all_(sock, s.data(), (size_t)n);
  return s;
}

void RL_DISPATCHER::sendMessage_(int sock, const std::string& msg) const
{
  send_u64_be_(sock, (uint64_t)msg.size());
  if (!msg.empty()) send_all_(sock, msg.data(), msg.size());
}

// ---------------- arrays receive (.npy) ----------------
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

cnpy::NpyArray RL_DISPATCHER::receiveData_(int sock, const std::string& tmp_path) const
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
  addr.sin_port   = htons((uint16_t)port);

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

  // Python sends "CONN" immediately after accept()
  const std::string hello = receiveMessage_(py_sock_);
  if (hello != "CONN") {
    disconnectPython();
    throw std::runtime_error("Python hello mismatch: expected CONN, got: " + hello);
  }
}

void RL_DISPATCHER::disconnectPython()
{
  if (py_sock_ >= 0) {
    ::close(py_sock_);
    py_sock_ = -1;
  }
}

// ---------------- decode SITE-only decision ----------------
bool RL_DISPATCHER::decodeSiteDecision_(const cnpy::NpyArray& decision, size_t S, size_t& out_site_i)
{
  if (decision.shape.size() == 1) {
    if (decision.shape[0] != S) return false;
  } else if (decision.shape.size() == 2) {
    if (decision.shape[0] != 1 || decision.shape[1] != S) return false;
  } else {
    return false;
  }

  if (decision.word_size == 1) {
    const uint8_t* p = decision.data<uint8_t>();
    for (size_t i = 0; i < S; ++i) {
      if (p[i] != 0) { out_site_i = i; return true; }
    }
    return false;
  }

  if (decision.word_size == 8) {
    const double* p = decision.data<double>();
    for (size_t i = 0; i < S; ++i) {
      if (p[i] != 0.0) { out_site_i = i; return true; }
    }
    return false;
  }

  return false;
}

// ---------------- choose random CPU in chosen site ----------------
Host* RL_DISPATCHER::pickRandomCpuInSite_(Site* site, int required_cores)
{
  if (!site) return nullptr;

  std::vector<Host*> feasible;
  feasible.reserve(site->cpus.size());
  for (auto* h : site->cpus) {
    if (!h) continue;
    if (h->cores_available >= required_cores) feasible.push_back(h);
  }
  if (feasible.empty()) return nullptr;

  std::uniform_int_distribution<size_t> dist(0, feasible.size() - 1);
  return feasible[dist(rng_)];
}

// ---------------- One-job RPC (CONN/SBMT/WAIT/CNFM) ----------------
bool RL_DISPATCHER::chooseSiteWithPython(Job* job, Site*& out_site)
{
  out_site = nullptr;
  if (!job) return false;
  if (py_sock_ < 0) connectPython("127.0.0.1", 5555);

  const auto sitesOrdered = getSitesOrderedByName_();
  const size_t S = sitesOrdered.size();
  const size_t maxC = getMaxCpuCount_();
  if (S == 0) return false;

  // Build arrays
  auto totalCores     = collectTotalCores();       // [S][maxC]
  auto availableCores = collectAvailableCores();   // [S][maxC]
  auto coreSpeeds     = collectCoreSpeeds();       // [S][maxC]

  auto totalFlat = flatten2D_(totalCores);
  auto availFlat = flatten2D_(availableCores);
  auto speedFlat = flatten2D_(coreSpeeds);

  // Reference site for flops feature (stable choice: max gflops)
  const Site* ref_site = nullptr;
  long long best_g = -1;
  for (auto* s : sites_) {
    if (s && s->gflops > best_g) { best_g = s->gflops; ref_site = s; }
  }
  auto jf = collectOneJobFeatures(job, ref_site);

  // ---- protocol ----
  sendMessage_(py_sock_, "SBMT");
  if (receiveMessage_(py_sock_) != "WAIT") return false;

  sendData_<int>(py_sock_, totalFlat.data(), {S, maxC});
  if (receiveMessage_(py_sock_) != "CNFM") return false;

  sendData_<int>(py_sock_, availFlat.data(), {S, maxC});
  if (receiveMessage_(py_sock_) != "CNFM") return false;

  sendData_<double>(py_sock_, speedFlat.data(), {S, maxC});
  if (receiveMessage_(py_sock_) != "CNFM") return false;

  sendData_<double>(py_sock_, jf.data(), {1, (size_t)4});
  if (receiveMessage_(py_sock_) != "CNFM") return false;

  sendMessage_(py_sock_, "WAIT");

  cnpy::NpyArray decision = receiveData_(py_sock_);

  size_t bestSi = 0;
  if (!decodeSiteDecision_(decision, S, bestSi)) return false;
  if (bestSi >= sitesOrdered.size()) return false;

  out_site = sitesOrdered[bestSi];
  return out_site != nullptr;
}

// ---------------- Assignment ----------------
Job* RL_DISPATCHER::assignJobToResource(Job* job)
{
  if (!job) return job;
  if (job->cores <= 0) job->cores = job->core_count;

  Site* chosenSite = nullptr;
  bool ok = false;
  try {
    ok = chooseSiteWithPython(job, chosenSite);
  } catch (...) {
    ok = false;
  }

  if (!ok || !chosenSite) {
    job->status = "pending";
    return job;
  }

  Host* chosenCpu = pickRandomCpuInSite_(chosenSite, job->cores);
  if (!chosenCpu) {
    job->status = "pending";
    return job;
  }

  job->flops = (long long)((double)chosenSite->gflops * (double)job->cpu_consumption_time * (double)job->cores);

  chosenCpu->cores_available -= job->cores;
  chosenCpu->jobs.insert(job->jobid);
  chosenSite->cpus_in_use++;

  if (!chosenCpu->disks.empty() && chosenCpu->disks[0]) {
    job->disk  = chosenCpu->disks[0]->name;
    job->mount = chosenCpu->disks[0]->mount;
  } else {
    job->disk.clear();
    job->mount.clear();
  }

  job->comp_site = chosenSite->name;
  job->comp_host = chosenCpu->name;
  job->status    = "assigned";

  return job;
}

void RL_DISPATCHER::free(Job* job)
{
  if (!job) return;
  auto sit_it = sites_map_.find(job->comp_site);
  if (sit_it == sites_map_.end()) return;
  Site* site = sit_it->second;
  if (!site) return;

  auto cpu_it = site->cpus_map.find(job->comp_host);
  if (cpu_it == site->cpus_map.end()) return;
  Host* cpu = cpu_it->second;
  if (!cpu) return;

  cpu->cores_available += job->cores;
  cpu->jobs.erase(job->jobid);
}
