#include "python_dispatcher.h"

void PYTHON_DISPATCHER::connectPython(const std::string& host, int port)
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

  const std::string hello = receiveMessage_(py_sock_);
  if (hello != "CONN") {
    disconnectPython();
    throw std::runtime_error("Python hello mismatch: expected CONN, got: " + hello);
  }
}

void PYTHON_DISPATCHER::disconnectPython()
{
  if (py_sock_ >= 0) {
    ::close(py_sock_);
    py_sock_ = -1;
  }
}

void PYTHON_DISPATCHER::recv_all_(int sock, void* out, size_t n)
{
  uint8_t* p = static_cast<uint8_t*>(out);
  while (n) {
    ssize_t r = ::recv(sock, p, n, 0);
    if (r <= 0) throw std::runtime_error("recv failed / socket closed");
    p += (size_t)r;
    n -= (size_t)r;
  }
}

void PYTHON_DISPATCHER::send_all_(int sock, const void* data, size_t n)
{
  const uint8_t* p = static_cast<const uint8_t*>(data);
  while (n) {
    ssize_t s = ::send(sock, p, n, 0);
    if (s <= 0) throw std::runtime_error("send failed / socket closed");
    p += (size_t)s;
    n -= (size_t)s;
  }
}

uint64_t PYTHON_DISPATCHER::recv_u64_be_(int sock)
{
  uint8_t b[8];
  recv_all_(sock, b, 8);
  uint64_t x = 0;
  for (int i = 0; i < 8; i++) x = (x << 8) | b[i];
  return x;
}

void PYTHON_DISPATCHER::send_u64_be_(int sock, uint64_t x)
{
  uint8_t b[8];
  for (int i = 7; i >= 0; --i) { b[i] = (uint8_t)(x & 0xFF); x >>= 8; }
  send_all_(sock, b, 8);
}

std::string PYTHON_DISPATCHER::receiveMessage_(int sock) const
{
  uint64_t n = recv_u64_be_(sock);
  std::string s;
  s.resize((size_t)n);
  if (n) recv_all_(sock, s.data(), (size_t)n);
  return s;
}

void PYTHON_DISPATCHER::sendMessage_(int sock, const std::string& msg) const
{
  send_u64_be_(sock, (uint64_t)msg.size());
  if (!msg.empty()) send_all_(sock, msg.data(), msg.size());
}

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

cnpy::NpyArray PYTHON_DISPATCHER::receiveData_(int sock, const std::string& tmp_path) const
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

template <typename T>
void PYTHON_DISPATCHER::sendData_(int sock, const T* data, const std::vector<size_t>& shape) const
{
  std::vector<char> header = cnpy::create_npy_header<T>(shape);

  size_t nels = 1;
  for (auto s : shape) nels *= s;

  const uint64_t total = (uint64_t)header.size() + (uint64_t)(nels * sizeof(T));
  send_u64_be_(sock, total);
  send_all_(sock, header.data(), header.size());
  if (nels) send_all_(sock, data, nels * sizeof(T));
}

// Only keep double instantiation (float-only protocol)
template void PYTHON_DISPATCHER::sendData_<double>(int, const double*, const std::vector<size_t>&) const;


// ======================= SITE FEATURES (DOUBLE-ONLY) =======================

// One feature matrix: always double
struct FeatureMatrix {
  std::string name;            // e.g. "cores"
  std::vector<size_t> shape;   // {S, maxCPU}
  std::vector<double> f64;     // flattened row-major
};

// ---- string + filtering helpers ----
static inline void trimInplace(std::string& x)
{
  while (!x.empty() && std::isspace((unsigned char)x.front())) x.erase(x.begin());
  while (!x.empty() && std::isspace((unsigned char)x.back()))  x.pop_back();
}

static std::vector<std::string> splitCSVTrim(const std::string& s)
{
  std::vector<std::string> out;
  std::string cur;
  for (char c : s) {
    if (c == ',') {
      trimInplace(cur);
      if (!cur.empty()) out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  trimInplace(cur);
  if (!cur.empty()) out.push_back(cur);
  return out;
}

static bool isComputeHost(sg4::Host* h)
{
  return h && (h->get_name().find("_communication") == std::string::npos);
}

static bool isRealSite(sg4::NetZone* z)
{
  return z && (z->get_name() != "JOB-SERVER");
}

// ---- matrix builder ----
static size_t maxCPU(const std::vector<std::vector<sg4::Host*>>& hostsPerSite)
{
  size_t m = 0;
  for (const auto& v : hostsPerSite) if (v.size() > m) m = v.size();
  return m;
}

static std::vector<double> buildSiteMatrix(
    const std::vector<std::vector<sg4::Host*>>& hostsPerSite,
    size_t maxCpu,
    const std::function<double(sg4::Host*)>& perHost)
{
  const size_t S = hostsPerSite.size();
  std::vector<double> out(S * maxCpu, 0.0);

  for (size_t si = 0; si < S; ++si) {
    const auto& hosts = hostsPerSite[si];
    for (size_t ci = 0; ci < hosts.size(); ++ci) {
      auto* h = hosts[ci];
      if (!h) continue;
      out[si * maxCpu + ci] = perHost(h);
    }
  }
  return out;
}

// ---- site feature maps (double-only) ----
static std::unordered_map<std::string, std::function<double(sg4::Host*)>> siteFeatureMap;
static std::unordered_map<std::string, std::function<double(sg4::Host*)>> activeSiteFeatureMap;

static void initSiteFeatureMap()
{
  if (!siteFeatureMap.empty()) return;

  siteFeatureMap["cores"] = [](sg4::Host* h) -> double {
    return (double)h->get_core_count();
  };

  siteFeatureMap["cores_available"] = [](sg4::Host* h) -> double {
    return (double)h->extension<HostExtensions>()->get_cores_available();
  };

  siteFeatureMap["speed"] = [](sg4::Host* h) -> double {
    return (double)h->get_speed();
  };

  // site-level remaining storage, replicated per host (same value within a site)
  siteFeatureMap["disk"] = [](sg4::Host* h) -> double {
    const std::string site = h->get_englobing_zone()->get_name();
    const long long bytes  = CGSim::FileManager::request_remaining_site_storage(site);
    return (double)bytes;
  };
}

static void registerSiteFeature(const std::string& featureName)
{
  initSiteFeatureMap();
  auto it = siteFeatureMap.find(featureName);
  if (it == siteFeatureMap.end()) {
    throw std::runtime_error("registerSiteFeature: unknown feature '" + featureName + "'");
  }
  activeSiteFeatureMap[featureName] = it->second;
}

// Read property, enable features in that order, return ordered list for send loop.
static std::vector<std::string> registerSite(sg4::NetZone* root, const std::string& propertyKey = "SiteFeatures")
{
  if (!root) throw std::invalid_argument("registerSite: root is null");

  initSiteFeatureMap();

  const char* p = root->get_property(propertyKey);
  const std::string csv = p ? std::string(p) : std::string();
  auto names = splitCSVTrim(csv);

  activeSiteFeatureMap.clear();
  for (const auto& n : names) registerSiteFeature(n);
  return names;
}

// Build ONE matrix for ONE enabled site feature (current snapshot).
static FeatureMatrix getSiteFeature(sg4::NetZone* root, const std::string& featureName)
{
  if (!root) throw std::invalid_argument("getSiteFeature: root is null");

  // engine order assumed stable
  std::vector<sg4::NetZone*> sites;
  for (auto* z : root->get_children()) {
    if (!isRealSite(z)) continue;
    sites.push_back(z);
  }

  std::vector<std::vector<sg4::Host*>> hostsPerSite;
  hostsPerSite.reserve(sites.size());

  for (auto* s : sites) {
    std::vector<sg4::Host*> hosts;
    for (auto* h : s->get_all_hosts()) {
      if (!isComputeHost(h)) continue;
      hosts.push_back(h);
    }
    hostsPerSite.push_back(std::move(hosts));
  }

  const size_t maxCpu = maxCPU(hostsPerSite);

  auto it = activeSiteFeatureMap.find(featureName);
  if (it == activeSiteFeatureMap.end()) {
    throw std::runtime_error("getSiteFeature: feature not enabled '" + featureName + "'");
  }

  FeatureMatrix m;
  m.name  = featureName;
  m.shape = {sites.size(), maxCpu};
  m.f64   = buildSiteMatrix(hostsPerSite, maxCpu, it->second);
  return m;
}


// ======================= JOB FEATURES (DOUBLE-ONLY, returns ONE [J,F] matrix) =======================

static std::unordered_map<std::string, std::function<double(const Job*)>> jobFeatureMap;
static std::unordered_map<std::string, std::function<double(const Job*)>> activeJobFeatureMap;

// Fill ALL supported job features (numeric only; double-only). inp/out bytes commented out.
static void initJobFeatureMap()
{
  if (!jobFeatureMap.empty()) return;

  // ints cast to double
  jobFeatureMap["cores"] = [](const Job* j) -> double { return j ? (double)j->cores : 0.0; };
  jobFeatureMap["core_count"] = [](const Job* j) -> double { return j ? (double)j->core_count : 0.0; };
  jobFeatureMap["no_of_inp_files"] = [](const Job* j) -> double { return j ? (double)j->no_of_inp_files : 0.0; };
  jobFeatureMap["no_of_out_files"] = [](const Job* j) -> double { return j ? (double)j->no_of_out_files : 0.0; };
  jobFeatureMap["retries"] = [](const Job* j) -> double { return j ? (double)j->retries : 0.0; };

  // doubles
  jobFeatureMap["cpu_consumption_time"] = [](const Job* j) -> double {
    return j ? (double)j->cpu_consumption_time : 0.0;
  };

  // jobid (double for now; exactness >2^53 not guaranteed)
  jobFeatureMap["jobid"] = [](const Job* j) -> double { return j ? (double)j->jobid : 0.0; };

  // --- COMMENTED OUT FOR NOW ---
  // jobFeatureMap["inp_file_bytes"] = [](const Job* j) -> double { return j ? (double)j->inp_file_bytes : 0.0; };
  // jobFeatureMap["out_file_bytes"] = [](const Job* j) -> double { return j ? (double)j->out_file_bytes : 0.0; };
}

static void registerJobFeature(const std::string& featureName)
{
  initJobFeatureMap();
  auto it = jobFeatureMap.find(featureName);
  if (it == jobFeatureMap.end()) {
    throw std::runtime_error("registerJobFeature: unknown feature '" + featureName + "'");
  }
  activeJobFeatureMap[featureName] = it->second;
}

// Read root->get_property("JobFeatures"), enable those in that order, return ordered list (column order).
static std::vector<std::string> registerJob(sg4::NetZone* root, const std::string& propertyKey = "JobFeatures")
{
  if (!root) throw std::invalid_argument("registerJob: root is null");

  initJobFeatureMap();

  const char* p = root->get_property(propertyKey);
  const std::string csv = p ? std::string(p) : std::string();
  auto names = splitCSVTrim(csv);

  activeJobFeatureMap.clear();
  for (const auto& n : names) registerJobFeature(n);

  return names; // column order in [J,F]
}

// Build ONE [J, F] matrix in the order of featureNames.
static std::vector<double> getJobFeaturesMatrix(const std::vector<Job*>& jobs,
                                                const std::vector<std::string>& featureNames,
                                                std::vector<size_t>& outShape)
{
  const size_t J = jobs.size();
  const size_t F = featureNames.size();
  outShape = {J, F};

  std::vector<double> M(J * F, 0.0);

  for (size_t fj = 0; fj < F; ++fj) {
    const auto& name = featureNames[fj];

    auto it = activeJobFeatureMap.find(name);
    if (it == activeJobFeatureMap.end()) {
      throw std::runtime_error("getJobFeaturesMatrix: feature not enabled '" + name + "'");
    }
    const auto& fn = it->second;

    for (size_t ji = 0; ji < J; ++ji) {
      const Job* j = jobs[ji];
      M[ji * F + fj] = j ? fn(j) : 0.0;  // row-major
    }
  }

  return M;
}

// 1) connectToPythonServer: wrapper around existing connectPython()
void PYTHON_DISPATCHER::connectToPythonServer(const std::string& host, int port)
{
  // connectPython() already does:
  // - socket/connect
  // - expects "CONN"
  // - throws on mismatch
  connectPython(host, port);
}

// 2) sendMatrix: sends ONE double matrix and expects "CNFM"
void PYTHON_DISPATCHER::sendMatrix(const double* data, const std::vector<size_t>& shape)
{
  if (py_sock_ < 0) throw std::runtime_error("sendMatrix: not connected");
  if (!data) throw std::invalid_argument("sendMatrix: data is null");
  if (shape.empty()) throw std::invalid_argument("sendMatrix: shape empty");

  // protocol is double-only
  sendData_<double>(py_sock_, data, shape);

  const std::string r = receiveMessage_(py_sock_);
  if (r != "CNFM") {
    throw std::runtime_error("sendMatrix: expected CNFM, got: " + r);
  }
}

// 3) waitDecision: C++ sends "WAIT", Python replies with npy decision [J,S]
cnpy::NpyArray PYTHON_DISPATCHER::waitDecision(const std::string& tmp_path)
{
  if (py_sock_ < 0) throw std::runtime_error("waitDecision: not connected");

  sendMessage_(py_sock_, "WAIT");
  return receiveData_(py_sock_, tmp_path);
}

// ======================= decodeDecision for [J,S] =======================

static bool decodeDecision(const cnpy::NpyArray& decision,
                           size_t J, size_t S,
                           std::vector<size_t>& out_site_idx)
{
  out_site_idx.assign(J, 0);

  if (decision.shape.size() != 2) return false;
  if (decision.shape[0] != J || decision.shape[1] != S) return false;

  // uint8 one-hot / mask
  if (decision.word_size == 1) {
    const uint8_t* p = decision.data<uint8_t>();
    for (size_t j = 0; j < J; ++j) {
      size_t chosen = S;
      for (size_t s = 0; s < S; ++s) {
        if (p[j * S + s] != 0) { chosen = s; break; }
      }
      if (chosen == S) return false;
      out_site_idx[j] = chosen;
    }
    return true;
  }

  // double (your protocol)
  if (decision.word_size == 8) {
    const double* p = decision.data<double>();
    for (size_t j = 0; j < J; ++j) {
      // prefer first nonzero (one-hot style)
      size_t chosen = S;
      for (size_t s = 0; s < S; ++s) {
        if (p[j * S + s] != 0.0) { chosen = s; break; }
      }
      if (chosen != S) {
        out_site_idx[j] = chosen;
        continue;
      }

      // fallback: argmax (soft scores)
      size_t best_s = 0;
      double best_v = p[j * S + 0];
      for (size_t s = 1; s < S; ++s) {
        const double v = p[j * S + s];
        if (v > best_v) { best_v = v; best_s = s; }
      }
      if (best_v == 0.0) return false; // all-zero row invalid
      out_site_idx[j] = best_s;
    }
    return true;
  }

  return false;
}

#include "python_dispatcher.h"
#include <simgrid/s4u.hpp>

namespace sg4 = simgrid::s4u;

std::vector<Job*> pythonDispatcher::assignJob(std::vector<Job*>& jobs)
{
  const size_t J = jobs.size();
  if (J == 0) return jobs;
  for (auto* j : jobs) {
    if (!j) throw std::invalid_argument("assignJob(batch): jobs contains nullptr");
  }

  auto* engine = sg4::Engine::get_instance();
  if (!engine) throw std::runtime_error("assignJob(batch): SimGrid Engine is null");
  sg4::NetZone* root = engine->get_netzone_root();
  if (!root) throw std::runtime_error("assignJob(batch): root netzone is null");

  const char* h = root->get_property("host");
  const char* p = root->get_property("port");

  if (!h || !*h) throw std::runtime_error("assignJob(batch): missing/empty root property 'host'");
  if (!p || !*p) throw std::runtime_error("assignJob(batch): missing/empty root property 'port'");

  const std::string host(h);

  int port = 0;
  try {
    port = std::stoi(std::string(p));
  } catch (...) {
    throw std::runtime_error(std::string("assignJob(batch): invalid root property 'port': '") + p + "'");
  }
  if (port <= 0 || port > 65535) {
    throw std::runtime_error("assignJob(batch): root property 'port' out of range: " + std::to_string(port));
  }

  // 1) Connect (connectPython() reads "CONN" once, and early-returns if already connected)
  connectToPythonServer(host, port);

  // 2) Job submit handshake for THIS submission:
  sendMessage_(py_sock_, "SBMT");
  {
    const std::string ack = receiveMessage_(py_sock_);
    if (ack != "WAIT") {
      throw std::runtime_error("assignJob(batch): after SBMT expected WAIT, got: " + ack);
    }
  }

  // 3) Register enabled features once (reads NetZone properties)
  if (!features_registered_) {
    site_feature_names_ = registerSite(root, "SiteFeatures");
    job_feature_names_  = registerJob(root,  "JobFeatures");
    features_registered_ = true;
  }

  // 4) Count sites S (skip JOB-SERVER)
  size_t S = 0;
  for (auto* z : root->get_children()) {
    if (!z) continue;
    if (z->get_name() == "JOB-SERVER") continue;
    ++S;
  }
  if (S == 0) throw std::runtime_error("assignJob(batch): no real sites found under root");

  auto idxToSiteName = [&](size_t chosen) -> std::string {
    size_t cur = 0;
    for (auto* z : root->get_children()) {
      if (!z) continue;
      if (z->get_name() == "JOB-SERVER") continue;
      if (cur == chosen) return z->get_name();
      ++cur;
    }
    return {};
  };

  // 5) Send site feature matrices (each expects CNFM)
  for (const auto& fname : site_feature_names_) {
    FeatureMatrix fm = getSiteFeature(root, fname);
    if (fm.shape.size() != 2 || fm.shape[0] != S) {
      throw std::runtime_error("assignJob(batch): site feature '" + fname + "' shape mismatch");
    }
    sendMatrix(fm.f64.data(), fm.shape); // sendData + expects CNFM
  }

  // 6) Send ONE job feature matrix [J,F] (expects CNFM)
  std::vector<size_t> jobShape;
  std::vector<double> jobM = getJobFeaturesMatrix(jobs, job_feature_names_, jobShape);

  if (jobShape.size() != 2 || jobShape[0] != J || jobShape[1] != job_feature_names_.size()) {
    throw std::runtime_error("assignJob(batch): job feature matrix shape mismatch");
  }
  sendMatrix(jobM.data(), jobShape); // expects CNFM

  // 7) Now C++ tells Python it’s time to return the decision npy
  cnpy::NpyArray decision = waitDecision(tmp_path_); // sends "WAIT", receives npy

  // 8) Decode and apply
  std::vector<size_t> site_idx;
  if (!decodeDecision(decision, J, S, site_idx) || site_idx.size() != J) {
    throw std::runtime_error("assignJob(batch): failed to decode decision [" +
                             std::to_string(J) + "," + std::to_string(S) + "]");
  }

  for (size_t j = 0; j < J; ++j) {
    const size_t chosen = site_idx[j];
    if (chosen >= S) throw std::runtime_error("assignJob(batch): chosen site out of range");

    const std::string siteName = idxToSiteName(chosen);
    if (siteName.empty()) throw std::runtime_error("assignJob(batch): site index mapping failed");

    jobs[j]->comp_site = siteName;
  }

  return jobs;
}