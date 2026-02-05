#ifndef PYTHON_DISPATCHER_H
#define PYTHON_DISPATCHER_H

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
#include <iostream>
#include <fstream>
#include <functional>
#include <cctype>

// Socket / net includes
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

#if defined(__linux__)
  #include <sys/syscall.h>
  #ifndef MFD_CLOEXEC
    #define MFD_CLOEXEC 0x0001U
  #endif
  #include <unistd.h>
#endif

#include <simgrid/s4u.hpp>

#include "CGSim.h"
#include "cnpy.h"
#include "job.h"

namespace sg4 = simgrid::s4u;

// ======================= Low-level socket + npy transport =======================
class PYTHON_DISPATCHER
{
public:
  PYTHON_DISPATCHER() = default;
  virtual ~PYTHON_DISPATCHER() = default;

  // Connection management
  void connectPython(const std::string& host, int port);
  void disconnectPython();

  // Public protocol helpers used by the dispatcher wrapper
  void connectToPythonServer(const std::string& host, int port);
  void sendMatrix(const double* data, const std::vector<size_t>& shape);
  cnpy::NpyArray waitDecision(const std::string& tmp_path);

protected:
  int py_sock_ = -1;

  // Shared I/O helpers (STATIC to allow calling from const methods)
  static void     recv_all_(int sock, void* out, size_t n);
  static void     send_all_(int sock, const void* data, size_t n);
  static uint64_t recv_u64_be_(int sock);
  static void     send_u64_be_(int sock, uint64_t x);

  std::string receiveMessage_(int sock) const;
  void        sendMessage_(int sock, const std::string& msg) const;

  cnpy::NpyArray receiveData_(int sock, const std::string& tmp_path) const;

  template <typename T>
  void sendData_(int sock, const T* data, const std::vector<size_t>& shape) const;
};

// ======================= Dispatcher wrapper (site assign) =======================
class pythonDispatcher : public PYTHON_DISPATCHER
{
public:
  pythonDispatcher() = default;
  ~pythonDispatcher() override = default;

  // Batch assignment: sets jobs[j]->comp_site (site-only decision from Python)
  std::vector<Job*> assignJob(std::vector<Job*>& jobs);

  // Single-job convenience wrapper (keeps protocol batch internally, J=1)
  Job* assignJob(Job* job)
  {
    if (!job) return nullptr;
    std::vector<Job*> v;
    v.push_back(job);
    auto out = assignJob(v);
    return out.empty() ? job : out[0];
  }

  // Optional: allow caller to set/change temp path used by waitDecision()
  void setTmpPath(std::string p) { tmp_path_ = std::move(p); }
  const std::string& getTmpPath() const { return tmp_path_; }

private:
  bool features_registered_ = false;
  std::vector<std::string> site_feature_names_;
  std::vector<std::string> job_feature_names_;

  std::string tmp_path_ = "/tmp/py_dispatcher_decision.npy";
};

#endif // PYTHON_DISPATCHER_H
