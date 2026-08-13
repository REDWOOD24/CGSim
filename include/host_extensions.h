#ifndef HOST_EXTENSIONS_H
#define HOST_EXTENSIONS_H

#include <simgrid/s4u.hpp>
#include <xbt/Extendable.hpp>
#include <set>
#include <string>
#include "job.h"
#include "site_manager.h"
#include <simgrid/simcall.hpp>

class Actions;
class JOB_EXECUTOR;

class HostExtensions {
public:
  static simgrid::xbt::Extension<simgrid::s4u::Host, HostExtensions> EXTENSION_ID;
  explicit HostExtensions(const simgrid::s4u::Host* h)
      : host(h), cores_used(0), cores_available(h->get_core_count()), name(h->get_name()) {}

  HostExtensions(const HostExtensions&) = delete;
  HostExtensions& operator=(const HostExtensions&) = delete;

  [[nodiscard]] unsigned int get_cores_used() const;
  [[nodiscard]] unsigned int get_cores_available() const;

private:

  void registerJob(Job* j);
  void onJobFinish(Job* j);

  const simgrid::s4u::Host* host;
  unsigned int cores_used;
  unsigned int cores_available;
  bool         available = true; //CPU being available means at least 1 core is free.
  std::set<std::string> job_ids;
  std::string name;

  friend class Actions;
  friend class JOB_EXECUTOR;
};

void host_extension_init();

#endif // HOST_EXTENSIONS_H
