#ifndef HOST_EXTENSIONS_H
#define HOST_EXTENSIONS_H

#include <simgrid/s4u.hpp>
#include <xbt/Extendable.hpp>
#include <set>
#include <string>
#include "job.h"
#include <simgrid/simcall.hpp>

class HostExtensions {
public:
  static simgrid::xbt::Extension<simgrid::s4u::Host, HostExtensions> EXTENSION_ID;
  explicit HostExtensions(const simgrid::s4u::Host* h)
      : cores_used(0), cores_available(h->get_core_count()), name(h->get_name()) {}

  HostExtensions(const HostExtensions&) = delete;
  HostExtensions& operator=(const HostExtensions&) = delete;

  void registerJob(Job* j);
  void onJobFinish(Job* j);

  [[nodiscard]] unsigned int get_cores_used() const;
  [[nodiscard]] unsigned int get_cores_available() const;

private:
  unsigned int cores_used;
  unsigned int cores_available;
  std::set<std::string> job_ids;
  std::string name;
};

void host_extension_init();

#endif // HOST_EXTENSIONS_H
