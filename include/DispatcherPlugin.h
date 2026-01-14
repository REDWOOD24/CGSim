#ifndef DISPATCHERPLUGIN_H
#define DISPATCHERPLUGIN_H

#include "job.h"
#include <simgrid/s4u.hpp>

class DispatcherPlugin {
public:
  // Constructor
  DispatcherPlugin() = default;

  // Destructor
  virtual ~DispatcherPlugin() = default;

  // Delete copy constructor and copy assignment operator
  DispatcherPlugin(const DispatcherPlugin&) = delete;
  DispatcherPlugin& operator=(const DispatcherPlugin&) = delete;
  
  // Delete move constructor and move assignment operator
  DispatcherPlugin(DispatcherPlugin&&) = delete;
  DispatcherPlugin& operator=(DispatcherPlugin&&) = delete;

  //Pure virtual function must be implemented by derived classes to get the Workload
  virtual JobQueue getWorkload() = 0;
  
  // Pure virtual function must be implemented by derived classes to assign Jobs
  virtual Job* assignJob(Job* job) = 0;

  /*-------------------------------------------------------------------------------------------*/

  // Virtual function can be implemented to execute code on simulation start
  virtual void onSimulationStart(){}

  // Virtual function can be implemented to execute code on simulation end
  virtual void onSimulationEnd(){}

  // Virtual function can be implemented when a job execution starts
  virtual void onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex){}

  // Virtual function can be implemented when a job execution finishes
  virtual void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex){}

  // Virtual function can be implemented when a job transfer starts
  virtual void onJobTransferStart(Job* job, simgrid::s4u::Mess const& me){}

  // Virtual function can be implemented when a job transfer ends
  virtual void onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me){}

  // Virtual function can be implemented when a file transfer starts
  virtual void onFileTransferStart(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site){}

  // Virtual function can be implemented when a file transfer ends
  virtual void onFileTransferEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site){}

  // Virtual function can be implemented when a file read starts
  virtual void onFileReadStart(Job* job,const std::string& filename, const long long filesize, simgrid::s4u::Io const& io){}

  // Virtual function can be implemented when a file read ends
  virtual void onFileReadEnd(Job* job,const std::string& filename, const long long filesize, simgrid::s4u::Io const& io){}

  // Virtual function can be implemented when a file write starts
  virtual void onFileWriteStart(Job* job,const std::string& filename, const long long filesize, simgrid::s4u::Io const& io){}

  // Virtual function can be implemented when a file write ends
  virtual void onFileWriteEnd(Job* job, const std::string& filename, const long long filesize, simgrid::s4u::Io const& io){}

};

#endif //DISPATCHERPLUGIN_H
