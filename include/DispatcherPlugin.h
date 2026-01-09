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
  virtual JobQueue getWorkload(long num_of_jobs) = 0;
  
  // Pure virtual function must be implemented by derived classes to assign Jobs
  virtual Job* assignJob(Job* job) = 0;
  
  // Virtual function can be implemented by derived classes when a job finishes
  virtual void onJobEnd(Job* job){};
  
  // Virtual function can be implemented by derived classes if they want to execute code on simulation end
  virtual void onSimulationEnd(){};

  // Pure virtual function must be implemented by derived classes to assign Resources
  virtual void getResourceInformation(simgrid::s4u::NetZone* platform) = 0;


  /*------------------------------------------------------------------------------*/

  // CGSim Initialization Level
  //virtual void onProgramStart(std::string& config_file){};
  //virtual void onPlatformCreation(std::string& config_file){};

  //CGSim Job level
  //virtual void onJobStatusAssignment(Job* job){};
  //virtual void onJobInPending(Job* job){};
  //virtual void onJobDispatch(Job* job){};



  //Hook to SimGrid activities
  // virtual void onReadActivityFinish (simgrid::s4u::Io   const& io,  std::string const& message) {};
  //virtual void onWriteActivityFinish(simgrid::s4u::Io   const& io,  std::string const& message) {};
  //virtual void onExecActivityFinish (simgrid::s4u::Exec const& ex,  std::string const& message) {};

};

#endif //DISPATCHERPLUGIN_H
