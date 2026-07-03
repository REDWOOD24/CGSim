#ifndef POLICY_H
#define POLICY_H

#include <iostream>
#include "CGSim.h"
#include "output.h"

class POLICY
{

public:
  POLICY(){};
 ~POLICY(){};
 
  void addPolicies();
  void make_background_transfer(const std::string& policy_name, const std::string& filename, 
    const std::string& src_site, const std::string& dst_site, CGSim::FileTransferDecisionMode& mode);
  CGSim::Policy* test_policy();
  void set_output(std::shared_ptr<OUTPUT> _ou){ou = std::move(_ou);}

private:
std::shared_ptr<OUTPUT> ou;
std::unordered_map<std::string, std::pair<sg4::CommPtr,bool>> active_background_transfers;
std::unordered_set<std::string> started_transfers; //Hack to avoid double start of comms in callback


};

#endif
