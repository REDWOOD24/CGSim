#include "simple_dispatcher.h"

std::vector<std::string> parseCSVLine(const std::string& line) {
    std::vector<std::string> row;
    std::string cell;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
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
                            [](unsigned char c) { return !std::isprint(c); }),
                            field.end());
    }
    return row;
}

JobQueue SIMPLE_DISPATCHER::getJobs(long max_jobs) {
    std::priority_queue<Job*> jobs;
    std::string jobFile = "/Users/raekhan/CGSim/data/jan_100_BNL.csv";
    std::ifstream file(jobFile);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file: " + jobFile);
    }
    std::string line;
    std::map<std::string, int> column_map;
    bool header_parsed = false;
    while (std::getline(file, line)) {
        std::vector<std::string> row = parseCSVLine(line);
        if (max_jobs != -1 && static_cast<long>(jobs.size()) >= max_jobs) {
            break;
        }
        if (!header_parsed) {
            header_parsed = true;
            int column_index = 0;
            for (std::string& column_name : row) {
                std::transform(column_name.begin(), column_name.end(), column_name.begin(), ::toupper);
                column_map.insert({column_name, column_index});
                column_index++;
            }
            continue;
        }
        try {

            Job* job = new Job();  // Allocate memory dynamically
            job->jobid = std::stol(row[column_map.at("PANDAID")]);
            job->creation_time = row[column_map.at("CREATIONTIME")];
            job->job_status = row[column_map.at("JOBSTATUS")];
            job->job_name = row[column_map.at("JOBNAME")];
            job->cpu_consumption_time = std::stod(row[column_map.at("CPUCONSUMPTIONTIME")]);
            job->comp_site = row[column_map.at("COMPUTINGSITE")];
            job->destination_dataset_name = row[column_map.at("DESTINATIONDBLOCK")];
            job->destination_SE = row[column_map.at("DESTINATIONSE")];
            job->source_site = row[column_map.at("SOURCESITE")];
            job->transfer_type = row[column_map.at("TRANSFERTYPE")];
            job->cores = row[column_map.at("CORECOUNT")].empty() ? 0 : std::stol(row[column_map.at("CORECOUNT")]);
            job->no_of_inp_files = std::stoi(row[column_map.at("NINPUTDATAFILES")]);
            job->inp_file_bytes = std::stod(row[column_map.at("INPUTFILEBYTES")]);
            job->no_of_out_files = std::stoi(row[column_map.at("NOUTPUTDATAFILES")]);
            job->out_file_bytes = std::stod(row[column_map.at("OUTPUTFILEBYTES")]);
            job->pilot_error_code = row[column_map.at("PILOTERRORCODE")];
            job->exe_error_code = row[column_map.at("EXEERRORCODE")];
            job->ddm_error_code = row[column_map.at("DDMERRORCODE")];
            job->dispatcher_error_code = row[column_map.at("JOBDISPATCHERERRORCODE")];
            job->taskbuffer_error_code = row[column_map.at("TASKBUFFERERRORCODE")];
            job->status = row[column_map.at("JOBSTATUS")];

            std::string prefix = "/input/user.input." + std::to_string(job->jobid) + ".00000";
            std::string suffix = ".root";
            size_t size_per_inp_file = job->inp_file_bytes / job->no_of_inp_files;
            for (int file = 1; file <= job->no_of_inp_files; file++) {
                std::string name = prefix + std::to_string(file) + suffix;
                job->input_files[name] = size_per_inp_file;
            }
            prefix = "/output/user.output." + std::to_string(job->jobid) + ".00000";
            suffix = ".root";
            size_t size_per_out_file = job->out_file_bytes / job->no_of_out_files;
            for (int file = 1; file <= job->no_of_out_files; file++) {
                std::string name = prefix + std::to_string(file) + suffix;
                job->output_files[name] = size_per_out_file;
            }
            jobs.push(job);  // Push pointer to queue
        } catch (const std::exception& e) {
            //LOG_WARN("Skipping invalid row: {}", line); // currently reasons for being invalid 1) no_of_inp_files,no_of_inp_files  is empty 2)  no_of_out_files,out_file_bytes is empty
            //LOG_WARN("Reason: {}", e.what());
        }
    }
    file.close();
    return jobs;
}

void SIMPLE_DISPATCHER::setPlatform(sg4::NetZone* platform)
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
        if (gflops_str) { // Ensure it's not nullptr
            try {
                _site->gflops = std::stol(gflops_str);
            } catch (const std::exception& e) {
	      //LOG_ERROR("Error: Failed to convert 'gflops' to integer. Exception: {}", e.what());
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
            Disk* d        = new Disk;
            d->name        = disk->get_cname();
            d->mount       = disk->get_property("mount");
            d->storage     = (simgrid::fsmod::FileSystem::get_file_systems_by_netzone(site).at(_site->name+cpu->name+d->name+"filesystem")->get_free_space_at_path(d->mount))/1000;
            d->read_bw     = disk->get_read_bandwidth();
            d->write_bw    = disk->get_write_bandwidth();
            cpu->disks.push_back(d);
            cpu->disks_map[d->name] =d;
            }
            _site->cpus.push_back(cpu);
            _site->cpus_map[cpu->name] = cpu;

            //Site priority is determined by quality of cpus available
            _site->priority += cpu->speed/1e8 * this->weights.at("speed") + cpu->cores * this->weights.at("cores");
        }
        _site->priority    = std::round(_site->priority/_site->cpus.size()); //Normalize
        _site->cpus_in_use = 0;
        site_queue.push(_site);
        _sites_map[_site->name] = _site;
    }

  while (!site_queue.empty()) {_sites.push_back(site_queue.top()); site_queue.pop();}
}



double SIMPLE_DISPATCHER::calculateWeightedScore(Host* cpu, Job* j, std::string& best_disk_name)
{
    double score = cpu->speed/1e8 * weights.at("speed") + cpu->cores * weights.at("cores");
    double best_disk_score = std::numeric_limits<double>::lowest();
    size_t total_required_storage = (this->getTotalSize(j->input_files) + this->getTotalSize(j->output_files));
    for (const auto& d : cpu->disks) {
        if (d->storage >= total_required_storage) {
	  double disk_score = (d->read_bw/10) * weights.at("disk_read_bw") + (d->write_bw/10) * weights.at("disk_write_bw") + (d->storage/1e10) * weights.at("disk_storage");
            if (disk_score > best_disk_score) {
                best_disk_score = disk_score;
                best_disk_name = d->name;
            }
        }
    }
    score += best_disk_score * weights.at("disk");
    return score;
}

double SIMPLE_DISPATCHER::getTotalSize(const std::unordered_map<std::string, size_t>& files)
{
  size_t total_size = 0;
  for (const auto& file : files) {total_size += file.second;}
  return total_size;
}

Host* SIMPLE_DISPATCHER::findBestAvailableCPU(std::vector<Host*>& cpus, Job* j)
{
    Host* best_cpu = nullptr;
    std::string best_disk;
    double best_score = std::numeric_limits<double>::lowest();

    // Create a priority queue from the CPU candidates.
    std::priority_queue<Host*> cpu_queue;
    for (auto* cpu : cpus)
    {
        if (!cpu) {
	  std::cerr << "Warning: Encountered a null CPU pointer." << std::endl;
	  continue;
        }
        cpu_queue.push(cpu);
    }

    int candidatesExamined = 0;
    const int maxCandidates = 1;

    while (!cpu_queue.empty() && candidatesExamined < maxCandidates)
    {
        Host* current = cpu_queue.top();
        cpu_queue.pop();
        ++candidatesExamined;
	    //LOG_DEBUG("Available Cores {}", sg4::Host::by_name(current->name)->extension<HostExtensions>()->get_cores_available());
        //LOG_DEBUG("JOB Cores needed {}", j->cores);
        if (sg4::Host::by_name(current->name)->extension<HostExtensions>()->get_cores_available() < j->cores)
        {   
	      //LOG_DEBUG("Cores not suffficient for job {} on CPU {}", j->jobid, current->name);
	      continue;
        }

        // For now, using a dummy score.
        double score = 1;
        std::string current_disk = "";
        size_t total_required_storage = this->getTotalSize(j->input_files) 
                                        + this->getTotalSize(j->output_files);

        for (const auto& d : current->disks) {
            if (d->storage >= total_required_storage) {
                current_disk = d->name;
                break;
            }
        }
        if (current_disk == "") {
            continue;
        }
        if (score >= best_score)
        {
            best_score = score;
            best_cpu = current;
            best_disk = current_disk;
        }
        // NOTE: Original code had additional assignments that may be unintended;
        // they are commented out here.
        // best_cpu = current;
        // best_disk = current_disk;
    }

    if (best_cpu)
    {
        // Deduct CPU cores and assign job.
        sg4::Host::by_name(best_cpu->name)->extension<HostExtensions>()->registerJob(j); 
        best_cpu->jobs.insert(j->jobid);
        best_cpu->cores_available -= j->cores;

        // Deduct storage from the chosen disk.
        const auto totalSize = this->getTotalSize(j->input_files) 
                             + this->getTotalSize(j->output_files);
        for (auto& disk : best_cpu->disks)
        {
            if (disk->name == best_disk)
            {
                disk->storage -= totalSize;
                break;
            }
        }
        j->disk = best_disk;
        j->comp_host = best_cpu->name;
    }
    else {
      //LOG_DEBUG("Could not find a suitable CPU for job {}", j->jobid );
    }

    return best_cpu;
}




// Job* SIMPLE_DISPATCHER::assignJobToResource(Job* job)
// {
//   Host*  best_cpu    = nullptr;

//   for(int i = 0; i < _sites.size(); ++i)
//     {
//       current_site_index = use_round_robin ? (current_site_index + 1) % _sites.size() : current_site_index;
//       auto site          = _sites[current_site_index];
//       //Add new metric called Site pressure site->num_of_jobs_cores/site->num_of_cpus_cores, if pressure > 80% skip
//       use_round_robin    = !use_round_robin && (site->cpus_in_use >= site->cpus.size() / 2);
//       best_cpu           = findBestAvailableCPU(site->cpus, job);
//       if(best_cpu) {site->cpus_in_use++; job->comp_site = site->name; job->status = "assigned"; break;}
//     }
//   if(!best_cpu){job->status = "pending";}
//   this->printJobInfo(job);
//   return job;
  
// }

Job* SIMPLE_DISPATCHER::assignJobToResource(Job* job)
{
  Host*  best_cpu    = nullptr;
  Site*  site        = nullptr;
  
  //LOG_DEBUG(" Waiting to assign job resources : {}", job->comp_site);
//   std::string site_name = job->comp_site;
//   auto site = findSiteByName(_sites, site_name);
try {
  site = _sites_map.at(job->comp_site);
  //LOG_DEBUG(" Found the site {}", job->comp_site);
}
catch (const std::out_of_range& e) {
  //LOG_DEBUG("Computing Site is not found: {}", job->comp_site);
}

  if (job == nullptr) {
    //LOG_DEBUG("JOB pointer null");
       
    }
    if (site == nullptr) {
        job->status = "failed";
        //LOG_DEBUG("Computing Site is not found: Site pointer NULL :{}", job->comp_site);
        return job;
    }
  job->flops = site->gflops*job->cpu_consumption_time*job->cores;
  best_cpu           = findBestAvailableCPU(site->cpus, job);
  if(best_cpu) {
    site->cpus_in_use++; 
    job->comp_site = site->name; 
    job->status = "assigned"; 
    //LOG_DEBUG("Job Status changed to assigned");
}
  else{
  job->status = "pending";
  //LOG_DEBUG("Job Status changed to pending");

  }
  this->printJobInfo(job);
  return job;
  
}


void SIMPLE_DISPATCHER::free(Job* job)
{
 Host* cpu               = _sites_map.at(job->comp_site)->cpus_map.at(job->comp_host);
 if(cpu->jobs.count(job->jobid) > 0)
 {
   Disk* disk              = cpu->disks_map.at(job->disk);
   cpu->cores_available   += job->cores;
   disk->storage          += (this->getTotalSize(job->input_files) + this->getTotalSize(job->output_files));
   cpu->jobs.erase(job->jobid);
   //LOG_DEBUG("Job {} freed from CPU {}", job->jobid, cpu->name);
   
 }
}


Site* SIMPLE_DISPATCHER::findSiteByName(std::vector<Site*>& sites, const std::string& site_name) {
  auto it = std::find_if(sites.begin(), sites.end(),
                         [&site_name](Site* site) {

                             return site->name == site_name;
                         });
  return it != sites.end() ? *it : nullptr;
}



void SIMPLE_DISPATCHER::printJobInfo(Job* job)
{
  /*LOG_DEBUG("----------------------------------------------------------------------");
  LOG_INFO("Submitting .. {}", job->jobid);
  LOG_DEBUG("FLOPs to be executed: {}", job->flops);
  LOG_DEBUG("Files to be read:");
  for (const auto& file : job->input_files) {
    LOG_DEBUG("File: {:<40} Size: {:>10}", file.first, file.second);
  }
  LOG_DEBUG("Files to be written:");
  for (const auto& file : job->output_files) {
    LOG_DEBUG("File: {:<40} Size: {:>10}", file.first, file.second);
  }
  LOG_DEBUG("Cores Used: {}", job->cores);
  LOG_DEBUG("Disk Used: {}", job->disk);
  LOG_DEBUG("Host: {}", job->comp_host);
  LOG_DEBUG("----------------------------------------------------------------------");*/
}

void SIMPLE_DISPATCHER::cleanup()
{
	for(auto& s : _sites){for(auto& h : s->cpus){for(auto&d : h->disks){delete d;}h->disks.clear(); delete h;}s->cpus.clear();delete s;}
	_sites.clear();
}
