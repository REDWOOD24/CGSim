#include "simple_dispatcher.h"

Output* SIMPLE_DISPATCHER::output;

std::vector<std::string> parseCSVLine(const std::string& line) {
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
        // Remove surrounding quotes
        if (!field.empty() && field.front() == '"' && field.back() == '"') {
            field = field.substr(1, field.size() - 2);
        }
        // Remove non-printable characters
        field.erase(std::remove_if(field.begin(), field.end(),
                    [](unsigned char c) { return !std::isprint(c); }),
                    field.end());
    }
    return row;
}

// Helper function to safely get a column value
std::string getColumn(const std::vector<std::string>& row,
                      const std::unordered_map<std::string,int>& column_map,
                      const std::string& key,
                      const std::string& default_val = "")
{
    auto it = column_map.find(key);
    if (it == column_map.end() || it->second >= static_cast<int>(row.size()) || row[it->second].empty()) {
        return default_val;
    }
    return row[it->second];
}

JobQueue SIMPLE_DISPATCHER::getJobs(long max_jobs) {
    std::string jobFile = "/Users/raekhan/CGSim/new_data/mimic_job.csv";
    std::ifstream file(jobFile);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file: " + jobFile);
    }

    JobQueue jobs;
    std::string line;
    std::unordered_map<std::string, int> column_map;
    bool header_parsed = false;

    while (std::getline(file, line)) {
        auto row = parseCSVLine(line);

        if (!header_parsed) {
            header_parsed = true;
            for (int i = 0; i < static_cast<int>(row.size()); ++i) {
                std::string col = row[i];
                // lowercase header names
                std::transform(col.begin(), col.end(), col.begin(), ::tolower);
                column_map[col] = i;
            }
            continue;
        }

        if (max_jobs != -1 && static_cast<long>(jobs.size()) >= max_jobs) {
            break;
        }

        try {
            Job* job = new Job();

            // Safely get each column, providing defaults if missing
            job->jobid                 = std::stoll(getColumn(row, column_map, "pandaid", "0"));
            job->creation_time         = getColumn(row, column_map, "creationtime", "");
            job->job_status            = getColumn(row, column_map, "jobstatus", "");
            job->job_name              = getColumn(row, column_map, "jobname", "");
            job->cpu_consumption_time  = std::stod(getColumn(row, column_map, "cpuconsumptiontime", "0"));
            job->comp_site             = "AGLT2_site_"+getColumn(row, column_map, "computingsite", "");
            job->destination_dataset_name = getColumn(row, column_map, "destinationdblock", "");
            job->destination_SE        = getColumn(row, column_map, "destinationse", "");
            job->source_site           = getColumn(row, column_map, "sourcesite", "");
            job->transfer_type         = getColumn(row, column_map, "transfertype", "");
            job->core_count            = getColumn(row, column_map, "corecount", "0").empty() ? 0 : std::stoi(getColumn(row, column_map, "corecount", "0"));
            job->cores                 = getColumn(row, column_map, "corecount", "0").empty() ? 0 : std::stoi(getColumn(row, column_map, "corecount", "0"));
            job->no_of_inp_files       = std::stoi(getColumn(row, column_map, "ninputdatafiles", "0"));
            job->inp_file_bytes        = std::stod(getColumn(row, column_map, "inputfilebytes", "0"));
            job->no_of_out_files       = std::stoi(getColumn(row, column_map, "noutputdatafiles", "0"));
            job->out_file_bytes        = std::stod(getColumn(row, column_map, "outputfilebytes", "0"));
            job->pilot_error_code      = getColumn(row, column_map, "piloterrorcode", "");
            job->exe_error_code        = getColumn(row, column_map, "exeerrorcode", "");
            job->ddm_error_code        = getColumn(row, column_map, "ddmerrorcode", "");
            job->dispatcher_error_code = getColumn(row, column_map, "jobdispatchererrorcode", "");
            job->taskbuffer_error_code = getColumn(row, column_map, "taskbuffererrorcode", "");
            job->status                = "created";

            // ---- Parse input files JSON ----
            std::string json_str = getColumn(row, column_map, "files_info", "");
            if (!json_str.empty() && json_str.front() == '"' && json_str.back() == '"') {
                json_str = json_str.substr(1, json_str.size()-2);
            }
            json_str.erase(std::remove(json_str.begin(), json_str.end(), '{'), json_str.end());
            json_str.erase(std::remove(json_str.begin(), json_str.end(), '}'), json_str.end());

            std::stringstream ss(json_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                auto colon_pos = token.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = token.substr(0, colon_pos);
                    key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
                    key.erase(std::remove(key.begin(), key.end(), '"'), key.end());
                    job->input_files[key] = {0.0, {}};
                }
            }

            // ---- Generate output files ----
            long long size_per_out_file = job->no_of_out_files > 0 ? job->out_file_bytes / job->no_of_out_files : 0;
            for (int f = 1; f <= job->no_of_out_files; ++f) {
                std::string filename = "/output/user.output." + std::to_string(job->jobid) + ".0000" + std::to_string(f) + ".root";
                job->output_files[filename] = size_per_out_file;
            }

            jobs.push(job);
        } catch (const std::exception& e) {
            std::cerr << "Skipping invalid row: " << line << "\n";
            std::cerr << "Reason: " << e.what() << "\n";
        }
    }

    file.close();
    return jobs;
}


void SIMPLE_DISPATCHER::setPlatform(sg4::NetZone* platform)
{
    if (!platform) {
        std::cerr << "Error: Null platform passed to setPlatform!\n";
        return;
    }

    std::priority_queue<Site*> site_queue;
    this->platform = platform;

    auto all_sites = platform->get_children();
    for (const auto& site : all_sites)
    {
        const char* site_cname = site->get_cname();
        if (!site_cname) {
            std::cerr << "Warning: Site with null cname found, skipping.\n";
            continue;
        }

        std::string site_name(site_cname);
        if (site_name == "JOB-SERVER") continue; // No computation on Job server

        Site* _site = new Site;
        _site->name = site_name;

        // Storage capacity
        const char* storage_str = site->get_property("storage_capacity_bytes");
        _site->storage = storage_str ? std::stol(storage_str) : 0;

        // GFLOPS
        const char* gflops_str = site->get_property("gflops");
        _site->gflops = gflops_str ? std::stol(gflops_str) : 0;

        _site->priority = 0;
        _site->cpus_in_use = 0;

        for (const auto& host : site->get_all_hosts())
        {
            if (!host) {
                std::cerr << "Warning: Null host found in site " << _site->name << ", skipping.\n";
                continue;
            }
            if ((host->get_name()).find("_communication") != std::string::npos) continue;
            Host* cpu = new Host;
            const char* host_cname = host->get_cname();
            cpu->name = host_cname ? host_cname : "UNKNOWN_HOST";
            cpu->cores = host->get_core_count();
            cpu->speed = host->get_speed();
            cpu->cores_available = cpu->cores;

            for (const auto& disk : host->get_disks())
            {
                if (!disk) {
                    std::cerr << "Warning: Null disk found in host " << cpu->name << ", skipping.\n";
                    continue;
                }

                Disk* d = new Disk;
                const char* disk_cname = disk->get_cname();
                d->name = disk_cname ? disk_cname : "UNKNOWN_DISK";

                const char* mount_prop = disk->get_property("mount");
                d->mount = mount_prop ? mount_prop : "/";

                d->read_bw = disk->get_read_bandwidth();
                d->write_bw = disk->get_write_bandwidth();

                cpu->disks.push_back(d);
                cpu->disks_map[d->name] = d;
            }

            _site->cpus.push_back(cpu);
            _site->cpus_map[cpu->name] = cpu;

            // Site priority is determined by quality of CPUs available
            _site->priority += cpu->speed / 1e8 * this->weights.at("speed") + cpu->cores * this->weights.at("cores");
        }

        if (!_site->cpus.empty()) {
            _site->priority = std::round(_site->priority / _site->cpus.size()); // Normalize
        }

        site_queue.push(_site);
        _sites_map[_site->name] = _site;
    }

    // Flatten priority queue into _sites vector
    while (!site_queue.empty()) {
        _sites.push_back(site_queue.top());
        site_queue.pop();
    }
}




double SIMPLE_DISPATCHER::calculateWeightedScore(Host* cpu, Job* j, std::string& best_disk_name)
{
    double score = cpu->speed/1e8 * weights.at("speed") + cpu->cores_available * weights.at("cores");
    double best_disk_score = std::numeric_limits<double>::lowest();
    for (const auto& d : cpu->disks) {
	      double disk_score = (d->read_bw/1e12 ) * weights.at("disk_read_bw") + (d->write_bw/1e12) * weights.at("disk_write_bw");
           if (disk_score > best_disk_score) {
               best_disk_score = disk_score;
               best_disk_name = d->name;
            }
    }
    score += best_disk_score * weights.at("disk");
    return score;
}

Host* SIMPLE_DISPATCHER::findBestAvailableCPU(std::vector<Host*>& cpus, Job* j)
{
    Host*          best_cpu       = nullptr;
    std::string    best_disk;
    double         best_score     = std::numeric_limits<double>::lowest();
    int           _search_depth   = 0;

    std::priority_queue<Host*> cpu_queue;
    for (const auto& cpu : cpus) { cpu_queue.push(cpu);}

    while(!cpu_queue.empty())
    {
        Host* current_best_cpu = cpu_queue.top();
        cpu_queue.pop();
        if(current_best_cpu->cores_available < j->cores) continue;
        std::string current_best_disk = "";
        double score = calculateWeightedScore(current_best_cpu, j, current_best_disk);
        if(current_best_disk.empty()) continue; //Not enough disk space left
        if (score > best_score)
        {
            best_score          = score;
            best_cpu            = current_best_cpu;
            best_disk           = current_best_disk;
        }
        //if(_search_depth++ > 10) break; //Optimization to not loop over too many CPUs
        //cpu_queue.pop();
    }

    if(best_cpu) //Found a CPU. Deduct storage from the selected disk.
    {
        best_cpu->jobs.insert(j->jobid);
        best_cpu->cores_available  -= j->cores;

        for(auto& d: best_cpu->disks)
        //{if(d->name == best_disk){d->storage -= (this->getTotalSize(j->input_files) + this->getTotalSize(j->output_files));}}

        j->disk       =  best_disk;
        j->comp_host  =  best_cpu->name;
    }

    return best_cpu;
}

Job* SIMPLE_DISPATCHER::assignJobToResource(Job* job)
{
  Host*  best_cpu    = nullptr;
  Site*  site        = nullptr;
  

try {
  job->comp_site = "AGLT2_site_0";
  site = _sites_map.at(job->comp_site);
}
catch (const std::out_of_range& e) {
}

  if (job == nullptr) {
    //LOG_DEBUG("JOB pointer null");

    }
    if (site == nullptr) {
        job->status = "failed";
        return job;
    }

  job->flops = site->gflops*job->cpu_consumption_time*job->cores;
  best_cpu           = findBestAvailableCPU(site->cpus, job);
  if(best_cpu) {
    site->cpus_in_use++; 
    job->status = "assigned";
}
  else{
  job->status = "pending";
  }
  return job;
  
}


void SIMPLE_DISPATCHER::free(Job* job)
{
 Host* cpu               = _sites_map.at(job->comp_site)->cpus_map.at(job->comp_host);
 cpu->cores_available   += job->cores;

}


Site* SIMPLE_DISPATCHER::findSiteByName(std::vector<Site*>& sites, const std::string& site_name) {
  auto it = std::find_if(sites.begin(), sites.end(),
                         [&site_name](Site* site) {

                             return site->name == site_name;
                         });
  return it != sites.end() ? *it : nullptr;
}

void SIMPLE_DISPATCHER::cleanup()
{
	for(auto& s : _sites){for(auto& h : s->cpus){for(auto&d : h->disks){delete d;}h->disks.clear(); delete h;}s->cpus.clear();delete s;}
	_sites.clear();
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