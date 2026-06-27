#include "track4_workload_manager.h"


std::vector<std::string> TRACK4_WORKLOAD_MANAGER::parseCSVLine(const std::string& line) {
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
std::string TRACK4_WORKLOAD_MANAGER::getColumn(const std::vector<std::string>& row,
                      const std::unordered_map<std::string,int>& column_map,
                      const std::string& key,
                      const std::string& default_val)
{
    auto it = column_map.find(key);
    if (it == column_map.end() || it->second >= static_cast<int>(row.size()) || row[it->second].empty()) {
        return default_val;
    }
    return row[it->second];
}

JobQueue TRACK4_WORKLOAD_MANAGER::getWorkload() {

    auto platform = sg4::Engine::get_instance()->get_netzone_root();
    std::string jobFile = platform->get_property("jobs_file");
    long max_jobs = std::stol(platform->get_property("Num_of_Jobs"));
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
            job->creation_time         = std::stod(getColumn(row, column_map, "creationtime_sec", "0"));
            job->cpu_consumption_time  = std::stod(getColumn(row, column_map, "cpuconsumptiontime", "0"));
            job->comp_site             = getColumn(row, column_map, "computingsite", "");
            job->cores                 = getColumn(row, column_map, "corecount", "0").empty() ? 0 : std::stoi(getColumn(row, column_map, "corecount", "0"));
            job->status                = "created";
            job->retries               = 0;
            //job->add_child(new Job(), 0.0);


            int no_of_out_files       = 1;/*std::stoi(getColumn(row, column_map, "noutputdatafiles", "0"));*/
            double out_file_bytes        = std::stod(getColumn(row, column_map, "outputfilebytes", "0"));


            unsigned long long size_per_out_file = no_of_out_files > 0 ? out_file_bytes / no_of_out_files : 0;
            for (int f = 1; f <= no_of_out_files; ++f) {
                std::string filename = "user.output." + std::to_string(job->jobid) + ".0000" + std::to_string(f) + ".root";
                job->output_files[filename] = size_per_out_file/10000;
            }

            int no_of_inp_files       = 1;/*std::stoi(getColumn(row, column_map, "ninputdatafiles", "0"));*/
            double inp_file_bytes        = std::stod(getColumn(row, column_map, "inputfilebytes", "0"));

            unsigned long long size_per_inp_file = no_of_inp_files > 0 ? inp_file_bytes / no_of_inp_files : 0;
            for (int f = 1; f <= no_of_inp_files; ++f) {
                std::string filename = "user.input." + std::to_string(job->jobid) + ".0000" + std::to_string(f) + ".root";
                job->input_files[filename] = {0.0, {}};
                CGSim::get_file_manager()->create(filename,size_per_inp_file/10000,job->comp_site);
            }

            if(job->comp_site!="") jobs.push(job);


        } catch (const std::exception& e) {
            std::cerr << "Skipping invalid row: " << line << "\n";
            std::cerr << "Reason: " << e.what() << "\n";
        }
    }
    file.close();
    return jobs;
}
