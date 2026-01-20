import os
import json
import pandas as pd
import time
import matplotlib.pyplot as plt
import random

def update_cfg(path, parameterValueDict):
    """
    Update the configuration file with the new parameters.
    """
    with open(path, 'r') as f:
        data = json.load(f)
    
    for parameter in parameterValueDict:
        data[parameter] = parameterValueDict[parameter]
        
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)

def update_site_info(path, site, cpu_speed):
    """
    Update the site information in the JSON file.
    """
    with open(path, 'r') as f:
        data = json.load(f)
    
    data[site]['CPUSpeed'] = cpu_speed
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)
 
# Sites to be calibrated
sites = ["NET2_Amherst"]
cal_jobs = 100
error_threshold = 15

config_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config.json"
site_info_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/site_info_cpu.json"
# Simulator command (ensure that the simulator uses the config file provided)
command = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/build/atlas-grid-simulator -c /home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config.json"

# Range of CPU speed precision values to test
cpu_speed_precision_range = [8]

# Initialize the best error values to infinity so that any valid result is lower.
best_error_single_core = float('inf')
best_error_multi_core = float('inf')
best_combination_single = None
best_combination_multi = None
with open(site_info_path, 'r') as f:
        site_info = json.load(f)

# Loop over sites, CPU speed precision, and cpu_min_max combinations
for site in sites:
    for speed_precision in cpu_speed_precision_range:
        for i in range(1, 9):
            for j in range(0, 8):
                if i + 1 + j <= 9:
                    # Prepare the configuration parameters
                    parameterValueDict = {}
                    parameterValueDict["Num_of_Jobs"] = 20
                    parameterValueDict["cpu_min_max"] = [i, i + 1 + j]
                    CPUSpeed = [ random.randint(i, i + 1 + j)*10**speed_precision for _ in range(site_info[site]['CPUCount'])]
                    print(CPUSpeed)
                    parameterValueDict["cpu_speed_precision"] = speed_precision
                    
                    # Use a site-specific value if needed.
                    parameterValueDict["Sites"] = [f"{site}"]
                    # Note: Output_DB is now a formatted string, not a list.
                    parameterValueDict["Output_DB"] = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/{site}_jobs_output_jan_{i}_{i+1+j}_{speed_precision}.db"
                    parameterValueDict["Input_Job_CSV"] = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/jan_10k_by_{site}.csv"
                    
                    # Update the configuration file
                    update_cfg(config_path, parameterValueDict)
                    update_site_info(site_info_path, site, CPUSpeed)
                    # Run the simulator command
                    os.system(command)
                    
                    # Sleep for 3 seconds to allow the command to complete processing.
                    time.sleep(3)
                    
                    # Define the path to the simulator output file
                    output_file = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/{site}_jobs_output_jan_{i}_{i+1+j}_{speed_precision}.db"
                    output_file_csv =f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/{site}_jobs_output_jan_{i}_{i+1+j}_{speed_precision}.csv"
                    # Attempt to read the output file once
                    try:
                        df_raw = pd.read_csv(output_file_csv)
                    except Exception as e:
                        print(f"Error reading output file for i={i}, j={i+1+j}, speed_precision={speed_precision} : {e}")
                        df_raw = None

                    # Initialize error metrics to infinity if reading fails.
                    single_core_mean_abs_error = float('inf')
                    multi_core_mean_abs_error = float('inf')

                    if df_raw is not None:
                        # Process Single-Core jobs
                        df_single = df_raw[(df_raw['STATUS'] == "finished") & (df_raw['CORES'] == 1)]
                        if not df_single.empty:
                            # Compute error as CPU consumption time minus execution time
                            df_single['error'] = df_single['CPU_CONSUMPTION_TIME'] - df_single['EXECUTION_TIME']
                            df_single['absolute_error'] = df_single['error'].abs()
                            single_core_mean_abs_error = df_single['absolute_error'].mean()
                        
                        # Process Multi-Core (8 cores) jobs
                        df_multi = df_raw[(df_raw['STATUS'] == "finished") & (df_raw['CORES'] == 8)]
                        print(df_multi)
                        if not df_multi.empty:
                            
                            # Error per core is computed as the difference divided by 8
                            df_multi['error'] = (df_multi['CPU_CONSUMPTION_TIME'] - df_multi['EXECUTION_TIME']) / 8
                            df_multi['absolute_error'] = df_multi['error'].abs()
                            multi_core_mean_abs_error = df_multi['absolute_error'].mean()
                    
                    # Print run results for the current configuration.
                    print(f"Run combination for site {site}: cpu_min_max={[i, i+1+j]}, speed_precision={speed_precision}")
                    print(f"    Single-core mean absolute error: {single_core_mean_abs_error}")
                    print(f"    Multi-core mean absolute error:  {multi_core_mean_abs_error}")
                    
                    # Update best configuration for Single-Core if the current error is lower.
                    if single_core_mean_abs_error < best_error_single_core:
                        best_error_single_core = single_core_mean_abs_error
                        best_combination_single = {"site": site, "cpu_min_max": [i, i+1+j], "speed_precision": speed_precision}
                    
                    # Update best configuration for Multi-Core (8 cores) if the current error is lower.
                    if multi_core_mean_abs_error < best_error_multi_core:
                        best_error_multi_core = multi_core_mean_abs_error
                        best_combination_multi = {"site": site, "cpu_min_max": [i, i+1+j], "speed_precision": speed_precision}
                    
                    # Optional: If you want to display plots for multi-core error, uncomment the block below.
                    # mean_abs_error = df_multi.groupby('SITE')['absolute_error'].mean()
                    # plt.figure(figsize=(10, 6))
                    # mean_abs_error.plot(kind='bar')
                    # plt.xlabel('SITE')
                    # plt.ylabel('Mean Absolute Error (Seconds)')
                    # plt.title('Mean Absolute Error by SITE (Multi-Core)')
                    # plt.tight_layout()
                    # plt.show()

                    # Delete the output file after computing the errors.
                    if os.path.exists(output_file):
                        os.remove(output_file)
                        print(f"Deleted output file: {output_file}")

# Print the best configurations after all combinations have been tried.
print("\nBest Single-Core Configuration:")
print(best_combination_single, "with mean absolute error:", best_error_single_core)

print("\nBest Multi-Core (8 cores) Configuration:")
print(best_combination_multi, "with mean absolute error:", best_error_multi_core)
