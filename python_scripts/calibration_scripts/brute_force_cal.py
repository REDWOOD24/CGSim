import os
import json
import pandas as pd
import time
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

def run_simulation(site, cpu_min_max, speed_precision, CPUSpeed, run_tag):
    """
    Run the simulator after updating the configuration and site info.
    
    The parameter `run_tag` is appended to the output file names (so that
    files from different simulation runs do not clash).
    
    Returns:
        single_core_mean_abs_error, multi_core_mean_abs_error
    """
    parameterValueDict = {
        "Num_of_Jobs": 10,
        "cpu_min_max": cpu_min_max,
        "cpu_speed_precision": speed_precision,
        "Sites": [site],
        "Output_DB": f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/NET2_jobs_output_{run_tag}.db",
        "Input_Job_CSV": "/home/sairam/ATLASGRID/ATLAS-GRID-SIMULATION/data/NET2_jobs_jan.csv"
    }
    update_cfg(config_path, parameterValueDict)
    update_site_info(site_info_path, site, CPUSpeed)
    
    # Run the simulator command.
    os.system(command)
    time.sleep(2)  # Allow some time for the simulation to complete.
    
    # Read the output CSV (assuming the simulator creates a CSV with a similar filename).
    output_file_csv = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/NET2_jobs_output_{run_tag}.csv"
    try:
        df_raw = pd.read_csv(output_file_csv)
    except Exception as e:
        print(f"Error reading output file for run {run_tag}: {e}")
        df_raw = None

    # Initialize errors.
    single_core_mean_abs_error = float('inf')
    multi_core_mean_abs_error = float('inf')

    if df_raw is not None:
        # Process Single-Core jobs:
        df_single = df_raw[(df_raw['STATUS'] == "finished") & (df_raw['CORES'] == 1)]
        if not df_single.empty:
            df_single['error'] = df_single['CPU_CONSUMPTION_TIME'] - df_single['EXECUTION_TIME']
            df_single['absolute_error'] = df_single['error'].abs()
            single_core_mean_abs_error = df_single['absolute_error'].mean()
        
        # Process Multi-Core jobs (8 cores):
        df_multi = df_raw[(df_raw['STATUS'] == "finished") & (df_raw['CORES'] == 8)]
        if not df_multi.empty:
            df_multi['error'] = (df_multi['CPU_CONSUMPTION_TIME'] - df_multi['EXECUTION_TIME']) / 8
            df_multi['absolute_error'] = df_multi['error'].abs()
            multi_core_mean_abs_error = df_multi['absolute_error'].mean()
    
    # Clean up: delete the output file if it exists.
    output_file = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/NET2_jobs_output_{run_tag}.db"
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Deleted output file: {output_file}")
    
    return single_core_mean_abs_error, multi_core_mean_abs_error

# === Global settings and paths ===
sites = ["NET2_Amherst"]
cal_jobs = 100
error_threshold = 15

config_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config.json"
site_info_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/site_info_cpu.json"
command = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/build/atlas-grid-simulator -c /home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config.json"

cpu_speed_precision_range = [6, 7]

with open(site_info_path, 'r') as f:
    site_info = json.load(f)

# === Phase 1: Search for best cpu_min_max and speed_precision values using a single CPUSpeed ordering ===

best_error_single_core = float('inf')
best_combination_single = None

best_error_multi_core = float('inf')
best_combination_multi = None

for site in sites:
    for speed_precision in cpu_speed_precision_range:
        for i in range(1, 9):
            for j in range(0, 8):
                if i + 1 + j <= 9:
                    cpu_min_max = [i, i + 1 + j]
                    # Generate one CPUSpeed ordering for the current configuration.
                    CPUSpeed = [random.randint(i, i + 1 + j) * (10 ** speed_precision) 
                                for _ in range(site_info[site]['CPUCount'])]
                    run_tag = f"phase1_{site}_{i}_{i+1+j}_{speed_precision}"
                    
                    single_err, multi_err = run_simulation(site, cpu_min_max, speed_precision, CPUSpeed, run_tag)
                    
                    print(f"Phase 1: Run for site {site} with cpu_min_max={cpu_min_max}, speed_precision={speed_precision}")
                    print(f"    Single-core mean absolute error: {single_err}")
                    print(f"    Multi-core mean absolute error: {multi_err}")
                    
                    # Update best combination for Single-Core.
                    if single_err < best_error_single_core:
                        best_error_single_core = single_err
                        best_combination_single = {"site": site, "cpu_min_max": cpu_min_max, "speed_precision": speed_precision}
                    
                    # Update best combination for Multi-Core.
                    if multi_err < best_error_multi_core:
                        best_error_multi_core = multi_err
                        best_combination_multi = {"site": site, "cpu_min_max": cpu_min_max, "speed_precision": speed_precision}

print("\nPhase 1 completed.")
print("\nBest Single-Core Configuration:")
print(best_combination_single, "with mean absolute error:", best_error_single_core)
print("\nBest Multi-Core (8 cores) Configuration:")
print(best_combination_multi, "with mean absolute error:", best_error_multi_core)

# === Phase 2: Fine-tune CPUSpeed ordering using the best configuration from Phase 1 ===

num_trials = 5  # Adjust the number of additional CPUSpeed ordering trials as needed

# Single-Core refinement:
print("\nPhase 2: Fine-tuning CPUSpeed ordering for best Single-Core configuration.")
if best_combination_single:
    best_site = best_combination_single["site"]
    best_cpu_min_max = best_combination_single["cpu_min_max"]
    best_speed_precision = best_combination_single["speed_precision"]
    
    best_order_single = None
    best_order_error_single = best_error_single_core  # initialize with the error from phase 1
    
    for trial in range(1, num_trials + 1):
        CPUSpeed = [random.randint(best_cpu_min_max[0], best_cpu_min_max[1]) * (10 ** best_speed_precision) 
                    for _ in range(site_info[best_site]['CPUCount'])]
        run_tag = f"phase2_single_{trial}"
        single_err, _ = run_simulation(best_site, best_cpu_min_max, best_speed_precision, CPUSpeed, run_tag)
        print(f"Trial {trial}: CPUSpeed = {CPUSpeed}, Single-core error = {single_err}")
        if single_err < best_order_error_single:
            best_order_error_single = single_err
            best_order_single = CPUSpeed
    print("\nBest CPUSpeed ordering for Single-Core configuration:")
    print(best_order_single, "with mean absolute error:", best_order_error_single)

# Multi-Core refinement:
print("\nPhase 2: Fine-tuning CPUSpeed ordering for best Multi-Core configuration.")
if best_combination_multi:
    best_site = best_combination_multi["site"]
    best_cpu_min_max = best_combination_multi["cpu_min_max"]
    best_speed_precision = best_combination_multi["speed_precision"]
    
    best_order_multi = None
    best_order_error_multi = best_error_multi_core
    
    for trial in range(1, num_trials + 1):
        CPUSpeed = [random.randint(best_cpu_min_max[0], best_cpu_min_max[1]) * (10 ** best_speed_precision)
                    for _ in range(site_info[best_site]['CPUCount'])]
        run_tag = f"phase2_multi_{trial}"
        _, multi_err = run_simulation(best_site, best_cpu_min_max, best_speed_precision, CPUSpeed, run_tag)
        print(f"Trial {trial}: CPUSpeed = {CPUSpeed}, Multi-core error = {multi_err}")
        if multi_err < best_order_error_multi:
            best_order_error_multi = multi_err
            best_order_multi = CPUSpeed
    print("\nBest CPUSpeed ordering for Multi-Core configuration:")
    print(best_order_multi, "with mean absolute error:", best_order_error_multi)
