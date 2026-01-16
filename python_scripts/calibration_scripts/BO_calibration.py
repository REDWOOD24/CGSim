import os
import json
import pandas as pd
import time
import random
import optuna
import math

# --------------------------- Helper Functions ---------------------------

def prepare_input_csv(path,site, cal_jobs):
    df = pd.read_csv(path)
    df = df[df['computingsite'] == site]
    df = df.sample(n=cal_jobs, random_state=1)
    df.to_csv(f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/{site}_{cal_jobs}_by_site.csv", index=False)
    

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

def run_simulation(site,cal_jobs, cpu_min_max, speed_precision, CPUSpeed, run_tag):
    """
    Run the simulator after updating the configuration and site info.
    The parameter `run_tag` is appended to the output file names.
    Returns:
        single_core_mean_abs_error, multi_core_mean_abs_error
    """
    parameterValueDict = {
        "Num_of_Jobs": cal_jobs,
        "cpu_min_max": cpu_min_max,
        "cpu_speed_precision": speed_precision,
        "Sites": [site],
        "Output_DB": f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/{site}_jobs_output_{run_tag}.db",
        "Input_Job_CSV": f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/{site}_{cal_jobs}_by_site.csv"
    }
    prepare_input_csv("/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/jan_100_by_site.csv", site, cal_jobs)
    update_cfg(config_path, parameterValueDict)
    update_site_info(site_info_path, site, CPUSpeed)
    
    # Run the simulator command.
    os.system(command)
    time.sleep(2)  # Allow some time for the simulation to complete.
    
    # Read the output CSV.
    output_file_csv = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/{site}_jobs_output_{run_tag}.csv"
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
            print(f"Single-core mean absolute error: {single_core_mean_abs_error}")
        
        # Process Multi-Core jobs (8 cores):
        df_multi = df_raw[(df_raw['STATUS'] == "finished") & (df_raw['CORES'] == 8)]
        if not df_multi.empty:
            df_multi['error'] = (df_multi['CPU_CONSUMPTION_TIME'] - df_multi['EXECUTION_TIME']) / 8
            df_multi['absolute_error'] = df_multi['error'].abs()
            multi_core_mean_abs_error = df_multi['absolute_error'].mean()
            print(f"Multi-core mean absolute error: {multi_core_mean_abs_error}")
    
    # Clean up: delete the output file if it exists.
    output_file = f"/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/output/{site}_jobs_output_{run_tag}.db"
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Deleted output file: {output_file}")
    
    return single_core_mean_abs_error, multi_core_mean_abs_error

# --------------------------- Global Settings ---------------------------
sites = ["NET2_Amherst"]
cal_jobs = 100
error_threshold = 15


config_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config_base.json"
site_info_path = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/site_info_cpu.json"
command = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/build/atlas-grid-simulator -c /home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config_base.json"

# Load site info (assumes site_info contains a key for each site with a CPUCount field)
with open(site_info_path, 'r') as f:
    site_info = json.load(f)

# --------------------------- Optuna Objective Function ---------------------------
def objective(trial):
    """
    Optuna objective function that uses the simulation as a black-box function.
    We optimize two parameters:
      - 'min_val' corresponds to the starting value in cpu_min_max.
      - 'range_offset' defines the offset so that cpu_min_max = [min_val, min_val + 1 + range_offset],
        with the constraint that (min_val + 1 + range_offset) <= 9.
      - 'speed_precision' is chosen from [6, 7].
    A CPUSpeed ordering is generated based on these, and the simulation is run.
    The objective returns the average of the single-core and multi-core mean absolute errors.
    """
    site = sites[0]  # Using the single site "NET2_Amherst"
    
    # Suggest values for cpu_min_max: choose min_val and range_offset with the constraint.
    min_val = trial.suggest_int("min_val", 1, 8)
    max_offset = 8 - min_val  # Because we need min_val + 1 + range_offset <= 9  -> range_offset <= (8 - min_val)
    range_offset = trial.suggest_int("range_offset", 0, max_offset)
    
    cpu_min_max = [min_val, min_val + 1 + range_offset]
    
    speed_precision = trial.suggest_categorical("speed_precision", [5, 6, 7, 8, 9, 10, 11])
    
    # Generate a CPUSpeed ordering for this configuration.
    CPUSpeed = [
        random.randint(min_val, min_val + 1 + range_offset) * (10 ** speed_precision)
        for _ in range(site_info[site]['CPUCount'])
    ]
    
    run_tag = f"optuna_{min_val}_{min_val+1+range_offset}_{speed_precision}_{trial.number}"
    single_err, multi_err = run_simulation(site, cal_jobs ,cpu_min_max, speed_precision, CPUSpeed, run_tag)
    
    if math.isfinite(single_err) and math.isfinite(multi_err):
    # Both present → normal average
        avg_error = 0.5 * (single_err + multi_err)
    elif math.isfinite(single_err):
        # No multi-core jobs → optimise on single-core only
        avg_error = single_err
    else:
        # Something really went wrong
        avg_error = float("inf")
    
    # Optionally, print/log the trial details.
    print(f"Trial {trial.number}: cpu_min_max={cpu_min_max}, speed_precision={speed_precision}, avg_error={avg_error}")
    
    return avg_error

# --------------------------- Main Optimization Routine ---------------------------
if __name__ == '__main__':
    # Create an Optuna study to minimize the average error.
    study = optuna.create_study(direction='minimize')
    
    # Set the number of trials.
    n_trials = 15  # Adjust based on available resources.
    
    study.optimize(objective, n_trials=n_trials)
    
    # Print the best trial information.
    print("\nOptimization completed.")
    print("Best trial:")
    best_trial = study.best_trial
    print(f"  Trial Number: {best_trial.number}")
    print(f"  Best Parameters: {best_trial.params}")
    print(f"  Best Average Error: {best_trial.value}")
