import os
import json
import pandas as pd
import time
import random
import optuna
import math
import shutil
import numpy as np # Added for mean calculation in RAE

# Small value to avoid division by zero in MAPE
EPSILON = 1e-9

# --------------------------- Helper Functions (Reduced Logging) ---------------------------

def prepare_input_csv(source_path, site, cal_jobs, target_dir):
    """Prepares the input CSV for a specific site and number of jobs. Quiet."""
    os.makedirs(target_dir, exist_ok=True)
    # Add random int to avoid potential naming clashes if cleanup fails
    target_path = os.path.join(target_dir, f"{site}_{cal_jobs}_by_site_{random.randint(1000,9999)}.csv")

    try:
        df = pd.read_csv(source_path)
        df_site = df[df['computingsite'] == site]
        if len(df_site) == 0:
             print(f"Error: No jobs found for site '{site}' in {source_path}")
             return None
        if len(df_site) < cal_jobs:
            print(f"Warning: Requested {cal_jobs} jobs for site {site}, but only {len(df_site)} available in source. Using all available.")
            df_sample = df_site.copy()
        else:
            # Vary random state for sampling different jobs each time if desired
            df_sample = df_site.sample(n=cal_jobs, random_state=random.randint(1, 10000))

        df_sample.to_csv(target_path, index=False)
        return target_path
    except FileNotFoundError:
        print(f"Error: Source input CSV not found at {source_path}")
        return None
    except Exception as e:
        print(f"Error preparing input CSV for {site}: {e}")
        return None


def update_cfg(path, parameterValueDict):
    """ Update the configuration file with the new parameters. Quiet. """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {path}")
        return False
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from config file {path}")
        return False

    for parameter, value in parameterValueDict.items():
        data[parameter] = value

    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except IOError as e:
        print(f"Error writing updated config file {path}: {e}")
        return False

def update_site_info(path, site, cpu_speed_list, verbose=False): # Added verbose flag
    """
    Update the site information (specifically CPUSpeed list) in the JSON file.
    Optionally prints success message.
    """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Site info file not found at {path}")
        return False
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from site info file {path}")
        return False

    if site not in data:
        print(f"Error: Site '{site}' not found in site info file {path}")
        return False

    data[site]['CPUSpeed'] = cpu_speed_list
    data[site]['CPUCount'] = len(cpu_speed_list)

    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        if verbose: # Only print if verbose is True
             print(f"Updated site info file '{path}' for site '{site}' with new CPUSpeed list (length {len(cpu_speed_list)}).")
        return True
    except IOError as e:
        print(f"Error writing updated site info file {path}: {e}")
        return False

def generate_cpuspeed(min_val, max_val, precision, num_cpus):
    """Generates a list of CPU speeds based on min/max values and precision. Quiet."""
    if not isinstance(num_cpus, int) or num_cpus <= 0:
         print(f"Error: Invalid number of CPUs ({num_cpus}). Must be a positive integer.")
         return []
    # Adjust precision interpretation if necessary - assuming 10**precision was intended scale factor
    scale_factor = 10 ** precision
    # Ensure min/max are integers before multiplying
    min_speed = int(min_val * scale_factor)
    max_speed = int(max_val * scale_factor)
    if min_speed > max_speed: # Ensure range is valid
        max_speed = min_speed
    return [
        random.randint(min_speed, max_speed)
        for _ in range(num_cpus)
    ]

def calculate_errors(df_raw):
    """
    Calculates MAE, MAPE, and RAE from simulation output DataFrame. Quiet.
    Returns dictionaries for single-core and multi-core errors.
    """
    results = {
        'single': {'mae': float('inf'), 'mape': float('inf'), 'rae': float('inf'), 'count': 0},
        'multi': {'mae': float('inf'), 'mape': float('inf'), 'rae': float('inf'), 'count': 0}
    }
    actual_col = 'EXECUTION_TIME'
    pred_col = 'CPU_CONSUMPTION_TIME' # This seems to be the 'predicted' equivalent

    if df_raw is not None and not df_raw.empty:
        df_finished = df_raw[df_raw['STATUS'] == "finished"].copy()
        df_finished[actual_col] = pd.to_numeric(df_finished[actual_col], errors='coerce')
        df_finished[pred_col] = pd.to_numeric(df_finished[pred_col], errors='coerce')
        df_finished.dropna(subset=[actual_col, pred_col], inplace=True)

        # --- Process Single-Core jobs ---
        df_single = df_finished[df_finished['CORES'] == 1].copy()
        if not df_single.empty:
            actual_single = df_single[actual_col]
            pred_single = df_single[pred_col]
            abs_error_single = (pred_single - actual_single).abs()

            results['single']['mae'] = abs_error_single.mean()
            # MAPE: Add EPSILON to actual_single to avoid division by zero
            results['single']['mape'] = (abs_error_single / (actual_single.abs() + EPSILON)).mean() * 100
            # RAE: Sum of absolute errors / Sum of absolute errors from mean prediction
            mean_actual_single = actual_single.mean()
            denom_rae_single = (actual_single - mean_actual_single).abs().sum()
            results['single']['rae'] = abs_error_single.sum() / (denom_rae_single + EPSILON)
            results['single']['count'] = len(df_single)

        # --- Process Multi-Core jobs (assuming 8 cores) ---
        # Important: Ensure '8' is the correct core count to filter for multi-core
        df_multi = df_finished[df_finished['CORES'] == 8].copy()
        if not df_multi.empty:
            # Scale prediction/actual based on cores? Original code adjusted the error.
            # Let's adjust the 'prediction' (CPU time) per core for comparison
            actual_multi = df_multi[actual_col] # Actual time is wall time
            pred_multi_per_core = df_multi[pred_col] / 8 # CPU time per core is the 'prediction' per core
            # Error calculation should be consistent. If EXECUTION_TIME is wall time,
            # compare it with CPU_CONSUMPTION_TIME / CORES.
            abs_error_multi = (pred_multi_per_core - actual_multi).abs()

            results['multi']['mae'] = abs_error_multi.mean()
            # MAPE: Add EPSILON to actual_multi to avoid division by zero
            results['multi']['mape'] = (abs_error_multi / (actual_multi.abs() + EPSILON)).mean() * 100
            # RAE: Sum of absolute errors / Sum of absolute errors from mean prediction
            mean_actual_multi = actual_multi.mean()
            denom_rae_multi = (actual_multi - mean_actual_multi).abs().sum()
            results['multi']['rae'] = abs_error_multi.sum() / (denom_rae_multi + EPSILON)
            results['multi']['count'] = len(df_multi)

    return results

def combine_errors(error_results, metric_key='mape'):
    """
    Combines single and multi-core errors for a specified metric ('mae', 'mape', 'rae').
    Returns the combined value.
    """
    if metric_key not in ['mae', 'mape', 'rae']:
        print(f"Error: Invalid metric_key '{metric_key}' in combine_errors. Defaulting to 'mape'.")
        metric_key = 'mape'

    single_err = error_results['single'][metric_key]
    multi_err = error_results['multi'][metric_key]

    # Simple average if both exist, otherwise use the one that exists.
    if math.isfinite(single_err) and math.isfinite(multi_err):
        # Avoid combining if counts are zero? For now, just average finite values.
        avg_error = 0.5 * (single_err + multi_err)
    elif math.isfinite(single_err):
        avg_error = single_err
    elif math.isfinite(multi_err):
         avg_error = multi_err
    else:
        avg_error = float("inf")
    return single_err


def run_simulation(site, cal_jobs, cpu_min_max, speed_precision, CPUSpeed, run_tag, base_config_path, site_info_path, sim_command, base_input_csv_path, output_dir, data_dir, optimization_metric_key):
    """
    Run the simulator after updating the configuration and site info. Quiet.
    Returns: Dictionary of detailed error metrics, and the combined value for the specified optimization_metric_key.
    """
    # 1. Prepare input CSV
    input_csv_path = prepare_input_csv(base_input_csv_path, site, cal_jobs, data_dir)
    if not input_csv_path:
        return None, float('inf') # Indicate failure

    # 2. Define output paths
    output_db = os.path.join(output_dir, f"{site}_jobs_output_{run_tag}.db")
    output_csv = os.path.join(output_dir, f"{site}_jobs_output_{run_tag}.csv")

    # 3. Create parameter dictionary
    parameterValueDict = {
        "Num_of_Jobs": cal_jobs,
        "cpu_min_max": cpu_min_max,
        "cpu_speed_precision": speed_precision, # Check simulator logic if this is needed when CPUSpeed is set
        "Sites": [site],
        "Output_DB": output_db,
        "Input_Job_CSV": input_csv_path
    }

    # 4. Update config file (quietly)
    if not update_cfg(base_config_path, parameterValueDict):
        if os.path.exists(input_csv_path): os.remove(input_csv_path)
        return None, float('inf')

    # 5. Update site info file (quietly, using verbose=False)
    if not update_site_info(site_info_path, site, CPUSpeed, verbose=False):
        if os.path.exists(input_csv_path): os.remove(input_csv_path)
        return None, float('inf')

    # 6. Run simulator
    status = os.system(sim_command + " > /dev/null 2>&1") # Redirect stdout/stderr

    # Add a small delay ONLY IF needed for filesystem consistency after sim finishes
    # time.sleep(0.5)

    if status != 0:
        print(f"Error: Simulator command failed for run {run_tag} with exit status {status}.")
        # Cleanup attempts
        for f_path in [input_csv_path, output_db, output_csv]:
             if f_path and os.path.exists(f_path):
                  try: os.remove(f_path)
                  except OSError: pass # Ignore cleanup errors here
        return None, float('inf')

    # 7. Read output CSV
    df_raw = None
    if os.path.exists(output_csv):
        try:
            # Add dtype specification if columns sometimes read incorrectly
            df_raw = pd.read_csv(output_csv)
        except pd.errors.EmptyDataError:
            print(f"Warning: Output CSV file is empty for run {run_tag}: {output_csv}")
        except Exception as e:
            print(f"Error reading output file {output_csv} for run {run_tag}: {e}")
    else:
        print(f"Warning: Output CSV file not found after simulation for run {run_tag}: {output_csv}")

    # 8. Calculate errors
    error_results = calculate_errors(df_raw)
    # Calculate the specific metric value needed for optimization
    combined_objective_value = combine_errors(error_results, metric_key=optimization_metric_key)

    # 9. Clean up run-specific files (quietly)
    for f_path in [input_csv_path, output_db, output_csv]:
        if f_path and os.path.exists(f_path):
             try:
                 os.remove(f_path)
             except OSError as e:
                 print(f"  Warning: Could not delete temp file {f_path}: {e}") # Keep cleanup warnings

    # Return the detailed results dictionary and the combined objective value
    # return only single core error
    return error_results, combined_objective_value

# --------------------------- Global Settings ---------------------------
BASE_DIR = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION" # Adjust if necessary
CONFIG_PATH = os.path.join(BASE_DIR, "config-files/config_base.json")
SITE_INFO_PATH = os.path.join(BASE_DIR, "data/site_info_cpu.json")
SIMULATOR_COMMAND = os.path.join(BASE_DIR, f"build/atlas-grid-simulator -c {CONFIG_PATH}")
BASE_INPUT_CSV_PATH = os.path.join(BASE_DIR, "data/jan_100_by_site.csv") # Source for job data
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DATA_DIR = os.path.join(BASE_DIR, "data") # Directory for prepared input files

# --- Simulation Parameters ---
# Specify ALL sites you want to optimize here
SITES_TO_OPTIMIZE =  pd.read_csv(BASE_INPUT_CSV_PATH)['computingsite'].unique().tolist() # Example: List multiple sites
SITES_TO_OPTIMIZE = ['NET2_Amherst'] # Example: List multiple sites
CALIBRATION_JOBS = 100 # Number of jobs to use for each simulation run

# --- Optimization Goal ---
# Choose 'MAE' or 'MAPE' (case-insensitive, will be converted to lower)
OPTIMIZATION_METRIC = 'MAPE'
OPTIMIZATION_METRIC_KEY = OPTIMIZATION_METRIC.lower() # Use lower-case key internally ('mae' or 'mape')
if OPTIMIZATION_METRIC_KEY not in ['mae', 'mape']:
    print(f"Warning: Invalid OPTIMIZATION_METRIC '{OPTIMIZATION_METRIC}'. Defaulting to 'MAPE'.")
    OPTIMIZATION_METRIC_KEY = 'mape'


# --- Optuna Parameters ---
N_OPTUNA_TRIALS = 30 # Number of trials for finding best cpu_min_max and precision (per site)

# --- CPUSpeed Search Parameters ---
N_CPUSPEED_TRIALS = 40 # Number of random CPUSpeed sets to test after finding best params (per site)

# Load site info ONCE at the start
try:
    with open(SITE_INFO_PATH, 'r') as f:
        site_info_data = json.load(f)
except FileNotFoundError:
    print(f"FATAL ERROR: Site info file not found at {SITE_INFO_PATH}. Exiting.")
    exit(1)
except json.JSONDecodeError:
    print(f"FATAL ERROR: Could not decode JSON from site info file {SITE_INFO_PATH}. Exiting.")
    exit(1)

# ------------------- Optuna Objective Function (Uses configured Metric) --------------------
def objective(trial, site, num_cpus, optimization_metric_key):
    """ Optuna objective function. Optimizes based on the configured metric (MAE or MAPE). """
    min_val = trial.suggest_int("min_val", 1, 8)
    max_offset = 8 - min_val
    range_offset = trial.suggest_int("range_offset", 0, max_offset)
    max_val = min_val + 1 + range_offset
    cpu_min_max = [min_val, max_val]
    # Ensure speed_precision is an integer
    speed_precision = int(trial.suggest_int("speed_precision", 5, 11))

    temp_CPUSpeed = generate_cpuspeed(min_val, max_val, speed_precision, num_cpus)
    if not temp_CPUSpeed:
         print(f"Optuna Trial {trial.number} (Site: {site}): Error generating CPUSpeed list. Returning infinite error.")
         # Inform Optuna this trial failed due to input generation
         raise optuna.exceptions.TrialPruned("CPUSpeed generation failed")


    run_tag = f"optuna_s{site}_t{trial.number}"

    # Pass the optimization_metric_key to run_simulation
    sim_results, combined_objective_value = run_simulation(
        site=site, cal_jobs=CALIBRATION_JOBS, cpu_min_max=cpu_min_max,
        speed_precision=speed_precision, CPUSpeed=temp_CPUSpeed, run_tag=run_tag,
        base_config_path=CONFIG_PATH, site_info_path=SITE_INFO_PATH,
        sim_command=SIMULATOR_COMMAND, base_input_csv_path=BASE_INPUT_CSV_PATH,
        output_dir=OUTPUT_DIR, data_dir=DATA_DIR,
        optimization_metric_key=optimization_metric_key
    )

    # Log the metric being optimized
    metric_name_upper = optimization_metric_key.upper()
    print(f"Optuna Trial {trial.number} (Site: {site}): Combined {metric_name_upper} = {combined_objective_value if math.isfinite(combined_objective_value) else 'Infinite'}")

    # Optionally log other metrics from sim_results for inspection
    if sim_results:
         single_mae = sim_results['single']['mae']
         multi_mae = sim_results['multi']['mae']
         single_mape = sim_results['single']['mape']
         multi_mape = sim_results['multi']['mape']
         print(f"    (Details: MAE(S):{single_mae:.2f} MAPE(S):{single_mape:.2f}% | MAE(M):{multi_mae:.2f} MAPE(M):{multi_mape:.2f}%)")

    # Handle simulation run failure
    if sim_results is None:
        print(f"Optuna Trial {trial.number} (Site: {site}): Simulation run failed. Pruning trial.")
        raise optuna.exceptions.TrialPruned("Simulation run failed")

    # Return the value Optuna should minimize
    # If the objective value is infinite, Optuna handles it as a poor trial
    return combined_objective_value


# --------------------------- Main Optimization Routine ---------------------------
if __name__ == '__main__':
    if not SITES_TO_OPTIMIZE:
        print("Error: No sites specified in SITES_TO_OPTIMIZE. Exiting.")
        exit(1)

    print(f"Starting optimization process for sites: {', '.join(SITES_TO_OPTIMIZE)}")
    print(f"Optimization Goal: Minimize Combined {OPTIMIZATION_METRIC_KEY.upper()}")
    print("-" * 60)

    # Dictionary to store results for all sites
    all_site_results = {}

    # --- Loop through each site to optimize ---
    for target_site in SITES_TO_OPTIMIZE:
        print(f"\n===== Processing Site: {target_site} =====")
        print("-" * 60)

        # Validate site exists in site_info_data and get CPU count
        if target_site not in site_info_data:
            print(f"FATAL ERROR: Target site '{target_site}' not found in {SITE_INFO_PATH}. Skipping site.")
            all_site_results[target_site] = {'status': 'error', 'message': 'Site not found in site_info file'}
            continue
        if 'CPUCount' not in site_info_data[target_site] or not isinstance(site_info_data[target_site]['CPUCount'], int) or site_info_data[target_site]['CPUCount'] <= 0:
             print(f"FATAL ERROR: Invalid or missing 'CPUCount' for site '{target_site}' in {SITE_INFO_PATH}. Skipping site.")
             all_site_results[target_site] = {'status': 'error', 'message': 'Invalid CPUCount in site_info file'}
             continue

        num_cpus_for_site = site_info_data[target_site]['CPUCount']
        print(f"Site '{target_site}' has {num_cpus_for_site} CPUs.")

        # == Phase 1: Optuna (Optimizing for configured Metric) ==
        print(f"\n=== Phase 1 (Site: {target_site}): Optuna ({N_OPTUNA_TRIALS} trials) ===")
        optuna.logging.set_verbosity(optuna.logging.WARNING) # Suppress Optuna's own logs

        # Use a unique study name per site
        study_name = f"optimization_{target_site}_{OPTIMIZATION_METRIC_KEY}"
        study = optuna.create_study(direction='minimize', study_name=study_name)

        try:
            # Pass the optimization metric key to the objective function lambda
            study.optimize(lambda trial: objective(trial, target_site, num_cpus_for_site, OPTIMIZATION_METRIC_KEY),
                           n_trials=N_OPTUNA_TRIALS)
        except Exception as e:
             print(f"Error during Optuna optimization for site {target_site}: {e}")
             all_site_results[target_site] = {'status': 'error', 'message': f'Optuna optimization failed: {e}'}
             continue # Skip to the next site

        optuna.logging.set_verbosity(optuna.logging.INFO) # Restore default logging

        print(f"\n--- Optuna Optimization Completed (Site: {target_site}) ---")
        try:
            best_trial = study.best_trial
            best_params_phase1 = best_trial.params
            best_value_phase1 = best_trial.value

            print(f"  Best Optuna Params (raw): {best_params_phase1}")
            print(f"  Best Optuna Combined {OPTIMIZATION_METRIC_KEY.upper()}: {best_value_phase1}")

            best_min_val = best_params_phase1['min_val']
            best_range_offset = best_params_phase1['range_offset']
            best_max_val = best_min_val + 1 + best_range_offset
            best_cpu_min_max = [best_min_val, best_max_val]
            best_speed_precision = int(best_params_phase1['speed_precision'])

            print(f"  --> Derived Best Params: cpu_min_max={best_cpu_min_max}, speed_precision={best_speed_precision}")

        except ValueError:
             print(f"\nError: Optuna study finished for site {target_site} without any successful trials. Cannot proceed for this site.")
             all_site_results[target_site] = {'status': 'error', 'message': 'Optuna study completed without successful trials'}
             continue # Skip to the next site


        # == Phase 2: CPUSpeed Search (Optimizing for configured Metric) ==
        print(f"\n=== Phase 2 (Site: {target_site}): CPUSpeed Search ({N_CPUSPEED_TRIALS} trials) ===")

        best_cpuspeed_list_found = None
        lowest_objective_value_phase2 = float('inf')
        best_result_details_phase2 = None # Store full results of the best run

        for i in range(N_CPUSPEED_TRIALS):
            current_cpuspeed_list = generate_cpuspeed(
                best_min_val, best_max_val, best_speed_precision, num_cpus_for_site
            )
            if not current_cpuspeed_list:
                print(f"CPUSpeed Trial {i+1}/{N_CPUSPEED_TRIALS} (Site: {target_site}): Error generating CPUSpeed list. Skipping.")
                continue

            run_tag = f"cpuspeed_s{target_site}_t{i+1}"

            sim_results, current_combined_objective_value = run_simulation(
                site=target_site, cal_jobs=CALIBRATION_JOBS,
                cpu_min_max=best_cpu_min_max, speed_precision=best_speed_precision,
                CPUSpeed=current_cpuspeed_list, run_tag=run_tag,
                base_config_path=CONFIG_PATH, site_info_path=SITE_INFO_PATH,
                sim_command=SIMULATOR_COMMAND, base_input_csv_path=BASE_INPUT_CSV_PATH,
                output_dir=OUTPUT_DIR, data_dir=DATA_DIR,
                optimization_metric_key=OPTIMIZATION_METRIC_KEY
            )

            # Log the metric being optimized
            metric_name_upper = OPTIMIZATION_METRIC_KEY.upper()
            print(f"CPUSpeed Trial {i+1}/{N_CPUSPEED_TRIALS} (Site: {target_site}): Combined {metric_name_upper} = {current_combined_objective_value if math.isfinite(current_combined_objective_value) else 'Infinite'}")

            # Optionally log other metrics
            if sim_results:
                 single_mae = sim_results['single']['mae']
                 multi_mae = sim_results['multi']['mae']
                 single_mape = sim_results['single']['mape']
                 multi_mape = sim_results['multi']['mape']
                 print(f"    (Details: MAE(S):{single_mae:.2f} MAPE(S):{single_mape:.2f}% | MAE(M):{multi_mae:.2f} MAPE(M):{multi_mape:.2f}%)")


            if math.isfinite(current_combined_objective_value) and current_combined_objective_value < lowest_objective_value_phase2:
                lowest_objective_value_phase2 = current_combined_objective_value
                best_cpuspeed_list_found = current_cpuspeed_list
                best_result_details_phase2 = sim_results # Save the detailed errors for the best list
                print(f"  >>> New best CPUSpeed list found with Combined {metric_name_upper}: {lowest_objective_value_phase2:.4f}")


        # == Phase 3: Final Update for the Current Site ==
        print(f"\n=== Phase 3 (Site: {target_site}): Final Update ===")

        site_result_data = {
            'status': 'completed',
            'optimized_metric': OPTIMIZATION_METRIC_KEY,
            'best_phase1_params': best_params_phase1,
            'best_phase1_value': best_value_phase1,
            'best_phase2_value': lowest_objective_value_phase2,
            'best_cpuspeed_list': best_cpuspeed_list_found,
            'best_result_details': best_result_details_phase2,
            'site_info_updated': False
        }

        if best_cpuspeed_list_found:
            print(f"  Lowest Combined {OPTIMIZATION_METRIC_KEY.upper()} found in search: {lowest_objective_value_phase2:.4f}")
            if best_result_details_phase2:
                 print(f"  Metrics for best CPUSpeed list:")
                 print(f"    Single-Core: MAE={best_result_details_phase2['single']['mae']:.2f}, MAPE={best_result_details_phase2['single']['mape']:.2f}%, RAE={best_result_details_phase2['single']['rae']:.3f} ({best_result_details_phase2['single']['count']} jobs)")
                 print(f"    Multi-Core:  MAE={best_result_details_phase2['multi']['mae']:.2f}, MAPE={best_result_details_phase2['multi']['mape']:.2f}%, RAE={best_result_details_phase2['multi']['rae']:.3f} ({best_result_details_phase2['multi']['count']} jobs)")

            print(f"\nUpdating '{SITE_INFO_PATH}' with the best CPUSpeed list for site '{target_site}'...")
            # Call update_site_info with verbose=True for the final confirmation
            if update_site_info(SITE_INFO_PATH, target_site, best_cpuspeed_list_found, verbose=True):
                site_result_data['site_info_updated'] = True
            else:
                print(f"Error: Failed to permanently update the site info file '{SITE_INFO_PATH}' for site {target_site}.")
                site_result_data['message'] = 'Failed to update site_info.json'
                site_result_data['status'] = 'warning'


        else:
            print(f"CPUSpeed search completed for site {target_site}, but no finite-error results were found or all trials failed.")
            print(f"The site info file '{SITE_INFO_PATH}' has NOT been updated for site {target_site}.")
            site_result_data['status'] = 'warning'
            site_result_data['message'] = 'No suitable CPUSpeed list found in Phase 2'

        # Store results for this site
        all_site_results[target_site] = site_result_data
        print("-" * 60)
        # End of loop for target_site

    # --- Final Summary Across All Sites ---
    print("\n" + "=" * 60)
    print("=== Overall Optimization Summary ===")
    print("=" * 60)

    for site, result in all_site_results.items():
        print(f"\n--- Site: {site} ---")
        print(f"  Status: {result.get('status', 'unknown')}")
        if result.get('status') == 'error':
            print(f"  Message: {result.get('message', 'No details')}")
            continue # Skip detailed results if basic error occurred

        metric_name_upper = result.get('optimized_metric', 'N/A').upper()
        print(f"  Optimized Metric: {metric_name_upper}")
        print(f"  Best Phase 1 Params: {result.get('best_phase1_params', 'N/A')}")
        print(f"  Best Phase 1 Combined {metric_name_upper}: {result.get('best_phase1_value', 'N/A'):.4f}")
        print(f"  Best Phase 2 Combined {metric_name_upper}: {result.get('best_phase2_value', 'N/A'):.4f}")
        if result.get('best_result_details'):
             details = result['best_result_details']
             print(f"  Metrics for Best CPUSpeed List:")
             print(f"    Single: MAE={details['single']['mae']:.2f}, MAPE={details['single']['mape']:.2f}%, RAE={details['single']['rae']:.3f} ({details['single']['count']} jobs)")
             print(f"    Multi:  MAE={details['multi']['mae']:.2f}, MAPE={details['multi']['mape']:.2f}%, RAE={details['multi']['rae']:.3f} ({details['multi']['count']} jobs)")
        else:
             print("  Metrics for Best CPUSpeed List: Not available.")

        if result.get('site_info_updated'):
            print(f"  Site Info File: Updated successfully.")
        elif result.get('status') == 'completed': # Completed but didn't update
            print(f"  Site Info File: Update failed or skipped (check logs).")
        else: # Warning status
             print(f"  Site Info File: Not updated ({result.get('message', 'Reason not specified')}).")

    print("\nOptimization process finished for all specified sites.")