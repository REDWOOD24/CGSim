import os
import json
import pandas as pd
import time
import random
import sys
import numpy as np
import logging
import subprocess # Use subprocess instead of os.system
import copy # For deep copying configurations (if needed, though not strictly here)
import tempfile # For safer temporary file handling

# ------------------------ Configuration ------------------------

# CHOOSE METRIC: 'MAE', 'MAPE', or 'RAE'
OPTIMIZATION_METRIC = 'MAE'  # <--- SET YOUR DESIRED METRIC HERE

# --- Optimization Parameters ---
NUM_PARAM_TRIALS_PHASE1 = 20  # How many (cpu_min_max, precision) sets to try in Phase 1
NUM_CPUSPEED_EVALS_PER_PARAM = 3 # How many random CPUSpeed lists to evaluate for EACH param set in Phase 1
NUM_CPUSPEED_TRIALS_PHASE2 = 30 # How many CPUSpeed lists to try in Phase 2 with the best params
CALIBRATION_JOB_SAMPLE_SIZE = 50 # Number of jobs to sample for each simulation run

# --- Paths ---
# Use absolute paths or ensure they are correct relative to execution directory
BASE_DIR = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION" # Adjust if needed
SOURCE_CSV_PATH = os.path.join(BASE_DIR, "data/jan_1k_by_site.csv")
CONFIG_PATH = os.path.join(BASE_DIR, "config-files/config_base.json")
SITE_INFO_PATH = os.path.join(BASE_DIR, "data/site_info_cpu_calib.json")
SIMULATOR_EXECUTABLE = os.path.join(BASE_DIR, "build/atlas-grid-simulator")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
TEMP_DIR = os.path.join(BASE_DIR, "data/temp_inputs") # Directory for temporary input CSVs

# --- Site ---
SITE_NAME = "NET2_Amherst" # Example site

# --- Setup Basic Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.StreamHandler(sys.stdout) # Ensure logs go to stdout
    # Optionally add logging.FileHandler('optimization.log')
])

# --- Global Placeholder for Site Data ---
SITE_SOURCE_DATA = None


# ------------------------ Helper Functions ------------------------

def load_source_data(source_path, site):
    """Loads and filters the source CSV data for the target site once."""
    global SITE_SOURCE_DATA
    logging.info(f"Loading source data for site '{site}' from {source_path}...")
    try:
        df = pd.read_csv(source_path)
        SITE_SOURCE_DATA = df[df['computingsite'] == site].copy()
        if SITE_SOURCE_DATA.empty:
            logging.critical(f"No jobs found for site '{site}' in {source_path}. Exiting.")
            sys.exit(1)
        logging.info(f"Loaded {len(SITE_SOURCE_DATA)} jobs for site '{site}'.")
        # Convert potential object columns to numeric early if possible and necessary
        # Example: SITE_SOURCE_DATA['some_numeric_col'] = pd.to_numeric(SITE_SOURCE_DATA['some_numeric_col'], errors='coerce')
    except FileNotFoundError:
        logging.critical(f"Source input CSV not found at {source_path}. Exiting.")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Error loading source data: {e}. Exiting.")
        sys.exit(1)

def prepare_input_csv(site_df, cal_jobs, target_dir):
    """Prepares the input CSV from the pre-filtered site DataFrame."""
    os.makedirs(target_dir, exist_ok=True)
    # Use tempfile for slightly safer temporary file creation
    try:
        # Create a temporary file in the target directory
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', dir=target_dir, prefix=f"{SITE_NAME}_{cal_jobs}_") as temp_f:
            target_path = temp_f.name

        if len(site_df) < cal_jobs:
            logging.warning(f"Requested {cal_jobs} jobs for site {SITE_NAME}, but only {len(site_df)} available in source data. Using all available.")
            df_sample = site_df.copy()
        else:
            df_sample = site_df.sample(n=cal_jobs, random_state=random.randint(1, 10000))

        df_sample.to_csv(target_path, index=False)
        # logging.debug(f"Prepared temporary input CSV: {target_path}") # Optional Debug
        return target_path
    except Exception as e:
        logging.error(f"Error preparing input CSV in {target_dir}: {e}")
        # Attempt to clean up if file was created but writing failed
        if 'target_path' in locals() and os.path.exists(target_path):
            try: os.remove(target_path)
            except OSError: pass
        return None

def update_json_file(path, update_dict):
    """Reads, updates, and writes a JSON file."""
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        data.update(update_dict) # More direct update
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except FileNotFoundError:
        logging.error(f"File not found for update: {path}")
        return False
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {path}")
        return False
    except Exception as e:
        logging.error(f"Error updating JSON file {path}: {e}")
        return False

def update_site_info_file(path, site, cpu_speed_list):
    """Updates the CPUSpeed list for a specific site in the site info JSON."""
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        if site not in data:
            logging.error(f"Site '{site}' not found in {path}")
            return False
        if not isinstance(cpu_speed_list, list):
             logging.error(f"CPUSpeed must be a list for site info update.")
             return False
        data[site]['CPUSpeed'] = cpu_speed_list
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except FileNotFoundError:
        logging.error(f"File not found for update: {path}")
        return False
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {path}")
        return False
    except Exception as e:
        logging.error(f"Error updating site info file {path}: {e}")
        return False

def calculate_metric(df, core_type, metric_type):
    """
    Calculates MAE, MAPE, or RAE for single or multi-core jobs.
    (Code is identical to previous version, assuming it's correct)
    """
    if df.empty:
        return float('inf'), 0

    actual_col = 'CPU_CONSUMPTION_TIME'
    predicted_col = 'EXECUTION_TIME'
    cores_col = 'CORES'

    try: # Add try-except for robustness during calculation
        # --- Prepare actual and predicted values based on core type ---
        if core_type == 'single':
            actual = df[actual_col]
            predicted = df[predicted_col]
        elif core_type == 'multi':
            valid_cores = df[cores_col].replace(0, np.nan).dropna()
            if valid_cores.empty:
                logging.warning(f"No {core_type}-core jobs with valid core counts found for metric calc.")
                return float('inf'), 0
            df_valid_cores = df.loc[valid_cores.index]
            # Ensure actual and cores are numeric before division
            actual_numeric = pd.to_numeric(df_valid_cores[actual_col], errors='coerce')
            cores_numeric = pd.to_numeric(df_valid_cores[cores_col], errors='coerce')
            # Handle potential NaNs introduced by coerce
            valid_div_mask = ~(actual_numeric.isna() | cores_numeric.isna() | (cores_numeric == 0))
            if not valid_div_mask.any():
                 logging.warning(f"No {core_type}-core jobs with valid numeric times/cores for division.")
                 return float('inf'), 0

            actual = actual_numeric[valid_div_mask] / cores_numeric[valid_div_mask]
            predicted = pd.to_numeric(df_valid_cores.loc[valid_div_mask.index, predicted_col], errors='coerce') # Align predicted

            # Further filter df based on successful division and numeric prediction
            df = df_valid_cores.loc[actual.index].copy() # Use index from successful 'actual' calculation
            df[actual_col] = actual # Store calculated per-core actual
            df[predicted_col] = predicted # Store coerced predicted

        else:
            raise ValueError(f"Unknown core_type: {core_type}")

        # --- Clean data: Remove rows where actual or predicted are NaN/Inf ---
        # Re-extract columns after potential modifications for multi-core
        actual_final = df[actual_col]
        predicted_final = df[predicted_col]
        valid_mask = ~(actual_final.isna() | predicted_final.isna() | np.isinf(actual_final) | np.isinf(predicted_final))

        if not valid_mask.any():
            logging.warning(f"No valid (non-NaN/Inf) actual/predicted values after processing for {core_type}-core jobs.")
            return float('inf'), 0

        actual_valid = actual_final[valid_mask]
        predicted_valid = predicted_final[valid_mask]
        num_jobs = len(actual_valid)

        if num_jobs == 0:
             logging.warning(f"Zero valid jobs after cleaning for {core_type}-core.")
             return float('inf'), 0

        # --- Calculate the chosen metric ---
        metric_value = float('inf')

        if metric_type == 'MAE':
            metric_value = (actual_valid - predicted_valid).abs().mean()

        elif metric_type == 'MAPE':
            mape_mask = actual_valid.abs() > 1e-6 # Avoid division by zero
            if not mape_mask.any():
                 logging.warning(f"No {core_type}-core jobs with non-zero '{actual_col}' found for MAPE calculation.")
                 return float('inf'), 0
            actual_mape = actual_valid[mape_mask]
            predicted_mape = predicted_valid[mape_mask]
            num_jobs = len(actual_mape) # Update num_jobs for MAPE calculation
            if num_jobs == 0: return float('inf'), 0 # Should not happen if mape_mask.any() is true, but safe check
            metric_value = ((actual_mape - predicted_mape).abs() / actual_mape.abs()).mean() * 100.0 # Use .abs() on actual_mape

        elif metric_type == 'RAE':
            sum_abs_error = (actual_valid - predicted_valid).abs().sum()
            mean_actual = actual_valid.mean()
            sum_abs_dev_from_mean = (actual_valid - mean_actual).abs().sum()

            if sum_abs_dev_from_mean < 1e-9:
                metric_value = 0.0 if sum_abs_error < 1e-9 else float('inf')
                if metric_value == float('inf'):
                    logging.warning(f"RAE calculation failed for {core_type}-core due to zero deviation from mean actual.")
            else:
                metric_value = sum_abs_error / sum_abs_dev_from_mean

        else:
            raise ValueError(f"Unknown metric_type: {metric_type}")

        if np.isnan(metric_value) or np.isinf(metric_value):
            metric_value = float('inf')

        return metric_value, num_jobs

    except KeyError as e:
        logging.error(f"Missing column during metric calculation for {core_type}-core: {e}")
        return float('inf'), 0
    except Exception as e:
        logging.error(f"Error during metric calculation for {core_type}-core: {e}")
        return float('inf'), 0


def run_simulation(site, cal_jobs, cpu_min_max, speed_precision, CPUSpeed, run_tag, metric_to_optimize):
    """
    Runs a single simulation instance and returns the single-core metric.
    Uses subprocess and handles temporary file creation/cleanup.
    """
    input_csv_path = None # Initialize for cleanup
    output_file_csv = None
    output_file_db = None

    try:
        # 1. Prepare Input CSV (using pre-loaded data)
        if SITE_SOURCE_DATA is None:
             logging.error("Site source data not loaded. Cannot prepare input CSV.")
             return float('inf')
        input_csv_path = prepare_input_csv(SITE_SOURCE_DATA, cal_jobs, TEMP_DIR)
        if not input_csv_path:
            return float('inf') # Error logged in prepare_input_csv

        # Define output paths based on run_tag
        output_file_base = os.path.join(OUTPUT_DIR, f"{site}_jobs_output_{run_tag}")
        output_file_csv = f"{output_file_base}.csv"
        output_file_db = f"{output_file_base}.db"

        # 2. Update Configuration Files
        cfg_updates = {
            "Num_of_Jobs": cal_jobs,
            "cpu_min_max": cpu_min_max,
            "cpu_speed_precision": speed_precision,
            "Sites": [site],
            "Output_DB": output_file_db, # Use generated DB path
            "Input_Job_CSV": input_csv_path
        }
        if not update_json_file(CONFIG_PATH, cfg_updates):
            logging.error(f"Failed to update main config for run {run_tag}.")
            return float('inf')

        if not update_site_info_file(SITE_INFO_PATH, site, CPUSpeed):
            logging.error(f"Failed to update site info for run {run_tag}.")
            return float('inf')

        logging.info(f"Running simulation {run_tag}...")
        # logging.debug(f"Params: cpu_min_max={cpu_min_max}, precision={speed_precision}, CPUSpeed[0..2]={CPUSpeed[:3]}...") # Debug

        # 3. Run Simulator using subprocess
        start_time = time.time()
        try:
            # Construct the command
            cmd = [SIMULATOR_EXECUTABLE, "-c", CONFIG_PATH]
            # Use timeout (e.g., 10 minutes) to prevent hangs
            timeout_seconds = 600
            result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout_seconds)
            run_duration = time.time() - start_time

            if result.returncode != 0:
                logging.error(f"Simulator failed for run {run_tag} with exit code {result.returncode}.")
                logging.error(f"Simulator stdout:\n{result.stdout}")
                logging.error(f"Simulator stderr:\n{result.stderr}")
                # No CSV will be generated, so metric calc will fail later (returning inf)
            else:
                logging.info(f"Simulation {run_tag} finished in {run_duration:.2f}s (Exit code: {result.returncode}).")
                # logging.debug(f"Simulator stdout:\n{result.stdout}") # Optional debug

        except FileNotFoundError:
             logging.error(f"Simulator executable not found at: {SIMULATOR_EXECUTABLE}")
             return float('inf')
        except subprocess.TimeoutExpired:
             logging.error(f"Simulator timed out after {timeout_seconds}s for run {run_tag}.")
             run_duration = time.time() - start_time
             logging.info(f"Simulation {run_tag} aborted after {run_duration:.2f}s.")
             return float('inf') # Treat timeout as failure
        except Exception as e:
             logging.error(f"Error running simulator process for run {run_tag}: {e}")
             return float('inf')


        # 4. Read Output CSV
        df_raw = None
        if os.path.exists(output_file_csv):
            try:
                # Add slight delay in case file system is slow
                time.sleep(0.1)
                df_raw = pd.read_csv(output_file_csv)
                if df_raw.empty:
                     logging.warning(f"Output file {output_file_csv} is empty for run {run_tag}.")
                     df_raw = None
            except pd.errors.EmptyDataError:
                 logging.warning(f"Output file {output_file_csv} is empty (pandas error) for run {run_tag}.")
                 df_raw = None
            except Exception as e:
                 logging.error(f"Error reading output CSV {output_file_csv} for run {run_tag}: {e}")
                 df_raw = None
        else:
             # Check if simulator *should* have created it (i.e., didn't fail early)
             if 'result' in locals() and result.returncode == 0:
                 logging.warning(f"Output CSV {output_file_csv} not found for run {run_tag}, despite simulator success.")
             # If simulator failed, this is expected, no need for extra warning.


        # 5. Calculate Metrics
        single_core_metric = float('inf')
        multi_core_metric = float('inf') # Keep for logging
        num_single_core = 0
        num_multi_core = 0

        if df_raw is not None:
            try:
                required_cols = ['STATUS', 'CORES', 'CPU_CONSUMPTION_TIME', 'EXECUTION_TIME']
                if not all(col in df_raw.columns for col in required_cols):
                     logging.warning(f"Output CSV {output_file_csv} is missing required columns. Needs: {required_cols}")
                else:
                    # Ensure key columns are numeric *before* filtering/calculation
                    df_raw['CORES'] = pd.to_numeric(df_raw['CORES'], errors='coerce')
                    df_raw['CPU_CONSUMPTION_TIME'] = pd.to_numeric(df_raw['CPU_CONSUMPTION_TIME'], errors='coerce')
                    df_raw['EXECUTION_TIME'] = pd.to_numeric(df_raw['EXECUTION_TIME'], errors='coerce')
                    # Filter for finished jobs *after* numeric conversion
                    df_finished = df_raw[df_raw['STATUS'] == "finished"].copy()

                    # Single-core
                    df_single = df_finished[df_finished['CORES'] == 1].copy()
                    if not df_single.empty:
                        single_core_metric, num_single_core = calculate_metric(df_single, 'single', metric_to_optimize)
                    else:
                        logging.warning(f"No finished single-core jobs found in output for run {run_tag}.")


                    # Multi-core (for logging)
                    df_multi = df_finished[df_finished['CORES'] > 1].copy()
                    if not df_multi.empty:
                        multi_core_metric, num_multi_core = calculate_metric(df_multi, 'multi', metric_to_optimize)
                    else:
                         logging.info(f"No finished multi-core jobs found in output for run {run_tag}.") # Info level ok

            except KeyError as e:
                 logging.error(f"Missing expected column processing output CSV {output_file_csv}: {e}")
                 single_core_metric = float('inf') # Ensure failure state
            except Exception as e:
                 logging.error(f"Error processing data from output CSV {output_file_csv}: {e}")
                 single_core_metric = float('inf') # Ensure failure state

        # --- Log results ---
        metric_unit = "%" if metric_to_optimize == 'MAPE' else ""
        metric_fmt = ".2f" if metric_to_optimize == 'MAPE' else ".4f"
        logging.info(f"Run {run_tag} Metrics: Single={single_core_metric:{metric_fmt}}{metric_unit} ({num_single_core} jobs), Multi={multi_core_metric:{metric_fmt}}{metric_unit} ({num_multi_core} jobs)")

        # Return the single-core metric for optimization
        return single_core_metric if single_core_metric != float('inf') and not np.isnan(single_core_metric) else float('inf')

    finally:
        # 6. Clean Up Temporary Files
        # Use a broader cleanup in case of early exits
        files_to_delete = [input_csv_path, output_file_csv, output_file_db]
        for f_path in files_to_delete:
             if f_path and os.path.exists(f_path):
                 try:
                     os.remove(f_path)
                     # logging.debug(f"Cleaned up: {f_path}")
                 except OSError as e:
                     logging.warning(f"Error deleting temp file {f_path}: {e}")

# ------------------------ Global Settings & Initial Load ------------------------

# Load site_info once to get CPU_COUNT
CPU_COUNT = 0
try:
    with open(SITE_INFO_PATH, 'r') as f:
        site_info_data = json.load(f)
    if SITE_NAME not in site_info_data:
        raise KeyError(f"Site '{SITE_NAME}' not found in {SITE_INFO_PATH}")
    cpu_info = site_info_data[SITE_NAME].get('CPUSpeed')
    if cpu_info is None or not isinstance(cpu_info, list):
         raise ValueError(f"'CPUSpeed' (must be a list) for site '{SITE_NAME}' is missing or invalid")
    CPU_COUNT = len(cpu_info)
    if CPU_COUNT <= 0:
         raise ValueError(f"'CPUSpeed' list for site '{SITE_NAME}' is empty")
    logging.info(f"Found {CPU_COUNT} CPUs for site '{SITE_NAME}'.")

except FileNotFoundError:
    logging.critical(f"Site info file not found at {SITE_INFO_PATH}. Exiting."); sys.exit(1)
except (KeyError, ValueError, json.JSONDecodeError) as e:
    logging.critical(f"Error loading or parsing site info {SITE_INFO_PATH}: {e}. Exiting."); sys.exit(1)
except Exception as e:
    logging.critical(f"Unexpected error loading site info: {e}. Exiting."); sys.exit(1)

# Load the source data for the site
load_source_data(SOURCE_CSV_PATH, SITE_NAME)


# ---------------- Phase 1: Enhanced Parameter Search ----------------

def find_best_parameters(num_trials, evals_per_param, cal_jobs, metric_to_optimize):
    """
    Finds best cpu_min_max and speed_precision.
    Evaluates each parameter set multiple times with different random CPUSpeed lists.
    """
    logging.info(f"--- Phase 1: Finding Best Parameters ---")
    logging.info(f"(Optimizing for SINGLE-CORE {metric_to_optimize}, {evals_per_param} CPUSpeed evaluations per parameter set)")

    best_avg_error = float('inf')
    best_params = None
    possible_precisions = [6, 7, 8, 9, 10, 11, 12] # Added 12 based on example

    metric_unit = "%" if metric_to_optimize == 'MAPE' else ""
    metric_fmt = ".2f" if metric_to_optimize == 'MAPE' else ".4f"

    for trial in range(num_trials):
        # 1. Generate Candidate Parameters
        min_val = random.randint(1, 9) # Wider range?
        upper_bound = random.randint(min_val, max(min_val, 10)) # Allow higher upper?
        cpu_min_max = [min_val, upper_bound]
        speed_precision = random.choice(possible_precisions)
        current_params = {"cpu_min_max": cpu_min_max, "speed_precision": speed_precision}

        logging.info(f"Phase 1 Trial {trial + 1}/{num_trials}: Testing params - {current_params}")

        # 2. Evaluate this parameter set multiple times
        param_errors = []
        for eval_idx in range(evals_per_param):
            # Generate a random CPUSpeed list using these parameters
            try:
                 temp_CPUSpeed = [
                     int(random.uniform(min_val, upper_bound) * (10 ** speed_precision)) # Use uniform for floats then int? or randint?
                     # random.randint(min_val, upper_bound) * (10 ** speed_precision) # Original
                     for _ in range(CPU_COUNT)
                 ]
            except OverflowError:
                 logging.warning(f"Overflow generating CPUSpeed for params {current_params}. Skipping this eval.")
                 continue # Skip this evaluation if numbers get too large

            run_tag = f"phase1_{trial}_eval{eval_idx}"
            # Pass the metric choice to run_simulation, receive SINGLE-CORE metric
            error = run_simulation(SITE_NAME, cal_jobs, cpu_min_max, speed_precision, temp_CPUSpeed, run_tag, metric_to_optimize)
            if error != float('inf'):
                param_errors.append(error)
            # Small sleep between runs? Might help file system contention if runs are very fast.
            # time.sleep(0.1)

        # 3. Calculate average error for this parameter set
        if not param_errors:
            logging.warning(f"No valid simulation results for params {current_params} in trial {trial + 1}. Skipping.")
            continue

        # --- Choose how to aggregate: average or minimum ---
        # Average might be more robust against one lucky run.
        # Minimum might find parameters that *allow* for a very good list, even if not consistently.
        # Let's use AVERAGE for now.
        current_avg_error = sum(param_errors) / len(param_errors)
        # current_min_error = min(param_errors) # Alternative aggregation

        logging.info(f"Phase 1 Trial {trial + 1}/{num_trials} Result: Params={current_params} --> Avg Single-Core {metric_to_optimize}: {current_avg_error:{metric_fmt}}{metric_unit} (from {len(param_errors)} evals)")

        # 4. Check if this parameter set is the best *average* performer so far
        if current_avg_error < best_avg_error:
            best_avg_error = current_avg_error
            best_params = current_params
            logging.info(f"*** New best parameter set found in Phase 1: {best_params} with Avg Single-Core {metric_to_optimize} {best_avg_error:{metric_fmt}}{metric_unit} ***")


    logging.info("\n--- Phase 1 Complete ---")
    if best_params:
        logging.info(f"Best parameters found (based on average performance): {best_params}")
        logging.info(f"Best average Single-Core {metric_to_optimize} during Phase 1: {best_avg_error:{metric_fmt}}{metric_unit}")
    else:
        logging.warning("No suitable parameters found in Phase 1.")

    return best_params, best_avg_error


# ------------- Phase 2: Optimize CPUSpeed List -------------

def optimize_cpuspeed_list(best_params, cal_jobs, num_trials, metric_to_optimize):
    """ Using best params from Phase 1, find the best CPUSpeed list via random search."""
    if not best_params:
        logging.error("Cannot run Phase 2: No best parameters found in Phase 1.")
        return None, float('inf')

    logging.info(f"\n--- Phase 2: Optimizing CPUSpeed List ---")
    logging.info(f"(Using parameters: {best_params}, Optimizing for SINGLE-CORE {metric_to_optimize})")

    best_cpuspeed_error = float('inf')
    best_cpuspeed_list = None
    min_val, upper_bound = best_params['cpu_min_max']
    speed_precision = best_params['speed_precision']

    metric_unit = "%" if metric_to_optimize == 'MAPE' else ""
    metric_fmt = ".2f" if metric_to_optimize == 'MAPE' else ".4f"
    print_len = min(5, CPU_COUNT) # For logging trial lists

    for trial in range(num_trials):
        # 1. Generate a candidate CPUSpeed list using the best parameters
        try:
            current_CPUSpeed = [
                int(random.uniform(min_val, upper_bound) * (10 ** speed_precision))
                # random.randint(min_val, upper_bound) * (10 ** speed_precision) # Original
                for _ in range(CPU_COUNT)
            ]
        except OverflowError:
            logging.warning(f"Overflow generating CPUSpeed for params {best_params} in Phase 2 Trial {trial+1}. Skipping.")
            continue

        logging.info(f"Phase 2 Trial {trial + 1}/{num_trials}: Testing CPUSpeed list starting with: {current_CPUSpeed[:print_len]}...")

        # 2. Run simulation
        run_tag = f"phase2_{trial}"
        error = run_simulation(SITE_NAME, cal_jobs, best_params['cpu_min_max'], speed_precision, current_CPUSpeed, run_tag, metric_to_optimize)

        logging.info(f"Phase 2 Trial {trial + 1}/{num_trials} Result: --> Single-Core {metric_to_optimize}: {error:{metric_fmt}}{metric_unit}")

        # 3. Check if this CPUSpeed list gives a better result
        if error < best_cpuspeed_error:
            best_cpuspeed_error = error
            best_cpuspeed_list = current_CPUSpeed # Save this list
            logging.info(f"*** New best CPUSpeed list found in Phase 2 with Single-Core {metric_to_optimize} {best_cpuspeed_error:{metric_fmt}}{metric_unit} ***")
            logging.info(f"  Best CPUSpeed list starts with: {best_cpuspeed_list[:print_len]}...")


    logging.info("\n--- Phase 2 Complete ---")
    if best_cpuspeed_list:
        logging.info(f"Best CPUSpeed list found with Single-Core {metric_to_optimize}: {best_cpuspeed_error:{metric_fmt}}{metric_unit}")
    else:
        logging.warning(f"No optimal CPUSpeed list found in Phase 2.")

    return best_cpuspeed_list, best_cpuspeed_error

# ------------------------ Main Execution ------------------------

if __name__ == '__main__':
    # Validate the chosen metric
    VALID_METRICS = ['MAE', 'MAPE', 'RAE']
    if OPTIMIZATION_METRIC not in VALID_METRICS:
        logging.critical(f"Invalid OPTIMIZATION_METRIC '{OPTIMIZATION_METRIC}'. Choose from {VALID_METRICS}. Exiting.")
        sys.exit(1)

    # Ensure output directories exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    logging.info(f"Starting optimization for site '{SITE_NAME}'")
    logging.info(f"Optimization Metric (Single-Core): {OPTIMIZATION_METRIC}")
    logging.info(f"Phase 1 Trials: {NUM_PARAM_TRIALS_PHASE1} parameter sets")
    logging.info(f"Phase 1 Evals per Param: {NUM_CPUSPEED_EVALS_PER_PARAM}")
    logging.info(f"Phase 2 Trials: {NUM_CPUSPEED_TRIALS_PHASE2} CPUSpeed lists")
    logging.info(f"Calibration Jobs per Run: {CALIBRATION_JOB_SAMPLE_SIZE}")
    logging.info("-" * 30)


    start_overall_time = time.time()

    # --- Phase 1 ---
    best_params_found, phase1_best_avg_error = find_best_parameters(
        NUM_PARAM_TRIALS_PHASE1,
        NUM_CPUSPEED_EVALS_PER_PARAM,
        CALIBRATION_JOB_SAMPLE_SIZE,
        OPTIMIZATION_METRIC
    )

    # --- Phase 2 ---
    final_best_cpuspeed = None
    final_best_error = float('inf')

    if best_params_found:
        final_best_cpuspeed, final_best_error = optimize_cpuspeed_list(
            best_params_found,
            CALIBRATION_JOB_SAMPLE_SIZE,
            NUM_CPUSPEED_TRIALS_PHASE2,
            OPTIMIZATION_METRIC
        )

        # --- Final Results & Update ---
        metric_unit = "%" if OPTIMIZATION_METRIC == 'MAPE' else ""
        metric_fmt = ".2f" if OPTIMIZATION_METRIC == 'MAPE' else ".4f"

        logging.info("\n" + "=" * 30 + " FINAL RESULTS " + "=" * 30)
        logging.info(f"Optimization Metric Used (Single-Core): {OPTIMIZATION_METRIC}")
        logging.info(f"Site: {SITE_NAME}")
        logging.info(f"Best parameters from Phase 1 (based on avg performance): {best_params_found}")
        logging.info(f"Best *average* Single-Core {OPTIMIZATION_METRIC} during Phase 1: {phase1_best_avg_error:{metric_fmt}}{metric_unit}")

        if final_best_cpuspeed:
            logging.info(f"Best CPUSpeed list found in Phase 2 achieves Single-Core {OPTIMIZATION_METRIC}: {final_best_error:{metric_fmt}}{metric_unit}")
            print_len = min(10, CPU_COUNT)
            logging.info(f"Final Optimized CPUSpeed List starts with: {final_best_cpuspeed[:print_len]}...")

            # Update site_info.json with the final best CPUSpeed list
            logging.info(f"\nUpdating {SITE_INFO_PATH} for site '{SITE_NAME}'...")
            if update_site_info_file(SITE_INFO_PATH, SITE_NAME, final_best_cpuspeed):
                 logging.info("Site info successfully updated.")
            else:
                 logging.error("Failed to update site info with the final list.")
        else:
            logging.warning(f"Could not find an optimal CPUSpeed list in Phase 2.")
            logging.warning("Site info file was NOT updated.")

    else:
        logging.error("\n" + "=" * 30 + " FINAL RESULTS " + "=" * 30)
        logging.error(f"Failed to find suitable parameters in Phase 1 using Single-Core {OPTIMIZATION_METRIC}.")
        logging.error("Cannot proceed to Phase 2. Site info file was NOT updated.")

    end_overall_time = time.time()
    logging.info(f"\nTotal script duration: {end_overall_time - start_overall_time:.2f} seconds.")
    logging.info("Script finished.")