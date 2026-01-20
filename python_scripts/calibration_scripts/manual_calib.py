import random
import numpy as np
import pandas as pd
import json
import os
np.set_printoptions(threshold=np.inf)

def update_site_info_in_json_file(file_path, new_site_name):
    """
    Reads a JSON file, updates site-specific information, and writes it back to the file.

    The function modifies the JSON file at the given path.
    Specifically, it:
    1.  Updates the "Sites" list to contain only the new_site_name.
    2.  Updates filenames in "Input_Job_CSV" and "Output_DB" paths
        by replacing the old site name (derived from the initial "Sites" list's
        first entry) with the new_site_name within their filename component.

    Args:
        file_path (str): The path to the JSON file to be updated.
        new_site_name (str): The new site name to use for updates.

    Raises:
        TypeError: If file_path or new_site_name are not strings.
        FileNotFoundError: If the file_path does not point to an existing file.
        ValueError: If the file does not contain a valid JSON object at its root,
                    or if the JSON is malformed.
        IOError: For other issues during file reading or writing.
    """
    if not isinstance(file_path, str):
        raise TypeError("Input 'file_path' must be a string.")
    if not isinstance(new_site_name, str):
        raise TypeError("Input 'new_site_name' must be a string.")

    # 1. Read the JSON data from the file
    try:
        # Specify encoding for broader compatibility
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        # Re-raise with a more specific message if desired, or let it propagate
        raise FileNotFoundError(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError as e:
        # Raise a ValueError with context about the malformed JSON
        raise ValueError(f"Invalid JSON format in file '{file_path}': {e}") from e
    except IOError as e: # Catch other potential IOErrors during read
        raise IOError(f"Error reading file '{file_path}': {e}") from e

    # Ensure the root of the JSON is a dictionary, as expected by subsequent logic
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON file '{file_path}' to contain a JSON object (dictionary) at its root, but found type: {type(data).__name__}.")

    # 2. Perform the updates on the loaded data
    old_site_name = None
    # Attempt to get the old site name from the "Sites" list
    # Assumes the relevant old site name is the first element if the list exists and is not empty
    if "Sites" in data and isinstance(data.get("Sites"), list) and data["Sites"]:
        old_site_name = data["Sites"][0]
    
    # Update the "Sites" field with the new site name
    data["Sites"] = [new_site_name]

    # If an old site name was identified, use it to update paths
    if old_site_name: # Only proceed if old_site_name was found
        fields_to_update_paths = ["Input_Job_CSV", "Output_DB"]
        for field_key in fields_to_update_paths:
            if field_key in data and isinstance(data.get(field_key), str):
                original_path = data[field_key]
                
                # Separate directory and filename.
                # This method is robust for POSIX-style paths.
                directory_part = ""
                filename_part = original_path # Assume it's all filename initially
                
                last_slash_idx = original_path.rfind('/')
                if last_slash_idx != -1: # If '/' is found
                    directory_part = original_path[:last_slash_idx + 1] # Includes the slash
                    filename_part = original_path[last_slash_idx + 1:]
                
                # Replace old site name with new site name in the filename part
                new_filename_part = filename_part.replace(old_site_name, new_site_name)
                
                # Reconstruct the path
                data[field_key] = directory_part + new_filename_part
    
    # 3. Write the updated data back to the same file
    try:
        # Specify encoding and indent for readable output
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except IOError as e: # Catch potential IOErrors during write
        raise IOError(f"Error writing updated JSON to file '{file_path}': {e}") from e


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

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# # random_numbers = [random.randint(3, 4) for _ in range(2329)]
# random_numbers = [random.randint(2, 3) for _ in range(10)]
# # Convert the list to a NumPy array to enable element-wise multiplication with a float
# import numpy as np
# random_numbers_np = np.array(random_numbers)
# random_numbers_np * 1e7

import random, numpy as np
np.set_printoptions(threshold=np.inf,          # show the whole array
                    formatter={'float_kind': lambda x: f'{x:.1f}'})  # <- key line

random_numbers = [random.randint(1,2) for _ in range(1009)]
random_numbers_np = np.array(random_numbers) * 1e7+2840000

site = 'MPPMU'
path = '/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/site_info_cpu_calib.json'
update_site_info(path, site, random_numbers_np.tolist(), verbose=True)
update_site_info_in_json_file('/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config_run.json', site)

command = "/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/build/atlas-grid-simulator -c /home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/config-files/config_run.json" 
os.system(command)