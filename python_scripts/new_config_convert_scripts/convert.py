import json
from collections import Counter
import numpy as np

from collections import Counter
import numpy as np

def compute_storage_capacity_bytes(data: dict) -> int:
    """
    Converts total storage from GB to bytes.
    
    Assumes:
    - All RSE values are in GB
    - 1 GB = 1e9 bytes
    """
    rse = data.get("RSE", {})

    # Sum all values (convert strings to float)
    total_gb = sum(float(v) for v in rse.values())

    # Convert GB to bytes
    total_bytes = int(total_gb * 1e9)

    return str(total_bytes)

def convert_site_data(old_data):
    new_data = {}

    for site_name, site_info in old_data.items():
        new_site = {}

        # ---- PROPERTIES ----
        properties_keys = [
            "GFLOPS",
            "storage_capacity_bytes",
            "storage_utilization_ratio",
            "file_count",
        ]
        new_site["SITE_PROPERTIES"] = {
            k: str(site_info[k]) for k in properties_keys if k in site_info
        }
        
        new_site["SITE_PROPERTIES"]["storage_capacity_bytes"] = compute_storage_capacity_bytes(site_info)
        

        # ---- CPU INFO ----
        cpu_list = site_info.get("CPUSpeed", [])
        cpu_counter = Counter(cpu_list)
        num_cpus = sum(cpu_counter.values())

        rse = site_info.get("RSE", {})

        cpu_infos = []
        for speed, count in cpu_counter.items():
            cpu_entry = {
                "units": count,
                "speed": speed,
                "cores": np.random.randint(20, 50),
                "BW_CPU": str(np.random.randint(1600, 4000))+"GBps" ,    # bytes/sec
                "LAT_CPU": str(np.random.randint(50, 100))+"ns",       # ns
                "properties": [],
                "disks": []
            }
            property = {"ram": str(np.random.randint(1, 20) * 1024)+"GB"}
            cpu_entry["properties"].append(property)
            # ---- DISKS (embedded, simplified) ----
            for disk_name in rse.keys():
                disk = {
                    "name": disk_name,
                    "read_bw": str(np.random.randint(500, 3500))+"MBps",
                    "write_bw": str(np.random.randint(200, 3000))+"MBps"
                }
                cpu_entry["disks"].append(disk)

            cpu_infos.append(cpu_entry)

        new_site["CPUInfo"] = cpu_infos

        # ---- FILES ----
        files = site_info.get("files", [])
        new_site["files"] = [
            [str(f["file_id"]), f["file_size"]]
            for f in files
            if "file_id" in f and "file_size" in f
        ]

        new_data[site_name] = new_site

    return new_data


# Example usage
if __name__ == "__main__":
    # Load your old JSON
    with open("site_info.json", "r") as f:
        old_data = json.load(f)

    new_data = convert_site_data(old_data)

    # Save to new JSON file
    with open("new_site_info.json", "w") as f:
        json.dump(new_data, f, indent=2)
