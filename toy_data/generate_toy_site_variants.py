import json
import random
from pathlib import Path


BASE_FILE = Path(__file__).with_name("toy_new_site_info.json")
JOBS_FILE = Path(__file__).with_name("toy_job.csv")

# Total numbers of files to test with
TOTAL_FILE_COUNTS = [20, 40, 60, 80, 100]

# We want 30 files total → 6 variants per total count
VARIANTS_PER_TOTAL = 6

# Reasonable file size range: 10 MB – 50 MB
# (kept small so we never exceed per-site capacity, and we don't need to drop files)
MIN_FILE_SIZE = 10 * 1024 * 1024
MAX_FILE_SIZE = 50 * 1024 * 1024


def extract_job_file_ids() -> list[str]:
    """Parse toy_job.csv and extract all file IDs that appear in the files_info column."""
    if not JOBS_FILE.exists():
        return []

    with JOBS_FILE.open("r") as f:
        lines = [line.rstrip("\n") for line in f]
    if not lines:
        return []

    header = lines[0].split(",")
    # Normalize header names to lowercase
    header_map = {name.strip().lower(): idx for idx, name in enumerate(header)}
    files_info_idx = header_map.get("files_info")
    if files_info_idx is None:
        return []

    file_ids: set[str] = set()
    for line in lines[1:]:
        if not line.strip():
            continue
        cols = line.split(",")
        if files_info_idx >= len(cols):
            continue
        raw = cols[files_info_idx].strip()
        if not raw:
            continue
        # Similar cleaning to WORKLOAD_MANAGER: strip quotes and braces, then split
        if raw.startswith('"') and raw.endswith('"'):
            raw = raw[1:-1]
        raw = raw.replace("{", "").replace("}", "")
        for token in raw.split(","):
            token = token.strip()
            if not token:
                continue
            colon_pos = token.find(":")
            if colon_pos == -1:
                continue
            key = token[:colon_pos]
            key = key.replace('"', "").strip()
            if key:
                file_ids.add(key)

    return sorted(file_ids)


def main() -> None:
    with BASE_FILE.open("r") as f:
        base = json.load(f)

    sites = list(base.keys())

    # Collect the set of file IDs actually referenced by jobs, so every such file
    # exists on at least one site in every generated variant.
    job_file_ids = extract_job_file_ids()
    if not job_file_ids:
        # Fallback: use IDs from the base site info if jobs file is empty
        base_file_ids = set()
        for site_obj in base.values():
            for fid, _ in site_obj.get("files", []):
                base_file_ids.add(str(fid))
        job_file_ids = sorted(base_file_ids)

    variant_index = 1

    for total_files in TOTAL_FILE_COUNTS:
        for v in range(VARIANTS_PER_TOTAL):
            # Make a deep-ish copy: we will overwrite only SITE_PROPERTIES.files_* fields
            data = json.loads(json.dumps(base))

            # Deterministic randomness per (total_files, v)
            rng = random.Random(1000 * total_files + v)

            site_files = {site: [] for site in sites}
            logical_ids: list[str] = []

            # First, ensure that every job file ID appears at least once
            for fid in job_file_ids:
                logical_ids.append(fid)

            # Then, if we want more total files than job IDs, add extra ones
            while len(logical_ids) < total_files:
                fid = rng.choice(job_file_ids)
                logical_ids.append(fid)

            # Choose "hot" and "cold" sites to bias utilization and trigger proactive moves.
            hot_count = max(1, len(sites) // 4)
            cold_candidates = [s for s in sites]
            hot_sites = rng.sample(cold_candidates, k=hot_count)
            cold_candidates = [s for s in sites if s not in hot_sites]
            cold_count = max(1, len(sites) // 4) if cold_candidates else 0
            cold_sites = rng.sample(cold_candidates, k=cold_count) if cold_candidates else []

            # For reactive-policy testing, allow replicas: each logical file
            # can appear on 1–3 random sites, with a bias:
            #   - more replicas on hot sites,
            #   - fewer on cold sites.
            for fid in logical_ids:
                replica_count = rng.randint(1, min(3, len(sites)))

                chosen_sites: list[str] = []
                for _ in range(replica_count):
                    r = rng.random()
                    if r < 0.6 and hot_sites:
                        chosen_sites.append(rng.choice(hot_sites))
                    elif r < 0.8 and cold_sites:
                        chosen_sites.append(rng.choice(cold_sites))
                    else:
                        chosen_sites.append(rng.choice(sites))

                # Deduplicate while preserving order
                seen = set()
                replica_sites: list[str] = []
                for s in chosen_sites:
                    if s not in seen:
                        seen.add(s)
                        replica_sites.append(s)

                size = rng.randint(MIN_FILE_SIZE, MAX_FILE_SIZE)
                for site in replica_sites:
                    site_files[site].append((fid, size))

            # Apply to data: update files, file_count, and storage_utilization_ratio
            for site in sites:
                site_obj = data[site]
                props = site_obj["SITE_PROPERTIES"]
                capacity = int(props["storage_capacity_bytes"])

                files_for_site = site_files[site]
                total_bytes = sum(size for _, size in files_for_site)

                # If we somehow exceed capacity, scale all sizes down proportionally
                # but keep every file present (no drops).
                if total_bytes > capacity and total_bytes > 0:
                    scale = capacity / total_bytes
                    scaled_files = []
                    total_bytes = 0
                    for fid, size in files_for_site:
                        new_size = max(1, int(size * scale))
                        scaled_files.append((fid, new_size))
                        total_bytes += new_size
                    files_for_site = scaled_files

                # Update JSON structure
                site_obj["files"] = [[fid, size] for fid, size in files_for_site]
                props["file_count"] = str(len(files_for_site))

                # storage_utilization_ratio = used / capacity
                ratio = 0.0
                if capacity > 0:
                    ratio = total_bytes / capacity
                props["storage_utilization_ratio"] = f"{ratio:.16f}"

            out_name = f"toy_new_site_info_{variant_index}.json"
            out_path = BASE_FILE.with_name(out_name)
            with out_path.open("w") as f:
                json.dump(data, f, indent=2)

            variant_index += 1


if __name__ == "__main__":
    main()

