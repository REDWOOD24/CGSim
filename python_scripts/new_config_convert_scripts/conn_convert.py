import json

# Hardcoded file paths
INPUT_JSON_FILE = "/Users/raekhan/CGSim/data/site_conn_info.json"
OUTPUT_JSON_FILE = "latency_bandwidth.json"

# Mapping closeness → latency (ms)
CLOSENESS_LATENCY_MAP = {
    0: 0.0,  1: 10.0, 2: 20.0, 3: 30.0,
    4: 40.0, 5: 50.0, 6: 60.0, 7: 70.0,
    8: 80.0, 9: 90.0, 10: 100.0,
    11: 110.0, 12: 120.0
}


def canonical_connection(connection: str) -> str | None:
    """
    Returns a canonical connection key (A:B with A < B),
    or None for self-connections / malformed keys.
    """
    try:
        site1, site2 = connection.split(":", 1)
        if site1 == site2:
            return None  # self-connection
        return ":".join(sorted((site1, site2)))
    except ValueError:
        return None


def extract_latency_bandwidth(input_json: dict) -> dict:
    output = {}

    for connection, data in input_json.items():
        canon = canonical_connection(connection)
        if canon is None:
            continue

        # Skip if we already processed the reverse direction
        if canon in output:
            continue

        try:
            closeness = data["closeness"]["latest"]
            bandwidth = data["mbps"]["dashb"]["1w"]

            if closeness not in CLOSENESS_LATENCY_MAP:
                continue

            output[canon] = {
                "latency": str(CLOSENESS_LATENCY_MAP[closeness])+"ms",
                "bandwidth": str(bandwidth)+"Mbps"
            }

        except (KeyError, TypeError):
            continue

    return output


def main():
    with open(INPUT_JSON_FILE, "r") as f:
        input_json = json.load(f)

    output_json = extract_latency_bandwidth(input_json)

    with open(OUTPUT_JSON_FILE, "w") as f:
        json.dump(output_json, f, indent=2, sort_keys=True)

    print(f"Wrote {len(output_json)} unique connections to {OUTPUT_JSON_FILE}")


if __name__ == "__main__":
    main()
