import json
import random

NUM_SITES = 20
SITES = [f"AGLT2_site_{i}" for i in range(NUM_SITES)]

def generate_connections():
    connections = {}

    for i in range(NUM_SITES):
        for j in range(i + 1, NUM_SITES):
            bw = random.gauss(100, 60)
            bw = max(0.1, round(bw, 2))   # prevent non-positive bandwidth
            bw = 40
            
            latency = random.randint(1, 40)
            latency = 20
            key = f"{SITES[i]}:{SITES[j]}"
            connections[key] = {
                "bandwidth": str(bw)+"Mbps",
                "latency": str(latency)+"ms"
            }

    return connections

if __name__ == "__main__":
    connections = generate_connections()

    with open("mimic_new_site_conn_info.json", "w") as f:
        json.dump(connections, f, indent=2)

    print("Wrote mimic_new_site_conn_info.json")
