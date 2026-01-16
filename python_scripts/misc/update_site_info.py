import json
import random

site_info = json.load(open("/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/site_info_cpu.json"))

print(site_info)

for site in site_info.keys():
    glops = site_info[site]['GFLOPS']
    # cpucount = int(glops/(32*500))+3
    cpucount = int(glops/(32*500))
    cpuspeed = [random.randint(1,2)*1e9 for _ in range(cpucount)]
    site_info[site]['CPUCount'] = cpucount
    site_info[site]['CPUSpeed'] = cpuspeed

with open("/home/sairam/ATLASGRIDV2/ATLAS-GRID-SIMULATION/data/site_info_cpu.json", "w") as f:
	json.dump(site_info, f, indent=4)