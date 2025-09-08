# Redwood Project

## Overview
This repository aims to streamline the process of configuring and testing grid computing environments.

## Prerequisites
Before you begin, ensure you have the following packages installed:

- [**SQLite3**](https://www.sqlite.org/)
- [**Boost**](https://www.boost.org/)
- [**SimGrid v3.36**](https://framagit.org/simgrid/simgrid/) 
- [**SimGrid File System Module v0.2**](https://github.com/simgrid/file-system-module)

These packages are essential for building and running the simulation.

## Build Instructions
Follow these steps to build the project locally:
   ```bash
   git clone https://github.com/REDWOOD24/CGSim.git
   cd CGSim
   mkdir build
   cd build
   cmake ..
   make -j
   sudo make install
   ```

## Run Instructions

   ```bash
  ./cg-sim -c config.json
   ```
   
## Platform

Basic Layout of the ATLAS Grid implemented in the simulation.

```bash
  +---------+                      +---------+                    +---------+
  | AGLT2   |                      | BNL-    |                    | OTHER   |
  |         |                      | ATLAS   |                    | SITES   |
  +---------+                      +---------+                    +---------+
    |  |  |                        |  |   |                       | |  |
    |  |  | [Link: To CPU Nodes]   |  |   | [Link: To CPU Nodes]  | |  | [Link: To CPU Nodes]
    |  |_ |_ _ _                   |  |   |_ _               _ _ _| |  |_ _
    v    v      v                  v  v       v             v       v      v
  +----+ +----+ +----+         +----+ +----+ +----+       +----+ +----+ +----+
  |CPU1| |CPU2| |... |         |CPU1| |CPU2| |... |       |CPU1| |CPU2| |... |
  +----+ +----+ +----+         +----+ +----+ +----+       +----+ +----+ +----+
    ^                            ^                            ^
    | [Link: To Other Sites]     | [Link: To Other Sites]     | [Link: To Other Sites]
    |                            |                            |
    |                            |                            |
    |                            |                            |
    |                            |                            |
 [Link: AGLT2 <--> BNL]       [Link: BNL <--> OTHER]       [Link: OTHER <--> AGLT2]
    |                            |                            |
    v                            v                            v
  +---------+                   +---------+                  +---------+
  | BNL-    |                   | OTHER   |                  | AGLT2   |
  | ATLAS   |                   | SITES   |                  |         |
  +---------+                   +---------+                  +---------+
    |                            |                            |
    | [CPU1 Gateway]             | [CPU1 Gateway]             | [CPU1 Gateway]
    |                            |                            |
    v                            v                            v
  +----+ +----+ +----+         +----+ +----+ +----+       +----+ +----+ +----+
  |CPU1| |CPU2| |... |         |CPU1| |CPU2| |... |       |CPU1| |CPU2| |... |
  +----+ +----+ +----+         +----+ +----+ +----+       +----+ +----+ +----+
```




## Initializing Sites (Sample Site)

### praguelcg2

**CPUs:**  1277 ±(3) 

**Cores:** 32

**Disk Information per CPU:**

- **PRAGUELCG2_DATADISK:**         2927 GiB
- **PRAGUELCG2_LOCALGROUPDISK:**   517 GiB
- **PRAGUELCG2_SCRATCHDISK:**      86 GiB

---

### Notes

- Information obtained from site data dumps.
- CPUs based off GFLOPS obtained from data dumps from site, estimating 500 GFLOPS per core per cpu.
- Other estimates such as latency for connections, disk read and write bandwidths, cpu speed based on estimates.

## Job Manager

 - Job has 3 parts -> (Read, Compute, Write).
 - Set sizes of files to be read and written from a gaussian distribution (mean and stddev estimated).
 - Estimate GFLOPS for tasks.
 - Example job shown below.


## Sample Job

## Job Details
- **Job ID:** Task-18-Job-13
- **FLOPs to be Executed:** 400,451

### Files to be Read
| File Path                            | Size (Bytes) |
|--------------------------------------|--------------|
| /input/user.input.18.0000035.root    | 9,992,671    |
| /input/user.input.18.0000036.root    | 9,990,360    |
| /input/user.input.18.0000037.root    | 10,011,170   |
| /input/user.input.18.0000038.root    | 9,986,847    |
| /input/user.input.18.0000039.root    | 10,011,480   |

### Files to be Written
| File Path                            | Size (Bytes) |
|--------------------------------------|--------------|
| /output/user.output.18.0000047.root  | 10,011,591   |
| /output/user.output.18.0000048.root  | 10,000,451   |
| /output/user.output.18.0000049.root  | 10,000,584   |
| /output/user.output.18.000005.root   | 9,991,906    |



### Resource Usage
- **Cores Used:** 32
- **Disks Used:** PRAGUELCG2_LOCALGROUPDISK

### Host
- **Compute Host:** praguelcg2_cpu-999


## Job Dispatcher 

    +-------------------+ 
    | Job Manager      |
    +-------------------+
                        \ Tasks
                         \
                          +------------+     +---------------------------------+       +---------+ 
                          | Dispatcher | --> | Jobs allocated to Resources     |  -->  | Output  |
                          +------------+     +---------------------------------+       +---------+ 
                         /
                        / Resources
    +-------------------+ 
    | Platform          |
    +-------------------+ 
    
## How it works.


The dispatcher operates by taking two key inputs: a prioritized queue of tasks and a platform containing details about all available resources. Using a straightforward algorithm, the dispatcher efficiently matches tasks to the appropriate resources.

### Resource Prioritization
Information about the various sites and their resources (such as CPU speed and cores) is collected into a queue, with priority given to sites that offer higher-quality resources.

### Task Breakdown
Each task is divided into a series of jobs based on the maximum storage and computational requirements (FLOP). These jobs, requiring similar resources, are then individually assigned to available CPUs.

### CPU Allocation
CPUs are evaluated site by site, in order of site priority. For each site the CPUs are ranked based on CPU quality factors such as storage, cores, speed, and available computation capacity. For efficiency, the search depth is limited to 20. When a job is assigned to a CPU, that CPU's quality decreases as its computational load increases.

To ensure no single site is overburdened, once 50% of a site's CPUs have been assigned jobs, a round-robin strategy is employed to distribute the remaining jobs across other sites. If all sites reach 50% CPU usage, the round-robin strategy continues to allocate jobs, rotating sites for each new assignment.

### Execution
After all jobs have been dispatched, the hosts with assigned jobs are passed to SimGrid for execution.


## SimGrid Output for Job

Output is saved in the form of an HDF5 file for efficiency reasons. The file has a number of datasets defined by the hosts which carry the jobs. Associated with each such dataset is information for all jobs assigned to that CPU, sample output for one of the jobs on a CPU is shown below.

### HOST-praguelcg2_cpu-999

- **ID**: "Task-18-Job-13"
- **FLOPS EXECUTED**: 400451 FLOPs
- **FILES READ SIZE**: 49992528 Bytes
- **FILES WRITTEN SIZE**: 40004532 Bytes
- **READ IO TIME**: 0.00207438 s
- **WRITE IO TIME**: 0.00370412 s
- **FLOPS EXEC TIME**: 0.000235559 s

## Test with 5000 Tasks

- Ran on Mac M1.
- 152222 jobs.
- Finished in 2 hour 41 minutes.
- 2.70 GB max memory usage.
- 57 MB output HDF5 file.

## Developer info

New code should be developed in private forks of the main repository. When a stable version is ready, make sure to check if there is already a new version available before creating the pull request to merge the new code with the main repository branch. See the steps below.

- Check if a remote repository is already set up: git remote -v
- If not, execute: git remote add upstream https://github.com/REDWOOD24/ATLAS-GRID-SIMULATION.git
- See if there is any new version available from upstream: git fetch upstream
- Merge with the private/local code: git merge upstream/main
- Update the private/local repo: git push
