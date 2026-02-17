# CGSim Docker Installation Guide

This guide explains how to run **CGSim** using Docker. Docker provides a
fully isolated runtime environment with all required dependencies
pre-installed and configured. This ensures consistent execution across
machines and simplifies deployment to remote systems.

You can either:

-   **Pull the pre-built Docker image** (recommended), or
-   **Build the image locally from the Dockerfile**

Both approaches ultimately require creating a container with the
appropriate bind-mounted directories.

------------------------------------------------------------------------

## Overview

Running CGSim in Docker involves three high-level steps:

1.  Ensure required directories and configuration files exist
2.  Obtain the Docker image (pull or build)
3.  Create and start the Docker container

------------------------------------------------------------------------

# 1. Required Directory Structure

CGSim uses three writable (mutable) directories that are bind-mounted
into the container:

    /config-files
    /output
    /logs

These directories must exist on the host machine and will be accessible
inside the container at:

    /opt/CGSim/config-files
    /opt/CGSim/output
    /opt/CGSim/logs

The `/config-files` directory must contain the necessary configuration
files before starting the container.

------------------------------------------------------------------------

# 2. Repository Setup

## Option A -- Repository Already Exists Locally

If you already have a working CGSim repository with:

    ./config-files
    ./output
    ./logs

then:

``` bash
cd CGSim
```

Ensure the directory contains:

-   `Dockerfile` (only required if building locally)
-   `./config-files`
-   `./output`
-   `./logs`

If any are missing, pull the repository again or manually create the
missing directories.

------------------------------------------------------------------------

## Option B -- Clone the Repository

If you do not yet have the repository:

``` bash
git clone https://github.com/REDWOOD24/CGSim.git
cd CGSim
```

If the `logs` directory does not exist, create it:

``` bash
mkdir logs
```

------------------------------------------------------------------------

# 3. Obtain the Docker Image

You only need to complete **one** of the following options.

------------------------------------------------------------------------

## Option 1 -- Pull the Pre-Built Image (Recommended)

The official image is available from GitHub Container Registry:

``` bash
docker pull ghcr.io/redwood24/cgsim:latest
```

This is the simplest and preferred method.

------------------------------------------------------------------------

## Option 2 -- Build the Image Locally

### Install Docker

Download and install Docker Desktop:

https://www.docker.com/products/docker-desktop/

An account is **not required** to install Docker Desktop.

Ensure Docker Desktop (or the Docker daemon) is running before
continuing.

------------------------------------------------------------------------

### Build the Image

From the root of the CGSim repository (where the `Dockerfile` is
located):

``` bash
docker build -t CGSim .
```

------------------------------------------------------------------------

# 4. Create the Docker Container

Once the image is available (either pulled or built), create the
container with bind-mounted directories.

If you pulled the official image:

``` bash
docker create \
  --name CGSim \
  -v ./config-files:/opt/CGSim/config-files \
  -v ./output:/opt/CGSim/output \
  -v ./logs:/opt/CGSim/logs \
  ghcr.io/redwood24/cgsim:latest
```

If you built the image locally:

``` bash
docker create \
  --name CGSim \
  -v ./config-files:/opt/CGSim/config-files \
  -v ./output:/opt/CGSim/output \
  -v ./logs:/opt/CGSim/logs \
  CGSim
```

------------------------------------------------------------------------

# 5. Configure the Simulation

Before starting the container, update the configuration file:

    ./config-files/config.json

Since this directory is bind-mounted, the configuration can be edited
**after container creation but before starting**.

Below is an example `config.json`:

``` json
{
  "Grid_Name": "ATLAS-GRID",
  "Sites_Information": "config-files/site_info.json",
  "Sites": [],
  "Sites_Connection_Information": "config-files/site_conn_info.json",
  "Dispatcher_Plugin": "dispatch_plugins/simple-test-plugin/build/libSimpleDispatcherPlugin.so",
  "Output_DB": "config-files/mimic_job.db",
  "Input_Job_CSV": "config-files/500_jobs_per_site.csv",
  "Num_of_Jobs": -1
}
```

The simulation will fail if required configuration values are not
properly set.

------------------------------------------------------------------------

# 6. Start the Container and Stream Logs

To start the existing container:

``` bash
docker start CGSim && sleep 2 && tail -f logs/atlas_grid_simulation.log
```

Explanation:

-   `docker start CGSim` -- Starts the previously created container\
-   `sleep 2` -- Allows time for the log file to be created\
-   `tail -f logs/atlas_grid_simulation.log` -- Streams simulation logs
    to the terminal

You should see the simulation output begin streaming shortly after
startup.

------------------------------------------------------------------------

# Notes on Required Files

Cloning the full repository is the easiest way to ensure all required
components are present. However:

-   Only the three mutable directories (`config-files`, `output`,
    `logs`) are required at runtime\
-   The `Dockerfile` is only required if building locally\
-   The `logs` directory may need to be created manually

------------------------------------------------------------------------

# Summary

Recommended workflow:

``` bash
git clone https://github.com/REDWOOD24/CGSim.git
cd CGSim
mkdir -p logs
docker pull ghcr.io/redwood24/cgsim:latest
docker create --name CGSim \
  -v ./config-files:/opt/CGSim/config-files \
  -v ./output:/opt/CGSim/output \
  -v ./logs:/opt/CGSim/logs \
  ghcr.io/redwood24/cgsim:latest
docker start CGSim && sleep 2 && tail -f logs/atlas_grid_simulation.log
```

This provides a clean, reproducible CGSim runtime environment with
minimal setup.
