## Prerequisites
Before you begin, ensure you have the following packages installed:

- [**Boost**](https://www.boost.org/)
- [**SimGrid v3.36**](https://framagit.org/simgrid/simgrid/)

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
   cg-sim -c config.json
   ```
