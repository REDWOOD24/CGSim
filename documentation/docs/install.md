## Local Build Instructions

Before you begin, ensure you have the following packages installed:

- [**Boost**](https://www.boost.org/)
- [**SimGrid v3.36**](https://framagit.org/simgrid/simgrid/)

Then follow these steps to build the project locally:
   ```bash
   git clone https://github.com/REDWOOD24/CGSim.git
   cd CGSim
   mkdir build
   cd build
   cmake ..
   make -j
   sudo make install
   ```

## Remote Build Instructions
Follow these steps to build the project remotely on a server (with no ```sudo``` privlidges):
   ```bash
   git clone https://github.com/REDWOOD24/CGSim.git
   cd CGSim
   git clone --branch v3.36 https://framagit.org/simgrid/simgrid.git
   cd simgrid
   mkdir build
   cd build
   cmake ..
   make -j
   cd ../../
   mkdir build
   cd build
   cmake -Dremote=ON ..
   make -j
   ```

!!! warning
    It is assumed that the ```boost``` libraries are already installed in this case. If this is not the case, they have to be built remotely as well and paths can be specifed by running ```ccmake ..``` in the build directory.

## Run Instructions

CGSim runs with a configuration file [[see example]](configuration.md) specified with the ```-c``` flag.

   ```bash
   cg-sim -c config.json
   ```
