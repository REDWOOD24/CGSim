## Data Management Plugins

This directory contains **data management policies** that can be plugged into CGSim.

- Each policy lives in its own subdirectory (similar to `dispatch_plugins`).
- The core simulator currently ships with a single example policy: a **timer-based storage balancing policy**.

### Timer-Based Policy

The timer-based policy lives under `timer-based-policy/` and is wired directly into the main `CGSim` shared library. It runs periodically during the simulation and can:

- Monitor site storage utilization.
- Copy or move files between sites based on utilization thresholds.

You configure it via the `Data_Management_Policy` block in your simulation config JSON (see the top-level `README.md` and `data_management_plugins/QUICK_START_TIMER_POLICY.md`).

### Adding New Policies

To add a new policy:

1. Create a new subdirectory under `data_management_plugins/` (for example, `my-custom-policy/`).
2. Add your `.cpp` files there and include the main header `include/data_management_policy.h` or your own headers.
3. Make sure the new sources are added to the build (either by updating the top-level `CMakeLists.txt` to include them or by creating a dedicated CMake target if you later move to a full plugin system).

