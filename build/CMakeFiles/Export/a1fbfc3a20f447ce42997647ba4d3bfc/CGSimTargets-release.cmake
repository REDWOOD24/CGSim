#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "CGSim::CGSim" for configuration "Release"
set_property(TARGET CGSim::CGSim APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(CGSim::CGSim PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libCGSim.so"
  IMPORTED_SONAME_RELEASE "libCGSim.so"
  )

list(APPEND _cmake_import_check_targets CGSim::CGSim )
list(APPEND _cmake_import_check_files_for_CGSim::CGSim "${_IMPORT_PREFIX}/lib/libCGSim.so" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
