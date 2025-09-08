cmake_minimum_required(VERSION 3.12)

message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")
project(CGSim VERSION 1.1)

# Disable annoying warnings
#add_definitions(-DSPDLOG_FMT_EXTERNAL)
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")
#set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find required packages
find_package(Boost     REQUIRED)
find_package(SimGrid   REQUIRED)
find_package(SQLite3   REQUIRED)
find_package(FSMod     REQUIRED)
find_package(spdlog    REQUIRED)
find_package(fmt REQUIRED)

# Add json subdir
add_subdirectory(json)
list(APPEND BUILT_PACKAGES "json")

# Include directories
include_directories(${SimGrid_INCLUDE_DIR} ${Boost_INCLUDE_DIR} ${FSMOD_INCLUDE_DIR} ${CMAKE_SOURCE_DIR}/json/single_include)
include_directories(include/)

# Source files
file(GLOB_RECURSE SOURCE_FILES
#        "src/*.cpp"
        "util/*.cpp"
)

# Build the library
add_library(CGSim SHARED ${SOURCE_FILES})

# Generating the executable
add_executable(cg-sim src/main.cpp)

target_link_libraries(
	CGSim
	${SimGrid_LIBRARY}
        ${Boost_LIBRARIES}
        ${FSMOD_LIBRARY}
        SQLite::SQLite3
        spdlog::spdlog
	fmt::fmt
)

target_link_libraries(cg-sim
    CGSim
)

# ==============================
# Installation Configuration
# ==============================

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)


# Install Library
install(TARGETS CGSim
    EXPORT CGSimTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install header files
install(FILES include/job.h include/DispatcherPlugin.h include/host_extensions.h include/logger.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/CGSim)

# Install the executable
install(TARGETS cg-sim
        DESTINATION ${CMAKE_INSTALL_BINDIR})

# Export targets to a file
install(EXPORT CGSimTargets
	NAMESPACE CGSim::
    	FILE CGSimTargets.cmake
    	DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cmake/CGSim
)

# Create a package configuration file
configure_package_config_file(
        "${CMAKE_SOURCE_DIR}/cmake/CGSimConfig.cmake.in"
        "${CMAKE_BINARY_DIR}/CGSimConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/cmake/CGSim"
)

install(FILES "${CMAKE_BINARY_DIR}/CGSimConfig.cmake"
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cmake/CGSim)

# Generate a version file
write_basic_package_version_file(
        "${CMAKE_BINARY_DIR}/CGSimConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(FILES "${CMAKE_BINARY_DIR}/CGSimConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cmake/CGSim)
