cmake_minimum_required(VERSION 3.12)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
project("RL_DISPATCHER")

add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

include_directories(include/)
file(GLOB SOURCES src/*.cpp)

# --- cnpy (vendored) ---
set(CNPY_DIR ${CMAKE_SOURCE_DIR}/external/cnpy)

add_library(cnpy STATIC ${CNPY_DIR}/cnpy.cpp)
set_target_properties(cnpy PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(cnpy PUBLIC ${CNPY_DIR})

find_package(ZLIB REQUIRED)
target_link_libraries(cnpy PUBLIC ZLIB::ZLIB)

# --- your library ---
add_library(RL_DISPATCHER SHARED ${SOURCES})

find_package(SimGrid REQUIRED)
find_package(FSMod REQUIRED)
find_package(Boost REQUIRED)
find_package(CGSim REQUIRED)
find_package(spdlog REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(fmt REQUIRED)

target_link_libraries(RL_DISPATCHER PUBLIC
  ${SimGrid_LIBRARY}
  ${Boost_LIBRARIES}
  ${FSMOD_LIBRARY}
  spdlog::spdlog
  fmt::fmt
  CGSim::CGSim
  SQLite::SQLite3
  cnpy
)

target_include_directories(RL_DISPATCHER PUBLIC
  ${SimGrid_INCLUDE_DIR}
  ${CGSim_INCLUDE_DIR}
  ${Boost_INCLUDE_DIR}
  ${FSMOD_INCLUDE_DIR}
  include
)

