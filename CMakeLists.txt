cmake_minimum_required(VERSION 3.8)
project(log_service C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

SET(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
message(NOTICE "source dir: ${SOURCE_DIR}")

find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
INCLUDE(build_cass_driver.cmake)

add_executable(cass_load_slice_stress_test ${SOURCE_DIR}/cass_load_slice_stress_test.cpp)
target_link_libraries(cass_load_slice_stress_test PUBLIC cassandra_static ${GFLAGS_LIBRARY})



