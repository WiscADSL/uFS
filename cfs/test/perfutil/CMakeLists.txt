cmake_minimum_required(VERSION 3.11)
project(perfutiltest)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

set(CMAKE_BUILD_TYPE RELEASE)

set(CFS_ROOT_DIR "${PROJECT_SOURCE_DIR}/../../")
set(PERF_UTIL_INC "${CFS_ROOT_DIR}/include/")
set(PERF_UTIL_SRC "${CFS_ROOT_DIR}/src/perfutil/")
message("PERF_UTIL_SRC:" ${PERF_UTIL_SRC})
#set(GTEST_DIR "../googletest/googletest")

include_directories(${PERF_UTIL_INC})

#include_directories(${GTEST_DIR}/include)
#link_directories(${GTEST_DIR}/lib)

add_executable(StatsTest StatsTest.cc
        ${PERF_UTIL_SRC}/Cycles.cc ${PERF_UTIL_SRC}/Histogram.cc
        ${PERF_UTIL_SRC}/Util.cc  ${PERF_UTIL_SRC}/Stats.cc)
