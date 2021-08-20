cmake_minimum_required(VERSION 3.11)
project(cfs_bench)

set(CMAKE_CXX_STANDARD 14)

# set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
# set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall") set(CMAKE_BUILD_TYPE
# DEBUG)

set(default_build_type Release)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(
    STATUS
      "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE
      "${default_build_type}"
      CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release")
endif()

message("CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")

if(CMAKE_BUILD_TYPE MATCHES Debug)
  message("Debug build.")
elseif(CMAKE_BUILD_TYPE MATCHES Release)
  message("Release build.")
else()
  message("Some other build type.")
endif()

set(CFS_ROOT_DIR "${PROJECT_SOURCE_DIR}/../cfs")
set(CFS_INC "${CFS_ROOT_DIR}/include")
set(CFS_BUILD "${CFS_ROOT_DIR}/build")
message("CFS_BUILD:" ${CFS_BUILD})

set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)

include_directories(${CFS_INC})
include_directories("${PROJECT_SOURCE_DIR}/include" "${PROJECT_SOURCE_DIR}")
include_directories("${CFS_ROOT_DIR}/lib/config4cpp/include")
include_directories("${CFS_ROOT_DIR}/lib/tbb/include")

link_directories(${CFS_BUILD})
link_directories("${CFS_ROOT_DIR}/lib/config4cpp/lib")

# folly dependencies find_package(gflags REQUIRED) set(GFLAGS_INCLUDE_DIR
# /usr/lib/x86_64-linux-gnu) include_directories(${gflags_INCLUDE_DIR})

# find_package(folly REQUIRED) set_and_check(FOLLY_INCLUDE_DIR
# /usr/local/include/folly) set_and_check(FOLLY_CMAKE_DIR
# /usr/local/lib/cmake/folly) if(NOT TARGET Folly::folly)
# include("${FOLLY_CMAKE_DIR}/folly-targets.cmake") endif() set(FOLLY_LIBRARIES
# Folly::folly)

execute_process(
  COMMAND uname -r
  COMMAND sed "s/-.*$//"
  RESULT_VARIABLE tbb_kver_res
  OUTPUT_VARIABLE tbb_kver_out
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT ${tbb_kver_res} EQUAL "0")
  message(FATAL_ERROR "failed to get kernel version")
endif()

file(GLOB tbb_build_path
     "${CFS_ROOT_DIR}/lib/tbb/build/linux_*_kernel${tbb_kver_out}_release")
link_directories("${tbb_build_path}")

option(ENABLE_PCM "Enable PCM" OFF)
if(ENABLE_PCM)
  message(STATUS "Enable PCM")
  include_directories("${CFS_ROOT_DIR}/lib/pcm/")
  message("${CFS_ROOT_DIR}/lib/pcm/")
  find_library(PCM_LIBRARY PCM HINTS "${CFS_ROOT_DIR}/lib/pcm/")
endif()

option(ENABLE_DUMPLOAD "Enable dump load stat" ON)
if (ENABLE_DUMPLOAD)
  message(STATUS "Enable dump load stat")
endif()

# include(TestBigEndian) test_big_endian(LEVELDB_IS_BIG_ENDIAN)

# build cfs_bench
set(RDTSC_SRC
    "${CFS_ROOT_DIR}/src/perfutil/Cycles.cc"
    "${CFS_ROOT_DIR}/src/perfutil/Util.cc" "${CFS_ROOT_DIR}/src/stats/stats.cc"
    "${CFS_ROOT_DIR}/src/stats/timer.cc")
set(BENCH_BIN_NAME cfs_bench)
set(BENCH_SRC
    bench/cfs_bench.cc
    util/testutil.cc
    util/histogram.cc
    util/env.cc
    util/env_posix.cc
    util/status.cc
    util/crc32c.cc
    util/coding.cc
    helpers/memenv/memenv.cc
    # TODO FIXME shmipc is also part of libcfs.so, but the build fails. This may
    # be because it isn't exported. For now, we just add the source here. At a
    # later point, we might want to clean this up.
    ${CFS_ROOT_DIR}/src/shmipc.c
    ${CFS_ROOT_DIR}/src/FsPageCache.cc)

add_executable(${BENCH_BIN_NAME} ${BENCH_SRC} ${RDTSC_SRC})
if(ENABLE_PCM)
  target_compile_definitions(${BENCH_BIN_NAME} PRIVATE USE_PCM_COUNTER)
  target_link_libraries(
    ${BENCH_BIN_NAME} PRIVATE ${PCM_LIBRARY} libconfig4cpp.a libtbb.so.2
                              pthread rt)
else()
  target_link_libraries(${BENCH_BIN_NAME} PRIVATE libconfig4cpp.a libtbb.so.2
                                                  pthread rt)
endif()
if(ENABLE_DUMPLOAD)
  target_compile_definitions(${BENCH_BIN_NAME} PRIVATE DO_DUMPLOAD)
endif()
target_link_libraries(
  ${BENCH_BIN_NAME} PRIVATE libconfig4cpp.a libcfs.so rt libtbb.so.2
                            libtbbmalloc.so.2 pthread)

target_compile_definitions(
  "${BENCH_BIN_NAME}" PRIVATE # falcon@cs.wisc.edu is little endian
                              LEVELDB_IS_BIG_ENDIAN=0)
target_compile_definitions(${BENCH_BIN_NAME} PRIVATE USE_SPDK)
target_compile_definitions(${BENCH_BIN_NAME} PRIVATE LEVELDB_PLATFORM_POSIX)
set_target_properties(
  ${BENCH_BIN_NAME}
  PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins"
             LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins"
             RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins")

# build cfs_bench_posix
set(BENCH_POSIX_BIN_NAME cfs_bench_posix)

add_executable(${BENCH_POSIX_BIN_NAME} ${BENCH_SRC} ${RDTSC_SRC})
if(ENABLE_PCM)
  target_compile_definitions(${BENCH_POSIX_BIN_NAME} PRIVATE USE_PCM_COUNTER)
  target_link_libraries(
    ${BENCH_POSIX_BIN_NAME} PRIVATE ${PCM_LIBRARY} libconfig4cpp.a libtbb.so.2
                                    pthread rt)
else()
  target_link_libraries(${BENCH_POSIX_BIN_NAME} PRIVATE libconfig4cpp.a
                                                        libtbb.so.2 pthread rt)
endif()

target_compile_definitions(
  "${BENCH_POSIX_BIN_NAME}" PRIVATE # falcon@cs.wisc.edu is little endian
                                    LEVELDB_IS_BIG_ENDIAN=0)
target_compile_definitions(${BENCH_POSIX_BIN_NAME}
                           PRIVATE LEVELDB_PLATFORM_POSIX)
target_compile_definitions(${BENCH_POSIX_BIN_NAME} PRIVATE CFS_USE_POSIX)

set_target_properties(
  ${BENCH_POSIX_BIN_NAME}
  PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins"
             LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins"
             RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins")

# build coordinator
set(BENCH_COORDINATOR_BIN_NAME cfs_bench_coordinator)
add_executable(${BENCH_COORDINATOR_BIN_NAME} "bench/coordinator.cc"
                                             "${CFS_ROOT_DIR}/src/shmipc.c")
target_link_libraries(${BENCH_COORDINATOR_BIN_NAME} rt pthread)
# TODO : merge set_target_properties for all three of them. They are the same.
set_target_properties(
  ${BENCH_COORDINATOR_BIN_NAME}
  PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins"
             LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins"
             RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bins")

file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/bins")
