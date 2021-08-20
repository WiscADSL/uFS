cmake_minimum_required(VERSION 3.12)
project(cfs)

set(CMAKE_CXX_STANDARD 20)

set(default_build_type Release)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(CFS_JOURNAL_PERF_METRICS "Toggle journal metrics collection" OFF)
option(CFS_DISK_LAYOUT_LEVELDB "Customize disk layout for leveldb" OFF)
option(CFS_DISK_LAYOUT_FILEBENCH "Customize disk layout for filebench" OFF)
# The assumption is that, for sequential read, it will call read
option(CFS_READ_ENABLE_RA "Enable readahead for read()" OFF)
# By default, only the primary runs and the others are started lazily. However,
# it is useful during testing to have all workers active.
option(CFS_START_ALL_WORKERS_ON_INIT "fsMain starts all workers on init" OFF)
# Disables checkpointing for benchmarks that don't care about recovery. It just
# acts as if checkpointing happened and resets the journal super block.
option(CFS_JOURNAL_DISABLE_CHKPT "disables checkpointing (unsafe)" OFF)
option(UFS_EXPR_LBNC "enable load management info output (for benchmark)" OFF)

set(CMAKE_CXX_FLAGS_RelWithAssert "-O3 -Wall")

set(CFS_JOURNAL_TYPE
    "GLOBAL_JOURNAL"
    CACHE STRING "Journal type")
set_property(CACHE CFS_JOURNAL_TYPE PROPERTY STRINGS "NO_JOURNAL"
                                             "GLOBAL_JOURNAL" "LOCAL_JOURNAL")
add_compile_definitions(CFS_JOURNAL_TYPE=CFS_${CFS_JOURNAL_TYPE})

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(
    STATUS
      "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE
      "${default_build_type}"
      CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
      "MinSizeRel" "RelWithDebInfo" "RelWithAssert")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
# NOTE: if comment the bellowing two, the default setting will disable the
# assertion which is useful when run experiment
# set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -DNDEBUG")
# This will enable the assertion without DEBUG output
# set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")
# Do not set build type here set(CMAKE_BUILD_TYPE RELEASE)


# folly dependencies
find_package(gflags REQUIRED)
set(GFLAGS_INCLUDE_DIR /usr/lib/x86_64-linux-gnu)
include_directories(${gflags_INCLUDE_DIR})

find_package(folly REQUIRED)
set_and_check(FOLLY_INCLUDE_DIR /usr/local/include/folly)
set_and_check(FOLLY_CMAKE_DIR /usr/local/lib/cmake/folly)
if(NOT TARGET Folly::folly)
  include("${FOLLY_CMAKE_DIR}/folly-targets.cmake")
endif()
set(FOLLY_LIBRARIES Folly::folly)

add_subdirectory(lib/abseil-cpp)

include_directories(include)
# include_directories(include/radixtree)
include_directories(lib/spdk/include)
include_directories(lib/cppconcurrentqueue)
include_directories(lib/spdlog)
include_directories(lib/abseil-cpp)
include_directories(lib/cxxopts/include)
include_directories(lib/json/single_include/)

find_package(PkgConfig REQUIRED)

option(BUNDLED_CONFIG4CPP "Use bundled config4cpp" ON)
if(BUNDLED_CONFIG4CPP)
  message(STATUS "Using bundled config4cpp")
  include_directories(lib/config4cpp/include)
  link_directories("lib/config4cpp/lib")
  set(CONFIG4CPP_LIBRARIES libconfig4cpp.a)
else()
  pkg_check_modules(CONFIG4CPP REQUIRED config4cpp)
  link_directories(${CONFIG4CPP_LIBRARY_DIRS})
  include_directories(${CONFIG4CPP_INCLUDE_DIRS})
endif()

execute_process(
  COMMAND uname -r
  COMMAND sed "s/-.*$//"
  RESULT_VARIABLE tbb_kver_res
  OUTPUT_VARIABLE tbb_kver_out
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT ${tbb_kver_res} EQUAL "0")
  message(FATAL_ERROR "failed to get kernel version")
endif()

option(BUNDLED_TBB "Use bundled tbb" ON)
if(BUNDLED_TBB)
  message(STATUS "Using bundled tbb")
  # Note: If ld reports `cannot find -ltbb`, check whether the path below exists
  include_directories(lib/tbb/include)
  file(GLOB tbb_build_path
       "lib/tbb/build/linux_*_kernel${tbb_kver_out}_release")
  link_directories("${tbb_build_path}")
  set(TBB_LIBRARIES libtbb.so.2 libtbbmalloc.so.2)
else()
  pkg_check_modules(TBB REQUIRED tbb)
  link_directories(${TBB_LIBRARY_DIRS})
  include_directories(${TBB_INCLUDE_DIRS})
endif()

# #################### Build options #####################
set(CFS_DISK_SIZE
    "858993459200UL"
    CACHE
      STRING
      "The disk size of the underlying physical device in bytes, don't forget the UL at the end"
)
add_definitions(-DEV_SIZE=${CFS_DISK_SIZE})

option(CFS_BUILD_POSIX "Build posix file variant of the executable" OFF)

link_directories("lib/spdk")

# ##############################################################################
# FsProc's main bin
# ##############################################################################

set(FS_MAIN_COMMON_SOURCES
    include/fs_defs.h
    include/util/concurrent-lru-cache.h
    include/rbtree.h
    src/rbtree.cc
    include/shmipc/shmipc.h
    src/shmipc.c
    src/FsProc.cc
    include/util/util_param.h
    include/util/util_common.h
    include/util/util_ring_c11_mem.h
    include/util/util_buf_ring.h
    src/util/util_buf_ring.c
    include/FsProc_FsImpl.h
    src/FsProc_FsImpl.cc
    src/FsProc_Messenger.cc
    src/BlockBuffer.cc
    include/BlockBuffer.h
    src/BlkDevSpdk.cc
    include/BlkDevSpdk.h
    include/typedefs.h
    include/util.h
    src/util.cc
    include/FsLibShared.h
    src/FsLibShared.cc
    include/FsLibProc.h
    include/stats/stats.h
    include/stats/timer.h
    src/stats/stats.cc
    src/stats/timer.cc
    src/FsProc_Worker.cc
    src/FsProc_FileMng.cc
    src/FsProc_FileMngOps.cc
    include/FsProc_Permission.h
    src/FsProc_Permission.cc
    include/FsProc_FileMng.h
    include/FsProc_SplitPolicy.h
    src/FsProc_SplitPolicy.cc
    include/FsProc_FsInternal.h
    # include/radixtree/radix_tree.h
    # include/radixtree/radix_tree_it.h
    # include/radixtree/radix_tree_node.h
    include/FsLibMalloc.h
    src/FsLibMalloc.cc
    include/perfutil/Cycles.h
    src/perfutil/Cycles.cc
    include/perfutil/Initialize.h
    include/perfutil/Util.h
    src/perfutil/Util.cc
    include/perfutil/Histogram.h
    src/perfutil/Histogram.cc
    include/perfutil/Stats.h
    src/perfutil/Stats.cc
    include/FsProc_WorkerMock.h
    src/FsProc_WorkerMock.cc
    include/FsProc_WorkerComm.h
    src/FsProc_WorkerComm.cc
    include/FsProc_App.h
    include/FsProc_FsReq.h
    include/FsProc_Lease.h
    include/FsLibLeaseShared.h
    src/FsLibLeaseShared.cc
    src/FsProc_Lease.cc
    include/FsProc_util.h
    include/FsProc_WorkerStats.h
    include/FsProc_LoadMng.h
    include/FsProc_LoadMngCommon.h
    include/FsProc_KnowParaLoadMng.h
    src/FsProc_KnowParaLoadMng.cc
    src/FsProc_KP_DS.cc
    src/FsProc_WorkerStats.cc
    src/FsProc_LoadMng.cc
    src/FsProc_LoadAllowance.cc
    include/FsPageCache_Shared.h
    include/FsProc_PageCache.h
    src/FsPageCache.cc
    src/FsProc_PageCache.cc
    src/FsProc_TLS.cc
    include/FsProc_UnixSock.h
    src/FsProc_UnixSock.cc
    src/FsProc_FsMain.cc)

# TODO: Use target_sources instead
set(FS_MAIN_JOURNAL_SOURCES src/FsProc_JournalBasic.cc src/FsProc_Journal.cc
                            include/FsProc_Journal.h)


# NOTE: uncomment this when would like to enable absl cpp libs 
set(ABSL_LIBS absl::flat_hash_map absl::hash absl::hashtablez_sampler Threads::Threads absl::strings)
#set(ABSL_LIBS "absl::strings")


# fsMainPosix
if(CFS_BUILD_POSIX)
  set(MAIN_BIN_NAME "fsMainPosix")
  add_executable(${MAIN_BIN_NAME} ${FS_MAIN_COMMON_SOURCES})

  target_link_libraries(
    ${MAIN_BIN_NAME} PRIVATE ${ABSL_LIBS} libspdk.so -lstdc++fs pthread rt
                             ${FOLLY_LIBRARIES})
  # absl::container absl::hash absl::hashtablez_sampler Threads::Threads)
  if(BUNDLED_CONFIG4CPP)
    target_link_libraries(${MAIN_BIN_NAME} PRIVATE ${CONFIG4CPP_LIBRARIES})
  else()
    target_link_libraries(${MAIN_BIN_NAME} PUBLIC ${CONFIG4CPP_LIBRARIES})
  endif()

  if(BUNDLED_TBB)
    target_link_libraries(${MAIN_BIN_NAME} PRIVATE ${TBB_LIBRARIES})
  else()
    target_link_libraries(${MAIN_BIN_NAME} PUBLIC ${TBB_LIBRARIES})
  endif()

  if(CMAKE_BUILD_TYPE MATCHES Debug)
    message("debug mode")
    target_compile_definitions(${MAIN_BIN_NAME}
                               PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
  else()
    message("release mode")
    target_compile_definitions(${MAIN_BIN_NAME}
                               PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
  endif(CMAKE_BUILD_TYPE MATCHES Debug)
endif()

# fsMain
if(ON)
  set(MAIN_BIN_NAME "fsMain")
  add_executable(${MAIN_BIN_NAME} "")
  target_sources(${MAIN_BIN_NAME} PRIVATE ${FS_MAIN_COMMON_SOURCES})
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE USE_SPDK)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE NONE_MT_LOCK)
endif()

message("MAIN_NAME: ${MAIN_BIN_NAME}")

if(CMAKE_BUILD_TYPE MATCHES Debug)
  message("debug mode")
  target_compile_definitions(${MAIN_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
else()
  message("release mode")
  target_compile_definitions(${MAIN_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

if(NOT (CFS_JOURNAL_TYPE STREQUAL "NO_JOURNAL"))
  target_sources(${MAIN_BIN_NAME} PRIVATE ${FS_MAIN_JOURNAL_SOURCES})
endif()

if(CFS_JOURNAL_PERF_METRICS)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE CFS_JOURNAL_PERF_METRICS)
endif()

if(CFS_JOURNAL_DISABLE_CHKPT)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE CFS_JOURNAL_DISABLE_CHKPT)
endif()

if(CFS_DISK_LAYOUT_LEVELDB)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE _EXTENT_FOR_LDB_)
endif()

if(CFS_DISK_LAYOUT_FILEBENCH)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE _EXTENT_FOR_FILEBENCH_)
endif()

if(CFS_READ_ENABLE_RA)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE FSP_ENABLE_ALLOC_READ_RA)
endif()

if(CFS_START_ALL_WORKERS_ON_INIT)
  target_compile_definitions(${MAIN_BIN_NAME}
                             PRIVATE CFS_START_ALL_WORKERS_ON_INIT)
endif()

if(UFS_EXPR_LBNC)
  target_compile_definitions(${MAIN_BIN_NAME} PRIVATE "UFS_EXPR_LBNC")
endif()

find_package(Threads)
# target_compile_definitions(${MAIN_BIN_NAME} PRIVATE MIMIC_FSP_ZC)
target_link_libraries(
  ${MAIN_BIN_NAME}
  PRIVATE ${ABSL_LIBS}
          libspdk.so
          -lstdc++fs
          pthread
          rt
          ${FOLLY_LIBRARIES}
          gflags)
if(BUNDLED_CONFIG4CPP)
  target_link_libraries(${MAIN_BIN_NAME} PRIVATE ${CONFIG4CPP_LIBRARIES})
else()
  target_link_libraries(${MAIN_BIN_NAME} PUBLIC ${CONFIG4CPP_LIBRARIES})
endif()

if(BUNDLED_TBB)
  target_link_libraries(${MAIN_BIN_NAME} PRIVATE ${TBB_LIBRARIES})
else()
  target_link_libraries(${MAIN_BIN_NAME} PUBLIC ${TBB_LIBRARIES})
endif()

# ##############################################################################
# library for client to use
# ##############################################################################
set(CFS_PUB_HDR include/fsapi.h include/fs_defs.h)
set(CFS_LIB_SRC
    include/fs_defs.h
    include/BlkDev.h
    include/FsLibShared.h
    include/FsLibApp.h
    include/FsLibMalloc.h
    include/rbtree.h
    include/shmipc/shmipc.h
    src/FsLib.cc
    src/FsLibShared.cc
    src/util/util_buf_ring.c
    src/FsLibMalloc.cc
    src/rbtree.cc
    src/shmipc.c
    include/perfutil/Cycles.h
    src/perfutil/Cycles.cc
    include/perfutil/Initialize.h
    include/lru.h
    include/FsLibLease.h
    include/FsLibLeaseShared.h
    src/FsLibLeaseShared.cc
    include/FsPageCache_Shared.h
    include/FsLibPageCache.h
    src/FsLibPageCache.cc
    src/FsPageCache.cc
    include/stats/stats.h
    include/stats/timer.h
    src/stats/stats.cc
    src/stats/timer.cc
    src/util.cc)
add_library(cfs SHARED ${CFS_LIB_SRC} ${CFS_PUB_HDR})
set_target_properties(cfs PROPERTIES PUBLIC_HEADER "${CFS_PUB_HDR}")
# link_directories("lib/spdk/build/lib") target_link_libraries(cfs PRIVATE
# libspdk.so) target_compile_definitions(cfs PRIVATE MIMIC_APP_ZC)
# target_compile_definitions(cfs PRIVATE _CFS_LIB_PRINT_REQ_)
target_compile_definitions(cfs PRIVATE NO_INCLUDE_JSON)

# target_link_libraries(cfs)
target_link_libraries(cfs PRIVATE ${ABSL_LIBS})


add_library(cfspg SHARED ${CFS_LIB_SRC} ${CFS_PUB_HDR})

set_target_properties(cfspg PROPERTIES PUBLIC_HEADER "${CFS_PUB_HDR}")
# link_directories("lib/spdk/build/lib")
target_link_libraries(cfspg PRIVATE libspdk.so)
# target_link_directories(cfspg PRIVATE libspdk_env_dpdk.a)
# target_compile_definitions(cfs PRIVATE MIMIC_APP_ZC)
# target_compile_definitions(cfspg PRIVATE _CFS_LIB_PRINT_REQ_)
target_compile_definitions(cfspg PRIVATE NO_INCLUDE_JSON)
target_compile_definitions(cfspg PRIVATE FS_LIB_SPPG)

option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
  add_subdirectory(test)
endif()
