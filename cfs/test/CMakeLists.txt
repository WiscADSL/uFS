message("CFS_BUILD:" ${CFS_BUILD})
add_subdirectory(googletest)

# utility tests ########

# add_subdirectory(utiltests/radixtree)

set(FS_PERF_UTIL_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/perfutil/Stats.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/perfutil/Cycles.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/perfutil/Histogram.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/perfutil/Util.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/stats/stats.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/stats/timer.cc)

set(FS_FUNC_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/util.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsLibMalloc.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_TLS.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_Worker.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_WorkerComm.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/util/util_buf_ring.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsLibShared.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_FileMng.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_WorkerStats.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_KnowParaLoadMng.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_KP_DS.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_FileMngOps.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_SplitPolicy.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_FsImpl.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_Messenger.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/BlockBuffer.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/shmipc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsPageCache.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_LoadMng.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_LoadAllowance.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_PageCache.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_Lease.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsLibLeaseShared.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_UnixSock.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/rbtree.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_Permission.cc)

if(NOT (CFS_JOURNAL_TYPE STREQUAL "NO_JOURNAL"))
  set(FS_FUNC_SOURCES
      ${FS_FUNC_SOURCES}
      ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_JournalBasic.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsProc_Journal.cc)
endif()

set(FS_SPDK_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/../src/BlkDevSpdk.cc)

set(FS_BLOCK_BUF_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../src/BlockBuffer.cc)

set(FS_BUF_RING_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../src/util/util_buf_ring.c
                    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FsLibShared.cc)

# ##############################################################################
# benchmarks #
if(0)
  add_executable(bench_simple_cfs benchmark/simpleBench.cc)
  target_link_libraries(bench_simple_cfs PRIVATE cfs pthread rt)
  target_compile_definitions(bench_simple_cfs PRIVATE USE_SPDK)
  set_target_properties(
    bench_simple_cfs
    PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
               LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
               RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark")
  file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark")

  add_executable(bench_simple_posix benchmark/simpleBench.cc
                                    benchmark/posix_wrapper.cc)
  target_link_libraries(bench_simple_posix pthread)
  set_target_properties(
    bench_simple_posix
    PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
               LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
               RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark")
  file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark")
endif()
#
# ##############################################################################

# ############### gtest Example #################
# add_executable(gtestSample gtestExample/factorial_utest.cc
# gtestExample/factorial.h gtestExample/factorial.cc)
# target_link_libraries(gtestSample gtest pthread)

# Filesystem main process tester

add_subdirectory(fsproc)
add_subdirectory(client)
## add_subdirectory(urcu_usage)
option(BUILD_FSCK "Build fsck" OFF)
if(BUILD_FSCK)
  add_subdirectory(fsck)
endif()
