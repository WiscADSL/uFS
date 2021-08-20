# test functionality of fsp ####
set(SPDLOG_LEVEL SPDLOG_LEVEL_INFO)
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(SPDLOG_LEVEL SPDLOG_LEVEL_DEBUG)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

if(OFF)
  set(FS_FUNC_TEST1_BIN fsFuncTest1)
  add_executable(
    ${FS_FUNC_TEST1_BIN}
    ${FS_SPDK_SOURCES} ${FS_FUNC_SOURCES} ${FS_PERF_UTIL_SOURCES}
    ../../src/FsLib.cc fsFuncTest1.cc)
  target_link_libraries(
    ${FS_FUNC_TEST1_BIN}
    PRIVATE libspdk.so
            ${CONFIG4CPP_LIBRARIES}
            -lstdc++fs
            ${TBB_LIBRARIES}
            gtest
            pthread
            rt)
  target_compile_definitions(${FS_FUNC_TEST1_BIN} PRIVATE USE_SPDK)
endif()

# Fs RW by blocking layer interace ####
set(RW_UTIL_BIN_NAME "testRWFsUtil")
add_executable(
  ${RW_UTIL_BIN_NAME} ${FS_SPDK_SOURCES} ${FS_FUNC_SOURCES}
                      ${FS_PERF_UTIL_SOURCES} ../../src/util.cc fsRW.cc)
target_link_libraries(
  ${RW_UTIL_BIN_NAME}
  PRIVATE libspdk.so
          ${CONFIG4CPP_LIBRARIES}
          ${ABSL_LIBS}
          -lstdc++fs
          ${TBB_LIBRARIES}
          pthread
          rt
          ${FOLLY_LIBRARIES}
          gflags)
target_compile_definitions(${RW_UTIL_BIN_NAME} PRIVATE USE_SPDK)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  target_compile_definitions(${RW_UTIL_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
  # target_compile_definitions(${FS_FUNC_TEST1_BIN} PRIVATE
  # SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
else()
  target_compile_definitions(${RW_UTIL_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

if(ON)
  # the compliation of testRWFsUtilPosix
  set(RW_UTIL_POSIX_BIN_NAME "testRWFsUtilPosix")
  add_executable(
    ${RW_UTIL_POSIX_BIN_NAME} ${FS_SPDK_SOURCES} ${FS_FUNC_SOURCES}
                              ${FS_PERF_UTIL_SOURCES} ../../src/util.cc fsRW.cc)
  target_link_libraries(
    ${RW_UTIL_POSIX_BIN_NAME}
    PRIVATE libspdk.so
            ${CONFIG4CPP_LIBRARIES}
            ${ABSL_LIBS}
            -lstdc++fs
            ${TBB_LIBRARIES}
            pthread
            rt
            ${FOLLY_LIBRARIES}
            gflags)
endif()

# Fs RW by blocking layer interace ####

if(NOT (CFS_JOURNAL_TYPE STREQUAL "NO_JOURNAL"))
  set(OFFLINE_CHKPT_BIN_NAME "fsProcOfflineCheckpointer")
  add_executable(
    ${OFFLINE_CHKPT_BIN_NAME} fsOfflineCheckpointer.cc ../../src/util.cc
                              ../../src/FsProc_JournalBasic.cc)
  target_link_libraries(${OFFLINE_CHKPT_BIN_NAME} PRIVATE libspdk.so
                                                          ${FOLLY_LIBRARIES})
  target_compile_definitions(${OFFLINE_CHKPT_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=${SPDLOG_LEVEL})
endif()

set(DUMP_METADATA_BIN_NAME "fsProcDumpMetadata")
add_executable(${DUMP_METADATA_BIN_NAME} fsProcDumpMetadata.cc
                                         ../../src/util.cc)
target_link_libraries(${DUMP_METADATA_BIN_NAME} PRIVATE libspdk.so
                                                        ${FOLLY_LIBRARIES})
target_compile_definitions(${DUMP_METADATA_BIN_NAME}
                           PRIVATE SPDLOG_ACTIVE_LEVEL=${SPDLOG_LEVEL})

target_link_libraries(
  ${RW_UTIL_BIN_NAME}
  PRIVATE libspdk.so
          ${CONFIG4CPP_LIBRARIES}
          ${ABSL_LIBS}
          -lstdc++fs
          ${TBB_LIBRARIES}
          pthread
          rt
          ${FOLLY_LIBRARIES}
          gflags)
target_compile_definitions(${RW_UTIL_BIN_NAME} PRIVATE USE_SPDK)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  target_compile_definitions(${RW_UTIL_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
  # target_compile_definitions(${FS_FUNC_TEST1_BIN} PRIVATE
  # SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
else()
  target_compile_definitions(${RW_UTIL_BIN_NAME}
                             PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# test spdkdev ####
if(0)
  add_executable(testBlkDevSpdk ${FS_SPDK_SOURCES} ${FS_FUNC_SOURCES}
                                testBlkDevSpdk.cc)
  target_link_libraries(
    testBlkDevSpdk PRIVATE libspdk.so ${CONFIG4CPP_LIBRARIES} -lstdc++fs
                           ${TBB_LIBRARIES} pthread rt)
  target_compile_definitions(testBlkDevSpdk PRIVATE USE_SPDK)
endif()

# test block buffer ####
add_executable(blockBuffer_utest ${FS_BLOCK_BUF_SRC} testBlockBuffer_utest.cc)
target_link_libraries(blockBuffer_utest gtest pthread rt)

# test block buffer 2 ####
add_executable(blockBuffer_utest2 ${FS_BLOCK_BUF_SRC} testBlockBuffer_utest2.cc)
target_link_libraries(blockBuffer_utest2 gtest pthread rt)

if(0)
  add_executable(blockBuffer_MT ${FS_BLOCK_BUF_SRC} testBlockBuffer_MT.cc)
  target_link_libraries(blockBuffer_MT gtest pthread rt)
endif()

# test buf ring ####
if(0)
  add_executable(testUtilRingBuf ${FS_BUF_RING_SRC} testUtilRingBuf.cc)
  add_executable(testOpRingBuf ${FS_BUF_RING_SRC} testOpRingBuffer_MT.cc)
  target_link_libraries(testOpRingBuf gtest pthread rt)
endif()

# test util ####
add_executable(testUtil_utest ../../include/util.h ../../src/util.cc
                              fsUtil_utest.cc)
target_link_libraries(testUtil_utest gtest pthread rt)
add_executable(fsTest_Util2 ../../include/util.h ../../src/util.cc
                            fsTest_Util2.cc)
target_link_libraries(fsTest_Util2 gtest pthread rt)

# fsproc_util
add_executable(fsTest_fsprocutil ../../include/FsProc_util.h
                                 fsTest_fsprocUtil.cc)
target_link_libraries(fsTest_fsprocutil gtest)

# kplm
add_executable(
  fsTest_kplm
  ../../include/FsProc_KnowParaLoadMng.h ../../include/FsProc_LoadMng.h
  ../../include/FsProc_LoadMngCommon.h ${FS_PERF_UTIL_SOURCES}
  ${FS_FUNC_SOURCES} fsTest_KPLm.cc)
target_link_libraries(
  fsTest_kplm
  PRIVATE libspdk.so
          ${CONFIG4CPP_LIBRARIES}
          ${ABSL_LIBS}
          -lstdc++fs
          ${TBB_LIBRARIES}
          gtest
          pthread
          rt
          ${FOLLY_LIBRARIES}
          gflags)

add_executable(
  fsTest_kpds
  ../../include/FsProc_KnowParaLoadMng.h ../../include/FsProc_LoadMngCommon.h
  ../../src/FsProc_KP_DS.cc fsTest_KP_DS.cc)

target_link_libraries(fsTest_kpds PRIVATE gtest)

# test FsInternal.h ####
add_executable(fsTest_FsInternal ../../include/FsProc_FsInternal.h
                                 fsTest_FsInternal.cc)
target_link_libraries(fsTest_FsInternal gtest pthread rt)
if(CFS_DISK_LAYOUT_LEVELDB)
  target_compile_definitions(fsTest_FsInternal PRIVATE _EXTENT_FOR_LDB_)
endif()

# test FsInternal.h (sepcifically the on-disk format) ####
add_executable(fsTest_FsInternal_DiskDataFmt ../../include/FsProc_FsInternal.h
                                             fsTest_FsInternal_DiskDataFmt.cc)
target_link_libraries(fsTest_FsInternal_DiskDataFmt gtest pthread rt)

# test FsProc_FsImpl.h (specifically the mem-buf format) ####
add_executable(fsTest_FsImpl_MemBufLayout ../../include/FsProc_FsImpl.h
                                          fsTest_FsImpl_MemBufLayout.cc)
target_link_libraries(fsTest_FsImpl_MemBufLayout pthread rt ${FOLLY_LIBRARIES}
                      gflags)

# test file path cache ####
add_executable(
  fsTest_FsReq_pathCache
  ../../include/FsProc_Fs.h ${FS_SPDK_SOURCES} ${FS_FUNC_SOURCES}
  ${FS_PERF_UTIL_SOURCES} fsTest_FsReq_pathCache.cc)
target_link_libraries(
  fsTest_FsReq_pathCache
  libspdk.so
  ${CONFIG4CPP_LIBRARIES}
  ${ABSL_LIBS}
  -lstdc++fs
  ${TBB_LIBRARIES}
  gtest
  pthread
  rt
  ${FOLLY_LIBRARIES}
  gflags)
target_compile_definitions(fsTest_FsReq_pathCache PRIVATE _CFS_TEST_)
target_compile_definitions(fsTest_FsReq_pathCache PRIVATE USE_SPDK)

add_executable(fsTest_rbtree ../../include/rbtree.h ../../src/rbtree.cc
                             fsTest_rbtree.cc)
target_link_libraries(fsTest_rbtree gtest pthread)

# test FsLib's malloc ####
add_executable(
  fsTest_FsLibMalloc ../../include/FsLibMalloc.h ../../src/FsLibMalloc.cc
                     fsTest_FsLibMalloc.cc ${FS_PERF_UTIL_SOURCES})
target_link_libraries(fsTest_FsLibMalloc gtest pthread rt)

# test FsLib's multi-threading ####
if(OFF)
  add_executable(
    fsTest_FsLibMultiThreads
    ${FS_SPDK_SOURCES}
    ${FS_FUNC_SOURCES}
    ${FS_PERF_UTIL_SOURCES}
    ../../include/util.h
    ../../include/FsLibMalloc.h
    ../../src/FsLibMalloc.cc
    ../../include/FsProc_WorkerMock.h
    ../../src/FsProc_WorkerMock.cc
    fsTest_FsLibMultiThreads.cc)
  target_link_libraries(
    fsTest_FsLibMultiThreads
    ${CONFIG4CPP_LIBRARIES}
    ${ABSL_LIBS}
    libspdk.so
    -lstdc++fs
    ${TBB_LIBRARIES}
    gtest
    pthread
    rt
    ${FOLLY_LIBRARIES}
    gflags)
endif()
