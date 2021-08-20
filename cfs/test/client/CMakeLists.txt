# ################# test client #################

add_executable(testAppCli testClient_CLI.cc testClient_common.h
                          ../../src/util.cc ../../include/util.h)
target_link_libraries(testAppCli PRIVATE cfs rt ${TBB_LIBRARIES})

add_executable(testAppCliVfs testClient_CLI.cc testClient_common.h)
target_compile_definitions(testAppCliVfs PRIVATE "TEST_VFS_INSTEAD")
target_link_libraries(testAppCli PRIVATE ${TBB_LIBRARIES})

add_executable(testAppCmd testClient_App.cc testClient_common.h
                          ../../src/util.cc ../../include/util.h)
target_link_libraries(testAppCmd PRIVATE cfs ${TBB_LIBRARIES} rt)

add_executable(testCLientCPC_mc testClientCPC_mt.cc testClient_common.h ../../src/util.cc)
target_link_libraries(testCLientCPC_mc cfs ${TBB_LIBRARIES} pthread rt)

add_executable(testDumpToVfs testDumpToVfs.cc testClient_common.h
                             ../../src/util.cc)
target_link_libraries(testDumpToVfs PUBLIC cfs ${TBB_LIBRARIES} rt)

configure_file(fstest_large_rw.py fstest_large_rw.py @ONLY)
configure_file(testCfsVersusVfs.py testCfsVersusVfs.py @ONLY)

add_subdirectory(batch)
add_subdirectory(journaltests)
