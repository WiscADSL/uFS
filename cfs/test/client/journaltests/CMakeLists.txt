set(COMMON_PRIVATE_LIBRARIES cfs ${TBB_LIBRARIES} rt)

add_executable(basicRWFsync basicRWFsync.cc journaltests_common.cc)
target_link_libraries(basicRWFsync PRIVATE ${COMMON_PRIVATE_LIBRARIES})

add_executable(chkpt chkpt.cc journaltests_common.cc)
target_link_libraries(chkpt PRIVATE ${COMMON_PRIVATE_LIBRARIES})

add_executable(migrate migrate.cc journaltests_common.cc)
target_link_libraries(migrate PRIVATE ${COMMON_PRIVATE_LIBRARIES})

add_executable(initexit init_exit.cc journaltests_common.cc)
target_link_libraries(initexit PRIVATE ${COMMON_PRIVATE_LIBRARIES})

add_executable(cctest_workload cctest_workload.cc journaltests_common.cc)
target_link_libraries(cctest_workload PRIVATE ${COMMON_PRIVATE_LIBRARIES})

add_executable(cctest_postworkload cctest_postworkload.cc
                                   journaltests_common.cc)
target_link_libraries(cctest_postworkload PRIVATE ${COMMON_PRIVATE_LIBRARIES})
