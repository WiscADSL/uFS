cmake_minimum_required(VERSION 3.11)

# set the project name
project(shmipc_tests)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# include the cfs include dir
include_directories("${PROJECT_SOURCE_DIR}/../../include/")

set(SHMIPC_SRC "${PROJECT_SOURCE_DIR}/../../src/shmipc.c")

# Binaries
add_executable(shmipc_test_server server.c ${SHMIPC_SRC})
target_link_libraries(shmipc_test_server rt)

add_executable(shmipc_test_client client.c ${SHMIPC_SRC})
target_link_libraries(shmipc_test_client rt)

add_executable(test_shmipc_async test_shmipc_async.c ${SHMIPC_SRC})
target_link_libraries(test_shmipc_async rt)

add_executable(check_ipc_messages check_ipc_messages.cc)
enable_testing()

add_test(NAME async_tests COMMAND test_shmipc_async)
add_test(NAME sync_test_simple
         COMMAND timeout -s9 5 ${PROJECT_SOURCE_DIR}/run_sync.sh
                 ${CMAKE_BINARY_DIR}/bin)
add_test(NAME sync_test_xreq
         COMMAND timeout -s9 10 ${PROJECT_SOURCE_DIR}/run_sync.sh
                 ${CMAKE_BINARY_DIR}/bin -x)
