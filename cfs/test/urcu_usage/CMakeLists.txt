add_executable(
  testLoadAllowance
  testLoadAllowance.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../../src/FsProc_LoadAllowance.cc)
target_link_libraries(testLoadAllowance pthread urcu)
