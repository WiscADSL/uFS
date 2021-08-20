set(FSCK_BIN_NAME "fsck")
include_directories(${PROJECT_SOURCE_DIR}/lib/Catch2/single_include)
add_executable(${FSCK_BIN_NAME} fsck.cc)
