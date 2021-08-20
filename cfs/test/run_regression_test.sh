#! /bin/bash
set -e

# The regression test of FSP

# Always build in this directory
TEST_BIN_DIR=../build/test/fsproc/

# If adding new test case, simply add testcase into this list.
# E.g., ("${TEST_BIN_DIR}/foo" "${TEST_BIN_DIR}/aaa")
declare -a TestBinArray=(\
    "${TEST_BIN_DIR}/blockBuffer_utest" \
    "${TEST_BIN_DIR}/testUtil_utest" \
    "${TEST_BIN_DIR}/fsTest_FsReq_pathCache" \
    "${TEST_BIN_DIR}/fsTest_Util2" \
    )

for val in ${TestBinArray[@]}; do
   echo "regression test case: $val"
   # invoke test case
   $val
done

