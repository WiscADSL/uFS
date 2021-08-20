
#include <stdint.h>
#include <iostream>
#include "FsProc_util.h"
#include "gtest/gtest.h"

namespace {

typedef uint32_t valT;
typedef uint64_t cntT;

TEST(maintain2largest, T1) {
  std::unordered_map<valT, cntT> valAcCntMap;
  valT defaultVal = 0, maxVal = 0, secMaxVal = 0;
  cntT cntMax = 0, secCntMax = 0;

  std::vector<valT> accessList = {1, 2, 3, 1, 2, 3, 1, 2, 1, 1};
  for (auto ele : accessList) {
    maintainTwoLargestEle(valAcCntMap, ele, defaultVal, cntMax, secCntMax,
                          maxVal, secMaxVal);
  }
  ASSERT_EQ(maxVal, 1);
  ASSERT_EQ(secMaxVal, 2);
  ASSERT_EQ(cntMax, 5);
  ASSERT_EQ(secCntMax, 3);
}

TEST(maintain2largest, T2) {
  std::unordered_map<valT, cntT> valAcCntMap;
  valT defaultVal = 0, maxVal = 0, secMaxVal = 0;
  cntT cntMax = 0, secCntMax = 0;

  std::vector<valT> accessList = {1, 2, 1, 2, 1, 2, 1, 2};
  for (auto ele : accessList) {
    maintainTwoLargestEle(valAcCntMap, ele, defaultVal, cntMax, secCntMax,
                          maxVal, secMaxVal);
  }
  ASSERT_EQ(maxVal, 1);
  ASSERT_EQ(secMaxVal, 2);
  ASSERT_EQ(cntMax, 4);
  ASSERT_EQ(secCntMax, 4);
}

TEST(maintain2largest, T3) {
  std::unordered_map<valT, cntT> valAcCntMap;
  valT defaultVal = 0, maxVal = 0, secMaxVal = 0;
  cntT cntMax = 0, secCntMax = 0;

  std::vector<valT> accessList = {1, 2, 1, 2, 1, 2, 1, 2};
  for (auto ele : accessList) {
    maintainTwoLargestEle(valAcCntMap, ele, defaultVal, cntMax, secCntMax,
                          maxVal, secMaxVal);
  }
  ASSERT_EQ(maxVal, 1);
  ASSERT_EQ(secMaxVal, 2);
  ASSERT_EQ(cntMax, 4);
  ASSERT_EQ(secCntMax, 4);
}

TEST(maintain2largest, T4) {
  std::unordered_map<valT, cntT> valAcCntMap;
  valT defaultVal = 0, maxVal = 0, secMaxVal = 0;
  cntT cntMax = 0, secCntMax = 0;

  std::vector<valT> accessList = {1, 1, 1, 1};
  for (auto ele : accessList) {
    maintainTwoLargestEle(valAcCntMap, ele, defaultVal, cntMax, secCntMax,
                          maxVal, secMaxVal);
  }
  ASSERT_EQ(maxVal, 1);
  ASSERT_EQ(secMaxVal, defaultVal);
  ASSERT_EQ(cntMax, 4);
  ASSERT_EQ(secCntMax, 0);
}

TEST(maintain2largest, T5) {
  std::unordered_map<valT, cntT> valAcCntMap;
  valT defaultVal = 0, maxVal = 0, secMaxVal = 0;
  cntT cntMax = 0, secCntMax = 0;

  std::vector<valT> accessList = {1, 1, 1, 1, 2, 2, 3, 3, 3};
  for (auto ele : accessList) {
    maintainTwoLargestEle(valAcCntMap, ele, defaultVal, cntMax, secCntMax,
                          maxVal, secMaxVal);
  }
  ASSERT_EQ(maxVal, 1);
  ASSERT_EQ(secMaxVal, 3);
  ASSERT_EQ(cntMax, 4);
  ASSERT_EQ(secCntMax, 3);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}