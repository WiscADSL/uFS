#include <stdio.h>

#include "gtest/gtest.h"
#include "util.h"

namespace {

static constexpr int kDirDepthMax = 32;

void outputDelimIdxArr(int *arr) {
  fprintf(stdout, "=======\n");
  for (int i = 0; i < kDirDepthMax; i++) {
    fprintf(stdout, "  %d", arr[i]);
  }
  fprintf(stdout, "\n=======\n");
}

void testHelper(char *path, int expectDepth) {
  int depth = 0;
  int delimArr[kDirDepthMax];
  memset(delimArr, -1, sizeof(int) * kDirDepthMax);
  char *spath = filepath2TokensStandardized(path, delimArr, depth);
  fprintf(stdout, "strlen:%ld\n", strlen(spath));
  fprintf(stdout, "depth:%d\tspath:%s\n", depth, spath);
  outputDelimIdxArr(delimArr);
  EXPECT_EQ(depth, expectDepth);
  free(spath);
}

//
// A set of test case to test the function: "filepath2TokensStandardized"
//

TEST(TestPathTokenStandardized, T1) {
  char path[] = "/abc";
  testHelper(path, 1);
}

TEST(TestPathTokenStandardized, T2) {
  char path[] = "//abc";
  testHelper(path, 1);
}

TEST(TestPathTokenStandardized, T3) {
  char path[] = "//abc//";
  testHelper(path, 1);
}

TEST(TestPathTokenStandardized, T4) {
  char path[] = "/abc/bc/";
  testHelper(path, 2);
}

TEST(TestPathTokenStandardized, T5) {
  char path[] = "/abc/bc";
  testHelper(path, 2);
}

TEST(TestPathTokenStandardized, T6) {
  char path[] = "///";
  testHelper(path, 0);
}

TEST(TestPathTokenStandardized, T7) {
  char path[] = "";
  testHelper(path, 0);
}

TEST(TestPathTokenStandardized, T8) {
  char path[] = "a/b/c/d/e/f/g/a/a/1/0///";
  testHelper(path, 11);
}

TEST(TestPathTokenStandardized, T9) {
  char path[] = "aa/bb/cc/dd/ee/ff/gg/hh/ii/jingliu";
  testHelper(path, 10);
}

template <typename T1, typename T2>
void TestAssembleDessembleCommon(T1 a, T2 b) {
  auto k = AssembleTwo32B<T1, T2>(a, b);
  T1 a_pos;
  T2 b_pos;
  DessembleOne64B<T1, T2>(k, a_pos, b_pos);
  ASSERT_EQ(a_pos, a);
  ASSERT_EQ(b_pos, b);
}

TEST(TestAssemble32To64, Case1) {
  int a = 0, b = 0;
  TestAssembleDessembleCommon(a, b);
  a = 1000;
  b = 1;
  TestAssembleDessembleCommon(a, b);
  a = 0;
  b = 10000;
  TestAssembleDessembleCommon(a, b);
  a = -1, b = 1000;
  TestAssembleDessembleCommon(a, b);
}

TEST(TestAssemble32To64, Case2) {
  uint a = 0, b = 0;
  TestAssembleDessembleCommon(a, b);
  a = 0, b = 1000;
  TestAssembleDessembleCommon(a, b);
  a = 10000, b = 0;
  TestAssembleDessembleCommon(a, b);
  a = 1;
  b = 32768;
  TestAssembleDessembleCommon(a, b);
  a = 999;
  b = 99999;
  TestAssembleDessembleCommon(a, b);
}

TEST(TestAssemble32To64, Case3) {
  int a = 0;
  uint b = 0;
  TestAssembleDessembleCommon(a, b);
  a = 0, b = 1000;
  TestAssembleDessembleCommon(a, b);
  a = 10000, b = 0;
  TestAssembleDessembleCommon(a, b);
  a = 1;
  b = 32768;
  TestAssembleDessembleCommon(a, b);
  a = 999;
  b = 99999;
  TestAssembleDessembleCommon(a, b);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}