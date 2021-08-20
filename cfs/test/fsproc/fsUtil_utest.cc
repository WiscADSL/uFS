#include "gtest/gtest.h"
#include "util.h"

// test the functions that declared in util.h
void outputToken(char *path, std::vector<std::string> &ts) {
  std::cout << path << " to " << std::endl;
  int i = 0;
  for (auto t : ts) std::cout << (++i) << ":" << t << std::endl;
  std::cout << "----" << std::endl;
}
namespace {

TEST(PathTokenTest, T1) {
  std::vector<std::string> tokenVec;
  char path[] = "/abc";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 1);
}

TEST(PathTokenTest, T2) {
  std::vector<std::string> tokenVec;
  char path[] = "//abc";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 1);
}

TEST(PathTokenTest, T3) {
  std::vector<std::string> tokenVec;
  char path[] = "//abc//";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 1);
}

TEST(PathTokenTest, T4) {
  std::vector<std::string> tokenVec;
  char path[] = "/abc/bc/";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 2);
}

TEST(PathTokenTest, T5) {
  std::vector<std::string> tokenVec;
  char path[] = "/abc/bc";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 2);
}

TEST(PathTokenTest, T6) {
  std::vector<std::string> tokenVec;
  char path[] = "///";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 0);
}

TEST(PathTokenTest, T7) {
  std::vector<std::string> tokenVec;
  char path[] = "";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 0);
}

TEST(PathTokenTest, T8) {
  std::vector<std::string> tokenVec;
  char path[] = "a/b/c/d/e/f/g/a/a/1/0///";
  int rc = filepath2Tokens(path, tokenVec);
  outputToken(path, tokenVec);
  EXPECT_EQ(rc, 11);
}

TEST(BLOCK_SET_TEST, T1) {
  char buf[4096];
  memset(buf, 0, 4096);
  std::cout << "first char:" << buf[0] - 0 << std::endl;
  block_set_bit(0, buf);
  std::cout << "first char:" << buf[0] - 0 << std::endl;
  EXPECT_NE(buf[0], 0);
}

TEST(FIND_MULTI_TEST, T1) {
  char buf[4096];
  memset(buf, 0, 4096);
  block_set_bit(1, buf);
  block_set_bit(11, buf);
  block_set_bit(21, buf);
  int start_no_single = find_block_free_bit_no(buf, 4096 * 8);
  EXPECT_EQ(start_no_single, 0);
  block_set_bit(0, buf);
  start_no_single = find_block_free_bit_no(buf, 4096 * 8);
  EXPECT_EQ(start_no_single, 2);

  int start_no_multi = find_block_free_multi_bits_no(buf, 4096 * 8, 8);
  EXPECT_EQ(start_no_multi, 2);

  start_no_multi = find_block_free_multi_bits_no(buf, 4096 * 8, 16);
  EXPECT_EQ(start_no_multi, 22);
}

TEST(FIND_MULTI_TEST, T2) {
  char buf[4096];
  memset(buf, 0, 4096);
  uint numBitsNeeds = 256;
  int maxAlloc = 4096 * 8 / numBitsNeeds;
  int start_no;
  for (int i = 0; i < maxAlloc; i++) {
    start_no = find_block_free_multi_bits_no(buf, 4096 * 8, numBitsNeeds);
    EXPECT_EQ(start_no, numBitsNeeds * i);
    for (uint32_t j = 0; j < numBitsNeeds; j++) {
      block_set_bit(start_no + j, buf);
    }
  }
  int64_t no_bit = find_block_free_bit_no(buf, 4096 * 8);
  EXPECT_LT(no_bit, 0);

  no_bit = find_block_free_multi_bits_no(buf, 4096 * 8, numBitsNeeds);
  fprintf(stdout, "no_bit:%ld\n", no_bit);
  EXPECT_LT(no_bit, 0);
}

TEST(TEST_NEXT_POW2, T1) {
  uint32_t val = 0;
  nextHighestPow2Val(val);
  EXPECT_EQ(val, 0);
  val = 1024;
  nextHighestPow2Val(val);
  EXPECT_EQ(val, 1024);
  val = 1025;
  nextHighestPow2Val(val);
  EXPECT_EQ(val, 2048);
  val = 2047;
  nextHighestPow2Val(val);
  EXPECT_EQ(val, 2048);
  val = 65535;
  nextHighestPow2Val(val);
  EXPECT_EQ(val, 65536);
  fprintf(stdout, "final value:%u\n", val);
}

TEST(TEST_FIND_FREE_BITS_JUMP, T1) {
  const int kBSize = 4096;
  char buf[kBSize];
  memset(buf, 0, kBSize);
  int numBitsJump = 2048;
  int maxAlloc = kBSize * 8 / numBitsJump;
  int64_t start_no;
  for (int i = 0; i < maxAlloc; i++) {
    start_no = find_block_free_jump_bits_no_start_from(buf, 0, kBSize * 8,
                                                       numBitsJump);
    EXPECT_EQ(start_no, numBitsJump * i);
    fprintf(stdout, "start_no:%ld\n", start_no);
    block_set_bit(start_no, buf);
  }
  int64_t no_bit = find_block_free_jump_bits_no_start_from(buf, 0, kBSize * 8, numBitsJump);
  EXPECT_LT(no_bit, 0);
  fprintf(stdout, "no_bit:%ld\n", no_bit);
}

TEST(TEST_FIND_FREE_BITS_JUMP, T2) {
  const int kBSize = 4096;
  char buf[kBSize];
  memset(buf, 0, kBSize);
  int numBitsJump = 2048;
  int maxAlloc = kBSize * 8 / numBitsJump;
  int64_t start_no = 0;
  for (int i = 0; i < maxAlloc; i++) {
    start_no = find_block_free_jump_bits_no_start_from(buf, start_no, kBSize * 8,
                                                       numBitsJump);
    EXPECT_EQ(start_no, numBitsJump * i);
    fprintf(stdout, "start_no:%ld\n", start_no);
    block_set_bit(start_no, buf);
  }
  int64_t no_bit = find_block_free_jump_bits_no_start_from(buf, 0, kBSize * 8, numBitsJump);
  EXPECT_LT(no_bit, 0);
  fprintf(stdout, "no_bit:%ld\n", no_bit);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}