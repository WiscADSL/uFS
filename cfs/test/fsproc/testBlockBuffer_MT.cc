#include "BlockBuffer.h"
#include "gtest/gtest.h"
#include <memory>
#include <unistd.h>

namespace {

void func1(BlockBuffer *buf, int tid) {
  int isInMemy;
  bool hasDirty;
  for (int i = 0; i < 100; i++) {
    int curBlockNo = 2000 + i;
  F1_RETRY:
    auto it = buf->getBlock(curBlockNo, isInMemy, hasDirty);
    if (it == nullptr) {
      goto F1_RETRY;
    }
    std::cout << tid << "-blockNo:" << curBlockNo << " idx:" << it->getIdx()
              << std::endl;
    usleep(500);
    buf->releaseBlock(it);
  }
  SUCCEED();
}

void func2(BlockBuffer *buf, int tid) {
  int isInMemy;
  bool hasDirty;
  for (int i = 0; i < 100; i++) {
    int curBlockNo = 2000 + i;
  F2_RETRY:
    auto it = buf->getBlock(curBlockNo, isInMemy, hasDirty);
    if (it == nullptr) {
      goto F2_RETRY;
    }
    std::cout << tid << "-blockNo:" << curBlockNo << " idx:" << it->getIdx()
              << std::endl;
    usleep(500);
    buf->releaseBlock(it);
  }
  SUCCEED();
}

TEST(Blockbuf_MT, T1) {
  uint32_t blockNum = 10;
  int blockSize = 32;
  char *memptr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memptr);
  std::thread t1(func1, &buffer, 1);
  std::thread t2(func2, &buffer, 2);
  t1.join();
  t2.join();
}

} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
