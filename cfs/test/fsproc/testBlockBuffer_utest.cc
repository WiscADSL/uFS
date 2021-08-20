
#include "BlockBuffer.h"
#include "gtest/gtest.h"

namespace {

TEST(BlockBufferTest, T1) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  int rc;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);
  int lockGrabbed;
  bool hasDirty;
  auto item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  auto firstItem = item;
  EXPECT_EQ(item->getIdx(), 0);
  EXPECT_TRUE(lockGrabbed);
  EXPECT_FALSE(item->isInMem());
  item = buffer.getBlock(1001, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 1);
  item = buffer.getBlock(1002, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 2);
  auto lastItem = buffer.getBlock(1003, lockGrabbed, hasDirty);
  EXPECT_EQ(lastItem->getIdx(), 3);
  // locked already, not supposed to get that
  item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(item, firstItem);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_NO);
  // cache full, all the slots are locked, not supposed to get one buffer
  item = buffer.getBlock(1005, lockGrabbed, hasDirty);
  EXPECT_EQ(item, nullptr);
  // start to release
  rc = buffer.releaseBlock(lastItem);
  EXPECT_TRUE(rc >= 0);
  rc = buffer.releaseBlock(1003);
  EXPECT_EQ(rc, -1);
  item = buffer.getBlock(1004, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 3);
  rc = buffer.releaseBlock(firstItem);
  EXPECT_TRUE(rc >= 0);
  EXPECT_EQ(firstItem->getStatus(), BlockBufferStatus::BLOCK_BUF_VALID);
  rc = buffer.releaseBlock(1001);
  EXPECT_TRUE(rc > 0);
  // access first item again
  item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  rc = buffer.releaseBlock(item);
  EXPECT_TRUE(rc > 0);
  item = buffer.getBlock(1010, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 1);

  // clean up
  free(memPtr);
}

}  // namespace

TEST(BlockBufferTest, T2) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  int lockGrabbed;
  bool hasDirty = true;

  auto item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.getBlock(1001, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.getBlock(1002, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.getBlock(1003, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  EXPECT_FALSE(hasDirty);

  bool itemOrigDirty = buffer.setItemDirty(item, true, 0);
  EXPECT_FALSE(itemOrigDirty);

  item = buffer.getBlock(1004, lockGrabbed, hasDirty);
  EXPECT_EQ(item, nullptr);
  EXPECT_NE(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  EXPECT_TRUE(hasDirty);

  // clean up
  free(memPtr);
}

TEST(BlockBufferTest, T3) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  int lockGrabbed;
  bool hasDirty = true;

  auto item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  buffer.releaseBlock(1000);
  buffer.getBlock(1001, lockGrabbed, hasDirty);
  buffer.releaseBlock(1001);
  buffer.getBlock(1002, lockGrabbed, hasDirty);
  buffer.releaseBlock(1002);
  buffer.getBlock(1003, lockGrabbed, hasDirty);
  buffer.releaseBlock(1003);

  // Use 1004-1007 to replace all the four items
  item = buffer.getBlock(1004, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 0);
  buffer.releaseBlock(1004);

  item = buffer.getBlock(1005, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 1);
  buffer.releaseBlock(1005);

  item = buffer.getBlock(1006, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 2);
  buffer.releaseBlock(1006);

  item = buffer.getBlock(1007, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 3);
  buffer.releaseBlock(1007);

  // First use 1004, make it the last one to be replaced
  buffer.getBlock(1004, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.releaseBlock(1004);
  item = buffer.getBlock(1008, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 1);
  buffer.releaseBlock(1008);

  item = buffer.getBlock(1009, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 2);
  buffer.releaseBlock(1009);

  item = buffer.getBlock(1010, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 3);
  buffer.releaseBlock(1010);

  item = buffer.getBlock(1011, lockGrabbed, hasDirty);
  EXPECT_EQ(item->getIdx(), 0);
  buffer.releaseBlock(1011);

  // clean up
  free(memPtr);
}

TEST(BlockBufferFlushTest, T1) {
  uint32_t blockNum = 1024;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  int lockGrabbed;
  bool hasDirty = true;

  auto item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  buffer.setItemDirty(item, true, 0);
  buffer.releaseBlock(1000);

  EXPECT_FALSE(buffer.checkIfNeedBgFlush());
}

TEST(BlockBufferFlushTest, T2) {
  uint32_t blockNum = 800;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  // This is actually the default setting. but explicitly set them here in case
  // that default setting is changed.
  buffer.setDirtyFlushRatio(0.2);
  buffer.setDirtyFlushOneTimeSubmitNum(100);

  int lockGrabbed;
  bool hasDirty = true;
  bool canFlush = true;

  block_no_t flushBlockNumThreshold = blockNum * buffer.getDirtyFlushRatio();
  block_no_t curBlockNo = 1000;
  block_no_t curBlockNum = 0;
  for (; curBlockNum < flushBlockNumThreshold; curBlockNum++) {
    auto item = buffer.getBlock(curBlockNo++, lockGrabbed, hasDirty);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
    buffer.setItemDirty(item, true, 0);
    buffer.releaseBlock(item);
  }

  EXPECT_EQ(buffer.getDirtyItemNum(), curBlockNum);
  EXPECT_FALSE(buffer.checkIfNeedBgFlush());

  curBlockNum++;
  auto item = buffer.getBlock(curBlockNo++, lockGrabbed, hasDirty);
  buffer.setItemDirty(item, true, 0);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.releaseBlock(item);

  // now, the buffer can be flushed.
  EXPECT_TRUE(buffer.checkIfNeedBgFlush());

  std::list<block_no_t> flushBlocks;
  buffer.doFlush(canFlush, flushBlocks);
  EXPECT_EQ(flushBlocks.size(), 100);
  EXPECT_TRUE(canFlush);
  // we need to do this accounting
  buffer.addFgFlushInflightNum(1);

  // Cannot submit another flush request if the last one has not been done.
  std::list<block_no_t> flushBlocksEmpty;
  buffer.setFgFlushLimit(1);
  buffer.doFlush(canFlush, flushBlocksEmpty);
  EXPECT_FALSE(canFlush);
  EXPECT_TRUE(flushBlocksEmpty.empty());

  std::vector<block_no_t> flushBlocksVec;
  for (auto ele : flushBlocks) flushBlocksVec.push_back(ele);
  int rc = buffer.doFlushDone(flushBlocksVec);
  EXPECT_EQ(rc, 0);
  buffer.addFgFlushInflightNum(-1);

  EXPECT_EQ(buffer.getDirtyItemNum(), curBlockNum - 100);
  EXPECT_FALSE(buffer.checkIfNeedBgFlush());

  flushBlocks.clear();
  flushBlocksVec.clear();
  buffer.doFlush(canFlush, flushBlocks);
  EXPECT_TRUE(canFlush);
  EXPECT_EQ(flushBlocks.size(), std::min(curBlockNum - 100, (uint32_t)100));
  buffer.addFgFlushInflightNum(1);
  for (auto ele : flushBlocks) flushBlocksVec.push_back(ele);
  buffer.doFlushDone(flushBlocksVec);
  EXPECT_EQ(buffer.getDirtyItemNum(), 0);
  buffer.addFgFlushInflightNum(-1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}