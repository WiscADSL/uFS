#include "BlockBuffer.h"
#include "gtest/gtest.h"

// NOTE: REQUIRED to run with valgrind for memory leak
// valgrind --leak-check=yes ${BIN}

namespace {

// split only one block
TEST(SplitBufferItem, T1) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  int index = 1;
  int lockGrabbed;
  bool hasDirty = true;

  int outBlockIdx;

  BlockBufferItem *item;
  item = buffer.getBlock(1000, lockGrabbed, hasDirty, index);
  outBlockIdx = item->getIdx();
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.releaseBlock(1000);
  // buffer.updateBlockIndex(item, index);

  std::unordered_set<BlockBufferItem *> itemSet;
  buffer.splitBufferItemsByIndex(index, itemSet);
  EXPECT_EQ(itemSet.size(), 1);

  for (uint i = 0; i < blockNum - 1; i++) {
    item = buffer.getBlock(1001 + i, lockGrabbed, hasDirty, index);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
    // NOTE, we do not release block here
  }

  item = buffer.getBlock(1000 + blockNum, lockGrabbed, hasDirty, index);
  EXPECT_EQ(item, nullptr);

  // if we get the block again from the buffer, will fail
  item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(item, nullptr);

  for (uint i = 0; i < blockNum - 1; i++) {
    item = buffer.getBlock(1001 + i, lockGrabbed, hasDirty, index);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_NO);
    buffer.releaseBlock(1001 + i);
  }

  std::unordered_set<int> blockIdxSet;
  for (uint i = 0; i < blockNum * 2; i++) {
    item = buffer.getBlock(1001 + i, lockGrabbed, hasDirty);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
    blockIdxSet.emplace(item->getIdx());
    EXPECT_LT(blockIdxSet.size(), blockNum);
    EXPECT_EQ(blockIdxSet.find(outBlockIdx), blockIdxSet.end());
    buffer.releaseBlock(item);
  }
  free(memPtr);
}

// split 2 out of 4 blocks
TEST(SplitBufferItem, T2) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  int index = 1;
  int lockGrabbed;
  bool hasDirty = true;

  BlockBufferItem *item;
  item = buffer.getBlock(1000, lockGrabbed, hasDirty, index);
  int outBlockIdx1 = item->getIdx();
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.releaseBlock(1000);
  // buffer.updateBlockIndex(item, index);

  item = buffer.getBlock(1001, lockGrabbed, hasDirty, index);
  int outBlockIdx2 = item->getIdx();
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  buffer.releaseBlock(1001);
  // buffer.updateBlockIndex(item, index);

  std::unordered_set<BlockBufferItem *> itemSet;
  buffer.splitBufferItemsByIndex(index, itemSet);
  EXPECT_EQ(itemSet.size(), 2);

  std::unordered_set<int> blockIdxSet;
  for (uint i = 0; i < 100; i++) {
    item = buffer.getBlock(1002 + i, lockGrabbed, hasDirty);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
    blockIdxSet.emplace(item->getIdx());
    EXPECT_LT(blockIdxSet.size(), blockNum);
    EXPECT_EQ(blockIdxSet.find(outBlockIdx1), blockIdxSet.end());
    EXPECT_EQ(blockIdxSet.find(outBlockIdx2), blockIdxSet.end());
    buffer.releaseBlock(item);
  }
  free(memPtr);
}

// split out one block, install to another
TEST(SplitBufferItem, T3) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtrSrc = (char *)malloc(blockNum * blockSize);
  BlockBuffer bufferSrc(blockNum, blockSize, memPtrSrc);
  char *memPtrDst = (char *)malloc(blockNum * blockSize);
  BlockBuffer bufferDst(blockNum, blockSize, memPtrDst);

  int index = 1;
  int lockGrabbed;
  bool hasDirty = true;
  BlockBufferItem *item;
  int outBlockIdx;

  item = bufferSrc.getBlock(1000, lockGrabbed, hasDirty, index);
  outBlockIdx = item->getIdx();
  bufferSrc.releaseBlock(1000);
  // bufferSrc.updateBlockIndex(item, index);

  std::unordered_set<BlockBufferItem *> itemSet;
  bufferSrc.splitBufferItemsByIndex(index, itemSet);
  EXPECT_EQ(itemSet.size(), 1);

  std::unordered_set<int> dstBlockIdxSet;
  std::unordered_set<BlockBufferItem *> items;
  for (uint i = 0; i < blockNum; i++) {
    item = bufferDst.getBlock(2000 + i, lockGrabbed, hasDirty);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
    dstBlockIdxSet.emplace(item->getIdx());
    items.emplace(item);
    // do not release block here
  }

  EXPECT_EQ(dstBlockIdxSet.size(), blockNum);
  EXPECT_NE(dstBlockIdxSet.find(outBlockIdx), dstBlockIdxSet.end());

  // now we cannot allocate buffer slot anymore
  item = bufferDst.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(item, nullptr);

  // now we install
  bufferDst.installBufferItemsOfIndex(index, itemSet);
  item = bufferDst.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
  EXPECT_NE(item, nullptr);
  EXPECT_EQ(item->getIdx(), outBlockIdx);
  EXPECT_EQ(items.find(item), items.end());
  bufferDst.releaseBlock(item);

  // release blocks and see if the cache still works
  int rt;
  for (uint i = 0; i < blockNum; i++) {
    rt = bufferDst.releaseBlock(2000 + i);
    EXPECT_TRUE(rt > 0);
  }

  std::unordered_set<BlockBufferItem *> dstItems;
  for (uint i = 0; i < 1000; i++) {
    item = bufferDst.getBlock(3000 + i, lockGrabbed, hasDirty);
    EXPECT_NE(item, nullptr);
    bufferDst.releaseBlock(item);
    dstItems.emplace(item);
  }

  // at this time, dstBuffer should contains 5 items
  EXPECT_EQ(dstItems.size(), blockNum + 1);

  free(memPtrSrc);
  free(memPtrDst);
}

TEST(ReclaimBufferItem, T1) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  std::list<BlockBufferItem *> itemList;
  int rt = buffer.reclaimBufferItemSlots(blockNum, itemList);
  EXPECT_EQ(rt, blockNum);

  BlockBufferItem *item;
  int lockGrabbed;
  bool hasDirty = true;
  item = buffer.getBlock(1000, lockGrabbed, hasDirty);
  EXPECT_EQ(item, nullptr);
  free(memPtr);
}

// cannot reclaim numSlots > current capacity
TEST(ReclaimBufferItem, T2) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  std::list<BlockBufferItem *> itemList;
  int rt = buffer.reclaimBufferItemSlots(blockNum + 1, itemList);
  EXPECT_TRUE(rt < 0);
  free(memPtr);
}

TEST(ReclaimBufferItem, T3) {
  uint32_t blockNum = 4;
  int blockSize = 32;
  char *memPtr = (char *)malloc(blockNum * blockSize);
  BlockBuffer buffer(blockNum, blockSize, memPtr);

  std::list<BlockBufferItem *> itemList;
  int rt = buffer.reclaimBufferItemSlots(2, itemList);
  EXPECT_EQ(rt, 2);

  BlockBufferItem *item;
  int lockGrabbed;
  bool hasDirty = true;

  // buffer can still work
  std::unordered_set<BlockBufferItem *> restItems;
  for (uint i = 0; i < 1000; i++) {
    item = buffer.getBlock(1000 + i, lockGrabbed, hasDirty);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(lockGrabbed, BLOCK_BUF_LOCK_GRAB_YES);
    buffer.releaseBlock(item);
    restItems.emplace(item);
  }

  for (auto ele : itemList) {
    EXPECT_EQ(restItems.find(ele), restItems.end());
  }

  EXPECT_EQ(restItems.size(), (uint)2);
  free(memPtr);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}