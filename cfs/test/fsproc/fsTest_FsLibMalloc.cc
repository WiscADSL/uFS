
#include "FsLibMalloc.h"
#include "gtest/gtest.h"

#include <string>

namespace {

TEST(TEST_FsLibBuddyAllocator, T1) {
  const fslib_malloc_block_sz_t blockSize = 4096;
  const fslib_malloc_block_cnt_t numBlocks = 1024;

  auto memBytes =
      SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(blockSize, numBlocks);
  auto headerBytes =
      SingleSizeMemBlockArr::computeBlockArrShmHeaderBytes(numBlocks);

  fprintf(stdout, "memBytes:%lu\n", memBytes);
  EXPECT_EQ(headerBytes % SingleSizeMemBlockArr::kHeaderAlignBytes, 0);
  EXPECT_EQ(headerBytes + blockSize * numBlocks, memBytes);

  char *memPtr = (char *)malloc(memBytes);
  std::string memName = "test1";
  auto singleSzMemArr = new SingleSizeMemBlockArr(blockSize, numBlocks,
                                                  memBytes, memPtr, memName);
  auto allocator = new FsLibBuddyAllocator(singleSzMemArr);
  allocator->dumpToStdout();

  int allocKb;
  allocKb = 1;
  MemBlockMeta *curMeta = nullptr;
  curMeta = allocator->doAllocate(allocKb);
  EXPECT_EQ(curMeta, nullptr);

  std::vector<int> kbVec = {4, 5, 6, 7, 4};
  std::vector<int> numPagesVec = {1, 2, 2, 2, 1};
  std::vector<MemBlockMeta *> blocks(kbVec.size());
  for (uint i = 0; i < kbVec.size(); i++) {
    allocKb = kbVec[i];
    fprintf(stdout, "-------doAllocate(%d KB)\n", allocKb);
    curMeta = allocator->doAllocate(allocKb);
    EXPECT_NE(curMeta, nullptr);
    EXPECT_EQ(curMeta->numPages, numPagesVec[i]);
    allocator->dumpToStdout();
    blocks[i] = curMeta;
  }

  for (auto metaPtr : blocks) {
    fprintf(stdout, "-------doFree(numPages:%d, shmId:%u)\n", metaPtr->numPages,
            metaPtr->shmInnerId);
    allocator->doFree(metaPtr);
    allocator->dumpToStdout();
  }

  delete allocator;
  delete singleSzMemArr;
  free(memPtr);
}

TEST(TEST_FsLibBuddyAllocator, T2) {
  const fslib_malloc_block_sz_t blockSize = 4096;
  const fslib_malloc_block_cnt_t numBlocks = 1024;

  auto memBytes =
      SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(blockSize, numBlocks);
  auto headerBytes =
      SingleSizeMemBlockArr::computeBlockArrShmHeaderBytes(numBlocks);

  fprintf(stdout, "memBytes:%lu\n", memBytes);
  EXPECT_EQ(headerBytes % SingleSizeMemBlockArr::kHeaderAlignBytes, 0);
  EXPECT_EQ(headerBytes + blockSize * numBlocks, memBytes);

  char *memPtr = (char *)malloc(memBytes);
  std::string memName = "test2";
  auto singleSzMemArr = new SingleSizeMemBlockArr(blockSize, numBlocks,
                                                  memBytes, memPtr, memName);
  auto allocator = new FsLibBuddyAllocator(singleSzMemArr);
  allocator->dumpToStdout();

  std::vector<int> kbVec = {4, 5, 6, 7, 4, 8, 12, 24, 16, 4, 5, 6, 8, 32};
  std::vector<int> numPagesVec;
  for (auto kb : kbVec) {
    numPagesVec.push_back((kb - 1) / 4 + 1);
  }
  std::vector<MemBlockMeta *> blocks(kbVec.size());
  int allocKb;
  MemBlockMeta *curMeta = nullptr;
  for (uint i = 0; i < kbVec.size(); i++) {
    allocKb = kbVec[i];
    fprintf(stdout, "-------doAllocate(%d KB)\n", allocKb);
    curMeta = allocator->doAllocate(allocKb);
    EXPECT_NE(curMeta, nullptr);
    EXPECT_EQ(curMeta->numPages, numPagesVec[i]);
    allocator->dumpToStdout();
    blocks[i] = curMeta;
  }

  for (auto metaPtr : blocks) {
    fprintf(stdout, "-------doFree(numPages:%d, shmId:%u)\n", metaPtr->numPages,
            metaPtr->shmInnerId);
    allocator->doFree(metaPtr);
    allocator->dumpToStdout();
  }

  delete allocator;
  delete singleSzMemArr;
  free(memPtr);
}

TEST(TEST_FsLibLinearListsAllocator, T1) {
  ////
  // test constant variables
  constexpr static int kBlockSizeNum = 3;
  constexpr static fslib_malloc_mem_sz_t kPerSizeTotalBytes[] = {
      2 * 4 * 1024,        /* 8 K for 4K pages*/
      2 * 128 * 1024,      /* 256 for 128K pages*/
      2 * 2 * 1024 * 1024, /* 4 M for 2M pages*/
  };
  constexpr static fslib_malloc_block_sz_t kBlockSizeArr[] = {
      /*4K*/ 4 * 1024,
      /*128K*/ 128 * 1024,
      /*2M*/ 2 * 1024 * 1024};
  ////
  // create and init allocator
  std::vector<SingleSizeMemBlockArr *> bufferMemArrVec(kBlockSizeNum);
  std::vector<void *> shmStartPtrVec(kBlockSizeNum);
  std::vector<fslib_malloc_mem_sz_t> shmBytesVec(kBlockSizeNum);
  fslib_malloc_block_sz_t curBlockSz;
  fslib_malloc_block_cnt_t curBlockCnt;
  int appid = 0x0318;
  int memMngId = 1;
  fslib_malloc_mem_sz_t bufferTotalDataSz = 0;
  for (int i = 0; i < kBlockSizeNum; i++) {
    curBlockSz = kBlockSizeArr[i];
    curBlockCnt = kPerSizeTotalBytes[i] / curBlockSz;
    auto curTotalMemSz = SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(
        curBlockSz, curBlockCnt);
    bufferTotalDataSz += (curBlockSz * curBlockCnt);
    std::stringstream ss;
    getSingleSizeMemBlockArrName(curBlockSz / 1024, appid, memMngId, ss);
    auto shmNameStr = ss.str();
    void *curTotalMemPtr = malloc(curTotalMemSz);
    ASSERT_NE(curTotalMemPtr, nullptr);
    if (curTotalMemPtr != nullptr) {
      bufferMemArrVec[i] = new SingleSizeMemBlockArr(
          curBlockSz, curBlockCnt, curTotalMemSz, curTotalMemPtr, shmNameStr);
      shmBytesVec[i] = curTotalMemSz;
      shmStartPtrVec[i] = curTotalMemPtr;
    }
  }

  auto linearAllocator = new FsLibLinearListsAllocator(
      kBlockSizeNum, kBlockSizeArr, bufferMemArrVec);
  EXPECT_EQ(bufferTotalDataSz, linearAllocator->getSize());
  EXPECT_EQ(bufferTotalDataSz, linearAllocator->getFreeSize());

  MemBlockMeta *curMeta = nullptr;
  void *addr = nullptr;
  int err;
  // allocate a block that is too large
  fslib_malloc_block_sz_t tooLargeblockSz = 4 * 1024 * 1024;
  curMeta = linearAllocator->doAllocate(&addr, tooLargeblockSz, err);
  ASSERT_EQ(curMeta, nullptr);
  ASSERT_EQ(err, FsLibMem_ERR_MALLOC_SIZE_NOT_SUPPORT);
  // all these should be okay to allocate and use up all the blocks
  std::vector<fslib_malloc_mem_sz_t> blkKbSzVec = {3, 4, 5, 128, 129, 256};
  err = 0;
  std::vector<MemBlockMeta *> blocks;
  for (auto blkKbSz : blkKbSzVec) {
    curMeta = linearAllocator->doAllocate(&addr, blkKbSz * 1024, err);
    ASSERT_NE(curMeta, nullptr);
    ASSERT_NE(addr, nullptr);
    fprintf(stdout, "doAllocateDone offset:%ld\n", curMeta->dataPtrOffset);
    blocks.push_back(curMeta);
  }

  ASSERT_EQ(linearAllocator->getFreeSize(), 0);

  // allocate more but cannot since no free block
  curMeta = linearAllocator->doAllocate(&addr, 1024, err);
  ASSERT_EQ(curMeta, nullptr);
  ASSERT_EQ(err, FsLibMem_ERR_FREE_NOT_FOUND);

  fprintf(stdout, "=== doFree() all allocated blocks ===\n");

  // test free
  for (auto b : blocks) {
    linearAllocator->doFree(b);
  }
  ASSERT_EQ(linearAllocator->getFreeSize(), bufferTotalDataSz);

  // tear down
  for (auto memPtr : shmStartPtrVec) {
    free(memPtr);
  }
}

TEST(TEST_FsLibMemMng, T1) {
  int appid = 0x0318;
  int memMngId = 1;
  // fslib_malloc_block_cnt_t numCachePages = 4;
  FsLibMemMng *fslibMem = new FsLibMemMng(appid, memMngId);
  ASSERT_FALSE(fslibMem->isInitDone());
  fslibMem->init(false);
  ASSERT_TRUE(fslibMem->isInitDone());

  char *dataPtr = nullptr;
  bool infoGet = false;
  int err = 0;
  dataPtr = static_cast<char *>(fslibMem->Malloc(16, err));
  ASSERT_NE(dataPtr, nullptr);
  ASSERT_EQ(err, 0);

  static int maxShmNameLen = 128;
  fslib_malloc_block_cnt_t ptrOffset;
  fslib_malloc_block_sz_t shmBlockSize;
  fslib_malloc_block_cnt_t shmNumBlocks;
  char shmName[maxShmNameLen];
  infoGet =
      fslibMem->getBufOwnerInfo(dataPtr, shmName, maxShmNameLen, ptrOffset,
                                shmBlockSize, shmNumBlocks, err);

  ASSERT_EQ(infoGet, true);
  ASSERT_EQ(shmBlockSize, 4096);
  // ASSERT_EQ(shmNumBlocks, 32 * 1024 * 1024 / (4096));

  int sizeIdx = -1;
  err = 0;
  auto curMeta = fslibMem->findDataPtrMeta(dataPtr, sizeIdx, err = 0);
  ASSERT_NE(sizeIdx, -1);
  ASSERT_EQ(err, 0);
  ASSERT_NE(curMeta, nullptr);
  fprintf(stdout, "findDataPtrMeta - sizeIdx:%d shmInnerId:%u\n", sizeIdx,
          curMeta->shmInnerId);

  fprintf(stdout, "=== Free() ===\n");
  err = 0;
  fslibMem->Free(dataPtr, err);
  ASSERT_EQ(err, 0);

  delete fslibMem;
}

} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}