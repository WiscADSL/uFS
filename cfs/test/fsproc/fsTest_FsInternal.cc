
#include "FsProc_FsInternal.h"
#include "gtest/gtest.h"

namespace {

TEST(FsBlockAllocUint, T1) {
  fprintf(stdout, "last extent byte:%lu\n", LAST_EXTENT_ALLOC_UNIT_BYTE);
  for (int i = 0; i < NEXTENT_ARR; i++) {
    auto cur_unit = extentArrIdx2BlockAllocUnit(i);
    fprintf(stdout, "i:%d unit:%u\n", i, cur_unit);
    EXPECT_TRUE(cur_unit > 0);
  }
}

TEST(FsBlockAllocUint, T2) {
  EXPECT_EQ(extentArrIdx2BlockAllocUnit(NEXTENT_ARR), (uint)0);
  EXPECT_EQ(extentArrIdx2BlockAllocUnit(-1), (uint)0);
}

TEST(FsExtentMapDiskBlock, T1) {
  uint32_t nBmapblocks = get_dev_bmap_num_blocks_for_worker(0);
  fprintf(stdout, "numBmapBlocks per worker:%u\n", nBmapblocks);
  uint32_t nMaxBmapBlockNum = 0;
  uint32_t blockStartNo;
  for (int i = 0; i < NEXTENT_ARR; i++) {
    blockStartNo = getDataBMapStartBlockNoForExtentArrIdx(i, nBmapblocks,
                                                          nMaxBmapBlockNum);
    EXPECT_FALSE(nMaxBmapBlockNum == 0);
    fprintf(stdout,
            "extent_idx:%d nMaxBmapBlockNum:%u blockStartNo:%u "
            "maxNumFiles:%d\n",
            i, nMaxBmapBlockNum, blockStartNo,
            ((nMaxBmapBlockNum * (BPB)) / extentArrIdx2BlockAllocUnit(i)));
  }
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}