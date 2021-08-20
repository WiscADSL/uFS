#include "FsProc_FsInternal.h"
#include "gtest/gtest.h"

namespace {

TEST(DiskDataFormat, T1) {
  EXPECT_EQ(get_imap_start_block(), 2);
  fprintf(stdout, "imap_start_block:%lu\n", get_imap_start_block());
  fprintf(stdout, "- imap_num_blocks:%lu\n", get_dev_imap_num_blocks());
  fprintf(stdout, "bmap_start_block:%lu\n", get_bmap_start_block_for_worker(0));
  fprintf(stdout, "- bmap_num_blocks:%lu\n",
          get_dev_bmap_num_blocks_in_total());
  uint64_t bmap_num_blocks_accu = 0;
  for (int i = 0; i < NMAX_FSP_WORKER; i++) {
    fprintf(stdout, "\t- bmap_num_blocks_for_worker(%d):%lu\n", i,
            get_dev_bmap_num_blocks_for_worker(i));
    fprintf(stdout, "\t  - bmap_start_block_for_worker(%d):%lu\n", i,
            get_bmap_start_block_for_worker(i));
    bmap_num_blocks_accu += get_dev_bmap_num_blocks_for_worker(i);
  }
  fprintf(stdout, "- bmap_num_blocks_by_accumulating:%lu\n",
          bmap_num_blocks_accu);
  fprintf(stdout, "inode_start_block:%lu\n", get_inode_start_block());
  fprintf(stdout, "- inode_num_blocks:%lu\n", get_dev_inode_num_blocks());
  fprintf(stdout, "data_start_block:%lu\n",
          get_data_new_alloc_start_block_for_worker(0));
  double k1G = 1024 * 1024 * 1024;
  fprintf(stdout, "- data_num_blocks:%lu (%f GB)\n",
          get_dev_bmap_num_blocks_in_total() * (BPB),
          float(4096 * get_dev_bmap_num_blocks_in_total() * (BPB)) / k1G);
  for (int i = 0; i < NMAX_FSP_WORKER; i++) {
    fprintf(stdout, "\t- data_num_blocks_for_worker(%d):%lu (%f GB)\n", i,
            get_dev_data_num_blocks_for_worker(i),
            float(get_dev_data_num_blocks_for_worker(i)) * 4096 / (k1G));
    fprintf(stdout, "\t  - data_start_block_for_worker(%d):%lu\n", i,
            get_data_new_alloc_start_block_for_worker(i));
  }
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}