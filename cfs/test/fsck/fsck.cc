#include <fcntl.h>
#include <malloc.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define CATCH_CONFIG_RUNNER
#include "FsProc_FsInternal.h"
#include "catch2/catch.hpp"
#include "cxxopts.hpp"
#include "typedefs.h"

// Simple FSCK for FSP's device status
// Usage:
// @bumble (otherwise, might adjust the grep matching accordingly.
// ./fsck -b /dev/`lsblk | grep "8[0-9][0-9].[0-9]G" | awk '{print $1}'`

constexpr static int gPageBytes = 4096;

static char gBdevName[256];
static int gDevFd = -1;
static struct cfs_superblock gSb;
static std::unordered_set<cfs_ino_t> gAllocatedInums;
static std::unordered_set<cfs_bno_t> gAllocatedDataBlocks;

// no bmap bit leak
void checkBmap() { REQUIRE(!gAllocatedDataBlocks.empty()); }

// no imap bit leak
void checkImap() { REQUIRE(!gAllocatedInums.empty()); }

// go through each inode
// if itself is marked as allocated in imap
// - save inode numbers
// if it's extents are marked as allocated in bmap
// - save data block numbers
void checkAllInodes() {}

void checkSuperBlock(char *buf) {
  auto sb = reinterpret_cast<struct cfs_superblock *>(buf);
  memcpy(&gSb, sb, sizeof(gSb));
  printSuperBlockToStream(stdout, &gSb);
  REQUIRE(gSb.imap_start == get_imap_start_block());

  // TODO: check missing fields?

  REQUIRE(gSb.bmap_start == get_bmap_start_block_for_worker(0));
  REQUIRE(gSb.inode_start == get_inode_start_block());
  REQUIRE(gSb.datablock_start == get_data_new_alloc_start_block_for_worker(0));
  REQUIRE(gSb.datablock_start == get_data_start_block());
}

TEST_CASE("fsck", "basic") {
  gDevFd = open(gBdevName, O_RDONLY | O_DIRECT);
  if (gDevFd == -1) {
    std::cerr << "cannot open device. Error:" << errno << std::endl;
    exit(1);
  }

  char *buf = static_cast<char *>(memalign(gPageBytes, gPageBytes));
  if (buf == NULL) {
    std::cerr << "cannot allocate memory. Error:" << errno << std::endl;
  }

  ssize_t nr = pread(gDevFd, buf, gPageBytes, (SB_BLOCK_NO)*gPageBytes);
  assert(nr == gPageBytes);
  checkSuperBlock(buf);
  free(buf);

  checkAllInodes();

  checkImap();
  checkBmap();

  close(gDevFd);
}

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "fsck for FSP disk layout");
  options.add_options()
      // Absolute path of block device. E.g., /dev/nvme0n1
      ("b,bdev", "path of the block device", cxxopts::value<std::string>())
      // Help
      ("h,help", "Print Usage");

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  if (result.count("bdev")) {
    auto bdev = result["bdev"].as<std::string>();
    strcpy(gBdevName, bdev.c_str());
    fprintf(stdout, "device name:%s\n", gBdevName);
  } else {
    std::cout << options.help() << std::endl;
    exit(1);
  }

  int fsckResult = Catch::Session().run(argc, argv);
  return fsckResult;
}