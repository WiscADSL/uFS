
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "BlkDevSpdk.h"
#include "FsProc_Fs.h"
#include "FsProc_FsInternal.h"
#include "FsProc_Journal.h"
#include "fsapi.h"
#include "param.h"
#include "util.h"

#if CFS_JOURNAL(ON)
// NOTE: Must be imported after FsProc_Journal.h
#include "FsProc_JSuper.h"
#endif

/*-----------------------------------------------------------------------*/
// io functions
void wsect(uint64_t, uint8_t *buf);
// will erase 32 sectors (aka. BLOCKS) at a time
// REQUIRED: sec_start % 32 == 0
void erase32sect(uint64_t sec_start);
void rsect(uint64_t sec, uint8_t *buf);
void winode(uint32_t, struct cfs_dinode *ip);
void rinode(uint32_t inum, struct cfs_dinode *ip);
uint32_t ialloc(uint16_t type);
void iappend(uint32_t inum, void *p, uint64_t n);
// Add data blocks to certain inode
// Note: the result status could be size=0, but num_block > 0, caller need to
// fix this.
void iadd_datablock(uint32_t inum, uint32_t block_num);
void readsb();
void writesb();
void fillBlock(char *buf, off_t off, size_t fillSize);

/**
 * Format the file system
 * @param create_tests
 *          whether to create test files
 * @param zero_out
 *          whether to zero out blocks
 * @param zero_data_block_num
 *          the number of blocks to erase if erasure is desired
 */
void mkfs_spdk(bool create_tests, bool zero_out, uint64_t zero_data_block_num);
void write_file(int fno, int64_t sizeBytes);
void read_file(int fno);
void write_inode_tables();
void zero_out_meta();
/*-----------------------------------------------------------------------*/

#ifdef USE_SPDK
BlkDevSpdk *devPtr;
#else
BlkDevPosix *devPtr;
#endif

// global fs worker
FsProc *gFsProcPtr;
char *spdkWriteBuf;
char *spdkReadBuf;
std::atomic_bool *workerRunPtr;

uint8_t zeroes[BSIZE];
struct cfs_superblock sb;

static std::string log_info = "[spdkfs-info]";

uint64_t gDefaultZeroDataBlockNum = 50000;

void cleanEnv() {
  assert(devPtr != nullptr);
  devPtr->freeBuf(spdkWriteBuf);
  devPtr->freeBuf(spdkReadBuf);
  delete (devPtr);
  delete gFsProcPtr;
  devPtr = nullptr;
  gFsProcPtr = nullptr;
}

void initEnv() {
  // init spdk environment
#ifdef USE_SPDK
  devPtr = new BlkDevSpdk("", DEV_SIZE / BSIZE, BSIZE);
#else
  devPtr = new BlkDevPosix(BLK_DEV_POSIX_FILE_NAME, DEV_SIZE / BSIZE, BSIZE);
#endif
  gFsProcPtr = new FsProc(/*doInline*/ true);
  workerRunPtr = new std::atomic_bool(false);
  gFsProcPtr->startInlineWorker(devPtr, false);

  spdkWriteBuf = (char *)devPtr->zmallocBuf(32 * BSIZE, BSIZE);
  spdkReadBuf = (char *)devPtr->zmallocBuf(32 * BSIZE, BSIZE);
}

void usage(int argc, char **argv) {
  std::cout << "Usage:" << std::endl;
  std::cout << argv[0] << " mkfs [-z zero-block-num] [--no-make-test]"
            << std::endl;
  std::cout << "    -z zero-block-num" << std::endl;
  std::cout << "             If specified, zero-block-num blocks will be "
               "zeroed out on the device."
            << std::endl;
  std::cout << "             Passing 0 will zero out the whole device"
            << std::endl;
  std::cout << "    --no-make-test" << std::endl;
  std::cout << "             If specified, default test file creation is "
               "skipped after formatting."
            << std::endl;
  std::cout << argv[0] << " rw w <FileNo> <sizeByte>" << std::endl;
  std::cout << argv[0] << " rw r <FileNo>" << std::endl;
  exit(1);
}

inline std::string fnoToFname(int fno) {
  std::ostringstream oss;
  oss << "t" << fno;
  return oss.str();
}

inline void outputCaseLine() {
  std::cout << "--------------------------------------------------"
            << std::endl;
}

void testBitmap() {
  initEnv();
  char buf[BSIZE];
  bzero(buf, BSIZE);
  std::cout << "-- " << buf[0] << std::endl;
  block_set_bit(0, (void *)buf);
  std::cout << "-- " << buf[0] << std::endl;
  block_set_bit(1, (void *)buf);
  std::cout << "-- " << buf[0] << std::endl;
  block_set_bit(2, (void *)buf);
  std::cout << "-- " << buf[0] << std::endl;
  block_set_bit(3, (void *)buf);
  std::cout << "-- " << buf[0] << std::endl;
  int freeBitNo = find_block_free_bit_no((void *)buf, BPB);
  std::cout << "free:" << freeBitNo << std::endl;
  // rsect(get_data_new_alloc_start_block_for_worker(), (uint8_t*)buf);
  // int freeBitNo = find_block_free_bit_no((void*)buf, BPB);
  // std::cout << freeBitNo << std::endl;
  exit(0);
}

int main(int argc, char **argv) {
  // testBitmap();
  if (argc < 2) {
    usage(argc, argv);
  }
  std::string task = std::string(argv[1]);

  if (task == "mkfs") {
    bool create_tests = true;
    bool zero_out = false;
    uint64_t zero_data_blocks = 0;
    for (int i = 2; i < argc; i++) {
      if (std::string(argv[i]) == "--no-make-test") {
        create_tests = false;
      } else if (std::string(argv[i]) == "-z" && i + 1 < argc) {
        zero_out = true;
        zero_data_blocks = std::stoull(argv[i + 1]);
        fprintf(stdout,
                "===>>> will mkfs for the device. %lu data blocks will be "
                "initialized to 0\n",
                zero_data_blocks);
        i++;
      }
    }
    initEnv();
    mkfs_spdk(create_tests, zero_out, zero_data_blocks);
  } else if (task == "rw") {
    if (argc < 3) {
      usage(argc, argv);
    }
    if (std::string(argv[2]) == "r" && argc == 4) {
      initEnv();
      read_file(atoi(argv[3]));
    } else if (std::string(argv[2]) == "w" && argc == 5) {
      initEnv();
      write_file(atoi(argv[3]), atoll(argv[4]));
    } else {
      usage(argc, argv);
    }
  } else {
    usage(argc, argv);
  }

  cleanEnv();
  return 0;
}

int32_t fromFnameToInum(std::string &fname) {
  struct cfs_dinode din;
  char buf[BSIZE];
  // read root inode
  rinode(ROOTINO, &din);
  int32_t fileIno = -1;
  struct cfs_extent *cur_extent_ptr;
  for (int i = 0; i < NEXTENT_ARR; i++) {
    cur_extent_ptr = &din.ext_array[i];
    if (cur_extent_ptr->block_no == 0) {
      break;
    }
    for (uint j = 0; j < cur_extent_ptr->num_blocks; j++) {
      uint32_t data_block_no = cur_extent_ptr->block_no + j;
      rsect(data_block_no + get_data_start_block(), (uint8_t *)buf);
      struct cfs_dirent *dirEntryPtr = (struct cfs_dirent *)buf;
      for (uint ii = 0; ii < BSIZE / (sizeof(cfs_dirent)); ii++) {
        if (std::string((dirEntryPtr + ii)->name) == fname) {
          fileIno = (dirEntryPtr + ii)->inum;
          break;
        }
      }
      if (fileIno > 0) break;
    }
  }
  return fileIno;
}

void write_file(int fno, int64_t sizeBytes) {
  outputCaseLine();
  readsb();
  std::string fname = fnoToFname(fno);
  std::cout << "fileName: " << fname << std::endl;
  char buf[BSIZE];
  int32_t fileIno = fromFnameToInum(fname);
  assert(fileIno > 0);
  std::cout << "fileInode: " << fileIno << std::endl;
  fillBlock(buf, 0, BSIZE);
  off_t off = 0;
  for (int i = 0; i < sizeBytes / BSIZE; i++) {
    iappend(fileIno, buf, BSIZE);
    off += BSIZE;
  }
  if (sizeBytes > off) {
    iappend(fileIno, buf, sizeBytes - off);
  }
  writesb();
  outputCaseLine();
}

void read_file(int fno) {
  outputCaseLine();
  readsb();
  struct cfs_dinode din;
  std::string fname = fnoToFname(fno);
  std::cout << "fileName: " << fname << std::endl;
  char *buf;
  int32_t fileIno = fromFnameToInum(fname);
  std::cout << "fileInode: " << fileIno << std::endl;
  rinode(fileIno, &din);
  std::cout << "file size: " << din.size << std::endl;
  std::cout << format_inode_output(&din) << std::endl;
  if (din.size > 0) {
    buf = (char *)malloc(din.size + 1);
    memset(buf, 0, din.size + 1);
    uint64_t off = 0, tot;
    uint32_t m;
    struct cfs_extent *extent_ptr;
    char *dstPtr = buf;
    uint32_t cur_inside_extent_idx, cur_extent_max_block_num;
    for (tot = 0; tot < din.size; tot += m, off += m, dstPtr += m) {
      uint32_t blockIdx = off / BSIZE;
      m = std::min(din.size - tot, BSIZE - off % BSIZE);
      int cur_extent_arr_idx = getCurrentExtentArrIdx(
          blockIdx, cur_inside_extent_idx, cur_extent_max_block_num);
      assert(cur_extent_arr_idx >= 0);
      extent_ptr = &din.ext_array[cur_extent_arr_idx];
      rsect(
          extent_ptr->block_no + cur_inside_extent_idx + get_data_start_block(),
          (uint8_t *)dstPtr);
      // fprintf(stdout, "din.size:%lu off:%lu %c\n", din.size, off,
      // (char)(*dstPtr));
    }
    ////std::cout << buf << std::endl;
    free(buf);
  }
  outputCaseLine();
}

static inline uint64_t getTimeSinceEpochMicro() {
  using namespace std::chrono;
  return duration_cast<microseconds>(system_clock::now().time_since_epoch())
      .count();
}

void zsectors(uint64_t sec_start, uint32_t nsectors) {
  const int kEraseBatchSize = 32;
  uint64_t block_start = sec_start / 8;
  uint32_t nblocks = 0;
  int i = 0;
  while (nblocks < (nsectors / 8 + 1)) {
    erase32sect(block_start);
    block_start += kEraseBatchSize;
    nblocks += kEraseBatchSize;
    i++;
  }
  // int rc = devPtr->blockingZeroSectors(sec_start, nsectors);
  // if (rc != 0) {
  //   std::cerr << "Failed to erase sectors from " << sec_start
  //             << ", got rc=" << rc << std::endl;
  //   throw std::runtime_error("Failed to erase sectors");
  // }
}

void zero_out_meta() {
  std::cerr << "Zeroing out metadata blocks" << std::endl;
  // erase the dummy block 0 and superblock (SB_BLOCK_NO = 1)
  zsectors(0, 16);

  // erase the bitmaps
  auto imap_start = get_imap_start_block();
  auto imap_nblocks = get_dev_imap_num_blocks();
  zsectors(imap_start * 8, imap_nblocks * 8);

  auto bmap_start = get_bmap_start_block_for_worker(0);
  auto bmap_nblocks = get_dev_bmap_num_blocks_in_total();
  zsectors(bmap_start * 8, bmap_nblocks * 8);

  // erase the inode table
  auto inode_start = get_inode_start_block();
  auto inode_nblocks = get_dev_inode_num_blocks();
  zsectors(inode_start * 8, inode_nblocks * 8);

  // zero out a littlebit of every journal block. No need to zero the entire
  // journal
  uint32_t jsb_nblocks = 40000;
  for (int i = 0; i < NMAX_FSP_WORKER; i++) {
    auto jsb_start = get_worker_journal_sb(i);
    zsectors(jsb_start * 8, jsb_nblocks * 8);
  }
}

void write_inode_tables() {
  std::cerr << "Writing inode tables" << std::endl;

  uint32_t inode_num = 0;
  auto inode_start_block = get_inode_start_block();
  auto inode_end_block = inode_start_block + get_dev_inode_num_blocks();
  char *devmem = spdkWriteBuf;
  memset(devmem, 0, BSIZE);
  struct cfs_dinode *devmem_dinode = (struct cfs_dinode *)devmem;
  for (auto block = inode_start_block; block < inode_end_block; block++) {
    // NOTE this works only if one inode of size struct cfs_dinode is immedately
    // followed by the next. If sizeof(struct cfs_dinode) != actual space on
    // disk, use an absolute value here.
    for (uint32_t i = 0; i < IPB; i++) {
      devmem_dinode[i].i_no = inode_num++;
      devmem_dinode[i].syncID = 0;
    }

    // TODO: make this faster by not making it blocking?
    int rc = devPtr->blockingWrite(block, devmem);
    if (rc < 0) {
      std::cerr << "Error writing out inode table at block " << block
                << std::endl;
      throw std::runtime_error("Failed to write inode tables");
    }
  }

  std::cerr << "Wrote " << inode_num << " inode structs" << std::endl;
}

void mkfs_spdk(bool create_tests, bool zero_out, uint64_t zero_data_block_num) {
  uint32_t rootino, off;  // inum;
  std::cout << "sizeof superblock:" << sizeof(struct cfs_superblock)
            << std::endl;
  std::cout << "sizeof cfs_dinode:" << sizeof(struct cfs_dinode) << std::endl;
  std::cout << "sizeof cfs_dirent:" << sizeof(struct cfs_dirent) << std::endl;
  static_assert(sizeof(struct cfs_superblock) <= BSIZE, "");
  static_assert((BSIZE) % sizeof(struct cfs_dinode) == 0, "");
  static_assert((BSIZE) % sizeof(struct cfs_dirent) == 0, "");

  uint64_t fs_size_byte = DEV_SIZE;
  uint64_t fs_size_nblocks = fs_size_byte / BSIZE;
  // uint32_t inode_nblocks = NINODES / IPB + 1;
  uint32_t inode_nblocks = get_dev_inode_num_blocks();
  // uint32_t imap_nblocks = NINODES / (BSIZE * 8) + 1;
  uint32_t imap_nblocks = get_dev_imap_num_blocks();
  // uint32_t bmap_nblocks = fs_size_nblocks / (BSIZE * 8) + 1;
  uint32_t bmap_nblocks = get_dev_bmap_num_blocks_in_total();
  uint32_t journal_nblocks = LOCAL_JOURNAL_NBLOCKS * NMAX_FSP_WORKER;
  // boot, sb, inode bitmap, bitmap, inode table, journal
  uint32_t meta_nblocks =
      1 + 1 + imap_nblocks + bmap_nblocks + inode_nblocks + journal_nblocks;
  uint64_t data_nblocks = fs_size_nblocks - meta_nblocks;

  // fill in superblock
  sb.size = fs_size_byte;
  sb.ndatablocks = data_nblocks;
  sb.ninodes = NINODES;
  sb.njournalblocks = journal_nblocks;

  // indicates that "/" rootdir will use datablock 1, and its inode number is
  // ROOTINO NOTE: must start from 1, otherwise, cannot judge addr[0] == 0?
  sb.next_freeinode_no = ROOTINO;
  // sb.imap_start = 2;
  sb.imap_start = get_imap_start_block();
  sb.journalblock_start = get_worker_journal_sb(0);
  // TODO (Jing): I've corrected this logic - please check.
  sb.bmap_start = get_bmap_start_block_for_worker(0);
  sb.inode_start = get_inode_start_block();
  sb.datablock_start = get_data_start_block();

  std::cout << "-------------------------" << std::endl;
  std::cout << "Creating File System ... " << std::endl;
  fprintf(stdout,
          "meta_nblocks:%u (boot=1, super=1, ibitmap blocks = %u bitmap "
          "blocks=%u inode blocks=%u, journal_nblocks = %u, "
          "data_nblocks:%lu\n",
          meta_nblocks, imap_nblocks, bmap_nblocks, inode_nblocks,
          journal_nblocks, data_nblocks);

  fprintf(stdout,
          "superblock {\n size=%lu,\n "
          "ndatablocks=%lu,\n "
          "njournalblocks=%u,\n "
          "ninodes=%u,\n "
          "imap_start=%lu,\n "
          "journalblock_start=%lu,\n "
          "bmap_start=%lu,\n "
          "inode_start=%lu,\n "
          "datablock_start=%lu\n "
          "}\n",
          sb.size, sb.ndatablocks, sb.njournalblocks, sb.ninodes, sb.imap_start,
          sb.journalblock_start, sb.bmap_start, sb.inode_start,
          sb.datablock_start);

  zero_out_meta();

  // Unless overridden, we will always zero out the first 25% of data blocks
  // NOTE: the zero_out flag is only used to calculate the number of blocks to
  // zero out. We will always zero out some data blocks regardless.
  uint64_t num_zero_nblocks = gDefaultZeroDataBlockNum;
  if (zero_out) {
    // reset data blocks
    if (zero_data_block_num > data_nblocks) {
      fprintf(stderr, "===>>> zero_data_block_num overflow, ignoring value\n");
    } else if (zero_data_block_num == 0) {
      fprintf(stderr,
              "===>>> zero_data_block_num is 0, all data blocks will be zeroed "
              "out\n");
      num_zero_nblocks = data_nblocks;
    } else {
      num_zero_nblocks = zero_data_block_num;
    }
  }

  fprintf(stderr, "===>>> zeroing out %lu data blocks\n", num_zero_nblocks);
  auto data_start = get_data_start_block();
  while (num_zero_nblocks > 0) {
    uint32_t nblocks = 5000;
    if (nblocks > num_zero_nblocks) {
      nblocks = num_zero_nblocks;
    }

    zsectors(data_start * 8, nblocks * 8);
    data_start += nblocks;
    num_zero_nblocks -= nblocks;
  }

  writesb();
  std::cout << "write superblock sb_block_no: " << SB_BLOCK_NO << std::endl;
  // write super block
  uint8_t sb_block[BSIZE];
  memcpy(sb_block, &sb, sizeof(sb));
  wsect(SB_BLOCK_NO, sb_block);

#if CFS_JOURNAL(ON)
  // NOTE: these blocks are wasted. we never use them even if the journal is
  // off.

  // write journal superblocks
  JSuperOnDisk js;
  uint8_t js_block[BSIZE];
  uint64_t cur_ts = (uint64_t)tap_ustime();
  for (int i = 0; i < NMAX_FSP_WORKER; i++) {
    js.jmagic = JSUPER_MAGIC;
    js.jsuper_blockno = get_worker_journal_sb(i);
    js.jstart_blockno = js.jsuper_blockno + 1;
    js.jend_blockno = get_worker_journal_sb(i + 1) - 1;
    js.capacity = js.jend_blockno - js.jstart_blockno + 1;
    js.last_chkpt_ts = cur_ts;
    if (js.capacity != (LOCAL_JOURNAL_NBLOCKS - 1)) {
      std::cout << "ERROR: journal block calculation is incorrect" << std::endl;
      std::cout << "start is " << js.jstart_blockno << "; end is "
                << js.jend_blockno << "; capacity is " << js.capacity
                << "; constant is " << LOCAL_JOURNAL_NBLOCKS << std::endl;
    }

    // indices relative to jstart_blockno
    js.head = 0;
    js.tail = 0;
    js.n_used = 0;
    memcpy(js_block, &js, sizeof(js));
#if CFS_JOURNAL(GLOBAL_JOURNAL)
    // zero out all journal superblocks in global journal mode
    memset(js_block, 0, sizeof(js));
#endif
    wsect(get_worker_journal_sb(i), js_block);
  }

  // make one big journal in global journal mode
#if CFS_JOURNAL(GLOBAL_JOURNAL)
  js.jmagic = JSUPER_MAGIC;
  js.jsuper_blockno = get_worker_journal_sb(0);
  js.jstart_blockno = js.jsuper_blockno + 1;
  js.jend_blockno = get_worker_journal_sb(NMAX_FSP_WORKER) - 1;
  js.capacity = js.jend_blockno - js.jstart_blockno + 1;
  if (js.capacity != (GLOBAL_JOURNAL_NBLOCKS - 1)) {
    std::cout << "ERROR: journal block calculation is incorrect" << std::endl;
    std::cout << "start is " << js.jstart_blockno << "; end is "
              << js.jend_blockno << "; capacity is " << js.capacity
              << "; constant is " << GLOBAL_JOURNAL_NBLOCKS << std::endl;
  }
  js.last_chkpt_ts = cur_ts;
  js.head = 0;
  js.tail = 0;
  js.n_used = 0;
  memcpy(js_block, &js, sizeof(js));
  wsect(js.jsuper_blockno, js_block);
#endif

#endif  // CFS_JOURNAL(ON)

  write_inode_tables();

  // create "/" directory
  struct cfs_dirent de;
  struct cfs_dinode din;
  rootino = ialloc(T_DIR);
  assert(rootino == ROOTINO);

  // init bmap (mark the first block as allocated)
  char dbmap[BSIZE];
  memset(dbmap, 0, BSIZE);
  block_set_bit(0, dbmap);
  wsect(get_bmap_start_block_for_worker(0), (uint8_t *)dbmap);

  // update inode bitmap
  char ibmap[BSIZE];
  memset(ibmap, 0, BSIZE);
  for (int i = 0; i <= ROOTINO; i++) {
    block_set_bit(i, ibmap);
  }
  wsect(get_imap_start_block(), (uint8_t *)ibmap);
  std::cout << "clear data blocks done !" << std::endl;

  // "."
  bzero(&de, sizeof(de));
  de.inum = rootino;
  strcpy(de.name, ".");
  iappend(rootino, &de, sizeof(de));
  // ".."
  bzero(&de, sizeof(de));
  de.inum = rootino;
  strcpy(de.name, "..");
  iappend(rootino, &de, sizeof(de));
  std::cout << "add . and .. for root directory Done" << std::endl;

  if (create_tests) {
    // add test file's
    int numTestFiles = 10;
    // int numTestFiles = 110;
    std::cout << "Create test files:" << std::endl;
    uint inum;
    for (int fno = 0; fno < numTestFiles; fno++) {
      inum = ialloc(T_FILE);
      bzero(&de, sizeof(de));
      de.inum = inum;
      strcpy(de.name, fnoToFname(fno).c_str());
      iappend(rootino, &de, sizeof(de));
    }
    std::cout << "create test files done !" << std::endl;
  }

  // add one test directory
  if (create_tests) {
    bzero(&de, sizeof(de));
    de.inum = ialloc(T_DIR);
    strcpy(de.name, "db");
    iappend(rootino, &de, sizeof(de));
    // add . and .. to test directory
    struct cfs_dirent subde;
    bzero(&subde, sizeof(subde));
    subde.inum = de.inum;
    strcpy(subde.name, ".");
    iappend(de.inum, &subde, sizeof(subde));
    bzero(&subde, sizeof(subde));
    subde.inum = rootino;
    strcpy(subde.name, "..");
    iappend(de.inum, &subde, sizeof(subde));

    struct cfs_dinode test_dir_inode;
    rinode(de.inum, &test_dir_inode);
    test_dir_inode.i_dentry_count = 2;
    winode(de.inum, &test_dir_inode);
  }

  // fix size of root inode dir
  rinode(rootino, &din);
  off = din.size;
  std::cout << log_info << "root inode init size:" << off << std::endl;
  din.i_block_count = (off - 1) / BSIZE + 1;
  din.i_dentry_count = (off - 1) / (sizeof(struct cfs_dirent)) + 1;
  std::cout << log_info
            << "root inode's dentry count is set to: " << din.i_dentry_count
            << std::endl;
  winode(rootino, &din);

  // write back the data bitmap
  // update sb
  writesb();
}

void wsect(uint64_t sec, uint8_t *buf) {
  memset(spdkWriteBuf, 0, BSIZE);
  memcpy(spdkWriteBuf, buf, BSIZE);
  devPtr->blockingWrite(sec, spdkWriteBuf);
}

void erase32sect(uint64_t sec_start) {
  int num_blocks = 32;
  // assert(sec_start % (num_blocks) == 0);
  memset(spdkWriteBuf, 0, BSIZE * (num_blocks));
  devPtr->blockingWriteMultiBlocks(sec_start, num_blocks, spdkWriteBuf);
}

/*
 * inode number to block number
 */
uint i2b(uint32_t inum) { return (inum / IPB) + get_inode_start_block(); }

void winode(uint32_t inum, struct cfs_dinode *ip) {
  uint8_t buf[BSIZE];
  uint32_t bn;
  struct cfs_dinode *dip;

  bn = i2b(inum);
  rsect(bn, buf);
  dip = ((struct cfs_dinode *)buf) + (inum % IPB);
  *dip = *ip;
  // std::cout << "winode: inum:" << inum << " 4k-bno:" << bn
  //          << " new writing size: " << dip->size << std::endl;
  wsect(bn, buf);
}

void rinode(uint32_t inum, struct cfs_dinode *ip) {
  uint8_t buf[BSIZE];
  uint32_t bn;
  struct cfs_dinode *dip;

  bn = i2b(inum);
  rsect(bn, buf);
  dip = ((struct cfs_dinode *)buf) + (inum % IPB);
  *ip = *dip;
  // std::cout << "rinode: inum:" << inum << " size:" << ip->size << "
  // i_block_count:" << ip->i_block_count << std::endl;
}

void rsect(uint64_t sec, uint8_t *buf) {
  memset(spdkReadBuf, 0, BSIZE);
  devPtr->blockingRead(sec, spdkReadBuf);
  memcpy(buf, spdkReadBuf, BSIZE);
}

/**
 * allocate a new file of certain type
 * @param type
 * @return allocated inode number
 */
uint32_t ialloc(uint16_t type) {
  uint32_t inum = sb.next_freeinode_no;
  sb.next_freeinode_no++;
  std::cout << "ialloc next_freeinode_no:" << sb.next_freeinode_no << std::endl;
  struct cfs_dinode din;

  // update imap
  char imap[BSIZE];
  rsect(get_imap_start_block(), (uint8_t *)imap);
  block_set_bit(inum, imap);
  wsect(get_imap_start_block(), (uint8_t *)imap);

  // write inode to inode table
  bzero(&din, sizeof(din));
  din.type = type;
  din.nlink = 1;
  din.size = 0;
  din.i_no = inum;

  din.i_gid = 0;
  din.i_uid = 0;
  din.i_block_count = 0;

  din.i_dentry_count = 0;
  gettimeofday(&din.ctime, NULL);
  gettimeofday(&din.mtime, NULL);
  // gettimeofday(&din.atime, NULL);
  winode(inum, &din);
  writesb();
  return inum;
}

void tmp_debug_hock() { return; }

/**
 * append new data to inode
 * @param inum: target inode
 * @param xp: data pointer
 * @param n: size (in bytes) of appended data
 */
void iappend(uint32_t inum, void *xp, uint64_t n) {
  char *p = (char *)xp;
  uint32_t tot, m;
  uint64_t off;
  struct cfs_dinode din;
  struct cfs_extent *extent_ptr;
  uint8_t buf[BSIZE];
  char *src_ptr = p;

  assert(sb.inode_start != 0);

  rinode(inum, &din);
  off = din.size;

  uint64_t real_nbytes = n;
  uint64_t new_off = off + n;
  if (off == 0) {
    iadd_datablock(inum, 1);
  }
  rinode(inum, &din);
  if ((new_off - 1) / BSIZE + 1 > din.i_block_count) {
    // need to allocate more data block to this inode
    iadd_datablock(inum, (new_off - 1) / BSIZE + 1 - din.i_block_count);
  }
  // If add data block, inode will be updated.
  rinode(inum, &din);

  uint32_t cur_inside_extent_idx, cur_extent_max_block_num;
  for (tot = 0; tot < n; tot += m, off += m, src_ptr += m) {
    uint32_t cur_block_idx = off / BSIZE;
    m = std::min(real_nbytes - tot, BSIZE - off % BSIZE);
    int cur_extent_arr_idx = getCurrentExtentArrIdx(
        cur_block_idx, cur_inside_extent_idx, cur_extent_max_block_num);
    assert(cur_extent_arr_idx >= 0);
    extent_ptr = &din.ext_array[cur_extent_arr_idx];
    rsect(extent_ptr->block_no + cur_inside_extent_idx + get_data_start_block(),
          buf);
    // cast cur_block_idx to 64 bit, otherwise *BSIZE will overflow
    bcopy(src_ptr, buf + (off - ((uint64_t)cur_block_idx) * BSIZE), m);
    wsect(extent_ptr->block_no + cur_inside_extent_idx + get_data_start_block(),
          buf);
  }

  // update inode.
  din.size = off;
  winode(inum, &din);
}

void iadd_datablock(uint32_t inum, uint32_t block_num) {
  struct cfs_dinode din;
  struct cfs_extent *extent_ptr;
  uint8_t buf[BSIZE];

  assert(sb.inode_start != 0);
  uint32_t num_bmap_blocks = sb.inode_start - sb.bmap_start;
  // fprintf(stdout, "number of bitmap blocks is:%u\n", num_bmap_blocks);
  rinode(inum, &din);
  uint32_t cur_iblock_count = din.i_block_count;
  fprintf(
      stdout,
      "iadd_datablock: inum:%u addition_block_num:%u, cur_iblock_count:%u\n",
      inum, block_num, cur_iblock_count);
  uint32_t inside_extent_idx, extent_max_block_num;
  uint32_t n = 0;
  while (n < block_num) {
    int cur_extent_arr_idx = getCurrentExtentArrIdx(
        cur_iblock_count, inside_extent_idx, extent_max_block_num);
    assert(cur_extent_arr_idx >= 0);
    extent_ptr = &din.ext_array[cur_extent_arr_idx];
    uint32_t cur_max_bmap_blocks;
    uint32_t cur_bmap_block_disk_block_no =
        get_bmap_start_block_for_worker(0) +
        getDataBMapStartBlockNoForExtentArrIdx(
            cur_extent_arr_idx, num_bmap_blocks, cur_max_bmap_blocks);
    if (extent_ptr->num_blocks == 0) {
      // need to update bitmap block
      uint32_t cur_allocation_unit =
          extentArrIdx2BlockAllocUnit(cur_extent_arr_idx);
      // try to find an empty unit for this extent_idx
      if (cur_allocation_unit == 1) {
        for (uint32_t i = 0; i < cur_max_bmap_blocks; i++) {
          rsect(cur_bmap_block_disk_block_no + i, buf);
          int64_t free_bit_no = find_block_free_bit_no((void *)buf, (BPB));
          if (free_bit_no >= 0) {
            block_set_bit(free_bit_no, (void *)buf);
            wsect(cur_bmap_block_disk_block_no + i, buf);
            extent_ptr->block_no = (cur_bmap_block_disk_block_no -
                                    get_bmap_start_block_for_worker(0) + i) *
                                       (BPB) +
                                   free_bit_no;
            extent_ptr->num_blocks++;
            extent_ptr->i_block_offset = cur_iblock_count;
            goto BMAP_ALLOCATE_SUCCESS;
          }
        }
        fprintf(stderr, "ERROR cannot find block in extant_idx:%d\n",
                cur_extent_arr_idx);
        throw;
      } else if (cur_allocation_unit < BPB) {
        for (uint32_t i = 0; i < cur_max_bmap_blocks; i++) {
          rsect(cur_bmap_block_disk_block_no + i, buf);
          uint64_t free_bit_no = find_block_free_multi_bits_no(
              (void *)buf, (BPB), cur_allocation_unit);
          if (free_bit_no >= 0) {
            for (uint32_t j = 0; j < cur_allocation_unit; j++) {
              block_set_bit(free_bit_no + j, (void *)buf);
            }
            wsect(cur_bmap_block_disk_block_no + i, buf);
            extent_ptr->block_no = (cur_bmap_block_disk_block_no -
                                    get_bmap_start_block_for_worker(0) + i) *
                                       (BPB) +
                                   free_bit_no;
            extent_ptr->num_blocks++;
            extent_ptr->i_block_offset = cur_iblock_count;
            goto BMAP_ALLOCATE_SUCCESS;
          }
        }
        fprintf(stderr, "ERROR cannot find block in extant_idx:%d\n",
                cur_extent_arr_idx);
        throw;
      } else {
        uint32_t cur_allocation_bmap_block_num = cur_allocation_unit / (BPB);
        for (uint32_t i = 0; i < cur_max_bmap_blocks;
             i += cur_allocation_bmap_block_num) {
          rsect(cur_bmap_block_disk_block_no + i, buf);
          int *buf_int_ptr = (int *)buf;
          if (*buf_int_ptr == 0) {
            extent_ptr->block_no = (cur_bmap_block_disk_block_no -
                                    get_bmap_start_block_for_worker(0) + i) *
                                   (BPB);
            extent_ptr->num_blocks++;
            extent_ptr->i_block_offset = cur_iblock_count;
            memset(buf, 1, BSIZE);
            for (uint j = 0; j < cur_allocation_bmap_block_num; j++) {
              wsect(cur_bmap_block_disk_block_no + i + j, buf);
            }
            goto BMAP_ALLOCATE_SUCCESS;
          }
        }
        fprintf(stderr, "ERROR cannot find block in extant_idx:%d\n",
                cur_extent_arr_idx);
        throw;
      }
    BMAP_ALLOCATE_SUCCESS:
      n++;
      cur_iblock_count++;
    } else {
      // do not need to modify bitmap blocks, only need to change the extent's
      // metadata
      uint32_t cur_extent_allocation_num = std::min(
          block_num - n, extent_max_block_num - extent_ptr->num_blocks);
      extent_ptr->num_blocks += cur_extent_allocation_num;
      n += cur_extent_allocation_num;
      cur_iblock_count += cur_extent_allocation_num;
    }
  }
  din.i_block_count = cur_iblock_count;
  winode(inum, &din);
}

void dumpsb() {
  // std::cout << "sb.next_freeinode_no: " << sb.next_freeinode_no << std::endl;
}

void readsb() {
  uint8_t buf[BSIZE];
  rsect(SB_BLOCK_NO, buf);
  memcpy(&sb, buf, sizeof(struct cfs_superblock));
  std::cout << "readsb done:" << std::endl;
  dumpsb();
}

void writesb() {
  uint8_t buf[BSIZE];
  memset(buf, 0, BSIZE);
  memcpy(buf, &sb, sizeof(struct cfs_superblock));
  wsect(SB_BLOCK_NO, buf);
  std::cout << "writesb done" << std::endl;
  dumpsb();
}

void leftrotate(std::string &s, int d) {
  reverse(s.begin(), s.begin() + d);
  reverse(s.begin() + d, s.end());
  reverse(s.begin(), s.end());
}

void fillBlock(char *buf, off_t off, size_t fillSize) {
  std::string tmp = "abcdefghijklmnopqrstuvwxyz";
  tmp += "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  tmp += "0123456789";
  tmp += ":)";
  assert(tmp.size() == 64);
  // 64 bytes tmp string
  char tmpBuf[BSIZE];
  for (int i = 0; i < BSIZE / 64; i++) {
    memcpy(tmpBuf + i * (64), tmp.c_str(), 64);
    leftrotate(tmp, 1);
  }
  assert(off + fillSize <= BSIZE);
  memcpy(buf + off, tmpBuf + off, fillSize);
}
