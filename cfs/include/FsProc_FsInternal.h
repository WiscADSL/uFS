#pragma once
#ifndef CFS_FSINTERNAL_H
#define CFS_FSINTERNAL_H

#include <sys/types.h>

#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <sstream>

#ifndef NO_INCLUDE_JSON
#include "nlohmann/json.hpp"
#endif
#include "param.h"

#define ROOTINO 1      //  root inode number, "/"
#define SB_BLOCK_NO 1  //  super block block number (on disk)

// on disk superblock
struct cfs_superblock {
  uint64_t size;         // size of file system image (in blocks)
  uint64_t ndatablocks;  // number of data blocks
  uint32_t ninodes;      // number of inodes
  uint32_t njournalblocks;

  uint32_t next_freeinode_no;

  uint64_t imap_start;          // block number of first inode bitmap block
  uint64_t bmap_start;          // block number of first bitmap block
  uint64_t inode_start;         // block number of first inode block
  uint64_t datablock_start;     // block number of first data block
  uint64_t journalblock_start;  // block number of first journal block
};

void inline printSuperBlockToStream(FILE *stream, struct cfs_superblock *sb) {
  fprintf(stream, "======= super-block =======\n");
  fprintf(stream, " size:%lu ndatablocks:%lu\n", sb->size, sb->ndatablocks);
  fprintf(stream, " ninodes:%u next_freeinode_no:%u\n", sb->ninodes,
          sb->next_freeinode_no);
  fprintf(stream,
          " ibmap_start:%lu\n bmap_start:%lu\n inode_start:%lu\n "
          "datablock_start:%lu:\n",
          sb->imap_start, sb->bmap_start, sb->inode_start, sb->datablock_start);
  fprintf(stream, "===========================\n");
  fflush(stream);
}

// file types
#define T_DIR 1
#define T_FILE 2

#define ISEC_SIZE (SSD_SEC_SIZE)  // 512B
#define IPSEC (ISEC_SIZE / sizeof(struct cfs_dinode))
// inode per BLOCK, here, per BLOCK use the unit of BSIZE (not sector size)
// That is, no matter how large each buffer slot of inodeBuffer, IPB means
// Number of Inode per (BSIZE) block
#define IPB (BSIZE / sizeof(struct cfs_dinode))

// bitmap bits per block
#define BPB (BSIZE * 8)

// dirent is 32 Byte
// FIXME: CFS dirent contains no info on file type due to alignment requirement.
//        Client need to do a stat() to know if this inode is a directory or a
//        file.
struct cfs_dirent {
  uint32_t inum;
  char name[DIRSIZE];
};

// an extent descriptor takes 16 bytes
struct cfs_extent {
  uint32_t i_block_offset;  // block no offset in this file
  // Note, this block_no's block zero is the first data-block
  // That is, it is not an absolute block number, but an indirect block number
  // according to the first data block (get_data_start_block()).
  uint32_t num_blocks;  // # of blocks in this extent
  uint64_t block_no;    // on disk block no
};

// on disk inode (512 B)
// NOTE: choose to use 512B to make sure one on-disk IO-unit (512 B) can
// just have one inode
// In future, if we need larger file, we can release more *cfs_extent* from
// __padding_for_sector
struct cfs_dinode {
  //
  // 1-Byte fields
  //
  // # of links to inodes in file system
  uint8_t nlink;

  //
  // 2-Byte fields
  //
  // inode's dentry count if this inode is actually a directory
  // Basically, for a directory, the size will keep increasing, never shrink
  // The *size* value will represent a directory's real size, while this
  // i_dentry_count will give the concept size of this inode.
  // That is, if 5 files in this directory excluded *.* and *..*,
  // The i_dentry_count will be 7. but the size could be larger if several
  // unlink() have been issued.
  uint16_t i_dentry_count;

  //
  // 4-Byte fields
  //
  mode_t type;  // file type
  uint32_t i_no;
  uid_t i_uid;
  gid_t i_gid;
  uint32_t i_block_count;  // inode block count

  //
  // 8-Byte fields
  //
  uint64_t size;    // size of file in bytes
  uint64_t syncID;  // non-decreasing integer for every metadata change

  struct timeval atime;  // each timeval is 16 Bytes
  struct timeval ctime;
  struct timeval mtime;

  // Ext-0, Ext-1, Ext-2 ... Ext6 will allocate certain size of data
  // 4K, 1M, 128M (one whole bitmap), 2*128M, 4*128M, 8*128M, 16*128M
  struct cfs_extent ext_array[NEXTENT_ARR];

  uint8_t __padding_for_sector[256];
  // Pad unused space to 512B
  uint8_t __padding[56];
};

// printing formats
std::string inline format_inode_output(struct cfs_dinode *inodePtr) {
  std::ostringstream ss;
  ss << "Inode:" << inodePtr->i_no << " type:" << inodePtr->type
     << "SyncID: " << inodePtr->syncID << " size:" << inodePtr->size
     << " extents:\n";
  for (int i = 0; i < NEXTENT_ARR; i++) {
    ss << "\t ext_array[" << i << "]:"
       << " i_block_offset:" << inodePtr->ext_array[i].i_block_offset
       << " block_no:" << inodePtr->ext_array[i].block_no
       << " num_blocks:" << inodePtr->ext_array[i].num_blocks
       << (i < NEXTENT_ARR - 1 ? "\n" : "");
  }
  return ss.str();
}

#ifndef NO_INCLUDE_JSON
#define DUMP_PACK_DINODE_ATTR_TO_JSON(j, inode, attr) (j[#attr] = inode->attr)
std::string inline from_cfsdinode_to_str(const cfs_dinode *dinode) {
  std::stringstream ss;
  nlohmann::json jsub;
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, nlink);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_dentry_count);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, type);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_no);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_uid);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_gid);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_block_count);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, size);
  DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, syncID);
  for (int i = 0; i < NEXTENT_ARR; i++) {
    std::string attrName = "extent-" + std::to_string(i);
    jsub[attrName] = {
        {"i_block_offset", dinode->ext_array[i].i_block_offset},
        {"num_blocks", dinode->ext_array[i].num_blocks},
        {"block_no", dinode->ext_array[i].block_no},
    };
  }
  ss << jsub;
  return ss.str();
}
#endif

#ifdef _EXTENT_FOR_LDB_
#define LAST_EXTENT_ALLOC_UNIT_BYTE (32768UL)
#else
// Last extent allocate 4G data at a time.
#define LAST_EXTENT_ALLOC_UNIT_BYTE (4294967296UL)
#endif
// Infer the block allocation unit size from the index of ext_array
// return the number of blocks (in case block size is changed)
// @param: extentArrIdx is the index of ext_array in cfs_dinode.
uint32_t inline extentArrIdx2BlockAllocUnit(int extentArrIdx) {
  if (extentArrIdx >= (NEXTENT_ARR) || extentArrIdx < 0) return 0;
  if (extentArrIdx == 0) {
    // 1 block
    return 1;
  } else if (extentArrIdx == 1) {
    // 1M data (if BSIZE=4K)
    // return 256;
    return 32;
  } else if (extentArrIdx == NEXTENT_ARR - 1) {
    // last extent
    return (LAST_EXTENT_ALLOC_UNIT_BYTE) / (BSIZE);
  } else {
    return (BPB)*pow(2, (extentArrIdx - 2));
  }
}

// For each work, it can get only one file in the largest extent
// #define MAX_LARGE_FILE_NUM (NMAX_FSP_WORKER)
#define MAX_LARGE_FILE_NUM (1)
#define LAST_EXTENT_MAX_BMAP_BLOCK_NUM \
  ((LAST_EXTENT_ALLOC_UNIT_BYTE / (BPB * BSIZE)) * MAX_LARGE_FILE_NUM)

// To allocate a block for certain extent_array index (certain size), first find
// the start block number for this size within all the bitmap blocks. E.g., 800
// bitmap blocks. [0 - 99]: allocation unit is 1 block --> extentArrIdx = 0 [100
// - 199]: allocation unit is 256 block --> extentArrIdx = 1 [200 - 299]:
// allocation unit is 32768 block --> extentArrIdx = 2 etc... Then for
// extentArrIdx = 1, return 100
// @param: maxBmapBlockNum indicates that the number of bitmap blocks which can
// be used for this extent index
//          e.g, for the above example, return 100 when extentArrIdx = 1;
// For now, each refers to extent describes size of:
//    - 4K, 1M, 128M, 256M, 512M, 1G, 2G
#if defined(_EXTENT_FOR_LDB_)
// const int kFsPerExtShareArr[] = {1, 80, 10, 0, 0, 0};  // levelDB
const int kFsPerExtShareArr[] = {1, 639, 0, 0, 0, 0};  // levelDB
#elif defined(_EXTENT_FOR_FILEBENCH_)
const int kFsPerExtShareArr[] = {20, 300, 1, 1, 1, 1};  // filebench
#else
const int kFsPerExtShareArr[] = {1, 1, 4, 8, 16, 20};  // micro-bench@bumble
#endif

const int kFsExtArrShareSum = kFsPerExtShareArr[0] + kFsPerExtShareArr[1] +
                              kFsPerExtShareArr[2] + kFsPerExtShareArr[3] +
                              kFsPerExtShareArr[4] + kFsPerExtShareArr[5];
uint32_t inline getDataBMapStartBlockNoForExtentArrIdx(
    int extentArrIdx, uint32_t totalBmapBlockNum, uint32_t &maxBmapBlockNum) {
  uint32_t bmap_start_block_no;
  if (extentArrIdx + 1 == (NEXTENT_ARR)) {
    maxBmapBlockNum = (LAST_EXTENT_MAX_BMAP_BLOCK_NUM);
  } else {
    maxBmapBlockNum = (totalBmapBlockNum - LAST_EXTENT_MAX_BMAP_BLOCK_NUM) /
                      kFsExtArrShareSum * kFsPerExtShareArr[extentArrIdx];
  }
  // assert(LAST_EXTENT_MAX_BMAP_BLOCK_NUM >= maxBmapBlockNum);
  int totalPreShare = 0;
  for (int i = 0; i < extentArrIdx; i++) {
    totalPreShare += kFsPerExtShareArr[i];
  }
  bmap_start_block_no =
      totalPreShare * ((totalBmapBlockNum - LAST_EXTENT_MAX_BMAP_BLOCK_NUM) /
                       kFsExtArrShareSum);

  // if (((maxBmapBlockNum * (BPB)) / extentArrIdx2BlockAllocUnit(extentArrIdx))
  // <
  //     MAX_LARGE_FILE_NUM) {
  //   // Make sure that to satisfy we can have MAX_LARGE_FILE_NUM (has largest)
  //   fprintf(stderr, "ERROR: extentIdx:%d maxBmapBlockNum:%u\n", extentArrIdx,
  //           maxBmapBlockNum);
  //   throw;
  // }
  return bmap_start_block_no;
}

// Find the extent's index of the array which is in charge of
// certain block of a file.
// @param: block # of an inode.
// @param: inside_extent_idx
// @param: max_accumulate_block_num
int inline getCurrentExtentArrIdx(uint32_t i_block_idx,
                                  uint32_t &inside_extent_idx,
                                  uint32_t &extent_max_block_num) {
  uint32_t blockNum = 0;
  inside_extent_idx = 0;
  extent_max_block_num = 0;
  for (int i = 0; i < NEXTENT_ARR; i++) {
    extent_max_block_num = extentArrIdx2BlockAllocUnit(i);
    blockNum += extent_max_block_num;
    if (i_block_idx < blockNum) {
      inside_extent_idx = i_block_idx - (blockNum - extent_max_block_num);
      return i;
    }
  }
  return -1;
}

//
// Helper functions to compute block offsets
//

// get the on-disk logic block number for the first inode bitmap block
static inline uint64_t get_imap_start_block() { return 1 + 1; }

// get the on-disk number of blocks to be used for inode bitmap
static inline uint64_t get_dev_imap_num_blocks() { return NINODES / (BPB) + 1; }

static inline uint64_t get_dev_bmap_num_blocks_in_total() {
  return (((DEV_SIZE) / (BSIZE)) / (BPB)) + 1;
}

static inline uint64_t get_dev_bmap_num_blocks_for_worker(int widIdx) {
  // NOTE: now, we just simply equally partition bmap blocks
  return (get_dev_bmap_num_blocks_in_total() / (NMAX_FSP_WORKER));
}

static inline uint64_t get_dev_inode_num_blocks() { return NINODES / IPB + 1; }

static inline uint64_t get_dev_data_num_blocks_for_worker(int widIdx) {
  return get_dev_bmap_num_blocks_for_worker(widIdx) * BPB;
}

static inline uint64_t get_worker_journal_sb(int widIdx) {
  uint64_t journal_start_idx =
      get_imap_start_block() + get_dev_imap_num_blocks();
  return journal_start_idx + (widIdx * LOCAL_JOURNAL_NBLOCKS);
}
// get the on-disk logic block number for the first data bitmap block
// Never all this from elsewhere
static inline uint64_t __get_bmap_start_block() {
  // immediately after last journal
  return get_worker_journal_sb(NMAX_FSP_WORKER);
}

// unit of data blocks that are partitioned to different worker thread
// set to 16 M blocks --> 64 G data per partition
static inline uint64_t get_bmap_start_block_for_worker(int widIdx) {
  uint64_t bmap_num_blocks = 0;
  for (int i = 0; i < widIdx; i++) {
    bmap_num_blocks += get_dev_bmap_num_blocks_for_worker(widIdx);
  }
  return __get_bmap_start_block() + bmap_num_blocks;
}

// get the on-disk logic block number for the first inode block
static inline uint64_t get_inode_start_block() {
  // (BSIZE * 8) -- one byte has 8 bits
  return get_bmap_start_block_for_worker(0) +
         get_dev_bmap_num_blocks_in_total();
}

// get the on-disk logic block number for the overall very first data block
static inline uint64_t get_data_start_block() {
  return get_inode_start_block() + get_dev_inode_num_blocks();
}

// get the on-disk logic block number for the first data block
// @param widIdx: this worker's index compared to *kMasterWidConst*
//   that is., 0 means, master itself.
// NOTE: this is risky, because mostly the real logic block # can be derived
// directly from the data bmap when allocating, thus this is mostly used for
// printing out the format and make sense of that
static inline uint64_t get_data_new_alloc_start_block_for_worker(int widIdx) {
  uint64_t num_blocks = 0;
  for (int i = 0; i < widIdx; i++)
    num_blocks += get_dev_data_num_blocks_for_worker(widIdx);
  return get_inode_start_block() + get_dev_inode_num_blocks() + num_blocks;
}

static inline int getWidForBitmapBlock(uint64_t bitmap_block_no) {
  int wid = (bitmap_block_no - __get_bmap_start_block()) /
            get_dev_bmap_num_blocks_for_worker(0);
  return wid;
}

static inline bool isBitmapBlock(uint64_t block_no) {
  return (block_no >= get_bmap_start_block_for_worker(0)) &&
         (block_no < get_inode_start_block());
}

static inline bool isInodeBitmap(uint64_t block_no) {
  auto start = get_imap_start_block();
  auto end = start + get_dev_imap_num_blocks();
  return (block_no >= start) && (block_no <= end);
}

static inline uint32_t calcSectorForInode(uint32_t ino) {
  uint32_t secNo = ino + (BSIZE / (ISEC_SIZE)) * get_inode_start_block();
  // fprintf(stderr, "ino2SectorNo ino:%u return:%u\n", ino, secNo);
  return secNo;
}

static inline uint64_t conv_lba_to_pba(uint64_t lba) {
  return lba + get_data_start_block();
}

static inline uint64_t conv_pba_to_lba(uint64_t pba) {
  return pba - get_data_start_block();
}

// calculates the bitmap block for a given logical block address
static inline uint32_t get_bmap_block_for_lba(uint32_t lba) {
  return (lba / (BPB)) + get_bmap_start_block_for_worker(0);
}

// calculates the bitmap block for a given logical block address
static inline uint32_t get_bmap_block_for_pba(uint32_t pba) {
  return get_bmap_block_for_lba(conv_pba_to_lba(pba));
}

static inline uint32_t get_imap_for_inode(uint64_t inode) {
  return (inode / (BPB)) + get_imap_start_block();
}
#endif  // CFS_FSINTERNAL_H
