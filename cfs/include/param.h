#ifndef CFS_PARAM_H
#define CFS_PARAM_H

////////////////////////////////////////////////////////////////////////////////
//
// on-disk structure
//

#define CACHE_LINE_BYTE (64)

#define KB (1024)
#define MB (1024 * KB)
#define GB (1024 * MB)
// TODO: use KB/MB/GB for better readability
// TODO: ensure this never causes overflow - add type UL to calculations.

// Optane SSD's sector size (size of min IO unit in bytes)
#define SSD_SEC_SIZE 512
// FSP's block size
// Since this ssd is regarded as a block device, BIO block size is set to 4K
#define BSIZE 4096  // block size
// The influence of changing DEV_SIZE:
// Each block need to be tracked in bitmap, so DEV_SIZE increase <-> more
// Bmapblocks. E.g., 1 bitmap block <=> 128 M
// #define DEV_SIZE 193273528320UL  // disk size in byte: 180 GB - (v0)
// #define DEV_SIZE 536870912000UL  // disk size in byte: 500 GB - (v1)
#ifndef DEV_SIZE
#define DEV_SIZE 858993459200UL  // disk size in byte: 800 GB - (v2)
#endif

// random picked # of inodes for disk layout
// NOTE: NINODES <= NMEMINODE, to make sure all inodes can be cached
#define NINODES 160000  // number of inodes
//#define IMAPBLOCK_NUM (((NINODES) / (BSIZE * 8)) + 1)  // number of imap
// blocks
// We assume 1 imap block is large enough to hold all the inode in-memory
// That is., 32768 inodes
#define IMAPBLOCK_NUM (5)

// make sure imap can contains all inodes
static_assert((NINODES) <= (IMAPBLOCK_NUM) * (BSIZE * 8));

////////////////////////////////////////////////////////////////////////////////
//
// FsProc_Fs.h
//
// directory name length (one layer)
#define DIRSIZE 28
// length of maximum multiple layer directory name, e.g., "/a/b/c/d/e/f"
#define MULTI_DIRSIZE 512
// for extent
#define NEXTENT_ARR 7
#define MAX_SHM_NAME_LEN 64

////////////////////////////////////////////////////////////////////////////////
//
// FsLibShared.h
//
#define RING_SIZE 64
// This is adjusted for opendir(), make it larger enough to read all the
// dentreis at one time
//#define RING_DATA_ITEM_SIZE (524288UL)  // each OP can R/W 512K data
#define RING_DATA_ITEM_SIZE (32768UL)  // each OP can R/W 32K data

////////////////////////////////////////////////////////////////////////////////
//
// FsProc_FileMng.h
//
#define DINODE_SIZE (256)
// number of inode cached in memory (assume all of them)
#define NMEMINODE (NINODES)
// number of inode blocks cached in memory (assume all of them)
#define NMEM_INODE_BLOCK ((NMEMINODE * DINODE_SIZE / BSIZE) + 1)
// number of bmap blocks chaced in memory (Assume all of them)
#define NMEM_BMAP_BLOCK (8192)  // represent up to 1T data

// number of data blocks cached in memory
////#define NMEM_DATA_BLOCK (1024 * 8)  // 32M
////#define NMEM_DATA_BLOCK (1024 * 32) // 128M
////#define NMEM_DATA_BLOCK (1024 * 256) // 1G
////#define NMEM_DATA_BLOCK (8 * 1024 * 256)  // 8G
#define NMEM_DATA_BLOCK ((10 * 1024 * 256UL))  // 10G
////#define NMEM_DATA_BLOCK (20)         // [for test]

// statically partition the bmap and dataBlockBuffers
// thus each of them can be managed by one FSP-thread
// #define NMAX_FSP_WORKER (6) // this is used in falcon
#define NMAX_FSP_WORKER (10)  // this is used in bumble

// max number of client processes supported
#define NMAX_APP_PROC (20)

// maximum number of messages (from other workers) each worker can have in its
// queue
#define DEFAULT_MESSENGER_BUFSIZE (4096)

////////////////////////////////////////////////////////////////////////////////
//
// Journal
//
#define LOCAL_JOURNAL_SIZE (4UL * GB)
#define GLOBAL_JOURNAL_SIZE (LOCAL_JOURNAL_SIZE * NMAX_FSP_WORKER)

#define LOCAL_JOURNAL_NBLOCKS (LOCAL_JOURNAL_SIZE / BSIZE)
#define GLOBAL_JOURNAL_NBLOCKS (GLOBAL_JOURNAL_SIZE / BSIZE)
// The rest are defined in journalparams.h
////////////////////////////////////////////////////////////////////////////////

// shmkey
#define FS_SHM_KEY_BASE 20190301

// max number of width in single directory
#define FS_DIR_MAX_WIDTH 50000

// block device (by default, do not use SPDK)
#define SPDK_THREAD_MAX_INFLIGHT 250

// FsProc.[cc/h]
#define ESTIMATE_BLOCK_WAIT_NUM 4
#define POLL_UNSUBMITTED_WRITE_THRESHOLW 20

////////////////////////////////////////////////////////////////////////////////
// SPDK's huge page
// Now we need to use global shared memory
#define SPDK_HUGEPAGE_GLOBAL_SHMID (9)

////////////////////////////////////////////////////////////////////////////////
//
// For BlockDev's Posix alternative
//
#define BLK_DEV_POSIX_FILE_NAME "/data/blkDevPosix"
#define BLK_DEV_POSIX_FILE_SIZE_BYTES (2147483648L)  // 2G

////////////////////////////////////////////////////////////////////////////////
// FsProc.cc
// choose which split policy to use
#define SPLIT_DEFAULT_POLICY_NUM 0

// open lease
#define LOCAL_OPEN_LIMIT 10

// NOTE: the unix socket is controlled by this macro
// UFS_SOCK_LISTEN
// if enabled
#define CRED_UNIX_SOCKET_PATH "/ufs-sock"

#endif  // CFS_PARAM_H
