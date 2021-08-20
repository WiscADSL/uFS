#ifndef CFS_FSLIBSHARED_H
#define CFS_FSLIBSHARED_H

#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <list>
#include <memory>
#include <vector>

#include "FsLibLeaseShared.h"
#include "FsLibMalloc.h"
#include "param.h"
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-register"
#include "util/util_buf_ring.h"
#pragma clang diagnostic pop
#else
#include "util/util_buf_ring.h"
#endif

//
// This header is Shared by both Proc and Lib part of FS
// Specify how they communicate
//

//
// macros for ret value of FSP ops
// TODO: have an errno field in responses that the client can set. Use only
// existing errno values.
#define FS_REQ_ERROR_NONE 0
#define FS_REQ_ERROR_POSIX_RET (-1)
#define FS_REQ_ERROR_FILE_NOT_FOUND (-2)
// POSIX_EPERM = 1, so use EAGAIN, -11
#define FS_REQ_ERROR_POSIX_EPERM (-EAGAIN)         // -11
#define FS_REQ_ERROR_POSIX_ENOENT (-ENOENT)        // -2
#define FS_REQ_ERROR_POSIX_EACCES (-EACCES)        // -13
#define FS_REQ_ERROR_POSIX_EEXIST (-EEXIST)        // -17
#define FS_REQ_ERROR_POSIX_EISDIR (-EISDIR)        // -21
#define FS_REQ_ERROR_POSIX_ENOTDIR (-ENOTDIR)      // -20
#define FS_REQ_ERROR_POSIX_EINVAL (-EINVAL)        // -22
#define FS_REQ_ERROR_POSIX_ENOTEMPTY (-ENOTEMPTY)  // -39. dir not empty
#define FS_REQ_ERROR_POSIX_EBADF (-EBADF)          // -9
#define FS_REQ_ERROR_POSIX_EAGAIN (-EAGAIN)        // -11
// target worker cannot handle the issued request
#define FS_REQ_ERROR_UNKNOWN_TYPE_FOR_WORKER (-101)
// NOTE: Do not place any more constants below these two. Among all the FS_REQ_*
// constants, the last ones must be FS_REQ_ERROR_INODE_IN_TRANSFER followed by
// FS_REQ_ERROR_INODE_REDIRECT.

// error code for FSP unique usage (inode splitting related)
#define FS_REQ_ERROR_INODE_IN_TRANSFER (-102)
#define FS_REQ_ERROR_INODE_REDIRECT (-103)
//
static_assert(FS_REQ_ERROR_FILE_NOT_FOUND == FS_REQ_ERROR_POSIX_ENOENT,
              "FS_REQ_ERROR_NOT FOUND not match posix error code");
///////

enum CfsOpCode {
  // data plane ops
  CFS_OP_READ = 1,
  CFS_OP_WRITE = 2,
  CFS_OP_PREAD = 3,
  CFS_OP_PWRITE = 4,
  CFS_OP_ALLOCED_PREAD = 5,
  CFS_OP_ALLOCED_READ = 6,
  CFS_OP_ALLOCED_PWRITE = 7,
  CFS_OP_ALLOCED_WRITE = 8,
  CFS_OP_LSEEK = 9,
  // control plane ops
  CFS_OP_OPEN = 11,
  CFS_OP_CLOSE = 12,
  CFS_OP_UNLINK = 13,
  CFS_OP_STAT = 14,
  CFS_OP_FSTAT = 15,
  CFS_OP_MKDIR = 21,
  CFS_OP_RENAME = 22,
  CFS_OP_OPENDIR = 23,
  CFS_OP_RMDIR = 24,
  // NOTE: both fsync and fdatasync will use this op code.
  CFS_OP_FSYNC = 31,
  CFS_OP_SYNCALL = 33,  // we reserve one for fsync
  CFS_OP_SYNCUNLINKED = 34,
  CFS_OP_WSYNC = 35,
  // NOTE: Whenever a client creates new shared memory and needs
  // to notify the server, it uses the following operation.
  CFS_OP_NEW_SHM_ALLOCATED = 60,
  // APP exit FSP access
  CFS_OP_EXIT = 99,
  // TEST ONLY
  CFS_OP_TEST = 100,
  CFS_OP_DUMPINODES = 101,
  CFS_OP_STARTDUMPLOAD = 102,
  CFS_OP_STOPDUMPLOAD = 103,
  // NOTE: allows a client to trigger fs checkpoint (for testing)
  CFS_OP_CHKPT = 120,
  // NOTE: allows a client to instruct server to migrate inode (for testing)
  CFS_OP_MIGRATE = 121,
  // Adding a new op code for ping
  CFS_OP_PING = 122,
  // NOTE: allows a client to trigger inode reassignments (for testing)
  // We still keep CFS_OP_MIGRATE as that works on a fd argument and will
  // remove it once this is stable.
  CFS_OP_INODE_REASSIGNMENT = 123,
  CFS_OP_THREAD_REASSIGNMENT = 124,
};

// if set, FSP is going to use the R/W the data in client's memory
#define _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_ (0b00010000)
// if set, it means this request is asking a LEASE for specific fd
#define _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_ (0b00100000)

// if set, the op is only used to ask for lease renewing (either, READ lease,
// or WRITE lease. It depends on the op is a write or read op)
// if set, the Op will be interpreted as leasOp
// NOTE: we only support read lease for now
#define _RWOP_FLAG_FSLIB_LEASE_RENEW_ (0b01000000)
// if set, it means ask for lease + use unified page cache
#define _RWOP_FLAG_FSLIB_USE_UNIFIED_CACHE_ (0b10000000)
////////////////////////////////////////////////////////
#define _RWOP_FLAG_FSP_DISABLE_CACHE_ (0b00000001)
#define _RWOP_FLAG_FSP_DATA_AT_APP_BUF_ (0b00000010)
struct rwOpCommon {
  int fd;
  // flag describes the mode of this operation
  // 8-bits: _ _ _ _ (high 4 bits are used by FsLib, low 4 FSP) _ _ _ _
  //   - if FsLib in app side intend to enable the client buffer
  //   - if FsLib in app side intend to use the client buffer as cache
  //   - if FsProc would like to invalidate/disable client cache
  //     - e.g., lease expires and held by others
  uint8_t flag;
  size_t count;
  ssize_t ret;
  // NOTE: realOffset is used to save the timestamp (rdtsc() value)
  // This is an temporary hack, yet since offset is going to be maintained by
  // FsLib, it is okay
  off_t realOffset;
  size_t realCount;
};

static_assert(
    sizeof(off_t) == sizeof(uint64_t),
    "rwOpCommon: cannot use realOffset(off_t) to store rdtsc() timestamp");

struct allocatedOpCommon {
  // shmInnerId of the start page for this piece of continuous memory
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;
  // set by FSP to serialize the requests
  uint64_t perAppSeqNo;
};

struct wsyncAlloc {
  // shmInnerId of the start page for this piece of continuous memory
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;
};

struct allocatedReadOp {
  struct rwOpCommon rwOp;
  struct allocatedOpCommon alOp;
};

struct allocatedPreadOp {
  struct rwOpCommon rwOp;
  struct allocatedOpCommon alOp;
  off_t offset;
};

struct allocatedPwriteOp {
  struct rwOpCommon rwOp;
  struct allocatedOpCommon alOp;
  off_t offset;
};

struct allocatedWriteOp {
  struct rwOpCommon rwOp;
  struct allocatedOpCommon alOp;
};

constexpr static int gLeaseOpWriteFlag = -1;
constexpr static int gLeaseOpReadFlag = 0;

template <class T>
inline void setLeaseOpForWrite(T *lsop) {
  lsop->rwOp.realOffset = gLeaseOpWriteFlag;
  lsop->rwOp.flag =
      (_RWOP_FLAG_FSLIB_ENABLE_APP_BUF_ | _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_);
}

template <class T>
inline void setLeaseOpForRead(T *lsop) {
  lsop->rwOp.realOffset = gLeaseOpReadFlag;
  lsop->rwOp.flag =
      (_RWOP_FLAG_FSLIB_ENABLE_APP_BUF_ | _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_);
}

template <class T>
inline void setLeaseOpForReadUC(T *lsop) {
  lsop->rwOp.realOffset = gLeaseOpReadFlag;
  lsop->rwOp.flag =
      (_RWOP_FLAG_FSLIB_ENABLE_APP_BUF_ | _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_ |
       _RWOP_FLAG_FSLIB_USE_UNIFIED_CACHE_);
}

template <class T>
inline void setLeaseOpRenewOnly(T *lsop) {
  lsop->rwOp.flag |= _RWOP_FLAG_FSLIB_LEASE_RENEW_;
}

inline bool isLeaseRwOpRenewOnly(rwOpCommon *rwOp) {
  return rwOp->flag & _RWOP_FLAG_FSLIB_LEASE_RENEW_;
}

template <class T>
inline bool isOpEnableCache(T *lsop) {
  return !(lsop->rwOp.flag ^ (_RWOP_FLAG_FSLIB_ENABLE_APP_BUF_ |
                              _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_));
}

template <class T>
inline bool isOpEnableUnifiedCache(T *lsop) {
  return !(lsop->rwOp.flag ^ (_RWOP_FLAG_FSLIB_ENABLE_APP_BUF_ |
                              _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_ |
                              _RWOP_FLAG_FSLIB_USE_UNIFIED_CACHE_));
}

template <class T>
inline bool isLeaseOpRenewOnly(T *lsop) {
  return lsop->rwOp.flag & _RWOP_FLAG_FSLIB_LEASE_RENEW_;
}

template <class T>
inline bool isLeaseOpForWrite(T *lsop) {
  return lsop->rwOp.realOffset == gLeaseOpWriteFlag;
}

inline void setLeaseTermTsIntoRwOp(struct rwOpCommon *rwop,
                                   FsLeaseCommon::rdtscmp_ts_t ts) {
  rwop->realOffset = ts;
}

inline bool isLeaseHeld(struct rwOpCommon *rwop) {
  return !(rwop->flag & _RWOP_FLAG_FSP_DISABLE_CACHE_);
}

inline FsLeaseCommon::rdtscmp_ts_t getLeaseTermTsFromRwOp(
    struct rwOpCommon *rwop) {
  return rwop->realOffset;
}

struct readOp {
  struct rwOpCommon rwOp;
};

struct preadOp {
  struct rwOpCommon rwOp;
  off_t offset;
};

struct writeOp {
  struct rwOpCommon rwOp;
};

struct pwriteOp {
  struct rwOpCommon rwOp;
  off_t offset;
};

struct lseekOp {
  int fd;
  long int offset;
  int whence;
  int ret;
};

struct openOp {
  int flags;
  int ret;
  mode_t mode;
  // if this open requests a lease
  bool lease;
  // the size of the file (needed by lease)
  uint64_t size;
  char path[MULTI_DIRSIZE];
};

struct closeOp {
  int fd;
  int ret;
};

struct mkdirOp {
  int ret;
  mode_t mode;
  char pathname[MULTI_DIRSIZE];
};

struct statOp {
  int ret;
  struct stat statbuf;
  char path[MULTI_DIRSIZE];
};

struct fstatOp {
  int fd;
  int ret;
  struct stat statbuf;
};

struct fsyncOp {
  int fd;
  int ret;
  // 1: => fdatasync(), 0: => fsync() semantically
  int8_t is_data_sync;
};

struct wsyncOp {
  int fd;
  int ret;
  struct wsyncAlloc alloc;
  size_t array_size;
  size_t file_size;
};

struct unlinkOp {
  int ret;
  char path[MULTI_DIRSIZE];
};

struct renameOp {
  int ret;
  char oldpath[MULTI_DIRSIZE];
  char newpath[MULTI_DIRSIZE];
};

struct opendirOp {
  int numDentry;
  char name[MULTI_DIRSIZE];
  struct allocatedOpCommon alOp;
};

struct rmdirOp {
  int ret;
  char pathname[MULTI_DIRSIZE];
};

struct syncallOp {
  int ret;
};

struct syncunlinkedOp {
  int ret;
};

struct newShmAllocatedOp {
  uint8_t shmid;
  // shmInnerId of the start page for this piece of continuous memory
  fslib_malloc_block_cnt_t shmNumBlocks;
  fslib_malloc_block_sz_t shmBlockSize;
  char shmFname[MULTI_DIRSIZE];
};

struct exitOp {
  int ret;
};

struct chkptOp {
  int ret;
};

struct migrateOp {
  int ret;
  int fd;
  // TODO add more params to control where migration happens
};

struct inodeReassignmentOp {
  int ret;
  int type;
  uint32_t inode;
  int curOwner;
  int newOwner;
};

struct threadReassignOp {
  int ret;
  int tid;
  int src_wid;
  int dst_wid;
  int flag;
};

struct pingOp {
  int ret;
};

struct startDumpLoadOp {
  int ret;
};

struct stopDumpLoadOp {
  int ret;
};

struct dumpinodesOp {
  int ret;
};

#ifdef _CFS_TEST_
struct testOp {
  char path[MULTI_DIRSIZE];
};
#endif

union CfsOp {
  struct readOp read;
  struct preadOp pread;
  struct writeOp write;
  struct pwriteOp pwrite;
  struct allocatedReadOp allocread;
  struct allocatedPreadOp allocpread;
  struct allocatedWriteOp allocwrite;
  struct allocatedPwriteOp allocpwrite;
  struct lseekOp lseek;
  struct openOp open;
  struct closeOp close;
  struct mkdirOp mkdir;
  struct statOp stat;
  struct fstatOp fstat;
  struct fsyncOp fsync;
  struct wsyncOp wsync;
  struct unlinkOp unlink;
  struct renameOp rename;
  struct opendirOp opendir;
  struct rmdirOp rmdir;
  struct syncallOp syncall;
  struct syncunlinkedOp syncunlinked;
  struct newShmAllocatedOp newshmop;
  struct exitOp exit;
  struct chkptOp chkpt;
  struct migrateOp migrate;
  struct pingOp ping;
  struct startDumpLoadOp startdumpload;
  struct stopDumpLoadOp stopdumpload;
  struct dumpinodesOp dumpinodes;
  struct inodeReassignmentOp inodeReassignment;
  struct threadReassignOp threadReassign;
#ifdef _CFS_TEST_
  struct testOp test;
#endif
};

//
// helper functions about extract/embed wid into op
// NOTE: once the wid needs to be changed. Lets have this agreement
//       The return *ret* in op, will be set to error number which indicates
//       that splitting/join is going on.
//       And the the first 8 bytes of this op, will be set to the corresponding
//       valid workerID.
// REQUIRED: at any given time, when *wid* want to make sense inside op, it
// needs
//       to go through these functions
//

// NOTE: Each of these EMBEDED_XXX argument should be of type struct xxOp in
// *union CfsOp*
#define EMBEDED_INO_FILED_OP_OPEN(OPENOP_PTR) ((OPENOP_PTR)->flags)

// For now, we assume both app and FSP knows that the master worker's wid
// NOTE: Please leave this as 0 for now. The wid and worker idx has been used
// interchangeably and this only works because this value is 0. Before changing
// this value, we need to fix all cases of wid and worker idx.
#define APP_CFS_COMMON_KNOWLEDGE_MASTER_WID (0)

// macros for opStatus field
#define OP_NEW 0
#define OP_DONE 1
//

struct clientOp {
  enum CfsOpCode opCode;
  volatile uint8_t opStatus;
  union CfsOp op;
};

struct dataBufItem {
  char buf[RING_DATA_ITEM_SIZE];
};

struct CommuOpDataBuf {
  struct clientOp ops[RING_SIZE];
  struct dataBufItem items[RING_SIZE];
};

class OpRingBuffer {
 public:
  OpRingBuffer(int size, int maxNo);
  ~OpRingBuffer();
  long getRingMemorySize();
  int initRingMemory(pid_t pid, const void *memPtr);
  int enqueue(int idx);
  int dequeue(int &sid);
  // Note: think about Do I need to dequeue bulk? Currently I do not think so
 private:
  int ringSize;
  int srcDstMaxNo;
  struct util_ring *ringPtr;
  long ringMemSize;
  void **src;
};

class MockAppProc;

class CommuChannel {
 public:
  CommuChannel(pid_t id, key_t key)
      : pid(id),
        shmKey(key),
        itemNum(RING_SIZE),
        clientOpVec(itemNum),
        dataBufVec(itemNum),
        ringBufferPtr(nullptr) {
    // to avoid the overhead of queue contention
    // Let's see if this is over-optimization or not ...
    realRingSize = 2 * RING_SIZE;
  }
  virtual ~CommuChannel();
  struct clientOp *slotOpPtr(int sid);
  void *slotDataPtr(int sid);

 protected:
  pid_t pid;
  key_t shmKey;
  int itemNum;
  int shmId;
  std::vector<clientOp *> clientOpVec;
  std::vector<dataBufItem *> dataBufVec;
  int realRingSize;
  OpRingBuffer *ringBufferPtr;
  long ringMemSize;
  struct CommuOpDataBuf *opDataBufPtr;
  uint64_t opDataBufMemSize;
  char *totalSharedMemPtr;
  uint64_t totalSharedMemSize;
  // @return: -1 if error happens
  virtual int initChannelMemLayout();
  // this function will be different for Lib/Proc part, so make it virtual
  virtual void *attachSharedMemory() = 0;
  virtual void cleanupSharedMemory() = 0;

  friend class MockAppProc;
};

struct UfsRegisterOp {
  pid_t emu_pid;
};

struct UfsRegisterAckOp {
  key_t shm_key_base;
  int num_worker_max;
  int worker_key_distance;
};

union ControlMsg {
  struct cmsghdr cmh;
  char control[CMSG_SPACE(sizeof(struct ucred))];
  // Space large enough to hold a ucred structure
};

#endif  // CFS_FSLIBSHARED_H
