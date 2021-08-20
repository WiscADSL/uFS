#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <atomic>
#include <iostream>

#ifdef _CFS_LIB_PRINT_REQ_
#include <sys/syscall.h>
#include <sys/types.h>
#endif

#ifdef FS_LIB_SPPG
#include <memory>
#include <mutex>

#include "spdk/env.h"
#endif

#include <sys/stat.h>
#include <unistd.h>
#include <util.h>

#include <cassert>
#include <cstring>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "FsLibApp.h"
#include "FsMsg.h"
#include "FsProc_FsInternal.h"
#include "fsapi.h"
#include "param.h"
#include "shmipc/shmipc.h"
#include "stats/stats.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/reader_writer_lock.h"
#include "tbb/scalable_allocator.h"

// if enabled, will print each api invocation
// #define _CFS_LIB_PRINT_REQ_
// #define LDB_PRINT_CALL

void print_open(const char *path, int flags, mode_t mode, bool ldb = false) {
  fprintf(stderr, "open%s(%s, %d, %d) from tid %d\n", ldb ? "_ldb" : "", path,
          flags, mode, threadFsTid);
}

void print_close(int fd, bool ldb = false) {
  fprintf(stderr, "close%s(%d) from tid %d\n", ldb ? "_ldb" : "", fd,
          threadFsTid);
}

void print_pread(int fd, void *buf, size_t count, off_t offset,
                 bool ldb = false) {
  fprintf(stderr, "pread%s(%d, %p, %lu, %ld) from tid %d\n", ldb ? "_ldb" : "",
          fd, buf, count, offset, threadFsTid);
}

void print_read(int fd, void *buf, size_t count, bool ldb = false) {
  fprintf(stderr, "read%s(%d, %p, %lu) from tid %d\n", ldb ? "_ldb" : "", fd,
          buf, count, threadFsTid);
}

void print_write(int fd, const void *buf, size_t count, bool ldb = false) {
  fprintf(stderr, "write%s(%d, %p, %lu) from tid %d\n", ldb ? "_ldb" : "", fd,
          buf, count, threadFsTid);
}

void print_fsync(int fd, bool ldb = false) {
  fprintf(stderr, "fsync%s(%d) from tid %d\n", ldb ? "_ldb" : "", fd,
          threadFsTid);
}

void print_unlink(const char *path) {
  fprintf(stderr, "unlink(%s) from tid %d\n", path, threadFsTid);
}

void print_rename(const char *oldpath, const char *newpath) {
  fprintf(stderr, "rename(%s, %s) from tid %d\n", oldpath, newpath,
          threadFsTid);
}

//
// helper functions to dump rbtree to stdout
//
constexpr static int kDumpRbtreeIndent = 4;
void dumpRbtree(rbtree_node n, int indent) {
  int i;
  if (n == nullptr) {
    fprintf(stdout, "<empty tree>");
    return;
  }
  if (n->right != nullptr) {
    dumpRbtree(n->right, indent + kDumpRbtreeIndent);
  }
  for (i = 0; i < indent; i++) {
    fprintf(stdout, " ");
  }
  if (n->color == BLACK) {
    fprintf(stdout, "%lu\n", (unsigned long)n->key);
  } else {
    fprintf(stdout, "<%lu>\n", (unsigned long)n->key);
  }
  if (n->left != NULL) {
    dumpRbtree(n->left, indent + kDumpRbtreeIndent);
  }
}

static inline off_t shmipc_mgr_alloc_slot_dbg(struct shmipc_mgr *mgr);

// REQUIRED: this must be called after caller calls fs_malloc()
// within it, \_setup_app_thread_mem_buf() will init threadFsTid
template <typename T>
static void EmbedThreadIdToAsOpRet(T &ret_field) {
#if 0
  if (threadFsTid <= 0 || threadFsTid >= 1000) {
    fprintf(stderr, "tid:%d\n", threadFsTid);
  }
#endif
  assert((threadFsTid != 0 && threadFsTid < 1000));
  ret_field = static_cast<T>(1);
}

static void dumpAllocatedOpCommon(allocatedOpCommonPacked *alOp) {
  if (alOp == nullptr) {
    return;
  }
  fprintf(stdout, "allocatedOp - shmId:%u dataPtrId:%u seqNo:%lu\n",
          alOp->shmid, alOp->dataPtrId, alOp->perAppSeqNo);
}

FsService::FsService(int wid, key_t key)
    : unusedSlotsLock(ATOMIC_FLAG_INIT),
      shmKey(key),
      wid(wid),
      channelPtr(nullptr) {
  for (auto i = 0; i < RING_SIZE; i++) {
    unusedRingSlots.push_back(i);
  }
  initService();
}

// Assume 0 means no shmKey available
FsService::FsService() : FsService(0, 0) {}

FsService::~FsService() { shmipc_mgr_destroy(shmipc_mgr); }

void FsService::initService() {
  // TODO: if shmkey is known, init memory. Otherwise, init with FsProc
  if (shmKey == 0) {
    std::cerr << "ERROR initService (shmKey==0) does not supported yet"
              << std::endl;
    exit(1);
  }

  char shmfile[128];
  snprintf(shmfile, 128, "/shmipc_mgr_%03d", (int)shmKey);

  // We create the memory on server side, so for client this is 0
  shmipc_mgr = shmipc_mgr_init(shmfile, RING_SIZE, 0);
  if (shmipc_mgr == NULL) {
    std::cerr << "ERROR initService failed to initialize shared memory."
              << " errno : " << strerror(errno) << std::endl;
    throw std::runtime_error("initservice failed to initialize shared memory");
  }

  std::cout << "INFO initService connected to contrl shm at " << shmfile
            << std::endl;
}

int FsService::allocRingSlot() {
  // try grab the lock
  while (unusedSlotsLock.test_and_set(std::memory_order_acquire))
    // spinlock
    ;
  if (unusedRingSlots.empty()) {
    unusedSlotsLock.clear(std::memory_order_release);
    return -1;
  }
  int slotId = unusedRingSlots.front();
  unusedRingSlots.pop_front();
  // release the lock
  unusedSlotsLock.clear(std::memory_order_release);
  return slotId;
}

void FsService::releaseRingSlot(int slotId) {
  assert(slotId >= 0 && slotId < RING_SIZE);
  while (unusedSlotsLock.test_and_set(std::memory_order_acquire))
    ;
  unusedRingSlots.push_back(slotId);
  unusedSlotsLock.clear(std::memory_order_release);
}

void *FsService::getSlotDataBufPtr(int slotId, bool reset) {
  if (!(slotId >= 0 && slotId < RING_SIZE)) {
    fprintf(stdout, "getSlotDataBufPtr for slotID:%d is not valid\n", slotId);
    throw std::runtime_error("getSlotDataBufPtr is not valid");
  }
  auto ptr = channelPtr->slotDataPtr(slotId);
  if (reset) memset(ptr, 0, RING_DATA_ITEM_SIZE);
  return ptr;
}

struct clientOp *FsService::getSlotOpPtr(int slotId) {
  return channelPtr->slotOpPtr(slotId);
}

// Assume the slot HAS BEEN filled with valid CfsOp before calling this function
// try submit until success
// @param slotId allocated by ->allocRingSlot
// @return
int FsService::submitReq(int slotId) {
  int ret;
  while ((ret = channelPtr->submitSlot(slotId)) != 1) {
    if (ret < 0) {
      std::cerr << "ERROR FsService::submitReq" << std::endl;
      break;
    }
  }
  return ret;
}

CommuChannelAppSide::CommuChannelAppSide(pid_t id, key_t shmKey)
    : CommuChannel(id, shmKey) {
  int rt = this->initChannelMemLayout();
  if (rt < 0) throw "FsLibInit Error: Cannot init chammel memory";
}

CommuChannelAppSide::~CommuChannelAppSide() {
  cleanupSharedMemory();
  delete ringBufferPtr;
}

void *CommuChannelAppSide::attachSharedMemory() {
  assert(ringBufferPtr != nullptr);
  shmId = shmget(shmKey, totalSharedMemSize, S_IRUSR | S_IWUSR);
  if (shmId < 0) {
    return nullptr;
  }
  totalSharedMemPtr = (char *)shmat(shmId, NULL, 0);
  return (void *)totalSharedMemPtr;
}

void CommuChannelAppSide::cleanupSharedMemory() {
  int rc = shmdt(totalSharedMemPtr);
  if (rc == -1) {
    std::cerr << "ERROR cannot detach memory" << std::endl;
    exit(1);
  }
}

int CommuChannelAppSide::submitSlot(int sid) {
  int ret = ringBufferPtr->enqueue(sid);
  return ret;
}

static inline void adjustPath(const char *path, char *opPath) {
  std::string s(path);
  strncpy(opPath, s.c_str(), MULTI_DIRSIZE);
}

static inline void fillRwOpCommon(struct rwOpCommon *opc, int fd,
                                  size_t count) {
  memset(opc, 0, sizeof(struct rwOpCommon));
  opc->fd = fd;
  opc->count = count;
  opc->flag = 0;
  if (FS_LIB_USE_APP_BUF) opc->flag |= _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_;
}

/* The following prepare_* functions are used to prepare a struct to be sent
 * over shmipc */
static inline void prepare_rwOpCommon(struct rwOpCommonPacked *op, int fd,
                                      size_t count) {
  op->fd = fd;
  op->count = count;
  op->flag = 0;
  EmbedThreadIdToAsOpRet(op->ret);
  if (FS_LIB_USE_APP_BUF) op->flag |= _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_;
}

struct allocatedReadOp *fillAllocedReadOp(struct clientOp *curCop, int fd,
                                          size_t count) {
  curCop->opCode = CFS_OP_ALLOCED_READ;
  curCop->opStatus = OP_NEW;
  struct allocatedReadOp *arop = &((curCop->op).allocread);
  fillRwOpCommon(&arop->rwOp, fd, count);
  return arop;
}

static inline void prepare_allocatedReadOp(struct shmipc_msg *msg,
                                           struct allocatedReadOpPacked *op,
                                           int fd, size_t count) {
  msg->type = CFS_OP_ALLOCED_READ;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct allocatedPreadOp *fillAllocedPreadOp(struct clientOp *curCop, int fd,
                                            size_t count, off_t offset) {
  curCop->opCode = CFS_OP_ALLOCED_PREAD;
  curCop->opStatus = OP_NEW;
  struct allocatedPreadOp *aprop = &((curCop->op).allocpread);
  fillRwOpCommon(&aprop->rwOp, fd, count);
  aprop->offset = offset;
  return aprop;
}

static inline void prepare_allocatedPreadOp(struct shmipc_msg *msg,
                                            struct allocatedPreadOpPacked *op,
                                            int fd, size_t count,
                                            off_t offset) {
  msg->type = CFS_OP_ALLOCED_PREAD;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct allocatedWriteOp *fillAllocedWriteOp(struct clientOp *curCop, int fd,
                                            size_t count) {
  curCop->opCode = CFS_OP_ALLOCED_WRITE;
  curCop->opStatus = OP_NEW;
  struct allocatedWriteOp *awop = &((curCop->op).allocwrite);
  fillRwOpCommon(&awop->rwOp, fd, count);
  return awop;
}

static inline void prepare_allocatedWriteOp(struct shmipc_msg *msg,
                                            struct allocatedWriteOpPacked *op,
                                            int fd, size_t count) {
  msg->type = CFS_OP_ALLOCED_WRITE;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct allocatedPwriteOp *fillAllocedPwriteOp(struct clientOp *curCop, int fd,
                                              size_t count, off_t offset) {
  curCop->opCode = CFS_OP_ALLOCED_PWRITE;
  curCop->opStatus = OP_NEW;
  struct allocatedPwriteOp *apwrop = &((curCop->op).allocpwrite);
  apwrop->offset = offset;
  fillRwOpCommon(&apwrop->rwOp, fd, count);
  return apwrop;
}

static inline void prepare_allocatedPwriteOp(struct shmipc_msg *msg,
                                             struct allocatedPwriteOpPacked *op,
                                             int fd, size_t count,
                                             off_t offset) {
  msg->type = CFS_OP_ALLOCED_PWRITE;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct readOp *fillReadOp(struct clientOp *curCop, int fd, size_t count) {
  curCop->opCode = CFS_OP_READ;
  curCop->opStatus = OP_NEW;
  struct readOp *rop = &((curCop->op).read);
  fillRwOpCommon(&rop->rwOp, fd, count);
  return rop;
}

static inline void prepare_readOp(struct shmipc_msg *msg,
                                  struct readOpPacked *op, int fd,
                                  size_t count) {
  msg->type = CFS_OP_READ;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct preadOp *fillPreadOp(struct clientOp *curCop, int fd, size_t count,
                            off_t offset) {
  curCop->opCode = CFS_OP_PREAD;
  curCop->opStatus = OP_NEW;
  struct preadOp *prop = &((curCop->op).pread);
  prop->offset = offset;
  fillRwOpCommon(&prop->rwOp, fd, count);
  return prop;
}

static inline void prepare_preadOp(struct shmipc_msg *msg,
                                   struct preadOpPacked *op, int fd,
                                   size_t count, off_t offset) {
  msg->type = CFS_OP_PREAD;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

static inline void prepare_preadOpForUC(struct shmipc_msg *msg,
                                        struct preadOpPacked *op, int fd,
                                        size_t count, off_t offset) {
  msg->type = CFS_OP_PREAD;
  op->offset = offset;
  op->rwOp.realCount = 0;
  op->rwOp.realOffset = 0;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct writeOp *fillWriteOp(struct clientOp *curCop, int fd, size_t count) {
  curCop->opCode = CFS_OP_WRITE;
  curCop->opStatus = OP_NEW;
  struct writeOp *wop = &((curCop->op).write);
  fillRwOpCommon(&wop->rwOp, fd, count);
  return wop;
}

static inline void prepare_writeOp(struct shmipc_msg *msg,
                                   struct writeOpPacked *op, int fd,
                                   size_t count) {
  msg->type = CFS_OP_WRITE;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

struct pwriteOp *fillPWriteOp(struct clientOp *curCop, int fd, size_t count,
                              off_t offset) {
  curCop->opCode = CFS_OP_PWRITE;
  curCop->opStatus = OP_NEW;
  struct pwriteOp *wop = &((curCop->op).pwrite);
  wop->offset = offset;
  fillRwOpCommon(&wop->rwOp, fd, count);
  return wop;
}

static inline void prepare_pwriteOp(struct shmipc_msg *msg,
                                    struct pwriteOpPacked *op, int fd,
                                    size_t count, off_t offset) {
  msg->type = CFS_OP_PWRITE;
  op->offset = offset;
  prepare_rwOpCommon(&(op->rwOp), fd, count);
}

static inline void prepare_lseekOp(struct shmipc_msg *msg, struct lseekOp *op,
                                   int fd, long int offset, int whence) {
  msg->type = CFS_OP_LSEEK;
  op->fd = fd;
  op->offset = offset;
  op->whence = whence;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct openOp *fillOpenOp(struct clientOp *curCop, const char *path, int flags,
                          mode_t mode) {
  curCop->opCode = CFS_OP_OPEN;
  curCop->opStatus = OP_NEW;
  struct openOp *oop = &((curCop->op).open);
  adjustPath(path, &(oop->path)[0]);
  oop->flags = flags;
  oop->mode = mode;
  return oop;
}

/* The remainder of prepare_* functions do not accept *Packed structs as we
 * aren't optimizing them for now.
 * TODO: find packed representations for the following structs as well */
static inline void prepare_openOp(struct shmipc_msg *msg, struct openOp *op,
                                  const char *path, int flags, mode_t mode,
                                  uint64_t *size = nullptr) {
  msg->type = CFS_OP_OPEN;
  op->flags = flags;
  op->mode = mode;
  op->lease = size != nullptr;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(path, &(op->path[0]));
}

struct closeOp *fillCloseOp(struct clientOp *curCop, int fd) {
  curCop->opCode = CFS_OP_CLOSE;
  curCop->opStatus = OP_NEW;
  struct closeOp *cloOp = &((curCop->op).close);
  cloOp->fd = fd;
  return cloOp;
}

static inline void prepare_closeOp(struct shmipc_msg *msg, struct closeOp *op,
                                   int fd) {
  msg->type = CFS_OP_CLOSE;
  op->fd = fd;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct statOp *fillStatOp(struct clientOp *curCop, const char *path,
                          struct stat *statbuf) {
  curCop->opCode = CFS_OP_STAT;
  curCop->opStatus = OP_NEW;
  struct statOp *statop = &((curCop->op).stat);
  adjustPath(path, &(statop->path)[0]);
  memset(&(statop->statbuf), 0, sizeof(struct stat));
#ifdef _CFS_LIB_DEBUG_
  // fprintf(stderr, "fillStatOp - path:%s\n", statop->path);
#endif
  return statop;
}

static inline void prepare_statOp(struct shmipc_msg *msg, struct statOp *op,
                                  const char *path) {
  msg->type = CFS_OP_STAT;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(path, &(op->path[0]));
}

struct mkdirOp *fillMkdirOp(struct clientOp *curCop, const char *pathname,
                            mode_t mode) {
  curCop->opCode = CFS_OP_MKDIR;
  curCop->opStatus = OP_NEW;
  struct mkdirOp *mkop = &((curCop->op).mkdir);
  mkop->mode = mode;
  adjustPath(pathname, &(mkop->pathname[0]));
  return mkop;
}

static inline void prepare_mkdirOp(struct shmipc_msg *msg, struct mkdirOp *op,
                                   const char *path, mode_t mode) {
  msg->type = CFS_OP_MKDIR;
  op->mode = mode;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(path, &(op->pathname[0]));
}

struct fstatOp *fillFstatOp(struct clientOp *curCop, int fd,
                            struct stat *statbuf) {
  curCop->opCode = CFS_OP_FSTAT;
  curCop->opStatus = OP_NEW;
  struct fstatOp *fstatop = &((curCop->op).fstat);
  fstatop->fd = fd;
  return fstatop;
}

static inline void prepare_fstatOp(struct shmipc_msg *msg, struct fstatOp *op,
                                   int fd) {
  msg->type = CFS_OP_FSTAT;
  op->fd = fd;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct renameOp *fillRenameOp(struct clientOp *curCop, const char *oldpath,
                              const char *newpath) {
  curCop->opCode = CFS_OP_RENAME;
  curCop->opStatus = OP_NEW;
  struct renameOp *rnmop = &((curCop->op).rename);
  adjustPath(oldpath, &(rnmop->oldpath[0]));
  adjustPath(newpath, &(rnmop->newpath[0]));
  return rnmop;
}

static inline void prepare_renameOp(struct shmipc_msg *msg, struct renameOp *op,
                                    const char *oldpath, const char *newpath) {
  msg->type = CFS_OP_RENAME;
  EmbedThreadIdToAsOpRet(op->ret);
  adjustPath(oldpath, &(op->oldpath[0]));
  adjustPath(newpath, &(op->newpath[0]));
}

struct rmdirOp *fillRmdirOp(struct clientOp *curCop, const char *pathname) {
  curCop->opCode = CFS_OP_RENAME;
  curCop->opStatus = OP_NEW;
  struct rmdirOp *rmdop = &((curCop->op).rmdir);
  EmbedThreadIdToAsOpRet(rmdop->ret);
  adjustPath(pathname, &(rmdop->pathname[0]));
  return rmdop;
}

struct fsyncOp *fillFsyncOp(struct clientOp *curCop, int fd, bool isDataSync) {
  curCop->opCode = CFS_OP_FSYNC;
  curCop->opStatus = OP_NEW;
  struct fsyncOp *fsyncop = &((curCop->op).fsync);
  fsyncop->fd = fd;
  if (isDataSync) {
    fsyncop->is_data_sync = 1;
  } else {
    fsyncop->is_data_sync = 0;
  }
  return fsyncop;
}

static inline void prepare_fsyncOp(struct shmipc_msg *msg, struct fsyncOp *op,
                                   int fd, bool isDataSync) {
  msg->type = CFS_OP_FSYNC;
  op->fd = fd;
  op->is_data_sync = (isDataSync) ? 1 : 0;
  EmbedThreadIdToAsOpRet(op->ret);
}

struct unlinkOp *fillUnlinkOp(struct clientOp *curCop, const char *pathname) {
  curCop->opCode = CFS_OP_UNLINK;
  curCop->opStatus = OP_NEW;
  struct unlinkOp *unlkop = &((curCop->op).unlink);
  adjustPath(pathname, &(unlkop->path)[0]);
  return unlkop;
}

static inline void prepare_unlinkOp(struct shmipc_msg *msg, struct unlinkOp *op,
                                    const char *pathname) {
  msg->type = CFS_OP_UNLINK;
  adjustPath(pathname, &(op->path[0]));
  EmbedThreadIdToAsOpRet(op->ret);
}

static inline void prepare_opendirOp(struct shmipc_msg *msg,
                                     struct opendirOp *op,
                                     const char *pathname) {
  msg->type = CFS_OP_OPENDIR;
  adjustPath(pathname, &(op->name[0]));
  EmbedThreadIdToAsOpRet(op->numDentry);
}

static inline void prepare_rmdirOp(struct shmipc_msg *msg, struct rmdirOp *op,
                                   const char *pathname) {
  msg->type = CFS_OP_RMDIR;
  adjustPath(pathname, &(op->pathname[0]));
  EmbedThreadIdToAsOpRet(op->ret);
}

static inline void prepare_exitOp(struct shmipc_msg *msg, struct exitOp *op) {
  msg->type = CFS_OP_EXIT;
  EmbedThreadIdToAsOpRet(op->ret);
}

static inline void prepare_chkptOp(struct shmipc_msg *msg, struct chkptOp *op) {
  msg->type = CFS_OP_CHKPT;
}

static inline void prepare_migrateOp(struct shmipc_msg *msg,
                                     struct migrateOp *op, int fd) {
  msg->type = CFS_OP_MIGRATE;
  op->fd = fd;
}

static inline void prepare_dumpinodesOp(struct shmipc_msg *msg,
                                        struct dumpinodesOp *op) {
  msg->type = CFS_OP_DUMPINODES;
}

static inline void prepare_syncallOp(struct shmipc_msg *msg,
                                     struct syncallOp *synop) {
  msg->type = CFS_OP_SYNCALL;
  EmbedThreadIdToAsOpRet(synop->ret);
}

static inline void prepare_syncunlinkedOp(struct shmipc_msg *msg,
                                          struct syncunlinkedOp *synop) {
  msg->type = CFS_OP_SYNCUNLINKED;
  EmbedThreadIdToAsOpRet(synop->ret);
}

// Check the size of request's io is less than one slot's attached data.
// TODO (jingliu): Ideally, we can hide this kind of detail and do the
// multiplexing when the request is larger. Currently limit the io size for
// simplicity, App can still multiplex by themselves. In the end, we need to
// support very large request.
static inline bool fs_check_req_io_size(size_t count, const char *func_name) {
  if (count > RING_DATA_ITEM_SIZE) {
    fprintf(stderr, "ERROR %s io too large (count=%lu) not supported\n",
            func_name, count);
    return false;
  }
  return true;
}

// ping Op
static inline void prepare_pingOp(struct shmipc_msg *msg, struct pingOp *op) {
  msg->type = CFS_OP_PING;
}

// send an op without argument to given FsService
template <typename NoArgOp>
static int send_noargop(FsService *fsServ, CfsOpCode opcode) {
  struct shmipc_msg msg;
  NoArgOp *op;
  int ret;

  off_t ring_idx;
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  op = reinterpret_cast<NoArgOp *> IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);

  // prep this msg with opcode only
  memset(&msg, 0, sizeof(msg));
  msg.type = opcode;

  // send msg
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  // extract result
  ret = op->ret;

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

/*----------------------------------------------------------------------------*/

// Global variables
// The master's workerID
static int gPrimaryServWid = APP_CFS_COMMON_KNOWLEDGE_MASTER_WID;
FsLibSharedContext *gLibSharedContext = nullptr;

static key_t g_registered_shm_key_base = 0;
static int g_registered_max_num_worker = -1;
static int g_registered_shmkey_distance = -1;
std::once_flag g_register_flag;

// Used for multiple Fs processes
static FsLibServiceMng *gServMngPtr = nullptr;
std::atomic_bool gCleanedUpDone;
std::atomic_flag gMultiFsServLock = ATOMIC_FLAG_INIT;

// Thread local variables
// if threadFsTid is 0, it means fsLib has not setup its FsLibMemMng
thread_local int threadFsTid = 0;

#ifdef CFS_LIB_SAVE_API_TS
thread_local FsApiTs *tFsApiTs;
#endif

#ifdef FS_LIB_SPPG
constexpr uint32_t kLocalPinnedMemSize = ((uint32_t)1024) * 1024 * 16;  // 16M

struct SpdkEnvWrapper {
  struct spdk_env_opts opts;
};

std::shared_ptr<SpdkEnvWrapper> gSpdkEnvPtr;
std::once_flag gSpdkEnvFlag;

struct FsLibPinnedMemMng {
  void *memPtr{nullptr};
};
thread_local std::unique_ptr<FsLibPinnedMemMng> tlPinnedMemPtr;
#endif

static inline off_t shmipc_mgr_alloc_slot_dbg(struct shmipc_mgr *mgr) {
#ifdef _SHMIPC_DBG_
  off_t ring_idx = shmipc_mgr_alloc_slot(mgr);
  fprintf(stdout, "tid:%d ring_idx:%zu\n", threadFsTid, ring_idx);
  return ring_idx;
#else
  return shmipc_mgr_alloc_slot(mgr);
#endif
}

/*----------------------------------------------------------------------------*/

// Global utility function for resolivng FsService given a file descriptor
inline FsService *getFsServiceForFD(int fd, int &wid) {
  auto &fdMap = gLibSharedContext->fdWidMap;
  auto search = fdMap.find(fd);
  if (search != fdMap.end()) {
    wid = search->second;
    auto service = gServMngPtr->multiFsServMap[wid];
    service->inUse = true;
    return service;
  }

  // TODO (ask jing) - do fd's also default to primary if they aren't in the
  // map?
  wid = gPrimaryServWid;
  return gServMngPtr->primaryServ;
}

inline FsService *getFsServiceForPath(const char *path, int &wid) {
  auto &pathMap = gLibSharedContext->pathWidMap;
  auto search = pathMap.find(path);
  if (search != pathMap.end()) {
    wid = search->second;
    auto service = gServMngPtr->multiFsServMap[wid];
    service->inUse = true;
    return service;
  }

  wid = gPrimaryServWid;
  return gServMngPtr->primaryServ;
}

// getWidFromReturnCode returns a wid >= 0 when rc indicates that an inode has
// migrated to a diffrent worker and needs to be retried. If wid < 0, it can be
// safely ignored.
inline int getWidFromReturnCode(int rc) {
  if (rc > FS_REQ_ERROR_INODE_REDIRECT) return -1;

  int newWid = FS_REQ_ERROR_INODE_REDIRECT - rc;
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "detected wid from return code (%d)\n", newWid);
#endif
  return newWid;
}

// FIXME: this is not thread-safe right now
// check the return value of one operation to see if we need to change wid
// If so, do the update according to the return value and op
// @retVal : do not want to check type inside, so let caller directly pass in
//   the return value. it should be mostly the *ret* field
// @return : if update, set to true
bool checkUpdateFdWid(int retVal, int fd) {
  // TODO : maintain an internal mapping of fd to path? We would want to
  // update the path mapping as well.
  int newWid = getWidFromReturnCode(retVal);
  if (newWid >= 0) {
    auto widIt = gLibSharedContext->fdWidMap.find(fd);
    if (widIt != gLibSharedContext->fdWidMap.end()) {
      widIt->second = newWid;
    } else {
      fprintf(stderr, "checkUpdateFdWid cannot find fd's worker\n");
      throw std::runtime_error("checkUpdateFdWid cannot find fd worker");
    }
    return true;
  }
  return false;
}

// NOTE: gLibSharedContent->pathWidMap is NOT supposed to have wid=0 as key
// Because the default wid is 0, if not found, then send to Primary worker
// NOTE: pathWidMap is not thread-safe for *erase*, let's see if this will
// result in something seriously bad (e.g., endless RETRY?)
void updatePathWidMap(int8_t newWid, const char *path) {
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stderr, "newWid is %d\n", (int)newWid);
#endif
  if (newWid == 0) {
    auto it = gLibSharedContext->pathWidMap.find(path);
    if (it != gLibSharedContext->pathWidMap.end()) {
      int cnt = gLibSharedContext->pathWidMap.unsafe_erase(path);
      if (cnt < 1) {
        // FIXME: what should we do here if it fail. not a big deal?
        fprintf(stderr, "pathWidMap erase path, cnt:%d\n", cnt);
      }
    }
  } else {
    gLibSharedContext->pathWidMap.emplace(path, newWid);
  }
}

// Spins for a little while if the return code indicates inode in transfer.
// Returns true/false to indicate whether it spun or not.
inline bool handle_inode_in_transfer(int rc) {
  if (rc == FS_REQ_ERROR_INODE_IN_TRANSFER) {
    uint64_t start_ts = tap_ustime();
    static const uint64_t k_interval = 1;
    while (tap_ustime() - start_ts < k_interval) continue;
    return true;
  }
  return false;
}

uint8_t fs_notify_server_new_shm(const char *shmFname,
                                 fslib_malloc_block_sz_t block_sz,
                                 fslib_malloc_block_sz_t block_cnt) {
  struct shmipc_msg msg;
  struct newShmAllocatedOp *aop;
  off_t ring_idx;
  uint8_t ret;

  memset(&msg, 0, sizeof(msg));
  msg.type = CFS_OP_NEW_SHM_ALLOCATED;

  ring_idx = shmipc_mgr_alloc_slot_dbg(gServMngPtr->primaryServ->shmipc_mgr);
  aop = (struct newShmAllocatedOp *)IDX_TO_XREQ(
      gServMngPtr->primaryServ->shmipc_mgr, ring_idx);
  nowarn_strncpy(aop->shmFname, shmFname, MULTI_DIRSIZE);
  aop->shmBlockSize = block_sz;
  aop->shmNumBlocks = block_cnt;
  shmipc_mgr_put_msg(gServMngPtr->primaryServ->shmipc_mgr, ring_idx, &msg);

  ret = aop->shmid;
  shmipc_mgr_dealloc_slot(gServMngPtr->primaryServ->shmipc_mgr, ring_idx);
  return ret;
}

// NOTE: this function is only used for check_app_thread_mem_buf_read()
// Never use it directly in the implementation of File system APIs
// REQUIRED: write lock of gLibSharedContext->tidIncrLock needs to be held to
// enter this function
void _setup_app_thread_mem_buf() {
  assert(threadFsTid == 0);
#ifdef _CFS_LIB_PRINT_REQ_
  pid_t curPid = syscall(__NR_gettid);
  fprintf(stderr, "===> _setup_app_thread_mem_buf() called for pid:%d\n",
          curPid);
#endif
  dumpAllocatedOpCommon(nullptr);
  int curThreadFsTid = gLibSharedContext->tidIncr++;
  threadFsTid = curThreadFsTid;
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs = gLibSharedContext->apiTsMng_.initForTid(threadFsTid);
  if (tFsApiTs == nullptr) throw std::runtime_error("error cannot get apiTs");
#endif
  auto curFsLibMemBuf = new FsLibMemMng(gLibSharedContext->key, threadFsTid);
  int rc = curFsLibMemBuf->init(true);
  if (rc < 0) {
    fprintf(stderr, "ERROR cannot init FsLibMemMng for tid:%d\n", threadFsTid);
  }

  int numShmFiles = curFsLibMemBuf->getNumShmFiles();
  char cur_shmFname[MULTI_DIRSIZE];
  fslib_malloc_block_sz_t cur_block_sz;
  fslib_malloc_block_sz_t cur_block_cnt;
  uint8_t cur_shmid;

  for (int idx = 0; idx < numShmFiles; idx++) {
    curFsLibMemBuf->getShmConfigForIdx(idx, cur_shmFname, cur_block_sz,
                                       cur_block_cnt, cur_shmid);
    cur_shmid =
        fs_notify_server_new_shm(cur_shmFname, cur_block_sz, cur_block_cnt);
    if (cur_shmid < 0) {
      fprintf(stderr, "ERROR cannot notify server of new shm for tid:%d\n",
              threadFsTid);
    }
    curFsLibMemBuf->setShmIDForIdx(idx, cur_shmid);
  }

  gLibSharedContext->tidMemBufMap.insert(
      std::make_pair(threadFsTid, curFsLibMemBuf));
}

FsLibMemMng *check_app_thread_mem_buf_ready(int fsTid) {
  // auto curLock = &(gLibSharedContext->tidIncrLock);
  FsLibMemMng *rt = nullptr;

  if (fsTid == 0) {
    _setup_app_thread_mem_buf();
    rt = gLibSharedContext->tidMemBufMap[threadFsTid];
#ifdef _CFS_LIB_DEBUG_
    fprintf(stderr, "check_app_thread_mem_buf_ready() threadFsTid == 0 -> %d\n",
            threadFsTid);
#endif
  } else {
    rt = gLibSharedContext->tidMemBufMap[fsTid];
  }

  assert(rt->isInitDone());
  return rt;
}

void fs_register_via_socket() {
  struct sockaddr_un addr;
  struct sockaddr_un server_addr;
  struct UfsRegisterOp rgst_op;
  struct UfsRegisterAckOp rgst_ack_op;
  memset(&rgst_op, 0, sizeof(rgst_op));
  memset(&rgst_ack_op, 0, sizeof(rgst_ack_op));
  pid_t my_pid = getpid();
  rgst_op.emu_pid = my_pid;

  constexpr int kPathLen = 64;
  char my_recv_sock_path[kPathLen];
  char server_sock_path[kPathLen];
  sprintf(my_recv_sock_path, "/ufs-app-%d", my_pid);
  sprintf(server_sock_path, "%s", CRED_UNIX_SOCKET_PATH);

  addr.sun_family = AF_UNIX;
  nowarn_strncpy(addr.sun_path, my_recv_sock_path, kPathLen);
  server_addr.sun_family = AF_UNIX;
  nowarn_strncpy(server_addr.sun_path, server_sock_path, kPathLen);

  int sfd = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (sfd == -1) {
    fprintf(stderr, "Error: socket failed [%s]\n", strerror(errno));
  }
  if (bind(sfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_un)) == -1) {
    printf("Error: bind failed [%s]\n", strerror(errno));
    close(sfd);
  }

  struct msghdr msgh;
  msgh.msg_name = (void *)&server_addr;
  msgh.msg_namelen = sizeof(server_addr);
  struct iovec iov;
  msgh.msg_iov = &iov;
  msgh.msg_iovlen = 1;
  iov.iov_base = &rgst_op;
  iov.iov_len = sizeof(rgst_op);
  msgh.msg_control = NULL;
  msgh.msg_controllen = 0;

  ssize_t ns = sendmsg(sfd, &msgh, 0);
  if (ns == -1) {
    fprintf(stderr, "Error: sendmsg return -1\n");
  }

  iov.iov_base = &rgst_ack_op;
  iov.iov_len = sizeof(rgst_ack_op);
  ssize_t nr = recvmsg(sfd, &msgh, 0);
  if (nr == -1) {
    fprintf(stderr, "Error: recv ack return -1\n");
  }
  g_registered_max_num_worker = rgst_ack_op.num_worker_max;
  g_registered_shm_key_base = rgst_ack_op.shm_key_base;
  g_registered_shmkey_distance = rgst_ack_op.worker_key_distance;
}

static void print_app_zc_mimic() {
#ifndef MIMIC_APP_ZC
  fprintf(stdout, "MIMIC_APP_ZC - OFF\n");
#else
  fprintf(stdout, "MIMIC_APP_ZC - ON\n");
#endif
}

// If app know's its key, allow init with the key
int fs_init(key_t key) {
  print_app_zc_mimic();
  if (gServMngPtr != nullptr) {
    fprintf(stderr, "ERROR fs has initialized\n");
    return -1;
  }

  while (gServMngPtr == nullptr) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      try {
        gServMngPtr = new FsLibServiceMng();
        gServMngPtr->primaryServ = new FsService(gPrimaryServWid, key);
        gServMngPtr->primaryServ->inUse = true;
        // gPrimaryServ = new FsService(gPrimaryServWid, key);
      } catch (const char *msg) {
        fprintf(stderr, "Cannot init FsService:%s\n", msg);
        return -1;
      }

      gServMngPtr->multiFsServMap.insert(
          std::make_pair(gPrimaryServWid, gServMngPtr->primaryServ));
      gServMngPtr->multiFsServNum = 1;

      if (gLibSharedContext == nullptr) {
        gLibSharedContext = new FsLibSharedContext();
        gLibSharedContext->tidIncr = 1;
        gLibSharedContext->key = key;
      }

      gMultiFsServLock.clear(std::memory_order_release);
    }
  }
  return 0;
}

// NOTE: guarded by gMultiFsServLock
int fs_init_after_registration() {
  if (g_registered_max_num_worker <= 0) return -1;
  while (gServMngPtr == nullptr) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      if (g_registered_max_num_worker > 0) {
        gServMngPtr = new FsLibServiceMng();
        int curWid;
        for (int i = 0; i < g_registered_max_num_worker; i++) {
          auto key =
              g_registered_shm_key_base + i * g_registered_shmkey_distance;
          fprintf(stdout,
                  "fs_init_after_registration-> init for key:%d map key is: %d "
                  "dist:%d\n",
                  key, (i + gPrimaryServWid), g_registered_shmkey_distance);
          curWid = i + gPrimaryServWid;
          gServMngPtr->multiFsServMap.insert(
              std::make_pair(curWid, new FsService(curWid, key)));
        }
        gServMngPtr->primaryServ = gServMngPtr->multiFsServMap[gPrimaryServWid];
        gServMngPtr->primaryServ->inUse = true;
        gServMngPtr->multiFsServNum = g_registered_max_num_worker;

        if (gLibSharedContext == nullptr) {
          gLibSharedContext = new FsLibSharedContext();
          gLibSharedContext->tidIncr = 1;
          gLibSharedContext->key = g_registered_shm_key_base;
        }

        gCleanedUpDone = false;
      }
      gMultiFsServLock.clear(std::memory_order_release);
    }
  }
  return 0;
}

// Use for app do not know its' key. which is for real application
int fs_register(void) {
  std::call_once(g_register_flag, fs_register_via_socket);
  // fprintf(stdout, "g_register_shm_key_base:%d max_nw:%d\n",
  //        g_registered_shm_key_base, g_registered_max_num_worker);
  int rt = fs_init_after_registration();
  return rt;
}

// each key will represent one FSP thread (instance/worker)
int fs_init_multi(int num_key, const key_t *keys) {
  print_app_zc_mimic();
  while (gServMngPtr == nullptr) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      gServMngPtr = new FsLibServiceMng();
      int curWid;
      for (int i = 0; i < num_key; i++) {
        auto key = keys[i];
        fprintf(stdout, "fs_init_multi-> init for key:%d map key is: %d\n", key,
                (i + gPrimaryServWid));
        curWid = i + gPrimaryServWid;
        gServMngPtr->multiFsServMap.insert(
            std::make_pair(curWid, new FsService(curWid, key)));
      }
      gServMngPtr->primaryServ = gServMngPtr->multiFsServMap[gPrimaryServWid];
      gServMngPtr->primaryServ->inUse = true;
      gServMngPtr->multiFsServNum = num_key;

      if (gLibSharedContext == nullptr) {
        gLibSharedContext = new FsLibSharedContext();
        gLibSharedContext->tidIncr = 1;
        gLibSharedContext->key = keys[0];
      }

      gCleanedUpDone = false;
      gMultiFsServLock.clear(std::memory_order_release);
    }
  }

  return 0;
}

// Now, it will busy wait the result by checking the op's status variable
void spinWaitOpDone(struct clientOp *copPtr) {
  // std::cout << "spinwaitdone init:" << (int) copPtr->opStatus << std::endl;
  while (copPtr->opStatus != OP_DONE) {
    // spin wait
  }
  // assert(copPtr->opStatus == OP_DONE);
}

//// Check if the access of fs has be initialized for calling process.
//// Return true when access is OK
static inline bool check_fs_access_ok() {
  // return !(gFsServ == nullptr && gMultiFsServNum == 0);
  return gServMngPtr->multiFsServNum != 0;
}

int fs_exit() {
  int ret = 0;

  for (auto iter : gServMngPtr->multiFsServMap) {
    auto service = iter.second;
    if (!(service->inUse)) continue;

    auto wid = iter.first;
    struct shmipc_msg msg;
    struct exitOp *eop;
    off_t ring_idx;
    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
    eop = (struct exitOp *)IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
    prepare_exitOp(&msg, eop);
    fprintf(stdout, "fs_exit: for wid %d\n", wid);
    shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);
    ret = eop->ret;
    shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);

    if (ret < 0) {
      fprintf(stderr, "fs_exit: failed for wid %d\n", wid);
      return ret;
    }
    shmipc_mgr_client_reset(service->shmipc_mgr);
  }
#ifdef CFS_LIB_SAVE_API_TS
  gLibSharedContext->apiTsMng_.reportAllTs();
#endif
  pid_t my_pid = getpid();
  char my_recv_sock_path[128];
  sprintf(my_recv_sock_path, "/ufs-app-%d", my_pid);
  remove(my_recv_sock_path);
  return ret;
}

int fs_ping() {
  int ret = -1;

  for (auto iter : gServMngPtr->multiFsServMap) {
    auto service = iter.second;
    if (!(service->inUse)) continue;

    auto wid = iter.first;
    struct shmipc_msg msg;
    struct pingOp *eop;
    off_t ring_idx;
    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
    eop = (struct pingOp *)IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
    prepare_pingOp(&msg, eop);
    shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);
    ret = eop->ret;
    shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);
    if (ret < 0) {
      fprintf(stderr, "fs_ping: failed for wid %d\n", wid);
      return ret;
    }
  }
  return ret;
}

int fs_start_dump_load_stats() {
  auto fsServ = gServMngPtr->primaryServ;
  int ret = send_noargop<startDumpLoadOp>(fsServ, CFS_OP_STARTDUMPLOAD);
  return ret;
}

int fs_stop_dump_load_stats() {
  auto fsServ = gServMngPtr->primaryServ;
  int ret = send_noargop<stopDumpLoadOp>(fsServ, CFS_OP_STOPDUMPLOAD);
  return ret;
}

int fs_cleanup() {
  if (gCleanedUpDone) return 0;
  while (!gCleanedUpDone) {
    if (gMultiFsServLock.test_and_set(std::memory_order_acquire)) {
      for (auto ele : gServMngPtr->multiFsServMap) {
        fprintf(stderr, "fs_cleanup: key:%d\n", ele.first);
        delete ele.second;
        ele.second = nullptr;
      }
      gServMngPtr->multiFsServMap.clear();
      gLibSharedContext = nullptr;
      gServMngPtr->primaryServ = nullptr;
      gCleanedUpDone = true;
      gMultiFsServLock.clear(std::memory_order_release);
    }
  }
  return 0;
}

int fs_checkpoint() {
  struct shmipc_msg msg;
  struct chkptOp *cop;
  off_t ring_idx;
  int ret;
  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(gServMngPtr->primaryServ->shmipc_mgr);
  cop = (struct chkptOp *)IDX_TO_XREQ(gServMngPtr->primaryServ->shmipc_mgr,
                                      ring_idx);
  prepare_chkptOp(&msg, cop);
  shmipc_mgr_put_msg(gServMngPtr->primaryServ->shmipc_mgr, ring_idx, &msg);

  ret = cop->ret;
  shmipc_mgr_dealloc_slot(gServMngPtr->primaryServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_migrate(int fd, int *dstWid) {
  struct shmipc_msg msg;
  struct migrateOp *mop;
  off_t ring_idx;
  int ret;
  memset(&msg, 0, sizeof(msg));

  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);

  ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
  mop = (struct migrateOp *)IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
  prepare_migrateOp(&msg, mop, fd);
  shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);

  ret = mop->ret;
  shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);

  checkUpdateFdWid(ret, fd);
  *dstWid = getWidFromReturnCode(ret);

  if ((ret == FS_REQ_ERROR_INODE_IN_TRANSFER) || (*dstWid >= 0)) {
    return 0;
  }
  return ret;
}

static inline void prepare_inodeReassignmentOp(struct shmipc_msg *msg,
                                               struct inodeReassignmentOp *op,
                                               int type, uint32_t inode,
                                               int curOwner, int newOwner) {
  msg->type = CFS_OP_INODE_REASSIGNMENT;
  op->type = type;
  op->inode = inode;
  op->curOwner = curOwner;
  op->newOwner = newOwner;
}

// admin command for testing, can be removed from header for user library.
// TODO: use an enum for type. However, I'd like one definition that can be
// shared by both fsapi and FsProc, which hasn't been done yet. For now, the
// values for type are
//
//      Messages must be sent to curOwner wid. i.e. if you send curOwner=2 to
//      wid 0, EACCES is returned. Returning EINVAL indicates the type is
//      invalid. Returning EPERM indicates not the current owner.
//
// (type=0) Check if curOwner is the owner of the inode. newOwner is ignored.
//      Returning 0 indicates success - it is the owner.
// (type=1) Move inode from curOwner to newOwner if curOwner is really the
// owner.
//      Returning 0 indicates successful migration.
int fs_admin_inode_reassignment(int type, uint32_t inode, int curOwner,
                                int newOwner) {
  auto search = gServMngPtr->multiFsServMap.find(curOwner);
  if (search == gServMngPtr->multiFsServMap.end()) {
    throw std::runtime_error("Wid not found in multiFsServMap");
  }

  auto service = search->second;
  struct shmipc_msg msg;
  struct inodeReassignmentOp *irop;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(service->shmipc_mgr);
  irop = (decltype(irop))IDX_TO_XREQ(service->shmipc_mgr, ring_idx);
  prepare_inodeReassignmentOp(&msg, irop, type, inode, curOwner, newOwner);
  shmipc_mgr_put_msg(service->shmipc_mgr, ring_idx, &msg);

  ret = irop->ret;
  shmipc_mgr_dealloc_slot(service->shmipc_mgr, ring_idx);

  return ret;
}

static inline void prepare_threadReassignOp(struct shmipc_msg *msg,
                                            struct threadReassignOp *op,
                                            int tid, int src_wid, int dst_wid,
                                            int flag = FS_REASSIGN_ALL) {
  msg->type = CFS_OP_THREAD_REASSIGNMENT;
  op->tid = tid;
  op->src_wid = src_wid;
  op->dst_wid = dst_wid;
  op->ret = 0;
  op->flag = flag;
}

static inline bool CheckValidWid(int wid) {
  return (wid >= 0 && wid < NMAX_FSP_WORKER);
}

// testing and experimenting
// change the handling work of the entity: pid+tid from src_wid to dst_wid
int fs_admin_thread_reassign(int src_wid, int dst_wid, int flag) {
  if (src_wid == dst_wid ||
      (!CheckValidWid(src_wid) || !CheckValidWid(dst_wid))) {
    return -1;
  }
  auto serv_it = gServMngPtr->multiFsServMap.find(src_wid);
  if (serv_it == gServMngPtr->multiFsServMap.end()) {
    throw std::runtime_error("src_wid not existing src_wid=" +
                             std::to_string(src_wid));
  }
  struct shmipc_msg msg;
  struct threadReassignOp *irop;
  off_t ring_idx;
  int ret = 0;

  int cur_tid = threadFsTid;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(serv_it->second->shmipc_mgr);
  irop = (decltype(irop))IDX_TO_XREQ(serv_it->second->shmipc_mgr, ring_idx);

  prepare_threadReassignOp(&msg, irop, cur_tid, src_wid, dst_wid, flag);
  shmipc_mgr_put_msg(serv_it->second->shmipc_mgr, ring_idx, &msg);

  ret = irop->ret;
  shmipc_mgr_dealloc_slot(serv_it->second->shmipc_mgr, ring_idx);
  if (irop->ret < 0) {
    return ret;
  }

  if (src_wid != gPrimaryServWid && flag != FS_REASSIGN_PAST) {
    // send to primary for updating future handling worker
    struct threadReassignOp *pri_irop;
    off_t pri_ring_idx;
    struct shmipc_msg pri_msg;
    auto pri_serv = gServMngPtr->primaryServ;

    pri_ring_idx = shmipc_mgr_alloc_slot_dbg(pri_serv->shmipc_mgr);
    pri_irop =
        (decltype(pri_irop))IDX_TO_XREQ(pri_serv->shmipc_mgr, pri_ring_idx);
    prepare_threadReassignOp(&pri_msg, pri_irop, cur_tid, src_wid, dst_wid);
    shmipc_mgr_put_msg(pri_serv->shmipc_mgr, pri_ring_idx, &pri_msg);
    if (pri_irop->ret < 0) {
      fprintf(stderr, "Error from pri orig_ret:%d ret:%d\n", ret,
              pri_irop->ret);
      ret = pri_irop->ret;
    }
    shmipc_mgr_dealloc_slot(pri_serv->shmipc_mgr, pri_ring_idx);
  }

  return ret;
}

int fs_dumpinodes_internal(FsService *fsServ) {
  int ret = send_noargop<dumpinodesOp>(fsServ, CFS_OP_DUMPINODES);
  return ret;
}

int fs_dumpinodes(int wid) {
  auto serv = gServMngPtr->multiFsServMap.find(wid);
  if (serv != gServMngPtr->multiFsServMap.end() && serv->second->inUse) {
    return fs_dumpinodes_internal(serv->second);
  } else {
    return -1;
  }
}

int fs_stat_internal(FsService *fsServ, const char *pathname,
                     struct stat *statbuf) {
  struct shmipc_msg msg;
  struct statOp *statOp;
  off_t ring_idx;
  int ret;
  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  statOp = (struct statOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_statOp(&msg, statOp, pathname);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = statOp->ret;
  if (ret != 0) goto cleanup;

  // copy the stats
  memcpy(statbuf, &(statOp->statbuf), sizeof(*statbuf));
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_stat(%s) ret:%d\n", pathname, ret);
#endif

cleanup:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_stat(const char *pathname, struct stat *statbuf) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_STAT);
#endif
  int delixArr[32];
  int dummy;
  char *standardPath = filepath2TokensStandardized(pathname, delixArr, dummy);

retry:
  int wid = -1;
  auto service = getFsServiceForPath(standardPath, wid);
  int rc = fs_stat_internal(service, standardPath, statbuf);

  if (rc >= 0) goto free_and_return;
  if (handle_inode_in_transfer(rc)) goto retry;

  wid = getWidFromReturnCode(rc);
  if (wid >= 0) {
    updatePathWidMap(wid, standardPath);
    goto retry;
  }

free_and_return:
  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_STAT, tsIdx);
#endif
  if (rc != 0) {
    errno = -rc;
    rc = -1;
  }
  return rc;
}

int fs_fstat_internal(FsService *fsServ, int fd, struct stat *statbuf) {
  struct shmipc_msg msg;
  struct fstatOp *fstatOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  fstatOp = (struct fstatOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_fstatOp(&msg, fstatOp, fd);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = fstatOp->ret;
  if (ret != 0) goto cleanup;

  memcpy(statbuf, &(fstatOp->statbuf), sizeof(struct stat));
cleanup:
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_fstat(int fd, struct stat *statbuf) {
  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  if (widIt != gLibSharedContext->fdWidMap.end()) {
    return fs_fstat_internal(gServMngPtr->multiFsServMap[widIt->second], fd,
                             statbuf);
  }
  return -1;
}

static int fs_open_internal(FsService *fsServ, const char *path, int flags,
                            mode_t mode, uint64_t *size = nullptr) {
  struct shmipc_msg msg;
  struct openOp *oop;
  off_t ring_idx;
  int ret;

  if (!check_fs_access_ok()) {
    std::cerr << "ERROR fs access uninitialized" << std::endl;
    return FS_ACCESS_UNINIT_ERROR;
  }

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  oop = (struct openOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_openOp(&msg, oop, path, flags, mode, size);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  // TODO make sure this doesn't get optimized out?
  ret = oop->ret;
  if (size) *size = oop->size;

#ifndef CFS_DISK_LAYOUT_LEVELDB
  if (ret >= 0) {
    auto curLock = &(gLibSharedContext->fdFileHandleMapLock);
    curLock->lock();
    // try to find if the fileName has already recorded
    struct FileHandle *curHandle = nullptr;
    auto fnameFhIt = gLibSharedContext->fnameFileHandleMap.find(oop->path);
    if (fnameFhIt != gLibSharedContext->fnameFileHandleMap.end()) {
      curHandle = fnameFhIt->second;
      curHandle->refCount++;
    }

    if (curHandle == nullptr) {
      // not found fileName, create new fileHandle
      curHandle = new FileHandle();
      // We directly use ino as filehandle
      curHandle->id = EMBEDED_INO_FILED_OP_OPEN(oop);
      // fprintf(stdout, "open return fdhd:%d\n", curHandle->id);
      curHandle->refCount = 1;
      strcpy(curHandle->fileName, oop->path);
      gLibSharedContext->fnameFileHandleMap.emplace(oop->path, curHandle);
    }
#ifdef _CFS_LIB_PRINT_REQ_
    fprintf(stderr, "insert to fdFileHandleMap - fd:%d, curHandle:%p\n", ret,
            curHandle);
#endif
    gLibSharedContext->fdFileHandleMap.insert(std::make_pair(ret, curHandle));
    gLibSharedContext->fdOffsetMap[ret] = 0;
    // set the offset to 0
    gLibSharedContext->fdCurOffMap.emplace(ret, 0);
    curLock->unlock();
  }
#endif

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_open(%s, flags:%d) ret:%d\n", path, flags, ret);
#endif
  // TODO reduce the amount of time the slot is held
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_open(const char *path, int flags, mode_t mode) {
  int delixArr[32];
  int dummy;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_OPEN);
#endif
#ifdef LDB_PRINT_CALL
  print_open(path, flags, mode);
#endif
  char *standardPath = filepath2TokensStandardized(path, delixArr, dummy);

  // NOTE: the first time invoking this is going to be extremely costly, ~0.2s
  // So, it is important for any user (thread) to invoke this
  //(void) check_app_thread_mem_buf_ready();
  assert(threadFsTid != 0);

retry:
  int wid = -1;
  auto service = getFsServiceForPath(standardPath, wid);
  int rc = fs_open_internal(service, standardPath, flags, mode);

  if (rc > 0) {
    gLibSharedContext->fdWidMap[rc] = wid;
    goto free_and_return;
  } else if (rc > FS_REQ_ERROR_INODE_IN_TRANSFER) {
    goto free_and_return;
  }

  // migration related errors
  if (handle_inode_in_transfer(rc)) goto retry;

  wid = getWidFromReturnCode(rc);
  if (wid >= 0) {
    updatePathWidMap(wid, standardPath);
    goto retry;
  }

free_and_return:
  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_OPEN, tsIdx);
#endif
  return rc;
}

int fs_open2(const char *path, int flags) { return fs_open(path, flags, 0); }

int fs_close_internal(FsService *fsServ, int fd) {
  struct shmipc_msg msg;
  struct closeOp *cloOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  cloOp = (struct closeOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_closeOp(&msg, cloOp, fd);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = cloOp->ret;

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_close(fd:%d) ret:%d\n", fd, ret);
#endif
  if (ret == 0) {
    auto curLock = &(gLibSharedContext->fdFileHandleMapLock);
    curLock->lock();
    auto fileHandleIt = gLibSharedContext->fdFileHandleMap.find(fd);
    if (fileHandleIt != gLibSharedContext->fdFileHandleMap.end()) {
      if (fileHandleIt->second->refCount > 1) {
        // we only delete [fd<->fileHandle] record if this is not the last FD
        // that is referring to that file. Then we can still find the cache
        // items once the file is re-opened.
        // TODO (jingliu): delete cache once unlink() is called
        // ==> needs to free the memory used by page cache
        gLibSharedContext->fdFileHandleMap.erase(fd);
      }
    }
    curLock->unlock();
  }
  // TODO (jingliu): it looks no need to delete it
  // Let's revisit it when doing experiments.
  // if (ret == 0) {
  //     fdWidMap.erase(fd);
  // }
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_close(int fd) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_CLOSE);
#endif
#ifdef LDB_PRINT_CALL
  print_close(fd);
#endif
retry:
  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);
  int rc = fs_close_internal(service, fd);

  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_CLOSE, tsIdx);
#endif
  return rc;
}

OpenLeaseMapEntry *LeaseRef(const char *path) {
  assert(false);
  return nullptr;
}

OpenLeaseMapEntry *LeaseRef(int fd) {
  std::shared_lock<std::shared_mutex> guard(
      gLibSharedContext->openLeaseMapLock);
  auto it = gLibSharedContext->fdOpenLeaseMap.find(fd);
  if (it == gLibSharedContext->fdOpenLeaseMap.end()) {
    // base fd lease not found, go to server
    return nullptr;
  } else {
    it->second->ref += 1;
    return it->second;
  }
}

void LeaseUnref(OpenLeaseMapEntry *entry, bool del = false) {
  if (del) {
    assert(false);
  } else {
    std::shared_lock<std::shared_mutex> lock(
        gLibSharedContext->openLeaseMapLock);
    int ref = entry->ref.fetch_sub(1);
    if (ref == 1) entry->cv.notify_all();
  }
}

int fs_open_lease(const char *path, int flags, mode_t mode) {
  int delixArr[32];
  int dummy;
  char *standardPath = filepath2TokensStandardized(path, delixArr, dummy);

  // reference step
  gLibSharedContext->openLeaseMapLock.lock_shared();
  auto it = gLibSharedContext->pathOpenLeaseMap.find(standardPath);
  OpenLeaseMapEntry *entry = nullptr;
  if (it == gLibSharedContext->pathOpenLeaseMap.end()) {
    // open, install lease, local open
    gLibSharedContext->openLeaseMapLock.unlock_shared();
    // do open to the server
    assert(threadFsTid != 0);
    uint64_t size;
  retry:
    int wid = -1;
    auto service = getFsServiceForPath(standardPath, wid);
    int rc = fs_open_internal(service, standardPath, flags, mode, &size);

    if (rc > 0) {
      gLibSharedContext->fdWidMap[rc] = wid;
      goto open_end;
    } else if (rc > FS_REQ_ERROR_INODE_IN_TRANSFER) {
      goto open_end;
    }
    // migration related errors
    if (handle_inode_in_transfer(rc)) goto retry;
    wid = getWidFromReturnCode(rc);
    if (wid >= 0) {
      updatePathWidMap(wid, standardPath);
      goto retry;
    }

  open_end:
    if (rc > 0 && size != 0) {
      {
        // install lease & local_open
        // reference step
        std::unique_lock<std::shared_mutex> lock(
            gLibSharedContext->openLeaseMapLock);
        // TODO: check for concurrent opens
        OpenLeaseMapEntry *new_entry =
            new OpenLeaseMapEntry(rc, standardPath, size);
        auto retval = gLibSharedContext->pathOpenLeaseMap.emplace(standardPath,
                                                                  new_entry);
        assert(retval.second);
        auto retval2 = gLibSharedContext->fdOpenLeaseMap.emplace(rc, new_entry);
        assert(retval2.second);
        entry = new_entry;
        entry->ref++;
      }
      {
        // local_open
        std::unique_lock<std::shared_mutex> lock(entry->lock);
        rc = entry->lease->Open(flags, mode);
      }
      LeaseUnref(entry);
    }
    free(standardPath);
    return rc;
  } else {
    // local open
    // reference done
    bool is_create = flags & O_CREAT;
    if (!is_create) {
      entry = it->second;
      entry->ref++;
    }
    gLibSharedContext->openLeaseMapLock.unlock_shared();
    // do local_open
    if (is_create) {
      free(standardPath);
      return -1;
    }
    int fd = -1;
    {
      // local_open
      std::unique_lock<std::shared_mutex> lock(entry->lock);
      fd = entry->lease->Open(flags, mode);
    }
    LeaseUnref(entry);
    free(standardPath);
    return fd;
  }
}

int fs_close_lease(int fd) {
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    if (entry) {
      {
        // local close
        int offset = OpenLease::FindOffset(fd);
        std::unique_lock<std::shared_mutex> lock(entry->lock);
        entry->lease->Close(offset);
      }
      LeaseUnref(entry);
      return 0;
    } else {
      return fs_close(fd);
    }
  } else {
    // global fd, go to server
    return fs_close(fd);
  }
}

int fs_unlink_internal(FsService *fsServ, const char *pathname) {
#ifdef LDB_PRINT_CALL
  print_unlink(pathname);
#endif
  struct shmipc_msg msg;
  struct unlinkOp *unlkop;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  unlkop = (struct unlinkOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_unlinkOp(&msg, unlkop, pathname);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = unlkop->ret;
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_unlink(pathname:%s) return:%d\n", pathname, ret);
#endif
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

// Man page of unlink:
// unlink() deletes a name from the filesystem.  If that name was the last link
// to a file and no processes have the file open, the file is deleted and the
// space it was using is made available for reuse.
int fs_unlink(const char *pathname) {
  int delixArr[32];
  int dummy;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_UNLINK);
#endif
  char *standardPath = filepath2TokensStandardized(pathname, delixArr, dummy);
  // NOTE: For now, unlink will always go to the primary worker
  int rt = fs_unlink_internal(gServMngPtr->primaryServ, standardPath);
  if (rt == 0) {
    // unlink succeed, remove path from pathWidmap
    updatePathWidMap(/*newWid*/ 0, standardPath);
    auto it =
        gLibSharedContext->pathLDBLeaseMap.find(std::string(standardPath));
    if (it != gLibSharedContext->pathLDBLeaseMap.end()) {
      delete it->second;
      gLibSharedContext->pathLDBLeaseMap.unsafe_erase(it);
    }
  }
  if (rt < 0 && rt >= FS_REQ_ERROR_INODE_IN_TRANSFER) rt = -1;
  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_UNLINK, tsIdx);
#endif
  return rt;
}

int fs_mkdir_internal(FsService *fsServ, const char *pathname, mode_t mode) {
  struct shmipc_msg msg;
  struct mkdirOp *mop;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  mop = (struct mkdirOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_mkdirOp(&msg, mop, pathname, mode);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = mop->ret;
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_mkdir(%s) ret:%d\n", pathname, ret);
#endif
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_mkdir(const char *pathname, mode_t mode) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_MKDIR);
#endif
  int rt = fs_mkdir_internal(gServMngPtr->primaryServ, pathname, mode);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_MKDIR, tsIdx);
#endif
  return rt;
}

CFS_DIR *fs_opendir_internal(FsService *fsServ, const char *name) {
  struct shmipc_msg msg;
  struct opendirOp *odop;
  cfs_dirent *cfsDentryPtr;
  CFS_DIR *dirp;
  off_t ring_idx;
  int numTotalDentry;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // zalloc is unnecessarily costly, use malloc
  void *dataPtr = fs_malloc((FS_DIR_MAX_WIDTH) * sizeof(cfs_dirent));

  int err = 0;
  threadMemBuf->getBufOwnerInfo(dataPtr, false, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_opendir_internal: Error in getBufOwnerInfo\n");
    return NULL;
  }

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  odop = (struct opendirOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_opendirOp(&msg, odop, name);
  odop->alOp.shmid = shmid;
  odop->alOp.dataPtrId = dataPtrId;

  // send request
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  // TODO ensure that the operation was successful
  numTotalDentry = odop->numDentry;
  cfsDentryPtr = (cfs_dirent *)(dataPtr);
  dirp = (CFS_DIR *)malloc(sizeof(*dirp));
  dirp->dentryNum = 0;
  dirp->dentryIdx = 0;
  if (numTotalDentry > 0) {
    dirp->firstDentry = (dirent *)malloc(sizeof(dirent) * numTotalDentry);
    for (int i = 0; i < numTotalDentry; i++) {
      if ((cfsDentryPtr + i)->inum == 0) {
        // Currently the openDir's FSP side implementation will read the whole
        // inode content, which contains the empty inode (after unlink)
        continue;
      }
      (dirp->firstDentry + dirp->dentryNum)->d_ino = (cfsDentryPtr + i)->inum;
      strcpy((dirp->firstDentry + dirp->dentryNum)->d_name,
             (cfsDentryPtr + i)->name);
      // Explicitly set d_type to UNKNOWN since we don't plan to carry type
      // info. See struct cfs_dirent comment.
      (dirp->firstDentry + dirp->dentryNum)->d_type = DT_UNKNOWN;
      dirp->dentryNum++;
    }
  }
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_opendir(%s)\n", name);
#endif
  // TODO measure cost to hold the slot. If it is too much, we might as well
  // copy the data and release the slot.
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  fs_free(dataPtr);
  return dirp;
}

CFS_DIR *fs_opendir(const char *name) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_OPENDIR);
#endif
  CFS_DIR *rt;
  rt = fs_opendir_internal(gServMngPtr->primaryServ, name);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_OPENDIR, tsIdx);
#endif
  return rt;
}

struct dirent *fs_readdir(CFS_DIR *dirp) {
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_readdir()\n");
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_READDIR);
#endif
  struct dirent *dent = NULL;
  if (dirp->dentryIdx > dirp->dentryNum) goto return_dent;
  if (dirp->dentryIdx == dirp->dentryNum) {
    goto return_dent;
  }
  dent = (dirp->firstDentry + (dirp->dentryIdx++));
return_dent:
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_READDIR, tsIdx);
#endif
  return dent;
}

int fs_closedir(CFS_DIR *dirp) {
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_closedir(dirp:%p)\n", dirp);
#endif
  if (dirp == nullptr) {
    return -1;
  }

#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_CLOSEDIR);
#endif

  // assume a close must follow an open, then free in close
  if (dirp->firstDentry != NULL) {
    free(dirp->firstDentry);
  }

  free(dirp);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_CLOSEDIR, tsIdx);
#endif
  return 0;
}

int fs_rmdir_internal(FsService *fsServ, const char *pathname) {
  struct shmipc_msg msg;
  struct rmdirOp *rmdOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  rmdOp = (struct rmdirOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_rmdirOp(&msg, rmdOp, pathname);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = rmdOp->ret;
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_rmdir(const char *pathname) {
  // return fs_rmdir_internal(gServMngPtr->primaryServ, pathname);
  // TODO (jingliu): implement a real rmdir here.
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_RMDIR);
#endif
  int rt = fs_unlink_internal(gServMngPtr->primaryServ, pathname);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_rmdir(%s) ret:%d\n", pathname, rt);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_RMDIR, tsIdx);
#endif
  return rt;
}

int fs_rename_internal(FsService *fsServ, const char *oldpath,
                       const char *newpath) {
  struct shmipc_msg msg;
  struct renameOp *rnmOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  rnmOp = (struct renameOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_renameOp(&msg, rnmOp, oldpath, newpath);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = rnmOp->ret;
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_rename(from:%s, to:%s) return %d\n", oldpath, newpath,
          ret);
#endif
  return ret;
}

int fs_rename(const char *oldpath, const char *newpath) {
#ifdef LDB_PRINT_CALL
  print_rename(oldpath, newpath);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_RENAME);
#endif
  int rt = fs_rename_internal(gServMngPtr->primaryServ, oldpath, newpath);
  if (rt < 0 && rt >= FS_REQ_ERROR_INODE_IN_TRANSFER) {
    errno = -rt;
    rt = -1;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_RENAME, tsIdx);
#endif
  return rt;
}

int fs_fsync_internal(FsService *fsServ, int fd, bool isDataSync) {
  struct shmipc_msg msg;
  struct fsyncOp *fsyncOp;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  fsyncOp = (struct fsyncOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_fsyncOp(&msg, fsyncOp, fd, isDataSync);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = fsyncOp->ret;
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

int fs_fsync(int fd) {
#ifdef LDB_PRINT_CALL
  print_fsync(fd);
#endif
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_FSYNC);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_fsync_internal(service, fd, false);
  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fsync(fd:%d) ret:%ld\n", fd, rc);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_FSYNC, tsIdx);
#endif
  return rc;
}

int fs_fdatasync(int fd) {
#ifdef LDB_PRINT_CALL
  print_fsync(fd);
#endif
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_FSYNC);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_fsync_internal(service, fd, true);
  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fdatasync(fd:%d) ret:%ld\n", fd, rc);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_FSYNC, tsIdx);
#endif
  return rc;
}

// template<bool UNLINK_ONLY> with -std=c++17, if constexpr can be used instead.
// For now, given the amount of reuse, I think we can let a single if statement
// check happen. Especially since syncall will do i/o.
// Once we move to c++17, this would resolve at compile time.
void generic_fs_syncall(bool UNLINK_ONLY) {
  using item_t = std::pair<FsService *, off_t>;
  // TODO: consider using a fixed size array if speed is an issue
  // Or maybe a custom stack allocator for the vector.
  // TODO: change signature to return success/error
  std::vector<item_t> async_ops;
  struct shmipc_msg msg;

  for (auto it : gServMngPtr->multiFsServMap) {
    if (!(it.second->inUse)) continue;

    auto serv = it.second;
    off_t ring_idx = shmipc_mgr_alloc_slot_dbg(serv->shmipc_mgr);
    memset(&msg, 0, sizeof(msg));
    if /*constexpr*/ (UNLINK_ONLY) {
      auto synop =
          (struct syncunlinkedOp *)IDX_TO_XREQ(serv->shmipc_mgr, ring_idx);
      prepare_syncunlinkedOp(&msg, synop);
    } else {
      auto synop = (struct syncallOp *)IDX_TO_XREQ(serv->shmipc_mgr, ring_idx);
      prepare_syncallOp(&msg, synop);
    }

    shmipc_mgr_put_msg_nowait(serv->shmipc_mgr, ring_idx, &msg);
    async_ops.emplace_back(serv, ring_idx);
  }

  for (auto &it : async_ops) {
    off_t ring_idx;
    FsService *serv;
    std::tie(serv, ring_idx) = it;

    memset(&msg, 0, sizeof(msg));
    shmipc_mgr_wait_msg(serv->shmipc_mgr, ring_idx, &msg);
    shmipc_mgr_dealloc_slot(serv->shmipc_mgr, ring_idx);
    if (msg.retval != 0) {
      if /*constexpr*/ (UNLINK_ONLY) {
        fprintf(stderr, "syncunlinked failed on a worker\n");
      } else {
        fprintf(stderr, "syncall failed on a worker\n");
      }
    }
  }
}

void fs_syncall() {
  // generic_fs_syncall</*UNLINK_ONLY*/false>();
  generic_fs_syncall(/*UNLINK_ONLY*/ false);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_syncall()\n");
#endif
}

void fs_syncunlinked() {
  // generic_fs_syncall</*UNLINK_ONLY*/true>();
  generic_fs_syncall(/*UNLINK_ONLY*/ true);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_syncunlinked()\n");
#endif
}

ssize_t fs_read_internal(FsService *fsServ, int fd, void *buf, size_t count) {
  ssize_t rc;
  /*
   * TODO change the code for read/write to resemble
   * while remaining > 0; do ....; done
   * That way we don't have to have an if and else condition.
   * and we keep decrementing by RING_DATA_ITEM_SIZE
   */

  if (count < RING_DATA_ITEM_SIZE) {
    struct shmipc_msg msg;
    struct readOpPacked *rop_p;
    off_t ring_idx;

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    rop_p = (struct readOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_readOp(&msg, rop_p, fd, count);
    shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

    // NOTE: FIXME This doesn't look right. If MIMIC_APP_ZC is defined, then we
    // don't copy into buf? That would mean that it is still in the ring
    // data region but can be overwritten by anyone after we dealloc the
    // slot.
#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
#endif

    rc = rop_p->rwOp.ret;
    // copy the data back to user
#ifndef MIMIC_APP_ZC
    if (rc > 0) {
      memcpy(buf, curDataPtr, count);
    }

    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
#endif
  } else {
    rc = -1;
  }
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_read(fd:%d count:%lu) ret:%ld\n", fd, count, rc);
#endif
  return rc;
}

ssize_t fs_read(int fd, void *buf, size_t count) {
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_READ);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_read_internal(service, fd, buf, count);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_READ, tsIdx);
#endif
  return rc;
}

static ssize_t fs_pread_internal(FsService *fsServ, int fd, void *buf,
                                 size_t count, off_t offset) {
  ssize_t rc;
#ifdef _CFS_LIB_PRINT_REQ_
  pid_t curPid = syscall(__NR_gettid);
  fprintf(stdout, "fs_pread(fd:%d, count:%ld, offset:%lu pid:%d)\n", fd, count,
          offset, curPid);
#endif
  if (count < RING_DATA_ITEM_SIZE) {
    struct shmipc_msg msg;
    struct preadOpPacked *prop_p;
    off_t ring_idx;

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    prop_p = (struct preadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_preadOp(&msg, prop_p, fd, count, offset);
    shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
#endif

    rc = prop_p->rwOp.ret;
    // copy the data back to user
#ifndef MIMIC_APP_ZC
    if (rc > 0) {
      memcpy(buf, curDataPtr, count);
    }
#endif
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  } else {
    // For now, this only support < RING_DATA_ITEM_SIZE
    rc = -1;
  }
  return rc;
}

ssize_t fs_pread(int fd, void *buf, size_t count, off_t offset) {
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PREAD);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_pread_internal(service, fd, buf, count, offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PREAD, tsIdx);
#endif

  return rc;
}

// bcause the regular file named by path or referenced by fd to be truncated
// to a size of precisely length bytes
int fs_ftruncate(int fd, off_t length) { return 0; }

// doAllocate() allows the caller to directly manipulate the allocated disk
// space for the file referred to  by  fd for the byte range starting at offset
// and continuing for len bytes
int fs_fallocate(int fd, int mode, off_t offset, off_t len) { return 0; }

static ssize_t fs_write_internal(FsService *fsServ, int fd, const void *buf,
                                 size_t count) {
  ssize_t rc;
  if (count < RING_DATA_ITEM_SIZE) {
    struct shmipc_msg msg;
    struct writeOpPacked *wop_p;
    off_t ring_idx;

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    wop_p = (struct writeOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_writeOp(&msg, wop_p, fd, count);

#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    memcpy(curDataPtr, buf, count);
#endif

    shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

    rc = wop_p->rwOp.ret;
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  } else {
    // NOTE: let's first issue the request one by one to see how bad we can be.
    rc = 0;
    size_t bytes = 0;
    size_t toWrite = count;
    int numNeedSlot = (count - 1) / (RING_DATA_ITEM_SIZE) + 1;
    //    fprintf(stdout, "fs_write, count:%ld numNeedSlot %d\n", count,
    //    numNeedSlot);
    for (int i = 0; i < numNeedSlot; i++) {
      struct shmipc_msg msg;
      struct writeOpPacked *wop_p;
      off_t ring_idx;

      int tmpBytes;
      if (toWrite > RING_DATA_ITEM_SIZE) {
        tmpBytes = RING_DATA_ITEM_SIZE;
      } else {
        tmpBytes = toWrite;
      }

      memset(&msg, 0, sizeof(struct shmipc_msg));
      ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
      wop_p = (struct writeOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
      prepare_writeOp(&msg, wop_p, fd, tmpBytes);

#ifndef MIMIC_APP_ZC
      void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
      memcpy(curDataPtr, ((char *)buf + bytes), tmpBytes);
#endif

      shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

      rc += wop_p->rwOp.ret;
      if (wop_p->rwOp.ret < 0) {
        shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
        return -1;
      }
      bytes += tmpBytes;
      toWrite -= tmpBytes;
      shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
    }
  }
#ifdef _CFS_LIB_PRINT_REQ_
  // fprintf(stdout, "fs_write(fd:%d) ret:%ld\n", fd, rc);
  // std::cout << "fs_write: tid:" << std::this_thread::get_id() << std::endl;
#endif
  return rc;
}

ssize_t fs_write(int fd, const void *buf, size_t count) {
  if (count == 0) return 0;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_WRITE);
#endif
  int wid = -1;
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_write_internal(service, fd, buf, count);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_WRITE, tsIdx);
#endif

  return rc;
}

static ssize_t fs_pwrite_internal(FsService *fsServ, int fd, const void *buf,
                                  size_t count, off_t offset) {
  ssize_t total_rc = 0;
  size_t bytes = 0;
  size_t toWrite = count;
  int numNeedSlot = (int)((count - 1) / (RING_DATA_ITEM_SIZE) + 1);

  for (int i = 0; i < numNeedSlot; i++) {
    struct shmipc_msg msg;
    struct pwriteOpPacked *pwop_p;
    off_t ring_idx;

    int tmpBytes;
    if (toWrite > RING_DATA_ITEM_SIZE) {
      tmpBytes = RING_DATA_ITEM_SIZE;
    } else {
      tmpBytes = toWrite;
    }

    memset(&msg, 0, sizeof(msg));
    ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
    pwop_p = (struct pwriteOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
    prepare_pwriteOp(&msg, pwop_p, fd, tmpBytes, offset);

#ifndef MIMIC_APP_ZC
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    memcpy(curDataPtr, ((char *)buf + bytes), tmpBytes);
#endif

    shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
    ssize_t rc = pwop_p->rwOp.ret;
    if (rc < 0) {
      shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
      return rc;
    }

    total_rc += rc;
    bytes += tmpBytes;
    toWrite -= tmpBytes;
    shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  }
  return total_rc;
}

ssize_t fs_pwrite(int fd, const void *buf, size_t count, off_t offset) {
  if (count == 0) return 0;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PWRITE);
#endif
  int wid = -1;
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_pwrite_internal(service, fd, buf, count, offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PWRITE, tsIdx);
#endif
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
// fs_allocated_xxx()

static ssize_t fs_allocated_read_internal(FsService *fsServ, int fd, void *buf,
                                          size_t count) {
  struct shmipc_msg msg;
  struct allocatedReadOpPacked *arop_p;
  struct allocatedReadOp arop;
  off_t ring_idx;
  ssize_t rc;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, false, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_read_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  arop_p =
      (struct allocatedReadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_allocatedReadOp(&msg, arop_p, fd, count);
  arop_p->alOp.shmid = shmid;
  arop_p->alOp.dataPtrId = dataPtrId;

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  unpack_allocatedReadOp(arop_p, &arop);
  rc = arop_p->rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "returned offset:%ld\n", arop.rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_read(int fd, void *buf, size_t count) {
#ifdef LDB_PRINT_CALL
  print_read(fd, buf, count);
#endif
  int wid = -1;
  if (count == 0) return 0;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_READ);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_read_internal(service, fd, buf, count);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_READ, tsIdx);
#endif
  return rc;
}

static ssize_t fs_allocated_pread_internal(FsService *fsServ, int fd, void *buf,
                                           size_t count, off_t offset) {
  struct shmipc_msg msg;
  struct allocatedPreadOpPacked *aprop_p;
  off_t ring_idx;
  ssize_t rc;
  struct allocatedPreadOp aprop;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, false, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_pread_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  } else {
#ifdef _CFS_LIB_DEBUG_
    dumpAllocatedOpCommon(&aprop_p->alOp);
#endif
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  aprop_p = (struct allocatedPreadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                         ring_idx);
  prepare_allocatedPreadOp(&msg, aprop_p, fd, count, offset);
  aprop_p->alOp.shmid = shmid;
  aprop_p->alOp.dataPtrId = dataPtrId;

  // reset sequential number
  aprop_p->alOp.perAppSeqNo = 0;
  aprop_p->rwOp.realCount = 0;

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  unpack_allocatedPreadOp(aprop_p, &aprop);
  rc = aprop.rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "returned offset:%ld\n", aprop_p->rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_pread(int fd, void *buf, size_t count, off_t offset) {
#ifdef LDB_PRINT_CALL
  print_pread(fd, buf, count, offset);
#endif
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);
    LeaseUnref(entry);
    fd = base_fd;
  }
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PREAD);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_pread_internal(service, fd, buf, count, offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PREAD, tsIdx);
#endif
  return rc;
}

static ssize_t fs_allocated_write_internal(FsService *fsServ, int fd, void *buf,
                                           size_t count) {
  struct shmipc_msg msg;
  struct allocatedWriteOpPacked *awop_p;
  off_t ring_idx;
  ssize_t rc;
  struct allocatedWriteOp awop;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, true, shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_write_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  awop_p = (struct allocatedWriteOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                        ring_idx);
  prepare_allocatedWriteOp(&msg, awop_p, fd, count);
  awop_p->alOp.shmid = shmid;
  awop_p->alOp.dataPtrId = dataPtrId;

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  unpack_allocatedWriteOp(awop_p, &awop);
  rc = awop.rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "flag:%u seqNo:%lu\n", awop.rwOp.flag, awop.alOp.perAppSeqNo);
  fprintf(stderr, "returned offset:%ld\n", awop.rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_write(int fd, void *buf, size_t count) {
#ifdef LDB_PRINT_CALL
  print_write(fd, buf, count);
#endif
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);

    ssize_t rc = -1;
    {
      std::unique_lock<std::shared_mutex> guard(entry->lock);
      int fd_offset = OpenLease::FindOffset(fd);
      LocalFileObj &file_object = entry->lease->GetFileObjects()[fd_offset];
      int offset = file_object.offset;

      rc = fs_allocated_pwrite(base_fd, buf, count, offset);
      if (rc >= 0) {
        size_t current_size = offset + rc;
        if (current_size > entry->lease->size)
          entry->lease->size = current_size;
        file_object.offset += rc;
      }
    }
    LeaseUnref(entry);
    return rc;
  }

#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_WRITE);
#endif
  int wid = -1;
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_write_internal(service, fd, buf, count);
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_allocated_write fd:%d count:%ld rc:%ld\n", fd, count, rc);
#endif
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }

#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_WRITE, tsIdx);
#endif
  return rc;
}

static ssize_t fs_allocated_pwrite_internal(FsService *fsServ, int fd,
                                            void *buf, size_t count,
                                            off_t offset) {
  struct shmipc_msg msg;
  struct allocatedPwriteOpPacked *apwop_p;
  off_t ring_idx;
  ssize_t rc;
  fslib_malloc_block_cnt_t dataPtrId;
  uint8_t shmid;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "first char:%c firstDataPtr:%c (dataPtr - shmStartPtr):%lu\n",
          *(static_cast<char *>(buf)),
          *(static_cast<char *>(threadMemBuf->firstDataPtr())),
          (static_cast<char *>(buf) -
           (reinterpret_cast<char *>(threadMemBuf->firstMetaPtr()))));
#endif

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo(buf, true, shmid, dataPtrId, err);
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "shmId:%u dataaPtrId:%u\n", shmid, dataPtrId);
#endif
  if (err) {
    fprintf(stderr, "fs_allocated_pwrite_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  apwop_p = (struct allocatedPwriteOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                          ring_idx);
  prepare_allocatedPwriteOp(&msg, apwop_p, fd, count, offset);
  apwop_p->alOp.shmid = shmid;
  apwop_p->alOp.dataPtrId = dataPtrId;

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  rc = apwop_p->rwOp.ret;

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_pwrite(int fd, void *buf, ssize_t count, off_t offset) {
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PWRITE);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_pwrite_internal(service, fd, buf, count, offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PWRITE, tsIdx);
#endif
  return rc;
}

#if 0
// used for rbtree
static int compare_file_offset(node n, void *leftp, void *rightp) {
  off_t left = (off_t)leftp;
  off_t right = (off_t)rightp;
  if (left < right)
    return -1;
  else if (left > right)
    return 1;
  else {
    assert(left == right);
    return 0;
  }
}
#endif

constexpr static int gRWLookupLocalErr_FdNotFound = -11;
constexpr static int gRWLookupLocalErr_NoData = -13;
constexpr static int gRWLookupLocalSuccess = 0;

static int fromFdToFileHandle(int fd, struct FileHandle **fhPtr) {
  gLibSharedContext->fdFileHandleMapLock.lock_read();
  auto fhIt = gLibSharedContext->fdFileHandleMap.find(fd);

  if (fhIt == gLibSharedContext->fdFileHandleMap.end()) {
    fprintf(stderr, "ERROR cannot find this fd's corresponding fileHandle\n");
    gLibSharedContext->fdFileHandleMapLock.unlock();
    return gRWLookupLocalErr_FdNotFound;
  }

  *fhPtr = fhIt->second;
  gLibSharedContext->fdFileHandleMapLock.unlock();
  return 0;
}

// lookup continuous file content in client cache
// REQUIRED: startOff is aligned to page size
// @param blkRcdVec: result will be saved to this array
//   - REQUIRED: len(buf_arr) == numPages
// @return 0 if success, or above error numbers
static int rwLookupLocalCache(
    struct FileHandle *fh, std::vector<struct BlockRecordOfFile *> &blkRcdVec,
    off_t startOff, int numPages) {
  assert(startOff % gFsLibMallocPageSize == 0);
  assert(blkRcdVec.size() == (uint)numPages);

  std::vector<struct BlockRecordOfFile *> &fhPageMap =
      gLibSharedContext->fhIdPageMap[fh->id];

  uint32_t max_offset = startOff / gFsLibMallocPageSize + numPages;
  if (fhPageMap.size() < max_offset + 1) {
    fhPageMap.resize(max_offset + 1, nullptr);
  }
  for (int i = 0; i < numPages; i++) {
    uint32_t block_offset = startOff / gFsLibMallocPageSize + i;
    BlockRecordOfFile *blk = fhPageMap[block_offset];
    if (blk == nullptr) {
      return gRWLookupLocalErr_NoData;
    } else {
      blkRcdVec[i] = blk;
    }
  }
  return gRWLookupLocalSuccess;
}

static int rwSaveDataToCache(struct FileHandle *fh, off_t alignedOff,
                             int numPages, void *buf,
                             FsLeaseCommon::rdtscmp_ts_t ts, bool isWrite) {
  if (gIsCCLeaseDbg)
    fprintf(stderr,
            "rwSaveDataToCache fh:%p alignedOff:%ld numPages:%d buf:%p\n", fh,
            alignedOff, numPages, buf);
  std::vector<struct BlockRecordOfFile *> &fhPageMap =
      gLibSharedContext->fhIdPageMap[fh->id];

  off_t curAlignedOff;
  uint32_t max_offset = numPages + alignedOff / gFsLibMallocPageSize;
  if (fhPageMap.size() < max_offset + 1) {
    fhPageMap.resize(max_offset + 1, nullptr);
  }
  for (int i = 0; i < numPages; i++) {
    uint32_t block_offset = alignedOff / gFsLibMallocPageSize + i;
    curAlignedOff = alignedOff + i * gFsLibMallocPageSize;
    BlockRecordOfFile *&blk = fhPageMap[block_offset];
    if (blk != nullptr) {
      fprintf(stderr, "ERROR curAlignedOffset:%ld is found in the cache\n",
              curAlignedOff);
      throw std::runtime_error("curAlignedOff cannot find in the cache");
    }
    auto blkRcdPtr = new BlockRecordOfFile();
    blkRcdPtr->alignedStartOffset = curAlignedOff;
    blkRcdPtr->addr = static_cast<char *>(buf) + i * gFsLibMallocPageSize;
    if (isWrite) blkRcdPtr->flag |= FSLIB_APP_BLK_RCD_FLAG_DIRTY;
    blk = blkRcdPtr;
  }
  return 0;
}

// @param count: size in bytes
static inline void computeAlignedPageNum(off_t startOffset, size_t count,
                                         off_t &alignedOffset, uint &numPages) {
  alignedOffset = (startOffset / gFsLibMallocPageSize) * gFsLibMallocPageSize;
  off_t endOffset = startOffset + count;
  numPages = 1 + (endOffset - alignedOffset - 1) / gFsLibMallocPageSize;
}

// @param offset: offset of this read/write.
//   if offset == kfsRwRenewLeaseOffsetRenewOnly, then it is a pure renew op,
//   will not put *buf*, *count*, *offset*, into the *packed_op*
//   REQUIRED: in this case, buf should be set to nullptr, count arg as 1
// @param ts: if set to > 0, then it indicates that lease granted
// @return return of the R/W data operation
//   NOTE: this must be set either for (p)read/(p)write, since once cache is
//   used, it might be that offset maintenance is not part of the FSP server job
//   I.e., whether App itself calls cached_read() or cached_pread(), FSP will
//   only regard it as pread()
constexpr static off_t kfsRwRenewLeaseOffsetRenewOnly = -1;
template <class T1, class T2>
static ssize_t fs_rw_renew_lease(FsService *fsServ, int fd, void *buf,
                                 void *buf_head, size_t count, off_t offset,
                                 FsLeaseCommon::rdtscmp_ts_t &ts,
                                 void (*set_lease_flag_func)(T2 *),
                                 void (*prep_op_fun)(struct shmipc_msg *, T2 *,
                                                     int, size_t, off_t),
                                 void (*unpack_func)(T2 *, T1 *)) {
  T2 *top_packed;

  struct shmipc_msg msg;
  off_t ring_idx;
  ssize_t rc;

  if (gIsCCLeaseDbg)
    fprintf(stderr, "fs_rw_new_lease: fd:%d buf:%p count:%lu offset:%ld\n", fd,
            buf, count, offset);

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  top_packed =
      reinterpret_cast<T2 *>(IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx));
  prep_op_fun(&msg, top_packed, fd, count, offset);

  set_lease_flag_func(top_packed);

  if (offset != kfsRwRenewLeaseOffsetRenewOnly) {
    assert(buf != nullptr);
    int err = 0;
    threadMemBuf->getBufOwnerInfo(buf_head, false, top_packed->alOp.shmid,
                                  top_packed->alOp.dataPtrId, err);
    long diff = (char *)(buf) - (char *)(buf_head);
    assert(diff >= 0);
    if (diff > 0) {
      top_packed->rwOp.realCount = diff;
    } else {
      top_packed->rwOp.realCount = 0;
    }
    if (err) {
      fprintf(stderr, "fs_rw_renew_lease: Error in getBufOwnerInfo\n");
    } else {
#ifdef _CFS_LIB_DEBUG_
      dumpAllocatedOpCommon(&top_packed->alOp);
#endif
    }
  } else {
    setLeaseOpRenewOnly(top_packed);
    // NOTE: if buf == nullptr, it is a renew only request
    assert(count == 0);
    assert(buf == nullptr);
  }

  // send request
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  T1 top;
  unpack_func(top_packed, &top);
  rc = top_packed->rwOp.ret;

  bool isWidUpdated = checkUpdateFdWid(static_cast<int>(rc), fd);

  if (!isWidUpdated) {
    if (isLeaseHeld(&top.rwOp)) {
      ts = getLeaseTermTsFromRwOp(&top.rwOp);
      if (gIsCCLeaseDbg) fprintf(stderr, "lease held ts is set to:%lu\n", ts);
    } else {
      // TODO (jingliu): what should we do here if lease cannot be granted?
      // Option-1: Let return the corresponding error to the caller
      // Option-2: We fall back to fs_allocated_read()?
      if (gIsCCLeaseDbg)
        fprintf(stderr, "lease not held, set ret to:%d\n",
                (FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE));
      ts = 0;
      rc = FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE;
    }
  }

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);

  return rc;
}

// NOTE: if offset < 0, we think it comes from fs_cached_read(), and will
// try to find the offset in *FsLib*
ssize_t fs_cached_pread(int fd, void **bufAddr, size_t count, off_t offset) {
  assert((*bufAddr) == nullptr);
  off_t curOffset = offset;
  // this buf is supposed to be aligned to page size and is exactly the starting
  // byte addr
  // void *buf = *(bufAddr);
  void *buf = nullptr;
  if (offset < 0) {
    // this comes from a read() request, set the offset via saved offset
    auto it = gLibSharedContext->fdCurOffMap.find(fd);
    assert(it != gLibSharedContext->fdCurOffMap.end());
    curOffset = it->second;
    if (gIsCCLeaseDbg)
      fprintf(
          stderr,
          "fs_cached_pread() count:%ld offset:%ld curOffset is set to:%ld\n",
          count, offset, curOffset);
  }

  off_t alignedOffset;
  uint numPages;
  computeAlignedPageNum(curOffset, count, alignedOffset, numPages);
  if (gIsCCLeaseDbg)
    fprintf(stderr, "alignedOffset:%ld numPages:%u\n", alignedOffset, numPages);

  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  if (widIt == gLibSharedContext->fdWidMap.end()) {
    return -1;
  }

  struct FileHandle *fhPtr = nullptr;
  int rc = fromFdToFileHandle(fd, &fhPtr);
  if (rc == gRWLookupLocalErr_FdNotFound) {
    // if fd can not be found, there is something seriously wrong
    throw std::runtime_error("ccache lookup. cannot find fd\n");
  }

  // try to lookup in local cache
  auto blkRcdVec = std::vector<struct BlockRecordOfFile *>(numPages);
  rc = rwLookupLocalCache(fhPtr, blkRcdVec, alignedOffset, numPages);
  int retnr = -1;
  switch (rc) {
    case gRWLookupLocalErr_NoData:
      // ccache miss, need to fetch the data from server
      FsLeaseCommon::rdtscmp_ts_t ts;
      buf = fs_malloc(count);
      if (gIsCCLeaseDbg) {
        fprintf(stderr,
                "cache miss, need to fetch data from the server, allocated "
                "addr:%p count:%ld\n",
                buf, count);
      }
    CACHED_PREAD_RW_NODATA_WID_UPDATE_RETRY:
      retnr = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
          gServMngPtr->multiFsServMap[widIt->second], fd, buf, buf,
          numPages * gFsLibMallocPageSize, alignedOffset, ts, setLeaseOpForRead,
          prepare_allocatedPreadOp, unpack_allocatedPreadOp);
      if (gIsCCLeaseDbg)
        fprintf(
            stderr,
            "fs_rw_renew_lease get retnr:%d bufPtr:%p count:%lu firstChar:%c\n",
            retnr, buf, count, static_cast<char *>(buf)[0]);
      if (retnr == FS_REQ_ERROR_INODE_REDIRECT) {
        fprintf(stdout, "FS_REQ_ERROR_INODE_REDIRECT retry\n");
        widIt = gLibSharedContext->fdWidMap.find(fd);
        goto CACHED_PREAD_RW_NODATA_WID_UPDATE_RETRY;
      }
      if (retnr >= (int64_t)count) {
        rwSaveDataToCache(fhPtr, alignedOffset, numPages, buf, ts,
                          /*isWrite*/ false);
      }

      // retnr comes from the *fs_rw_renew_lease* which is page aligned in terms
      // of R/W uints
      if (retnr > (ssize_t)count) {
        retnr = count;
      }

      // set the valid data addrs
      if (retnr >= 0) {
        gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
        *(bufAddr) = static_cast<char *>(buf) + (curOffset - alignedOffset);
      }
      break;
    case gRWLookupLocalSuccess:
      // ccache hit
      // first check the lease
      if (gLibSharedContext->leaseMng_.ifFileLeaseValid(fhPtr->id)) {
        if (gIsCCLeaseDbg) fprintf(stderr, "LookupSucceed: lease is valid\n");
        // we can use ccache without contacting the server
        off_t offDiff = curOffset - alignedOffset;
        buf = static_cast<void *>((static_cast<char *>(blkRcdVec[0]->addr)));
        if (gIsCCLeaseDbg)
          fprintf(stderr, "buf addr:%p firstChar:%c\n", buf,
                  static_cast<char *>(buf)[0]);
        *(bufAddr) = static_cast<char *>(buf) + offDiff;
        retnr = count;
      } else {
        if (gIsCCLeaseDbg) {
          fprintf(stderr, "LookupSucceed: lease is not valid, need to renew\n");
        }
        // lease expired, need to renew the lease first
        FsLeaseCommon::rdtscmp_ts_t ts = 0;
      CACHED_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY:
        int lrnrt = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
            gServMngPtr->multiFsServMap[widIt->second], fd, /*buf*/ nullptr,
            nullptr,
            /*count*/ 0, kfsRwRenewLeaseOffsetRenewOnly, ts, setLeaseOpForRead,
            prepare_allocatedPreadOp, unpack_allocatedPreadOp);
        gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
        if (lrnrt != 0) {
          if (gIsCCLeaseDbg) fprintf(stderr, "lease renew fail\n");
          if (lrnrt == FS_REQ_ERROR_INODE_REDIRECT) {
            fprintf(stdout, "FS_REQ_ERROR_INODE_REDIRECT retry\n");
            widIt = gLibSharedContext->fdWidMap.find(fd);
            goto CACHED_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
          }
          retnr = (FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE);
          // TODO (jingliu): once fail to extend lease, need to clean up local
          // cached data, and let App knows to fail-back to fs_allocated_r/w
        } else {
          // lease renew success
          off_t offDiff = curOffset - alignedOffset;
          buf = static_cast<void *>((static_cast<char *>(blkRcdVec[0]->addr)));
          if (gIsCCLeaseDbg)
            fprintf(stderr,
                    "retrieve data from ccache offDiff:%ldf firstChar:%c\n",
                    offDiff, static_cast<char *>(buf)[0]);
          *(bufAddr) = static_cast<char *>(buf) + offDiff;
          retnr = count;
        }
      }
      break;
    default:
      fprintf(stderr, "ERROR return of lookup is not supported\n");
  }
  return retnr;
}

ssize_t fs_cached_read(int fd, void **bufAddr, size_t count) {
  ssize_t rc = fs_cached_pread(fd, bufAddr, count, /*offset*/ -2);
  // update offset
  gLibSharedContext->fdCurOffMap[fd] += count;
  return rc;
}

ssize_t fs_cached_posix_pread(int fd, void *buf, size_t count, off_t offset) {
  void *tmpBuf = nullptr;
  ssize_t ret = fs_cached_pread(fd, &tmpBuf, count, offset);
  assert(ret > 0);
  if (gIsCCLeaseDbg) {
    fprintf(stderr, "fs_cached_pread return tmpBuf is set to:%p\n", tmpBuf);
  }
  memcpy(buf, tmpBuf, count);
  return ret;
}

static constexpr int k_cpc_page_size = 4096;

// #define CPC_DEBUG_PRINT
// #define CPC_HIT_RATIO_REPORT

#ifdef CPC_HIT_RATIO_REPORT
static constexpr int k_cpc_num_report_thr = 100000;
thread_local int cpc_num_hit = 0;
thread_local int cpc_num_op = 0;
#endif

ssize_t fs_cpc_pread(int fd, void *buf, size_t count, off_t offset) {
#ifdef CPC_DEBUG_PRINT
  fprintf(stderr, "fs_cpc_pread: fd:%d count:%lu off_t:%lu\n", fd, count,
          offset);
#endif
#ifdef CPC_HIT_RATIO_REPORT
  cpc_num_op++;
  if (cpc_num_op > k_cpc_num_report_thr) {
    fprintf(stderr, "tid:%d num_op_done:%d num_hit:%d hit_ratio:%f\n",
            threadFsTid, cpc_num_op, cpc_num_hit,
            float(cpc_num_hit) / (cpc_num_op));
    cpc_num_op = 0;
    cpc_num_hit = 0;
  }
#endif

  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);
    LeaseUnref(entry);
    fd = base_fd;
  }

  struct FileHandle *fhPtr = nullptr;
  fromFdToFileHandle(fd, &fhPtr);
  assert(fhPtr != nullptr);
  if (count == 0) return 0;
  // lookup in the local page cache
  // std::unordered_map<off_t, BlockRecordOfFile *> &fhPageMap =
  //     gLibSharedContext->fhIdPageMap[fhPtr->id];
  std::vector<BlockRecordOfFile *> &fhPageMap =
      gLibSharedContext->fhIdPageMap[fhPtr->id];

  off_t aligned_start_off = (offset / k_cpc_page_size) * k_cpc_page_size;
  size_t count_from_aligned_off = count;
  if (aligned_start_off < offset)
    count_from_aligned_off += (offset - aligned_start_off);

  uint64_t off = offset, tot;
  uint32_t m;
  char *dst = (char *)buf;
  bool success = true;
  fhPtr->lock_.lock_read();
  uint32_t page_offset = (count + offset) / k_cpc_page_size;
  if (fhPageMap.size() < page_offset + 1) {
    fhPageMap.resize(page_offset + 1, nullptr);
  } else {
    for (tot = 0; tot < count; tot += m, off += m, dst += m) {
      uint32_t block_idx = off / k_cpc_page_size;
      m = std::min(count - tot, k_cpc_page_size - off % k_cpc_page_size);
      auto block = fhPageMap[block_idx];
      if (block == nullptr) {
        success = false;
        break;
      }
      // if any page except the last one is not full, the lookup fail
      if (block->count < k_cpc_page_size && m + tot != count) {
        success = false;
        break;
      }
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr,
              "find: off:%lu aligned_off:%lu count:%lu count- tot:%lu\n", off,
              cur_aligned_off, it->second->count, it->second->count - tot);
#endif
      memcpy(dst, (char *)(block->addr) + off % k_cpc_page_size, m);
    }
  }
  fhPtr->lock_.unlock();
  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  assert(widIt != gLibSharedContext->fdWidMap.end());
  int retnr = -1;
  if (success && tot == count) {
    // see if lease expire
    if (gLibSharedContext->leaseMng_.ifFileLeaseValid(fhPtr->id)) {
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr, "cache hit fd:%d off:%ld\n", fd, offset);
#endif
#ifdef CPC_HIT_RATIO_REPORT
      cpc_num_hit++;
#endif
      return count;
    } else {
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr, "renew only fd:%d off:%ld\n", fd, offset);
#endif
      // renew lease
    CPC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY:
      FsLeaseCommon::rdtscmp_ts_t ts = 0;
      int lrnrt = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
          gServMngPtr->multiFsServMap[widIt->second], fd, /*buf*/ nullptr,
          nullptr,
          /*count*/ 0, kfsRwRenewLeaseOffsetRenewOnly, ts, setLeaseOpForRead,
          prepare_allocatedPreadOp, unpack_allocatedPreadOp);
      gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
      if (lrnrt != 0) {
        // lease renew fail
        if (lrnrt == FS_REQ_ERROR_INODE_REDIRECT) {
          widIt = gLibSharedContext->fdWidMap.find(fd);
          goto CPC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
        }
        // TODO (jingliu)
        throw std::runtime_error("lease renew fail");
      } else {
        // lease renew succeed
        return count;
      }
    }
  } else {
    // fetch data from the server
#ifdef CPC_DEBUG_PRINT
    fprintf(stderr, "fetch data from the server\n");
#endif
    FsLeaseCommon::rdtscmp_ts_t ts;
  CPC_PREAD_RW_NODATA_WID_UPDATE_RETRY:
    retnr = fs_rw_renew_lease<allocatedPreadOp, allocatedPreadOpPacked>(
        gServMngPtr->multiFsServMap[widIt->second], fd,
        (char *)buf - (offset - aligned_start_off),
        (char *)buf - k_cpc_page_size, count_from_aligned_off,
        aligned_start_off, ts, setLeaseOpForRead, prepare_allocatedPreadOp,
        unpack_allocatedPreadOp);
    if (retnr > 0) {
#ifdef CPC_DEBUG_PRINT
      fprintf(stderr, "save data to cache: fd:%d off:%ld\n", fd, offset);
#endif
      // save the data to local page cache
      uint64_t off = aligned_start_off, tot;
      uint32_t m;
      fhPtr->lock_.lock();
      gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
      char *src = (char *)buf - (offset - aligned_start_off);
      for (tot = 0; tot < count_from_aligned_off;
           tot += m, off += m, src += m) {
        uint32_t block_idx = off / k_cpc_page_size;
        assert(off % k_cpc_page_size == 0);
        m = std::min(count_from_aligned_off - tot,
                     k_cpc_page_size - off % k_cpc_page_size);
        // uint64_t cur_aligned_off = (uint64_t)block_idx * k_cpc_page_size;
        // BlockRecordOfFile *&blk = fhPageMap[cur_aligned_off];
        BlockRecordOfFile *&blk = fhPageMap[block_idx];
        if (blk == nullptr) {
          auto cur_blk = new BlockRecordOfFile();
          blk = cur_blk;
          blk->alignedStartOffset = off;
          blk->addr = scalable_aligned_malloc(k_cpc_page_size, k_cpc_page_size);
          // blk->addr = malloc(k_cpc_page_size);
          // assert(blk->addr != nullptr);
          if (blk->addr == nullptr) {
            fprintf(stderr, "cannot allocate memory\n");
            fhPtr->lock_.unlock();
            goto CPC_ERROR;
          }
        }
        if (m > blk->count) blk->count = m;
#ifdef CPC_DEBUG_PRINT
        fprintf(stderr, "--off:%ld m:%u count:%ld\n", off, m, blk->count);
#endif
        memcpy(blk->addr, src, m);
      }
      fhPtr->lock_.unlock();
      return retnr - (offset - aligned_start_off);
    } else if (retnr == 0) {
      return 0;
    } else {
      // printf("cpc retval: %d\n", retnr);
      if (handle_inode_in_transfer(retnr))
        goto CPC_PREAD_RW_NODATA_WID_UPDATE_RETRY;
      if (checkUpdateFdWid(retnr, fd))
        goto CPC_PREAD_RW_NODATA_WID_UPDATE_RETRY;
    }
  }
CPC_ERROR:
  return -1;
}

ssize_t fs_uc_read(int fd, void *buf, size_t count) {
  auto it = gLibSharedContext->fdOffsetMap.find(fd);
  assert(it != gLibSharedContext->fdOffsetMap.end());
  off_t off = it->second;
  ssize_t nread = fs_uc_pread(fd, buf, count, off);
  if (nread >= 0) {
    gLibSharedContext->fdOffsetMap[fd] = off + nread;
  }
  return nread;
}

// static const uint64_t kUcOpByteMax =
//     ((RING_DATA_ITEM_SIZE) / (CACHE_LINE_BYTE)) * (PAGE_CACHE_PAGE_SIZE);

void updateRcTsForLease(rwOpCommon *op, FsLeaseCommon::rdtscmp_ts_t &ts,
                        ssize_t &rc) {
  if (isLeaseHeld(op)) {
    ts = getLeaseTermTsFromRwOp(op);
  } else {
    ts = 0;
    rc = FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE;
  }
}

ssize_t fs_uc_pread_renewonly_internal(FsService *fsServ, int fd,
                                       FsLeaseCommon::rdtscmp_ts_t &ts) {
  struct shmipc_msg msg;
  struct preadOpPacked *prop_p;
  struct preadOp prop;
  ssize_t rc;
  off_t ring_idx;
  memset(&msg, 0, sizeof(msg));

  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  prop_p = (struct preadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_preadOpForUC(&msg, prop_p, fd, 0, 0);
  setLeaseOpRenewOnly(prop_p);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  unpack_preadOp(prop_p, &prop);
  rc = prop.rwOp.ret;

  updateRcTsForLease(&prop.rwOp, ts, rc);

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_uc_pread_internal(FsService *fsServ, int fd, void *buf,
                             size_t orig_count, off_t orig_off,
                             size_t count_from_aligned, off_t offset_aligned,
                             FileHandle *fh, FsLeaseCommon::rdtscmp_ts_t &ts) {
  // fprintf(stdout, "fs_uc_pread_internal\n");
  // assert(count_from_aligned < kUcOpByteMax);
  struct shmipc_msg msg;
  struct preadOpPacked *prop_p;
  struct preadOp prop;
  ssize_t rc;
  off_t ring_idx;
  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  prop_p = (struct preadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);

  prepare_preadOpForUC(&msg, prop_p, fd, count_from_aligned, offset_aligned);
  setLeaseOpForReadUC<preadOpPacked>(prop_p);

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  unpack_preadOp(prop_p, &prop);
  rc = prop.rwOp.ret;
  if (rc > 0) {
    // record pages to cacheHelper
    void *curDataPtr = (void *)IDX_TO_DATA(fsServ->shmipc_mgr, ring_idx);
    PageDescriptorMsg *pdmsg =
        reinterpret_cast<PageDescriptorMsg *>(curDataPtr);
    PageDescriptorMsg *curmsg;
    uint64_t off = orig_off, tot;
    uint32_t m;
    fh->lock_.lock();
    char *dstPtr = static_cast<char *>(buf);
    int pdmsgIdx = 0;
    for (tot = 0; tot < orig_count;
         tot += m, off += m, dstPtr += m, pdmsgIdx++) {
      uint32_t pageIdx = off / (PAGE_CACHE_PAGE_SIZE);
      curmsg = pdmsg + pdmsgIdx;
      m = std::min(orig_count - tot,
                   (PAGE_CACHE_PAGE_SIZE)-off % (PAGE_CACHE_PAGE_SIZE));
      assert(checkPdMsgMAGIC(curmsg));
      assert(curmsg->pageIdxWithinIno == pageIdx);
      // fprintf(stdout, "off:%lu curmsg->gPageIdx:%u\n", off,
      // curmsg->gPageIdx);
      auto curpair =
          gLibSharedContext->pageCacheHelper.findStablePage(curmsg->gPageIdx);
      // copy the data to destination
      memcpy(dstPtr,
             static_cast<char *>(curpair.second) + off % (PAGE_CACHE_PAGE_SIZE),
             m);
      // install the cache pages
      fh->cacheReader.installCachePage(pageIdx * (PAGE_CACHE_PAGE_SIZE),
                                       curpair);
    }
    fh->lock_.unlock();
    rc = rc - (orig_off - offset_aligned);
  }
  // extract ts
  updateRcTsForLease(&prop.rwOp, ts, rc);

  // process returned list of pages
  // now it's safe to dealloc this shm slot
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_uc_pread(int fd, void *buf, size_t count, off_t offset) {
  struct FileHandle *fhPtr = nullptr;
  fromFdToFileHandle(fd, &fhPtr);
  assert(fhPtr != nullptr);
  if (count == 0) return 0;
#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_uc_pread count:%lu offset:%lu\n", count, offset);
#endif
  // lookup page cache
  off_t aligned_start_off =
      (offset / PAGE_CACHE_PAGE_SIZE) * (PAGE_CACHE_PAGE_SIZE);
  size_t count_from_aligned_off = count;
  if (aligned_start_off < offset)
    count_from_aligned_off += (offset - aligned_start_off);
  fhPtr->lock_.lock_read();
  bool success = fhPtr->cacheReader.lookupRange(offset, count, buf);
  fhPtr->lock_.unlock();
  // deal with lease
  auto widIt = gLibSharedContext->fdWidMap.find(fd);
  assert(widIt != gLibSharedContext->fdWidMap.end());
  int retnr = -1;
  int wid = -1;
  if (success) {
    if (gLibSharedContext->leaseMng_.ifFileLeaseValid(fhPtr->id)) {
      // fprintf(stdout, "cacheHit\n");
      return count;
    } else {
      // fprintf(stdout, "only need to renew\n");
      // renew lease
      FsLeaseCommon::rdtscmp_ts_t ts = 0;
    UC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY:
      auto service = getFsServiceForFD(fd, wid);
      int lrnrt = fs_uc_pread_renewonly_internal(service, fd, ts);
      if (handle_inode_in_transfer(lrnrt))
        goto UC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
      bool should_retry = checkUpdateFdWid(static_cast<int>(lrnrt), fd);
      if (should_retry) goto UC_PREAD_RW_LEASERENEW_WID_UPDATE_RETRY;
      if (lrnrt < 0) {
        throw std::runtime_error("cannot renew lease");
      } else {
        gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
        return count;
      }
    }
  } else {
    // fprintf(stdout, "fetch data from server\n");
    // fetch data from the server
    FsLeaseCommon::rdtscmp_ts_t ts = 0;
  UC_PREAD_DATA_FETCH_RETRY:
    auto service = getFsServiceForFD(fd, wid);
    retnr = fs_uc_pread_internal(service, fd, buf, count, offset,
                                 count_from_aligned_off, aligned_start_off,
                                 fhPtr, ts);
    if (handle_inode_in_transfer(retnr)) goto UC_PREAD_DATA_FETCH_RETRY;
    bool should_retry = checkUpdateFdWid(retnr, fd);
    if (should_retry) goto UC_PREAD_DATA_FETCH_RETRY;
    gLibSharedContext->leaseMng_.updateFileTs(fhPtr->id, ts);
    return retnr;
  }
  return 0;
}

off_t fs_lseek_internal(FsService *fsServ, int fd, long int offset,
                        int whence) {
  struct shmipc_msg msg;
  struct lseekOp *op;
  off_t ring_idx;
  int ret;

  memset(&msg, 0, sizeof(msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  op = (struct lseekOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);
  prepare_lseekOp(&msg, op, fd, offset, whence);
  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = op->ret;

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fs_lseek(fd:%d) ret:%d\n", fd, ret);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return ret;
}

off_t fs_lseek(int fd, long int offset, int whence) {
  if (OpenLease::IsLocalFd(fd)) {
    int base_fd = OpenLease::FindBaseFd(fd);
    OpenLeaseMapEntry *entry = LeaseRef(base_fd);
    // we are currently not handling revoked lease case
    assert(entry);
    int fd_offset = OpenLease::FindOffset(fd);
    {
      std::unique_lock<std::shared_mutex> guard(entry->lock);
      LocalFileObj &file_object = entry->lease->GetFileObjects()[fd_offset];
      struct stat *stat_buffer = nullptr;
      int stat_ret = -1;
      switch (whence) {
        case SEEK_SET:
          file_object.offset = offset;
          break;
        case SEEK_CUR:
          file_object.offset += offset;
          break;
        case SEEK_END:
          stat_buffer = new struct stat;
          stat_ret = fs_stat(entry->lease->GetPath().c_str(), stat_buffer);
          assert(stat_ret == 0);
          entry->lease->size = stat_buffer->st_size;
          file_object.offset = entry->lease->size + offset;
          delete stat_buffer;
          break;
        default:
          // not supporting SEEK_DATA & SEEK_HOLE
          assert(false);
      }
    }
    LeaseUnref(entry);
    return 0;
  }

retry:
  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);
  int rc = fs_lseek_internal(service, fd, offset, whence);

  if (rc >= 0) return rc;
  if (handle_inode_in_transfer(rc)) goto retry;

  bool should_retry = checkUpdateFdWid(rc, fd);
  if (should_retry) goto retry;
  return rc;
}

#ifdef FS_LIB_SPPG
void init_global_spdk_env() {
  gSpdkEnvPtr.reset(new SpdkEnvWrapper());
  spdk_env_opts_init(&gSpdkEnvPtr->opts);
  gSpdkEnvPtr->opts.name = "spdkclient";
  gSpdkEnvPtr->opts.shm_id = SPDK_HUGEPAGE_GLOBAL_SHMID;
  if (spdk_env_init(&(gSpdkEnvPtr->opts)) < 0) {
    fprintf(stderr, "cannot init client pinned mem\n");
    exit(1);
  }
}

void check_init_mem() {
  assert(gServMngPtr != nullptr);
  std::call_once(gSpdkEnvFlag, init_global_spdk_env);
  if (!tlPinnedMemPtr) {
    tlPinnedMemPtr.reset(new FsLibPinnedMemMng());
    tlPinnedMemPtr->memPtr = spdk_dma_zmalloc(kLocalPinnedMemSize, 4096, NULL);
  }
}

ssize_t fs_sppg_cpc_pread(int fd, void *buf, size_t count, off_t offset) {
  check_init_mem();
  fprintf(stderr, "memPtr:%p\n", tlPinnedMemPtr->memPtr);
  return 0;
}
#endif

////////////////////////////////////////////////////////////////////////////////

void *fs_malloc_pad(size_t size) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready();
  void *ptr = threadMemBuf->Malloc(size + k_cpc_page_size, err);
  // fprintf(stderr, "fs_malloc_pad sz:%ld ret:%p ret_pad:%p\n", size, ptr,
  // (void*)((char*)ptr + k_cpc_page_size));
  return (char *)ptr + k_cpc_page_size;
}

void fs_free_pad(void *ptr, int fsTid) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready(fsTid);
  // fprintf(stderr, "fs_free_pad ptr:%p ptr_orig:%p\n", ptr, (char*)ptr -
  // k_cpc_page_size);
  threadMemBuf->Free((char *)ptr - k_cpc_page_size, err);
}

void fs_free_pad(void *ptr) { fs_free_pad(ptr, threadFsTid); }

void fs_init_thread_local_mem() { check_app_thread_mem_buf_ready(); }

void *fs_malloc(size_t size) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready();
  void *ptr = threadMemBuf->Malloc(size + k_cpc_page_size, err);
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "malloc_threadMemBuf %p tid:%d return ptr:%p\n", threadMemBuf,
          threadFsTid, ptr);
#endif
  // fprintf(stderr, "fs_malloc: return:%p tid:%d\n", ptr, threadFsTid);
  return ptr;
}

void *fs_zalloc(size_t size) {
  int err;
  auto threadMemBuf = check_app_thread_mem_buf_ready();
  void *ptr = threadMemBuf->Zalloc(size, err);
  return ptr;
}

void fs_free(void *ptr, int fsTid) {
  int err = 0;
  auto threadMemBuf = check_app_thread_mem_buf_ready(fsTid);
#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "free_threadMemBuf %p tid:%d ptr:%p firstDataPtr:%p\n",
          threadMemBuf, fsTid, ptr, threadMemBuf->firstDataPtr());
#endif
  // fprintf(stderr, "fs_free:%p tid:%d\n", ptr, threadFsTid);
  threadMemBuf->Free(ptr, err);
  if (err) fprintf(stderr, "free error: %d\n", err);
}

void fs_free(void *ptr) { fs_free(ptr, threadFsTid); }

/////////// ldb specific /////

int fs_open_ldb(const char *path, int flags, mode_t mode) {
  int delixArr[32];
  int dummy;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_OPEN);
#endif
#ifdef LDB_PRINT_CALL
  print_open(path, flags, mode, true);
#endif
  char *standardPath = filepath2TokensStandardized(path, delixArr, dummy);

  // NOTE: the first time invoking this is going to be extremely costly, ~0.2s
  // So, it is important for any user (thread) to invoke this
  //(void) check_app_thread_mem_buf_ready();
  assert(threadFsTid != 0);

retry:
  int wid = -1;
  auto service = getFsServiceForPath(standardPath, wid);
  size_t file_size;
  int rc = fs_open_internal(service, standardPath, flags, mode, &file_size);

  if (rc > 0) {
    gLibSharedContext->fdWidMap[rc] = wid;
    goto free_and_return;
  } else if (rc > FS_REQ_ERROR_INODE_IN_TRANSFER) {
    goto free_and_return;
  }

  // migration related errors
  if (handle_inode_in_transfer(rc)) goto retry;

  wid = getWidFromReturnCode(rc);
  if (wid >= 0) {
    updatePathWidMap(wid, standardPath);
    goto retry;
  }

free_and_return:
  if (rc > 0) {
    LDBLease *lease = nullptr;
    std::string path_string(standardPath);
    auto it = gLibSharedContext->pathLDBLeaseMap.find(path_string);
    if (it == gLibSharedContext->pathLDBLeaseMap.end()) {
      lease = new LDBLease(file_size);
      gLibSharedContext->pathLDBLeaseMap.emplace(path_string, lease);
    } else {
      lease = it->second;
    }
    gLibSharedContext->fdLDBLeaseMap.emplace(rc, lease);
  }

  free(standardPath);
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_OPEN, tsIdx);
#endif
  return rc;
}

int fs_open_ldb2(const char *path, int flags) {
  return fs_open_ldb(path, flags, 0);
}

static ssize_t fs_allocated_pread_internal_ldb(FsService *fsServ, int fd,
                                               void *buf, size_t count,
                                               off_t offset,
                                               uint32_t mem_offset) {
  struct shmipc_msg msg;
  struct allocatedPreadOpPacked *aprop_p;
  off_t ring_idx;
  ssize_t rc;
  struct allocatedPreadOp aprop;
  uint8_t shmid;
  fslib_malloc_block_cnt_t dataPtrId;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  // fill the shmName and dataPtr offset
  int err = 0;
  threadMemBuf->getBufOwnerInfo((char *)buf - gFsLibMallocPageSize, false,
                                shmid, dataPtrId, err);
  if (err) {
    fprintf(stderr, "fs_allocated_pread_internal: Error in getBufOwnerInfo\n");
    return -1;  // TODO set errno
  } else {
#ifdef _CFS_LIB_DEBUG_
    dumpAllocatedOpCommon(&aprop_p->alOp);
#endif
  }

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  aprop_p = (struct allocatedPreadOpPacked *)IDX_TO_XREQ(fsServ->shmipc_mgr,
                                                         ring_idx);
  prepare_allocatedPreadOp(&msg, aprop_p, fd, count, offset);
  aprop_p->alOp.shmid = shmid;
  aprop_p->alOp.dataPtrId = dataPtrId;

  // reset sequential number
  aprop_p->alOp.perAppSeqNo = 0;
  aprop_p->rwOp.realCount = mem_offset;

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);
  unpack_allocatedPreadOp(aprop_p, &aprop);
  rc = aprop.rwOp.ret;

#ifdef _CFS_LIB_DEBUG_
  fprintf(stderr, "returned offset:%ld\n", aprop_p->rwOp.realOffset);
#endif

  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  return rc;
}

ssize_t fs_allocated_pread_ldb(int fd, void *buf, size_t count, off_t offset) {
#ifdef LDB_PRINT_CALL
  print_pread(fd, buf, count, offset, true);
#endif

  auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
  if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
    fprintf(stderr, "LDBLease not found by pread %d\n", fd);
    throw std::runtime_error("LDBLease not found by pread");
  }

  LDBLease *lease = it->second;
  size_t new_count = 0;
  off_t new_offset = 0;
  int num_hit = 0;
  bool success =
      lease->Read(buf, count, offset, &new_count, &new_offset, &num_hit);
  if (success) {
    return count;
  }
  uint32_t mem_offset = gFsLibMallocPageSize - offset % gFsLibMallocPageSize +
                        gFsLibMallocPageSize * num_hit;

  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_PREAD);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_allocated_pread_internal_ldb(service, fd, buf, new_count,
                                               new_offset, mem_offset);
  if (rc < 0) {
    if (handle_inode_in_transfer(static_cast<int>(rc))) goto retry;
    bool should_retry = checkUpdateFdWid(static_cast<int>(rc), fd);
    if (should_retry) goto retry;
  }

  lease->Write((char *)buf - gFsLibMallocPageSize + mem_offset, new_count,
               new_offset, threadFsTid);

#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_PREAD, tsIdx);
#endif
  return count;
}

ssize_t fs_allocated_write_ldb(int fd, void *buf, size_t count) {
#ifdef LDB_PRINT_CALL
  print_write(fd, buf, count, true);
#endif

  auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
  if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
    fprintf(stderr, "LDBLease not found by write %d\n", fd);
    throw std::runtime_error("LDBLease not found by write");
  }

  LDBLease *lease = it->second;
  lease->Write(buf, count, threadFsTid);
  return count;
}

int fs_close_ldb(int fd) {
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_CLOSE);
#endif
#ifdef LDB_PRINT_CALL
  print_close(fd, true);
#endif
retry:
  int wid = -1;
  auto service = getFsServiceForFD(fd, wid);
  int rc = fs_close_internal(service, fd);

  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

  if (rc == 0) {
    auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
    if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
      fprintf(stderr, "LDBLease not found by close %d\n", fd);
      throw std::runtime_error("LDBLease not found by close");
    }
    gLibSharedContext->fdLDBLeaseMap.unsafe_erase(it);
    //    gLibSharedContext->fdLDBLeaseMap.erase(fd);
  }

#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_CLOSE, tsIdx);
#endif
  return rc;
}

int fs_wsync_internal(FsService *fsServ, int fd, bool isDataSync) {
  struct shmipc_msg msg;
  struct wsyncOp *wsyncOp;
  off_t ring_idx;
  int ret;
  int err;

  auto threadMemBuf = check_app_thread_mem_buf_ready();

  auto it = gLibSharedContext->fdLDBLeaseMap.find(fd);
  if (it == gLibSharedContext->fdLDBLeaseMap.end()) {
    fprintf(stderr, "LDBLease not found by fsync %d\n", fd);
    throw std::runtime_error("LDBLease not found by fsync");
  }
  LDBLease *lease = it->second;

  memset(&msg, 0, sizeof(struct shmipc_msg));
  ring_idx = shmipc_mgr_alloc_slot_dbg(fsServ->shmipc_mgr);
  wsyncOp = (struct wsyncOp *)IDX_TO_XREQ(fsServ->shmipc_mgr, ring_idx);

  msg.type = CFS_OP_WSYNC;
  wsyncOp->fd = fd;
  EmbedThreadIdToAsOpRet(wsyncOp->ret);
  void *data_array = lease->GetData(threadMemBuf, &(wsyncOp->array_size),
                                    &(wsyncOp->file_size));
  if (data_array) {
    threadMemBuf->getBufOwnerInfo(data_array, false, wsyncOp->alloc.shmid,
                                  wsyncOp->alloc.dataPtrId, err);
  }

  shmipc_mgr_put_msg(fsServ->shmipc_mgr, ring_idx, &msg);

  ret = wsyncOp->ret;
  shmipc_mgr_dealloc_slot(fsServ->shmipc_mgr, ring_idx);
  if (data_array) {
    fs_free(data_array);
  }
  return ret;
}

int fs_wsync(int fd) {
#ifdef LDB_PRINT_CALL
  print_fsync(fd, true);
#endif
  int wid = -1;
#ifdef CFS_LIB_SAVE_API_TS
  int tsIdx = tFsApiTs->addApiStart(FsApiType::FS_FSYNC);
#endif
retry:
  auto service = getFsServiceForFD(fd, wid);
  ssize_t rc = fs_wsync_internal(service, fd, true);
  if (rc < 0) {
    if (handle_inode_in_transfer(rc)) goto retry;
    bool should_retry = checkUpdateFdWid(rc, fd);
    if (should_retry) goto retry;
  }

#ifdef _CFS_LIB_PRINT_REQ_
  fprintf(stdout, "fdatasync(fd:%d) ret:%ld\n", fd, rc);
#endif
#ifdef CFS_LIB_SAVE_API_TS
  tFsApiTs->addApiNormalDone(FsApiType::FS_FSYNC, tsIdx);
#endif
  return rc;
}
