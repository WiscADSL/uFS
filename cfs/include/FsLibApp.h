#ifndef CFS_FSLIBAPP_H
#define CFS_FSLIBAPP_H

#include <atomic>
#include <condition_variable>
#include <list>
#include <shared_mutex>
#include <unordered_map>

#include "FsLibLease.h"
#include "FsLibMalloc.h"
#include "FsLibPageCache.h"
#include "FsLibShared.h"
#include "absl/container/flat_hash_map.h"
#include "fsapi.h"
#include "rbtree.h"
#include "shmipc/shmipc.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"
#include "tbb/reader_writer_lock.h"
#include "tbb/scalable_allocator.h"

//
// Only used in FsLib
//

// debug print flag
////#define _CFS_LIB_DEBUG_
// number of blocks (pages) that is used for each thread's cache
// set to 64 * 1024 blocks (i.e., 64 * 1024 * 4K = 256 M)
//#define FS_LIB_THREAD_CACHE_NUM_BLOCK (64 * 1024)
#define FS_LIB_THREAD_CACHE_NUM_BLOCK (10 * 64 * 1024)
// On/Off of using app buffer, assume always use this if using allocated_X API
// By default, enable APP BUF
#define FS_LIB_USE_APP_BUF (1)

// INVARIANT: threadFsTid == 0: thread uninitialized
// Otherwise, threadFsTid >= 1
extern thread_local int threadFsTid;

class CommuChannelAppSide : public CommuChannel {
 public:
  CommuChannelAppSide(pid_t id, key_t shmKey);
  ~CommuChannelAppSide();
  int submitSlot(int sid);

 protected:
  void *attachSharedMemory(void);
  void cleanupSharedMemory(void);
};

// single block of file whose status is flag
#define FSLIB_APP_BLK_RCD_FLAG_DIRTY (0b00000001)
struct BlockRecordOfFile {
  // REQUIRED: alignedStartOffset is aligned with page/block size
  // start offset within the file (in bytes)
  off_t alignedStartOffset{0};
  // valid content length (in bytes)
  size_t count{0};
  // memory address of data
  // REQUIRED: corresponds to alignedStartOffset (strictly aligned)
  void *addr{nullptr};
  // status of this block
  uint8_t flag{0};
};

// The FileHandle is mainly designed for client cache
// Because One app might open a file for several times and get several FDs,
// But we only want to keep one copy of the data inside client cache
// Thus, the sharing granularity is each file
struct FileHandle {
  char fileName[MULTI_DIRSIZE];
  // this fileHandle's reference count
  // we do not want to delete this item if all the FD is closed
  // REQUIRED: modification needs to be done with fdFileHandleMapLock held
  int refCount;
  int id;
  FileHandleCacheReader cacheReader;
  tbb::reader_writer_lock lock_;
};

class FsService {
 public:
  FsService();
  FsService(int wid, key_t key);
  ~FsService();
  void initService();
  int allocRingSlot();
  void releaseRingSlot(int slotId);
  // get the corresponding buffer associated with one slot
  // if reset is set to true, will init the memory buffer to zeros
  void *getSlotDataBufPtr(int slotId, bool reset);
  struct clientOp *getSlotOpPtr(int slotId);
  int submitReq(int slotId);

  key_t GetShmkey() { return shmKey; }

  struct shmipc_mgr *shmipc_mgr = NULL;
  bool inUse = false;

 private:
  std::list<int> unusedRingSlots;
  std::atomic_flag unusedSlotsLock;
  key_t shmKey;
  int wid;
  CommuChannelAppSide *channelPtr;
};

struct FsLibServiceMng {
  std::unordered_map<int, FsService *> multiFsServMap;
  std::atomic_int multiFsServNum{0};
  FsService *primaryServ{nullptr};
};

// NOTE: enable this flag will record the entry and exit timestamp
// of each fs api
// The point is to see how frequently the FS is stressed in realy apps
// in contrast to the case the in the microbenchmark, we keep issue fs request
// #define CFS_LIB_SAVE_API_TS

#ifdef CFS_LIB_SAVE_API_TS

enum class FsApiType {
  _DUMMY_FIRST,
  FS_STAT,
  FS_OPEN,
  FS_CLOSE,
  FS_UNLINK,
  FS_MKDIR,
  FS_OPENDIR,
  FS_READDIR,
  FS_CLOSEDIR,
  FS_RMDIR,
  FS_RENAME,
  FS_FSYNC,
  FS_READ,
  FS_PREAD,
  FS_WRITE,
  FS_PWRITE,
  _DUMMY_LAST,
};

using perApiTsVec = std::vector<std::array<uint64_t, 2>>;
const static int kNumApi = FsApiType::_DUMMY_LAST - FsApiType::_DUMMY_FIRST - 1;

#define FSAPI_TYPE_TO_IDX(tp)  \
  (static_cast<uint32_t>(tp) - \
   static_cast<uint32_t>(FsApiType::_DUMMY_FIRST) - 1)

#define FSAPI_TYPE_TO_STR(A)                              \
  {                                                       \
    (static_cast<uint32_t>(FsApiType::A) -                \
     static_cast<uint32_t>(FsApiType::_DUMMY_FIRST) - 1), \
        #A                                                \
  }

const static std::unordered_map<uint32_t, const char *> gFsApiTypeStrMap{
    FSAPI_TYPE_TO_STR(FS_STAT),    FSAPI_TYPE_TO_STR(FS_OPEN),
    FSAPI_TYPE_TO_STR(FS_CLOSE),   FSAPI_TYPE_TO_STR(FS_UNLINK),
    FSAPI_TYPE_TO_STR(FS_MKDIR),   FSAPI_TYPE_TO_STR(FS_OPENDIR),
    FSAPI_TYPE_TO_STR(FS_READDIR), FSAPI_TYPE_TO_STR(FS_CLOSEDIR),
    FSAPI_TYPE_TO_STR(FS_RMDIR),   FSAPI_TYPE_TO_STR(FS_RENAME),
    FSAPI_TYPE_TO_STR(FS_FSYNC),   FSAPI_TYPE_TO_STR(FS_READ),
    FSAPI_TYPE_TO_STR(FS_PREAD),   FSAPI_TYPE_TO_STR(FS_WRITE),
    FSAPI_TYPE_TO_STR(FS_PWRITE),
};

#define FSAPI_TS_START_ARR_POS (0)
#define FSAPI_TS_END_ARR_POS (1)

class FsApiTs {
 public:
  FsApiTs(int tid)
      : tid_(tid),
        apiTsList(kNumApi, perApiTsVec(kFsApiTsResizeStep)),
        apiTsIdxList(kNumApi, 0) {}

  void reportToStream(FILE *stream) {
    for (int typei = 0; typei < kNumApi; typei++) {
      auto &vec = apiTsList[typei];
      auto it = gFsApiTypeStrMap.find(typei);
      assert(it != gFsApiTypeStrMap.end());
      for (size_t j = 0; j < apiTsIdxList[typei]; j++) {
        fprintf(
            stream, "%s start:%lu end:%lu\n", it->second,
            PerfUtils::Cycles::toNanoseconds(vec[j][FSAPI_TS_START_ARR_POS]),
            PerfUtils::Cycles::toNanoseconds(vec[j][FSAPI_TS_END_ARR_POS]));
      }
    }
  }

  uint32_t addApiStart(FsApiType tp) {
    int typeidx = FSAPI_TYPE_TO_IDX(tp);
    auto &vec = apiTsList[typeidx];
    if (apiTsIdxList[typeidx] >= kFsApiTsResizeStep)
      vec.reserve(kFsApiTsResizeStep);
    (vec[apiTsIdxList[typeidx]])[FSAPI_TS_START_ARR_POS] = genTs();
    (vec[apiTsIdxList[typeidx]])[FSAPI_TS_END_ARR_POS] = 0;
    return apiTsIdxList[typeidx]++;
  }

  // @param inApiIdx is returned by addApiStart
  void addApiNormalDone(FsApiType tp, uint32_t inApiIdx) {
    int typeidx = FSAPI_TYPE_TO_IDX(tp);
    auto &vec = apiTsList[typeidx];
    if (inApiIdx > apiTsIdxList[typeidx] ||
        vec[inApiIdx][FSAPI_TS_END_ARR_POS] != 0) {
      throw std::runtime_error(
          "the index for this timestamp cannot match Or the finish has already "
          "been set");
    }
    vec[inApiIdx][FSAPI_TS_END_ARR_POS] = genTs();
  }

 private:
  int tid_;
  uint64_t genTs() { return PerfUtils::Cycles::rdtscp(); }
  const static uint32_t kFsApiTsResizeStep = 1000000;
  std::vector<perApiTsVec> apiTsList;
  std::vector<uint32_t> apiTsIdxList;
};

struct FsApiTsMng {
  // <tid, ts>
 public:
  FsApiTsMng() {}

  ~FsApiTsMng() {
    for (auto ele : ioThreadsApiTs) delete ele.second;
  }

  FsApiTs *initForTid(int tid) {
    auto it = ioThreadsApiTs.find(tid);
    if (it != ioThreadsApiTs.end()) return (it->second);
    FsApiTs *curApiTs = new FsApiTs(tid);
    ioThreadsApiTs[tid] = curApiTs;
    return curApiTs;
  }

  void reportAllTs() {
    for (auto ele : ioThreadsApiTs) {
      fprintf(stdout, "FSAPI report --- tid:%d\n", ele.first);
      (ele.second)->reportToStream(stdout);
    }
  }

  // <tid, ts>
  std::unordered_map<int, FsApiTs *> ioThreadsApiTs;
};

#undef FSAPI_TYPE_TO_IDX
#undef FSAPI_TS_START_ARR_POS
#undef FSAPI_TS_END_ARR_POS

#endif  // CFS_LIB_SAVE_API_TS

struct OpenLeaseMapEntry {
  std::atomic_char32_t ref;
  std::condition_variable cv;
  std::shared_mutex lock;
  OpenLease *lease;

  OpenLeaseMapEntry(int base_fd, const std::string &path, uint64_t size)
      : ref(0), lease(new OpenLease(base_fd, path, size)) {}
  ~OpenLeaseMapEntry() { delete lease; }
};

OpenLeaseMapEntry *LeaseRef(const char *path);
OpenLeaseMapEntry *LeaseRef(int fd);
void LeaseUnref(bool del = false);

struct FsLibSharedContext {
  /// used as unique ID for this application process
  key_t key;
  // <fd, workerId>: store each id is handled by which worker.
  // NOTE: since fd is assigned by FSP, it is guaranteed that
  // within the App, all fd is incremental
  tbb::concurrent_unordered_map<int, int8_t> fdWidMap;
  tbb::concurrent_unordered_map<std::string, int8_t> pathWidMap;
  // <tid, pointer to memCache>
  // tid: thread id assigned by this library
  // REQUIRED: access should be guarded by tidIncrLock
  // int tidIncr;
  tbb::concurrent_unordered_map<int, FsLibMemMng *> tidMemBufMap;
  std::atomic_int tidIncr;
  tbb::concurrent_unordered_map<int, std::vector<struct BlockRecordOfFile *>>
      fhIdPageMap;

  // <fd, fileHandleId>
  // REQUIRED: access should be guarded by fdFileHandleMapLock
  absl::flat_hash_map<int, struct FileHandle *> fdFileHandleMap;
  std::unordered_map<std::string, struct FileHandle *> fnameFileHandleMap;
  tbb::reader_writer_lock fdFileHandleMapLock;
  absl::flat_hash_map<int, off_t> fdOffsetMap;

  // <fd, currentOffset>
  tbb::concurrent_unordered_map<int, off_t> fdCurOffMap;

  PageCacheHelper pageCacheHelper;

  // not related to data, keep track of the lease timer
  FsLibLeaseMng leaseMng_;

  std::shared_mutex openLeaseMapLock;
  std::unordered_map<std::string, OpenLeaseMapEntry *> pathOpenLeaseMap;
  std::unordered_map<int, OpenLeaseMapEntry *> fdOpenLeaseMap;

  tbb::concurrent_unordered_map<std::string, LDBLease *> pathLDBLeaseMap;
  tbb::concurrent_unordered_map<int, LDBLease *> fdLDBLeaseMap;

#ifdef CFS_LIB_SAVE_API_TS
  FsApiTsMng apiTsMng_;
#endif
};

// =======
// op Helper functions
// from posix api arguments to ops
struct readOp *fillReadOp(struct clientOp *curCop, int fd, size_t count);
struct preadOp *fillPreadOp(struct clientOp *curCop, int fd, size_t count,
                            off_t offset);
struct writeOp *fillWriteOp(struct clientOp *curCop, int fd, size_t count);
struct writeOp *fillWriteOp(struct clientOp *curCop, int fd, size_t count);
struct pwriteOp *fillPWriteOp(struct clientOp *curCop, int fd, size_t count,
                              off_t offset);
struct openOp *fillOpenOp(struct clientOp *curCop, const char *path, int flags,
                          mode_t mode);
struct closeOp *fillCloseOp(struct clientOp *curCop, int fd);
struct statOp *fillStatOp(struct clientOp *curCop, const char *path,
                          struct stat *statbuf);
struct mkdirOp *fillMkdirOp(struct clientOp *curCop, const char *pathname,
                            mode_t mode);
struct fstatOp *fillFstatOp(struct clientOp *curCop, int fd,
                            struct stat *statbuf);
struct fsyncOp *fillFsyncOp(struct clientOp *curCop, int fd, bool isDataSync);

struct unlinkOp *fillUnlinkOp(struct clientOp *curCop, const char *pathname);

// Setup per thread mem buffer
// Check if the thread has already setup its mem buffer, if not set it up.
FsLibMemMng *check_app_thread_mem_buf_ready(int fsTid = threadFsTid);
// =======

#endif  // CFS_FSLIBAPP_H
