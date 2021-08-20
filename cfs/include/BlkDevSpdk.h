#ifndef CFS_BLKDEVSPDK_H
#define CFS_BLKDEVSPDK_H

#include <stdio.h>

#include <deque>
#include <list>
#include <unordered_map>
#include <vector>

#include "BlkDev.h"
#include "param.h"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/stdinc.h"
#include "typedefs.h"
#include "util.h"

struct ctrlr_entry {
  struct spdk_nvme_ctrlr *ctrlr;
  struct ctrlr_entry *next;
  char name[1024];
};

struct ns_entry {
  struct spdk_nvme_ctrlr *ctrlr;
  struct spdk_nvme_ns *ns;
  struct ns_entry *next;
};

typedef std::vector<struct BdevIoContext *> threadReqVec;

class BlkDevSpdk;

struct SpdkProbeIoContext {
  BlkDevSpdk *devPtr;
};

class BlkDevSpdk : public BlkDev {
 public:
  static constexpr int kMaxInflightWriteReq =
      4 * (SPDK_THREAD_MAX_INFLIGHT / 5);
  // if enabled, each name_read/write will go through checking of the memory
  // address to make sure the address is within the valid pinned memory
  static constexpr bool kCheckRWMem = false;
  BlkDevSpdk(const std::string &path, uint32_t blockNum, uint32_t blockSize);
  BlkDevSpdk(const std::string &path, uint32_t blockNum, uint32_t blockSize,
             std::string configName);
  virtual ~BlkDevSpdk();
  // inherited functions
  virtual int devInit();
  virtual int read(uint64_t blockNo, char *data);
  virtual int write(uint64_t blockNo, uint64_t blockNoSeqNo, char *data);
  virtual void *zmallocBuf(uint64_t size, uint64_t align);
  virtual int freeBuf(void *ptr);
  virtual int devExit(void);
  // BlkDevSpdk specific functions (not inherited)
  virtual int blockingRead(uint64_t blockNo, char *data);
  virtual int blockingWrite(uint64_t blockNo, char *data);
  virtual int blockingWriteMultiBlocks(uint64_t blockStartNo, int numBlocks,
                                       char *data);
  virtual void addController(struct ctrlr_entry *entry);
  virtual void registerNamespace(struct spdk_nvme_ctrlr *ctrlr,
                                 struct spdk_nvme_ns *ns);
  virtual struct spdk_nvme_ns *getCurrentThreadNS(void);
  virtual struct spdk_nvme_qpair *getCurrentThreadQPair(void);
  // init one thread's NVMe Device access
  // including, but not limit to allocated IO Queue Pair for the thread
  virtual int initWorker(int wid);
  virtual void updateWorkerNum(int workerNum) { _workerNum = workerNum; }
  virtual bool ifAllworkerReady() { return allWorkerReady.load(); }
  // @param maxCmplNum: max number of completion event to get
  virtual int checkCompletion(int maxCmplNum);
  virtual void releaseBdevIoContext(struct BdevIoContext *ctx);
  // called once an write block IO is done to reduce the current inflight #
  virtual int reduceInflightWriteNum(int num);
  // clean up the access to this device (not only the worker's access)
  virtual int cleanup();

  // Read/Write by one sector (512B, SSD's minimal IO size)
  virtual int readSector(uint64_t sectorNo, char *data);
  int blockingReadSector(uint64_t sectorNo, char *data);
  int writeSector(uint64_t sectorNo, uint64_t sectorNoSeqNo, char *data);
  virtual int blockingWriteSector(uint64_t sectorNo, uint64_t sectorNoSeqNo,
                                  char *data);
  int blockingZeroSectors(uint64_t lba, uint32_t lba_count);

  void setPinMemBound(char *addr, char *end) {
    if (pinMemBase == 0) {
      pinMemBase = addr;
      pinMemEnd = end;
      fprintf(stderr, "[BlkDevSpdk] setPinMemBound addr:%p end:%p\n", addr,
              end);
    }
  }

  // we let the caller decide to fail the sys or not
  bool checkRWBufWithinBound(char *addr, size_t bytes) {
    if (!kCheckRWMem) return true;
    if (pinMemBase == 0) return true;
    fprintf(stderr, "tid:%d pinMemBase:%p pinMemBound:%p addr:%p bytes:%lu\n",
            cfsGetTid(), pinMemBase, pinMemEnd, addr, bytes);
    if (addr >= pinMemBase && addr + bytes <= pinMemEnd) {
      return true;
    }
    return false;
  }

  // Function to output to the Block device's log
  void statsReport();
  // Control if output the stats or not
  void enableReportStats(bool b) { isReportStats = b; }
  bool isSpdk() { return isSpdkDev; }

 protected:
  BlkDevSpdk(bool isPosix, const std::string &path, uint32_t blockNum,
             uint32_t blockSize);
  std::string configFileName;
  bool isSpdkDev = true;

 private:
  // A flag to control the debug output
  // Even using SPDLOG_DEBUG(), it is going to be to heavyweight if we log
  // debugging info for each BIO
  const static bool isDebug = false;

  // test and store the status whether it is initialized
  bool initialized = false;

  // namespace section size
  uint32_t nsSecSize;
  // total size of namespace (device's storage size)
  uint64_t nsTotalSize;
  uint32_t defaultBlockLbaNum;  // # of lbas in each block
  int kThreadMaxInflightReqs;

  // <wid, number of inflightWriteRequest>
  std::unordered_map<int, int> threadInflightWriteReqNumMap;

  struct ctrlr_entry *gControllers;
  struct ns_entry *gNamespaces;
  // Note: assume only has one controller and one namespace now, always use this
  // one
  struct spdk_env_opts opts;
  static constexpr int kNumMaxThreads = 20;
  std::vector<std::deque<bdev_reqid_t>> tidUnusedReqidList;
  std::vector<threadReqVec> tidReqvecList;
  std::vector<struct spdk_nvme_qpair *> tidQpairList;
  std::vector<int> tidWidList;

  int _workerNum = 1;
  int _readyWorkerNum = 0;
  std::atomic_bool allWorkerReady{false};

  // IO context used only for initialization
  SpdkProbeIoContext probeIoContext;

  char *pinMemBase = nullptr;
  char *pinMemEnd = nullptr;

  // Stats reporting
  bool isReportStats = false;
  static constexpr uint32_t kReportAllocCalledNumThreshold = 10000;
  // <wid, number of inflightRequest>
  std::vector<uint64_t> threadInDevQueueReqNumVec;
  std::vector<uint32_t> threadAllocCntVec;
  std::vector<uint32_t> threadDoneCntVec;
  std::vector<uint64_t> lastReportTs;

  // std::vector<uint8_t> inflightNumStatsVec;
  // uint32_t inflightNumStatsVecIdx = 0;
  int64_t lastStatReportTime = 0;

  struct BdevIoContext *allocReqContext(cfs_tid_t tid);
  void releaseReqContext(struct BdevIoContext *ctx);
  // submit a device IO request
  // @param blockNo : logical block number (in the unit of *curBlockSize*)
  //      The start offset of device R/W will be computed by:
  //      -> blockNo * (curBlockSize)
  //          [curBlockSize will be set to devBlockSize if it is 0]
  // @param curBlockSize : if 0, will use *devBlockSize* as the unit, otherwise,
  //      will submit IO of *curBlockSize* Bytes
  //      REQUIRED: curBlockSize % 512 == 0
  //      This optional parameter is mainly used to allow more flexible size of
  //      DISK IO. E.g., 1M at a time, or 512 B at a time
  int submitDevReq(uint64_t blockNo, uint64_t blockNoReqNo, char *data,
                   enum BlkDevReqType reqType, uint32_t curBlockSize = 0);
  int submitBlockingDevReq(uint64_t blockNo, char *data,
                           enum BlkDevReqType reqType,
                           uint32_t curBlockSize = 0);

  // TODO: helper functions to write/flush with FUA flag as well...
  // The callback should be able to access either journal manager or the
  // worker..
 public:
  bool zero_supported = false;
  bool flush_supported = false;
};

#ifndef USE_SPDK
class BlkDevPosix : public BlkDevSpdk {
 public:
  BlkDevPosix(const std::string &path, uint32_t blockNum, uint32_t blockSize);
  BlkDevPosix(const std::string &path, uint32_t blockNum, uint32_t blockSize,
              std::string configNam);
  ~BlkDevPosix();
  // inherited functions
  int devInit();
  int read(uint64_t blockNo, char *data);
  int write(uint64_t blockNo, uint64_t blockNoSeqNo, char *data);
  void *zmallocBuf(uint64_t size, uint64_t align);
  int freeBuf(void *ptr);
  int devExit(void);
  // overwrite BlkDevSpdk's functions
  int blockingRead(uint64_t blockNo, char *data);
  int blockingWrite(uint64_t blockNo, char *data);
  int blockingWriteMultiBlocks(uint64_t blockStartNo, int numBlocks,
                               char *data);

  int readSector(uint64_t sectNo, char *data);
  int blockingWriteSector(uint64_t secNo, uint64_t sectorNoSeqNo, char *data);

  // DO_NOT_SUPPORT
  void addController(struct ctrlr_entry *entry);
  void registerNamespace(struct spdk_nvme_ctrlr *ctrlr,
                         struct spdk_nvme_ns *ns);
  int initWorker(int wid);
  int checkCompletion(int maxCmplNum);
  void releaseBdevIoContext(struct BdevIoContext *ctx);
  int reduceInflightWriteNum(int num);
  int cleanup(void);

 private:
  FILE *diskFile;
};
#endif

#ifdef USE_SPDK
typedef BlkDevSpdk CurBlkDev;
#else
typedef BlkDevPosix CurBlkDev;
#endif  // USE_SPDK

#endif  // CFS_BLKDEVSPDK_H
