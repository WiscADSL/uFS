#include "BlkDevSpdk.h"

#include <param.h>
#include <string.h>

#include <experimental/filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>

#include "FsProc_Fs.h"
#include "config4cpp/Configuration.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "util.h"

// extern BlkDevSpdk *devPtr;
extern FsProc *gFsProcPtr;
static const char logInfoStr[] = "[BlkDevSpdk] ";
static std::mutex lmtx;
static std::mutex devmtx;

/* callback functions */
static void read_complete(void *arg, const struct spdk_nvme_cpl *completion);
static void write_complete(void *arg, const struct spdk_nvme_cpl *completion);
static void blocking_read_complete(void *arg,
                                   const struct spdk_nvme_cpl *completion);
static void blocking_write_complete(void *arg,
                                    const struct spdk_nvme_cpl *completion);
/*--------------------*/

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                     struct spdk_nvme_ctrlr_opts *opts) {
  std::cout << logInfoStr << "Attaching to " << trid->traddr << std::endl;
  return true;
}

void BlkDevSpdk::registerNamespace(struct spdk_nvme_ctrlr *ctrlr,
                                   struct spdk_nvme_ns *ns) {
  struct ns_entry *entry;
  const struct spdk_nvme_ctrlr_data *cdata;

  cdata = spdk_nvme_ctrlr_get_data(ctrlr);

  if (!spdk_nvme_ns_is_active(ns)) {
    printf("Controller %-20.20s (%-20.20s): Skipping inactive NS %u\n",
           cdata->mn, cdata->sn, spdk_nvme_ns_get_id(ns));
    return;
  }

  uint32_t flags = spdk_nvme_ns_get_flags(ns);
  if (flags & SPDK_NVME_NS_WRITE_ZEROES_SUPPORTED) {
    zero_supported = true;
    std::cout << "WRITE_ZEROES supported" << std::endl;
  }

  if (flags & SPDK_NVME_NS_FLUSH_SUPPORTED) {
    flush_supported = true;
    std::cout << "FLUSH supported" << std::endl;
  }

  entry = new ns_entry();
  assert(entry != nullptr);

  entry->ctrlr = ctrlr;
  entry->ns = ns;
  entry->next = gNamespaces;
  gNamespaces = entry;

  if (nsSecSize == 0) {
    nsSecSize = spdk_nvme_ns_get_sector_size(ns);
    nsTotalSize = spdk_nvme_ns_get_size(ns);
    defaultBlockLbaNum = devBlockSize / nsSecSize;
  }
  assert(nsSecSize != 0);
  assert(nsSecSize <= devBlockSize);
  assert(devBlockSize % nsSecSize == 0);

  printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
         spdk_nvme_ns_get_size(ns) / 1000000000);
  printf(" And the sector size is:%uB \n", nsSecSize);
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts) {
  int nsid, num_ns;
  struct ctrlr_entry *entry;
  struct spdk_nvme_ns *ns;
  const struct spdk_nvme_ctrlr_data *cdata = spdk_nvme_ctrlr_get_data(ctrlr);
  auto probeIoContext = (struct SpdkProbeIoContext *)(cb_ctx);

  entry = new ctrlr_entry();
  assert(entry != nullptr);

  std::cout << logInfoStr << "Attached to " << trid->traddr << std::endl;
  snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn,
           cdata->sn);

  std::cout << logInfoStr << "SGL support: " << cdata->sgls.supported
            << std::endl;

  entry->ctrlr = ctrlr;
  printf("SPDK controller memaddr:%p\n", ctrlr);
  probeIoContext->devPtr->addController(entry);

  num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
  printf("Using controller %s with %d namespaces.\n", entry->name, num_ns);
  for (nsid = 1; nsid <= num_ns; nsid++) {
    ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL) {
      printf("WARNING: spdk_nvme_ctrlr_get_ns returns null\n");
      continue;
    }
    probeIoContext->devPtr->registerNamespace(ctrlr, ns);
  }
}

BlkDevSpdk::BlkDevSpdk(const std::string &path, uint32_t blockNum,
                       uint32_t blockSize)
    : BlkDev(path, blockNum, blockSize),
      tidUnusedReqidList(kNumMaxThreads),
      tidQpairList(kNumMaxThreads, nullptr),
      tidWidList(kNumMaxThreads, -1),
      threadInDevQueueReqNumVec(kNumMaxThreads, 0),
      threadAllocCntVec(kNumMaxThreads, 0),
      threadDoneCntVec(kNumMaxThreads, 0),
      lastReportTs(kNumMaxThreads, 0) {
  nsSecSize = 0;
  kThreadMaxInflightReqs = SPDK_THREAD_MAX_INFLIGHT;
  gControllers = nullptr;
  gNamespaces = nullptr;
  for (int i = 0; i < kNumMaxThreads; i++) {
    tidReqvecList.push_back(threadReqVec(kThreadMaxInflightReqs, nullptr));
  }
}

BlkDevSpdk::BlkDevSpdk(const std::string &path, uint32_t blockNum,
                       uint32_t blockSize, std::string configName)
    : BlkDevSpdk(path, blockNum, blockSize) {
  configFileName = std::move(configName);
}

BlkDevSpdk::BlkDevSpdk(bool isPosix, const std::string &path, uint32_t blockNum,
                       uint32_t blockSize)
    : BlkDev(path, blockNum, blockSize) {
  // Do nothing here
  isSpdkDev = (!isPosix);
}

BlkDevSpdk::~BlkDevSpdk() {
  if (isSpdkDev) {
    cleanup();
  }
}

int BlkDevSpdk::devInit() {
  int rc;
  std::lock_guard<std::mutex> lock(lmtx);
  {
    if (initialized) {
      SPDLOG_WARN("devInit called, but initialized is set");
      return 0;
    }
    initialized = true;
  }
  spdk_env_opts_init(&opts);
  probeIoContext.devPtr = this;
  if (!configFileName.empty()) {
    // Check if the config file exists.
    if (std::experimental::filesystem::exists(configFileName)) {
      // Parse configuration file.
      config4cpp::Configuration *cfg = config4cpp::Configuration::create();
      try {
        cfg->parse(configFileName.c_str());
        opts.name = cfg->lookupString("", "dev_name");
        opts.core_mask = cfg->lookupString("", "core_mask");
        opts.shm_id = cfg->lookupInt("", "shm_id");
      } catch (const config4cpp::ConfigurationException &ex) {
        SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
        cfg->destroy();
        return 1;
      }
      // TODO (jingliu):
      // cfg->destroy();
      // I have no idea why adding destroy() here will result in SPDK mem
      // init fail, we may add this cfg as class object and deconstruct then
    } else {
      SPDLOG_ERROR("config file:{} not exist", configFileName);
    }
  } else {
    opts.name = "spdkdiskDefault";
    // 9 is randomly picked.
  }

  // NOTE: we force to overwrite this shmid here
  opts.shm_id = SPDK_HUGEPAGE_GLOBAL_SHMID;

  if (spdk_env_init(&opts) < 0) {
    fprintf(stderr, "Unable to initialize SPDK env\n");
    return 1;
  }

  SPDLOG_INFO("{} spdk core_mask:{}", logInfoStr, opts.core_mask);
  std::cout << logInfoStr << "Initializing Nvme Controllers" << std::endl;

  // TODO (jingliu), this first argument is  *spdk_nvme_transport_id * 	trid*
  // which can be used to control the device to probe
  rc = spdk_nvme_probe(NULL, &probeIoContext, probe_cb, attach_cb, NULL);
  if (rc != 0) {
    std::cerr << "spdk_nvme_probe() failed" << std::endl;
    cleanup();
    return -1;
  }

  if (gControllers == NULL) {
    std::cerr << "No NVMe controller found" << std::endl;
    cleanup();
    return -1;
  }

  while (gNamespaces == NULL) {
    // wait until initialization completed
  }
  SPDLOG_INFO("{} Initialization complete :)", logInfoStr);
  return 0;
}

int BlkDevSpdk::submitDevReq(uint64_t blockNo, uint64_t blockNoSeqNo,
                             char *data, BlkDevReqType reqType,
                             uint32_t curBlockSize) {
  uint32_t curBlockLbaNum = defaultBlockLbaNum;
  if (curBlockSize != 0) {
    assert(curBlockSize % nsSecSize == 0);
    curBlockLbaNum = curBlockSize / (nsSecSize);
  }
  int rc = -1;
  uint64_t lbaStartNo;
  cfs_tid_t tid = cfsGetTid();
  struct BdevIoContext *ctx_ptr = allocReqContext(tid);
  if (ctx_ptr == nullptr) {
    // SPDLOG_INFO("submitDevReq cannot allocate request context");
    return -1;
  }
  auto qp = tidQpairList[tid];
  // auto qpit = tidQpairMap.find(tid);
  // assert(qpit != tidQpairMap.end());
  lbaStartNo = blockNo * curBlockLbaNum;

  ctx_ptr->buf = data;
  ctx_ptr->blockNo = blockNo;
  ctx_ptr->blockNoSeqNo = blockNoSeqNo;
  ctx_ptr->reqType = reqType;
  if (isDebug) {
    SPDLOG_DEBUG("submitDevReq libstart:{} blockLbaNum:{}", lbaStartNo,
                 curBlockLbaNum);
  }

  switch (reqType) {
    case (BlkDevReqType::BLK_DEV_REQ_READ):
    case (BlkDevReqType::BLK_DEV_REQ_SECTOR_READ): {
      if (kCheckRWMem) {
        bool ok = checkRWBufWithinBound(ctx_ptr->buf, curBlockLbaNum * 512);
        if (!ok) throw std::runtime_error("read mem out bound\n");
      }
      rc = spdk_nvme_ns_cmd_read(gNamespaces->ns, qp, ctx_ptr->buf, lbaStartNo,
                                 curBlockLbaNum, read_complete, ctx_ptr, 0);
      // fprintf(stderr,
      //        "spdk_nvme_ns_cmd_read lhaStart:%lu ctx->ptr:%p "
      //        "ctx_ptr->blockNo:%ld\n",
      //        lbaStartNo, data, blockNo);
      break;
    }
    case BlkDevReqType::BLK_DEV_REQ_WRITE: {
      if (kCheckRWMem) {
        bool ok = checkRWBufWithinBound(ctx_ptr->buf, curBlockLbaNum * 512);
        if (!ok) throw std::runtime_error("write mem out bound\n");
      }
      rc = spdk_nvme_ns_cmd_write(gNamespaces->ns, qp, ctx_ptr->buf, lbaStartNo,
                                  curBlockLbaNum, write_complete, ctx_ptr, 0);
      // fprintf(stdout, "write lhaStart:%lu ctx->ptr:%p
      // ctx_ptr->blockNo:%ld\n",
      //        lbaStartNo, data, blockNo);
      break;
    }
    default:
      SPDLOG_ERROR("reqType not supported");
  }
  if (rc < 0) {
    std::cerr << "ERROR, spdk rc:" << rc << std::endl;
  }
  return rc;
}

int BlkDevSpdk::submitBlockingDevReq(uint64_t blockNo, char *data,
                                     BlkDevReqType reqType,
                                     uint32_t curBlockSize) {
  uint32_t curBlockLbaNum = defaultBlockLbaNum;
  if (curBlockSize != 0) {
    assert(curBlockSize % nsSecSize == 0);
    curBlockLbaNum = curBlockSize / (nsSecSize);
  }
  int rc = -1;
  uint64_t lbaStartNo;
  cfs_tid_t tid = cfsGetTid();
  struct BdevIoContext *ctx_ptr = allocReqContext(tid);
  // auto qpit = tidQpairList.find(tid);
  // assert(qpit != tidQpairMap.end());
  auto qpit = tidQpairList[tid];
  lbaStartNo = blockNo * curBlockLbaNum;

  ctx_ptr->buf = data;
  ctx_ptr->blockNo = blockNo;
  ctx_ptr->reqType = reqType;
  if (isDebug) {
    SPDLOG_DEBUG("BlkDevSpdk::submitBlockingDevReq lbastart:{} blockLbaNum:{}",
                 lbaStartNo, curBlockLbaNum);
  }

  switch (reqType) {
    case BlkDevReqType::BLK_DEV_REQ_READ:
    case BlkDevReqType::BLK_DEV_REQ_SECTOR_READ:
      rc = spdk_nvme_ns_cmd_read(gNamespaces->ns, qpit, ctx_ptr->buf,
                                 lbaStartNo, curBlockLbaNum,
                                 blocking_read_complete, ctx_ptr, 0);
      break;
    case BlkDevReqType::BLK_DEV_REQ_WRITE:
    case BlkDevReqType::BLK_DEV_REQ_SECTOR_WRITE:
      rc = spdk_nvme_ns_cmd_write(gNamespaces->ns, qpit, ctx_ptr->buf,
                                  lbaStartNo, curBlockLbaNum,
                                  blocking_write_complete, ctx_ptr, 0);
      break;
    default:
      SPDLOG_DEBUG(
          "reqType is not supported typeNo:{}",
          static_cast<uint32_t>(reqType) -
              static_cast<uint32_t>(BlkDevReqType::BLK_DEV_REQ_DEFAULT));
  }

  // busy wait here
  while (!ctx_ptr->isDone) {
    checkCompletion(0);
  }
  // release req context
  releaseReqContext(ctx_ptr);
  return rc;
}

int BlkDevSpdk::read(uint64_t blockNo, char *data) {
  return submitDevReq(blockNo, /*blockNoSeqNo*/ 0, data,
                      BlkDevReqType::BLK_DEV_REQ_READ);
}

int BlkDevSpdk::write(uint64_t blockNo, uint64_t blockNoSeqNo, char *data) {
#if 0
  int curWid = tidWidMap[cfsGetTid()];
  threadInflightWriteReqNumMap[curWid]++;
  auto curval = threadInflightWriteReqNumMap[curWid];
  // fprintf(stderr, "write() threadInflightWrite:%d curWid_:%d\n", curval,
  //        curWid);
  if (curval >= kMaxInflightWriteReq) {
    SPDLOG_DEBUG("Cannot submit write Request because INFLIGHT LIMIT");
    return -1;
  }
#endif
  return submitDevReq(blockNo, blockNoSeqNo, data,
                      BlkDevReqType::BLK_DEV_REQ_WRITE);
}

void *BlkDevSpdk::zmallocBuf(uint64_t size, uint64_t align) {
  void *addr = spdk_dma_malloc(size, align, NULL);
  if (addr == NULL) {
    SPDLOG_ERROR("zmallocBuf failed");
  }
  return addr;
}

int BlkDevSpdk::freeBuf(void *ptr) {
  spdk_dma_free(ptr);
  return 0;
}

int BlkDevSpdk::devExit() {
  cleanup();
  return 0;
}

int BlkDevSpdk::blockingRead(uint64_t blockNo, char *data) {
  return submitBlockingDevReq(blockNo, data, BlkDevReqType::BLK_DEV_REQ_READ);
}

int BlkDevSpdk::blockingWrite(uint64_t blockNo, char *data) {
  return submitBlockingDevReq(blockNo, data, BlkDevReqType::BLK_DEV_REQ_WRITE);
}

int BlkDevSpdk::blockingWriteMultiBlocks(uint64_t blockStartNo, int numBlocks,
                                         char *data) {
  int rc = -1;
  assert(numBlocks >= 1);
  uint64_t lbaStartNo;
  cfs_tid_t tid = cfsGetTid();
  struct BdevIoContext *ctx_ptr = allocReqContext(tid);
  auto qp = tidQpairList[tid];
  // auto qpit = tidQpairMap.find(tid);
  // assert(qpit != tidQpairMap.end());
  lbaStartNo = blockStartNo * defaultBlockLbaNum;
  if (isDebug) {
    SPDLOG_DEBUG("BlkDevSpdk::submitBlockingDevReq lbastart:{} blockLbaNum:{}",
                 lbaStartNo, defaultBlockLbaNum);
  }
  ctx_ptr->buf = data;
  ctx_ptr->blockNo = blockStartNo;
  ctx_ptr->reqType = BlkDevReqType::BLK_DEV_REQ_WRITE;

  rc = spdk_nvme_ns_cmd_write(gNamespaces->ns, qp, ctx_ptr->buf, lbaStartNo,
                              (defaultBlockLbaNum * numBlocks),
                              blocking_write_complete, ctx_ptr, 0);

  // busy wait here
  while (!ctx_ptr->isDone) {
    checkCompletion(0);
  }
  // release req context
  releaseReqContext(ctx_ptr);
  return rc;
}

int BlkDevSpdk::blockingZeroSectors(uint64_t lba, uint32_t lba_count) {
  if (!zero_supported) {
    SPDLOG_ERROR("nvme-cmd ZEROES does not supported\n");
    return -1;
  }
  cfs_tid_t tid = cfsGetTid();
  struct BdevIoContext *ctx_ptr = allocReqContext(tid);
  auto qp = tidQpairList[tid];
  // auto qpit = tidQpairMap.find(tid);
  // assert(qpit != tidQpairMap.end());
  // doesn't really matter, what we want is just the isDone variable
  ctx_ptr->buf = NULL;
  ctx_ptr->blockNo = lba;
  ctx_ptr->reqType = BlkDevReqType::BLK_DEV_REQ_WRITE;
  ctx_ptr->isDone = false;

  int rc = spdk_nvme_ns_cmd_write_zeroes(gNamespaces->ns, qp, lba, lba_count,
                                         blocking_write_complete, ctx_ptr, 0);

  if (rc < 0) return rc;

  while (!(ctx_ptr->isDone)) {
    checkCompletion(0);
  }

  releaseReqContext(ctx_ptr);
  return rc;
}

void BlkDevSpdk::addController(struct ctrlr_entry *entry) {
  entry->next = gControllers;
  gControllers = entry;
}

int BlkDevSpdk::initWorker(int wid) {
  cfsSetTid(wid);
  cfs_tid_t tid = cfsGetTid();
  if (tidQpairList[tid] != nullptr) {
    SPDLOG_WARN("initWorker: this worker has been set to SpdkDev, but it's ok");
    return -1;
  }
  // if (tidQpairMap.find(tid) != tidQpairMap.end()) {
  //
  // }
  std::lock_guard<std::mutex> lock(lmtx);
  {
    // init qpair for this thread
    struct spdk_nvme_qpair *p =
        spdk_nvme_ctrlr_alloc_io_qpair(gNamespaces->ctrlr, NULL, 0);
    if (p == nullptr) {
      SPDLOG_ERROR("ERROR cannot allocate qpair for thread {}", tid);
      return -1;
    }
    int curWid = wid;
    threadInflightWriteReqNumMap[curWid] = 0;
    threadInDevQueueReqNumVec[curWid] = 0;
    threadAllocCntVec[curWid] = 0;
    threadDoneCntVec[curWid] = 0;
    lastReportTs[curWid] = PerfUtils::Cycles::rdtsc();
    if (tid != curWid) {
      throw std::runtime_error("Sorry tid wid not match");
    }
    tidWidList[tid] = curWid;
    tidQpairList[tid] = p;
    fprintf(stderr, "====== tid:%d wid:%d qpair:%p\n", tid, curWid, p);
    SPDLOG_DEBUG("initWorker-AllocateQpair-tid:{} wid:{} qpir:{}", tid, curWid,
                 reinterpret_cast<uintptr_t>(p));
    // init the req context list
    struct BdevIoContext *ctx_ptr;
    for (int i = 0; i < kThreadMaxInflightReqs; i++) {
      tidUnusedReqidList[tid].push_back(i);
      ctx_ptr = new BdevIoContext();
      ctx_ptr->tid = tid;
      ctx_ptr->rid = i;
      tidReqvecList[tid][i] = ctx_ptr;
    }
    _readyWorkerNum++;
    SPDLOG_INFO("readWorkerNum:{}", _readyWorkerNum);
    if (_readyWorkerNum == _workerNum) {
      bool expct = false;
      while (!allWorkerReady.compare_exchange_weak(expct, true) && !expct)
        ;
    }
  }
  SPDLOG_INFO("initWorker Completed for tid:{} wid:{}", tid, wid);
  return 0;
}

int BlkDevSpdk::cleanup() {
  struct ns_entry *ns_entry = gNamespaces;
  struct ctrlr_entry *ctrlr_entry = gControllers;
  for (auto ele : tidQpairList) {
    if (ele != nullptr) {
      spdk_nvme_ctrlr_free_io_qpair(ele);
    }
  }
  tidQpairList.clear();

  // for (auto it = tidQpairMap.begin(); it != tidQpairMap.end(); ++it) {
  //   // free qpair
  //   spdk_nvme_ctrlr_free_io_qpair(it->second);
  // }
  // tidQpairMap.clear();

  while (ns_entry) {
    struct ns_entry *next = ns_entry->next;
    delete (ns_entry);
    ns_entry = next;
  }

  while (ctrlr_entry) {
    struct ctrlr_entry *next = ctrlr_entry->next;
    spdk_nvme_detach(ctrlr_entry->ctrlr);
    delete (ctrlr_entry);
    ctrlr_entry = next;
  }

  return 0;
}

int BlkDevSpdk::readSector(uint64_t sectorNo, char *data) {
  SPDLOG_DEBUG("readSector - sectorNo:{}", sectorNo);
  return submitDevReq(sectorNo, 0, data, BlkDevReqType::BLK_DEV_REQ_SECTOR_READ,
                      (SSD_SEC_SIZE));
}

int BlkDevSpdk::blockingReadSector(uint64_t sectorNo, char *data) {
  return submitBlockingDevReq(
      sectorNo, data, BlkDevReqType::BLK_DEV_REQ_SECTOR_READ, (SSD_SEC_SIZE));
}

int BlkDevSpdk::writeSector(uint64_t sectorNo, uint64_t sectorNoSeqNo,
                            char *data) {
  return submitDevReq(sectorNo, sectorNoSeqNo, data,
                      BlkDevReqType::BLK_DEV_REQ_SECTOR_WRITE, (SSD_SEC_SIZE));
}

int BlkDevSpdk::blockingWriteSector(uint64_t sectorNo, uint64_t sectorNoSeqNo,
                                    char *data) {
  return submitBlockingDevReq(
      sectorNo, data, BlkDevReqType::BLK_DEV_REQ_SECTOR_WRITE, (SSD_SEC_SIZE));
}

void BlkDevSpdk::statsReport() {
  // auto cur_time = tap_ustime();
  // lastStatReportTime = cur_time;
  int curWid = tidWidList[cfsGetTid()];
  uint64_t curTs = PerfUtils::Cycles::rdtsc();
  auto curAllocCntPtr = &(threadAllocCntVec[curWid]);
  float time_lapse = PerfUtils::Cycles::toSeconds(curTs - lastReportTs[curWid]);
  float average = float(threadInDevQueueReqNumVec[curWid]) / (*curAllocCntPtr);
  float avgIOPS = float(threadDoneCntVec[curWid]) / time_lapse;
  float avgJournal =
      float(gFsProcPtr->QueryWorkerJournalMngNumWrite(curWid, true)) /
      time_lapse;

  std::cout << "[BlkDevSpdk]-wid-" << curWid
            << " averageInflightNum:" << average << " iops:" << avgIOPS
            << " journal iops:" << avgJournal << " sum:" << avgJournal + avgIOPS
            << std::endl;
  threadInDevQueueReqNumVec[curWid] = 0;
  *curAllocCntPtr = 0;
  threadDoneCntVec[curWid] = 0;
  lastReportTs[curWid] = curTs;
}

struct BdevIoContext *BlkDevSpdk::allocReqContext(cfs_tid_t tid) {
  if (isReportStats) {
    int curWid = tidWidList[cfsGetTid()];
    auto curAllocCntPtr = &(threadAllocCntVec[curWid]);
    if (*curAllocCntPtr == (kReportAllocCalledNumThreshold - 1)) {
      statsReport();
    }
    threadInDevQueueReqNumVec[curWid] +=
        kThreadMaxInflightReqs - tidUnusedReqidList[tid].size();
    threadAllocCntVec[curWid]++;
  }
  if (tidUnusedReqidList.empty() || tidUnusedReqidList[tid].empty()) {
    SPDLOG_INFO("Cannot allocate BdevIoContext");
    return nullptr;
  }
  // do the allocation
  bdev_reqid_t rid = tidUnusedReqidList[tid].front();
  tidUnusedReqidList[tid].pop_front();
  struct BdevIoContext *ctx_ptr = (tidReqvecList[tid])[rid];
  ctx_ptr->isDone = false;
  return ctx_ptr;
}

void BlkDevSpdk::releaseReqContext(struct BdevIoContext *ctx) {
  cfs_tid_t tid = ctx->tid;
  ctx->buf = nullptr;
  tidUnusedReqidList[tid].push_back(ctx->rid);
  threadDoneCntVec[tid]++;
}

int BlkDevSpdk::reduceInflightWriteNum(int num) {
#if 0
  int curWid = tidWidMap[cfsGetTid()];
  threadInflightWriteReqNumMap[curWid] -= num;
  auto curval = threadInflightWriteReqNumMap[curWid];
  if (curval < 0) {
    // fprintf(stderr,
    //        "threadInflightWriteReqNum - result:%d reduced by :%d curTid_%d, "
    //        "sizeOfMap:%ld\n",
    //        curval, num, curWid, threadInflightWriteReqNumMap.size());
    return -1;
  }
#endif
  return 0;
}

struct spdk_nvme_ns *BlkDevSpdk::getCurrentThreadNS(void) {
  // Only one namespace, no need to lookup based on thread
  return gNamespaces->ns;
}

struct spdk_nvme_qpair *BlkDevSpdk::getCurrentThreadQPair(void) {
  cfs_tid_t tid = cfsGetTid();
  auto qp = tidQpairList[tid];
  // auto qpIt = tidQpairMap.find(tid);
  // if (qpIt == tidQpairMap.end()) throw std::runtime_error("qpair not found");
  if (qp == nullptr) throw std::runtime_error("qpair not found");

  return qp;
}

// @param maxCmplNum: # of completion fo be processed, 0 --> unlimited
// @return # of completion processed (can be 0, <0 --> error)
int BlkDevSpdk::checkCompletion(int maxCmplNum) {
  cfs_tid_t tid = cfsGetTid();
  // auto qpIt = tidQpairMap.find(tid);
  auto qp = tidQpairList[tid];
  if (qp == nullptr) {
    // if (qpIt == tidQpairMap.end()) {
    fprintf(stderr, " \t=====>cannot find qpair for tid:%d  ", tid);
    // for (auto k : tidQpairMap) {
    //   fprintf(stderr, "tidQpairMap:%d\t", k.first);
    // }
    // fprintf(stderr, "\n");
    // for (auto k : tidUnusedReqidList) {
    //   fprintf(stderr, "tidQpairMap:%d\t", k.size());
    // }
    fprintf(stderr, "\n");
    throw std::runtime_error("qpair cannot found");
  }

  int inflightReqNum = kThreadMaxInflightReqs - tidUnusedReqidList[tid].size();
  int checkNum = maxCmplNum == 0 ? 0 : std::max(maxCmplNum, inflightReqNum);

  //  if (inflightReqNum > 0) {
  //    fprintf(stderr,
  //            "checkCompletion: checkNum:%d qpair:%p wid:%d inflightNum:%d
  //            tid:%p\n", checkNum, tidQpairMap[tid], tidWidMap[tid],
  //            inflightReqNum, &tid);
  //  }
  // return spdk_nvme_qpair_process_completions(tidQpairMap[tid], checkNum);
  return spdk_nvme_qpair_process_completions(tidQpairList[tid], checkNum);
}

void BlkDevSpdk::releaseBdevIoContext(struct BdevIoContext *ctx) {
  releaseReqContext(ctx);
}

// gFsProcPtr only used by these call back functions
extern FsProc *gFsProcPtr;

// Callback functions
// triggered by checkCompletion(int)
// will submit request to per thread completion requests queue
static void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  struct BdevIoContext *ctx = (struct BdevIoContext *)arg;
  gFsProcPtr->submitDevAsyncReadReqCompletion(ctx);
}

static void write_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  struct BdevIoContext *ctx = (struct BdevIoContext *)arg;
  gFsProcPtr->submitBlkWriteReqCompletion(ctx);
}

static void blocking_read_complete(void *arg,
                                   const struct spdk_nvme_cpl *completion) {
  struct BdevIoContext *ctx = (struct BdevIoContext *)arg;
  ctx->isDone = true;
}

static void blocking_write_complete(void *arg,
                                    const struct spdk_nvme_cpl *completion) {
  struct BdevIoContext *ctx = (struct BdevIoContext *)arg;
  ctx->isDone = true;
}

// ------------------------------------------------------------------------
// BlkDevPosix
// ------------------------------------------------------------------------

#ifndef USE_SPDK
BlkDevPosix::BlkDevPosix(const std::string &path, uint32_t blockNum,
                         uint32_t blockSize)
    : BlkDevSpdk(true, path, blockNum, blockSize), diskFile(nullptr) {}

BlkDevPosix::BlkDevPosix(const std::string &path, uint32_t blockNum,
                         uint32_t blockSize, std::string configName)
    : BlkDevSpdk(true, path, blockNum, blockSize), diskFile(nullptr) {
  configFileName = std::move(configName);
}

BlkDevPosix::~BlkDevPosix() {}

int BlkDevPosix::devInit() {
  diskFile = fopen(devPath.c_str(), "r+");
  if (!diskFile) {
    SPDLOG_WARN("ERROR cannot open DISK_FILE:{}. errStr:{}", devPath,
                strerror(errno));
    SPDLOG_INFO("Try to create the DISK_FILE");
    diskFile = fopen(devPath.c_str(), "w+");
    if (!diskFile) {
      SPDLOG_ERROR("Cannot create the DISK_FILE");
      return -1;
    }
  }
  fseek(diskFile, 0L, SEEK_END);
  long int res = ftell(diskFile);
  if (res < (BLK_DEV_POSIX_FILE_SIZE_BYTES)) {
    SPDLOG_WARN("size of file:{}, need to resize", res);
    int rc = truncate(devPath.c_str(), (BLK_DEV_POSIX_FILE_SIZE_BYTES));
    if (rc < 0) {
      SPDLOG_ERROR("Cannot truncate DISK_FILE. err:{}", strerror(errno));
      return -1;
    }
  }

  // reset the cursor
  fseek(diskFile, 0L, SEEK_SET);

  SPDLOG_INFO("[BlkDevPosix] Device ready!");
  return 0;
}

int BlkDevPosix::read(uint64_t blockNo, char *data) {
  int rc = fseek(diskFile, blockNo * devBlockSize, SEEK_SET);
  if (rc < 0) {
    SPDLOG_DEBUG("ERROR cannot fseek");
    return -1;
  }
  if (fread(data, devBlockSize, 1, diskFile) != 1) {
    SPDLOG_ERROR("Cannot read file: {}", strerror(3));
    return -1;
  }
  return 0;
}

int BlkDevPosix::readSector(uint64_t secNo, char *data) {
  // SPDLOG_DEBUG("Posix::readSecotr(secNo:{})", secNo);
  int rc = fseek(diskFile, secNo * (SSD_SEC_SIZE), SEEK_SET);
  if (rc < 0) {
    SPDLOG_DEBUG("BlkDevPosix::readSector ERROR cannot fseek");
    return -1;
  }
  if (fread(data, (SSD_SEC_SIZE), 1, diskFile) != 1) {
    SPDLOG_ERROR("BlkDevPosix::readSector Cannot read file: {}", strerror(3));
    return -1;
  }
  return 0;
}

int BlkDevPosix::blockingWriteSector(uint64_t secNo, uint64_t sectorNoSeqNo,
                                     char *data) {
  // SPDLOG_DEBUG("Posix::blockingWriteSector(secNo:{})", secNo);
  fseek(diskFile, secNo * (SSD_SEC_SIZE), SEEK_SET);
  if (fwrite(data, (SSD_SEC_SIZE), 1, diskFile) != 1) {
    SPDLOG_ERROR("Cannot write file");
    return -1;
  }
  return 0;
}

int BlkDevPosix::write(uint64_t blockNo, uint64_t blockNoSeqNo, char *data) {
  fseek(diskFile, blockNo * devBlockSize, SEEK_SET);
  if (fwrite(data, devBlockSize, 1, diskFile) != 1) {
    SPDLOG_ERROR("Cannot write file");
    return -1;
  }
  return 0;
}

void *BlkDevPosix::zmallocBuf(uint64_t size, uint64_t align) {
  return malloc(size);
}

int BlkDevPosix::freeBuf(void *ptr) {
  if (ptr == NULL) {
    return -1;
  }
  free(ptr);
  return 0;
}

int BlkDevPosix::devExit() {
  if (diskFile) {
    SPDLOG_INFO("Close file ...");
    fclose(diskFile);
    diskFile = NULL;
  }
  return 0;
}

int BlkDevPosix::blockingRead(uint64_t blockNo, char *data) {
  return read(blockNo, data);
}

int BlkDevPosix::blockingWrite(uint64_t blockNo, char *data) {
  return write(blockNo, 0, data);
}

int BlkDevPosix::blockingWriteMultiBlocks(uint64_t blockStartNo, int numBlocks,
                                          char *data) {
  int rt = 0;
  for (int i = 0; i < numBlocks; i++) {
    rt = write(blockStartNo + i, 0, data + i * (devBlockSize));
    if (rt < 0) {
      break;
    }
  }
  return rt;
}

//
// NOTE: all the following functions are not supported for posix device
// annotated by *DO_NOT_SUPPORT*
//

// DO_NOT_SUPPORT
void BlkDevPosix::addController(struct ctrlr_entry *entry) { throw; }
// DO_NOT_SUPPORT
void BlkDevPosix::registerNamespace(struct spdk_nvme_ctrlr *ctrlr,
                                    struct spdk_nvme_ns *ns) {
  throw;
}
// DO_NOT_SUPPORT
int BlkDevPosix::initWorker(int wid) { return 0; }
// DO_NOT_SUPPORT
int BlkDevPosix::checkCompletion(int maxCmplNum) { throw; }
// DO_NOT_SUPPORT
void BlkDevPosix::releaseBdevIoContext(struct BdevIoContext *ctx) { throw; }

// DO_NOT_SUPPORT
// because the posix based device is a *blocking device*
int BlkDevPosix::reduceInflightWriteNum(int num) { return 0; }

// DO_NOT_SUPPORT
int BlkDevPosix::cleanup(void) { return 0; }
#endif
