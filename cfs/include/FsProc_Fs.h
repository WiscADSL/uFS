#ifndef CFS_FS_H
#define CFS_FS_H

#include <folly/concurrency/ConcurrentHashMap.h>

#include <array>
#include <map>
#include <set>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "BlkDevSpdk.h"
#include "BlockBuffer.h"
#include "FsLibMalloc.h"
#include "FsLibProc.h"
#include "FsProc_App.h"
#include "FsProc_FileMng.h"
#include "FsProc_FsInternal.h"
#include "FsProc_FsReq.h"
#include "FsProc_Journal.h"
#include "FsProc_KnowParaLoadMng.h"
#include "FsProc_LoadMng.h"
#include "FsProc_Messenger.h"
#include "FsProc_SplitPolicy.h"
#include "FsProc_UnixSock.h"
#include "FsProc_WorkerComm.h"
#include "FsProc_WorkerStats.h"
#include "concurrentqueue.h"
#include "config4cpp/Configuration.h"
#include "perfutil/Stats.h"
#include "shmipc/shmipc.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "tbb/concurrent_unordered_map.h"
#include "util.h"

//
class FsProcWorker;
//

static void inline SpinSleep(uint64_t ns) {
  uint64_t cur_ts = 0;
  uint64_t start_ts = PerfUtils::Cycles::rdtsc();
  volatile int waitNum = 0;
  while (true) {
    cur_ts = PerfUtils::Cycles::rdtsc();
    // c++20 deprecate ‘++’ expression of ‘volatile’
    waitNum = waitNum + 1;
    if (PerfUtils::Cycles::toNanoseconds(cur_ts - start_ts) > ns) {
      break;
    }
  }
}

struct FsProcConfig {
  int raNumBlock = 16;
  int splitPolicyNum{SPLIT_DEFAULT_POLICY_NUM};
  int serverCorePolicyNo{SERVER_CORE_ALLOC_POLICY_NOOP};
  float lb_cgst_ql{0};
  float nc_percore_ut{-1};
  float dirtyFlushRatio{0.9};
  config4cpp::Configuration *cfg{nullptr};

  // NOTE: assume the fields in configuration file is exactly the same as the
  // variable in this struct
  void initConfigByFile(const std::string &fname) {
    cfg = config4cpp::Configuration::create();
    try {
      cfg->parse(fname.c_str());
      splitPolicyNum = cfg->lookupInt("", "splitPolicyNum", splitPolicyNum);
    } catch (const config4cpp::ConfigurationException &ex) {
      SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
    }

    try {
      serverCorePolicyNo =
          cfg->lookupInt("", "serverCorePolicyNo", serverCorePolicyNo);
    } catch (const config4cpp::ConfigurationException &ex) {
      SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
    }

    try {
      dirtyFlushRatio =
          cfg->lookupFloat("", "dirtyFlushRatio", dirtyFlushRatio);
    } catch (const config4cpp::ConfigurationException &ex) {
      SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
    }

    try {
      raNumBlock = cfg->lookupInt("", "raNumBlock", raNumBlock);
    } catch (const config4cpp::ConfigurationException &ex) {
      SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
    }

    try {
      lb_cgst_ql = cfg->lookupFloat("", "lb_cgst_ql", lb_cgst_ql);
    } catch (const config4cpp::ConfigurationException &ex) {
      SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
    }

    try {
      nc_percore_ut = cfg->lookupFloat("", "nc_percore_ut", nc_percore_ut);
    } catch (const config4cpp::ConfigurationException &ex) {
      SPDLOG_INFO("Config Parse Error:{}", ex.c_str());
    }

    cfg->destroy();
    cfg = nullptr;
  }
};

class GlobalAppShmIds {
 public:
  explicit GlobalAppShmIds() {}

  void AddAppShm(pid_t pid, uint8_t id, SingleSizeMemBlockArr *arr) {
    auto &app_shm_map = app_shmid_arr_map_[pid];
    app_shm_map[id] = arr;
  }

  SingleSizeMemBlockArr *QueryAppShm(pid_t pid, uint8_t id) {
    auto it = app_shmid_arr_map_.find(pid);
    if (it == app_shmid_arr_map_.end()) return nullptr;
    auto in_it = it->second.find(id);
    if (in_it == it->second.end()) return nullptr;
    return in_it->second;
  }

  // NOTE: make sure only master call it and no chance any reader is
  // going on, we could ensure these since fs_exit() does not need shms
  void HandleAppExit(pid_t pid) { app_shmid_arr_map_.unsafe_erase(pid); }

  // written by the master and read by all the workers
  tbb::concurrent_unordered_map<
      pid_t, tbb::concurrent_unordered_map<uint8_t, SingleSizeMemBlockArr *>>
      app_shmid_arr_map_;
};

class GlobalAppCredTable {
 public:
  explicit GlobalAppCredTable() {}
  void AddAppCredential(struct ucred &cred, int idx) {
    assert(idx >= 0);
    auto app_cred = new AppCredential(cred.pid, cred.gid, cred.uid, idx);
    app_cred_map[cred.pid] = app_cred;
  }

  int QueryAppCred(pid_t pid, AppCredential &app_cred) {
    auto it = app_cred_map.find(pid);
    if (it == app_cred_map.end()) return -1;
    // copy it
    app_cred = *(it->second);
  }

  void HandleAppExit(pid_t pid) {
    // NOTE: for now we don't do anything here, since we will never delete
    // something from appMap, thus want to keep exited credential there
    // to make sure GetAllAppCred() works with *min_num_app*
    // auto it = app_cred_map.find(pid);
    // if (it != app_cred_map.end()) {
    //   auto app_cred = it->second;
    //   app_cred_map.unsafe_erase(it);
    //   delete app_cred;
    // }
  }

  void GetAllAppCred(std::vector<std::pair<pid_t, AppCredential *>> &ret_vec,
                     size_t min_num_app) {
    auto cur_size = app_cred_map.size();
    // if current num app is <= min_num_app, avoid the costly iteration and copy
    if (cur_size <= min_num_app) return;
    ret_vec.reserve(cur_size);
    for (const auto [pid, cred_ptr] : app_cred_map) {
      ret_vec.push_back({pid, new AppCredential(*cred_ptr)});
    }
  }

 private:
  // will only be written by the socket listener
  tbb::concurrent_unordered_map<pid_t, AppCredential *> app_cred_map;
};

class FsProc {
 public:
  using fsproc_wid_t = int;

  static const uint64_t kCheckExitLoopNum = 50000000L;
  const char *kLogDir = "logs/";

  // The appearance of this file indicates that FSP needs to exit
  const char *exitSignalFileName;

  // TODO: do not make this public
  PageCacheManager *pageCacheMng{nullptr};

  // global valid knowledge of app data shm
  GlobalAppShmIds g_app_shm_ids;

  // global valid knowledge of app credential
  GlobalAppCredTable g_cred_table;

  FsProc(int tNum, int appNum, const char *exitFname);
  FsProc(bool doInline);
  ~FsProc() {}

  int getNumThreads() { return numThreads; }

  // wrap the generation of log's names
  // @param ssLogger : store the name of logger object
  // @param ssFile : store the name of log's file
  void getWorkerLogName(std::stringstream &ssLogger, std::stringstream &ssFile,
                        int wid);

  // start workers for FSP
  // @param shmOffsetVec
  // @param devVec: each worker will manage one item in devVec
  //    one identical dev can be presented in this vec several times.
  // @param workerCoresVec: cores to pin each worker on.
  void startWorkers(std::vector<int> &shmOffsetVec,
                    std::vector<CurBlkDev *> &devVec,
                    std::vector<int> &workerCoresVec);

  // start inline worker (main thread itself as worker)
  // mostly for testing and data directly R/W
  // @param loadFs: whether or not load FS data buffers
  //        if loadFs is true, it will setup all the necessary runtime of fsp,
  //        but without start the running loop.
  FsProcWorker *startInlineWorker(CurBlkDev *dev, bool loadFs);

  // Stop FSP, will do destruction
  // Note: the argument for #startWorkers()'s CurBlkDev is not destructed.
  void stop();

  // all the callback functions of device will share this entry.
  int submitDevAsyncReadReqCompletion(struct BdevIoContext *ctx);
  int submitBlkWriteReqCompletion(struct BdevIoContext *ctx);

  // A temporary function to pre-initialize some apps
  // Must be called after the worker is fully initialized
  void initAppsToWorker(FsProcWorker *worker);

  // Workers notify FsProc that they are ready to accept requests
  // by calling workerReady just before they start polling for requests.
  void workerReady(void);

  // access to workerActive array
  bool checkWorkerActive(int wid) {
    assert(wid < numThreads);
    return workerActive[wid];
  }

  void setWorkerActive(int wid, bool state) {
    assert(wid < numThreads);
    assert(state != workerActive[wid]);
    workerActive[wid] = state;
  }

  uint64_t QueryWorkerJournalMngNumWrite(int wid, bool do_reset);

  void redirectZombieAppReqs(int wid);

  bool checkNeedOutputLoadStats() { return outputLoadStats; }

  void setNeedOutputLoadStats(bool b) { outputLoadStats = b; }

  void setConfigFname(const char *fname) {
    assert(fname != nullptr);
    configFname = std::string(fname);
    config.initConfigByFile(configFname);
  }

#if CFS_JOURNAL(GLOBAL_JOURNAL)
  JournalManager *GetPrimaryJournalManager();
#endif

  int getRaNumBlock() { return config.raNumBlock; }
  int getSplitPolicy() { return config.splitPolicyNum; }
  int getServerCorePolicyNo() { return config.serverCorePolicyNo; }
  float GetLbCgstQl() { return config.lb_cgst_ql; }
  float GetNcPerCoreUt() { return config.nc_percore_ut; }
  float getDirtyFlushRato() { return config.dirtyFlushRatio; }

  // send message to the primary worker to initialize the checkpointing
  // @return: -1 if cannot send message, 0 on success
  int proposeJournalCheckpointing();

  bool setInCheckpointing() {
    if (inGlobalCheckpointing) return false;
    inGlobalCheckpointing = true;
    return true;
  }
  bool resetInCheckpointing() {
    if (!inGlobalCheckpointing) return false;
    inGlobalCheckpointing = false;
    return true;
  }

  void performCheckpointing(int wid);

  void CheckNewedInMemInodeDst(int wid, cfs_ino_t ino, int pid, int tid);

  void HandleManualReassignForLoadMng(int dst_wid, pid_t pid, int tid) {
    loadMng->ManualThreadReassignment(dst_wid, pid, tid);
  }

#ifdef UFS_SOCK_LISTEN
  void RetireAppIdx(int app_idx) { retired_app_idx_.emplace(app_idx); }

  int GetNextAppIdxThenIncr() {
    int cur_idx = rgst_app_idx_incr_++;
    if (cur_idx >= numAppProc) {
      rgst_app_idx_incr_--;
      // try to find an app_idx from the retired idx
      if (!retired_app_idx_.empty()) {
        auto it = retired_app_idx_.begin();
        cur_idx = *it;
        retired_app_idx_.erase(it);
        return cur_idx;
      }
      std::cerr << "app_idx:" << cur_idx << std::endl;
      throw std::runtime_error("app idx overflow");
    }
    return cur_idx;
  }

  int GetNextAppIdxNoIncr() { return rgst_app_idx_incr_; }
  int GetWorkerShmkeyDistance() { return worker_shmkey_distance; }
#endif

 private:
  // number of workers to carry FSP function
  int numThreads;
  // number of pre-registered apps
  int numAppProc;
  // each variable control the runnable status of one single worker
  // This controls the start/exit of FSP workers
  // once this is set to false, worker will reach the end of lifetime
  // REQUIRED: length(workerRunning) == numThreads
  std::atomic_bool *workerRunning;
  std::atomic_bool outputLoadStats{false};
  // by default (single-thread FSP), we log to console
  // If FSP has multiple threads, then will set this to true
  bool logToFile = false;

  std::string configFname;
  FsProcConfig config;

  std::unordered_map<cfs_tid_t, FsProcWorker *> workerMap;
  std::unordered_map<int, FsProcWorker *> widWorkerMap;
  std::vector<FsProcWorker *> workerList;
  std::vector<std::thread *> threadList;

  std::unordered_map<int, worker_stats::RecorderType *> recorders_;

  // state of a worker whether it is asleep or not
  std::atomic_bool *workerActive;

#ifdef UFS_SOCK_LISTEN
  // a thread that listens UNIX domain socket to establish
  // connection with clients
  std::thread *sock_listen_thread_{nullptr};
  fsp_sock::UnixSocketListener *sock_listener_{nullptr};
  // used for assign app id slot for the pre-allocated msg rings
  // std::atomic<int> rgst_app_idx_incr_{0};
  int rgst_app_idx_incr_{0};
  std::set<int> retired_app_idx_;
  int worker_shmkey_distance{-1};
#endif

  // load managing thread
  std::thread *loadMngThread_{nullptr};
  worker_stats::LoadMngType *loadMng{nullptr};

  // if FsProc is in checkpointing
  // NOTE: this will only be modified by the worker that decides to
  // start the checkpointing and monitor the completion of all checkpointing
  std::atomic_bool inGlobalCheckpointing;

  void cleanup();
};

// A FS Worker's load stats (Local representation)
class FsWorkerLoadStatsLocal {
 public:
  perfstat_cycle_t idleStartTs = 0;
  perfstat_cycle_t lastResetTs = 0;
  perfstat_cycle_t totalIdleCycles = 0;
  bool lastLoopIsEffective = false;
  uint32_t thisWindowEffectNum = 0;
  // a local copy of LoadStatsSR
  // we will always make a copy to the loadStatsSR that is visible
  // to the monitoring thread
  // We delagate the calculation of several fields to FsReqPool:
  //     - opTotalEnqueueReadyCnt
  //     - readyQueueLenAccumulate
  //     - secMaxInodeAccessCnt
  fsp_lm::PerWorkerLoadStatsSR localSRCopy;
};

struct FsWorkerOpStats {
  // if collect Per operation stats.
  // NOTE: if set to true, each op will invoke one additional timestamp
  static constexpr bool kCollectWorkerOpStats = false;

  static bool isAccountingFsReqType(FsReqType &typ) {
    switch (typ) {
      case FsReqType::ALLOC_PREAD:
      case FsReqType::STAT:
        return true;
      default:
        return false;
    }
  }

  uint64_t numOpDone{0};
  uint64_t firstOpNano{0};
  uint64_t lastOpNano{0};
  uint64_t doneBytes{0};
  void outputToLogFile(std::shared_ptr<spdlog::logger> logger) {
    double intervalNano = (lastOpNano - firstOpNano) == 0
                              ? 1
                              : static_cast<double>(lastOpNano - firstOpNano);
    // raw stats
    logger->info(
        "[opstats] firstNs:{} lastNs:{} intervalNs:{} totalBytes:{} numOp:{}",
        firstOpNano, lastOpNano, intervalNano, doneBytes, numOpDone);
    logger->info("[opstats] iops:{} bw(MB/s):{}",
                 double(numOpDone) / (1e-9 * intervalNano),
                 double(doneBytes / 1048576.0) / (1e-9 * intervalNano));
  }
};

//
// See fsMain.cc for "HOWTO" use FsProcWorker
//
class FsProcWorker {
 public:
  // Interval for reporting CPU usage (in us).
  static constexpr int64_t kCpuReportInterval = 1e6;  // set to 1s
  // The default workerId, which handles all the metadata operation
  // FIXME: the code uses wid and workerIdx interchangeably. We should just use
  // one to avoid confusion.
  static constexpr int kMasterWidConst = APP_CFS_COMMON_KNOWLEDGE_MASTER_WID;

  // error codes
  static constexpr int kSplitNot = -9990;  // not in splitting
  static constexpr int kSplitJoinErrBase = -9999;
  static constexpr int kOnTargetFiguredOutErrInSplitting = -10000;
  static constexpr int kOnTargetFiguredOutErrNeedRedirect = -10001;
  static constexpr int kOnTargetFiguredOutErrUnlinkRedirected = -10002;
  static constexpr int kOnTargetFiguredOutErrRenameRedirected = -10003;
  static constexpr int kOnTargetFiguredOutErrInJoining = -10004;

  JournalManager *jmgr;
  FsProcMessenger *messenger;

  // inotau_map_t inotauTickMap;

  // save inos moved
  std::unordered_set<cfs_ino_t> moved_ino_set;

  static bool inline isSplitError(int errNo) {
    return errNo <= kSplitJoinErrBase;
  }

  static int calWorkerIdx(FsProc::fsproc_wid_t wid) {
    return wid - kMasterWidConst;
  }

  // This set sets the request type that will be tracked and dump the stats in
  // bio layer.
  const std::unordered_set<FsReqType> kFsReqBioNumTracking = {};
  // E.g., if we track PREAD's bio
  // const std::unordered_set<FsReqType> kFsReqBioNumTracking =
  // {FsReqType::PREAD};

  FsProcWorker(FsProc::fsproc_wid_t wid, CurBlkDev *d, int shmBaseOffset,
               std::atomic_bool *workerRunning, PerWorkerLoadStatsSR *stats);
  // ensures to invoke the actual destructor
  virtual ~FsProcWorker();

  //
  // worker identity
  //
  // Note here, I try to decouple the WID vs. the idx
  // Basically, the idx is strictly started from 0, but wid can be computed
  // Let's see if someday master's crown can be transferred?
  virtual bool isMasterWorker() final { return wid == kMasterWidConst; }
  virtual FsProc::fsproc_wid_t getWid() final { return wid; }
  virtual int getWorkerIdx() const { return wid - kMasterWidConst; }
  static int fromIdx2WorkerId(int idx) { return kMasterWidConst + idx; }

  //
  // Pinning cores
  int getPinnedCPUCore() { return pinnedCPUCore; }
  void setPinnedCPUCore(int x) { pinnedCPUCore = x; }

  AppProc *GetApp(pid_t pid) {
    auto it = appMap.find(pid);
    if (it == appMap.end()) {
      return nullptr;
    }
    return it->second;
  }

  FsReq *genGenericRequest(FsReq::GenericCallbackFn fn, void *ctx) {
    auto req = fsReqPool_->genNewReq();
    req->setType(FsReqType::GENERIC_CALLBACK);
    req->generic_callback = fn;
    req->generic_callback_ctx = ctx;
    return req;
  }

  FsReq *GetEmptyRequestFromPool() { return fsReqPool_->genNewReq(); }
  //
  // logger
  //
  // init the log FILE (on-disk), by default will log to the *stdout/err*
  void initFileLogger(const std::string &loggerName,
                      const std::string &logFileName);

  // write to this worker's log
  // if logger is not initialized (e.g., only has one-thread fsp), will write
  // to the console
  // @param : isFlush indicates if flush the log
  // @return : false on error
  bool writeToWorkerLog(const std::string &str, bool isFlush = false);
  void writeToErrorLog(const std::string &str);
  void writeToDbgLog(const std::string &str);

  // mostly used when a background req is done, and we cannot return the request
  // in *submitFsReqCompleteion*
  void releaseFsReq(FsReq *req) { fsReqPool_->returnFsReq(req); }

  void ProcessLmRedirectCreation(LmMsgRedirectCreationCtx *ctx);
  inline void CheckCreationRedirection(pid_t pid, int tid, cfs_ino_t ino);

  void ProcessLmRebalance(LmMsgRedirectFileSetCtx *ctx);
  void ProcessLmRedirectFuture(LmMsgRedirectFutureCtx *ctx);
  void ProcessLmJoinAll(LmMsgJoinAllCtx *ctx);
  void ProcessLmJoinAllCreation(LmMsgJoinAllCreationCtx *ctx);

  int CheckFutureRoutingOnReqCompletion(AppProc *app, int tid,
                                        InMemInode *inode);

  // register a new application to FSP.
  // @param cred: credential information of application
  // will setup the communication channel of this application
  [[deprecated("use real kernel credential instead")]] virtual int initNewApp(
      struct AppCredential &cred);
  void InitEmptyApp(int num_app_obj);
  int InitCredToEmptyApp(struct AppCredential &cred);

  virtual void recvJoinAll(fsp_lm::PerWorkerLoadAllowance *allow) {}
  virtual void recvLoadRebalanceShare(fsp_lm::PerWorkerLoadAllowance *allow);

  // Used by device's callback for completion submission
  virtual int submitDevAsyncReadReqCompletion(struct BdevIoContext *ctx);
  virtual int submitBlkWriteReqCompletion(struct BdevIoContext *ctx);
  virtual int submitBlkFlushWriteReqCompletion(block_no_t bno,
                                               BufferFlushReq *flushReq);

  // req that does not need to do IO
  virtual void submitReadyReq(FsReq *req) { internalReadyReqQueue.push(req); }

  // will not go through FsImpl, like DirectIO
  virtual int submitDirectReadDevReq(BlockReq *req);
  virtual int submitDirectWriteDevReq(BlockReq *req);

  // several functions used to submit device IO request
  // submit a BIO request to device because of fsReq needs the data
  virtual int submitAsyncReadDevReq(FsReq *fsReq, BlockReq *blockReq);
  virtual int submitAsyncWriteDevReq(FsReq *fsReq, BlockReq *blockReq);

  // NOTE:  for these two submitPendOn????FsReq. it is the caller's job
  // to make sure the blockNo does not overlap or whaever, can retrieve
  // the corresponding fsReq
  // put fsReq in pending state, because the blockNo is in IO
  // @return : return number of request be pended, it is possible that, this Req
  //  will be directly put into the readyQueue
  virtual int submitPendOnBlockFsReq(FsReq *fsReq, uint64_t blockNo);
  // put fsReq in pending state, because the sectorNo is in IO
  virtual int submitPendOnSectorFsReq(FsReq *fsReq, uint64_t blockNo);

  virtual int submitAsyncBufFlushWriteDevReq(BufferFlushReq *bufFlushReq,
                                             BlockReq *blockReq);
  virtual void recordInoFsyncReqSubmitToDev(cfs_ino_t ino) {
    // fprintf(stderr, "===>recordInoFsyncReqSubmitToDev ino:%u\n", ino);
    inoFsyncLatestSeqNoMap[ino] = getNextFsyncSeq();
    latestFsyncSeqNo_ = inoFsyncLatestSeqNoMap[ino];
  }
  virtual void recordInoActiveAppFsync(uint32_t ino) {
    inoHasActiveAppFsyncSet.emplace(ino);
  }

  virtual void recordBatchInoActiveAppFsync(
      std::unordered_set<cfs_ino_t> &inodes) final {
    inoHasActiveAppFsyncSet.insert(inodes.cbegin(), inodes.cend());
  }

  virtual void recordInoAppFsyncDone(uint32_t ino) {
    inoHasActiveAppFsyncSet.erase(ino);
    fileManager->fsImpl_->resetFileIndoeDirty(ino);
    // fprintf(stderr, "==>recordInoAppFsyncDone. curLen:%ld\n",
    //        inoHasActiveAppFsyncSet.size());
  }

  virtual void recordBatchInoAppFsyncDone(
      std::unordered_set<cfs_ino_t> &inodes) final {
    for (const cfs_ino_t ino : inodes) {
      inoHasActiveAppFsyncSet.erase(ino);
      fileManager->fsImpl_->resetFileIndoeDirty(ino);
    }
  }

  // fsync()--> doesn not return, fsync() from different threads

  // check if a fsync for one ino should not be submitted directly, because
  // it is the latest fsynced one, and there is other ino that is currently
  // potentially waiting for fsync
  virtual bool checkInoFsyncToWait(cfs_ino_t ino) {
    auto it = inoFsyncLatestSeqNoMap.find(ino);
    if (it == inoFsyncLatestSeqNoMap.end()) {
      return false;
    }
    // uint64_t curSeqNo = inoFsyncLatestSeqNoMap[ino];
    uint64_t curSeqNo = it->second;
    bool rt = false;
    if (inoHasActiveAppFsyncSet.size() > BlockBuffer::kNumFgFlushLimit &&
        curSeqNo == latestFsyncSeqNo_) {
      // if the latest fsync was done for this seqNo and the there are other
      // fsync() requests that are waiting, put this one to readyList
      rt = true;
    }
    // fprintf(stderr,
    //        "==>checkInoFsyncToWait. ino:%u size:%ld curSeqNo:%lu, "
    //        "latestSeq:%lu return false:%d\n",
    //        ino, inoHasActiveAppFsyncSet.size(), curSeqNo, latestFsyncSeqNo_,
    //        rt);
    return rt;
  }
  virtual uint64_t getNextFsyncSeq() { return ++latestFsyncSeqNo_; }

  // when a state machine of one fsReq is finished
  virtual int submitFsReqCompletion(FsReq *fsReq);

  // static handlers for completion of different requests
  static void onSyncallCompletion(FsReq *req, void *ctx);
  //
  // Client Cache
  //
  // keep track of the inode that contains partial cache data at client-side
  // basically store all the corresponding WRITE/PWRITE requests in AppProc*
  // and if needs to flush, copy it to the FSP's buffer.
  // @return: -1 if error, 0 if ok
  virtual int addInAppInodeRWFsReq(FsReq *req, InMemInode *fileInode);
  // For READ_OP processing, retrieve the records that is kept in FSP to see
  // if the req's related blocks has already been in the client-cache
  // For the sequence number, it will set to the MAX(SeqNos) across all the
  // related blocks (for simplicity).
  // @param allInApp: if all the blocks are stored in App, set to true
  // @param maxSeq: the max seqNo across all the related blocks
  // @return: -1 if error, 0 if ok
  virtual int retrieveInAppInode4RWFsReq(FsReq *req, InMemInode *fileInode,
                                         bool &allInApp, uint64_t &maxSeq);
  // check if one inode is already in App
  // @return: if found, return the current pid, -1 if not found
  virtual bool checkInodeInOtherApp(uint32_t ino, pid_t curPid);
  // set one inode in Shared mode once find out that another App is actively
  // using this inode (return value of checkInodeInOtherApp())
  // @return: 0 if okay. -1 if error
  virtual int setInodeInShareMode(FsReq *req, InMemInode *fileInode);
  // when an inode's fd has a close() request, we can try to reset *SHARE* mode
  // TODO (jingliu) integrate calling of this func into processing of CLOSE_OP
  virtual int tryResetInodeShareMode(FsReq *req, InMemInode *fileInode);
  virtual uint64_t genNewRwOpSeqNo() { return inAppRwSeqNoIncr++; }

  //
  // Inode Split/Join
  //
  // All the workers must be able to record <ino, list<FsReq*>>
  // So, once the inode is clear to this FsReq (there are two cases:
  //   #1. need to do lookup, this will all be done in master thread, and once
  //     lookup is done (likely, the inode is fetched into memory), fileMng
  //     needs to set the fileInode into this fsReq and record this
  //   #2. no need to do lookup, then this fsReq can find the inode via
  //     AppProc's FileObj (fd), one the *FsReq* object is created, we can
  //     record this type of fsReq
  virtual void recordInProgressFsReq(FsReq *fsReq);  // use targetInode
  virtual void recordInProgressFsReq(FsReq *fsReq, InMemInode *inodePtr);
  // once fsReq is done, delete it from inProgress
  // a clear sign of this moment is the destructor of fsReq
  virtual void delInProgressFsReq(FsReq *fsReq);
  virtual size_t NumInProgressRequests(cfs_ino_t ino) final;

  virtual void ProcessManualThreadReassign(FsReq *req) {}

  virtual void ProcessLmRedirectOneTau(LmMsgRedirectOneTauCtx *ctx) {}

  // coordinate the process once the target inode is found
  // each worker need to record in its own context
  // for the inode that has already open()ed, it can be done once it is received
  // for the inode that needs lookup or allocation, it needs to go deeper into
  // the file system state machine processing
  // So the key moment is when the req's inode is first set from nullptr to
  // a valid InMemInode, we just need to record since that moment
  // NOTE: ONLY inode of type T_FILE need to invoke this once figured out
  //  i.e., must be the leaf of directory tree
  // E.g., one key state associated with each inode is its current owner
  virtual int onTargetInodeFiguredOut(FsReq *req, InMemInode *targetInode);
  // used for rename only
  virtual int onDstTargetInodeFiguredOut(FsReq *req,
                                         InMemInode *dstTargetInode);

  // after the FsReq is received, and *recordInProgressFsReq* is successful
  // on receiving (that is, we can retrieve the inode directly in memory, no
  // need to do any lookup)
  // i.e. the *<ino, set<InProgressFsReq>>* bookkeeping data structure has
  // new changes, then it is a good time to make this split/join decision
  // TODO (jingliu) : in the end, if this function is override in each subclass
  // we gotta make it pure virtual
  // @param splitErrNo : set to the error number due to *split/join*
  //   this indicates that this request need to be rejected immediately
  //   and how to reply to client ( #1. please request me again, #2. plase
  //   talk to another worker)
  // @param dstWid : set to the destination wid if a redirection is in need
  // NOTE: DEPRECATED
  virtual void checkSplitJoinOnFsReqReceived(FsReq *req, int &splitErrNo,
                                             int &dstWid) {}
  // Each worker should regularly check the commBridge between each of them
  // @return number of msgs processed
  virtual int checkSplitJoinComm() = 0;
  // Each Worker Class must be able to specify its capability in terms of
  // FsReqType
  virtual bool isFsReqTypeWithinCapability(const FsReqType &typ) = 0;
  // Each Worker Class must be able to specify the set of ReqType that it
  // does not care, or no need to record.
  // E.g., mkdir(), rmdir() for master, it will never happen in the servant
  virtual bool isFsReqNotRecord(const FsReqType &typ) = 0;
  // get the total memory size (in bytes) to store all the in-memory buffer
  // assume on of the worker is going to allocate this large of memory at a time
  // to use
  virtual uint64_t getTotalBlockBufferMemByte() {
    return FsImpl::totalBlockBufferMemByte();
  }

  //
  // main run-loop of this worker
  //
  virtual void workerRunLoop() = 0;

  // =======
  // NOTE: These function is only used for testing if called as public API,
  // otherwise should only be used by FsProc itself.
  // @param pid: if pid == 0, will pick one app if there is any in appMap
  virtual AppProc *testOnlyGetAppProc(pid_t pid);

  virtual void blockingFlushBufferOnExit() final;
  // =======

 protected:
  FsProc::fsproc_wid_t wid;
  int shmBaseOffset;
  CurBlkDev *dev;
  uint64_t loopCountBeforeCheckExit;
  std::thread *acceptHandler;
  int pinnedCPUCore{-1};

  // fileManger will execute FS functionality (~= VFS layer)
  FileMng *fileManager;
  // the buffer that is essentially HugePages allocated by SPDK interface.
  char *devBufMemPtr;

  // A variable which is read only to check if keep running or stop
  std::atomic_bool *workerRunning;

  // Identifier of application, incrementally assigned to NewedApp
  uint32_t appIdx;

  SplitPolicy *splitPolicy_{nullptr};
  SplitPolicyDynamicDstKnown *dst_known_split_policy_{nullptr};

  // FsReq tracing for one specific type of request
  uint64_t fsReqTraceTotalNs{0};
  uint64_t fsReqTraceTotalCnt{0};
  uint64_t fsReqTraceTotalFsm{0};
  uint64_t fsReqTraceTotalTickCounter{0};
  uint32_t fsReqTraceReportCounter{1000};

  worker_stats::RecorderType stats_recorder_;

  std::map<pid_t, std::map<int, int>> crt_routing_tb_;
  std::vector<std::pair<cfs_ino_t, int>> pending_crt_redirect_inos_;

  FsWorkerOpStats *opStatsPtr_{nullptr};
  cfs_tid_t workerTid_;

  FsReqPool *fsReqPool_{nullptr};

  std::queue<FsReq *> internalReadyReqQueue;
  std::queue<FsReq *> recvReadyReqQueue;

  std::unordered_map<pid_t, AppProc *> appMap;
  // pre-initialized app pool
  // index is used to match AppProc's appIdx, which should be
  // the unique identifier within the server
  // once an pid make sense comes, it will be put into appMap
  std::vector<AppProc *> app_pool_;
  uint32_t appmap_fill_loop_counter_ = 0;
  constexpr static uint32_t kAppMapFillLoopCnt = 1000000;

  // Used for master to note the redirection for new Inodes
  // *new Inodes* only generated when fetch it from the disk
  // or create one
  // once the pid+tid is handled by master itself, do erase
  // on forwarding through master, change the record
  int GetPidtidHandlingWid(pid_t pid, int tid) {
    auto it = pid_tid_handling_wid_.find(pid);
    if (it != pid_tid_handling_wid_.end()) {
      auto in_it = it->second.find(tid);
      if (in_it != it->second.end()) {
        return in_it->second;
      }
    }
    return -1;
  }

  std::unordered_map<pid_t, std::unordered_map<int, int>> pid_tid_handling_wid_;

  // <blockNo, queue of FsReq relies on this block>
  std::unordered_map<uint64_t, std::unordered_set<FsReq *>>
      blkReadPendingFsReqs;
  std::unordered_map<uint64_t, FsReq *> blkReadPendingSubmitFsReq;

  // <sectorNo, queue of FsReq relies on this sector>
  std::unordered_map<uint64_t, std::unordered_set<FsReq *>>
      sectorReadPendingFsReqs;
  std::unordered_map<uint64_t, FsReq *> sectorReadPendingSubmitFsReq;

  // For writing. (Not in use currently).
  std::unordered_map<uint64_t, std::list<FsReq *>> blockInflightWriteReqs;

  // For flushing.
  std::unordered_map<block_no_t, BufferFlushReq *> blockFlushReqMap;
  // ensure the fairness of each inode's flushing
  uint32_t inoFsyncResetCounter{0};
  uint64_t latestFsyncSeqNo_{0};
  std::unordered_map<cfs_ino_t, uint64_t> inoFsyncLatestSeqNoMap;
  std::unordered_set<cfs_ino_t> inoHasActiveAppFsyncSet;

  // <ino, set<FsReq*>>, record all the inProgressFsReq, thus while splitting
  // happens, we can either abort the fsReq or finish it first and then start
  // the splitting procedure
  std::unordered_map<cfs_ino_t, std::unordered_set<FsReq *>>
      inodeInProgressFsReqMap_;

  // keeps track of messages processed
  size_t num_messages_processed_{0};

 public:
  // FIXME: making this public for now while moving to a better reassignment
  // mechanism. We do not need this map later. Every inode will track the list
  // of apps (since it is anyway an app from inode to set of apps) Because
  // open() needs to always go to master currently, master always has the
  // valid information of inodeAppRefMap_
  std::unordered_map<cfs_ino_t, std::unordered_set<pid_t>> inodeAppRefMap_;

 protected:
  // Each vector item record the number of device access bio for single fsReq
  std::unordered_map<cfs_tid_t, std::vector<uint32_t>> workerFsReqNumBioMap_;

  // logger
  std::shared_ptr<spdlog::logger> logger;

  // <ino, App*> record if one inode is referred by one App process
  // when read/write a file (and client cache is enabled), this map is going
  // to be checked, if not existing, then add one entry, otherwise, will
  // disable client cache for certain inodes. Besides, we delay this *Sharing
  // invalidation* until each of App do *read/write* request. Because inode
  // might not be processing in the master FSP thread, so actually only data
  // plane operation will try to operation on this map Once in inode is
  // shared, it will be removed from this map and added into sharedInodes map,
  // that is, *activeNoShareInodes* and *shareInodes* has no overlap NOTE: now
  // these two maps are meaningful only if *client-cache* is enabled. Do I
  // need these maps for other purposes? NOTE (04/18): split/join needs
  // similar structure to keep track of which AppProc is referring one inode,
  // decide to let it kept only meaningful in ClientCache
  std::unordered_map<cfs_ino_t, AppProc *> activeNoShareInodes;
  // <inode, map of <pid, App*> that is current referring this inode>
  // records how an inodes is shared across different apps
  std::unordered_map<cfs_ino_t, std::unordered_map<pid_t, AppProc *>>
      sharedInodes;
  // Intend to make it start from 1, thus we can differentiate those without
  // assigning any seqNo ( == 0 <-> something wrong)
  constexpr static uint32_t kInAppRwSeqNoStart = 1;
  uint64_t inAppRwSeqNoIncr{kInAppRwSeqNoStart};

  // before processing request, we need to create a clientOp for the shmipc
  // msg. NOTE: The struct returned is allocated on the heap and needs to be
  // freed when the FsReq destructs.
  virtual struct clientOp *getClientOpForMsg(AppProc *app,
                                             struct shmipc_msg *msg,
                                             off_t ring_idx);

  virtual InMemInode *queryPermissionMap(FsReq *req, bool &reqDone);
  void inline RefillAppMap();
  int inline pollReqFromApps();
  // process request pointed by slotId for app (proc).
  virtual void processReqOnRecv(FsReq *req) final;
  virtual int processInternalReadyQueue();
  virtual void primaryHandleUnknownFdReq(FsReq *req) final;
  virtual void ownerProcessFdReq(FsReq *req) final;
  virtual void ownerProcessPathReq(FsReq *req) final;
  virtual void primaryProcessPathReq(FsReq *req) final;
  virtual void processClientControlPlaneReq(FsReq *req) final;
  virtual void ownerHandleUnknownPathReq(FsReq *req,
                                         FsPermission::PCR perm) final;

  // Called in new thread if the device is not initialized (probed into
  // memory), will init the device. It also record the running thread
  // information into device (IO data path).
  virtual int initMakeDevReady();
  virtual bool checkAllWorkerRegisteredToDev() {
    return dev->ifAllworkerReady();
  }

  // initialization work after dev is ready (build the in-memory buffers)
  // e.g., reading the super blocks, doAllocate the data buffer etc
  // REQUIRED: the calling thread of this function must have been recorded via
  // initMakeDevReady().
  virtual int initInMemDataAfterDevReady() = 0;

  virtual void initJournalManager(void);

  // retrieve the *AppProc* object that maintains certain inode's client cache
  // if the corresponding has not been recorded yet, will add that into
  // *activeNoShareInodes*
  // @param missAdd: if we cannot find the inode across current records of
  // <inode, AppProc>, either add inode record (missAdd == true) or return NOT
  // FOUND
  // @return: valid AppProc which record the blocks of that inodes that is in
  // client cache. return nullptr if error happens
  // errors:
  //   other app has already cached the inode, needs to trigger INVALIDATE
  // if App is not recorded -- serious error, will throw exception
  virtual AppProc *getInAppInodeCtx(FsReq *req, InMemInode *fileInode,
                                    bool missAdd);

  // pin worker to cpu (according to wid currently)
  virtual int pinToCpu();

  // this will be invoked inside onTargetInodeFiguredOut()
  // basically if one FsReq's inode is successfully set to a valid *inodePtr*
  // This will be called, and each sub-class is supposed to implement its own
  // bookkeeping, since they may have different data structures
  // REQUIRED: this function should only change the data that is utterly
  // defined by the subclass itself REQUIRED: if this returns < 0 (error),
  // then FsProcWorker will not put this request into <ino, list<FsReq>> for
  // in-progress req
  virtual int onTargetInodeFiguredOutChildBookkeep(FsReq *req,
                                                   InMemInode *targetInode) = 0;

  // InnerLoop of Worker's Job
  // @return if the loop is effective or not
  virtual bool workerRunLoopInner(perfstat_ts_t loop_start_ts) final;
  virtual void redirectZombieAppReqsToMaster() final {
    redirectZombieAppReqs(false);
  }
  // redirect the old app access
  // @param raiseError: if true, will directly reply error to client
  virtual void redirectZombieAppReqs(bool raiseError) final;

  virtual void monitorFsReqBio();

  // this will do op level accounting without considering the type of request
  // and user can let worker output this stats while exiting
  virtual void opStatsAccountSingleOpDone(FsReqType reqType,
                                          size_t bytes) final;

  virtual void processFsProcMessage(const FsProcMessage &msg) final;
  // @return number of msg processed
  virtual int processInterWorkerMessages() final;
  virtual int ProcessPendingCreationRedirect() final;

 public:
  // iterate App that are referring this inode, and fill the opened files
  // into the map
  // @param perAppFds: supposed to be cleared before passed in
  using inodeRefSetIt =
      std::unordered_map<cfs_ino_t, std::unordered_set<pid_t>>::iterator;
  virtual inodeRefSetIt collectInodeMigrateOpenedFd(
      const InMemInode *inodePtr,
      std::unordered_map<pid_t, std::vector<FileObj *>> &perAppFds) final;
  virtual void installOpenedFd(
      cfs_ino_t ino, std::unordered_map<pid_t, std::vector<FileObj *>> &fobjs,
      bool isMasterForward = false);

  // friends
  friend class FsProc;
  friend class SplitPolicyDynamicBasic;
  friend class SplitPolicyDynamicDstKnown;
  friend class worker_stats::AccessBucketedRecorder;
  friend class worker_stats::ParaAwareRecorder;
};

class FsProcWorkerServant : public FsProcWorker {
 public:
  FsProcWorkerServant(int w, CurBlkDev *d, int shmBaseOffset,
                      std::atomic_bool *workerRunning,
                      PerWorkerLoadStatsSR *stats);
  ~FsProcWorkerServant() override {}

  // NOTE: DEPRECATED
  virtual void checkSplitJoinOnFsReqReceived(FsReq *req, int &splitErrNo,
                                             int &dstWid) override;
  virtual void recvJoinAll(fsp_lm::PerWorkerLoadAllowance *allow) override;
  virtual int checkSplitJoinComm() override;

  static bool isFsReqServantCapable(const FsReqType &typ) {
    return typ > FsReqType::_LANDMARK_SERVANT_CAN_DO_START_ &&
           typ < FsReqType::_LANDMARK_SERVANT_CAN_DO_END_;
  }

  virtual bool isFsReqTypeWithinCapability(const FsReqType &typ) override {
    return isFsReqServantCapable(typ);
  }

  virtual bool isFsReqNotRecord(const FsReqType &typ) override {
    switch (typ) {
      case FsReqType::SYNCALL:
        return true;
      default:
        return false;
    }
  }

  virtual void workerRunLoop() override;

  virtual void ProcessManualThreadReassign(FsReq *req) override final;
  virtual void ProcessLmRedirectOneTau(
      LmMsgRedirectOneTauCtx *ctx) override final;

  void setCommBridge(WorkerCommBridge *b) { commBridgeWithMaster_ = b; }
  void startRunning();
  void initFileManager(FileMng *fm) {
    assert(fm != nullptr);
    fileManager = fm;
  }
  // NOTE: for redirect ino, @inoDstWid is the final destination wid
  // For servant worker, we know it can only send message to the master
  bool submitFileInodeJoinMsg(InMemInode *inodePtr, FsReq *req, int inoDstWid);

 private:
  WorkerCommBridge *commBridgeWithMaster_{nullptr};
  // inode number that is in joining back
  std::unordered_set<cfs_ino_t> inJoinInodeSet_;
  std::unordered_set<cfs_ino_t> inMngInodeSet_;

  virtual int initInMemDataAfterDevReady() override { return 0; }
  virtual int onTargetInodeFiguredOutChildBookkeep(
      FsReq *req, InMemInode *targetInode) override {
    return 0;
  }

  void waitForStartGunFire();
  bool recvFileInodeSplitMsg(WorkerCommMessage *msg);
  bool recvShmMsg(WorkerCommMessage *msg);

  bool recvFileInodeJoinMsgReply(WorkerCommMessage *msg);

  // friends
  friend class FsProc;
  friend class SplitPolicyDynamicDstKnown;
};

class FsProcWorkerMaster : public FsProcWorker {
 public:
  FsProcWorkerMaster(int w, CurBlkDev *d, int shmBaseOffset,
                     std::atomic_bool *workerRunning,
                     PerWorkerLoadStatsSR *stats);

  ~FsProcWorkerMaster() {}

  // NOTE: DEPRECATED
  virtual void checkSplitJoinOnFsReqReceived(FsReq *req, int &splitErrNo,
                                             int &dstWid) override;
  virtual int checkSplitJoinComm() override;

  virtual bool isFsReqTypeWithinCapability(const FsReqType &typ) override {
    // master can do anything
    return true;
  }

  // Q: what's the standrad of recording or not?
  // if an operation's target is a FILE inode, we need to record it
  virtual bool isFsReqNotRecord(const FsReqType &typ) override {
    switch (typ) {
      case FsReqType::MKDIR:
      case FsReqType::RMDIR:
      // case FsReqType::UNLINK:
      // case FsReqType::RENAME:
      case FsReqType::OPENDIR:
      case FsReqType::APP_EXIT:
      case FsReqType::NEW_SHM_ALLOCATED:
      case FsReqType::SYNCALL:
        return true;
      default:
        return false;
    }
  }

  virtual void workerRunLoop() override;

  virtual void ProcessManualThreadReassign(FsReq *req) override final;
  virtual void ProcessLmRedirectOneTau(
      LmMsgRedirectOneTauCtx *ctx) override final;

  // register servant workers to the master thread
  // let the master initialize the commBridge
  void registerServantWorkersToMaster(
      const std::vector<FsProcWorkerServant *> &servantsVec);

  // after this, those servant threads will be waken up
  void shotServantGunfire();

  bool submitFileInodeSplitMsg(int dstWid, InMemInode *inodePtr, FsReq *req);
  bool submitFileInodeForwardMsg(int dstWid, InMemInode *inodePtr,
                                 WorkerCommMessage *origMsg);
  bool submitFileInodeZeroLinkMsg(int dstWid, InMemInode *inodePtr, FsReq *req);
  bool submitShmMsg(int dstWid, uint8_t shmid, FsReq *req);

  // Although the primary should not access inodes it does not own, we currently
  // need to access nlink in order to perform unlink operation. Until nlink is
  // removed from the inode and put in its own data structure, we need to have a
  // way to access InMemInodes that have been reassigned elsewhere. The
  // unsafe_reassigned_inodes provides access to all inodes that have been
  // reassigned away from master. TODO: remove this once unlink does not require
  // inode access.
  std::unordered_map<cfs_ino_t, InMemInode *> unsafe_reassigned_inodes;

  void markInodeOwnershipChangeInProgress(cfs_ino_t ino);
  void markInodeOwnershipUpdated(cfs_ino_t ino, int wid);
  void addRecentlyReassignedInodeFds(ExportedInode &exp);
  void delRecentlyReassignedInodeEntries(pid_t app_id);
  void delRecentlyReassignedInodeEntries(pid_t app_id, int fd);
  cfs_ino_t getRecentlyReassignedInodeFromFd(pid_t app_id, int fd);
  int getInodeOwner(cfs_ino_t ino) const;
  // finds owner and submits fs req completion. returns the error code used
  // which may be FS_REQ_ERROR_INODE_IN_TRANSFER or FS_REQ_ERROR_INODE_REDIRECT
  int primaryRouteToOwner(FsReq *req, cfs_ino_t ino);

 private:
  std::unordered_map<FsProc::fsproc_wid_t, WorkerCommBridge *>
      commBridgesWithServantMap_;
  std::vector<FsProcWorkerServant *> servantsVec_;

  // Ownership map for inodes.
  // Values < 0 indicate states. Values >= 0 indicate wid.
  // NOTE: for now < 0 always indicates IN_PROGRESS
  // Missing keys indicate inode has not been loaded.
  // NOTE: This means that primary must always know about
  // 1. Owner of every inode
  // 2. Fd=>inode mapping for every application
  std::unordered_map<cfs_ino_t, int> inodeToOwnerMap_;

  std::unordered_map<pid_t, std::unordered_map<int, cfs_ino_t>>
      recentlyReassignedAppFdToInodes_;
  // TODO: (anthony) remove these two.
  // <ino, wid> : describes the inode that is currently in splitting process
  // wid : destination worker id
  std::unordered_map<uint32_t, int> inSplitInodeMap_;
  // <ino, wid> : describes which worker is currently managing specific inode
  // wid < 0 => currently no worker can handle request for that inode
  std::unordered_map<uint32_t, int> inodeOwnershipMap_;

  // Once a InMemInode is newed, it is going to be put into this queue
  // We should always immediately reassign them to the right owner
  // <ino, dst_wid>
  // Invariant, we only add each inode once when it is created
  // by *new InMemInode*
  // std::map<int, std::unordered_set<cfs_ino_t>> pending_newed_inodes;
  std::unordered_map<
      pid_t, std::unordered_map<int, std::vector<std::pair<cfs_ino_t, int>>>>
      pending_newed_inodes;

  std::unordered_map<WorkerCommMessage *, WorkerCommMessage *>
      forwardMsgToOrigJoinMsgMap_;

  virtual int initInMemDataAfterDevReady() override;

  virtual int onTargetInodeFiguredOutChildBookkeep(
      FsReq *req, InMemInode *targetInode) override;

  int ProcessPendingNewedInodesMigration();

  // init partitions of its own bmap an dataBlockBuffer to the servants
  void initMemDataForServants();

  void collectInodeShmPtrForSecondaryWorker(
      std::unordered_map<pid_t, shmIdBufPairs *> &appShms);

  void recvFileInodeSplitMsgReply(
      const std::pair<FsProc::fsproc_wid_t, WorkerCommBridge *> &cmmBridge,
      WorkerCommMessage *msg);
  void recvShmMsgReply(
      const std::pair<FsProc::fsproc_wid_t, WorkerCommBridge *> &cmmBridge,
      WorkerCommMessage *msg);

  void recvFileInodeJoinMsg(
      const std::pair<FsProc::fsproc_wid_t, WorkerCommBridge *> &cmmBridge,
      WorkerCommMessage *msg);

  // friends
  friend class FsProc;
  friend class FileMng;
};

inline size_t FsProcWorker::NumInProgressRequests(cfs_ino_t ino) {
  auto search = inodeInProgressFsReqMap_.find(ino);
  if (search != inodeInProgressFsReqMap_.end()) return (search->second).size();
  return 0;
}

inline void FsProcWorker::CheckCreationRedirection(pid_t pid, int tid,
                                                   cfs_ino_t ino) {
  auto pid_it = crt_routing_tb_.find(pid);
  if (pid_it == crt_routing_tb_.end()) return;
  auto tid_it = pid_it->second.find(tid);
  if (tid_it == pid_it->second.end()) return;
  int dst_wid = tid_it->second;
  pending_crt_redirect_inos_.push_back({ino, dst_wid});
}

inline void FsProcWorkerMaster::markInodeOwnershipChangeInProgress(
    cfs_ino_t ino) {
  inodeToOwnerMap_[ino] = -1;
}

inline void FsProcWorkerMaster::markInodeOwnershipUpdated(cfs_ino_t ino,
                                                          int wid) {
  if (wid == FsProcWorker::kMasterWidConst) {
    inodeToOwnerMap_.erase(ino);
  } else {
    inodeToOwnerMap_[ino] = wid;
  }
}

inline int FsProcWorkerMaster::getInodeOwner(cfs_ino_t ino) const {
  auto search = inodeToOwnerMap_.find(ino);
  // presume primary as owner if not in map
  if (search == inodeToOwnerMap_.end()) return FsProcWorker::kMasterWidConst;
  // NOTE: this function is called indirectly after we obtain fd to inode
  // mapping, which means this inode is valid.
  return search->second;
}

#if CFS_JOURNAL(GLOBAL_JOURNAL)
inline JournalManager *FsProc::GetPrimaryJournalManager() {
  auto search = widWorkerMap.find(FsProcWorker::kMasterWidConst);
  if (search == widWorkerMap.end())
    throw std::runtime_error("could not find primary worker");

  return search->second->jmgr;
}
#endif

#endif  // CFS_FS_H
