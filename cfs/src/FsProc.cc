#include <stdio.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>

#include <cassert>
#include <iostream>
#include <numeric>

#include "FsProc_Fs.h"
#include "FsProc_FsImpl.h"
#include "FsProc_LoadMng.h"
#include "FsProc_Messenger.h"
#include "FsProc_PageCache.h"
#include "FsProc_UnixSock.h"
#include "FsProc_util.h"
#include "param.h"
#include "perfutil/Cycles.h"
#include "shmipc/shmipc.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"

extern FsProc *gFsProcPtr;

void FsProc::getWorkerLogName(std::stringstream &ssLogger,
                              std::stringstream &ssFile, int wid) {
  ssLogger.clear();
  ssFile.clear();
  ssLogger.str(std::string());
  ssFile.str(std::string());
  ssFile << kLogDir;
  ssLogger << "worker-";
  ssLogger << wid;
  ssLogger << "-logger";
  ssFile << ssLogger.rdbuf();
  ssFile << ".out";
}

FsProc::FsProc(int tNum, int appNum, const char *exitFname)
    : exitSignalFileName(exitFname),
      numThreads(tNum),
      numAppProc(appNum),
      workerRunning(new std::atomic_bool[tNum]),
      workerActive(new std::atomic_bool[tNum]) {
  for (int i = 0; i < tNum; i++) {
    workerRunning[i].store(false);
    workerActive[i].store(false);
  }
  pageCacheMng = new PageCacheManager();
  loadMng = new worker_stats::LoadMngType(tNum);
#ifdef UFS_SOCK_LISTEN
  sock_listener_ = new fsp_sock::UnixSocketListener(tNum);
#endif
}

FsProc::FsProc(bool doInline)
    : exitSignalFileName(""),
      numThreads(1),
      numAppProc(1),
      workerRunning(new std::atomic_bool[1]),
      workerActive(new std::atomic_bool[1]) {
  loadMng = new worker_stats::LoadMngType(1);
#ifdef UFS_SOCK_LISTEN
  sock_listener_ = new fsp_sock::UnixSocketListener(1);
#endif
}

void FsProc::workerReady(void) {
  const char *readyFile = std::getenv("READY_FILE_NAME");
  if (readyFile == nullptr) {
    fprintf(stderr, "readyFile not specified\n");
    return;
  } else {
    fprintf(stderr, "readyFile specified %s\n", readyFile);
  }

  int fd = ::open(readyFile, O_WRONLY | O_CREAT, 0666);
  if (fd < 0) {
    SPDLOG_ERROR("failed to create ready file {}", readyFile);
    return;
  }

  int rc = ::close(fd);
  if (rc < 0) {
    SPDLOG_ERROR("failed to close ready file {}", readyFile);
    return;
  }
}

uint64_t FsProc::QueryWorkerJournalMngNumWrite(int wid, bool do_reset) {
#if CFS_JOURNAL(ON)
  if (JournalManager::kEnableNvmeWriteStats) {
    auto jmr = workerList[wid]->jmgr;
    if (jmr != nullptr) {
      uint64_t ret = jmr->QueryNumNvmeWriteDone();
      if (do_reset) {
        jmr->ResetNumNvmeWriteDone();
      }
      return ret;
    }
  }
#endif
  return 0;
}

void FsProc::redirectZombieAppReqs(int wid) {
  workerMap[wid]->redirectZombieAppReqsToMaster();
}

void FsProc::cleanup() {
  sleep(1);
  SPDLOG_INFO("cleanup ...");
  if (loadMng != nullptr) loadMng->shutDown();
#ifdef UFS_SOCK_LISTEN
  if (sock_listener_ != nullptr) sock_listener_->ShutDown();
#endif
  for (auto wk : workerMap) {
    delete wk.second;
  }
  delete[] workerRunning;
  fprintf(stdout, "delete pageCacheMng:%p\n", pageCacheMng);
  if (pageCacheMng != nullptr) delete pageCacheMng;
  fprintf(stdout, "delete loadMng:%p\n", loadMng);
  if (loadMng != nullptr) {
    loadMngThread_->join();
    delete loadMng;
    delete loadMngThread_;
  }
#ifdef UFS_SOCK_LISTEN
  fprintf(stdout, "cleanup socket listening");
  if (sock_listener_ != nullptr) {
    assert(sock_listen_thread_ != nullptr);
    sock_listen_thread_->join();
    delete sock_listener_;
    delete sock_listen_thread_;
  }
#endif
  SPDLOG_INFO("Bye :)");
}


int AppProc::GetDstWid(int tau_id, cfs_ino_t ino) {
  auto cur_plc_no = gFsProcPtr->getSplitPolicy();
  if (cur_plc_no != SPLIT_POLICY_NO_TWEAK_MOD &&
      cur_plc_no != SPLIT_POLICY_NO_TWEAK_MOD_SHARED) {
    auto it = tau_dst_note_.find(tau_id);
    if (it == tau_dst_note_.end()) return -1;
    auto note_ptr = it->second;
    return note_ptr->GetCurDstWid(ino);
  } else {
    return ino % gFsProcPtr->getNumThreads();
  }
}

void FsProc::startWorkers(std::vector<int> &shmOffsetVec,
                          std::vector<CurBlkDev *> &devVec,
                          std::vector<int> &workerCoresVec) {
#ifdef TWEAK_SPLIT_DO_MOD
  SPDLOG_WARN("!!!!!!!!!! Using TWEAK_SPLIT_DO_MOD");
#endif
  bool exit = checkFileExistance(exitSignalFileName);
  if (exit) {
    SPDLOG_ERROR("exitSignalFile:{} is already there, will not start FS!",
                 exitSignalFileName);
    return;
  }

  bool logDirExist = checkFileExistance(kLogDir);
  if (!logDirExist) {
    SPDLOG_INFO("create log directory:{}", kLogDir);
    int rc = ::mkdir(kLogDir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (rc < 0) {
      SPDLOG_ERROR("cannot create logdir:{} err:{}", strerror(errno));
      return;
    }
  }

  const char *readyFile = std::getenv("READY_FILE_NAME");
  if (readyFile != nullptr && checkFileExistance(readyFile)) {
    SPDLOG_ERROR("readyFile:{} is already there, will not start FS!",
                 readyFile);
    return;
  }

  if (shmOffsetVec.size() > 1) {
    worker_shmkey_distance = shmOffsetVec[1] - shmOffsetVec[0];
    for (size_t shmoff_idx = 1; shmoff_idx < shmOffsetVec.size();
         shmoff_idx++) {
      if (shmOffsetVec[shmoff_idx] - shmOffsetVec[shmoff_idx - 1] !=
          worker_shmkey_distance) {
        std::cerr << "worker_shmkey_distance:" << worker_shmkey_distance
                  << " idx:" << shmoff_idx << " distance:"
                  << shmOffsetVec[shmoff_idx] - shmOffsetVec[shmoff_idx - 1]
                  << std::endl;
        throw std::runtime_error("shmkey distance not consitent");
      }
    }
  } else if (shmOffsetVec.size() == 1) {
    worker_shmkey_distance = 0;
  }

  auto messenger = new FsProcMessenger(numThreads, DEFAULT_MESSENGER_BUFSIZE);
  loadMng->setMessenger(messenger);
#if defined SCALE_USE_BUCKETED_DATA
  auto masterWorker = new FsProcWorkerMaster(
      FsProcWorker::kMasterWidConst + 0, devVec[0], shmOffsetVec[0],
      &workerRunning[0],
      loadMng->getWorkerStatsPtr(FsProcWorker::kMasterWidConst));
#elif defined SCALE_USE_ATOMIC_DATA
  auto masterWorker =
      new FsProcWorkerMaster(FsProcWorker::kMasterWidConst + 0, devVec[0],
                             shmOffsetVec[0], &workerRunning[0], nullptr);
#else
  ASSERT_STATS_TYPE_NOT_VALID
#endif
  FsProcWorker *curWorker;
  FsProcWorkerServant *peripheryWorker = nullptr;
  std::vector<FsProcWorkerServant *> servantWorkers(numThreads - 1);
  for (int i = 0; i < numThreads; i++) {
    if (i == 0) {
      curWorker = masterWorker;
    } else {
      SPDLOG_DEBUG("start servant thread-idx:{}", i);
#if defined SCALE_USE_BUCKETED_DATA
      peripheryWorker = new FsProcWorkerServant(
          FsProcWorker::kMasterWidConst + i, devVec[i], shmOffsetVec[i],
          &workerRunning[i],
          loadMng->getWorkerStatsPtr(FsProcWorker::kMasterWidConst + i));
#elif defined SCALE_USE_ATOMIC_DATA
      peripheryWorker =
          new FsProcWorkerServant(FsProcWorker::kMasterWidConst + i, devVec[i],
                                  shmOffsetVec[i], &workerRunning[i], nullptr);
#else
      ASSERT_STATS_TYPE_NOT_VALID
#endif
      curWorker = peripheryWorker;
      servantWorkers[i - 1] = peripheryWorker;
    }

    if (!workerCoresVec.empty()) {
      curWorker->setPinnedCPUCore(workerCoresVec[i]);
    }
    curWorker->messenger = messenger;
    recorders_[curWorker->getWid()] = &(curWorker->stats_recorder_);
    (curWorker->stats_recorder_).SetWorker(curWorker);
#ifdef SCALE_USE_ATOMIC_DATA
    loadMng->SetStatsComb(curWorker->getWid(),
                          (curWorker->stats_recorder_.GetStatsCombPtr()));
#endif
    if (curWorker->dst_known_split_policy_ != nullptr) {
      curWorker->dst_known_split_policy_->SetWorker(curWorker);
    }
    threadList.push_back(
        new std::thread(&FsProcWorker::workerRunLoop, curWorker));
    workerList.push_back(curWorker);
    cfs_tid_t curTid = curWorker->getWid();
    SPDLOG_INFO("FsProc thread created tid: {}", curTid);
    workerMap.emplace(curTid, curWorker);
    widWorkerMap.emplace(curWorker->getWid(), curWorker);
  }

  // register servant workers to the master
  masterWorker->registerServantWorkersToMaster(servantWorkers);

  SPDLOG_INFO("is std::atomic_bool lock free? {}",
              workerRunning[0].is_lock_free());

  // init loggers for the worker
  if (numThreads > 0) {
    logToFile = true;
    std::stringstream ssLogger;
    std::stringstream ssFile;
    for (int i = 0; i < numThreads; i++) {
      getWorkerLogName(ssLogger, ssFile, workerList[i]->getWid());
      SPDLOG_INFO("init logger for wid:{} fileName:{}", workerList[i]->getWid(),
                  ssFile.str());
      workerList[i]->initFileLogger(ssLogger.str(), ssFile.str());
    }
  }

#ifdef UFS_SOCK_LISTEN
  // init listener for unix domain socket
  sock_listen_thread_ = new std::thread(
      &fsp_sock::UnixSocketListener::SockListenRunner, sock_listener_);
#endif

  // init monitor
  loadMngThread_ =
      new std::thread(&worker_stats::LoadMngType::loadManagerRunner, loadMng);

  // start master
  workerRunning[0].store(true);

#ifdef UFS_SOCK_LISTEN
  // start socket listener
  sock_listener_->StartRun();
#endif

  // wait to join ...
  for (int i = 0; i < numThreads; i++) {
    threadList[i]->join();
  }
  SPDLOG_INFO("All the workers have terminated.");
  for (auto w : workerList) {
    w->redirectZombieAppReqs(true);
  }
  cleanup();
}

FsProcWorker *FsProc::startInlineWorker(CurBlkDev *dev, bool loadFs) {
#if defined SCALE_USE_BUCKETED_DATA
  auto *curWorker = reinterpret_cast<FsProcWorker *>(new FsProcWorkerMaster(
      FsProcWorker::kMasterWidConst, dev, 0, &workerRunning[0],
      loadMng->getWorkerStatsPtr(FsProcWorker::kMasterWidConst)));
#elif defined SCALE_USE_ATOMIC_DATA
  auto *curWorker = reinterpret_cast<FsProcWorker *>(new FsProcWorkerMaster(
      FsProcWorker::kMasterWidConst, dev, 0, &workerRunning[0], nullptr));
#else
  ASSERT_STATS_TYPE_NOT_VALID
#endif
  // make the device ready
  curWorker->initMakeDevReady();
  if (loadFs) {
    curWorker->initInMemDataAfterDevReady();
    initAppsToWorker(curWorker);
  }
  cfs_tid_t curTid = cfsGetTid();
  workerMap.emplace(curTid, curWorker);
  widWorkerMap.emplace(curWorker->getWid(), curWorker);
  return curWorker;
}

void FsProc::stop() {
  SPDLOG_INFO("Stop file system process ...");
  bool expect;
  for (int i = 0; i < numThreads; i++) {
    expect = true;
    while (!workerRunning[i].compare_exchange_weak(expect, false) && expect)
      ;
  }
  SPDLOG_INFO("Signal sent to each workers");
}

int FsProc::submitDevAsyncReadReqCompletion(struct BdevIoContext *ctx) {
  // SPDLOG_DEBUG("submitDevAsyncReadReqCompletion blockNo:{}", ctx->blockNo);
  auto curWorkerIt = workerMap.find(cfsGetTid());
  if (curWorkerIt == workerMap.end()) {
    SPDLOG_ERROR("Cannot find worker for current thread");
    return -1;
  }
  return curWorkerIt->second->submitDevAsyncReadReqCompletion(ctx);
}

int FsProc::submitBlkWriteReqCompletion(struct BdevIoContext *ctx) {
  auto curWorkerIt = workerMap.find(cfsGetTid());
  if (curWorkerIt == workerMap.end()) {
    SPDLOG_ERROR("Cannot find worker for current thread");
    return -1;
  }
  return curWorkerIt->second->submitBlkWriteReqCompletion(ctx);
}

void FsProc::initAppsToWorker(FsProcWorker *worker) {
#ifdef UFS_SOCK_LISTEN
  worker->InitEmptyApp(numAppProc);
#else
  for (int i = 0; i < numAppProc; i++) {
    AppCredential cred(9000 + i, 9, 9);
    worker->initNewApp(cred);
  }
#endif
}

#if CFS_JOURNAL(ON)
int FsProc::proposeJournalCheckpointing() {
  auto curWorkerIt = workerMap.find(cfsGetTid());
  if (curWorkerIt == workerMap.end()) {
    SPDLOG_ERROR("Cannot find worker for current thread");
    throw std::runtime_error("cannot find worker for current thread");
  }
  if (inGlobalCheckpointing) return -1;
  FsProcMessage msg;
  msg.type = FsProcMessageType::PROPOSE_CHECKPOINT;
  auto ctx = new ProposeForCheckpoingCtx();
  ctx->propose_widIdx = curWorkerIt->second->getWorkerIdx();
  msg.ctx = reinterpret_cast<void *>(ctx);
  // we propose this checkpointing to the primary worker
  bool success =
      curWorkerIt->second->messenger->send_message(0 /*master's index*/, msg);
  if (success) return 0;
  return -1;
}
#else
int FsProc::proposeJournalCheckpointing() { return -1; }
#endif

#if CFS_JOURNAL(ON)
void FsProc::performCheckpointing(int widIdx) {
  FsProcWorker *worker = workerList[widIdx];
  assert(worker != nullptr);
  // Ensure each worker can only invoke this on its own behalf
  assert(cfsGetTid() == worker->getWid());
  worker->jmgr->checkpointAllJournals(worker->fileManager, workerList,
                                      numThreads, widIdx);
}
#else
void FsProc::performCheckpointing(int widIdx) {}
#endif

void FsProc::CheckNewedInMemInodeDst(int wid, cfs_ino_t ino, int pid, int tid) {
  if (wid == FsProcWorker::kMasterWidConst) {
    auto master = reinterpret_cast<FsProcWorkerMaster *>(workerList[wid]);
    int dst_wid = master->GetPidtidHandlingWid(pid, tid);
    if (dst_wid > 0) {
      (master->pending_newed_inodes)[pid][tid].push_back({ino, dst_wid});
    }
  }
}

/*---- FsReq.h ----*/

AppProc::AppProc(int appIdx, int shmBaseOffset, AppCredential &credential)
    : appIdx(appIdx),
      shmBaseOffset(shmBaseOffset),
      fdIncr(kFdBase),
      fdLock(ATOMIC_FLAG_INIT) {
  cred = credential;
  shmKey = FS_SHM_KEY_BASE + shmBaseOffset + appIdx;
  SPDLOG_INFO("AppProc initialized shmKey:{}", shmKey);

  char shmfile[128];
  // TODO have a constant for this format?
  snprintf(shmfile, 128, "/shmipc_mgr_%03d", (int)shmKey);

  // We create the memory on server side, hence 1.
  shmipc_mgr = shmipc_mgr_init(shmfile, RING_SIZE, 1);
  if (shmipc_mgr == NULL) {
    SPDLOG_ERROR("Failed to initialize shared memory for AppProc {}", appIdx);
    throw std::runtime_error("failed to initialize shared memory for appProc");
  }

  SPDLOG_INFO("AppProc {} control shm accessible at {}", appIdx, shmfile);
}

AppProc::~AppProc() { shmipc_mgr_destroy(shmipc_mgr); }

FileObj *AppProc::allocateFd(InMemInode *inodePtr, int openFlags,
                             mode_t openMode) {
  int curFd = fdIncr++;
  auto curFileObj = new FileObj();
  curFileObj->readOnlyFd = curFd;
  curFileObj->ip = inodePtr;
  curFileObj->shadow_ino = inodePtr->i_no;
  if (curFileObj->flags & O_APPEND) {
    curFileObj->off = inodePtr->inodeData->size;
  } else {
    curFileObj->off = 0;
  }
  curFileObj->flags = openFlags;
  curFileObj->mode = openMode;
  curFileObj->type = FileObj::FD_INODE;
  SPDLOG_DEBUG(
      "(allocateFd to inode) size: {} ondisk i_no: {} in-mem i_no: {}, fd:{} "
      "off:{}",
      inodePtr->inodeData->size, inodePtr->inodeData->i_no, inodePtr->i_no,
      curFd, curFileObj->off);
  return curFileObj;
}

int AppProc::updateFdIncrByWid(int wid) {
  fdIncr = wid * 10000000 + kFdBase;
  return fdIncr;
}

void FsReqIoUnitHelper::addIoUnitReq(cfs_bno_t iouNo, bool isSubmit) {
  SPDLOG_DEBUG("addIoUnitReq iouNo:{}, isSubmit:{}", iouNo, isSubmit);
  if (ioUnitPendingMap_->find(iouNo) != ioUnitPendingMap_->end()) {
    if (isDebug_) {
      SPDLOG_ERROR(
          "ioUnitPendingMap already has the item for block:{} w/ value:{} -> "
          "new value:{}",
          iouNo, ioUnitPendingMap_->find(iouNo)->second, isSubmit);
    }
    return;
  }
  ioUnitPendingMap_->emplace(iouNo, isSubmit);
}

BlockReq *FsReqIoUnitHelper::addIoUnitSubmitReq(cfs_bno_t iouNo,
                                                BlockBufferItem *item,
                                                char *dst,
                                                FsBlockReqType type) {
  SPDLOG_DEBUG("addIoUnitSubmitReq iouNo:{} ioSize:{}", iouNo, ioUintSizeByte_);
  addIoUnitReq(iouNo, true);
  auto *req = new BlockReq(iouNo, item, dst, type);
  submitIoUnitReqMap_->insert({iouNo, req});
  return req;
}

BlockReq *FsReqIoUnitHelper::getIoUnitSubmitReq(cfs_bno_t iouNo) {
  auto it = submitIoUnitReqMap_->find(iouNo);
  if (it != submitIoUnitReqMap_->end()) {
    return it->second;
  }
  SPDLOG_ERROR("Error iouNo is not a submit req. iouNo:{} ioUnitSize:{}", iouNo,
               ioUintSizeByte_);
  return nullptr;
}

void FsReqIoUnitHelper::submittedIoUnitReqDone(cfs_bno_t iouNo) {
  BlockReq *bReq = getIoUnitSubmitReq(iouNo);
  if (bReq == nullptr) {
    SPDLOG_INFO("submittedIoUnitReqDone fail");
    throw std::runtime_error("submittedIoUnitReqDone fail");
  }
  (bReq->getBufferItem())->blockFetchedCallback();
  submitIoUnitReqMap_->erase(iouNo);
  delete bReq;
}

void FsReqIoUnitHelper::ioUnitWaitReqDone(cfs_bno_t iouNo) {
  ioUnitPendingMap_->erase(iouNo);
}

FsReq::~FsReq() {
  // if (isDebug_) {
  //   SPDLOG_INFO("~FsReq() called debug is true");
  // }

  delete blockIoHelper;
  delete sectorIoHelper;

#ifndef _CFS_TEST_
  if (copPtr != NULL) {
    // TODO when we use a pool, give it back to the pool
    // instead of free'ing it.
    // Also, make sure to zero out the struct.
    free(copPtr);
    copPtr = NULL;
  }
#endif
  free(standardFullPath);
  free(newStandardFullPath);
  standardFullPath = nullptr;
  newStandardFullPath = nullptr;
  // TODO (jingliu, mem leak check here?)
}

std::ostream &operator<<(std::ostream &os, const FsReqType &tp) {
  os << "FsReqType::" << getFsReqTypeOutputString(tp);
  return os;
}

void FsReq::initReqFromCop(AppProc *curApp, off_t curSlotId,
                           struct clientOp *cop, char *curData,
                           FsProcWorker *worker) {
  app = curApp;
  appRingSlotId = curSlotId;
  copPtr = cop;
  channelDataPtr = curData;

  switch (copPtr->opCode) {
    // data plane operations
    case CFS_OP_READ: {
      setType(FsReqType::READ);
      reqState = FsReqState::READ_FETCH_DATA;
      fd = cop->op.read.rwOp.fd;
      rwopPtr = &cop->op.read.rwOp;
      tid = cop->op.read.rwOp.ret;
      break;
    }
    case CFS_OP_PREAD: {
      fd = cop->op.pread.rwOp.fd;
      rwopPtr = &cop->op.pread.rwOp;
      tid = cop->op.read.rwOp.ret;
      if (isOpEnableUnifiedCache(&cop->op.pread)) {
        if (isLeaseRenewOnly()) {
          setType(FsReqType::PREAD_UC_LEASE_RENEW_ONLY);
          reqState = FsReqState::CCACHE_RENEW;
        } else {
          setType(FsReqType::PREAD_UC);
          reqState = FsReqState::UCPREAD_GEN_READ_PLAN;
        }
      } else {
        // old version of pread
        setType(FsReqType::PREAD);
        reqState = FsReqState::PREAD_FETCH_DATA;
      }
      break;
    }
    case CFS_OP_WRITE: {
      setType(FsReqType::WRITE);
      reqState = FsReqState::WRITE_MODIFY;
      fd = cop->op.write.rwOp.fd;
      rwopPtr = &cop->op.write.rwOp;
      tid = rwopPtr->ret;
      break;
    }
    case CFS_OP_PWRITE: {
      setType(FsReqType::PWRITE);
      reqState = FsReqState::PWRITE_MODIFY;
      fd = cop->op.pwrite.rwOp.fd;
      rwopPtr = &cop->op.pwrite.rwOp;
      tid = rwopPtr->ret;
      break;
    }
    case CFS_OP_ALLOCED_READ: {
      setType(FsReqType::ALLOC_READ);
      reqState = FsReqState::ALLOCREAD_FETCH_DATA;
      fd = cop->op.allocread.rwOp.fd;
      rwopPtr = &cop->op.allocread.rwOp;
      alopPtr = &cop->op.allocread.alOp;
      tid = rwopPtr->ret;
      dataPtrId = cop->op.allocread.alOp.dataPtrId;
      shmId = alopPtr->shmid;
      break;
    }
    case CFS_OP_ALLOCED_PREAD: {
      setType(FsReqType::ALLOC_PREAD);
      fd = cop->op.allocpread.rwOp.fd;
      rwopPtr = &cop->op.allocpread.rwOp;
      tid = rwopPtr->ret;
      // FIXME: wrap this hack
      // for the client-inited mem segment, we can only retrieve the mem by
      // MEMid-offId when clent use fs_malloc() return ptr, with offset like
      // 5000, what they want is 5000 put to ptr But to do client cache, we want
      // to put more data, start from 4096 so we allocate a dummy page before
      // the ptr, and return ptr - (5000 - 4096) to the user reference:
      // https://github.com/jingliu9/ApparateFS/blob/0fbe29f7a6727701b6c5a99c712f6a285992e8a6/cfs/src/FsLib.cc#L2198
      if (rwopPtr->realCount > 0) {
        mem_start_offset = rwopPtr->realCount;
      }
      SPDLOG_DEBUG("mem_start_offset:{}", mem_start_offset);
      alopPtr = &cop->op.allocpread.alOp;
      dataPtrId = cop->op.allocpread.alOp.dataPtrId;
      shmId = alopPtr->shmid;
      if (isLeaseRenewOnly()) {
        setType(FsReqType::ALLOC_PREAD_RENEW_ONLY);
        reqState = FsReqState::CCACHE_RENEW;
      } else if (isAppCacheAvailable()) {
        SPDLOG_DEBUG("cache enabled fd:{} count:{} realCount:{}, realOffset:{}",
                     fd, cop->op.allocpread.rwOp.count,
                     cop->op.allocpread.rwOp.realCount,
                     cop->op.allocpread.rwOp.realOffset);
        reqState = FsReqState::ALLOCPREAD_UNCACHE_FETCH_DATA;
      } else {
        reqState = FsReqState::ALLOCPREAD_UNCACHE_FETCH_DATA;
      }
      break;
    }
    case CFS_OP_ALLOCED_WRITE: {
      setType(FsReqType::ALLOC_WRITE);
      fd = cop->op.allocwrite.rwOp.fd;
      rwopPtr = &cop->op.allocwrite.rwOp;
      tid = rwopPtr->ret;
      alopPtr = &cop->op.allocwrite.alOp;
      dataPtrId = cop->op.allocwrite.alOp.dataPtrId;
      shmId = alopPtr->shmid;
      reqState = FsReqState::ALLOCWRITE_UNCACHE_MODIFY;
      break;
    }
    case CFS_OP_ALLOCED_PWRITE: {
      setType(FsReqType::ALLOC_PWRITE);
      fd = cop->op.allocpwrite.rwOp.fd;
      rwopPtr = &cop->op.allocpwrite.rwOp;
      tid = rwopPtr->ret;
      alopPtr = &cop->op.allocpwrite.alOp;
      dataPtrId = cop->op.allocpwrite.alOp.dataPtrId;
      shmId = alopPtr->shmid;
      reqState = FsReqState::ALLOCPWRITE_UNCACHE_MODIFY;
      // if (isLeaseRenewOnly()) {
      //   setType(FsReqType::ALLOC_PREAD_RENEW_ONLY);
      // } else if (isAppCacheAvailable()) {
      //   reqState = FsReqState::ALLOCPWRITE_TOCACHE_MODIFY;
      // } else {
      //   reqState = FsReqState::ALLOCPWRITE_UNCACHE_MODIFY;
      // }
      break;
    }
    case CFS_OP_LSEEK: {
      setType(FsReqType::LSEEK);
      reqState = FsReqState::LSEEK_INIT;
      tid = cop->op.lseek.ret;
      fd = cop->op.lseek.fd;
      break;
    }
      //
      // Control plane operations
    case CFS_OP_OPEN: {
      // NOTE: If O_CREAT flag is present, this path will not be found and this
      // request is modified to type FsReqType::CREATE.
      setType(FsReqType::OPEN);
      reqState = FsReqState::OPEN_GET_CACHED_INODE;
      tid = copPtr->op.open.ret;

      // init path tokens
      standardFullPath = filepath2TokensStandardized(copPtr->op.open.path,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      break;
    }
    case CFS_OP_CLOSE: {
      setType(FsReqType::CLOSE);
      reqState = FsReqState::CLOSE_INIT;
      fd = cop->op.close.fd;
      tid = cop->op.close.ret;
      break;
    }
    case CFS_OP_MKDIR: {
      setType(FsReqType::MKDIR);
      reqState = FsReqState::MKDIR_GET_PRT_INODE;
      standardFullPath = filepath2TokensStandardized(copPtr->op.mkdir.pathname,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      tid = cop->op.mkdir.ret;
      break;
    }
    case CFS_OP_STAT: {
      setType(FsReqType::STAT);
      reqState = FsReqState::STAT_GET_CACHED_INODE;
      standardFullPath = filepath2TokensStandardized(copPtr->op.stat.path,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      tid = cop->op.stat.ret;
      break;
    }
    case CFS_OP_FSTAT: {
      setType(FsReqType::FSTAT);
      reqState = FsReqState::FSTAT_INIT;
      fd = cop->op.fstat.fd;
      tid = cop->op.fstat.ret;
      break;
    }
    case CFS_OP_UNLINK: {
      setType(FsReqType::UNLINK);
      reqState = FsReqState::UNLINK_PRIMARY_LOAD_PRT_INODE;
      standardFullPath = filepath2TokensStandardized(copPtr->op.unlink.path,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      tid = copPtr->op.unlink.ret;
      break;
    }
    case CFS_OP_RENAME: {
      setType(FsReqType::RENAME);
      reqState = FsReqState::RENAME_LOOKUP_SRC_DIR;
      standardFullPath = filepath2TokensStandardized(copPtr->op.rename.oldpath,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      newStandardFullPath = filepath2TokensStandardized(
          copPtr->op.rename.newpath, dstStandardFullPathDelimIdx,
          dstStandardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      dstPathTokens = absl::StrSplit(newStandardFullPath, "/");
      tid = copPtr->op.rename.ret;
      break;
    }
    case CFS_OP_FSYNC: {
      if (copPtr->op.fsync.is_data_sync) {
        setType(FsReqType::FDATA_SYNC);
      } else {
        setType(FsReqType::FSYNC);
      }
      reqState = FsReqState::FSYNC_DATA_BIO;
      fd = cop->op.fsync.fd;
      tid = copPtr->op.fsync.ret;
      break;
    }
    case CFS_OP_WSYNC: {
      setType(FsReqType::WSYNC);
      reqState = FsReqState::WSYNC_ALLOC_ENTIRE;
      fd = cop->op.wsync.fd;
      tid = copPtr->op.wsync.ret;
      dataPtrId = cop->op.wsync.alloc.dataPtrId;
      shmId = cop->op.wsync.alloc.shmid;
      break;
    }
    case CFS_OP_SYNCALL: {
      setType(FsReqType::SYNCALL);
      reqState = FsReqState::SYNCALL_GUARD;
      // NOTE: SYNCALL and SYNCUNLINKED can be triggered through client api,
      // periodically, or on shutdown. We use the callback method to accommodate
      // all of them.
      completionCallback = FsProcWorker::onSyncallCompletion;
      completionCallbackCtx = static_cast<void *>(worker);
      tid = copPtr->op.syncall.ret;
      break;
    }
    case CFS_OP_SYNCUNLINKED: {
      setType(FsReqType::SYNCUNLINKED);
      reqState = FsReqState::SYNCUNLINKED_GUARD;
      completionCallback = FsProcWorker::onSyncallCompletion;
      completionCallbackCtx = static_cast<void *>(worker);
      tid = copPtr->op.syncunlinked.ret;
      break;
    }
    case CFS_OP_OPENDIR: {
      setType(FsReqType::OPENDIR);
      reqState = FsReqState::OPENDIR_GET_CACHED_INODE;
      alopPtr = &cop->op.opendir.alOp;
      dataPtrId = cop->op.opendir.alOp.dataPtrId;
      shmId = alopPtr->shmid;
      tid = cop->op.opendir.numDentry;
      standardFullPath = filepath2TokensStandardized(copPtr->op.opendir.name,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      break;
    }
    case CFS_OP_RMDIR: {
      setType(FsReqType::RMDIR);
      reqState = FsReqState::RMDIR_START_FAKE;
      standardFullPath = filepath2TokensStandardized(copPtr->op.rmdir.pathname,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      pathTokens = absl::StrSplit(standardFullPath, "/");
      tid = copPtr->op.rmdir.ret;
      break;
    }
    case CFS_OP_NEW_SHM_ALLOCATED: {
      setType(FsReqType::NEW_SHM_ALLOCATED);
      reqState = FsReqState::NEW_SHM_ALLOC_SEND_MSG;
      shmName = std::string(copPtr->op.newshmop.shmFname);
      break;
    }
    case CFS_OP_EXIT: {
      setType(FsReqType::APP_EXIT);
      tid = copPtr->op.exit.ret;
      break;
    }
    case CFS_OP_CHKPT: {
      setType(FsReqType::APP_CHKPT);
      break;
    }
    case CFS_OP_MIGRATE: {
      setType(FsReqType::APP_MIGRATE);
      fd = cop->op.migrate.fd;
      break;
    }
    case CFS_OP_PING: {
      setType(FsReqType::PING);
      break;
    }
    case CFS_OP_DUMPINODES: {
      setType(FsReqType::DUMPINODES);
      break;
    }
    case CFS_OP_STARTDUMPLOAD: {
      setType(FsReqType::START_DUMP_LOADSTAT);
      break;
    }
    case CFS_OP_STOPDUMPLOAD: {
      setType(FsReqType::STOP_DUMP_LOADSTAT);
      break;
    }
    case CFS_OP_INODE_REASSIGNMENT: {
      setType(FsReqType::INODE_REASSIGNMENT);
      break;
    }
    case CFS_OP_THREAD_REASSIGNMENT: {
      setType(FsReqType::THREAD_REASSIGN);
      break;
    }
#ifdef _CFS_TEST_
    case CFS_OP_TEST: {
      standardFullPath = filepath2TokensStandardized(copPtr->op.test.path,
                                                     standardFullPathDelimIdx,
                                                     standardFullPathDepth);
      break;
    }
#endif
    default:
      SPDLOG_ERROR("op not supported opcode:{}", copPtr->opCode);
      break;
  }

  SPDLOG_DEBUG("received req with opcode {} at worker {}, type = {}",
               copPtr->opCode, worker->getWid(), reqType);
  if (reqType == FsReqType::OPEN || reqType == FsReqType::CREATE)
    SPDLOG_DEBUG("Open flag: {}", copPtr->op.open.flags);
  if (standardFullPath) SPDLOG_DEBUG("path: {}", standardFullPath);
}

void FsReq::resetReq() {
  free(standardFullPath);
  free(newStandardFullPath);
  standardFullPath = nullptr;
  newStandardFullPath = nullptr;

  // assume these are clean?
  blockIoBlkIdxDoneMap.clear();
  blockIoDoneMap.clear();
  // numBlockRead = 0;

  ucReqDstVec.clear();
  ucReqPagesToRead.clear();

  tid = 0;
  in_use = false;

  dstExist = true;
  dstParDirMap = nullptr;
  dstTargetInodePtr = nullptr;
  dstParDirInodePtr = nullptr;
  dstWithinBlockDentryIndex = 0;
  dstFileDentryDataBlockNo = 0;
  dstFileIno = 0;

  totalOnCpuCycles = 0;
  curOnCpuTermStartTick = 0;
  // use_oncpu_timer = kUseOncpuTimer;

  parDirMap = nullptr;
  parDirInodePtr = nullptr;
  targetInodePtr = nullptr;

  withinBlockDentryIndex = 0;
  fileDentryDataBlockNo = 0;
  fileIno = 0;

  completionCallbackCtx = nullptr;
  completionCallback = nullptr;

  rwopPtr = nullptr;
  fd = -1;
  errorNo = FS_REQ_ERROR_NONE;

  append_start_off = 0;

  pendingShmMsg = 0;

  if (copPtr != nullptr) {
    // TODO don't free if copPtr uses a pool
    free(copPtr);
    copPtr = nullptr;
  }
}

void FsReq::resetLoadStatsVars() {
  recvQWaitCnt = 0;
  recvQLenAccumulate = 0;
}

std::string FsReq::getOutputStr() {
  std::stringstream ss;
  ss << "appPid:" << app->getPid() << " slotId:" << appRingSlotId
     << " opCode:" << copPtr->opCode;
  return ss.str();
}

void FsReq::markComplete() {
  // TODO change the member variable type
  off_t ring_idx = (off_t)appRingSlotId;
  struct shmipc_msg *msg = NULL;
  msg = IDX_TO_MSG(app->shmipc_mgr, ring_idx);
  SHMIPC_SET_MSG_STATUS(msg, shmipc_STATUS_READY_FOR_CLIENT);
}

void FsReq::addBlockSubmitReq(uint32_t blockNo, BlockBufferItem *item,
                              char *dst, FsBlockReqType type) {
  // numBlockRead++;
  // By default, we do not track all the bno
  //  if (isDebug_) {
  //    readBnoVec.push_back(blockNo);
  //  }
  blockIoHelper->addIoUnitSubmitReq(blockNo, item, dst, type);
}

void FsReq::addSectorSubmitReq(uint32_t sectorNo, BlockBufferItem *bufItem,
                               char *dst, FsBlockReqType type) {
  sectorIoHelper->addIoUnitSubmitReq(sectorNo, bufItem, dst, type);
}

// Note: It is possible that, during the processing of a FsReq, several
// addBlockWriteReq is called for single blockNo.
// Here, we only add the first one.
// void FsReq::addBlockWriteReq(uint32_t blockNo, BlockBufferItem *bufItem,
//                             char *src) {
//  if (writeReqMap.find(blockNo) == writeReqMap.end()) {
//    auto req =
//        new BlockReq(blockNo, bufItem, src, FsBlockReqType::WRITE_NOBLOCKING);
//    writeReqMap.insert(std::make_pair(blockNo, req));
//  } else {
//    // SPDLOG_INFO("FsReq::addBlockWriteReq blockNo:{} already in map",
//    // blockNo);
//  }
//}

void FsReq::addSectorWriteReq(uint32_t sectorNo, BlockBufferItem *bufItem,
                              char *src) {
  // TODO (jingliu)
  throw std::runtime_error("addSectorWriteReq not supportted");
}

void FsReq::submittedBlockReqDone(uint32_t blockNo) {
  blockIoHelper->submittedIoUnitReqDone(blockNo);
}

void FsReq::submittedSectorReqDone(uint32_t sectorNo) {
  sectorIoHelper->submittedIoUnitReqDone(sectorNo);
}

bool FsReq::writeReqDone(uint64_t blockNo, uint64_t blockNoSeqNo,
                         bool &allWriteDone) {
  // if (isDebug_) {
  //   SPDLOG_DEBUG("FsReq::writeReqDone blockNO {} blockNoSeqNo {}", blockNo,
  //                blockNoSeqNo);
  // }
  throw std::runtime_error("writeReqDone should not be called");
  //  auto it = writeReqMap.find(blockNo);
  //  if (it != writeReqMap.end()) {
  //    BlockReq *bReq = it->second;
  //    if (bReq->getBlockNoSeqNo() == blockNoSeqNo) {
  //      writeReqMap.erase(blockNo);
  //      BlockBufferItem *item = bReq->getBufferItem();
  //      item->blockWritenCallback(blockNoSeqNo);
  //      delete bReq;
  //      allWriteDone = writeReqMap.empty();
  //      return true;
  //    }
  //  }
  //  allWriteDone = writeReqMap.empty();
  return false;
}

bool FsReq::writeSectorReqDone(uint64_t sectorNo, uint64_t sectorNoSecNo,
                               bool &allWriteDone) {
  // TODO (jingliu)
  throw std::runtime_error("writeSectorReqDone not supported");
}

void FsReq::setDirInode(InMemInode *dirInode) {
  if (dirInode == nullptr) {
    SPDLOG_ERROR("setDirInode src is nullptr");
  }
  parDirInodePtr = dirInode;
}

void FsReq::setDstDirInode(InMemInode *dstDirInode) {
  if (dstDirInode == nullptr) {
    SPDLOG_ERROR("setDstDirInode to nullptr");
  }
  dstParDirInodePtr = dstDirInode;
}

const InMemInode *FsReq::setTargetInode(InMemInode *inode) {
  return setTargetInodeCommon(inode, fileIno, &targetInodePtr);
}

const InMemInode *FsReq::setDstTargetInode(InMemInode *inode) {
  return setTargetInodeCommon(inode, dstFileIno, &dstTargetInodePtr);
}

const InMemInode *FsReq::setTargetInodeCommon(InMemInode *inode, uint32_t &ino,
                                              InMemInode **inodeAddr) {
  auto origInodePtr = *inodeAddr;
  if (inode == nullptr) {
    SPDLOG_ERROR("setTargetInodeCommon arg-inode is nullptr");
    return origInodePtr;
  }
  if (origInodePtr != nullptr) {
    assert(inode == origInodePtr);
    return origInodePtr;
  }
  ino = inode->i_no;
  *inodeAddr = inode;
  return origInodePtr;
}

block_no_t FsReq::getFileDirentryBlockNo(int &idx) {
  idx = withinBlockDentryIndex;
  return fileDentryDataBlockNo;
}

void FsReq::setFileDirentryBlockNo(block_no_t blkno, int idx) {
  SPDLOG_DEBUG("setFileDirentryBlockNo:{} {}", blkno, idx);
  withinBlockDentryIndex = idx;
  fileDentryDataBlockNo = blkno;
}

block_no_t FsReq::getDstFileDirentryBlockNo(int &idx) {
  idx = dstWithinBlockDentryIndex;
  return dstFileDentryDataBlockNo;
}

void FsReq::setDstFileDirentryBlockNo(block_no_t blkno, int idx) {
  SPDLOG_DEBUG("setDstFileDirentryBlockNo:{} {}", blkno, idx);
  dstWithinBlockDentryIndex = idx;
  dstFileDentryDataBlockNo = blkno;
}

// std::string FsReq::getFileName() {
//   int fileNameIdx = standardFullPathDelimIdx[standardFullPathDepth - 1];
//   return std::string((standardFullPath + fileNameIdx));
// }

// std::string FsReq::getNewFileName() {
//   int fileNameIdx = dstStandardFullPathDelimIdx[dstStandardFullPathDepth -
//   1]; return std::string((newStandardFullPath + fileNameIdx));
// }

std::string FsReq::getStandardFullPath(int &pathDepth) {
  pathDepth = standardFullPathDepth;
  return std::string(standardFullPath);
}

std::string FsReq::getNewStandardFullPath(int &pathDepth) {
  pathDepth = dstStandardFullPathDepth;
  return std::string(newStandardFullPath);
}

std::string FsReq::getStandardPartialPath(absl::string_view leafName,
                                          int &pathDepth) {
  absl::string_view pathView{standardFullPath};
  int endViewIdx = -1;
  pathDepth = -1;
  for (int i = 0; i < standardFullPathDepth; i++) {
    int curDirStartIdx = standardFullPathDelimIdx[i];
    int nextDirStartIdx;
    if (i < standardFullPathDepth - 1) {
      nextDirStartIdx = standardFullPathDelimIdx[i + 1] - 1;
    } else {
      nextDirStartIdx = strlen(standardFullPath);
    }
    if (standardFullPath[nextDirStartIdx] == '/') nextDirStartIdx--;
    if (leafName.compare(pathView.substr(
            curDirStartIdx, nextDirStartIdx - curDirStartIdx + 1)) == 0) {
      pathDepth = i + 1;
      endViewIdx = nextDirStartIdx;
      break;
    }
  }
  if (endViewIdx >= 0) {
    return std::string(standardFullPath, endViewIdx + 1);
  }
  return std::string("");
}

std::string FsReq::getStandardParPath(int &pathDepth) {
  pathDepth = standardFullPathDepth - 1;
  if (standardFullPathDepth <= 1) {
    return "";
  }
  int endViewIdx = standardFullPathDelimIdx[standardFullPathDepth - 1] - 1;
  return std::string(standardFullPath, endViewIdx);
}

std::string FsReq::getNewStandardParPath(int &pathDepth) {
  pathDepth = dstStandardFullPathDepth - 1;
  if (dstStandardFullPathDepth <= 1) {
    return "";
  }
  int endViewIdx =
      dstStandardFullPathDelimIdx[dstStandardFullPathDepth - 1] - 1;
  return std::string(newStandardFullPath, endViewIdx);
}

uint32_t FsReq::getFileInum() {
  if (targetInodePtr != nullptr && targetInodePtr->inodeData->type == T_FILE &&
      fileIno != targetInodePtr->i_no) {
    SPDLOG_ERROR("FsReq::fileIno:{} does not match InMemInode item ino:{}",
                 fileIno, targetInodePtr->i_no);
  }
  return fileIno;
}

uint32_t FsReq::getDstFileInum() {
  if (dstTargetInodePtr != nullptr &&
      dstTargetInodePtr->inodeData->type == T_FILE &&
      fileIno != targetInodePtr->i_no) {
    SPDLOG_ERROR("FsReq::dstfileIno:{} does not match InMemInode item ino:{}",
                 dstFileIno, dstTargetInodePtr->i_no);
  }
  return dstFileIno;
}

std::string FsReq::getErrorMsg() {
  switch (errorNo) {
    case FS_REQ_ERROR_FILE_NOT_FOUND:
      return "File Not Found";
    default:
      return "NO ERROR";
  }
}

// std::string FsReq::outputReadBnos() {}

void FsReq::setBlockIoDone(char *addr) {
  std::uintptr_t dst = reinterpret_cast<std::uintptr_t>(addr);
  assert(blockIoDoneMap.find(dst) == blockIoDoneMap.end());
  // blockIoDoneMap.insert(std::make_pair(dst, true));
  blockIoDoneMap[dst] = true;
  // NOTE: it turns out that emplace() is not more efficient than insert() in
  // this case. There is no constructor overhead here
  // blockIoDoneMap.emplace(dst, true);
}

bool FsReq::getBlockIoDone(char *addr) {
  std::uintptr_t dst = reinterpret_cast<std::uintptr_t>(addr);
  return blockIoDoneMap.find(dst) != blockIoDoneMap.end();
}

void FsReq::setBlockIoDoneByBlockIdx(uint32_t blockIdx) {
  assert(blockIoBlkIdxDoneMap.find(blockIdx) == blockIoBlkIdxDoneMap.end());
  blockIoBlkIdxDoneMap.insert(std::make_pair(blockIdx, true));
}

bool FsReq::getBlockIoDoneByBlockIdx(uint32_t blockIdx) {
  return blockIoBlkIdxDoneMap.find(blockIdx) != blockIoBlkIdxDoneMap.end();
}

char *FsReq::getChannelDataPtr() { return channelDataPtr; }

bool FsReq::isAppCacheAvailable() {
  assert(rwopPtr != nullptr);
#if 0
  SPDLOG_DEBUG("val:{} ENABLE_BUF?{} ENABLE_CACHE?{} BOTH?{}", rwopPtr->flag,
               ((rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_)),
               ((rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_)),
               ((rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_) &&
                (rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_)));
#endif
  return ((rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_) &&
          (rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_CACHE_));
}

void FsReq::extractRwReqPageNos(
    std::unordered_map<off_t, InAppCachedBlock *> &saveTo, bool isWrite) {
  assert(rwopPtr->realCount > 0);
  uint32_t numPages = 0;
  off_t startAlignedOff =
      (rwopPtr->realOffset / (gFsLibMallocPageSize)) * (gFsLibMallocPageSize);
  SPDLOG_DEBUG(
      "extractRwReqPageNos realOffset:{} startAlignedOff:{} isWrite:{}",
      rwopPtr->realOffset, startAlignedOff, isWrite);
  off_t curAlignedOff;
  InAppCachedBlock *curCachedBlock = nullptr;
  while ((uint32_t)(rwopPtr->realOffset + rwopPtr->realCount) >=
         (uint32_t)(startAlignedOff + numPages * (gFsLibMallocPageSize))) {
    curAlignedOff = startAlignedOff + (numPages * gFsLibMallocPageSize);
    SPDLOG_DEBUG("extractRwReqPageNos curAlignedOff:{}", curAlignedOff);
    auto it = saveTo.find(curAlignedOff);
    if (it != saveTo.end()) {
      curCachedBlock = it->second;
    } else {
      curCachedBlock = new InAppCachedBlock();
      curCachedBlock->startOffWithinInode = curAlignedOff;
      if (isWrite) {
        // curCachedBlock->flag |= CFS_FSP_IN_APP_CACHED_BLOCK_DIRTY;
        curCachedBlock->flag = UTIL_BIT_FLIP(
            curCachedBlock->flag, (CFS_FSP_IN_APP_CACHED_BLOCK_DIRTY));
      }
      saveTo[curAlignedOff] = curCachedBlock;
    }
    if (isWrite) {
      if (curCachedBlock->accessSeqNo > getAccessSeqNo()) {
        SPDLOG_ERROR(
            "extractRwReqPageNos cannot updateSeq currentSeq:{} newSeq:{}",
            curCachedBlock->accessSeqNo, getAccessSeqNo());
      }
      SPDLOG_DEBUG("extractRwReqPageNos updateSeq currentSeq:{} newSeq:{}",
                   curCachedBlock->accessSeqNo, getAccessSeqNo());
      curCachedBlock->accessSeqNo = getAccessSeqNo();
    }
    numPages++;
  }
}

FsReqPool::FsReqPool(int wid) : wid_(wid) {
  for (int i = 0; i < kPerWorkerReqPoolCapacity; i++) {
    auto req = new FsReq(wid);
    fsReqQueue_.push_back(req);
  }
}

FsReqPool::~FsReqPool() {
  while (!fsReqQueue_.empty()) {
    auto req = fsReqQueue_.front();
    assert(req != nullptr);
    fsReqQueue_.pop_front();
    delete req;
  }
}

FsReq *FsReqPool::genNewReq(AppProc *appProc, off_t slotId,
                            struct clientOp *cop, char *data,
                            FsProcWorker *worker) {
  if (fsReqQueue_.empty()) {
    return nullptr;
  }
  FsReq *req = fsReqQueue_.front();
  req->initReqFromCop(appProc, slotId, cop, data, worker);
  fsReqQueue_.pop_front();
  req->in_use = true;
  return req;
}

FsReq *FsReqPool::genNewReq() {
  if (fsReqQueue_.empty()) {
    return nullptr;
  }

  FsReq *req = fsReqQueue_.front();
  fsReqQueue_.pop_front();
  req->in_use = true;
  return req;
}

void FsReqPool::returnFsReq(FsReq *req) {
  assert(!req->isReqBgGC());
  req->resetLoadStatsVars();
  // TODO consider using placement new instead of resetting.
  req->resetReq();
  req->wid = wid_;
  fsReqQueue_.push_back(req);
}
// block Req which should be able to translated into BlockDev's API call
// @param blockNo: on-disk block number
// @param dst: memBuff used to read/write data
// @param t: type to chose which dev's function to call (R? W?)
BlockReq::BlockReq(uint64_t blockNo, BlockBufferItem *bufItem, char *dst,
                   FsBlockReqType t)
    : blockNo(blockNo),
      blockNoSeqNo(0),
      memPtr(dst),
      bufferItem(bufItem),
      reqType(t) {}

BlockReq::~BlockReq() {
  // SPDLOG_DEBUG("~BlockReq called blockNo:{}", blockNo);
}

void BlockReq::setBlockNoSeqNo(uint64_t seqNo) {
  blockNoSeqNo = seqNo;
  // TODO (jingliu): the way to handle flushing here is hackish
  assert(bufferItem != nullptr);
  bufferItem->setLastFlushBlockReqSeqNo(seqNo);
}

uint64_t BlockReq::getBlockNoSeqNo() { return blockNoSeqNo; }

bool BlockReq::getSubmitted() { return isSubmitted; }

void BlockReq::setSubmitted(bool b) { isSubmitted = b; }

BlockBufferItem *BlockReq::getBufferItem() { return bufferItem; }

int BufferFlushReq::initFlushReqs(int index) {
  bool canFlush = true;
  // std::vector<block_no_t> flushBlockNo;
  if (index > 0) {
    srcBuf->doFlushByIndex(index, canFlush, toSubmitFlushBlocks);
  } else {
    srcBuf->doFlush(canFlush, toSubmitFlushBlocks);
  }
  assert(canFlush);
  for (auto bno : toSubmitFlushBlocks) {
    if (flushBlockReqMap.find(bno) != flushBlockReqMap.end()) {
      SPDLOG_INFO("ERROR flushBlock Req duplicate item index:{} bno:{}", index,
                  bno);
      return -1;
    }
    BlockBufferItem *curBufItem = srcBuf->getBlockWithoutAccess(bno);
    if (!curBufItem->isBufDirty()) {
      std::cerr << "initFlushReqs: bno:" << bno << " not_dirty" << std::endl;
      if (fsReq != nullptr) {
        std::cerr << "reqType:" << fsReq->getType()
                  << " ino:" << fsReq->getTargetInode()->inodeData->i_no
                  << std::endl;
      } else {
        std::cerr << "no req" << std::endl;
      }
      throw std::runtime_error("buffer is not dirty");
    }
    assert(curBufItem->isBufDirty());
    auto req = new BlockReq(bno, curBufItem, curBufItem->getBufPtr(),
                            FsBlockReqType::WRITE_NOBLOCKING);
    flushBlockReqMap.emplace(bno, req);
  }
  return toSubmitFlushBlocks.size();
}

int BufferFlushReq::submitFlushReqs() {
  int rc;
  if (enableTrace_) {
    submitTs = tapFlushTs();
  }

  int numSubmit = 0;
  block_no_t bno;
  while (!toSubmitFlushBlocks.empty()) {
    if (submittedFlushBlocks.size() > kMaxSubmitBatchSize) break;
    bno = toSubmitFlushBlocks.front();
    rc = submitWorker->submitAsyncBufFlushWriteDevReq(this,
                                                      flushBlockReqMap[bno]);
    if (rc < 0) {
      // submit fail
      SPDLOG_DEBUG("submitFlushReq submit fail. bno:{}", bno);
      break;
    } else {
      numSubmit++;
      toSubmitFlushBlocks.pop_front();
      submittedFlushBlocks.insert(bno);
    }
  }
  return numSubmit;
}

bool BufferFlushReq::checkValidFlushReq(block_no_t blockNo, uint64_t seqNo) {
  auto it = flushBlockReqMap.find(blockNo);
  if (it == flushBlockReqMap.end()) {
    // This block no is not in this flushReq
    return false;
  }
  return it->second->getBlockNoSeqNo() == seqNo;
}

void BufferFlushReq::blockReqFinished(block_no_t blockNo) {
  // here, we do not delete blockReq, which will be deleted
  // by flushDonePropagate().
  submittedFlushBlocks.erase(blockNo);
  if (!toSubmitFlushBlocks.empty()) {
    block_no_t bno = toSubmitFlushBlocks.front();
    int rc = submitWorker->submitAsyncBufFlushWriteDevReq(
        this, flushBlockReqMap[bno]);
    assert(rc >= 0);
    toSubmitFlushBlocks.pop_front();
    submittedFlushBlocks.insert(bno);
  }
}

bool BufferFlushReq::flushDone() {
  return submittedFlushBlocks.empty() && toSubmitFlushBlocks.empty();
}

void BufferFlushReq::flushDonePropagate() {
  std::vector<block_no_t> doneBlocks(flushBlockReqMap.size());
  int i = 0;
  for (auto bnoReqPair : flushBlockReqMap) {
    doneBlocks[i] = bnoReqPair.first;
    i++;
  }
  for (auto bno : doneBlocks) {
    BlockReq *req = flushBlockReqMap[bno];
    delete req;
    flushBlockReqMap[bno] = nullptr;
  }
  int rc = srcBuf->doFlushDone(doneBlocks);
  if (rc < 0) {
    std::cerr << " flushDoenPropage numBlocks:" << doneBlocks.size()
              << std::endl;
    if (fsReq != nullptr) {
      std::cerr << "reqType:" << fsReq->getType()
                << " ino:" << fsReq->getTargetInode()->inodeData->i_no
                << std::endl;
    } else {
      std::cerr << "no req" << std::endl;
    }
    if (fsReq->getType() != FsReqType::SYNCALL) {
      throw std::runtime_error("doFlushDone is wrong");
    }
  }
  if (isAssociatedWithFsReq()) {
    srcBuf->addFgFlushInflightNum(-1);
    if (enableTrace_) {
      doneTs = tapFlushTs();
      fprintf(stderr,
              "--JL-- flushUs:%lu num_blk:%lu submit_ts:%lu "
              "done_ts:%lu inum:%u\n",
              PerfUtils::Cycles::toMicroseconds(doneTs - submitTs),
              flushBlockReqMap.size(), submitTs, doneTs, fsReq->getFileInum());
    }
    if (fsReq->getType() != FsReqType::WSYNC) {
      fsReq->setState(FsReqState::FSYNC_DATA_SYNC_DONE);
    } else {
      fsReq->setState(FsReqState::WSYNC_DATA_SYNC_DONE);
    }
    srcBuf->removeFgFlushWaitIndex(fsReq->getFileInum());
    // NOTE: submitWorker is initially designed to only submit device request
    // Currently, the worker to submit to device is the same as the one that
    // manage client requests. Let's see if there is problems here.
    // 02/04/2020
    submitWorker->submitReadyReq(fsReq);
  } else {
    srcBuf->setIfBgFlushInflight(false);
  }
  assert(rc >= 0);
}

void BufferFlushReq::setFsReq(FsReq *req) {
  if (isAssociatedWithFsReq()) {
    SPDLOG_WARN("This BufferFlushReq already has a fsReq associated with it");
  }
  fsReq = req;
}

/*---- FsLibProc.h ----*/

// TODO: delete after switching to new inode reassignment
int AppProc::findAllOpenedFiles4Ino(uint32_t ino,
                                    std::vector<FileObj *> &files) {
  int numOpened = 0;
  FileObj *newFileObj;
  // Here, the issue is: for openedFiles, when inode is out of this worker
  // the fileObj is still valid
  for (auto &ele : openedFiles) {
    if (ele.second->ip->i_no == ino) {
      // use copy constructor here
      newFileObj = new FileObj(*(ele.second));
      // fprintf(stderr, "copyfobj pid:%d fd:%d ino:%u %p\n", getPid(),
      //        newFileObj->readOnlyFd, ino, newFileObj);
      // ele.second->ip = nullptr;
      files.emplace_back(newFileObj);
      numOpened++;
    }
  }
  return numOpened;
}

// TODO: delete after switching to new inode reassignment
int AppProc::overwriteOpenedFiles(std::vector<FileObj *> &files,
                                  bool directUseVec) {
  int rt = 0;
  for (auto ele : files) {
    int curFd = ele->readOnlyFd;
    auto it = openedFiles.find(curFd);
    if (it != openedFiles.end()) {
      if (directUseVec) {
        FileObj *legacyFileObj = it->second;
        // fprintf(stderr, "overwrite oldfd:%d oldino:%u %p",
        //         legacyFileObj->readOnlyFd, legacyFileObj->ip->i_no,
        //         legacyFileObj);
        // fprintf(stderr, " -- new fd:%d ino:%u %p\n", ele->readOnlyFd,
        //        ele->ip->i_no, ele);
        it->second = ele;
        legacyFileObj->ip = nullptr;
        delete legacyFileObj;
      } else {
        assert(it->second->shadow_ino == ele->shadow_ino);
        if (it->second->shadow_ino != ele->shadow_ino) {
          throw std::runtime_error(
              "cannot overwrite fd. old not contistent with new");
        }
        fileObjCpy(ele, it->second);
      }
    } else {
      if (directUseVec) {
        openedFiles[curFd] = ele;
      } else {
        openedFiles[curFd] = new FileObj(*ele);
      }
    }
    rt++;
    // Here, we make sure the servant's FD will never be duplicate with the FD
    // that has already been used in the master
    // E.g., open() in master get 10, 11 and then split to servant, we need to
    // use 12 here.
    if (curFd >= fdIncr) fdIncr = curFd + 1;
  }

  return rt;
}

int AppProc::addInAppAllocOpDataBlocks(FsReq *req, bool isWrite) {
  auto inode = req->getTargetInode();
  assert(inode != nullptr);
  uint32_t ino = inode->inodeData->i_no;
  SPDLOG_DEBUG("addInAppAllocOpDataBlocks fd:{} ino:{} inMemIno:{}",
               req->getFd(), ino, inode->i_no);
  auto it = pendingInAppBlocks.find(ino);
  if (it == pendingInAppBlocks.end()) {
    pendingInAppBlocks[ino] = std::unordered_map<off_t, InAppCachedBlock *>();
  }
  req->extractRwReqPageNos(pendingInAppBlocks[ino], isWrite);
  if (kIsDebug) {
    dumpInoCachedBlocks(ino);
  }
  return 0;
}

int AppProc::retrieveInAppAllocOpDataBlocks(FsReq *req, bool &allInApp,
                                            uint64_t &maxSeq) {
  auto inode = req->getTargetInode();
  assert(inode != nullptr);
  uint32_t ino = inode->inodeData->i_no;
  SPDLOG_DEBUG("retrieveInAppAllocOpDataBlocks fd:{} ino:{} inMemIno:{}",
               req->getFd(), ino, inode->i_no);

  auto it = pendingInAppBlocks.find(ino);
  if (it == pendingInAppBlocks.end()) {
    // we cannot find this ino in this app's map, which means the record in
    // ProcWorker is wrong
    SPDLOG_ERROR("this inode cannot be found in AppProc's <ino, BlockMap>");
    throw std::runtime_error("inode cannot be found in <ino, BlockMap>");
  }

  if (kIsDebug) {
    dumpInoCachedBlocks(ino);
  }

  auto &offBlkMap = it->second;

  auto rwopPtr = req->getRwOp();

  assert(rwopPtr->realCount > 0);
  off_t startAlignedOff =
      (rwopPtr->realOffset / (gFsLibMallocPageSize)) * (gFsLibMallocPageSize);
  SPDLOG_DEBUG(
      "retrieveInAppAllocOpDataBlocks() realOffset:{} "
      "startAlignedOff:{}",
      rwopPtr->realOffset, startAlignedOff);
  off_t curAlignedOff;
  uint32_t numPages = 0;
  InAppCachedBlock *curCachedBlock = nullptr;
  allInApp = true;

  // fprintf(stderr, "offBlkMap Size:%ld realOffset:%ld startAlignedOff:%ld\n",
  // offBlkMap.size(),
  //        rwopPtr->realOffset, startAlignedOff);
  while ((uint32_t)(rwopPtr->realOffset + rwopPtr->realCount) >=
         (uint32_t)(startAlignedOff + numPages * (gFsLibMallocPageSize))) {
    curAlignedOff = startAlignedOff + (numPages * gFsLibMallocPageSize);

    auto it = offBlkMap.find(curAlignedOff);
    if (it == offBlkMap.end()) {
      SPDLOG_DEBUG(" curAlignedOff:{} not found", curAlignedOff);
      allInApp = false;
      maxSeq = 0;
      break;
    } else {
      curCachedBlock = it->second;
      SPDLOG_DEBUG(
          " curAlignedOff:{} found. CachedBlock Info -"
          " startOffWithinInode:{} flag:{} accessSeqNo:{}",
          curAlignedOff, curCachedBlock->startOffWithinInode,
          curCachedBlock->flag, curCachedBlock->accessSeqNo);
      if (curCachedBlock->accessSeqNo > maxSeq) {
        maxSeq = curCachedBlock->accessSeqNo;
      }
    }

    numPages++;
  }

  if (maxSeq == 0) {
    allInApp = false;
  }

  return 0;
}

void AppProc::dumpInoCachedBlocks(uint32_t ino) {
  auto it = pendingInAppBlocks.find(ino);
  if (it == pendingInAppBlocks.end()) {
    SPDLOG_INFO("cannot find ino");
    return;
  }
  auto &offBlockMap = it->second;
  fprintf(stdout, "======= dumpInoCachedBlocks: ino:%u =======\n", ino);
  for (auto &item : offBlockMap) {
    fprintf(stdout, " --<off:%lu, startOffWithinInode:%lu flag:%d seqNo:%lu ",
            item.first, item.second->startOffWithinInode, item.second->flag,
            item.second->accessSeqNo);
    fprintf(stdout, "\n");
  }
  fprintf(stdout, "======= END =======\n");
}

void *AppProc::getDataPtrByShmNameAndDataId(
    std::string &shmName, fslib_malloc_block_cnt_t dataId,
    fslib_malloc_block_sz_t shmBlockSize,
    fslib_malloc_block_cnt_t shmNumBlocks) {
  int err;
  auto it = shmNameMemArrMap.find(shmName);
  SingleSizeMemBlockArr *memArrPtr = nullptr;
  void *addr;
  SPDLOG_DEBUG(
      "getDataPtrByShmName shmName:{} dataId:{} shmBlockSize:{} "
      "shmNumBlocks:{}",
      shmName, dataId, shmBlockSize, shmNumBlocks);
  if (it == shmNameMemArrMap.end()) {
    SPDLOG_INFO("shm:{} already exists", shmName);
    auto totalBytes = SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(
        shmBlockSize, shmNumBlocks);
    // try to attach to the shm
    addr = shmOpenAttach(shmFd, shmName, totalBytes, err);
    SPDLOG_INFO("shmOpenAttach for shmName:{}", shmName);
    if (addr != nullptr) {
      memArrPtr = new SingleSizeMemBlockArr(shmBlockSize, shmNumBlocks,
                                            totalBytes, addr, shmName, true);
      shmNameMemArrMap.emplace(shmName, memArrPtr);
    } else {
      SPDLOG_WARN("shm attach error name:{} bytes:{}", shmName, totalBytes);
    }
  } else {
    memArrPtr = it->second;
  }
  assert(memArrPtr != nullptr);
  // fprintf(stderr, "dataId:%u startDataPtrVal:%c\n", dataId);
  void *rtPtr = memArrPtr->getDataPtrFromId(dataId);
  return rtPtr;
}

void *AppProc::getDataPtrByShmIdAndDataId(uint8_t shmId,
                                          fslib_malloc_block_cnt_t dataId) {
  auto it = shmIdArrMap.find(shmId);
  SingleSizeMemBlockArr *memArrPtr = nullptr;
  //    SPDLOG_INFO("number of shmId in server:{} recvedID:{} aid:{}",
  //    shmIdArrMap.size(), shmId, appIdx);
  if (it == shmIdArrMap.end()) {
    // let's try to grab it from the global map
    memArrPtr = gFsProcPtr->g_app_shm_ids.QueryAppShm(getPid(), shmId);
    if (memArrPtr == nullptr) {
      throw std::runtime_error(
          "bad shmid received from client. appIdx: " + std::to_string(appIdx) +
          "shmKey:" + std::to_string(shmKey));
    } else {
      shmIdArrMap.emplace(shmId, memArrPtr);
    }
  } else {
    memArrPtr = it->second;
    assert(memArrPtr != nullptr);
  }

  void *rtPtr = memArrPtr->getDataPtrFromId(dataId);
  return rtPtr;
}

std::pair<uint8_t, void *> AppProc::initNewAllocatedShm(
    std::string &shmName, fslib_malloc_block_sz_t shmBlockSize,
    fslib_malloc_block_cnt_t shmNumBlocks) {
  // fprintf(stderr, "===AppProc: initNewAllocatedShm shmName:%s\n",
  // shmName.c_str());
  // FIXME (jingliu), not sure if this is strictly necessary, but I see
  // sometimes it happens that, the same shmName is sent to FSP several times
  auto shmNameIt = shmNameMemArrMap.find(shmName);
  SingleSizeMemBlockArr *arr_ptr = nullptr;
  if (shmNameIt != shmNameMemArrMap.end()) {
    SPDLOG_ERROR("initNewAllocatedShm {} has already been initialized to FSP",
                 shmName);
    int shmId = -1;
    for (auto ele : shmIdArrMap) {
      if (ele.second == shmNameIt->second) {
        shmId = ele.first;
        arr_ptr = ele.second;
        break;
      }
    }
    if (shmId > 0) {
      return {static_cast<uint8_t>(shmId), static_cast<void *>(arr_ptr)};
    }
    // never reach here, if one shmName is recorded, it is only supposed to
    // have single shmId
    throw std::runtime_error("shmName is already recorded");
  }
  // uint8_t curShmId = static_cast<uint8_t>(shmIdArrMap.size()) + 1;
  // use shmIdIncr++ here since, the shmIdArrMap size may be decreased
  // due to app's worker exit()
  uint8_t curShmId = shmIdIncr++;
  auto totalBytes = SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(
      shmBlockSize, shmNumBlocks);
  int err;
  SingleSizeMemBlockArr *memArrPtr = nullptr;
  void *addr;
  // try to attach to the shm
  addr = shmOpenAttach(shmFd, shmName, totalBytes, err);
  //  SPDLOG_INFO("shmOpenAttach for shmName:{} curAppKey:{} appIdx:{}",
  //  shmName,
  //              shmKey, appIdx);
  if (addr != nullptr) {
    memArrPtr = new SingleSizeMemBlockArr(shmBlockSize, shmNumBlocks,
                                          totalBytes, addr, shmName, true);
    shmIdArrMap.emplace(curShmId, memArrPtr);
    shmNameMemArrMap.emplace(shmName, memArrPtr);
    arr_ptr = memArrPtr;
  } else {
    SPDLOG_WARN("shm attach error name:{} bytes:{}", shmName, totalBytes);
  }
  return {curShmId, static_cast<void *>(arr_ptr)};
}

int AppProc::invalidateAppShm() {
  SPDLOG_INFO("invalidateAppShmByName size:{}", shmNameMemArrMap.size());
  SPDLOG_INFO("invalidateAppShmById size:{}", shmIdArrMap.size());
  shmNameMemArrMap.clear();
  shmIdArrMap.clear();
  shmipc_mgr_server_reset(shmipc_mgr);
  return 0;
}

const std::unordered_set<cfs_ino_t> AppProc::_empty_{};
const std::unordered_set<cfs_ino_t> &AppProc::GetAccessedInos(int tid) {
  auto it = accessed_ino.find(tid);
  if (it == accessed_ino.end() || it->second.empty()) {
    return _empty_;
  }
  return (it->second);
}

void AppProc::GetAccessedInoAndErase(int tid,
                                     std::unordered_set<cfs_ino_t> &inos) {
  auto it = accessed_ino.find(tid);
  assert(inos.empty());
  if (it == accessed_ino.end()) {
    return;
  }
  if (!it->second.empty()) {
    std::swap(inos, it->second);
  }
  accessed_ino.erase(it);
}

CommuChannelFsSide::CommuChannelFsSide(pid_t id, key_t shmKey)
    : CommuChannel(id, shmKey) {
  initChannelMemLayout();
}

CommuChannelFsSide::~CommuChannelFsSide() {
  cleanupSharedMemory();
  delete ringBufferPtr;
}

int CommuChannelFsSide::recvSlot(int &dstSid) {
  return ringBufferPtr->dequeue(dstSid);
}

void *CommuChannelFsSide::attachSharedMemory() {
  assert(ringBufferPtr != nullptr);
  shmId = shmget(shmKey, totalSharedMemSize, IPC_CREAT | 0666);
  if (shmId == -1) {
    fprintf(stderr, "error %s\n", strerror(errno));
    throw std::runtime_error("shmget fail");
  }
  assert(shmId != -1);
  totalSharedMemPtr = (char *)shmat(shmId, NULL, 0);
  assert(totalSharedMemPtr != nullptr);
  // FS is in charge of zero the buffer
  memset(totalSharedMemPtr, 0, totalSharedMemSize);
  return totalSharedMemPtr;
}

void CommuChannelFsSide::cleanupSharedMemory() {
  int rc;
  struct shmid_ds shmbuf;
  rc = shmdt(totalSharedMemPtr);
  if (rc < 0) {
    std::cerr << "ERROR FS cannot detach memory" << std::endl;
    exit(1);
  }
  rc = shmctl(shmId, IPC_RMID, &shmbuf);
  if (rc < 0) {
    std::cerr << "ERROR FS cannot remove shmID" << std::endl;
    exit(1);
  }
}

struct clientOp *MockAppProc::emulateGetClientOp(int sid) {
  return nullptr;
}
int MockAppProc::emulateClientOpSubmit(int sid) { return 0; }
void *MockAppProc::emulateGetDataBufPtr(int sid) { return nullptr; }
