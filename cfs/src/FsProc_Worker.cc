#include <cassert>
#include <functional>
#include <iostream>
#include <list>
#include <numeric>

#include "FsMsg.h"
#include "FsProc_App.h"
#include "FsProc_FileMng.h"
#include "FsProc_Fs.h"
#include "FsProc_FsImpl.h"
#include "FsProc_FsInternal.h"
#include "FsProc_Journal.h"
#include "FsProc_TLS.h"
#include "fs_defs.h"
#include "perfutil/Cycles.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "stats/stats.h"

extern FsProc *gFsProcPtr;

// NOTE: must start acceptHandler after all the initialization finished
// read the superblock into the beginning of devBufMemPtr
FsProcWorker::FsProcWorker(int wid, CurBlkDev *d, int shmBaseOffset,
                           std::atomic_bool *workerRunning,
                           PerWorkerLoadStatsSR *stats)
    : jmgr(nullptr),
      wid(wid),
      shmBaseOffset(shmBaseOffset),
      dev(d),
      loopCountBeforeCheckExit(0),
      acceptHandler(nullptr),
      fileManager(nullptr),
      devBufMemPtr(nullptr),
      workerRunning(workerRunning),
      appIdx(0),
      stats_recorder_(wid, reinterpret_cast<uintptr_t>(stats)) {
  // TODO (jingliu): init acceptHandler here
  // Ideally, we want to have another thread that listens on some socket
  // to init the FSP access, which is "handling fs_register()"
  SPDLOG_INFO("FsProc init");
  if (d == nullptr) {
    SPDLOG_WARN("start worker without valid device. FOR TEST USAGE ONLY");
  }
  if (wid < kMasterWidConst) {
    SPDLOG_ERROR("Invalid workerId:{}", wid);
  }

  if (FsWorkerOpStats::kCollectWorkerOpStats) {
    opStatsPtr_ = new FsWorkerOpStats();
  }

  fsReqPool_ = new FsReqPool(wid);
}

FsProcWorker::~FsProcWorker() {
  // for (auto ele : appMap) delete (ele.second);
  for (auto ele : app_pool_) delete ele;
  ////freeBuf here will result in core dump, relies on dev's cleanup
  ////dev->freeBuf(devBufMemPtr);
  if (FsWorkerOpStats::kCollectWorkerOpStats) {
    delete opStatsPtr_;
    opStatsPtr_ = nullptr;
  }
  delete fsReqPool_;
  delete fileManager;
  if (jmgr != nullptr) delete jmgr;
  // TODO: de-construct more
}

void FsProcWorker::initFileLogger(const std::string &loggerName,
                                  const std::string &logFileName) {
  assert(logger == nullptr);
  try {
    logger = spdlog::basic_logger_st(loggerName, logFileName);
    logger->info("======= wid:{} =======", wid);
    logger->flush();
  } catch (const spdlog::spdlog_ex &ex) {
    fprintf(stderr, "Logger to file init fail %s\n", ex.what());
  }
}

bool FsProcWorker::writeToWorkerLog(const std::string &str, bool isFlush) {
  if (logger == nullptr) {
    SPDLOG_INFO(str);
    fflush(stdout);
  } else {
    logger->info(str);
    if (isFlush) {
      logger->flush();
    }
  }
  return true;
}

void FsProcWorker::writeToErrorLog(const std::string &str) {
  if (logger != nullptr) {
    logger->error(str);
  }
}

void FsProcWorker::writeToDbgLog(const std::string &str) {
  if (logger != nullptr) {
    logger->debug(str);
  }
}

void FsProcWorker::ProcessLmRedirectCreation(LmMsgRedirectCreationCtx *ctx) {
  int src_wid = ctx->src_wid;
  int dst_wid = ctx->dst_wid;
  pid_t pid = ctx->pid;
  int tid = ctx->tid;
  if (src_wid == FsProcWorker::kMasterWidConst) {
    assert(dst_wid != FsProcWorker::kMasterWidConst);
    crt_routing_tb_[pid][tid] = dst_wid;
  } else {
    auto pid_it = crt_routing_tb_.find(pid);
    assert(pid_it != crt_routing_tb_.end());
    auto tid_it = pid_it->second.find(tid);
    assert(tid_it != pid_it->second.end() && tid_it->second == src_wid);
    if (dst_wid != FsProcWorker::kMasterWidConst) {
      tid_it->second = dst_wid;
    } else {
      pid_it->second.erase(tid_it);
    }
  }
  FsProcMessage msg;
  msg.type = FsProcMessageType::kLM_RedirectCreationAck;
  msg.ctx = ctx;
  messenger->send_message_to_loadmonitor(getWid(), msg);
}

void FsProcWorker::ProcessLmRebalance(LmMsgRedirectFileSetCtx *ctx) {
  if (dst_known_split_policy_ != nullptr) {
    dst_known_split_policy_->AddCpuAllowance(ctx);
  } else {
    throw std::runtime_error(
        "current policy cannot process LmMsgRedirectFileSet");
  }
}

void FsProcWorker::ProcessLmRedirectFuture(LmMsgRedirectFutureCtx *ctx) {
  int src_wid = ctx->src_wid;
  int dst_wid = ctx->dst_wid;
  pid_t pid = ctx->pid;
  int tid = ctx->tid;
  if (ctx->plan_map.empty() && pid > 0) {
    ctx->plan_map[pid][tid][dst_wid] = 1;
  }

  for (auto &[pid, tid_dst_pct_map] : ctx->plan_map) {
    auto app = GetApp(pid);
    assert(app != nullptr);
    for (auto &[tid, dst_pct_map] : tid_dst_pct_map) {
      // TODO: should this be src_wid not getWid()?
      auto dst_note_ptr = app->ResetDstWid(tid, getWid());
      for (auto [dst_wid, pct] : dst_pct_map) {
        assert(src_wid != dst_wid);
        if (dst_wid == getWid()) {
          app->EraseDstWid(tid);
          break;
        } else {
          app->UpdateDstWid(dst_note_ptr, dst_wid, pct);
        }
      }
      // dst_note_ptr->Print(getWid(), pid, tid, std::cerr);
    }
  }

  FsProcMessage msg;
  msg.type = FsProcMessageType::kLM_RedirectFutureAck;
  msg.ctx = ctx;
  messenger->send_message_to_loadmonitor(getWid(), msg);
  return;
}

void FsProcWorker::ProcessLmJoinAll(LmMsgJoinAllCtx *ctx) {
  int num_ino_in_processing = inodeInProgressFsReqMap_.size();
  if (num_ino_in_processing > 0) {
    SPDLOG_WARN("JoinAll with in_processing:{}", num_ino_in_processing);
    for (auto &[ino, req_set] : inodeInProgressFsReqMap_) {
      fprintf(stderr, "[JoinAll] in_progress ino:%u", ino);
      for (auto req : req_set) {
        std::cerr << req->getType();
      }
      fprintf(stderr, "\n");
    }
    ctx->num_join = -num_ino_in_processing;
    fprintf(stderr, "ctx:%p num_join set to %d\n", ctx, -num_ino_in_processing);
    FsProcMessage msg;
    msg.type = FsProcMessageType::kLM_JoinAllAck;
    msg.ctx = ctx;
    messenger->send_message_to_loadmonitor(getWid(), msg);
    return;
  }
  auto inode_map = fileManager->fsImpl_->GetInodeMapCopy();
  if (!inode_map.empty()) {
    using FR = FileMng::ReassignmentOp;
    int *shared_counter_ptr = new int;
    *shared_counter_ptr = inode_map.size();
    ctx->num_join = *shared_counter_ptr;

    struct OldOwnerCtx {
      int *shared_counter;
      LmMsgJoinAllCtx *orig_ctx;
    };
    struct NewOwnerCtx {
      InMemInode *inode_ptr;
    };
    auto old_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                 void *myctx) {
      auto ctx = reinterpret_cast<OldOwnerCtx *>(myctx);
      *(ctx->shared_counter) = *(ctx->shared_counter) - 1;
      if (*ctx->shared_counter == 0) {
        FsProcMessage msg;
        msg.type = FsProcMessageType::kLM_JoinAllAck;
        msg.ctx = ctx->orig_ctx;
        auto messenger = mng->fsWorker_->messenger;
        messenger->send_message_to_loadmonitor(mng->fsWorker_->getWid(), msg);
        delete ctx->shared_counter;
        ctx->shared_counter = nullptr;
      }
      delete ctx;
    };
    auto new_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                 void *myctx) {
      auto ctx = reinterpret_cast<NewOwnerCtx *>(myctx);
      auto inode_ptr = ctx->inode_ptr;
      mng->fsWorker_->stats_recorder_.ResetInodeStatsOnSplitJoin(inode_ptr);
      ctx->inode_ptr = nullptr;
      delete ctx;
    };
    for (auto [ino, inode] : inode_map) {
      auto old_ctx = new OldOwnerCtx();
      old_ctx->orig_ctx = ctx;
      old_ctx->shared_counter = shared_counter_ptr;
      auto new_ctx = new NewOwnerCtx();
      new_ctx->inode_ptr = inode;
      FR::OwnerExportThroughPrimary(fileManager, ino, ctx->dst_wid,
                                    old_owner_callback, old_ctx,
                                    new_owner_callback, new_ctx);
    }
  } else {
    // directly ACK
    FsProcMessage msg;
    fprintf(stderr, "directAck kLm_JoinAllAck\n");
    msg.type = FsProcMessageType::kLM_JoinAllAck;
    msg.ctx = ctx;
    ctx->num_join = 0;
    messenger->send_message_to_loadmonitor(getWid(), msg);
  }
}

void FsProcWorker::ProcessLmJoinAllCreation(LmMsgJoinAllCreationCtx *ctx) {
  fprintf(stderr, "ProcessLmJoinAllCreation wid:%d\n", getWid());
  int src_wid = ctx->src_wid;
  int dst_wid = ctx->dst_wid;
  // TODO remove this brute force
  auto pid_it = crt_routing_tb_.begin();
  int num_found_tid = 0;
  while (pid_it != crt_routing_tb_.end()) {
    auto &tid_dst = pid_it->second;
    auto it = tid_dst.begin();
    while (it != tid_dst.end()) {
      if (it->second == src_wid) {
        if (dst_wid == FsProcWorker::kMasterWidConst) {
          tid_dst.erase(it++);
        } else {
          it->second = dst_wid;
          ++it;
        }
        num_found_tid++;
      } else {
        ++it;
      }
    }
    if (tid_dst.empty()) {
      crt_routing_tb_.erase(pid_it++);
    } else {
      ++pid_it;
    }
  }
  FsProcMessage msg;
  msg.ctx = ctx;
  msg.type = FsProcMessageType::kLM_JoinAllCreationAck;
  fprintf(stderr, "num_found_tid:%d\n", num_found_tid);
  messenger->send_message_to_loadmonitor(getWid(), msg);
}

int FsProcWorker::CheckFutureRoutingOnReqCompletion(AppProc *app, int tid,
                                                    InMemInode *inode) {
  using FR = FileMng::ReassignmentOp;
  int dst_wid = app->GetDstWid(tid, inode->i_no);
  if (dst_wid >= 0 && dst_wid != getWid()) {
    // Some heuristics of shared files
    //  int inode_last_window_num_tau = inode->GetLastWindowNumTau();
    //  if (inode_last_window_num_tau > 1) {
    //    if (inode->i_no % inode_last_window_num_tau !=
    //        dst_wid % inode_last_window_num_tau) {
    //      return 0;
    //   }
    // }
    auto cur_ino = inode->i_no;
    int num_in_prog = NumInProgressRequests(cur_ino);
    if (num_in_prog > 0) {
      SPDLOG_INFO("cur_ino:{} numInProgress:{}", cur_ino, num_in_prog);
      return -1;
    }
    if (gFsProcPtr->getSplitPolicy() == SPLIT_POLICY_NO_TWEAK_MOD_SHARED) {
      if (getWid() != 0) {
        return getWid();
      }
      auto it = moved_ino_set.find(inode->i_no);
      if (it != moved_ino_set.end()) {
        return -1;
      }
      moved_ino_set.emplace(inode->i_no);
    }
    // SPDLOG_INFO("pid:{} tid:{} ino:{} from:{} to:{}", app->getPid(), tid,
    //             cur_ino, getWid(), dst_wid);

    // fprintf(stderr, "nano:%lu pid:%d tid:%d ino:%u from:%d to:%d\n",
    //         PerfUtils::Cycles::toNanoseconds(PerfUtils::Cycles::rdtsc()),
    //         app->getPid(), tid, cur_ino, getWid(), dst_wid);
    // do migration
    auto old_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                 void *myctx) {
      SPDLOG_DEBUG("CheckFutureRt old_owner");
    };
    auto new_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                 void *myctx) {
      SPDLOG_DEBUG("CheckFutureRt new_owner");
    };
    FR::OwnerExportThroughPrimary(fileManager, cur_ino, dst_wid,
                                  old_owner_callback, nullptr,
                                  new_owner_callback, nullptr);
  }
  return 0;
}

int FsProcWorker::initNewApp(struct AppCredential &cred) {
  SPDLOG_INFO("wid:{} initApp appId:{} shmBaseOffset:{} ", wid, appIdx,
              shmBaseOffset);
  auto *app = new AppProc(appIdx++, shmBaseOffset, cred);
  if (appMap.find(app->getPid()) != appMap.end()) {
    delete app;
    std::cerr << "ERROR FsProc::initNewApp pid already in map" << std::endl;
    return -1;
  }
  app->updateFdIncrByWid(getWid());
  appMap.emplace(app->getPid(), app);
  return 0;
}

int FsProcWorker::InitCredToEmptyApp(struct AppCredential &cred) {
  SPDLOG_INFO("wid:{} initCredToEmptyApp appId:{} pid:{} ", wid, cred.app_idx,
              cred.pid);
  if (cred.app_idx < 0) {
    throw std::runtime_error("app index error (-1)");
  }
  auto app = app_pool_[cred.app_idx];
  app->UpdateCred(cred);
  app->updateFdIncrByWid(getWid());
  appMap.emplace(app->getPid(), app);
  return 0;
}

void FsProcWorker::InitEmptyApp(int num_app_obj) {
  if ((uint32_t)num_app_obj <= appIdx) return;
  struct AppCredential empty_cred;
  app_pool_.resize(num_app_obj, nullptr);
  while (appIdx < (uint32_t)num_app_obj) {
    auto app = new AppProc(appIdx++, shmBaseOffset, empty_cred);
    app_pool_[appIdx - 1] = app;
  }
}

void FsProcWorker::recvLoadRebalanceShare(
    fsp_lm::PerWorkerLoadAllowance *allow) {
  assert(allow->wid == getWid());
  assert(splitPolicy_ != nullptr);
  splitPolicy_->recvRebalanceCallback(allow);
}

// used by async device to submit completion request
// will check the pending requests and add them to the thread's pending list
// Only used in read-path
// @param ctx
// @return
int FsProcWorker::submitDevAsyncReadReqCompletion(struct BdevIoContext *ctx) {
  uint64_t blockNo = ctx->blockNo;
  std::unordered_map<uint64_t, std::unordered_set<FsReq *>>
      *curUnitPendingFsReqs = &blkReadPendingFsReqs;
  std::unordered_map<uint64_t, FsReq *> *curUnitPendingSubmitFsReq =
      &blkReadPendingSubmitFsReq;
  if (ctx->reqType == BlkDevReqType::BLK_DEV_REQ_SECTOR_READ) {
    SPDLOG_DEBUG("submitDevAsyncReadReqComp - reqType BlkDEV req SECTOR read");
    curUnitPendingFsReqs = &sectorReadPendingFsReqs;
    curUnitPendingSubmitFsReq = &sectorReadPendingSubmitFsReq;
  } else {
    // SPDLOG_DEBUG("submitDevAsyncReadReqComp - reqType Normal");
  }

  if (isBitmapBlock(blockNo)) {
    jmgr->CopyBufToStableDataBitmap(blockNo, ctx->buf);
  }

  // inode bitmaps are loaded once in FsProcWorkerMaster
  assert(!isInodeBitmap(blockNo));

  // submit ready FsReq
  auto blockReqQ = curUnitPendingFsReqs->find(blockNo);
  if (blockReqQ == curUnitPendingFsReqs->end()) {
    throw std::runtime_error(
        "cannot find the FsReqQ that waits on this block's completion");
  }
  std::unordered_set<FsReq *> &reqs = blockReqQ->second;
  for (auto curReq : reqs) {
    if (ctx->reqType == BlkDevReqType::BLK_DEV_REQ_SECTOR_READ) {
      curReq->sectorWaitReqDone(blockNo);
    } else {
      curReq->blockWaitReqDone(blockNo);
    }
    if (curReq->numTotalPendingIoReq() == 0) {
      submitReadyReq(curReq);
    }
  }
  reqs.clear();
  curUnitPendingFsReqs->erase(blockNo);
  // clean up the FsReq which submit the block request and thus set the block
  // status by the blockBufferItem's callback (inMem status)
  auto it = curUnitPendingSubmitFsReq->find(blockNo);
  FsReq *submitReq;
  if (it != curUnitPendingSubmitFsReq->end()) {
    submitReq = it->second;
    curUnitPendingSubmitFsReq->erase(it);
    if (ctx->reqType == BlkDevReqType::BLK_DEV_REQ_SECTOR_READ) {
      submitReq->submittedSectorReqDone(blockNo);
    } else {
      submitReq->submittedBlockReqDone(blockNo);
    }
  } else {
    SPDLOG_ERROR(
        "submitDevAsyncReadReqCompletion cannot find submission request");
    dev->releaseBdevIoContext(ctx);
    return -1;
  }
  dev->releaseBdevIoContext(ctx);
  return 0;
}

int FsProcWorker::submitBlkWriteReqCompletion(struct BdevIoContext *ctx) {
  // First check if this request is a flushing write request.
  auto flushReqIt = blockFlushReqMap.find(ctx->blockNo);
  if (flushReqIt != blockFlushReqMap.end()) {
    // Check if this write request is truly issued by flushing by comparing
    // sequence number. When *fsync* is issued to a block that is managed by
    // flushReq. It is possible that a blockNo exists both in
    // *blockInflightWriteReqs* and *blockFlushReq*.
    if (flushReqIt->second->checkValidFlushReq(ctx->blockNo,
                                               ctx->blockNoSeqNo)) {
      submitBlkFlushWriteReqCompletion(ctx->blockNo, flushReqIt->second);
    }
  } else {
    uint64_t blockNo = ctx->blockNo;
    std::list<FsReq *> &fsReqs = blockInflightWriteReqs.find(blockNo)->second;
    FsReq *reqPtr;
    throw std::runtime_error(
        "submitBlkWriteReqCompletion: deprecated code path");

    // SPDLOG_DEBUG("submitBlkWriteReqCompletion blockNo: {} blockNoSeq: {}
    // blockListLen:{}",
    //             blockNo, ctx->blockNoSeqNo, fsReqs.size());

    for (auto it = fsReqs.begin(); it != fsReqs.end(); ++it) {
      reqPtr = *it;
      bool allWriteDone;
      bool blockReqFound =
          reqPtr->writeReqDone(blockNo, ctx->blockNoSeqNo, allWriteDone);
      if (blockReqFound) {
        // Remove this request.
        fsReqs.erase(it++);  // <=> it = fsReqs.erase(it);
        if (allWriteDone) {
          stats_recorder_.RecordOnFsReqCompletion(reqPtr,
                                                  dst_known_split_policy_);
          // Destrustor of FsReq is called here.
          fsReqPool_->returnFsReq(reqPtr);
        }
      }
    }
    if (fsReqs.empty()) {
      blockInflightWriteReqs.erase(blockNo);
    }
  }

  // Release the context
  if (dev->reduceInflightWriteNum(1) < 0) {
    SPDLOG_ERROR("Cannot reduce inflight request num");
  }
  dev->releaseBdevIoContext(ctx);
  return 0;
}

int FsProcWorker::submitBlkFlushWriteReqCompletion(block_no_t bno,
                                                   BufferFlushReq *flushReq) {
  // fprintf(stderr, "flushWriteReq Completion bno:%u\n", bno);
  // SPDLOG_INFO("flushWriteReq completion bno:{} mapSize:{}", bno,
  // blockFlushReqMap.size());
  flushReq->blockReqFinished(bno);
  if (flushReq->flushDone()) {
    flushReq->flushDonePropagate();
    delete flushReq;
  }
  blockFlushReqMap.erase(bno);
  return 0;
}

int FsProcWorker::submitDirectReadDevReq(BlockReq *req) {
  if (req->getReqType() == FsBlockReqType::READ_BLOCKING) {
    return dev->blockingRead(req->getBlockNo(), req->getBufPtr());
  } else if (req->getReqType() == FsBlockReqType::READ_BLOCKING_SECTOR) {
    return dev->blockingReadSector(req->getBlockNo(), req->getBufPtr());
  } else {
    throw std::runtime_error("FsBlockReqType not supported");
  }
}

int FsProcWorker::submitDirectWriteDevReq(BlockReq *req) {
  // SPDLOG_DEBUG("submitDirectWriteReq: blockNo:{}", req->getBlockNo());
  int rt;
  if (req->getReqType() == FsBlockReqType::WRITE_BLOCKING_SECTOR) {
    rt = dev->blockingWriteSector(req->getBlockNo(), 0, req->getBufPtr());
  } else {
    rt = dev->blockingWrite(req->getBlockNo(), req->getBufPtr());
  }
  return rt;
}

int FsProcWorker::submitAsyncReadDevReq(FsReq *fsReq, BlockReq *blockReq) {
  int rc;
  // only reach here if using SPDK to access device
  std::unordered_map<uint64_t, std::unordered_set<FsReq *>>
      *curUnitPendingFsReqs = &blkReadPendingFsReqs;
  std::unordered_map<uint64_t, FsReq *> *curUnitPendingSubmitFsReq =
      &blkReadPendingSubmitFsReq;
  if (blockReq->getReqType() == FsBlockReqType::READ_NOBLOCKING_SECTOR) {
    curUnitPendingFsReqs = &sectorReadPendingFsReqs;
    curUnitPendingSubmitFsReq = &sectorReadPendingSubmitFsReq;
  }
  auto it = curUnitPendingSubmitFsReq->find(blockReq->getBlockNo());
  if (it != curUnitPendingSubmitFsReq->end()) {
    SPDLOG_ERROR(
        "ERROR submitAsyncReadDevReq, block blockReq has been submitted, not "
        "yet finished ");
    throw std::runtime_error("submit asyncRead, blockReq submit, not done");
  }
  curUnitPendingSubmitFsReq->insert({blockReq->getBlockNo(), fsReq});
  curUnitPendingFsReqs->insert(
      {blockReq->getBlockNo(), std::unordered_set<FsReq *>()});
  if (blockReq->getReqType() == FsBlockReqType::READ_NOBLOCKING) {
    rc = dev->read(blockReq->getBlockNo(), blockReq->getBufPtr());
  } else if (blockReq->getReqType() == FsBlockReqType::READ_NOBLOCKING_SECTOR) {
    rc = dev->readSector(blockReq->getBlockNo(), blockReq->getBufPtr());
  } else {
    SPDLOG_ERROR("FsBlockReqType not supposed to come here");
    throw std::runtime_error("BlockReqType not supported");
  }
  return rc;
}

// NOTE: this is currently not used
// Now, the disk write happens in: submitAsyncBufFlushWriteDevReq
// We may still need this for writing the metadata
int FsProcWorker::submitAsyncWriteDevReq(FsReq *fsReq, BlockReq *blockReq) {
  int rc = 0;
  if (!dev->isSpdk()) {
    rc = dev->write(blockReq->getBlockNo(), blockReq->getBlockNoSeqNo(),
                    blockReq->getBufPtr());
    if (rc > 0) {
      blockReq->getBufferItem()->blockWritenCallback(
          blockReq->getBlockNoSeqNo());
    }
    return rc;
  }
  // SPDLOG_DEBUG("submitAsyncWriteDevReq blockNo:{}", blockReq->getBlockNo());
  auto it = blockInflightWriteReqs.find(blockReq->getBlockNo());
  if (it == blockInflightWriteReqs.end()) {
    blockInflightWriteReqs.insert(
        {blockReq->getBlockNo(), std::list<FsReq *>()});
  }
  uint64_t cur_req_seq = PerfUtils::Cycles::rdtsc();
  (blockInflightWriteReqs[blockReq->getBlockNo()]).push_back(fsReq);
  if (blockReq->getReqType() == FsBlockReqType::WRITE_NOBLOCKING) {
    rc = dev->write(blockReq->getBlockNo(), cur_req_seq, blockReq->getBufPtr());
  } else {
    throw std::runtime_error("write req does not support to be blocking");
  }
  blockReq->setSubmitted(true);
  blockReq->setBlockNoSeqNo(cur_req_seq);
  return rc;
}

int FsProcWorker::submitPendOnBlockFsReq(FsReq *fsReq, uint64_t blockNo) {
  // SPDLOG_DEBUG("submitPendOnBlockFsReq blockNo:{}", blockNo);
  auto it = blkReadPendingSubmitFsReq.find(blockNo);
  if (it == blkReadPendingSubmitFsReq.end()) {
    SPDLOG_INFO(
        "Cannot pend for the request, block(#:{}) not submitted, "
        "pendingMap size:{}",
        blockNo, blkReadPendingSubmitFsReq.size());
    // try send it to ready list
    submitReadyReq(fsReq);
    return 0;
  }
  auto qIter = blkReadPendingFsReqs.find(blockNo);
  (qIter->second).emplace(fsReq);
  return 1;
}

int FsProcWorker::submitPendOnSectorFsReq(FsReq *fsReq, uint64_t blockNo) {
  SPDLOG_DEBUG("submitPendOnSectorFsReq blockNo:{}", blockNo);
  auto it = sectorReadPendingSubmitFsReq.find(blockNo);
  if (it == sectorReadPendingSubmitFsReq.end()) {
    SPDLOG_INFO(
        "Cannot pend for the request, sector(#:{}) not submitted, "
        "pendingMap size:{}",
        blockNo, sectorReadPendingSubmitFsReq.size());
    // try send it to ready list
    submitReadyReq(fsReq);
    return 0;
  }
  auto qIter = sectorReadPendingFsReqs.find(blockNo);
  (qIter->second).emplace(fsReq);
  return 1;
}

int FsProcWorker::submitAsyncBufFlushWriteDevReq(BufferFlushReq *bufFlushReq,
                                                 BlockReq *blockReq) {
  if (blockFlushReqMap.find(blockReq->getBlockNo()) != blockFlushReqMap.end()) {
    SPDLOG_ERROR(
        "subimtAsyncBufFlushWrite block already submitted blockNo:{} "
        "mapSize:{}",
        blockReq->getBlockNo(), blockFlushReqMap.size());
    auto req = bufFlushReq->fsReq;
    if (req != nullptr) {
      uint32_t ino = 0;
      auto inode = req->getTargetInode();
      if (inode != nullptr) {
        ino = inode->i_no;
      }
      SPDLOG_INFO("reqType:{} ino:{} fileType:{}", req->getType(), ino,
                  inode->inodeData->type);
    } else {
      SPDLOG_INFO("no req associated");
    }
    return -1;
  }
  int rc;
  uint64_t curReqSeq = PerfUtils::Cycles::rdtsc();
  // SPDLOG_INFO("submitBlockNo:{}", blockReq->getBlockNo());
  blockFlushReqMap.insert(std::make_pair(blockReq->getBlockNo(), bufFlushReq));
  if (blockReq->getReqType() == FsBlockReqType::WRITE_NOBLOCKING) {
    rc = dev->write(blockReq->getBlockNo(), curReqSeq, blockReq->getBufPtr());
  } else {
    SPDLOG_ERROR("Only support WRITE_NOBLOCKING");
    throw std::runtime_error("only support WRITE_NOBLOCKING");
  }
  if (rc >= 0) {
    blockReq->setSubmitted(true);
    blockReq->setBlockNoSeqNo(curReqSeq);
  } else {
    throw std::runtime_error("cannot submit device request");
  }
  return rc;
}

void FsProcWorker::onSyncallCompletion(FsReq *req, void *ctx) {
  // TODO : eventually when every type has it's own completion function this
  // will expand to whatever is implemented in submitFsReqCompletion for
  // Syncall.
  auto worker = static_cast<FsProcWorker *>(ctx);
  worker->submitFsReqCompletion(req);
}

// handle the FINI task of each FsReq
// NOTE: directly set the cop's status to OP_DONE, which
// will be checked by client, not sure if there is race here.
// May be use atomic test and set? Let's see if race here
// @param fsReq
// @return
int FsProcWorker::submitFsReqCompletion(FsReq *fsReq) {
  auto curType = fsReq->getType();
  auto app = fsReq->getApp();
  assert(app != nullptr);
  clientOp *cop = nullptr;

  // TODO measure using closure vs argument for this lambda?
  auto getReturnValueForFailedReq = [](FsReq *fsReq) {
    assert(fsReq->hasError());
    auto errNo = static_cast<int>(fsReq->getErrorNo());
    if (errNo != FS_REQ_ERROR_INODE_REDIRECT) return errNo;
    // NOTE: we embed the wid in the return value itself
    return FS_REQ_ERROR_INODE_REDIRECT - fsReq->getWid();
  };

  char *packedMsg = (char *)IDX_TO_XREQ(app->shmipc_mgr, fsReq->getSlotId());
#define pack_msg(st, op_name) \
  pack_##st(&(cop->op.op_name), (struct st##Packed *)packedMsg)
#define copy_msg(op_name) \
  memcpy(packedMsg, &(cop->op.op_name), sizeof(cop->op.op_name))

  // See if this request enables the tracking of number of BIO requests
#if 0
  if (kFsReqBioNumTracking.find(curType) != kFsReqBioNumTracking.end()) {
    cfs_tid_t curTid = cfsGetTid();
    workerFsReqNumBioMap_[curTid].push_back(fsReq->getNumReadBlocks());
  }
#endif
  bool do_check_inode_migration =
      ((fsReq->getReqTypeFlags() & FsReqFlags::handled_by_owner) &&
       !fsReq->hasError());

  // process completion according to different req types
  if (curType == FsReqType::READ) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      SPDLOG_DEBUG("submitFsReqCompletion for read ret:{}",
                   cop->op.read.rwOp.ret);
      if (fsReq->hasError()) {
        cop->op.read.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("read-err set return value to:{}", cop->op.read.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(readOp, read);
    }
  } else if (curType == FsReqType::PREAD) {
    SPDLOG_DEBUG("submitFsReqCompletion for pread");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.pread.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("pread-err set return value to:{}",
                     cop->op.pread.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(preadOp, pread);
    }
  } else if (curType == FsReqType::PREAD_UC) {
    SPDLOG_DEBUG("submitFsReqCompletion for pread_uc");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.pread.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("pread-uc set return value to:{}", cop->op.pread.rwOp.ret);
      }
      cop->opStatus = OP_DONE;
      pack_msg(preadOp, pread);
    }
  } else if (curType == FsReqType::PREAD_UC_LEASE_RENEW_ONLY) {
    cop = fsReq->getClientOp();
    cop->opStatus = OP_DONE;
    pack_msg(preadOp, pread);
  } else if (curType == FsReqType::WRITE) {
    SPDLOG_DEBUG("submitFsReqCompletion for write");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.write.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("write-err set return value to:{}",
                     cop->op.write.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(writeOp, write);
    }
  } else if (curType == FsReqType::PWRITE) {
    SPDLOG_DEBUG("submitFsReqCompletion for pwrite");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.pwrite.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("pwrite-err set return value to:{}",
                     cop->op.pwrite.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(pwriteOp, pwrite);
    }
  } else if (curType == FsReqType::ALLOC_READ) {
    SPDLOG_DEBUG("submitFsReqCompletion for allocread");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.allocread.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("allocpread-err set return value to:{}",
                     cop->op.allocread.rwOp.ret);
      } else {
        opStatsAccountSingleOpDone(FsReqType::ALLOC_READ,
                                   cop->op.allocread.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(allocatedReadOp, allocread);
    }
  } else if (curType == FsReqType::ALLOC_PREAD) {
    SPDLOG_DEBUG("submitFsReqCompletion for allocpread");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.allocpread.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("allocpread-err set return value to:{}",
                     cop->op.allocpread.rwOp.ret);
      } else {
        opStatsAccountSingleOpDone(FsReqType::ALLOC_PREAD,
                                   cop->op.allocpread.rwOp.ret);
      }
      cop->opStatus = OP_DONE;
      pack_msg(allocatedPreadOp, allocpread);
    }
  } else if (curType == FsReqType::ALLOC_WRITE) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      SPDLOG_DEBUG("submitFsReqCompletion for allocwrite ret:{}",
                   cop->op.allocwrite.rwOp.ret);
      if (fsReq->hasError()) {
        cop->op.allocwrite.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("allocwrite-err set return value to:{}",
                     cop->op.allocwrite.rwOp.ret);
      } else {
        opStatsAccountSingleOpDone(FsReqType::ALLOC_WRITE,
                                   cop->op.allocwrite.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(allocatedWriteOp, allocwrite);
    }
  } else if (curType == FsReqType::ALLOC_PWRITE) {
    SPDLOG_DEBUG("submitFsReqCompletion for allocpwrite");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.allocpwrite.rwOp.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("allocpwrite-err set return value to:{}",
                     cop->op.allocpwrite.rwOp.ret);
      } else {
        opStatsAccountSingleOpDone(FsReqType::ALLOC_PWRITE,
                                   cop->op.allocpwrite.rwOp.ret);
      }

      cop->opStatus = OP_DONE;
      pack_msg(allocatedPwriteOp, allocpwrite);
    }
  } else if (curType == FsReqType::ALLOC_PREAD_RENEW_ONLY) {
    SPDLOG_DEBUG("submitFsReqCompletion for ALLOC_PREAD_RENEW_ONLY");
    // if (it != appMap.end()) {
    cop = fsReq->getClientOp();
    cop->opStatus = OP_DONE;
    pack_msg(allocatedPreadOp, allocpread);
  } else if (curType == FsReqType::CREATE &&
             fsReq->getClientOp()->opCode != CfsOpCode::CFS_OP_OPEN) {
    SPDLOG_ERROR("NOT SUPPORTED: create called not via open()");
    throw std::runtime_error("not SUPPORTED create calld not via open");
  } else if (curType == FsReqType::LSEEK) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      SPDLOG_DEBUG("submitFsReqCompletion for lseek fd:{}", cop->op.lseek.fd);
      if (fsReq->hasError()) {
        cop->op.lseek.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("lseek-err set return value to:{}", cop->op.lseek.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(lseek);
    }
  } else if (curType == FsReqType::OPEN ||
             fsReq->getClientOp()->opCode == CfsOpCode::CFS_OP_OPEN) {
    // handle CREATE in OPEN, which is similar to POSIX open() semantics
    SPDLOG_DEBUG("submitFsReqCompletion for open wid:{}", getWid());

    cop = fsReq->getClientOp();
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      if (fsReq->hasError()) {
        cop->op.open.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("open-err set return value to:{}", cop->op.open.ret);
      } else {
        assert(fsReq->getTargetInode() != nullptr);
        if (cop->op.open.lease) {
          cop->op.open.size = fsReq->getTargetInode()->inodeData->size;
          fsReq->getTargetInode()->leased_app.insert(fsReq->getApp()->getPid());
        }
        auto fobj = it->second->allocateFd(
            fsReq->getTargetInode(), cop->op.open.flags, cop->op.open.mode);
        fileManager->addFdMappingOnOpen(fsReq->getPid(), fobj);
        fsReq->getApp()->AccessIno(fsReq->getTid(), fsReq->getFileInum());
        // fill the return
        cop->op.open.ret = fobj->readOnlyFd;
        EMBEDED_INO_FILED_OP_OPEN(&(cop->op.open)) =
            (int)(fsReq->getTargetInode()->i_no);
        SPDLOG_DEBUG("open return fd:{} fname:{} ino:{}", cop->op.open.ret,
                     cop->op.open.path, fsReq->getTargetInode()->i_no);
        // if create, we add it here
        do_check_inode_migration = true;
      }
      cop->opStatus = OP_DONE;
      copy_msg(open);

    }  // it != appMap.end()
  } else if (curType == FsReqType::CLOSE) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      SPDLOG_DEBUG("submitFsReqCompletion for close fd:{} wid:{}",
                   cop->op.close.fd, getWid());
      if (fsReq->hasError()) {
        cop->op.close.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("close-err set return value to:{}", cop->op.close.ret);
      } else {
        // TODO: temporary fix
        // fsReq->getApp()->EraseIno(fsReq->getTid(), fsReq->getFileInum());
      }
      do_check_inode_migration = false;
      cop->opStatus = OP_DONE;
      copy_msg(close);
    }
  } else if (curType == FsReqType::STAT) {
    SPDLOG_DEBUG("submitFsReqCompletion for stat wid:{}", getWid());
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.stat.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("stat-err set return value to:{}", cop->op.stat.ret);
      } else {
        if (fsReq->getTargetInode()->inodeData->type == T_FILE)
          fsReq->getApp()->AccessIno(fsReq->getTid(), fsReq->getFileInum());
        opStatsAccountSingleOpDone(FsReqType::STAT, 1);
      }
      cop->opStatus = OP_DONE;
      copy_msg(stat);
    }
  } else if (curType == FsReqType::FSTAT) {
    SPDLOG_DEBUG("submitFsReqCompletion for fstat");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.fstat.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("fstat-err set return value to:{}", cop->op.fstat.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(fstat);
    }
  } else if (curType == FsReqType::MKDIR) {
    SPDLOG_DEBUG("submitFsReqCompletion for mkdir");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.mkdir.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("mkdir-err set return value to:{}", cop->op.mkdir.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(mkdir);
    }
  } else if (curType == FsReqType::UNLINK) {
    SPDLOG_DEBUG("submitFsReqCompletion for unlink");
    // std::cerr << "unlink ino:" << fsReq->getTargetInode()->inodeData->i_no
    //          << std::endl;
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      SPDLOG_DEBUG("unlink name:{} ret:{}", cop->op.unlink.path,
                   cop->op.unlink.ret);
      if (fsReq->hasError()) {
        cop->op.unlink.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("unlink-err set return value to:{}", cop->op.unlink.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(unlink);
    }
  } else if (curType == FsReqType::RENAME) {
    SPDLOG_DEBUG("submitFsReqCompletion for rename");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      SPDLOG_DEBUG("rename from:{} to:{}", cop->op.rename.oldpath,
                   cop->op.rename.newpath);
      if (fsReq->hasError()) {
        cop->op.rename.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("rename-err set return value to:{}", cop->op.rename.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(rename);
    }
  } else if (curType == FsReqType::OPENDIR) {
    SPDLOG_DEBUG("submitFsReqCompletion for opendir");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        SPDLOG_DEBUG("OPENDIR error setNUmDentry to -1");
        // TODO have returncode for opendir as well
        cop->op.opendir.numDentry = getReturnValueForFailedReq(fsReq);
      }
      cop->opStatus = OP_DONE;
      copy_msg(opendir);
    }
  } else if (curType == FsReqType::FDATA_SYNC || curType == FsReqType::FSYNC) {
    SPDLOG_DEBUG("submitFsReqCompletion for fsync ret:{}",
                 fsReq->getClientOp()->op.fsync.ret);
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.fsync.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("fsync-err set return value to:{}", cop->op.fsync.ret);
      } else {
        cop->op.fsync.ret = 0;
      }
      cop->opStatus = OP_DONE;
      copy_msg(fsync);
    }
  } else if (curType == FsReqType::WSYNC) {
    SPDLOG_DEBUG("submitFsReqCompletion for wsync ret:{}");

    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.fsync.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("fsync-err set return value to:{}", cop->op.fsync.ret);
      } else {
        auto wsync_control_it = fileManager->inflight_wsync_req.find(fsReq);
        if (wsync_control_it == fileManager->inflight_wsync_req.end()) {
          throw std::runtime_error(
              "submit req completion req cannot find in control");
        }
        fileManager->inflight_wsync_req.erase(wsync_control_it);
        cop->op.wsync.ret = 0;
      }
      cop->opStatus = OP_DONE;
      copy_msg(fsync);
    }
  } else if (curType == FsReqType::SYNCALL) {
    SPDLOG_DEBUG("submitFsReqCompletion for Syncall");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        SPDLOG_DEBUG("syncall has error");
      }
      cop->opStatus = OP_DONE;
      copy_msg(syncall);
    }
  } else if (curType == FsReqType::SYNCUNLINKED) {
    SPDLOG_DEBUG("submitFsReqCompletion for Syncunlinked");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        SPDLOG_DEBUG("syncunlinked has error");
      }
      cop->opStatus = OP_DONE;
      copy_msg(syncunlinked);
    }
  } else if (curType == FsReqType::NEW_SHM_ALLOCATED) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      cop->opStatus = OP_DONE;
      copy_msg(newshmop);
    }
  } else if (curType == FsReqType::START_DUMP_LOADSTAT) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      cop->opStatus = OP_DONE;
      copy_msg(startdumpload);
    }
  } else if (curType == FsReqType::STOP_DUMP_LOADSTAT) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      cop->opStatus = OP_DONE;
      copy_msg(stopdumpload);
    }
  } else if (curType == FsReqType::APP_EXIT) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.exit.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("exit-err set return value to:{}", cop->op.exit.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(exit);
    }
  } else if (curType == FsReqType::APP_CHKPT) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      clientOp *cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.chkpt.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("chkpt-err set return value to:{}", cop->op.chkpt.ret);
      }
      cop->opStatus = OP_DONE;
      copy_msg(chkpt);
    }
  } else if (curType == FsReqType::APP_MIGRATE) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      clientOp *cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.migrate.ret = getReturnValueForFailedReq(fsReq);
      }
      cop->opStatus = OP_DONE;
      copy_msg(migrate);
    }
  } else if (curType == FsReqType::PING) {
    SPDLOG_DEBUG("submitFsReqCompletion for ping");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.ping.ret = getReturnValueForFailedReq(fsReq);
        SPDLOG_DEBUG("ping-err set return value to:{}", cop->op.ping.ret);
      }
      // opStatsAccountSingleOpDone(FsReqType::PING, 1);
      cop->opStatus = OP_DONE;
      copy_msg(ping);
    }
  } else if (curType == FsReqType::DUMPINODES) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      clientOp *cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.dumpinodes.ret = getReturnValueForFailedReq(fsReq);
      }
      cop->opStatus = OP_DONE;
      copy_msg(dumpinodes);
    }
  } else if (curType == FsReqType::INODE_REASSIGNMENT) {
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.inodeReassignment.ret = getReturnValueForFailedReq(fsReq);
      }
      cop->opStatus = OP_DONE;
      copy_msg(inodeReassignment);
    }
  } else if (curType == FsReqType::THREAD_REASSIGN) {
    SPDLOG_DEBUG("completion THREAD_REASSIGN");
    auto it = appMap.find(fsReq->getPid());
    if (it != appMap.end()) {
      cop = fsReq->getClientOp();
      if (fsReq->hasError()) {
        cop->op.threadReassign.ret = getReturnValueForFailedReq(fsReq);
      }
      cop->opStatus = OP_DONE;
      copy_msg(threadReassign);
    }
  } else {
    SPDLOG_INFO("ERROR reqType not supported");
    return -1;
  }

#undef copy_msg
#undef pack_msg

  // make the STATUS "DONE" visible to clients
  fsReq->markComplete();

  if (fsReq->getState() != FsReqState::OP_OWNERSHIP_UNKNOWN &&
      fsReq->getState() != FsReqState::OP_OWNERSHIP_REDIRECT) {
    if (fsReq->getState() != FsReqState::OP_REQTYPE_OUT_OF_CAP) {
      // for above states, we do not need to do delInProgress, because
      // they were rejected before sent into inProgress records
      if (!isFsReqNotRecord(fsReq->getType())) {
        // certain types of FsReq will not be recorded
        if (fsReq->targetInodePtr != nullptr &&
            fsReq->targetInodePtr->inodeData->type == T_FILE)
          delInProgressFsReq(fsReq);
      }
    }
  }

  fsReq->stopOnCpuTimer();

  if (do_check_inode_migration &&
      fileManager->fsImpl_->BufferSlotAllowMigrate()) {
    auto inode = fsReq->getTargetInode();
    if (inode != nullptr && inode->inodeData->type == T_FILE) {
      auto cur_ino = inode->i_no;
      // NOTE we get cur_ino here incase the inode is already in reassignment
      CheckFutureRoutingOnReqCompletion(app, fsReq->getTid(), inode);
      fsReq->SetLazyFileInoCopy(cur_ino);
    }
  }

  if (!fsReq->isReqBgGC()) {
    stats_recorder_.RecordOnFsReqCompletion(fsReq, dst_known_split_policy_);
    releaseFsReq(fsReq);
  }
  return 0;
}

int FsProcWorker::addInAppInodeRWFsReq(FsReq *req, InMemInode *fileInode) {
  auto curApp = getInAppInodeCtx(req, fileInode, true);
  if (curApp == nullptr) {
    return -1;
  }
  // save this req's related block into App object
  bool isWrite = req->getType() == FsReqType::ALLOC_PWRITE ||
                 req->getType() == FsReqType::ALLOC_WRITE;
  if (isWrite) {
    // for write request, generate new sequence number and assign it to *Op*
    // when adding this to App's client cache context, it needs to update its
    // own sequence number
    // for read request, will set the sequence number in *Op* by reading the
    // record inside App's context
    auto seqNo = genNewRwOpSeqNo();
    SPDLOG_DEBUG(
        "==>==> addInAppInodeRWFsReq genSeqForWriteReq seqNoSetTo:{}\n", seqNo);
    req->setAccessSeqNo(seqNo);
  }

  // delegate to app for changing the per app's <INODE-Cache> states
  curApp->addInAppAllocOpDataBlocks(req, isWrite);

  return 0;
}

int FsProcWorker::retrieveInAppInode4RWFsReq(FsReq *req, InMemInode *fileInode,
                                             bool &allInApp, uint64_t &maxSeq) {
  auto curApp = getInAppInodeCtx(req, fileInode, false);
  if (curApp == nullptr) {
    return -1;
  }
  int rt = curApp->retrieveInAppAllocOpDataBlocks(req, allInApp, maxSeq);
  return rt;
}

bool FsProcWorker::checkInodeInOtherApp(uint32_t ino, pid_t curPid) {
  auto it = activeNoShareInodes.find(ino);
  if (it == activeNoShareInodes.end() || curPid == it->second->getPid()) {
    SPDLOG_DEBUG("checkInodeInOtherApp ino:{} curPid:{} will return false", ino,
                 curPid);
    // pid_t is a int
    return false;
  }
  SPDLOG_DEBUG("checkInodeInOtherApp ino:{} curPid:{} will existingPid:{}", ino,
               curPid, it->second->getPid());
  return (it->second->getPid() == curPid);
}

int FsProcWorker::setInodeInShareMode(FsReq *req, InMemInode *fileInode) {
  uint32_t ino = fileInode->inodeData->i_no;
  auto firstAppit = activeNoShareInodes.find(ino);
  auto appIt = appMap.find(req->getPid());
  if (appIt == appMap.end()) {
    throw std::runtime_error("cannot find app");
  }
  auto *curApp = appIt->second;
  auto *firstApp = firstAppit->second;
  activeNoShareInodes.erase(ino);
  sharedInodes[ino][req->getPid()] = curApp;
  sharedInodes[ino][firstApp->getPid()] = firstApp;
  return 0;
}

int FsProcWorker::tryResetInodeShareMode(FsReq *req, InMemInode *fileInode) {
  throw std::runtime_error("try Reset Inode ShareMode not implemneted");
  return 0;
}

void FsProcWorker::recordInProgressFsReq(FsReq *req) {
  InMemInode *inodePtr = req->getTargetInode();
  if (inodePtr == nullptr) {
    SPDLOG_ERROR("recordInProgressFsReq called with inode unknown");
    return;
  }
  recordInProgressFsReq(req, inodePtr);
}

void FsProcWorker::recordInProgressFsReq(FsReq *req, InMemInode *inodePtr) {
  if (isFsReqNotRecord(req->getType())) {
    SPDLOG_DEBUG("req type:{} not recorded",
                 getFsReqTypeOutputString(req->getType()));
    return;
  }
  if (inodePtr->i_no != inodePtr->inodeData->i_no) {
    SPDLOG_ERROR("inodePtr->i_no:{} inodePtr->inodeData->i_no:{}",
                 inodePtr->i_no, inodePtr->inodeData->i_no);
    assert(false);
  }

  if (inodePtr->inodeData->type != T_FILE) return;

  auto it = inodeInProgressFsReqMap_.find(inodePtr->i_no);
  if (it == inodeInProgressFsReqMap_.end()) {
    inodeInProgressFsReqMap_.emplace(inodePtr->i_no,
                                     std::unordered_set<FsReq *>({req}));
    // logger->debug("req record succeed, ino:{}", inodePtr->i_no);
  } else {
    std::unordered_set<FsReq *> *curSet =
        &inodeInProgressFsReqMap_[inodePtr->i_no];
    auto setIt = curSet->find(req);
    if (setIt == curSet->end()) {
      curSet->emplace(req);
    }
  }
}

void FsProcWorker::delInProgressFsReq(FsReq *fsReq) {
  assert(fsReq != nullptr);
  uint32_t ino = fsReq->getFileInum();
  auto it = inodeInProgressFsReqMap_.find(ino);
  if (it == inodeInProgressFsReqMap_.end()) {
    SPDLOG_ERROR(
        "Cannot find the fsReq record found (no key) for ino:{} "
        "reqType:{} wid:{}",
        ino, getFsReqTypeOutputString(fsReq->getType()), getWid());
    return;
  }
  auto curSet = &(it->second);
  auto setIt = curSet->find(fsReq);
  if (setIt == curSet->end()) {
    SPDLOG_ERROR(
        "Cannot find this fsReq in this inode's hash set. reqType:{} wid:{} "
        "ino:{} reqPtr:{}",
        getFsReqTypeOutputString(fsReq->getType()), getWid(), ino,
        reinterpret_cast<uintptr_t>(fsReq));
    return;
  } else {
    curSet->erase(setIt);
  }
  if (curSet->empty()) {
    inodeInProgressFsReqMap_.erase(it);
  }
}

int FsProcWorker::onTargetInodeFiguredOut(FsReq *req, InMemInode *targetInode) {
  auto origInodePtr = req->setTargetInode(targetInode);
  int rt = 0;
  logger->debug("onTargetInodeFiguredOut reqType:{} ino:{} inodeType:{}",
                getFsReqTypeOutputString(req->getType()), targetInode->i_no,
                targetInode->inodeData->type);
  if (origInodePtr == nullptr && targetInode->inodeData->type == T_FILE) {
    // It is the first time that the inode of this FsReq is found
    // Need to try to do bookkeeping
    rt = onTargetInodeFiguredOutChildBookkeep(req, targetInode);
    logger->debug("onTargetInodeFiguredOut after child rt:{}", rt);
    if (rt == FsProcWorker::kOnTargetFiguredOutErrInSplitting ||
        rt == FsProcWorker::kOnTargetFiguredOutErrNeedRedirect) {
      // in FileMng that we needs to return some error number to the client.
      // NOTE: here in the case that one FsReq needs to be rejected immediately
      // once the inode is figured out, we won't record it into the
      // <ino, list<FsReq>> map for inProgress requests.
      return rt;
    }
    recordInProgressFsReq(req, targetInode);
  }
  return rt;
}

int FsProcWorker::onDstTargetInodeFiguredOut(FsReq *req,
                                             InMemInode *dstTargetInode) {
  assert(req->getType() == FsReqType::RENAME);
  assert(getWid() == FsProcWorker::kMasterWidConst);
  auto origDstInodePtr = req->setDstTargetInode(dstTargetInode);
  int rt = 0;
  logger->debug("onDstTargetInodeFiguredOut ino:{}", dstTargetInode->i_no);
  if (origDstInodePtr == nullptr && dstTargetInode->inodeData->type == T_FILE) {
    rt = onTargetInodeFiguredOutChildBookkeep(req, dstTargetInode);
    if (rt == FsProcWorker::kOnTargetFiguredOutErrInSplitting ||
        rt == FsProcWorker::kOnTargetFiguredOutErrNeedRedirect) {
      return rt;
    }
    recordInProgressFsReq(req, dstTargetInode);
  }
  return rt;
}

AppProc *FsProcWorker::testOnlyGetAppProc(pid_t pid) {
  if (pid == 0) {
    for (auto ele : appMap) {
      return ele.second;
    }
  }
  if (appMap.find(pid) == appMap.end()) {
    return nullptr;
  } else {
    return appMap[pid];
  }
}

struct clientOp *FsProcWorker::getClientOpForMsg(AppProc *app,
                                                 struct shmipc_msg *msg,
                                                 off_t ring_idx) {
  // TODO use a pool (per app) for creating clientOps
  struct clientOp *copPtr = NULL;
  char *packedMsg = NULL;

  copPtr = (struct clientOp *)malloc(sizeof(*copPtr));
  assert(copPtr != NULL);

  memset(copPtr, 0, sizeof(*copPtr));
  copPtr->opCode = (CfsOpCode)msg->type;

  SPDLOG_DEBUG("Received request opcode:{}", copPtr->opCode);

  packedMsg = (char *)IDX_TO_XREQ(app->shmipc_mgr, ring_idx);

#define unpack_msg(st, op_name) \
  unpack_##st((struct st##Packed *)packedMsg, &(copPtr->op.op_name))
#define copy_msg(op_name) \
  memcpy(&(copPtr->op.op_name), packedMsg, sizeof(copPtr->op.op_name))

  switch (copPtr->opCode) {
    case CFS_OP_READ:
      unpack_msg(readOp, read);
      break;
    case CFS_OP_PREAD:
      unpack_msg(preadOp, pread);
      break;
    case CFS_OP_WRITE:
      unpack_msg(writeOp, write);
      break;
    case CFS_OP_PWRITE:
      unpack_msg(pwriteOp, pwrite);
      break;
    case CFS_OP_ALLOCED_READ:
      unpack_msg(allocatedReadOp, allocread);
      break;
    case CFS_OP_ALLOCED_PREAD:
      unpack_msg(allocatedPreadOp, allocpread);
      break;
    case CFS_OP_ALLOCED_WRITE:
      unpack_msg(allocatedWriteOp, allocwrite);
      break;
    case CFS_OP_ALLOCED_PWRITE:
      unpack_msg(allocatedPwriteOp, allocpwrite);
      break;
    case CFS_OP_LSEEK:
      copy_msg(lseek);
      break;
    case CFS_OP_OPEN:
      copy_msg(open);
      break;
    case CFS_OP_CLOSE:
      copy_msg(close);
      break;
    case CFS_OP_MKDIR:
      copy_msg(mkdir);
      break;
    case CFS_OP_STAT:
      copy_msg(stat);
      break;
    case CFS_OP_FSTAT:
      copy_msg(fstat);
      break;
    case CFS_OP_UNLINK:
      copy_msg(unlink);
      break;
    case CFS_OP_RENAME:
      copy_msg(rename);
      break;
    case CFS_OP_FSYNC:
      copy_msg(fsync);
      break;
    case CFS_OP_WSYNC:
      copy_msg(wsync);
      break;
    case CFS_OP_OPENDIR:
      copy_msg(opendir);
      break;
    case CFS_OP_RMDIR:
      copy_msg(rmdir);
      break;
    case CFS_OP_NEW_SHM_ALLOCATED:
      copy_msg(newshmop);
      break;
    case CFS_OP_EXIT:
      copy_msg(exit);
      break;
    case CFS_OP_CHKPT:
      copy_msg(chkpt);
      break;
    case CFS_OP_SYNCALL:
      copy_msg(syncall);
      break;
    case CFS_OP_SYNCUNLINKED:
      copy_msg(syncunlinked);
      break;
    case CFS_OP_MIGRATE:
      copy_msg(migrate);
      break;
    case CFS_OP_PING:
      copy_msg(ping);
      break;
    case CFS_OP_STARTDUMPLOAD:
      copy_msg(startdumpload);
      break;
    case CFS_OP_STOPDUMPLOAD:
      copy_msg(stopdumpload);
      break;
    case CFS_OP_DUMPINODES:
      copy_msg(dumpinodes);
      break;
    case CFS_OP_INODE_REASSIGNMENT:
      copy_msg(inodeReassignment);
      break;
    case CFS_OP_THREAD_REASSIGNMENT:
      copy_msg(threadReassign);
      break;
#ifdef _CFS_TEST_
    case CFS_OP_TEST:
      copy_msg(test);
      break;
#endif
    default:
      SPDLOG_ERROR("could not unpack/copy msg for opcode:{}", copPtr->opCode);
      break;
  }

#undef copy_msg
#undef unpack_msg
  return copPtr;
}

InMemInode *FsProcWorker::queryPermissionMap(FsReq *req, bool &reqDone) {
  InMemInode *inode = nullptr;
  reqDone = false;
  auto result = fileManager->checkPermission(req);
  if (result == FsPermission::PCR::DENIED) {
    // access denied
    req->setError(FS_REQ_ERROR_POSIX_EACCES);
    reqDone = true;
    return nullptr;
  } else if (result == FsPermission::PCR::NOTFOUND && !isMasterWorker()) {
    // SPDLOG_DEBUG("not found wid:{} will redirect to master", getWid());
    logger->debug("not found wid:{} will redirect to master", getWid());
    // redirect to master
    req->setError(FS_REQ_ERROR_INODE_REDIRECT);
    req->setWid(FsProcWorker::kMasterWidConst);
    reqDone = true;
    return nullptr;
  } else {
    // OK
    inode = req->getTargetInode();
  }

  if (req->getType() == FsReqType::RENAME) {
    auto result = fileManager->checkDstPermission(req);
    if (result == FsPermission::PCR::DENIED) {
      req->setError(FS_REQ_ERROR_POSIX_EACCES);
      reqDone = true;
      return nullptr;
    } else {
      assert(getWid() == kMasterWidConst);
      SPDLOG_DEBUG("queryPermissionMap dstTargetInode:{}",
                   reinterpret_cast<uintptr_t>(req->getDstTargetInode()));
    }
  }
  return inode;
}

void FsProcWorker::RefillAppMap() {
  auto init_app_num = appMap.size();
  if (appmap_fill_loop_counter_ == kAppMapFillLoopCnt || init_app_num == 0) {
    std::vector<std::pair<pid_t, AppCredential *>> result_vec;
    gFsProcPtr->g_cred_table.GetAllAppCred(result_vec, init_app_num);
    if (!result_vec.empty()) {
      // we get something, (a new app?!)
      for (auto [cur_pid, cur_cred] : result_vec) {
        auto cur_app_it = appMap.find(cur_pid);
        if (cur_app_it == appMap.end()) {
          InitCredToEmptyApp(*cur_cred);
        } else {
          assert(cur_app_it->second->GetAppIdx() == cur_cred->app_idx);
        }
        delete cur_cred;
      }
    }
    appmap_fill_loop_counter_ = 0;
  } else {
    appmap_fill_loop_counter_++;
  }
}

int FsProcWorker::pollReqFromApps() {
  int numAppReqPolled = 0;
  AppProc *app;
  FsReq *reqPtr = nullptr;
  shmipc_msg *msgPtr = nullptr;
  off_t ringIdx;
  char *dataBufPtr = NULL;
  struct clientOp *copPtr = NULL;
  // pull request from Apps
  // TODO: how many requests we should poll here?
  // considering the multi-threading apps?
  for (auto ele : appMap) {
    app = ele.second;
    do {
      msgPtr = shmipc_mgr_get_msg_nowait(app->shmipc_mgr, &ringIdx);
      if (msgPtr == nullptr) break;
      msgPtr->status = shmipc_STATUS_IN_PROGRESS;
      numAppReqPolled++;
      copPtr = getClientOpForMsg(app, msgPtr, ringIdx);
      dataBufPtr = (char *)IDX_TO_DATA(app->shmipc_mgr, ringIdx);
      reqPtr = fsReqPool_->genNewReq(app, ringIdx, copPtr, dataBufPtr, this);
      if (reqPtr == nullptr) {
        fflush(stdout);
        SPDLOG_ERROR("Cannot generate request\n");
      }
      recvReadyReqQueue.push(reqPtr);
    } while (true);
  }
  return numAppReqPolled;
}

void FsProcWorker::processReqOnRecv(FsReq *req) {
  int reqFlags = req->getReqTypeFlags();
  // every request should have some flags
  assert(reqFlags != FsReqFlags::no_flags);
  // NOTE: server_control_plane requests cannot come here as processReq is only
  // called when polling client rings. So we can only handle the following three
  // commands.
  constexpr int req_category_mask = FsReqFlags::handled_by_owner |
                                    FsReqFlags::handled_by_primary |
                                    FsReqFlags::client_control_plane;

  req->startOnCpuTimer();
  switch (reqFlags & req_category_mask) {
    case FsReqFlags::handled_by_owner: {
      // Any req that has an "owner" must mean there is some associated inode
      // information. This can be in the form of fd's or paths.
      constexpr int owner_mask =
          FsReqFlags::uses_file_descriptors | FsReqFlags::uses_paths;

      switch (reqFlags & owner_mask) {
        case FsReqFlags::uses_file_descriptors:
          ownerProcessFdReq(req);
          // TODO (anthony) : just out of curiosity, if I made all these return
          // statements into break statements, would the compiler actually
          // optimize them to early return? Or would it break out of inner, then
          // outer, and then return? Try in godbolt at somepoint.
          return;
        case FsReqFlags::uses_paths:
          ownerProcessPathReq(req);
          return;
        default:
          SPDLOG_ERROR("Cannot handle owner request, type={}", req->getType());
          throw std::runtime_error("Unknown owner request");
      }
    }  // case handled_by_owner
      return;
    case FsReqFlags::handled_by_primary:
      // NOTE: right now all primary requests are path based requests
      primaryProcessPathReq(req);
      return;
    case FsReqFlags::client_control_plane:
      processClientControlPlaneReq(req);
      return;
    default:
      SPDLOG_ERROR("Cannot handle request, type={}", req->getType());
      std::cerr << "reqType cannot find handle" << req->getType() << std::endl;
      throw std::runtime_error("unknown request category");
  }
}

int FsProcWorker::processInternalReadyQueue() {
  std::queue<FsReq *> &curReadyReqQ = internalReadyReqQueue;
  // process old requests
  FsReq *curReq;
  int reqNum = curReadyReqQ.size();
  int curReqNum = 0;
  while (!curReadyReqQ.empty()) {
    curReq = curReadyReqQ.front();
    curReq->startOnCpuTimer();
    curReadyReqQ.pop();
    fileManager->processReq(curReq);
    // fprintf(stderr, "inProcessReady, processReq Done");
    curReqNum++;
    if (curReqNum == reqNum) {
      break;
    }
  }
  return reqNum;
}

void FsProcWorker::primaryHandleUnknownFdReq(FsReq *req) {
  // TODO: anyway to restrict this to only calls from ownerProcessFdReq? It is
  // just a helper function for it.
  auto reqFlags = req->getReqTypeFlags();
  assert(reqFlags & FsReqFlags::handled_by_owner);
  assert(reqFlags & FsReqFlags::uses_file_descriptors);

  if (!isMasterWorker()) {
    req->setError(FS_REQ_ERROR_INODE_REDIRECT);
    req->setWid(FsProcWorker::kMasterWidConst);
    submitFsReqCompletion(req);
    return;
  }

  auto master = static_cast<FsProcWorkerMaster *>(this);
  // unknown fd may belong to recently assigned inode
  auto ino = master->getRecentlyReassignedInodeFromFd(req->getPid(), req->fd);
  if (ino == 0) {
    SPDLOG_INFO("POSIX_EBADF getWid:{} pid:{} fd:{} reqType:{} ino:{}",
                getWid(), req->getPid(), req->fd, req->getType(), ino);
    // fd not found
    req->setError(FS_REQ_ERROR_POSIX_EBADF);
    submitFsReqCompletion(req);
    return;
  }

  // primaryRouteToOwner calls submitFsReqCompletion which invalidates req obj.
  // Therefore, saving the necessary arguments for future processing
  auto pid = req->getPid();
  auto fd = req->fd;
  // primaryRouteToOwner calls submitFsReqCompletion which invalidates req.
  // NOTE: if primary was the owner, then it would have resolved the inode and
  // this function would not have been called.
  auto req_errno = master->primaryRouteToOwner(req, ino);
  if (req_errno == FS_REQ_ERROR_INODE_REDIRECT) {
    // successful inode redirect, fd has been notified of the redirect so no
    // more notifications will be given to this fd.
    master->delRecentlyReassignedInodeEntries(pid, fd);
  }
}

void FsProcWorker::ownerProcessFdReq(FsReq *req) {
  auto reqFlags = req->getReqTypeFlags();
  assert(reqFlags & FsReqFlags::handled_by_owner);
  assert(reqFlags & FsReqFlags::uses_file_descriptors);

  SPDLOG_DEBUG("WID:{} processing request of type {}", getWid(),
               req->getType());
  // If we are able to convert the file descriptor to an inode, then we
  // are the owner.
  auto fobj = fileManager->getFileObjForFd(req->getPid(), req->fd);
  if (fobj == nullptr) {
    // Either fd is invalid or the inode has been reassigned. Only primary can
    // resolve this problem. If primary cannot, then it is invalid.
    primaryHandleUnknownFdReq(req);
    return;
  }

  SPDLOG_DEBUG("WID:{} successfully resolved fd", getWid());
  req->setFileObj(fobj);
  req->setTargetInode(fobj->ip);
  recordInProgressFsReq(req, fobj->ip);
  fileManager->processReq(req);
}

int FsProcWorkerMaster::primaryRouteToOwner(FsReq *req, cfs_ino_t i_no) {
  auto owner = getInodeOwner(i_no);
  // This function will only be called when we need to route to owner.
  // If primary was the owner, it would never have called this function.
  assert(owner != FsProcWorker::kMasterWidConst);
  if (owner == -1) {
    req->setError(FS_REQ_ERROR_INODE_IN_TRANSFER);
  } else {
    req->setError(FS_REQ_ERROR_INODE_REDIRECT);
    req->setWid(owner);
  }

  int ret = req->getErrorNo();
  submitFsReqCompletion(req);
  return ret;
}

void FsProcWorker::ownerHandleUnknownPathReq(FsReq *req,
                                             FsPermission::PCR perm) {
  // The default behavior for owners is to redirect unknown paths to primary.
  if (!isMasterWorker()) {
    req->setError(FS_REQ_ERROR_INODE_REDIRECT);
    req->setWid(FsProcWorker::kMasterWidConst);
    submitFsReqCompletion(req);
    return;
  }

  auto master = static_cast<FsProcWorkerMaster *>(this);
  // We have permissions but could not access the inode - ownership issue.
  if (perm == FsPermission::PCR::OK) {
    auto unsafeInodePtr = req->getTargetInode();
    assert(unsafeInodePtr != nullptr);
    master->primaryRouteToOwner(req, unsafeInodePtr->i_no);
    return;
  }

  assert(perm == FsPermission::PCR::NOTFOUND);
  // Currently, we never evict from directory cache. So any inode that is
  // reassigned away from master must have been loaded into memory and therefore
  // its path will in be in the directory cache. If the path is not in the
  // directory cache -
  // - The path may not exist (valid for O_CREAT): handled by primary
  // - The path may exist but this is the first time accessing it: handled by
  // primary
  // FIXME: When directory cache supports eviction, ownership check must be done
  // in primary after we try resolving the inode by reading the directory
  // contents from disk. We must then route to the right owner if it exists. For
  // now, primary can just proceed with the request.
  auto copPtr = req->getClientOp();
  if ((req->getType() == FsReqType::OPEN) &&
      (((copPtr->op).open).flags & O_CREAT)) {
    // This is the only place where we allow a transition from OPEN to CREATE.
    // We are in the primary and we have not found an existing path so we can
    // modify this to a create request.
    SPDLOG_DEBUG("Open({}) modified to Create", copPtr->op.open.path);
    // FIXME: If the file system just starts up and we try to create a file that
    // already exists, the create function should automatically transition to
    // open again.
    req->setType(FsReqType::CREATE);
    req->setState(FsReqState::CREATE_GET_PRT_INODE);
  }

  // TODO: recordInProgressFsReq if inode has been associated with it
  fileManager->processReq(req);
}

void FsProcWorker::ownerProcessPathReq(FsReq *req) {
  assert(req->getReqTypeFlags() & FsReqFlags::handled_by_owner);
  assert(req->getReqTypeFlags() & FsReqFlags::uses_paths);

  // FIXME: PCR::DENIED is never really generated.
  auto perm = fileManager->checkPermission(req);
  switch (perm) {
    case FsPermission::PCR::OK:
      break;
    case FsPermission::PCR::NOTFOUND:
      ownerHandleUnknownPathReq(req, perm);
      return;
    case FsPermission::PCR::DENIED:
      req->setError(FS_REQ_ERROR_POSIX_EACCES);
      submitFsReqCompletion(req);
      return;
    default:
      throw std::runtime_error("Unknown PCR value");
  }

  // FIXME: checkPermission sets the target inode if it finds it. Instead, it
  // should only check if the path exists and tell us the inode number. Inode
  // ownership check happens outside.
  auto unsafeInodePtr = req->getTargetInode();
  // since we got an OK
  assert(unsafeInodePtr != nullptr);

  // NOTE: The above FIXME must be fixed. We shouldn't be accessing an inode at
  // all on this worker. Harmless readonly, but still brought into our cache.
  auto inodePtr = fileManager->GetInMemInode(unsafeInodePtr->i_no);
  if (inodePtr == nullptr) {
    ownerHandleUnknownPathReq(req, perm);
    return;
  }

  recordInProgressFsReq(req, inodePtr);
  fileManager->processReq(req);
}

void FsProcWorker::primaryProcessPathReq(FsReq *req) {
  assert(req->getReqTypeFlags() & FsReqFlags::handled_by_primary);
  assert(req->getReqTypeFlags() & FsReqFlags::uses_paths);

  if (!isMasterWorker()) {
    // TODO: This is meant to redirect to master. Although the outcome is the
    // same, it isn't because an inode was reassigned. It's because only primary
    // can handle this request.
    req->setError(FS_REQ_ERROR_INODE_REDIRECT);
    req->setWid(FsProcWorker::kMasterWidConst);
    submitFsReqCompletion(req);
    return;
  }

  auto perm = fileManager->checkPermission(req);
  if (perm == FsPermission::PCR::DENIED) {
    req->setError(FS_REQ_ERROR_POSIX_EACCES);
    submitFsReqCompletion(req);
    return;
  }

  // NOTE: it is okay for requests on primary to not be in the directory cache
  // and return NOT_FOUND. However, once found, permission checking must happen
  // again.
  if (perm == FsPermission::PCR::OK) {
    assert(req->getTargetInode() != nullptr);
  }

  if (req->getType() != FsReqType::RENAME) goto end;

  perm = fileManager->checkDstPermission(req);
  if (perm == FsPermission::PCR::DENIED) {
    req->setError(FS_REQ_ERROR_POSIX_EACCES);
    submitFsReqCompletion(req);
    return;
  }

  if (perm == FsPermission::PCR::OK) {
    assert(req->getDstTargetInode() != nullptr);
  }

end:
  // TODO (ask jing during pull-request): recordInProgressFsReq requires an
  // inodePtr. However, when on primary, these may not exist in memory to
  // retrieve inodePtr (parameter for the function). Do any of these requests
  // (mkdir, unlink, rename, opendir) require recordInProgressFsReq.
  // TODO: recordInProgressFsReq if inode has been found
  fileManager->processReq(req);
}

void FsProcWorker::processClientControlPlaneReq(FsReq *req) {
  assert(req->getReqTypeFlags() & FsReqFlags::client_control_plane);
  switch (req->getType()) {
    case FsReqType::NEW_SHM_ALLOCATED:
    case FsReqType::SYNCALL:
    case FsReqType::SYNCUNLINKED:
    case FsReqType::DUMPINODES:
      fileManager->processReq(req);
      goto end;

    case FsReqType::APP_EXIT: {
      // Process exit here directly.
      // the main purpose of this now is to invalidate the opened shared memory
      // data buffer. (2020/03/06)
      req->getApp()->invalidateAppShm();
      pid_t cur_pid = req->getApp()->getPid();
      if (getWid() == FsProcWorker::kMasterWidConst) {
        gFsProcPtr->g_app_shm_ids.HandleAppExit(cur_pid);
#ifdef UFS_SOCK_LISTEN
        gFsProcPtr->RetireAppIdx(req->getApp()->GetAppIdx());
#endif
        // gFsProcPtr->g_cred_table.HandleAppExit(cur_pid);
      }
      fileManager->closeAllFileDescriptors(cur_pid);
      req->getApp()->ClearAllInoAccess();
#ifdef UFS_SOCK_LISTEN
      req->getApp()->ResetCred();
#endif
      req->getClientOp()->op.exit.ret = 0;
      goto submit_completion;
    }

    case FsReqType::APP_CHKPT:
      gFsProcPtr->performCheckpointing(getWid()); /* blocking */
      req->getClientOp()->op.chkpt.ret = 0;
      goto submit_completion;

    case FsReqType::START_DUMP_LOADSTAT:
      gFsProcPtr->setNeedOutputLoadStats(true);
      req->getClientOp()->op.startdumpload.ret = 0;
      goto submit_completion;

    case FsReqType::STOP_DUMP_LOADSTAT:
      gFsProcPtr->setNeedOutputLoadStats(false);
      req->getClientOp()->op.stopdumpload.ret = 0;
      goto submit_completion;

    case FsReqType::INODE_REASSIGNMENT: {
      // Might be the case the worker has not been waken up yet
      // So check and set here.
      // Might be some chance for race, but this is a test-purpose op
      auto new_owner = (req->copPtr->op.inodeReassignment).newOwner;
      if (new_owner != getWid() && !gFsProcPtr->checkWorkerActive(new_owner)) {
        gFsProcPtr->setWorkerActive(new_owner, true);
      }
      FileMng::ReassignmentOp::ProcessInodeReassignmentReq(fileManager, req);
      goto end;  // not submitting completion here, handled internally.
    }
    case FsReqType::THREAD_REASSIGN:
      ProcessManualThreadReassign(req);
      goto end;
    default:
      throw std::runtime_error("Unknown client control plane request");
  }

submit_completion:
  submitFsReqCompletion(req);
end:
  return;
}
int FsProcWorker::initMakeDevReady() {
  int rc = dev->devInit();
  if (rc < 0) {
    exit(-1);
  }
  // need to register this main thread to block device, thus it can do init IO
  if (dev->isSpdk()) {
    dev->initWorker(getWid());
  }
  return 0;
}

void FsProcWorker::initJournalManager(void) {
  if (jmgr != nullptr) throw std::runtime_error("jmgr already initialized");
  int workerId = getWorkerIdx();
  uint64_t jsuper_blockno = get_worker_journal_sb(workerId);
  JournalManager *primary_jmgr = nullptr;
#if CFS_JOURNAL(GLOBAL_JOURNAL)
  // In a global journal, all secondaries will use same jsuper as primary
  if (!isMasterWorker()) {
    primary_jmgr = gFsProcPtr->GetPrimaryJournalManager();
  }
#endif
  jmgr = new JournalManager(primary_jmgr, jsuper_blockno, dev);
}

AppProc *FsProcWorker::getInAppInodeCtx(FsReq *req, InMemInode *fileInode,
                                        bool missAdd) {
  SPDLOG_DEBUG("getInAppInodeCtx() ino:{} diskIno:{} opCode:{}",
               fileInode->i_no, fileInode->inodeData->i_no,
               req->getClientOp()->opCode);
  uint32_t ino = fileInode->inodeData->i_no;
  auto it = activeNoShareInodes.find(ino);
  auto appIt = appMap.find(req->getPid());
  if (appIt == appMap.end()) {
    SPDLOG_ERROR("getInAppInodeCtx() app-{} not found", req->getPid());
    throw std::runtime_error("getInAppInodeCtx app not found");
  }

  AppProc *curApp = appIt->second;

  if (it == activeNoShareInodes.end()) {
    // the inode has not been recorded yet
    if (missAdd) {
      activeNoShareInodes[ino] = curApp;
    } else {
      return nullptr;
    }
  } else {
    if (it->second->getPid() != req->getPid()) {
      SPDLOG_DEBUG(
          "addInAppInodeRWFsReq app-{} already cache ino-{} "
          "newPid-{}",
          it->second->getPid(), ino, req->getPid());
      return nullptr;
    }
  }
  return curApp;
}

int FsProcWorker::pinToCpu() {
  if (pinnedCPUCore != -1) {
    auto rc = pin_to_cpu_core(pinnedCPUCore);
    if (rc != 0) {
      SPDLOG_ERROR("ERROR cannot pin to cpu_core:{}", pinnedCPUCore);
      return -1;
    }
    logger->info("Pin to CPU core-{}", pinnedCPUCore);
    logger->flush();
    return 0;
  }

  // legacy logic
  if (!dev->isSpdk()) {
    auto rc = pin_to_cpu_core(shmBaseOffset / 10 + 1);
    if (rc != 0) {
      SPDLOG_ERROR("ERROR cannot pin to cpu_core:{}", shmBaseOffset / 10 + 1);
      return -1;
    }
  } else {
    if (gFsProcPtr->getNumThreads() > 1) {
      // Only explicitly pin cpu core for more than one worker
      auto rc = pin_to_cpu_core(wid + 1 - kMasterWidConst);
      if (rc != 0) {
        SPDLOG_ERROR("ERROR cannot pin to cpu_core:{}",
                     wid + 1 - kMasterWidConst);
        return -1;
      }
      logger->info("Pin to CPU core-{}", (wid + 1 - kMasterWidConst));
      logger->flush();
    }
  }
  return 0;
}

void FsProcWorker::redirectZombieAppReqs(bool raiseError) {
  if (getWid() != FsProcWorker::kMasterWidConst) {
    AppProc *app;
    shmipc_msg *new_msg;
    off_t ring_idx;
    char *dataBufPtr = nullptr;
    struct clientOp *copPtr = nullptr;
    for (auto ele : appMap) {
      app = ele.second;
      new_msg = shmipc_mgr_get_msg_nowait(app->shmipc_mgr, &ring_idx);
      if (new_msg == nullptr) continue;
      SHMIPC_SET_MSG_STATUS(new_msg, shmipc_STATUS_IN_PROGRESS);

      copPtr = getClientOpForMsg(app, new_msg, ring_idx);
      dataBufPtr = (char *)IDX_TO_DATA(app->shmipc_mgr, ring_idx);

      auto req = fsReqPool_->genNewReq(app, ring_idx, copPtr, dataBufPtr, this);
      if (req == nullptr) {
        throw std::runtime_error("redirectZombie cannot genNewReq");
      }

      auto reqType = req->getType();

      SPDLOG_DEBUG("======== redirectZombie raiseE?:{} wid:{} app:{} type:{}\n",
                   raiseError, getWid(), app->getPid(),
                   getFsReqTypeOutputString(reqType).c_str());

      if (reqType == FsReqType::APP_EXIT) {
        app->invalidateAppShm();
        req->getClientOp()->op.exit.ret = 0;
        submitFsReqCompletion(req);
        continue;
      }

      // We start timer here and then completion will need to stop it
      req->startOnCpuTimer();
      if (raiseError) {
        req->setError(FS_REQ_ERROR_POSIX_RET);
        submitFsReqCompletion(req);
      } else {
        req->setState(FsReqState::OP_OWNERSHIP_REDIRECT);
        req->setWid(FsProcWorker::kMasterWidConst);
        req->setError(FS_REQ_ERROR_INODE_REDIRECT);
        submitFsReqCompletion(req);
      }
    }
  }
}

bool FsProcWorker::workerRunLoopInner(perfstat_ts_t loop_start_ts) {
  bool loopEffective = false;

  RefillAppMap();
  int numAppReqPolled = pollReqFromApps();
  loopEffective |= (numAppReqPolled > 0);

  FsReq *curRecvReq;
  // NOTE: checking .empty() is faster but we need to record the size anyway.
  int cur_batch_idx = 0;
  int recv_q_rm_share_qlen_agg = 0;
  int recv_qlen_agg = 0;
  std::map<cfs_ino_t, std::vector<int>> ino_cur_batch_idx;
  cfs_ino_t cur_ino;
  while (size_t size = recvReadyReqQueue.size()) {
    curRecvReq = recvReadyReqQueue.front();
    curRecvReq->recordDequeueReady(size);
    recv_qlen_agg += size;
    processReqOnRecv(curRecvReq);
    // try to eliminate shared files in this batch
    if (curRecvReq->IsInUse()) {
      // this req has not finished (maybe due to io)
      cur_ino = curRecvReq->getFileInum();
    } else {
      // this request has go through the "submitFsReqCompletion"
      cur_ino = curRecvReq->GetFileInoLazyCopy();
      curRecvReq->ResetLazyFileInoCopy();
    }
    ino_cur_batch_idx[cur_ino].push_back(cur_batch_idx);
    recvReadyReqQueue.pop();
    cur_batch_idx++;
  }

  {
    // get the calibrated queue length without the shared file
    for (auto &[cur_ino, cur_idx_vec] : ino_cur_batch_idx) {
      int within_ino_idx = 0;
      for (auto cur_within_batch_idx : cur_idx_vec) {
        int cur_delta = 0;
        if (cur_ino > 0) cur_delta = (-within_ino_idx);
        recv_q_rm_share_qlen_agg += (1 + cur_within_batch_idx + cur_delta);
        within_ino_idx++;
      }
    }
    stats_recorder_.RecordRecvQueuingStats(recv_qlen_agg,
                                           recv_q_rm_share_qlen_agg);
  }

#ifdef USE_SPDK
  // poll completion of dev IO requests
  int numNvmeCompletion = dev->checkCompletion(0);
  //   fprintf(stdout, "===>numNvmeCompletion:%d\n", numNvmeCompletion);
  loopEffective |= (numNvmeCompletion > 0);
#endif  // USE_SPDK

  // process the request that have been sent to ready list (internally)
  int numReadyProcessed = processInternalReadyQueue();
  loopEffective |= (numReadyProcessed > 0);
  // fprintf(stdout, "===>numReadyProcessed:%d wid:%d\n", numReadyProcessed,
  // getWid());

  // check if need to flush buffer
  auto numFlushBlock = fileManager->checkAndFlushBufferDirtyItems();
  loopEffective |= (numFlushBlock > 0);

  // fprintf(stderr, "===>numFlushBlock:%ld\n", numFlushBlock);
  return loopEffective;
}

void FsProcWorker::processFsProcMessage(const FsProcMessage &msg) {
  // TODO when a bitmap clear is seen, track the last alloc inode for that.
  // If we ever alloc that block for another inode, fill depends_on field.
  num_messages_processed_ += 1;
  FsProcMessageType msg_type = static_cast<FsProcMessageType>(msg.type);
  switch (msg_type) {
    case FsProcMessageType::BITMAP_CHANGES: {
      SPDLOG_DEBUG("Worker {} received message: BITMAP_CHANGES", getWid());
      auto req = fsReqPool_->genNewReq();
      req->setType(FsReqType::REMOTE_BITMAP_CHANGES);
      req->setState(FsReqState::REMOTE_BITMAP_CHANGES_INIT);
      req->completionCallbackCtx = msg.ctx;
      fileManager->processRemoteBitmapChanges(req);
    } break;
#if CFS_JOURNAL(ON)  // journal specific messages
    case FsProcMessageType::PREPARE_FOR_CHECKPOINTING: {
      SPDLOG_DEBUG("Worker {} received message: PREPARE_FOR_CHECKPOINTING",
                   getWid());
      auto ctx = (PrepareForCheckpointingCtx *)msg.ctx;
      jmgr->prepareForCheckpointing(ctx->ci, fileManager);
      *(ctx->completed) = true;
    } break;
    case FsProcMessageType::CHECKPOINTING_COMPLETE: {
      SPDLOG_DEBUG("Worker {} received message: CHECKPOINTING_COMPLETE",
                   getWid());
      auto ctx = (CheckpointingCompleteCtx *)msg.ctx;
      if (ctx->success)
        jmgr->onCheckpointSuccess(ctx->ci, fileManager);
      else
        jmgr->onCheckpointFailure(ctx->ci);
      delete ctx;
    } break;
    case FsProcMessageType::PROPOSE_CHECKPOINT: {
      SPDLOG_DEBUG("Worker {} received message: PROPOSE_CHECKPOINT", getWid());
      // NOTE: we only allow primary to recv this kind of msg for now
      assert(getWid() == FsProcWorker::kMasterWidConst);
      bool b = gFsProcPtr->setInCheckpointing();
      auto ctx = (ProposeForCheckpoingCtx *)msg.ctx;
      int wididx = ctx->propose_widIdx;
      // NOTE for now, we let the propose wid to do the checkpointing
      // TODO (jingliu): better decision making of which thread to do
      // checkpointing
      if (b) {
        FsProcMessage curmsg;
        auto asctx = new AssignCheckpointWorkerCtx();
        curmsg.ctx = reinterpret_cast<void *>(asctx);
        curmsg.type = FsProcMessageType::ASSIGN_CHECKPOINT_TO_WORKER;
        messenger->send_message(wididx, curmsg);
      }
      delete ctx;
    } break;
    case FsProcMessageType::ASSIGN_CHECKPOINT_TO_WORKER: {
      SPDLOG_DEBUG("Worker {} received message: ASSIGN_CHECKPOINT_TO_WORKER",
                   getWid());
      gFsProcPtr->performCheckpointing(getWid());
    } break;
#endif  // Journal specific messages
    case FsProcMessageType::LM_JOINALL: {
      // we reuse the same structures
      auto allow = reinterpret_cast<fsp_lm::PerWorkerLoadAllowance *>(msg.ctx);
      recvJoinAll(allow);
    } break;
    case FsProcMessageType::LM_REBALANCE_ALLOC_SHARE: {
      auto allow = reinterpret_cast<fsp_lm::PerWorkerLoadAllowance *>(msg.ctx);
      recvLoadRebalanceShare(allow);
    } break;
    case FsProcMessageType::kLM_RedirectOneTau: {
      auto ctx = reinterpret_cast<LmMsgRedirectOneTauCtx *>(msg.ctx);
      assert(ctx->src_wid == getWid() ||
             getWid() == FsProcWorker::kMasterWidConst);
      ProcessLmRedirectOneTau(ctx);
    } break;
    case FsProcMessageType::kLM_RedirectCreation: {
      auto ctx = reinterpret_cast<LmMsgRedirectCreationCtx *>(msg.ctx);
      assert(getWid() == FsProcWorker::kMasterWidConst);
      ProcessLmRedirectCreation(ctx);
    } break;
    case FsProcMessageType::kLM_Rebalance: {
      auto ctx = reinterpret_cast<LmMsgRedirectFileSetCtx *>(msg.ctx);
      ProcessLmRebalance(ctx);
    } break;
    case FsProcMessageType::kLM_RedirectFuture: {
      auto ctx = reinterpret_cast<LmMsgRedirectFutureCtx *>(msg.ctx);
      ProcessLmRedirectFuture(ctx);
    } break;
    case FsProcMessageType::kLM_JoinAll: {
      auto ctx = reinterpret_cast<LmMsgJoinAllCtx *>(msg.ctx);
      // w0 will never recv such msg
      fprintf(stderr, "kLM_JoinAll recv\n");
      assert(getWid() != FsProcWorker::kMasterWidConst);
      ProcessLmJoinAll(ctx);
    } break;
    case FsProcMessageType::kLM_JoinAllCreation: {
      auto ctx = reinterpret_cast<LmMsgJoinAllCreationCtx *>(msg.ctx);
      assert(getWid() == FsProcWorker::kMasterWidConst);
      ProcessLmJoinAllCreation(ctx);
    } break;
    case FsProcMessageType::kReassignment: {
      auto ctx = static_cast<FileMng::ReassignmentOp::Ctx *>(msg.ctx);
      FileMng::ReassignmentOp::ProcessReassignmentCtx(fileManager, ctx);
      // NOTE: not deleting context here as it may be used later.
    } break;
    case FsProcMessageType::kOwnerUnlinkInode: {
      cfs_ino_t *ctx = static_cast<cfs_ino_t *>(msg.ctx);
      FileMng::UnlinkOp::OwnerUnlinkInode(fileManager, *ctx, nullptr);
      delete ctx;
    } break;
    default:
      SPDLOG_WARN("Ignoring fsproc message, unknown type {}", msg.type);
  }
}

int FsProcWorker::processInterWorkerMessages() {
  int wid = getWorkerIdx();
  bool valid_message = false;
  FsProcMessage msg;
  int numMsg = 0;
  do {
    valid_message = messenger->recv_message(wid, msg);
    if (!valid_message) break;

    processFsProcMessage(msg);
    numMsg++;
  } while (1);
  return numMsg;
}

int FsProcWorker::ProcessPendingCreationRedirect() {
  using FR = FileMng::ReassignmentOp;
  int num = pending_crt_redirect_inos_.size();
  // if (num > 0) fprintf(stdout, "processpendingcreat num:%d\n", num);
  for (auto [cur_ino, dst_wid] : pending_crt_redirect_inos_) {
    // migrate this inode to dest
    auto old_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                 void *myctx) {
      SPDLOG_DEBUG("ProcessPendingCreationRedirect old_owner");
    };
    auto new_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                 void *myctx) {
      SPDLOG_DEBUG("ProcessPendingCreationRedirect new_owner");
    };
    FR::OwnerExportThroughPrimary(fileManager, cur_ino, dst_wid,
                                  old_owner_callback, nullptr,
                                  new_owner_callback, nullptr);
  }
  pending_crt_redirect_inos_.clear();
  return num;
}

FsProcWorker::inodeRefSetIt FsProcWorker::collectInodeMigrateOpenedFd(
    const InMemInode *inodePtr,
    std::unordered_map<pid_t, std::vector<FileObj *>> &perAppFds) {
  AppProc *curApp = nullptr;
  auto appRefIt = inodeAppRefMap_.find(inodePtr->i_no);
  if (appRefIt != inodeAppRefMap_.end()) {
    for (auto pid : (appRefIt->second)) {
      auto appIt = appMap.find(pid);
      if (appIt != appMap.end()) {
        curApp = appIt->second;
        perAppFds[pid] = {};
        int numFiles =
            curApp->findAllOpenedFiles4Ino(inodePtr->i_no, perAppFds[pid]);
        logger->debug("[migration] # of opened files:{}", numFiles);

      } else {
        logger->error("cannot find app for pid:{}", pid);
        logger->flush();
        throw std::runtime_error("cannot find app");
      }
    }
    return appRefIt;
  } else {
    logger->debug("[migration] This inode cannot find app refer to it");
    return inodeAppRefMap_.end();
  }
}

void FsProcWorker::installOpenedFd(
    cfs_ino_t ino, std::unordered_map<pid_t, std::vector<FileObj *>> &fobjs,
    bool isMasterForward) {
  for (auto &ele : fobjs) {
    pid_t curPid = ele.first;
    inodeAppRefMap_[ino].emplace(curPid);
    logger->debug(
        "master recvFileInodeJoin::checkSplitJoinComm() recv - appProc - "
        "pid:{}",
        curPid);
    auto appIt = appMap.find(curPid);
    if (appIt != appMap.end()) {
      if (!isMasterForward) {
        appIt->second->overwriteOpenedFiles(ele.second,
                                            /*directUseVec*/ true);
      } else {
        appIt->second->overwriteOpenedFiles(ele.second,
                                            /*directUseVec*/ false);
      }
      for (auto opF : ele.second) {
        logger->debug(" recv: fd:{} ino:{}", opF->readOnlyFd, opF->ip->i_no);
      }
    }
  }
}

void FsProcWorker::monitorFsReqBio() {
  if (!kFsReqBioNumTracking.empty()) {
    double fsReqAvgBio = 0;
    auto &cur_vec = (workerFsReqNumBioMap_[workerTid_]);
    if (!cur_vec.empty()) {
      double fsReqBiosum = std::accumulate(cur_vec.begin(), cur_vec.end(), 0.0);
      fsReqAvgBio = fsReqBiosum / cur_vec.size();
      std::cout << "[ReqBio]" << fsReqAvgBio << std::endl;
    }
    // reset the stats variables
    workerFsReqNumBioMap_[workerTid_].clear();
  }
}

void FsProcWorker::opStatsAccountSingleOpDone(FsReqType reqType, size_t bytes) {
  if (!FsWorkerOpStats::kCollectWorkerOpStats) return;
  if (!FsWorkerOpStats::isAccountingFsReqType(reqType)) return;

  if (opStatsPtr_->numOpDone == 0) {
    opStatsPtr_->firstOpNano =
        PerfUtils::Cycles::toNanoseconds(PerfUtils::Cycles::rdtscp());
  }
  opStatsPtr_->lastOpNano =
      PerfUtils::Cycles::toNanoseconds(PerfUtils::Cycles::rdtscp());
  opStatsPtr_->numOpDone++;
  opStatsPtr_->doneBytes += bytes;
}

FsProcWorkerMaster::FsProcWorkerMaster(int w, CurBlkDev *d, int shmBaseOffset,
                                       std::atomic_bool *workerRunning,
                                       PerWorkerLoadStatsSR *stats)
    : FsProcWorker(w, d, shmBaseOffset, workerRunning, stats) {
  SPDLOG_INFO("policy NUMBER:{}", gFsProcPtr->getSplitPolicy());
  switch (gFsProcPtr->getSplitPolicy()) {
    case SPLIT_POLICY_NO_MOD_PROACTIVE:
      splitPolicy_ = new SplitPolicyModProactive();
      break;
    case SPLIT_POLICY_NO_DIR_FAMILRY:
      splitPolicy_ = new SplitPolicyDirFamily();
      break;
    case SPLIT_POLICY_NO_DELAYED_REALLOC:
      splitPolicy_ = new SplitPolicyDelaySeveralReq();
      break;
    case SPLIT_POLICY_NO_CLIENT_TRIGGERED:
      splitPolicy_ = new SplitPolicyClientTriggered();
      break;
    case SPLIT_POLICY_NO_ADD_HOC_APP_GROUPING:
      splitPolicy_ = new SplitPolicyAdHocAppGroupping(w);
      break;
    case SPLIT_POLICY_NO_DYNAMIC_BASIC:
      splitPolicy_ = new SplitPolicyDynamicBasic(w);
      break;
    case SPLIT_POLICY_NO_TWEAK_MOD:
    case SPLIT_POLICY_NO_TWEAK_MOD_SHARED:
      for (int i = 1; i < gFsProcPtr->getNumThreads(); i++) {
        gFsProcPtr->setWorkerActive(i, true);
      }
      splitPolicy_ = nullptr;
      break;
    case SPLIT_POLICY_NO_DYNAMIC_SPREAD_FIRST:
    case SPLIT_POLICY_NO_DYNAMIC_MIN_WAIT:
    case SPLIT_POLICY_NO_DYNAMIC_EXPR_LB:
    case SPLIT_POLICY_NO_DYNAMIC_EXPR_NC:
      // for these policies, worker does not have right to
      // decide a subset of inodes to be migrated out
      // it must follow LM's instruction strictly
      // while for dst_known_split_policy_, itself needs
      // to select according to some cpu allowance
      splitPolicy_ = nullptr;
      break;
    case SPLIT_POLICY_NO_DYNAMIC_PACK_ACCESS:
      dst_known_split_policy_ = new SplitPolicyDynamicDstKnown(w);
      break;
    default:
      assert(false && "Invalid Policy Number");
  }
}

// NOTE: this is not invoked anymore
// TODO: remove it once the alternative is working
void FsProcWorkerMaster::checkSplitJoinOnFsReqReceived(FsReq *req,
                                                       int &splitErrNo,
                                                       int &dstWid) {
  // TODO: is there a way to quickly return when the req's inode
  // has been decided to stay on master?
  splitErrNo = kSplitNot;  // make sure normal is NO_SPLIT
  if (gFsProcPtr->getNumThreads() < 2)
    // single-thread FSP is running
    return;

  InMemInode *inodePtr = req->getTargetInode();

  // check if this inode is in splitting
  cfs_ino_t cur_ino = inodePtr->i_no;
  auto itSplit = inSplitInodeMap_.find(cur_ino);
  if (itSplit != inSplitInodeMap_.end()) {
    // the corresponding inode is already in splitting mode
    splitErrNo = kOnTargetFiguredOutErrInSplitting;
    return;
  }

  // check if this inode is managed by other workers and need redirection
  // NOTE: we should never try to read the  *mngWid_* here
  auto ownerIt = inodeOwnershipMap_.find(cur_ino);
  if (ownerIt != inodeOwnershipMap_.end()) {
    if (ownerIt->second != getWid()) {
      // the inode's ownership belongs to another worker
      assert(ownerIt->second > 0);
      dstWid = ownerIt->second;
      if (req->getType() == FsReqType::UNLINK) {
        splitErrNo = kOnTargetFiguredOutErrUnlinkRedirected;
      } else if (req->getType() == FsReqType::RENAME) {
        splitErrNo = kOnTargetFiguredOutErrRenameRedirected;
      } else {
        splitErrNo = kOnTargetFiguredOutErrNeedRedirect;
      }
      return;
    }
  }

  if (splitPolicy_->isSplitReqType(req->getType())) {
    InMemInode *inodePtr = req->getTargetInode();
    dstWid = splitPolicy_->decideDestWorker(req->getFileInum(), req);
    SPDLOG_DEBUG("{} reqRecved, this ino:{} will be sent to wid:{}",
                 getFsReqTypeOutputString(req->getType()), req->getFileInum(),
                 dstWid);
    if (dstWid != getWid()) {
      if (splitPolicy_->isDynamic()) {
        FsProcMessageType type;
        // fprintf(stderr, "wid:%d dstWid:%d\n", getWid(), dstWid);
        if (dstWid < 0) {
          splitPolicy_->ifAckLMInflightMsg(this, type, /*isAbort*/ true);
          // we give up this round of rebalance
          FsProcMessage curmsg;
          curmsg.ctx = nullptr;
          curmsg.type = type;
          fprintf(stderr, "master send ACK for give up\n");
          messenger->send_message_to_loadmonitor(getWid(), curmsg);
        }
        // splitPolicy_->beforeStatsResetCallback(this);
        // resetLocalCopyAndPushToSR();
      } else {
        SPDLOG_INFO("directly activate");
        // check if this worker is currently asleep. If so, wake it up.
        // TODO (jingliu): move this piece to policy
        bool isWorkerAlive = gFsProcPtr->checkWorkerActive(dstWid);
        if (!isWorkerAlive) gFsProcPtr->setWorkerActive(dstWid, true);
      }

      if (dstWid >= 0) {
        // send split msg if this is a valid dst wid
        SPDLOG_INFO("{} ino:{} master decide to split out. dstWid:{}",
                    getFsReqTypeOutputString(req->getType()),
                    req->getFileInum(), dstWid);
        splitErrNo = kOnTargetFiguredOutErrInSplitting;
        submitFileInodeSplitMsg(dstWid, inodePtr, req);
        splitPolicy_->updateSplitInitialized(req->getFileInum());
      }
    }
  }
}

int FsProcWorkerMaster::checkSplitJoinComm() {
  int numMsg = 0;
  for (auto &cmmBridge : commBridgesWithServantMap_) {
    WorkerCommMessage *msg = nullptr;
    WorkerCommMessage *sendMsg = nullptr;
    msg = cmmBridge.second->Get(this, &sendMsg, logger);
    if (msg != nullptr) {
      numMsg++;
      // receive a valid reply
      if (!ifCommMsgServantsInitiated(msg->msgType)) {
        assert(msg->targetInode != nullptr ||
               msg->msgType == WorkerCommMessageType::ShmMsgReply);
      }
      // this log will segfault as inode can be null now
      // logger->debug("master recv msg. msgType:{} reply:{} tarIno:{}",
      //               getWkCommMsgTypeOutputString(msg->msgType), msg->reply,
      //               msg->targetInode->i_no);
      switch (msg->msgType) {
        case WorkerCommMessageType::FileInodeSplitOutMsgReply:
          recvFileInodeSplitMsgReply(cmmBridge, msg);
          break;
        case WorkerCommMessageType::ShmMsgReply:
          recvShmMsgReply(cmmBridge, msg);
          break;
        case WorkerCommMessageType::FileInodeJoinBackMsg:
          recvFileInodeJoinMsg(cmmBridge, msg);
          // no need to free msg and sendmsg
          // because this is a message not a reply
          continue;
        default:
          throw std::runtime_error("unsupported reply recved in master");
          break;
      }
      delete msg;
      if (sendMsg != nullptr) {
        delete sendMsg;
      }
    }  // msg != nullptr
  }    // for-loop
  return numMsg;
}

void FsProcWorkerMaster::workerRunLoop() {
  while (!(*workerRunning).load(std::memory_order_acquire)) {
    // wait until the main()'s signal to start
    usleep(10);
  }
  printMasterSymbol();
  SPDLOG_INFO("Master thread is running. wid:{}", wid);
  logger->info("Master thread started");
  logger->flush();
  FsProcTLS::SetWid(wid);

  // Initialization in new thread.
  logger->debug("master tid is reset to:{}", cfsGetTid());

  // setup device access
  initMakeDevReady();

  // device access (spdk) man set cpu affinity, so put pinToCpu() here
  pinToCpu();

  // in-memory data structures setup and pre-fetch
  initInMemDataAfterDevReady();

  // install App credentials
  gFsProcPtr->initAppsToWorker(this);

  // init split/join stuff
  if (splitPolicy_ != nullptr) {
    splitPolicy_->addWorker(getWid());
    for (auto svt : servantsVec_) {
      splitPolicy_->addWorker(svt->getWid());
    }
  }

  // setup the buffers (partition its own buffer) to the servants
  initMemDataForServants();

  // wakeup servants
  shotServantGunfire();

  fileManager->fsImpl_->BlockingInitRootInode(this);

  // we must init tid once this worker is running as a new thread
  workerTid_ = cfsGetTid();

  // initialize the bio monitor
  workerFsReqNumBioMap_[workerTid_] = {};
  SPDLOG_INFO("Master wid:{} tid:{}", wid, workerTid_);

  // wait for all the workers to be registered into the device
  while (!checkAllWorkerRegisteredToDev()) {
    usleep(1);
  }

  // sleep to make sure everything is set up
  sleep(1);

  gFsProcPtr->setWorkerActive(kMasterWidConst, true);
#ifdef CFS_START_ALL_WORKERS_ON_INIT
  for (int i = 1; i < gFsProcPtr->getNumThreads(); i++) {
    gFsProcPtr->setWorkerActive(kMasterWidConst + i, true);
  }
#endif

  // worker ready to accept requests
  gFsProcPtr->workerReady();

  while (*workerRunning) {
    // execute the file system finite-state-machine engine
    // REQUIRED: the ts must be immediately passed into *workerRunLoopInner*
    auto ts = FsReq::genTickCycle();
    bool loopEffective = workerRunLoopInner(ts);
    loopEffective |= (processInterWorkerMessages() > 0);
    loopEffective |= (ProcessPendingNewedInodesMigration() > 0);
    loopEffective |= (ProcessPendingCreationRedirect() > 0);
    loopEffective |= (checkSplitJoinComm() > 0);
    stats_recorder_.RecordLoopEffective(loopEffective, ts, splitPolicy_);
    if (dst_known_split_policy_ != nullptr) {
      dst_known_split_policy_->PerLoopCallback();
    }
    // master needs to check the EXIT event (a file's appearance)
    if ((loopCountBeforeCheckExit++) > FsProc::kCheckExitLoopNum) {
      bool exit = checkFileExistance(gFsProcPtr->exitSignalFileName);
      SPDLOG_DEBUG("check exit time:{} fileName:{}", tap_ustime(),
                   gFsProcPtr->exitSignalFileName);
      if (exit) {
        gFsProcPtr->stop();
        break;
      }
      loopCountBeforeCheckExit = 0;
    }
  }

  adgMod::Stats *instance = adgMod::Stats::GetInstance();
  instance->ReportTime();
  logger->info("Master out of run loop, ===> stats ===>");
  if (FsWorkerOpStats::kCollectWorkerOpStats)
    opStatsPtr_->outputToLogFile(logger);
  logger->flush();
  // master need to do flush
  auto flushStartTs = tap_ustime();
  dev->initWorker(getWid());
  blockingFlushBufferOnExit();
  auto flushEndTs = tap_ustime();
  SPDLOG_INFO("Flushing cached buffer time(us):{}", flushEndTs - flushStartTs);
  SPDLOG_INFO("wid: {} processed {} messages when alive", getWid(),
              num_messages_processed_);
}

void FsProcWorkerMaster::ProcessManualThreadReassign(FsReq *req) {
  auto op_ptr = &(req->getClientOp()->op.threadReassign);
  pid_t pid = req->getApp()->getPid();
  int tid = op_ptr->tid;
  SPDLOG_DEBUG("processThreadReassign  pid:{} tid:{}", pid, tid);
  if (op_ptr->dst_wid == FsProcWorker::kMasterWidConst) {
    // from others to master
    auto it = pid_tid_handling_wid_.find(pid);
    assert(it != pid_tid_handling_wid_.end());
    it->second.erase(tid);
    if (it->second.size() == 0) {
      pid_tid_handling_wid_.erase(it);
    }
  } else {
    if (op_ptr->src_wid == FsProcWorker::kMasterWidConst) {
      if (req->getClientOp()->op.threadReassign.flag != FS_REASSIGN_PAST)
        pid_tid_handling_wid_[pid][tid] = op_ptr->dst_wid;

      bool is_worker_active = gFsProcPtr->checkWorkerActive(op_ptr->dst_wid);
      if (!is_worker_active) {
        gFsProcPtr->setWorkerActive(op_ptr->dst_wid, true);
        gFsProcPtr->HandleManualReassignForLoadMng(op_ptr->dst_wid, pid, tid);
        // gFsProcPtr->loadMng->ManualThreadReassignment(op_ptr->dst_wid, pid,
        //                                               tid);
      }

      if (req->getClientOp()->op.threadReassign.flag != FS_REASSIGN_FUTURE) {
        std::unordered_set<cfs_ino_t> ino_set;
        req->getApp()->GetAccessedInoAndErase(tid, ino_set);
        // from master to others
        SPDLOG_DEBUG(
            "processThreadReassign src_wid:{} dst_wid:{}, ino_set size:{}",
            op_ptr->src_wid, op_ptr->dst_wid, ino_set.size());

        using FR = FileMng::ReassignmentOp;
        if (!ino_set.empty()) {
          // split out
          auto old_owner_callback = [](FileMng *mng,
                                       const FR::BatchedCtx *reassign_ctx,
                                       void *myctx) {
            auto req = reinterpret_cast<FsReq *>(myctx);
            req->copPtr->op.threadReassign.ret = 0;
            mng->fsWorker_->submitFsReqCompletion(req);
          };
          struct NewOwnerCtx {
            pid_t pid = 0;
            int tid = 0;
            std::unordered_set<cfs_ino_t> inos{};
          };
          auto newctx = new NewOwnerCtx();
          newctx->pid = pid;
          newctx->tid = tid;
          // we only want one copy of this set<ino>
          std::swap(newctx->inos, ino_set);
          auto new_owner_callback =
              [](FileMng *mng,
                 const FileMng::ReassignmentOp::BatchedCtx *reassign_ctx,
                 void *myctx) {
                auto newctx = reinterpret_cast<NewOwnerCtx *>(myctx);
                auto app = mng->fsWorker_->GetApp(newctx->pid);
                for (auto ino : newctx->inos) {
                  app->AccessIno(newctx->tid, ino);
                }
                delete newctx;
              };
          FR::BatchedOwnerExportThroughPrimary(
              fileManager, newctx->inos, op_ptr->dst_wid, old_owner_callback,
              req, new_owner_callback, newctx);
          goto no_completion;
        }
      }
    } else {
      // it's going to be some forwarding through master
      // master only needs to note it
      bool is_worker_active = gFsProcPtr->checkWorkerActive(op_ptr->dst_wid);
      if (!is_worker_active) {
        gFsProcPtr->setWorkerActive(op_ptr->dst_wid, true);
      }
      if (req->getClientOp()->op.threadReassign.flag != FS_REASSIGN_PAST) {
        auto it = pid_tid_handling_wid_.find(pid);
        assert(it != pid_tid_handling_wid_.end());
        auto in_it = it->second.find(tid);
        assert(in_it != it->second.end());
        // assert(in_it->second == op_ptr->src_wid);
        in_it->second = op_ptr->dst_wid;
      }
    }
  }
  submitFsReqCompletion(req);
no_completion:
  return;
}

void FsProcWorkerMaster::ProcessLmRedirectOneTau(LmMsgRedirectOneTauCtx *ctx) {
  using TimeRange = LmMsgRedirectOneTauCtx::EffectiveTimeRange;
  pid_t pid = ctx->pid;
  int tid = ctx->tid;
  int src_wid = ctx->src_wid;
  int dst_wid = ctx->dst_wid;
  auto app = GetApp(pid);
  if (app == nullptr) {
    // TODO: we should not throw here since app might exit
    // but our app_exit for now does not really clear the app pointer
    throw std::runtime_error("cannot find app pid:" + std::to_string(pid));
  }
  if (dst_wid == FsProcWorker::kMasterWidConst) {
    // from others to master
    auto it = pid_tid_handling_wid_.find(pid);
    if (it != pid_tid_handling_wid_.end()) {
      it->second.erase(tid);
      if (it->second.size() == 0) {
        pid_tid_handling_wid_.erase(it);
      }
    }
  } else {
    if (src_wid == FsProcWorker::kMasterWidConst) {
      if (ctx->time_range != TimeRange::kPast)
        pid_tid_handling_wid_[pid][tid] = dst_wid;

      if (ctx->time_range != TimeRange::kFuture) {
        std::unordered_set<cfs_ino_t> ino_set;
        app->GetAccessedInoAndErase(tid, ino_set);
        // from master to others
        SPDLOG_DEBUG(
            "ProcessLmredirectOneTau src_wid:{} dst_wid:{}, ino_set size:{}",
            src_wid, dst_wid, ino_set.size());

        using FR = FileMng::ReassignmentOp;
        if (!ino_set.empty()) {
          // split out
          auto old_owner_callback = [](FileMng *mng,
                                       const FR::BatchedCtx *reassign_ctx,
                                       void *myctx) {
            FsProcMessage msg;
            msg.type = FsProcMessageType::kLM_RedirectOneTauAck;
            msg.ctx = myctx;
            mng->fsWorker_->messenger->send_message_to_loadmonitor(
                mng->fsWorker_->getWid(), msg);
          };
          struct NewOwnerCtx {
            pid_t pid = 0;
            int tid = 0;
            std::unordered_set<cfs_ino_t> inos{};
          };
          auto newctx = new NewOwnerCtx();
          newctx->pid = pid;
          newctx->tid = tid;
          // we only want one copy of this set<ino>
          std::swap(newctx->inos, ino_set);
          auto new_owner_callback =
              [](FileMng *mng,
                 const FileMng::ReassignmentOp::BatchedCtx *reassign_ctx,
                 void *myctx) {
                auto newctx = reinterpret_cast<NewOwnerCtx *>(myctx);
                auto app = mng->fsWorker_->GetApp(newctx->pid);
                for (auto ino : newctx->inos) {
                  app->AccessIno(newctx->tid, ino);
                }
                delete newctx;
              };
          FR::BatchedOwnerExportThroughPrimary(fileManager, newctx->inos,
                                               dst_wid, old_owner_callback, ctx,
                                               new_owner_callback, newctx);
          goto no_ack;
        }
      }
    } else {
      // it's going to be some forwarding through master
      // master only needs to note it
      if (ctx->time_range != TimeRange::kPast) {
        auto it = pid_tid_handling_wid_.find(pid);
        assert(it != pid_tid_handling_wid_.end());
        auto in_it = it->second.find(tid);
        assert(in_it != it->second.end());
        // assert(in_it->second == op_ptr->src_wid);
        in_it->second = dst_wid;
      }
    }
  }
  FsProcMessage msg;
  msg.type = FsProcMessageType::kLM_RedirectOneTauAck;
  msg.ctx = ctx;
  messenger->send_message_to_loadmonitor(getWid(), msg);
no_ack:
  return;
}

void FsProcWorkerMaster::registerServantWorkersToMaster(
    const std::vector<FsProcWorkerServant *> &servantsVec) {
  WorkerCommBridge *commBridge = nullptr;
  for (auto servantWorkerPtr : servantsVec) {
    commBridge = new WorkerCommBridge(this, getWid(), servantWorkerPtr,
                                      servantWorkerPtr->getWid());
    servantWorkerPtr->setCommBridge(commBridge);
    commBridgesWithServantMap_.emplace(servantWorkerPtr->getWid(), commBridge);
    servantsVec_.push_back(servantWorkerPtr);
  }
}

void FsProcWorkerMaster::shotServantGunfire() {
  SPDLOG_INFO("Start Servants --- servant num:{}", servantsVec_.size());
  for (auto svt : servantsVec_) {
    svt->startRunning();
  }
}

bool FsProcWorkerMaster::submitFileInodeSplitMsg(int dstWid,
                                                 InMemInode *inodePtr,
                                                 FsReq *req) {
  // update bookkeeping information
  inSplitInodeMap_.emplace(inodePtr->i_no, dstWid);
  inodeOwnershipMap_[inodePtr->i_no] = InMemInode::kUnknownManageWorkerId;
  auto inProgIt = inodeInProgressFsReqMap_.find(inodePtr->i_no);
  assert(inProgIt != inodeInProgressFsReqMap_.end());
  logger->debug("[splitting] # of inProgres reqs:{}", inProgIt->second.size());

  // release the ownership of inode
  bool canResetInode = inodePtr->setManageWorkerUnknown(getWid());
  assert(canResetInode);

  // iterate each of the in progress FsReq that is targeting this inode
  // either reply or abort
  // TODO (jingliu)

  // Set the FsReq state, then this request will be directly returned
  // with return value indicates its status as *IN_TRANSFER*
  // NOTE: if comment out this, we can serve at least one data op
  // The right semantics should be: once this request is received,
  // and the inode is decided to be moved according to some policy,
  // immediately this FsReq is rejected and ACKed with the error number
  req->setState(FsReqState::OP_OWNERSHIP_UNKNOWN);

  // generate splitting request
  auto curCommMsg = genFileInodeOutMsg(
      WorkerCommMessageType::FileInodeSplitOutMsg, inodePtr, req);

  stats_recorder_.ResetInodeStatsOnSplitJoin(inodePtr);

  // make sure we can inform all shms to another worker
  collectInodeShmPtrForSecondaryWorker(curCommMsg->perAppOpenedShms);

  // We want to keep FDs in master such that, when in migration, an FD+app can
  // find its inode
  collectInodeMigrateOpenedFd(inodePtr, curCommMsg->perAppOpenedFiles);
  // if (it != inodeAppRefMap_.end()) inodeAppRefMap_.erase(it);

  // find all the data blocks that associated with the split-out inode
  // and put them into that *curCommMsg*
  fileManager->splitInodeDataBlockBufferSlot(inodePtr, curCommMsg->dataBlocks);

  fileManager->fsImpl_->uninstallInode(inodePtr);

#if CFS_JOURNAL(ON)
  // target worker will process this inode's checkpointing
  // NOTE: should we put the value into the msg and then let the target worker
  // pick it up? If so, need to add the field into WorkerCommMessage
  auto numErased =
      jmgr->checkpointInput->inodesToCheckpoint.erase(inodePtr->i_no);
  curCommMsg->journalled = static_cast<bool>(numErased);
#endif

  // submit the SplitReq to the CommBridge
  bool rt = commBridgesWithServantMap_[dstWid]->Put(this, curCommMsg, logger);
  if (!rt) {
    logger->info("cannot submit split msg");
  }
  return true;
}

bool FsProcWorkerMaster::submitFileInodeForwardMsg(int dstWid,
                                                   InMemInode *inodePtr,
                                                   WorkerCommMessage *origMsg) {
  SPDLOG_DEBUG("submitFileInodeForwardMsg");
  inSplitInodeMap_.emplace(inodePtr->i_no, dstWid);
  inodeOwnershipMap_[inodePtr->i_no] = InMemInode::kUnknownManageWorkerId;

  assert(inodePtr->getManageWorkerId() == InMemInode::kUnknownManageWorkerId);

  auto curCommMsg = genFileInodeOutMsg(
      WorkerCommMessageType::FileInodeSplitOutMsg, inodePtr, nullptr);
  // fprintf(stderr, "forward msg origWid set to %d\n", origMsg->origWid);
  curCommMsg->origWid = origMsg->origWid;
  forwardMsgToOrigJoinMsgMap_[curCommMsg] = origMsg;

  std::swap(curCommMsg->perAppOpenedFiles, origMsg->perAppOpenedFiles);

  collectInodeShmPtrForSecondaryWorker(curCommMsg->perAppOpenedShms);
  bool rt = commBridgesWithServantMap_[dstWid]->Put(this, curCommMsg, logger);
  if (!rt) {
    logger->info("cannot submit split msg");
  }
  return rt;
}

bool FsProcWorkerMaster::submitFileInodeZeroLinkMsg(int dstWid,
                                                    InMemInode *inodePtr,
                                                    FsReq *req) {
  logger->debug("[splitting] unlink request sent to wid:{} ino:{}", dstWid,
                inodePtr->i_no);
  auto curCommMsg = genFileInodeOutMsg(
      WorkerCommMessageType::FileInodeUnlinkOutMsg, inodePtr, req);
  bool rt = commBridgesWithServantMap_[dstWid]->Put(this, curCommMsg, logger);
  if (!rt) {
    logger->info("cannot submit unlink request to dst:{} ino:{}", wid,
                 inodePtr->i_no);
  }
  return rt;
}

bool FsProcWorkerMaster::submitShmMsg(int dstWid, uint8_t shmid, FsReq *req) {
  SPDLOG_DEBUG("shm request sent to wid:{} shmid:{}", dstWid, shmid);
  auto curCommMsg =
      genFileInodeOutMsg(WorkerCommMessageType::ShmMsg, nullptr, req);
  curCommMsg->perAppOpenedShms[req->getApp()->getPid()] =
      req->getApp()->findAllOpenedShmArrs();
  bool rt = commBridgesWithServantMap_[dstWid]->Put(this, curCommMsg, logger);
  return rt;
}

int FsProcWorkerMaster::initInMemDataAfterDevReady() {
  // init the memory
  uint64_t devBlockBufferSize = getTotalBlockBufferMemByte();
  SPDLOG_INFO("Init Memory: total buffer (huge-page as backend) sizeBytes:{}",
              devBlockBufferSize);
  // device allocated memory for DMA io
  devBufMemPtr = (char *)dev->zmallocBuf(devBlockBufferSize, BSIZE);
  fprintf(stdout, "PINNED memory start:%p end:%p\n", devBufMemPtr,
          devBufMemPtr + devBlockBufferSize);
  dev->setPinMemBound((devBufMemPtr), (devBufMemPtr + devBlockBufferSize));
  // pre-fetched metadata
  uint32_t sbBlockNo = FsImpl::superBlockNumber();
  if (sbBlockNo >= 0) {
    BlockReq req(sbBlockNo, nullptr, devBufMemPtr + sbBlockNo * BSIZE,
                 FsBlockReqType::READ_BLOCKING);
    int rt = submitDirectReadDevReq(&req);
    if (rt < 0) {
      SPDLOG_ERROR("FsProc cannot read super block");
      return rt;
    }
  }

  // NOTE: We need to initialize journal before any imaps as we need to maintain
  // a stable copy of them in the journal manager.
  initJournalManager();

  // pre-fetched imap
  uint32_t imapStartNo, imapNumBlocks;
  FsImpl::imapBlockNumber(imapStartNo, imapNumBlocks);
  if (imapNumBlocks > 0) {
    for (unsigned i = 0; i < imapNumBlocks; i++) {
      BlockReq req(imapStartNo + i, nullptr,
                   devBufMemPtr + (imapStartNo + i) * BSIZE,
                   FsBlockReqType::READ_BLOCKING);
      int rt = submitDirectReadDevReq(&req);
      if (rt < 0) {
        SPDLOG_ERROR("FsProc cannot read imap block");
        return rt;
      }
    }
  }
  // TODO (what if we pre-fetch all the bmap into memory)?
  // But i already has kina robust way to fetch bmap though
  // it might be a good idea to make it one optional flag

  // init file manager
  int numPartition =
      gFsProcPtr->getNumThreads() > 1 ? gFsProcPtr->getNumThreads() : 1;
  fileManager = new FileMng(this, devBufMemPtr, gFsProcPtr->getDirtyFlushRato(),
                            numPartition);
  for (unsigned i = 0; i < imapNumBlocks; i++) {
    char *inodeBitmapDevBuf = devBufMemPtr + (imapStartNo + i) * BSIZE;
    jmgr->CopyBufToStableInodeBitmap(imapStartNo + i, inodeBitmapDevBuf);
  }
  return 0;
}

int FsProcWorkerMaster::onTargetInodeFiguredOutChildBookkeep(
    FsReq *req, InMemInode *targetInode) {
  logger->debug("onTargetInodeFiguredOutDoBookkeep ino:{}", targetInode->i_no);
  auto itSplit = inSplitInodeMap_.find(targetInode->i_no);
  if (itSplit != inSplitInodeMap_.end()) {
    logger->debug("this i_no is in splitting mode, ino:{}", targetInode->i_no);
    // this inode is in splitting
    return kOnTargetFiguredOutErrInSplitting;
  }

  auto it = inodeOwnershipMap_.find(targetInode->i_no);
  if (it == inodeOwnershipMap_.end()) {
    // this inode is new
    logger->debug("new inode insert. ino:{}", targetInode->i_no);
    inodeOwnershipMap_.emplace(targetInode->i_no, getWid());
  } else {
    // this inode has already been recorded, that is, some worker is taking
    // care of it (can be this worker, or another worker...)
    int recordWid = it->second;
    logger->debug("new inode already recorded. ino:{} wid", targetInode->i_no,
                  recordWid);
    if (recordWid != getWid() && (recordWid >= 0)) {
      // this inode is out of this worker's scope
      std::string exInfo = std::string("Master find wid for ino") +
                           std::to_string(getWid()) + std::string(" ino-:") +
                           std::to_string(targetInode->i_no);
      logger->debug(exInfo);
      return kOnTargetFiguredOutErrNeedRedirect;
    } else {
      // it is okay that we do not do anything here
    }
  }
  return 0;
}

int FsProcWorkerMaster::ProcessPendingNewedInodesMigration() {
  if (pending_newed_inodes.empty()) return 0;
  using FR = FileMng::ReassignmentOp;
  int num_sent = 0;
  auto pid_it = pending_newed_inodes.begin();
  while (pid_it != pending_newed_inodes.end()) {
    auto app = GetApp(pid_it->first);
    std::unordered_map<int, std::vector<std::pair<cfs_ino_t, int>>>
        &tid_inovec_map = pid_it->second;
    auto tid_it = tid_inovec_map.begin();
    while (tid_it != tid_inovec_map.end()) {
      std::vector<std::pair<cfs_ino_t, int>> &inovec = tid_it->second;
      for (auto ino_it = inovec.begin(); ino_it != inovec.end(); ino_it++) {
        cfs_ino_t cur_ino = ino_it->first;
        int dst_wid = ino_it->second;
        assert(dst_wid > 0);
        if (NumInProgressRequests(cur_ino) > 0) {
          continue;
        } else {
          SPDLOG_DEBUG("ProcessPendingNewed dst_wid:{} ino:{}", dst_wid,
                       cur_ino);
          assert(fileManager->GetInMemInode(cur_ino) != nullptr);
          // migrate this inode to dest
          auto old_owner_callback =
              [](FileMng *mng, const FR::Ctx *reassign_ctx, void *myctx) {
                SPDLOG_DEBUG("ProcessPendingNewed old_owner");
              };
          struct NewOwnerCtx {
            pid_t pid;
            int tid;
            cfs_ino_t ino;
          };
          auto new_ctx = new NewOwnerCtx;
          new_ctx->pid = pid_it->first;
          new_ctx->tid = tid_it->first;
          new_ctx->ino = cur_ino;
          auto new_owner_callback = [](FileMng *mng,
                                       const FR::Ctx *reassign_ctx,
                                       void *myctx) {
            auto newctx = reinterpret_cast<NewOwnerCtx *>(myctx);
            SPDLOG_DEBUG("ProcessPendingNewed new_owner, app:{} tid:{} ino:{}",
                         newctx->pid, newctx->tid, newctx->ino);
            auto app = mng->fsWorker_->GetApp(newctx->pid);
            // SPDLOG_INFO("reassign done for ino:{} wid:{}", newctx->ino,
            // mng->fsWorker_->getWid());
            app->AccessIno(newctx->tid, newctx->ino);

            delete newctx;
          };
          FR::OwnerExportThroughPrimary(fileManager, cur_ino, dst_wid,
                                        old_owner_callback, nullptr,
                                        new_owner_callback, new_ctx);

          app->EraseIno(tid_it->first, cur_ino);
          inovec.erase(ino_it--);
          num_sent++;
        }
      }
      if (inovec.empty()) {
        tid_inovec_map.erase(tid_it++);
      } else {
        ++tid_it;
      }
    }
    if (tid_inovec_map.empty()) {
      pending_newed_inodes.erase(pid_it++);
    } else {
      ++pid_it;
    }
  }  // iterate pending_newed_inodes
  return num_sent;
}

void FsProcWorkerMaster::initMemDataForServants() {
  FileMng *fm = nullptr;
  for (uint i = 0; i < servantsVec_.size(); i++) {
    fm = fileManager->generateSubFileMng(servantsVec_[i]);
    servantsVec_[i]->initFileManager(fm);
  }
}

void FsProcWorkerMaster::collectInodeShmPtrForSecondaryWorker(
    std::unordered_map<pid_t, shmIdBufPairs *> &appShms) {
  AppProc *curApp = nullptr;
  for (auto &appIt : appMap) {
    curApp = appIt.second;
    appShms[appIt.first] = curApp->findAllOpenedShmArrs();
    logger->debug("[splitting] # of shms:{}", (appShms[appIt.first])->size());
  }
}

void FsProcWorkerMaster::recvFileInodeSplitMsgReply(
    const std::pair<FsProc::fsproc_wid_t, WorkerCommBridge *> &cmmBridge,
    WorkerCommMessage *msg) {
  // Update the bookkeeping information, thus we can re-direct the
  // subsequent client *FsReq* to the right worker
  auto inSplitIt = inSplitInodeMap_.find(msg->targetInode->i_no);
  auto ownIt = inodeOwnershipMap_.find(msg->targetInode->i_no);
  if (inSplitIt == inSplitInodeMap_.end() ||
      ownIt == inodeOwnershipMap_.end()) {
    throw std::runtime_error("inode cannot find in both ownmap or splitMap");
  } else {
    int preWid = inodeOwnershipMap_[msg->targetInode->i_no];
    inodeOwnershipMap_[msg->targetInode->i_no] =
        cmmBridge.second->getOtherSideWid(this);
    logger->debug("ownership map, ino:{} item is set to :{} from {}",
                  msg->targetInode->i_no,
                  inodeOwnershipMap_[msg->targetInode->i_no], preWid);

    // reset this inode's inSplit status
    inSplitInodeMap_.erase(inSplitIt);
  }
  if (splitPolicy_->isDynamic()) {
    fprintf(stderr, "revFileInodeSplitMsgReply origWid:%d\n",
            msg->sendMsg->origWid);
    if (msg->sendMsg->origWid == FsProcWorker::kMasterWidConst) {
      FsProcMessageType type;
      if (splitPolicy_->ifAckLMInflightMsg(this, type, /*isAbort*/ false)) {
        FsProcMessage curmsg;
        curmsg.ctx = nullptr;
        curmsg.type = type;
        fprintf(stderr, "recvFileInodeSplitReply master ACK\n");
        messenger->send_message_to_loadmonitor(getWid(), curmsg);
      }
      splitPolicy_->updateSplitDone(msg->targetInode->i_no);
    } else {
      auto msgIt = forwardMsgToOrigJoinMsgMap_.find(msg->sendMsg);
      if (msgIt == forwardMsgToOrigJoinMsgMap_.end()) {
        throw std::runtime_error(
            "we cannot find the original msg for forwarding");
      }
      WorkerCommMessage *origMsg = msgIt->second;
      // It is a forward message, we send ACK to the origWid
      auto replyMsg = genFileInodeOutReplyMsg(
          WorkerCommMessageType::FileInodeJoinBackMsgReply, origMsg);
      replyMsg->reply = WK_COMM_RPL_GENERAL_OK;
      bool rt = commBridgesWithServantMap_[msg->sendMsg->origWid]->Put(
          this, replyMsg, logger);
      if (!rt) {
        SPDLOG_ERROR("cannot submit inodeJoinBackMsg Reply for forward");
      }
    }
  }
  assert(msg->targetInode != nullptr);
}

void FsProcWorkerMaster::recvShmMsgReply(
    const std::pair<FsProc::fsproc_wid_t, WorkerCommBridge *> &cmmBridge,
    WorkerCommMessage *msg) {
  SPDLOG_DEBUG("shm reply msg");
  assert(msg->reply == WK_COMM_RPL_GENERAL_OK);
  auto fsReq = msg->req;
  assert(fsReq != nullptr);
  assert(fsReq->getState() == FsReqState::NEW_SHM_ALLOC_WAIT_REPLY);
  if (--fsReq->pendingShmMsg == 0) submitReadyReq(fsReq);
}

// NOTE: It might be some race there.
// Client get from W1 that inode does not belongs to W1, then it reaches
// out to W0, but W0 has not yet invoke this recvFileInodeJoinMsg.
// W0 is going to find the owner is W1, and send wid=1 with REDIRECT to client
// Might be some ping-pong, but it's fine because:
// 1) no validation of correctness
// 2) W1 will process this message eventually
void FsProcWorkerMaster::recvFileInodeJoinMsg(
    const std::pair<FsProc::fsproc_wid_t, WorkerCommBridge *> &cmmBridge,
    WorkerCommMessage *msg) {
  auto inode = msg->targetInode;
  assert(inode != nullptr);
  int dstWid = msg->dstWid;
  SPDLOG_INFO("wid:{} recvFileInodeJoinMsg ino:{} dstWid:{}", getWid(),
              inode->i_no, dstWid);
  assert(dstWid != cmmBridge.first && dstWid >= 0);
  if (dstWid != FsProcWorker::kMasterWidConst) {
    // we first activate the worker if needed (according to policy)
    splitPolicy_->activateDestWorker(dstWid);
    installOpenedFd(msg->targetInode->i_no, msg->perAppOpenedFiles,
                    /*isMasterForward*/ true);
    // forward this inode to another worker
    submitFileInodeForwardMsg(dstWid, inode, msg);
  } else {
    logger->debug("Install inode to master: ino{}", msg->targetInode->i_no);
    SPDLOG_INFO("recvJoinMsg: Install inode to master ino:{}",
                msg->targetInode->i_no);
    // install the inode to master
    auto inode = msg->targetInode;
    bool claimOk =
        inode->setManageWorkerId(InMemInode::kUnknownManageWorkerId, getWid());
    assert(claimOk);
    // install to fsImpl_
    fileManager->installInode(inode);
#if CFS_JOURNAL(ON)
    if (msg->journalled) {
      SPDLOG_DEBUG("adding to inodesToCheckpoint in migrated worker");
      jmgr->checkpointInput->inodesToCheckpoint.emplace(inode->i_no, nullptr);
    }
#endif
    // install data block buffers
    std::unordered_set<BlockBufferItem *> &bufferItems = msg->dataBlocks;
    if (bufferItems.size() > 0) {
      // SPDLOG_INFO("installDataBlock itemNum:{}", bufferItems.size());
      fileManager->installDataBlockBufferSlot(inode, bufferItems);
    }

    // install fds
    cfs_ino_t ino = msg->targetInode->i_no;
    SPDLOG_DEBUG("master install opened fds. fd num:{} for ino:{}",
                 msg->perAppOpenedFiles.size(), ino);
    installOpenedFd(ino, msg->perAppOpenedFiles);

    // fix the ownership map
    int preWid = inodeOwnershipMap_[ino];
    if (preWid != cmmBridge.first) {
      logger->error("recv joinning inode whose ownership is:{} but from wid:{}",
                    preWid, cmmBridge.first);
    }
    inodeOwnershipMap_[ino] = kMasterWidConst;

    // send ACK to the original owner i only master it the dst
    auto replyMsg = genFileInodeOutReplyMsg(
        WorkerCommMessageType::FileInodeJoinBackMsgReply, msg);
    replyMsg->reply = WK_COMM_RPL_GENERAL_OK;
    bool rt = commBridgesWithServantMap_[cmmBridge.first]->Put(this, replyMsg,
                                                               logger);
    if (!rt) {
      logger->error("cannot submit inodeJoinReply msg");
    }
  }
}

void FsProcWorkerMaster::addRecentlyReassignedInodeFds(ExportedInode &exp) {
  for (const auto &[pid, fdToFobj] : exp.inode->getAppFdMap()) {
    auto &fdmap = recentlyReassignedAppFdToInodes_[pid];
    for (const auto &kv : fdToFobj) {
      fdmap[kv.first] = exp.inode->i_no;
    }
  }
}

void FsProcWorkerMaster::delRecentlyReassignedInodeEntries(pid_t app_id) {
  // NOTE: since the map is of the form <int, <int, int>>, manual cleanup of
  // inner map is not required.
  recentlyReassignedAppFdToInodes_.erase(app_id);
}

void FsProcWorkerMaster::delRecentlyReassignedInodeEntries(pid_t app_id,
                                                           int fd) {
  auto search = recentlyReassignedAppFdToInodes_.find(app_id);
  if (search != recentlyReassignedAppFdToInodes_.end())
    search->second.erase(fd);
}

cfs_ino_t FsProcWorkerMaster::getRecentlyReassignedInodeFromFd(pid_t app_id,
                                                               int fd) {
  auto outer_search = recentlyReassignedAppFdToInodes_.find(app_id);
  if (outer_search == recentlyReassignedAppFdToInodes_.end())
    return 0;  // FIXME: ensure 0 is not a vaild inode

  auto &fdset = outer_search->second;
  auto inner_search = fdset.find(fd);
  if (inner_search == fdset.end()) return 0;

  return inner_search->second;
}

FsProcWorkerServant::FsProcWorkerServant(int w, CurBlkDev *d, int shmBaseOffset,
                                         std::atomic_bool *workerRunning,
                                         PerWorkerLoadStatsSR *stats)
    : FsProcWorker(w, d, shmBaseOffset, workerRunning, stats) {
  switch (gFsProcPtr->getSplitPolicy()) {
    case SPLIT_POLICY_NO_CLIENT_TRIGGERED:
      splitPolicy_ = new SplitPolicyClientTriggeredServant();
      break;
    case SPLIT_POLICY_NO_DYNAMIC_BASIC:
      splitPolicy_ = new SplitPolicyDynamicBasic(w);
      break;
    case SPLIT_POLICY_NO_DYNAMIC_SPREAD_FIRST:
    case SPLIT_POLICY_NO_DYNAMIC_MIN_WAIT:
    case SPLIT_POLICY_NO_DYNAMIC_EXPR_LB:
      splitPolicy_ = nullptr;
      break;
    case SPLIT_POLICY_NO_DYNAMIC_PACK_ACCESS:
      dst_known_split_policy_ = new SplitPolicyDynamicDstKnown(w);
      break;
    default:
      splitPolicy_ = new SplitPolicyServantNoop(w);
  }
}

void FsProcWorkerServant::checkSplitJoinOnFsReqReceived(
    FsReq *req, int &splitErrNo, int &reqRedirectDstWid) {
  splitErrNo = kSplitNot;
  reqRedirectDstWid = getWid();
  if (splitPolicy_->isDynamic()) {
    SPDLOG_DEBUG("servant checkSplitJoin on Req received");
    InMemInode *inodePtr = req->getTargetInode();
    cfs_ino_t cur_ino = inodePtr->i_no;
    auto mngIt = inMngInodeSet_.find(cur_ino);
    if (mngIt == inMngInodeSet_.end()) {
      reqRedirectDstWid = FsProcWorker::kMasterWidConst;
      splitErrNo = kOnTargetFiguredOutErrNeedRedirect;
      return;
    }

    auto itJoin = inJoinInodeSet_.find(cur_ino);
    if (itJoin != inJoinInodeSet_.end()) {
      // if it is already in joining state, then the worker must be w0
      reqRedirectDstWid = FsProcWorker::kMasterWidConst;
      splitErrNo = kOnTargetFiguredOutErrInJoining;
      return;
    }
    if (splitPolicy_->isSplitReqType(req->getType())) {
      int inoDstWid = splitPolicy_->decideDestWorker(cur_ino, req);
      SPDLOG_DEBUG("curWid:{} dstWid is:{}", getWid(), inoDstWid);
      if (inoDstWid != getWid()) {
        if (inoDstWid < 0) {
          FsProcMessage curmsg;
          FsProcMessageType type;
          curmsg.ctx = nullptr;
          fprintf(stderr, "wid:%d giveup ACK\n", getWid());
          splitPolicy_->ifAckLMInflightMsg(this, type, /*isAbort*/ true);
          curmsg.type = type;
          SPDLOG_INFO("wid{} give up ACK", getWid());
          messenger->send_message_to_loadmonitor(getWid(), curmsg);
        } else {
          SPDLOG_INFO("wid:{} join back inode ino:{} newdst:{}", getWid(),
                      cur_ino, inoDstWid);
          // fprintf(stderr, "wid:%d submitFileInodeJoin dst:%d ino:%u\n",
          //         getWid(), inoDstWid, cur_ino);
          splitErrNo = kOnTargetFiguredOutErrInJoining;
          submitFileInodeJoinMsg(inodePtr, req, inoDstWid);
          reqRedirectDstWid = FsProcWorker::kMasterWidConst;
          splitPolicy_->updateSplitInitialized(cur_ino);
        }
      }
    }  // isSplitReqType
  }    // splitPolicy_->isDynamic()
}

void FsProcWorkerServant::recvJoinAll(fsp_lm::PerWorkerLoadAllowance *allow) {
  assert(allow->wid == getWid());
  assert(splitPolicy_ != nullptr);
  fprintf(stderr, "wid-%d recvJoinAll\n", getWid());
  splitPolicy_->recvRebalanceCallback(allow);
  // we iterate all the inodes and issue the JOIN request
  int dstWid = allow->rwid;
  InMemInode *inodePtr = fileManager->fsImpl_->inodeBegin();
  int num_msg = 0;
  while (inodePtr != nullptr) {
    submitFileInodeJoinMsg(inodePtr, nullptr, dstWid);
    splitPolicy_->updateSplitInitialized(inodePtr->i_no);
    num_msg++;
    inodePtr = fileManager->fsImpl_->inodeBegin();
  }
  SPDLOG_INFO("wid-{} recvJoinAll numMsgSent:{} inJoinNum:{}", getWid(),
              num_msg, inJoinInodeSet_.size());
  if (num_msg > 0) {
    // wait for ACKs
    constexpr uint64_t numSleepNs = 1000 * 1000;
    while (inJoinInodeSet_.size() > 0) {
      SpinSleep(numSleepNs);
      checkSplitJoinComm();
    }
  } else {
    // we directly send ACK
    FsProcMessage curmsg;
    curmsg.ctx = nullptr;
    curmsg.type = FsProcMessageType::LM_JOINALL_ACK;
    SPDLOG_INFO("wid:{} recvJoinAll directly ACK", getWid());
    // fprintf(stderr, "wid-%d directly ACK JoinALL\n", getWid());

#ifdef SCALE_USE_BUCKETED_DATA
    splitPolicy_->beforeStatsResetCallback(&stats_recorder_);
    auto ts = worker_stats::TickTs();
    stats_recorder_.ResetStats(ts);
    stats_recorder_.PushLoadStatFromLocalToSR(ts);
#endif

    messenger->send_message_to_loadmonitor(getWid(), curmsg);
    splitPolicy_->updateJoinDone();
  }
  redirectZombieAppReqsToMaster();
  SPDLOG_INFO("wid-{} now is idle\n", getWid());
}

int FsProcWorkerServant::checkSplitJoinComm() {
  WorkerCommMessage *msg = nullptr;
  WorkerCommMessage *sendMsg = nullptr;
  msg = commBridgeWithMaster_->Get(this, &sendMsg, logger);
  int numMsg = 0;
  if (msg != nullptr) {
    numMsg++;
    switch (msg->msgType) {
      case WorkerCommMessageType::FileInodeSplitOutMsg:
        recvFileInodeSplitMsg(msg);
        break;
      case WorkerCommMessageType::ShmMsg:
        recvShmMsg(msg);
        break;
      case WorkerCommMessageType::FileInodeJoinBackMsgReply: {
        recvFileInodeJoinMsgReply(msg);
        delete msg->sendMsg;
        delete msg;
      } break;
      default:
        logger->info("recv invalid msg");
        assert(false);
        break;
    }
  }  // end of processing message (msg != nullptr)
  return numMsg;
}

void FsProcWorkerServant::workerRunLoop() {
  SPDLOG_DEBUG("Servant (wid:{}) is waiting for starting gunfire! tid:{}", wid,
               cfsGetTid());
  FsProcTLS::SetWid(wid);
  waitForStartGunFire();

  // Initialization in new thread
  initMakeDevReady();

  pinToCpu();

  while (!checkAllWorkerRegisteredToDev()) {
    SpinSleep(/*ns*/ 1000);
  }

  initJournalManager();

  // TODO (jingliu): directly install the credential info or let the
  // split/join message pass the corresponding info? - Install it directly
  // now.
  gFsProcPtr->initAppsToWorker(this);
  while (*workerRunning) {
    if (!gFsProcPtr->checkWorkerActive(wid)) {
      while (!gFsProcPtr->checkWorkerActive(wid) && *workerRunning)
        ;
      // TODO: put more wake up preparation here
      SPDLOG_INFO("wid:{} activated localvid:{}", getWid(),
                  stats_recorder_.GetVersion());
    }

    // REQUIRED: ts must be passed immediately into *workerRunLoopInner*
    auto ts = FsReq::genTickCycle();
    bool loopEffective = workerRunLoopInner(ts);
    loopEffective |= (checkSplitJoinComm() > 0);
    // TODO (jing) : should we process messages inside the inner loop instead?
    loopEffective |= (processInterWorkerMessages() > 0);
    stats_recorder_.RecordLoopEffective(loopEffective, ts, splitPolicy_);
    if (dst_known_split_policy_ != nullptr) {
      dst_known_split_policy_->PerLoopCallback();
    }
  }

  logger->info("Servant Done. out of the loop... ===> stats ===>");
  if (FsWorkerOpStats::kCollectWorkerOpStats)
    opStatsPtr_->outputToLogFile(logger);
  logger->flush();
  auto flushStartTs = tap_ustime();
  // TODO: send a syncall command and run workerRunLoopInner
  // Master should also flush all metadata
  // fileManager->flushBufferOnExit();
  blockingFlushBufferOnExit();
  auto flushEndTs = tap_ustime();
  logger->info("Flushing cached buffer time(us):{}", flushEndTs - flushStartTs);
  logger->flush();
  SPDLOG_INFO("wid: {} processed {} messages when alive", getWid(),
              num_messages_processed_);
}

void FsProcWorkerServant::ProcessManualThreadReassign(FsReq *req) {
  auto op_ptr = &(req->getClientOp()->op.threadReassign);
  if (op_ptr->flag == FS_REASSIGN_FUTURE) {
    submitFsReqCompletion(req);
    return;
  }
  auto app = req->getApp();
  pid_t pid = app->getPid();
  SPDLOG_INFO("wid:{} ProcessManualThreadReassign src_wid:{} dst_wid:{} app:{}",
              getWid(), op_ptr->src_wid, op_ptr->dst_wid, pid);
  int tid = op_ptr->tid;
  assert(op_ptr->src_wid == getWid());
  std::unordered_set<cfs_ino_t> ino_set;
  app->GetAccessedInoAndErase(tid, ino_set);
  if (!ino_set.empty()) {
    using FR = FileMng::ReassignmentOp;
    // split out
    auto old_owner_callback =
        [](FileMng *mng, const FR::BatchedCtx *reassign_ctx, void *myctx) {
          auto req = reinterpret_cast<FsReq *>(myctx);
          req->copPtr->op.threadReassign.ret = 0;
          mng->fsWorker_->submitFsReqCompletion(req);
        };
    struct NewOwnerCtx {
      pid_t pid;
      int tid;
      std::unordered_set<cfs_ino_t> inos = {};
    };
    auto newctx = new NewOwnerCtx();
    newctx->pid = pid;
    newctx->tid = tid;
    std::swap(newctx->inos, ino_set);
    auto new_owner_callback =
        [](FileMng *mng,
           const FileMng::ReassignmentOp::BatchedCtx *reassign_ctx,
           void *myctx) {
          auto newctx = reinterpret_cast<NewOwnerCtx *>(myctx);
          auto app = mng->fsWorker_->GetApp(newctx->pid);
          for (auto ino : newctx->inos) {
            app->AccessIno(newctx->tid, ino);
          }
        };
    FR::BatchedOwnerExportThroughPrimary(fileManager, newctx->inos,
                                         op_ptr->dst_wid, old_owner_callback,
                                         req, new_owner_callback, newctx);
  } else {
    submitFsReqCompletion(req);
  }
}

void FsProcWorkerServant::ProcessLmRedirectOneTau(LmMsgRedirectOneTauCtx *ctx) {
  using TimeRange = LmMsgRedirectOneTauCtx::EffectiveTimeRange;
  if (ctx->time_range == TimeRange::kFuture) {
    throw std::runtime_error("kFuture should not be sent to worker:" +
                             std::to_string(getWid()));
  }
  pid_t pid = ctx->pid;
  auto app = GetApp(pid);
  int tid = ctx->tid;
  assert(ctx->src_wid == getWid());
  assert(app != nullptr);
  std::unordered_set<cfs_ino_t> ino_set;
  app->GetAccessedInoAndErase(tid, ino_set);
  SPDLOG_INFO(
      "wid:{} ProcessLmRedirectOneTau src_wid:{} dst_wid:{} app:{} tid:{} "
      "num_inode:{}",
      getWid(), ctx->src_wid, ctx->dst_wid, pid, tid, ino_set.size());
  if (!ino_set.empty()) {
    using FR = FileMng::ReassignmentOp;
    // split out
    auto old_owner_callback =
        [](FileMng *mng, const FR::BatchedCtx *reassign_ctx, void *myctx) {
          FsProcMessage msg;
          msg.type = FsProcMessageType::kLM_RedirectOneTauAck;
          msg.ctx = myctx;
          mng->fsWorker_->messenger->send_message_to_loadmonitor(
              mng->fsWorker_->getWid(), msg);
        };
    struct NewOwnerCtx {
      pid_t pid;
      int tid;
      std::unordered_set<cfs_ino_t> inos = {};
    };
    auto newctx = new NewOwnerCtx();
    newctx->pid = pid;
    newctx->tid = tid;
    std::swap(newctx->inos, ino_set);
    auto new_owner_callback =
        [](FileMng *mng,
           const FileMng::ReassignmentOp::BatchedCtx *reassign_ctx,
           void *myctx) {
          auto newctx = reinterpret_cast<NewOwnerCtx *>(myctx);
          auto app = mng->fsWorker_->GetApp(newctx->pid);
          for (auto ino : newctx->inos) {
            app->AccessIno(newctx->tid, ino);
          }
        };
    FR::BatchedOwnerExportThroughPrimary(fileManager, newctx->inos,
                                         ctx->dst_wid, old_owner_callback, ctx,
                                         new_owner_callback, newctx);
  } else {
    FsProcMessage msg;
    msg.type = FsProcMessageType::kLM_RedirectOneTauAck;
    msg.ctx = ctx;
    messenger->send_message_to_loadmonitor(getWid(), msg);
  }
}

void FsProcWorker::blockingFlushBufferOnExit() {
  auto bypass_shutdown_sync = std::getenv("FSP_BYPASS_SHUTDOWN_NOSYNC");
  if (bypass_shutdown_sync != nullptr &&
      std::string(bypass_shutdown_sync) == "YES") {
    SPDLOG_INFO("FSP_BYPASS_SHUTDOWN_NOSYNC set to {}", bypass_shutdown_sync);
    return;
  }
  auto req = new FsReq(wid);
  req->setType(FsReqType::SYNCALL);
  req->reqState = FsReqState::SYNCALL_GUARD;

  auto completionCallback = [](FsReq *req, void *ctx) {
    int *completed = static_cast<int *>(ctx);
    *completed = 1;
  };

  int *ctx = new int;
  *ctx = 0;
  req->completionCallback = completionCallback;
  req->completionCallbackCtx = static_cast<void *>(ctx);

  fileManager->processReq(req);
  volatile int *completed = ctx;
  while (*completed == 0) {
    auto ts = FsReq::genTickCycle();
    // TODO remove all apps so that we only check device completions and
    // process from ready queue. We don't want to service new app requests.
    workerRunLoopInner(ts);
  };

#if CFS_JOURNAL(PERF_METRICS)
  std::string filename =
      std::string("./logs/journalPerf_") + std::to_string(wid) + ".json";
  jmgr->dumpJournalPerfMetrics(filename);
#endif  // CFS_JOURNAL(PERF_METRICS)

#if CFS_JOURNAL(ON)
  // TODO write out journal super here.
  // It isn't much of a problem for now as fsOfflineCheckpointer can defend
  // against a stale journal superblock.
  // TODO should we flush fs superblock? Or should journal replay do that?
  // fsOfflineCheckpointer may not be flushing the superblock currently.
#else
  fileManager->flushMetadataOnExit();
#endif

  SPDLOG_INFO("wid:{} All buffers successfully flushed to disk ~~~", getWid());
}

void FsProcWorkerServant::startRunning() {
  bool expect = false;
  while (
      !(*workerRunning)
           .compare_exchange_strong(expect, true, std::memory_order_acquire) &&
      (!expect))
    ;
}

void FsProcWorkerServant::waitForStartGunFire() {
  while (!(*workerRunning).load(std::memory_order_acquire)) {
    // wait until the main()'s signal to start
    usleep(100);
  }
  printServantSymbol();
  logger->info("Received starting gunfire from the master, time for working");
  logger->flush();
}

bool FsProcWorkerServant::recvFileInodeSplitMsg(WorkerCommMessage *msg) {
  auto inodePtr = msg->targetInode;
  cfs_ino_t ino = (inodePtr == nullptr) ? 0 : inodePtr->i_no;
  if (ino == 0) {
    logger->warn(
        "checkSplitJoinComm: msg received from the master. ino is zero");
  }
  SPDLOG_DEBUG("checkSplitJoinComm: msg received from the master. ino:{}", ino);

  // update inodeAppRefMap_
  auto referringIt = inodeAppRefMap_.find(ino);
  if (referringIt == inodeAppRefMap_.end()) {
    inodeAppRefMap_.emplace(ino, std::unordered_set<pid_t>({}));
  }

  inMngInodeSet_.insert(ino);

  // extract app shmIds
  for (auto &ele : msg->perAppOpenedShms) {
    pid_t curPid = ele.first;
    auto appIt = appMap.find(curPid);
    assert(appIt != appMap.end());
    appIt->second->overwriteOpenedShmIds((msg->perAppOpenedShms)[curPid]);
  }

  // extract the opened FileObjs
  installOpenedFd(ino, msg->perAppOpenedFiles);

  bool claimOk =
      inodePtr->setManageWorkerId(InMemInode::kUnknownManageWorkerId, getWid());
  logger->debug("claim inode's manage worker. succeed? {} inodeWid:{}", claimOk,
                inodePtr->getManageWorkerId());
  // install inode to FileMng (FsImpl's inodeMap_)
  fileManager->installInode(inodePtr);

  // install data block buffers
  std::unordered_set<BlockBufferItem *> &bufferItems = msg->dataBlocks;
  if (bufferItems.size() > 0) {
    // SPDLOG_INFO("installDataBlock itemNum:{}", bufferItems.size());
    fileManager->installDataBlockBufferSlot(inodePtr, bufferItems);
  }

#if CFS_JOURNAL(ON)
  if (msg->journalled) {
    SPDLOG_DEBUG("adding to inodesToCheckpoint in migrated worker");
    jmgr->checkpointInput->inodesToCheckpoint.emplace(inodePtr->i_no, nullptr);
  }
#endif

  // send reply back to the master thread
  auto replyMsg = genFileInodeOutReplyMsg(
      WorkerCommMessageType::FileInodeSplitOutMsgReply, msg);
  // fprintf(stderr, "wid:%d FileInodeSplitOutMsgReply origWid:%d\n",
  // getWid(),
  //         replyMsg->sendMsg->origWid);
  replyMsg->reply = WK_COMM_RPL_GENERAL_OK;

  // submit the reply
  bool rt = commBridgeWithMaster_->Put(this, replyMsg, logger);
  if (!rt) {
    logger->error("cannot submit reply");
  }
  logger->debug("[splitting] reply submitted");
  return rt;
}

bool FsProcWorkerServant::recvShmMsg(WorkerCommMessage *msg) {
  // extract app shmIds
  for (auto &ele : msg->perAppOpenedShms) {
    pid_t curPid = ele.first;
    auto appIt = appMap.find(curPid);
    assert(appIt != appMap.end());
    appIt->second->overwriteOpenedShmIds((msg->perAppOpenedShms)[curPid]);
  }

  // send reply back to the master thread
  auto replyMsg =
      genFileInodeOutReplyMsg(WorkerCommMessageType::ShmMsgReply, msg);
  replyMsg->reply = WK_COMM_RPL_GENERAL_OK;

  // submit the reply
  bool rt = commBridgeWithMaster_->Put(this, replyMsg, logger);
  if (!rt) {
    logger->error("cannot submit reply");
  }
  logger->debug("[splitting] reply submitted");
  return rt;
}

bool FsProcWorkerServant::recvFileInodeJoinMsgReply(WorkerCommMessage *msg) {
  auto inodePtr = msg->targetInode;
  auto inJoinIt = inJoinInodeSet_.find(inodePtr->i_no);
  assert(inJoinIt != inJoinInodeSet_.end());
  inJoinInodeSet_.erase(inJoinIt);
  SPDLOG_INFO("wid:{} recvFileInodeJoinMsgReply ino:{}", getWid(),
              inodePtr->i_no);
  // fprintf(stderr, "recvFileInodeJoinMsgReply ino:%u\n", inodePtr->i_no);
  // For forward message
  if (splitPolicy_->isDynamic()) {
    FsProcMessageType type;
    // TODO (jingliu): we'd better do it all together. i.e., using one message
    // this make sure that we have sent all msg out
    splitPolicy_->updateSplitDone(inodePtr->i_no);
    if (splitPolicy_->ifAckLMInflightMsg(this, type, /*isAbort*/ false)) {
      if (inJoinInodeSet_.size() == 0) {
        FsProcMessage curmsg;
        curmsg.ctx = nullptr;
        curmsg.type = type;
        // fprintf(stderr, "wid:%d recvFileInodeJoinMsgReply send ACK
        // typ:%d\n",
        //        getWid(), int(type));
        SPDLOG_INFO("wid:{} recvFileInodeJoinMsgReply send ACK", getWid());
        if (type == FsProcMessageType::LM_JOINALL_ACK) {
#ifdef SCALE_USE_BUCKED_DATA
          splitPolicy_->beforeStatsResetCallback(&stats_recorder_);
          auto ts = worker_stats::TickTs();
          stats_recorder_.ResetStats(ts);
          stats_recorder_.PushLoadStatFromLocalToSR(ts);
#endif
          splitPolicy_->updateJoinDone();
        }
        messenger->send_message_to_loadmonitor(getWid(), curmsg);
      }
    }
  }

  return false;
}

bool FsProcWorkerServant::submitFileInodeJoinMsg(InMemInode *inodePtr,
                                                 FsReq *req, int inoDstWid) {
  SPDLOG_DEBUG("submitFileInodeJoinMsg ino:{}", inodePtr->i_no);
  inJoinInodeSet_.emplace(inodePtr->i_no);
  inMngInodeSet_.erase(inodePtr->i_no);

  bool canResetInode = inodePtr->setManageWorkerUnknown(getWid());
  assert(canResetInode);

  if (req != nullptr) {
    req->setState(FsReqState::OP_OWNERSHIP_REDIRECT);
  }

  auto curCommMsg = genFileInodeOutMsg(
      WorkerCommMessageType::FileInodeJoinBackMsg, inodePtr, req);
  curCommMsg->dstWid = inoDstWid;
  curCommMsg->origWid = getWid();

  stats_recorder_.ResetInodeStatsOnSplitJoin(inodePtr);

  auto it =
      collectInodeMigrateOpenedFd(inodePtr, curCommMsg->perAppOpenedFiles);
  if (it != inodeAppRefMap_.end()) inodeAppRefMap_.erase(it);

  fileManager->splitInodeDataBlockBufferSlot(inodePtr, curCommMsg->dataBlocks);
  fileManager->fsImpl_->uninstallInode(inodePtr);

#if CFS_JOURNAL(ON)
  size_t numErased =
      jmgr->checkpointInput->inodesToCheckpoint.erase(inodePtr->i_no);
  curCommMsg->journalled = static_cast<bool>(numErased);
#endif

  bool rt = commBridgeWithMaster_->Put(this, curCommMsg, logger);
  if (!rt) {
    logger->error("cannot submit comm message for join back inode");
  }
  logger->debug("submit join back msg");
  return rt;
}
