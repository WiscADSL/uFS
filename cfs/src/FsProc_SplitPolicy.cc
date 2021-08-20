
#include "FsProc_Fs.h"
#include "FsProc_FsImpl.h"
#include "FsProc_LoadMng.h"
#include "FsProc_Messenger.h"

extern FsProc *gFsProcPtr;

bool SplitPolicy::isSplitReqType(FsReqType tp) {
  bool isSplitTyp;
  switch (tp) {
    case FsReqType::READ:
    case FsReqType::PREAD:
    case FsReqType::WRITE:
    case FsReqType::PWRITE:
    case FsReqType::ALLOC_READ:
    case FsReqType::ALLOC_PREAD:
    case FsReqType::ALLOC_WRITE:
    case FsReqType::ALLOC_PWRITE:
    case FsReqType::LSEEK:
    case FsReqType::OPEN:
    case FsReqType::STAT:
    case FsReqType::FSTAT:
      isSplitTyp = true;
      break;
    default:
      isSplitTyp = false;
  }
  return isSplitTyp;
}

int SplitPolicy::activateDestWorker(int dstWid) {
  bool isWorkerAlive = gFsProcPtr->checkWorkerActive(dstWid);
  if (!isWorkerAlive) gFsProcPtr->setWorkerActive(dstWid, true);
  return 0;
}

int SplitPolicyModProactive::decideDestWorker(uint32_t ino, FsReq *req) {
  size_t numWorkers = widSet_.size();
  uint idx = ino % numWorkers;
  auto it = widSet_.begin();
  std::advance(it, idx);
  return *it;
}

int SplitPolicyDirFamily::decideDestWorker(uint32_t ino, FsReq *req) {
  if (!req->isPathBasedReq()) return 0;
  int depth = req->getStandardPathDepth();
  if (depth < 2) return 0;

  std::string parentPath = req->getStandardPartialPath(2);
  auto it = dirMap.find(parentPath);
  int target = 0;
  if (it == dirMap.end()) {
    target = currentCount;
    dirMap.emplace(parentPath, currentCount++);
  } else {
    target = it->second;
  }

  auto setIt = widSet_.begin();
  std::advance(setIt, target % widSet_.size());
  return *setIt;
}

int SplitPolicyDelaySeveralReq::decideDestWorker(uint32_t ino, FsReq *req) {
  auto it = inodeAccessCounter.find(ino);
  if (it == inodeAccessCounter.end()) {
    inodeAccessCounter.emplace(ino, 1);
    return FsProcWorker::kMasterWidConst;
  }
  inodeAccessCounter[ino]++;
  if (inodeAccessCounter[ino] > kSplitThreshold) {
    size_t numWorkers = widSet_.size();
    uint idx = ino % numWorkers;
    auto it = widSet_.begin();
    std::advance(it, idx);
    return *it;
  }
  return FsProcWorker::kMasterWidConst;
}

bool SplitPolicyClientTriggered::isSplitReqType(FsReqType tp) {
  return tp == FsReqType::APP_MIGRATE;
}

int SplitPolicyClientTriggered::decideDestWorker(uint32_t ino, FsReq *req) {
  // Simple round robin
  SPDLOG_DEBUG("Client Triggered split policy for inode {}", ino);
  int next_tid = static_cast<int>(cfsGetTid()) + 1;
  int num_threads = gFsProcPtr->getNumThreads();
  return next_tid % num_threads;
}

int SplitPolicyClientTriggeredServant::decideDestWorker(uint32_t, FsReq *req) {
  return FsProcWorker::kMasterWidConst;
}

int SplitPolicyAdHocAppGroupping::decideDestWorker(uint32_t ino, FsReq *req) {
  auto pid = req->getApp()->getPid();
  auto it = appDstMap.find(pid);
  int curAppIdx = 0;
  if (it == appDstMap.end()) {
    curAppIdx = currentAppCount++;
    appDstMap.emplace(pid, curAppIdx);
  } else {
    curAppIdx = it->second;
  }

  auto setIt = widSet_.begin();
  std::advance(setIt, curAppIdx % widSet_.size());
  return *setIt;
}

int SplitPolicyDynamicBasic::decideDestWorker(uint32_t ino, FsReq *req) {
#ifdef SCALE_USE_BUCKETED_DATA
  if (recvedRebalanceVid == finishedRebalanceVid) return wid_;
  if (localLa_.is_join_all) {
    // Deal with message type: JOINALL
    if (lastWindowUtilization > localLa_.cpu_allow) return wid_;
    // join back no matter the ino
    return localLa_.rwid;
  } else {
    // Deal with message type: ALLOC_SHARE
    if (needRebalance() && !isInSplitting()) {
      int lastIdx = req->getTargetInode()->lastWindowFinalAccessIdx;
      SPDLOG_INFO(
          "w:{} get valid allowance cid:{} allow:{} is_join_all:{} lastIdx:{} "
          "ino:{} vid:{}",
          wid_, localLa_.rwid, localLa_.cpu_allow, localLa_.is_join_all,
          lastIdx, req->getTargetInode()->i_no, localLa_.vid);
      // we have valid share to use, let's rebalance
      // fprintf(stdout, "SplitPolicy: lastIdx:%d\n", lastIdx);
      if (lastIdx >= 0 &&
          lastIdx < ((localLa_.cpu_allow * (INO_CYCLE_BUCKET_NUM)) - 1)) {
        return localLa_.rwid;
      }
    }  // needRebalance() && !isInSplitting
  }    // is_join_all

  // Unified way to give up this message or not
  // Even with JoinAll Message, there is a chance that, after LM makes the
  // decision, W_i suddenly receive bursty load.
  if (!isInSplitting() && recvedRebalanceVid != finishedRebalanceVid) {
    // fprintf(stderr, "wid:%d numDecisionRecved:%d\n", wid_,
    // numDecisionSinceRecvedRebalance);
    numDecisionSinceRecvedRebalance++;
    if (numDecisionSinceRecvedRebalance > kNumDecisionRebalanceLimit) {
      // we won't deal with this vid of rebalance anymore
      finishedRebalanceVid = recvedRebalanceVid;
      return -1000 + wid_;
    }
  }
#endif
  return wid_;
}

bool SplitPolicyDynamicBasic::ifAckLMInflightMsg(void *ctx,
                                                 FsProcMessageType &type,
                                                 bool isAbort) {
  // fprintf(stderr, "wid:%d ifAckLM? joinAll:%d isAbort:%d\n", wid_,
  //        localLa_.is_join_all, isAbort);
  if (localLa_.is_join_all) {
    auto worker = reinterpret_cast<FsProcWorker *>(ctx);
    if (worker->fileManager->getNumInodes() > 0) {
      // fprintf(stderr, " ifAvkLM return false numInode:%lu\n",
      //        worker->fileManager->getNumInodes());
      return false;
    }
    if (isAbort) {
      type = FsProcMessageType::LM_JOINALL_ACK_ABORT;
    } else {
      type = FsProcMessageType::LM_JOINALL_ACK;
    }
  } else {
    if (isAbort) {
      type = FsProcMessageType::LM_REBALANCE_ACK_ABORT;
    } else {
      type = FsProcMessageType::LM_REBALANCE_ACK;
    }
  }
  return true;
}

int SplitPolicyDynamicBasic::activateDestWorker(int dstWid) {
  // for dynamic policy, it's not worker's job to activate cores
  return 0;
}

void SplitPolicyDynamicBasic::beforeStatsResetCallback(
    worker_stats::AccessBucketedRecorder *recorder) {
  numInoAccessed = recorder->GetInoAccessedNum();
  lastWindowUtilization = recorder->GetWindowUtilization();
}

void SplitPolicyDynamicDstKnown::PerLoopCallback() {
  if (cur_allowance_effective_loop_count_down_ == 0) return;
  if ((--cur_allowance_effective_loop_count_down_) == 0 && num_ino_sent_ == 0) {
    // even though we don't finish, we ack to the LM
    allowance_ctx_->ack_done_allowance_cycles = total_done_cycles_;
    FsProcMessage msg;
    msg.ctx = allowance_ctx_;
    msg.type = FsProcMessageType::kLM_RebalanceAck;
    fprintf(stderr, "perlloopcallback kLM_RebalanceAck\n");
    fs_worker_->messenger->send_message_to_loadmonitor(wid_, msg);
    AllowanceAckDone();
  }
}

void SplitPolicyDynamicDstKnown::ReqCompletionCallback(FsReq *req,
                                                       InMemInode *inode) {
  if (cur_allowance_effective_loop_count_down_ == 0) return;
  auto app = req->getApp();
  int cur_tid = req->getTid();
  if (app->getPid() == allowance_ctx_->pid && cur_tid == allowance_ctx_->tid) {
    using FR = FileMng::ReassignmentOp;
    cfs_ino_t cur_ino = inode->i_no;
    // fprintf(stdout,
    //         "cur_ino:%u last_window_cycles:%lu numInProgs:%ld "
    //         "last_effect_cycles:%lu\n",
    //         cur_ino, inode->last_window_cycles_,
    //         fs_worker_->NumInProgressRequests(cur_ino),
    //         inode->last_effective_cycles_);
    if (((inode->last_effective_cycles_ > 0 &&
          inode->cur_stats_window_vid_ - inode->last_effective_vid_ < 100) ||
         inode->cur_window_cycles_ > 0) &&
        fs_worker_->NumInProgressRequests(cur_ino) == 0) {
      auto inode_account_cycles =
          inode->last_effective_cycles_ > inode->cur_window_cycles_
              ? inode->last_effective_cycles_
              : inode->cur_window_cycles_;
      bool do_split = AddDoneCycles(inode_account_cycles);
      if (!do_split) return;
      struct OldOwnerCtx {
        SplitPolicyDynamicDstKnown *policy;
        perfstat_cycle_t ino_last_cycles;
      };
      auto old_ctx = new OldOwnerCtx();
      old_ctx->policy = this;
      old_ctx->ino_last_cycles = inode_account_cycles;
      SPDLOG_INFO("[SPDDstKnown] wid:{} to_wid:{} ino:{} cycles:{}", wid_,
                  allowance_ctx_->dst_wid, inode->i_no,
                  old_ctx->ino_last_cycles);
      auto old_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                   void *myctx) {
        SPDLOG_DEBUG("ReqCompletionCallback old_owner");
        auto ctx = reinterpret_cast<OldOwnerCtx *>(myctx);
        auto cur_policy = ctx->policy;
        cur_policy->num_ino_sent_--;
        if (cur_policy->num_ino_sent_ == 0 &&
            cur_policy->allowance_ctx_ != nullptr) {
          FsProcMessage msg;
          msg.ctx = cur_policy->allowance_ctx_;
          msg.type = FsProcMessageType::kLM_RebalanceAck;
          auto messenger = mng->fsWorker_->messenger;
          fprintf(stderr,
                  "old_owner kLM_RebalanceAck cycles:%lu a_cycles:%lu "
                  "ct_down:%d num_ino:%d \n",
                  cur_policy->total_done_cycles_,
                  cur_policy->allowance_ctx_->allowance_cycles,
                  cur_policy->cur_allowance_effective_loop_count_down_,
                  cur_policy->num_ino_sent_);
          messenger->send_message_to_loadmonitor(mng->fsWorker_->getWid(), msg);
          cur_policy->AllowanceAckDone();
        }
        delete ctx;
      };
      struct NewOwnerCtx {
        InMemInode *inode_ptr;
      };
      auto new_ctx = new NewOwnerCtx();
      new_ctx->inode_ptr = inode;
      auto new_owner_callback = [](FileMng *mng, const FR::Ctx *reassign_ctx,
                                   void *myctx) {
        SPDLOG_DEBUG("ReqCompletionCallback new_owner");
        auto ctx = reinterpret_cast<NewOwnerCtx *>(myctx);
        auto inode_ptr = ctx->inode_ptr;
        mng->fsWorker_->stats_recorder_.ResetInodeStatsOnSplitJoin(inode_ptr);
        ctx->inode_ptr = nullptr;
        delete ctx;
      };
      FR::OwnerExportThroughPrimary(fs_worker_->fileManager, cur_ino,
                                    allowance_ctx_->dst_wid, old_owner_callback,
                                    old_ctx, new_owner_callback, new_ctx);
      num_ino_sent_++;
    }
  }
}
