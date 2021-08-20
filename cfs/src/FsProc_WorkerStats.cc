#include "FsProc_WorkerStats.h"
#include "FsProc_Fs.h"
#include "FsProc_SplitPolicy.h"

#include <cassert>
#include <tuple>

static std::list<InMemInode *> InitGlobalInodeAccessRecorder() {
  std::list<InMemInode *> tmp;
  tmp.push_back(nullptr);
  return tmp;
}

std::list<InMemInode *> worker_stats::AccessBucketedRecorder::dummy_list_(
    InitGlobalInodeAccessRecorder());

void worker_stats::AccessBucketedRecorder::RecordLoopEffective(
    bool is_effective, perfstat_ts_t ts, SplitPolicy *split_policy) {
  lef_.AccountLoopEffective(is_effective, ts);
  if (ts - lef_.last_reset_ts > kPerWorkerLoadStatsResetWindowCycle) {
    PushLoadStatFromLocalToSR(ts);
    split_policy->beforeStatsResetCallback(this);
    ResetStats(ts);
  }
}

void worker_stats::AccessBucketedRecorder::RecordOnFsReqCompletion(
    FsReq *req, void *wk_policy_ptr) {
  auto tp = req->getType();
  if (tp == FsReqType::SYNCALL || tp >= FsReqType::_LANDMARK_ADMIN_OP_) {
    // SYNCALL is supposed to be out of critical path
    // eliminate ADMIN_OP to focus on FS ops
    return;
  }

  // Mainly, this is to avoid the *SPLIT_ERROR* request contribute to
  // the stats and result in statsReset
  // E.g., stat(), migrate, then , this request is accounted into the old worker
  if (req->hasError()) {
    return;
  }

  int opIdx = KPrimOpIdx;
  if (FsProcWorkerServant::isFsReqServantCapable(tp)) {
    auto inode = req->getTargetInode();
    // we only account for the access # for data operations
    if (inode != nullptr) {
      AddInodeAccessCycles(inode, req->retrieveReqOnCpuCycles());
    }

    // it is an op that account to secondary type
    opIdx = KSecOpIdx;
  }
  local_sr_copy_.opTotalEnqueueReadyCnt[opIdx] += req->GetReadyEnqueCnt();
  local_sr_copy_.numCommonReqDone++;
  local_sr_copy_.readyQueueLenAccumulate += req->GetQLenAccumulate();
}

void worker_stats::AccessBucketedRecorder::ResetStats(perfstat_ts_t ts) {
  assert(version_ == local_sr_copy_.version);
  memset(&(local_sr_copy_), 0, sizeof(fsp_lm::PerWorkerLoadStatsSR));
  // increment version
  local_sr_copy_.version = (++version_);
  // reset conditioning variables
  lef_.last_reset_ts = ts;
  lef_.this_window_effect_num = 0;

  lef_.Reset(ts);

  if (num_ino_ > 0) {
    for (int i = 0; i < INO_CYCLE_BUCKET_NUM; i++) {
      on_cpu_cycle_bucketed_inodes_[i].clear();
    }
  }

  num_ino_ = 0;
}

void worker_stats::AccessBucketedRecorder::ResetInodeStatsOnSplitJoin(
    InMemInode *inode) {
#ifdef SCALE_USE_BUCKETED_DATA
  // fprintf(stderr, "resetInodeAccessRecordOnSplitJoin ino:%u\n",
  // inode->i_no);
  if (inode->accessRecorderIter != GetGlobalDummyIter()) {
    if (inode->accessRecordVid == version_) {
      int idx = inode->curWindowAccessIdx;
      on_cpu_cycle_bucketed_inodes_[idx].erase(inode->accessRecorderIter);
    }
    inode->accessRecorderIter = GetGlobalDummyIter();
    inode->accessRecordVid = 0;
    inode->lastWindowFinalAccessIdx = -1;
    inode->curWindowAccessIdx = -1;
  }
#endif
}

void worker_stats::AccessBucketedRecorder::AddInodeAccessCycles(
    InMemInode *inode, perfstat_cycle_t cycles) {
#ifdef SCALE_USE_BUCKETED_DATA
  int oldIdx = -1;
  if (inode->accessRecordVid < version_) {
    // The accessing stats are just reset and this is the first time
    // in this window this inode is accessed
    inode->accessRecorderCycles = 0;
    inode->accessRecordVid = version_;
    inode->lastWindowFinalAccessIdx = inode->curWindowAccessIdx;
    num_ino_++;
  } else {
    oldIdx = inode->curWindowAccessIdx;
  }

  inode->accessRecorderCycles += cycles;
  int idx = calcInoCycleBucketIdx(inode->accessRecorderCycles);

  if (oldIdx != idx) {
    if (oldIdx >= 0) {
      assert(inode->accessRecorderIter != GetGlobalDummyIter());
      on_cpu_cycle_bucketed_inodes_[oldIdx].erase(inode->accessRecorderIter);
    }
    inode->curWindowAccessIdx = idx;
    on_cpu_cycle_bucketed_inodes_[idx].push_front(inode);
    inode->accessRecorderIter = (on_cpu_cycle_bucketed_inodes_[idx]).begin();
  }
#endif
}

void worker_stats::AccessBucketedRecorder::PushLoadStatFromLocalToSR(
    perfstat_ts_t ts) {
  // calculate the utilization
  if (lef_.this_window_effect_num == 0 && lef_.total_idle_cycles == 0) {
    local_sr_copy_.lastResetWindowUtilization = 0;
  } else {
    local_sr_copy_.lastResetWindowUtilization =
        1.0 - float(lef_.total_idle_cycles) / (ts - lef_.last_reset_ts);
  }
  ConvertToAccessNumBucketArr(local_sr_copy_.inoCycleCntArr);
  local_sr_copy_.numCommonAccessedIno = num_ino_;
  // push the data to SR by memcpy
  memcpy(sr_shared_with_lm_, &(local_sr_copy_),
         sizeof(fsp_lm::PerWorkerLoadStatsSR));
  __sync_synchronize();
}

void worker_stats::ParaAwareRecorder::SetWorker(FsProcWorker *worker) {
  assert(worker_ptr_ == nullptr && worker != nullptr);
  worker_ptr_ = worker;
  app_map_ptr_ = &(worker->appMap);
}

// only on recordLoopEffective we have the chance to also update
// and clean data in an idle loop
void worker_stats::ParaAwareRecorder::RecordLoopEffective(
    bool is_effective, perfstat_ts_t ts, SplitPolicy *split_policy) {
  if (!kEnable) return;
  if (ts - last_check_ts > worker_stats::kCheckResetIntervalCycles) {
    if (shared_stats_comb_.reset_ts == 0) {
      PushAllStatsToSharedData(ts);
      // here is a heavy-weight atomic store
      shared_stats_comb_.reset_ts.store(ts);
      last_push_and_reset_ts = ts;
      // clear local data
      ResetStats(ts);
    }
    last_check_ts = ts;
  }
  lef_.AccountLoopEffective(is_effective, ts);
}

void worker_stats::ParaAwareRecorder::RecordOnFsReqCompletion(
    FsReq *req, void *wk_policy_ptr) {
  if (!kEnable) return;
  if (req->hasError()) {
    return;
  }
  auto flags = req->getReqTypeFlags();
  if (flags & FsReqFlags::client_control_plane) {
    return;
  }
  InMemInode *inode = nullptr;
  // TODO (jingliu): revist this?
  // auto cur_req_type = req->getType();
  if (flags & FsReqFlags::handled_by_owner) {
    // if (!(cur_req_type == FsReqType::OPEN && wid_ == 0)) {
    inode = req->getTargetInode();
    assert(inode != nullptr);
    AddInodeAccessCycles(inode, req);
    wlocal_queue_stats_.splittable_done_cnt += 1;
    if (wk_policy_ptr != nullptr) {
      auto policy =
          reinterpret_cast<SplitPolicyDynamicDstKnown *>(wk_policy_ptr);
      policy->ReqCompletionCallback(req, inode);
    }
    //}
  } else {
    wlocal_queue_stats_.primary_only_done_cnt += 1;
  }
  wlocal_queue_stats_.num_ready_qlen_accumulate += req->GetQLenAccumulate();
  // Q: why I put in this way, only need to do this when Push
  // shared_stats_comb_.q_stats = (wlocal_queue_stats_);
}

void worker_stats::ParaAwareRecorder::ResetStats(perfstat_ts_t ts) {
  if (!kEnable) return;
  version_++;
  lef_.Reset(ts);
  wlocal_queue_stats_.Reset();
  wlocal_recv_queuing_stats_.Reset();
  ResetAllAppExclusiveData();
#ifdef KP_DISTINCT_SHARED_FILES
  shared_files_.clear();
#endif
}

// Worker thread
void worker_stats::ParaAwareRecorder::ResetInodeStatsOnSplitJoin(
    InMemInode *inode) {
  if (!kEnable) return;
#ifdef SCALE_USE_ATOMIC_DATA
  inode->cur_stats_window_vid_ = 0;
  inode->cur_window_cycles_ = 0;
  inode->cur_stats_window_accessed_taus_.clear();
  inode->last_stats_window_vid_ = 0;
  inode->last_window_cycles_ = 0;
  inode->last_window_num_tau = 0;
  inode->last_effective_cycles_ = 0;
  inode->last_effective_vid_ = 0;
#endif
}

void worker_stats::ParaAwareRecorder::ResetAllAppExclusiveData() {
  if (!kEnable) return;
  const worker_app_map_t &app_map(*app_map_ptr_);
  for (auto [pid, app_ptr] : app_map) {
    app_ptr->ResetExclusiveCycles();
#ifdef KP_SPECIAL_NUM_CREATE
    app_ptr->ResetNumCreates();
#endif
  }
}

void worker_stats::ParaAwareRecorder::PushAllStatsToSharedData(
    perfstat_ts_t ts) {
  if (!kEnable) return;
#ifdef SCALE_USE_ATOMIC_DATA
#ifdef KP_DISTINCT_SHARED_FILES
  // shared files
  for (auto cur_pair : shared_files_) {
    shared_stats_comb_.shared_file_cpu_cycles.push_back(
        {cur_pair.first, cur_pair.second->cur_window_cycles_});
  }
#endif
  // app and their exclusive files
  const worker_app_map_t &app_map(*app_map_ptr_);
  for (const auto [pid, app_ptr] : app_map) {
    for (const auto [tau_id, cur_cycles] : app_ptr->GetPerThreadCycleMap()) {
      if (cur_cycles > 0) {
        shared_stats_comb_.tau_exclusive_cycles.push_back(
            {pid, tau_id, cur_cycles});
      }
    }
    if (wid_ == FsProcWorker::kMasterWidConst) {
#ifdef KP_SPECIAL_NUM_CREATE
      for (const auto [tau_id, num_creates] :
           app_ptr->GetPerThreadNumCreateMap()) {
        assert(num_creates > 0);
        shared_stats_comb_.tau_num_create.push_back({pid, tau_id, num_creates});
      }
#endif
    }
  }
  // overall stats
  if (!lef_.last_loop_is_effective) {
    lef_.total_idle_cycles += ts - lef_.idle_start_ts;
    shared_stats_comb_.idle_cycles = lef_.total_idle_cycles;
  } else {
    shared_stats_comb_.idle_cycles = lef_.total_idle_cycles;
  }
  if (lef_.total_idle_cycles > ts - last_push_and_reset_ts &&
      last_push_and_reset_ts != 0) {
    throw std::runtime_error("cycles not valid");
  }
  // queue stats
  shared_stats_comb_.q_stats = wlocal_queue_stats_;
  shared_stats_comb_.recv_queuing_stats = wlocal_recv_queuing_stats_;
#endif
}

void worker_stats::ParaAwareRecorder::AddInodeAccessCycles(InMemInode *inode,
                                                           FsReq *req) {
  if (!kEnable) return;
#ifdef SCALE_USE_ATOMIC_DATA
  auto cur_app = req->getApp();
  auto pid = cur_app->getPid();
  auto tid = req->getTid();
  auto cur_req_cycles = req->retrieveReqOnCpuCycles();
  auto req_type = req->getType();
  if (req_type == FsReqType::FDATA_SYNC) {
    cur_req_cycles *= 2;
  }
  if (req_type == FsReqType::WSYNC) {
    cur_req_cycles *= 8;
  }
  uint64_t pid_tid_unify = AssembleTwo32B(pid, tid);
  if (inode->cur_stats_window_vid_ < version_) {
    // this inode is accessed first time in this window
    // we clear the old data and set the vid
    inode->last_stats_window_vid_ = inode->cur_stats_window_vid_;
    inode->last_window_num_tau = inode->cur_stats_window_accessed_taus_.size();
    inode->last_window_cycles_ = inode->cur_window_cycles_;
    if (inode->last_window_cycles_ > 0) {
      inode->last_effective_cycles_ = inode->last_window_cycles_;
      inode->last_effective_vid_ = inode->last_stats_window_vid_;
    }
    inode->cur_stats_window_vid_ = version_;
    inode->cur_window_cycles_ = 0;
    inode->cur_stats_window_accessed_taus_.clear();
    inode->cur_stats_window_accessed_taus_.insert(pid_tid_unify);
  } else {
    auto it = inode->cur_stats_window_accessed_taus_.find(pid_tid_unify);
    if (it == inode->cur_stats_window_accessed_taus_.end()) {
      inode->cur_stats_window_accessed_taus_.insert(pid_tid_unify);
    }
  }  // version_

#ifdef KP_DISTINCT_SHARED_FILES
  const worker_app_map_t &app_map(*app_map_ptr_);
  if (inode->cur_stats_window_accessed_taus_.size() > 1) {
    // it becomes a shared inode
    if (inode->cur_stats_window_accessed_taus_.size() == 2) {
      shared_files_.emplace(inode->i_no, inode);
      for (auto pid_tid : inode->cur_stats_window_accessed_taus_) {
        if (pid_tid == pid_tid_unify) continue;
        int tmp_pid, tmp_tid;
        DessembleOne64B(pid_tid, tmp_pid, tmp_tid);
        auto it = app_map.find(tmp_pid);
        if (it != app_map.end()) {
          auto tmp_app_ptr = it->second;
          tmp_app_ptr->SubstractExclusiveCycles(tmp_tid,
                                                inode->cur_window_cycles_);
        }
      }
    }
    cur_app->AddTauWaitingNano(tid, );
  } else {
    // it is an exclusive inode
    cur_app->AddTauExclusiveCycles(tid, cur_req_cycles);
  }
#else
  cur_app->AddTauExclusiveCycles(tid, cur_req_cycles);
#endif  // KP_DISTINCT_SHARED_FILES
  // always record the request to the target inode
  // NOTE: for those inodes that includes the metadata operation
  // e.g., lookup(), find the datablocks, which includes the operations
  // on the parent directory's operation, we still include that time
  // into this leaf inode
  inode->cur_window_cycles_ += cur_req_cycles;
#endif  // SCALE_USE_ATOMIC_DATA
}
