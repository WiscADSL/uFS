#ifndef CFS_WORKER_STATS_H
#define CFS_WORKER_STATS_H

#include "FsProc_FsReq.h"
#include "FsProc_KnowParaLoadMng.h"
#include "FsProc_LoadMng.h"
#include "perfutil/Cycles.h"
#include "typedefs.h"

#include <array>

class SplitPolicy;

namespace worker_stats {

// 2.9GHZ computer --> 1000us
// each interval, worker will check the atomic variable that
// is set by the LoadManager
constexpr static int kCheckResetIntervalCycles = 2000000;

// #define SCALE_USE_BUCKETED_DATA
#define SCALE_USE_ATOMIC_DATA

class AccessBucketedRecorder;
class ParaAwareRecorder;

#define ASSERT_STATS_TYPE_NOT_VALID \
  static_assert(false, "invalid load stats types");

#if defined SCALE_USE_BUCKETED_DATA
using RecorderType = AccessBucketedRecorder;
using LoadMngType = LoadManager;
#elif defined SCALE_USE_ATOMIC_DATA
using RecorderType = ParaAwareRecorder;
using LoadMngType = fsp_lm::KPLoadManager;
#else
ASSERT_STATS_TYPE_NOT_VALID
#endif

static perfstat_ts_t inline TickTs() { return PerfUtils::Cycles::rdtsc(); }

struct LoopEffectiveStats {
  void AccountLoopEffective(bool is_effective, perfstat_ts_t ts) {
    if (is_effective) {
      this_window_effect_num++;
      if (!last_loop_is_effective) {
        // from idle to effective
        last_loop_is_effective = true;
        total_idle_cycles += (ts - idle_start_ts);
      }
    } else {
      if (last_loop_is_effective) {
        // from effective to idle
        idle_start_ts = ts;
        last_loop_is_effective = false;
      }
    }
  }

  void Reset(perfstat_ts_t ts) {
    // reset conditioning variables
    last_reset_ts = ts;
    this_window_effect_num = 0;

    // adjust timeline
    idle_start_ts = ts;
    total_idle_cycles = 0;
  }

  perfstat_ts_t idle_start_ts = 0;
  perfstat_ts_t last_reset_ts = 0;
  perfstat_cycle_t total_idle_cycles = 0;
  bool last_loop_is_effective = false;
  uint32_t this_window_effect_num = 0;
};

class AccessBucketedRecorder {
 public:
  static std::list<InMemInode *>::iterator GetGlobalDummyIter() {
    return dummy_list_.begin();
  }

  explicit AccessBucketedRecorder(int wid, void *extra_data_ptr) : wid_(wid) {
    memset(&(local_sr_copy_), 0, sizeof(fsp_lm::PerWorkerLoadStatsSR));
    version_ = 1;
    local_sr_copy_.version = version_;
    sr_shared_with_lm_ =
        reinterpret_cast<fsp_lm::PerWorkerLoadStatsSR *>(extra_data_ptr);
  }

  void SetWorker(FsProcWorker *worker) {
    assert(worker_ptr_ == nullptr && worker != nullptr);
    worker_ptr_ = worker;
  }

  // update stats in different situations
  void RecordLoopEffective(bool is_effective, perfstat_ts_t ts,
                           SplitPolicy *split_policy);
  void RecordOnFsReqCompletion(FsReq *req, void *wk_policy_ptr);
  void ResetStats(perfstat_ts_t ts);
  void ResetInodeStatsOnSplitJoin(InMemInode *inode);

  // getters
  int32_t GetInoAccessedNum() { return num_ino_; }
  double GetWindowUtilization() {
    return local_sr_copy_.lastResetWindowUtilization;
  }
  uint64_t GetVersion() { return version_; }

  // Used by the load manager
  // i.e., different threads other than above ones
  void ReadFromLoadMng(void *ctx) {}
  void PushLoadStatFromLocalToSR(perfstat_ts_t ts);

 private:
  int wid_ = 0;
  uint64_t version_ = 0;
  FsProcWorker *worker_ptr_{nullptr};

  LoopEffectiveStats lef_;

  // a local copy of LoadStatsSR
  // we will always make a copy to the loadStatsSR that is visible
  // to the monitoring thread
  fsp_lm::PerWorkerLoadStatsSR local_sr_copy_;
  fsp_lm::PerWorkerLoadStatsSR *sr_shared_with_lm_{nullptr};

  std::array<std::list<InMemInode *>, INO_CYCLE_BUCKET_NUM>
      on_cpu_cycle_bucketed_inodes_;
  static std::list<InMemInode *> dummy_list_;
  int num_ino_;

  void ConvertToAccessNumBucketArr(uint16_t *arr) {
    for (int i = 0; i < INO_CYCLE_BUCKET_NUM; i++) {
      arr[i] = static_cast<uint16_t>(on_cpu_cycle_bucketed_inodes_[i].size());
    }
  }
  void AddInodeAccessCycles(InMemInode *inode, perfstat_cycle_t cycles);
};

class ParaAwareRecorder {
 public:
  using worker_app_map_t = const std::unordered_map<pid_t, AppProc *>;
  constexpr static bool kEnable = true;

  explicit ParaAwareRecorder(int wid, uintptr_t extra_data_ptr) : wid_(wid) {}
  void SetWorker(FsProcWorker *worker);

  // update stats in different situations
  void RecordLoopEffective(bool is_effective, perfstat_ts_t ts,
                           SplitPolicy *split_policy);
  void inline RecordRecvQueuingStats(int qlen_agg, int rm_share_qlen_agg);
  void RecordOnFsReqCompletion(FsReq *req, void *wk_policy_ptr);
  void ResetStats(perfstat_ts_t ts);
  void ResetInodeStatsOnSplitJoin(InMemInode *inode);

  // getters
  int32_t GetInoAccessedNum() { return 0; }
  double GetWindowUtilization() { return 0; }
  uint64_t GetVersion() { return version_; }
  KPLoadStatsComb *GetStatsCombPtr() { return &shared_stats_comb_; }

 private:
  int wid_ = 0;
  uint64_t version_ = 0;
  perfstat_ts_t last_check_ts = 0;
  perfstat_ts_t last_push_and_reset_ts = 0;

  // local stats for overall utilization
  LoopEffectiveStats lef_;
  // worker local queue stats
  KPLoadQueueStats wlocal_queue_stats_;
  KPLoadRecvQueueStats wlocal_recv_queuing_stats_;
#ifdef KP_DISTINCT_SHARED_FILES
  // local stats for shared file cpu utilization
  std::map<cfs_ino_t, InMemInode *> shared_files_;
#endif
  // read-only pointer worker's appMap
  worker_app_map_t *app_map_ptr_{nullptr};
  FsProcWorker *worker_ptr_{nullptr};

  KPLoadStatsComb shared_stats_comb_;

  void PushAllStatsToSharedData(perfstat_ts_t ts);
  void ResetAllAppExclusiveData();
  void AddInodeAccessCycles(InMemInode *inode, FsReq *req);
};

}  // namespace worker_stats

void worker_stats::ParaAwareRecorder::RecordRecvQueuingStats(
    int qlen_agg, int rm_share_qlen_agg) {
  wlocal_recv_queuing_stats_.recv_qlen_agg += qlen_agg;
  wlocal_recv_queuing_stats_.recv_qlen_rm_share_agg += rm_share_qlen_agg;
}

#endif
