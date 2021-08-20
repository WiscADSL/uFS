#ifndef CFS_KNOWPARA_LOAD_MNG_H
#define CFS_KNOWPARA_LOAD_MNG_H

#include "FsProc_LoadMngCommon.h"
#include "FsProc_Messenger.h"
#include "fs_defs.h"
#include "typedefs.h"
#include "util.h"

#include "absl/container/flat_hash_map.h"
#include "perfutil/Cycles.h"

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

// Don't want to include FsProc_Fs.h
constexpr static int kMasterWid = 0;

struct LmWorkerFutureRoutingTable {
  using OneTauDstHistory = std::map<uint64_t, int>;
  // <tid, round-id:dst>
  using TidDstHistoryMap = std::map<int, OneTauDstHistory>;
  std::map<pid_t, TidDstHistoryMap> app_thread_dst_map;
  // current dst workers
  std::map<int, std::set<uint64_t>> dst_workers;
  std::set<uint64_t> empty_;
  OneTauDstHistory empty_history_;

  void AddWid(int wid) {
    auto it = dst_workers.find(wid);
    if (it != dst_workers.end()) dst_workers[wid] = {};
  }

  const std::set<uint64_t> &GetWidTaus(int wid) {
    auto it = dst_workers.find(wid);
    if (it == dst_workers.end()) return empty_;
    return it->second;
  }

  // retrieve the latest dst wid
  int GetTauCurDstWid(pid_t pid, int tid) {
    auto it = app_thread_dst_map.find(pid);
    if (it == app_thread_dst_map.end()) {
      return -1;
    }
    auto &tid_dst_map = it->second;
    auto it2 = tid_dst_map.find(tid);
    if (it2 == tid_dst_map.end()) return -1;
    OneTauDstHistory &his_map = it2->second;
    return his_map.crbegin()->second;
  }

  const OneTauDstHistory &GetTauHistory(pid_t pid, int tid) {
    auto it = app_thread_dst_map.find(pid);
    if (it == app_thread_dst_map.end()) {
      return empty_history_;
    }
    auto &tid_dst_map = it->second;
    auto it2 = tid_dst_map.find(tid);
    if (it2 == tid_dst_map.end()) return empty_history_;
    return it2->second;
  }

  void InitTauToMaster(pid_t pid, int tid, uint64_t key, uint64_t round_id) {
    app_thread_dst_map[pid][tid] = {{round_id - 1, kMasterWid}};
    dst_workers[kMasterWid].emplace(key);
  }

  int UpdateTauDstWid(pid_t pid, int tid, uint64_t key, int from, int to,
                      uint64_t round_id) {
    auto cur_app_map = app_thread_dst_map[pid];
    auto it = cur_app_map.find(tid);
    if (it == cur_app_map.end()) return -1;
    int orig_dst = it->second.crbegin()->second;
    if (orig_dst != from) return -1;
    dst_workers[orig_dst].erase(key);
    // do deduplicate
    OneTauDstHistory &his_map = it->second;
    auto his_it = his_map.begin();
    while (his_it != his_map.end()) {
      if (his_it->second == to) {
        his_map.erase(his_it++);
      } else {
        ++his_it;
      }
    }
    app_thread_dst_map[pid][tid].emplace(round_id, to);
    dst_workers[to].emplace(key);
    return 0;
  }
};

struct LmWorkerCreationRoutingTable {
  using TidDstMap = std::map<int, std::pair<int, uint64_t>>;
  // <pid, <tid, <dst, round_id>>
  std::map<pid_t, TidDstMap> app_thread_dst_map;
  // <wid, tau_id>
  std::map<int, std::set<uint64_t>> dst_workers;

  void AddWid(int wid) {
    auto it = dst_workers.find(wid);
    if (it != dst_workers.end()) {
      dst_workers[wid] = {};
    }
  }

  void EraseWid(int wid) {
    auto it = dst_workers.find(wid);
    if (it == dst_workers.end()) return;
    pid_t pid;
    int tid;
    for (auto pid_tid : it->second) {
      DessembleOne64B(pid_tid, pid, tid);
      auto pid_it = app_thread_dst_map.find(pid);
      assert(pid_it != app_thread_dst_map.end());
      pid_it->second.erase(tid);
      if (pid_it->second.empty()) {
        app_thread_dst_map.erase(pid_it);
      }
    }
    dst_workers.erase(it);
  }

  // We consider this pid:tid as doing creation in this round
  // and thus update the round_id
  // By constract, if round_id is 0 (default), regard it as
  // query this info for other purpose
  // E.g., when do rebalancing, we want to keep one thread's
  // file at the same core
  int GetDstWid(pid_t pid, int tid, uint64_t round_id = 0) {
    auto it = app_thread_dst_map.find(pid);
    if (it == app_thread_dst_map.end()) {
      return -1;
    }
    auto &tid_dst_map = it->second;
    auto it2 = tid_dst_map.find(tid);
    if (it2 == tid_dst_map.end()) return -1;
    auto &cur_pair = it2->second;
    if (round_id != 0) cur_pair.second = round_id;
    return cur_pair.first;
  }

  void UpdateDstWid(pid_t pid, int tid, uint64_t key, int wid,
                    uint64_t round_id) {
    auto cur_app_map = app_thread_dst_map[pid];
    auto it = cur_app_map.find(tid);
    if (it != cur_app_map.end()) {
      int orig_dst = it->second.first;
      cur_app_map.erase(it);
      dst_workers[orig_dst].erase(key);
    }
    app_thread_dst_map[pid][tid] = {wid, round_id};
    dst_workers[wid].emplace(key);
  }

  auto GetActiveTausInDstWithinNumRound(int wid, int cur_round_id,
                                        int num_round) {
    std::vector<uint64_t> active_taus;
    auto wid_it = dst_workers.find(wid);
    if (wid_it == dst_workers.end()) return active_taus;
    int pid, tid;
    for (auto tau_id : wid_it->second) {
      DessembleOne64B(tau_id, pid, tid);
      auto it1 = app_thread_dst_map.find(pid);
      if (it1 == app_thread_dst_map.end())
        throw std::runtime_error("GetNumDstWithin");
      auto it2 = it1->second.find(tid);
      if (it2 == it1->second.end())
        throw std::runtime_error("GetNumDstWithin it2");
      if (it2->second.first != wid) {
        throw std::runtime_error("GetNumDstWithin cannot match wid");
      }
      if (int(cur_round_id - it2->second.second) <= num_round) {
        active_taus.push_back(tau_id);
      }
    }
    return active_taus;
  }

  size_t GetNumDstInWid(int wid) {
    auto it = dst_workers.find(wid);
    if (it == dst_workers.end()) return 0;
    return (it->second).size();
  }

  auto GetNumDst() { return dst_workers.size(); }
};

struct KPLoadQueueStats {
  void Reset() {
    primary_only_done_cnt = 0;
    splittable_done_cnt = 0;
    num_ready_qlen_accumulate = 0;
  }
  uint16_t primary_only_done_cnt = 0;
  uint16_t splittable_done_cnt = 0;
  uint32_t num_ready_qlen_accumulate = 0;
};

struct KPLoadRecvQueueStats {
  void Reset() {
    recv_qlen_agg = 0;
    recv_qlen_rm_share_agg = 0;
  }
  int recv_qlen_agg = 0;
  int recv_qlen_rm_share_agg = 0;
};

struct KPLoadStatsSummary {
  KPLoadStatsSummary(int w, perfstat_ts_t ts) : wid(w), summary_ts(ts) {}
  KPLoadStatsSummary(int w) : wid(w), is_imagine(true) {}

  int wid;
  bool is_imagine = false;

  perfstat_ts_t summary_ts = 0;
  perfstat_ts_t last_reset_ts = 0;
  // directly fetch from shared data
  perfstat_ts_t comb_reset_ts;
  perfstat_cycle_t comb_idle_cycles;
  KPLoadQueueStats comb_q_stats;
  KPLoadRecvQueueStats comb_recv_queuing_stats;

  // computed by load manager it self
  float avg_recv_qlen = 0;
  float recv_qlen = 0;
  float recv_noshare_qlen = 0;
  float cpu_ut = 0;

  // max cpu utilization across all taus
  uint64_t max_ut_thread = 0;
  float max_ut = 0;
  // min cpu utilization across all taus
  uint64_t min_ut_thread = 0;
  float min_ut = 1;
  float total_ut = 0;

  // [[deprecated]]
  int64_t total_wait_nano = 0;

  // pid:tid, pair<utilization, cycles>
  std::map<uint64_t, std::pair<float, uint64_t>> tau_utilization;
  std::map<uint64_t, int> tau_num_creates;
  // [[deprecated]]
  std::map<uint64_t, int> tau_wait_nano;

  double est_include_wait_cycles = 0;
  double useful_window_cycles = 0;
  perfstat_cycle_t window_cycles = 0;

#ifdef KP_DISTINCT_SHARED_FILES
  cfs_ino_t max_share_ino = 0;
  float max_share_ut = 0;
  float total_share_ut = 0;
  std::vector<std::tuple<cfs_ino_t, float, uint64_t>> shared_file_cpu_cycles;
#endif

  template <typename OStream>
  friend OStream &operator<<(OStream &os, const KPLoadStatsSummary &c) {
    os << "[KPLoadStatsSummary]"
       << " wid:" << c.wid
       << " real_nano:" << PerfUtils::Cycles::toNanoseconds(c.summary_ts)
       << " last_ts:" << c.last_reset_ts << " comb_reset_ts:" << c.comb_reset_ts
       << " comb_idle_cycles:" << c.comb_idle_cycles
       << " num_rq_ac:" << c.comb_q_stats.num_ready_qlen_accumulate
       << " primary_cnt:" << c.comb_q_stats.primary_only_done_cnt
       << " splittable_cnt:" << c.comb_q_stats.splittable_done_cnt
       << " avg_recv_qlen:" << c.avg_recv_qlen << " cpu_ut:" << c.cpu_ut
       << " recv_ql:" << c.recv_qlen << " recv_ns_ql:" << c.recv_noshare_qlen
#ifdef KP_DISTINCT_SHARED_FILES
       << " num_shared:" << c.shared_file_cpu_cycles.size()
       << " share_ut:" << c.total_share_ut;
#else
        ;
#endif
    os << std::endl;
    os << "[KPLoadStatsSummary] [map]-tau_utilization num_tid:"
       << c.tau_utilization.size();
    for (const auto &it : c.tau_utilization) {
      auto key = it.first;
      pid_t pid;
      int tid;
      DessembleOne64B(key, pid, tid);
      // int num_creates = 0;
      // int wait_nano = 0;
#ifdef KP_SPECIAL_NUM_CREATE
      auto ncrt_it = c.tau_num_creates.find(key);
      if (ncrt_it != c.tau_num_creates.end()) {
        num_creates = ncrt_it->second;
      }
#endif
      // auto wait_nano_it = c.tau_wait_nano.find(key);
      // if (wait_nano_it != c.tau_wait_nano.end()) {
      //   wait_nano = wait_nano_it->second;
      // }
      os << " | pid:" << pid << " tid:" << tid << " ut:" << it.second.first
         << " cycles:" << it.second.second
#ifdef KP_SPECIAL_NUM_CREATE
         << " ncrt:" << num_creates
#endif
          //  << " wt_nano:" << wait_nano;
          ;
    }
    os << "\n";
    return os;
  }
};

struct KPLoadStatsComb {
  KPLoadStatsComb() {}
  // W_i write to this when it finds reset_ts is 0
  // Meanwhile, it will reset its local copy of any data
  // NOTE: try to make sure w_i only need to write to atomic variables
  // but not read-then-modify
  // This variable is used to synchronize writer/reader for this shared data
  std::atomic<perfstat_ts_t> reset_ts{0};
  // W_i will have a local copy on that to avoid read-copy-write every time
  perfstat_cycle_t idle_cycles;
  KPLoadQueueStats q_stats;
  KPLoadRecvQueueStats recv_queuing_stats;
  // tuple<pid,tid,on cpu cycles for exclusive files>
  std::vector<std::tuple<int, int, perfstat_cycle_t>> tau_exclusive_cycles;
#ifdef KP_SPECIAL_NUM_CREATE
  // tuple<pid,tid, number of file created>
  std::vector<std::tuple<int, int, int>> tau_num_create;
#endif
#ifdef KP_DISTINCT_SHARED_FILES
  std::vector<std::pair<cfs_ino_t, perfstat_cycle_t>> shared_file_cpu_cycles;
#endif
};

struct LmMsgRedirectOneTauCtx {
  enum class EffectiveTimeRange {
    kFutureAndPast = FS_REASSIGN_ALL,
    kPast = FS_REASSIGN_PAST,
    kFuture = FS_REASSIGN_FUTURE,
  };
  int src_wid;
  int dst_wid;
  int recv_wid;
  pid_t pid;
  int tid;
  EffectiveTimeRange time_range;
};

struct LmMsgRedirectFileSetCtx {
  int src_wid;
  int dst_wid;
  int recv_wid;
  int pid;
  int tid;
  float allowance;
  // float ack_done_allowance;
  perfstat_cycle_t allowance_cycles;
  perfstat_cycle_t ack_done_allowance_cycles;
};

struct LmMsgRedirectFutureCtx {
  int src_wid;
  int dst_wid;
  int recv_wid;
  pid_t pid;
  int tid;
  // pid, tid, <dst, pct>
  std::map<pid_t, std::map<int, std::map<int, float>>> plan_map;
};

struct LmMsgRedirectCreationCtx {
  int src_wid;
  int dst_wid;
  int recv_wid;
  int pid;
  int tid;
};

struct LmMsgJoinAllCtx {
  int src_wid;
  int dst_wid;
  int recv_wid;
  int num_join;
};

struct LmMsgJoinAllCreationCtx {
  int src_wid;
  int dst_wid;
  int recv_wid;
};

namespace fsp_lm {

struct LmMemLessRoutingTable {
  void UpdateItems(
      const std::map<pid_t, std::map<int, std::map<int, float>>> &items);
  void GetWidHandling(int dst_wid, std::map<pid_t, std::set<int>> &tids);
  void Print(std::ostream &out);

  static void PrintGottenTids(int dst_wid,
                              const std::map<pid_t, std::set<int>> &tids,
                              std::ostream &out);

  // <tau, <dst_wid, pct>>
  std::map<uint64_t, std::map<int, float>> src_routing_map;
  // wid, taus
  std::map<int, std::set<uint64_t>> dst_handling_taus;
};

using wrt_tb = LmWorkerCreationRoutingTable;
using ftb = LmWorkerFutureRoutingTable;

struct LBPlanItem {
  ~LBPlanItem() { ClearDstDesc(); }
  enum Type {
    ONE_TAU_ALL,
    ONE_TAU_PCT,
    ACROSS_TAU_PCT,
    MERGE_SRC_ALL,
  };

  struct DstDesc {
    pid_t pid;
    int tid;
    perfstat_cycle_t delta_cycles;
    Type type;
    // the thread's pct of access, such that each c-thread contributes to 1
    float pct;
    float src_norm_cpu_reduce;
    float dst_norm_cpu_add;
  };

  // remove all the *DstDesc* whose pct is < pct_thr
  void inline PrunePartialForCompactByPct(double pct_thr = 1);
  // if one src give away 100% pct of threads, then
  // prune the partial ones to avoid traffic noise
  void inline PrunePartialForCompactSuccess();

  bool Empty() { return dst_core_map.empty(); }

  void Reset() {
    ClearDstDesc();
    dst_core_map.clear();
  }

  void Print(std::ostream &out, std::string prefix = "");

  int src_cid;
  std::map<int, std::vector<DstDesc *>> dst_core_map;
  // if no tid is accessing this core, will set this to true
  // NOTE: we only merge_all when we think this core is idle
  // so the shrinking of traffic is done by REDIRECT_FUTURE
  // if the core's utilization is very small
  bool is_merge_all = false;

 private:
  void ClearDstDesc() {
    for (auto &[cid, vec] : dst_core_map) {
      for (auto ptr : vec) delete ptr;
    }
  }
};

class KPLoadManager {
 public:
  // don't want to include FsProc_Fs.h for this
  KPLoadManager(int nw) : num_worker_(nw), core_states_(nw) {}
  ~KPLoadManager() {}

  // thread enter point of this runner thread
  void loadManagerRunner();
  void shutDown() { running_ = false; }

  // then, wait for response
  // will exit if server needs to shutdown
  void WaitMsgWorkerResponse();
  template <typename TCTX>
  int CheckOneMsgAckMatch(FsProcMessage &msg, FsProcMessageType tp);

  void setMessenger(FsProcMessenger *msger) {
    assert(messenger_ == nullptr && msger != nullptr);
    messenger_ = msger;
    running_ = true;
  }
  void SetStatsComb(int wid, KPLoadStatsComb *c) { shared_comb_[wid] = c; }
  // NOTE: only used for testing
  CoreStatesWrapper *TestOnlyGetCoreStateWrapper() { return &core_states_; }

  enum class KPLoadCondition {
    kNormalKeep,
    kOverloadAddOneCore,
    kDoRebalance,
    kDoMerge,
    kDoDeactivate,
  };

  enum class KPCoreLoadState {
    kNormal,
    kUnderutilized,
    kOverloadedMaybeAdapt,
    kOverloadedBottleneck,
  };

  class AbstractPolicy {
   public:
    virtual void execute(
        std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
        KPLoadManager *lm) = 0;
    // If the policy need to invoke gFsProc->setWorkerActive, i.e., adjust
    // the number of active cores, return false, otherwise true
    // Nay, if true, then will only use LB algorithm
    virtual bool StaticNumCore() { return false; };
    virtual int32_t GetEvalWindowUs() { return KLoadMngSleepUs; }
  };

  class SpreadThenGroupPolicy : public AbstractPolicy {
   public:
    SpreadThenGroupPolicy() {}
    virtual void execute(
        std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
        KPLoadManager *lm) override;
  };

  class PackAccessFirstPolicy : public AbstractPolicy {
   public:
    PackAccessFirstPolicy() { creation_routing_tb_.AddWid(kMasterWid); }
    virtual void execute(
        std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
        KPLoadManager *lm) override;
    // remember the changing of creation's routing
    wrt_tb creation_routing_tb_;

   private:
    const std::map<FsProcMessageType, int> kMsgEffectCountDownMap = {
        {FsProcMessageType::kLM_RedirectCreation, 5},
        {FsProcMessageType::kLM_Rebalance, 3}};
    int TryRedirectCreation(int src_wid, int dst_wid,
                            KPLoadStatsSummary *src_smr,
                            KPLoadStatsSummary *dst_smr,
                            KPLoadStatsSummary *w0_smr, KPLoadManager *lm);
    // We know dst_wid has enough share to offer to src_wid
    // find out which specific tid is more appropriate for getting this share
    int TryRebalance(int src_wid, int dst_wid, KPLoadStatsSummary *src_smr,
                     KPLoadStatsSummary *dst_smr, KPLoadManager *lm);
    int TryJoinBack(int src_wid, int dst_wid, KPLoadManager *lm);
    int GetMsgEffectCountDown(FsProcMessageType tp);
  };

  class MinWaitingPolicy : public AbstractPolicy {
   public:
    MinWaitingPolicy() { future_tb.AddWid(kMasterWid); }
    virtual void execute(
        std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
        KPLoadManager *lm) override;
    int TryRebalance(int src_wid, int dst_wid, KPLoadStatsSummary *src_smr,
                     KPLoadStatsSummary *dst_smr, KPLoadManager *lm);
    ftb future_tb;
  };
  struct LBParams {
    float kCongestQlenThr;
  };

  // A policy use fix number of cores, but with load balancing
  class ExprDecomposeLBPolicy : public AbstractPolicy {
   public:
    using cstats_vec = std::vector<std::pair<int, KPLoadStatsSummary *>>;

    constexpr static perfstat_cycle_t kMinWindowCycles = 5e6;
    constexpr static float kCgstQlenDefault = 1.2;

    ExprDecomposeLBPolicy() { lb_params.kCongestQlenThr = kCgstQlenDefault; }
    ExprDecomposeLBPolicy(float cgst_ql) : ExprDecomposeLBPolicy() {
      lb_params.kCongestQlenThr = cgst_ql;
    }

    virtual void execute(cstats_vec &summaries, KPLoadManager *lm) override;

    virtual bool StaticNumCore() override { return true; }
    virtual int32_t GetEvalWindowUs() override { return 2000; }

    // load balancing algorithm
    // @param new_act_cid: if emulate n+1, new_act_cid > 0
    virtual LBPlanItem *FormLbPlan(const cstats_vec &summaries,
                                   KPLoadManager *lm, int new_act_cid = -1);
    virtual LBPlanItem *FormLbMinusPlan(const cstats_vec &summaries,
                                        KPLoadManager *lm);
    virtual void ExecLbPlan(const LBPlanItem &b_plan, KPLoadManager *lm);

    LmMemLessRoutingTable rt_tb;
    LBParams lb_params;
    float agg_cgst_ut = 1;
    uint32_t cgst_num = 1;

   protected:
    double inline NormCpuToQlen(KPLoadStatsSummary *cgst_smr, double norm_cpu);
    double inline GetAdjustableQlen(KPLoadStatsSummary *cgst_smr);
    virtual void CategorizeCores(
        std::vector<int> &cgst_cores, std::vector<int> &under_cores,
        std::map<int, KPLoadStatsSummary *> &cid_smr_map,
        const cstats_vec &summaries);
    void UpdateCgstUtAvg(float cgst_ut) {
      agg_cgst_ut += cgst_ut;
      cgst_num++;
    }
    float GetCgstUtAvg() { return agg_cgst_ut / cgst_num; }
  };

  enum class LBPlanType {
    kLbnMinus,
    kLbn,
    kLbnPlus,
    kLbSkip,
  };

  std::string static GetLbPlanTypeStr(const LBPlanType &tp) {
    return "n+" + std::to_string((int)tp - (int)LBPlanType::kLbn);
  }

  class PerCoreEfficiencyNCPolicy : public ExprDecomposeLBPolicy {
   public:
    constexpr static bool kObservePlanOnly = false;
    // ~=50ms
    constexpr static int kNumMergeConfidentNum = 25;
    constexpr static int kNumMinusConfidentNum = 5;
    struct NCParam {
      float kPerCoreUtThr = 0.4;
    };

    PerCoreEfficiencyNCPolicy() : ExprDecomposeLBPolicy() {}
    PerCoreEfficiencyNCPolicy(float cgst_ql, float per_core_ut)
        : ExprDecomposeLBPolicy(cgst_ql) {
      nc_param.kPerCoreUtThr = per_core_ut;
    }

    virtual bool StaticNumCore() override { return kObservePlanOnly; }
    virtual int32_t GetEvalWindowUs() override { return 2000; }

    // number of cores (NC) policy
    virtual LBPlanType CompareLbPlan(std::map<LBPlanType, LBPlanItem *> &plans,
                                     const cstats_vec &summaries,
                                     const KPLoadManager *lm);

    virtual void execute(
        std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
        KPLoadManager *lm) override;

    void TryExecLbMergePlan(const LBPlanItem &b_plan, KPLoadManager *lm);

    NCParam nc_param;
    uint64_t merge_all_update_vid = 0;
    std::map<int, int> core_merge_all_cnt;
    int nminus_redirect_cnt = 0;
    uint64_t nminus_redirect_vid = 0;
  };

  class NoopPolicy : public AbstractPolicy {
   public:
    NoopPolicy() {}
    void execute(std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
                 KPLoadManager *lm) {}
  };

  void ManualThreadReassignment(int cid, int pid, int tid) {
    core_states_.UpdateCoreState(cid, CoreState::SLEEP, CoreState::ACT_FS_RUN);
    core_idle_tau_counter_[cid][AssembleTwo32B(pid, tid)] = 0;
  }

 private:
  static constexpr int kNumIdleTauCount = 5000;
  static constexpr int kNumDeactivateCount = 1000;
  void inline IncrCondCounter(KPLoadCondition cond);
  bool inline CondPassValidRounds(KPLoadCondition cond);
  void inline ResetCond();
  bool inline CondMergeKeepValid(int wid1, int wid2);
  void inline RecordMergeData(int wid1, int wid2);
  bool inline CondDeactivateKeepValid(int wid);
  void inline RecordDeactivateData(int wid);

  template <typename TCTX>
  void inline SendMsg(int dst_wid, FsProcMessage *msg);

  std::atomic_bool running_{false};
  int num_worker_ = 0;
  CoreStatesWrapper core_states_;
  // shared with the workers
  FsProcMessenger *messenger_{nullptr};
  std::map<int, KPLoadStatsComb *> shared_comb_;

  //
  // load manager's local variables
  //
  // <cid, ts>: per-core
  std::map<int, perfstat_ts_t> last_summary_ts_;
  // <cid, ts>: per-core
  std::map<int, perfstat_ts_t> last_reset_ts_;
  // each core is only allowed to have one message sent after
  // each evaluation round
  std::map<int, FsProcMessageType> core_has_msg_sent_;

  // each evaluation round, increase this id
  uint64_t eval_round_id_ = 0;
  // wait for # of rounds to see the effect of last decision
  // if this > 0, will only read the stats, but without doing
  // the decision making (execute())
  int wait4decision_effect_count_down_ = 0;

  // memorize the history of condition evaluation of each round
  // because we need to adjust the steps(rate) of different decisions
  // The rule is: if after several
  KPLoadCondition pending_cond_;
  int pending_cond_counter_;
  const std::map<KPLoadCondition, int> kCondValidRounds = {
      {KPLoadCondition::kDoMerge, 10}, {KPLoadCondition::kDoDeactivate, 100}};

  // state associated with kCondValidRounds[kDoMerge]
  std::set<int> pending_merge_core_pair_;
  // state associated with kCondValidRounds[kDoDeactivate]
  int pending_idle_core_;

  // <cid, <tau_id, numer_idle_count>>
  // this only contains no-shared files
  // once a tau is redirected into one core, will put a record into this map
  std::map<int, std::map<uint64_t, int>> core_idle_tau_counter_;
  // add a counter that knows how many rounds a core has been not any thread
  // assigned to it
  std::map<int, int> core_no_access_counter_;
};

// ----------------------------------------------------------------------- //
// inline functions definition
// ----------------------------------------------------------------------- //

void LBPlanItem::PrunePartialForCompactByPct(double pct_thr) {
  auto dst_it = dst_core_map.begin();
  while (dst_it != dst_core_map.end()) {
    auto &desc_vec = dst_it->second;
    auto it = desc_vec.begin();
    while (it != desc_vec.end()) {
      if ((*it)->pct < pct_thr) {
        it = desc_vec.erase(it);
      } else {
        ++it;
      }
    }
    if (desc_vec.empty()) {
      dst_core_map.erase(dst_it++);
    } else {
      ++dst_it;
    }
  }
}

void LBPlanItem::PrunePartialForCompactSuccess() {
  int num_pct1_thread = 0;
  float total_pct1_dst_add_cpu = 0;
  for (const auto &[dst, dst_vec] : dst_core_map) {
    for (auto desc : dst_vec) {
      if (desc->pct > 0.99) {
        num_pct1_thread++;
        total_pct1_dst_add_cpu += desc->dst_norm_cpu_add;
      }
    }
  }
  if (num_pct1_thread > 0 && total_pct1_dst_add_cpu > 0.2) {
    // we really do pruning
    PrunePartialForCompactByPct(0.99);
  }
}

void inline KPLoadManager::IncrCondCounter(KPLoadCondition cond) {
  pending_cond_counter_++;
}

bool inline KPLoadManager::CondPassValidRounds(KPLoadCondition cond) {
  assert(cond == pending_cond_);
  auto it = kCondValidRounds.find(cond);
  if (it != kCondValidRounds.end()) {
    return it->second == pending_cond_counter_;
  }
  return false;
}

// totally forget previous condition
void inline KPLoadManager::ResetCond() {
  pending_cond_counter_ = 0;
  pending_cond_ = KPLoadCondition::kNormalKeep;
  pending_merge_core_pair_.clear();
}

bool inline KPLoadManager::CondMergeKeepValid(int wid1, int wid2) {
  if (pending_cond_ != KPLoadCondition::kDoMerge) return false;
  if (pending_merge_core_pair_.find(wid1) == pending_merge_core_pair_.end())
    return false;
  if (pending_merge_core_pair_.find(wid2) == pending_merge_core_pair_.end())
    return false;
  return true;
}

void inline KPLoadManager::RecordMergeData(int wid1, int wid2) {
  pending_merge_core_pair_ = {wid1, wid2};
  pending_cond_ = KPLoadCondition::kDoMerge;
  pending_cond_counter_ = 1;
}

bool inline KPLoadManager::CondDeactivateKeepValid(int wid) {
  if (pending_cond_ != KPLoadCondition::kDoDeactivate) return false;
  return wid == pending_idle_core_;
}

void inline KPLoadManager::RecordDeactivateData(int wid) {
  pending_idle_core_ = wid;
  pending_cond_ = KPLoadCondition::kDoDeactivate;
  pending_cond_counter_ = 1;
}

int inline KPLoadManager::PackAccessFirstPolicy::GetMsgEffectCountDown(
    FsProcMessageType tp) {
  auto it = kMsgEffectCountDownMap.find(tp);
  if (it != kMsgEffectCountDownMap.end()) {
    return it->second;
  }
  return 1;
}

double inline KPLoadManager::ExprDecomposeLBPolicy::NormCpuToQlen(
    KPLoadStatsSummary *cgst_smr, double norm_cpu) {
  return ((((norm_cpu)*cgst_smr->window_cycles) /
           cgst_smr->est_include_wait_cycles) *
          (cgst_smr->recv_noshare_qlen - 1));
}

double inline KPLoadManager::ExprDecomposeLBPolicy::GetAdjustableQlen(
    KPLoadStatsSummary *cgst_smr) {
  return std::min(cgst_smr->recv_noshare_qlen, float(2));
}

}  // namespace fsp_lm

#endif  // CFS_KNOWPARA_LOAD_MNG_H
