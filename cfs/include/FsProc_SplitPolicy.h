#ifndef CFS_SPLIT_POLICY_H
#define CFS_SPLIT_POLICY_H

#include <cstdint>
#include <limits>
#include <map>
#include <set>

#include "FsProc_WorkerStats.h"
#include "typedefs.h"

// inode spliting, joining policy

// static policies
#define SPLIT_POLICY_NO_MOD_PROACTIVE (0)
#define SPLIT_POLICY_NO_DIR_FAMILRY (1)

// testing/ad-hoc policies (mostly static)
// deprecated
// TODO: 2, 3, 101 do not work
#define SPLIT_POLICY_NO_DELAYED_REALLOC (2)
#define SPLIT_POLICY_NO_CLIENT_TRIGGERED (3)
#define SPLIT_POLICY_NO_ADD_HOC_APP_GROUPING (101)
#define SPLIT_POLICY_NO_TWEAK_MOD (102)
#define SPLIT_POLICY_NO_TWEAK_MOD_SHARED (103)

// dynamic policies
#define SPLIT_POLICY_NO_DYNAMIC_BASIC (4)
#define SPLIT_POLICY_NO_DYNAMIC_SPREAD_FIRST (5)
// when src, dst, tid is passed from LM
#define SPLIT_POLICY_NO_DYNAMIC_PACK_ACCESS (6)
#define SPLIT_POLICY_NO_DYNAMIC_MIN_WAIT (7)
#define SPLIT_POLICY_NO_DYNAMIC_EXPR_LB (8)
#define SPLIT_POLICY_NO_DYNAMIC_EXPR_NC (9)

bool inline checkSplitPolicyNoIsDynamic(int pno) {
  if (pno >= SPLIT_POLICY_NO_DYNAMIC_BASIC &&
      pno < SPLIT_POLICY_NO_ADD_HOC_APP_GROUPING) {
    return true;
  }
  return false;
}

enum class FsReqType;
class FsReq;
struct PerWorkerLoadStatsSR;
struct LmMsgRedirectFileSetCtx;
class InMemInode;

class SplitPolicy {
 public:
  virtual void addWorker(int wid) { widSet_.emplace(wid); }
  virtual bool isDynamic() { return false; }
  virtual bool isSplitReqType(FsReqType tp);
  virtual void beforeStatsResetCallback(
      worker_stats::AccessBucketedRecorder *recorder) {}
  virtual void beforeStatsResetCallback(
      worker_stats::ParaAwareRecorder *recorder) {}
  virtual void recvRebalanceCallback(fsp_lm::PerWorkerLoadAllowance *allow) {}
  // @return : destination worker's wid
  virtual int decideDestWorker(uint32_t ino, FsReq *req) = 0;
  virtual int activateDestWorker(int dstWid);
  virtual bool ifAckLMInflightMsg(void *ctx, FsProcMessageType &type,
                                  bool isAbort) {
    // no dynamic policy is not supposed to send message to LM
    return false;
  }
  virtual void updateSplitInitialized(uint32_t ino) {}
  virtual void updateSplitDone(uint32_t ino) {}
  virtual void updateJoinDone() {}

 protected:
  std::set<int> widSet_;
};

// A split policy that triggers splits only when it receives a request from the
// client to migrate an inode. This is useful when testing FSP for scenarios
// where inodes have to migrate.
class SplitPolicyClientTriggered : public SplitPolicy {
 public:
  virtual bool isSplitReqType(FsReqType tp);
  virtual int decideDestWorker(uint32_t ino, FsReq *req);
};

class SplitPolicyClientTriggeredServant : public SplitPolicyClientTriggered {
 public:
  virtual int decideDestWorker(uint32_t ino, FsReq *req);
};

class SplitPolicyServantNoop : public SplitPolicy {
 public:
  SplitPolicyServantNoop(int wid) : wid_(wid) {}
  virtual int decideDestWorker(uint32_t ino, FsReq *req) final { return wid_; }
  int wid_;
};

class SplitPolicyAdHocAppGroupping : public SplitPolicy {
 public:
  SplitPolicyAdHocAppGroupping(int wid) : wid_(wid) {}
  virtual int decideDestWorker(uint32_t ino, FsReq *req) final;
  int wid_;

 protected:
  std::map<pid_t, int> appDstMap;
  int currentAppCount = 0;
};

// A very simple split policy based on *mod*
// It will proactively decide to split inodes, the destination is decided on
// the result of (ino % numWorkers). Thus the mapping of inode to each worker
// is fixed and mostly static (unless more workers are added at run time)
class SplitPolicyModProactive : public SplitPolicy {
 public:
  virtual int decideDestWorker(uint32_t ino, FsReq *req);
};

// A policy that will distribute files in the same directory to the same worker
class SplitPolicyDirFamily : public SplitPolicy {
 public:
  virtual int decideDestWorker(uint32_t ino, FsReq *req);

 protected:
  std::map<std::string, int> dirMap;
  uint64_t currentCount = 0;
};

// A policy that will start assign after one inode is accessed several times
// Used to test if we can move inode at any time (with data blocks with it)
// target is decided by (ino % numWorkers)
class SplitPolicyDelaySeveralReq : public SplitPolicy {
 public:
  virtual int decideDestWorker(uint32_t ino, FsReq *req);

 protected:
  std::map<cfs_ino_t, int> inodeAccessCounter;
  constexpr static int kSplitThreshold = 3;
};

///////////////////////////////////////////////////////////////////////////////
// All right, bellow are dynamic policies.
// -------
// The protocol between W_i and LM
// - Each W_i will keep collecting data into its own *LoadStatsLocalCopy*
//     - It periodically pushes that to a shared buffer and barrier()
// - LM sleep(), wakeup and looks at the {W_i's LoadStatsShared}
//     - LM makes the core allocation decision, and forms a plan
//         - The decision (inst to each included worker) will be written
//           into a single data structure and use RCU to atomically change the
//           pointer. (TODO: check if the rcu_write do barrier)
//           - TODO: (we may directly use the messenger, LM send message)?
//     - LM then starts to wait for ACK from each of the sent out decision
//         - Invariant: LM will only start to evaluate next round if all ACKs
//         received
// - W_i checks the REBALANCE decision (share, dst core)
//     - In a given time-window (# of decision), W_i will see if it can benefit
//       according to its own policy of *inode selection*
//         - Either use that share or ignore the share, W_i must ACK this
//         REBALANCE decision by sending the message to LM
//         - NOTE: we trust W_i's policy is (our program anyway) nice
//     - Before ACK, W_i will push an idle stats() to LM, such that next-round
//     LM evaluation can be start from empty and based on conditions after
//     rebalancing
// -------

class SplitPolicyDynamicBasic : public SplitPolicy {
 public:
  SplitPolicyDynamicBasic(int wid) : wid_(wid) {}

  virtual bool isDynamic() override final { return true; }
  virtual void beforeStatsResetCallback(
      worker_stats::AccessBucketedRecorder *recorder) override;
  virtual void beforeStatsResetCallback(
      worker_stats::ParaAwareRecorder *recorder) override {}

  virtual void recvRebalanceCallback(
      fsp_lm::PerWorkerLoadAllowance *allow) override final {
    fprintf(
        stderr, "wid:%d recv balance rwid:%d allow:%f vid:%lu is_join_all:%d\n",
        wid_, allow->rwid, allow->cpu_allow, allow->vid, allow->is_join_all);
    memcpy(&localLa_, allow, sizeof(fsp_lm::PerWorkerLoadAllowance));
    if (localLa_.rwid >= 0 && localLa_.cpu_allow > 0) {
      if (recvedRebalanceVid < localLa_.vid) {
        recvedRebalanceVid = localLa_.vid;
        numDecisionSinceRecvedRebalance = 0;
      }
    }
    // fprintf(stderr, "update rvidto:%lu\n", recvedRebalanceVid);
  }
  virtual int decideDestWorker(uint32_t ino, FsReq *req);
  virtual int activateDestWorker(int dstWid) final;
  virtual bool ifAckLMInflightMsg(void *ctx, FsProcMessageType &type,
                                  bool isAbort) final;
  virtual void updateSplitInitialized(uint32_t ino) final {
    numInodeInSplitting++;
    // fprintf(stderr, "w:%d init-numInodeInSplitting:%d\n", wid_,
    // numInodeInSplitting);
  }
  virtual void updateSplitDone(uint32_t ino) final {
    numInodeInSplitting--;
    // fprintf(stderr, "w:%d done-numInodeSplitting:%d\n", wid_,
    // numInodeInSplitting);
    if (!localLa_.is_join_all) {
      // Essentially, this is to say, each time we can only rebalance one-inode
      finishedRebalanceVid = recvedRebalanceVid;
    }
  }

  virtual void updateJoinDone() override {
    if (numInodeInSplitting != 0) {
      throw std::runtime_error("updateJoinDone numInSplit:" +
                               std::to_string(numInodeInSplitting));
    }
    // fprintf(stderr, "updateJoinDone fvid:%lu rvid:%lu\n",
    // finishedRebalanceVid,
    //         recvedRebalanceVid);
    finishedRebalanceVid = recvedRebalanceVid;
  }

  bool isInSplitting() { return numInodeInSplitting > 0; }
  bool needRebalance() {
    bool needRebalance = lastWindowUtilization > 0.8 && numInoAccessed > 1;
    needRebalance |= lastWindowUtilization < 0.5;
    return needRebalance;
  }

 protected:
  // if after this number of decision, we cannot do rebalance
  // we simply ACK without doing that
  constexpr static int kNumDecisionRebalanceLimit = 100;
  int wid_;
  fsp_lm::PerWorkerLoadAllowance localLa_;
  int numInodeInSplitting = 0;
  int numInoAccessed = 0;
  uint64_t recvedRebalanceVid = 0;
  uint64_t finishedRebalanceVid = 0;
  int numDecisionSinceRecvedRebalance = 0;
  float lastWindowUtilization = 0;
};

class SplitPolicyDynamicDstKnown {
 public:
  constexpr static int kNumAllowanceEffectLoopNum = 200;
  SplitPolicyDynamicDstKnown(int wid) : wid_(wid) {}

  void SetWorker(FsProcWorker *worker) { fs_worker_ = worker; }

  void AddCpuAllowance(LmMsgRedirectFileSetCtx *ctx) {
    assert(allowance_ctx_ == nullptr);
    allowance_ctx_ = ctx;
    cur_allowance_effective_loop_count_down_ = kNumAllowanceEffectLoopNum;
  }

  void PerLoopCallback();
  void ReqCompletionCallback(FsReq *req, InMemInode *inode);
  bool AddDoneCycles(perfstat_cycle_t cycles) {
    if (cur_allowance_effective_loop_count_down_ == 0) return false;
    if (total_done_cycles_ > allowance_ctx_->allowance_cycles) return false;
    total_done_cycles_ += cycles;
    if (total_done_cycles_ > allowance_ctx_->allowance_cycles) return true;
    return false;
  }

  void AllowanceAckDone() {
    cur_allowance_effective_loop_count_down_ = 0;
    allowance_ctx_ = nullptr;
    total_done_cycles_ = 0;
  }

 private:
  int wid_;
  int cur_allowance_effective_loop_count_down_ = 0;
  int num_ino_sent_ = 0;
  perfstat_cycle_t total_done_cycles_ = 0;
  LmMsgRedirectFileSetCtx *allowance_ctx_{nullptr};
  FsProcWorker *fs_worker_{nullptr};
};

#endif
