#ifndef CFS_LOAD_MNG_COMMON_H
#define CFS_LOAD_MNG_COMMON_H

#include "FsProc_Messenger.h"
#include "param.h"
#include "typedefs.h"

#include <cassert>
#include <cstdint>
#include <set>
#include <vector>

// For now, we pin the loadMonitor to one remote core
#define KLoadMngCoreId (30)
#define KLoadMngSleepUs (1000)

#define SERVER_CORE_ALLOC_POLICY_NOOP (0)
// A manually set policy that change core number after # of operations
// For testing
#define SERVER_CORE_ALLOC_POLICY_BASEDON_NUMOP (1)
#define SERVER_CORE_ALLOC_POLICY_ADDONE (2)

#define SERVER_CORE_SPREAD_GROUP (5)
#define SERVER_CORE_PACK_GROUP (6)
#define SERVER_CORE_MIN_WAIT (7)
#define SERVER_CORE_EXPR_LB (8)
#define SERVER_CORE_LB_NC_PER_CORE_TP (9)

namespace fsp_lm {

enum class CoreState {
  ACT_FS_RUN = 0,
  SLEEP,
};

constexpr static int GetCoreStateNo(CoreState s) {
  return static_cast<int>(s) - static_cast<int>(CoreState::ACT_FS_RUN);
}

const static std::vector<CoreState> CORE_STATES = {CoreState::ACT_FS_RUN,
                                                   CoreState::SLEEP};

class CoreStatesWrapper {
 public:
  CoreStatesWrapper(int nw) : state_core_set_vec_(CORE_STATES.size()) {
    // we init all the cores into inactive state
    for (int i = 0; i < nw; i++) {
      (state_core_set_vec_[GetCoreStateNo(CoreState::SLEEP)]).insert(i);
    }
  }

  bool UpdateCoreState(int wid, CoreState from, CoreState to) {
    auto &from_set = state_core_set_vec_[GetCoreStateNo(from)];
    auto &to_set = state_core_set_vec_[GetCoreStateNo(to)];
    int num = from_set.erase(wid);
    if (num != 1) return false;
    to_set.emplace(wid);
    return true;
  }

  int GetCoreActiveNum() {
    return state_core_set_vec_[GetCoreStateNo(CoreState::ACT_FS_RUN)].size();
  }

  const std::set<int> &GetReadOnlyStateCoreSet(CoreState state) {
    return state_core_set_vec_[GetCoreStateNo(state)];
  }

  int FindInactiveCore() {
    auto &sleep_cores = state_core_set_vec_[GetCoreStateNo(CoreState::SLEEP)];
    if (sleep_cores.size() > 0) {
      return *(sleep_cores.begin());
    }
    return -1;
  }

  bool QueryCoreStateMatch(int wid, CoreState state) {
    auto &cur_set = state_core_set_vec_[GetCoreStateNo(state)];
    return cur_set.find(wid) != cur_set.end();
  }

 private:
  std::vector<std::set<int>> state_core_set_vec_;
};

struct PerWorkerLoadAllowance {
  // This worker's id
  int wid;
  // Remote worker's id
  // Later we may consider allocate share for different workers
  int rwid;
  pid_t pid;
  int tid;
  // TODO: do we still need this field of cpuAllow?
  float cpu_allow;
  bool is_join_all;
  uint64_t vid;
  char padding_[30];
};

static_assert(sizeof(PerWorkerLoadAllowance) == CACHE_LINE_BYTE,
              "Allowance not aligned");
}  // namespace fsp_lm

#endif