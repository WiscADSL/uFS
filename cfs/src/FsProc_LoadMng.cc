#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include "FsProc_Fs.h"
#include "FsProc_LoadMng.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"

extern FsProc *gFsProcPtr;

namespace fsp_lm {

// Summarize the load of different workers in one struct
static uint64_t inline getWorkerTotalEnqueueNum(SnapShotLoadStats *sslst) {
  return sslst->totalReadyCnt[0] + sslst->totalReadyCnt[1];
}

// @return if this snapshot shows the core is 100% idle (true)
bool genSnapShotStatsFromSR(const PerWorkerLoadStatsSR &sr,
                            SnapShotLoadStats &ssls, int wid,
                            uint64_t rel_nano) {
  ssls.wid = wid;
  ssls.version = sr.version;
  ssls.rel_nano = rel_nano;
  ssls.totalReadyCnt[KPrimOpIdx] = sr.opTotalEnqueueReadyCnt[KPrimOpIdx];
  ssls.totalReadyCnt[KSecOpIdx] = sr.opTotalEnqueueReadyCnt[KSecOpIdx];
  auto totalReadyCnt =
      ssls.totalReadyCnt[KPrimOpIdx] + ssls.totalReadyCnt[KSecOpIdx];
  ssls.lastWindowCpuUtilization = sr.lastResetWindowUtilization;
  for (int i = 0; i < INO_CYCLE_BUCKET_NUM; i++) {
    ssls.inoCycleCntArr[i] = sr.inoCycleCntArr[i];
  }
  ssls.nReqDone = sr.numCommonReqDone;
  ssls.nAcIno = sr.numCommonAccessedIno;
  if (totalReadyCnt > 0) {
    ssls.avgQlen = double(sr.readyQueueLenAccumulate) / totalReadyCnt;
  } else {
    ssls.avgQlen = 0;
  }
  return (totalReadyCnt == 0);
}

CoreAllocationDecisionData::CoreAllocationDecisionData(int corenum)
    : coreMaxNum(corenum), snapshots(corenum), core_states(corenum) {
  pulledStats = std::make_unique<PerWorkerLoadStatsSR[]>(corenum);
  memset(pulledStats.get(), 0, sizeof(PerWorkerLoadStatsSR) * corenum);
}

class ServerCorePolicyNoop : public ServerCorePolicy {
 public:
  // noop does nothing
  virtual void execute(CoreAllocationDecisionData &data,
                       LoadAllowanceContainer *lac) final {}
};

class ServerCorePolicyAddOneAtATime : public ServerCorePolicy {
 public:
  ServerCorePolicyAddOneAtATime() {}
  virtual void execute(CoreAllocationDecisionData &data,
                       LoadAllowanceContainer *lac) final {
    constexpr int kNumDoneTooLittleSkip = 5;
    numRoundAfterLastAdapt++;
    if (numRoundAfterLastAdapt <= 1) {
      SPDLOG_INFO("=== skip round");
      // we skip a round of evaluation to avoid reading the legacy stats data
      return;
    }

    curCondition = FsLoadCondition::NORMAL_KEEP;
    // cid, view, max offering share
    std::vector<std::pair<int, const SnapShotLoadStats *>>
        notFullyUtilizedCores;
    // cid, view, need-share
    std::vector<std::tuple<int, const SnapShotLoadStats *, float>>
        overloadedCores;
    int firstInactiveCid = -1;
    auto activeCoreSet = data.getReadOnlyStateCoreSet(CoreState::ACT_FS_RUN);
    auto inActiveCoreSet = data.getReadOnlyStateCoreSet(CoreState::SLEEP);
    if (inActiveCoreSet.size() > 0)
      firstInactiveCid = *(inActiveCoreSet.begin());

    SnapShotLoadStats *curViewPtr = nullptr;
    for (auto cid : activeCoreSet) {
      curViewPtr = data.getSnapshotView(cid);
      if (curViewPtr->nReqDone < kNumDoneTooLittleSkip &&
          curViewPtr->lastWindowCpuUtilization > 0.9) {
        // avoid some strange cases that spdk's polling path is pretty costly
        // continue;
        return;
      }
      if (curViewPtr->lastWindowCpuUtilization < 0.95) {
        notFullyUtilizedCores.push_back({cid, curViewPtr});
      } else {
        // an overloaded core
        if (curViewPtr->avgQlen > 1.1 &&
            curViewPtr->lastWindowCpuUtilization > 0.95) {
          float demandShare = 2;
          for (int i = 0; i < INO_CYCLE_BUCKET_NUM; i++) {
            if (curViewPtr->inoCycleCntArr[i] > 0) {
              demandShare = 0.1 * (i + 1);
              break;
            }
          }
          if (demandShare < 2) {
            overloadedCores.push_back({cid, curViewPtr, demandShare});
          }
        }
      }
    }

    if (overloadedCores.size() > 0) {
      curCondition = FsLoadCondition::OVERLOADED;
      std::sort(overloadedCores.begin(), overloadedCores.end(),
                [](auto &a, auto &b) -> bool {
                  return std::get<2>(a) > std::get<2>(b);
                });
      // fprintf(stderr, "set to OVERLOADED notfulsize:%lu\n",
      //        notFullyUtilizedCores.size());
    }

    if (notFullyUtilizedCores.size() > 0) {
      if (curCondition == FsLoadCondition::OVERLOADED) {
        std::sort(notFullyUtilizedCores.begin(), notFullyUtilizedCores.end(),
                  [](auto &a, auto &b) -> bool {
                    return (a.second)->lastWindowCpuUtilization <
                           (b.second)->lastWindowCpuUtilization;
                  });
        auto curMaxOfferingShare =
            1 - notFullyUtilizedCores[0].second->lastWindowCpuUtilization;
        auto curMaxDemand = std::get<2>(overloadedCores[0]);
        if (curMaxOfferingShare > curMaxDemand) {
          curCondition = FsLoadCondition::NORMAL_REBALANCE;
        }
      } else {
        // NOT OVERLOADED
        // we have at lease one underutilized core
        if (notFullyUtilizedCores.size() > 1) {
          // fprintf(stderr, "first cid:%d util:%f cid:%d util:%f\n",
          //        (notFullyUtilizedCores[0].first),
          //        (notFullyUtilizedCores[0].second)->lastWindowCpuUtilization,
          //        (notFullyUtilizedCores[1].first),
          //        (notFullyUtilizedCores[1].second)->lastWindowCpuUtilization);
          std::sort(notFullyUtilizedCores.begin(), notFullyUtilizedCores.end(),
                    [](auto &a, auto &b) -> bool {
                      // sort from the smallest utilization
                      if (a.first == FsProcWorker::kMasterWidConst) {
                        // we always keep master at the tail such that, we
                        // further swap to the middle
                        return false;
                      }
                      if (b.first == FsProcWorker::kMasterWidConst) {
                        return true;
                      }
                      return (a.second)->lastWindowCpuUtilization <
                             (b.second)->lastWindowCpuUtilization;
                    });
          // we have > 1 underutilized cores, so we try to merge them together
          auto smallestUtilization =
              (notFullyUtilizedCores[0].second)->lastWindowCpuUtilization;
          auto smallestMergeCandidateUtilization =
              (notFullyUtilizedCores[1].second)->lastWindowCpuUtilization;
          float mergeThr = 0.9;
          if (notFullyUtilizedCores[1].first == FsProcWorker::kMasterWidConst) {
            mergeThr = 0.8;
          }
          if (smallestUtilization + smallestMergeCandidateUtilization <
              mergeThr) {
            // fprintf(stderr,
            //         "UNDERUTILIZED_MERGE: smallestUtil:%f cid:%d "
            //         "smallestCandUtil:%f cid:%d\n",
            //         smallestUtilization, notFullyUtilizedCores[0].first,
            //         smallestMergeCandidateUtilization,
            //         notFullyUtilizedCores[1].first);
            SPDLOG_INFO(
                "pendingCnt: {} set to UNDERUTILIZED MERGE cid:{} util:{} "
                "cid:{} util:{} pending[0]:{} pending[1]:{}",
                currentMergePendingRound, notFullyUtilizedCores[0].first,
                smallestUtilization, notFullyUtilizedCores[1].first,
                smallestMergeCandidateUtilization, pendingMergeWids[0],
                pendingMergeWids[1]);
            // curCondition = FsLoadCondition::NORMAL_KEEP;
            curCondition = FsLoadCondition::UNDERUTILIZED_MERGE;
          }
        }
      }
    }

    if (curCondition == FsLoadCondition::UNDERUTILIZED_MERGE) {
      if (pendingMergeWids[0] < 0) {
        pendingMergeWids[0] = notFullyUtilizedCores[0].first;
        pendingMergeWids[1] = notFullyUtilizedCores[1].first;
      }
      if (checkMergeMatch(notFullyUtilizedCores[0].first,
                          notFullyUtilizedCores[1].first)) {
        currentMergePendingRound++;
        if (currentMergePendingRound < numMergeCounter) {
          curCondition = FsLoadCondition::NORMAL_KEEP;
        }
      } else {
        curCondition = FsLoadCondition::NORMAL_KEEP;
        resetMergePending();
      }
    } else if (pendingMergeWids[0] >= 0) {
      resetMergePending();
    }

    switch (curCondition) {
      case FsLoadCondition::OVERLOADED: {
        if (data.getCoreActiveNum() < data.getCoreMaxNum()) {
          // we can allocate one more core
          if (firstInactiveCid < 0) {
            throw std::runtime_error("cid to be activated not set");
          }
          // we simply grant all share to the most overloaded core
          int wid = std::get<0>(overloadedCores[0]);
          SPDLOG_INFO("wid:{} OVERLOADED activate rwid:{}", wid,
                      firstInactiveCid);
          data.updateCoreState(firstInactiveCid, CoreState::SLEEP,
                               CoreState::ACT_FS_RUN);
          gFsProcPtr->setWorkerActive(firstInactiveCid, true);
          std::vector<PerWorkerLoadAllowance *> allowVec{
              lac->getWorkerLoadAllowance(wid)};
          allowVec[0]->wid = wid;
          allowVec[0]->rwid = firstInactiveCid;
          allowVec[0]->cpu_allow = 1;
          allowVec[0]->is_join_all = false;
          allowVec[0]->vid = lac->getVersionId();
          // fprintf(stderr, "OVERLOAD rebalance\n");
          data.updateCoreHasmsg(wid, true,
                                FsProcMessageType::LM_REBALANCE_ALLOC_SHARE);
          int ret = lac->sendUpdateAllowanceMessageToWorkers(allowVec);
          assert(ret == 1);
          numRoundAfterLastAdapt = 0;
        }
      } break;
      case FsLoadCondition::UNDERUTILIZED_KEEP:
        break;
      case FsLoadCondition::UNDERUTILIZED_MERGE: {
        SPDLOG_INFO("EXECUTE UNDERUTILIZED MERGE");
#define GET_UTIL(pair) ((pair.second)->lastWindowCpuUtilization)
        // we use simple greedy algorithm here for now
        // basically we merge two low-utilized worker together
        // we want to move the core that has lowest load
        // so we will send to msg to mergeSrcIdx's wid
        uint mergeSrcIdx = 0;
        uint mergeTargetIdx = mergeSrcIdx + 1;
        std::vector<PerWorkerLoadAllowance *> allowVec;
        float curAllow =
            1.0 - (GET_UTIL(notFullyUtilizedCores[mergeTargetIdx]));
        float curDemand = GET_UTIL(notFullyUtilizedCores[mergeSrcIdx]);
        fprintf(stderr, "merge src:%d target:%d allow:%f demand:%f\n",
                mergeSrcIdx, mergeTargetIdx, curAllow, curDemand);
        if (curAllow >= curDemand) {
          int wid = notFullyUtilizedCores[mergeSrcIdx].first;
          int rwid = notFullyUtilizedCores[mergeTargetIdx].first;
          allowVec.push_back(lac->getWorkerLoadAllowance(wid));
          allowVec.back()->cpu_allow = curAllow;
          allowVec.back()->wid = wid;
          allowVec.back()->rwid = rwid;
          allowVec.back()->is_join_all = true;
          allowVec.back()->vid = lac->getVersionId();
          SPDLOG_INFO("MERGE_ALL to wid:{} rwid:{}", wid, rwid);
          data.updateCoreHasmsg(wid, true, FsProcMessageType::LM_JOINALL);
        }
        int numReq = allowVec.size();
        int ret = lac->sendMergeAllMessageToWorkers(allowVec);
        numRoundAfterLastAdapt = 0;
        assert(ret == numReq);
        resetMergePending();
#undef GET_UTIL
      } break;
      case FsLoadCondition::NORMAL_REBALANCE: {
        // let's rebalance here
        int dstWid = notFullyUtilizedCores[0].first;
        int wid = std::get<0>(overloadedCores[0]);
        SPDLOG_INFO("NORMAL_REBALANCE wid:{} dstWid:{}", wid, dstWid);
        std::vector<PerWorkerLoadAllowance *> allowVec{
            lac->getWorkerLoadAllowance(wid)};
        allowVec[0]->wid = wid;
        allowVec[0]->rwid = dstWid;
        allowVec[0]->cpu_allow =
            1 - notFullyUtilizedCores[0].second->lastWindowCpuUtilization;
        allowVec[0]->is_join_all = false;
        allowVec[0]->vid = lac->getVersionId();
        data.updateCoreHasmsg(wid, true,
                              FsProcMessageType::LM_REBALANCE_ALLOC_SHARE);
        int ret = lac->sendUpdateAllowanceMessageToWorkers(allowVec);
        numRoundAfterLastAdapt = 0;
        assert(ret == 1);
      } break;
      case FsLoadCondition::NORMAL_KEEP:
        break;
      default:
        throw std::runtime_error("unknown condition");
    }
  }

 private:
  void resetMergePending() {
    pendingMergeWids = {-1, -1};
    currentMergePendingRound = 0;
  }

  bool checkMergeMatch(int cid0, int cid1) {
    bool b = (cid0 == pendingMergeWids[0] && cid1 == pendingMergeWids[1]);
    b |= (cid1 == pendingMergeWids[1] && cid0 == pendingMergeWids[0]);
    return b;
  }

  FsLoadCondition curCondition{FsLoadCondition::NORMAL_KEEP};
  uint32_t numRoundAfterLastAdapt = 0;
  constexpr static int numMergeCounter = 2;
  std::vector<int> pendingMergeWids = {-1, -1};
  int currentMergePendingRound = 0;
};

// We add one core by predefined number of operations (kNumOpAddCore)
class ServerCorePolicyBasedOnNumop : public ServerCorePolicy {
 public:
  static const uint32_t kNumOpAddCore = 10000;
  ServerCorePolicyBasedOnNumop(int c) : corenum(c), statsVersion(c, 0) {}
  virtual void execute(CoreAllocationDecisionData &data,
                       LoadAllowanceContainer *lac) final {
    for (int i = 0; i < corenum; i++) {
      auto view = data.getSnapshotView(i);
      if (view->version > statsVersion[i]) {
        numTotalLeafOps += view->totalReadyCnt[1];
        statsVersion[i] = view->version;
      }
    }

    // we add one core after serving every *kNumOpAddCore* secondary-ops
    if (numTotalLeafOps > kNumOpAddCore &&
        data.getCoreActiveNum() < data.getCoreMaxNum()) {
      int cid = -1;
      for (int i = 0; i < corenum; i++) {
        if (!(data.queryCoreStateMatch(i, CoreState::ACT_FS_RUN))) {
          cid = i;
          break;
        }
      }

      if (cid > 0) {
        int oldCoreNum = data.getCoreActiveNum();
        data.updateCoreState(cid, CoreState::SLEEP, CoreState::ACT_FS_RUN);
        gFsProcPtr->setWorkerActive(cid, true);
        // Then we allocate load allowance to existing workers
        std::vector<PerWorkerLoadAllowance *> allowVec;
        for (auto wid : data.getReadOnlyStateCoreSet(CoreState::ACT_FS_RUN)) {
          if (wid != cid) {
            auto curWorkerAllow = lac->getWorkerLoadAllowance(wid);
            curWorkerAllow->wid = wid;
            curWorkerAllow->rwid = cid;
            curWorkerAllow->cpu_allow = 1.0 / oldCoreNum;
            curWorkerAllow->vid = lac->getVersionId();
            allowVec.push_back(curWorkerAllow);
            // record the wid thus we can wait for that ACK
            data.updateCoreHasmsg(wid, true,
                                  FsProcMessageType::LM_REBALANCE_ALLOC_SHARE);
          }
        }
        auto sz = allowVec.size();
        if (sz > 0) {
          int ret = lac->sendUpdateAllowanceMessageToWorkers(allowVec);
          assert((uint)ret == sz);
        }
      }
      numTotalLeafOps = 0;
    }
  }

 private:
  int corenum;
  uint32_t numTotalLeafOps = 0;
  bool lastTimeInBalance = false;
  std::vector<uint64_t> statsVersion;
};

FsProcServerCoreAllocator::FsProcServerCoreAllocator(
    int policyNo, std::atomic_bool &r, FsProcMessenger *msger,
    CoreAllocationDecisionData &d)
    : running_readonly_(r), messenger(msger), data_(d) {
  switch (policyNo) {
    case SERVER_CORE_ALLOC_POLICY_BASEDON_NUMOP:
      policy_ =
          std::make_unique<ServerCorePolicyBasedOnNumop>(data_.getCoreMaxNum());
      break;
    case SERVER_CORE_ALLOC_POLICY_ADDONE:
      policy_ = std::make_unique<ServerCorePolicyAddOneAtATime>();
      break;
    default:
      policy_ = std::make_unique<ServerCorePolicyNoop>();
      break;
  }
}

int FsProcServerCoreAllocator::waitForAllWorkersAckRebalance() {
  int numAcked = 0;
  if (messenger == nullptr) return numAcked;
  int numLeft = data_.coreHasInflightMsg.size();
  if (numLeft == 0) return numAcked;

  FsProcMessage msg;
  bool validMessage = false;
  std::set<int> ackedWid;

  while (numLeft > 0 && running_readonly_) {
    if (!gFsProcPtr->checkNeedOutputLoadStats()) break;
    validMessage = messenger->recv_message(FsProcMessenger::kLmWid, msg);
    if (validMessage) {
      auto curType = static_cast<FsProcMessageType>(msg.type);
      auto fromWidCtx = static_cast<int *>(msg.ctx);
      int wid = *fromWidCtx;
      auto widIt = ackedWid.find(wid);
      assert(widIt == ackedWid.end());
      auto msgIt = data_.coreHasInflightMsg.find(wid);
      // fprintf(stderr, "WAIT_FOR recv msg from:%d type:%d\n", wid,
      // int(curType));
      assert(msgIt != data_.coreHasInflightMsg.end());
      if (msgIt->second == FsProcMessageType::LM_JOINALL) {
        if (curType == FsProcMessageType::LM_JOINALL_ACK) {
          fprintf(stderr, "LM_JOINALL_ACK from %d\n", wid);
          data_.updateCoreState(wid, CoreState::ACT_FS_RUN, CoreState::SLEEP);
          gFsProcPtr->setWorkerActive(wid, false);
        } else if (curType == FsProcMessageType::LM_JOINALL_ACK_ABORT) {
          fprintf(stderr, "LM_JOINALL_ACK_ABORT from %d\n", wid);
        } else {
          throw std::runtime_error("waitForAll recv Type:" +
                                   std::to_string(curType));
        }
      } else if (msgIt->second == FsProcMessageType::LM_REBALANCE_ALLOC_SHARE) {
        assert(curType == FsProcMessageType::LM_REBALANCE_ACK ||
               curType == FsProcMessageType::LM_REBALANCE_ACK_ABORT);
      }
      delete fromWidCtx;
      numAcked++;
      numLeft--;
      ackedWid.emplace(wid);
    }
  }
  data_.coreHasInflightMsg.clear();
  return numAcked;
}

void FsProcServerCoreAllocator::scanInactiveAppWorkerMsgRing() {
  auto inActiveCoreSet = data_.getReadOnlyStateCoreSet(CoreState::SLEEP);
  for (auto wid : inActiveCoreSet) {
    gFsProcPtr->redirectZombieAppReqs(wid);
  }
}

void LoadManager::loadManagerRunner() {
  // wait for master to start running
  pin_to_cpu_core(KLoadMngCoreId);
  while (!gFsProcPtr->checkWorkerActive(FsProcWorker::kMasterWidConst))
    ;
  FsProcServerCoreAllocator coreAllocator(gFsProcPtr->getServerCorePolicyNo(),
                                          running, messenger, decData);
  decData.updateCoreState(FsProcWorker::kMasterWidConst, CoreState::SLEEP,
                          CoreState::ACT_FS_RUN);
  SPDLOG_INFO("LoadManager is running ------");
  uint64_t start_ts = PerfUtils::Cycles::rdtscp();
  while (running) {
    std::this_thread::sleep_for(std::chrono::microseconds(KLoadMngSleepUs));
    // FIXME: here is there anyway to guarantee this monitor thread wake up
    // on the same core?

    // pull all the worker's load stats by one bulk copy
    decData.pullSR(stats);
    uint64_t cur_nano = PerfUtils::Cycles::toNanoseconds(
        PerfUtils::Cycles::rdtscp() - start_ts);

    for (int wid = 0; wid < numWorker; wid++) {
      bool isACT = decData.queryCoreStateMatch(wid, CoreState::ACT_FS_RUN);
      // In case we are using other type of policy and not set the core state,
      // but still want output
      bool completelyIdle = decData.genWorkerSnapshot(wid, cur_nano);
      if (isACT || (!completelyIdle)) {
        if (gFsProcPtr->checkNeedOutputLoadStats()) {
          decData.printSnapShots(wid);
        }
      }
    }

    // simple check of split policy
    // we assume the old static policy does not deal with various # of cores
    if (!checkSplitPolicyNoIsDynamic(gFsProcPtr->getSplitPolicy())) continue;

    if (running) {
      // we double check running here because it uses FsProcWorker data
      // No matter we output stats or not, LM periodically scan inactive
      // worker's message ring Q: do we need to do it less frequently?
      coreAllocator.scanInactiveAppWorkerMsgRing();
    }

    // This is a simple hack to control if the system is doing adapting or not
    // If we recv a request to start dump loads, then the core allocation is
    // going to be executed
    if (!gFsProcPtr->checkNeedOutputLoadStats()) continue;

    // let's see if we need to add core or not
    coreAllocator.executeCoreAllocation(&laContainer);
  }

  SPDLOG_INFO("---- Load Monitoring exit ----");
}

void LoadManager::resetStats() {
  for (int i = 0; i < numWorker; i++) {
    resetWorkerLoadStats(&stats[i], 0);
  }
}

}  // namespace fsp_lm