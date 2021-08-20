#include "FsProc_KnowParaLoadMng.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <map>
#include <set>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>

#include "FsProc_Fs.h"
#include "param.h"
#include "typedefs.h"
#include "util.h"

extern FsProc *gFsProcPtr;

namespace fsp_lm {

void KPLoadManager::loadManagerRunner() {
  pin_to_cpu_core(KLoadMngCoreId);
  std::string logger_name = std::string(gFsProcPtr->kLogDir) + "/fsp_stats";
  std::ofstream ofs;
  ofs.open(logger_name, std::ofstream::out | std::ofstream::app);

  while (!gFsProcPtr->checkWorkerActive(FsProcWorker::kMasterWidConst))
    ;

  std::unique_ptr<AbstractPolicy> policy_ptr;
  int cur_policy_no = gFsProcPtr->getServerCorePolicyNo();
  switch (cur_policy_no) {
    case (SERVER_CORE_SPREAD_GROUP): {
      policy_ptr = std::make_unique<SpreadThenGroupPolicy>();
    } break;
    case (SERVER_CORE_PACK_GROUP): {
#ifndef KP_SPECIAL_NUM_CREATE
      throw std::runtime_error(
          "PackAccessFirstPolicy must define macro KP_SPECIAL_NUM_CREATE");
#endif
      policy_ptr = std::make_unique<PackAccessFirstPolicy>();
    } break;
    case (SERVER_CORE_MIN_WAIT): {
      SPDLOG_INFO("SERVER_CORE_MIN_WAIT");
      policy_ptr = std::make_unique<MinWaitingPolicy>();
    } break;
    case (SERVER_CORE_EXPR_LB): {
      float cfg_cgst_ql = gFsProcPtr->GetLbCgstQl();
      if (cfg_cgst_ql > 1) {
        policy_ptr = std::make_unique<ExprDecomposeLBPolicy>(cfg_cgst_ql);
      } else {
        policy_ptr = std::make_unique<ExprDecomposeLBPolicy>();
      }
    } break;
    case (SERVER_CORE_LB_NC_PER_CORE_TP): {
      float cfg_cgst_ql = gFsProcPtr->GetLbCgstQl();
      float cfg_percore_ut = gFsProcPtr->GetNcPerCoreUt();
      if (cfg_cgst_ql < 1) {
        cfg_cgst_ql = ExprDecomposeLBPolicy::kCgstQlenDefault;
      }
      if (cfg_percore_ut >= 0) {
        policy_ptr = std::make_unique<PerCoreEfficiencyNCPolicy>(
            cfg_cgst_ql, cfg_percore_ut);
      } else {
        policy_ptr = std::make_unique<PerCoreEfficiencyNCPolicy>();
      }
    } break;
    default:
      policy_ptr = std::make_unique<NoopPolicy>();
  }

  if (policy_ptr->StaticNumCore()) {
    sleep(1);
    SPDLOG_INFO("Activate {} workers", num_worker_);
    for (int i = 0; i < num_worker_; i++) {
      core_states_.UpdateCoreState(i, CoreState::SLEEP, CoreState::ACT_FS_RUN);
      gFsProcPtr->setWorkerActive(i, true);
    }
  } else {
    core_states_.UpdateCoreState(FsProcWorker::kMasterWidConst,
                                 CoreState::SLEEP, CoreState::ACT_FS_RUN);
  }

  SPDLOG_INFO("{} is running -------", typeid(this).name());
  fprintf(stderr, "LM started\n");
  uint64_t lm_start_ts = PerfUtils::Cycles::rdtscp();
  while (running_) {
    std::this_thread::sleep_for(
        std::chrono::microseconds(policy_ptr->GetEvalWindowUs()));
    [[maybe_unused]] uint64_t cur_nano = PerfUtils::Cycles::toNanoseconds(
        PerfUtils::Cycles::rdtscp() - lm_start_ts);
    eval_round_id_++;

    auto active_core_set =
        core_states_.GetReadOnlyStateCoreSet(CoreState::ACT_FS_RUN);
    std::vector<std::pair<int, KPLoadStatsSummary *>> summaries;

    std::set<int> wids;
    // continue;

    if (!checkSplitPolicyNoIsDynamic(gFsProcPtr->getSplitPolicy())) {
      // static policy, we only do print
      for (int wid = 0; wid < num_worker_; wid++) {
        wids.insert(wid);
      }
    } else {
      // dynamic policy
      wids = core_states_.GetReadOnlyStateCoreSet(CoreState::ACT_FS_RUN);
    }

    bool not_stable = false;

    // read data
    for (auto wid : wids) {
      auto ts = PerfUtils::Cycles::rdtscp();
      auto summary = new KPLoadStatsSummary(wid, ts);
      auto shared_comb_ptr = shared_comb_[wid];
      perfstat_ts_t cur_last_reset_ts = last_reset_ts_[wid];
      summary->comb_reset_ts = shared_comb_ptr->reset_ts.load();
      if (summary->comb_reset_ts > 0) {
        perfstat_cycle_t cur_window_cycles =
            summary->comb_reset_ts - cur_last_reset_ts;
        summary->last_reset_ts = cur_last_reset_ts;
        // assert(cur_window_cycles > 1000);
        if (cur_window_cycles > 1e9) {
          // fprintf(stderr, "%lu %lu\n", summary->comb_reset_ts,
          //         cur_last_reset_ts);
          // when system start, we may wait a long while to read the data
          // or a core activated again, we regard it as invalid
          last_reset_ts_[wid] = summary->comb_reset_ts;
          last_summary_ts_[wid] = ts;
          shared_comb_ptr->reset_ts.store(0);
          not_stable = true;
          continue;
        }
        // read the data

        // queue stats
        summary->comb_q_stats = shared_comb_ptr->q_stats;
        summary->comb_recv_queuing_stats = shared_comb_ptr->recv_queuing_stats;
        auto q_stats_ptr = &(summary->comb_q_stats);
        // per-thread utilization
        auto &tau_cycle_vec = shared_comb_ptr->tau_exclusive_cycles;
        for (auto [pid, tid, cur_cycles] : tau_cycle_vec) {
          auto key = AssembleTwo32B(pid, tid);
          float cur_ut = float(cur_cycles) / cur_window_cycles;
          if (cur_ut > 0.9) {
            // It is unreasonable one thread ut could > 0.9
            // If that is a blocking thread
            not_stable = true;
          }
          if (cur_ut > summary->max_ut) {
            summary->max_ut = cur_ut;
            summary->max_ut_thread = key;
          }
          if (cur_ut > 0.01) {
            summary->tau_utilization.emplace(
                key, std::make_pair(cur_ut, cur_cycles));
            if (cur_ut < summary->min_ut) {
              summary->min_ut = cur_ut;
              summary->min_ut_thread = key;
            }
            summary->total_ut += cur_ut;
          }
        }
        // overall utilization
        summary->comb_idle_cycles = shared_comb_ptr->idle_cycles;
        uint32_t total_done_cnt = q_stats_ptr->primary_only_done_cnt +
                                  q_stats_ptr->splittable_done_cnt;
        auto cur_ut = 1 - float(summary->comb_idle_cycles) / cur_window_cycles;
        summary->useful_window_cycles =
            cur_window_cycles - summary->comb_idle_cycles;
        summary->window_cycles = cur_window_cycles;
        if (total_done_cnt < 10 &&
            (summary->comb_idle_cycles > 0 && cur_ut > 0.8)) {
          // There is some chance that *nvme_spdk_poll_completion*
          // takes a lot of cpu time
          not_stable = true;
          summary->avg_recv_qlen = 1;
          summary->cpu_ut = cur_ut;
        } else {
          summary->cpu_ut = cur_ut;
          if (total_done_cnt != 0) {
            summary->avg_recv_qlen =
                float(q_stats_ptr->num_ready_qlen_accumulate) / total_done_cnt;
            summary->recv_qlen =
                float(shared_comb_ptr->recv_queuing_stats.recv_qlen_agg) /
                total_done_cnt;
            summary->recv_noshare_qlen =
                float(shared_comb_ptr->recv_queuing_stats
                          .recv_qlen_rm_share_agg) /
                total_done_cnt;
            if (summary->recv_qlen < 1.0) {
              summary->recv_qlen = 1;
              summary->recv_noshare_qlen = 1;
            }
            summary->est_include_wait_cycles =
                summary->recv_noshare_qlen * summary->useful_window_cycles;
          } else {
            summary->avg_recv_qlen = 1;
          }
        }

        if (summary->tau_utilization.size() == 0 &&
            summary->comb_q_stats.primary_only_done_cnt < 10) {
          summary->cpu_ut = 0;
          summary->avg_recv_qlen = 1;
          summary->recv_noshare_qlen = 1;
          summary->recv_qlen = 1;
        }

#ifdef KP_SPECIAL_NUM_CREATE
        for (auto [pid, tid, num_creates] : shared_comb_ptr->tau_num_create) {
          auto key = AssembleTwo32B(pid, tid);
          summary->tau_num_creates[key] = num_creates;
          if (num_creates <= 0) {
            throw std::runtime_error("num_creates is invalid:" +
                                     std::to_string(num_creates));
          }
        }
        shared_comb_ptr->tau_num_create.clear();
#endif

#ifdef KP_DISTINCT_SHARED_FILES
        auto ino_cycle_vec = shared_comb_ptr->shared_file_cpu_cycles;
        for (auto [ino, cur_cycles] : ino_cycle_vec) {
          if (cur_cycles > cur_window_cycles) {
            not_stable = true;
            continue;
          }
          auto cur_ut = float(cur_cycles) / cur_window_cycles;
          if (cur_ut < 0) continue;
          if (cur_ut > summary->max_share_ut) {
            summary->max_share_ut = cur_ut;
            summary->max_share_ino = ino;
          }
          summary->total_share_ut += cur_ut;
          summary->shared_file_cpu_cycles.push_back({ino, cur_ut, cur_cycles});
        }
        if (summary->total_share_ut > 1) {
          for (auto [ino, cur_cycles] : ino_cycle_vec) {
            fprintf(stdout, " [i:%u u:%f]", ino,
                    float(cur_cycles) / cur_window_cycles);
          }
          fprintf(stdout, "\n");
        }
        shared_comb_ptr->shared_file_cpu_cycles.clear();
#endif

        shared_comb_ptr->tau_exclusive_cycles.clear();
        last_reset_ts_[wid] = summary->comb_reset_ts;
        last_summary_ts_[wid] = ts;
        summaries.push_back({wid, summary});
        tau_cycle_vec.clear();
        // reset the data after successfully reading
        shared_comb_ptr->reset_ts.store(0);
      } else {
        not_stable = true;
        // W_i haven't set the reset_ts
        delete summary;
        // goto cleanup_summries;
        continue;
      }
    }

    // print stats
    if (gFsProcPtr->checkNeedOutputLoadStats()) {
      // if (!not_stable) {
      for (auto it : summaries) {
#ifdef UFS_EXPR_LBNC
        std::cout << *(it.second);
#else
        ofs << *(it.second);
#endif
      }
      // }
    }

    if (!checkSplitPolicyNoIsDynamic(gFsProcPtr->getSplitPolicy()))
      goto cleanup_summries;

    if (running_) {
      auto inactive_core_set =
          core_states_.GetReadOnlyStateCoreSet(CoreState::SLEEP);
      for (auto wid : inactive_core_set) {
        gFsProcPtr->redirectZombieAppReqs(wid);
      }
    }

    // execute core scheduling and data repartitioning
    if (wait4decision_effect_count_down_ == 0) {
      if (!not_stable && gFsProcPtr->checkNeedOutputLoadStats()) {
        policy_ptr->execute(summaries, this);
        WaitMsgWorkerResponse();
      }
    } else {
      wait4decision_effect_count_down_--;
    }

  cleanup_summries:
    for (auto it : summaries) {
      delete it.second;
      it.second = nullptr;
    }

  }  // while(running_)
  ofs.close();
}

template <typename TCTX>
int KPLoadManager::CheckOneMsgAckMatch(FsProcMessage &ack_msg,
                                       FsProcMessageType tp) {
  auto ctx = reinterpret_cast<TCTX *>(ack_msg.ctx);
  assert(ctx != nullptr);
  auto src_wid = ctx->recv_wid;
  auto it = core_has_msg_sent_.find(src_wid);
  if (it == core_has_msg_sent_.end() || (int)tp - (int)it->second != 1) {
    return -1;
  }
  core_has_msg_sent_.erase(it);
  delete ctx;
  return 0;
}

void KPLoadManager::WaitMsgWorkerResponse() {
  FsProcMessage msg;
  bool msg_valid = false;
  if (!core_has_msg_sent_.empty()) {
    // fprintf(stderr, "WaitMsgWorkerResponse\n");
  }
  while (running_ && !core_has_msg_sent_.empty()) {
    msg_valid = messenger_->recv_message(FsProcMessenger::kLmWid, msg);
    if (!msg_valid) continue;
    auto cur_type = static_cast<FsProcMessageType>(msg.type);
    int rt = 0;
    switch (cur_type) {
      case FsProcMessageType::kLM_RedirectOneTauAck: {
        rt = CheckOneMsgAckMatch<LmMsgRedirectOneTauCtx>(msg, cur_type);
        // fprintf(stderr, "kLM_RedirectOneTauAck\n");
      } break;
      case FsProcMessageType::kLM_RedirectCreationAck: {
        rt = CheckOneMsgAckMatch<LmMsgRedirectCreationCtx>(msg, cur_type);
        // fprintf(stderr, "kLM_RedirectCreationAck\n");
      } break;
      case FsProcMessageType::kLM_RebalanceAck: {
        rt = CheckOneMsgAckMatch<LmMsgRedirectFileSetCtx>(msg, cur_type);
        // fprintf(stderr, "kLM_RebalanceAck\n");
      } break;
      case FsProcMessageType::kLM_RedirectFutureAck: {
        rt = CheckOneMsgAckMatch<LmMsgRedirectFutureCtx>(msg, cur_type);
        // fprintf(stderr, "kLM_RedirectFutureAck\n");
      } break;
      case FsProcMessageType::kLM_JoinAllAck: {
        auto ctx = reinterpret_cast<LmMsgJoinAllCtx *>(msg.ctx);
        auto src_wid = ctx->src_wid;
        int num_join = ctx->num_join;
        auto cur_nano =
            PerfUtils::Cycles::toNanoseconds(PerfUtils::Cycles::rdtscp());
        fprintf(stderr, "kLm_JoinAllAck num_join:%d ctx:%p nano:%lu\n",
                num_join, msg.ctx, cur_nano);
        if (num_join >= 0) {
          SPDLOG_INFO("nano:{} wid:{} deactivated", (cur_nano), src_wid);
          gFsProcPtr->setWorkerActive(src_wid, false);
          core_states_.UpdateCoreState(src_wid, /*from*/ CoreState::ACT_FS_RUN,
                                       /*to*/ CoreState::SLEEP);
        }
        // this will delete ctx
        rt = CheckOneMsgAckMatch<LmMsgJoinAllCtx>(msg, cur_type);
      } break;
      default:
        break;
    }
    if (rt < 0) {
      throw std::runtime_error("Error ack check fail");
    }
  }
}

// Prefer 1-cthread:1-fsp-worker
// TODO: there is no rebalance support
void KPLoadManager::SpreadThenGroupPolicy::execute(
    std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
    KPLoadManager *lm) {
  // is there any core that has multiple c-threads?
  std::sort(summaries.begin(), summaries.end(), [](auto &a, auto &b) -> bool {
    KPLoadStatsSummary *a_data = a.second;
    KPLoadStatsSummary *b_data = b.second;
    auto a_num_tau = a_data->tau_utilization.size();
    auto b_num_tau = b_data->tau_utilization.size();
    // we delibrately put w_0 to the head if there is any c-thread
    // more than one
    if (a_data->wid == FsProcWorker::kMasterWidConst && a_num_tau > 1) {
      return true;
    }
    if (b_data->wid == FsProcWorker::kMasterWidConst && b_num_tau > 1) {
      return false;
    }
    // put the worker with more client access first
    if (a_num_tau != b_num_tau) {
      return a_num_tau > b_num_tau;
    }
    if (a_data->avg_recv_qlen != b_data->avg_recv_qlen) {
      return a_data->avg_recv_qlen > b_data->avg_recv_qlen;
    }
    return a_data->cpu_ut > b_data->cpu_ut;
  });

  if (summaries[0].second->tau_utilization.size() > 1) {
    auto cur_summary = summaries[0].second;
    // we have at least one core that has two c-threads
    int cid = lm->core_states_.FindInactiveCore();
    if (cid > 0) {
      // we will activate a core and redirecting the c-thread
      auto ctx = new LmMsgRedirectOneTauCtx();
      ctx->src_wid = cur_summary->wid;
      ctx->dst_wid = cid;
      DessembleOne64B(cur_summary->max_ut_thread, ctx->pid, ctx->tid);
      ctx->time_range =
          LmMsgRedirectOneTauCtx::EffectiveTimeRange::kFutureAndPast;
      gFsProcPtr->setWorkerActive(cid, true);
      lm->core_states_.UpdateCoreState(cid, /*from*/ CoreState::SLEEP,
                                       /*to*/ CoreState::ACT_FS_RUN);
      fprintf(stderr, "Worker: %d waken up\n", cid);
      // send messages
      FsProcMessage curmsg;
      curmsg.type = FsProcMessageType::kLM_RedirectOneTau;
      curmsg.ctx = ctx;
      lm->SendMsg<LmMsgRedirectOneTauCtx>(ctx->src_wid, &curmsg);
      if (ctx->src_wid != FsProcWorker::kMasterWidConst) {
        FsProcMessage secmsg;
        // directly make a copy
        auto sec_ctx = new LmMsgRedirectOneTauCtx(*ctx);
        secmsg.ctx = sec_ctx;
        secmsg.type = FsProcMessageType::kLM_RedirectOneTau;
        lm->SendMsg<LmMsgRedirectOneTauCtx>(FsProcWorker::kMasterWidConst,
                                            &secmsg);
      }
      lm->core_idle_tau_counter_[cid][cur_summary->max_ut_thread] = 0;
      return;
    } else {
      // we already activate all the cores
      // TODO: try to rebalance
    }
  }

  // now let's iterate the summaries to see if some of c-thread is idle
  for (auto [cid, smr] : summaries) {
    // we will never redirect back from w0
    if (cid == FsProcWorker::kMasterWidConst) continue;

    std::set<uint64_t> tids;
    for (auto [tid, _] : lm->core_idle_tau_counter_[cid]) {
      tids.emplace(tid);
    }
    for (auto [tid, _] : smr->tau_utilization) {
      tids.emplace(tid);
    }

    for (auto tid : tids) {
      auto tid_ut_it = smr->tau_utilization.find(tid);
      float ut = 0;
      if (tid_ut_it != smr->tau_utilization.end()) {
        ut = tid_ut_it->second.first;
      }
      auto idle_it = lm->core_idle_tau_counter_[cid].find(tid);
      if (idle_it != lm->core_idle_tau_counter_[cid].end()) {
        if (ut < 0.00001) {
          idle_it->second += 1;
          if (idle_it->second == kNumIdleTauCount) {
            // we think this tau is no longer active
            // send msg to w0
            auto ctx = new LmMsgRedirectOneTauCtx();
            ctx->src_wid = cid;
            ctx->dst_wid = FsProcWorker::kMasterWidConst;
            DessembleOne64B(tid, ctx->pid, ctx->tid);
            ctx->time_range =
                LmMsgRedirectOneTauCtx::EffectiveTimeRange::kFutureAndPast;
            FsProcMessage msg;
            msg.type = FsProcMessageType::kLM_RedirectOneTau;
            msg.ctx = ctx;
            lm->SendMsg<LmMsgRedirectOneTauCtx>(FsProcWorker::kMasterWidConst,
                                                &msg);
            // send msg to target
            auto svt_ctx = new LmMsgRedirectOneTauCtx(*ctx);
            FsProcMessage svt_msg;
            svt_msg.type = FsProcMessageType::kLM_RedirectOneTau;
            svt_msg.ctx = svt_ctx;
            lm->SendMsg<LmMsgRedirectOneTauCtx>(cid, &svt_msg);
            lm->core_idle_tau_counter_[cid].erase(idle_it);
            fprintf(stderr, "tid:%lu no longer active\n", tid);
            return;
          }
        } else {
          // if (idle_it->second > 0) {
          //   fprintf(stderr, "before reset num:%d\n", idle_it->second);
          // }
          idle_it->second = 0;
        }
      } else {
        // SPDLOG_ERROR("the tid({}) is not in wid-{}'s assigned map", tid,
        // cid);
      }
    }
  }  // [cid, smr]:summaries

  // ok, now we try to shutdown a core and return the core to the kernel
  auto active_cores =
      lm->core_states_.GetReadOnlyStateCoreSet(CoreState::ACT_FS_RUN);
  for (auto cid : active_cores) {
    if (cid == FsProcWorker::kMasterWidConst) continue;
    if (lm->core_idle_tau_counter_[cid].empty()) {
      auto it = lm->core_no_access_counter_.find(cid);
      if (it == lm->core_no_access_counter_.end()) {
        lm->core_no_access_counter_.emplace(cid, 1);
      } else {
        it->second++;
        if (it->second == kNumDeactivateCount) {
          lm->core_states_.UpdateCoreState(cid, CoreState::ACT_FS_RUN,
                                           CoreState::SLEEP);
          fprintf(stderr, "set active to false wid:%d\n", cid);
          gFsProcPtr->setWorkerActive(cid, false);
          it->second = 0;
        }
      }
    } else {
      lm->core_no_access_counter_[cid] = 0;
    }
  }
}

// Consider CPU efficiency first
// Not until a core is overloaded (or cannot put more load in)
// Start add another core
void KPLoadManager::PackAccessFirstPolicy::execute(
    std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
    KPLoadManager *lm) {
  constexpr float kCongestQlenThr = 1.2;
  constexpr float kOverloadUtThr = 0.8;
  constexpr float kUnderutilizedUtThr = 0.7;
  constexpr float kUnderutilizedQlenThr = 1.2;
  // constexpr float kUnderutilizedRebalanceDstUtThr = 0.5;
  constexpr float kUnderutilizedDefiniteDstUtThr = 0.4;
  constexpr float kUnderutilizedMergeThr = 0.4;
  constexpr float kCoreIdleUtThr = 0.01;
  using CLS = KPCoreLoadState;

  std::map<int, KPLoadStatsSummary *> cid_smr_map;

  // we first judge each active core's individual load state
  std::map<KPCoreLoadState, std::vector<KPLoadStatsSummary *>>
      core_load_states_map;
  for (auto [cid, smr] : summaries) {
    auto cur_core_load_state = CLS::kNormal;
    cid_smr_map[cid] = smr;
#ifdef KP_SPECIAL_NUM_CREATE
    for (auto [crt_tau, n_crt] : smr->tau_num_creates) {
      pid_t pid;
      int tid;
      DessembleOne64B(crt_tau, pid, tid);
      int cur_crt_cid =
          creation_routing_tb_.GetDstWid(pid, tid, lm->eval_round_id_);
      if (cur_crt_cid < 0) {
        // this pid, tid has not been initialized to creation table
        creation_routing_tb_.UpdateDstWid(pid, tid, crt_tau, kMasterWid,
                                          lm->eval_round_id_);
      }
    }
#endif

    // We need both of these metrics to decide if this core is overloaded or not
    // E.g., every time two request comes at the same time, then the
    // avg_recv_qlen is always high, but in each window, there are only tens of
    // requests, that is still sort of idle cores.
    //       -- need other way to solve this problem
    // The problem with utilization is: maximal can only be 1, we have no idea
    // if 1 means just ok or overloaded
    //       -- another point is, from 0-0.5 is far easy that 0.5-1
    if ((smr->avg_recv_qlen > kCongestQlenThr &&
         smr->cpu_ut > kOverloadUtThr)) {
      if (smr->comb_q_stats.splittable_done_cnt > 1) {
        cur_core_load_state = CLS::kOverloadedMaybeAdapt;
      } else {
        cur_core_load_state = CLS::kOverloadedBottleneck;
      }
    }
    if ((smr->cpu_ut < kUnderutilizedUtThr &&
         smr->avg_recv_qlen < kUnderutilizedQlenThr) ||
        smr->cpu_ut < kUnderutilizedDefiniteDstUtThr) {
      cur_core_load_state = CLS::kUnderutilized;
    }

    core_load_states_map[cur_core_load_state].push_back(smr);
  }

  auto &over_cores = core_load_states_map[CLS::kOverloadedMaybeAdapt];
  auto &underut_cores = core_load_states_map[CLS::kUnderutilized];
  std::sort(underut_cores.begin(), underut_cores.end(),
            [](auto a, auto b) -> bool {
              // so the one that has less cpu utilization (can offer more cpu
              // share) would be put near to the head
              int a_cid = a->wid;
              int b_cid = b->wid;
              // we put W_0 at the tail (most overloaded)
              if (a_cid == FsProcWorker::kMasterWidConst) return false;
              if (b_cid == FsProcWorker::kMasterWidConst) return true;
              return a->cpu_ut < b->cpu_ut;
            });

  std::sort(over_cores.begin(), over_cores.end(), [](auto a, auto b) -> bool {
    int a_cid = a->wid;
    int b_cid = b->wid;
    // we put W_0 at the head (most overloaded)
    if (a_cid == FsProcWorker::kMasterWidConst) return true;
    if (b_cid == FsProcWorker::kMasterWidConst) return false;
    // otherwise sort by cpu utilization (from larger to the smaller)
    return a->cpu_ut > b->cpu_ut;
  });

  // let's see if any of the underutilized core can save us by having load
  // from the over_cores
  int overload_src_cid = -1;
  KPLoadStatsSummary *overload_src_smr = nullptr;
  int underutilize_dst_cid = -1;
  KPLoadStatsSummary *underutilized_dst_smr = nullptr;

  if (!underut_cores.empty()) {
    underutilized_dst_smr = underut_cores[0];
    underutilize_dst_cid = underutilized_dst_smr->wid;
  }

  if (!over_cores.empty()) {
    overload_src_smr = over_cores[0];
    overload_src_cid = overload_src_smr->wid;
  }

  int new_activate_cid = lm->core_states_.FindInactiveCore();

  KPLoadCondition condition = KPLoadCondition::kNormalKeep;
  // now we decide the overall load condition of the whole fsp server
  if (!over_cores.empty()) {
    if (!underut_cores.empty()) {
      // we have both overloaded and underutilized cores
      if (underutilize_dst_cid >= 0 &&
          underutilized_dst_smr->cpu_ut < kUnderutilizedUtThr) {
        condition = KPLoadCondition::kDoRebalance;
      } else if (new_activate_cid > 0) {
        condition = KPLoadCondition::kOverloadAddOneCore;
      }
    } else {
      // we have overloaded core but no underutilized core
      if (new_activate_cid > 0) {
        condition = KPLoadCondition::kOverloadAddOneCore;
      }
    }
  } else {
    // no overloaded cores, either normal, or underutilized
    if (!underut_cores.empty()) {
      if (underut_cores[0]->cpu_ut < kCoreIdleUtThr &&
          underut_cores[0]->wid != FsProcWorker::kMasterWidConst) {
        condition = KPLoadCondition::kDoDeactivate;
      } else {
        if (underut_cores.size() > 1) {
          if (underut_cores[0]->cpu_ut < kUnderutilizedMergeThr &&
              underut_cores[1]->cpu_ut < kUnderutilizedMergeThr) {
            condition = KPLoadCondition::kDoMerge;
          }
        }
      }  // have no idle core
    }    // !underut_cores.empty()
  }

  // Done with the condition of whole fs load assessment

  switch (condition) {
    case KPLoadCondition::kDoRebalance: {
      SPDLOG_INFO("DoRebalance src:{} dst:{} redirect_private",
                  overload_src_cid, underutilize_dst_cid);
      assert(underutilized_dst_smr != nullptr);
      auto w0_smr = cid_smr_map[FsProcWorker::kMasterWidConst];
      int num_redirect_crt = TryRedirectCreation(
          overload_src_cid, underutilize_dst_cid, overload_src_smr,
          underutilized_dst_smr, w0_smr, lm);
      if (num_redirect_crt > 0) {
        SPDLOG_INFO("RedirectionCreation");
        return;
      }
      TryRebalance(overload_src_cid, underutilize_dst_cid, overload_src_smr,
                   underutilized_dst_smr, lm);
    } break;
    case KPLoadCondition::kOverloadAddOneCore: {
      overload_src_cid = over_cores[0]->wid;
      overload_src_smr = over_cores[0];
      SPDLOG_INFO("OverloadAddOneCore cid:{} src:{}", new_activate_cid,
                  overload_src_cid);
      gFsProcPtr->setWorkerActive(new_activate_cid, true);
      lm->core_states_.UpdateCoreState(new_activate_cid,
                                       /*from*/ CoreState::SLEEP,
                                       /*to*/ CoreState::ACT_FS_RUN);
      creation_routing_tb_.AddWid(new_activate_cid);
      auto w0_smr = cid_smr_map[FsProcWorker::kMasterWidConst];
      int num_redirect_crt = TryRedirectCreation(
          overload_src_cid, new_activate_cid, overload_src_smr,
          /*dst_smr*/ nullptr, w0_smr, lm);
      if (num_redirect_crt > 0) {
        SPDLOG_INFO("RedirectionCreation");
        return;
      }
      TryRebalance(overload_src_cid, new_activate_cid, overload_src_smr,
                   /*dst_smr*/ nullptr, lm);
    } break;
    case KPLoadCondition::kDoMerge: {
      auto wid1 = underut_cores[0]->wid;
      auto wid2 = underut_cores[1]->wid;
      SPDLOG_INFO("DoMerge counter:{} from:{} to:{}", lm->pending_cond_counter_,
                  wid1, wid2);
      if (lm->CondMergeKeepValid(wid1, wid2)) {
        lm->IncrCondCounter(condition);
        fprintf(stdout, "after Inc round:%d\n", lm->pending_cond_counter_);
        if (lm->CondPassValidRounds(condition)) {
          fprintf(stdout, "kDoMerge");
          // TryJoinBack(wid1, wid2, lm);
          // lm->ResetCond();
        }
      } else {
        lm->RecordMergeData(wid1, wid2);
      }
    } break;
    case KPLoadCondition::kDoDeactivate: {
      auto cur_wid = underut_cores[0]->wid;
      SPDLOG_INFO("DoDeactivate counter:{} from:{}", lm->pending_cond_counter_,
                  cur_wid);
      if (lm->CondDeactivateKeepValid(cur_wid)) {
        lm->IncrCondCounter(condition);
        if (lm->CondPassValidRounds(condition)) {
          // TryJoinBack(cur_wid, FsProcWorker::kMasterWidConst, lm);
          // lm->ResetCond();
        }
      } else {
        lm->RecordDeactivateData(cur_wid);
      }
    } break;
    case KPLoadCondition::kNormalKeep: {
      // SPDLOG_INFO("NormalKeep");
    } break;
    default: { throw std::runtime_error("invalid condition"); }
  }
}

// Given the fact that src is overloaded and dst is overloaded,
// We first see if the problem is *src_wid* has some actively
// served as the *routing target worker* of new creation.
int KPLoadManager::PackAccessFirstPolicy::TryRedirectCreation(
    int src_wid, int dst_wid, KPLoadStatsSummary *src_smr,
    KPLoadStatsSummary *dst_smr, KPLoadStatsSummary *w0_smr,
    KPLoadManager *lm) {
  assert(src_smr != nullptr);
  int num_creation_tau = w0_smr->tau_num_creates.size();
  if (num_creation_tau == 0) return 0;
  std::vector<std::tuple<uint64_t, float, int>> src_routing_target_taus;
  std::vector<std::tuple<uint64_t, float, int>> dst_routing_target_taus;

  constexpr int kNumRoundAsActiveCreation = 3;
  auto src_active_creation_taus =
      creation_routing_tb_.GetActiveTausInDstWithinNumRound(
          src_wid, lm->eval_round_id_, kNumRoundAsActiveCreation);
  auto dst_active_creation_taus =
      creation_routing_tb_.GetActiveTausInDstWithinNumRound(
          dst_wid, lm->eval_round_id_, kNumRoundAsActiveCreation);

  for (auto tau_id : src_active_creation_taus) {
    float cur_ut = 0;
    int num_crt = 0;
    auto it = src_smr->tau_utilization.find(tau_id);
    if (it != src_smr->tau_utilization.end()) cur_ut = (it->second).first;
    auto it2 = w0_smr->tau_num_creates.find(tau_id);
    if (it2 != w0_smr->tau_num_creates.end()) num_crt = it2->second;
    src_routing_target_taus.push_back({tau_id, cur_ut, num_crt});
  }

  if (dst_smr != nullptr) {
    // dst_smr will be null if this is a new activated wid
    for (auto tau_id : dst_active_creation_taus) {
      float cur_ut = 0;
      int num_crt = 0;
      auto it = dst_smr->tau_utilization.find(tau_id);
      if (it != dst_smr->tau_utilization.end()) cur_ut = (it->second).first;
      auto it2 = w0_smr->tau_num_creates.find(tau_id);
      if (it2 != w0_smr->tau_num_creates.end()) num_crt = it2->second;
      dst_routing_target_taus.push_back({tau_id, cur_ut, num_crt});
    }
  }

  int num_src_routing_target = src_routing_target_taus.size();
  int num_dst_routing_target = dst_routing_target_taus.size();
  if (num_src_routing_target > num_dst_routing_target) {
    // fprintf(stderr, "num_src_rt:%d num_dst_rt:%d\n", num_src_routing_target,
    //   num_dst_routing_target);
    // we want to adjust the routing table now
    // so we choose the one that has higher cpu utilization
    std::sort(src_routing_target_taus.begin(), src_routing_target_taus.end(),
              [](auto a, auto b) -> bool {
                auto a_ut = std::get<1>(a);
                auto b_ut = std::get<1>(b);
                if (std::abs(a_ut - b_ut) < 0.01) {
                  // they have same level of utilization
                  auto a_num_crt = std::get<2>(a);
                  auto b_num_crt = std::get<2>(b);
                  if (a_num_crt == b_num_crt) return a_ut > b_ut;
                  return a_num_crt > b_num_crt;
                }
                return a_ut > b_ut;
              });
    auto tau_id = std::get<0>(src_routing_target_taus[0]);
    pid_t cur_pid;
    int cur_tid;
    DessembleOne64B(tau_id, cur_pid, cur_tid);
    // SPDLOG_INFO("src_wid:{} dst_wid:{} pid:{} tid:{}", src_wid, dst_wid,
    //             cur_pid, cur_tid);
    // fprintf(stderr, "redirectCreation sent to src:%d dst:%d pid:%d tid:%d\n",
    //         src_wid, dst_wid, cur_pid, cur_tid);
    FsProcMessage msg;
    auto ctx = new LmMsgRedirectCreationCtx();
    ctx->src_wid = src_wid;
    ctx->dst_wid = dst_wid;
    ctx->pid = cur_pid;
    ctx->tid = cur_tid;
    msg.type = FsProcMessageType::kLM_RedirectCreation;
    msg.ctx = ctx;
    fprintf(stderr, "kLM_RedirectCreation\n");
    lm->SendMsg<LmMsgRedirectCreationCtx>(FsProcWorker::kMasterWidConst, &msg);
    creation_routing_tb_.UpdateDstWid(cur_pid, cur_tid, tau_id, dst_wid,
                                      lm->eval_round_id_);
    lm->wait4decision_effect_count_down_ =
        GetMsgEffectCountDown(FsProcMessageType::kLM_RedirectCreation);
    return 1;
  }
  return 0;
}

int KPLoadManager::PackAccessFirstPolicy::TryRebalance(
    int src_wid, int dst_wid, KPLoadStatsSummary *src_smr,
    KPLoadStatsSummary *dst_smr, KPLoadManager *lm) {
  // SPDLOG_INFO("TryRebalance src_wid:{} dst_wid:{}", src_wid, dst_wid);
  assert(src_smr != nullptr);
  std::vector<uint64_t> dst_active_creation_taus =
      creation_routing_tb_.GetActiveTausInDstWithinNumRound(
          dst_wid, lm->eval_round_id_, /*num_round*/ 10);
  std::vector<std::tuple<uint64_t, float, perfstat_cycle_t>> cur_taus;
  constexpr float max_one_time_allowance = 0.3;
  float max_dst_share_offer = 1.0;
  if (dst_smr != nullptr) {
    max_dst_share_offer = 1 - dst_smr->cpu_ut;
  }
  if (dst_active_creation_taus.size() > 0) {
    // we first see if we can find some tau that has the creation target at the
    // dst_wid
    for (auto tau_id : dst_active_creation_taus) {
      auto it = src_smr->tau_utilization.find(tau_id);
      if (it == src_smr->tau_utilization.end()) continue;
      float cur_ut = it->second.first;
      if (src_smr->max_ut > 0.1 && cur_ut < 0.05) {
        // even though the creation target is in dst_wid, this thread's load is
        // very low, so I think it won't benefit the fs that much if there is
        // higher load thread
        continue;
      }
      cur_taus.push_back({tau_id, it->second.first, it->second.second});
    }
  }
  if (cur_taus.empty()) {
    for (auto [tau_id, ut_cycles] : src_smr->tau_utilization) {
      cur_taus.push_back({tau_id, ut_cycles.first, ut_cycles.second});
    }
  }
  if (cur_taus.empty()) return -1;
  std::sort(cur_taus.begin(), cur_taus.end(), [](auto a, auto b) -> bool {
    return std::get<1>(a) > std::get<1>(b);
  });
  pid_t pid;
  int tid;
  uint64_t chosen_tau_id = std::get<0>(cur_taus[0]);
  DessembleOne64B(chosen_tau_id, pid, tid);
  SPDLOG_INFO("tau_id:{} pid:{} tid:{} ut:{}", chosen_tau_id, pid, tid,
              std::get<1>(cur_taus[0]));
  FsProcMessage msg;
  auto cur_type = FsProcMessageType::kLM_Rebalance;
  auto ctx = new LmMsgRedirectFileSetCtx();
  ctx->src_wid = src_wid;
  ctx->dst_wid = dst_wid;
  ctx->pid = pid;
  ctx->tid = tid;
  float cur_share = std::min(max_one_time_allowance, max_dst_share_offer);
  float cur_share_cycles =
      cur_share / (std::get<1>(cur_taus[0]) / std::get<2>(cur_taus[0]));
  ctx->allowance = cur_share;
  ctx->allowance_cycles = (perfstat_cycle_t)cur_share_cycles;
  ctx->ack_done_allowance_cycles = 0;
  msg.ctx = ctx;
  msg.type = cur_type;
  fprintf(stderr, "kLM_Rebalance sent\n");
  lm->SendMsg<LmMsgRedirectFileSetCtx>(src_wid, &msg);
  lm->wait4decision_effect_count_down_ = GetMsgEffectCountDown(cur_type);
  return 0;
}

int KPLoadManager::PackAccessFirstPolicy::TryJoinBack(int src_wid, int dst_wid,
                                                      KPLoadManager *lm) {
  auto num_crt_dst = creation_routing_tb_.GetNumDstInWid(src_wid);
  if (num_crt_dst > 0) {
    FsProcMessage msg;
    auto cur_type = FsProcMessageType::kLM_JoinAllCreation;
    auto ctx = new LmMsgJoinAllCreationCtx();
    ctx->src_wid = src_wid;
    ctx->dst_wid = dst_wid;
    msg.ctx = ctx;
    msg.type = cur_type;
    lm->messenger_->send_message(FsProcWorker::kMasterWidConst, msg);
    fprintf(stderr, "kLM_JoinAllCreation sent\n");
    FsProcMessage recv_msg;
    bool recv_msg_valid = false;
    do {
      recv_msg_valid =
          lm->messenger_->recv_message(FsProcMessenger::kLmWid, recv_msg);
      if (recv_msg_valid &&
          recv_msg.type != FsProcMessageType::kLM_JoinAllCreationAck) {
        throw std::runtime_error("kLM_JoinAllCreationAck not match");
      }
    } while (!recv_msg_valid);
  }
  creation_routing_tb_.EraseWid(src_wid);
  FsProcMessage msg;
  auto cur_type = FsProcMessageType::kLM_JoinAll;
  auto ctx = new LmMsgJoinAllCtx();
  ctx->src_wid = src_wid;
  ctx->dst_wid = dst_wid;
  ctx->num_join = 0;
  msg.type = cur_type;
  msg.ctx = ctx;
  lm->SendMsg<LmMsgJoinAllCtx>(src_wid, &msg);
  lm->wait4decision_effect_count_down_ = 3;
  fprintf(stderr, "kLm_JoinAll sent ctx:%p msg.ctx:%p\n", ctx, msg.ctx);
  return 0;
}

// This favors the performance extremely
// The performance goal is to get the overall shorted finish time
// for all the clients, therefore, the optimal solution is to
// minimize the waiting time of all the client thread
void KPLoadManager::MinWaitingPolicy::execute(
    std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
    KPLoadManager *lm) {
  using CLS = KPCoreLoadState;
  constexpr int64_t kCoreWaitNano_WaterMark_Low = 2000;
  constexpr int64_t kCoreWaitNano_WaterMark_High = 12000;
  constexpr float kUtilization_WaterMark_Low = 0.08;
  [[maybe_unused]] constexpr float kUtilization_WaterMark_High = 0.3;
  constexpr float kUtilization_Idle_Thr = 0.01;

  std::map<int, KPLoadStatsSummary *> cid_smr_map;

  std::map<KPCoreLoadState, std::vector<KPLoadStatsSummary *>>
      core_load_states_map;
  for (auto [cid, smr] : summaries) {
    auto cur_core_load_state = CLS::kNormal;
    cid_smr_map[cid] = smr;
    if (smr->total_wait_nano > kCoreWaitNano_WaterMark_High) {
      if (smr->comb_q_stats.splittable_done_cnt > 1) {
        cur_core_load_state = CLS::kOverloadedMaybeAdapt;
      } else {
        cur_core_load_state = CLS::kOverloadedBottleneck;
      }
    }
    if (smr->total_wait_nano < kCoreWaitNano_WaterMark_Low &&
        smr->cpu_ut < kUtilization_WaterMark_Low) {
      // the metric to say it is an underutilized core is:
      // it may be safe to put some more load there
      cur_core_load_state = CLS::kUnderutilized;
    }
    core_load_states_map[cur_core_load_state].push_back(smr);
  }
  auto &congest_cores = core_load_states_map[CLS::kOverloadedMaybeAdapt];
  auto &under_cores = core_load_states_map[CLS::kUnderutilized];
  std::sort(congest_cores.begin(), congest_cores.end(),
            [](auto a, auto b) -> bool {
              // Q: do we need to force w0 to be at the head?
              return a->total_wait_nano > b->total_wait_nano;
            });
  std::sort(under_cores.begin(), under_cores.end(), [](auto a, auto b) -> bool {
    int a_cid = a->wid;
    int b_cid = b->wid;
    // we put W_0 at the tail
    if (a_cid == FsProcWorker::kMasterWidConst) return false;
    if (b_cid == FsProcWorker::kMasterWidConst) return true;
    return a->cpu_ut < b->cpu_ut;
  });

  KPLoadCondition condition = KPLoadCondition::kNormalKeep;
  int new_activate_cid = -1;
  if (!congest_cores.empty()) {
    if (!under_cores.empty()) {
      condition = KPLoadCondition::kDoRebalance;
    } else {
      new_activate_cid = lm->core_states_.FindInactiveCore();
      if (new_activate_cid > 0) {
        condition = KPLoadCondition::kOverloadAddOneCore;
      }
    }  // !under_cores.empty()
  } else {
    // we don't have congestion cores, any merging possible?
    if (!under_cores.empty()) {
      if (under_cores.size() > 1) {
        condition = KPLoadCondition::kDoMerge;
      } else if (under_cores[0]->cpu_ut < kUtilization_Idle_Thr) {
        condition = KPLoadCondition::kDoDeactivate;
      }
    }  // !under_cores.empty()
  }

  switch (condition) {
    case KPLoadCondition::kDoRebalance: {
      fprintf(stderr, "kDoRebalance\n");
      int rt = -1;
      KPLoadStatsSummary *src_smr = nullptr;
      KPLoadStatsSummary *dst_smr = nullptr;
      for (auto cur_src_smr : congest_cores) {
        for (auto cur_dst_smr : under_cores) {
          rt = TryRebalance(cur_src_smr->wid, cur_dst_smr->wid, cur_src_smr,
                            cur_dst_smr, lm);
          if (rt == 0) {
            src_smr = cur_src_smr;
            dst_smr = cur_dst_smr;
            break;
          }
        }
      }
      if (rt == 0) {
        SPDLOG_INFO("kDoRebalance src:{} dst:{}", src_smr->wid, dst_smr->wid);
      }
    } break;
    case KPLoadCondition::kOverloadAddOneCore: {
      fprintf(stderr, "kAddOneCore\n");
      auto src_smr = congest_cores[0];
      SPDLOG_INFO("kOverloadAddOneCore src:{} dst:{}", src_smr->wid,
                  new_activate_cid);
      gFsProcPtr->setWorkerActive(new_activate_cid, true);
      lm->core_states_.UpdateCoreState(new_activate_cid, CoreState::SLEEP,
                                       CoreState::ACT_FS_RUN);
      int rt;
      for (auto cur_src_smr : congest_cores) {
        rt = TryRebalance(cur_src_smr->wid, new_activate_cid, cur_src_smr,
                          nullptr, lm);
        if (rt == 0) break;
      }
    } break;
    case KPLoadCondition::kDoMerge: {
      // fprintf(stderr, "kDoMerge\n");
    } break;
    case KPLoadCondition::kDoDeactivate: {
      // fprintf(stderr, "kDoDeactivate\n");
    } break;
    case KPLoadCondition::kNormalKeep: {
    } break;
    default:
      throw std::runtime_error("invalid condition");
  }
}

int KPLoadManager::MinWaitingPolicy::TryRebalance(int src_wid, int dst_wid,
                                                  KPLoadStatsSummary *src_smr,
                                                  KPLoadStatsSummary *dst_smr,
                                                  KPLoadManager *lm) {
  assert(src_smr != nullptr);
  auto src_tau_set = future_tb.GetWidTaus(src_wid);
  auto dst_tau_set = future_tb.GetWidTaus(dst_wid);
  std::vector<std::tuple<uint64_t, float, int>> redirect_tau_candidates;
  fprintf(stderr, "try rebalance from:%d to:%d\n", src_wid, dst_wid);
  for (auto [tau_id, ut_cycles] : src_smr->tau_utilization) {
    pid_t pid;
    int tid;
    DessembleOne64B(tau_id, pid, tid);
    int tau_orig_wid = future_tb.GetTauCurDstWid(pid, tid);
    if (tau_orig_wid < 0) {
      future_tb.InitTauToMaster(pid, tid, tau_id, lm->eval_round_id_);
      tau_orig_wid = FsProcWorker::kMasterWidConst;
    }
    if (tau_orig_wid == src_wid) {
      redirect_tau_candidates.push_back(
          {tau_id, ut_cycles.first, src_smr->tau_wait_nano[tau_id]});
    }
  }
  if (redirect_tau_candidates.empty()) return -1;
  std::sort(redirect_tau_candidates.begin(), redirect_tau_candidates.end(),
            [](auto a, auto b) -> bool {
              auto [id_a, ut_a, wait_a] = a;
              auto [id_b, ut_b, wait_b] = b;
              if (wait_a != wait_b) return wait_a > wait_b;
              return ut_a > ut_b;
            });
  uint64_t chosen_tau_id = std::get<0>(redirect_tau_candidates[0]);
  FsProcMessage msg;
  auto cur_type = FsProcMessageType::kLM_RedirectFuture;
  auto ctx = new LmMsgRedirectFutureCtx();
  ctx->src_wid = src_wid;
  ctx->dst_wid = dst_wid;
  pid_t pid;
  int tid;
  DessembleOne64B(chosen_tau_id, pid, tid);
  ctx->pid = pid;
  ctx->tid = tid;
  msg.ctx = ctx;
  msg.type = cur_type;
  lm->SendMsg<LmMsgRedirectFutureCtx>(dst_wid, &msg);
  int rt = future_tb.UpdateTauDstWid(pid, tid, chosen_tau_id, src_wid, dst_wid,
                                     lm->eval_round_id_);
  assert(rt >= 0);
  fprintf(stderr, "round:%lu pid:%d tid:%d update from:%d to:%d rt:%d\n",
          lm->eval_round_id_, pid, tid, src_wid, dst_wid, rt);
  auto his_map = future_tb.GetTauHistory(pid, tid);
  bool has_w0_in_history = false;
  // TODO: need to have a threshold to say: if a routing item is over #rounds
  // send msg to the old dst to let it return all the reassigned but not active
  // inodes
  // because of the routing item added to the full historical dst worker,
  // if the inode is active, it must be reassigned already
  for (auto [round_id, his_dst_wid] : his_map) {
    if (his_dst_wid == FsProcWorker::kMasterWidConst) has_w0_in_history = true;
    if (his_dst_wid == dst_wid) continue;
    FsProcMessage secmsg;
    auto sec_ctx = new LmMsgRedirectFutureCtx(*ctx);
    secmsg.ctx = sec_ctx;
    secmsg.type = cur_type;
    fprintf(stderr, "send RedirectFuture to:%d", his_dst_wid);
    lm->SendMsg<LmMsgRedirectFutureCtx>(his_dst_wid, &secmsg);
  }
  if (!has_w0_in_history) {
    throw std::runtime_error("we don't have w0 in history");
  }
  lm->wait4decision_effect_count_down_ = 10;
  return 0;
}

void LBPlanItem::Print(std::ostream &out, std::string prefix) {
  // return;
  if (dst_core_map.empty()) return;
  out << "[LbPlan-" << prefix << "] src_cid:" << src_cid << "\n";
  for (auto &[cid, v] : dst_core_map) {
    for (auto dst_desc : v) {
      out << " dst_cid:" << cid << " delta:" << dst_desc->delta_cycles
          << " pid:" << dst_desc->pid << " tid:" << dst_desc->tid
          << " pct:" << dst_desc->pct
          << " s_nm_red:" << dst_desc->src_norm_cpu_reduce
          << " d_nm_add:" << dst_desc->dst_norm_cpu_add;
      switch (dst_desc->type) {
        case Type::ONE_TAU_ALL:
          out << " ONE_TAU_ALL";
          break;
        case Type::ONE_TAU_PCT:
          out << " ONE_TAU_PCT";
          break;
        case Type::ACROSS_TAU_PCT:
          out << " ACROSS_TAU_PCT";
          break;
        case Type::MERGE_SRC_ALL:
          out << " MERGE_SRC_ALL";
          break;
        default:
          out << " Invalid";
      }
      out << "\n";
    }
  }
  out << "-------"
      << "\n";
}

void KPLoadManager::ExprDecomposeLBPolicy::execute(cstats_vec &summaries,
                                                   KPLoadManager *lm) {
  auto lb_n_plan = FormLbPlan(summaries, lm);
  // auto lb_n_minus_plan = FormLbMinusPlan(summaries, lm);
  // if (lb_n_minus_plan != nullptr) {
  //   auto &cur_stream = std::cerr;
  //   lb_n_minus_plan->Print(cur_stream, "n_minus");
  //   delete lb_n_minus_plan;
  // }
  if (lb_n_plan != nullptr) {
    lb_n_plan->Print(std::cout, "n");
    lb_n_plan->Print(std::cerr, "n");
    // execute plan by sending msg
    ExecLbPlan(*lb_n_plan, lm);
    delete lb_n_plan;
  }
}

void KPLoadManager::ExprDecomposeLBPolicy::CategorizeCores(
    std::vector<int> &cgst_cores, std::vector<int> &under_cores,
    std::map<int, KPLoadStatsSummary *> &cid_smr_map,
    const cstats_vec &summaries) {
  for (auto [cid, smr] : summaries) {
    // std::cerr << *smr;
    if (smr->recv_noshare_qlen > lb_params.kCongestQlenThr) {
      cgst_cores.push_back(cid);
    } else {
      under_cores.push_back(cid);
    }
    if (smr->recv_noshare_qlen < 0.99 && smr->recv_noshare_qlen > 0.0001) {
      // throw std::runtime_error("qlen < 1, invalid " +
      //                          std::to_string(smr->recv_noshare_qlen));
      SPDLOG_WARN("qlen invalid {}", smr->recv_noshare_qlen);
    }
    cid_smr_map[cid] = smr;
  }
}

#define P8_CAL_SRC_CPU_REDUCE(_ncpu, _src_smr) \
  (((_ncpu * ((_src_smr)->window_cycles))) /   \
   ((_src_smr)->est_include_wait_cycles)) *    \
      ((_src_smr)->cpu_ut)

// #define LB_ALGO_DBG_PRINT
LBPlanItem *KPLoadManager::ExprDecomposeLBPolicy::FormLbPlan(
    const cstats_vec &summaries, KPLoadManager *lm, int new_act_cid) {
  std::vector<int> congest_cores;
  std::vector<int> under_cores;
  std::map<int, KPLoadStatsSummary *> cid_smr_map;

  CategorizeCores(congest_cores, under_cores, cid_smr_map, summaries);

  if (congest_cores.empty()) {
    // no congest core to deal with
    return nullptr;
  }

  if (new_act_cid > 0) {
    auto new_smr = new KPLoadStatsSummary(new_act_cid);
    new_smr->window_cycles = kMinWindowCycles;
    new_smr->cpu_ut = 0;
    new_smr->recv_noshare_qlen = 1;
    under_cores.push_back(new_act_cid);
    cid_smr_map[new_act_cid] = new_smr;
  }

  if (under_cores.empty()) {
    // no destination
    return nullptr;
  }

  // we first try to fit all the congested ones into the under ones
  // and make sure don't break one thread's access
  std::sort(under_cores.begin(), under_cores.end(),
            [&cid_smr_map](auto a, auto b) -> bool {
              auto a_smr = cid_smr_map[a];
              auto b_smr = cid_smr_map[b];
              return a_smr->cpu_ut < b_smr->cpu_ut;
            });
  std::sort(congest_cores.begin(), congest_cores.end(),
            [&cid_smr_map](auto a, auto b) -> bool {
              auto a_smr = cid_smr_map[a];
              auto b_smr = cid_smr_map[b];
              return a_smr->recv_noshare_qlen > b_smr->recv_noshare_qlen;
            });
  auto cur_plan = new LBPlanItem();
  cur_plan->src_cid = congest_cores[0];
  auto cgst_smr = cid_smr_map[congest_cores[0]];
  std::vector<std::tuple<pid_t, int, double>> cgst_tau_norm_cpu;
  for (const auto &[tau_id, ut_pair] : cgst_smr->tau_utilization) {
    pid_t pid;
    int tid;
    DessembleOne64B(tau_id, pid, tid);
    double norm_demand = std::min(((ut_pair.first / cgst_smr->total_ut) *
                                   cgst_smr->est_include_wait_cycles) /
                                      cgst_smr->window_cycles,
                                  0.9);
#ifdef LB_ALGO_DBG_PRINT
    fprintf(stdout, "cur_ut:%f norm_demand:%f\n", ut_pair.first, norm_demand);
#endif
    cgst_tau_norm_cpu.push_back({pid, tid, norm_demand});
  }
  std::sort(
      cgst_tau_norm_cpu.begin(), cgst_tau_norm_cpu.end(),
      [](auto a, auto b) -> bool { return std::get<2>(a) > std::get<2>(b); });

  // mixmal one attempt can only alleviate qlen by 1
  // since we are doing 1 src to 1 dst
  double adjustable_ql = GetAdjustableQlen(cgst_smr);
  double src_norm_cpu_split =
      ((double(adjustable_ql - lb_params.kCongestQlenThr)) /
       (adjustable_ql - 1)) *
      (cgst_smr->est_include_wait_cycles / cgst_smr->window_cycles);
  // Q: should we do src_norm_cpu_split*cgst_smr->cpu_ut?

#ifdef LB_ALGO_DBG_PRINT
  fprintf(stdout, "src_norm_cpu_split:%f\n", src_norm_cpu_split);
  fprintf(stdout, "src_norm_cpu feed to ql:%f\n",
          NormCpuToQlen(cgst_smr, src_norm_cpu_split));
#endif

  // first pass, can we form the plan without breaking thread access?
  bool find_no_break_sol = false;
  {
    size_t under_idx = 0, cgst_tau_idx = 0;
    double agg_splitted_norm_cpu = 0;
    int cur_cid = under_cores[under_idx];
    std::map<int, double> under_cores_added;
    while (cgst_tau_idx < cgst_tau_norm_cpu.size()) {
      auto [pid, tid, norm_cpu] = cgst_tau_norm_cpu[cgst_tau_idx];
      auto cur_under_smr = cid_smr_map[cur_cid];
      // #ifdef LB_ALGO_DBG_PRINT
      fprintf(stdout, "pid:%d tid:%d norm_cpu:%f under_ut:%f\n", pid, tid,
              norm_cpu, cur_under_smr->cpu_ut);
      // #endif
      if (cur_under_smr->cpu_ut + under_cores_added[cur_cid] + norm_cpu <
          cgst_smr->cpu_ut) {
        // able to fit in
        agg_splitted_norm_cpu += norm_cpu;
        auto cur_ptr = new LBPlanItem::DstDesc();
        cur_ptr->pid = pid;
        cur_ptr->tid = tid;
        cur_ptr->type = LBPlanItem::ONE_TAU_ALL;
        cur_ptr->pct = 1;
        cur_ptr->dst_norm_cpu_add = norm_cpu;
        cur_ptr->src_norm_cpu_reduce =
            P8_CAL_SRC_CPU_REDUCE(norm_cpu, cgst_smr);
        cur_plan->dst_core_map[cur_cid].push_back(cur_ptr);
        under_cores_added[cur_cid] += norm_cpu;
        under_idx++;
      }

      if (adjustable_ql - NormCpuToQlen(cgst_smr, agg_splitted_norm_cpu) <
          lb_params.kCongestQlenThr + 0.0001) {
        find_no_break_sol = true;
        break;
      }
      cgst_tau_idx++;
    }
  }

#ifdef LB_ALGO_DBG_PRINT
  fprintf(stdout, "find_no_break_sol:?%d\n", find_no_break_sol);
#endif

  // second pass
  // let's try if we can do the job by break c-thread
  if (!find_no_break_sol) {
    if (cgst_tau_norm_cpu.empty()) {
      return nullptr;
    }

    cur_plan->Reset();
    double agg_splitted_norm_cpu = 0;
    size_t under_idx = 0, cgst_tau_idx = 0;
    pid_t pid;
    int tid;
    double norm_cpu;
    double cur_under_allocated = 0;
    std::tie(pid, tid, norm_cpu) = cgst_tau_norm_cpu[cgst_tau_idx];
    double cur_tau_need_remain = norm_cpu;
    int cur_cid = under_cores[under_idx];
    KPLoadStatsSummary *cur_under_smr = cid_smr_map[cur_cid];
    while (true) {
      if (cur_under_allocated + cur_under_smr->cpu_ut + cur_tau_need_remain <
          cgst_smr->cpu_ut) {
        auto cur_ptr = new LBPlanItem::DstDesc();
        cur_ptr->pid = pid;
        cur_ptr->tid = tid;
        cur_ptr->type = LBPlanItem::ONE_TAU_PCT;
        cur_ptr->pct = cur_tau_need_remain / norm_cpu;
        cur_ptr->dst_norm_cpu_add = norm_cpu;
        cur_ptr->src_norm_cpu_reduce =
            P8_CAL_SRC_CPU_REDUCE(norm_cpu, cgst_smr);
        cur_plan->dst_core_map[cur_cid].push_back(cur_ptr);
#ifdef LB_ALGO_DBG_PRINT
        fprintf(stderr, "pid:%d tid:%d dst:%d norm_cpu:%f\n", pid, tid, cur_cid,
                cur_tau_need_remain);
#endif
        cur_under_allocated += cur_tau_need_remain;
        agg_splitted_norm_cpu += cur_tau_need_remain;
        cgst_tau_idx++;
        if (cgst_tau_idx < cgst_tau_norm_cpu.size()) {
          std::tie(pid, tid, norm_cpu) = cgst_tau_norm_cpu[cgst_tau_idx];
          cur_tau_need_remain =
              std::min(norm_cpu /*need to arrange all of this tau or not?*/,
                       src_norm_cpu_split - agg_splitted_norm_cpu);

#ifdef LB_ALGO_DBG_PRINT
          fprintf(stderr, "cgst_tau_idx:%ld pid:%d tid:%d tau_need_remain:%f\n",
                  cgst_tau_idx, pid, tid, cur_tau_need_remain);
#endif
        } else {
          break;
        }
      } else {
        // let's try to divide this tau
        double cur_under_allow =
            cgst_smr->cpu_ut - cur_under_allocated - cur_under_smr->cpu_ut;
        if (cur_under_allow > 0) {
          auto cur_ptr = new LBPlanItem::DstDesc();
          cur_ptr->pid = pid;
          cur_ptr->tid = tid;
          cur_ptr->pct = cur_under_allow / norm_cpu;
          cur_ptr->type = LBPlanItem::ONE_TAU_PCT;
          cur_ptr->dst_norm_cpu_add = cur_under_allow;
          cur_ptr->src_norm_cpu_reduce =
              P8_CAL_SRC_CPU_REDUCE(cur_under_allow, cgst_smr);
          cur_plan->dst_core_map[cur_cid].push_back(cur_ptr);
          cur_tau_need_remain -= cur_under_allow;
          agg_splitted_norm_cpu += cur_under_allow;
#ifdef LB_ALGO_DBG_PRINT
          fprintf(stderr, "pid:%d tid:%d dst:%d norm_cpu:%f\n", pid, tid,
                  cur_cid, cur_under_allow);
#endif
        }
        under_idx++;
        if (under_idx < under_cores.size()) {
          cur_under_allocated = 0;
          cur_cid = under_cores[under_idx];
          cur_under_smr = cid_smr_map[cur_cid];
#ifdef LB_ALGO_DBG_PRINT
          fprintf(stderr, "under_idx:%ld cid:%d\n", under_idx, cur_cid);
#endif
        } else {
          break;
        }
      }
      if (adjustable_ql - NormCpuToQlen(cgst_smr, agg_splitted_norm_cpu) <
          lb_params.kCongestQlenThr + 0.0001) {
        break;
      }
    }

#ifdef LB_ALGO_DBG_PRINT
    fprintf(stderr, "final_result: agg_splitted_norm:%f agg_ql:%f\n",
            agg_splitted_norm_cpu,
            NormCpuToQlen(cgst_smr, agg_splitted_norm_cpu));
#endif

    // we cannot make it a plan
    if (adjustable_ql - NormCpuToQlen(cgst_smr, agg_splitted_norm_cpu) >=
        lb_params.kCongestQlenThr + 0.0001) {
      fprintf(stdout, "best effort but fail, then do compact\n");
      // ok, we do best effort
      cur_plan->PrunePartialForCompactByPct();
      if (cur_plan->Empty()) {
        cur_plan->Reset();
        delete cur_plan;
        cur_plan = nullptr;
      }
    } else {
      cur_plan->PrunePartialForCompactSuccess();
      assert(!cur_plan->Empty());
    }
  }  // !find_no_break_sol
  return cur_plan;
}

LBPlanItem *KPLoadManager::ExprDecomposeLBPolicy::FormLbMinusPlan(
    const cstats_vec &summaries, KPLoadManager *lm) {
  std::vector<int> congest_cores;
  std::vector<int> under_cores;
  std::map<int, KPLoadStatsSummary *> cid_smr_map;

  CategorizeCores(congest_cores, under_cores, cid_smr_map, summaries);
  if (congest_cores.size() > 0) {
    for (auto cid : congest_cores) {
      UpdateCgstUtAvg(cid_smr_map[cid]->cpu_ut);
    }
    // Don't allow any n_plus if there is congestion
    return nullptr;
  }

  if (under_cores.size() <= 1) {
    return nullptr;
  }

  // Let's try to merge the two least used cores
  std::sort(under_cores.begin(), under_cores.end(),
            [&cid_smr_map](auto a, auto b) -> bool {
              auto a_smr = cid_smr_map[a];
              auto b_smr = cid_smr_map[b];
              // we really don't want to merge into w0, unless there are only 2
              // cores left
              if (a_smr->wid == kMasterWid) return false;
              if (b_smr->wid == kMasterWid) return true;
              const double kZero = 0.0005;
              if (a_smr->cpu_ut < kZero && b_smr->cpu_ut < kZero) {
                // sort by wid to make merging easy
                // so if a core is idle for a while, it will always
                // be there and then as the source of merging
                return a > b;
              }
              return a_smr->cpu_ut < b_smr->cpu_ut;
            });

  // we will merge all traffic from lst0_smr to lst1_smr
  auto lst0_smr = cid_smr_map[under_cores[0]];
  auto lst1_smr = cid_smr_map[under_cores[1]];

  LBPlanItem *cur_plan = nullptr;
  if (lst0_smr->cpu_ut + lst1_smr->cpu_ut < GetCgstUtAvg()) {
    cur_plan = new LBPlanItem();
    cur_plan->src_cid = lst0_smr->wid;
    for (auto [tau_id, ut_pair] : lst0_smr->tau_utilization) {
      pid_t pid;
      int tid;
      DessembleOne64B(tau_id, pid, tid);
      auto cur_ptr = new LBPlanItem::DstDesc();
      cur_ptr->pid = pid;
      cur_ptr->tid = tid;
      cur_ptr->type = LBPlanItem::ONE_TAU_ALL;
      cur_ptr->pct = 1;
      cur_ptr->dst_norm_cpu_add =
          ((ut_pair.first) / lst0_smr->total_ut) * lst0_smr->cpu_ut;
      cur_plan->dst_core_map[lst1_smr->wid].push_back(cur_ptr);
    }
    if (cur_plan->Empty()) {
      auto cur_ptr = new LBPlanItem::DstDesc();
      cur_ptr->type = LBPlanItem::MERGE_SRC_ALL;
      cur_plan->dst_core_map[lst1_smr->wid].push_back(cur_ptr);
      cur_plan->is_merge_all = true;
    }
  }

  return cur_plan;
}

#undef P8_CAL_SRC_CPU_REDUCE

void KPLoadManager::ExprDecomposeLBPolicy::ExecLbPlan(const LBPlanItem &lb_plan,
                                                      KPLoadManager *lm) {
  if (lb_plan.dst_core_map.empty()) {
    SPDLOG_ERROR("ExecLbPlan empty");
    return;
  }
  auto wids = lm->core_states_.GetReadOnlyStateCoreSet(CoreState::ACT_FS_RUN);
  std::vector<FsProcMessage> msg_vec(wids.size() + 1);
  int msg_vec_idx = 0;
  std::map<int, FsProcMessage *> to_send_msg_map;

  auto cur_type = FsProcMessageType::kLM_RedirectFuture;
  auto from_wid = lb_plan.src_cid;
  FsProcMessage *msg_ptr = nullptr;

  msg_ptr = &msg_vec[msg_vec_idx++];
  to_send_msg_map[FsProcWorker::kMasterWidConst] = msg_ptr;
  msg_ptr->type = cur_type;
  auto w0_ctx = new LmMsgRedirectFutureCtx();
  w0_ctx->src_wid = from_wid;
  w0_ctx->pid = w0_ctx->tid = -1;
  for (auto &[cid, v] : lb_plan.dst_core_map) {
    for (auto desc : v) {
      auto norm_pct =
          static_cast<float>(static_cast<int>(desc->pct * 10.)) / 10.;
      if (norm_pct > 0 && norm_pct <= 1) {
        w0_ctx->plan_map[desc->pid][desc->tid][cid] = norm_pct;
      }
    }
  }
  msg_ptr->ctx = w0_ctx;

  msg_ptr = &msg_vec[msg_vec_idx++];
  if (from_wid != FsProcWorker::kMasterWidConst) {
    msg_ptr->type = cur_type;
    auto sec_ctx = new LmMsgRedirectFutureCtx(*w0_ctx);
    msg_ptr->ctx = sec_ctx;
    to_send_msg_map[from_wid] = msg_ptr;
  }

  for (auto notify_wid : wids) {
    if (notify_wid == FsProcWorker::kMasterWidConst) continue;
    if (notify_wid == from_wid) continue;
    msg_ptr = &msg_vec[msg_vec_idx++];
    msg_ptr->type = cur_type;
    auto ntf_ctx = new LmMsgRedirectFutureCtx(*w0_ctx);
    msg_ptr->ctx = ntf_ctx;
    to_send_msg_map[notify_wid] = msg_ptr;
  }

  rt_tb.UpdateItems(w0_ctx->plan_map);

  // send msg
  for (auto [wid, msg_ptr] : to_send_msg_map) {
    lm->SendMsg<LmMsgRedirectFutureCtx>(wid, msg_ptr);
  }

  lm->wait4decision_effect_count_down_ = 10;
}

void KPLoadManager::PerCoreEfficiencyNCPolicy::execute(
    std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries,
    KPLoadManager *lm) {
  auto pre_active_wids =
      lm->core_states_.GetReadOnlyStateCoreSet(CoreState::ACT_FS_RUN);
  int new_active_cid = lm->core_states_.FindInactiveCore();
  std::map<LBPlanType, LBPlanItem *> lb_plans;

  int num_valid_plans = 0;
  auto lb_n_plan = FormLbPlan(summaries, lm);
  lb_plans.emplace(LBPlanType::kLbn, lb_n_plan);
  if (lb_n_plan != nullptr) num_valid_plans++;

  LBPlanItem *lb_nplus_plan = nullptr;
  if (new_active_cid > 0) {
    lb_nplus_plan = FormLbPlan(summaries, lm, new_active_cid);
    if (lb_nplus_plan != nullptr) num_valid_plans++;
  }
  lb_plans.emplace(LBPlanType::kLbnPlus, lb_nplus_plan);

  auto lb_nminus_plan = FormLbMinusPlan(summaries, lm);
  if (lb_nminus_plan != nullptr) num_valid_plans++;
  lb_plans.emplace(LBPlanType::kLbnMinus, lb_nminus_plan);

  auto pick_type = LBPlanType::kLbSkip;
  if (num_valid_plans > 0) {
    pick_type = CompareLbPlan(lb_plans, summaries, lm);
  }
  switch (pick_type) {
    case LBPlanType::kLbn: {
      fprintf(stderr, "exec lbplan ptr:%p\n", lb_n_plan);
      // lb_n_plan->Print(std::cerr);
      ExecLbPlan(*lb_n_plan, lm);
    } break;
    case LBPlanType::kLbnMinus: {
#ifdef UFS_EXPR_LBNC
      if (!lb_nminus_plan->is_merge_all) {
        // TODO: should we allow more merging in the middle
        // maybe not now
        if (lm->eval_round_id_ - nminus_redirect_vid == 1) {
          nminus_redirect_cnt++;
          if (nminus_redirect_cnt == kNumMinusConfidentNum) {
            fprintf(stderr, "exec lbnminus nminus_redirect_cnt:%d\n",
                    nminus_redirect_cnt);
            SPDLOG_INFO("exec lb_nminus_plan");
            ExecLbPlan(*lb_nminus_plan, lm);
            nminus_redirect_cnt = 0;
          }
        } else {
          nminus_redirect_cnt = 0;
        }
        nminus_redirect_vid = lm->eval_round_id_;
      } else {
        TryExecLbMergePlan(*lb_nminus_plan, lm);
      }
#else
      // TODO
#endif
    } break;
    case LBPlanType::kLbnPlus: {
      // lb_nplus_plan->Print(std::cerr);
      gFsProcPtr->setWorkerActive(new_active_cid, true);
      lm->core_states_.UpdateCoreState(new_active_cid, CoreState::SLEEP,
                                       CoreState::ACT_FS_RUN);
      ExecLbPlan(*lb_nplus_plan, lm);
    } break;
    case LBPlanType::kLbSkip:
      break;
  }
  if (lb_n_plan != nullptr) {
    delete lb_n_plan;
  }
  if (lb_nminus_plan != nullptr) {
    delete lb_nminus_plan;
  }
  if (lb_nplus_plan != nullptr) {
    delete lb_nplus_plan;
  }
}

KPLoadManager::LBPlanType
KPLoadManager::PerCoreEfficiencyNCPolicy::CompareLbPlan(
    std::map<LBPlanType, LBPlanItem *> &plans, const cstats_vec &summaries,
    const KPLoadManager *lm) {
  LBPlanType type = LBPlanType::kLbSkip;
  if (kObservePlanOnly) {
    // For experimenting: only want to observe the plan, but
    // not really compares them or enforce any
    for (const auto [tp, cur_plan] : plans) {
      if (tp == LBPlanType::kLbn && cur_plan != nullptr) {
        type = LBPlanType::kLbn;
      }
      if (cur_plan != nullptr) {
        cur_plan->Print(std::cout, GetLbPlanTypeStr(tp));
      }
    }
  } else {
    LBPlanItem *lb_nminus_plan = nullptr, *lb_n_plan = nullptr,
               *lb_nplus_plan = nullptr;
    std::map<int, double> wid_cur_cpu_uts;
    double orig_total_ut = 0;
    double orig_per_core_ut = 0;
    for (const auto [cid, smr] : summaries) {
      wid_cur_cpu_uts[smr->wid] = smr->cpu_ut;
      orig_total_ut += smr->cpu_ut;
    }
    orig_per_core_ut = orig_total_ut / summaries.size();

    // wid, ut
    using expect_uts = std::map<int, double>;
    std::map<LBPlanType, expect_uts> all_plan_results;
    // <num cores, per-core ut>
    std::map<LBPlanType, std::pair<int, double>> all_plan_cpu_usage;
    // summarize all plans
    for (const auto [tp, cur_plan] : plans) {
      if (cur_plan == nullptr) continue;
      if (tp == LBPlanType::kLbnMinus) {
        lb_nminus_plan = cur_plan;
      } else if (tp == LBPlanType::kLbn) {
        lb_n_plan = cur_plan;
      } else if (tp == LBPlanType::kLbnPlus) {
        lb_nplus_plan = cur_plan;
      }
      all_plan_results.emplace(tp, wid_cur_cpu_uts);
      auto &cur_expect_ut_map = all_plan_results[tp];
      int src_cid = cur_plan->src_cid;
      auto src_ut_it = cur_expect_ut_map.find(src_cid);
      if (src_ut_it == cur_expect_ut_map.end()) {
        throw std::runtime_error("src_ut cannot find");
      }
      for (const auto &[dst_cid, desc_vec] : cur_plan->dst_core_map) {
        for (const auto desc : desc_vec) {
          auto cur_dst_ut_it = cur_expect_ut_map.find(dst_cid);
          if (cur_dst_ut_it != cur_expect_ut_map.end()) {
            cur_dst_ut_it->second += desc->dst_norm_cpu_add;
          } else {
            cur_expect_ut_map.emplace(dst_cid, desc->dst_norm_cpu_add);
          }
          src_ut_it->second -= desc->src_norm_cpu_reduce;
          if (src_ut_it->second < 0) {
            for (auto smr : summaries) {
              std::cerr << *(smr.second);
            }
            std::cerr << "plan:" << int(tp) << " src_ut:" << src_ut_it->second
                      << " orig_ut:" << wid_cur_cpu_uts[src_cid] << std::endl;
            cur_plan->Print(std::cerr);
            SPDLOG_ERROR("src_ut becomes negative");
            // throw std::runtime_error("error src_ut becomes negative");
          }
        }
      }
      auto cur_plan_expect_num_core = cur_expect_ut_map.size();
      if (cur_plan_expect_num_core > 0) {
        auto &cur_plan_cpu_usage = all_plan_cpu_usage[tp];
        cur_plan_cpu_usage.first = cur_plan_expect_num_core;
        double cur_total_ut = 0;
        for (const auto [cid, expect_ut] : cur_expect_ut_map) {
          cur_total_ut += expect_ut;
        }
        cur_plan_cpu_usage.second = cur_total_ut / cur_plan_expect_num_core;
        // SPDLOG_INFO(
        //     "tp:{} orig_num_core:{} orig_per_core_ut:{} expect_num_core:{} "
        //     "per_core_cpu:{}",
        //     int(tp), summaries.size(), orig_per_core_ut,
        //     cur_plan_cpu_usage.first, cur_plan_cpu_usage.second);
      }
      cur_plan->Print(std::cout);
    }

    // fprintf(stdout, "minus:%p n_plan:%p plus:%p\n", lb_nminus_plan,
    // lb_n_plan,
    //         lb_nplus_plan);
    if (lb_n_plan != nullptr && lb_nplus_plan != nullptr) {
      // does not matter if there is lb_n_minus, will choose on from lb_n_plan
      // or lb_nplus_plan
      if (all_plan_cpu_usage[LBPlanType::kLbnPlus].second >
          nc_param.kPerCoreUtThr) {
        SPDLOG_INFO("decide to do lbnplus lm_vid:{}", lm->eval_round_id_);
        type = LBPlanType::kLbnPlus;
      } else {
        SPDLOG_INFO("decide to do lbn lm_vid:{}", lm->eval_round_id_);
        type = LBPlanType::kLbn;
      }
    } else {
      if (lb_nplus_plan == nullptr && lb_n_plan == nullptr) {
        // the only chance that we may invoke n_minus
        if (orig_per_core_ut < nc_param.kPerCoreUtThr &&
            lb_nminus_plan != nullptr) {
          // SPDLOG_INFO("decide to do lb_nminus. lm_vid:{}",
          // lm->eval_round_id_);
          type = LBPlanType::kLbnMinus;
        }
      } else {
        // one of nplus, nminus is not null
        if (lb_nplus_plan != nullptr) {
          // have nplus but not n
          SPDLOG_INFO("have nplus but not n. lm_vid:{}", lm->eval_round_id_);
          if (all_plan_cpu_usage[LBPlanType::kLbnPlus].second >
              nc_param.kPerCoreUtThr) {
            type = LBPlanType::kLbnPlus;
          }
        } else {
          // have n but none nplus, e.g., not necessarily to use a new core,
          // will this ever happen?
          SPDLOG_INFO("have n but not nplus lm_vid:{}", lm->eval_round_id_);
          type = LBPlanType::kLbn;
        }
      }
    }
  }
  return type;
}

void KPLoadManager::PerCoreEfficiencyNCPolicy::TryExecLbMergePlan(
    const LBPlanItem &lb_nminus_plan, KPLoadManager *lm) {
  int src_wid = lb_nminus_plan.src_cid;
  if (lm->eval_round_id_ == merge_all_update_vid + 1) {
    auto it = core_merge_all_cnt.find(src_wid);
    if (it == core_merge_all_cnt.end()) {
      core_merge_all_cnt.clear();
      core_merge_all_cnt[src_wid] = 1;
    } else {
      it->second++;
      if (it->second > kNumMergeConfidentNum) {
        auto &dst_core_map = lb_nminus_plan.dst_core_map;
        assert(!dst_core_map.empty());
        auto it = dst_core_map.begin();
        int dst_wid = it->first;
        // auto smr = it->second[0];
        SPDLOG_INFO("ExecLbMerge from:{} to:{}", src_wid, dst_wid);
        auto cur_type = FsProcMessageType::kLM_JoinAll;
        auto ctx = new LmMsgJoinAllCtx();
        FsProcMessage msg;
        ctx->src_wid = src_wid;
        ctx->dst_wid = dst_wid;
        ctx->num_join = 0;
        msg.type = cur_type;
        msg.ctx = ctx;
        lm->SendMsg<LmMsgJoinAllCtx>(dst_wid, &msg);
        lm->wait4decision_effect_count_down_ = 3;
        core_merge_all_cnt.clear();
      }
    }
  } else {
    core_merge_all_cnt.clear();
    core_merge_all_cnt[src_wid] = 1;
  }
  merge_all_update_vid = lm->eval_round_id_;
}

template <typename TCTX>
void KPLoadManager::SendMsg(int dst_wid, FsProcMessage *msg) {
  reinterpret_cast<TCTX *>(msg->ctx)->recv_wid = dst_wid;
  auto it = core_has_msg_sent_.find(dst_wid);
  if (it != core_has_msg_sent_.end()) {
    throw std::runtime_error("core has msg inflight");
  }
  core_has_msg_sent_.emplace(dst_wid,
                             static_cast<FsProcMessageType>(msg->type));
  messenger_->send_message(dst_wid, *msg);
}

}  // namespace fsp_lm
