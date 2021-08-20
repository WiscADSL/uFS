#include <iostream>
#include "FsProc_Fs.h"
#include "FsProc_KnowParaLoadMng.h"
#include "gtest/gtest.h"

FsProc *gFsProcPtr = nullptr;

///////
// @brief NOTE: Need to check with `valgrind --leak-check=yes`
//
///////

namespace {
using namespace fsp_lm;
using pid_tid_ut_vec_t =
    std::vector<std::map<pid_t, std::map<int, std::pair<float, uint64_t>>>>;

void ClearSummaries(
    std::vector<std::pair<int, KPLoadStatsSummary *>> &summaries) {
  for (auto cur_pair : summaries) {
    delete cur_pair.second;
  }
  summaries.clear();
}

LBPlanItem *TestFormLbPlanCommon(KPLoadManager::ExprDecomposeLBPolicy &policy,
                                 int nwk,
                                 std::vector<float> &recv_noshare_qlen_vec,
                                 std::vector<float> &cpu_ut_vec,
                                 pid_tid_ut_vec_t &per_tau_ut) {
  KPLoadManager lm(10);
  auto core_states = lm.TestOnlyGetCoreStateWrapper();
  core_states->UpdateCoreState(kMasterWid, CoreState::SLEEP,
                               CoreState::ACT_FS_RUN);
  std::vector<std::pair<int, KPLoadStatsSummary *>> summaries;

  for (int i = 0; i < nwk; i++) {
    auto cur_smr = new KPLoadStatsSummary(0, 110000);
    double total_ut = 0;
    cur_smr->window_cycles = 10000;
    cur_smr->recv_noshare_qlen = recv_noshare_qlen_vec[i];
    cur_smr->cpu_ut = cpu_ut_vec[i];
    summaries.push_back({i, cur_smr});
    for (auto [pid, tid_map] : per_tau_ut[i]) {
      for (auto [tid, ut_pair] : tid_map) {
        auto tau = AssembleTwo32B(pid, tid);
        cur_smr->tau_utilization[tau] = ut_pair;
        total_ut += ut_pair.first;
      }
    }
    cur_smr->total_ut = total_ut;
    cur_smr->est_include_wait_cycles =
        cur_smr->recv_noshare_qlen * cur_smr->window_cycles * total_ut;
  }
  auto plan_n = policy.FormLbPlan(summaries, &lm);
  ClearSummaries(summaries);
  return plan_n;
}

TEST(LB_PLAN, c1) {
  KPLoadManager::ExprDecomposeLBPolicy policy;
  policy.lb_params.kCongestQlenThr = 1.3;
  int num_worker = 3;
  std::vector<float> recv_noshare_qlen_vec = {1.35, 1.1, 1};
  std::vector<float> cpu_ut_vec = {0.9, 0.5, 0.1};
  pid_tid_ut_vec_t per_tau_ut(num_worker);
  per_tau_ut[0] = {
      {9000,
       {{1, std::make_pair(0.2, 0)},
        {2, std::make_pair(0.2, 0)},
        {3, std::make_pair(0.2, 0)},
        {4, std::make_pair(0.1, 0)}}},
  };
  per_tau_ut[1] = {
      {9001, {{1, std::make_pair(0.2, 0)}, {2, std::make_pair(0.2, 0)}}},
  };
  per_tau_ut[2] = {
      {9002, {{1, std::make_pair(0.1, 0)}}},
  };
  auto lb_plan = TestFormLbPlanCommon(policy, num_worker, recv_noshare_qlen_vec,
                                      cpu_ut_vec, per_tau_ut);
  ASSERT_NE(lb_plan, nullptr);
  lb_plan->Print(std::cout);
  ASSERT_EQ(lb_plan->src_cid, 0);
  ASSERT_EQ(lb_plan->dst_core_map.size(), 1);
  ASSERT_EQ(lb_plan->dst_core_map[2].size(), 1);
  delete lb_plan;
}

TEST(LB_PLAN, c2) {
  KPLoadManager::ExprDecomposeLBPolicy policy;
  policy.lb_params.kCongestQlenThr = 1.2;
  int num_worker = 3;
  std::vector<float> recv_noshare_qlen_vec = {1.05, 1.1, 1.15};
  std::vector<float> cpu_ut_vec = {0.7, 0.8, 0.85};
  pid_tid_ut_vec_t per_tau_ut(num_worker);
  per_tau_ut[0] = {
      {9000, {{1, std::make_pair(0.2, 0)}, {2, std::make_pair(0.3, 0)}}},
  };
  per_tau_ut[1] = {
      {9001, {{1, std::make_pair(0.3, 0)}, {2, std::make_pair(0.25, 0)}}},
  };
  per_tau_ut[2] = {
      {9002, {{1, std::make_pair(0.3, 0)}, {2, std::make_pair(0.35, 0)}}},
  };

  auto lb_plan = TestFormLbPlanCommon(policy, num_worker, recv_noshare_qlen_vec,
                                      cpu_ut_vec, per_tau_ut);
  ASSERT_EQ(lb_plan, nullptr);
}

TEST(LB_PLAN, c3) {
  KPLoadManager::ExprDecomposeLBPolicy policy;
  policy.lb_params.kCongestQlenThr = 1.2;
  int num_worker = 1;
  std::vector<float> recv_noshare_qlen_vec = {1.25};
  std::vector<float> cpu_ut_vec = {0.7};
  pid_tid_ut_vec_t per_tau_ut(num_worker);
  per_tau_ut[0] = {
      {9000, {{1, std::make_pair(0.25, 0)}, {2, std::make_pair(0.3, 0)}}},
  };

  auto lb_plan = TestFormLbPlanCommon(policy, num_worker, recv_noshare_qlen_vec,
                                      cpu_ut_vec, per_tau_ut);
  ASSERT_EQ(lb_plan, nullptr);
}

TEST(LB_PLAN, c4) {
  KPLoadManager::ExprDecomposeLBPolicy policy;
  policy.lb_params.kCongestQlenThr = 1.2;
  int num_worker = 1;
  std::vector<float> recv_noshare_qlen_vec = {1};
  std::vector<float> cpu_ut_vec = {0.3};
  pid_tid_ut_vec_t per_tau_ut(num_worker);
  per_tau_ut[0] = {
      {9000, {{1, std::make_pair(0.1, 0)}, {2, std::make_pair(0.05, 0)}}},
  };

  TestFormLbPlanCommon(policy, num_worker, recv_noshare_qlen_vec, cpu_ut_vec,
                       per_tau_ut);
}

TEST(LB_PLAN, c5) {
  KPLoadManager::ExprDecomposeLBPolicy policy;
  policy.lb_params.kCongestQlenThr = 1.2;
  int num_worker = 4;
  std::vector<float> recv_noshare_qlen_vec = {1.85, 1.1, 1, 1};
  std::vector<float> cpu_ut_vec = {0.9, 0.5, 0.2, 0.15};
  pid_tid_ut_vec_t per_tau_ut(num_worker);
  per_tau_ut[0] = {
      {9000,
       {{1, std::make_pair(0.25, 0)},
        {2, std::make_pair(0.22, 0)},
        {3, std::make_pair(0.18, 0)},
        {4, std::make_pair(0.15, 0)}}},
  };
  per_tau_ut[1] = {
      {9001, {{1, std::make_pair(0.2, 0)}, {2, std::make_pair(0.2, 0)}}},
  };
  per_tau_ut[2] = {
      {9002, {{1, std::make_pair(0.18, 0)}}},
  };
  per_tau_ut[3] = {
      {9003, {{1, std::make_pair(0.1, 0)}}},
  };
  auto lb_plan = TestFormLbPlanCommon(policy, num_worker, recv_noshare_qlen_vec,
                                      cpu_ut_vec, per_tau_ut);
  ASSERT_NE(lb_plan, nullptr);
  lb_plan->Print(std::cout);
  ASSERT_EQ(lb_plan->src_cid, 0);
  ASSERT_EQ(lb_plan->dst_core_map.size(), 2);
  ASSERT_EQ(lb_plan->dst_core_map[2].size(), 2);
  ASSERT_EQ(lb_plan->dst_core_map[3].size(), 2);
  delete lb_plan;
}

// must break one c-thread
TEST(LB_PLAN, c6) {
  KPLoadManager::ExprDecomposeLBPolicy policy;
  policy.lb_params.kCongestQlenThr = 1.2;
  int num_worker = 4;
  std::vector<float> recv_noshare_qlen_vec = {2.4, 1.1, 1, 1};
  std::vector<float> cpu_ut_vec = {0.9, 0.5, 0.2, 0.15};
  pid_tid_ut_vec_t per_tau_ut(num_worker);
  per_tau_ut[0] = {
      {9000, {{1, std::make_pair(0.7, 0)}, {4, std::make_pair(0.1, 0)}}},
  };
  per_tau_ut[1] = {
      {9001, {{1, std::make_pair(0.2, 0)}, {2, std::make_pair(0.2, 0)}}},
  };
  per_tau_ut[2] = {
      {9002, {{1, std::make_pair(0.18, 0)}}},
  };
  per_tau_ut[3] = {
      {9003, {{1, std::make_pair(0.1, 0)}}},
  };
  auto lb_plan = TestFormLbPlanCommon(policy, num_worker, recv_noshare_qlen_vec,
                                      cpu_ut_vec, per_tau_ut);
  ASSERT_NE(lb_plan, nullptr);
  lb_plan->Print(std::cout);
  ASSERT_EQ(lb_plan->src_cid, 0);
  ASSERT_EQ(lb_plan->dst_core_map.size(), 2);
  ASSERT_EQ(lb_plan->dst_core_map[3].size(), 1);
  ASSERT_EQ(lb_plan->dst_core_map[2].size(), 1);
  delete lb_plan;
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}