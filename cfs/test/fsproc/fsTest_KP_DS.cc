
#include "FsProc_KnowParaLoadMng.h"
#include "gtest/gtest.h"

namespace {

using namespace fsp_lm;
using rt_item_t = std::map<pid_t, std::map<int, std::map<int, float>>>;

TEST(LB_MEMLESS_RT_TB, T1) {
  LmMemLessRoutingTable rt_tb;
  rt_item_t item;
  std::map<pid_t, std::set<int>> get_tids;

  item[10][0][1] = 0.2;
  item[10][0][2] = 0.8;
  item[10][1][1] = 1;
  item[11][0][1] = 0.5;
  rt_tb.UpdateItems(item);
  rt_tb.Print(std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(1, get_tids);
  EXPECT_EQ(get_tids.size(), 2);
  rt_tb.PrintGottenTids(1, get_tids, std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(2, get_tids);
  EXPECT_EQ(get_tids.size(), 1);
  rt_tb.PrintGottenTids(2, get_tids, std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(3, get_tids);
  EXPECT_EQ(get_tids.size(), 0);
  rt_tb.PrintGottenTids(3, get_tids, std::cout);

  std::cout << ">>>>>>>>" << std::endl;
  item.clear();
  item[10][0][3] = 1;
  item[11][0][2] = 0.2;
  item[11][1][3] = 0.2;
  item[11][1][2] = 0.5;
  rt_tb.UpdateItems(item);
  rt_tb.Print(std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(1, get_tids);
  EXPECT_EQ(get_tids.size(), 1);
  rt_tb.PrintGottenTids(1, get_tids, std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(2, get_tids);
  EXPECT_EQ(get_tids.size(), 1);
  rt_tb.PrintGottenTids(2, get_tids, std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(3, get_tids);
  EXPECT_EQ(get_tids.size(), 2);
  rt_tb.PrintGottenTids(3, get_tids, std::cout);

  std::cout << ">>>>>>>>" << std::endl;
  item.clear();
  item[10][1][2] = 1;
  rt_tb.UpdateItems(item);
  rt_tb.Print(std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(1, get_tids);
  EXPECT_EQ(get_tids.size(), 0);
  rt_tb.PrintGottenTids(1, get_tids, std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(2, get_tids);
  EXPECT_EQ(get_tids.size(), 2);
  rt_tb.PrintGottenTids(2, get_tids, std::cout);
  get_tids.clear();
  rt_tb.GetWidHandling(3, get_tids);
  EXPECT_EQ(get_tids.size(), 2);
  rt_tb.PrintGottenTids(3, get_tids, std::cout);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}