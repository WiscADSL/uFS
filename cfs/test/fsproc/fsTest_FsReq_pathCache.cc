
#include "FsProc_Fs.h"
#include "gtest/gtest.h"

FsProc *gFsProcPtr = nullptr;

namespace {

TEST(GET_STD_FULL_PATH, T1) {
  clientOp cop;
  cop.opCode = CFS_OP_TEST;
  strcpy(cop.op.test.path, "snake/monkey/tea/foo");
  FsReq req(FsProcWorker::kMasterWidConst);
  req.initReqFromCop(nullptr, 0, &cop, nullptr);

  int curDepth;
  auto fullPath = req.getStandardFullPath(curDepth);
  EXPECT_EQ(fullPath, std::string("snake/monkey/tea/foo"));
  EXPECT_EQ(curDepth, 4);

  auto parPath = req.getStandardParPath(curDepth);
  EXPECT_EQ(parPath, std::string("snake/monkey/tea"));
  EXPECT_EQ(curDepth, 3);

  std::string leafName = "monkey";
  auto partialPath = req.getStandardPartialPath(leafName, curDepth);
  EXPECT_EQ(curDepth, 2);
  EXPECT_EQ(partialPath, std::string("snake/monkey"));

  leafName = "noop";
  partialPath = req.getStandardPartialPath(leafName, curDepth);
  EXPECT_EQ(curDepth, -1);

  leafName = "foo";
  partialPath = req.getStandardPartialPath(leafName, curDepth);
  EXPECT_EQ(curDepth, 4);
}

TEST(GET_STD_FULL_PATH, T2) {
  clientOp cop;
  cop.opCode = CFS_OP_TEST;
  strcpy(cop.op.test.path, "abc");
  FsReq req(FsProcWorker::kMasterWidConst);
  req.initReqFromCop(nullptr, 0, &cop, nullptr);

  int curDepth;
  auto fullPath = req.getStandardFullPath(curDepth);
  EXPECT_EQ(fullPath, std::string("abc"));
  EXPECT_EQ(curDepth, 1);

  auto parPath = req.getStandardParPath(curDepth);
  EXPECT_EQ(parPath, std::string(""));
  EXPECT_EQ(curDepth, 0);
}

TEST(GET_STD_FULL_PATH, T3) {
  clientOp cop;
  cop.opCode = CFS_OP_TEST;
  strcpy(cop.op.test.path, "/");
  FsReq req(FsProcWorker::kMasterWidConst);
  req.initReqFromCop(nullptr, 0, &cop, nullptr);

  int curDepth;
  auto fullPath = req.getStandardFullPath(curDepth);
  EXPECT_EQ(fullPath, std::string(""));
  EXPECT_EQ(curDepth, 0);

  auto parPath = req.getStandardParPath(curDepth);
  EXPECT_EQ(parPath, std::string(""));
  EXPECT_EQ(curDepth, -1);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
