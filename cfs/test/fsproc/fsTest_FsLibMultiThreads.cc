
#include "FsProc_WorkerMock.h"
#include "gtest/gtest.h"

// global fs object (simply to avoid the g++ error)
FsProc *gFsProcPtr = nullptr;

namespace {

class FsLibMockTestLauncher : public ::testing::Test {
 protected:
  FsLibMockTestLauncher() {}
  void SetUp() override {
    workerMock = new FsProcWorkerMock(kWid, kShmOffset, (kExitSignalFileName));
    if (fork() == 0) {
      // mock process
      workerMock->workerRunLoop();
    } else {
      // test process
    }
  }
  void TearDown() override { *curRunning = false; }

  constexpr static int kWid = 0;
  constexpr static int kShmOffset = 1;
  constexpr static char kExitSignalFileName[] = "MockExitSignal";
  FsProcWorkerMock *workerMock = nullptr;
  std::atomic_bool *curRunning = nullptr;
};

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}