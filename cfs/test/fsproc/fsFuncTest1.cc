#include "BlkDevSpdk.h"
#include "FsLibApp.h"
#include "FsProc_Fs.h"
#include "fsFuncTestCommon.h"
#include "gtest/gtest.h"

// The unit tests for fs functions.
// This is single-thread test cases and the main purpose is testing
// functionality.
// REQUIRED: mkfs is called and several test file is created.

// global fs object
FsProc *gFsProcPtr = nullptr;
FsProcWorker *gFsWorker = nullptr;
BlkDevSpdk *devPtr = nullptr;

namespace {

std::vector<int> allTestsDone;
int kTestCaseNum = 3;

constexpr char kStatFilePath[] = "/t0";
constexpr char kWRFilePath[] = "/t1";
constexpr char kWRLargeFilePath[] = "/t2";
constexpr int kWriteLen = 100;
constexpr double kWRLargeFileSize = 1200000;
constexpr int kLargeWriteLen = 30000;
char gWriteBuf[kWriteLen];
char gLargeWriteBuf[kLargeWriteLen];

void fillWriteBuf(char *bufPtr, int len) {
  for (int i = 0; i < len; i++) {
    bufPtr[i] = (i % 50) + 'B';
  }
}

// for the file pointed by kStatFilePath, stat().
// NOTE: if T1 is run after T2, it will not be executed.
//        because T2 will call tearDownFsProc() and set gFsProcPtr to nullptr.
TEST(FS_FUNC_TEST1, T1) {
  initFsProc(&gFsProcPtr, &devPtr, &gFsWorker, allTestsDone, kTestCaseNum);
  // We do not allow gFsProcPtr to be initialized twice.
  if (gFsProcPtr != nullptr) {
    EXPECT_NE(gFsWorker, nullptr);

    auto app = gFsWorker->testOnlyGetAppProc(0);

    MockAppProc mockApp(app);
    int curSid = 0;
    auto cop = mockApp.emulateGetClientOp(curSid);
    struct stat statbuf;
    fillStatOp(cop, /*path*/ kStatFilePath, &statbuf);
    mockApp.emulateClientOpSubmit(curSid);

    int fsWorkerRecvSid = 0;
    cfs_tid_t curTid = cfsGetTid();
    FsWorkerLoadStatsLocal lpstats;

    auto startTs = tap_ustime();
    auto endTs = tap_ustime();

    int loopEffectiveNum = 0;
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }

    // the return value of fs_stat() is 0 for normal case.
    EXPECT_EQ(cop->op.stat.ret, 0);
    EXPECT_EQ(cop->op.stat.statbuf.st_size, 0);
    EXPECT_GT(loopEffectiveNum, 0);

    allTestsDone[0] = 1;
    tearDownFsProc(&gFsProcPtr, &devPtr, &gFsWorker, allTestsDone);
  }
}

// for the file pointed by kWRFilePath.
// open(), pwrite() and pread().
TEST(FS_FUNC_TEST1, T2) {
  initFsProc(&gFsProcPtr, &devPtr, &gFsWorker, allTestsDone, kTestCaseNum);
  EXPECT_NE(gFsWorker, nullptr);

  int64_t startTs, endTs;
  int curSid = 0;
  int fsWorkerRecvSid = 0;
  int loopEffectiveNum = 0;
  cfs_tid_t curTid = cfsGetTid();
  FsWorkerLoadStatsLocal lpstats;
  auto app = gFsWorker->testOnlyGetAppProc(0);
  MockAppProc mockApp(app);

  auto cop = mockApp.emulateGetClientOp(curSid);

  // open the file
  fillOpenOp(cop, kWRFilePath, O_RDWR, 0);
  mockApp.emulateClientOpSubmit(curSid);

  {
    startTs = tap_ustime();
    endTs = tap_ustime();
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }
  }
  int fd = cop->op.open.ret;

  EXPECT_TRUE(fd >= 0);

  // write to the file
  fillPWriteOp(cop, fd, kWriteLen, /*offset*/ 0);
  char *slotDataPtr = (char *)(mockApp.emulateGetDataBufPtr(curSid));
  EXPECT_EQ(slotDataPtr[0], (char)0);
  fillWriteBuf(gWriteBuf, kWriteLen);
  memcpy(slotDataPtr, gWriteBuf, kWriteLen);
  mockApp.emulateClientOpSubmit(curSid);

  {
    startTs = tap_ustime();
    endTs = tap_ustime();
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }
  }
  // check return value of pwrite()
  EXPECT_EQ(cop->op.pwrite.rwOp.ret, kWriteLen);

  // read the result back
  fillPreadOp(cop, fd, kWriteLen, 0);
  slotDataPtr = (char *)(mockApp.emulateGetDataBufPtr(curSid));
  EXPECT_EQ(slotDataPtr[0], (char)0);
  mockApp.emulateClientOpSubmit(curSid);

  {
    startTs = tap_ustime();
    endTs = tap_ustime();
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }
  }

  // check return value of pread
  EXPECT_EQ(cop->op.pread.rwOp.ret, kWriteLen);
  for (int i = 0; i < kWriteLen; i++) {
    EXPECT_EQ(slotDataPtr[i], gWriteBuf[i]);
  }

  allTestsDone[1] = 1;
  tearDownFsProc(&gFsProcPtr, &devPtr, &gFsWorker, allTestsDone);
}

//  write and read large file
TEST(FS_FUNC_TEST1, T3) {
  initFsProc(&gFsProcPtr, &devPtr, &gFsWorker, allTestsDone, kTestCaseNum);
  EXPECT_NE(gFsWorker, nullptr);

  int64_t startTs, endTs;
  int curSid = 0;
  int fsWorkerRecvSid = 0;
  int loopEffectiveNum = 0;
  cfs_tid_t curTid = cfsGetTid();
  FsWorkerLoadStatsLocal lpstats;
  auto app = gFsWorker->testOnlyGetAppProc(0);
  MockAppProc mockApp(app);

  auto cop = mockApp.emulateGetClientOp(curSid);

  // open the file
  fillOpenOp(cop, kWRLargeFilePath, O_RDWR, 0);
  mockApp.emulateClientOpSubmit(curSid);

  {
    startTs = tap_ustime();
    endTs = tap_ustime();
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }
  }
  int fd = cop->op.open.ret;

  EXPECT_TRUE(fd >= 0);

  // write to the file
  fillWriteOp(cop, fd, kLargeWriteLen);
  char *slotDataPtr = (char *)(mockApp.emulateGetDataBufPtr(curSid));
  EXPECT_EQ(slotDataPtr[0], (char)0);
  fillWriteBuf(gLargeWriteBuf, kLargeWriteLen);
  memcpy(slotDataPtr, gLargeWriteBuf, kLargeWriteLen);

  fprintf(stderr, "=======\nwrite(append) to the large file\n=======\n");
  for (int i = 0; i < (kWRLargeFileSize / kLargeWriteLen); i++) {
    mockApp.emulateClientOpSubmit(curSid);
    fprintf(stderr, "=======:write:%d\n", i);
    {
      startTs = tap_ustime();
      endTs = tap_ustime();
      while ((endTs - startTs) <= 1e6) {
        gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
        if (lpstats.loopEffective) {
          loopEffectiveNum += 1;
        }
        endTs = tap_ustime();
      }
    }
    // check return value of write()
    EXPECT_EQ(cop->op.write.rwOp.ret, kLargeWriteLen);
  }

  // close the file (reset the offset)
  fillCloseOp(cop, fd);
  mockApp.emulateClientOpSubmit(curSid);
  {
    startTs = tap_ustime();
    endTs = tap_ustime();
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }
  }
  EXPECT_EQ(cop->op.close.ret, 0);

  // reopen the file
  fillOpenOp(cop, kWRLargeFilePath, O_RDWR, 0);
  mockApp.emulateClientOpSubmit(curSid);

  {
    startTs = tap_ustime();
    endTs = tap_ustime();
    while ((endTs - startTs) <= 1e6) {
      gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
      if (lpstats.loopEffective) {
        loopEffectiveNum += 1;
      }
      endTs = tap_ustime();
    }
  }
  fd = cop->op.open.ret;
  EXPECT_TRUE(fd >= 0);

  // read the result back
  fillReadOp(cop, fd, kLargeWriteLen);
  slotDataPtr = (char *)(mockApp.emulateGetDataBufPtr(curSid));
  EXPECT_EQ(slotDataPtr[0], (char)0);

  fprintf(stderr, "=======\nread the large file\n=======\n");
  for (int i = 0; i < (kWRLargeFileSize / kLargeWriteLen); i++) {
    mockApp.emulateClientOpSubmit(curSid);
    fprintf(stderr, "=======:read:%d\n", i);
    {
      startTs = tap_ustime();
      endTs = tap_ustime();
      while ((endTs - startTs) <= 1e6) {
        gFsWorker->workerRunLoopInner(&fsWorkerRecvSid, curTid, lpstats);
        if (lpstats.loopEffective) {
          loopEffectiveNum += 1;
        }
        endTs = tap_ustime();
      }
    }
    // check return value of read
    EXPECT_EQ(cop->op.read.rwOp.ret, kLargeWriteLen);
    for (int j = 0; j < kLargeWriteLen; j++) {
      ASSERT_EQ(slotDataPtr[j], gLargeWriteBuf[j]);
    }
  }

  allTestsDone[2] = 1;
  tearDownFsProc(&gFsProcPtr, &devPtr, &gFsWorker, allTestsDone);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}