
#include "FsProc_WorkerMock.h"
#include "FsProc_SplitPolicy.h"

FsProcWorkerMock::~FsProcWorkerMock() {
  // TODO
}

void FsProcWorkerMock::workerRunLoop() {}

bool FsProcWorkerMock::workerRunLoopInner(int *dstSidPtr, cfs_tid_t curTid,
                                          FsWorkerLoadStatsLocal &lpStats) {
  return true;
}

int FsProcWorkerMock::initMakeDevReady() { return 0; }
