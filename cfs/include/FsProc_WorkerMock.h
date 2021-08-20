#ifndef CFS_FSPROCMOCK_H
#define CFS_FSPROCMOCK_H

#include "FsProc_Fs.h"

// A mock class that will not use the devices and never perform FS functions
// It will directly echo back the result for each request
// This is mainly to test if FsLib works properly.
// It will use the Posix Backend Block Device instead of SPDK
class FsProcWorkerMock : public FsProcWorker {
 public:
  FsProcWorkerMock(int wid, int shmBaseOffset, std::string exitSignalFileName)
      : FsProcWorker(wid, nullptr, shmBaseOffset, nullptr, nullptr) {}
  ~FsProcWorkerMock();
  virtual int checkSplitJoinComm() final { return 0; };
  virtual bool isFsReqTypeWithinCapability(const FsReqType &typ) final {
    return true;
  };
  virtual bool isFsReqNotRecord(const FsReqType &typ) override { return false; }
  virtual int initInMemDataAfterDevReady() final { return 0; }
  virtual int onTargetInodeFiguredOutChildBookkeep(
      FsReq *req, InMemInode *targetInode) override {
    return 0;
  }
  virtual void workerRunLoop() final;
  virtual bool workerRunLoopInner(int *dstSidPtr, cfs_tid_t curTid,
                                  FsWorkerLoadStatsLocal &lpStats) final;

 private:
  int initMakeDevReady();
};

#endif  // CFS_FSPROCMOCK_H
