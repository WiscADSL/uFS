#ifndef CFSTEST_FSFUNCTESTCOMMON_H
#define CFSTEST_FSFUNCTESTCOMMON_H

#include <vector>

void inline initFsProc(FsProc **fspPtr, BlkDevSpdk **devPtr,
                       FsProcWorker **workerPtr, std::vector<int> &allTestsDone,
                       int testCaseNum) {
  if (*fspPtr == nullptr && *devPtr == nullptr) {
    // initialize device
    *devPtr = new BlkDevSpdk("", DEV_SIZE / BSIZE, BSIZE);
    *fspPtr = new FsProc(/*numWorker*/ 1, /*appnum*/ 1, "/tmp/fsp_test_exit");
    *workerPtr = (*fspPtr)->startInlineWorker(*devPtr, true);
    allTestsDone.clear();
    for (int i = 0; i < testCaseNum; i++) {
      allTestsDone.push_back(0);
    }
  }
}

void inline tearDownFsProc(FsProc **fspPtr, BlkDevSpdk **devPtr,
                           FsProcWorker **workerPtr,
                           std::vector<int> &allTestsDone) {
  // test if all the test cases are done, otherwise do not do testDown().
  for (auto val : allTestsDone) {
    if (!val) {
      return;
    }
  }

  // actually tear down the runtime
  (*fspPtr)->stop();
  delete *devPtr;
  *fspPtr = nullptr;
  *devPtr = nullptr;
  *workerPtr = nullptr;
} // namespace fsfunctest

#endif // CFSTEST_FSFUNCTESTCOMMON_H
