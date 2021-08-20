#ifndef CFS_INCLUDE_FSPROC_LEASE_H_
#define CFS_INCLUDE_FSPROC_LEASE_H_

#include <unordered_map>

#include "FsLibLeaseShared.h"
#include "typedefs.h"

enum class FsProcLeaseType {
  UNKNOWN = 0,
  READ_SHARE = 1,
  WRITE_EXCLUSIVE = 2,
};

struct FsProcWorkerInodeLeaseCtx {
  explicit FsProcWorkerInodeLeaseCtx(cfs_ino_t i, pid_t p) : ino(i), pid(p) {}
  cfs_ino_t ino;
  pid_t pid;
  FsLeaseCommon::rdtscmp_ts_t startTs{0};
  FsProcLeaseType type{FsProcLeaseType::READ_SHARE};
};

// Lease is granted at an inode (file) granularity
// Lease type see *FsProcLeaseType*
//   Basically, READ lease can be granted to several apps for file sharing
//   while WRITE lease is absolutely exclusive
class FsProcWorkerLeaseMng {
 public:
  FsProcWorkerLeaseMng() { lc = FsLeaseCommon::getInstance(); }

  // REQUIRED: this ino must NOT has any lease w/ pid when calling this
  void initLeaseRecord(cfs_ino_t ino, pid_t pid, FsProcLeaseType typ);

  bool queryValidExistingLease(cfs_ino_t ino, FsProcLeaseType &type);

  FsProcWorkerInodeLeaseCtx *queryValidExistingLeaseForApp(cfs_ino_t ino,
                                                           pid_t pid);

  bool ifLeaseExpire(FsProcWorkerInodeLeaseCtx *ctxPtr) {
    return lc->checkIfLeaseExpire(ctxPtr->startTs,
                                  FsLeaseCommon::genTimestamp());
  }

  // extend lease for App(pid) on *ino*
  // @return -1 if error
  // REQUIRED: caller must make sure it hold the valid lease first
  int tryExtendLease(FsProcWorkerInodeLeaseCtx *ctxPtr);

  int tryInvalidateLease(FsProcWorkerInodeLeaseCtx *ctxPtr);

 private:
  std::unordered_map<cfs_ino_t,
                     std::unordered_map<pid_t, FsProcWorkerInodeLeaseCtx *>>
      readLeaseRecordsMap_;
  std::unordered_map<cfs_ino_t, FsProcWorkerInodeLeaseCtx *>
      writeLeaseRecordMap_;
  FsLeaseCommon *lc;
};

#endif  // CFS_INCLUDE_FSPROC_LEASE_H_
