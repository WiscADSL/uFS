
#include "FsProc_Lease.h"

#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"

void FsProcWorkerLeaseMng::initLeaseRecord(cfs_ino_t ino, pid_t pid,
                                           FsProcLeaseType typ) {
  SPDLOG_DEBUG("initLeaseRecord for ino:{} pid:{} type:{}", ino, pid,
               ((int)typ - (int)FsProcLeaseType::READ_SHARE));
  FsProcWorkerInodeLeaseCtx* ctxPtr = nullptr;

  // make sure there is no existing lease on this pair of [ino,pid]
  FsProcLeaseType tmpTyp;
  bool found = queryValidExistingLease(ino, tmpTyp);
  if (found) assert(tmpTyp != FsProcLeaseType::WRITE_EXCLUSIVE);

  // make sure there is no write lease on this inode
  ctxPtr = queryValidExistingLeaseForApp(ino, pid);
  assert(ctxPtr == nullptr);

  if (typ == FsProcLeaseType::READ_SHARE) {
    std::unordered_map<pid_t, FsProcWorkerInodeLeaseCtx*>* rmapPtr;
    auto rmapIt = readLeaseRecordsMap_.find(ino);
    if (rmapIt == readLeaseRecordsMap_.end()) {
      readLeaseRecordsMap_.emplace(
          ino, std::unordered_map<pid_t, FsProcWorkerInodeLeaseCtx*>());
    }
    rmapPtr = &(readLeaseRecordsMap_[ino]);
    ctxPtr = new FsProcWorkerInodeLeaseCtx(ino, pid);
    rmapPtr->emplace(pid, ctxPtr);
  } else if (typ == FsProcLeaseType::WRITE_EXCLUSIVE) {
    ctxPtr = new FsProcWorkerInodeLeaseCtx(ino, pid);
    writeLeaseRecordMap_.emplace(ino, ctxPtr);
  } else {
    SPDLOG_ERROR("Lease Type Not Supported");
    return;
  }
  ctxPtr->type = typ;
  ctxPtr->startTs = FsLeaseCommon::genTimestamp();
}

bool FsProcWorkerLeaseMng::queryValidExistingLease(cfs_ino_t ino,
                                                   FsProcLeaseType& type) {
  type = FsProcLeaseType::UNKNOWN;
  bool isWriteLeaseFound = false;
  bool found = false;

  auto wit = writeLeaseRecordMap_.find(ino);
  if (wit != writeLeaseRecordMap_.end()) {
    isWriteLeaseFound = true;
    found = true;
    type = FsProcLeaseType::WRITE_EXCLUSIVE;
  }

  auto it = readLeaseRecordsMap_.find(ino);
  if (it != readLeaseRecordsMap_.end()) {
    if (isWriteLeaseFound) {
      SPDLOG_ERROR("Read and Write Lease both found for this ino:{}", ino);
      throw std::runtime_error("Lease Duplicated");
    }
    type = FsProcLeaseType::READ_SHARE;
    found = true;
  }
  return found;
}

FsProcWorkerInodeLeaseCtx* FsProcWorkerLeaseMng::queryValidExistingLeaseForApp(
    cfs_ino_t ino, pid_t pid) {
  FsProcWorkerInodeLeaseCtx* ctxPtr = nullptr;
  bool isWriteLeaseFound = false;

  auto wit = writeLeaseRecordMap_.find(ino);
  if (wit != writeLeaseRecordMap_.end()) {
    isWriteLeaseFound = true;
    if (pid > 0 && wit->second->pid == pid) {
      ctxPtr = (wit->second);
    }
  }
  auto it = readLeaseRecordsMap_.find(ino);
  if (it != readLeaseRecordsMap_.end()) {
    if (isWriteLeaseFound) {
      SPDLOG_ERROR("Read and Write Lease both found for this ino:{}", ino);
      throw std::runtime_error("Lease Duplicated");
    }
    if (pid > 0) {
      auto rit = it->second.find(pid);
      if (rit != it->second.end()) {
        ctxPtr = (rit->second);
      }
    }
  }
  return ctxPtr;
}

int FsProcWorkerLeaseMng::tryExtendLease(FsProcWorkerInodeLeaseCtx* ctxPtr) {
  ctxPtr->startTs = FsLeaseCommon::genTimestamp();
  return 0;
}

int FsProcWorkerLeaseMng::tryInvalidateLease(
    FsProcWorkerInodeLeaseCtx* ctxPtr) {
  switch (ctxPtr->type) {
    case FsProcLeaseType::READ_SHARE: {
      auto it = &(readLeaseRecordsMap_.find(ctxPtr->ino)->second);
      it->erase(ctxPtr->pid);
      delete ctxPtr;
      break;
    }
    case FsProcLeaseType::WRITE_EXCLUSIVE: {
      writeLeaseRecordMap_.erase(ctxPtr->pid);
      delete ctxPtr;
      break;
    }
    default:
      SPDLOG_ERROR("Lease Type Not Supported");
  }
  return 0;
}