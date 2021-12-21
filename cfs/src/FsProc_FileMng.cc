#include <stdio.h>

#include <algorithm>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <tuple>

#include "FsProc_Fs.h"
#include "FsProc_Journal.h"
#include "FsProc_Messenger.h"
#include "perfutil/Cycles.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "stats/stats.h"

#if CFS_JOURNAL(ON)
#include "FsReq_SyncBatchCtx.h"
#endif

extern FsProc *gFsProcPtr;

//
// the memPtr should has super block been fetched in the initialization
// of FsProc
// @param memPtr
//
FileMng::FileMng(FsProcWorker *worker, const char *memPtr,
                 float dataDirtyFlushRatio, int numPartitions)
    : fsWorker_(worker), memPtr_(memPtr) {
  if (worker->isMasterWorker()) {
    fsImpl_ = new FsImpl(worker, memPtr, dataDirtyFlushRatio, numPartitions);
  } else {
    SPDLOG_ERROR(
        "FileMng constructor is called for whole memPtr but not master. "
        "FORBIDDEN");
  }
}

FileMng::FileMng(FsProcWorker *worker, const char *bmapMemPtr,
                 char *dataBlockMemPtr, uint32_t numBmapBlocksTotal,
                 float dataDirtyFlushRatio, int numPartitions)
    : fsWorker_(worker), memPtr_(nullptr) {
  if (worker->isMasterWorker()) {
    SPDLOG_ERROR("FileMng construct should not be master for partial mng");
  } else {
    fsImpl_ =
        new FsImpl(worker, bmapMemPtr, dataBlockMemPtr, numBmapBlocksTotal,
                   dataDirtyFlushRatio, numPartitions);
  }
}

FileMng::~FileMng() { delete fsImpl_; }

void FileMng::flushMetadataOnExit() { fsImpl_->flushMetadataOnExit(fsWorker_); }

int64_t FileMng::checkAndFlushBufferDirtyItems() {
  return fsImpl_->checkAndFlushDirty(fsWorker_);
}

int FileMng::processReq(FsReq *req) {
  if (req->getState() == FsReqState::OP_OWNERSHIP_UNKNOWN) {
    req->setError(FS_REQ_ERROR_INODE_IN_TRANSFER);
    fsWorker_->submitFsReqCompletion(req);
    return 0;
  }
  if (req->getState() == FsReqState::OP_OWNERSHIP_REDIRECT) {
    req->setError(FS_REQ_ERROR_INODE_REDIRECT);
    fsWorker_->submitFsReqCompletion(req);
    return 0;
  }

  switch (req->getType()) {
    // data plane operation
    case FsReqType::WRITE:
      // dbgWriteCounter++;
      processWrite(req);
      break;
    case FsReqType::PWRITE:
      processPwrite(req);
      break;
    case FsReqType::READ:
      processRead(req);
      break;
    case FsReqType::PREAD:
      processPread(req);
      break;
    case FsReqType::PREAD_UC:
      processPreadUC(req);
      break;
    case FsReqType::PREAD_UC_LEASE_RENEW_ONLY:
      processCacheLeaseRenew(req);
      break;
    case FsReqType::ALLOC_WRITE:
      // dbgWriteCounter++;
      processAllocWrite(req);
      break;
    case FsReqType::ALLOC_PWRITE:
      processAllocPwrite(req);
      break;
    case FsReqType::ALLOC_PWRITE_RENEW_ONLY:
      processCacheLeaseRenew(req);
      break;
    case FsReqType::ALLOC_READ:
      processAllocRead(req);
      break;
    case FsReqType::ALLOC_PREAD:
      processAllocPread(req);
      break;
    case FsReqType::ALLOC_PREAD_RENEW_ONLY:
      processCacheLeaseRenew(req);
      break;
    case FsReqType::LSEEK:
      processLseek(req);
      break;
    // control plane operation
    case FsReqType::CREATE:
      processCreate(req);
      break;
    case FsReqType::OPEN:
      processOpen(req);
      break;
    case FsReqType::CLOSE:
      processClose(req);
      break;
    case FsReqType::STAT:
      processStat(req);
      break;
    case FsReqType::FSTAT:
      processFstat(req);
      break;
    case FsReqType::UNLINK:
      FileMng::UnlinkOp::ProcessReq(this, req);
      break;
    case FsReqType::RENAME:
      FileMng::RenameOp::ProcessReq(this, req);
      break;
    case FsReqType::FDATA_SYNC:
      // TODO (jingliu): now fsync and fdatasync has the same implementation.
      // need to separate them, basically fsync will sync the inode's metadata.
    case FsReqType::FSYNC:
      processFdataSync(req);
      break;
    case FsReqType::WSYNC:
      processLdbWsync(req);
      break;
    case FsReqType::SYNCALL:
      if (req->getState() <= FsReqState::FSYNC_ERR &&
          req->getState() >= FsReqState::FSYNC_DATA_BIO) {
        processFdataSync(req);
      } else {
        processSyncall(req);
      }
      break;
    case FsReqType::SYNCUNLINKED:
      processSyncunlinked(req);
      break;
    case FsReqType::MKDIR:
      processMkdir(req);
      break;
    case FsReqType::OPENDIR:
      processOpendir(req);
      break;
    case FsReqType::NEW_SHM_ALLOCATED:
      processNewShmAllocated(req);
      break;
    case FsReqType::PING:
      processPing(req);
      break;
    case FsReqType::DUMPINODES:
      processDumpInodes(req);
      break;
    case FsReqType::REMOTE_BITMAP_CHANGES:
      processRemoteBitmapChanges(req);
      break;
    case FsReqType::GENERIC_CALLBACK:
      assert(req->generic_callback != nullptr);
      req->generic_callback(this, req);
      break;
    default:
      // FIXME: memory leak. Mark as failure and reclaim req memory.
      SPDLOG_ERROR("FileMng::processReq not supported");
  }
  return 0;
}

void FileMng::submitFsGeneratedRequestsCheckSinglePendingMap(
    FsReq *req, iou_map_t::const_iterator beginIt,
    iou_map_t::const_iterator endIt, bool isSector) {
  SPDLOG_DEBUG("submitFsGeneratedReqs isSector:{}", isSector);
#ifndef USE_SPDK
  // POSIX DEV
  std::vector<block_no_t> submittedBlockNos;
  std::vector<block_no_t> submittedSectorNos;
#endif
  bool submitFail = false;
  for (auto it = beginIt; it != endIt; ++it) {
    if (it->second) {
      // need to do submit
      BlockReq *bReq = nullptr;
      if (isSector) {
        bReq = req->getSectorSubmitReq(it->first);
      } else {
        bReq = req->getBlockSubmitReq(it->first);
      }
      if (bReq == nullptr) {
        SPDLOG_INFO("submitFsGeneratedRequests fail");
        throw std::runtime_error(
            "cannot find the corresponding submission blockReq");
      }
      if (fsWorker_->submitAsyncReadDevReq(req, bReq) >= 0) {
#ifndef USE_SPDK
        // POSIX DEV
        if (isSector) {
          submittedSectorNos.push_back(bReq->getBlockNo());
        } else {
          submittedBlockNos.push_back(bReq->getBlockNo());
        }
#endif
      } else {
        submitFail = true;
      }
    }  // it->second

    if (!submitFail) {
#ifndef USE_SPDK
      // POSIX DEV
      // nothing here, just to be consistent about using ifndef
#else
      // Either submission succeed or the blockNo read has been inflight,
      // we will pend on this block/sector #.
      if (isSector) {
        fsWorker_->submitPendOnSectorFsReq(req, it->first);
      } else {
        fsWorker_->submitPendOnBlockFsReq(req, it->first);
      }
#endif
    } else {  // submitFail
      if (it == beginIt)
        throw std::runtime_error(
            "submitFsGeneratedRequestsCheckSinglePendingMap can't proceed");
      break;
    }
  }

#ifndef USE_SPDK
  // POSIX DEV
  // because it is a blocking device, so directly mark it DONE
  for (auto bno : submittedBlockNos) {
    req->blockWaitReqDone(bno);
    req->submittedBlockReqDone(bno);
  }
  for (auto secno : submittedSectorNos) {
    req->sectorWaitReqDone(secno);
    req->submittedSectorReqDone(secno);
  }
#endif
}

void FileMng::submitFsGeneratedRequests(FsReq *req) {
  if (req->numPendingBlockReq() > 0) {
    submitFsGeneratedRequestsCheckSinglePendingMap(
        req, req->blockPendingBegin(), req->blockPendingEnd());
  }
  if (req->numPendingSectorReq() > 0) {
    submitFsGeneratedRequestsCheckSinglePendingMap(
        req, req->sectorPendingBegin(), req->sectorPendingEnd(), true);
  }
  req->stopOnCpuTimer();
}

FileMng *FileMng::generateSubFileMng(FsProcWorker *curWorker) {
  assert(curWorker != nullptr);
  assert(!curWorker->isMasterWorker());
  auto fm = new FileMng(
      curWorker, fsImpl_->getBmapMemPtr(), fsImpl_->getDataBlockMemPtr(),
      fsImpl_->getOnDiskNumBmapBlocksTotal(), fsImpl_->getDataDirtuFlushRatio(),
      fsImpl_->getNumPartitions());
  return fm;
}

void FileMng::installInode(InMemInode *inode) { fsImpl_->installInode(inode); }

void FileMng::addFdMappingOnOpen(pid_t pid, FileObj *fobj) {
  assert(fobj != nullptr);
  auto inodePtr = fobj->ip;
  inodePtr->addFd(pid, fobj);
  ownerAppFdMap_[pid].emplace(fobj->readOnlyFd, fobj);
}

void FileMng::delFdMappingOnClose(pid_t pid, FileObj *fobj) {
  assert(fobj != nullptr);
  auto inodePtr = fobj->ip;
  inodePtr->delFd(pid, fobj);

  auto search = ownerAppFdMap_.find(pid);
  assert(search != ownerAppFdMap_.end());
  search->second.erase(fobj->readOnlyFd);

  // NOTE: not erasing pid from ownerAppFdMap_ if appFdMap is empty as it can be
  // erased when process dies.
  // NOTE: if this function ever does anything more than delFd and delete from
  // ownerAppFdMap_, please also consider modifying closeAllFileDescriptors as
  // that is a batch optimized version of this function.
  delete fobj;
}

void FileMng::closeAllFileDescriptors(pid_t pid) {
  SPDLOG_DEBUG("[wid={}] closing all fd's for pid {}", fsWorker_->getWid(),
               pid);
  auto search = ownerAppFdMap_.find(pid);
  if (search == ownerAppFdMap_.end()) return;

  auto app_fd_map = search->second;
  std::unordered_map<cfs_ino_t, InMemInode *> ino_map;

  for (auto &kv : app_fd_map) {
    FileObj *fobj = kv.second;
    InMemInode *inodePtr = fobj->ip;
    ino_map[inodePtr->i_no] = inodePtr;
    inodePtr->delFd(pid, fobj);
    delete fobj;
  }

  for (auto &[ino, inodePtr] : ino_map) {
    if ((inodePtr->unlinkDeallocResourcesOnClose) &&
        inodePtr->noAppReferring()) {
      UnlinkOp::OwnerDeallocResources(this, ino, inodePtr);
    }
  }

  ownerAppFdMap_.erase(search);
}

// When importing an inode, all the file descriptor mappings need to be copied
// to an outer level so fd's can be resolved.
void FileMng::addImportedInodeFdMappings(const InMemInode *inodePtr) {
  for (const auto &[pid, inodeFdMap] : inodePtr->getAppFdMap()) {
    auto &fdMap = ownerAppFdMap_[pid];
    fdMap.insert(inodeFdMap.begin(), inodeFdMap.end());
  }
}

// When exporting an inode, all outer level mappings must be deleted so this fd
// can never be resolved.
void FileMng::delExportedInodeFdMappings(const InMemInode *inodePtr) {
  for (const auto &[pid, inodeFdMap] : inodePtr->getAppFdMap()) {
    auto search = ownerAppFdMap_.find(pid);
    if (search == ownerAppFdMap_.end()) continue;

    auto &fdMap = search->second;
    for (const auto &kv : inodeFdMap) {
      SPDLOG_DEBUG("Erasing fd {} from exporter wid {}", kv.first,
                   fsWorker_->getWid());
      fdMap.erase(kv.first);
    }

    if (fdMap.empty()) ownerAppFdMap_.erase(search);
  }
}

FileObj *FileMng::getFileObjForFd(pid_t pid, int fd) {
  auto outer_search = ownerAppFdMap_.find(pid);
  if (outer_search == ownerAppFdMap_.end()) return nullptr;

  auto &fdMap = outer_search->second;
  auto inner_search = fdMap.find(fd);
  if (inner_search == fdMap.end()) return nullptr;

  return inner_search->second;
}

bool FileMng::exportInode(cfs_ino_t inum, ExportedInode &exp) {
  // NOTE: exportInode only exports the inode and removes all state related to
  // it from the FileMng. It does not update primary about the export. Caller
  // must inform the primary about ownership changes.

  assert(fsWorker_->NumInProgressRequests(inum) == 0);
  exp.inode = PopInMemInode(inum);
  // FIXME: currently primary also has this inode in memory. So for primary,
  // this does not assert that it is the owner.
  // TODO: temporary fix for shared file
  if (exp.inode == nullptr) return false;
  // assert(exp.inode);
  exp.exporter_wid = fsWorker_->getWid();

  // removing from data structures in current worker and setting flags to ensure
  // they are added to data structures in future worker that imports it.
  size_t numErased = 0;
  numErased = fsImpl_->dirtyInodeSet_.erase(exp.inode->i_no);
  exp.in_exporter_dirty_set = (numErased != 0);

#if CFS_JOURNAL(ON)
  numErased = fsImpl_->unlinkedInodeSet_.erase(exp.inode->i_no);
  exp.in_exporter_unlinked_set = (numErased != 0);

  numErased = fsWorker_->jmgr->checkpointInput->inodesToCheckpoint.erase(
      exp.inode->i_no);
  exp.in_exporter_journal = (numErased != 0);
#endif

  delExportedInodeFdMappings(exp.inode);

  // Exporting block buffers
  splitInodeDataBlockBufferSlot(exp.inode, exp.block_buffers);

  bool canReset = exp.inode->setManageWorkerUnknown(exp.exporter_wid);
  assert(canReset);
  return true;
}

void FileMng::importInode(const ExportedInode &exp) {
  // NOTE: importInode only imports into the FileMng. It does not update primary
  // about the import. Caller must inform the primary about ownership change.
  // NOTE: primary should have already set this inode wid to the importer while
  // updating bookkeeping information.
  assert(exp.inode->getManageWorkerId() == fsWorker_->getWid());

  auto search = fsImpl_->inodeMap_.find(exp.inode->i_no);
  if (search != fsImpl_->inodeMap_.end()) {
    SPDLOG_WARN("trying to import inode {} that already exists, wid:{}",
                exp.inode->i_no, fsWorker_->getWid());
  } else {
    fsImpl_->inodeMap_.emplace(exp.inode->i_no, exp.inode);
  }

  // NOTE: currently inodeMap_ on primary may contain inodes that it doesn't
  // really own. So we must still continue with the rest of the import and
  // modify data structures appropriately.

  if (exp.in_exporter_dirty_set) {
    assert(exp.inode->getDirty());
    fsImpl_->dirtyInodeSet_.insert(exp.inode->i_no);
  }

#if CFS_JOURNAL(ON)
  if (exp.in_exporter_unlinked_set) {
    fsImpl_->unlinkedInodeSet_.insert(exp.inode->i_no);
  }

  if (exp.in_exporter_journal) {
    fsWorker_->jmgr->checkpointInput->inodesToCheckpoint.emplace(
        exp.inode->i_no, nullptr);
  }
#endif

  addImportedInodeFdMappings(exp.inode);

  if (!exp.block_buffers.empty()) {
    installDataBlockBufferSlot(exp.inode, exp.block_buffers);
  }
}

void FileMng::processCreate(FsReq *req) {
  // req->incrNumFsm();
  if (req->getState() == FsReqState::CREATE_GET_PRT_INODE) {
    InMemInode *parInode = req->getDirInode();
    if (parInode != nullptr) {
      // successfully retrieve parent's inode from cache
      req->setState(FsReqState::CREATE_ALLOC_INODE);
    } else {
      // recursively lookup the dir according to req's pathToken
      bool is_err;
      parInode = fsImpl_->getParDirInode(req, is_err);
      if (is_err) {
        req->setState(FsReqState::CREATE_ERR);
      } else if (req->numTotalPendingIoReq() == 0) {
        // only transfer the state when dirInode is okay to use (in memory)
        req->setDirInode(parInode);
        req->setState(FsReqState::CREATE_ALLOC_INODE);
      } else {
        // next time, when this request is ready, it will start from
        // GET_PRT_INODE state
        submitFsGeneratedRequests(req);
      }
    }
  }

  if (req->getState() == FsReqState::CREATE_ALLOC_INODE) {
    InMemInode *dirInode = req->getDirInode();
    while (!dirInode->tryLock()) {
      // spin
    }

    // FIXME: AllocateInode can perform io and so we return here if that
    // happens. However, can we do more processing for this request that doesn't
    // require i/o while the i/o is being done.
    InMemInode *inode = fsImpl_->AllocateInode(req);
    if (req->numTotalPendingIoReq() > 0) {
      dirInode->unLock();
      submitFsGeneratedRequests(req);
      return;
    }

    if (inode != nullptr) {
      SPDLOG_DEBUG("AllocateInode() for CREAT return i_no:{} d:i_no:{}",
                   inode->i_no, inode->inodeData->i_no);
      req->setFileIno(inode->i_no);
    } else {
      // force FSP down since cannot allocate inode
      throw std::runtime_error("CREATE_ALLOC_INODE cannot allocate inode");
    }
    if (req->numTotalPendingIoReq() == 0) {
      // TODO Currently, the inode table for this inode is populated in memory
      // and not read from disk. This will cause syncID to be 0 instead of
      // whatever it is on disk. Read a few free inodes into memory and maintain
      // a pool so that create is fast and doesn't have to read from disk.
      // initNewAllocatedDinodeContent(inode->inodeData, T_FILE, inode->i_no);
      inode->initNewAllocatedDinodeContent(T_FILE);
      fsWorker_->onTargetInodeFiguredOut(req, inode);
      gFsProcPtr->CheckNewedInMemInodeDst(fsWorker_->getWid(), inode->i_no,
                                          req->getApp()->getPid(),
                                          req->getTid());
      req->setState(FsReqState::CREATE_FILL_DIR);
    } else {
      req->setState(FsReqState::CREATE_INODE_ALLOCED_NOT_IN_MEM);
      // submitFsGeneratedRequests(req);
    }
    dirInode->unLock();
  }

  if (req->getState() == FsReqState::CREATE_INODE_ALLOCED_NOT_IN_MEM) {
    SPDLOG_DEBUG("CREATE_INODE_ALLOCED_NOT_IN_MEM inum:{}", req->getFileInum());
    if (req->numTotalPendingIoReq() > 0) {
      submitFsGeneratedRequests(req);
    } else {
      InMemInode *inode = fsImpl_->getFileInode(req, req->getFileInum());
      if (req->numTotalPendingIoReq() == 0) {
        // initNewAllocatedDinodeContent(inode->inodeData, T_FILE, inode->i_no);
        inode->initNewAllocatedDinodeContent(T_FILE);
        fsWorker_->onTargetInodeFiguredOut(req, inode);
        gFsProcPtr->CheckNewedInMemInodeDst(fsWorker_->getWid(), inode->i_no,
                                            req->getApp()->getPid(),
                                            req->getTid());
        req->setState(FsReqState::CREATE_FILL_DIR);
      } else {
        submitFsGeneratedRequests(req);
      }
    }
  }

  if (req->getState() == FsReqState::CREATE_FILL_DIR) {
    SPDLOG_DEBUG("CREATE_FILL_DIR");
    InMemInode *dirInode = req->getDirInode();
    InMemInode *fileInode = req->getTargetInode();
    // assume once fetched, inode will never be replaced now
    while ((!dirInode->tryLock()) || (!fileInode->tryLock())) {
      // spin
    }
    fsImpl_->appendToDir(req, dirInode, fileInode);
    if (req->numTotalPendingIoReq() == 0) {
      dirInode->adjustDentryCount(1);
      SPDLOG_DEBUG("processCreate incr dentry_count to:{}",
                   dirInode->inodeData->i_dentry_count);
      int curPathDepth;
      // add cache item
      auto fullPath = req->getStandardFullPath(curPathDepth);
      fsImpl_->addPathInodeCacheItem(req, req->getTargetInode());

      req->setState(FsReqState::CREATE_UPDATE_INODE);
    } else {
      // TODO (jingliu): is that reasonable for this fill dir to do IO?
      // SPDLOG_INFO("create fill dir need to do io");
      submitFsGeneratedRequests(req);
    }
    fileInode->unLock();
    dirInode->unLock();
  }

  // Set this created file's inode and its parent directory's inode to flags
  // If necessary, issue the writing to device
  if (req->getState() == FsReqState::CREATE_UPDATE_INODE) {
    InMemInode *dirInode = req->getDirInode();
    InMemInode *fileInode = req->getTargetInode();
    while ((!dirInode->tryLock()) || (!fileInode->tryLock())) {
      // spin
    }
    int rc = fsImpl_->writeFileInode(req, fileInode->i_no);
    if (rc > 0) {
      rc = fsImpl_->writeFileInode(req, dirInode->i_no);
      if (rc <= 0) {
        SPDLOG_ERROR("CREATE_UPDATE_INODE: cannot update parentDIR");
        req->setState(FsReqState::CREATE_ERR);
      } else {
        // submit write request and done.
        // submitFsWriteRequests(req);
        req->setState(FsReqState::CREATE_FINI);
      }
    } else {
      // need to read inode block (though it should be rare)
      submitFsGeneratedRequests(req);
    }
    fileInode->unLock();
    dirInode->unLock();
  }

  if (req->getState() == FsReqState::CREATE_FINI) {
#ifdef KP_SPECIAL_NUM_CREATE
    req->getApp()->IncrementNumCreate(tid);
#endif
#ifdef CHECK_CREATION_REDIRECT
    int tid = req->getTid();
    pid_t pid = req->getPid();
    cfs_ino_t ino = req->getFileInum();
    fsWorker_->CheckCreationRedirection(pid, tid, ino);
#endif
    fsWorker_->submitFsReqCompletion(req);
  }

  if (req->getState() == FsReqState::CREATE_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processOpen(FsReq *req) {
  if (fsWorker_->isMasterWorker()) {
    if (req->getState() == FsReqState::OPEN_GET_CACHED_INODE) {
      int fullPathDepth = req->getStandardPathDepth();
      if (fullPathDepth > 0) {
        InMemInode *targetFileInode = req->getTargetInode();
        if (targetFileInode != nullptr) {
          fsWorker_->onTargetInodeFiguredOut(req, targetFileInode);
          req->setState(FsReqState::OPEN_FINI);
        } else {
          // try to see if parent directory is in cache
          InMemInode *dirInode = nullptr;
          if (fullPathDepth > 1) {
            dirInode = req->getDirInode();
            if (dirInode != nullptr) {
              req->setState(FsReqState::OPEN_GET_FILE_INUM);
            }
          }
          // must do lookup
          if (dirInode == nullptr) {
            req->setState(FsReqState::OPEN_GET_PRT_INODE);
          }
        }
      } else {
        SPDLOG_WARN("OPEN root directory");
        auto inode = fsImpl_->rootDirInode(req);
        assert(req->numTotalPendingIoReq() == 0);
        fsWorker_->onTargetInodeFiguredOut(req, inode);
        req->setState(FsReqState::OPEN_FINI);
      }
    }

    if (req->getState() == FsReqState::OPEN_GET_PRT_INODE) {
      bool is_err;
      InMemInode *dirInode = fsImpl_->getParDirInode(req, is_err);
      if (is_err) {
        req->setState(FsReqState::OPEN_ERR);
      } else if (req->numTotalPendingIoReq() == 0) {
        // only transfer the state when dirInode is okay to use (in memory)
        req->setDirInode(dirInode);
        req->setState(FsReqState::OPEN_GET_FILE_INUM);
      } else {
        // next time, when this request is ready, it will start from
        // GET_PRT_INODE state
        submitFsGeneratedRequests(req);
      }
    }

    if (req->getState() == FsReqState::OPEN_GET_FILE_INUM) {
      InMemInode *dirInode = req->getDirInode();
      while (!dirInode->tryLock()) {
        // spin
      }
      bool is_err;
      uint32_t fileIno =
          fsImpl_->lookupDir(req, dirInode, req->getLeafName(), is_err);
      if (is_err) {
        req->setState(FsReqState::OPEN_ERR);
      } else {
        if (fileIno > 0) {
          req->setFileIno(fileIno);
          req->setState(FsReqState::OPEN_GET_FILE_INODE);
        }
        if (req->numTotalPendingIoReq() != 0) {
          submitFsGeneratedRequests(req);
        }
      }
      dirInode->unLock();
    }

    if (req->getState() == FsReqState::OPEN_GET_FILE_INODE) {
      InMemInode *targetFileInode =
          fsImpl_->getFileInode(req, req->getFileInum());
      if (req->numTotalPendingIoReq() == 0) {
        fsWorker_->onTargetInodeFiguredOut(req, targetFileInode);
        FsImpl::fillInodeDentryPositionAfterLookup(req, targetFileInode);
        req->setState(FsReqState::OPEN_FINI);
        // add cache item
        int fullPathDepth;
        auto fullPath = req->getStandardFullPath(fullPathDepth);
        if (fullPathDepth > 0) {
          fsImpl_->addPathInodeCacheItem(req, targetFileInode);
        }
      } else {
        submitFsGeneratedRequests(req);
      }
    }

    if (req->getState() == FsReqState::OPEN_FINI) {
      gFsProcPtr->CheckNewedInMemInodeDst(
          FsProcWorker::fromIdx2WorkerId(fsImpl_->idx_),
          req->targetInodePtr->i_no, req->getApp()->getPid(), req->getTid());
      fsWorker_->submitFsReqCompletion(req);
    }
  } else {
    // not master
    fsWorker_->submitFsReqCompletion(req);
  }  // isMasterWorker()

  if (req->getState() == FsReqState::OPEN_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processWrite(FsReq *req) {
  if (req->getState() == FsReqState::WRITE_MODIFY) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.write.rwOp.count;
    if (fileObj != nullptr) {
      SPDLOG_DEBUG("processWrite size:{} writeCounter:{} offSet:{}", reqCount,
                   dbgWriteCounter, fileObj->off);
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      uint64_t fobjStartOff = fileObj->off;
      char *src = req->getChannelDataPtr();
      int64_t nWrite =
          fsImpl_->writeInode(req, fileInode, src, fobjStartOff, reqCount);
      if (nWrite < 0) {
        req->setState(FsReqState::WRITE_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // success
          req->getClientOp()->op.write.rwOp.ret = nWrite;
          fileObj->off += nWrite;
          req->setState(FsReqState::WRITE_UPDATE_INODE);
        } else {
          submitFsGeneratedRequests(req);
        }
      }
      fileInode->unLock();
    } else {
      req->setState(FsReqState::WRITE_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::WRITE_UPDATE_INODE) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.write.rwOp.count;
    if (reqCount > RING_DATA_ITEM_SIZE || reqCount <= 0) {
      req->setState(FsReqState::WRITE_RET_ERR);
      return;
    }
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      int rc = fsImpl_->writeFileInode(req, fileInode->i_no);
      if (rc > 0) {
        req->setState(FsReqState::WRITE_MODIFY_DONE);
      } else {
        // need to read inode block (though it should be rare)
        submitFsGeneratedRequests(req);
      }
      fileInode->unLock();
    }
  }

  if (req->getState() == FsReqState::WRITE_MODIFY_DONE) {
    fsWorker_->submitFsReqCompletion(req);
  }

  if (req->getState() == FsReqState::WRITE_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processPwrite(FsReq *req) {
  if (req->getState() == FsReqState::PWRITE_MODIFY) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.pwrite.rwOp.count;
    uint64_t fobjStartOff = req->getClientOp()->op.pwrite.offset;
    SPDLOG_DEBUG("processPWrite offset:{} count:{}\n", fobjStartOff, reqCount);
    if (reqCount > RING_DATA_ITEM_SIZE || reqCount <= 0) {
      req->setState(FsReqState::PWRITE_RET_ERR);
      return;
    }
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      char *src = req->getChannelDataPtr();
      int64_t nWrite =
          fsImpl_->writeInode(req, fileInode, src, fobjStartOff, reqCount);
      if (nWrite < 0) {
        req->setState(FsReqState::PWRITE_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // success
          req->getClientOp()->op.pwrite.rwOp.ret = nWrite;
          //// do not update offset fo pwrite
          //// fileObj->off += nWrite;
          req->setState(FsReqState::PWRITE_UPDATE_INODE);
        } else {
          submitFsGeneratedRequests(req);
        }
      }
      fileInode->unLock();
    } else {
      req->setState(FsReqState::PWRITE_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::PWRITE_UPDATE_INODE) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.pwrite.rwOp.count;
    if (reqCount > RING_DATA_ITEM_SIZE || reqCount <= 0) {
      req->setState(FsReqState::PWRITE_RET_ERR);
      return;
    }
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      // This is needed anyway because ideally the timestamp need to be updated
      // fileInode->inodeData->atime;
      int rc = fsImpl_->writeFileInode(req, fileInode->i_no);
      if (rc > 0) {
        req->setState(FsReqState::PWRITE_MODIFY_DONE);
      } else {
        // need to read inode block (though it should be rare)
        submitFsGeneratedRequests(req);
      }
      fileInode->unLock();
    }
  }

  if (req->getState() == FsReqState::PWRITE_MODIFY_DONE) {
    fsWorker_->submitFsReqCompletion(req);
  }

  if (req->getState() == FsReqState::PWRITE_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processRead(FsReq *req) {
  if (req->getState() == FsReqState::READ_FETCH_DATA) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.read.rwOp.count;
    // check here to avoid that, malicious or buggy client directly to our
    // shared memory which bypass the FsLib's check, could be removed for
    // performance
    if (reqCount > RING_DATA_ITEM_SIZE) {
      req->setState(FsReqState::READ_RET_ERR);
      return;
    }
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      uint64_t fobjStartOff = fileObj->off;
      char *dst = req->getChannelDataPtr();
      int64_t nRead =
          fsImpl_->readInode(req, fileInode, dst, fobjStartOff, reqCount);
      if (nRead < 0) {
        req->setState(FsReqState::READ_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // req success, set the return value, set state
          req->getClientOp()->op.read.rwOp.ret = nRead;
          // update offset
          fileObj->off += nRead;
          req->setState(FsReqState::READ_FETCH_DONE);
        } else {
          submitFsGeneratedRequests(req);
        }
      }
      fileInode->unLock();
    } else {
      req->setState(FsReqState::READ_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::READ_FETCH_DONE) {
    fsWorker_->submitFsReqCompletion(req);
  }

  if (req->getState() == FsReqState::READ_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processPread(FsReq *req) {
  if (req->getState() == FsReqState::PREAD_FETCH_DATA) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.pread.rwOp.count;
    uint64_t fobjStartOff = req->getClientOp()->op.pread.offset;
    SPDLOG_DEBUG(
        "processPread - fd:{} offset: {} count:{} fsize:{} wid:{} "
        "isAppCacheAvailable:{}",
        req->getClientOp()->op.pread.rwOp.fd, fobjStartOff, reqCount,
        fileObj->ip->inodeData->size, fsWorker_->getWid(),
        req->isAppCacheAvailable());

    if (fileObj != nullptr) {
      fsWorker_->onTargetInodeFiguredOut(req, fileObj->ip);
    }

    if (fobjStartOff + reqCount > fileObj->ip->inodeData->size) {
      req->setState(FsReqState::PREAD_RET_ERR);
      goto PREAD_ERR_PROCESS;
    }

    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      char *dst = req->getChannelDataPtr();
      int64_t nRead =
          fsImpl_->readInode(req, fileInode, dst, fobjStartOff, reqCount);
      if (nRead < 0) {
        req->setState(FsReqState::PREAD_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // req success, set the return value, set state
          req->getClientOp()->op.pread.rwOp.ret = nRead;
          //// do not update offset for pread
          //// fileObj->off += nRead;
          req->setState(FsReqState::PREAD_FETCH_DONE);
          SPDLOG_DEBUG("===> readInode CACHE HIT wid:{}", fsWorker_->getWid());
        } else {
          SPDLOG_DEBUG("===> readInode CACHE MISS wid:{}", fsWorker_->getWid());
          submitFsGeneratedRequests(req);
        }
      }
      fileInode->unLock();
    } else {
      req->setState(FsReqState::PREAD_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::PREAD_FETCH_DONE) {
    fsWorker_->submitFsReqCompletion(req);
  }

PREAD_ERR_PROCESS:
  if (req->getState() == FsReqState::PREAD_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processPreadUC(FsReq *req) {
  if (req->getState() == FsReqState::UCPREAD_GEN_READ_PLAN) {
#ifdef USE_UC_PAGE_CACHE
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.pread.rwOp.count;
    uint64_t fobjStartOff = req->getClientOp()->op.pread.offset;
    SPDLOG_DEBUG("fobjStartOff:{} reqCount:{} realCountInit:{}", fobjStartOff,
                 reqCount, req->getClientOp()->op.pread.rwOp.realCount);
    // when use UC, this must be aligned
    assert(fobjStartOff % (BSIZE) == 0);
    if (fileObj != nullptr) {
      fsWorker_->onTargetInodeFiguredOut(req, fileObj->ip);
      if (fobjStartOff == fileObj->ip->inodeData->size) {
        req->getClientOp()->op.pread.rwOp.ret = 0;
        fsWorker_->submitFsReqCompletion(req);
      } else if (fobjStartOff > fileObj->ip->inodeData->size) {
        req->setState(FsReqState::UCPREAD_RET_ERR);
      } else {
        if (fileObj->ip->inodeData->size - fobjStartOff < reqCount) {
          reqCount = fileObj->ip->inodeData->size - fobjStartOff;
        }
        InMemInode *fileInode = fileObj->ip;
        // figure out the pages that need to read from disk/hugepage
        auto &pageCache = fileInode->inodePageCache;
        uint64_t curOff;
        size_t curCount;
        uint32_t blockIdx;
        int numPages = 1 + (reqCount - 1) / (BSIZE);
        PageDescriptorMsg *pdmsg =
            reinterpret_cast<PageDescriptorMsg *>(req->getChannelDataPtr());
        PageDescriptorMsg *curmsg;
        PageDescriptor *curPd;
        std::vector<std::pair<cfs_bno_t, size_t>> blockReqs;
        for (int i = 0; i < numPages; i++) {
          curOff = fobjStartOff + (BSIZE)*i;
          blockIdx = curOff / (BSIZE);
          curCount = std::min(fobjStartOff + reqCount - curOff,
                              (unsigned long)(BSIZE));
          int64_t globalPgid = pageCache.getFromCache(blockIdx);
          req->getClientOp()->op.pread.rwOp.realCount += curCount;
          if (globalPgid < 0) {
            blockReqs.push_back({blockIdx, curCount});
          } else {
            curPd = gFsProcPtr->pageCacheMng->findStablePage(globalPgid);
            if (curPd->validSize >= (int)curCount) {
              // this page is ready in the page cache
              curmsg = pdmsg + i;
              curmsg->gPageIdx = curPd->gPageIdx;
              curmsg->MAGIC = PAGE_DESCRIPTOR_MSG_MAGIC;
              curmsg->pageIdxWithinIno = blockIdx;
              assert(curPd->pageIdxWithinIno == blockIdx);
            } else {
              blockReqs.push_back({blockIdx, curCount});
            }
          }
        }
        // We reset the next page's magic to verify correctness
        (pdmsg + numPages)->MAGIC &= 0;
        // for(auto br: blockReqs) {
        //   fprintf(stdout, "bno:%u count:%lu\n", br.first, br.second);
        // }
        // save the pages that need to be read from dev/hugepage
        size_t numPageToFetch = blockReqs.size();
        SPDLOG_DEBUG("numPageToFetch:{}", numPageToFetch);
        if (numPageToFetch > 0) {
          std::vector<std::pair<PageDescriptor *, void *>> cachePages;
          std::vector<void *> addrs;
          addrs.reserve(numPageToFetch);
          int na = gFsProcPtr->pageCacheMng->allocPageToIno(
              numPageToFetch, cachePages, fileInode->i_no);
          assert(na == 0);
          assert(cachePages.size() == numPageToFetch);
          req->ucReqDstVec = std::move(cachePages);
          req->ucReqPagesToRead = std::move(blockReqs);
          req->setState(FsReqState::UCPREAD_FETCH_DATA);
        } else {
          req->setState(FsReqState::UCPREAD_RENEW_LEASE);
        }
      }
    } else {
      // fileObj == nullptr
      req->setState(FsReqState::UCPREAD_RET_ERR);
    }
#endif
  }

  if (req->getState() == FsReqState::UCPREAD_FETCH_DATA) {
#ifdef USE_UC_PAGE_CACHE
    SPDLOG_DEBUG("UCPREAD_FETCH_DATA");
    auto fileInode = req->getTargetInode();
    assert(fileInode != nullptr);
    int nread = fsImpl_->readInodeToUCache(
        req, fileInode, req->ucReqPagesToRead, req->ucReqDstVec);
    if (nread < 0) {
      req->setState(FsReqState::UCPREAD_RET_ERR);
    } else {
      if (req->numTotalPendingIoReq() == 0) {
        if (isOpEnableUnifiedCache(&req->getClientOp()->op.pread)) {
          // now we should save the rest into the cache
          uint32_t startBlockIdx =
              req->getClientOp()->op.pread.offset / (BSIZE);
          int reqBlockIdx;
          uint32_t curBlockIdx;
          PageDescriptorMsg *pdmsg =
              reinterpret_cast<PageDescriptorMsg *>(req->getChannelDataPtr());
          PageDescriptorMsg *curmsg;
          PageDescriptor *curpd;
          for (size_t i = 0; i < req->ucReqDstVec.size(); i++) {
            curBlockIdx = req->ucReqPagesToRead[i].first;
            reqBlockIdx = curBlockIdx - startBlockIdx;
            curpd = req->ucReqDstVec[i].first;
            fileInode->inodePageCache.putToCache(curBlockIdx, curpd->gPageIdx);
            curmsg = pdmsg + reqBlockIdx;
            curmsg->MAGIC = PAGE_DESCRIPTOR_MSG_MAGIC;
            curmsg->pageIdxWithinIno = curBlockIdx;
            curmsg->gPageIdx = curpd->gPageIdx;

            curpd->pageIdxWithinIno = curBlockIdx;
            curpd->validSize = req->ucReqPagesToRead[i].second;
            // fprintf(stdout, "i:%lu msg->pageIdxWithinIno:%u\n", i,
            // curmsg->pageIdxWithinIno); print_pd(curpd,
            // req->ucReqDstVec[i].second);
          }
          req->setState(FsReqState::UCPREAD_RENEW_LEASE);
        } else {
          throw std::runtime_error("REQ NOT SET UC bit");
        }
      } else if (req->numTotalPendingIoReq() > 0) {
        submitFsGeneratedRequests(req);
      }
    }
#endif
  }

  if (req->getState() == FsReqState::UCPREAD_RENEW_LEASE) {
    SPDLOG_DEBUG("UCPREAD_RENEW_LEASE");
    int rt = _renewLeaseForRead(req);
    if (rt == 0) {
      req->getClientOp()->op.pread.rwOp.ret = req->getRwOp()->realCount;
      SPDLOG_DEBUG("ret set to:{}", req->getClientOp()->op.pread.rwOp.ret);
      fsWorker_->submitFsReqCompletion(req);
    } else {
      req->setState(FsReqState::ALLOCPREAD_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::UCPREAD_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processAllocWrite(FsReq *req) {
  // req->incrNumFsm();
  if (req->getState() == FsReqState::ALLOCWRITE_TOCACHE_MODIFY) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.allocwrite.rwOp.count;
    SPDLOG_DEBUG("ALLOCWRITE_TOCACHE_MODIFY size:{} writeCounter:{}", reqCount,
                 dbgWriteCounter);
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      // first check if this inode is already in some other app
      if (fsWorker_->checkInodeInOtherApp(fileInode->inodeData->i_no,
                                          req->getPid())) {
        // let this inode enter *SHARE* mode
        int rc = fsWorker_->setInodeInShareMode(req, fileInode);
        if (rc >= 0) {
          req->disableAppCache();
        } else {
          SPDLOG_ERROR("processAllocWrite cannot setInodeInShareMode");
        }
      }
      uint64_t fobjStartOff = fileObj->off;
      auto nNumBlocks = fsImpl_->writeInodeAllocDataBlock(
          req, fileInode, fobjStartOff, reqCount);
      if (nNumBlocks < 0) {
        req->setState(FsReqState::ALLOCWRITE_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // Success.
          // update inode's file size
          fileInode->inodeData->size =
              std::max(fobjStartOff + reqCount, fileInode->inodeData->size);
          fileInode->logEntry->set_size(fileInode->inodeData->size);
          // update fileObj's (fd) offset
          req->getClientOp()->op.allocwrite.rwOp.realOffset = fileObj->off;
          req->getClientOp()->op.allocwrite.rwOp.realCount = reqCount;
          fileObj->off += reqCount;
          req->setState(FsReqState::ALLOCWRITE_UPDATE_INODE);
        } else {
          submitFsGeneratedRequests(req);
        }
      }
    } else {
      // fileObj == nullptr
      req->setState(FsReqState::ALLOCWRITE_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::ALLOCWRITE_UNCACHE_MODIFY) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.allocwrite.rwOp.count;
    SPDLOG_DEBUG("processAllocWrite size:{} writeCounter:{}", reqCount,
                 dbgWriteCounter);
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      uint64_t fobjStartOff = fileObj->off;
      if (fileObj->flags & O_APPEND) {
        auto cur_append_off =
            std::max(fileInode->inodeData->size, fileInode->next_append_offset);
        auto cur_req_append_off = req->GetAppendOffOrInit(cur_append_off);
        if (cur_req_append_off == cur_append_off) {
          fileInode->next_append_offset = cur_req_append_off + reqCount;
        }
        fobjStartOff = cur_req_append_off;
      }
      assert(req->isAppBufferAvailable());
      char *src = req->getMallocedDataPtr();
      int64_t nWrite =
          fsImpl_->writeInode(req, fileInode, src, fobjStartOff, reqCount);
      if (nWrite < 0) {
        req->setState(FsReqState::ALLOCWRITE_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // success
          req->getClientOp()->op.allocwrite.rwOp.ret = nWrite;
          req->getClientOp()->op.allocwrite.rwOp.realOffset = fileObj->off;
          req->getClientOp()->op.allocwrite.rwOp.realCount = nWrite;
          fileObj->off += nWrite;
          req->setState(FsReqState::ALLOCWRITE_UPDATE_INODE);
        } else {
          submitFsGeneratedRequests(req);
        }
      }
    } else {
      // fileObj == nullptr
      req->setState(FsReqState::ALLOCWRITE_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::ALLOCWRITE_UPDATE_INODE) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.allocwrite.rwOp.count;
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      int rc = fsImpl_->writeFileInode(req, fileInode->i_no);
      if (rc > 0) {
        // AllocWrite Process Done
        if (req->isAppCacheAvailable()) {
          // we are going to use client cache
          int rt = fsWorker_->addInAppInodeRWFsReq(req, fileInode);
          if (rt < 0) {
            // the inode already has been in one client-cache
            // TODO (jingliu) invalidate client cache
            throw std::runtime_error(
                "inode is already in one other CCache, need to invalidate. NOT "
                "SUPPORTED now");
          }
          req->getClientOp()->op.allocwrite.rwOp.flag =
              UTIL_BIT_FLIP(req->getClientOp()->op.allocwrite.rwOp.flag,
                            (_RWOP_FLAG_FSP_DATA_AT_APP_BUF_));
          SPDLOG_DEBUG("ALOCWRITE: cache enabled flag:{} seqNoSetTo:{}",
                       req->getClientOp()->op.allocwrite.rwOp.flag,
                       req->getClientOp()->op.allocwrite.alOp.perAppSeqNo);
        }
        req->getClientOp()->op.allocwrite.rwOp.ret = reqCount;
        fsWorker_->submitFsReqCompletion(req);
      } else {
        // need to read inode block (though it should be rare)
        submitFsGeneratedRequests(req);
      }
    }  // fileObj != nullptr
  }

  if (req->getState() == FsReqState::ALLOCWRITE_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processAllocPwrite(FsReq *req) {
  if (req->getState() == FsReqState::ALLOCPWRITE_TOCACHE_MODIFY) {
    // TO be implemented
    throw std::runtime_error("allocpwrite not supported");
  }

  if (req->getState() == FsReqState::ALLOCPWRITE_UNCACHE_MODIFY) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.allocpwrite.rwOp.count;
    SPDLOG_DEBUG("processAllocPWrite size:{} writeCounter:{}", reqCount,
                 dbgWriteCounter);
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      uint64_t fobjStartOff = req->getClientOp()->op.allocpwrite.offset;
      assert(req->isAppBufferAvailable());
      char *src = req->getMallocedDataPtr();
      SPDLOG_DEBUG("processAllocPwrite first char:{}", src[0]);
      int64_t nWrite =
          fsImpl_->writeInode(req, fileInode, src, fobjStartOff, reqCount);
      if (nWrite < 0) {
        req->setState(FsReqState::ALLOCPWRITE_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // success
          req->getClientOp()->op.allocpwrite.rwOp.ret = nWrite;
          req->getClientOp()->op.allocpwrite.rwOp.realOffset = fobjStartOff;
          req->getClientOp()->op.allocpwrite.rwOp.realCount = nWrite;
          req->setState(FsReqState::ALLOCPWRITE_UPDATE_INODE);
        } else {
          submitFsGeneratedRequests(req);
        }
      }
    } else {
      // fileObj == nullptr
      req->setState(FsReqState::ALLOCPWRITE_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::ALLOCPWRITE_UPDATE_INODE) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.allocpwrite.rwOp.count;
    if (reqCount <= 0) {
      req->setState(FsReqState::ALLOCPWRITE_RET_ERR);
      return;
    }
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      int rc = fsImpl_->writeFileInode(req, fileInode->i_no);
      if (rc > 0) {
        // AllocPWrite Process Done
        req->getClientOp()->op.allocpwrite.rwOp.ret = reqCount;
        fsWorker_->submitFsReqCompletion(req);
      } else {
        // need to read inode block (though it should be rare)
        submitFsGeneratedRequests(req);
      }
    }  // fileObj != nullptr
  }

  if (req->getState() == FsReqState::ALLOCPWRITE_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processAllocRead(FsReq *req) {
  if (req->getState() == FsReqState::ALLOCREAD_FETCH_DATA) {
    FileObj *fileObj = req->getFileObj();
    size_t reqCount = req->getClientOp()->op.allocread.rwOp.count;
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      uint64_t fobjStartOff = fileObj->off;
      char *dst = req->getMallocedDataPtr();
      int64_t nRead =
          fsImpl_->readInode(req, fileInode, dst, fobjStartOff, reqCount);
      if (nRead < 0) {
        req->setState(FsReqState::ALLOCREAD_RET_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          // req success, set the return value, set state
          req->getClientOp()->op.allocread.rwOp.ret = nRead;
          // update offset
          fileObj->off += nRead;
#ifdef FSP_ENABLE_ALLOC_READ_RA
          if (fileInode->readaheadBgReqNum == 0) {
            fileInode->readaheadBgReqNum = 1;
            req->setReqBgGC(true);
            req->setState(FsReqState::ALLOCREAD_DO_READAHEAD);
          }
#endif
          fsWorker_->submitFsReqCompletion(req);
        } else {
          // submit IO requests
          submitFsGeneratedRequests(req);
        }
      }
    } else {
      // fileObj == nullptr
      SPDLOG_DEBUG("processRead - Error: cannot find fd - fd:{}",
                   req->getClientOp()->op.allocread.rwOp.fd);
      req->setState(FsReqState::ALLOCREAD_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::ALLOCREAD_DO_READAHEAD) {
    InMemInode *fileInode = req->getTargetInode();
    FileObj *fileObj = req->getFileObj();
    assert(fileInode != nullptr);
    if (fileObj != nullptr && fileInode != nullptr) {
      if (fileInode->readaheadInflight) {
        assert(req->numTotalPendingIoReq() == 0);
        fileInode->readaheadInflight = false;
      }
      if (!fileInode->readaheadInflight) {
        // issue another readahead request
        size_t raCount = gFsProcPtr->getRaNumBlock() * (BSIZE);
        if (fileInode->readaheadOff + raCount >= fileInode->inodeData->size) {
          raCount = fileInode->inodeData->size - fileInode->readaheadOff;
        }
        if (raCount > 0) {
          // try to do readahead
          int64_t nread = fsImpl_->readInode(
              req, fileInode, nullptr, fileInode->readaheadOff, raCount, true);
          if (nread < 0) {
            throw std::runtime_error("fail to do readahead");
          } else {
            fileInode->readaheadOff += nread;
            if (req->numTotalPendingIoReq() == 0) {
              // readInode is satisfied in memory, want to keep readahead
              fsWorker_->submitReadyReq(req);
            } else {
              fileInode->readaheadInflight = true;
              submitFsGeneratedRequests(req);
            }
          }
        } else if (raCount == 0 ||
                   fileInode->readaheadOff > ((off_t)BSIZE) * 500000) {
          // NOTE: this 500000 is just for benchmark, we don't want to readahead
          // more all fetched
          req->setReqBgGC(false);
          fsWorker_->releaseFsReq(req);
        }
      }
    }  // fileObj != nullptr && fileInode != nullptr;
  }    // ALLOCREAD_DO_READAHEAD

  if (req->getState() == FsReqState::ALLOCREAD_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processAllocPread(FsReq *req) {
  if (req->getState() == FsReqState::ALLOCPREAD_UNCACHE_FETCH_DATA) {
    FileObj *fileObj = req->getFileObj();
    req->getRwOp()->realOffset = req->getClientOp()->op.allocpread.offset;
    size_t reqCount = req->getClientOp()->op.allocpread.rwOp.count;
    uint64_t fobjStartOff = req->getClientOp()->op.allocpread.offset;
    if (fileObj != nullptr) {
      SPDLOG_DEBUG(
          "processAllocPread - wid:{} fd:{} offset:{} count:{} fsize:{} "
          "wid:{} ",
          req->getClientOp()->op.allocpread.rwOp.fd, fsWorker_->getWid(),
          fobjStartOff, reqCount, fileObj->ip->inodeData->size,
          fsWorker_->getWid());
      assert(fileObj->ip != nullptr);

      fsWorker_->onTargetInodeFiguredOut(req, fileObj->ip);
      if (fobjStartOff == fileObj->ip->inodeData->size) {
        req->getClientOp()->op.allocpread.rwOp.ret = 0;
        fsWorker_->submitFsReqCompletion(req);
      } else if (fobjStartOff > fileObj->ip->inodeData->size) {
        req->setState(FsReqState::ALLOCPREAD_RET_ERR);
        // should directly go to ALLOCPREAD_RET_ERR's code
      } else {
        if (fileObj->ip->inodeData->size - fobjStartOff < reqCount) {
          reqCount = fileObj->ip->inodeData->size - fobjStartOff;
        }
        int64_t nRead = 0;
        InMemInode *fileInode = fileObj->ip;
        char *dst = req->getMallocedDataPtr();
        SPDLOG_DEBUG("mem_start_offset:{} startOff:{} reqcount:{}",
                     req->getMemOffset(), fobjStartOff, reqCount);
        nRead = fsImpl_->readInode(req, fileInode, dst + req->getMemOffset(),
                                   fobjStartOff, reqCount);
        if (nRead < 0) {
          req->setState(FsReqState::ALLOCPREAD_RET_ERR);
        } else {
          if (req->numTotalPendingIoReq() == 0) {
            // fprintf(stderr, "allocpread ino:%u off:%lu\n", fileInode->i_no,
            //        req->getClientOp()->op.allocpread.offset);
            // req success, set the return value, set state
            req->getClientOp()->op.allocpread.rwOp.ret = nRead;
            // This is just to make sure we set the valid value, this field
            // is not used if CACHE disabled
            req->getRwOp()->realCount = nRead;
            if (isOpEnableCache(&(req->getClientOp()->op.allocpread))) {
              req->setState(FsReqState::ALLOCPREAD_TOCACHE_RENEW_LEASE);
              SPDLOG_DEBUG("req set to ALLOCPREAD_TO_CACHE_RENEW_LEASE");
            } else {
#ifdef FSP_ENABLE_ALLOC_READ_RA
              if (fileInode->readaheadBgReqNum == 0) {
                fileInode->readaheadBgReqNum = 1;
                req->setReqBgGC(true);
                req->setState(FsReqState::ALLOCPREAD_DO_READAHEAD);
              }
#endif
              fsWorker_->submitFsReqCompletion(req);
            }
          } else {
            submitFsGeneratedRequests(req);
          }
        }
      }
    } else {
      // fileObj == nullptr
      SPDLOG_DEBUG(
          "processPread - ERROR: cannot find fd - fd:{} offset:{} count:{} ",
          req->getClientOp()->op.allocpread.rwOp.fd, fobjStartOff, reqCount);
      req->setState(FsReqState::ALLOCPREAD_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::ALLOCPREAD_TOCACHE_RENEW_LEASE) {
    int rt = _renewLeaseForRead(req);
    if (rt == 0) {
#ifdef FSP_ENABLE_ALLOC_READ_RA
      InMemInode *fileInode = req->getTargetInode();
      if (fileInode->readaheadBgReqNum == 0) {
        fileInode->readaheadBgReqNum = 1;
        req->setReqBgGC(true);
        req->setState(FsReqState::ALLOCPREAD_DO_READAHEAD);
      }
#endif
      fsWorker_->submitFsReqCompletion(req);
    } else {
      req->setState(FsReqState::ALLOCPREAD_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::ALLOCPREAD_DO_READAHEAD) {
    InMemInode *fileInode = req->getTargetInode();
    FileObj *fileObj = req->getFileObj();
    assert(fileInode != nullptr);
    if (fileObj != nullptr && fileInode != nullptr) {
      if (fileInode->readaheadInflight) {
        assert(req->numTotalPendingIoReq() == 0);
        fileInode->readaheadInflight = false;
      }
      if (!fileInode->readaheadInflight) {
        // issue another readahead request
        size_t raCount = gFsProcPtr->getRaNumBlock() * (BSIZE);
        if (fileInode->readaheadOff + raCount >= fileInode->inodeData->size) {
          raCount = fileInode->inodeData->size - fileInode->readaheadOff;
        }
        if (raCount > 0) {
          // try to do readahead
          int64_t nread = fsImpl_->readInode(
              req, fileInode, nullptr, fileInode->readaheadOff, raCount, true);
          if (nread < 0) {
            throw std::runtime_error("fail to do readahead");
          } else {
            fileInode->readaheadOff += nread;
            if (req->numTotalPendingIoReq() == 0) {
              // readInode is satisfied in memory, want to keep readahead
              fsWorker_->submitReadyReq(req);
            } else {
              fileInode->readaheadInflight = true;
              submitFsGeneratedRequests(req);
            }
          }
        } else if (raCount == 0) {
          req->setReqBgGC(false);
          fsWorker_->releaseFsReq(req);
        }
      }
    }  // fileObj != nullptr && fileInode != nullptr;
  }    // ALLOCPREAD_DO_READAHEAD

  if (req->getState() == FsReqState::ALLOCPREAD_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processCacheLeaseRenew(FsReq *req) {
  SPDLOG_DEBUG("processCachedLeaseRenew wid:{} reqType:{}", req->getWid(),
               getFsReqTypeOutputString(req->getType()));
  // TODO (jingliu): currently we assume all the extension is succeed
  // It requires the application to not share the files
  setLeaseTermTsIntoRwOp(req->getRwOp(), FsLeaseCommon::genTimestamp());
  req->getRwOp()->ret = 0;
  fsWorker_->submitFsReqCompletion(req);
}

int FileMng::_renewLeaseForRead(FsReq *req) {
  FsProcLeaseType type;
  bool leaseFound = leaseMng_.queryValidExistingLease(req->getFileInum(), type);
  struct FsProcWorkerInodeLeaseCtx *leaseCtxPtr = nullptr;
  // TODO (jingliu): we need to make sure wrting to a file will
  // disable this lease
  if (leaseFound && type == FsProcLeaseType::WRITE_EXCLUSIVE) {
    SPDLOG_DEBUG("WRITE_EXCLUSIZE lease found");
    // there is writing on-going for that file
    req->disableAppCache();
    // now we set the inode into shared mode
    fsWorker_->setInodeInShareMode(req, req->getTargetInode());
    return -1;
  } else if (leaseFound) {
    SPDLOG_DEBUG("READ LEASE FOUND");
    // read lease is found, we can keep working on ccache
    leaseCtxPtr = leaseMng_.queryValidExistingLeaseForApp(req->getFileInum(),
                                                          req->getPid());
    if (leaseCtxPtr == nullptr) {
      SPDLOG_DEBUG(
          "READ lease is found for ino:{} but no lease found for pid:{}",
          req->getFileInum(), req->getPid());
    }
  } else {
    // non existing lease is found, wee need to init the lease
  }

  if (leaseCtxPtr == nullptr) {
    leaseMng_.initLeaseRecord(req->getFileInum(), req->getPid(),
                              FsProcLeaseType::READ_SHARE);
    leaseCtxPtr = leaseMng_.queryValidExistingLeaseForApp(req->getFileInum(),
                                                          req->getPid());
  }
  SPDLOG_DEBUG("before extend timeTs:{}", leaseCtxPtr->startTs);
  leaseMng_.tryExtendLease(leaseCtxPtr);
  SPDLOG_DEBUG("will set startTs to:{}", leaseCtxPtr->startTs);
  setLeaseTermTsIntoRwOp(req->getRwOp(), leaseCtxPtr->startTs);
  return 0;
}

void FileMng::processLseek(FsReq *req) {
  if (req->getState() == FsReqState::LSEEK_INIT) {
    FileObj *fileObj = req->getFileObj();
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      lseekOp *lop = &req->getClientOp()->op.lseek;
      while (!fileInode->tryLock()) {
        // spin
      }
      size_t file_size = fileInode->inodeData->size;
      fileInode->unLock();

      switch (lop->whence) {
        case SEEK_SET:
          fileObj->off = lop->offset;
          break;
        case SEEK_CUR:
          fileObj->off += lop->offset;
          break;
        case SEEK_END:
          fileObj->off = file_size + lop->offset;
          break;
        default:
          req->setState(FsReqState::LSEEK_RET_ERR);
      }

      if (fileObj->off < 0 || fileObj->off > file_size) {
        req->setState(FsReqState::LSEEK_RET_ERR);
      }

      if (req->getState() == FsReqState::LSEEK_RET_ERR) {
        req->setError(FS_REQ_ERROR_POSIX_EINVAL);
      } else {
        lop->ret = fileObj->off;
      }
      fsWorker_->submitFsReqCompletion(req);
      return;
    } else {
      // we cannot find the FileObj for this Fd --> invalid fd
      req->setState(FsReqState::LSEEK_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::LSEEK_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processClose(FsReq *req) {
  if (req->getState() == FsReqState::CLOSE_INIT) {
    FileObj *fileObj = req->getFileObj();
    if (fileObj != nullptr) {
      req->getClientOp()->op.close.ret = 0;

      InMemInode *fileInode = fileObj->ip;
      delFdMappingOnClose(req->getPid(), fileObj);
      fsWorker_->submitFsReqCompletion(req);
      if ((fileInode->unlinkDeallocResourcesOnClose) &&
          fileInode->noAppReferring()) {
        UnlinkOp::OwnerDeallocResources(this, fileInode->i_no, fileInode);
      }
    } else {
      // we cannot find the FileObj for this Fd --> invalid fd
      req->setState(FsReqState::CLOSE_RET_ERR);
    }
  }

  if (req->getState() == FsReqState::CLOSE_RET_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processStat(FsReq *req) {
  bool isMaster = fsWorker_->isMasterWorker();
  if (isMaster) {
    if (req->getState() == FsReqState::STAT_GET_CACHED_INODE) {
      SPDLOG_DEBUG("STAT_GET_CACHED_INODE");
      // If this req have already resolved inode, avoid another lookup
      if (!req->isPathRoot()) {
        InMemInode *inodePtr = req->getTargetInode();
        if (inodePtr != nullptr) {
          // finish request here
          clientOp *cop = req->getClientOp();
          struct stat *cur_stat_ptr = &(cop->op.stat.statbuf);
          fromInMemInode2Statbuf(inodePtr, cur_stat_ptr);
          req->getClientOp()->op.stat.ret = 0;
          fsWorker_->submitFsReqCompletion(req);
        } else {
          // current file is not in cache, try to see if parent directory is in
          // cache
          int parPathDepth;
          auto parPath = req->getStandardParPath(parPathDepth);
          InMemInode *dirInode = nullptr;
          if (!parPath.empty()) {
            dirInode = req->getDirInode();
            if (dirInode != nullptr) {
              req->setState(FsReqState::STAT_GET_FILE_INUM);
            }
          }

          // must do lookup
          if (dirInode == nullptr) {
            req->setState(FsReqState::STAT_GET_PRT_INODE);
          }
        }
      } else {
        // stat is called for root directory
        auto inodePtr = fsImpl_->rootDirInode(req);
        if (req->numPendingSectorReq() > 0) {
          SPDLOG_DEBUG("numPendingBlock:{} numPendingSec:{}",
                       req->numTotalPendingIoReq(), req->numPendingSectorReq());
          submitFsGeneratedRequests(req);
        } else {
          fsWorker_->onTargetInodeFiguredOut(req, inodePtr);
          req->setFileIno(inodePtr->i_no);
          req->setState(FsReqState::STAT_FILE_INODE);
        }
      }
    }

    if (req->getState() == FsReqState::STAT_GET_PRT_INODE) {
      SPDLOG_DEBUG("STAT_GET_PRT_INODE path:{}", req->getLeafName());
      bool is_err;
      InMemInode *dirInode = fsImpl_->getParDirInode(req, is_err);
      if (is_err) {
        req->setState(FsReqState::STAT_ERR);
      } else if (req->numTotalPendingIoReq() == 0) {
        req->setDirInode(dirInode);
        req->setState(FsReqState::STAT_GET_FILE_INUM);
      } else {
        submitFsGeneratedRequests(req);
      }
    }

    if (req->getState() == FsReqState::STAT_GET_FILE_INUM) {
      SPDLOG_DEBUG("STAT_GET_FILE_INUM");
      InMemInode *dirInode = req->getDirInode();
      while (!dirInode->tryLock()) {
        // spin
      }
      bool is_err;
      uint32_t fileIno =
          fsImpl_->lookupDir(req, dirInode, req->getLeafName(), is_err);
      if (is_err) {
        req->setState(FsReqState::STAT_ERR);
      } else {
        if (fileIno > 0) {
          req->setFileIno(fileIno);
          req->setState(FsReqState::STAT_FILE_INODE);
        }
        if (req->numTotalPendingIoReq() != 0) {
          if (fileIno != 0) {
            SPDLOG_DEBUG("processStats fileIno:{}, totalPendingIO:{}", fileIno,
                         req->numTotalPendingIoReq());
          }
          assert(fileIno == 0);
          submitFsGeneratedRequests(req);
        }
      }
      dirInode->unLock();
    }

    if (req->getState() == FsReqState::STAT_FILE_INODE) {
      SPDLOG_DEBUG("STAT_FILE_INODE");
      InMemInode *inodePtr = req->getTargetInode();
      bool inodeSet = (inodePtr != nullptr);
      if (!inodeSet) {
        inodePtr = fsImpl_->getFileInode(req, req->getFileInum());
      }
      if (inodeSet || req->numTotalPendingIoReq() == 0) {
        if (!req->isPathRoot())
          FsImpl::fillInodeDentryPositionAfterLookup(req, inodePtr);

        // add cache item
        int fullPathDepth;
        auto fullPath = req->getStandardFullPath(fullPathDepth);
        if (!req->isPathRoot()) fsImpl_->addPathInodeCacheItem(req, inodePtr);

        // do work
        clientOp *cop = req->getClientOp();
        struct stat *cur_stat_ptr = &(cop->op.stat.statbuf);
        fromInMemInode2Statbuf(inodePtr, cur_stat_ptr);
        fsWorker_->onTargetInodeFiguredOut(req, inodePtr);

        // Request is finished.
        // first check manual redirection
        gFsProcPtr->CheckNewedInMemInodeDst(
            FsProcWorker::fromIdx2WorkerId(fsImpl_->idx_), inodePtr->i_no,
            req->getApp()->getPid(), req->getTid());
        // return
        req->getClientOp()->op.stat.ret = 0;

        fsWorker_->submitFsReqCompletion(req);
      } else {
        submitFsGeneratedRequests(req);
      }
    }
  } else {
    // isMaster() == false
    InMemInode *inodePtr = req->getTargetInode();
    assert(inodePtr);
    fsWorker_->onTargetInodeFiguredOut(req, inodePtr);
    // do work
    clientOp *cop = req->getClientOp();
    struct stat *cur_stat_ptr = &(cop->op.stat.statbuf);
    fromInMemInode2Statbuf(inodePtr, cur_stat_ptr);
    // Request is finished.
    req->getClientOp()->op.stat.ret = 0;
    fsWorker_->submitFsReqCompletion(req);
  }  // isMaster();

  if (req->getState() == FsReqState::STAT_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processFstat(FsReq *req) {
  if (req->getState() == FsReqState::FSTAT_INIT) {
    FileObj *fileObj = req->getFileObj();
    if (fileObj != nullptr) {
      InMemInode *fileInode = fileObj->ip;
      while (!fileInode->tryLock()) {
        // spin
      }
      clientOp *cop = req->getClientOp();
      struct stat *cur_stat_ptr = &(cop->op.fstat.statbuf);
      fromInMemInode2Statbuf(fileInode, cur_stat_ptr);
      fileInode->unLock();
      // request is finished
      fsWorker_->onTargetInodeFiguredOut(req, fileInode);
      cop->op.fstat.ret = 0;
      fsWorker_->submitFsReqCompletion(req);
    } else {
      req->setState(FsReqState::FSTAT_ERR);
    }
  }

  if (req->getState() == FsReqState::FSTAT_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
void FileMng::processRename(FsReq *req) {
  // req->incrNumFsm();
  // only master can execute rename
  assert(fsWorker_->getWid() == FsProcWorker::kMasterWidConst);

  if (req->getState() == FsReqState::RENAME_LOOKUP_SRC) {
    SPDLOG_DEBUG("RENAME_LOOKUP_SRC");
    InMemInode *srcDirInode = req->getDirInode();

    if (srcDirInode != nullptr) {
      InMemInode *fileInode = req->getTargetInode();
      if (fileInode != nullptr) {
        req->setState(FsReqState::RENAME_VALIDATE_SRC_DENTRY_INMEM);
        req->setFileIno(fileInode->i_no);
      } else {
        // fileInode == nullptr && srcDirInode != nullptr
        req->setState(FsReqState::RENAME_GET_SRC_FILE_INO);
      }
    } else {
      // srcDirInode == nullptr, lookup parent dir
      bool is_err;
      srcDirInode = fsImpl_->getParDirInode(req, is_err);
      if (is_err) {
        req->setState(FsReqState::RENAME_ERR);
        req->setError(FS_REQ_ERROR_POSIX_ENOENT);
      } else {
        // no error when lookup parent directory
        if (req->numTotalPendingIoReq() == 0) {
          req->setDirInode(srcDirInode);
          req->setState(FsReqState::RENAME_GET_SRC_FILE_INO);
        } else {
          submitFsGeneratedRequests(req);
        }
      }  // is_err
    }    // srcDir != nullptr
  }      // RENAME_LOOKUP_SRC

  if (req->getState() == FsReqState::RENAME_VALIDATE_SRC_DENTRY_INMEM) {
    SPDLOG_DEBUG("RENAME_VALIDATE_SRC_DENTRY_INMEM");
    InMemInode *srcTargetInode = req->getTargetInode();
    InMemInode *srcDirInode = req->getDirInode();
    assert(srcTargetInode != nullptr);
    while ((!srcTargetInode->tryLock()) || (!srcDirInode->tryLock())) {
      // spin
    }
    int rt = fsImpl_->checkInodeDentryInMem(req, srcDirInode, srcTargetInode,
                                            req->getLeafName());
    if (rt < 0) {
      throw std::runtime_error("checkInodeDentryInMem error");
    }
    if (req->numTotalPendingIoReq() == 0) {
      req->setState(FsReqState::RENAME_LOOKUP_DST);
    } else {
      submitFsGeneratedRequests(req);
    }
    srcDirInode->unLock();
    srcTargetInode->unLock();
  }  // RENAME_VALIDATE_SRC_DENTRY_INMEM

  if (req->getState() == FsReqState::RENAME_GET_SRC_FILE_INO) {
    SPDLOG_DEBUG("RENAME_GET_SRC_FILE_INO");
    InMemInode *srcDirInode = req->getDirInode();
    while (!srcDirInode->tryLock()) {
      // spin
    }

    bool is_err;
    uint32_t fileIno =
        fsImpl_->lookupDir(req, srcDirInode, req->getLeafName(), is_err);
    if (is_err) {
      // cannot find source file inode
      req->setState(FsReqState::RENAME_ERR);
      assert(req->getErrorNo() == FS_REQ_ERROR_FILE_NOT_FOUND);
    } else {
      if (fileIno > 0) {
        // lookup succeed
        req->setFileIno(fileIno);
        req->setState(FsReqState::RENAME_GET_SRC_INODE);
      }
      if (req->numTotalPendingIoReq() > 0) {
        assert(fileIno == 0);
        submitFsGeneratedRequests(req);
      }
    }  // is_err

    srcDirInode->unLock();
  }  // RENAME_GET_SRC_FILE_INO

  if (req->getState() == FsReqState::RENAME_GET_SRC_INODE) {
    SPDLOG_DEBUG("RENAME_GET_SRC_INODE");
    InMemInode *srcInode = req->getTargetInode();
    if (srcInode == nullptr) {
      srcInode = fsImpl_->getFileInode(req, req->getFileInum());
    }
    while (!srcInode->tryLock()) {
      // spin
    }
    if (req->numTotalPendingIoReq() == 0) {
      FsImpl::fillInodeDentryPositionAfterLookup(req, srcInode);
      fsWorker_->onTargetInodeFiguredOut(req, srcInode);
      req->setState(FsReqState::RENAME_LOOKUP_DST);
    } else {
      submitFsGeneratedRequests(req);
    }
    srcInode->unLock();
  }  // RENAME_GET_SRC_INODE

  if (req->getState() == FsReqState::RENAME_LOOKUP_DST) {
    SPDLOG_DEBUG("RENAME_LOKUP_DST dstExist:{}", req->isDstExist());
    InMemInode *dstDirInode = req->getDstDirInode();

    if (dstDirInode != nullptr) {
      InMemInode *dstFileInode = req->getDstTargetInode();
      if (dstFileInode != nullptr) {
        // both file inode and parent inode have been resolved
        // req->setState(FsReqState::RENAME_TRANSACTION);
        req->setState(FsReqState::RENAME_VALIDATE_DST_DENTRY_INMEM);
        req->setDstFileIno(dstFileInode->i_no);
      } else {
        // dstFileInode == nullptr && dstFileInode != nullptr
        req->setState(FsReqState::RENAME_GET_DST_FILE_INO);
      }
    } else {
      // dstDirInode == nullptr, lookup parent dir
      bool is_err;
      dstDirInode =
          fsImpl_->getParDirInode(req, req->getDstPathTokens(), is_err);
      if (is_err) {
        // There is some NOT_FOUND in the directory path
        assert(req->getErrorNo() == FS_REQ_ERROR_FILE_NOT_FOUND);
        req->setState(FsReqState::RENAME_ERR);
      } else {
        if (req->numTotalPendingIoReq() == 0) {
          req->setDstDirInode(dstDirInode);
          req->setState(FsReqState::RENAME_GET_DST_FILE_INO);
        } else {
          submitFsGeneratedRequests(req);
        }
      }  // is_err
    }    // dstDirInode != nullptr
  }      // RENAME_LOOKUP_DST

  if (req->getState() == FsReqState::RENAME_VALIDATE_DST_DENTRY_INMEM) {
    SPDLOG_DEBUG("RENAME_VALIDATE_DST");
    InMemInode *dstTargetInode = req->getDstTargetInode();
    InMemInode *dstDirInode = req->getDstDirInode();
    assert(dstTargetInode != nullptr);
    while ((!dstTargetInode->tryLock()) || (!dstDirInode->tryLock())) {
      // spin
    }
    int rt = fsImpl_->checkInodeDentryInMem(req, dstDirInode, dstTargetInode,
                                            req->getNewLeafName());
    if (rt < 0) {
      assert(req->numTotalPendingIoReq() > 0);
    }
    if (req->numTotalPendingIoReq() == 0) {
      req->setState(FsReqState::RENAME_TRANSACTION);
    } else {
      submitFsGeneratedRequests(req);
    }
    dstDirInode->unLock();
    dstTargetInode->unLock();
  }  // RENAME_VALIDATE_DST_DENTRY_INMEM

  if (req->getState() == FsReqState::RENAME_GET_DST_FILE_INO) {
    SPDLOG_DEBUG("RENAME_GET_DST_FILE_INO");
    InMemInode *dstDirInode = req->getDstDirInode();
    InMemInode *srcInode = req->getTargetInode();
    InMemInode *rootDirInode = fsImpl_->rootDirInode(req);
    assert(srcInode != nullptr);
    assert(dstDirInode != nullptr);
    assert(rootDirInode != nullptr);
    while (dstDirInode->tryLock()) {
      // spin
    }

    auto curDirMap = req->getDstDirMap();
    // curDirMap can be 0 if the dir is root dir
    assert((curDirMap != nullptr) ||
           (curDirMap == nullptr && dstDirInode == rootDirInode));
    bool curDstPathNotExist;
    if (dstDirInode == rootDirInode) {
      curDstPathNotExist = (req->getDstTargetInode() == nullptr);
    } else {
      curDstPathNotExist =
          (req->getDstTargetInode() == nullptr &&
           (curDirMap->map.size() + 2) ==
               req->getDstDirInode()->inodeData->i_dentry_count);
    }
    SPDLOG_DEBUG("Rename dstNoExist?:{}", curDstPathNotExist);
    if (!curDstPathNotExist) {
      bool is_err;
      uint32_t fileIno =
          fsImpl_->lookupDir(req, dstDirInode, req->getNewLeafName(), is_err);
      if (is_err) {
        // cannot find dst file inode
        req->setDstExist(false);
        req->resetErr();
        req->setState(FsReqState::RENAME_TRANSACTION);
      } else {
        if (fileIno > 0) {
          // dst ino found
          req->setState(FsReqState::RENAME_GET_DST_INODE);
          req->setDstFileIno(fileIno);
        } else {
          assert(fileIno == 0);
          submitFsGeneratedRequests(req);
        }
      }  // is_err
    } else {
      req->setState(FsReqState::RENAME_TRANSACTION);
      req->setDstExist(false);
    }
    dstDirInode->unLock();
  }  // RENAME_GET_DST_FILE_INO

  if (req->getState() == FsReqState::RENAME_GET_DST_INODE) {
    SPDLOG_DEBUG("RENAME_GET_DST_INODE");
    InMemInode *srcInode = req->getTargetInode();
    InMemInode *dstInode = req->getDstTargetInode();
    if (dstInode == nullptr) {
      dstInode = fsImpl_->getFileInode(req, req->getDstFileInum());
    }
    while (!dstInode->tryLock()) {
      // spin
    }
    if (req->numTotalPendingIoReq() == 0) {
      // we find the dst inode
      if (srcInode->inodeData->type == T_DIR) {
        if (dstInode->inodeData->type != T_DIR) {
          // src is a directory, dst is not a directory
          req->setError(FS_REQ_ERROR_POSIX_ENOTDIR);
          req->setState(FsReqState::RENAME_ERR);
        } else {
          // src and dst are both directores
          if (dstInode->inodeData->size > 2 * sizeof(cfs_dirent)) {
            // dst dir is not empty
            req->setError(FS_REQ_ERROR_POSIX_ENOTEMPTY);
            req->setState(FsReqState::RENAME_ERR);
          }
        }
      } else {
        // srcInode is T_FILE
        if (dstInode->inodeData->type == T_DIR) {
          // rename a file to a directory
          req->setError(FS_REQ_ERROR_POSIX_ENOENT);
          req->setState(FsReqState::RENAME_ERR);
        }
      }  // src.type == T_DIR

      if (!req->hasError()) {
        // no error, then go on processing
        FsImpl::fillInodeDentryPositionAfterLookup(req, dstInode,
                                                   /*dentryNo*/ 1);
        fsWorker_->onDstTargetInodeFiguredOut(req, dstInode);
        req->setState(FsReqState::RENAME_TRANSACTION);
      }
    } else {
      submitFsGeneratedRequests(req);
    }  // numTotalPendingIoReq() == 0
    dstInode->unLock();
  }  // RENAME_GET_DST_INODE

  if (req->getState() == FsReqState::RENAME_TRANSACTION) {
    SPDLOG_DEBUG("RENAME_TRANSACTION");
    InMemInode *dstLeafInode = req->getDstTargetInode();
    InMemInode *dstDirInode = req->getDstDirInode();
    InMemInode *srcLeafInode = req->getTargetInode();
    InMemInode *srcDirInode = req->getDirInode();
    assert(dstDirInode != nullptr);
    assert(srcLeafInode != nullptr);
    assert(srcDirInode != nullptr);
    SPDLOG_DEBUG("req->isDstExist():{}", req->isDstExist());
    if (req->isDstExist()) {
      // dst is existing
      int srcInoBlockDentryIdx = -1;
      block_no_t srcDentryBlockNo = -1;
      SPDLOG_DEBUG("srcDirIno:{} dstDirIno:{}", srcDirInode->i_no,
                   dstDirInode->i_no);
      auto posPair = srcLeafInode->getDentryDataBlockPosition(
          srcDirInode, req->getLeafName());
      SET_DENTRY_DB_POS_FROM_PAIR(posPair, srcDentryBlockNo,
                                  srcInoBlockDentryIdx);
      assert(dstLeafInode != nullptr);
      // Step-1: modify dst's dentry, let it point to src's inode
      int rt = fsImpl_->dentryPointToNewInode(
          req, dstLeafInode, srcLeafInode, dstDirInode, req->getNewLeafName());
      assert(rt >= 0);
      auto dstPosPair = dstLeafInode->getDentryDataBlockPosition(
          dstDirInode, req->getNewLeafName());
      int dstInoBlockDentryIdx = -1;
      block_no_t dstDentryBlockNo = -1;
      SET_DENTRY_DB_POS_FROM_PAIR(dstPosPair, dstDentryBlockNo,
                                  dstInoBlockDentryIdx);
      dstLeafInode->delDentryDataBlockPosition(dstDirInode,
                                               req->getNewLeafName());
      srcLeafInode->addDentryDataBlockPosition(
          dstDirInode, req->getNewLeafName(), dstDentryBlockNo,
          dstInoBlockDentryIdx);
      assert(req->numTotalPendingIoReq() == 0);
      // Step-2: remove old dentry from srcDir
      rt = fsImpl_->removeFromDir(req, srcDirInode, srcInoBlockDentryIdx,
                                  srcDentryBlockNo, req->getLeafName());
      assert(req->numTotalPendingIoReq() == 0);
      srcLeafInode->delDentryDataBlockPosition(srcDirInode, req->getLeafName());
      srcDirInode->adjustDentryCount(-1);
      // Step-3: unlink the existing inode on the owner since the directory
      // changes have already been made here. But before that, notify the user
      // of the rename completion.
      req->setState(FsReqState::RENAME_RETURN_ORIG_DST_DATABLOCK);
      req->setReqBgGC(true);

      // delete entries in the permissionMap
      FsPermission::MapEntry dum;
      fsImpl_->removePathInodeCacheItem(req, &dum);
      fsImpl_->permission->RegisterGC(dum.first);
      fsImpl_->removeSecondPathInodeCacheItem(req, &dum);
      fsImpl_->permission->RegisterGC(dum.first);
      fsImpl_->addSecondPathInodeCacheItem(req, srcLeafInode);
      // DONE
      req->getClientOp()->op.rename.ret = 0;
      fsWorker_->submitFsReqCompletion(req);
    } else {
      assert(dstLeafInode == nullptr);
      // dst is not existing
      int srcInoBlockDentryIdx = -1;
      block_no_t srcDentryBlockNo = -1;
      auto posPair = srcLeafInode->getDentryDataBlockPosition(
          srcDirInode, req->getLeafName());
      SET_DENTRY_DB_POS_FROM_PAIR(posPair, srcDentryBlockNo,
                                  srcInoBlockDentryIdx);
      // Step-1: append the new dentry to dstDir
      // NOTE: we must put appendToDir() first because, it may happen that
      // appendToDir() requires to addDataBlock to dst directory data block,
      // which requires IO If we put this in the beginning of transaction, it
      // will be correct
      fsImpl_->appendToDir(req, dstDirInode, srcLeafInode,
                           req->getNewLeafName());
      if (req->numTotalPendingIoReq() == 0) {
        dstDirInode->adjustDentryCount(1);
        SPDLOG_DEBUG(
            "processRename DST not existing: incr dst dentry Count to:{}",
            dstDirInode->inodeData->i_dentry_count);
        // Step-2: remove old dentry from srcDir
        fsImpl_->removeFromDir(req, srcDirInode, srcInoBlockDentryIdx,
                               srcDentryBlockNo, req->getLeafName());
        srcLeafInode->delDentryDataBlockPosition(srcDirInode,
                                                 req->getLeafName());
        // Here, we cannot yield for IO, since appendToDir() has succeed
        // We assume the dentry's data block has not been replaced, since we
        // force lookup in the state RENAME_VALIDATE_XXX_DENTRY_INMEM
        // TODO (jingliu): we also need to do this for UNLINK
        assert(req->numTotalPendingIoReq() == 0);
        srcDirInode->adjustDentryCount(-1);
        // delete entries in the permissionMap
        FsPermission::MapEntry dum;
        fsImpl_->removePathInodeCacheItem(req, &dum);
        fsImpl_->permission->RegisterGC(dum.first);
        // we must add this entry to path-perm map in the end
        fsImpl_->addSecondPathInodeCacheItem(req, srcLeafInode);
        // DONE
        req->getClientOp()->op.rename.ret = 0;
        fsWorker_->submitFsReqCompletion(req);
      } else {
        submitFsGeneratedRequests(req);
      }  // numTotalPendingIoReq() == 0
    }    // isDstExist()
  }      // RENAME_TRANSACTION

  if (req->getState() == FsReqState::RENAME_RETURN_ORIG_DST_DATABLOCK) {
    auto inode = req->getDstTargetInode();
    assert(inode != nullptr);
    auto master = static_cast<FsProcWorkerMaster *>(fsWorker_);
    auto owner = master->getInodeOwner(inode->i_no);
    req->setReqBgGC(false);
    fsWorker_->releaseFsReq(req);

    if (owner == fsWorker_->getWid()) {
      FileMng::UnlinkOp::OwnerUnlinkInode(this, inode->i_no, inode);
    } else {
      cfs_ino_t *ptr = new cfs_ino_t;
      *ptr = inode->i_no;

      FsProcMessage msg;
      msg.type = FsProcMessageType::kOwnerUnlinkInode;
      msg.ctx = static_cast<void *>(ptr);
      fsWorker_->messenger->send_message(owner, msg);
    }
  }

  if (req->getState() == FsReqState::RENAME_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }  // RENAME_ERROR
}  // processRename()
#pragma GCC diagnostic pop

#if CFS_JOURNAL(ON)
inline static BitmapChangeOps *getOrCreateBitmapChangeOpsForWid(
    std::unordered_map<int, BitmapChangeOps *> &m, int wid) {
  auto search = m.find(wid);
  if (search != m.end()) return search->second;

  auto value = new BitmapChangeOps();
  m[wid] = value;
  return value;
}
#endif

#if CFS_JOURNAL(ON)
inline static void updateWidToBitmapChangeOps(
    std::unordered_map<int, BitmapChangeOps *> &m, int wid, bool add_or_del,
    uint64_t block_no) {
  auto changeOps = getOrCreateBitmapChangeOpsForWid(m, wid);
  if (add_or_del)
    changeOps->blocksToSet.push_back(block_no);
  else
    changeOps->blocksToClear.push_back(block_no);
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::updateBitmapsOnJournalWriteComplete(JournalEntry *jentry) {
  // TODO How much of a performance penalty are we paying by using this lambda
  // instead of just duplicating the code for those variables?
  auto glambda = [](std::unordered_map<int, BitmapChangeOps *> &m,
                    const auto &blocks, bool set_or_clear) {
    for (auto block_no : blocks) {
      cfs_bno_t bmap_disk_bno = get_bmap_block_for_pba(block_no);
      auto wid_for_bmap = getWidForBitmapBlock(bmap_disk_bno);
      updateWidToBitmapChangeOps(m, wid_for_bmap, set_or_clear, block_no);
    }
  };

  std::unordered_map<int, BitmapChangeOps *> widToBitmapOps;
  glambda(widToBitmapOps, jentry->blocks_for_bitmap_set, true);
  glambda(widToBitmapOps, jentry->blocks_for_bitmap_clear, false);

  if (!(jentry->inode_alloc_dealloc.empty())) {
    // update inode bitmaps here if primary, otherwise send mgs to primary (0)
    auto primaryChangeOps = getOrCreateBitmapChangeOpsForWid(widToBitmapOps, 0);
    primaryChangeOps->inodeBitmapChanges.insert(
        jentry->inode_alloc_dealloc.begin(), jentry->inode_alloc_dealloc.end());
  }

  auto wid = fsWorker_->getWid();
  auto search = widToBitmapOps.find(wid);
  if (search != widToBitmapOps.end()) {
    processBitmapChanges(search->second, /*journalled_locally*/ true);
    // remove local changes from the map so the rest can be sent as messages
    widToBitmapOps.erase(search);
  }

  FsProcMessage msg;
  msg.type = FsProcMessageType::BITMAP_CHANGES;
  for (auto iter : widToBitmapOps) {
    SPDLOG_DEBUG("BITMAP_CHANGES: wid {} sending message to {}", wid,
                 iter.first);
    assert(iter.first != wid);  // cannot send message to self!
    msg.ctx = (void *)iter.second;
    fsWorker_->messenger->send_message(iter.first, msg);
  }
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::onJournalWriteComplete(void *arg, bool success) {
  auto ctxPtr = (std::tuple<FileMng *, FsReq *, JournalEntry *> *)arg;
  FileMng *mng;
  FsReq *req;
  JournalEntry *jentry;
  std::tie(mng, req, jentry) = (*ctxPtr);

  // TODO reset inodeLogEntry on success, revert to old one on failure
  if (success) {
    mng->updateBitmapsOnJournalWriteComplete(jentry);
    if (req->getType() != FsReqType::WSYNC) {
      req->setState(FsReqState::FSYNC_DONE);
    } else {
      // WSYNC
      req->setState(FsReqState::WSYNC_DONE);
      if (!success) {
        req->setState(FsReqState::WSYNC_ERR);
      }
    }
  } else {
    req->setState(FsReqState::FSYNC_ERR);
  }
  req->startOnCpuTimer();
  mng->onFdataSyncComplete(req, success);

  // NOTE: onJournalWriteComplete is only called on single file descriptors by
  // the client. Therefore, a file descriptor must be active and so, this
  // operation can not be due to an unlink. Fsync after unlink can only happen
  // periodically or throught syncall / syncunlinked which use
  // onBatchedJournalWriteComplete.

  // TODO: syncall should use the group interface
  // Since syncall currently calls fsync on each dirty inode, it includes
  // unlinked inodes. However, by ensuring that syncunlinked is called before
  // syncall, we can avoid this until syncall also uses the group interface and
  // calls onBatchedJournalWriteComplete
  assert([](auto inode_alloc_dealloc) -> bool {
    for (auto const &[ino, created] : inode_alloc_dealloc) {
      if (!created) return false;
    }
    return true;
  }(jentry->inode_alloc_dealloc));
  delete jentry;
  delete ctxPtr;
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::updateBlockBitmaps(std::vector<uint64_t> &blocks_to_set,
                                 std::vector<uint64_t> &blocks_to_clear,
                                 std::unordered_set<cfs_bno_t> &bmaps_cleared) {
  // NOTE: in release mode, this function does not check if the bitmap changes
  // are for bitmaps owned by this FileMng worker. The caller / message creator
  // is responsible for ensuring the message reaches the right worker.

  auto bitmapsToCheckpoint =
      fsWorker_->jmgr->checkpointInput->bitmapsToCheckpoint;

  // if a block was allocated to an inode, the block bitmap bit needs to be set.
  // While the bit is set in the dirty block bitmap, we need to update the
  // stable bitmap on fsync.
  for (auto block_no : blocks_to_set) {
    cfs_bno_t lba = conv_pba_to_lba(block_no);
    cfs_bno_t bmap_disk_bno = get_bmap_block_for_lba(lba);
    SPDLOG_DEBUG("data block: pba={}, lba={}", block_no, lba);
    SPDLOG_DEBUG("belongs to bitmap: pba={}, lba={}", bmap_disk_bno,
                 bmap_disk_bno - get_bmap_start_block_for_worker(0));
    assert((getWidForBitmapBlock(bmap_disk_bno)) == (fsWorker_->getWid()));
    bitmapsToCheckpoint[bmap_disk_bno] = nullptr;
    SPDLOG_DEBUG("setting bit {} in stable bitmap", lba % BPB);
    char *stable_bmap = fsWorker_->jmgr->GetStableDataBitmap(bmap_disk_bno);
    assert(stable_bmap != nullptr);
    block_set_bit(lba % BPB, stable_bmap);
  }

  // if a block was deallocated, the dirty block bitmap does not know about it.
  // In addition to stable bitmap, we must clear the bit in dirty block bitmap.
  // However, if this deallocation was NOT journalled_locally, then we must lock
  // the block bitmap so that no future blocks are allocated until checkpointing
  // completes.
  for (auto block_no : blocks_to_clear) {
    cfs_bno_t lba = conv_pba_to_lba(block_no);
    cfs_bno_t bmap_disk_bno = get_bmap_block_for_lba(lba);
    SPDLOG_DEBUG("data block: pba={}, lba={}", block_no, lba);
    SPDLOG_DEBUG("belongs to bitmap: pba={}, lba={}", bmap_disk_bno,
                 bmap_disk_bno - get_bmap_start_block_for_worker(0));

    assert((getWidForBitmapBlock(bmap_disk_bno)) == (fsWorker_->getWid()));
    bmaps_cleared.insert(bmap_disk_bno);
    bitmapsToCheckpoint[bmap_disk_bno] = nullptr;
    SPDLOG_DEBUG("clearing bit {} in stable+dirty bitmap", lba % BPB);
    char *stable_bmap = fsWorker_->jmgr->GetStableDataBitmap(bmap_disk_bno);
    assert(stable_bmap != nullptr);
    block_clear_bit(lba % BPB, stable_bmap);
    auto dirty_bmap = fsImpl_->getLockedDirtyBitmap(bmap_disk_bno);
    block_clear_bit(lba % BPB, dirty_bmap->getBufPtr());
    fsImpl_->releaseLockedDirtyBitmap(dirty_bmap);
  }
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::processInodeBitmapChanges(
    std::unordered_map<cfs_ino_t, bool> &inodeBitmapChanges) {
  auto wid = fsWorker_->getWid();
  if (inodeBitmapChanges.size() > 0 && wid != FsProcWorker::kMasterWidConst) {
    throw std::runtime_error(
        "inodeBitmapChanges cannot be handled by secondary workers");
  }
  auto inodeBitmapsToCheckpoint =
      fsWorker_->jmgr->checkpointInput->inodeBitmapsToCheckpoint;
  for (const auto &it : inodeBitmapChanges) {
    cfs_ino_t ino = it.first;
    cfs_bno_t bmap_disk_bno = get_imap_for_inode(ino);
    inodeBitmapsToCheckpoint[bmap_disk_bno] = nullptr;
    char *stable_bmap = fsWorker_->jmgr->GetStableInodeBitmap(bmap_disk_bno);
    assert(stable_bmap != nullptr);
    if (it.second) {
      block_set_bit(ino % BPB, stable_bmap);
    } else {
      block_clear_bit(ino % BPB, stable_bmap);
      auto dirtyInodeBitmap = fsImpl_->getDirtyInodeBitmap(bmap_disk_bno);
      block_clear_bit(ino % BPB, dirtyInodeBitmap);
    }
  }
}
#endif

#if CFS_JOURNAL(ON)
bool FileMng::allBitmapsInMemory(BitmapChangeOps *changes) {
  std::unordered_set<cfs_bno_t> bmap_blocks;
  for (cfs_bno_t pba : changes->blocksToSet) {
    bmap_blocks.insert(get_bmap_block_for_pba(pba));
  }

  for (cfs_bno_t pba : changes->blocksToClear) {
    bmap_blocks.insert(get_bmap_block_for_pba(pba));
  }

  // NOTE: not checking imap as it always is in memory.
  // Ensuring every bmap in bmap_blocks is in memory.
  // We refer to stable bmap instead of the dirty bitmaps as a bitmap
  // must be in memory if it is in the stable map.
  for (cfs_bno_t bmap_pba : bmap_blocks) {
    if (fsWorker_->jmgr->GetStableDataBitmap(bmap_pba) == nullptr) {
      SPDLOG_ERROR("Bitmap block {} not in memory", bmap_pba);
      return false;
    }
  }

  return true;
}
#endif

#if CFS_JOURNAL(ON)
// When the journal is enabled, we process bitmap changes when things are
// written to the journal that involved bitmaps from other workers.
void FileMng::processRemoteBitmapChanges(FsReq *req) {
  BitmapChangeOps *changes =
      static_cast<BitmapChangeOps *>(req->completionCallbackCtx);
  auto state = req->getState();
  if (state == FsReqState::REMOTE_BITMAP_CHANGES_BMAPS_READY) {
    fsWorker_->releaseFsReq(req);
    processBitmapChanges(changes, /*journalled_locally*/ false);
    return;
  }

  assert(state == FsReqState::REMOTE_BITMAP_CHANGES_INIT);

  std::unordered_set<cfs_bno_t> bmap_blocks;
  for (cfs_bno_t pba : changes->blocksToClear) {
    bmap_blocks.insert(get_bmap_block_for_pba(pba));
  }

  for (cfs_bno_t bmap_block : bmap_blocks) {
    if (fsWorker_->jmgr->GetStableDataBitmap(bmap_block) != nullptr) {
      continue;
    }

    auto curBmapItemPtr =
        fsImpl_->getBlock(fsImpl_->bmapBlockBuf_, bmap_block, req);
    if (curBmapItemPtr->isInMem()) {
      throw std::runtime_error("Bitmap block not in stable map");
    }
  }

  req->setState(FsReqState::REMOTE_BITMAP_CHANGES_BMAPS_READY);
  // If there is io remaining, it will complete before processing this request.
  // If no i/o remaining, we repeat this function again.
  if (req->numTotalPendingIoReq() != 0) {
    submitFsGeneratedRequests(req);
  } else {
    processRemoteBitmapChanges(req);
  }
}
#else
// When the journal is NOT enabled, we process bitmap changes when an inode is
// unlinked and must deallocate blocks belonging to other workers.
void FileMng::processRemoteBitmapChanges(FsReq *req) {
  BitmapChangeOps *changes =
      static_cast<BitmapChangeOps *>(req->completionCallbackCtx);

  struct ValCtx {
    BlockBufferItem *bmap;
    std::vector<uint64_t> blocks;
  };

  std::unordered_map<cfs_bno_t, struct ValCtx> bmap_bno_to_blocks;
  for (cfs_bno_t pba : changes->blocksToClear) {
    auto bmap_bno = get_bmap_block_for_pba(pba);
    auto [it, inserted] = bmap_bno_to_blocks.try_emplace(
        bmap_bno, nullptr, std::vector<uint64_t>{});
    auto &val = it->second;
    val.blocks.push_back(pba);
    if (inserted) {
      assert(val.bmap == nullptr);
      val.bmap = fsImpl_->getBlock(fsImpl_->bmapBlockBuf_, bmap_bno, req);
    }
  }

  if (req->numTotalPendingIoReq() != 0) {
    submitFsGeneratedRequests(req);
    goto end;
  }

  for (auto &[bmap_bno, val] : bmap_bno_to_blocks) {
    char *bmap_ptr = val.bmap->getBufPtr();
    for (uint64_t pba : val.blocks) {
      cfs_bno_t lba = conv_pba_to_lba(pba);
      SPDLOG_DEBUG("data block: pba={}, lba={}", pba, lba);
      SPDLOG_DEBUG("belongs to bitmap: pba={}, lba={}", bmap_bno,
                   bmap_bno - get_bmap_start_block_for_worker(0));
      assert((getWidForBitmapBlock(bmap_bno)) == (fsWorker_->getWid()));
      SPDLOG_DEBUG("clearing bit {} in data block bitmap", lba % BPB);
      block_clear_bit(lba % BPB, bmap_ptr);
    }
  }

  if (fsWorker_->isMasterWorker()) {
    // handle inode bitmap changes
    for (const auto ino : changes->inodesToClear) {
      cfs_bno_t bmap_disk_bno = get_imap_for_inode(ino);
      SPDLOG_DEBUG("clearing bit {} in inode bitmap block ({}) for inode {}",
                   ino % BPB, bmap_disk_bno, ino);
      auto dirtyInodeBitmap = fsImpl_->getDirtyInodeBitmap(bmap_disk_bno);
      block_clear_bit(ino % BPB, dirtyInodeBitmap);
    }
  }

  fsWorker_->releaseFsReq(req);
  delete changes;

end:
  for (auto &kv : bmap_bno_to_blocks) {
    auto &val = kv.second;
    if (val.bmap->isInMem()) fsImpl_->releaseLockedDirtyBitmap(val.bmap);
  }
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::processBitmapChanges(BitmapChangeOps *changes,
                                   bool journalled_locally) {
  assert(allBitmapsInMemory(changes));
  processInodeBitmapChanges(changes->inodeBitmapChanges);
  std::unordered_set<cfs_bno_t> bmaps_cleared;
  // TODO: two different functions? In the fast path when journalled locally we
  // do not need access to bmaps to clear.
  updateBlockBitmaps(changes->blocksToSet, changes->blocksToClear,
                     bmaps_cleared);

  delete changes;
  if (journalled_locally || bmaps_cleared.empty()) {
    return;
  }

#if CFS_JOURNAL(LOCAL_JOURNAL)
  // deallocations came as messages from other workers, the deallocations are in
  // another journal if using local journal mode. We need to "lock" these
  // bitmaps so that we do not allocate from them till checkpointing is done.
  SPDLOG_DEBUG("adding {} entries to immutableBlockBitmaps",
               bmaps_cleared.size());
  fsImpl_->immutableBlockBitmaps_.insert(bmaps_cleared.cbegin(),
                                         bmaps_cleared.cend());

  // We register the bmaps we've performed deallocations on with the journal
  // manager. If we are currently checkpointing, the journal manager will make
  // sure to still keep those bmaps locked after checkpointing completes.
  fsWorker_->jmgr->registerBitmapsWithDeallocations(bmaps_cleared);
#endif  // CFS_JOURNAL(LOCAL_JOURNAL)
}
#endif

void FileMng::onFdataSyncComplete(FsReq *req, bool success) {
  // TODO recordInoAppFsyncDone marks the inode clean. Do we want to do that
  // on failure?
  fsWorker_->recordInoAppFsyncDone(req->getFileInum());
  if (req->reqType != FsReqType::SYNCALL) {
    if (!success) req->setError();
    fsWorker_->submitFsReqCompletion(req);
    return;
  }

  // reqtype is syncall
  if (success) {
    req->setState(FsReqState::SYNCALL_INIT);
  } else {
    req->setError();
    req->setState(FsReqState::SYNCALL_ERR);
  }
  fsWorker_->submitReadyReq(req);
}

// Specialized for ldb
// ASSUME: append (no previous data reach to the server)
// ASSUME: inode is in memory (for whatever reason, open, creation)
// Will allocate blocks for this and with a fsync() followed
// Assume the inode can be 100% resolved by retrieve its fd
// Assume the inode never shared with other applications
void FileMng::processLdbWsync(FsReq *req) {
  if (req->getState() == FsReqState::WSYNC_ALLOC_ENTIRE) {
    auto req_control_it = inflight_wsync_req.find(req);
    if (req_control_it == inflight_wsync_req.end()) {
      // this is a new wsync request
      if (inflight_wsync_req.size() < kNumMaxInflightWsync) {
        inflight_wsync_req.emplace(req);
        // fprintf(stderr, "insert num inflight:%lu\n",
        // inflight_wsync_req.size());
      } else {
        // fprintf(stderr, "put to ready num inflight:%lu\n",
        //        inflight_wsync_req.size());
        req->stopOnCpuTimer();
        fsWorker_->submitReadyReq(req);
        return;
      }
    }

    FileObj *fileobj = req->getFileObj();
    assert(fileobj->off == 0);
    struct wsyncOp *op_ptr = &(req->getClientOp()->op.wsync);
    InMemInode *fileInode = req->getTargetInode();
    assert(fileInode != nullptr);
    fsWorker_->onTargetInodeFiguredOut(req, fileInode);
    // TODO (jingliu): test to see if this function works as expected
    uint64_t n_num_blocks = 0;
    if (op_ptr->file_size > 0) {
      n_num_blocks = fsImpl_->writeInodeAllocDataBlock(
          req, fileInode, /*start_offset*/ 0, op_ptr->file_size);
    }
    if (n_num_blocks < 0) {
      req->setState(FsReqState::WSYNC_ERR);
    } else {
      if (req->numTotalPendingIoReq() == 0) {
        // successfully allocate data blocks
        req->setState(FsReqState::WSYNC_DATA_MODIFY);
      } else {
        submitFsGeneratedRequests(req);
      }
    }
  }  // WSYNC_ALLOC_WRITE

  // we copy the data one block by one block
  if (req->getState() == FsReqState::WSYNC_DATA_MODIFY) {
    auto fileInode = req->getTargetInode();
    assert(fileInode != nullptr);
    struct wsyncOp *op_ptr = &(req->getClientOp()->op.wsync);
    int64_t nw = 0;
    if (op_ptr->file_size > 0) {
      nw = fsImpl_->writeEntireInode(req, req->getTargetInode(), op_ptr);
    }
    if (nw < 0) {
      req->setState(FsReqState::WSYNC_ERR);
    } else {
      assert(req->numTotalPendingIoReq() == 0);
      // now we dirty the inode directly
      int rc = fsImpl_->writeFileInode(req, fileInode->i_no, fileInode);
      if (rc > 0) {
        // we update inode's info and journal entries
        fileInode->inodeData->size = op_ptr->file_size;
        fileInode->logEntry->set_size(fileInode->inodeData->size);
        req->setState(FsReqState::WSYNC_DATA_BIO);
      } else {
        throw std::runtime_error(
            "We don't allow IO happening in WSYNC's update inode");
      }
    }
  }

  // WSYNC_DATA_MODIFY
  if (req->getState() == FsReqState::WSYNC_DATA_BIO) {
    InMemInode *fileInode = req->getTargetInode();
    struct wsyncOp *op_ptr = &(req->getClientOp()->op.wsync);
    assert(fileInode != nullptr);
    fsWorker_->recordInoActiveAppFsync(fileInode->i_no);
    if (fsWorker_->checkInoFsyncToWait(fileInode->i_no)) {
      SPDLOG_DEBUG("force fsync to readyList. ino:{}", fileInode->i_no);
      req->stopOnCpuTimer();
      fsWorker_->submitReadyReq(req);
    } else {
      bool needFlush;
      int numFlushed = fsImpl_->flushInodeData(fsWorker_, req, needFlush);
      // SPDLOG_INFO("WSYNC_DATA_BIO numFlushed:{} ino:{} needFlush:{}
      // fileSize:{}",
      //             numFlushed, fileInode->i_no, needFlush,
      //             fileInode->inodeData->size);
      if (needFlush) {
        if (numFlushed > 0) {
          fsWorker_->recordInoFsyncReqSubmitToDev(fileInode->i_no);
          req->setState(FsReqState::WSYNC_DATA_SYNC_WAIT);
          req->stopOnCpuTimer();
        } else {
          SPDLOG_WARN("num flush is {} for WSYNC. fileSize:{}", numFlushed,
                      op_ptr->file_size);
          // std::cerr << "numFlushed:" << numFlushed << std::endl;
          // throw std::runtime_error("WSYNC_DATA_BIO must have numflush");
          // assert(req->numTotalPendingIoReq() == 0);
          req->setState(FsReqState::WSYNC_DATA_SYNC_DONE);
        }
      } else {
        // throw std::runtime_error("wsync, this inode must be flushed");
        SPDLOG_WARN("wsync this inode does not flush. fileSize:{}",
                    op_ptr->file_size);
        req->setState(FsReqState::WSYNC_DATA_SYNC_DONE);
      }
    }
  }  // WSYNC_DATA_BIO

  if (req->getState() == FsReqState::WSYNC_DATA_SYNC_WAIT) {
    // intended to be nothing here
  }  // WSYNC_DATA_SYNC_WAIT

#if CFS_JOURNAL(ON)
  // TODO (anthony): how can we deal with CFS_DISABLE_JOURNAL for this?
  // For now, we will only run leveldb with journal enabled, should be fine
  // for wsync request, we will never need to read-in the bitmap
  // because it is guaranteed to be written in this run
  if (req->getState() == FsReqState::WSYNC_DATA_SYNC_DONE) {
    auto inode = req->getTargetInode();
    assert(!inode->logEntry->empty());
    req->stopOnCpuTimer();
    req->setState(FsReqState::WSYNC_JOURNAL);
  }  // WSYNC_DATA_SYNC_DONE

  if (req->getState() == FsReqState::WSYNC_JOURNAL) {
    auto inode = req->getTargetInode();
    auto jentry = new JournalEntry();
    jentry->addInodeLogEntry(inode->logEntry);
    auto ctx =
        new std::tuple<FileMng *, FsReq *, JournalEntry *>(this, req, jentry);
    fsWorker_->jmgr->submitJournalEntry(jentry, onJournalWriteComplete, ctx);
    // The journal will handle it from here
    return;
  }
#else
  if (req->getState() == FsReqState::WSYNC_DATA_SYNC_DONE) {
    // No journaling, so we just sync data.
    // TODO: what about the inode table / bitmaps?
    // As per previous implementation, all metadata is sync'd at shutdown.
    req->setState(FsReqState::WSYNC_DONE);
    onFdataSyncComplete(req, /*success*/ true);
    return;
  }
#endif

  if (req->getState() == FsReqState::WSYNC_DONE) {
    throw std::runtime_error("WSYNC_DONE should not be handled by mng");
    // in onDataSyncComplete(), which is JOURNAL's last completion
    // it will directly submitFsReqCompletion
    // req->stopOnCpuTimer();
    // fsWorker_->recordInoAppFsyncDone(req->getFileInum());
    // fsWorker_->submitFsReqCompletion(req);
  }

  if (req->getState() == FsReqState::WSYNC_ERR) {
    req->setError();
    onFdataSyncComplete(req, /*success*/ false);
    // fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processFdataSync(FsReq *req) {
  if (req->getState() == FsReqState::FSYNC_DATA_BIO) {
    InMemInode *fileInode = req->getTargetInode();
    if (fileInode != nullptr && fileInode->i_no > 0) {
      fsWorker_->recordInoActiveAppFsync(fileInode->i_no);
      if (fsWorker_->checkInoFsyncToWait(fileInode->i_no)) {
        SPDLOG_DEBUG("force fsync to readyList. ino:{}", fileInode->i_no);
        req->stopOnCpuTimer();
        fsWorker_->submitReadyReq(req);
      } else {
        bool needFlush;
        int numFlushed = fsImpl_->flushInodeData(fsWorker_, req, needFlush);
        SPDLOG_DEBUG("FSYNC_DATA_BIO numFLushed:{} ino:{} needFlush:{}",
                     numFlushed, fileInode->i_no, needFlush);
        if (needFlush) {
          if (numFlushed > 0) {
            fsWorker_->recordInoFsyncReqSubmitToDev(fileInode->i_no);
            req->setState(FsReqState::FSYNC_DATA_SYNC_WAIT);
            req->stopOnCpuTimer();
          } else {
            // if (req->isReadCounterLargeShowWarning()) {
            //   SPDLOG_WARN(
            //       "cannot submit flushReq, resubmit to ready, this Req's "
            //       "readyQueue Counter:{}",
            //       req->getReadyQueueCounter());
            // }
            if (req->numTotalPendingIoReq() == 0) {
              req->stopOnCpuTimer();
              fsWorker_->submitReadyReq(req);
            }
          }
        } else {
          // SPDLOG_ERROR("this inode does not need to be flushed");
          req->setState(FsReqState::FSYNC_DATA_SYNC_DONE);
        }
      }
    } else {
      SPDLOG_WARN("fsync fileInode not exiting");
      req->setState(FsReqState::FSYNC_ERR);
    }
  }

  if (req->getState() == FsReqState::FSYNC_DATA_SYNC_WAIT) {
    // we do not do anything here, but the call-back of device-writing will
    // finish the states transfer
  }

  // BIO is done
#if CFS_JOURNAL(ON)
  if (req->getState() == FsReqState::FSYNC_DATA_SYNC_DONE) {
    auto inode = req->getTargetInode();
    if (inode->logEntry->empty()) {
      // NOTE: Currently, if we do a write(), the mtime is not updated. So if
      // the filesize does not change and no new blocks are allocated, the log
      // entry should be empty, which means no journal write (which is unfair
      // since ext4 would write to the journal). Luckily, we always set the size
      // on a write(), so inode log entry is not empty and journal write will
      // trigger. However, that is not safe to rely on as we may fix that
      // problem in the future and only add size to log entry if changed. TODO:
      // update mtime on a write().

      req->setState(FsReqState::FSYNC_DONE);
    } else {
      // incase we are deleting some files, their blocks belong to bitmaps that
      // may not be in memory. We need to first ensure they are in memory before
      // proceeding.
      auto wid = fsWorker_->getWid();
      std::unordered_set<cfs_bno_t> bmap_blocks;
      inode->logEntry->get_bitmap_for_dealloc_blocks_for_wid(wid, bmap_blocks);
      for (const cfs_bno_t bmap_block : bmap_blocks) {
        if (fsWorker_->jmgr->GetStableDataBitmap(bmap_block) != nullptr) {
          continue;
        }

        auto curBmapItemPtr =
            fsImpl_->getBlock(fsImpl_->bmapBlockBuf_, bmap_block, req);
        // Since it is not in the stable map, we are issuing a request.
        // If it is in memory, something is wrong. Because everytime we read a
        // block, if it is a bitmap block, we place it in the stable map. See
        // FsProc_Worker::submitDevAsyncReadReqCompletion
        if (curBmapItemPtr->isInMem()) {
          throw std::runtime_error("Bitmap Block not in stable map");
        }
      }

      if (req->numTotalPendingIoReq() == 0) {
        // NOTE: we don't include the journal part into this timer
        // Q: should we do that?
        req->stopOnCpuTimer();
        req->setState(FsReqState::FSYNC_JOURNAL);
      } else {
        submitFsGeneratedRequests(req);
      }
    }
  }
  if (req->getState() == FsReqState::FSYNC_JOURNAL) {
    // TODO current code assumes that any write to an inode that is being
    // journalled will be stalled until the journal operation has completed. We
    // will have to come up with a better solution to prevent inode writes from
    // stalling. Example - keeping 2 inode log entries, and merging / selecting
    // based on failure / success of fsync.
    auto inode = req->getTargetInode();
    auto jentry = new JournalEntry();
    jentry->addInodeLogEntry(inode->logEntry);
    auto ctx =
        new std::tuple<FileMng *, FsReq *, JournalEntry *>(this, req, jentry);
    fsWorker_->jmgr->submitJournalEntry(jentry, onJournalWriteComplete, ctx);
    // The journal will handle it from here
    return;
  }
#else
  if (req->getState() == FsReqState::FSYNC_DATA_SYNC_DONE) {
    // No journaling, so we just sync data.
    // TODO: what about the inode table / bitmaps?
    // As per previous implementation, all metadata is sync'd at shutdown.
    req->setState(FsReqState::FSYNC_DONE);
    onFdataSyncComplete(req, /*success*/ true);
    return;
  }
#endif

  if (req->getState() == FsReqState::FSYNC_DONE) {
    // if onFdataSyncComplete is called here
    // no journal writing is done
    // so the CPU timer must already been started (since it's dequeued from
    // ready)
    onFdataSyncComplete(req, /*success*/ true);
  }

  if (req->getState() == FsReqState::FSYNC_ERR) {
    onFdataSyncComplete(req, /*success*/ false);
  }
}

void FileMng::ProcessSyncUnlinkedBeforeSyncall(FsReq *req) {
  // can only be called in the initial state of syncall
  assert(req->getState() == FsReqState::SYNCALL_INIT);
  struct LocalReqCtx {
    FileMng *mng;
    FsReq *req;
  };

  auto after_syncunlinked = [](FsReq *sync_unlinked_req, void *vctx) {
    // assert sync unlinked succeeded
    SPDLOG_DEBUG("syncunlinked completed, resuming syncall");
    auto ctx = static_cast<struct LocalReqCtx *>(vctx);
    auto mng = ctx->mng;
    auto syncall_req = ctx->req;

    if (sync_unlinked_req->hasError()) {
      SPDLOG_ERROR("syncunlinked called by syncall failed, errno={}",
                   sync_unlinked_req->getErrorNo());
      syncall_req->setState(FsReqState::SYNCALL_ERR);
    }

    delete ctx;
    mng->fsWorker_->releaseFsReq(sync_unlinked_req);
    mng->processSyncall(syncall_req);
  };

  struct LocalReqCtx *ctx = new struct LocalReqCtx();
  ctx->mng = this;
  ctx->req = req;

  FsReq *sync_unlinked_req = fsWorker_->GetEmptyRequestFromPool();
  sync_unlinked_req->setType(FsReqType::SYNCUNLINKED);
  sync_unlinked_req->setState(FsReqState::SYNCUNLINKED_GUARD);
  sync_unlinked_req->completionCallback = after_syncunlinked;
  sync_unlinked_req->completionCallbackCtx = ctx;
  // TODO (ask jing) : what should tid be when this is created on server side?
  // for now choosing same tid as syncall_req
  sync_unlinked_req->tid = req->tid;
  processSyncunlinked(sync_unlinked_req);
}

void FileMng::processSyncall(FsReq *req) {
  SPDLOG_DEBUG("WORKER {} is processing SYNCALL", fsWorker_->getWid());
  if (req->getState() == FsReqState::SYNCALL_GUARD) {
    if (syncall_in_progress_) {
      SPDLOG_DEBUG("Syncall already in progress, queuing for later");
      syncall_requests_.push(req);
      return;
    } else {
      syncall_in_progress_ = true;
      req->setState(FsReqState::SYNCALL_INIT);
    }
  }

  if (req->getState() == FsReqState::SYNCALL_INIT) {
#if CFS_JOURNAL(ON)
    // NOTE: Since our current syncall implementation is not in batch mode, and
    // only batch mode handles syncunlinked, we should not call fdatasync on any
    // unlinked inodes. We can either ignore any thing unlinked, or a safer way
    // (which also guarantees freeing up of space), is to call syncunlinked
    // before syncall starts. It is also fast as syncunlinked uses the batch
    // mode.
    // TODO: convert syncall to use batch mode.
    if (!fsImpl_->unlinkedInodeSet_.empty()) {
      SPDLOG_DEBUG(
          "syncall must first remove inodes from unlinked inode set: {} "
          "remaining",
          fsImpl_->unlinkedInodeSet_.size());
      ProcessSyncUnlinkedBeforeSyncall(req);
      return;
    }
#endif

    if (fsImpl_->dirtyInodeSet_.empty()) {
      SPDLOG_DEBUG("WORKER {} dirtyInodeSet_ is empty", fsWorker_->getWid());
      req->setState(FsReqState::SYNCALL_DONE);
    } else {
      SPDLOG_DEBUG("processSyncall numDirtyInode:{}",
                   fsImpl_->dirtyInodeSet_.size());
      // We randomly pick the inode to flush here for performance.
      // If several syncall is issued to this worker, we don't want
      // them to keep wait on single inode and waste the device bandwidth
      auto r = rand() % fsImpl_->dirtyInodeSet_.size();
      auto it = fsImpl_->dirtyInodeSet_.begin();
      std::advance(it, r);
      auto inodePtr = GetInMemInode(*it);
      if (inodePtr != nullptr) {
        req->targetInodePtr = nullptr;
        req->fileIno = *it;
        req->setTargetInode(inodePtr);
        req->setState(FsReqState::FSYNC_DATA_BIO);
        req->stopOnCpuTimer();
        fsWorker_->submitReadyReq(req);
      }
    }
  }

  bool complete = false;
  if (req->getState() == FsReqState::SYNCALL_DONE) {
    req->onFsReqCompletion();
    complete = true;
  }

  if (req->getState() == FsReqState::SYNCALL_ERR) {
    req->setError();
    req->onFsReqCompletion();
    complete = true;
  }

  if (complete) {
    syncall_in_progress_ = false;
    if (!syncall_requests_.empty()) {
      FsReq *queued_req = syncall_requests_.front();
      assert(queued_req->getState() == FsReqState::SYNCALL_GUARD);
      syncall_requests_.pop();
      processSyncall(queued_req);
    }
  }
}

#if CFS_JOURNAL(ON)
void FileMng::onBatchedJournalWriteComplete(void *arg, bool success) {
  auto ctx = reinterpret_cast<SyncBatchedContext *>(arg);
  FsReq *req = ctx->req;
  FileMng *mng = ctx->mng;
  // FIXME: recordBatchInoAppFsyncDone calls resetFileIndoeDirty which happens
  // regardless of success or failure.
  mng->fsWorker_->recordBatchInoAppFsyncDone(ctx->inodes);

  req->syncBatchesCompleted += 1;
  if (!success) {
    req->syncBatchesFailed += 1;
    goto end;
  }

  // success path
  mng->updateBitmapsOnJournalWriteComplete(ctx->jentry);

  for (auto const &[ino, created] : ctx->jentry->inode_alloc_dealloc) {
    if (!created) {  // destroyed
      FileMng::UnlinkOp::OwnerOnFsyncComplete(mng, ino, nullptr);
    }
  }

end:
  delete ctx;
  if (req->syncBatchesCompleted == req->syncBatchesCreated) {
    req->setState(FsReqState::SYNC_BATCHES_COMPLETE);
    // TODO once we know the right callback (processSyncAll or
    // processSyncUnlinked), we don't have to submit to ready queue, we can call
    // it here directly instead.
    mng->fsWorker_->submitReadyReq(req);
  }
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::submitSyncBatches(FsReq *req,
                                std::unordered_set<cfs_ino_t> &inodes) {
  if (inodes.empty()) {
    req->setState(FsReqState::SYNC_BATCHES_COMPLETE);
    return;
  }

  req->setState(FsReqState::SYNC_BATCHES_INPROGRESS);

  // create and submit batches
  std::vector<SyncBatchedContext *> batches;
  auto curBatch =
      new SyncBatchedContext(this, req, JournalManager::kMaxJournalBodySize);
  batches.push_back(curBatch);

  for (cfs_ino_t inum : inodes) {
    auto inode = GetInMemInode(inum);
    bool added = curBatch->try_add(inode->logEntry);
    if (added) continue;

    // couldn't add to batch, need to make a new batch
    curBatch =
        new SyncBatchedContext(this, req, JournalManager::kMaxJournalBodySize);
    batches.push_back(curBatch);
    added = curBatch->try_add(inode->logEntry);
    if (!added) {
      throw std::runtime_error("Failed to add inode to empty batch");
    }
  }

  req->syncBatchesCreated = batches.size();
  req->syncBatchesCompleted = 0;
  req->syncBatchesFailed = 0;

  for (SyncBatchedContext *ctx : batches) {
    fsWorker_->recordBatchInoActiveAppFsync(ctx->inodes);
    fsWorker_->jmgr->submitJournalEntry(ctx->jentry,
                                        onBatchedJournalWriteComplete, ctx);
  }
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::processSyncunlinked(FsReq *req) {
  // NOTE: each function can change state, so we always call the function
  // getState again.
  if (req->getState() == FsReqState::SYNCUNLINKED_GUARD) {
    if (syncunlinked_in_progress_) {
      SPDLOG_DEBUG("syncunlink in progress, queueing for later");
      syncunlinked_requests_.push(req);
      return;
    } else {
      syncunlinked_in_progress_ = true;
      req->setState(FsReqState::SYNCUNLINKED_INIT);
    }
  }

  if (req->getState() == FsReqState::SYNCUNLINKED_INIT) {
    submitSyncBatches(req, fsImpl_->unlinkedInodeSet_);
  }

  if (req->getState() == FsReqState::SYNC_BATCHES_COMPLETE) {
    assert(req->syncBatchesCompleted == req->syncBatchesCreated);
    if (req->syncBatchesFailed != 0) {
      req->setError();
    }

    // TODO write periodic syncunlinked path (right now only triggered via
    // client api)
    // TODO as part of syncall, remove inodes part of unlinked set.
    req->onFsReqCompletion();
    syncunlinked_in_progress_ = false;
    if (!syncunlinked_requests_.empty()) {
      FsReq *queued_req = syncunlinked_requests_.front();
      assert(queued_req->getState() == FsReqState::SYNCUNLINKED_GUARD);
      syncunlinked_requests_.pop();
      processSyncunlinked(queued_req);
    }
  }
}
#else
void FileMng::processSyncunlinked(FsReq *req) {
  // no sync required when you don't have a journal
  SPDLOG_DEBUG("No journal mode, NOP");
  req->onFsReqCompletion();
}
#endif

void FileMng::processMkdir(FsReq *req) {
  // req->incrNumFsm();
  if (req->getState() == FsReqState::MKDIR_GET_PRT_INODE) {
    bool is_err;
    InMemInode *parInode = nullptr;

    int curPathDepth;
    auto curPath = req->getStandardFullPath(curPathDepth);
    if (curPath.empty()) {
      // req wants to create root directory, not allowed
      req->setState(FsReqState::MKDIR_ERR);
    } else {
      // mkdir's target is not root directory

      InMemInode *curInode = req->getTargetInode();
      if (curInode != nullptr) {
        // here we do a trick to check if this dir's name is already
        // existing in target directory by checking the path cache.
        req->setState(FsReqState::MKDIR_ERR);
      } else {
        parInode = req->getDirInode();
        if (parInode != nullptr) {
          req->setState(FsReqState::MKDIR_ALLOC_INODE);
        } else {
          parInode = fsImpl_->getParDirInode(req, is_err);
          if (is_err) {
            req->setState(FsReqState::MKDIR_ERR);
          } else if (req->numTotalPendingIoReq() == 0) {
            req->setDirInode(parInode);
            req->setState(FsReqState::MKDIR_ALLOC_INODE);
          } else {
            submitFsGeneratedRequests(req);
          }
        }
      }
    }
  }

  if (req->getState() == FsReqState::MKDIR_ALLOC_INODE) {
    InMemInode *dirInode = req->getDirInode();
    while (!dirInode->tryLock()) {
      // spin
    }

    // FIXME: if allocating an inode can do io, maybe during that time, other
    // parts of this request can complete. For now, just proceeding sequentially
    InMemInode *inode = fsImpl_->AllocateInode(req);
    if (req->numTotalPendingIoReq() > 0) {
      dirInode->unLock();
      submitFsGeneratedRequests(req);
      return;
    }

    if (inode != nullptr) {
      SPDLOG_DEBUG("AllocateInode() for MKDIR return i_no:{}", inode->i_no);
      req->setFileIno(inode->i_no);
    } else {
      // force FSP down since cannot allocate inode
      throw std::runtime_error("MKDIR_ALLOC_INODE cannot allocate inode");
    }

    if (req->numTotalPendingIoReq() == 0) {
      // NOTE: initNewAllocateDinodeContent() must be called before
      // onTargetInodeFigureOut()
      // TODO (jingliu): warp this order as a function
      // That include, initNewContext->onTargetFiguredOut->setNextState
      // initNewAllocatedDinodeContent(inode->inodeData, T_DIR, inode->i_no);
      inode->initNewAllocatedDinodeContent(T_DIR);
      fsWorker_->onTargetInodeFiguredOut(req, inode);
      req->setState(FsReqState::MKDIR_INIT_DOTS);
      // add cache item
      int fullPathDepth;
      auto fullPath = req->getStandardFullPath(fullPathDepth);
      fsImpl_->addPathInodeCacheItem(req, inode);
    } else {
      req->setState(FsReqState::MKDIR_INODE_ALLOCED_NOT_IN_MEM);
      // see the MKDIR_INODE_ALLOCED_NOT_IN_MEM's first check of submit
      // submitFsGeneratedRequests(req);
    }
    dirInode->unLock();
  }

  if (req->getState() == FsReqState::MKDIR_INODE_ALLOCED_NOT_IN_MEM) {
    SPDLOG_DEBUG("MKDIR_INODE_ALLOCED_NOT_IN_MEM inum:{}", req->getFileInum());
    if (req->numTotalPendingIoReq() > 0) {
      // if it is just set to this state, we finish the IO here
      submitFsGeneratedRequests(req);
    } else {
      InMemInode *inode = fsImpl_->getFileInode(req, req->getFileInum());
      if (req->numTotalPendingIoReq() == 0) {
        // initNewAllocatedDinodeContent(inode->inodeData, T_DIR, inode->i_no);
        inode->initNewAllocatedDinodeContent(T_DIR);
        fsWorker_->onTargetInodeFiguredOut(req, inode);
        req->setState(FsReqState::MKDIR_INIT_DOTS);
        // add cache item
        int fullPathDepth;
        auto fullPath = req->getStandardFullPath(fullPathDepth);
        fsImpl_->addPathInodeCacheItem(req, inode);
      } else {
        submitFsGeneratedRequests(req);
      }
    }
  }

  if (req->getState() == FsReqState::MKDIR_INIT_DOTS) {
    // Create . and .. entries
    InMemInode *dirInode = req->getDirInode();
    InMemInode *fileInode = req->getTargetInode();
    while (!fileInode->tryLock()) {
      // spin
    }
    struct cfs_dirent cur_dirent[2];
    memset(&cur_dirent, 0, 2 * sizeof(struct cfs_dirent));
    cur_dirent[0].inum = fileInode->i_no;
    strncpy(&(cur_dirent[0].name[0]), ".", DIRSIZE);
    cur_dirent[1].inum = dirInode->i_no;
    strncpy(&(cur_dirent[1].name[0]), "..", DIRSIZE);
    auto rc = fsImpl_->writeInode(req, fileInode, (char *)(&cur_dirent),
                                  fileInode->inodeData->size,
                                  2 * sizeof(struct cfs_dirent));
    if (rc < 0) {
      req->setState(FsReqState::MKDIR_ERR);
    } else if (rc == 0 && req->numTotalPendingIoReq() > 0) {
      submitFsGeneratedRequests(req);
    } else {
      fileInode->setDentryCount(2);
      req->setState(FsReqState::MKDIR_UPDATE_PAR_DIR_DATA);
    }
    fileInode->unLock();
  }

  if (req->getState() == FsReqState::MKDIR_UPDATE_PAR_DIR_DATA) {
    // write the directory entry (name, inum) into the parent directory's data
    // block
    InMemInode *dirInode = req->getDirInode();
    InMemInode *fileInode = req->getTargetInode();
    while ((!dirInode->tryLock()) || (!fileInode->tryLock())) {
      // spin
    }
    fsImpl_->appendToDir(req, dirInode, fileInode);
    if (req->numTotalPendingIoReq() == 0) {
      // we can only reduce this *nlink* in the last entrace of this state
      // since this decrement is stateful
      dirInode->adjustDentryCount(1);
      dirInode->adjustNlink(1);
      req->setState(FsReqState::MKDIR_INODES_SET_DIRTY);
    } else {
      submitFsGeneratedRequests(req);
    }
    fileInode->unLock();
    dirInode->unLock();
  }

  if (req->getState() == FsReqState::MKDIR_INODES_SET_DIRTY) {
    InMemInode *dirInode = req->getDirInode();
    InMemInode *fileInode = req->getTargetInode();
    while ((!dirInode->tryLock()) || (!fileInode->tryLock())) {
      // spin
    }
    int rc = fsImpl_->writeFileInode(req, fileInode->i_no);
    if (rc > 0) {
      rc = fsImpl_->writeFileInode(req, dirInode->i_no);
      if (rc <= 0) {
        SPDLOG_ERROR("MKDIR_INODES_SET_DIRTY: cannot update parent directory");
        req->setState(FsReqState::MKDIR_ERR);
      } else {
        // DONE
        req->setState(FsReqState::MKDIR_FINI);
      }
    } else {
      // need to read inode block (though it should be rare)
      // Basically, it means the inode buffer is replaced between this
      // processing.
      submitFsGeneratedRequests(req);
    }
    fileInode->unLock();
    dirInode->unLock();
  }

  if (req->getState() == FsReqState::MKDIR_FINI) {
    req->getClientOp()->op.mkdir.ret = 0;
    fsWorker_->submitFsReqCompletion(req);
  }

  if (req->getState() == FsReqState::MKDIR_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processOpendir(FsReq *req) {
  if (req->getState() == FsReqState::OPENDIR_GET_CACHED_INODE) {
    int fullPathDepth = req->getStandardPathDepth();
    InMemInode *inodePtr = nullptr;
    if (fullPathDepth > 0) {
      inodePtr = req->getTargetInode();
      if (inodePtr != nullptr) {
        fsWorker_->onTargetInodeFiguredOut(req, inodePtr);
        req->setState(FsReqState::OPENDIR_READ_WHOLE_INODE);
      } else {
        // try to see if parent directory is in cache
        if (fullPathDepth > 1) {
          inodePtr = req->getDirInode();
          if (inodePtr != nullptr) {
            req->setState(FsReqState::OPENDIR_GET_FILE_INUM);
          }
        }
        if (inodePtr == nullptr) {
          // either cannot check cache, or check fail, must do lookup
          req->setState(FsReqState::OPENDIR_GET_PRT_INODE);
        }
      }
    } else {
      // req is opening root directory
      inodePtr = fsImpl_->rootDirInode(req);
      if (req->numTotalPendingIoReq() > 0) {
        // here, inodePtr will not be nullptr
        submitFsGeneratedRequests(req);
      } else {
        fsWorker_->onTargetInodeFiguredOut(req, inodePtr);
        req->setState(FsReqState::OPENDIR_READ_WHOLE_INODE);
      }
    }
  }

  if (req->getState() == FsReqState::OPENDIR_GET_PRT_INODE) {
    bool is_err;
    InMemInode *dirInode = fsImpl_->getParDirInode(req, is_err);
    if (is_err) {
      req->setState(FsReqState::OPENDIR_ERR);
    } else if (req->numTotalPendingIoReq() == 0) {
      // only transfer the state when dirInode is okay to use (in memory)
      req->setDirInode(dirInode);
      req->setState(FsReqState::OPENDIR_GET_FILE_INUM);
    } else {
      // next time, when this request is ready, it will start from GET_PRT_INODE
      // state
      submitFsGeneratedRequests(req);
    }
  }

  if (req->getState() == FsReqState::OPENDIR_GET_FILE_INUM) {
    InMemInode *dirInode = req->getDirInode();
    bool is_err;
    uint32_t fileIno =
        fsImpl_->lookupDir(req, dirInode, req->getLeafName(), is_err);
    if (is_err) {
      req->setState(FsReqState::OPENDIR_ERR);
    } else {
      if (fileIno > 0) {
        req->setFileIno(fileIno);
        req->setState(FsReqState::OPENDIR_GET_FILE_INODE);
      }
      if (req->numTotalPendingIoReq() != 0) {
        submitFsGeneratedRequests(req);
      }
    }
  }

  if (req->getState() == FsReqState::OPENDIR_GET_FILE_INODE) {
    InMemInode *inodePtr = fsImpl_->getFileInode(req, req->getFileInum());
    if (req->numTotalPendingIoReq() == 0) {
      fsWorker_->onTargetInodeFiguredOut(req, inodePtr);
      req->setState(FsReqState::OPENDIR_READ_WHOLE_INODE);
      FsImpl::fillInodeDentryPositionAfterLookup(req, inodePtr);
      // add cache item
      int fullPathDepth;
      auto fullPath = req->getStandardFullPath(fullPathDepth);
      fsImpl_->addPathInodeCacheItem(req, inodePtr);
    } else {
      submitFsGeneratedRequests(req);
    }
  }

  if (req->getState() == FsReqState::OPENDIR_READ_WHOLE_INODE) {
    InMemInode *fileInode = req->getTargetInode();
    char *dst = req->getMallocedDataPtr();
    SPDLOG_DEBUG(
        "processOpendir() READ_WHOLE_INODE inode size field - inMem:"
        " {} numDentry:{}",
        fileInode->inodeData->size, fileInode->inodeData->i_dentry_count);
    int64_t nread =
        fsImpl_->readInode(req, fileInode, dst, 0, fileInode->inodeData->size);
    if (nread < 0) {
      req->setState(FsReqState::OPENDIR_ERR);
    } else {
      if (req->numTotalPendingIoReq() == 0) {
        req->getClientOp()->op.opendir.numDentry = (nread / sizeof(cfs_dirent));
        fsWorker_->submitFsReqCompletion(req);
      } else {
        submitFsGeneratedRequests(req);
      }
    }
  }

  if (req->getState() == FsReqState::OPENDIR_ERR) {
    req->setError();
    fsWorker_->submitFsReqCompletion(req);
  }
}

void FileMng::processNewShmAllocated(FsReq *req) {
  if (req->getState() == FsReqState::NEW_SHM_ALLOC_SEND_MSG) {
    SPDLOG_DEBUG("FsReq::processNewShmAllocated wid:{} reqType:{}",
                 req->getWid(), getFsReqTypeOutputString(req->getType()));
    auto id_arrptr_pair = req->initAppNewAllocatedShm();
    auto id = id_arrptr_pair.first;
    auto arr_ptr = static_cast<SingleSizeMemBlockArr *>(id_arrptr_pair.second);
    auto app = req->getApp();
    req->getClientOp()->op.newshmop.shmid = id;
    assert(arr_ptr != nullptr);
    if (fsWorker_->getWid() == FsProcWorker::kMasterWidConst) {
      gFsProcPtr->g_app_shm_ids.AddAppShm(app->getPid(), id, arr_ptr);
    }
    for (int i = FsProcWorker::kMasterWidConst + 1;
         i < gFsProcPtr->getNumThreads(); ++i) {
      if (gFsProcPtr->checkWorkerActive(i)) {
        bool sent = static_cast<FsProcWorkerMaster *>(fsWorker_)->submitShmMsg(
            i, id, req);
        assert(sent);
        ++req->pendingShmMsg;
      }
    }
    req->setState(FsReqState::NEW_SHM_ALLOC_WAIT_REPLY);
  }

  if (req->getState() == FsReqState::NEW_SHM_ALLOC_WAIT_REPLY) {
    if (req->pendingShmMsg == 0) {
      fsWorker_->submitFsReqCompletion(req);
    }
  }
}

void FileMng::processPing(FsReq *req) {
  // to mark completion
  req->getClientOp()->op.ping.ret = 0;
  fsWorker_->submitFsReqCompletion(req);
}

void FileMng::processDumpInodes(FsReq *req) {
  std::stringstream ss;
  ss << "/tmp/dumpInodesPid_";
  ss << fsWorker_->getWid();
  ss << "-";
  std::time_t t = std::time(nullptr);
  std::tm tm = *std::localtime(&t);
  ss << std::put_time(&tm, "%a_%b_%d_%H-%M-%S-%Y");
  std::string dumpDirname = ss.str();
  req->getClientOp()->op.dumpinodes.ret =
      fsImpl_->dumpAllInodesToFile(dumpDirname.c_str());
  fsWorker_->submitFsReqCompletion(req);
}

void FileMng::fromInMemInode2Statbuf(InMemInode *inodePtr,
                                     struct stat *cur_stat_ptr) {
  SPDLOG_DEBUG(
      "fromInMemInode2Statbuf inode data fields - in-mem_ino:{}, "
      "size:{}, i_block_count",
      inodePtr->i_no, inodePtr->inodeData->size,
      inodePtr->inodeData->i_block_count);
  if (inodePtr->inodeData->type == T_DIR) {
    cur_stat_ptr->st_size =
        sizeof(struct cfs_dirent) * inodePtr->inodeData->i_dentry_count;
    cur_stat_ptr->st_mode = cur_stat_ptr->st_mode | S_IFDIR;
  } else {
    cur_stat_ptr->st_size = inodePtr->inodeData->size;
    cur_stat_ptr->st_mode = cur_stat_ptr->st_mode | S_IFREG;
  }
  // TODO(jingliu): fill more field into stbuf
  cur_stat_ptr->st_gid = inodePtr->inodeData->i_gid;
  cur_stat_ptr->st_blocks = inodePtr->inodeData->i_block_count;
  cur_stat_ptr->st_nlink = inodePtr->inodeData->nlink;
  cur_stat_ptr->st_ino = inodePtr->inodeData->i_no;
  assert(cur_stat_ptr->st_ino == inodePtr->i_no);
}

void FileMng::splitInodeDataBlockBufferSlot(
    InMemInode *inode, std::unordered_set<BlockBufferItem *> &items) {
  fsImpl_->splitInodeDataBlockBufferSlot(inode, items);
}

void FileMng::installDataBlockBufferSlot(
    InMemInode *inode, const std::unordered_set<BlockBufferItem *> &items) {
  fsImpl_->installInodeDataBlockBufferSlot(inode, items);
}

FsPermission::PCR FileMng::checkPermission(FsReq *req) {
  InMemInode *temp = nullptr;
  auto ret = fsImpl_->permission->checkPermission(req->getPathTokens(), {0, 0},
                                                  &req->parDirMap,
                                                  &req->parDirInodePtr, &temp);
  if (temp != nullptr) fsWorker_->onTargetInodeFiguredOut(req, temp);
  if (req->getPathTokens().size() == 1) {
    req->parDirInodePtr = fsImpl_->root_inode_;
  }
  return ret;
}

FsPermission::PCR FileMng::checkDstPermission(FsReq *req) {
  InMemInode *temp = nullptr;
  auto ret = fsImpl_->permission->checkPermission(
      req->getDstPathTokens(), {0, 0}, &req->dstParDirMap,
      &req->dstParDirInodePtr, &temp);
  if (temp != nullptr) fsWorker_->onDstTargetInodeFiguredOut(req, temp);
  if (req->getDstPathTokens().size() == 1) {
    req->dstParDirInodePtr = fsImpl_->root_inode_;
  }
  return ret;
}

InMemInode::InMemInode(uint32_t ino, int wid)
    : i_no(ino),
      ref(0),
      flags(0),
      isLocked(false),
      logEntry(nullptr),
      inodeData(nullptr),
      jinodeData(nullptr),
      isValid_(false),
      lock_(ATOMIC_FLAG_INIT),
      mngWid_(wid) {}

void InMemInode::initNewAllocatedDinodeContent(mode_t tp) {
  assert(inodeData != nullptr);
  memset(inodeData, 0, sizeof(cfs_dinode));
  inodeData->type = tp;
  setNlink(1);
  inodeData->i_no = i_no;
  logEntry->set_mode(tp);
}

bool InMemInode::tryLock() {
  bool lockSuccess = false;
#ifndef NONE_MT_LOCK
  while (lock_.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  if (!isLocked) {
    isLocked = true;
    lockSuccess = true;
  }
#ifndef NONE_MT_LOCK
  lock_.clear(std::memory_order_release);
#endif
  SPDLOG_DEBUG("tryLock() ino:{} return:{}", i_no, lockSuccess);
  return lockSuccess;
}

bool InMemInode::unLock() {
  bool rt = false;
#ifndef NONE_MT_LOCK
  while (lock_.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  if (isLocked) {
    isLocked = false;
    rt = true;
  }
#ifndef NONE_MT_LOCK
  lock_.clear(std::memory_order_release);
#endif
  SPDLOG_DEBUG("unLock() ino:{} return:{}", i_no, rt);
  return rt;
}

bool InMemInode::isAppProcReferring(pid_t pid) {
  auto it = appFdMap_.find(pid);
  return !((it == appFdMap_.end()) || it->second.empty());
}

void InMemInode::addDentryDataBlockPosition(InMemInode *parInode,
                                            const std::string &fileName,
                                            block_no_t dentryDataBlockNo,
                                            int withinBlockDentryIndex) {
  // assert(parInode != nullptr);
  auto it = inodeDentryDataBlockPosMap_.find(parInode);
  if (it == inodeDentryDataBlockPosMap_.end()) {
    inodeDentryDataBlockPosMap_.emplace(
        parInode, std::unordered_map<std::string, inode_dentry_dbpos_t>());
  }
  (inodeDentryDataBlockPosMap_[parInode])[fileName] =
      std::make_pair(dentryDataBlockNo, withinBlockDentryIndex);
}

inode_dentry_dbpos_t *InMemInode::getDentryDataBlockPosition(
    InMemInode *parInode, const std::string &fileName) {
  assert(parInode != nullptr);
  auto it = inodeDentryDataBlockPosMap_.find(parInode);
  if (it != inodeDentryDataBlockPosMap_.end()) {
    auto fileIt = it->second.find(fileName);
    if (fileIt != it->second.end()) {
      return &(fileIt->second);
    }
  }
  return nullptr;
}

int InMemInode::delDentryDataBlockPosition(InMemInode *parInode,
                                           const std::string &fileName) {
  assert(parInode != nullptr);
  auto it = inodeDentryDataBlockPosMap_.find(parInode);
  assert(it != inodeDentryDataBlockPosMap_.end());
  auto inIt = it->second.find(fileName);
  assert(inIt != (it->second).end());
  (it->second).erase(inIt);
  return 0;
}
