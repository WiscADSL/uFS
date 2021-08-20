#include <cassert>
#include <vector>

#include "stats/stats.h"

#include "spdlog/spdlog.h"

#include "FsProc_Fs.h"
#include "FsProc_Messenger.h"

void FileMng::ReassignmentOp::SendMessageOrExecute(FileMng *mng, int dst_wid,
                                                   Ctx *ctx) {
  if (mng->fsWorker_->getWid() == dst_wid) {
    ProcessReassignmentCtx(mng, ctx);
    return;
  }

  FsProcMessage msg;
  msg.type = FsProcMessageType::kReassignment;
  msg.ctx = static_cast<void *>(ctx);
  mng->fsWorker_->messenger->send_message(dst_wid, msg);
}

void FileMng::ReassignmentOp::OwnerExportThroughPrimary(
    FileMng *mng, cfs_ino_t ino, int new_owner_wid,
    ReassignmentOp::CallbackFn oldOwnerCallback, void *oldOwnerCallbackCtx,
    ReassignmentOp::CallbackFn newOwnerCallback, void *newOwnerCallbackCtx,
    bool batched) {
  // TODO assert that both exporter and importer wid are valid values.
  assert(new_owner_wid != mng->fsWorker_->getWid());

  // TODO use a pool
  auto ctx = new FileMng::ReassignmentOp::Ctx();
  ExportedInode &exp = ctx->exported_inode;
  bool exported = mng->exportInode(ino, exp);
  if (!exported && batched && mng->fsWorker_->isMasterWorker()) {
    ctx->state = State::kOldOwnerExportSuccessNotification;
    if (oldOwnerCallback != nullptr)
      oldOwnerCallback(mng, ctx, oldOwnerCallbackCtx);
    delete ctx;
    return;
  }

  assert(exported);

  // TODO: safeguard against invalid new_owner_wid values
  exp.importer_wid = new_owner_wid;

  // TODO better indication that exporting was successful
  assert((exp.exporter_wid != -1) && (exp.importer_wid != -1));

  ctx->state = State::kPrimaryRequestNewOwnerImport;
  ctx->oldOwnerCallback = oldOwnerCallback;
  ctx->oldOwnerCallbackCtx = oldOwnerCallbackCtx;
  ctx->newOwnerCallback = newOwnerCallback;
  ctx->newOwnerCallbackCtx = newOwnerCallbackCtx;

  // Primary needs to keep a copy of all inodes it reassigns as unlink needs
  // access to the nlink.
  if (exp.exporter_wid == FsProcWorker::kMasterWidConst) {
    // TODO: remove when nlink can be queried without InMemInode.
    assert(mng->fsWorker_->isMasterWorker());
    auto master = static_cast<FsProcWorkerMaster *>(mng->fsWorker_);
    master->unsafe_reassigned_inodes[ino] = exp.inode;
  }

  SendMessageOrExecute(mng, FsProcWorker::kMasterWidConst, ctx);
}

void FileMng::ReassignmentOp::BatchedOwnerExportThroughPrimary(
    FileMng *mng, const std::unordered_set<cfs_ino_t> &ino_vec,
    int new_owner_wid, ReassignmentOp::BatchedCallbackFn oldOwnerCallback,
    void *oldOwnerCallbackCtx,
    ReassignmentOp::BatchedCallbackFn newOwnerCallback,
    void *newOwnerCallbackCtx) {
  assert(new_owner_wid != mng->fsWorker_->getWid());
  assert(!ino_vec.empty());

  auto ctx = new FileMng::ReassignmentOp::BatchedCtx();
  ctx->n_total = ino_vec.size();
  // completions on old
  ctx->n_complete_old = 0;
  ctx->n_complete_new = 0;
  ctx->oldOwnerCallback = oldOwnerCallback;
  ctx->oldOwnerCallbackCtx = oldOwnerCallbackCtx;
  ctx->newOwnerCallback = newOwnerCallback;
  ctx->newOwnerCallbackCtx = newOwnerCallbackCtx;

  if (ctx->oldOwnerCallback == nullptr)
    ctx->oldOwnerCallback = BatchedCallbackNoOpFn;

  if (ctx->newOwnerCallback == nullptr)
    ctx->newOwnerCallback = BatchedCallbackNoOpFn;

  auto perInodeOldOwnerCallback = [](FileMng *mng, const Ctx *ctx, void *bctx) {
    assert(ctx->state == State::kOldOwnerExportSuccessNotification);
    auto batch = static_cast<BatchedCtx *>(bctx);
    batch->n_complete_old++;
    if (batch->n_complete_old == batch->n_total) {
      batch->oldOwnerCallback(mng, batch, batch->oldOwnerCallbackCtx);
      delete batch;
    }
  };

  auto perInodeNewOwnerCallback = [](FileMng *mng, const Ctx *ctx, void *bctx) {
    assert(ctx->state == State::kNewOwnerImport);
    auto batch = static_cast<BatchedCtx *>(bctx);
    batch->n_complete_new++;
    if (batch->n_complete_new == batch->n_total) {
      batch->newOwnerCallback(mng, batch, batch->newOwnerCallbackCtx);
    }
  };
  for (auto ino : ino_vec) {
    // NOTE: While both workers share the same batched context, they access two
    // different variables.
    FileMng::ReassignmentOp::OwnerExportThroughPrimary(
        mng, ino, new_owner_wid, perInodeOldOwnerCallback, ctx,
        perInodeNewOwnerCallback, ctx, true);
  }
}

void FileMng::ReassignmentOp::PrimaryRequestNewOwnerImport(
    FileMng *mng, FileMng::ReassignmentOp::Ctx *ctx) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(ctx->state == State::kPrimaryRequestNewOwnerImport);

  auto inode = ctx->exported_inode.inode;
  auto master = static_cast<FsProcWorkerMaster *>(mng->fsWorker_);
  master->markInodeOwnershipChangeInProgress(inode->i_no);
  // NOTE: These entries can be deleted when:
  // 1. The app queries for it: Application is expected to store the new owner.
  // If an application is multithreaded, it is expected to synchronize itself.
  // 2. The app exits
  master->addRecentlyReassignedInodeFds(ctx->exported_inode);

  bool canChange = inode->setManageWorkerId(InMemInode::kUnknownManageWorkerId,
                                            ctx->exported_inode.importer_wid);
  assert(canChange);

  ctx->state = State::kNewOwnerImport;
  SendMessageOrExecute(mng, ctx->exported_inode.importer_wid, ctx);
}

void FileMng::ReassignmentOp::NewOwnerImport(
    FileMng *mng, FileMng::ReassignmentOp::Ctx *ctx) {
  assert(ctx->state == State::kNewOwnerImport);

  mng->importInode(ctx->exported_inode);
  // Primary kept a copy of all reassigned inodes as unlink needed it. Since we
  // are importing back on primary, it can be cleaned up.
  auto exp = ctx->exported_inode;
  if (exp.importer_wid == FsProcWorker::kMasterWidConst) {
    assert(mng->fsWorker_->isMasterWorker());
    auto master = static_cast<FsProcWorkerMaster *>(mng->fsWorker_);
    auto erased = master->unsafe_reassigned_inodes.erase(exp.inode->i_no);
    assert(erased > 0);
  }

  // TODO update app file descriptors? (part of inode?)
  if (ctx->newOwnerCallback != nullptr) {
    (*(ctx->newOwnerCallback))(mng, ctx, ctx->newOwnerCallbackCtx);
  }

  ctx->state = State::kPrimaryImportSuccessNotification;
  // TODO handle failure cases, for now ensure any failure crashes the system.
  SendMessageOrExecute(mng, FsProcWorker::kMasterWidConst, ctx);
}

void FileMng::ReassignmentOp::PrimaryImportSuccessNotification(
    FileMng *mng, FileMng::ReassignmentOp::Ctx *ctx) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(ctx->state == State::kPrimaryImportSuccessNotification);

  auto master = static_cast<FsProcWorkerMaster *>(mng->fsWorker_);
  master->markInodeOwnershipUpdated(ctx->exported_inode.inode->i_no,
                                    ctx->exported_inode.importer_wid);

  SPDLOG_DEBUG("inode {} ownership updated to {}",
               ctx->exported_inode.inode->i_no,
               ctx->exported_inode.importer_wid);
  ctx->state = State::kOldOwnerExportSuccessNotification;
  SendMessageOrExecute(mng, ctx->exported_inode.exporter_wid, ctx);
}

void FileMng::ReassignmentOp::OldOwnerExportSuccessNotification(
    FileMng *mng, FileMng::ReassignmentOp::Ctx *ctx) {
  assert(ctx->state == State::kOldOwnerExportSuccessNotification);

  if (ctx->oldOwnerCallback != nullptr) {
    (*(ctx->oldOwnerCallback))(mng, ctx, ctx->oldOwnerCallbackCtx);
  }

  delete ctx;
}

void FileMng::ReassignmentOp::ProcessReassignmentCtx(
    FileMng *mng, FileMng::ReassignmentOp::Ctx *ctx) {
  switch (ctx->state) {
    case State::kPrimaryRequestNewOwnerImport:
      PrimaryRequestNewOwnerImport(mng, ctx);
      break;
    case State::kNewOwnerImport:
      NewOwnerImport(mng, ctx);
      break;
    case State::kPrimaryImportSuccessNotification:
      PrimaryImportSuccessNotification(mng, ctx);
      break;
    case State::kOldOwnerExportSuccessNotification:
      OldOwnerExportSuccessNotification(mng, ctx);
      break;
    case State::kOwnerExportThroughPrimary:
      // This case should not exist as it is only an init stage.
      throw std::runtime_error("Unsupported kOwnerExportThroughPrimary");
    default:
      throw std::runtime_error("Unknown ReassignmentOp::Ctx state");
  }
}

// Client triggered reassignment request
void FileMng::ReassignmentOp::ProcessInodeReassignmentReq(FileMng *mng,
                                                          FsReq *req) {
  assert(req->getType() == FsReqType::INODE_REASSIGNMENT);

  struct inodeReassignmentOp &irop = req->copPtr->op.inodeReassignment;
  if (mng->fsWorker_->getWid() != irop.curOwner) {
    req->setError(FS_REQ_ERROR_POSIX_EACCES);
    mng->fsWorker_->submitFsReqCompletion(req);
    return;
  }

  InMemInode *minode = mng->GetInMemInode(irop.inode);
  // Existence in map does not mean it owns it (atleast right now, since primary
  // also has it in the map).
  if (minode == nullptr || minode->getManageWorkerId() != irop.curOwner) {
    req->setError(FS_REQ_ERROR_POSIX_EPERM);
    mng->fsWorker_->submitFsReqCompletion(req);
    return;
  }

  // this worker is the owner
  switch (irop.type) {
    case 0:
      // Returns 0 if this wid is the owner.
      irop.ret = 0;
      mng->fsWorker_->submitFsReqCompletion(req);
      return;
    case 1: {
      // TODO safeguard against invalid newOwner values
      auto oldOwnerCallback = [](FileMng *mng, const Ctx *reassignmentCtx,
                                 void *myctx) {
        auto req = static_cast<FsReq *>(myctx);
        if (reassignmentCtx->state ==
            State::kOldOwnerExportSuccessNotification) {
          req->copPtr->op.inodeReassignment.ret = 0;
          SPDLOG_INFO("{}: successful reassignment - old owner",
                      mng->fsWorker_->getWid());
        } else {
          req->setError(FS_REQ_ERROR_POSIX_RET);
        }
        mng->fsWorker_->submitFsReqCompletion(req);
      };

      auto newOwnerCallback = [](FileMng *mng, const Ctx *reassignmentCtx,
                                 void *myctx) {
        SPDLOG_INFO("{}: successful reassignment - new owner",
                    mng->fsWorker_->getWid());
      };

      FileMng::ReassignmentOp::OwnerExportThroughPrimary(
          mng, irop.inode, irop.newOwner,
          /* oldOwnerCallback, oldOwnerCallbackCtx */
          oldOwnerCallback, static_cast<void *>(req),
          /* newOwnerCallback, newOwnerCallbackCtx */
          newOwnerCallback, nullptr);
    } break;
    default:
      req->setError(FS_REQ_ERROR_POSIX_EINVAL);
      mng->fsWorker_->submitFsReqCompletion(req);
      return;
  }
}

// TODO consider having a folder for filemng ops, each op has its own file.
void FileMng::UnlinkOp::PrimaryHandleError(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  // TODO add specific error codes later once we have per op state
  assert(req->getState() == FsReqState::UNLINK_ERR);
  req->getClientOp()->op.unlink.ret = -1;
  mng->fsWorker_->submitFsReqCompletion(req);
}

void FileMng::UnlinkOp::PrimaryLoadParentInode(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(req->getState() == FsReqState::UNLINK_PRIMARY_LOAD_PRT_INODE);
  InMemInode *dirInode = req->getDirInode();
  // TODO: read permission check
  if (dirInode != nullptr) {
    req->setState(FsReqState::UNLINK_PRIMARY_GET_FILE_INUM);
    return;
  }

  // dirInode not in memory, need to read it in.
  bool is_err = false;
  dirInode = mng->fsImpl_->getParDirInode(req, is_err);
  if (is_err) {
    req->setState(FsReqState::UNLINK_ERR);
    return;
  }

  if (req->numTotalPendingIoReq() == 0) {
    req->setDirInode(dirInode);
    req->setState(FsReqState::UNLINK_PRIMARY_GET_FILE_INUM);
  } else {
    mng->submitFsGeneratedRequests(req);
  }
}

// Once we read the directory inode, we need to find the inode number for the
// given name. The directory data pages may need to be read for this.
void FileMng::UnlinkOp::PrimaryGetFileInum(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(req->getState() == FsReqState::UNLINK_PRIMARY_GET_FILE_INUM);
  InMemInode *dirInode = req->getDirInode();
  assert(dirInode != nullptr);

  auto target_inode = req->getTargetInode();
  if (target_inode != nullptr) {
    req->setFileIno(target_inode->i_no);
    req->setState(FsReqState::UNLINK_PRIMARY_REMOVE_DENTRY);
    return;
  }

  bool is_err = false;
  uint32_t fileIno =
      mng->fsImpl_->lookupDir(req, dirInode, req->getLeafName(), is_err);
  if (is_err) {
    req->setState(FsReqState::UNLINK_ERR);
    return;
  }

  if (fileIno > 0) {
    req->setFileIno(fileIno);
    req->setState(FsReqState::UNLINK_PRIMARY_LOAD_INODE);
  }

  if (req->numTotalPendingIoReq() != 0) {
    assert(fileIno == 0);
    mng->submitFsGeneratedRequests(req);
  }
}

void FileMng::UnlinkOp::PrimaryLoadInode(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(req->getState() == FsReqState::UNLINK_PRIMARY_LOAD_INODE);
  InMemInode *inodePtr = req->getTargetInode();
  if (inodePtr == nullptr) {
    inodePtr = mng->fsImpl_->getFileInode(req, req->getFileInum());
    // TODO (ask jing) - inodePtr is not null but numTotalPendingIoReq() > 0
    if (req->numTotalPendingIoReq() != 0) {
      mng->submitFsGeneratedRequests(req);
      return;
    }
  }

  assert(req->numTotalPendingIoReq() == 0);
  FsImpl::fillInodeDentryPositionAfterLookup(req, inodePtr);
  mng->fsWorker_->onTargetInodeFiguredOut(req, inodePtr);
  req->setState(FsReqState::UNLINK_PRIMARY_REMOVE_DENTRY);
}

void FileMng::UnlinkOp::PrimaryRemoveDentry(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(req->getState() == FsReqState::UNLINK_PRIMARY_REMOVE_DENTRY);
  InMemInode *dirInode = req->getDirInode();
  InMemInode *fileInode = req->getTargetInode();
  assert(dirInode != nullptr);
  assert(fileInode != nullptr);
  // TODO: write permission check
  FsPermission::MapEntry entry;
  mng->fsImpl_->removePathInodeCacheItem(req, &entry);
  mng->fsImpl_->permission->RegisterGC(entry.first);
  int ret = mng->fsImpl_->removeFromDir(req, dirInode, req->getLeafName());
  if (ret != 0) {
    req->setState(FsReqState::UNLINK_ERR);
    return;
  }

  assert(req->numTotalPendingIoReq() == 0);
  dirInode->adjustDentryCount(-1);
  req->setState(FsReqState::UNLINK_PRIMARY_NOTIFY_USER);
}

// notifies user of unlink completion
void FileMng::UnlinkOp::PrimaryNotifyUser(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  req->setReqBgGC(true);
  req->setState(FsReqState::UNLINK_PRIMARY_HANDOFF_TO_OWNER);

  req->getClientOp()->op.unlink.ret = 0;
  mng->fsWorker_->submitFsReqCompletion(req);
}

void FileMng::UnlinkOp::PrimaryHandoffToOwner(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::UNLINK_PRIMARY_HANDOFF_TO_OWNER);
  assert(req->isReqBgGC());
  assert(mng->fsWorker_->isMasterWorker());
  auto master = static_cast<FsProcWorkerMaster *>(mng->fsWorker_);
  auto inodePtr = req->getTargetInode();
  assert(inodePtr != nullptr);
  auto ino = inodePtr->i_no;
  auto owner_wid = master->getInodeOwner(inodePtr->i_no);

  req->setReqBgGC(false);
  mng->fsWorker_->releaseFsReq(req);

  if (owner_wid == mng->fsWorker_->getWid()) {
    OwnerUnlinkInode(mng, ino, inodePtr);
    return;
  }

  cfs_ino_t *ptr = new cfs_ino_t;
  *ptr = ino;

  FsProcMessage msg;
  msg.type = FsProcMessageType::kOwnerUnlinkInode;
  msg.ctx = static_cast<void *>(ptr);
  mng->fsWorker_->messenger->send_message(owner_wid, msg);
}

void FileMng::UnlinkOp::OwnerUnlinkInode(FileMng *mng, cfs_ino_t ino,
                                         InMemInode *inode) {
  // NOTE: it is assumed that directory entry has been removed before this
  // function is called.
  if (inode == nullptr) [[unlikely]] { 
      inode = mng->GetInMemInode(ino); 
  }

  assert(inode != nullptr);

  inode->adjustNlink(-1);
  auto nlink = inode->getNlink();
  SPDLOG_DEBUG("[wid={}] reduce link on inode {}, now nlink = {}",
               mng->fsWorker_->getWid(), ino, nlink);
  if (nlink < 0) {
    SPDLOG_ERROR("[wid={}] inode {} has nlink {} < 0", mng->fsWorker_->getWid(),
                 ino, nlink);
    throw std::runtime_error("negative nlink");
  }

  if (nlink == 0) OwnerDeallocResources(mng, ino, inode);
}

#if CFS_JOURNAL(ON)
bool FileMng::UnlinkOp::OwnerEnsureLocalBitmapsInMem(FileMng *mng,
                                                     cfs_ino_t ino,
                                                     InMemInode *inode) {
  if (inode == nullptr) [[unlikely]] { 
    inode = mng->GetInMemInode(ino); 
  } 
  
  auto this_wid = mng->fsWorker_->getWid();
  uint64_t bmap_start = get_bmap_start_block_for_worker(this_wid);
  uint64_t bmap_stop = get_bmap_start_block_for_worker(this_wid + 1);
  int i = 0;
  ssize_t size = inode->inodeData->size;
  std::unordered_set<cfs_bno_t> local_bmaps;
  while (size > 0) {
    auto extentPtr = &(inode->inodeData->ext_array[i++]);
    size -= (extentPtr->num_blocks * BSIZE);

    auto bmap = get_bmap_block_for_lba(extentPtr->block_no);
    if ((bmap >= bmap_start) && (bmap < bmap_stop)) {
      local_bmaps.insert(bmap);
    }
  }

  std::unordered_set<cfs_bno_t> local_unloaded_bmaps;
  // TODO consider an ordered map so that set differences can be used
  for (cfs_bno_t bmap : local_bmaps) {
    if (mng->fsWorker_->jmgr->GetStableDataBitmap(bmap) == nullptr)
      local_unloaded_bmaps.insert(bmap);
  }

  if (local_unloaded_bmaps.empty()) return true;

  // NOTE: we need an FsReq to be able to use fsImpl_->getBlock. The following
  // code uses a generic request with a callback.
  auto cb = [](FileMng *mgr, FsReq *req) {
    // TODO check that all io completed successfully
    InMemInode *minode = static_cast<InMemInode *>(req->generic_callback_ctx);
    cfs_ino_t ino = minode->i_no;
    mgr->fsWorker_->releaseFsReq(req);

    // resume function again
    FileMng::UnlinkOp::OwnerDeallocResources(mgr, ino, minode);
  };

  void *ctx = static_cast<void *>(inode);
  auto req = mng->fsWorker_->genGenericRequest(cb, ctx);

  for (cfs_bno_t bmap : local_unloaded_bmaps) {
    auto curBmapItemPtr =
        mng->fsImpl_->getBlock(mng->fsImpl_->bmapBlockBuf_, bmap, req);
    if (curBmapItemPtr->isInMem())
      throw std::runtime_error("Bitmap block not in stable map");
  }

  assert(req->numTotalPendingIoReq() > 0);
  mng->submitFsGeneratedRequests(req);
  return false;
}
#endif

#if CFS_JOURNAL(ON)
static int CalculateWorkerWithMaximumDeallocs(ssize_t file_size,
                                              const struct cfs_extent *arr) {
  int i = 0;
  int max_idx = 0;
  size_t bmap_counts[NMAX_FSP_WORKER];
  memset(bmap_counts, 0, sizeof(size_t) * NMAX_FSP_WORKER);

  while (file_size > 0) {
    auto extentPtr = &(arr[i++]);
    file_size -= (extentPtr->num_blocks * BSIZE);

    auto bmap = get_bmap_block_for_lba(extentPtr->block_no);
    auto wid = getWidForBitmapBlock(bmap);
    bmap_counts[wid] += extentPtr->num_blocks;

    if (bmap_counts[max_idx] < bmap_counts[wid]) max_idx = wid;
  }

  if (bmap_counts[max_idx] == 0) return -1;
  return max_idx;
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::UnlinkOp::OwnerDeallocResources(FileMng *mng, cfs_ino_t ino,
                                              InMemInode *inode) {
  if (inode == nullptr) [[unlikely]] { 
    inode = mng->GetInMemInode(ino); 
  } 
  
  assert(inode != nullptr);
  if (!(inode->noAppReferring())) {
    // NOTE: we cannot proceed further as some apps are still referring to this
    // inode. Instead, we mark this inode for deallocation later and come back
    // to it whenever a file descriptor is closed.
    inode->unlinkDeallocResourcesOnClose = true;
    // TODO: ensure that open() never opens a path to an inode that has
    // unlinkDeallocResourcesOnClose set to true. It will most likely never
    // happen because the entry is removed from the directory cache. But it
    // doesn't hurt to make this assertion atleast in debug mode.
    return;
  }

  auto best_wid = CalculateWorkerWithMaximumDeallocs(
      inode->inodeData->size, inode->inodeData->ext_array);
  SPDLOG_DEBUG("Unlink deallocating inode {}, calculated best_wid as {}", ino,
               best_wid);
  if (best_wid != -1 && best_wid != mng->fsWorker_->getWid()) {
    auto newOwnerCallback = [](FileMng *mgr,
                               const FileMng::ReassignmentOp::Ctx *ctx,
                               void *) {
      auto &exp = ctx->exported_inode;
      SPDLOG_DEBUG(
          "Inode reassigned from wid {} -> wid {} for better deallocation",
          exp.exporter_wid, exp.importer_wid);
      FileMng::UnlinkOp::OwnerDeallocResources(mgr, exp.inode->i_no, exp.inode);
    };

    FileMng::ReassignmentOp::OwnerExportThroughPrimary(
        mng, ino, best_wid,
        /* oldOwnerCallback, oldOwnerCallbackCtx */
        nullptr, nullptr,
        /* newOwnerCallback, newOwnerCallbackCtx */
        newOwnerCallback, nullptr);

    return;
  }

  // If the bitmaps aren't loaded, the function will first load it and then call
  // back into OwnerDeallocResources
  if (!OwnerEnsureLocalBitmapsInMem(mng, ino, inode)) return;

  // safe to deallocate. Marking items to be deallocated during fsync.
  // TODO: handle non journal case.
  inode->logEntry->set_bitmap_op(0);
  // TODO: calculate based on file size instead of going over all extents
  for (int i = 0; i < NEXTENT_ARR; i++) {
    auto extentPtr = &(inode->inodeData->ext_array[i]);
    if ((extentPtr->block_no > 0) && (extentPtr->num_blocks > 0)) {
      // this deallocation must lead to clearing the bit in bmap for extent
      inode->logEntry->update_extent(extentPtr, false, /*bmap_modified*/ true);
    }
  }

  // mark this inode dirty so that it is eventually synced to disk
  inode->setDirty(true);
  mng->fsImpl_->releaseInodeDataBuffers(inode);
  mng->fsImpl_->unlinkedInodeSet_.insert(inode->i_no);
  mng->fsImpl_->dirtyInodeSet_.insert(inode->i_no);

  // all done, now we wait for a periodic fsync which will trigger
  // FileMng::UnlinkOp::OwnerOnFsyncComplete
}
#else
void FileMng::UnlinkOp::OwnerDeallocResources(FileMng *mng, cfs_ino_t ino,
                                              InMemInode *inode) {
  if (inode == nullptr) [[unlikely]] { 
    inode = mng->GetInMemInode(ino); 
  } 
  
  assert(inode != nullptr);
  if (!(inode->noAppReferring())) {
    inode->unlinkDeallocResourcesOnClose = true;
    return;
  }

  std::unordered_map<int, BitmapChangeOps *> wid_changes_map;
  {
    // minimally, the inode bitmap must be cleared
    auto changes = new BitmapChangeOps();
    changes->inodesToClear.push_back(ino);
    wid_changes_map[FsProcWorker::kMasterWidConst] = changes;
  }

  // segregate changes based on which blocks belong to which worker
  for (int i = 0; i < NEXTENT_ARR; i++) {
    auto extentPtr = &(inode->inodeData->ext_array[i]);
    if (extentPtr->block_no == 0) continue;
    // NOTE: in current implementation clearing the first block in extent is
    // enough to mark the entire extent "free"
    cfs_bno_t block_pba = conv_lba_to_pba(extentPtr->block_no);
    cfs_bno_t bmap_disk_bno = get_bmap_block_for_pba(block_pba);
    int bmap_wid = getWidForBitmapBlock(bmap_disk_bno);
    auto [it, inserted] = wid_changes_map.try_emplace(bmap_wid, nullptr);
    if (inserted) {
      assert(it->second == nullptr);
      it->second = new BitmapChangeOps();
    }
    assert(it->second != nullptr);
    it->second->blocksToClear.push_back(block_pba);
  }

  mng->fsImpl_->releaseInodeDataBuffers(inode);
  // set it to clean, remove from dirty set
  mng->fsImpl_->resetFileIndoeDirty(ino);
  inode->setDeleted();
  inode->unlinkDeallocResourcesOnClose = false;

  // send messages
  // NOTE: easier to just send messages to all workers including this one. There
  // shouldn't be any problem of race conditions etc. as it is just a
  // deallocation of blocks / bitmaps which is independent of migrating an inode
  // back to primary.
  FsProcMessage msg;
  msg.type = FsProcMessageType::BITMAP_CHANGES;
  for (auto &[wid, changes] : wid_changes_map) {
    msg.ctx = static_cast<void *>(changes);
    mng->fsWorker_->messenger->send_message(wid, msg);
  }

  // migrate to master if master is not owner
  if (!(mng->fsWorker_->isMasterWorker())) {
    FileMng::ReassignmentOp::OwnerExportThroughPrimary(
        mng, ino, FsProcWorker::kMasterWidConst,
        /* oldOwnerCallback, oldOwnerCallbackCtx */
        nullptr, nullptr,
        /* newOwnerCallback, newOwnerCallbackCtx */
        nullptr, nullptr);
  }
}
#endif

#if CFS_JOURNAL(ON)
void FileMng::UnlinkOp::OwnerOnFsyncComplete(FileMng *mng, cfs_ino_t ino,
                                             InMemInode *minode) {
  if (minode == nullptr) [[unlikely]] {
    minode = mng->GetInMemInode(ino);
  } 
  
  assert(minode != nullptr);

  // TODO: (ask jing) ensure fields reset when inode allocated on create
  minode->setDeleted();
  minode->unlinkDeallocResourcesOnClose = false;
  // The journal has been synced and all bitmaps have been modified. However,
  // the journal has not been checkpointed yet. So we cannot just forget about
  // this inode as it contains a "stable" jinode that is used during
  // checkpointing. Instead, we migrate the inode back to master. So during
  // checkpointing, this inode is also checkpointed. Also, if create() + fsync()
  // uses this inode, that information will also be stored in the jinode and
  // used during checkpointing, taking care of any realloc issues.
  if (!(mng->fsWorker_->isMasterWorker())) {
    FileMng::ReassignmentOp::OwnerExportThroughPrimary(
        mng, ino, FsProcWorker::kMasterWidConst,
        /* oldOwnerCallback, oldOwnerCallbackCtx */
        nullptr, nullptr,
        /* newOwnerCallback, newOwnerCallbackCtx */
        nullptr, nullptr);
  }
}
#endif

void FileMng::UnlinkOp::ProcessReq(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());

  bool state_changed = false;
  do {
    auto curState = req->getState();
    switch (curState) {
      case FsReqState::UNLINK_PRIMARY_LOAD_PRT_INODE:
        UnlinkOp::PrimaryLoadParentInode(mng, req);
        break;
      case FsReqState::UNLINK_PRIMARY_GET_FILE_INUM:
        UnlinkOp::PrimaryGetFileInum(mng, req);
        break;
      case FsReqState::UNLINK_PRIMARY_LOAD_INODE:
        UnlinkOp::PrimaryLoadInode(mng, req);
        break;
      case FsReqState::UNLINK_PRIMARY_REMOVE_DENTRY:
        UnlinkOp::PrimaryRemoveDentry(mng, req);
        break;
      case FsReqState::UNLINK_PRIMARY_NOTIFY_USER:
        UnlinkOp::PrimaryNotifyUser(mng, req);
        break;
      case FsReqState::UNLINK_PRIMARY_HANDOFF_TO_OWNER:
        UnlinkOp::PrimaryHandoffToOwner(mng, req);
        break;
      case FsReqState::UNLINK_ERR:
        UnlinkOp::PrimaryHandleError(mng, req);
        return;  // <-- early return
      default:
        SPDLOG_WARN("Received unknown state: {}", static_cast<int>(curState));
        throw std::runtime_error("Unknown state");

        /* Other phases that unlink goes through are (in order) -
         * * OwnerUnlinkInode
         * * OwnerDeallocResources
         * * OwnerOnFsyncComplete
         */
    }
    state_changed = (curState != req->getState());
  } while (state_changed);
}

inline static bool isSrcPrefixOfDst(const std::vector<std::string> &src,
                                    const std::vector<std::string> &dst) {
  assert(!src.empty());
  assert(!dst.empty());

  size_t src_size = src.size();
  size_t dst_size = dst.size();
  if (src_size > dst_size) return false;

  ssize_t i = src_size - 1;
  while (i >= 0) {
    if (src[i] != dst[i]) return false;
    i--;
  }
  return true;
}

// RenameOp helper function to check if paths are valid and sets appropriate
// error in req if not.
inline static bool hasValidPaths(FsReq *req) {
  assert(req->getType() == FsReqType::RENAME);
  auto &srcTokens = req->getPathTokens();
  auto &dstTokens = req->getDstPathTokens();
  if (srcTokens.empty() || dstTokens.empty()) {
    // ENOENT: either old or new points to an empty string.
    req->setError(FS_REQ_ERROR_POSIX_ENOENT);
    req->setState(FsReqState::RENAME_ERR);
    return false;
  }

  // FIXME rename "/" "/" ends up with srcTokens = dstTokens = [""]  which has
  // size of 1. Instead of returning ENOENT, it ends up returning EINVAL.

  if (isSrcPrefixOfDst(srcTokens, dstTokens)) {
    // EINVAL: The new directory pathname contains a path prefix that names the
    // old directory. For example, `mv /foo/bar` /foo/bar/blah which should
    // return EINVAL
    req->setError(FS_REQ_ERROR_POSIX_EINVAL);
    req->setState(FsReqState::RENAME_ERR);
    return false;
  }

  return true;
}

void FileMng::RenameOp::LookupSrcDir(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_LOOKUP_SRC_DIR);
  // NOTE: since this is also the first function called in the rename state, we
  // validate the two paths here.
  if (!hasValidPaths(req)) return;

  InMemInode *src_dir_inode = req->getDirInode();
  if (src_dir_inode != nullptr) {
    req->setState(FsReqState::RENAME_GET_SRC_INUM);
    return;
  }

  bool is_err = false;
  src_dir_inode = mng->fsImpl_->getParDirInode(req, is_err);
  if (req->numTotalPendingIoReq() != 0) {
    mng->submitFsGeneratedRequests(req);
    return;
  }

  if (is_err) {
    req->setState(FsReqState::RENAME_ERR);
    // the only reason we fail is if we cannot find the directory
    assert(req->getErrorNo() == FS_REQ_ERROR_FILE_NOT_FOUND);
    // TODO if write permissions are denied, then EACCES must be set.
    req->setError(FS_REQ_ERROR_POSIX_ENOTDIR);
    req->setState(FsReqState::RENAME_ERR);
    return;
  }

  assert(src_dir_inode != nullptr);
  req->setDirInode(src_dir_inode);
  req->setState(FsReqState::RENAME_GET_SRC_INUM);
}

void FileMng::RenameOp::GetSrcInum(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_GET_SRC_INUM);
  auto src_inode = req->getTargetInode();
  if (src_inode != nullptr) {
    req->setFileIno(src_inode->i_no);
    req->setState(FsReqState::RENAME_LOOKUP_DST_DIR);
    return;
  }
  InMemInode *src_dir_inode = req->getDirInode();
  assert(src_dir_inode != nullptr);

  bool is_err = false;
  uint32_t src_ino =
      mng->fsImpl_->lookupDir(req, src_dir_inode, req->getLeafName(), is_err);
  if (req->numTotalPendingIoReq() != 0) {
    mng->submitFsGeneratedRequests(req);
    return;
  }

  if (is_err) {
    assert(req->getErrorNo() == FS_REQ_ERROR_FILE_NOT_FOUND);
    // The link named by old does not name an existing file
    req->setError(FS_REQ_ERROR_POSIX_ENOENT);
    req->setState(FsReqState::RENAME_ERR);
    return;
  }

  assert(src_ino > 0);
  req->setFileIno(src_ino);
  req->setState(FsReqState::RENAME_LOAD_SRC_INODE);
}

void FileMng::RenameOp::LoadSrcInode(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_LOAD_SRC_INODE);
  assert(req->getFileInum() > 0);

  InMemInode *src_inode = req->getTargetInode();
  if (src_inode == nullptr) {
    src_inode = mng->fsImpl_->getFileInode(req, req->getFileInum());
    if (req->numTotalPendingIoReq() != 0) {
      mng->submitFsGeneratedRequests(req);
      return;
    }
  }

  assert(req->numTotalPendingIoReq() == 0);
  assert(src_inode != nullptr);
  FsImpl::fillInodeDentryPositionAfterLookup(req, src_inode);
  mng->fsWorker_->onTargetInodeFiguredOut(req, src_inode);
  req->setState(FsReqState::RENAME_LOOKUP_DST_DIR);
}

void FileMng::RenameOp::LookupDstDir(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_LOOKUP_DST_DIR);
  InMemInode *dst_dir_inode = req->getDstDirInode();
  if (dst_dir_inode != nullptr) {
    req->setState(FsReqState::RENAME_GET_DST_INUM);
    return;
  }

  bool is_err = false;
  dst_dir_inode =
      mng->fsImpl_->getParDirInode(req, req->getDstPathTokens(), is_err);
  if (req->numTotalPendingIoReq() != 0) {
    mng->submitFsGeneratedRequests(req);
    return;
  }

  if (is_err) {
    assert(req->getErrorNo() == FS_REQ_ERROR_FILE_NOT_FOUND);
    // TODO if write permissions are denied, then EACCES must be set.
    req->setError(FS_REQ_ERROR_POSIX_ENOTDIR);
    req->setState(FsReqState::RENAME_ERR);
    return;
  }

  assert(dst_dir_inode != nullptr);
  req->setDstDirInode(dst_dir_inode);
  req->setState(FsReqState::RENAME_GET_DST_INUM);
}

void FileMng::RenameOp::GetDstInum(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_GET_DST_INUM);
  InMemInode *dst_dir_inode = req->getDstDirInode();
  assert(dst_dir_inode != nullptr);

  bool is_err = false;
  auto curDirMap = req->getDstDirMap();
  bool curDstPathNotExist =
      (req->getDstTargetInode() == nullptr &&
       (curDirMap->map.size() + 2) ==
           req->getDstDirInode()->inodeData->i_dentry_count);
  if (curDstPathNotExist) {
    req->setDstExist(false);
    req->setState(FsReqState::RENAME_MODIFY_DIRENTRIES);
    return;
  }
  uint32_t dst_ino = mng->fsImpl_->lookupDir(req, dst_dir_inode,
                                             req->getNewLeafName(), is_err);
  if (req->numTotalPendingIoReq() != 0) {
    mng->submitFsGeneratedRequests(req);
    return;
  }

  if (is_err) {
    if (req->getErrorNo() != FS_REQ_ERROR_FILE_NOT_FOUND) {
      req->setState(FsReqState::RENAME_ERR);
      return;
    }

    // it is okay if dst does not exist, skip directly to rename transaction
    req->resetErr();
    req->setDstExist(false);
    req->setState(FsReqState::RENAME_MODIFY_DIRENTRIES);
    return;
  }

  assert(dst_ino > 0);
  req->setDstFileIno(dst_ino);
  req->setDstExist(true);
  req->setState(FsReqState::RENAME_LOAD_DST_INODE);
}

void FileMng::RenameOp::LoadDstInode(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_LOAD_DST_INODE);
  assert(req->isDstExist());
  assert(req->getDstFileInum() > 0);

  InMemInode *dst_inode = req->getDstTargetInode();
  if (dst_inode == nullptr) {
    dst_inode = mng->fsImpl_->getFileInode(req, req->getDstFileInum());
    if (req->numTotalPendingIoReq() != 0) {
      mng->submitFsGeneratedRequests(req);
      return;
    }
  }

  assert(req->numTotalPendingIoReq() == 0);
  assert(dst_inode != nullptr);
  FsImpl::fillInodeDentryPositionAfterLookup(req, dst_inode, /*dentryNo*/ 1);
  mng->fsWorker_->onDstTargetInodeFiguredOut(req, dst_inode);

  // NOTE: at this point, we have src inode and dst inode, so we have to
  // validate the types.
  bool src_is_dir = req->getTargetInode()->inodeData->type == T_DIR;
  bool dst_is_dir = dst_inode->inodeData->type == T_DIR;

  if (src_is_dir && !dst_is_dir) {
    // If the old argument points to the pathname of a directory, the new
    // argument shall not point to the pathname of a file that is not a
    // directory.
    // ENOTDIR:  the old argument names a directory and new argument
    // names a non-directory file
    req->setState(FsReqState::RENAME_ERR);
    req->setError(FS_REQ_ERROR_POSIX_ENOTDIR);
    return;
  }

  if (!src_is_dir && dst_is_dir) {
    // If the old argument points to the pathname of a file that is not a
    // directory, the new argument shall not point to the pathname of a
    // directory.
    // EISDIR: The new argument points to a directory and the old
    // argument points to a file that is not a directory.
    req->setState(FsReqState::RENAME_ERR);
    req->setError(FS_REQ_ERROR_POSIX_EISDIR);
    return;
  }

  if (dst_is_dir && dst_inode->inodeData->i_dentry_count != 2) {
    // If new names an existing directory, it shall be required to be an empty
    // directory.
    // EEXIST or ENOTEMPTY: The link named by new is a directory that is not an
    // empty directory.
    req->setState(FsReqState::RENAME_ERR);
    req->setError(FS_REQ_ERROR_POSIX_ENOTEMPTY);
    return;
  }

  req->setState(FsReqState::RENAME_MODIFY_DIRENTRIES);
}

// NOTE: Helper function only called by ModifyDirEntries.
void FileMng::RenameOp::ModifyDstDirHelper(FileMng *mng, FsReq *req,
                                           InMemInode *src_dir_inode,
                                           InMemInode *src_inode,
                                           InMemInode *dst_dir_inode,
                                           InMemInode *dst_inode) {
  if (!req->isDstExist()) {
    mng->fsImpl_->appendToDir(req, dst_dir_inode, src_inode,
                              req->getNewLeafName());
    if (req->numTotalPendingIoReq() != 0) {
      return;
    }

    dst_dir_inode->adjustDentryCount(1);
    mng->fsImpl_->addSecondPathInodeCacheItem(req, src_inode);
    return;
  }

  // dst exists
  int rt = mng->fsImpl_->dentryPointToNewInode(
      req, dst_inode, src_inode, dst_dir_inode, req->getNewLeafName());
  assert(rt == 0);

  // The existing inode (dst_inode) would have the new name position cached. We
  // need to copy that into the inode that is replacing it.
  auto pos_pair = dst_inode->getDentryDataBlockPosition(dst_dir_inode,
                                                        req->getNewLeafName());
  assert(pos_pair != nullptr);
  auto [dentry_block, dentry_idx] = *pos_pair;
  src_inode->addDentryDataBlockPosition(dst_dir_inode, req->getNewLeafName(),
                                        dentry_block, dentry_idx);
  dst_inode->delDentryDataBlockPosition(dst_dir_inode, req->getNewLeafName());

  FsPermission::MapEntry dum;
  mng->fsImpl_->removeSecondPathInodeCacheItem(req, &dum);
  mng->fsImpl_->permission->RegisterGC(dum.first);

  mng->fsImpl_->addSecondPathInodeCacheItem(req, src_inode);
}

// NOTE: Helper function only called by ModifyDirEntries.
void FileMng::RenameOp::ModifySrcDirHelper(FileMng *mng, FsReq *req,
                                           InMemInode *src_dir_inode,
                                           InMemInode *src_inode) {
  auto pos_pair =
      src_inode->getDentryDataBlockPosition(src_dir_inode, req->getLeafName());
  assert(pos_pair != nullptr);
  auto [dentry_block, dentry_idx] = *pos_pair;
  mng->fsImpl_->removeFromDir(req, src_dir_inode, dentry_idx, dentry_block,
                              req->getLeafName());
  src_inode->delDentryDataBlockPosition(src_dir_inode, req->getLeafName());
  assert(req->numTotalPendingIoReq() == 0);
  src_dir_inode->adjustDentryCount(-1);

  FsPermission::MapEntry dum;
  mng->fsImpl_->removePathInodeCacheItem(req, &dum);
  mng->fsImpl_->permission->RegisterGC(dum.first);
}

void FileMng::RenameOp::ModifyDirEntries(FileMng *mng, FsReq *req) {
  assert(req->getState() == FsReqState::RENAME_MODIFY_DIRENTRIES);
  // TODO: (ask jing) : why was checkInodeDentryInMem necessary?

  InMemInode *src_dir_inode = req->getDirInode();
  InMemInode *src_inode = req->getTargetInode();
  InMemInode *dst_dir_inode = req->getDstDirInode();
  InMemInode *dst_inode = nullptr;

  assert(src_dir_inode != nullptr);
  assert(src_inode != nullptr);
  assert(dst_dir_inode != nullptr);
  if (req->isDstExist()) {
    dst_inode = req->getDstTargetInode();
    assert(dst_inode != nullptr);
  }

  /* Steps for rename
   * 1. Add the new entry
   *      a. If dst does not exist, add a new entry
   *      b. If dst exists, change the inode number it points to
   * 2. Remove the old entry - common regardless of whether dst exists
   * 3. Unlink dst if it existed
   *      We deallocate resources after responding to user.
   */
  RenameOp::ModifyDstDirHelper(mng, req, src_dir_inode, src_inode,
                               dst_dir_inode, dst_inode);
  if (req->numTotalPendingIoReq() != 0) {
    mng->submitFsGeneratedRequests(req);
    return;
  }

  RenameOp::ModifySrcDirHelper(mng, req, src_dir_inode, src_inode);
  assert(req->numTotalPendingIoReq() == 0);

  // NOTE: if dst_inode exists, we deallocate after notifying user.
  req->setState(FsReqState::RENAME_NOTIFY_USER);
}

void FileMng::RenameOp::PrimaryNotifyUser(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(req->getState() == FsReqState::RENAME_NOTIFY_USER);

  InMemInode *dst_inode = nullptr;
  if (req->isDstExist()) {
    dst_inode = req->getDstTargetInode();
    assert(dst_inode != nullptr);
  }

  req->getClientOp()->op.rename.ret = 0;
  mng->fsWorker_->submitFsReqCompletion(req);

  if (dst_inode == nullptr) return;

  // NOTE: right now there is no difference between unlinking an empty directory
  // and a regular file. If there is a difference later, this unlinking logic
  // should change as well. Since all directory related operations have been
  // performed, we only need to call UnlinkOp::OwnerUnlinkInode on the worker
  // that owns the inode.
  auto master = static_cast<FsProcWorkerMaster *>(mng->fsWorker_);
  auto ino = dst_inode->i_no;
  auto owner_wid = master->getInodeOwner(ino);
  if (owner_wid == mng->fsWorker_->getWid()) {
    FileMng::UnlinkOp::OwnerUnlinkInode(mng, ino, dst_inode);
    return;
  }

  cfs_ino_t *ptr = new cfs_ino_t;
  *ptr = ino;
  FsProcMessage msg;
  msg.type = FsProcMessageType::kOwnerUnlinkInode;
  msg.ctx = static_cast<void *>(ptr);
  mng->fsWorker_->messenger->send_message(owner_wid, msg);
}

void FileMng::RenameOp::PrimaryHandleError(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());
  assert(req->getState() == FsReqState::RENAME_ERR);
  req->getClientOp()->op.rename.ret = -1;
  mng->fsWorker_->submitFsReqCompletion(req);
}

void FileMng::RenameOp::ProcessReq(FileMng *mng, FsReq *req) {
  assert(mng->fsWorker_->isMasterWorker());

  // Intended to work as per
  // https://pubs.opengroup.org/onlinepubs/009604599/functions/rename.html
  // T0D0's in place for incomplete parts such as symlinks and write permissions
  bool state_changed = false;
  do {
    auto curState = req->getState();
    switch (curState) {
      // TODO: when we support symbolic links, make sure not to resolve the
      // links and to operate on the link paths themselves.
      // TODO: check for write access permissions for both src and dst
      // directories
      // TODO: on success, update the st_ctime and st_mtime fields of the parent
      // directory of each file
      // TODO: rmdir
      case FsReqState::RENAME_LOOKUP_SRC_DIR:
        RenameOp::LookupSrcDir(mng, req);
        break;
      case FsReqState::RENAME_GET_SRC_INUM:
        RenameOp::GetSrcInum(mng, req);
        break;
      case FsReqState::RENAME_LOAD_SRC_INODE:
        RenameOp::LoadSrcInode(mng, req);
        break;
      case FsReqState::RENAME_LOOKUP_DST_DIR:
        RenameOp::LookupDstDir(mng, req);
        break;
      case FsReqState::RENAME_GET_DST_INUM:
        RenameOp::GetDstInum(mng, req);
        break;
      case FsReqState::RENAME_LOAD_DST_INODE:
        RenameOp::LoadDstInode(mng, req);
        break;
      case FsReqState::RENAME_MODIFY_DIRENTRIES:
        RenameOp::ModifyDirEntries(mng, req);
        break;
      case FsReqState::RENAME_NOTIFY_USER:
        RenameOp::PrimaryNotifyUser(mng, req);
        break;
      case FsReqState::RENAME_ERR:
        RenameOp::PrimaryHandleError(mng, req);
        return;  // <-- early return
      default:
        SPDLOG_WARN("Received unknown state: {}", static_cast<int>(curState));
        throw std::runtime_error("Unknown state");
    }
    state_changed = (curState != req->getState());
  } while (state_changed);
}
