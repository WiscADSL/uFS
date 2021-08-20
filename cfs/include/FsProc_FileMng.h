#ifndef CFS_INCLUDE_FSPROC_FILEMNG_H_
#define CFS_INCLUDE_FSPROC_FILEMNG_H_

#include <cstdint>
#include <unordered_map>
#include <unordered_set>

#include "FsProc_FsImpl.h"
#include "FsProc_FsInternal.h"
#include "FsProc_FsReq.h"
#include "FsProc_Lease.h"

//
class FsProcWorker;
class FsProcWorkerMaster;
class JournalEntry;
class JournalManager;
struct BitmapChangeOps;
//

//
// Like vfs, manage all open file, global metadata
// It contains the logic of state machine
class FileMng {
 public:
  FileMng(FsProcWorker *worker, const char *memPtr, float dataDirtyFlushRatio,
          int numPartitions);
  FileMng(FsProcWorker *worker, const char *bmapMemPtr, char *dataBlockMemPtr,
          uint32_t numBmapBlocksTotal, float dataDirtyFlushRatio,
          int numPartitions);
  ~FileMng();

  // Flush metadata buffers when FS exits
  void flushMetadataOnExit();

  // At runtime, check and flush the buffer
  // return the number of items being flushed.
  int64_t checkAndFlushBufferDirtyItems();

  // main (single) entry of FS request processing, will init the FSM
  int processReq(FsReq *req);

  // main entry for IO request submission
  void submitFsGeneratedRequests(FsReq *req);

  void processRemoteBitmapChanges(FsReq *req);
#if CFS_JOURNAL(ON)
  // callback after a journal entry has been processed
  static void onJournalWriteComplete(void *arg, bool success);
  void updateBitmapsOnJournalWriteComplete(JournalEntry *jentry);
  bool allBitmapsInMemory(BitmapChangeOps *changes);
  void processBitmapChanges(BitmapChangeOps *changes, bool journalled_locally);
  void updateBlockBitmaps(std::vector<uint64_t> &blocks_to_set,
                          std::vector<uint64_t> &blocks_to_clear,
                          std::unordered_set<cfs_bno_t> &bmaps_cleared);
  void processInodeBitmapChanges(
      std::unordered_map<cfs_ino_t, bool> &inodeBitmapChanges);

  // batching journal operations
  static void onBatchedJournalWriteComplete(void *arg, bool success);
  void submitSyncBatches(FsReq *req, std::unordered_set<cfs_ino_t> &inodes);
#endif

  // Submit FsReq's write request to device
  // return submitted device request number, if error happened (like some
  // requests are not submitted, return -1). This could be further used by fsync
  // DEPRECATED
  ////int submitFsWriteRequests(FsReq *req);

  // REQUIRED: only master thread can invoke this
  // it will generate new FileMng object from according to the memory buffers
  //  of this current FileMng object
  FileMng *generateSubFileMng(FsProcWorker *curWorker);

  void installInode(InMemInode *inode);
  size_t getNumInodes() { return fsImpl_->getNumInodes(); }

  void splitInodeDataBlockBufferSlot(
      InMemInode *inode, std::unordered_set<BlockBufferItem *> &items);
  void installDataBlockBufferSlot(
      InMemInode *inode, const std::unordered_set<BlockBufferItem *> &items);

  InMemInode *GetInMemInode(cfs_ino_t ino);
  InMemInode *PopInMemInode(cfs_ino_t ino);

  std::set<FsReq *> inflight_wsync_req;
  constexpr static int kNumMaxInflightWsync = 5;

  uint64_t dbgWriteCounter = 0;

 private:
  using iou_map_t = FsReqIoUnitHelper::pendingIoUnit_map_t;
  // data plane
  //// deprecated: old implementation using server initialized data buffer
  // But if this type is specified by AppProc, it can still work
  void processWrite(FsReq *req);
  void processPwrite(FsReq *req);
  void processRead(FsReq *req);
  void processPread(FsReq *req);
  void processPreadUC(FsReq *req);
  // Assume all the request seen by FSP is of Alloc** type (03/15/2020)
  // Alloc** --> use user-supplied buffer
  void processAllocWrite(FsReq *req);
  void processAllocPwrite(FsReq *req);
  void processAllocRead(FsReq *req);
  void processAllocPread(FsReq *req);
  void processCacheLeaseRenew(FsReq *req);
  void processLseek(FsReq *req);
  // control plane
  void processCreate(FsReq *req);
  void processOpen(FsReq *req);
  void processClose(FsReq *req);
  void processStat(FsReq *req);
  void processFstat(FsReq *req);
  void processUnlink(FsReq *req);
  void processRename(FsReq *req);
  void processFdataSync(FsReq *req);
  void processLdbWsync(FsReq *req);
  void processSyncall(FsReq *req);
  void processSyncunlinked(FsReq *req);
  void processMkdir(FsReq *req);
  void processOpendir(FsReq *req);
  void processNewShmAllocated(FsReq *req);
  void processPing(FsReq *req);
  void processDumpInodes(FsReq *req);

  // helper function when fsync completes
  void onFdataSyncComplete(FsReq *req, bool success);
  // helper function for unlinking an inode after every reference closed
  void processUnlinkDeleteInode(InMemInode *inode);
  // helper function for processSyncall to first call syncunlinked
  void ProcessSyncUnlinkedBeforeSyncall(FsReq *req);

  int _renewLeaseForRead(FsReq *req);

  void submitFsGeneratedRequestsCheckSinglePendingMap(
      FsReq *req, iou_map_t::const_iterator beginIt,
      iou_map_t::const_iterator endIt, bool isSector = false);

  // Fill the stat struct from in memory inode
  void fromInMemInode2Statbuf(InMemInode *inodePtr, struct stat *cur_stat_ptr);

  // check permission and get inode for target and parent directory
  FsPermission::PCR checkPermission(FsReq *req);
  FsPermission::PCR checkDstPermission(FsReq *req);

  // helper function to set up InMemInode into FsReq and record it into fsWorker
  // NOTE: this is supposed to be called only for those do lookup, that is. for
  // all the operation that targets some fd that is already lookuped (opened),
  // the fileObj is supposed to be set by FsProcWorker once the request is
  // received
  // void inline setupFileInode4ReqAndWorker(FsReq *req, InMemInode
  // *targetInode);

  FsProcWorker *fsWorker_;
  // Memory region of the whole buffer cache
  const char *memPtr_;
  FsProcWorkerLeaseMng leaseMng_;
  // File System implementation
  FsImpl *fsImpl_;
  // Maps app pid's to file descriptors
  std::unordered_map<pid_t, std::unordered_map<int, FileObj *>> ownerAppFdMap_;
  // Do not allow more than one syncall/syncunlinked at a time.
  bool syncall_in_progress_{false};
  std::queue<FsReq *> syncall_requests_;
  bool syncunlinked_in_progress_{false};
  std::queue<FsReq *> syncunlinked_requests_;

  // Import/Export inodes
  bool exportInode(cfs_ino_t inode, ExportedInode &exp);
  void importInode(const ExportedInode &exp);
  // Helper functions for import/export of file descriptor mappings
  void delExportedInodeFdMappings(const InMemInode *inodePtr);
  void addImportedInodeFdMappings(const InMemInode *inodePtr);
  // Helper function to resolve app request with fd to inode
  FileObj *getFileObjForFd(pid_t pid, int fd);
  void addFdMappingOnOpen(pid_t pid, FileObj *fobj);
  void delFdMappingOnClose(pid_t pid, FileObj *fobj);
  // When an app exits, all file descriptors must be closed
  void closeAllFileDescriptors(pid_t pid);

  class ReassignmentOp {
   public:
    enum class State {
      kOwnerExportThroughPrimary,
      kPrimaryRequestNewOwnerImport,
      kNewOwnerImport,
      kPrimaryImportSuccessNotification,
      kOldOwnerExportSuccessNotification,
      // TODO error states
    };

    struct Ctx;  // forward declaration
    using CallbackFn =
        std::add_pointer<void(FileMng *, const Ctx *, void *)>::type;

    struct Ctx {
      mutable State state{State::kOwnerExportThroughPrimary};
      CallbackFn oldOwnerCallback{nullptr};
      void *oldOwnerCallbackCtx{nullptr};
      CallbackFn newOwnerCallback{nullptr};
      void *newOwnerCallbackCtx{nullptr};
      ExportedInode exported_inode;
    };

    struct BatchedCtx;
    using BatchedCallbackFn =
        std::add_pointer<void(FileMng *, const BatchedCtx *, void *)>::type;

    // no-op function for batched callbacks
    static void BatchedCallbackNoOpFn(FileMng *, const BatchedCtx *, void *){};

    struct BatchedCtx {
      int n_total;
      // NOTE: completions on old and new should be exactly the same. While the
      // new callback is always called before the new callback, to avoid any
      // dependency, each one is decremented when the callback is called. and
      // when each is equal to the total, it calls the callers final callback.
      int n_complete_old;
      int n_complete_new;
      // TODO: if in the future, reassignment can have failures, then n_failed
      // will have to be added. Right now, if there are any reassignment
      // failures, they lead to a total fs shutdown.
      BatchedCallbackFn oldOwnerCallback{nullptr};
      void *oldOwnerCallbackCtx{nullptr};
      BatchedCallbackFn newOwnerCallback{nullptr};
      void *newOwnerCallbackCtx{nullptr};
      // TODO: have access to all the exported inodes as a list?
    };

    // Reassignment ops deal with a lot of messages. SendMessageOrExecute tests
    // if the target recepient is same as sender. If true, it executes the
    // function rather than sending a message to itself.
    static void SendMessageOrExecute(FileMng *mng, int wid, Ctx *ctx);
    static void OwnerExportThroughPrimary(FileMng *mng, cfs_ino_t ino,
                                          int new_owner_wid,
                                          CallbackFn oldOwnerCallback,
                                          void *oldOwnerCallbackCtx,
                                          CallbackFn newOwnerCallback,
                                          void *newOwnerCallbackCtx,
                                          bool batched = false);
    static void BatchedOwnerExportThroughPrimary(
        FileMng *mng, const std::unordered_set<cfs_ino_t> &ino_vec,
        int new_owner_wid, BatchedCallbackFn oldOwnerCallback,
        void *oldOwnerCallbackCtx, BatchedCallbackFn newOwnerCallback,
        void *newOwnerCallbackCtx);
    static void PrimaryRequestNewOwnerImport(FileMng *mng, Ctx *ctx);
    static void NewOwnerImport(FileMng *mng, Ctx *ctx);
    static void PrimaryImportSuccessNotification(FileMng *mng, Ctx *ctx);
    static void OldOwnerExportSuccessNotification(FileMng *mng, Ctx *ctx);
    static void ProcessReassignmentCtx(FileMng *mng, Ctx *ctx);
    static void ProcessInodeReassignmentReq(FileMng *mng, FsReq *req);
  };

  class UnlinkOp {
   public:
    // Initial entry point to process unlink requests
    static void ProcessReq(FileMng *mng, FsReq *req);
    // Handles all errors and submit completion
    static void PrimaryHandleError(FileMng *mng, FsReq *req);
    // Loads the parent directory which may not be in memory
    static void PrimaryLoadParentInode(FileMng *mng, FsReq *req);
    // Extracts the inode from the directory
    static void PrimaryGetFileInum(FileMng *mng, FsReq *req);
    // Loads the inode
    static void PrimaryLoadInode(FileMng *mng, FsReq *req);
    // Removes the dentry and decrements nlinks
    static void PrimaryRemoveDentry(FileMng *mng, FsReq *req);
    // Notfies the user (critical path) that unlink is complete
    static void PrimaryNotifyUser(FileMng *mng, FsReq *req);
    // Primary finds the owner and sends a message to complete unlink
    static void PrimaryHandoffToOwner(FileMng *mng, FsReq *req);

    // For the following functions, if inode is provided, no lookup for `ino` is
    // required. If inode is null, we query the inode map to get the inode.
    // TODO: assert that when using the InMemInode, the worker actually owns it.
    // Owner decrements nlink and calls dealloc if nlink is 0
    static void OwnerUnlinkInode(FileMng *mng, cfs_ino_t ino,
                                 InMemInode *inode);
    // Owner deallocates all inode resources once fd's closed
    static void OwnerDeallocResources(FileMng *mng, cfs_ino_t ino,
                                      InMemInode *inode);
#if CFS_JOURNAL(ON)
    // NOTE: we do not require these two functions in the no-journal case as
    // fsync is not required for the unlink and we deallocate bitmaps using the
    // remote bitmap changes logic which internally ensures bitmaps are in mem.

    // Owner migrates inode back to primary after fsync unlinked inode
    static void OwnerOnFsyncComplete(FileMng *mng, cfs_ino_t ino,
                                     InMemInode *inode);
    // OwnerDeallocResources can only start deallocating blocks once the bitmap
    // is loaded into memory. It ensures this condition by calling
    // OwnerEnsureLocalBitmapsInMem.
    static bool OwnerEnsureLocalBitmapsInMem(FileMng *mng, cfs_ino_t ino,
                                             InMemInode *inode);
#endif
  };

  class RenameOp {
   public:
    static void ProcessReq(FileMng *mng, FsReq *req);
    static void LookupSrcDir(FileMng *mng, FsReq *req);
    static void GetSrcInum(FileMng *mng, FsReq *req);
    static void LoadSrcInode(FileMng *mng, FsReq *req);
    static void LookupDstDir(FileMng *mng, FsReq *req);
    static void GetDstInum(FileMng *mng, FsReq *req);
    static void LoadDstInode(FileMng *mng, FsReq *req);
    static void ModifyDirEntries(FileMng *mng, FsReq *req);
    static void PrimaryNotifyUser(FileMng *mng, FsReq *req);
    static void PrimaryHandleError(FileMng *mng, FsReq *req);

    // helper functions
   private:
    static void ModifySrcDirHelper(FileMng *mng, FsReq *req,
                                   InMemInode *src_dir_inode,
                                   InMemInode *src_inode);
    static void ModifyDstDirHelper(FileMng *mng, FsReq *req,
                                   InMemInode *src_dir_inode,
                                   InMemInode *src_inode,
                                   InMemInode *dst_dir_inode,
                                   InMemInode *dst_inode);
  };

  friend class FsProcWorker;
  friend class JournalManager;
  friend class FsProcWorkerMaster;
  friend class FsProcWorkerServant;
  friend class SplitPolicyDynamicDstKnown;
};

inline InMemInode *FileMng::GetInMemInode(cfs_ino_t ino) {
  auto search = fsImpl_->inodeMap_.find(ino);
  if (search != fsImpl_->inodeMap_.end()) return search->second;
  return nullptr;
}

inline InMemInode *FileMng::PopInMemInode(cfs_ino_t ino) {
  auto search = fsImpl_->inodeMap_.find(ino);
  if (search == fsImpl_->inodeMap_.end()) return nullptr;

  InMemInode *inode = search->second;
  fsImpl_->inodeMap_.erase(search);
  return inode;
}
#endif  // CFS_INCLUDE_FSPROC_FILEMNG_H_
