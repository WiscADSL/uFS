#ifndef CFS_INCLUDE_FSPROC_FSREQ_H_
#define CFS_INCLUDE_FSPROC_FSREQ_H_

#include <array>
#include <cstdint>

#include "BlockBuffer.h"
#include "FsLibProc.h"
#include "FsPageCache_Shared.h"
#include "FsProc_App.h"
#include "FsProc_LoadMng.h"
#include "FsProc_Permission.h"
#include "perfutil/Cycles.h"
#include "util.h"

// TODO (jingliu) move this into cmake once we decide to have switch-on-off
// usage of absl. This is for simple experiments
// #define USE_ABSL_LIB

#ifdef USE_ABSL_LIB
#include "absl/container/flat_hash_map.h"
#endif
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"

//
class FsProcWorker;
class InMemInode;
class FsImpl;
class FsReqPool;
class FileMng;
//

enum class FsBlockReqType {
  // in BSIZE (one BSIZE <=> one block)
  READ_NOBLOCKING,
  WRITE_NOBLOCKING,
  READ_BLOCKING,
  WRITE_BLOCKING,
  // in sectors (one sector <=> one block)
  READ_NOBLOCKING_SECTOR,
  WRITE_NOBLOCKING_SECTOR,
  READ_BLOCKING_SECTOR,
  WRITE_BLOCKING_SECTOR,
};

enum class FsReqType {
  _LANDMARK_SERVANT_CAN_DO_START_,  // never used for real type, internally
                                    // used to help figure out if one FsReq
                                    // can be handled at one specific thread
  // data plane operations
  READ,
  PREAD,
  PREAD_UC_LEASE_RENEW_ONLY,
  PREAD_UC,
  WRITE,
  PWRITE,
  ALLOC_READ,
  ALLOC_PREAD,
  ALLOC_PREAD_RENEW_ONLY,
  ALLOC_WRITE,
  ALLOC_PWRITE,
  ALLOC_PWRITE_RENEW_ONLY,
  LSEEK,
  // control plane operations
  OPEN,
  CLOSE,
  FSTAT,
  FSYNC,
  FDATA_SYNC,
  WSYNC,
  STAT,
  SYNCALL,
  SYNCUNLINKED,
  APP_MIGRATE,
  INODE_REASSIGNMENT,  // TODO enable only with flag at compile time
  THREAD_REASSIGN,
  REMOTE_BITMAP_CHANGES,
  PING,
  _LANDMARK_COMMON_ADMIN_OP,
  DUMPINODES,
  _LANDMARK_COMMON_ADMIN_OP_END,
  _LANDMARK_SERVANT_CAN_DO_END_,  // never used for real type
  UNLINK,
  CREATE,
  MKDIR,
  RENAME,
  OPENDIR,
  RMDIR,
  _LANDMARK_ADMIN_OP_,  // operations that without inclusion of *inode* (aka.
                        // file)
  APP_EXIT,
  NEW_SHM_ALLOCATED,
  APP_CHKPT,
  START_DUMP_LOADSTAT,
  STOP_DUMP_LOADSTAT,
  GENERIC_CALLBACK,
};

namespace FsReqFlags {
constexpr int no_flags = 0;
constexpr int uses_file_descriptors = 1 << 0;
constexpr int uses_paths = 1 << 1;
constexpr int handled_by_primary = 1 << 2;
constexpr int handled_by_owner = 1 << 3;
// control plane operations like exiting, allocating more shm etc that
// are initiated by the client.
constexpr int client_control_plane = 1 << 4;
// control plane operations that are initiated by the server itself such
// as periodic sync, checkpointing, etc.
constexpr int server_control_plane = 1 << 5;
// NOTE: uses_inodes is (uses_file_descriptors || uses_paths)
// TODO fill in other flags

static constexpr int inline getFlags(const FsReqType t) {
  switch (t) {
    case FsReqType::READ:
    case FsReqType::PREAD:
    case FsReqType::PREAD_UC:
    case FsReqType::PREAD_UC_LEASE_RENEW_ONLY:
    case FsReqType::WRITE:
    case FsReqType::PWRITE:
    case FsReqType::ALLOC_READ:
    case FsReqType::ALLOC_PREAD:
    case FsReqType::ALLOC_WRITE:
    case FsReqType::ALLOC_PWRITE:
    case FsReqType::ALLOC_PREAD_RENEW_ONLY:
    // TODO: PREAD_UC*, ALLOC_PWRITE_RENEW_ONLY
    case FsReqType::LSEEK:
    case FsReqType::CLOSE:
    case FsReqType::FSTAT:
    case FsReqType::FSYNC:
    case FsReqType::FDATA_SYNC:
    case FsReqType::WSYNC:
      return uses_file_descriptors | handled_by_owner;

    case FsReqType::OPEN:
    case FsReqType::STAT:
      return uses_paths | handled_by_owner;

    // NOTE: CREATE is not directly called. An OPEN who's path cannot be
    // resolved is sent to primary. If the path still cannot be resolved,
    // O_CREAT flag is checked to transition to CREATE.
    case FsReqType::CREATE:
    case FsReqType::UNLINK:
    case FsReqType::MKDIR:
    case FsReqType::RENAME:
    case FsReqType::OPENDIR:
    case FsReqType::RMDIR:
      return uses_paths | handled_by_primary;

    case FsReqType::APP_EXIT:
    // NOTE: NEW_SHM is supposed only be handled by primary
    // now it is enforced by FsLib.cc and then primary do broadcast
    // to all the *active* workers
    case FsReqType::NEW_SHM_ALLOCATED:
    case FsReqType::SYNCALL:
      return client_control_plane;

    // TODO: The following should be further categorized because normal apps
    // are not supposed to use these. Example categories - (a) things we need
    // for debugging (b) experimental - to make things faster like
    // SYNCUNLINKED
    case FsReqType::SYNCUNLINKED:
    case FsReqType::INODE_REASSIGNMENT:
    case FsReqType::THREAD_REASSIGN:
    case FsReqType::APP_MIGRATE:
    case FsReqType::PING:
    case FsReqType::DUMPINODES:
    case FsReqType::APP_CHKPT:
    case FsReqType::START_DUMP_LOADSTAT:
    case FsReqType::STOP_DUMP_LOADSTAT:
      return client_control_plane;

    case FsReqType::GENERIC_CALLBACK:
    case FsReqType::REMOTE_BITMAP_CHANGES:
      // NOTE: Checkpointing and SYNCALL can happen on server side too.
      // TODO: For commands that can be performed either periodically or
      // triggered by clients, find a better representation. Maybe, precede them
      // with APP_*
      return server_control_plane;

    default:
      return no_flags;
  }
  return no_flags;
}
}  // namespace FsReqFlags

// used to check this invariant while FSP started (or anytime out of
// critical-path) Invariant: All of the FsReqType value that can be handled by
// servant (Typ) satisfy this:
//   _LANDMARK_SERVANT_CAN_DO_START_ < Typ < _LANDMARK_SERVANT_CAN_DO_END_
// and we rely on *static_assert()* to enforce this
static constexpr bool inline preCheckServantLandmarkInvariant() {
  FsReqType servantTyps[] = {
      // exactly the same as the ones between _LANDMARK_xxx_START(END)_
      FsReqType::READ,
      FsReqType::PREAD,
      FsReqType::PREAD_UC_LEASE_RENEW_ONLY,
      FsReqType::PREAD_UC,
      FsReqType::WRITE,
      FsReqType::PWRITE,
      FsReqType::ALLOC_READ,
      FsReqType::ALLOC_PREAD,
      FsReqType::ALLOC_PREAD_RENEW_ONLY,
      FsReqType::ALLOC_WRITE,
      FsReqType::ALLOC_PWRITE,
      FsReqType::ALLOC_PWRITE_RENEW_ONLY,
      FsReqType::LSEEK,
      FsReqType::CLOSE,
      FsReqType::FSTAT,
      FsReqType::FSYNC,
      FsReqType::FDATA_SYNC,
      FsReqType::WSYNC,
      FsReqType::STAT,
      FsReqType::SYNCALL,
      FsReqType::SYNCUNLINKED,
      FsReqType::PING,
      FsReqType::DUMPINODES,
  };
  for (auto typ : servantTyps) {
    if (!(typ > FsReqType::_LANDMARK_SERVANT_CAN_DO_START_ &&
          typ < FsReqType::_LANDMARK_SERVANT_CAN_DO_END_)) {
      return false;
    }
  }
  return true;
}
static_assert(preCheckServantLandmarkInvariant(),
              "RsReqType::_LANDMARK_SERVANT_INVARIANT CHECK_FAIL");

static constexpr bool inline preCheckAdminOpLandmarkInvariant() {
  FsReqType adminTyps[] = {
      FsReqType::APP_EXIT,
      FsReqType::NEW_SHM_ALLOCATED,
  };
  for (auto typ : adminTyps) {
    if (typ < FsReqType::_LANDMARK_ADMIN_OP_) {
      return false;
    }
  }
  return true;
}
static_assert(preCheckAdminOpLandmarkInvariant(),
              "FsReqType::_LANDMARK_ADMIN_OP_ does not work");

// utility functions to transfer enum class into string for output
static FsReqType gFsReqTpBase = FsReqType::READ;
using FsReqTpEnumVal_t = uint32_t;
#define FS_REQ_TYPE_AD_HOC_TO_STR(A)               \
  {                                                \
    (static_cast<FsReqTpEnumVal_t>(FsReqType::A) - \
     static_cast<FsReqTpEnumVal_t>(gFsReqTpBase)), \
        #A                                         \
  }
const static std::unordered_map<uint32_t, const char *> gFsReqTypeStrMap{
    // data plane operations
    FS_REQ_TYPE_AD_HOC_TO_STR(READ),
    FS_REQ_TYPE_AD_HOC_TO_STR(PREAD),
    FS_REQ_TYPE_AD_HOC_TO_STR(PREAD_UC_LEASE_RENEW_ONLY),
    FS_REQ_TYPE_AD_HOC_TO_STR(PREAD_UC),
    FS_REQ_TYPE_AD_HOC_TO_STR(WRITE),
    FS_REQ_TYPE_AD_HOC_TO_STR(PWRITE),
    FS_REQ_TYPE_AD_HOC_TO_STR(ALLOC_READ),
    FS_REQ_TYPE_AD_HOC_TO_STR(ALLOC_PREAD),
    FS_REQ_TYPE_AD_HOC_TO_STR(ALLOC_PREAD_RENEW_ONLY),
    FS_REQ_TYPE_AD_HOC_TO_STR(ALLOC_WRITE),
    FS_REQ_TYPE_AD_HOC_TO_STR(ALLOC_PWRITE),
    FS_REQ_TYPE_AD_HOC_TO_STR(ALLOC_PWRITE_RENEW_ONLY),
    FS_REQ_TYPE_AD_HOC_TO_STR(LSEEK),
    // control plane operations
    FS_REQ_TYPE_AD_HOC_TO_STR(CREATE),
    FS_REQ_TYPE_AD_HOC_TO_STR(OPEN),
    FS_REQ_TYPE_AD_HOC_TO_STR(CLOSE),
    FS_REQ_TYPE_AD_HOC_TO_STR(MKDIR),
    FS_REQ_TYPE_AD_HOC_TO_STR(STAT),
    FS_REQ_TYPE_AD_HOC_TO_STR(FSTAT),
    FS_REQ_TYPE_AD_HOC_TO_STR(UNLINK),
    FS_REQ_TYPE_AD_HOC_TO_STR(RENAME),
    FS_REQ_TYPE_AD_HOC_TO_STR(FSYNC),
    FS_REQ_TYPE_AD_HOC_TO_STR(FDATA_SYNC),
    FS_REQ_TYPE_AD_HOC_TO_STR(WSYNC),
    FS_REQ_TYPE_AD_HOC_TO_STR(OPENDIR),
    FS_REQ_TYPE_AD_HOC_TO_STR(RMDIR),
    FS_REQ_TYPE_AD_HOC_TO_STR(APP_EXIT),
    FS_REQ_TYPE_AD_HOC_TO_STR(NEW_SHM_ALLOCATED),
    FS_REQ_TYPE_AD_HOC_TO_STR(SYNCALL),
    FS_REQ_TYPE_AD_HOC_TO_STR(SYNCUNLINKED),
    FS_REQ_TYPE_AD_HOC_TO_STR(PING),
    FS_REQ_TYPE_AD_HOC_TO_STR(DUMPINODES),
    FS_REQ_TYPE_AD_HOC_TO_STR(APP_MIGRATE),
    FS_REQ_TYPE_AD_HOC_TO_STR(START_DUMP_LOADSTAT),
    FS_REQ_TYPE_AD_HOC_TO_STR(STOP_DUMP_LOADSTAT),
    FS_REQ_TYPE_AD_HOC_TO_STR(INODE_REASSIGNMENT),
    FS_REQ_TYPE_AD_HOC_TO_STR(THREAD_REASSIGN),
};

std::string inline getFsReqTypeOutputString(FsReqType tp) {
  auto it = gFsReqTypeStrMap.find(static_cast<uint32_t>(tp) -
                                  static_cast<uint32_t>(gFsReqTpBase));
  if (it != gFsReqTypeStrMap.end()) {
    return it->second;
  } else {
    return "NOT SUPPORT";
  }
}

std::ostream &operator<<(std::ostream &os, const FsReqType &tp);

bool inline isFsReqAdminOp(FsReqType tp) {
  return tp > FsReqType::_LANDMARK_ADMIN_OP_ ||
         (tp > FsReqType::_LANDMARK_COMMON_ADMIN_OP &&
          tp < FsReqType::_LANDMARK_COMMON_ADMIN_OP_END);
}

enum class FsReqState {
  //
  // data plane operations
  //
  // read
  READ_FETCH_DATA,
  READ_FETCH_DONE,
  READ_RET_ERR,  // end of read
  // pread
  PREAD_FETCH_DATA,
  PREAD_FETCH_DONE,
  PREAD_RET_ERR,  // end of pread
  UCPREAD_GEN_READ_PLAN,
  UCPREAD_FETCH_DATA,
  UCPREAD_RENEW_LEASE,
  UCPREAD_RET_ERR,
  // write
  WRITE_MODIFY,
  WRITE_UPDATE_INODE,
  WRITE_MODIFY_DONE,
  WRITE_RET_ERR,  // end of write
  // pwrite
  PWRITE_MODIFY,
  PWRITE_UPDATE_INODE,
  PWRITE_MODIFY_DONE,
  PWRITE_RET_ERR,  // end of pwrite
  // allocread
  ALLOCREAD_FETCH_DATA,
  ALLOCREAD_DO_READAHEAD,
  ALLOCREAD_RET_ERR,
  // allocpread
  ALLOCPREAD_UNCACHE_FETCH_DATA,
  ALLOCPREAD_TOCACHE_RENEW_LEASE,
  ALLOCPREAD_DO_READAHEAD,
  ALLOCPREAD_RET_ERR,
  // allocwrite
  ALLOCWRITE_UNCACHE_MODIFY,
  ALLOCWRITE_TOCACHE_MODIFY,
  ALLOCWRITE_UPDATE_INODE,
  ALLOCWRITE_RET_ERR,
  // allocpwrite
  ALLOCPWRITE_UNCACHE_MODIFY,
  ALLOCPWRITE_TOCACHE_MODIFY,
  ALLOCPWRITE_UPDATE_INODE,
  ALLOCPWRITE_RET_ERR,
  // cache + lease
  CCACHE_RENEW,
  // lseek
  LSEEK_INIT,
  LSEEK_RET_ERR,
  //
  // control plane operations
  //
  // create
  CREATE_GET_PRT_INODE,
  CREATE_ALLOC_INODE,
  CREATE_INODE_ALLOCED_NOT_IN_MEM,
  CREATE_FILL_DIR,
  CREATE_UPDATE_INODE,
  CREATE_FINI,
  CREATE_ERR,  // end of create
  // open
  OPEN_GET_CACHED_INODE,
  OPEN_GET_PRT_INODE,
  OPEN_GET_FILE_INUM,
  OPEN_GET_FILE_INODE,
  OPEN_FINI,
  OPEN_ERR,  // end of open
  // close
  CLOSE_INIT,
  CLOSE_RET_ERR,  // end of close
  // mkdir
  MKDIR_GET_PRT_INODE,
  MKDIR_ALLOC_INODE,
  MKDIR_INODE_ALLOCED_NOT_IN_MEM,
  MKDIR_INIT_DOTS,
  MKDIR_UPDATE_PAR_DIR_DATA,
  MKDIR_INODES_SET_DIRTY,
  MKDIR_FINI,
  MKDIR_ERR,  // end of mkdir
  // stat
  STAT_GET_CACHED_INODE,
  STAT_GET_PRT_INODE,
  STAT_GET_FILE_INUM,
  STAT_FILE_INODE,
  STAT_ERR,  // end of stat
  // fstat
  FSTAT_INIT,
  FSTAT_ERR,  // end of fstat
  // unlink
  // TODO: use kUnlink* naming convention?
  UNLINK_PRIMARY_LOAD_PRT_INODE,
  UNLINK_PRIMARY_GET_FILE_INUM,
  UNLINK_PRIMARY_LOAD_INODE,
  UNLINK_PRIMARY_REMOVE_DENTRY,
  UNLINK_PRIMARY_NOTIFY_USER,
  UNLINK_PRIMARY_HANDOFF_TO_OWNER,
  UNLINK_ERR,  // TODO: have common error func
  // rename
  RENAME_LOOKUP_SRC [[deprecated]],
  RENAME_VALIDATE_SRC_DENTRY_INMEM [[deprecated]],
  RENAME_GET_SRC_FILE_INO [[deprecated]],  // cannot found in dcache
  RENAME_GET_SRC_INODE [[deprecated]],
  RENAME_LOOKUP_DST [[deprecated]],
  RENAME_VALIDATE_DST_DENTRY_INMEM [[deprecated]],
  RENAME_GET_DST_FILE_INO [[deprecated]],  // cannot found in dcache
  RENAME_GET_DST_INODE [[deprecated]],
  RENAME_TRANSACTION [[deprecated]],
  RENAME_RETURN_ORIG_DST_DATABLOCK [[deprecated]],  // BG job
  RENAME_RETURN_ORIG_DST_DATABLOCK_ERASEBYRENAME_MSG_OUT [[deprecated]],
  RENAME_RETURN_ORIG_DST_INODE [[deprecated]],  // BG job
  RENAME_LOOKUP_SRC_DIR,
  RENAME_GET_SRC_INUM,
  RENAME_LOAD_SRC_INODE,
  RENAME_LOOKUP_DST_DIR,
  RENAME_GET_DST_INUM,
  RENAME_LOAD_DST_INODE,
  RENAME_MODIFY_DIRENTRIES,
  RENAME_NOTIFY_USER,
  RENAME_ERR,
  // fsync
  // fdatasync
  FSYNC_DATA_BIO,
  FSYNC_DATA_SYNC_WAIT,  // BIO data blocks flushing
  FSYNC_DATA_SYNC_DONE,  // BIO data blocks done
  FSYNC_JOURNAL,
  FSYNC_DONE,
  FSYNC_ERR,
  // wsync
  WSYNC_ALLOC_ENTIRE,
  WSYNC_DATA_MODIFY,
  WSYNC_DATA_BIO,
  WSYNC_DATA_SYNC_WAIT,
  WSYNC_DATA_SYNC_DONE,
  WSYNC_JOURNAL,
  WSYNC_DONE,
  WSYNC_ERR,
  // syncall
  SYNCALL_GUARD,
  SYNCALL_INIT,
  SYNCALL_DONE,
  SYNCALL_ERR,
  // syncunlinked
  SYNCUNLINKED_GUARD,
  SYNCUNLINKED_INIT,
  SYNC_BATCHES_INPROGRESS,
  SYNC_BATCHES_COMPLETE,
  // remote_bitmap_changes
  REMOTE_BITMAP_CHANGES_INIT,
  REMOTE_BITMAP_CHANGES_BMAPS_READY,
  // opendir
  OPENDIR_GET_CACHED_INODE,
  OPENDIR_GET_PRT_INODE,
  OPENDIR_GET_FILE_INUM,  // regard dir as file
  OPENDIR_GET_FILE_INODE,
  OPENDIR_READ_WHOLE_INODE,
  OPENDIR_ERR,
  // rmdir
  RMDIR_START_FAKE,
  // newshmalloc
  NEW_SHM_ALLOC_SEND_MSG,
  NEW_SHM_ALLOC_WAIT_REPLY,
  // common status
  DECONSTRUCT,
  // common status about split/join
  OP_OWNERSHIP_UNKNOWN,
  OP_OWNERSHIP_REDIRECT,
  OP_REQTYPE_OUT_OF_CAP,
  OP_STATE_DEFAULT_OR_SOMETHING_WRONG,
};

class BlockReq {
 public:
  BlockReq(uint64_t blockNo, BlockBufferItem *bufIterm, char *dst,
           enum FsBlockReqType t);
  ~BlockReq();
  enum FsBlockReqType getReqType() { return reqType; }
  uint64_t getBlockNo() { return blockNo; }
  char *getBufPtr() { return memPtr; }
  BlockBufferItem *getBufferItem();
  void setBlockNoSeqNo(uint64_t seqNo);
  uint64_t getBlockNoSeqNo();
  bool getSubmitted();
  void setSubmitted(bool s);

 private:
  uint64_t blockNo;
  uint64_t blockNoSeqNo;
  bool isSubmitted = false;
  char *memPtr;
  BlockBufferItem *bufferItem;
  FsBlockReqType reqType;
};

// Maintain the BIO related stuff in this separate class
class FsReqIoUnitHelper {
 public:
  typedef std::unordered_map<cfs_bno_t, bool> pendingIoUnit_map_t;

  FsReqIoUnitHelper(std::unordered_map<cfs_bno_t, bool> *pendingMap,
                    std::unordered_map<cfs_bno_t, BlockReq *> *subMap, int ioSz,
                    bool dbg)
      : ioUnitPendingMap_(pendingMap),
        submitIoUnitReqMap_(subMap),
        ioUintSizeByte_(ioSz),
        isDebug_(dbg) {}

  ~FsReqIoUnitHelper() {}

  // add one io request
  // @param iouNo : logical # of that ioUnit, E.g., blockNo, sectorNo
  // @param isSubmit : if this request is the one to submit the device access
  //   the location pointed out by *iouNo*
  void addIoUnitReq(cfs_bno_t iouNo, bool isSubmit);

  BlockReq *addIoUnitSubmitReq(cfs_bno_t iouNo, BlockBufferItem *item,
                               char *dst, FsBlockReqType type);
  BlockReq *getIoUnitSubmitReq(cfs_bno_t iouNo);
  void submittedIoUnitReqDone(cfs_bno_t iouNo);

  // mark this request DONE for iouNo
  // That is, since called (supposedly as a callback), the corresponding FsReq
  // do not need to wait for this *iouNo*, it is in memory
  void ioUnitWaitReqDone(cfs_bno_t iouNo);
  int getIoUnitSize() { return ioUintSizeByte_; }

 private:
  std::unordered_map<cfs_bno_t, bool> *ioUnitPendingMap_;
  std::unordered_map<cfs_bno_t, BlockReq *> *submitIoUnitReqMap_;
  int ioUintSizeByte_;
  bool isDebug_;
};

class FsReq {
  typedef void (*FsReqCompletionCallback)(FsReq *req, void *ctx);

 public:
  using iou_map_t = FsReqIoUnitHelper::pendingIoUnit_map_t;
  // <blockNO, isPending>
  //    FsReq(int wid, AppProc *appProc, off_t slotId, struct clientOp *cop,
  //          char *data);
  FsReq(int w) : wid(w) { initBioHelpers(); }
  ~FsReq();

  void initReqFromCop(AppProc *app, off_t slotId, struct clientOp *cop,
                      char *data, FsProcWorker *worker);
  void initReqFromCop(AppProc *app, off_t slotId, struct clientOp *cop,
                      char *data) {
    initReqFromCop(app, slotId, cop, data, nullptr);
  }

  // reset the req's general functional data
  void resetReq();

  // reset the stats that associated with load (like waiting, queuelen)
  void resetLoadStatsVars();

  perfstat_cycle_t retrieveReqOnCpuCycles() const;
  // get the cpu cycles spent on this req, but more accurate
  perfstat_cycle_t accountedReqOnCpuCycles() const;
  perfstat_ts_t getStartTick() const;

  //
  // simple getter and setters
  //
  int getFd() { return fd; }
  int getWid() { return wid; }
  void setWid(int w) { wid = w; }
  pid_t getPid() { return app->getPid(); }
  AppProc *getApp() { return app; }
  off_t getSlotId() { return appRingSlotId; }
  int getTid() { return tid; }
  std::string getOutputStr();
  clientOp *getClientOp() { return copPtr; }
  rwOpCommon *getRwOp() { return rwopPtr; }
  allocatedOpCommon *getAlOp() { return alopPtr; }
  FsReqType getType() { return reqType; }
  void setType(const FsReqType t) {
    // TODO set these back to without _. This was just for refactoring to use
    // setType
    reqType = t;
    reqTypeFlags = FsReqFlags::getFlags(t);
  }
  int getReqTypeFlags() { return reqTypeFlags; }
  FsReqState getState() { return reqState; }
  void setState(FsReqState s) { reqState = s; }
  void resetCreateToOpenExisting() {
    setType(FsReqType::OPEN);
    reqState = FsReqState::OPEN_GET_CACHED_INODE;
  }

  void onFsReqCompletion() {
    if (completionCallback != nullptr) {
      (*completionCallback)(this, completionCallbackCtx);
    }
  }

  //
  // mark request as completed so client can read response
  //
  void markComplete();

  //
  // read BIO
  //
  void addBlockSubmitReq(uint32_t blockNo, BlockBufferItem *bufItem, char *dst,
                         FsBlockReqType type);
  void addSectorSubmitReq(uint32_t sectorNo, BlockBufferItem *bufItem,
                          char *dst, FsBlockReqType type);
  // void addBlockWriteReq(uint32_t blockNo, BlockBufferItem *bufItem, char
  // *src);
  void addSectorWriteReq(uint32_t sectorNo, BlockBufferItem *bufItem,
                         char *src);
  BlockReq *getBlockSubmitReq(uint32_t blockNo) {
    return blockIoHelper->getIoUnitSubmitReq(blockNo);
  }
  BlockReq *getSectorSubmitReq(uint32_t sectorNo) {
    return sectorIoHelper->getIoUnitSubmitReq(sectorNo);
  }

  void submittedBlockReqDone(uint32_t blockNo);
  void submittedSectorReqDone(uint32_t sectorNo);

  void addBlockWaitReq(uint32_t blockNo) {
    blockIoHelper->addIoUnitReq(blockNo, false);
  }
  void addSectorWaitReq(uint32_t sectorNo) {
    sectorIoHelper->addIoUnitReq(sectorNo, false);
  }

  // Compare the blockNo w/ BlockReq's BlockNoSeqNo.
  // If matching, then will erase req from this FsReq's list
  // return true if This FsReq submit corresponding blockNoSeqNo's request
  bool writeReqDone(uint64_t blockNo, uint64_t blockNoSeqNo,
                    bool &allWriteDone);
  bool writeSectorReqDone(uint64_t sectorNo, uint64_t sectorNoSecNo,
                          bool &allWriteDone);

  void blockWaitReqDone(uint32_t blockNo) {
    blockIoHelper->ioUnitWaitReqDone(blockNo);
  }
  void sectorWaitReqDone(uint32_t sectorNo) {
    sectorIoHelper->ioUnitWaitReqDone(sectorNo);
  }

  int numTotalPendingIoReq() {
    // if (isDebug_) {
    //   fprintf(stderr, "numBlock:%lu numSec:%lu\n", blockPendingMap.size(),
    //           sectorPendingMap.size());
    // }
    return blockPendingMap.size() + sectorPendingMap.size();
  }
  int numPendingBlockReq() { return blockPendingMap.size(); }
  int numPendingSectorReq() { return sectorPendingMap.size(); }

  uint64_t GetAppendOffOrInit(off_t off) {
    if (append_start_off == 0) append_start_off = off;
    return append_start_off;
  }
  uint64_t GetAppendOff() { return append_start_off; }

  //
  // establish context (especially once IO is done for metadata)
  //
  bool IsInUse() { return in_use; }
  void setDirInode(InMemInode *dirInode);
  void setDirMap(FsPermission::LevelMap *map) { parDirMap = map; };
  FsPermission::LevelMap *getDirMap() { return parDirMap; };
  InMemInode *getDirInode() { return parDirInodePtr; };
  // context for rename's destination
  void setDstDirInode(InMemInode *dstDirInode);
  void setDstDirMap(FsPermission::LevelMap *map) { dstParDirMap = map; }
  FsPermission::LevelMap *getDstDirMap() { return dstParDirMap; }
  InMemInode *getDstDirInode() { return dstParDirInodePtr; }

  void setFileIno(uint32_t ino) { fileIno = ino; }
  uint32_t getFileInum();
  // NOTE: only call this when sure the request has completed and not
  // just newed from the req pool because we never reset it
  // caller need to reset it once it calls getter
  cfs_ino_t GetFileInoLazyCopy() { return after_reset_ino_copy; }
  void ResetLazyFileInoCopy() { after_reset_ino_copy = 0; }
  void SetLazyFileInoCopy(cfs_ino_t ino) { after_reset_ino_copy = ino; }
  void setDstFileIno(uint32_t ino) { dstFileIno = ino; }
  uint32_t getDstFileInum();

  InMemInode *getTargetInode() { return targetInodePtr; }
  InMemInode *getDstTargetInode() { return dstTargetInodePtr; }

  uint16_t GetReadyEnqueCnt() const { return recvQWaitCnt; }
  uint16_t GetQLenAccumulate() const { return recvQLenAccumulate; }

#define FSREQ_LOCAL_TIMER_DBG

  void startOnCpuTimer() {
    if (ifUseOnCpuTimer()) {
#ifdef FSREQ_LOCAL_TIMER_DBG
      if (curOnCpuTermStartTick != 0 &&
          (reqTypeFlags != FsReqFlags::client_control_plane &&
           reqTypeFlags != FsReqFlags::server_control_plane)) {
        std::cerr << reqType << std::endl;
        throw std::runtime_error("timer not reset reqType");
      }
#endif
      curOnCpuTermStartTick = PerfUtils::Cycles::rdtsc();
    }
  }

  void stopOnCpuTimer() {
    if (ifUseOnCpuTimer()) {
#ifdef FSREQ_LOCAL_TIMER_DBG
      if (curOnCpuTermStartTick == 0 &&
          (reqTypeFlags != FsReqFlags::client_control_plane &&
           reqTypeFlags != FsReqFlags::server_control_plane)) {
        std::cerr << reqType << " isError?" << int(getErrorNo()) << std::endl;
        throw std::runtime_error("timer not started");
      }
#endif
      perfstat_cycle_t diff =
          PerfUtils::Cycles::rdtsc() - curOnCpuTermStartTick;
      if (diff > 0) {
        totalOnCpuCycles += diff;
      }
#ifdef FSREQ_LOCAL_TIMER_DBG
      curOnCpuTermStartTick = 0;
#endif
    }
  }

#ifdef FSREQ_LOCAL_TIMER_DBG
#undef FSREQ_LOCAL_TIMER_DBG
#endif

  bool ifUseOnCpuTimer() { return kUseOncpuTimer; }
  auto GetCurStartTick() { return curOnCpuTermStartTick; }
  // void SetUseOnCpuTimer(bool b) { use_oncpu_timer = b;}

  block_no_t getFileDirentryBlockNo(int &idx);
  void setFileDirentryBlockNo(block_no_t blkno, int inblockIndex);
  block_no_t getDstFileDirentryBlockNo(int &idx);
  void setDstFileDirentryBlockNo(block_no_t blockno, int inblockIndex);

  // for rename's semantics according to if destionation exist or not
  bool isDstExist() const { return dstExist; }
  void setDstExist(bool b) { dstExist = b; }

  void setFileObj(FileObj *fobj) { fileObj_ = fobj; }
  FileObj *getFileObj() { return fileObj_; }

  //
  // path resolution
  //
  // return the filename (no parent directories ...)
  // std::string getFileName();
  // std::string getNewFileName();
  bool hasStandardFullPath() { return standardFullPath != nullptr; }

  void fillStandardFullPath(const std::string &p) {
    assert(standardFullPath == nullptr);
    assert(p.length() < MULTI_DIRSIZE);
    standardFullPath = static_cast<char *>(malloc(p.length() + 1));
    strcpy(standardFullPath, p.c_str());
  }
  // return the file (or directory)'s full path in a standard format
  std::string getStandardFullPath(int &pathDepth);
  std::string getNewStandardFullPath(int &pathDepth);
  // return the standard-format path with leafName as the end layer
  std::string getStandardPartialPath(absl::string_view leafName,
                                     int &pathDepth);
  // return this request's leaf's parent directory path
  std::string getStandardParPath(int &pathDepth);
  std::string getNewStandardParPath(int &pathDepth);
  const std::vector<std::string> &getPathTokens() { return pathTokens; }
  const std::vector<std::string> &getDstPathTokens() { return dstPathTokens; }

  int getStandardPathDelimIdx(int depth) {
    assert(depth < standardFullPathDepth);
    return standardFullPathDelimIdx[depth];
  }

  int *getStandardPathDelimArr(int &size) {
    size = standardFullPathDepth;
    return standardFullPathDelimIdx;
  }

  // a new override of the function that runs faster
  // get the partial path as if the path has depth at @param depth
  std::string getStandardPartialPath(int depth) {
    assert(depth > 0);
    assert(depth <= standardFullPathDepth);
    if (depth == standardFullPathDepth) return standardFullPath;
    return std::string(standardFullPath, getStandardPathDelimIdx(depth) - 1);
  }

  const std::string &getLeafName() {
    return pathTokens[standardFullPathDepth - 1];
  }

  const std::string &getNewLeafName() {
    return dstPathTokens[dstStandardFullPathDepth - 1];
  }

  int getStandardPathDepth() { return standardFullPathDepth; }

  bool isPathRoot() { return standardFullPathDepth == 0; }

  //
  // error handling
  //
  bool hasError() { return errorNo != FS_REQ_ERROR_NONE; }
  // Set default error if errorno is not set
  void setError() {
    if (!hasError()) errorNo = FS_REQ_ERROR_POSIX_RET;
  }
  void setError(int8_t eno) { errorNo = eno; }
  void resetErr() { errorNo = FS_REQ_ERROR_NONE; }
  int8_t getErrorNo() { return errorNo; }
  // void setDebug(bool val);
  // bool isDebug() { return isDebug_; }
  std::string getErrorMsg();

  static perfstat_cycle_t genTickCycle() { return PerfUtils::Cycles::rdtscp(); }
  // static perfstat_cycle_t genTickCycle() { return PerfUtils::Cycles::rdtsc();
  // }

  // @param qlen: readyQueue Length when the request is dequeued
  //              NOTE: include itself
  void recordDequeueReady(int qlen) {
    recvQLenAccumulate += qlen;
    recvQWaitCnt++;
  }

  // uint32_t getNumReadBlocks() { return numBlockRead; }
  //// std::string outputReadBnos();
  void setBlockIoDone(char *dst);
  bool getBlockIoDone(char *dst);
  void setBlockIoDoneByBlockIdx(uint32_t blockIdx);
  bool getBlockIoDoneByBlockIdx(uint32_t blockIdx);
  char *getChannelDataPtr();

  //
  // client buffer related
  //
  bool isAppBufferAvailable() {
    assert(rwopPtr != nullptr);
    return rwopPtr->flag & _RWOP_FLAG_FSLIB_ENABLE_APP_BUF_;
  }
  fslib_malloc_block_cnt_t getDataPtrId() { return dataPtrId; }
  char *getMallocedDataPtr() {
    auto ptr = app->getDataPtrByShmIdAndDataId(shmId, dataPtrId);
    return static_cast<char *>(ptr);
  }

  uint32_t getMemOffset() { return mem_start_offset; }

  // @return : the new allocated shmId
  std::pair<uint8_t, void *> initAppNewAllocatedShm() {
    assert(app != nullptr);
    return app->initNewAllocatedShm(shmName, copPtr->op.newshmop.shmBlockSize,
                                    copPtr->op.newshmop.shmNumBlocks);
  }

  //
  // client cache related
  //
  bool isAppCacheAvailable();
  void disableAppCache() {
    // flop the bit
    rwopPtr->flag =
        UTIL_BIT_FLIP(rwopPtr->flag, (_RWOP_FLAG_FSP_DISABLE_CACHE_));
  }
  bool isLeaseRenewOnly() { return (isLeaseRwOpRenewOnly(rwopPtr)); }
  // Extract the offset of each page int this data operation into a hash map
  // NOTE: off_t (key of saveTo) will be strictly aligned to PAGE_SIZE
  void extractRwReqPageNos(
      std::unordered_map<off_t, InAppCachedBlock *> &saveTo, bool isWrite);
  uint64_t getAccessSeqNo() { return accessSeqNo; }
  void setAccessSeqNo(uint64_t seq) {
    assert(alopPtr != nullptr);
    alopPtr->perAppSeqNo = seq;
    accessSeqNo = seq;
  }

  iou_map_t::const_iterator blockPendingBegin() const {
    return blockPendingMap.begin();
  }
  iou_map_t::const_iterator blockPendingEnd() const {
    return blockPendingMap.end();
  }

  iou_map_t::const_iterator sectorPendingBegin() const {
    return sectorPendingMap.begin();
  }
  iou_map_t::const_iterator sectorPendingEnd() const {
    return sectorPendingMap.end();
  }

  bool isPathBasedReq() {
    bool rt = reqType == FsReqType::CREATE || reqType == FsReqType::UNLINK ||
              reqType == FsReqType::MKDIR || reqType == FsReqType::RMDIR ||
              reqType == FsReqType::RENAME || reqType == FsReqType::OPEN ||
              reqType == FsReqType::OPENDIR || reqType == FsReqType::STAT;
    return rt;
  }

  void setReqBgGC(bool b) { isBgGC = b; }
  bool isReqBgGC() { return isBgGC; }

 private:
  AppProc *app{nullptr};
  off_t appRingSlotId;

  bool in_use = false;
  int tid = 0;

  FsReqType reqType;
  int reqTypeFlags;
  FsReqState reqState{FsReqState::OP_STATE_DEFAULT_OR_SOMETHING_WRONG};

  struct clientOp *copPtr{nullptr};
  // save the rwCommon op here, must be set when de-code App request
  struct rwOpCommon *rwopPtr{nullptr};
  struct allocatedOpCommon *alopPtr{nullptr};
  char *channelDataPtr{nullptr};

  // While most request types currently use
  // FsProc_Worker::submitFsReqCompletion, syncall uses the completion callback.
  // Eventually we can port all requests to use the completion callback which
  // breaks up large functions with long if-else chains.
  FsReqCompletionCallback completionCallback{nullptr};
  void *completionCallbackCtx{nullptr};

  // We allow filePath to be this long in terms of directories
  // NOTE: root directory "/" has depth 0
  static constexpr int kMaxPathDepth = 32;
  char *standardFullPath{nullptr};
  std::vector<std::string> pathTokens;
  int standardFullPathDelimIdx[kMaxPathDepth];
  int standardFullPathDepth{0};
  // Used for fs ops that includes two paths, E.g., rename(oldpath, newpath)
  char *newStandardFullPath{nullptr};
  std::vector<std::string> dstPathTokens;
  int dstStandardFullPathDelimIdx[kMaxPathDepth];
  int dstStandardFullPathDepth{0};

  // for r/w client cache, initialized in constructor.
  std::string shmName;
  fslib_malloc_block_cnt_t dataPtrId = 0;
  uint32_t mem_start_offset = 0;
  uint8_t shmId = 0;
  uint64_t accessSeqNo = 0;

  int fd;
  FileObj *fileObj_{nullptr};
  // workerId that will handle this request.
  int8_t wid;
  // if isError is set, the result of this operation will be set to ERROR
  int8_t errorNo{FS_REQ_ERROR_NONE};
  // bool isDebug_ = false;

  uint16_t recvQWaitCnt = 0;
  uint16_t recvQLenAccumulate = 0;

  // [Stats]: record the number of block-level io this FsReq object issues.
  // Only record the request that actually goes down to device.
  // uint32_t numBlockRead = 0;

  // <blockNo, isSubmit>
  // a submission request must be both in blockPendingMap and submitBlockReqMap
  std::unordered_map<cfs_bno_t, bool> blockPendingMap;
  std::unordered_map<cfs_bno_t, BlockReq *> submitBlockReqMap;
  FsReqIoUnitHelper *blockIoHelper;

  std::vector<std::pair<cfs_bno_t, size_t>> ucReqPagesToRead;
  std::vector<std::pair<PageDescriptor *, void *>> ucReqDstVec;

  // <sectorNo, isSubmit>
  std::unordered_map<cfs_bno_t, bool> sectorPendingMap;
  std::unordered_map<cfs_bno_t, BlockReq *> submitSectorReqMap;
  FsReqIoUnitHelper *sectorIoHelper;

  // the inode number which this request will use
  uint32_t fileIno = 0;
  cfs_ino_t after_reset_ino_copy = 0;
  // the file inode's dentry data block no.
  block_no_t fileDentryDataBlockNo = 0;
  // in the data block of direntry pointed by fileDentryDataBlockNo, the index
  // of which that contains <fileName, fileIno> mapping.
  int withinBlockDentryIndex = 0;

  // inode of target file, note this can be either FILE, or DIR inode
  // depends on specific types of fsReq. E.g., openDir() -> DIR, open() -> FILE
  InMemInode *targetInodePtr{nullptr};
  InMemInode *parDirInodePtr{nullptr};
  FsPermission::LevelMap *parDirMap{nullptr};

  // inode centered cpu time accounting
  perfstat_cycle_t totalOnCpuCycles = 0;
  perfstat_ts_t curOnCpuTermStartTick = 0;
  constexpr static bool kUseOncpuTimer = true;
  // bool use_oncpu_timer = kUseOncpuTimer;

  uint32_t dstFileIno = 0;
  block_no_t dstFileDentryDataBlockNo = 0;
  int dstWithinBlockDentryIndex = 0;
  InMemInode *dstTargetInodePtr{nullptr};
  InMemInode *dstParDirInodePtr{nullptr};
  FsPermission::LevelMap *dstParDirMap{nullptr};
  bool dstExist = true;

  // for read/write
#ifdef USE_ABSL_LIB
  absl::flat_hash_map<char *, bool> blockIoDoneMap;
#else
  // std::unordered_map<char *, bool> blockIoDoneMap;
  std::unordered_map<std::uintptr_t, bool> blockIoDoneMap{8};
#endif
  // It is basically another representation of blockIoDoneMap
  // The difference is that the above will be using the memory address of target
  // data destination/source while bellow use the blockIdx within this file
  // as the key. This is because some of the request will not have to do data
  // manipulation, but FSP can do some background work or simply release the
  // data blocks.
  std::unordered_map<uint32_t, bool> blockIoBlkIdxDoneMap;

  // for append
  uint64_t append_start_off = 0;

  // set the inode to this fsReq
  // @return : the original value of targetFileInode
  //   this will indicate if fileMng needs to record this fsReq to worker
  // REQUIRED: this should only be called via FsProcWorker's
  // onTargetInodeFiguredout. So make it private and use friend to enforce
  // FileMng's permission check also access this now
  const InMemInode *setTargetInode(InMemInode *inode);
  const InMemInode *setDstTargetInode(InMemInode *inode);
  const InMemInode *setTargetInodeCommon(InMemInode *inode, uint32_t &ino,
                                         InMemInode **inodeAddr);

  void initBioHelpers() {
    blockIoHelper = new FsReqIoUnitHelper(&blockPendingMap, &submitBlockReqMap,
                                          (BSIZE), false);
    sectorIoHelper = new FsReqIoUnitHelper(
        &sectorPendingMap, &submitSectorReqMap, (SSD_SEC_SIZE), false);
  }

  // whether or not this request still has background GC remained
  // Used for request like unlink, rename, which needs GC of the disk space
  // for inode, inode's data blocks etc.
  // Considering the capacity of the master thread, it might be a better idea
  // to aggregate the GC's and process one batch at a time when worker is idle.
  bool isBgGC{false};

  // used for shm msg
  int pendingShmMsg = 0;

  // used for syncUnlinked and syncAll
  // TODO: make separate structures for different requests. Put them as contexts
  // with callbacks.
  size_t syncBatchesCreated{0};
  size_t syncBatchesCompleted{0};
  size_t syncBatchesFailed{0};

  // used on the server side with type GENERIC_CALLBACK
  // Initial usecase - a request to make block i/o requests to read bmaps before
  // background unlink operations take place. The callback will first ensure i/o
  // completes and then the callback will resume the unlink operation. Take a
  // look at how its used in FileMng::UnlinkOp::OwnerEnsureLocalBitmapsInMem
  using GenericCallbackFn = std::add_pointer<void(FileMng *, FsReq *)>::type;
  GenericCallbackFn generic_callback{nullptr};
  void *generic_callback_ctx{nullptr};

  friend class FsProcWorker;
  friend class FileMng;
  friend class FsReqPool;
  friend class FsProcWorkerMaster;
  friend class FsProcWorkerServant;
};

class FsReqPool {
 public:
  FsReqPool(int wid);
  ~FsReqPool();
  constexpr static int kPerWorkerReqPoolCapacity = 65536;

  FsReq *genNewReq(AppProc *appProc, off_t slotId, struct clientOp *cop,
                   char *data, FsProcWorker *worker);
  FsReq *genNewReq();
  void returnFsReq(FsReq *req);
  size_t freeNum() { return fsReqQueue_.size(); }

  // @param version: new version id
  void resetReqPoolStats(uint64_t version);

 private:
  int wid_;
  std::deque<FsReq *> fsReqQueue_;

  friend class FsProcWorker;
};

// Buffer flush request that is generated by FS itself.
class BufferFlushReq {
 public:
  constexpr static bool kEnableTrace = false;
  constexpr static int kMaxSubmitBatchSize = 32;
  BufferFlushReq(BlockBuffer *srcBuf, FsProcWorker *worker)
      : srcBuf(srcBuf), submitWorker(worker) {}
  ~BufferFlushReq() {}
  // initialize the block level request for flushing.
  // will return -1 if error happens.
  // @param: index, if set to value > 0, will initFlushReqs according to index.
  //      otherwise, the blockBuffer will choose the blocks by itself
  // @return: return number of blocks that are flags
  int initFlushReqs(int index);
  // Submit the flushing request to FSP
  // return the number of blocks to be flushed.
  int submitFlushReqs();
  // Check if this block request is a valid flush request.
  // Because when *fsync* is issued from client, it is possible that
  // a write-disk request is issued device while a flushing request is inflight.
  bool checkValidFlushReq(block_no_t blockNo, uint64_t seqNo);
  void blockReqFinished(block_no_t blockNo);
  bool flushDone();
  // let the blockBuffer reset the buffer's item's status at a time.
  // meanwhile, will destruct finished BlockReq objects.
  void flushDonePropagate();
  // number of blocks included in this req
  uint32_t getBlockNum() { return flushBlockReqMap.size(); }

  // whether or not if this flushReq comes from an explicit fsync comes
  // from client
  bool isAssociatedWithFsReq() { return fsReq != nullptr; }
  void setFsReq(FsReq *req);
  // FsReq *getFsReq() { return fsReq; }
  uint64_t submitTs = 0;
  uint64_t doneTs = 0;
  static uint64_t tapFlushTs() { return PerfUtils::Cycles::rdtsc(); }
  void setEnableTrace(bool b) { enableTrace_ = b; }

 private:
  BlockBuffer *srcBuf;
  // if this flushRequest directly comes from app request, store it here
 public:
  FsReq *fsReq = nullptr;

 private:
  std::unordered_map<block_no_t, BlockReq *> flushBlockReqMap;
  std::list<block_no_t> toSubmitFlushBlocks;
  // blocks that has been submitted to the device, but not done yet
  std::unordered_set<block_no_t> submittedFlushBlocks;
  FsProcWorker *submitWorker;

  bool enableTrace_{kEnableTrace};
};

inline perfstat_cycle_t FsReq::retrieveReqOnCpuCycles() const {
  return accountedReqOnCpuCycles();
}

inline perfstat_cycle_t FsReq::accountedReqOnCpuCycles() const {
  return totalOnCpuCycles;
}

inline perfstat_ts_t FsReq::getStartTick() const {
  return curOnCpuTermStartTick;
}

#endif  // CFS_INCLUDE_FSPROC_FSREQ_H_
