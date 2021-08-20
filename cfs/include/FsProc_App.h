#ifndef CFS_INCLUDE_FSPROC_APP_H_
#define CFS_INCLUDE_FSPROC_APP_H_

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <unordered_set>
#include <utility>

#include "FsLibMalloc.h"
#include "FsLibProc.h"
#include "shmipc/shmipc.h"
#include "typedefs.h"
//
class InMemInode;
class FsReq;
//

#define CFS_FSP_IN_APP_CACHED_BLOCK_DIRTY (0b00000001)
struct InAppCachedBlock {
  InAppCachedBlock() : startOffWithinInode(0), flag(0), accessSeqNo(1) {}
  // REQUIRED: aligned to client-cache's page size
  off_t startOffWithinInode;
  // bit attributes from the lowest:
  // - if the corresponding pages is dirty
  uint8_t flag;
  // This sequence number will be updated in two situation:
  //   - data is newly fetched from disk
  //   - data is modified by a write operation
  uint64_t accessSeqNo;
};

struct AppCredential {
  pid_t pid = 0;
  uid_t uid = 0;
  gid_t gid = 0;
  int app_idx = -1;
  AppCredential() {}
  AppCredential(pid_t p, uid_t u, gid_t g) : pid(p), uid(u), gid(g) {}
  AppCredential(pid_t p, uid_t u, gid_t g, int idx)
      : pid(p), uid(u), gid(g), app_idx(idx) {}
  void Reset() {
    pid = 0;
    uid = 0;
    gid = 0;
  }
};

// represents opened file to each process
struct FileObj {
  enum {
    FD_NONE,
    FD_INODE,
  } type;
  ////int ref; // reference count, not sure why need this ref.
  InMemInode *ip;
  int flags;
  mode_t mode;
  uint64_t off;
  int readOnlyFd;
  // TODO (jingliu): delete this, unnecessary now
  cfs_ino_t shadow_ino;
};

static void inline fileObjCpy(const FileObj *src, FileObj *dst) {
  dst->type = src->type;
  dst->ip = src->ip;
  dst->flags = src->flags;
  dst->mode = src->mode;
  dst->readOnlyFd = src->readOnlyFd;
  dst->shadow_ino = src->shadow_ino;
}

class AppProc {
 public:
  constexpr static bool kIsDebug = false;

  AppProc(int appIdx, int shmBaseOffset, AppCredential &credential);
  ~AppProc();

  // NOTE: This getPid() is supposed to be the only single unique ID for this
  // AppProc across the whole system
  pid_t getPid() { return cred.pid; }
  uid_t getUid() { return cred.uid; }
  gid_t getGid() { return cred.gid; }

  // Allocate file descriptor
  FileObj *allocateFd(InMemInode *inodePtr, int openFlags, mode_t openMode);
  int updateFdIncrByWid(int wid);

  // retrieve channel data via the slot id
  struct clientOp *channelSlotOpPtr(int sid);
  void *channelSlotDataPtr(int sid);

  FileObj *getFileObj(int fd);

  //
  // inode transfer
  //
  // Put all the opened File (FileObj, <-> FD) that is associated with specific
  // inode into the passed-in vector
  // @param : ino, inode # that is used to specify which inode *files* pointed
  //           to.
  //        : files, it is the caller's job to give an empty vector
  //           this function will only do *emplace_back*
  // @return : the number of opened files (i.e., files.size())
  int findAllOpenedFiles4Ino(uint32_t ino, std::vector<FileObj *> &files);
  // Copy a list of opened *FileObj* into this *AppProc*.
  // Will do overwrite if the Fd is already existed
  // We assume the *files* vector contains the updated info. of FDs
  // @directUseVec: whether or not we use the \files' fobj directly
  //  e.g., if this is just for master to bookkeep a forward request
  // then we will need to avoid using this obj, otherwise dst and master
  // will share the fobj
  int overwriteOpenedFiles(std::vector<FileObj *> &files,
                           bool directUseVec = true);

  std::unordered_map<uint8_t, SingleSizeMemBlockArr *> *findAllOpenedShmArrs() {
    return &shmIdArrMap;
  }
  int overwriteOpenedShmIds(
      const std::unordered_map<uint8_t, SingleSizeMemBlockArr *> *shmArrs) {
    shmIdArrMap.insert(shmArrs->begin(), shmArrs->end());
    return shmIdArrMap.size();
  }

  const std::map<int, perfstat_cycle_t> &GetPerThreadCycleMap() {
    return per_tau_exclusive_cycles_;
  }

#ifdef KP_SPECIAL_NUM_CREATE
  const std::map<int, int> &GetPerThreadNumCreateMap() {
    return per_tau_num_create_;
  }
  void IncrementNumCreate(int tid) {
    auto it = per_tau_num_create_.find(tid);
    if (it != per_tau_num_create_.end()) {
      it->second++;
    } else {
      per_tau_num_create_.emplace(tid, 1);
    }
  }
  void ResetNumCreates() { per_tau_num_create_.clear(); }
#endif

  void AddTauExclusiveCycles(int tau_id, perfstat_cycle_t cycles) {
    auto it = per_tau_exclusive_cycles_.find(tau_id);
    if (it == per_tau_exclusive_cycles_.end()) {
      per_tau_exclusive_cycles_.emplace(tau_id, cycles);
    } else {
      it->second += cycles;
    }
  }

  void SubstractExclusiveCycles(int tau_id, perfstat_cycle_t cycles) {
    auto it = per_tau_exclusive_cycles_.find(tau_id);
    // assert(it != per_tau_exclusive_cycles_.end());
    // TODO (jingliu): check why this could be *cannot find*
    if (it != per_tau_exclusive_cycles_.end()) {
      if (it->second >= cycles) {
        it->second -= cycles;
      } else {
        it->second = 0;
      }
    }
  }

  void ResetExclusiveCycles() { per_tau_exclusive_cycles_.clear(); }

  //
  // client cache
  //
  int addInAppAllocOpDataBlocks(FsReq *req, bool isWrite);
  int retrieveInAppAllocOpDataBlocks(FsReq *req, bool &allInApp,
                                     uint64_t &maxSeq);
  void dumpInoCachedBlocks(uint32_t ino);

  void *getDataPtrByShmNameAndDataId(std::string &shmName,
                                     fslib_malloc_block_cnt_t dataId,
                                     fslib_malloc_block_sz_t shmBlockSize,
                                     fslib_malloc_block_cnt_t shmNumBlocks);

  void *getDataPtrByShmIdAndDataId(uint8_t shmId,
                                   fslib_malloc_block_cnt_t dataId);

  std::pair<uint8_t, void *> initNewAllocatedShm(
      std::string &shmName, fslib_malloc_block_sz_t shmBlockSize,
      fslib_malloc_block_cnt_t shmNumBlocks);

  // Invalidate the <shmName, shmPtr> once the application exits
  // TODO (jingliu) :clean all the client related context once a client exits
  int invalidateAppShm();

  const static std::unordered_set<cfs_ino_t> _empty_;
  const std::unordered_set<cfs_ino_t> &GetAccessedInos(int tid);
  void GetAccessedInoAndErase(int tid, std::unordered_set<cfs_ino_t> &inos);
  // TODO (when we reset this assessed_ino?)
  // Maybe when app exits
  inline void AccessIno(int tid, cfs_ino_t ino);
  inline void EraseIno(int tid, cfs_ino_t ino);
  void ClearAllInoAccess() { accessed_ino.clear(); }

  struct TauDstNote {
    TauDstNote(int8_t lwid) : local_wid(lwid) {}
    void inline UpdateDst(int8_t wid, float pct);
    void inline ResetDst() {
      only_one_dst = false;
      total_cnt = 0;
      wid_count.clear();
      wid_pct.clear();
      total_inos.clear();
    }
    void inline Print(int cur_wid, pid_t pid, int tid, std::ostream &out);
    int inline GetCurDstWid(cfs_ino_t ino);

    std::map<int8_t, int> wid_count;
    std::map<int8_t, float> wid_pct;
    std::set<cfs_ino_t> total_inos;
    uint32_t total_cnt = 0;
    bool only_one_dst = false;
    int only_dst_wid = 0;
    int8_t local_wid;
  };

  int GetDstWid(int tau_id, cfs_ino_t ino);
  inline void UpdateDstWid(TauDstNote *note_ptr, int to_wid, float pct);
  inline TauDstNote *ResetDstWid(int tau_id, int local_wid);
  inline void EraseDstWid(int tau_id);

  int GetAppIdx() { return appIdx; }
  void ResetCred() { cred.Reset(); }
  void UpdateCred(AppCredential &new_cred) {
    assert(cred.pid == 0);
    cred = new_cred;
    assert(cred.app_idx >= 0);
  }

  struct shmipc_mgr *shmipc_mgr = NULL;
  static constexpr int kFdBase = 100000;

 private:
  int appIdx;         // used for computing shmKey
  int shmBaseOffset;  // used for computing shmKey
  AppCredential cred;
  key_t shmKey;
  // <fd, opened file object>
  std::unordered_map<int, FileObj *> openedFiles;
  // incrementally assign fd based on this value
  int fdIncr;
  std::atomic_flag fdLock;
  uint8_t shmIdIncr = 0;

  // <ino, list of FsReq> that is of type FsReqType::ALLOC_R/W with Cache
  // enabled.
  // store the FsReq that has been ACKed, but the data is not actually copied
  // to FSP's own buffer
  std::unordered_map<uint32_t, std::unordered_map<off_t, InAppCachedBlock *>>
      pendingInAppBlocks;

  int shmFd = -1;
  std::unordered_map<std::string, SingleSizeMemBlockArr *> shmNameMemArrMap;
  std::unordered_map<uint8_t, SingleSizeMemBlockArr *> shmIdArrMap;

  // <pid,<tid, ino>>
  // used for redirect all inode that is exclusively accessed by one app-thread
  std::unordered_map<int, std::unordered_set<cfs_ino_t>> accessed_ino;
  // tid, cycles
  std::map<int, perfstat_cycle_t> per_tau_exclusive_cycles_;

#ifdef KP_SPECIAL_NUM_CREATE
  std::map<int, int> per_tau_num_create_;
#endif

  // std::map<int, int> tau_dst_wid_;
  std::map<int, TauDstNote *> tau_dst_note_;

  friend class MockAppProc;
};

// Used for test only
// It will emulate the work done by fsLib, where client-side op submission
// happens.
class MockAppProc {
 public:
  MockAppProc(AppProc *app) : app(app){};
  struct clientOp *emulateGetClientOp(int sid);
  int emulateClientOpSubmit(int sid);
  void *emulateGetDataBufPtr(int sid);

 private:
  AppProc *app;
};

void AppProc::AccessIno(int tid, cfs_ino_t ino) {
  auto it = accessed_ino.find(tid);
  if (it != accessed_ino.end()) {
    it->second.emplace(ino);
  } else {
    std::unordered_set<cfs_ino_t> m{ino};
    accessed_ino.emplace(tid, m);
  }
}

void AppProc::EraseIno(int tid, cfs_ino_t ino) {
  auto it = accessed_ino.find(tid);
  if (it != accessed_ino.end()) {
    it->second.erase(ino);
  }
}

void AppProc::TauDstNote::UpdateDst(int8_t wid, float pct) {
  if (pct > 0.95) {
    only_dst_wid = wid;
    only_one_dst = true;
    return;
  }
  if (pct >= 0.05) wid_pct[wid] = pct;
}

int AppProc::TauDstNote::GetCurDstWid(cfs_ino_t ino) {
  if (only_one_dst) {
    return only_dst_wid;
  }
  int dst_wid = local_wid;
  for (auto [wid, pct] : wid_pct) {
    auto cnt_it = wid_count.find(wid);
    if (cnt_it == wid_count.end()) {
      if (total_cnt > 0 && 1.0 / total_cnt > pct) {
        wid_count.emplace(wid, 1);
        dst_wid = wid;
        break;
      }
    } else {
      if ((float(cnt_it->second) / total_cnt) < pct &&
          float(cnt_it->second) / total_inos.size() < pct) {
        cnt_it->second++;
        dst_wid = wid;
        break;
      }
    }
  }
  total_inos.emplace(ino);
  total_cnt++;
  return dst_wid;
}

void AppProc::EraseDstWid(int tau_id) {
  auto it = tau_dst_note_.find(tau_id);
  TauDstNote *note_ptr = nullptr;
  if (it != tau_dst_note_.end()) {
    note_ptr = it->second;
    tau_dst_note_.erase(it);
    delete note_ptr;
  }
}

void AppProc::UpdateDstWid(TauDstNote *note_ptr, int to_wid, float pct) {
  note_ptr->UpdateDst((int8_t)to_wid, pct);
}

AppProc::TauDstNote *AppProc::ResetDstWid(int tau_id, int orig_wid) {
  auto it = tau_dst_note_.find(tau_id);
  TauDstNote *note_ptr = nullptr;
  if (it == tau_dst_note_.end()) {
    note_ptr = new TauDstNote(orig_wid);
    tau_dst_note_.emplace(tau_id, note_ptr);
  } else {
    note_ptr = it->second;
    note_ptr->ResetDst();
  }
  return note_ptr;
}

void AppProc::TauDstNote::Print(int cur_wid, pid_t pid, int tid,
                                std::ostream &out) {
  if (wid_pct.empty()) return;
  out << "[DstNote-" << cur_wid << "] pid:" << pid << " tid:" << tid
      << " one_dst?:" << only_one_dst << "--";
  for (auto [wid, pct] : wid_pct) {
    out << " wid:" << (int)wid << " pct:" << pct << " cnt:" << wid_count[wid];
  }
  out << std::endl;
}

#endif  // CFS_INCLUDE_FSPROC_APP_H_
