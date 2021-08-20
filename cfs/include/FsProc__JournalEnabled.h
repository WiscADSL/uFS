/*
 * NOTE: This file should only be imported by FsProc_Journal.h
 */

#include <cstdint>
#include <queue>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "BlkDevSpdk.h"
#include "FsProc_FsInternal.h"
#include "journalparams.h"

// TODO: add magic headers for all metadata so we
// know whether a block is a metadata block or not.
#define JSUPER_MAGIC 0xBEEFBEEFBEEFBEEF
#define JBODY_MAGIC 0xBEEFBEEFBEEFBBBB
#define JCOMMIT_MAGIC 0xBEEFBEEFBEEFCCCC

#if CFS_JOURNAL(PERF_METRICS)

#include "perfutil/Cycles.h"
#define SetJournalPerfTS(x) x = PerfUtils::Cycles::rdtscp()
#define RecordJournalPerfMetrics() record_perf_metrics()

#else

#define SetJournalPerfTS(x) \
  do {                      \
  } while (0)

#define RecordJournalPerfMetrics() \
  do {                             \
  } while (0)

#endif  // CFS_JOURNAL_PERF_METRICS

// TODO consider using std::function?
typedef void (*JournalCallbackFn)(void *arg, bool success);

//
class JSuper;
class InMemInode;
class FsProcWorker;
class JournalEntry;
class FileMng;
//

// TODO (jing) when inode i migrates from WorkerX to WorkerY, add i to
// inodesToCheckpoint on WorkerY if i was fsync'd previously.
class CheckpointInput {
 public:
  // The number of used blocks that are part of this checkpoint
  uint64_t n_used;
  std::unordered_map<cfs_ino_t, void *> inodesToCheckpoint;
  std::unordered_map<uint64_t, void *> bitmapsToCheckpoint;
  std::unordered_map<uint64_t, void *> inodeBitmapsToCheckpoint;
  void *inodeDevMem;
  void *bitmapDevMem;
};

struct PrepareForCheckpointingCtx {
  CheckpointInput **ci;
  volatile bool *completed;
};

struct ProposeForCheckpoingCtx {
  int propose_widIdx;
};

struct AssignCheckpointWorkerCtx {
  // TODO (jingliu) what we need to put here
};

struct CheckpointingCompleteCtx {
  CheckpointInput *ci;
  bool success;
};

// TODO preallocate a sizeable portion for vectors?
struct BitmapChangeOps {
  std::vector<uint64_t> blocksToSet;
  std::vector<uint64_t> blocksToClear;
  std::unordered_map<cfs_ino_t, bool> inodeBitmapChanges;
};

class InodeLogEntry {
 public:
  // deserializes a buffer and populates the InodeLogEntry
  InodeLogEntry(uint8_t *buf, size_t buf_size);
  // initializes an empty inode entry with inode & syncID
  InodeLogEntry(uint64_t inode, uint64_t syncID, InMemInode *minode);

  size_t calcSize(void);
  size_t serialize(uint8_t *buf);
  void display(void);
  // applies changes in the log entry to dst and populates blocks_add_or_del to
  // indicate whether bit in bitmap for block needs to be set/cleared by
  // comparing against the dst inode.
  void applyChangesTo(cfs_dinode *dst,
                      std::unordered_map<uint64_t, bool> &blocks_add_or_del);
  void on_successful_journal_write(CheckpointInput *ci, JournalEntry *je);

  void set_mode(uint32_t mode);
  void set_uid(uint32_t uid);
  void set_gid(uint32_t gid);
  void set_block_count(uint32_t size);
  void set_bitmap_op(uint32_t bitval);
  void set_atime(struct timeval atime);
  void set_mtime(struct timeval mtime);
  void set_ctime(struct timeval ctime);
  void set_size(uint64_t size);
  void set_dentry_count(uint16_t dentry_count);
  void set_nlink(uint8_t nlink);
  void update_extent(struct cfs_extent *extent, bool add_or_del,
                     bool bmap_modified);
  void add_dependency(uint64_t inode, uint64_t syncID);
  std::string as_json_str();
  bool empty();
  void get_bitmap_for_dealloc_blocks_for_wid(
      int wid, std::unordered_set<cfs_bno_t> &bmap_blocks);
  InMemInode *get_minode();

 private:
  // must have fields
  uint64_t inode;
  uint64_t syncID;

  // the presence of optional fields are recorded in the optfield_bitarr
  uint32_t optfield_bitarr;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
  uint32_t block_count;
  // bitmap_op = 0/1 when we deallocate/allocate inode
  uint32_t bitmap_op;  // FIXME use uint8_t to save space
  uint16_t dentry_count;
  uint8_t nlink;
  uint64_t size;
  struct timeval atime;
  struct timeval mtime;
  struct timeval ctime;

  // extent_seq_no is used to keep track of ordering between additions and
  // deletion of extents. It is incremented anytime we add to ext_add or
  // ext_del.
  uint32_t extent_seq_no;
  struct ExtMapVal {
    uint32_t seq_no;
    uint32_t i_block_offset;
    uint32_t num_blocks;
    bool bmap_modified;
    ExtMapVal(uint32_t seq_no, uint32_t i_block_offset, uint32_t num_blocks,
              bool bmap_modified)
        : seq_no(seq_no), i_block_offset(i_block_offset),
          num_blocks(num_blocks), bmap_modified(bmap_modified) {}
    ExtMapVal() = default;
    ExtMapVal(const ExtMapVal &) = default;
    ExtMapVal(ExtMapVal &&) = default;
    ExtMapVal &operator=(const ExtMapVal &) = default;
    ExtMapVal &operator=(ExtMapVal &&) = default;
  };
  std::unordered_map<uint64_t, struct ExtMapVal> ext_add;
  std::unordered_map<uint64_t, struct ExtMapVal> ext_del;
  std::unordered_map<uint64_t, uint64_t> depends_on;

  InMemInode *minode;

  friend class FsProcOfflineCheckpointer;
  friend class JournalIterator;
};

enum class JournalEntryState {
  PREPARE,
  QUEUED,
  WRITE_BODY,
  FLUSH,
  WRITE_COMMIT,
  SUCCESS,
  FAILURE,
  /* for deserializing header, and then body*/
  DESERIALIZED_ERROR_MAGIC,
  DESERIALIZED_ERROR_ENTRY_SIZE,
  DESERIALIZED_PARTIAL,
  DESERIALIZED,
};

class JournalManager;

struct PinnedMemPtr {
  char *jbody;
  char *jcommit;
};

class JournalEntry {
 public:
  // TODO: The most common case is a single inode log entry.
  // But we also have to accomodate multiple inode log entries when
  // dealing with directories. Find a clean way to make the common
  // case much faster (incase vector access & allocation is slow).
  JournalEntry();
  JournalEntry(uint8_t *buf, size_t buf_size);
  static void ParseJournalEntryHeader(uint8_t *buf, size_t buf_size,
                                      JournalEntry &je);
  void deserializeBody(uint8_t *buf, size_t buf_size);
  bool isValidCommitBlock(uint8_t *commit_buf, size_t buf_size);
  void logCommitBlockMismatches(uint8_t *commit_buf, size_t buf_size);
  ~JournalEntry();

  JournalEntryState getState(void);
  JournalManager *getManager(void);
  void addInodeLogEntry(InodeLogEntry *ile);
  size_t calcBodySize(void);
  size_t serializeBody(uint8_t *buf);
  void serializeCommit(uint8_t *buf);  // fixed size commit message
  uint64_t GetSerializedTime() { return ustime_serialized; }
  std::vector<uint64_t> blocks_for_bitmap_set;
  std::vector<uint64_t> blocks_for_bitmap_clear;
  std::unordered_map<cfs_ino_t, bool> inode_alloc_dealloc;
  std::string as_json_str();

 private:
  JournalCallbackFn cb;
  void *cb_arg;
  JournalEntryState state;
  std::vector<InodeLogEntry *> ile_vec;
  uint64_t start_block{0};
  // The number of blocks occupied by journal entry including commit block
  // (which is the last block) nblocks is always >= 2 as 1 block for header +
  // body, 1 for commit
  uint64_t nblocks{0};
  uint64_t ustime_serialized{0};
  JournalManager *mgr{nullptr};
  PinnedMemPtr *pinned_mem_{nullptr};
  friend class JournalManager;
  friend class FsProcOfflineCheckpointer;
  friend class JournalIterator;
};

// Each worker has it's own local journal manager to write
// to a dedicated journal region.
class JournalManager {
 public:
  static constexpr float kJournalCheckpointRatio = 0.9;
  static constexpr uint64_t kJournalCheckpointNLeftBlocks = 1000;
  static constexpr bool kEnableNvmeWriteStats = false;
  static constexpr uint64_t kMaxJournalEntrySize = MAX_JOURNAL_ENTRY_SIZE;
  // JournalEntry = Body + Commit where Commit is right now 1 block
  static constexpr uint64_t kMaxJournalBodySize = kMaxJournalEntrySize - BSIZE;

  JournalManager(JournalManager *primary_jmgr, uint64_t jsuper_blockno,
                 CurBlkDev *dev);
  ~JournalManager();

  void submitJournalEntry(JournalEntry *je, JournalCallbackFn cb, void *cb_arg);
  void processJournalEntry(JournalEntry *je);
  static void writeComplete(void *arg, const struct spdk_nvme_cpl *completion);
  static void checkpointWriteComplete(void *arg,
                                      const struct spdk_nvme_cpl *completion);
  // Starts a new transaction if queue not empty and nothing in progress.
  void tryNextTransaction();

  // Necessary state to track for checkpointing
  char *GetStableDataBitmap(uint64_t bno) const;
  char *GetStableInodeBitmap(uint64_t bno) const;
  void CopyBufToStableDataBitmap(uint64_t bno, const char *buf);
  void CopyBufToStableInodeBitmap(uint64_t bno, const char *buf);

  CheckpointInput *checkpointInput;
  void prepareForCheckpointing(CheckpointInput **dst, FileMng *mgr);
  void onCheckpointSuccess(CheckpointInput *src, FileMng *mgr);
  void onCheckpointFailure(CheckpointInput *src);
  void checkpointAllJournals(FileMng *mng, std::vector<FsProcWorker *> &workers,
                             size_t n_workers, size_t this_worker_idx);

  bool isCheckpointInProgress();
#if CFS_JOURNAL(LOCAL_JOURNAL)
  void registerBitmapsWithDeallocations(
      const std::unordered_set<cfs_bno_t> &bmaps);
#endif

#if CFS_JOURNAL(PERF_METRICS)
  void dumpJournalPerfMetrics(const std::string &filename);
#endif

  uint64_t QueryNumNvmeWriteDone() { return n_num_nvme_write_done_; }
  void ResetNumNvmeWriteDone() { n_num_nvme_write_done_ = 0; }

 private:
  JSuper *jsuper;
  char *pinnedMemJournalSuper;

  // TODO: create abstractions in CurBlkDev so that dev_ns and dev_qpair aren't
  // needed. For now, we use dev_ns and dev_qpair to issue spdk calls directly.
  CurBlkDev *dev;
  struct spdk_nvme_ns *dev_ns;
  struct spdk_nvme_qpair *dev_qpair;
  bool flush_supported = false;
  uint32_t fua_flag = 0;
  bool checkpointInProgress = false;
#if CFS_JOURNAL(LOCAL_JOURNAL)
  std::unordered_set<cfs_bno_t> bmapsToLockAfterCheckpointCompletes;
#endif
  static constexpr int kMaxInFlight_ = MAX_INFLIGHT_JOURNAL_TRANSACTIONS;
  // We write the entry, flush, commit. And only then service another.
  // The journal can only operate on kMaxInFlight_ transactions at a time.
  std::queue<JournalEntry *> je_queue;
  std::queue<PinnedMemPtr *> pinned_mem_queue_;

  // Checkpointing state variables
  uint64_t n_checkpoint_requests_issued;
  volatile uint64_t n_checkpoint_requests_completed;
  volatile uint64_t n_checkpoint_requests_failed;
  // To avoid recomputing state of metadata, every inode has it's own stable
  // inodeData field (jinodeData). However, we also need stable structures for
  // data block and inode bitmaps to avoid recomputing the bitmaps.
  std::unordered_map<uint64_t, char *> stableDataBitmaps_;
  std::unordered_map<uint64_t, char *> stableInodeBitmaps_;

  uint64_t n_num_nvme_write_done_ = 0;

  void AllocPinnedMem();
  // Every journal entry has to acquire some pinned memory from
  // pinned_mem_queue_ using these helper functions.
  void AcquirePinnedMem(JournalEntry *je);
  void ReleasePinnedMem(JournalEntry *je);

  // Attempts to write jsuper if a write isn't already in progress
  void WriteJSuper();

  void blockingReadJournalSuper(uint64_t jsuper_blockno);
  void populateCheckpointInput(FileMng *mgr);
  void cleanupCheckpointInput(CheckpointInput *ci);
  void waitForCheckpointWriteCompletions();
  void notifyAllProcsOnCheckpointComplete(
      FileMng *mng, std::vector<FsProcWorker *> &workers, size_t n_workers,
      size_t this_worker_idx, CheckpointInput **per_worker_ci, bool success);
#if CFS_JOURNAL(PERF_METRICS)
  // When jmgr is initialized, we set ts_ref to an rdtscp counter.
  // All other timestamps are relative to this and in nanoseconds.
  // Calculations done in dumpJournalPerfMetrics when fsMain shuts down.
  uint64_t ts_ref;

  struct journal_metrics {
    // Timestamp when JournalEntry is submitted
    uint64_t ts_queued_start;
    // Timestamp when JournalEntry starts to write body
    uint64_t ts_write_body_start;
    // Timestamp when JournalEntry starts to flush
    uint64_t ts_flush_start;
    // Timestamp when JournalEntry starts to write commit blcok
    uint64_t ts_commit_start;
    // Timestamp when JournalEntry calls the callback function
    uint64_t ts_callback_start;
    // Timestamp when JournalEntry callback returns
    uint64_t ts_callback_stop;
  };

  struct journal_metrics cur_metric;
  size_t metrics_ctr{0};
  // NOTE if it goes over 250k, we will overwrite the list. When dumping metrics
  // at the end, we detect and print a warning if it happened.
  // TODO: make this configurable atleast at compile time.
  size_t metrics_max_capacity{250000};
  std::vector<struct journal_metrics> metrics_list{metrics_max_capacity};

  inline void record_perf_metrics() {
    // NOTE: if we go over max capacity, it will overwrite the list.
    metrics_list[metrics_ctr % metrics_max_capacity] = cur_metric;
    metrics_ctr++;
  }
#endif  // CFS_JOURNAL_PERF_METRICS
};

// inline Definitions for InodeLogEntry

#define mode_IDX 0x1
#define uid_IDX 0x2
#define gid_IDX 0x4
#define atime_IDX 0x8
#define mtime_IDX 0x10
#define ctime_IDX 0x20
#define size_IDX 0x40
#define block_count_IDX 0x80
#define bitmap_op_IDX 0x100
#define dentry_count_IDX 0x200
#define nlink_IDX 0x400

#define DEFINE_SIMPLE_SETTER(field, dtype)          \
  inline void InodeLogEntry::set_##field(dtype v) { \
    this->field = v;                                \
    optfield_bitarr |= field##_IDX;                 \
  }

DEFINE_SIMPLE_SETTER(mode, uint32_t)
DEFINE_SIMPLE_SETTER(uid, uint32_t)
DEFINE_SIMPLE_SETTER(gid, uint32_t)
DEFINE_SIMPLE_SETTER(block_count, uint32_t)
DEFINE_SIMPLE_SETTER(bitmap_op, uint32_t)
DEFINE_SIMPLE_SETTER(atime, struct timeval)
DEFINE_SIMPLE_SETTER(mtime, struct timeval)
DEFINE_SIMPLE_SETTER(ctime, struct timeval)
DEFINE_SIMPLE_SETTER(size, uint64_t)
DEFINE_SIMPLE_SETTER(dentry_count, uint16_t)
DEFINE_SIMPLE_SETTER(nlink, uint8_t)

#undef DEFINE_SIMPLE_SETTER

inline bool InodeLogEntry::empty() {
  bool maps_empty = ext_add.empty() && ext_del.empty() && depends_on.empty();
  bool fields_empty = (optfield_bitarr == 0);
  return fields_empty && maps_empty;
}

inline void InodeLogEntry::update_extent(struct cfs_extent *extent,
                                         bool add_or_del, bool bmap_modified) {
  uint32_t ts = extent_seq_no;
  // NOTE: extent->block_no is block address relative to starting data block
  // We want to store the physical block address
  // TODO: make get_data_start_block a constexpr function?
  uint64_t block_no = conv_lba_to_pba(extent->block_no);
  extent_seq_no = extent_seq_no + 1;
  using ExtMap = std::unordered_map<uint64_t, struct ExtMapVal>;
  ExtMap *ext_map = (add_or_del) ? &ext_add : &ext_del;
  auto [it, inserted] = ext_map->try_emplace(
      block_no, ts, extent->i_block_offset, extent->num_blocks, bmap_modified);
  if (inserted) return;
  // Something already exists - we need to overwrite. However, if bmap_modified
  // was originally true, we must not set it to false.
  struct ExtMapVal &val = it->second;
  val.seq_no = ts;
  val.i_block_offset = extent->i_block_offset;
  val.num_blocks = extent->num_blocks;
  val.bmap_modified |= bmap_modified;
}

inline void InodeLogEntry::add_dependency(uint64_t inode, uint64_t syncID) {
  // TODO how to wait on same inode but different syncID?
  // Try to prove that we won't ever have a deadlock while waiting.
  // Enumerate cases for truncate and directories.
  // Will we ever have a case where we wait on same inode with multiple
  // syncIDs? For now, assuming that isn't the case.
  depends_on[inode] = syncID;
}

inline size_t InodeLogEntry::calcSize(void) {
  // (entrySize, inode, syncID) = 8 * 3
  // (optfield, mode, uid, gid, block_count, bitmap_op) = 4 * 6
  // (atime, mtime, ctime) = sizeof(struct timeval) * 3
  // (dentry_count) = 2 * 1
  // (nlink) = 1
  // (size, n_ext_add, n_ext_del, n_depends_on) = 8 * 4
  size_t dsize =
      (8 * 3) + (4 * 6) + (sizeof(struct timeval) * 3) + 2 + 1 + (8 * 4);

  // each element in ext_{add,del} takes 8 bytes for key, ExtMapVal for val
  // key: uint64_t, value: struct ExtMapVal
  constexpr size_t entry_size = sizeof(uint64_t) + sizeof(struct ExtMapVal);
  dsize += (ext_add.size() * entry_size);
  dsize += (ext_del.size() * entry_size);
  // each element in a map takes up 16 bytes (8 for key, 8 for value)
  dsize += (depends_on.size() * 16);
  return dsize;
}

inline void InodeLogEntry::get_bitmap_for_dealloc_blocks_for_wid(
    int wid, std::unordered_set<cfs_bno_t> &bmap_blocks) {
  if (ext_del.empty()) return;

  for (const auto &[block_no, val] : ext_del) {
    if (!val.bmap_modified) continue;
    cfs_bno_t bmap_disk_bno = get_bmap_block_for_pba(block_no);
    if (getWidForBitmapBlock(bmap_disk_bno) == wid) {
      bmap_blocks.insert(bmap_disk_bno);
    }
  }
}

inline InMemInode *InodeLogEntry::get_minode() { return minode; }

// inline definitions for JournalEntry

inline JournalEntryState JournalEntry::getState() { return state; }

inline JournalManager *JournalEntry::getManager(void) { return mgr; }

inline void JournalEntry::addInodeLogEntry(InodeLogEntry *ile) {
  ile_vec.push_back(ile);
}

inline size_t JournalEntry::calcBodySize(void) {
  // 8 bytes for magic
  // 8 bytes for a timestamp
  // 8 bytes for size of journal entry
  // 8 bytes for number of InodeLogEntry items
  // 8 bytes for start block
  // 8 bytes for nblocks
  // followed by the entries themselves
  size_t bytes = 48;
  for (InodeLogEntry *ile : ile_vec) {
    bytes += ile->calcSize();
  }
  return bytes;
}

inline void JournalEntry::serializeCommit(uint8_t *buf) {
  uint64_t *uint64_buf = (uint64_t *)buf;
  uint64_buf[0] = JCOMMIT_MAGIC;
  uint64_buf[1] = ustime_serialized;
  uint64_buf[2] = ile_vec.size();
  uint64_buf[3] = start_block;
  uint64_buf[4] = nblocks;
}

// inline definitions for JournalManager

inline char *JournalManager::GetStableDataBitmap(uint64_t bno) const {
  assert(isBitmapBlock(bno));
  auto search = stableDataBitmaps_.find(bno);
  if (search != stableDataBitmaps_.end()) return search->second;
  return nullptr;
}

inline void JournalManager::CopyBufToStableDataBitmap(uint64_t bno,
                                                      const char *buf) {
  assert(isBitmapBlock(bno));
  assert(buf != nullptr);

  // we only update stable for the first time when reading bitmap. After that,
  // modifications are done when inodes are fsynced - never through this
  // function.
  if (GetStableDataBitmap(bno) != nullptr) return;

  auto ptr = new char[BSIZE];
  memcpy(ptr, buf, BSIZE);
  stableDataBitmaps_[bno] = ptr;
}

inline char *JournalManager::GetStableInodeBitmap(uint64_t bno) const {
  assert(isInodeBitmap(bno));
  auto search = stableInodeBitmaps_.find(bno);
  if (search != stableInodeBitmaps_.end()) return search->second;
  return nullptr;
}

inline void JournalManager::CopyBufToStableInodeBitmap(uint64_t bno,
                                                       const char *buf) {
  assert(isInodeBitmap(bno));
  assert(buf != nullptr);

  // we only update stable for the first time when reading bitmap. After that,
  // modifications are done when inodes are fsynced - never through this
  // function.
  if (GetStableInodeBitmap(bno) != nullptr) return;

  auto ptr = new char[BSIZE];
  memcpy(ptr, buf, BSIZE);
  stableInodeBitmaps_[bno] = ptr;
}

inline bool JournalManager::isCheckpointInProgress() {
  return checkpointInProgress;
}

#if CFS_JOURNAL(LOCAL_JOURNAL)
inline void JournalManager::registerBitmapsWithDeallocations(
    const std::unordered_set<cfs_bno_t> &bmaps) {
  if (!checkpointInProgress) return;
  bmapsToLockAfterCheckpointCompletes.insert(bmaps.cbegin(), bmaps.cend());
}
#endif  // CFS_JOURNAL(LOCAL_JOURNAL)
