/*
 * Inline journal jsuper declaraitons/definitions included by FsProc_Journal.cc
 * and fsProcOfflineCheckpointer. We are intentionaly not placing it in a
 * separate cc file and using it as included header till pgo is enabled.
 *
 * Make sure this is imported *after* FsProc_Journal.h and not before.
 * Clang-format sometimes reorders imports and we need this particular ordering
 * here. (I know I know, it's ugly - but its only till we try out pgo.
 */

#ifndef __CFS_JSUPER_H
#define __CFS_JSUPER_H

#include "cfs_feature_macros.h"
#include "journalparams.h"

#if CFS_JOURNAL(NO_JOURNAL)
#error "Incorrectly included"
#endif

#if CFS_JOURNAL(LOCAL_JOURNAL)
#define JSUPER_LOCK_RAII() \
  do {                     \
  } while (0)
#endif

#if CFS_JOURNAL(GLOBAL_JOURNAL)
#include <mutex>
#include "util.h"
#define JSUPER_LOCK_RAII() const std::lock_guard<UtilSpinLock> lock(mutex_)
// TODO consider finer grain locking with std::unique_lock
// TODO consider basic spinlock http://anki3d.org/spinlock/

#endif

// When starting up for the first time, the journal
// super block (per worker) is read.
// If we detect that everything was written cleanly,
// no journal blocks will need to be read and we can
// start using the journal immediately.
//
// TODO: main FS superblock should contain info about
// journal super, so we know that everything was written
// properly.
//
// If not, we will have to scan journal from its last
// known entry to initialize correctly.

struct alignas(64) JSuperOnDisk {
  uint64_t jmagic;
  uint64_t jsuper_blockno;
  uint64_t jstart_blockno;
  uint64_t jend_blockno;
  uint64_t capacity;  // jend_blockno - jstart_blockno + 1
  // NOTE: padding to make sure constants not on same cacheline
  // TODO evaluate performance without this padding?
  char pad[24];

  uint64_t n_used;
  uint64_t head;
  uint64_t tail;
  uint64_t last_chkpt_ts;  // compared with future entries to see if stale
};

class JSuper {
 public:
  static constexpr uint64_t kMaxUnaccountedBlocks =
      MAX_UNACCOUNTED_JOURNAL_BYTES / BSIZE;
  // We block transactions if it reaches kMax, but we write jsuper at kSoftmax
  // so that we don't have to block that often.
  static constexpr uint64_t kSoftMaxUnaccountedBlocks =
      0.8 * kMaxUnaccountedBlocks;

  JSuper(){};
  JSuper(const struct JSuperOnDisk *j) { SetDiskRepr(j); }

  // The following can be accessed without thread safety because they do not
  // access modifiable data.
  uint64_t Capacity() const { return djsuper_.capacity; }
  // Converts slot idx to block number -
  uint64_t IdxToBlockNo(uint64_t idx) const;
  // Checks whether a given extent is not contiguous and needs to loop around
  bool NoLoopArounds(uint64_t start_idx, uint64_t nblocks) const;

  // Unless marked "Unsafe", all these functions must be concurrent safe in
  // global journal mode.

  struct JSuperOnDisk GetDiskRepr() const;
  void CopyToBuf(struct JSuperOnDisk *buf) const;
  bool Empty() const;
  uint64_t Size() const;
  uint64_t GetLastCheckpoint() const;
  void SetDiskRepr(const struct JSuperOnDisk *j);
  void SetLastCheckpoint(uint64_t ts);

  // Ask to write the jsuper block. If there is no write in process, copies the
  // relevant info for the operation.
  bool TrySetWriteInProgress(struct JSuperOnDisk &j,
                             uint64_t &old_nblocks_unaccounted);
  // Modifies state to decrement the unaccounted blocks as it has just been
  // written to disk.
  void OnWriteComplete(uint64_t old_nblocks_unaccounted);
  // Checks if jsuper needs to be written.
  bool ShouldWriteJSuper() const;

  // When acquiring a slot to write to the journal, we return a little bit more
  // information for the caller.
  struct Slot {
    // Specifies the start index that the caller should use.
    // Use IdxToBlockNo to convert the index to actual block number.
    uint64_t start_idx;
    // TODO: make these flags in an integer/enum?
    // There is no space left in the journal and the fsync must fail.
    bool no_space;
    // The soft watermark has been reached. Caller *must* attempt to write
    // jsuper.
    bool write_jsuper;
    // The hard watermark has been reached.
    bool retry_after_jsuper_written;
    // A slot may be used if either no_space or retry_after_jsuper_written are
    // false. If either is true, we either fail or retry the request.
  };

  // Acquire a given number of slots to write to (common path)
  struct JSuper::Slot AcquireNextNSlots(uint64_t n);
  // Release the first N slots (checkpointing)
  void ReleaseFirstNSlots(uint64_t n);

 private:
  struct JSuperOnDisk djsuper_ {};
  // TODO align and reorder members
#if CFS_JOURNAL(GLOBAL_JOURNAL)
  // NOTE: compared to std::mutex UtilSpinLock uses std::atomic_flag which is 1
  // byte. Rearranging the members of djsuper_ and the private members below may
  // improve performance.
  mutable UtilSpinLock mutex_;
#endif
  // When writing to the journal, the super block may not be updated to reflect
  // that entry. nblocks_unaccounted captures the number of bytes written to the
  // journal that is not captured in the superblock yet. Periodically, after a
  // particular watermark, the superblock will be written and this field will be
  // reset to 0. This allows us to only look at a maximum limit of blocks during
  // offline checkpointing instead of reading the entire journal.
  uint64_t nblocks_unaccounted_{0};
  bool write_in_progress_{false};

  // Checks if jsuper should be written
  bool UnsafeShouldWriteJSuper() const;
  // Checks if we have enough space to write n blocks to the journal
  bool UnsafeCanAllocateNSlots(uint64_t n) const;
  // Modifies jsuper to reflect n new blocks in the journal (fsync)
  void UnsafeNItemsEnqueued(uint64_t n);
  // Modifies jsuper to reflect n blocks removed from journal (checkpoint)
  void UnsafeNItemsDequeued(uint64_t n);
  // Checks if journal should stop writing
  bool UnsafeShouldWaitTillJSuperWritten() const;
  // To avoid journal entries that wrap, we always ensure there is a contigous
  // portion of MAX_JOURNAL_ENTRY_SIZE. If there is going to be a wrap, we fill
  // the remaining part of the journal so next free slot is at the beginning. We
  // waste at max MAX_JOURNAL_ENTRY_SIZE - 1 bytes
  uint64_t UnsafeEnsureNextContiguousRegion();
};

inline struct JSuperOnDisk JSuper::GetDiskRepr() const {
  JSUPER_LOCK_RAII();
  return djsuper_;
}

inline void JSuper::CopyToBuf(struct JSuperOnDisk *buf) const {
  JSUPER_LOCK_RAII();
  *buf = djsuper_;
}

inline bool JSuper::Empty() const {
  JSUPER_LOCK_RAII();
  return djsuper_.n_used == 0;
}

inline uint64_t JSuper::Size() const {
  JSUPER_LOCK_RAII();
  return djsuper_.n_used;
}

inline uint64_t JSuper::GetLastCheckpoint() const {
  JSUPER_LOCK_RAII();
  return djsuper_.last_chkpt_ts;
}

inline void JSuper::SetDiskRepr(const struct JSuperOnDisk *j) {
  JSUPER_LOCK_RAII();
  djsuper_ = *j;
}

inline bool JSuper::ShouldWriteJSuper() const {
  JSUPER_LOCK_RAII();
  return UnsafeShouldWriteJSuper();
}

inline struct JSuper::Slot JSuper::AcquireNextNSlots(uint64_t n) {
  JSUPER_LOCK_RAII();
  struct JSuper::Slot slot;

  slot.retry_after_jsuper_written = UnsafeShouldWaitTillJSuperWritten();
  slot.no_space = (!UnsafeCanAllocateNSlots(n));

  bool abort = slot.no_space || slot.retry_after_jsuper_written;
  if (!abort) [[likely]] {
      slot.start_idx = djsuper_.head;
      UnsafeNItemsEnqueued(n);
      nblocks_unaccounted_ += n;
      nblocks_unaccounted_ += UnsafeEnsureNextContiguousRegion();
      slot.write_jsuper = UnsafeShouldWriteJSuper();
    }
  return slot;
}

inline void JSuper::ReleaseFirstNSlots(uint64_t n) {
  JSUPER_LOCK_RAII();
  UnsafeNItemsDequeued(n);
  // NOTE: We always add and never subtract from nblocks_unaccounted_ as it is a
  // measure of staleness to decide whether to write to disk or not.
  nblocks_unaccounted_ += n;
}

inline bool JSuper::TrySetWriteInProgress(struct JSuperOnDisk &j,
                                          uint64_t &old_nblocks_unaccounted) {
  JSUPER_LOCK_RAII();
  if (write_in_progress_) return false;

  write_in_progress_ = true;
  j = djsuper_;
  old_nblocks_unaccounted = nblocks_unaccounted_;
  return write_in_progress_;
}

inline void JSuper::OnWriteComplete(uint64_t old_nblocks_unaccounted) {
  JSUPER_LOCK_RAII();
  assert(write_in_progress_);
  assert(old_nblocks_unaccounted <= nblocks_unaccounted_);

  write_in_progress_ = false;
  nblocks_unaccounted_ -= old_nblocks_unaccounted;
}

inline void JSuper::SetLastCheckpoint(uint64_t ts) {
  JSUPER_LOCK_RAII();
  // NOTE: caller *must* call WriteJSuper() after a checkpoint
  djsuper_.last_chkpt_ts = ts;

  // NOTE: when setting a checkpoint we always want to force a jsuper write.
  // However, just calling WriteJSuper() may not work as a write may already be
  // in progress. Instead, we artificially add many nblocks_unaccounted_ so that
  // even if there is a write in progress, it will trigger another write.
  nblocks_unaccounted_ += kSoftMaxUnaccountedBlocks;
}

inline uint64_t JSuper::IdxToBlockNo(uint64_t idx) const {
  return djsuper_.jstart_blockno + (idx % djsuper_.capacity);
}

inline bool JSuper::NoLoopArounds(uint64_t start_idx, uint64_t nblocks) const {
  uint64_t start_block = IdxToBlockNo(start_idx);
  uint64_t end_idx = start_idx + nblocks - 1;
  uint64_t end_block = IdxToBlockNo(end_idx);
  return start_block < end_block;
}

inline bool JSuper::UnsafeCanAllocateNSlots(uint64_t n) const {
  return (djsuper_.n_used + n) <= djsuper_.capacity;
}

inline bool JSuper::UnsafeShouldWriteJSuper() const {
  bool above_watermark = nblocks_unaccounted_ > kSoftMaxUnaccountedBlocks;
  return above_watermark && !write_in_progress_;
}

inline bool JSuper::UnsafeShouldWaitTillJSuperWritten() const {
  return nblocks_unaccounted_ >= kMaxUnaccountedBlocks;
}

inline uint64_t JSuper::UnsafeEnsureNextContiguousRegion() {
  constexpr uint64_t max_blocks = JournalManager::kMaxJournalEntrySize / BSIZE;
  uint64_t nblocks_till_end =
      djsuper_.jend_blockno - IdxToBlockNo(djsuper_.head) + 1;
  if (nblocks_till_end > max_blocks) [[likely]] return 0;

  // rare path - happens only when the journal wraps around, but we can fill it
  // up only if there is space to do so.
  if (UnsafeCanAllocateNSlots(nblocks_till_end)) {
    UnsafeNItemsEnqueued(nblocks_till_end);
    return nblocks_till_end;
  }

  return 0;
}

inline void JSuper::UnsafeNItemsEnqueued(uint64_t n) {
  // NOTE: the assert shouldn't really be required since we will always call
  // UnsafeCanAllocateNSlots before UnsafeNItemsEnqueued.
  assert((djsuper_.n_used + n) <= djsuper_.capacity);
  // NOTE: (head + n) will not overflow as our journal will never be so large
  djsuper_.head = (djsuper_.head + n) % djsuper_.capacity;
  djsuper_.n_used += n;
}

inline void JSuper::UnsafeNItemsDequeued(uint64_t n) {
  assert(n <= djsuper_.n_used);
  // NOTE: (tail + n) will not overflow as our journal will never be so large
  djsuper_.tail = (djsuper_.tail + n) % djsuper_.capacity;
  djsuper_.n_used -= n;
}

#endif  // __CFS_JSUPER_H
