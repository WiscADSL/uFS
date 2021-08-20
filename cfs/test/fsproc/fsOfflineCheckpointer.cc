/*
 * Standalone utility module to checkpoint whatever is in the journal.
 * This simplifies FSP to always just start-up and read metadata rather
 * than replaying the journal.
 * Eventually, we can make this part of the FSP startup process.
 * For now, it is simpler to perform this recovery outside as we need
 * to examine all journals and not just for the active threads on startup.
 */

#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <regex>
#include <sstream>
#include <stdexcept>

#include "cxxopts.hpp"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdlog/spdlog.h"

#include "BlkDevSpdk.h"
#include "FsProc_FsInternal.h"
#include "FsProc_Journal.h"
#include "param.h"
#include "util.h"

// NOTE: Must be imported after FsProc_Journal.h
#include "FsProc_JSuper.h"

/* spdk startup functions */

static struct ctrlr_entry *g_controllers = NULL;
static struct ns_entry *g_namespaces = NULL;
static struct spdk_nvme_qpair *g_qpair = NULL;

// util func
static int SafeCreateExportFd(std::filesystem::path p);
static void SafeReadFile(const char *fname, void *buf, size_t sz, bool sz_chk);
static uint64_t PreserveTSAndSerialize(JournalEntry &je, uint8_t *buf);

static void register_ns(struct spdk_nvme_ctrlr *ctrlr,
                        struct spdk_nvme_ns *ns) {
  if (!spdk_nvme_ns_is_active(ns)) {
    SPDLOG_WARN("nvme ns is not active");
    return;
  }

  auto entry = (struct ns_entry *)malloc(sizeof(struct ns_entry));
  if (entry == NULL) {
    SPDLOG_ERROR("Failed to allocate ns_entry");
    exit(1);
  }

  entry->ctrlr = ctrlr;
  entry->ns = ns;
  entry->next = g_namespaces;
  g_namespaces = entry;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts) {
  auto entry = (struct ctrlr_entry *)malloc(sizeof(struct ctrlr_entry));
  if (entry == NULL) {
    SPDLOG_ERROR("Failed to allocate ctrlr_entry");
    exit(1);
  }

  auto *cdata = spdk_nvme_ctrlr_get_data(ctrlr);
  snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn,
           cdata->sn);
  entry->ctrlr = ctrlr;
  entry->next = g_controllers;
  g_controllers = entry;

  int num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
  SPDLOG_INFO("using controller {} with {} namespaces", entry->name, num_ns);
  for (int nsid = 1; nsid <= num_ns; nsid++) {
    auto ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL) continue;
    register_ns(ctrlr, ns);
  }
}

static void cleanup(void) {
  struct ns_entry *ns_entry = g_namespaces;
  struct ctrlr_entry *ctrlr_entry = g_controllers;

  while (ns_entry != NULL) {
    struct ns_entry *next = ns_entry->next;
    free(ns_entry);
    ns_entry = next;
  }

  while (ctrlr_entry != NULL) {
    struct ctrlr_entry *next = ctrlr_entry->next;
    spdk_nvme_detach(ctrlr_entry->ctrlr);
    free(ctrlr_entry);
    ctrlr_entry = next;
  }

  // TODO any other spdk functions to call for cleanup?
}

static int initSpdkEnv() {
  struct spdk_env_opts opts;

  spdk_env_opts_init(&opts);
  opts.name = "FsProcOfflineCheckpointer";
  opts.shm_id = 9;  // TODO what is this shm_id for?

  if (spdk_env_init(&opts) < 0) {
    SPDLOG_ERROR("failed to initialize spdk env");
    return 1;
  }

  auto probe_cb = [](auto ctx, auto trid, auto opts) { return true; };
  int rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
  if (rc != 0) {
    SPDLOG_ERROR("spdk_nvme_probe() failed, rc={}", rc);
    return 1;
  }

  // We choose the first namespace
  auto ns_entry = g_namespaces;
  if (ns_entry == NULL) {
    SPDLOG_ERROR("No namespace found");
    return 1;
  }

  g_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
  if (g_qpair == NULL) {
    SPDLOG_ERROR("failed to alloc io qpair");
    return 1;
  }

  SPDLOG_DEBUG("SPDK environment initialized successfully");
  return 0;
}

/* class declarations */
class SpdkThrottler {
  using CallbackFn =
      std::add_pointer<void(void *, const struct spdk_nvme_cpl *)>::type;
  class WrappedArg {
   public:
    WrappedArg(SpdkThrottler *throttler, uint64_t nbytes, CallbackFn cb,
               void *arg)
        : throttler{throttler}, nbytes(nbytes), cb(cb), arg(arg) {}
    SpdkThrottler *throttler{nullptr};
    uint64_t nbytes{0};
    CallbackFn cb{nullptr};
    void *arg{nullptr};
  };

 public:
  SpdkThrottler(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair);
  int Read(void *buf, uint64_t start, uint64_t nsectors, CallbackFn cb,
           void *arg, int flags);
  int Write(void *buf, uint64_t start, uint64_t nsectors, CallbackFn cb,
            void *arg, int flags);
  int32_t ProcessCompletions(uint32_t max_completions = 0);
  // Returns the maximum buffer size any user of throttler should use. This is
  // not always equal to max_bytes_ as we could have multiple objects using the
  // throttler
  uint64_t GetBufSize() const;

 private:
  struct spdk_nvme_ns *ns_{nullptr};
  struct spdk_nvme_qpair *qpair_{nullptr};
  uint64_t max_bytes_{0};
  uint64_t bytes_in_flight_{0};

  uint64_t FindLargestBuf();
  void BlockTillSafe(uint64_t nbytes);
  static void WrappedCallback(void *ctx, const struct spdk_nvme_cpl *cpl);
};

class JournalIterator {
 public:
  JournalIterator(int idx, SpdkThrottler *throttler);
  ~JournalIterator();
  // checks if there is any pending io
  bool PendingIO() const;
  // checks if the journal is empty
  bool Empty() const;
  // checks if there are any io errors
  bool HasIOError() const;
  // writes jsuper, but only if the jsuper is empty (e.g. after checkpointing
  // all entries)
  void WriteEmptyJSuper() noexcept;
  // display the jsuper along with some stats
  void Display() const;
  // returns a valid journal entry if successful
  // returns nullptr if we reach the end of journal or if i/o is required
  JournalEntry *GetNextEntry() noexcept;
  // Alters jsuper to allow us to read more entries incase jsuper is stale.
  void ExtendToUnaccountedRegion();
  // blocks till pendingIO returns false
  void WaitForCompletions();
  // Ensures that the super block is valid, throws errors if not.
  void ValidateJSuper() const;
  // Logs minimum and maximum jentry sizes seen in the journal iterators.
  static void LogMinAndMaxJEntrySizes(std::vector<JournalIterator *> &ji_vec);
  // getters
  int Idx() const { return journal_idx_; }
  struct JSuperOnDisk CopyJSuperOnDisk() {
    return jsuper_.GetDiskRepr();
  }

 private:
  int journal_idx_{-1};
  SpdkThrottler *throttler_{nullptr};
  size_t dev_buf_size_{0};
  char *dev_buf_{nullptr};
  char *dev_buf_end_{nullptr};
  char *dev_buf_cursor_{nullptr};
  JSuper jsuper_{};
  uint64_t nblocks_read{0};
  uint64_t nblocks_read_valid{0};
  uint64_t nblocks_read_invalid{0};
  uint64_t total_io_completed{0};
  uint64_t total_io_failed{0};
  uint64_t total_io_issued{0};
  size_t max_jentry_size{0};
  size_t min_jentry_size{std::numeric_limits<size_t>::max()};

  void LoadJSuperOnInit() noexcept;
  bool IsJournalHeaderValid(JournalEntry &je) const;
  void FillDevBuffer() noexcept;
  void Consume(uint64_t nblocks, bool valid) noexcept;
};

enum OfflineCheckpointerState { READING_JSUPERS };
/* FsProcOfflineCheckpointer declaration + definition */
class FsProcOfflineCheckpointer {
 public:
  const std::filesystem::path kExportOriginalPath{"chkpt_export/original"};
  const std::filesystem::path kExportModifiedPath{"chkpt_export/modified"};

  FsProcOfflineCheckpointer(struct spdk_nvme_ns *ns,
                            struct spdk_nvme_qpair *qpair);
  ~FsProcOfflineCheckpointer();
  void checkpoint();
  void inspect();
  void dumpBlock(uint64_t block_num);
  void SetExportBlocks(bool v);
  void ImportData(std::string &import_dir);
  void ImportBitmapBlock(std::filesystem::path p, uint64_t block);
  void ImportInode(std::filesystem::path p, uint64_t ino);
  // Import jsuper, when not in restore mode, it aims to rewrite journal entries
  // so we start the journal from 0 even if internally after checkpointing it
  // started somewhere in the middle.
  void ImportJSuper(std::filesystem::path p, bool restore = false);
  void ImportJournalEntry(std::filesystem::path p);
  void ImportJournalEntryCommit(std::filesystem::path p, JournalEntry &je,
                                uint64_t ts);

  static void onIOComplete(void *arg, const struct spdk_nvme_cpl *completion);

  class QItem {
   public:
    JournalIterator *ji;
    struct JournalEntry *jentry{nullptr};
    int times_jentry_enqueued{0};
    QItem(JournalIterator *ji) : ji(ji) {}
  };

 private:
  bool export_blocks_{false};
  int exported_jentry_idx_{0};
  int imported_jentry_idx_{0};
  int imported_journal_block_ctr_{0};
  std::unordered_map<uint64_t, uint64_t> seen_inode_syncids;
  std::unordered_map<uint64_t, cfs_dinode *> sector_to_inode;
  std::unordered_map<uint64_t, char *> block_to_bitmap;
  struct spdk_nvme_ns *ns;
  struct spdk_nvme_qpair *qpair;
  volatile int total_issued;
  volatile int total_completed;
  volatile int total_failed;
  char *dev_mem;
  char *inode_dev_mem;
  char *bitmap_dev_mem;
  SpdkThrottler *throttler_{nullptr};
  std::vector<JournalIterator *> journal_iterators_;

  OfflineCheckpointerState state;
  void InitJournalIterators();
  void readNBlocks(uint64_t starting_block, size_t num_blocks, void *buf,
                   bool blocking);
  void writeNBlocks(uint64_t starting_block, size_t num_blocks, void *buf,
                    bool blocking);
  void readNSectors(uint64_t starting_sector, size_t num_sectors, void *buf,
                    bool blocking);
  void writeNSectors(uint64_t starting_sector, size_t num_sectors, void *buf,
                     bool blocking);
  bool syncIDConstraintSatisfied(uint64_t dsync_id, uint64_t jsync_id);
  bool dependsOnConstraintSatisfied(std::unordered_map<uint64_t, uint64_t> &m,
                                    bool verbose);
  cfs_dinode *getInode(uint64_t inode);
  char *getBitmapBlock(uint64_t block_no);
  bool canProcessJournalEntry(JournalEntry *je, bool verbose);
  void processJournalEntry(JournalEntry *je);
  void blockingProcessCompletions();
  void writeAllMetadata();
  void writeJSupers();
  void DisplayJournalIterators();
  void ExtendJSupersToUnaccountedRegion();
  void ExportJSuper(const struct JSuperOnDisk *dj);
  // FIXME: make this const later ... right now can't be const cause serialize
  // isn't const
  void ExportJournalEntry(JournalEntry *je);
  void ExportInode(const struct cfs_dinode *dinode, bool is_original);
  void ExportBitmapBlock(uint64_t block_no, const char *buf, bool is_original);
  void ExportOriginalInode(const struct cfs_dinode *dinode);
  void ExportModifiedInode(const struct cfs_dinode *dinode);
  void ExportOriginalBitmapBlock(uint64_t block_no, const char *buf);
  void ExportModifiedBitmapBlock(uint64_t block_no, const char *buf);
};

/* class definitions */
SpdkThrottler::SpdkThrottler(struct spdk_nvme_ns *ns,
                             struct spdk_nvme_qpair *qpair)
    : ns_(ns), qpair_(qpair) {
  max_bytes_ = FindLargestBuf();
  SPDLOG_INFO("Will throttle if more than {} bytes in flight", max_bytes_);
  SPDLOG_INFO("Each user can use buffer size of {} bytes", GetBufSize());
}

uint64_t SpdkThrottler::GetBufSize() const {
  // ensure that it is aligned to a block
  uint64_t buf_size = max_bytes_ / 4;
  return (buf_size / BSIZE) * BSIZE;
}

void SpdkThrottler::WrappedCallback(void *ctx,
                                    const struct spdk_nvme_cpl *cpl) {
  auto wrapped_arg = static_cast<WrappedArg *>(ctx);
  wrapped_arg->throttler->bytes_in_flight_ -= wrapped_arg->nbytes;
  CallbackFn cb = wrapped_arg->cb;
  void *arg = wrapped_arg->arg;
  delete wrapped_arg;
  if (cb != nullptr) cb(arg, cpl);
}

void SpdkThrottler::BlockTillSafe(uint64_t nbytes) {
  volatile uint64_t *in_flight = &bytes_in_flight_;
  while ((*in_flight + nbytes) >= max_bytes_) {
    int rc = 0;
    do {
      rc = ProcessCompletions(0);
    } while (rc < 1);
  }
}

int32_t SpdkThrottler::ProcessCompletions(uint32_t max_completions) {
  return spdk_nvme_qpair_process_completions(qpair_, max_completions);
}

int SpdkThrottler::Read(void *buf, uint64_t start, uint64_t nsectors,
                        CallbackFn cb, void *arg, int flags) {
  uint64_t nbytes = nsectors * 512;
  BlockTillSafe(nbytes);

  WrappedArg *warg = new WrappedArg(this, nbytes, cb, arg);
  bytes_in_flight_ += nbytes;
  int rc = spdk_nvme_ns_cmd_read(ns_, qpair_, buf, start, nsectors,
                                 WrappedCallback, warg, flags);
  if (rc != 0) {
    SPDLOG_WARN(
        "throttler read failed rc={}, bytes_in_flight={}, this_read: start={}, "
        "nsectors={}",
        rc, bytes_in_flight_, start, nsectors);
  }
  return rc;
}

int SpdkThrottler::Write(void *buf, uint64_t start, uint64_t nsectors,
                         CallbackFn cb, void *arg, int flags) {
  uint64_t nbytes = nsectors * 512;
  BlockTillSafe(nbytes);

  bytes_in_flight_ += nbytes;
  WrappedArg *warg = new WrappedArg(this, nbytes, cb, arg);
  int rc = spdk_nvme_ns_cmd_write(ns_, qpair_, buf, start, nsectors,
                                  WrappedCallback, warg, flags);
  if (rc != 0) {
    SPDLOG_WARN(
        "throttler write failed rc={}, bytes_in_flight={}, this_write: "
        "start={}, nsectors={}",
        rc, bytes_in_flight_, start, nsectors);
  }
  return rc;
}

uint64_t SpdkThrottler::FindLargestBuf() {
  char *buf = NULL;
  volatile int completed = 0;
  auto cb = [](void *ctx, const struct spdk_nvme_cpl *completion) {
    *(int *)ctx = 1;
    if (spdk_nvme_cpl_is_error(completion)) {
      throw std::runtime_error("Failed spdk request");
    }
  };

  uint64_t max_size = 128 * 1024 * 1024;  // 128 MB
  buf = (char *)spdk_dma_zmalloc(max_size, BSIZE, NULL);
  if (buf == NULL) {
    throw std::runtime_error("failed to spdk zmalloc");
  }

  uint64_t min_size = 0;
  for (int i = 0; i < 10; i++) {
    completed = 0;
    uint64_t buf_size = (min_size / 2) + (max_size / 2);
    buf_size = (buf_size / 4096) * 4096;  // round to a block size
    int rc = spdk_nvme_ns_cmd_read(ns_, qpair_, buf, 0, buf_size / 512, cb,
                                   (void *)(&completed), 0);
    SPDLOG_DEBUG("exploring...buf_size={} ({} blocks), rc={}", buf_size,
                 buf_size / 4096, rc);
    if (rc == 0) {
      while (completed != 1) {
        spdk_nvme_qpair_process_completions(qpair_, 0);
      }
      // successful at buf_size
      min_size = buf_size;
    } else {
      max_size = buf_size;
    }
  }

  spdk_dma_free(buf);
  SPDLOG_INFO("Found buf_size of {}, i.e. {} blocks", min_size,
              min_size / 4096);
  return min_size;
}

JournalIterator::JournalIterator(int idx, SpdkThrottler *throttler) {
  assert(idx >= 0 && throttler != nullptr);

  journal_idx_ = idx;
  throttler_ = throttler;

  dev_buf_size_ = throttler->GetBufSize();
  if (dev_buf_size_ < JournalManager::kMaxJournalEntrySize) {
    // NOTE: 10 MB for MAX_JOURNAL_ENTRY_SIZE is too huge, we should consider
    // making it smaller after sampling how large it really can get..
    SPDLOG_ERROR("Buffer (size {}) too small to fit a single journal entry",
                 dev_buf_size_);
    throw std::runtime_error("buffer too small");
  }

  dev_buf_ = (char *)spdk_dma_zmalloc(dev_buf_size_, BSIZE, NULL);
  if (dev_buf_ == nullptr) throw std::runtime_error("spdk_dma_zmalloc failed");

  LoadJSuperOnInit();
}

JournalIterator::~JournalIterator() {
  if (dev_buf_ != nullptr) spdk_dma_free(dev_buf_);
}

void JournalIterator::Display() const {
  const struct JSuperOnDisk dj = jsuper_.GetDiskRepr();

  SPDLOG_INFO(
      "[{}] jsuper_blockno={}, n_used={}, head={}, tail={}, total_read={}, "
      "valid={}, invalid={}, last_chkpt_ts={}",
      journal_idx_, dj.jsuper_blockno, dj.n_used, dj.head, dj.tail,
      nblocks_read, nblocks_read_valid, nblocks_read_invalid, dj.last_chkpt_ts);
}

void JournalIterator::WriteEmptyJSuper() noexcept {
  if (!jsuper_.Empty()) {
    SPDLOG_ERROR("Journal {} still has {} items, not writing superblock",
                 journal_idx_, jsuper_.Size());
    return;
  }

  auto cb = [](void *ctx, const struct spdk_nvme_cpl *completion) {
    auto ji = static_cast<JournalIterator *>(ctx);
    ji->total_io_completed++;
    if (spdk_nvme_cpl_is_error(completion)) {
      ji->total_io_failed++;
      return;
    }
  };

  // TODO: since we always reset to 0, make sure to test the journal circular
  // wraparound path elsewhere.
  struct JSuperOnDisk dj;
  jsuper_.CopyToBuf(&dj);
  dj.last_chkpt_ts = (uint64_t)tap_ustime();
  dj.head = 0;
  dj.tail = 0;
  dj.n_used = 0;
  assert(dj.jsuper_blockno == get_worker_journal_sb(journal_idx_));
  *((struct JSuperOnDisk *)(dev_buf_)) = dj;

  total_io_issued++;
  int rc = throttler_->Write(dev_buf_, dj.jsuper_blockno * 8, 1, cb, this, 0);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to write journal super block {}, journal {}",
                 dj.jsuper_blockno, journal_idx_);
    total_io_completed++;
    total_io_failed++;
  }
}

void JournalIterator::LoadJSuperOnInit() noexcept {
  auto cb = [](void *ctx, const struct spdk_nvme_cpl *completion) {
    auto ji = static_cast<JournalIterator *>(ctx);
    ji->total_io_completed++;

    if (spdk_nvme_cpl_is_error(completion)) {
      ji->total_io_failed++;
      return;
    }

    ji->jsuper_.SetDiskRepr((struct JSuperOnDisk *)ji->dev_buf_);
  };

  total_io_issued++;
  uint64_t jsb_block_no = get_worker_journal_sb(journal_idx_);
  int rc = throttler_->Read(dev_buf_, jsb_block_no * 8, 1, cb, this, 0);
  if (rc != 0) {
    total_io_failed++;
    total_io_completed++;
    SPDLOG_ERROR("Failed to read journal superblock {} for journal {}",
                 jsb_block_no, journal_idx_);
  }
}

void JournalIterator::ValidateJSuper() const {
  uint64_t jsb_block_no = get_worker_journal_sb(journal_idx_);
  const struct JSuperOnDisk dj = jsuper_.GetDiskRepr();
  if (dj.jmagic != JSUPER_MAGIC) {
    SPDLOG_ERROR("JSUPER_MAGIC mismatch for journal {}, block {}", journal_idx_,
                 jsb_block_no);
    throw std::runtime_error("invalid jsuper");
  }

  if (jsb_block_no != dj.jsuper_blockno) {
    SPDLOG_ERROR(
        "Journal superblock block number mismatch, read block {} but contents "
        "say it is block {}",
        jsb_block_no, dj.jsuper_blockno);
    throw std::runtime_error("invalid jsuper");
  }

  if ((dj.n_used + JSuper::kMaxUnaccountedBlocks) >= dj.capacity) {
    // If we search the unaccounted region, we will be cycling through the
    // valid entries in the journal. This case shouldn't arise as we must
    // always have max_unaccounted free space in the journal. i.e.
    // checkpointing must happen well before this is reached. This check is to
    // avoid such cases and indicates problems to be fixed in fsp.
    throw std::runtime_error(
        "unaccounted journal blocks overflowing into valid journal entries");
  }
}

void JournalIterator::ExtendToUnaccountedRegion() {
  uint64_t n_used = jsuper_.Size();
  uint64_t capacity = jsuper_.Capacity();
  uint64_t n_free = capacity - n_used;
  uint64_t n = n_free;
  if (n > JSuper::kMaxUnaccountedBlocks) n = JSuper::kMaxUnaccountedBlocks;

  auto slot = jsuper_.AcquireNextNSlots(n);
  if (slot.no_space)
    throw std::runtime_error("Failed to extend past the journal");
}

bool JournalIterator::HasIOError() const { return total_io_failed > 0; }

bool JournalIterator::PendingIO() const {
  return total_io_issued > total_io_completed;
}

bool JournalIterator::Empty() const { return jsuper_.Empty(); }

bool JournalIterator::IsJournalHeaderValid(JournalEntry &je) const {
  // a well formed entry may be a stale one so we check ts when it is well
  // formed.
  return (je.state == JournalEntryState::DESERIALIZED_PARTIAL) &&
         (je.ustime_serialized > jsuper_.GetLastCheckpoint());
}

void JournalIterator::FillDevBuffer() noexcept {
  uint64_t blocks_to_read = dev_buf_size_ / BSIZE;
  const struct JSuperOnDisk dj = jsuper_.GetDiskRepr();

  if (blocks_to_read > dj.n_used) blocks_to_read = dj.n_used;

  if (blocks_to_read == 0) return;

  // read from (including) tail
  uint64_t start_idx = dj.tail;
  uint64_t start_block = jsuper_.IdxToBlockNo(start_idx);

  uint64_t end_idx = start_idx + blocks_to_read - 1;
  uint64_t end_block = jsuper_.IdxToBlockNo(end_idx);

  if (start_block > end_block) {
    // avoid wrap around
    end_block = dj.jend_blockno;
  }

  blocks_to_read = end_block - start_block + 1;
  assert(blocks_to_read >= 1);

  dev_buf_cursor_ = dev_buf_;
  dev_buf_end_ = dev_buf_ + (blocks_to_read * BSIZE);

  auto cb = [](void *ctx, const struct spdk_nvme_cpl *completion) {
    auto ji = static_cast<JournalIterator *>(ctx);
    ji->total_io_completed++;

    if (spdk_nvme_cpl_is_error(completion)) {
      ji->total_io_failed++;
    }
  };

  total_io_issued++;
  int rc = throttler_->Read(dev_buf_, start_block * 8, blocks_to_read * 8, cb,
                            this, 0);
  if (rc != 0) {
    total_io_failed++;
    total_io_completed++;
    SPDLOG_ERROR("Failed to issue read io cmd, nblocks={}, return code = {}",
                 blocks_to_read, rc);
  }
}

void JournalIterator::WaitForCompletions() {
  while (PendingIO()) {
    throttler_->ProcessCompletions(0);
  }
}

void JournalIterator::Consume(uint64_t nblocks, bool valid) noexcept {
  nblocks_read += nblocks;
  dev_buf_cursor_ += (nblocks * BSIZE);
  jsuper_.ReleaseFirstNSlots(nblocks);
  if (valid)
    nblocks_read_valid += nblocks;
  else
    nblocks_read_invalid += nblocks;
}

JournalEntry *JournalIterator::GetNextEntry() noexcept {
  if (Empty() || PendingIO()) return nullptr;

  do {
    // read until we find a valid jsuper
    JournalEntry je = JournalEntry();
    while ((!Empty()) && (dev_buf_cursor_ < dev_buf_end_)) {
      je.state = JournalEntryState::DESERIALIZED_ERROR_MAGIC;
      JournalEntry::ParseJournalEntryHeader((uint8_t *)dev_buf_cursor_, BSIZE,
                                            je);
      if (!IsJournalHeaderValid(je)) {
        // try the next block
        Consume(1, /*valid*/ false);
        continue;
      }

      break;  // found valid journal entry
    }

    if (!IsJournalHeaderValid(je)) {
      if (Empty()) return nullptr;

      // not empty but invalid je,
      FillDevBuffer();
      return nullptr;
    }

    // valid header but we may not have all the blocks for the entire body
    uint64_t nblocks_remaining = (dev_buf_end_ - dev_buf_cursor_) / BSIZE;
    if (je.nblocks > nblocks_remaining) {
      FillDevBuffer();
      return nullptr;
    }

    // NOTE: if there is an error here, we can safely skip je.nblocks as that
    // was a valid header. So the next header has to be atleast after nblocks.
    JournalEntry *je_to_return =
        new JournalEntry((uint8_t *)dev_buf_cursor_, je.nblocks * BSIZE);
    try {
      je_to_return->deserializeBody((uint8_t *)dev_buf_cursor_,
                                    je.nblocks * BSIZE);
    } catch (std::runtime_error &e) {
      SPDLOG_WARN(
          "Found valid journal header and commit starting at block {}, but "
          "unable to deserialize body, skipping it",
          je.start_block);
      SPDLOG_WARN("{}", e.what());
      delete je_to_return;
      Consume(je.nblocks, /*valid*/ false);
      continue;
    }

    // the header is valid, and so is the body, but we need to make sure there
    // was a commit block
    char *commit_block = dev_buf_cursor_ + (je.nblocks - 1) * BSIZE;
    if (!je_to_return->isValidCommitBlock((uint8_t *)commit_block, BSIZE)) {
      je_to_return->logCommitBlockMismatches((uint8_t *)commit_block, BSIZE);
      SPDLOG_WARN(
          "Found valid journal header but invalid commit block, skipping it");
      delete je_to_return;
      Consume(je.nblocks, /*valid*/ false);
      continue;  // invalid, so repeat the loop starting from new cursor
    }

    Consume(je.nblocks, /*valid*/ true);

    size_t jentry_size = je_to_return->calcBodySize();
    if (jentry_size > max_jentry_size) max_jentry_size = jentry_size;
    if (jentry_size < min_jentry_size) min_jentry_size = jentry_size;

    return je_to_return;

  } while (1);
  return nullptr;
}

void JournalIterator::LogMinAndMaxJEntrySizes(
    std::vector<JournalIterator *> &ji_vec) {
  size_t min_sz = std::numeric_limits<size_t>::max();
  size_t max_sz = 0;

  for (const auto &ji : ji_vec) {
    if (ji == nullptr) continue;
    if (ji->max_jentry_size > max_sz) max_sz = ji->max_jentry_size;
    if (ji->min_jentry_size < min_sz) min_sz = ji->min_jentry_size;
  }

  if (min_sz != std::numeric_limits<size_t>::max())
    SPDLOG_INFO("min_jentry_size = {}", min_sz);
  if (max_sz != 0) SPDLOG_INFO("max_jentry_size = {}", max_sz);
}

FsProcOfflineCheckpointer::FsProcOfflineCheckpointer(
    struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair)
    : ns(ns),
      qpair(qpair),
      total_issued(0),
      total_completed(0),
      total_failed(0),
      dev_mem(NULL),
      inode_dev_mem(NULL),
      bitmap_dev_mem(NULL) {
  uint64_t num_bytes_to_alloc = 0;
  // We read inodes one at a time, so just need 1 sector, but taking a block for
  // simplicity (alignment etc..)
  num_bytes_to_alloc += BSIZE;
  // We read bitmap blocks one at a time
  num_bytes_to_alloc += BSIZE;
  // FIXME: right now throttler is only used by the journal iterators. Make the
  // rest of the code use it too.
  throttler_ = new SpdkThrottler(ns, qpair);

  dev_mem = (char *)spdk_dma_zmalloc(num_bytes_to_alloc, BSIZE, NULL);
  if (dev_mem == NULL)
    throw std::runtime_error("Failed to alloc device memory");

  inode_dev_mem = dev_mem;
  bitmap_dev_mem = inode_dev_mem + BSIZE;
}

FsProcOfflineCheckpointer::~FsProcOfflineCheckpointer() {
  for (auto ji : journal_iterators_) delete ji;
  journal_iterators_.clear();

  if (dev_mem != NULL) spdk_dma_free(dev_mem);

  dev_mem = NULL;
  inode_dev_mem = NULL;
  bitmap_dev_mem = NULL;
}

void FsProcOfflineCheckpointer::InitJournalIterators() {
#if CFS_JOURNAL(GLOBAL_JOURNAL)
  constexpr int num_journals = 1;
#else
  constexpr int num_journals = NMAX_FSP_WORKER;
#endif

  for (int i = 0; i < num_journals; i++) {
    auto ji = new JournalIterator(i, throttler_);
    journal_iterators_.push_back(ji);
  }

  // all iterators load the super on init
  for (auto ji : journal_iterators_) {
    ji->WaitForCompletions();

    if (ji->HasIOError())
      throw std::runtime_error("Failed to initialize journal iterators");

    ji->ValidateJSuper();
    if (export_blocks_) {
      const struct JSuperOnDisk dj = ji->CopyJSuperOnDisk();
      ExportJSuper(&dj);
    }
  }
}

void FsProcOfflineCheckpointer::ExtendJSupersToUnaccountedRegion() {
  for (auto ji : journal_iterators_) ji->ExtendToUnaccountedRegion();
}

inline void FsProcOfflineCheckpointer::readNBlocks(uint64_t starting_block,
                                                   size_t num_blocks, void *buf,
                                                   bool blocking) {
  readNSectors(starting_block * 8, num_blocks * 8, buf, blocking);
}

void FsProcOfflineCheckpointer::readNSectors(uint64_t starting_sector,
                                             size_t num_sectors, void *buf,
                                             bool blocking) {
  total_issued = total_issued + 1;
  int rc = spdk_nvme_ns_cmd_read(ns, qpair, buf, starting_sector, num_sectors,
                                 onIOComplete, this, 0);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to issue read i/o cmd");
    total_completed = total_completed + 1;
    total_failed = total_failed + 1;
    return;
  }
  if (blocking) blockingProcessCompletions();
}

inline void FsProcOfflineCheckpointer::writeNBlocks(uint64_t starting_block,
                                                    size_t num_blocks,
                                                    void *buf, bool blocking) {
  writeNSectors(starting_block * 8, num_blocks * 8, buf, blocking);
}

//#define offline_dryrun 1
void FsProcOfflineCheckpointer::writeNSectors(uint64_t starting_sector,
                                              size_t num_sectors, void *buf,
                                              bool blocking) {
  total_issued = total_issued + 1;
  /* For now, no writes - just shallow check...
  if (offline_dryrun) {
    total_completed++;
    return;
  }*/

  int rc = spdk_nvme_ns_cmd_write(ns, qpair, buf, starting_sector, num_sectors,
                                  onIOComplete, this, 0);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to issue write i/o cmd");
    total_completed = total_completed + 1;
    total_failed = total_failed + 1;
    return;
  }

  if (blocking) blockingProcessCompletions();
}

void FsProcOfflineCheckpointer::DisplayJournalIterators() {
  for (auto ji : journal_iterators_) ji->Display();
}

void FsProcOfflineCheckpointer::onIOComplete(
    void *arg, const struct spdk_nvme_cpl *completion) {
  auto chkptr = (FsProcOfflineCheckpointer *)arg;
  chkptr->total_completed = chkptr->total_completed + 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    chkptr->total_failed = chkptr->total_failed + 1;
    return;
  }
}

void FsProcOfflineCheckpointer::blockingProcessCompletions() {
  do {
    // TODO capture return code
    spdk_nvme_qpair_process_completions(qpair, 0);
  } while (total_issued != total_completed);
}

// Must be called with syncID on disk (dsync) and on journal (jsync) for the
// same inode
inline bool FsProcOfflineCheckpointer::syncIDConstraintSatisfied(
    uint64_t dsync_id, uint64_t jsync_id) {
  // normal case: jsync_id == (dsync_id + 1)
  // complex case: failure during checkpointing - journal operations applied to
  // inode but the journal isn't truncated. jsync_id < dsync_id NOTE: for now we
  // do not support the complex case. We won't be failing checkpoint at the
  // point where it finishes applying changes but then crashes when updating
  // jsupers. NOTE: this could be done by ensuring that the checkpoint writes to
  // a special checkpoint journal for atomicity. Alternatively, we make two
  // passes over the journals.
#if CFS_JOURNAL(LOCAL_JOURNAL)
  return jsync_id == (dsync_id + 1);
#else
  // In global journal mode we do not need to look at sync_id as we process
  // entries left to right
  return true;
#endif
}

inline bool FsProcOfflineCheckpointer::dependsOnConstraintSatisfied(
    std::unordered_map<uint64_t, uint64_t> &m, bool verbose) {
  for (const auto &kv : m) {
    uint64_t inode = kv.first;
    uint64_t syncID = kv.second;
    cfs_dinode *dinode = getInode(inode);
    if (syncID >= dinode->syncID) {
      if (verbose) {
        SPDLOG_WARN(
            "dependsOnConstraint NOT satisfied,  need: inode {} syncID {}, "
            "have: inode {} syncID {}",
            inode, syncID, inode, dinode->syncID);
      }
      return false;
    }
  }
  return true;
}

cfs_dinode *FsProcOfflineCheckpointer::getInode(uint64_t inode) {
  uint64_t sector = calcSectorForInode(inode);
  auto search = sector_to_inode.find(sector);
  if (search != sector_to_inode.end()) return search->second;

  // TODO: optimize this to avoid blocking
  readNSectors(sector, 1, inode_dev_mem, /*blocking*/ true);
  cfs_dinode *dinode = (cfs_dinode *)malloc(sizeof(cfs_dinode));
  if (dinode == NULL) throw std::runtime_error("failed to malloc dinode");
  memcpy(dinode, inode_dev_mem, sizeof(cfs_dinode));
  sector_to_inode[sector] = dinode;

  if (export_blocks_) ExportOriginalInode(dinode);
  return dinode;
}

char *FsProcOfflineCheckpointer::getBitmapBlock(uint64_t block_no) {
  auto search = block_to_bitmap.find(block_no);
  if (search != block_to_bitmap.end()) return search->second;

  // TODO: optimize this to avoid blocking
  readNBlocks(block_no, 1, bitmap_dev_mem, /*blocking*/ true);
  char *buf = (char *)malloc(BSIZE);
  if (buf == NULL) throw std::runtime_error("failed to malloc bitmap block");
  memcpy(buf, bitmap_dev_mem, BSIZE);
  block_to_bitmap[block_no] = buf;
  if (export_blocks_) ExportOriginalBitmapBlock(block_no, buf);
  return buf;
}

bool FsProcOfflineCheckpointer::canProcessJournalEntry(JournalEntry *je,
                                                       bool verbose) {
  if (je == NULL) return false;
  // returns true if the seq no & depends_on conditions have been met
  for (InodeLogEntry *ile : je->ile_vec) {
    cfs_dinode *dinode = getInode(ile->inode);
    if (!syncIDConstraintSatisfied(dinode->syncID, ile->syncID)) {
      if (verbose) {
        SPDLOG_WARN(
            "syncIDConstraint NOT satisfied, inode={}, syncID={}, "
            "logEntry.syncID={}",
            dinode->i_no, dinode->syncID, ile->syncID);
      }
      return false;
    }
    if (!dependsOnConstraintSatisfied(ile->depends_on, verbose)) {
      return false;
    }
  }

  return true;
}

void FsProcOfflineCheckpointer::processJournalEntry(JournalEntry *je) {
  // TODO: have some identifier for journal entries when written to journal like
  // a sequence number?
  SPDLOG_DEBUG("Processing JournalEntry: ");
  if (export_blocks_) ExportJournalEntry(je);
  for (InodeLogEntry *ile : je->ile_vec) {
    std::unordered_map<uint64_t, bool> blocks_add_or_del;
    cfs_dinode *dinode = getInode(ile->inode);

    SPDLOG_DEBUG("Applying changes for inode {}", dinode->i_no);
    ile->applyChangesTo(dinode, blocks_add_or_del);
    for (auto const &iter : blocks_add_or_del) {
      uint64_t pba = iter.first;
      uint64_t lba = conv_pba_to_lba(pba);
      cfs_bno_t bmap_disk_bno = get_bmap_block_for_lba(lba);
      char *bmap = getBitmapBlock(bmap_disk_bno);
      if (iter.second) {
        block_set_bit(lba % BPB, bmap);
        SPDLOG_DEBUG("Setting bit in bmap for block {}, lba = {}", pba, lba);
      } else {
        block_clear_bit(lba % BPB, bmap);
        SPDLOG_DEBUG("Clearing bit in bmap for block {}, lba = {}", pba, lba);
      }
    }

    if (ile->optfield_bitarr & bitmap_op_IDX) {
      cfs_bno_t bmap_disk_bno = get_imap_for_inode(ile->inode);
      char *bmap = getBitmapBlock(bmap_disk_bno);
      if (ile->bitmap_op) {
        block_set_bit(ile->inode % BPB, bmap);
        SPDLOG_DEBUG("Setting bit in imap for inode {}", ile->inode);
      } else {
        block_clear_bit(ile->inode % BPB, bmap);
        SPDLOG_DEBUG("Clearing bit in imap for inode {}", ile->inode);
      }
    }
  }
}

void FsProcOfflineCheckpointer::writeAllMetadata() {
  // TODO : right now it writes one by one. Make it non blocking.
  for (auto const &iter : sector_to_inode) {
    cfs_dinode *dinode = iter.second;
    memcpy(inode_dev_mem, dinode, sizeof(cfs_dinode));
    writeNSectors(iter.first, 1, inode_dev_mem, true);
    if (export_blocks_) ExportModifiedInode(dinode);
  }

  for (auto const &iter : block_to_bitmap) {
    char *bitmap = iter.second;
    memcpy(bitmap_dev_mem, bitmap, BSIZE);
    writeNBlocks(iter.first, 1, bitmap_dev_mem, true);
    if (export_blocks_) ExportModifiedBitmapBlock(iter.first, bitmap);
  }

  // issue flush for safety
  total_issued = total_issued + 1;
  int rc = spdk_nvme_ns_cmd_flush(ns, qpair, onIOComplete, this);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to send flush command to device");
    total_completed = total_completed + 1;
    total_failed = total_failed + 1;
  }
  blockingProcessCompletions();
}

void FsProcOfflineCheckpointer::writeJSupers() {
  for (auto ji : journal_iterators_) {
    ji->WriteEmptyJSuper();
  }

  for (auto ji : journal_iterators_) {
    ji->WaitForCompletions();

    if (ji->HasIOError()) {
      throw std::runtime_error("Failed to write jsuper");
    }

    // NOTE: no need to export modified jsuper because that just resets it.
  }
}

void FsProcOfflineCheckpointer::checkpoint() {
  InitJournalIterators();
  SPDLOG_INFO("------ Initial state of iterators...");
  DisplayJournalIterators();
  // journal super may be stale
  ExtendJSupersToUnaccountedRegion();

  std::queue<QItem *> q;
  // initial seed for the queue
  for (auto ji : journal_iterators_) {
    QItem *item = new QItem(ji);
    item->jentry = item->ji->GetNextEntry();
    item->times_jentry_enqueued = 0;
    // we will only increment enqueued when it is a valid jentry as the item may
    // be requeued for pending io
    if (item->jentry != nullptr) item->times_jentry_enqueued++;
    q.push(item);
  }

  int num_jentries_processed = 0;
  while (!q.empty()) {
    throttler_->ProcessCompletions(0);
    auto item = q.front();
    q.pop();
    if (item->jentry == nullptr && item->ji->Empty()) {
      // reached the end for this journal
      delete item;
      continue;
    }

    if (item->ji->PendingIO()) {
      // this is not a retry. we stopped because of io, so place it at the back
      // of the queue.
      q.push(item);
      continue;
    }

    if (item->ji->HasIOError()) {
      SPDLOG_ERROR("IO Error in journal iterator, aborting");
      throw std::runtime_error("Error iterating over journal");
    }

    // even if io completed, it could have happened after the call of the
    // previous GetNextEntry, so we have to check again
    if (item->jentry == nullptr) {
      item->jentry = item->ji->GetNextEntry();
      item->times_jentry_enqueued = 0;
      if (item->jentry == nullptr) {
        q.push(item);
        continue;  // requeue for later
      }
      item->times_jentry_enqueued++;
    }
    // if jentry was null but journal wasn't empty, it has to be pending io or
    // io that completed after that..

    assert(item->jentry != nullptr);

    // No pending io, no error, jentry isn't null
    if (item->times_jentry_enqueued > 5) {
      bool canProc = canProcessJournalEntry(item->jentry, /*verbose*/ true);
      if (!canProc) {
        SPDLOG_WARN("QItem not making progress for journal {}",
                    item->ji->Idx());
        throw std::runtime_error("QItem cannot make progress");
      }
    }

    while (canProcessJournalEntry(item->jentry, /*verbose*/ true)) {
      num_jentries_processed++;
      processJournalEntry(item->jentry);
      delete item->jentry;
      item->jentry = nullptr;
      item->jentry = item->ji->GetNextEntry();
      item->times_jentry_enqueued = 0;
      if (item->jentry != nullptr) item->times_jentry_enqueued++;
    }

    q.push(item);
    if (item->jentry != nullptr) item->times_jentry_enqueued++;
  }

  bool abort = false;
  for (auto &ji : journal_iterators_) {
    ji->WaitForCompletions();

    if (ji->HasIOError()) {
      SPDLOG_INFO("Journal {} has io errors, aborting", ji->Idx());
      abort = true;
    }

    if (!ji->Empty()) {
      SPDLOG_INFO("Journal {} not empty after checkpointing, aborting",
                  ji->Idx());
      abort = true;
    }
  }

  if (abort) return;

  if (num_jentries_processed == 0) {
    SPDLOG_INFO("No journal entries to process, aborting");
    return;
  }

  writeAllMetadata();
  if (total_failed > 0) {
    // TODO: fail faster earlier on rather than one aggregate at the end
    SPDLOG_WARN("{} IO failures, aborting checkpointing", total_failed);
    return;
  }

  // The journal superblocks have already been modified to represent an empty
  // journal. The getNextJournalEntry dequeues from jsuper and decrements
  // n_used. NOTE: if we add timestamps for last checkpoint etc. then jsuper
  // needs to be modified. For now, we just need to write the jsupers out.
  writeJSupers();
  // TODO issue flush after writing jsupers
  // NOTE: technically, we can fail here when writing jsupers - so make sure to
  // update all jsupers atomically. Although, it is harmless if some jsupers
  // aren't updated. During next checkpoint, it may depend on entries in the
  // other journal but since they've been checkpointed, the dinodes will have
  // the latest syncID, so it won't be a problem.
  SPDLOG_INFO("------ Final state of iterators...");
  DisplayJournalIterators();
  JournalIterator::LogMinAndMaxJEntrySizes(journal_iterators_);
}

void FsProcOfflineCheckpointer::SetExportBlocks(bool e) {
  export_blocks_ = e;
  if (!export_blocks_) return;

  namespace fs = std::filesystem;
  fs::create_directories(kExportOriginalPath);
  fs::create_directories(kExportModifiedPath);
}

int SafeCreateExportFd(std::filesystem::path p) {
  const char *pstr = p.c_str();
  int fd = open(pstr, O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (fd <= 0) {
    SPDLOG_ERROR("Failed to create {}", p.string());
    throw std::runtime_error("Failed to create export file");
  }
  return fd;
}

void FsProcOfflineCheckpointer::ExportJSuper(const struct JSuperOnDisk *dj) {
  int fd = SafeCreateExportFd(kExportOriginalPath / "jsuper");

  ssize_t ssret = write(fd, static_cast<const void *>(dj), sizeof(*dj));
  if (ssret <= 0) throw std::runtime_error("write() failure");

  int iret = close(fd);
  if (iret != 0) throw std::runtime_error("close() failure");
}

void FsProcOfflineCheckpointer::ExportJournalEntry(JournalEntry *je) {
  size_t buf_size = je->calcBodySize();
  uint8_t *buf = new uint8_t[buf_size];
  PreserveTSAndSerialize(*je, buf);

  auto jentry_idx = ++exported_jentry_idx_;
  std::stringstream ss;
  ss << "jentry_" << jentry_idx;
  std::string fname = ss.str();
  int fd = SafeCreateExportFd(kExportOriginalPath / fname);
  ssize_t ssret = write(fd, buf, buf_size);
  if (ssret < 0) throw std::runtime_error("write() failure");
  if ((size_t)ssret != buf_size) throw std::runtime_error("write() incomplete");

  int iret = close(fd);
  if (iret != 0) throw std::runtime_error("close() failure");

  // write file to signal "commit", it internally has json data about the jentry
  ss << "_commit";
  fname = ss.str();
  int num_inodes = 0;
  std::ofstream commit_file;
  commit_file.open(kExportOriginalPath / fname);
  commit_file << "{";
  commit_file << "\"idx\":" << jentry_idx << ",";
  commit_file << "\"inodes_in_entry\": [";
  for (auto ile : je->ile_vec) {
    num_inodes += 1;
    commit_file << ile->inode << ",";
  }

  if (num_inodes > 0) {
    commit_file.seekp(-1, commit_file.cur);
  }

  commit_file << "]}";
  commit_file.close();
}

void FsProcOfflineCheckpointer::ExportInode(const struct cfs_dinode *dinode,
                                            bool is_original) {
  std::stringstream ss;
  ss << "inode_" << dinode->i_no;
  std::string fname = ss.str();
  int fd;
  if (is_original) {
    fd = SafeCreateExportFd(kExportOriginalPath / fname);
  } else {
    fd = SafeCreateExportFd(kExportModifiedPath / fname);
  }

  ssize_t ssret = write(fd, static_cast<const void *>(dinode), sizeof(*dinode));
  if (ssret < 0) throw std::runtime_error("write() failure");
  if ((size_t)ssret != sizeof(*dinode))
    throw std::runtime_error("write() incomplete");

  int iret = close(fd);
  if (iret != 0) throw std::runtime_error("close() failure");
}

void FsProcOfflineCheckpointer::ExportOriginalInode(
    const struct cfs_dinode *dinode) {
  ExportInode(dinode, /*is_original*/ true);
}

void FsProcOfflineCheckpointer::ExportModifiedInode(
    const struct cfs_dinode *dinode) {
  ExportInode(dinode, /*is_original*/ false);
}

void FsProcOfflineCheckpointer::ExportBitmapBlock(uint64_t block_no,
                                                  const char *buf,
                                                  bool is_original) {
  std::stringstream ss;
  ss << "bmap_" << block_no;
  std::string fname = ss.str();
  int fd;
  if (is_original) {
    fd = SafeCreateExportFd(kExportOriginalPath / fname);
  } else {
    fd = SafeCreateExportFd(kExportModifiedPath / fname);
  }

  ssize_t ssret = write(fd, buf, 4096);
  if (ssret != 4096) throw std::runtime_error("write() failure");

  int iret = close(fd);
  if (iret != 0) throw std::runtime_error("close() failure");
}

void FsProcOfflineCheckpointer::ExportOriginalBitmapBlock(uint64_t block_no,
                                                          const char *buf) {
  ExportBitmapBlock(block_no, buf, /*is_original*/ true);
}

void FsProcOfflineCheckpointer::ExportModifiedBitmapBlock(uint64_t block_no,
                                                          const char *buf) {
  ExportBitmapBlock(block_no, buf, /*is_original*/ false);
}

template <typename T>
T as_type(const std::string &s) {
  T v;
  std::istringstream ss(s);
  ss >> v;
  return v;
}

void SafeReadFile(const char *fname, void *buf, size_t sz, bool sz_check) {
  int fd = open(fname, O_RDONLY);
  if (fd <= 0) throw std::runtime_error("Failed to open file");

  ssize_t ssret = read(fd, buf, sz);
  if (ssret < 0) throw std::runtime_error("Failed to read file");
  if (sz_check && ((size_t)ssret != sz)) {
    SPDLOG_ERROR("Read {} bytes from {}, required {} bytes", ssret, fname, sz);
    throw std::runtime_error("Failed to read file entirely");
  }

  int iret = close(fd);
  if (iret != 0) throw std::runtime_error("Failed to close file");
}

void FsProcOfflineCheckpointer::ImportBitmapBlock(std::filesystem::path p,
                                                  uint64_t block) {
  SPDLOG_INFO("Importing block {} from file {}", block, p.string());
  auto filesize = std::filesystem::file_size(p);
  if (filesize < 4096) {
    throw std::runtime_error("Require filesize >= 4096B");
  }

  SafeReadFile(p.c_str(), bitmap_dev_mem, 4096, /*sz_check*/ true);
  // NOTE: Simpler for now to just write one by one - these are correctness
  // checks, not performance.
  // TODO: make this faster/non blocking in the future, requires more pinned
  // memory
  writeNBlocks(block, 1, bitmap_dev_mem, /*blocking*/ true);
}

void FsProcOfflineCheckpointer::ImportInode(std::filesystem::path p,
                                            uint64_t ino) {
  SPDLOG_INFO("Importing inode {} from file {}", ino, p.string());
  auto filesize = std::filesystem::file_size(p);

  if (filesize < 512) {
    throw std::runtime_error("Require filesize >= 512B");
  }

  SafeReadFile(p.c_str(), inode_dev_mem, 512, /*sz_check*/ true);
  uint64_t sector = calcSectorForInode(ino);
  writeNSectors(sector, 1, inode_dev_mem, /*blocking*/ true);
}

void FsProcOfflineCheckpointer::ImportJSuper(std::filesystem::path p,
                                             bool restore) {
  SPDLOG_INFO("Importing jsuper from file {}", p.string());
  auto filesize = std::filesystem::file_size(p);
  if (filesize < sizeof(struct JSuperOnDisk)) {
    throw std::runtime_error("jsuper file too small");
  }

  memset(bitmap_dev_mem, 0, 4096);
  SafeReadFile(p.c_str(), bitmap_dev_mem, sizeof(struct JSuperOnDisk), true);

  if (!restore) {
    // Reset the journal entries head/tail to start from 0th index rather than
    // somewhere in the middle as we are going to write our own journal entries.
    // Everything else remains the same (most importantly - the timestamp
    // remains the same)
    JSuperOnDisk *js = (JSuperOnDisk *)bitmap_dev_mem;
    js->tail = 0;
    js->head = js->n_used;
  }

  uint64_t jsb = get_worker_journal_sb(0);
  writeNBlocks(jsb, 1, bitmap_dev_mem, /*blocking*/ true);
}

void FsProcOfflineCheckpointer::ImportJournalEntryCommit(
    std::filesystem::path p, JournalEntry &je, uint64_t ts) {
  memset(bitmap_dev_mem, 0, 4096);
  uint64_t jblock_ctr = ++imported_journal_block_ctr_;
  uint64_t jcommit_block = get_worker_journal_sb(0) + jblock_ctr;

  // The user specifies whether commits should be imported by the presence of
  // the file jentry_*_commit
  if (!std::filesystem::exists(p)) {
    // we still ensure that a commit block space is kept
    SPDLOG_WARN("Commit file {} does not exist, writing zeroes to block",
                p.string());
    // FIXME: assert that this block does not exceed journal limits. Hardly
    // likely as we won't be crash testing with so many files for now.
    writeNBlocks(jcommit_block, 1, bitmap_dev_mem, /*blocking*/ true);
    return;
  }

  je.serializeCommit((uint8_t *)bitmap_dev_mem);
  // The serialize commit has the timestamp of new serialization, so changing
  // back to old one.
  uint64_t *buf_ptr = (uint64_t *)bitmap_dev_mem;
  buf_ptr[1] = ts;
  // FIXME: assert that this block does not exceed journal limits. Hardly likely
  // as we won't be crash testing with so many files for now.
  writeNBlocks(jcommit_block, 1, bitmap_dev_mem, /*blocking*/ true);
}

uint64_t PreserveTSAndSerialize(JournalEntry &je, uint8_t *buf) {
  uint64_t old_ts = je.GetSerializedTime();
  je.serializeBody(buf);
  uint64_t *uint64_header_fields = (uint64_t *)buf;
  uint64_header_fields[1] = old_ts;
  return old_ts;
}

void FsProcOfflineCheckpointer::ImportJournalEntry(std::filesystem::path p) {
  int idx = ++imported_jentry_idx_;
  auto filesize = std::filesystem::file_size(p);
  SPDLOG_INFO("Importing jentry idx{} from file {}", idx, p.string());
  if (filesize == 0) {
    throw std::runtime_error("jentry size too small");
  }
  if (filesize > 4096) {
    SPDLOG_WARN(
        "We do not support large log entries for correctness testing right "
        "now");
    throw std::runtime_error("jentry size too large");
  }

  uint64_t jblock_ctr = ++imported_journal_block_ctr_;
  // FIXME: assert that this block does not exceed journal limits. Hardly likely
  // as we won't be crash testing with so many files for now.
  uint64_t jentry_block = get_worker_journal_sb(0) + jblock_ctr;

  // NOTE: right now we just use one block at a time
  memset(bitmap_dev_mem, 0, 4096);
  SafeReadFile(p.c_str(), bitmap_dev_mem, 4096, /*sz_check*/ false);
  JournalEntry je = JournalEntry((uint8_t *)bitmap_dev_mem, BSIZE);
  je.deserializeBody((uint8_t *)bitmap_dev_mem, BSIZE);
  je.start_block = jentry_block;

  memset(bitmap_dev_mem, 0, 4096);
  uint64_t old_ts = PreserveTSAndSerialize(je, (uint8_t *)bitmap_dev_mem);

  writeNBlocks(jentry_block, 1, bitmap_dev_mem, /*blocking*/ true);

  // Always followed by a commit block. This is guaranteed by fsp. There cannot
  // be another journal entry starting where a commit block should be as we
  // allocate the entire slots for the entry including commit atomically.
  std::stringstream ss;
  ss << p.filename().string();
  ss << "_commit";
  auto commit_path = p.parent_path() / ss.str();
  ImportJournalEntryCommit(commit_path, je, old_ts);
}

void FsProcOfflineCheckpointer::ImportData(std::string &import_dir) {
  SPDLOG_INFO("Importing data from {}", import_dir);
  std::regex bitmap_block_re("^bmap_([0-9]+)$");
  std::regex inode_re("^inode_([0-9]+)$");
  std::regex jentry_re("^jentry_([0-9]+)$");
  std::regex jcommit_re("^jentry_[0-9]+_commit$");
  std::regex jsuper_re("^jsuper$");

  std::vector<uint64_t> jentries_to_import;
  std::vector<uint64_t> inodes_to_import;
  std::vector<uint64_t> bmaps_to_import;
  for (auto &p : std::filesystem::directory_iterator(import_dir)) {
    auto path = p.path();
    auto path_name = path.filename();
    auto name = path_name.string();

    std::smatch sm;
    std::regex_match(name, sm, bitmap_block_re);
    if (!sm.empty()) {
      bmaps_to_import.push_back(as_type<uint64_t>(sm[1]));
      continue;
    }

    std::regex_match(name, sm, inode_re);
    if (!sm.empty()) {
      inodes_to_import.push_back(as_type<uint64_t>(sm[1]));
      continue;
    }

    std::regex_match(name, sm, jentry_re);
    if (!sm.empty()) {
      jentries_to_import.push_back(as_type<uint64_t>(sm[1]));
      continue;
    }

    std::regex_match(name, sm, jsuper_re);
    if (!sm.empty()) continue;

    std::regex_match(name, sm, jcommit_re);
    if (!sm.empty()) continue;

    std::cout << path << ", name = " << name << ", unknown/skipped"
              << std::endl;
  }

  auto import_path = std::filesystem::path(import_dir);
  if (!jentries_to_import.empty()) {
    // make sure jsuper exists
    if (!std::filesystem::exists(import_path / "jsuper")) {
      SPDLOG_ERROR("JEntries exist but Jsuper does not exist, aborting");
      throw std::runtime_error("jsuper does not exist");
    }

    std::sort(jentries_to_import.begin(), jentries_to_import.end());

    // validation done, proceed to write to journal
    ImportJSuper(import_path / "jsuper");
    for (auto i : jentries_to_import) {
      std::stringstream ss;
      ss << "jentry_" << i;
      std::string fname = ss.str();
      ImportJournalEntry(import_path / fname);
    }
  }

  for (auto i : bmaps_to_import) {
    std::stringstream ss;
    ss << "bmap_" << i;
    std::string fname = ss.str();
    ImportBitmapBlock(import_path / fname, i);
  }

  for (auto i : inodes_to_import) {
    std::stringstream ss;
    ss << "inode_" << i;
    std::string fname = ss.str();
    ImportInode(import_path / fname, i);
  }

  SPDLOG_INFO(
      "Import completed successfully, wrote {} bmaps, {} inodes, {} journal "
      "entries to disk",
      bmaps_to_import.size(), inodes_to_import.size(),
      jentries_to_import.size());
}

void FsProcOfflineCheckpointer::dumpBlock(uint64_t block_num) {
  readNBlocks(block_num, 1, dev_mem, true);
  int fd = open("/tmp/dumpBlock", O_CREAT | O_WRONLY, 0755);
  ssize_t ssret = write(fd, dev_mem, 4096);
  if (ssret != 4096) {
    throw std::runtime_error("Failed to write");
  }
  int iret = close(fd);
  if (iret != 0) {
    throw std::runtime_error("Failed to close");
  }
}

void FsProcOfflineCheckpointer::inspect() {
  using namespace std;
  // Load all the journal supers
  InitJournalIterators();
  SPDLOG_INFO("------ Initial state of iterators...");
  DisplayJournalIterators();
  // Journal super maybe stale so we have to look past it
  // TODO: add flag to toggle whether to extend or not while inspecting
  ExtendJSupersToUnaccountedRegion();
  SPDLOG_INFO("Journal Inspect Output: ");
  int worker = -1;
  for (auto ji : journal_iterators_) {
    worker++;
    int jentry_idx = -1;
    JournalEntry *jentry = nullptr;
    while (!ji->Empty()) {
      jentry_idx++;
      jentry = ji->GetNextEntry();
      if (ji->PendingIO()) ji->WaitForCompletions();

      if (ji->HasIOError()) throw std::runtime_error("io reading journal");

      if (jentry == nullptr) continue;

      cout << "{\"grep_magic\": \"jinspect_json\","
           << "\"worker\":" << worker << ","
           << "\"journal_entry_idx\":" << jentry_idx << ","
           << "\"journal_entry\": " << jentry->as_json_str() << "}" << endl;

      for (auto &ile : jentry->ile_vec) {
        getInode(ile->inode);
      }
    }
  }

  // dump all inodes that were loaded
  cout << "Initial on-disk inodes" << endl;
  for (const auto &kv : sector_to_inode) {
    cfs_dinode *dinode = kv.second;
    cout << "Inode " << dinode->i_no << ", syncID " << dinode->syncID << endl;
  }

  SPDLOG_INFO("------ Final state of iterators...");
  DisplayJournalIterators();
  JournalIterator::LogMinAndMaxJEntrySizes(journal_iterators_);
}

static void doCheckpoint(bool export_blocks, std::string import_dir) {
  assert(g_namespaces != NULL);
  assert(g_qpair != NULL);
  auto chkptr = new FsProcOfflineCheckpointer(g_namespaces->ns, g_qpair);
  if (export_blocks) chkptr->SetExportBlocks(true);
  if (!import_dir.empty()) chkptr->ImportData(import_dir);
  chkptr->checkpoint();
  delete chkptr;
}

static void doInspect() {
  assert(g_namespaces != NULL);
  assert(g_qpair != NULL);
  // TODO: Structure checkpoint and inspection as separate classes in different
  // files while reusing common functionality.
  auto chkptr = new FsProcOfflineCheckpointer(g_namespaces->ns, g_qpair);
  chkptr->inspect();
  delete chkptr;
}

static void doDumpBlock(uint64_t block_num) {
  auto chkptr = new FsProcOfflineCheckpointer(g_namespaces->ns, g_qpair);
  chkptr->dumpBlock(block_num);
  delete chkptr;
}

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "FSP Journal Tool");
  auto opts = options.add_options();
  opts("i,inspect", "Journal Inspection Mode",
       cxxopts::value<bool>()->default_value("false"));
  opts("d,dump", "Dumps a blocks content", cxxopts::value<uint64_t>());
  opts("m,import_dir", "Imports state from folder before continuing",
       cxxopts::value<std::string>());
  opts("x,export",
       "Exports all content read and written by checkpointer into "
       "./chkpt_export",
       cxxopts::value<bool>()->default_value("false"));
  opts("h,help", "Print usage");

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  int rc = initSpdkEnv();
  if (rc != 0) {
    cleanup();
    return rc;
  }

  if (result.count("dump")) {
    uint64_t block = result["dump"].as<uint64_t>();
    doDumpBlock(block);
    return 0;
  }

  bool inspect = result["inspect"].as<bool>();
  if (inspect) {
    doInspect();
  } else {
    bool flag_export = result["export"].as<bool>();
    std::string import_dir;
    if (result.count("import_dir")) {
      import_dir = result["import_dir"].as<std::string>();
    }

    doCheckpoint(flag_export, import_dir);
  }

  // TODO use RAII so even when there is an error, cleanup is called..
  cleanup();
  return 0;
}
