#include <cassert>
#include <cstdint>

#include "spdlog/spdlog.h"

#include "FsProc_FileMng.h"
#include "FsProc_Fs.h"
#include "FsProc_FsImpl.h"
#include "FsProc_Journal.h"

// NOTE: Must be imported after FsProc_Journal.h
#include "FsProc_JSuper.h"

#if CFS_JOURNAL(PERF_METRICS)
// Required for dumping out the metrics
#include <fstream>
#include "nlohmann/json.hpp"
#endif

extern FsProc *gFsProcPtr;

// definitions for InodeLogEntry
// resets InodeLogEntry state and increments the syncID
void InodeLogEntry::on_successful_journal_write(CheckpointInput *ci,
                                                JournalEntry *je) {
  // Update the version to be checkpointed.
  // NOTE: if we do not allow any modifications while fsync in progress,
  // then we just have to copy inodeData instead of applying to jinodeData.
  std::unordered_map<uint64_t, bool> blocks_add_or_del;
  applyChangesTo(minode->jinodeData, blocks_add_or_del);
  minode->inodeData->syncID = minode->jinodeData->syncID;
  // TODO preallocate the vectors and use indices instead of push_back
  for (auto const &iter : blocks_add_or_del) {
    if (iter.second)
      je->blocks_for_bitmap_set.push_back(iter.first);
    else
      je->blocks_for_bitmap_clear.push_back(iter.first);
  }

  ci->inodesToCheckpoint[inode] = nullptr;
  if (optfield_bitarr & bitmap_op_IDX)
    je->inode_alloc_dealloc[static_cast<cfs_ino_t>(inode)] = (bitmap_op == 1);

  // Reset this entry.
  // NOTE: no need to reset individual fields as we always check flags
  optfield_bitarr = 0;
  extent_seq_no = 0;
  ext_add.clear();
  ext_del.clear();
  depends_on.clear();
  syncID++;
}

// definitions for JournalManager
JournalManager::JournalManager(JournalManager *primary_jmgr,
                               uint64_t jsuper_blockno, CurBlkDev *dev)
    : checkpointInput(nullptr),
      jsuper(nullptr),
      pinnedMemJournalSuper(nullptr),
      dev(dev),
      dev_ns(nullptr),
      dev_qpair(nullptr) {
  checkpointInput = new CheckpointInput();
  dev_ns = dev->getCurrentThreadNS();
  dev_qpair = dev->getCurrentThreadQPair();
  flush_supported = dev->flush_supported;
  if (flush_supported) fua_flag = SPDK_NVME_IO_FLAGS_FORCE_UNIT_ACCESS;

  AllocPinnedMem();

  // read the journal super if it is a nullptr, otherwise use the same one.
  if (primary_jmgr == nullptr) {
    blockingReadJournalSuper(jsuper_blockno);
    // TODO evaluate whether keeping a local jsuper instead of pinned is faster
    // for regular updates. Then, when we need to write it out, we can memcpy.
    jsuper = new JSuper((struct JSuperOnDisk *)pinnedMemJournalSuper);
    const struct JSuperOnDisk dj = jsuper->GetDiskRepr();
    if (dj.jmagic != JSUPER_MAGIC)
      throw std::runtime_error("jsuper magic mismatch");

    // NOTE: this is not a complete check. Consider the following sequence
    // mount, write, fsync, unmount. JSuper may not be written and n_used will
    // be 0.
    if (dj.n_used != 0)
      throw std::runtime_error(
          "journal has entries, please run fsOfflineCheckpointer");

    SPDLOG_INFO("Initialized JournalManager, jsuper at {}, capacity={}",
                dj.jsuper_blockno, jsuper->Capacity());
  } else {
    jsuper = primary_jmgr->jsuper;
    const struct JSuperOnDisk dj = jsuper->GetDiskRepr();
    SPDLOG_INFO("JournalManager reusing jsuper at {}", dj.jsuper_blockno);
  }

#if CFS_JOURNAL(PERF_METRICS)
  PerfUtils::Cycles::init();
  // Set the initial reference rdtscp value
  SetJournalPerfTS(ts_ref);
#endif
}

void JournalManager::AllocPinnedMem() {
  // FIXME: Not all transactions require kMaxJournalEntrySize of memory.
  // Pre-allocate large array and use chunks when required - maybe by forming an
  // extent tree. Alternatively, keep small sized chunks and use writev/readv.
  // BSIZE for journal super, and the rest for each transaction
  // TODO: maybe make this configurable runtime rather than build time
  static_assert(kMaxJournalEntrySize == (kMaxJournalBodySize + BSIZE));
  // We add an extra BSIZE in the beginning for the super block.
  constexpr size_t nbytes = BSIZE + (kMaxJournalEntrySize * kMaxInFlight_);
  char *ptr = (char *)dev->zmallocBuf(nbytes, BSIZE);
  if (ptr == NULL) {
    SPDLOG_ERROR("Failed to allocate {} bytes of pinned memory to journal",
                 nbytes);
    throw std::runtime_error("Failed to allocate journal memory");
  }

  SPDLOG_INFO("JournalManager: preallocated {} bytes", nbytes);

  pinnedMemJournalSuper = ptr;
  ptr += BSIZE;
  for (int i = 0; i < kMaxInFlight_; i++) {
    auto pinned_mem_ptr = new PinnedMemPtr();
    pinned_mem_ptr->jbody = ptr;
    ptr += JournalManager::kMaxJournalBodySize;
    pinned_mem_ptr->jcommit = ptr;
    ptr += BSIZE;

    pinned_mem_queue_.push(pinned_mem_ptr);
  }
}

JournalManager::~JournalManager() {
  if (!je_queue.empty()) {
    SPDLOG_ERROR("JournalManager: je_queue not empty!");
    return;
  }

  if (pinned_mem_queue_.size() != kMaxInFlight_) {
    SPDLOG_ERROR("JournalManager: pinned_mem_queue_ still being used");
    return;
  }

  while (!pinned_mem_queue_.empty()) {
    auto entry = pinned_mem_queue_.front();
    delete entry;
    pinned_mem_queue_.pop();
  }

  if (pinnedMemJournalSuper == nullptr) return;

  int rt = dev->freeBuf(pinnedMemJournalSuper);
  if (rt != 0) {
    SPDLOG_ERROR("JournalManager: failed to free pinned memory");
    return;
  }

  pinnedMemJournalSuper = nullptr;

  if (jsuper != nullptr) {
#if CFS_JOURNAL(LOCAL_JOURNAL)
    delete jsuper;
    jsuper = nullptr;
#else
    // TODO: Only primary will delete jsuper
#endif
  }

  delete checkpointInput;
}

#if CFS_JOURNAL(PERF_METRICS)
void JournalManager::dumpJournalPerfMetrics(const std::string &filename) {
  SPDLOG_INFO("Dumping journal performance metrics to {}", filename);

  if (metrics_ctr == 0) {
    SPDLOG_INFO("no journal performance metrics recorded");
    return;
  }

  if (metrics_ctr > metrics_max_capacity) {
    SPDLOG_WARN(
        "Some metrics may have been overwritten. Capacity is {} but we have "
        "recorded {} entries",
        metrics_max_capacity, metrics_ctr);
  }

  std::ofstream dumpf;
  dumpf.open(filename, std::ios::app);
  if (!dumpf.is_open()) {
    SPDLOG_ERROR("Failed to open file {}", filename);
    return;
  }

  /*
   * All timestamps are relative to ts_ref. Therefore, we can calculate when a
   * request arrived into the journal by ts_queued_start - ts_ref. This gives us
   * nanoseconds from the startup of the process. ts_write_body_start -
   * ts_queued_start is the time spent in the queue and so on.
   */

  auto it = metrics_list.begin();
  size_t i = 0;
  SPDLOG_INFO("metrics_list capacity {}, size {}, metrics_ctr {}",
              metrics_list.capacity(), metrics_list.size(), metrics_ctr);
  while ((it != metrics_list.end()) && (i < metrics_ctr)) {
    nlohmann::json jsub;
#define calcNS PerfUtils::Cycles::toNanoseconds
#define recordMetricInJSON(x) jsub[#x] = calcNS(it->x - ts_ref)
    recordMetricInJSON(ts_queued_start);
    recordMetricInJSON(ts_write_body_start);
    recordMetricInJSON(ts_flush_start);
    recordMetricInJSON(ts_commit_start);
    recordMetricInJSON(ts_callback_start);
    recordMetricInJSON(ts_callback_stop);
#undef recordMetricsAsJson
#undef calcNS
    dumpf << jsub << std::endl;
    it++;
    i++;
  }

  dumpf.flush();
  dumpf.close();
}
#endif  // CFS_JOURNAL(PERF_METRICS)

void JournalManager::blockingReadJournalSuper(uint64_t jsuper_blockno) {
  int rt = dev->blockingRead(jsuper_blockno, pinnedMemJournalSuper);
  if (rt < 0) {
    SPDLOG_ERROR("JournalManager: failed to read jsuper ({})", rt);
    throw std::runtime_error("I/O Error: Failed to read journal super");
  }
  // TODO verify by checking magic and the block no
}

void JournalManager::submitJournalEntry(JournalEntry *je, JournalCallbackFn cb,
                                        void *cb_arg) {
  SetJournalPerfTS(cur_metric.ts_queued_start);
  je->cb = cb;
  je->cb_arg = cb_arg;
  je->mgr = this;
  je_queue.push(je);
  tryNextTransaction();
  float num_used = (float)jsuper->Size();
  float used_ratio = num_used / ((float)jsuper->Capacity());
  if (used_ratio > JournalManager::kJournalCheckpointRatio) {
#if CFS_JOURNAL(CHECKPOINTING)
    SPDLOG_INFO(
        "used {} blocks, ratio {}  > checkpoint ratio {}, proposing "
        "checkpointing",
        num_used, used_ratio, JournalManager::kJournalCheckpointRatio);

    gFsProcPtr->proposeJournalCheckpointing();
#else
    jsuper->ReleaseFirstNSlots(num_used);
    jsuper->SetLastCheckpoint((uint64_t)tap_ustime());
    WriteJSuper();
#endif
  }
}

void JournalManager::AcquirePinnedMem(JournalEntry *je) {
  assert(!pinned_mem_queue_.empty());
  assert(je->pinned_mem_ == nullptr);

  je->pinned_mem_ = pinned_mem_queue_.front();
  pinned_mem_queue_.pop();
}

void JournalManager::ReleasePinnedMem(JournalEntry *je) {
  if (je->pinned_mem_ == nullptr) return;
  assert(pinned_mem_queue_.size() < MAX_INFLIGHT_JOURNAL_TRANSACTIONS);

  // NOTE: not zeroing out the memory as we always encode how many bytes to
  // read/write.
  pinned_mem_queue_.push(je->pinned_mem_);
  je->pinned_mem_ = nullptr;
}

void JournalManager::WriteJSuper() {
  struct JSuperOnDisk dj;
  uint64_t nblocks_unaccounted;

  if (!jsuper->TrySetWriteInProgress(dj, nblocks_unaccounted)) return;

  SPDLOG_DEBUG("Writing jsuper, nblocks_unaccounted = {}", nblocks_unaccounted);

  struct JSuperWriteCtx {
    JournalManager *jmgr;
    uint64_t nblocks_unaccounted;
  };

  auto jsuper_write_complete = [](void *arg,
                                  const struct spdk_nvme_cpl *completion) {
    auto ctx = static_cast<struct JSuperWriteCtx *>(arg);
    auto jmgr = ctx->jmgr;

    if (spdk_nvme_cpl_is_error(completion))
      throw std::runtime_error("Failed to write journal superblock");

    jmgr->jsuper->OnWriteComplete(ctx->nblocks_unaccounted);
    delete ctx;

    // other transactions may have been throttled due to this write so we can
    // start them again.
    // NOTE: since we throttle n concurrent transactions, we should try atleast
    // n concurrent ones. tryNextTransaction will return early if it cannot try
    // the transaction so its not a costly operation. Additionally, internally,
    // tryNextTransaction checks if nblocks_unaccounted is acceptable. So incase
    // we had a lot of journal writes while this jsuper was being written, it
    // will issue another write to jsuper.
    int i = 0;
    while (!(jmgr->je_queue.empty() || jmgr->pinned_mem_queue_.empty())) {
      jmgr->tryNextTransaction();
      i++;
    }

    // Incase we have no new transactions but jsuper was stale by the time this
    // write completed, we need to write it again.
    if ((i == 0) && jmgr->jsuper->ShouldWriteJSuper()) jmgr->WriteJSuper();
  };

  struct JSuperWriteCtx *ctx = new struct JSuperWriteCtx();
  ctx->jmgr = this;
  ctx->nblocks_unaccounted = nblocks_unaccounted;
  *((struct JSuperOnDisk *)(pinnedMemJournalSuper)) = dj;

  int rc = spdk_nvme_ns_cmd_write(dev_ns, dev_qpair, pinnedMemJournalSuper,
                                  dj.jsuper_blockno * 8, 8,
                                  jsuper_write_complete, ctx, fua_flag);
  if (rc != 0) {
    SPDLOG_ERROR(
        "JournalManager: failed to submit jsuper write request, errno={}", rc);
    // TODO: failing for now. If spdk_nvme_ns_cmd_write does fail often due to
    // high load, then we can fix this path.
    throw std::runtime_error("Failed to write journal superblock");
  }
}

void JournalManager::processJournalEntry(JournalEntry *je) {
  int rc;
  switch (je->state) {
    case JournalEntryState::FAILURE:
      SetJournalPerfTS(cur_metric.ts_callback_start);
      // TODO more info about where it failed - in which state?
      SPDLOG_DEBUG("JournalEntryState::FAILURE");
      ReleasePinnedMem(je);
      if (je->cb != nullptr) (*(je->cb))(je->cb_arg, false);
      SetJournalPerfTS(cur_metric.ts_callback_stop);
      RecordJournalPerfMetrics();
      tryNextTransaction();
      break;
    case JournalEntryState::SUCCESS:
      SetJournalPerfTS(cur_metric.ts_callback_start);
      SPDLOG_DEBUG("JournalEntryState::SUCCESS");
      for (InodeLogEntry *ile : je->ile_vec) {
        ile->on_successful_journal_write(checkpointInput, je);
      }
      ReleasePinnedMem(je);
      if (je->cb != nullptr) (*(je->cb))(je->cb_arg, true);
      SetJournalPerfTS(cur_metric.ts_callback_stop);
      RecordJournalPerfMetrics();
      tryNextTransaction();
      break;
    case JournalEntryState::WRITE_BODY:
      SetJournalPerfTS(cur_metric.ts_write_body_start);
      SPDLOG_DEBUG("JournalEntryState::WRITE_BODY");
      je->serializeBody((uint8_t *)je->pinned_mem_->jbody);
      // TODO consider submitting data in chunks of 4K even if sequential.
      // According to Jing's experiments, 4x4K > 1x16K.
      // TODO Change the constant 8 to a variable from param.h
      assert(je->nblocks >= 2);
      rc = spdk_nvme_ns_cmd_write(dev_ns, dev_qpair, je->pinned_mem_->jbody,
                                  je->start_block * 8, (je->nblocks - 1) * 8,
                                  writeComplete, je, 0);
      if (rc != 0) {
        // TODO (Jing) BlkDevSpdk.cc:252 doesn't check rc. Should I?
        // Will the callback function always be called? I couldn't find
        // this anywhere in the doc...
        SPDLOG_ERROR("JournalManager: failed to submit write request, errno={}",
                     rc);
        // NOTE: we should not just be logging these
        // If this does become an issue in our workloads, this needs to be fixed
        // by pausing the journal entry and submitting later.
        throw std::runtime_error("Failed to submit body write request");
      }

      break;
    case JournalEntryState::FLUSH:
      SetJournalPerfTS(cur_metric.ts_flush_start);
      SPDLOG_DEBUG("JournalEntryState::FLUSH");
      if (flush_supported) {
        // FIXME: keep track of how many journal entries completed before this
        // flush was issued so that we don't have to issue flush for every
        // single journal entry, and can batch for some that are in flight.
        rc = spdk_nvme_ns_cmd_flush(dev_ns, dev_qpair, writeComplete, je);
        if (rc != 0) {
          SPDLOG_ERROR(
              "JournalManager: failed to submit flush request, errno={}", rc);
          throw std::runtime_error("Failed to submit flush request");
        }
        break;
      } else {
        je->state = JournalEntryState::WRITE_COMMIT;
        SPDLOG_DEBUG("Fallthrough from FLUSH to WRITE_COMMIT");
        // NOTE no break here. We want to fall through.
      }
    case JournalEntryState::WRITE_COMMIT: {
      SetJournalPerfTS(cur_metric.ts_commit_start);
      SPDLOG_DEBUG("JournalEntryState::WRITE_COMMIT");
      je->serializeCommit((uint8_t *)je->pinned_mem_->jcommit);
      uint64_t commit_block = je->start_block + je->nblocks - 1;
      rc = spdk_nvme_ns_cmd_write(dev_ns, dev_qpair, je->pinned_mem_->jcommit,
                                  commit_block * 8, 8, writeComplete, je,
                                  fua_flag);
      if (rc != 0) {
        SPDLOG_ERROR(
            "JournalManager: failed to submit commit request, errno={}", rc);
        throw std::runtime_error("Failed ot submit commit write request");
      }
    } break;
    default:
      assert(false);
  }
}

void JournalManager::tryNextTransaction() {
  // something already in progress / nothing new to process
  if (je_queue.empty() || pinned_mem_queue_.empty()) return;

  JournalEntry *je = je_queue.front();
  je->start_block = 0;
  je->nblocks = 0;

  size_t je_body_size = je->calcBodySize();
  uint64_t je_body_nblocks = (je_body_size + BSIZE - 1) / BSIZE;
  uint64_t jblocks_required = je_body_nblocks + 1;  // body + commit
  constexpr uint64_t max_journal_entry_blocks = kMaxJournalEntrySize / BSIZE;

  if (jblocks_required > max_journal_entry_blocks) {
    SPDLOG_ERROR(
        "JournalManager: journal entry exceeds maximum number of blocks {}",
        jblocks_required);
    je->state = JournalEntryState::FAILURE;
    goto end;
  }

  {
    auto slot = jsuper->AcquireNextNSlots(jblocks_required);
    if (slot.no_space) {
      je->state = JournalEntryState::FAILURE;
      goto end;
    }

    if (slot.retry_after_jsuper_written) return;
    if (slot.write_jsuper) WriteJSuper();

    assert(jsuper->NoLoopArounds(slot.start_idx, jblocks_required));
    je->start_block = jsuper->IdxToBlockNo(slot.start_idx);
    je->nblocks = jblocks_required;
    je->state = JournalEntryState::WRITE_BODY;
    AcquirePinnedMem(je);
  }

end:
  je_queue.pop();
  processJournalEntry(je);
}

// called whenever a journal write completes
void JournalManager::writeComplete(void *arg,
                                   const struct spdk_nvme_cpl *completion) {
  JournalEntry *je = (JournalEntry *)arg;
  JournalManager *mgr = je->getManager();

  if (spdk_nvme_cpl_is_error(completion)) {
    // TODO add more information about which state this failure occurred...
    je->state = JournalEntryState::FAILURE;
    goto end;
  }

  switch (je->state) {
    case JournalEntryState::WRITE_BODY:
      je->state = JournalEntryState::FLUSH;
      break;
    case JournalEntryState::FLUSH:
      je->state = JournalEntryState::WRITE_COMMIT;
      break;
    case JournalEntryState::WRITE_COMMIT:
      je->state = JournalEntryState::SUCCESS;
      break;
    default:
      assert(false);
  }
end:
  if (kEnableNvmeWriteStats) {
    mgr->n_num_nvme_write_done_++;
  }
  mgr->processJournalEntry(je);
}

void JournalManager::prepareForCheckpointing(CheckpointInput **dst,
                                             FileMng *mgr) {
  // TODO if we have millions of inodes that need to be checkpointed, we will
  // have to perform them in batches.
  auto numInodes = (checkpointInput->inodesToCheckpoint).size();
  auto numDataBitmaps = (checkpointInput->bitmapsToCheckpoint).size();
  auto numInodeBitmaps = (checkpointInput->inodeBitmapsToCheckpoint).size();
  auto numBitmaps = numDataBitmaps + numInodeBitmaps;

  assert(checkpointInput->inodeDevMem == NULL);
  assert(checkpointInput->bitmapDevMem == NULL);

  if (numInodes != 0) {
    // NOTE: assumes each inode occupies a single sector and that I can
    // send each sector to the nvme device
    checkpointInput->inodeDevMem =
        (void *)dev->zmallocBuf(numInodes * SSD_SEC_SIZE, SSD_SEC_SIZE);
    if (checkpointInput->inodeDevMem == NULL) {
      SPDLOG_ERROR("Failed to alloc inodeDevMem");
      goto failure;
    }
  }

  if (numBitmaps != 0) {
    checkpointInput->bitmapDevMem =
        (void *)dev->zmallocBuf(numBitmaps * BSIZE, BSIZE);
    if (checkpointInput->bitmapDevMem == NULL) {
      SPDLOG_ERROR("Failed to alloc bitmapDevMem");
      goto failure;
    }
  }

  populateCheckpointInput(mgr);
  // NOTE: we make a new checkpointInput so that this one can be sent to the
  // checkpointer. While checkpoint is in progress, workers can use the new
  // checkpointer. Incase of failure, we will merge the two.
  *dst = checkpointInput;
  checkpointInput = new CheckpointInput();
  checkpointInProgress = true;
  return;
failure:
  if (checkpointInput->inodeDevMem != NULL) {
    dev->freeBuf(checkpointInput->inodeDevMem);
    checkpointInput->inodeDevMem = NULL;
  }
  if (checkpointInput->bitmapDevMem != NULL) {
    dev->freeBuf(checkpointInput->bitmapDevMem);
    checkpointInput->bitmapDevMem = NULL;
  }
  *dst = NULL;
}

void JournalManager::populateCheckpointInput(FileMng *mgr) {
  // private function called by prepareForCheckpointing after device memory has
  // been allocated. This function saves copies of inodes & bitmaps to be
  // checkpointed in the device memory.
  auto dinodes = (struct cfs_dinode *)checkpointInput->inodeDevMem;
  InMemInode *minode = nullptr;
  for (auto &it : checkpointInput->inodesToCheckpoint) {
    minode = mgr->GetInMemInode(it.first);
    // NOTE: whenever an inode migrates from W1 to W2, we remove it from
    // W1.inodesToCheckpoint and add it to W2.inodesToCheckpoint.
    // the debug mode will check this for correctness.
    // TODO (jing) is cfsGetTid() the same as workerId?
    assert(minode->getManageWorkerId() == cfsGetTid());
    memcpy(dinodes, minode->jinodeData, sizeof(struct cfs_dinode));
    it.second = dinodes;
    dinodes++;
  }

  char *ptr = (char *)checkpointInput->bitmapDevMem;
  for (auto &it : checkpointInput->bitmapsToCheckpoint) {
    assert((getWidForBitmapBlock(it.first) == cfsGetTid()));
    char *stable_bmap = mgr->fsWorker_->jmgr->GetStableDataBitmap(it.first);
    assert(stable_bmap != nullptr);
    memcpy(ptr, stable_bmap, BSIZE);
    it.second = ptr;
    ptr += BSIZE;
  }

  for (auto &it : checkpointInput->inodeBitmapsToCheckpoint) {
    // loop should run only on primary
    assert(cfsGetTid() == 0);
    char *stable_bmap = mgr->fsWorker_->jmgr->GetStableInodeBitmap(it.first);
    assert(stable_bmap != nullptr);
    memcpy(ptr, stable_bmap, BSIZE);
    it.second = ptr;
    ptr += BSIZE;
  }

  // Required because we will need to change the circular buffer's start & end
  // if checkpoint is successful.
  checkpointInput->n_used = jsuper->Size();
}

void JournalManager::cleanupCheckpointInput(CheckpointInput *ci) {
  if (ci->inodeDevMem != NULL) dev->freeBuf(ci->inodeDevMem);

  if (ci->bitmapDevMem != NULL) dev->freeBuf(ci->bitmapDevMem);

  delete ci;
}

void JournalManager::onCheckpointSuccess(CheckpointInput *src, FileMng *mgr) {
  jsuper->ReleaseFirstNSlots(src->n_used);
  jsuper->SetLastCheckpoint((uint64_t)tap_ustime());
  WriteJSuper();

  cleanupCheckpointInput(src);
  checkpointInProgress = false;

#if CFS_JOURNAL(LOCAL_JOURNAL)
  mgr->fsImpl_->immutableBlockBitmaps_.swap(
      bmapsToLockAfterCheckpointCompletes);
  bmapsToLockAfterCheckpointCompletes.clear();
#endif
}

void JournalManager::onCheckpointFailure(CheckpointInput *src) {
  // TODO flag to indicate that checkpoint not in progress anymore
  // When preparing for checkpoint, we created new checkpoint input for inodes
  // that are fsync'd when checkpoint is in progress. Those need to be updated.
  // TODO can be smarter about this and choose to update the larger of the two
  for (auto const it : src->inodesToCheckpoint)
    checkpointInput->inodesToCheckpoint[it.first] = nullptr;

  for (auto const it : src->bitmapsToCheckpoint)
    checkpointInput->bitmapsToCheckpoint[it.first] = nullptr;

  cleanupCheckpointInput(src);
  checkpointInProgress = false;
#if CFS_JOURNAL(LOCAL_JOURNAL)
  // NOTE: since we do not unlock any of the bitmaps that were locked, we can
  // just clear the bmapsToLockAfterCheckpointCompletes without actually
  // locking.
  bmapsToLockAfterCheckpointCompletes.clear();
#endif
}

void JournalManager::notifyAllProcsOnCheckpointComplete(
    FileMng *mng, std::vector<FsProcWorker *> &workers, size_t n_workers,
    size_t this_worker_idx, CheckpointInput **per_worker_ci, bool success) {
  FsProcWorker *wkr_ptr = nullptr;
  FsProcMessage msg;
  msg.type = FsProcMessageType::CHECKPOINTING_COMPLETE;
  for (size_t i = 0; i < n_workers; i++) {
    if (i == this_worker_idx) continue;

    // NOTE: allocating on the heap as the worker may process this message after
    // this function has returned. We may want to add another variable to the
    // ctx to wait for all workers to say they've completed - and only then
    // persist the jsuper blocks.
    auto ctx = new CheckpointingCompleteCtx();
    ctx->ci = per_worker_ci[i];
    ctx->success = success;
    msg.ctx = (void *)ctx;
    wkr_ptr = workers[i];
    wkr_ptr->messenger->send_message(wkr_ptr->getWorkerIdx(), msg);
  }

  // TODO: Fault tolerance when checkpointing has succeeded and we are updating
  // the journal superblocks. Consider the scenario where we crash just after a
  // few workers update their journal superblocks because checkpoint succeeded.
  // On recovery, we don't have access to the journal entries of checkpointed
  // journals. But the ones that weren't able to save their superblocks would
  // try replaying and may be stuck if they depend on entries from the
  // checkpointed journal. Actually, they won't be stuck because the
  // checkpointed inode would have a higher syncID so it would meet dependencies
  // and we may be able to ignore it. But for simplicity, it is easier to do
  // this in two stages. First write all the new journal superblocks to some
  // location successfully (possibly with a checkpoint_epoch incremented). Then
  // update every other one. On restart, we will be able to get the correct
  // state.
  if (success) {
    onCheckpointSuccess(per_worker_ci[this_worker_idx], mng);
  } else {
    onCheckpointFailure(per_worker_ci[this_worker_idx]);
  }
}

void JournalManager::waitForCheckpointWriteCompletions() {
  while (n_checkpoint_requests_issued != n_checkpoint_requests_completed) {
    // check for completions
    // TODO: check returncode for errors
    spdk_nvme_qpair_process_completions(dev_qpair, 0);
  }
}

// blocking function call that checkpoints all journals.
void JournalManager::checkpointAllJournals(FileMng *mng,
                                           std::vector<FsProcWorker *> &workers,
                                           size_t n_workers,
                                           size_t this_worker_idx) {
  // TODO possibly just use one struct instead of these three for this code. It
  // could serve as the ctx as well
  PrepareForCheckpointingCtx ctx[n_workers];
  CheckpointInput *per_worker_ci[n_workers];
  volatile bool per_worker_completions[n_workers];

  FsProcWorker *wkr_ptr = nullptr;
  FsProcMessage msg;
  msg.type = static_cast<uint8_t>(PREPARE_FOR_CHECKPOINTING);
  for (size_t i = 0; i < n_workers; i++) {
    per_worker_ci[i] = nullptr;
    per_worker_completions[i] = false;
    if (i == this_worker_idx) continue;  // skip this thread

    ctx[i].ci = &(per_worker_ci[i]);
    ctx[i].completed = &(per_worker_completions[i]);

    msg.ctx = (void *)(&(ctx[i]));
    wkr_ptr = workers[i];
    wkr_ptr->messenger->send_message(wkr_ptr->getWorkerIdx(), msg);
  }

  // TODO: make async version where they all send messages back to this worker.
  prepareForCheckpointing(&(per_worker_ci[this_worker_idx]), mng);
  per_worker_completions[this_worker_idx] = true;

  size_t num_successful = 0;
  for (size_t i = 0; i < n_workers; i++) {
    while (per_worker_completions[i] == false) continue;

    if (per_worker_ci[i] != nullptr) num_successful++;
  }

  if (num_successful != n_workers) {
    SPDLOG_ERROR("Some workers failed to prepare for checkpointing");
    notifyAllProcsOnCheckpointComplete(mng, workers, n_workers, this_worker_idx,
                                       per_worker_ci, false);
    return;
  }

  n_checkpoint_requests_issued = 0;
  n_checkpoint_requests_completed = 0;
  n_checkpoint_requests_failed = 0;

  // For each worker, we first write out the inodes and then the bitmaps.
  for (size_t i = 0; i < n_workers; i++) {
    CheckpointInput *ci = per_worker_ci[i];
    for (auto it = ci->inodesToCheckpoint.cbegin();
         it != ci->inodesToCheckpoint.cend();) {  // increment on success
      uint32_t disk_sector_no = FsImpl::ino2SectorNo(it->first);
      // TODO check rc to see if queue is full. Wait for completions before
      // sending any more
      int rc =
          spdk_nvme_ns_cmd_write(dev_ns, dev_qpair, it->second, disk_sector_no,
                                 1, checkpointWriteComplete, this, 0);
      if (rc == 0) {
        n_checkpoint_requests_issued++;
        it++;
        continue;
      }

      if (rc != -ENOMEM) {
        SPDLOG_ERROR(
            "Failed to submit inode write request, errno={}, skipping it", rc);
        it++;  // TODO : maybe fail checkpointing entirely?
        continue;
      }

      // rc is -ENOMEM, need to wait till a few have finished and then resume
      waitForCheckpointWriteCompletions();
      // NOTE: not incrementing it, trying again for the same failed inode
    }

    std::unordered_map<uint64_t, void *> bitmap_block_to_data_arr[2];
    bitmap_block_to_data_arr[0] = ci->bitmapsToCheckpoint;
    bitmap_block_to_data_arr[1] = ci->inodeBitmapsToCheckpoint;
    for (auto &bitmap_block_to_data : bitmap_block_to_data_arr) {
      for (auto it = bitmap_block_to_data.cbegin();
           it != bitmap_block_to_data.cend();) {  // increment on success
        // TODO (jing) instead of 8, would BSIZE / SSD_SEC_SIZE be correct for
        // lba_count?
        int rc =
            spdk_nvme_ns_cmd_write(dev_ns, dev_qpair, it->second, it->first * 8,
                                   8, checkpointWriteComplete, this, 0);
        if (rc == 0) {
          n_checkpoint_requests_issued++;
          it++;
          continue;
        }

        if (rc != -ENOMEM) {
          SPDLOG_ERROR(
              "Failed to submit bitmap write request, errno={}, skipping it",
              rc);
          it++;  // TODO : maybe fail checkpointing entirely?
          continue;
        }

        waitForCheckpointWriteCompletions();
        // NOTE: not incrementing it, trying again for same failed bitmap
      }
    }
  }

  // TODO all blocking waits for io completions can be made asynchronous, where
  // the worker still services requests and on process completions, we come back
  // into the checkpointer
  waitForCheckpointWriteCompletions();

  if (n_checkpoint_requests_failed > 0) {
    SPDLOG_ERROR("Checkpoint failued: I/O errors");
    notifyAllProcsOnCheckpointComplete(mng, workers, n_workers, this_worker_idx,
                                       per_worker_ci, false);
    return;
  }

  // Checkpoint succeeded, notify of success
  SPDLOG_INFO("Checkpoint completed successfully");
  notifyAllProcsOnCheckpointComplete(mng, workers, n_workers, this_worker_idx,
                                     per_worker_ci, true);
}

void JournalManager::checkpointWriteComplete(
    void *arg, const struct spdk_nvme_cpl *completion) {
  JournalManager *mgr = (JournalManager *)arg;
  mgr->n_checkpoint_requests_completed =
      mgr->n_checkpoint_requests_completed + 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    mgr->n_checkpoint_requests_failed = mgr->n_checkpoint_requests_failed + 1;
  }
}
