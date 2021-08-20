#ifndef __syncbatch_ctx_h
#define __syncbatch_ctx_h

#include <unordered_set>

#include "FsProc_Journal.h"

class FileMng;
class FsReq;

// TODO move this to a location for all FsReq contexts once we split up FsReq on
// a per request basis
class SyncBatchedContext {
 public:
  SyncBatchedContext(FileMng *mng, FsReq *req, size_t max_size);
  ~SyncBatchedContext();
  bool try_add(InodeLogEntry *ile);

 private:
  size_t max_size{0};
  size_t curLogEntrySize{0};

 public:
  FileMng *mng{nullptr};
  FsReq *req{nullptr};
  JournalEntry *jentry{nullptr};
  std::unordered_set<cfs_ino_t> inodes;
};

inline SyncBatchedContext::SyncBatchedContext(FileMng *mng, FsReq *req,
                                              size_t max_size)
    : max_size(max_size), mng(mng), req(req) {
  jentry = new JournalEntry();
  curLogEntrySize = jentry->calcBodySize();
}

inline SyncBatchedContext::~SyncBatchedContext() { delete jentry; }

inline bool SyncBatchedContext::try_add(InodeLogEntry *ile) {
  auto ile_size = ile->calcSize();
  if ((curLogEntrySize + ile_size) > max_size) {
    return false;
  }

  jentry->addInodeLogEntry(ile);
  curLogEntrySize += ile_size;
  auto minode = ile->get_minode();
  inodes.insert(minode->i_no);
  return true;
}

#endif  // __syncbatch_ctx_h
