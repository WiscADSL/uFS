/*
 * NOTE: This file should only be imported by FsProc_Journal.h
 */

#include <sys/time.h>
#include <cstdint>

#include "BlkDevSpdk.h"
#include "FsProc_FsInternal.h"

// To avoid too many changes to source files, the InodeLogEntry still exists in
// the InMemInode. However, it is a pointer and can be NULL. The methods in the
// InodeLogEntry class are empty and will be optimized out by the compiler.
// However, this has only the methods that are used during write/create and not
// during fsync. We deliberatly remove the methods that may be used during
// fsync. FsProcWorker/FileMng/FsImpl should use flags and avoid calls. TODO
// more here..

class InMemInode;

class InodeLogEntry {
 public:
  InodeLogEntry(uint64_t inode, uint64_t syncID, InMemInode *minode) {}
  void set_mode(uint32_t mode) {}
  void set_uid(uint32_t uid) {}
  void set_gid(uint32_t gid) {}
  void set_block_count(uint32_t size) {}
  void set_bitmap_op(uint32_t bitval) {}
  void set_atime(struct timeval atime) {}
  void set_mtime(struct timeval mtime) {}
  void set_ctime(struct timeval ctime) {}
  void set_size(uint64_t size) {}
  void set_dentry_count(uint16_t dentry_count) {}
  void set_nlink(uint8_t nlink) {}
  void update_extent(struct cfs_extent *extent, bool add_or_del,
                     bool bmap_modified) {}
  void add_dependency(uint64_t inode, uint64_t syncID) {}
  bool empty() { return true; }
};

class JournalEntry {
 public:
  JournalEntry() {}
};

class JournalManager {
 public:
  JournalManager(JournalManager *primary_jmgr, uint64_t jsuper_blockno,
                 CurBlkDev *dev) {}
  void CopyBufToStableInodeBitmap(uint64_t bno, const char *buf) const {}
  void CopyBufToStableDataBitmap(uint64_t bno, const char *buf) const {}
};

class CheckpointInput {};
struct PrepareForCheckpointingCtx {};
struct ProposeForCheckpoingCtx {};
struct AssignCheckpointWorkerCtx {};
struct CheckpointingCompleteCtx {};

// BitmapChangeOps cannot be no-op as it is still used during unlink to dealloc
// blocks through message passing
struct BitmapChangeOps {
  std::vector<uint64_t> blocksToClear;
  std::vector<cfs_ino_t> inodesToClear;
};
