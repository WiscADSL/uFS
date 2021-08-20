#ifndef CFS_FSLIBPAGECACHE_H
#define CFS_FSLIBPAGECACHE_H

#include <unordered_map>
#include "FsPageCache_Shared.h"
#include "tbb/concurrent_hash_map.h"
#include "typedefs.h"

// we use inode number as filehandle
typedef cfs_ino_t file_handle_idx_t;

// This is not thread-safe
// Supposed to be guarded by fileHandle's read-write lock
class FileHandleCacheReader {
 public:
  FileHandleCacheReader() {}
  bool lookupRange(off_t alignedOff, size_t sizeByte, void *buf);
  void installCachePage(off_t off,
                        std::pair<PageDescriptor *, void *> &pdAddrPair);

 private:
  std::unordered_map<off_t, std::pair<PageDescriptor *, void *>> pageMap;
};

class PageCacheHelper {
 public:
  PageCacheHelper();
  ~PageCacheHelper();

  std::pair<PageDescriptor *, void *> findStablePage(page_idx_t pgid) {
    void *buf = fromPageIdxToPagePtr(layout, pgid);
    PageDescriptor *pdPtr = fromPageIdxToPd(layout, pgid);
    return {pdPtr, buf};
  }

 private:
  int shmFd = -1;
  void *shmPtr = nullptr;
  PageCacheLayout *layout = nullptr;

  int libAttachCacheShm();
  void libDetachCacheShm();
};

#endif