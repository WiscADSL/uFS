#ifndef CFS_FSPROCPAGECACHE_H
#define CFS_FSPROCPAGECACHE_H

#include <array>
#include <unordered_map>
#include <vector>
#include "FsPageCache_Shared.h"
#include "param.h"
#include "tbb/concurrent_hash_map.h"
#include "tbb/concurrent_queue.h"

class InodePageCache {
 public:
  InodePageCache() {}
  void putToCache(page_idx_t inoPageIdx, page_idx_t globalPageIdx) {
    pageMap[inoPageIdx] = globalPageIdx;
  }
  int64_t getFromCache(page_idx_t inoPageIdx) {
    auto it = pageMap.find(inoPageIdx);
    if (it != pageMap.end()) {
      return it->second;
    }
    return -1;
  }

 private:
  // <page within inode, global page idx>
  std::unordered_map<page_idx_t, page_idx_t> pageMap;
};

class PageCacheManager {
 public:
  PageCacheManager();
  ~PageCacheManager();
  int allocPageToIno(int numPg,
                     std::vector<std::pair<PageDescriptor *, void *>> &pages,
                     cfs_ino_t ino);
  void freePage(std::vector<page_idx_t> &pages);

  PageDescriptor *findStablePage(page_idx_t pgid) {
    PageDescriptor *pdPtr = fromPageIdxToPd(layout, pgid);
    return pdPtr;
  }

 private:
  int initCacheShm();
  void destroyCacheShm();
  const static size_t kNoPageForCache = 100;

  PageCacheLayout *layout = nullptr;
  int shmFd = -1;
  void *shmPtr = nullptr;

  tbb::concurrent_queue<page_idx_t> freePages;
};

#endif