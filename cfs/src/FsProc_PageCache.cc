

#include "FsProc_PageCache.h"
#include <spdlog/spdlog.h>

#include <cstdio>

PageCacheManager::PageCacheManager() {
  initCacheShm();
  layout = reinterpret_cast<struct PageCacheLayout *>(shmPtr);
  struct PageDescriptor *pd;
  for (uint32_t i = 0; i < N_PAGE_CACHE_PAGE; i++) {
    pd = &(layout->pds[i]);
    pd->gPageIdx = i;
    pd->ino = 0;
    pd->pageStatus = (PAGE_CACHE_PAGE_STATUS_IDLE);
    freePages.push(i);
  }
}

PageCacheManager::~PageCacheManager() { destroyCacheShm(); }

int PageCacheManager::allocPageToIno(
    int numPg, std::vector<std::pair<PageDescriptor *, void *>> &pages,
    cfs_ino_t ino) {
  if (freePages.unsafe_size() < kNoPageForCache + numPg) {
    SPDLOG_ERROR("Cannot allocate page");
    return -1;
  }
  assert(numPg > 0);
  assert(pages.size() == 0);
  int numAllocated = 0;
  page_idx_t curPageIdx = 0;
  while (true) {
    bool success = freePages.try_pop(curPageIdx);
    if (success) {
      numAllocated++;
      pages.push_back({fromPageIdxToPd(layout, curPageIdx),
                       fromPageIdxToPagePtr(layout, curPageIdx)});
      pages.back().first->ino = ino;
      pages.back().first->pageStatus = PAGE_CACHE_PAGE_STATUS_USED;
    }
    if (numAllocated == numPg) break;
  }
  return 0;
}

void PageCacheManager::freePage(std::vector<page_idx_t> &pages) {
  for (auto pgid : pages) {
    freePages.push(pgid);
    auto pd = fromPageIdxToPd(layout, pgid);
    pd->ino = 0;
    pd->pageStatus = PAGE_CACHE_PAGE_STATUS_IDLE;
  }
}

int PageCacheManager::initCacheShm() {
  shmPtr = attachPageCacheShm(true, shmFd);
  return shmPtr == nullptr;
}

void PageCacheManager::destroyCacheShm() {
  detachPageCacheShm(true, shmFd, shmPtr);
}
