#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "FsLibPageCache.h"

bool FileHandleCacheReader::lookupRange(off_t offStart, size_t count,
                                        void *buf) {
  bool success = true;
  assert(count > 0);
  uint64_t off = offStart, tot;
  uint32_t m;
  uint64_t realBytes = count;
  char *dstPtr = (char *)buf;
  off_t alignedOff;
  // fprintf(stdout, "lookuprange off:%lu count:%lu pageMapSize:%lu\n", off,
  // count,
  //        pageMap.size());
  for (tot = 0; tot < realBytes; tot += m, off += m, dstPtr += m) {
    uint32_t blockIdx = off / PAGE_CACHE_PAGE_SIZE;
    alignedOff = ((off_t)blockIdx) * PAGE_CACHE_PAGE_SIZE;
    m = std::min(realBytes - tot,
                 PAGE_CACHE_PAGE_SIZE - off % PAGE_CACHE_PAGE_SIZE);
    auto it = pageMap.find(alignedOff);
    if (it != pageMap.end()) {
      // fprintf(stdout, "curoff found:%lu ", alignedOff);
      // print_pd((it->second).first, (it->second).second);
      memcpy(
          dstPtr,
          static_cast<char *>((it->second).second) + off % PAGE_CACHE_PAGE_SIZE,
          m);
    } else {
      success = false;
      break;
    }
  }
  return success;
}

void FileHandleCacheReader::installCachePage(
    off_t off, std::pair<PageDescriptor *, void *> &pdAddrPair) {
  // fprintf(stdout, "install off:%lu ", off);
  // print_pd((pdAddrPair).first, pdAddrPair.second);
  pageMap[off] = pdAddrPair;
}

PageCacheHelper::PageCacheHelper() {
  libAttachCacheShm();
  layout = reinterpret_cast<struct PageCacheLayout *>(shmPtr);
}

PageCacheHelper::~PageCacheHelper() { libDetachCacheShm(); }

int PageCacheHelper::libAttachCacheShm() {
  shmPtr = attachPageCacheShm(false, shmFd);
  return shmPtr == nullptr;
}

void PageCacheHelper::libDetachCacheShm() {
  detachPageCacheShm(false, shmFd, shmPtr);
}
