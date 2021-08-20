#ifndef CFS_FSPAGECACHESHARED_H
#define CFS_FSPAGECACHESHARED_H

#include <assert.h>
#include <stdint.h>
#include <array>
#include <cstdio>
#include "param.h"
#include "typedefs.h"

typedef uint32_t page_idx_t;

#define PAGE_CACHE_PAGE_SIZE (4096)
#define N_PAGE_CACHE_BYTE (((uint64_t)8) * 1024)
#define N_PAGE_CACHE_PAGE ((N_PAGE_CACHE_BYTE) / PAGE_CACHE_PAGE_SIZE)

#define PAGE_CACHE_PAGE_STATUS_IDLE 0
#define PAGE_CACHE_PAGE_STATUS_USED 1

#define PAGE_DESCRIPTOR_MSG_MAGIC (20201111U)

// Describe the page as the message, don't need to pad it to cacheline
struct PageDescriptorMsg {
  page_idx_t gPageIdx;
  page_idx_t pageIdxWithinIno;
  int validSize;
  uint32_t MAGIC;
};

inline bool checkPdMsgMAGIC(PageDescriptorMsg *msg) {
  return msg->MAGIC == (PAGE_DESCRIPTOR_MSG_MAGIC);
}

static_assert(sizeof(struct PageDescriptorMsg) == 16,
              "PageDescriptor Message size error");

struct PageDescriptor {
  // absolute page idx
  page_idx_t gPageIdx;
  cfs_ino_t ino;
  page_idx_t pageIdxWithinIno;
  int validSize;  // number of bytes that contains valid data
  int pageStatus;
  char __padding[44];
};

static_assert(sizeof(struct PageDescriptor) == CACHE_LINE_BYTE,
              "PageDescriptor not cache aligned");

inline void print_pd(PageDescriptor *pd, void *ptr) {
  fprintf(stdout, " ino:%u gpgid:%u lpgid:%u vsz:%d data:%p\n", pd->ino,
          pd->gPageIdx, pd->pageIdxWithinIno, pd->validSize, ptr);
}

using pgc_page = std::array<char, PAGE_CACHE_PAGE_SIZE>;
struct PageCacheLayout {
  PageDescriptor pds[N_PAGE_CACHE_PAGE];
  pgc_page pages[N_PAGE_CACHE_PAGE];
};

static_assert(sizeof(PageCacheLayout) ==
                  N_PAGE_CACHE_PAGE * (PAGE_CACHE_PAGE_SIZE + CACHE_LINE_BYTE),
              "PageCacheLayout invalid");

void *attachPageCacheShm(bool isServer, int &shmFd);
void detachPageCacheShm(bool isServer, int shmFd, void *shmPtr);

inline PageDescriptor *fromPageIdxToPd(PageCacheLayout *layout,
                                       page_idx_t pgid) {
  return &(layout->pds[pgid]);
}

inline char *fromPageIdxToPagePtr(PageCacheLayout *layout, page_idx_t pgid) {
  assert(pgid < (N_PAGE_CACHE_PAGE));
  pgc_page *startptr = &(layout->pages[0]);
  return reinterpret_cast<char *>(startptr + pgid);
}

#endif
