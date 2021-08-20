#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "FsPageCache_Shared.h"

static const char *gPageCacheShmName = "fspPageCache";

// common functions for both client and server to use
// refers to shmipc.cc

void *attachPageCacheShm(bool isServer, int &shmFd) {
  void *shmPtr = nullptr;
  mode_t tmp_mask, old_mask;
  old_mask = umask((mode_t)0);
  int shmprot = PROT_READ;

  if (isServer) {
    shmFd = shm_open(gPageCacheShmName, O_RDWR | O_CREAT, 0666);
  } else {
    // shmFd = shm_open(gPageCacheShmName, O_RDWR, 0666);
    shmFd = shm_open(gPageCacheShmName, O_RDONLY, 0666);
  }
  tmp_mask = umask(old_mask);
  // simply avoid warning
  (void)(tmp_mask);

  if (shmFd < 0) goto error;

  if (isServer) {
    if (ftruncate(shmFd, sizeof(PageCacheLayout)) != 0) goto error;
  }

  if (isServer) shmprot |= PROT_WRITE;

  shmPtr = mmap(0, sizeof(PageCacheLayout), shmprot, MAP_SHARED, shmFd, 0);
  if (shmPtr == MAP_FAILED) goto error;
  if (isServer) memset(shmPtr, 0, sizeof(PageCacheLayout));
  fprintf(stdout, "========> attachPageCacheShm server?:%d=========>\n",
          isServer);
  return shmPtr;

error:
  fprintf(stderr, "attachPageCacheShm Fail. server?:%d\n", isServer);
  detachPageCacheShm(isServer, shmFd, shmPtr);
  return nullptr;
}

void detachPageCacheShm(bool isServer, int shmFd, void *shmPtr) {
  fprintf(stdout, "detachPageCacheShm\n");
  if (shmPtr != nullptr && shmPtr != MAP_FAILED) {
    munmap(shmPtr, sizeof(PageCacheLayout));
  }
  fprintf(stdout, "shmFd:%d\n", shmFd);
  if (shmFd >= 0) {
    close(shmFd);
    if (isServer) {
      fprintf(stdout, "server unlink\n");
      shm_unlink(gPageCacheShmName);
    }
  }
}
