#include "FsProc_FsImpl.h"

void testDataBlockBuf() {
  const uint64_t kMemSize = ((uint64_t)NMEM_DATA_BLOCK) * (BSIZE);
  const int kNumPtt = 4;
  char *memPtr = (char *)malloc(kMemSize);
  char *pttMemPtr[kNumPtt];
  block_no_t pttNumBlocks = 0;
  for (int i = 0; i < kNumPtt; i++) {
    pttMemPtr[i] =
        FsImpl::getDataBlockMemPtrPartition(memPtr, kNumPtt, i, pttNumBlocks);
    fprintf(stderr, "pttMemPtr[%d] addr:%p pttNumBlocks:%u\n", i, pttMemPtr[i],
            pttNumBlocks);
  }
  for (int i = 1; i < kNumPtt; i++) {
    uint64_t addrDiff = (pttMemPtr[i] - pttMemPtr[i - 1]);
    uint64_t expectDiff = ((uint64_t)pttNumBlocks) * BSIZE;
    fprintf(stderr, "addrDiff:%lu expectDiff:%lu\n", addrDiff, expectDiff);
    if (expectDiff != addrDiff) {
      fprintf(stderr, "ERROR: diff not match\n");
    }
  }
  free(memPtr);
}

int main(int argc, char **argv) { testDataBlockBuf(); }