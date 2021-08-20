#include "FsLibMalloc.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "stats/stats.h"
#include "util.h"

FsLibLinkedList::~FsLibLinkedList() {
  while (length_ > 0) {
    auto item = head_;
    removeBlockStart();
#ifdef FS_LIB_MALLOC_DEBUG
    fprintf(stdout, "~FsLibLinkedList delete item:%p\n", item);
#endif
    delete item;
  }
}

FsLibLLItem *FsLibLinkedList::getBlockAt(fslib_malloc_block_cnt_t position) {
  FsLibLLItem *current = head_;
  if (position > 0) {
    for (uint i = 0; i < position; i++) {
      current = current->next;
    }
  }
  return current;
}

void FsLibLinkedList::addBlockStart(MemBlockMeta *meta) {
  auto newBlock = new FsLibLLItem;
  newBlock->meta = meta;
  newBlock->next = nullptr;
  if (length_ == 0) {
    head_ = newBlock;
    tail_ = newBlock;
  } else {
    newBlock->next = head_;
    head_ = newBlock;
  }
  length_++;
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "addBlockStart newItem addr:%p length:%u\n", newBlock,
          length_);
#endif
}

void FsLibLinkedList::addBlockEnd(MemBlockMeta *meta) {
  auto newBlock = new FsLibLLItem;
  newBlock->meta = meta;
  newBlock->next = nullptr;
  if (length_ == 0) {
    head_ = newBlock;
    tail_ = newBlock;
  } else {
    auto secondBlock = getBlockAt((length_ == 1) ? 0 : length_ - 1);
    secondBlock->next = newBlock;
    tail_ = newBlock;
  }
  length_++;
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "addBlockEnd newItem addr:%p length:%u\n", newBlock, length_);
#endif
}

void FsLibLinkedList::addBlockAt(fslib_malloc_block_cnt_t position,
                                 MemBlockMeta *meta) {
  if (length_ == 0 || position == 0) {
    addBlockStart(meta);
  } else {
    auto newBlock = new FsLibLLItem;
    newBlock->meta = meta;
    auto beforeBlock = getBlockAt(position - 1);
    newBlock->next = beforeBlock->next;
    beforeBlock->next = newBlock;
    length_++;
#ifdef FS_LIB_MALLOC_DEBUG
    fprintf(stdout, "addBlockAt newItem addr:%p length:%u\n", newBlock,
            length_);
#endif
  }
}

void FsLibLinkedList::removeBlockStart() { removeBlockAt(0); }

void FsLibLinkedList::removeBlockEnd() { removeBlockAt(length_ - 1); }

void FsLibLinkedList::removeBlockAt(fslib_malloc_block_cnt_t position) {
  if (length_ != 0 && position < length_) {
    auto removeBlock = getBlockAt(position);
    if (length_ == 1) {
      head_ = nullptr;
      tail_ = nullptr;
    } else {
      if (position == 0) {
        head_ = removeBlock->next;
      } else {
        auto blockBefore = getBlockAt(position - 1);
        blockBefore->next = removeBlock->next;
        if (position == length_ - 1) {
          tail_ = blockBefore;
        }
      }
    }
    length_--;
#ifdef FS_LIB_MALLOC_DEBUG
    fprintf(stdout, "FsLibLinkedList::removeBlockAt(%u) length:%u\n", position,
            length_);
#endif
  }
}

void FsLibLinkedList::printToStdout() {
  FsLibLLItem *it = head_;
  int i = 0;
  while (it != nullptr) {
    fprintf(stdout, " -pos:%d metaIdx:%u", i, it->meta->shmInnerId);
    it = it->next;
    i++;
  }
  fprintf(stdout, "\n");
}

fslib_malloc_mem_sz_t SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(
    fslib_malloc_block_sz_t blockSize, fslib_malloc_block_cnt_t numBlocks) {
  uint64_t headerSz, dataSz;
  // metadata header size
  headerSz = computeBlockArrShmHeaderBytes(numBlocks);
  dataSz = blockSize * numBlocks;
  return headerSz + dataSz;
}

uint64_t SingleSizeMemBlockArr::computeBlockArrShmHeaderBytes(
    fslib_malloc_block_cnt_t numBlocks) {
  return ROUND_UP(sizeof(MemBlockMeta) * numBlocks, (kHeaderAlignBytes));
}

SingleSizeMemBlockArr::SingleSizeMemBlockArr(fslib_malloc_block_sz_t blockSize,
                                             fslib_malloc_block_cnt_t numBlocks,
                                             uint64_t sizeBytes, void *addr,
                                             std::string &name, bool skipLayout)
    : firstBlockMetaPtr(nullptr),
      blockSize(blockSize),
      numBlocks(numBlocks),
      headerSize(0),
      totalMemBytes(sizeBytes),
      memArrName(name),
      blockVec(numBlocks) {
  assert(skipLayout);
  assert(sizeBytes == computeBlockArrShmSizeBytes(blockSize, numBlocks));
  headerSize = computeBlockArrShmHeaderBytes(numBlocks);
  // init memory layout
  auto *metaPtr = static_cast<MemBlockMeta *>(addr);
  dataStartPtr = static_cast<char *>(addr) + headerSize;
  firstBlockMetaPtr = metaPtr;
  for (uint32_t i = 0; i < numBlocks; i++) {
    blockVec[i] = metaPtr;
    metaPtr++;
  }
}

SingleSizeMemBlockArr::SingleSizeMemBlockArr(fslib_malloc_block_sz_t blockSize,
                                             fslib_malloc_block_cnt_t numBlocks,
                                             fslib_malloc_mem_sz_t sizeBytes,
                                             void *addr, std::string &name)
    : firstBlockMetaPtr(nullptr),
      blockSize(blockSize),
      numBlocks(numBlocks),
      headerSize(0),
      totalMemBytes(sizeBytes),
      memArrName(name),
      blockVec(numBlocks) {
  assert(sizeBytes == computeBlockArrShmSizeBytes(blockSize, numBlocks));
  memset(addr, 0, sizeBytes);
  headerSize = computeBlockArrShmHeaderBytes(numBlocks);
  // init memory layout
  auto *metaPtr = static_cast<MemBlockMeta *>(addr);
  dataStartPtr = static_cast<char *>(addr) + headerSize;
  firstBlockMetaPtr = metaPtr;
  for (uint32_t i = 0; i < numBlocks; i++) {
    blockVec[i] = metaPtr;
    metaPtr->flags = 0;
    metaPtr->dataPtrOffset = blockSize * i;
    metaPtr->numPages = blockSize / (gFsLibMallocPageSize);
    metaPtr->bSize = blockSize;
    metaPtr->shmInnerId = i;
    metaPtr++;
  }
}

SingleSizeMemBlockArr::~SingleSizeMemBlockArr() {
  fprintf(stderr, "~SingleSizeMemBlockArr\n");
  // NOTE: shm is not initialized by this SingleSizeMemBlockArr, so, it is
  // not supposed to be freed here
  // releaseShm(reinterpret_cast<void *>(firstBlockMetaPtr), totalMemBytes);
}

struct MemBlockMeta *SingleSizeMemBlockArr::dataPtr2MemBlock(void *dataPtr) {
  uint64_t curOff;
  curOff = static_cast<char *>(dataPtr) - dataStartPtr;
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr,
          "dataPtr2MemBlock:curOff:%lu dataPtr:%p firstPtr:%lu "
          "shmName:%s\n",
          curOff, dataPtr, firstBlockMetaPtr->dataPtrOffset,
          memArrName.c_str());
#endif

  return blockVec[curOff / blockSize];
}

void *SingleSizeMemBlockArr::getDataPtrFromId(fslib_malloc_block_cnt_t id) {
  auto curMeta = getMetaFromId(id);
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr, "getDataPtrFromId(%u) dataPtrOffset:%lu\n", id,
          curMeta->dataPtrOffset);
#endif
  return (dataStartPtr) + curMeta->dataPtrOffset;
}

struct MemBlockMeta *SingleSizeMemBlockArr::getMetaFromId(
    fslib_malloc_block_cnt_t id) {
  if (id >= numBlocks) {
    fprintf(stderr, "data block id overflow:%u\n", id);
    return nullptr;
  }
  auto curMeta = firstBlockMetaPtr + id;
  return curMeta;
}

bool SingleSizeMemBlockArr::isAddrInBlock(void *dataPtr) {
  auto dp = static_cast<char *>(dataPtr);
  int64_t curOff = dp - dataStartPtr;
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr, "isAddrInBlock curOff:%ld numBlocks:%u blockSize:%u\n",
          curOff, numBlocks, blockSize);
#endif
  return (curOff >= 0 && curOff < (numBlocks * blockSize));
}

char *SingleSizeMemBlockArr::fromDataPtrOffset2Addr(off_t offset) {
  return dataStartPtr + offset;
}

int FsLibBuddyAllocator::getListsNum() { return getListIdx(getSizeKb()) + 1; }

uint32_t FsLibBuddyAllocator::getSizeKb() { return memBytes_ / 1024; }

int FsLibBuddyAllocator::getListIdx(int sizeKb) {
  if (sizeKb < static_cast<int>(kMinBlockSize / 1024)) {
    fprintf(stderr, "min block sizeKB:%d\n", kMinBlockSize / 1024);
    return -1;
  }
  return ceil((log2(sizeKb)) - log2(kMinBlockSize / 1024));
}

void FsLibBuddyAllocator::init() {
  auto listsSz = getListsNum();
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "listSz:%d\n", listsSz);
#endif
  blocks_ = new FsLibLinkedList *[listsSz];
  for (int i = 0; i < listsSz; i++) {
    blocks_[i] = new FsLibLinkedList(pow(2, i + log2(kMinBlockSize / 1024)));
  }
  blocks_[getListIdx(getSizeKb())]->addBlockEnd(memArr_->firstBlockMetaPtr);
}

void FsLibBuddyAllocator::sortList(int listIdx) {
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "sortList:%d\n", listIdx);
#endif
  auto unsorted = blocks_[listIdx];
  auto sorted = new FsLibLinkedList(unsorted->getBlockSizeKb());
  sorted->addBlockStart(unsorted->getBlockAt(0)->meta);
  for (uint i = 1; i < unsorted->getLength(); i++) {
    auto curMeta = unsorted->getBlockAt(i)->meta;
    for (uint j = 0; j < sorted->getLength(); j++) {
      if (curMeta->shmInnerId <= sorted->getBlockAt(j)->meta->shmInnerId) {
        sorted->addBlockStart(curMeta);
        break;
      } else if (j == sorted->getLength() - 1 &&
                 curMeta->shmInnerId >
                     (sorted->getBlockAt(j)->meta->shmInnerId)) {
        sorted->addBlockEnd(curMeta);
        break;
      }
    }
  }
  blocks_[listIdx] = sorted;
  delete unsorted;
}

void FsLibBuddyAllocator::findBuddies(int listIdx, int *buddies) {
  auto ll = blocks_[listIdx];
  if (ll->getLength() > 1) {
    for (uint i = 0; i <= ll->getLength() - 2; i++) {
#ifdef FS_LIB_MALLOC_DEBUG
      fprintf(stdout, "findBuddies i:%u length:%u listIdx:%d\n", i,
              ll->getLength(), listIdx);
#endif
      if (ll->getBlockAt(i)->meta->shmInnerId ==
          ll->getBlockAt(i + 1)->meta->shmInnerId - 1) {
        buddies[0] = i;
        buddies[1] = i + 1;
        return;
      }
    }
  }
  buddies[0] = -1;
  buddies[1] = -1;
}

void FsLibBuddyAllocator::merge(int listIdx) {
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "FsLibBuddyAllocator::merge(%d)\n", listIdx);
#endif
  int buddies[2] = {-1, -1};
  findBuddies(listIdx, buddies);
  while (buddies[0] >= 0) {
    auto item0 = blocks_[listIdx]->getBlockAt(buddies[0]);
    auto item1 = blocks_[listIdx]->getBlockAt(buddies[1]);
    auto curMeta = item0->meta;
    blocks_[listIdx]->removeBlockAt(buddies[1]);
    blocks_[listIdx]->removeBlockAt(buddies[0]);
    blocks_[listIdx + 1]->addBlockStart(curMeta);
    delete item0;
    delete item1;
#ifdef FS_LIB_MALLOC_DEBUG
    fprintf(stdout, "merge() remove item0:%p item1:%p\n", item0, item1);
#endif
    findBuddies(listIdx, buddies);
  }
}

void FsLibBuddyAllocator::mergeAll() {
  for (int i = 0; i < getListsNum(); i++) {
    merge(i);
  }
}

MemBlockMeta *FsLibBuddyAllocator::doAllocate(int sizeKb) {
  int listIdx = getListIdx(sizeKb);
  if (listIdx < 0) {
    return nullptr;
  }
  bool found = false;
  FsLibLLItem *curItem = nullptr;
  while (!found) {
    if (blocks_[listIdx]->getLength() > 0) {
      curItem = blocks_[listIdx]->getBlockAt(0);
      blocks_[listIdx]->removeBlockAt(0);
      found = true;
    } else if (listIdx < getListsNum()) {
      listIdx++;
      if (blocks_[listIdx]->getLength() > 0) {
        FsLibLLItem *removeItem;
        removeItem = blocks_[listIdx]->getBlockAt(0);
        blocks_[listIdx - 1]->addBlockStart(memArr_->dataPtr2MemBlock(
            memArr_->fromDataPtrOffset2Addr(removeItem->meta->dataPtrOffset) +
            (blocks_[listIdx - 1]->getBlockSizeKb() * 1024)));
        blocks_[listIdx - 1]->addBlockStart(removeItem->meta);
        blocks_[listIdx]->removeBlockAt(0);
#ifdef FS_LIB_MALLOC_DEBUG
        fprintf(stdout, "doAllocate removeItem:%p\n", removeItem);
#endif
        delete removeItem;
        listIdx = getListIdx(sizeKb);
      }
    } else {
      break;
    }
  }
  if (found) {
    curItem->meta->flags =
        UTIL_BIT_FLIP(curItem->meta->flags, (MEM_BLOCK_META_IN_USE_MASK));
    curItem->meta->numPages = ROUND_UP(sizeKb, (gFsLibMallocPageSize / 1024)) /
                              (gFsLibMallocPageSize / 1024);
    auto curMeta = curItem->meta;
#ifdef FS_LIB_MALLOC_DEBUG
    fprintf(stdout, "doAllocate delete curItem:%p\n", curItem);
#endif
    // free the memory here
    delete curItem;
    totalAllocatedBytes_ += curMeta->numPages * (gFsLibMallocPageSize);
    return curMeta;
  }
  return nullptr;
}

void FsLibBuddyAllocator::doFree(MemBlockMeta *meta) {
  int listIdx = getListIdx(
      static_cast<int>(meta->numPages * (gFsLibMallocPageSize / 1024)));
  blocks_[listIdx]->addBlockStart(meta);
  sortList(listIdx);
  mergeAll();
  totalAllocatedBytes_ -= meta->numPages * (gFsLibMallocPageSize);
}

FsLibBuddyAllocator::FsLibBuddyAllocator(SingleSizeMemBlockArr *memArr)
    : memArr_(memArr), totalAllocatedBytes_(0) {
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "FsLibBuddyAlloctor()\n");
#endif
  memBytes_ = memArr->getTotalDataBytes();
  init();
}

FsLibBuddyAllocator::~FsLibBuddyAllocator() {
  auto listsSz = getListsNum();
  for (int i = 0; i < listsSz; i++) {
    delete blocks_[i];
  }
  delete[] blocks_;
}

void FsLibBuddyAllocator::dumpToStdout() {
  fprintf(stdout, "\n-------\n");
  for (int i = 0; i < getListsNum(); i++) {
    fprintf(stdout, "list-[%d]-sizeKb=%d-length=%u: ", i,
            blocks_[i]->getBlockSizeKb(), blocks_[i]->getLength());
    blocks_[i]->printToStdout();
  }
  fprintf(stdout, "\n-------\n");
}

FsLibLinearListsAllocator::FsLibLinearListsAllocator(
    int blockSizeNum, const fslib_malloc_block_sz_t *blockSizeArr,
    const std::vector<SingleSizeMemBlockArr *> &memArrVec)
    : blockSizeNum_(blockSizeNum), memBytes_(0), totalAllocatedBytes_(0) {
  helperBlockSizeVec_ = std::vector<fslib_malloc_block_sz_t>(blockSizeNum + 1);
  perSizeMemArrVec_ = std::vector<SingleSizeMemBlockArr *>(blockSizeNum);
  perSizeFreeListVec_ = std::vector<std::list<MemBlockMeta *>>(blockSizeNum);
  helperBlockSizeVec_[0] = 0;
  for (int i = 0; i < blockSizeNum; i++) {
    helperBlockSizeVec_[i + 1] = blockSizeArr[i];
    perSizeMemArrVec_[i] = memArrVec[i];
    // init free list for each sizes' blocks
    for (fslib_malloc_block_cnt_t j = 0; j < memArrVec[i]->numBlocks; j++) {
      auto curMeta = memArrVec[i]->blockVec[j];
      assert(curMeta != nullptr);
      perSizeFreeListVec_[i].push_back(curMeta);
    }
    memBytes_ +=
        perSizeMemArrVec_[i]->numBlocks * perSizeMemArrVec_[i]->blockSize;
  }
}

MemBlockMeta *FsLibLinearListsAllocator::doAllocate(
    void **dataPtr, fslib_malloc_block_sz_t sizeBytes, int &err) {
  int idx = fromBlockSize2VecIdx(sizeBytes);
  if (idx < 0) {
    // sizeBytes not supported
    err = FsLibMem_ERR_MALLOC_SIZE_NOT_SUPPORT;
    return nullptr;
  }
  auto curFreeList = &perSizeFreeListVec_[idx];

  MemBlockMeta *curMetaPtr = nullptr;
  {
    std::lock_guard<std::mutex> guard(lock_);
    // fprintf(stderr, "freeList.size():%lu idx:%d\n", curFreeList->size(), idx;
    if (curFreeList->empty()) {
      // no available buffer to allocate
      err = FsLibMem_ERR_FREE_NOT_FOUND;
      return nullptr;
    }
    // TODO (jingliu): change back to pop_front()
    // Just to be sure it works in all cases, not only for the first block
    // auto curMetaPtr = curFreeList->front();
    // curFreeList->pop_front();
    curMetaPtr = curFreeList->back();
    curFreeList->pop_back();
    totalAllocatedBytes_ += helperBlockSizeVec_[idx + 1];
  }

#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr,
          "pos-doAlloc totalAllocBytes :%lu idx:%d thisSize:%u offset:%ld "
          "numPages:%d\n",
          totalAllocatedBytes_, idx, helperBlockSizeVec_[idx + 1],
          curMetaPtr->dataPtrOffset, curMetaPtr->numPages);
#endif
  *dataPtr =
      perSizeMemArrVec_[idx]->fromDataPtrOffset2Addr(curMetaPtr->dataPtrOffset);
  return curMetaPtr;
}

int FsLibLinearListsAllocator::doFree(MemBlockMeta *meta) {
  int idx = fromBlockSize2VecIdx(meta->bSize);
  if (idx < 0) {
    return -1;
  }
  std::lock_guard<std::mutex> guard(lock_);
  totalAllocatedBytes_ -= helperBlockSizeVec_[idx + 1];
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr,
          "pos-doFree totalAllocBytes:%lu idx:%d thisSize:%u offset:%ld "
          "numPages:%d\n",
          totalAllocatedBytes_, idx, helperBlockSizeVec_[idx + 1],
          meta->dataPtrOffset, meta->numPages);
#endif
  perSizeFreeListVec_[idx].push_back(meta);
  // fprintf(stderr, "free: freeListSize:%lu idx:%d\n",
  // perSizeFreeListVec_[idx].size(),
  //       idx);
  return 0;
}

int FsLibLinearListsAllocator::fromBlockSize2VecIdx(
    fslib_malloc_block_sz_t bSize) {
  int idx = -1;
  for (int i = 0; i < blockSizeNum_; i++) {
    if (helperBlockSizeVec_[i] < bSize && bSize <= helperBlockSizeVec_[i + 1]) {
      idx = i;
      break;
    }
  }
  return idx;
}

FsLibLinearCacheAllocator::FsLibLinearCacheAllocator(
    SingleSizeMemBlockArr *memArr)
    : memArr_(memArr), memBytes_(0), totalAllocatedBytes_(0) {
  assert(memArr_ != nullptr);
  memBytes_ = memArr_->getTotalDataBytes();
  for (fslib_malloc_block_cnt_t i = 0; i < memArr_->numBlocks; i++) {
    auto curMeta = memArr_->blockVec[i];
    assert(curMeta != nullptr);
    freeList_.push_back(curMeta);
  }
}

MemBlockMeta *FsLibLinearCacheAllocator::doAllocateOnePage() {
  if (freeList_.empty()) {
    return nullptr;
  }
  auto curMeta = freeList_.back();
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr, "pos-doAlloc totalAllocBytes:%lu offset:%ld numPages:%d\n",
          totalAllocatedBytes_, curMeta->dataPtrOffset, curMeta->numPages);
#endif
  freeList_.pop_back();
  assert(!(curMeta->flags & MEM_BLOCK_META_IN_USE_MASK));
  curMeta->flags = UTIL_BIT_FLIP(curMeta->flags, MEM_BLOCK_META_IN_USE_MASK);
  memBytes_ -= gFsLibMallocPageSize;
  return curMeta;
}

void FsLibLinearCacheAllocator::doFree(MemBlockMeta *meta) {
  assert(meta->flags & MEM_BLOCK_META_IN_USE_MASK);
  if (meta->flags & MEM_BLOCK_META_DIRTY_MASK) {
    fprintf(stderr, "CacheAllocator() WARNING doFree() for a dirty page\n");
  }
  freeList_.push_back(meta);
  meta->flags = UTIL_BIT_FLIP(meta->flags, MEM_BLOCK_META_IN_USE_MASK);
  memBytes_ += gFsLibMallocPageSize;
}

void FsLibLinearCacheAllocator::dumpToStdout() {
  fslib_malloc_block_cnt_t numAllocated =
      totalAllocatedBytes_ / (gFsLibMallocPageSize);
  fprintf(stdout, "======= CacheAllocator =======\n");
  fprintf(stdout, " bSize:%u bNum:%u", memArr_->blockSize, memArr_->numBlocks);
  fprintf(stdout, " allocatedNumBlocks:%u", numAllocated);
  fprintf(stdout, " \n-- allocated blocks --\n");
  for (uint i = 0; i < memArr_->numBlocks; ++i) {
    auto meta = memArr_->blockVec[i];
    if (meta->flags & (MEM_BLOCK_META_IN_USE_MASK)) {
      fprintf(stdout,
              " mapKey:%u dataPtrOffset:%ld mid:%u bSize:%u numPages:%u", i,
              meta->dataPtrOffset, meta->shmInnerId, meta->bSize,
              meta->numPages);
      fprintf(stdout, " flag:%d", meta->flags);
      fprintf(stdout, "\n");
    }
  }
  fprintf(stdout, "=======\n");
}

// Actually define the static member to get rid of undefined reference err.
constexpr fslib_malloc_mem_sz_t FsLibMemMng::kPerSizeTotalBytes[];
constexpr fslib_malloc_block_sz_t FsLibMemMng::kBlockSizeArr[];

FsLibMemMng::FsLibMemMng(int appid, int memMngId)
    : appid_(appid), memMngId_(memMngId), initDone_(false) {}

FsLibMemMng::~FsLibMemMng() {
  if (!isShm_) {
    for (auto memPtr : bufferMemArrVec_) {
      if (memPtr != nullptr) {
        free(memPtr);
      }
    }
  } else {
    // TODO (jingliu): detach shm here?
  }
}

int FsLibMemMng::init(bool initShm) {
  assert(!initDone_);
  isShm_ = initShm;
  fslib_malloc_block_sz_t curBlockSz;
  fslib_malloc_block_cnt_t curBlockCnt;
  // init for buffer management
  bufferMemArrVec_ = std::vector<SingleSizeMemBlockArr *>(kBlockSizeNum);
  shmStartPtrVec_ = std::vector<void *>(kBlockSizeNum);
  shmBytesVec_ = std::vector<fslib_malloc_mem_sz_t>(kBlockSizeNum);
  shmidVec_ = std::vector<uint8_t>(kBlockSizeNum);
  shmFnameVec_ = std::vector<std::string>(kBlockSizeNum);

  for (int i = 0; i < kBlockSizeNum; i++) {
    curBlockSz = kBlockSizeArr[i];
    curBlockCnt = kPerSizeTotalBytes[i] / curBlockSz;
    int shmFd = -1, err;
    auto curTotalMemSz = SingleSizeMemBlockArr::computeBlockArrShmSizeBytes(
        curBlockSz, curBlockCnt);
    std::stringstream ss;
    assert(curBlockSz % kPageSize == 0);
    getSingleSizeMemBlockArrName(curBlockSz / 1024, appid_, memMngId_, ss);
    auto shmNameStr = ss.str();
    void *curTotalMemPtr = nullptr;
    if (initShm) {
      curTotalMemPtr = shmOpenInit(shmFd, shmNameStr, curTotalMemSz, err);
    } else {
      curTotalMemPtr = malloc(curTotalMemSz);
    }
    if (curTotalMemPtr != nullptr) {
      bufferMemArrVec_[i] = new SingleSizeMemBlockArr(
          curBlockSz, curBlockCnt, curTotalMemSz, curTotalMemPtr, shmNameStr);
      shmBytesVec_[i] = curTotalMemSz;
      shmStartPtrVec_[i] = curTotalMemPtr;
    } else {
      return -1;
    }
    shmidVec_[i] = -1;
    shmFnameVec_[i] = shmNameStr;
  }
  fprintf(stdout,
          "FsLibMemMng:init() will create FsLibLinearListsAllocator "
          "threadFsTid: %d\n",
          memMngId_);
  bufferAllocator_ = new FsLibLinearListsAllocator(kBlockSizeNum, kBlockSizeArr,
                                                   bufferMemArrVec_);

  // initialization done
  initDone_ = true;
  return 0;
}

void *FsLibMemMng::mallocInner(fslib_malloc_block_sz_t sizeBytes, bool isZero,
                               int &err) {
  void *addr = nullptr;
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stderr, "mallocInner(sizeBytes:%u) threadFsTid:%d\n", sizeBytes,
          memMngId_);
#endif
  auto curItem = bufferAllocator_->doAllocate(&addr, sizeBytes, err);
  if (curItem == nullptr) {
    return nullptr;
  }
  if (isZero) {
    memset(addr, 0, sizeBytes);
  }
  return (addr);
}

void *FsLibMemMng::Malloc(fslib_malloc_block_sz_t sizeBytes, int &err) {
  return mallocInner(sizeBytes, false, err);
}

void *FsLibMemMng::Zalloc(fslib_malloc_block_sz_t sizeBytes, int &err) {
  return mallocInner(sizeBytes, true, err);
}

void FsLibMemMng::Free(void *ptr, int &err) {
  int sizeIdx;
  // fprintf(stderr, "free threadFsTid:%d \n", memMngId_);
  auto curMeta = findDataPtrMeta(ptr, sizeIdx, err);
  if (curMeta != nullptr) {
    bufferAllocator_->doFree(curMeta);
  } else {
    // fprintf(stderr, "free cannot found tid:%d\n", memMngId_);
    err = FsLibMem_ERR_FREE_NOT_FOUND;
  }
}

bool FsLibMemMng::getBufOwnerInfo(void *ptr, char *shmName,
                                  int maxShmNameInfoLen,
                                  fslib_malloc_block_cnt_t &ptrOffset,
                                  fslib_malloc_block_sz_t &shmBlockSize,
                                  fslib_malloc_block_cnt_t &shmNumBlocks,
                                  int &err) {
  return getBufOwnerInfoAndSetDirty(ptr, shmName, maxShmNameInfoLen, ptrOffset,
                                    shmBlockSize, shmNumBlocks, false, err);
}

bool FsLibMemMng::getBufOwnerInfo(void *ptr, bool isDirty, uint8_t &shmid,
                                  fslib_malloc_block_cnt_t &ptrOffset,
                                  int &err) {
  int idx;
  err = 0;
  auto meta = findDataPtrMeta(ptr, idx, err);
  if (meta != nullptr) {
    if (isDirty) {
      meta->flags = UTIL_BIT_FLIP(meta->flags, (MEM_BLOCK_META_DIRTY_MASK));
    }
    shmid = shmidVec_[idx];
    ptrOffset = meta->shmInnerId;
    return true;
  } else {
    fprintf(stderr, "getBufOwnerInfo ptr:%p cannot find \n", ptr);
    err = FsLibMem_ERR_FREE_NOT_FOUND;
  }
  return false;
}

bool FsLibMemMng::getBufOwnerInfoAndSetDirty(
    void *ptr, char *shmName, int maxShmNameInfoLen,
    fslib_malloc_block_cnt_t &ptrOffset, fslib_malloc_block_sz_t &shmBlockSize,
    fslib_malloc_block_cnt_t &shmNumBlocks, bool isDirty, int &err) {
  int idx;
  err = 0;
  auto meta = findDataPtrMeta(ptr, idx, err);
  if (meta != nullptr) {
    if (isDirty) {
      meta->flags = UTIL_BIT_FLIP(meta->flags, (MEM_BLOCK_META_DIRTY_MASK));
    }
    ptrOffset = meta->shmInnerId;
    bufferMemArrVec_[idx]->getMemArrName(shmName, maxShmNameInfoLen);
    shmBlockSize = bufferMemArrVec_[idx]->getBlockSize();
    shmNumBlocks = bufferMemArrVec_[idx]->getBlockCount();
    return true;
  } else {
    err = FsLibMem_ERR_FREE_NOT_FOUND;
  }
  return false;
}

struct MemBlockMeta *FsLibMemMng::findDataPtrMeta(void *ptr, int &sizeIdx,
                                                  int &err) {
  SingleSizeMemBlockArr *curMemArr = nullptr;
  sizeIdx = -1;
  for (int i = 0; i < kBlockSizeNum; i++) {
    curMemArr = bufferMemArrVec_[i];
    if (curMemArr->isAddrInBlock(ptr)) {
      sizeIdx = i;
      break;
    }
  }
  if (sizeIdx < 0 || curMemArr == nullptr) {
    err = FsLibMem_ERR_FREE_NOT_FOUND;
    return nullptr;
  }
  return curMemArr->dataPtr2MemBlock(ptr);
}

void *shmOpenInit(int &fd, std::string &shmNameStr, uint64_t sizeBytes,
                  int &err) {
  err = 0;
  auto shmName = shmNameStr.c_str();
  int shmFd = shm_open(shmName, O_CREAT | O_EXCL | O_RDWR, 0666);
  if (shmFd < 0) {
    fprintf(stderr,
            "shm_open(O_CREAT, %s) fail warning (will go on to attach) "
            "error:%s. \n",
            shmName, strerror(errno));
    if (errno != EEXIST) {
      err = 1;
      return nullptr;
    } else {
      shmFd = shm_open(shmName, O_RDWR, 0666);
      if (shmFd < 0) {
        fprintf(stderr, "shm_open(O_RDWR) error:%s\n", strerror(errno));
        err = 1;
        return nullptr;
      }
    }
  }
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "shmOpenInit->ftruncate() sizeBytes:%lu\n", sizeBytes);
#endif
  int rt = ftruncate(shmFd, sizeBytes);
  if (rt != 0) {
    fprintf(stderr, "ftruncate(%d) error %s, shmName:%s\n", shmFd,
            strerror(errno), shmName);
    err = 1;
    return nullptr;
  }

  void *shmPtr =
      mmap(nullptr, sizeBytes, PROT_READ | PROT_WRITE, MAP_SHARED, shmFd, 0);
  if (shmPtr == MAP_FAILED) {
    fprintf(stderr, "shm mmap() failed error:%s\n", strerror(errno));
    err = 1;
    return nullptr;
  }

  return shmPtr;
}

void *shmOpenAttach(int &fd, std::string &shmNameStr, uint64_t sizeBytes,
                    int &err) {
  err = 0;
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "shmOpenAttach->shm_open + mmap(), sizeBytes:%lu\n",
          sizeBytes);
#endif
  auto shmName = shmNameStr.c_str();
  int shmFd = shm_open(shmName, O_RDWR, 0666);
  if (shmFd < 0) {
    fprintf(stderr, "shm_open(%s) error - %s\n", shmName, strerror(errno));
    err = 1;
  }

  void *shmPtr =
      mmap(nullptr, sizeBytes, PROT_READ | PROT_WRITE, MAP_SHARED, shmFd, 0);
  if (shmPtr == MAP_FAILED) {
    fprintf(stderr,
            "shmOpenAttach() shm mmap() failed sizeBytes:%lu, errno:%s\n",
            sizeBytes, strerror(errno));
    err = 1;
  }

  if (err) exit(1);

  return shmPtr;
}

int releaseShm(void *ptr, uint64_t sizeBytes) {
  fprintf(stderr, "releaseShm\n");
  assert(ptr != nullptr);
#ifdef FS_LIB_MALLOC_DEBUG
  fprintf(stdout, "releaseShm->munmap sizeBytes:%lu\n", sizeBytes);
#endif
  int rt = munmap(ptr, sizeBytes);
  if (rt == -1) {
    fprintf(stderr, "shm munmap() failed errstr: %s\n", strerror(errno));
  }
  return 0;
}
