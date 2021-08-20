#ifndef CFS_FSLIBMALLOC_H
#define CFS_FSLIBMALLOC_H

#include <string.h>
#include <sys/types.h>

#include <atomic>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "lru.h"
#include "param.h"
#include "util.h"

constexpr static uint32_t gFsLibMallocPageSize = 4096;
// On/off of debug prints
//// #define FS_LIB_MALLOC_DEBUG

//
// helper functions for mmap based shared memory, if ok err set to 0.
//
// open a shared memory file, create it and init the size (truncate)
void *shmOpenInit(int &fd, std::string &shmNameStr, uint64_t sizeBytes,
                  int &err);
// open an existing shared memory file
void *shmOpenAttach(int &fd, std::string &shmNameStr, uint64_t sizeBytes,
                    int &err);

int releaseShm(void *ptr, uint64_t sizeBytes);
////////////////////////////////////////////////////////////////////////////////

// get the name of memory region which is organized as certain size
// Actually, this is used as the name of underlining shared memory's name
// (used as the argument to shm_open)
// @param sizeKb: block size in KB
// @param gUniqueMemId: offset that uniquely identify different user clients
// @param withinSizeId: id to distinguish shared memory pieces of the same
// size, e.g., we can have 2 shared memory (from shm_open) of size 1G, 2G that
// is used for 4K blocks.
// @param ss: store the result
static void inline getSingleSizeMemBlockArrName(uint32_t sizeKb,
                                                int gUniqueMemId,
                                                int withinSizeId,
                                                std::stringstream &ss) {
  char prefix[] = "fsp_";
  // ss.clear();
  ss << prefix;
  ss << sizeKb;
  ss << '_';
  if (gUniqueMemId > (FS_SHM_KEY_BASE)) {
    ss << (gUniqueMemId - (FS_SHM_KEY_BASE));
  } else {
    ss << gUniqueMemId;
  }
  ss << '_';
  ss << withinSizeId;
}

////////////////////////////////////////////////////////////////////////////////

typedef uint32_t fslib_malloc_block_sz_t;
typedef uint32_t fslib_malloc_block_cnt_t;
typedef uint64_t fslib_malloc_mem_sz_t;

#define MEM_BLOCK_META_IN_USE_MASK (0b0001)
#define MEM_BLOCK_META_DIRTY_MASK (0b0010)

// All the content of this MemBlockMeta is the header of shared memory region
struct MemBlockMeta {
  // once contains modified data, will be set to flags by FsLib and set to
  // back clean once a flushing request contains this block is ACKed.
  __volatile uint8_t flags;
  // offset compares to the start point of data region
  off_t dataPtrOffset;
  // this single memBlock's id (in order, from 0, used to retrieve the data
  // addr in remote process)
  // REQUIRED: it is never going to be modified once the initialization of
  // whole mem buffer is finished.
  // NOTE: now this name is confusing, the meta might not be referring shared
  // memory
  uint32_t shmInnerId;
  fslib_malloc_block_sz_t bSize;
  // USED for cache (number of PAGES)
  fslib_malloc_block_cnt_t numPages;
};

struct FsLibLLItem {
  struct MemBlockMeta *meta;
  struct FsLibLLItem *next;
};

class FsLibLinkedList {
 public:
  FsLibLinkedList(int blockSizeKb)
      : head_(nullptr), tail_(nullptr), length_(0), blockSzKb_(blockSizeKb) {
#ifdef FS_LIB_MALLOC_DEBUG
    fprintf(stdout, "FsLibLinkedList: blockSizeKb:%d\n", blockSizeKb);
#endif
  };
  ~FsLibLinkedList();
  bool isEmpty() { return head_ == NULL && tail_ == NULL; }
  fslib_malloc_block_sz_t getBlockSizeKb() { return blockSzKb_; }
  fslib_malloc_block_cnt_t getLength() { return length_; }
  FsLibLLItem *getBlockAt(fslib_malloc_block_cnt_t position);
  // Note: all these addBlockXXX functions will invoke *new* linked list
  // object
  void addBlockStart(MemBlockMeta *meta);
  void addBlockEnd(MemBlockMeta *meta);
  void addBlockAt(fslib_malloc_block_cnt_t position, MemBlockMeta *meta);
  // REQUIRED: user-app needs to take care of the deletion of the item
  // returned by getBlockAt(), if the following removeXXX are called,
  // otherwise, will result in memory leak
  void removeBlockStart();
  void removeBlockEnd();
  void removeBlockAt(fslib_malloc_block_cnt_t position);

  void printToStdout();

 private:
  FsLibLLItem *head_;
  FsLibLLItem *tail_;
  fslib_malloc_block_cnt_t length_;
  int blockSzKb_;
};

class SingleSizeMemBlockArr {
 public:
  // align the header to cache-line, not sure if necessary
  constexpr static int kHeaderAlignBytes = 64;

  // @param blockSize: in bytes
  // @return size in bytes, if any error, then set to 0
  static fslib_malloc_mem_sz_t computeBlockArrShmSizeBytes(
      fslib_malloc_block_sz_t blockSize, fslib_malloc_block_cnt_t numBlocks);

  static fslib_malloc_mem_sz_t computeBlockArrShmHeaderBytes(
      fslib_malloc_block_cnt_t numBlocks);

  SingleSizeMemBlockArr(fslib_malloc_block_sz_t blockSize,
                        fslib_malloc_block_cnt_t numBlocks, uint64_t sizeBytes,
                        void *addr, std::string &name, bool skipLayout);
  SingleSizeMemBlockArr(fslib_malloc_block_sz_t blockSize,
                        fslib_malloc_block_cnt_t numBlocks, uint64_t sizeBytes,
                        void *addr, std::string &name);

  ~SingleSizeMemBlockArr();

  // use dataPtr to retrieve the block's metadata
  // REQUIRED: dataPtr must be the start of a dataBlock (in data region)
  struct MemBlockMeta *dataPtr2MemBlock(void *dataPtr);
  // corresponds to MemBlockMeta::shmInnerId
  void *getDataPtrFromId(fslib_malloc_block_cnt_t id);
  // corresponds to MemBlockMeta::shmInnerId
  struct MemBlockMeta *getMetaFromId(fslib_malloc_block_cnt_t id);
  // evaluate if an memory address is within the blocks of this array
  // REQUIRED: dataPtr must be within the data region
  bool isAddrInBlock(void *dataPtr);
  // basically translate MemBlockMeta::dataPtrOffset to real virtual address
  // Since we are using shared memory across different processes, direct
  // address (pointer) cannot be used in MemBlockMeta (it is located in the
  // shared memory region)
  char *fromDataPtrOffset2Addr(off_t offset);

  fslib_malloc_block_sz_t getBlockSize() { return blockSize; }
  fslib_malloc_block_cnt_t getBlockCount() { return numBlocks; }
  fslib_malloc_block_cnt_t getTotalDataBytes() { return blockSize * numBlocks; }
  fslib_malloc_mem_sz_t getTotalBytes() { return totalMemBytes; }
  void getMemArrName(char *dst, size_t len) {
    strncpy(dst, memArrName.c_str(),
            memArrName.size() < len ? memArrName.size() : len);
  }

 private:
  struct MemBlockMeta *firstBlockMetaPtr;
  // address of starting point of data region
  // | header (metadata) | data |
  // ->firstBlockMetaPtr
  //                     -> dataStartPtr
  char *dataStartPtr;
  // block size in bytes
  fslib_malloc_block_sz_t blockSize;
  fslib_malloc_block_cnt_t numBlocks;
  fslib_malloc_mem_sz_t headerSize;
  fslib_malloc_mem_sz_t totalMemBytes;
  std::string memArrName;
  // std::list<MemBlockMeta *> freeList;
  // key: the MemBlockMeta's dataPtr's offset (in bytes) according to the
  // start of data region
  // I.e., key of this is aligned to blockSize
  std::vector<struct MemBlockMeta *> blockVec;

  friend class FsLibBuddyAllocator;
  friend class FsLibLinearListsAllocator;
  friend class FsLibLinearCacheAllocator;
};

class FsLibBuddyAllocator {
 public:
  constexpr static uint32_t kMinBlockSize = gFsLibMallocPageSize;

  FsLibBuddyAllocator(SingleSizeMemBlockArr *memArr);
  ~FsLibBuddyAllocator();

  MemBlockMeta *doAllocate(int sizeKb);
  void doFree(MemBlockMeta *meta);

  fslib_malloc_mem_sz_t getSize() { return memBytes_; }
  fslib_malloc_mem_sz_t getFreeSize() {
    return memBytes_ - totalAllocatedBytes_;
  }
  void dumpToStdout();

 private:
  SingleSizeMemBlockArr *memArr_;
  fslib_malloc_mem_sz_t memBytes_;
  fslib_malloc_mem_sz_t totalAllocatedBytes_;
  // char *dataPtr_;
  FsLibLinkedList **blocks_;

  int getListsNum();
  uint32_t getSizeKb();
  int getListIdx(int sizeKb);
  void init();
  void sortList(int listIdx);
  // REQUIRED: buddies of type int[2];
  // if found will set buddies[0], buddies[1] to non-negative value
  void findBuddies(int listIdx, int *buddies);
  void merge(int listIdx);
  void mergeAll();
};

class FsLibLinearListsAllocator {
 public:
  FsLibLinearListsAllocator(
      int blockSizeNum, const fslib_malloc_block_sz_t *blockSizeArr,
      const std::vector<SingleSizeMemBlockArr *> &memArrVec);
  ~FsLibLinearListsAllocator() = default;
  // @param dataPtr: save the result data pointer
  MemBlockMeta *doAllocate(void **dataPtr, fslib_malloc_block_sz_t sizeBytes,
                           int &err);
  int doFree(MemBlockMeta *meta);
  fslib_malloc_mem_sz_t getSize() { return memBytes_; }
  fslib_malloc_mem_sz_t getFreeSize() {
    std::lock_guard<std::mutex> guard(lock_);
    return memBytes_ - totalAllocatedBytes_;
  }

 private:
  int blockSizeNum_;
  std::vector<fslib_malloc_block_sz_t> helperBlockSizeVec_;
  std::vector<SingleSizeMemBlockArr *> perSizeMemArrVec_;
  std::vector<std::list<MemBlockMeta *>> perSizeFreeListVec_;
  // bytes of all the data fields
  fslib_malloc_mem_sz_t memBytes_;
  fslib_malloc_mem_sz_t totalAllocatedBytes_;
  // lock for concurrent cross-thread free
  std::mutex lock_;

  // if cannot support this size will return -1
  int fromBlockSize2VecIdx(fslib_malloc_block_sz_t bSize);
};

// Compared to above allocator, this CacheAllocator will allow the cross-page
// allocation
class FsLibLinearCacheAllocator {
 public:
  FsLibLinearCacheAllocator(SingleSizeMemBlockArr *memArr);
  ~FsLibLinearCacheAllocator() = default;
  // allocate single page
  MemBlockMeta *doAllocateOnePage();
  // free single page
  void doFree(MemBlockMeta *meta);
  fslib_malloc_mem_sz_t getSize() { return memBytes_; }
  fslib_malloc_mem_sz_t getFreeSize() {
    return memBytes_ - totalAllocatedBytes_;
  }
  void dumpToStdout();

 private:
  SingleSizeMemBlockArr *memArr_;
  fslib_malloc_mem_sz_t memBytes_;
  fslib_malloc_mem_sz_t totalAllocatedBytes_;

  std::list<MemBlockMeta *> freeList_;
};

#define FsLibMem_ERR_MALLOC_SIZE_NOT_SUPPORT (-1)
#define FsLibMem_ERR_MALLOC_USED_UP (-2)
#define FsLibMem_ERR_FREE_NOT_FOUND (-3)
class FsLibMemMng {
 public:
  constexpr static fslib_malloc_block_sz_t kPageSize = gFsLibMallocPageSize;

  FsLibMemMng(int appid, int memCacheId);
  ~FsLibMemMng();

  // initialize the memory buffer, including pre-allocated shared-memory, mmap()
  // REQUIRED: must be called after constructor
  // @param initShm: if init shared memory, otherwise will use malloc()
  int init(bool initShm);
  bool isInitDone() { return initDone_; }

  // NOTE: bellowing several function names are intended to be capitalized to
  // better distinguish from POSIX counterparts
  void *Malloc(fslib_malloc_block_sz_t sizeBytes, int &err);
  // malloc + set the buffer to 0
  void *Zalloc(fslib_malloc_block_sz_t sizeBytes, int &err);
  void Free(void *ptr, int &err);
  //////////////////////////////////////////////////////////////////////////////

  bool getBufOwnerInfo(void *ptr, char *shmName, int maxShmNameInfoLen,
                       fslib_malloc_block_cnt_t &ptrOffset,
                       fslib_malloc_block_sz_t &shmBlockSize,
                       fslib_malloc_block_cnt_t &shmNumBlocks, int &err);
  bool getBufOwnerInfo(void *ptr, bool isDirty, uint8_t &shmid,
                       fslib_malloc_block_cnt_t &ptrOffset, int &err);
  bool getBufOwnerInfoAndSetDirty(void *ptr, char *shmName,
                                  int maxShmNameInfoLen,
                                  fslib_malloc_block_cnt_t &ptrOffset,
                                  fslib_malloc_block_sz_t &shmBlockSize,
                                  fslib_malloc_block_cnt_t &shmNumBlocks,
                                  bool isDirty, int &err);

  // TODO (jingliu): temp function for debug, remove
  // temp
  void *firstDataPtr() { return bufferMemArrVec_[1]->getDataPtrFromId(0); }
  void *firstMetaPtr() { return bufferMemArrVec_[1]->getMetaFromId(0); }

  // according to *ptr, find out the corresponding metadata
  // REQUIRED: ptr should be the start addr of each block, otherwise cannot
  // be found
  // @param: sizeIdx, err will be set accordingly
  // @return: if found, the metadata will be returned
  struct MemBlockMeta *findDataPtrMeta(void *ptr, int &sizeIdx, int &err);

  void dumpCache() {  // TODO
  }
  void setShmIDForIdx(int idx, uint8_t shmid) { shmidVec_[idx] = shmid; }
  int getNumShmFiles() { return kBlockSizeNum; }
  void getShmConfigForIdx(int idx, char *filename,
                          fslib_malloc_block_sz_t &blksz,
                          fslib_malloc_block_sz_t &blkcnt, uint8_t &shmid) {
    blksz = kBlockSizeArr[idx];
    blkcnt = kPerSizeTotalBytes[idx] / blksz;
    nowarn_strncpy(filename, shmFnameVec_[idx].c_str(), MULTI_DIRSIZE);
    shmid = shmidVec_[idx];
  }

 private:
  // default buffer configuration
  constexpr static int kBlockSizeNum = 3;
  constexpr static fslib_malloc_mem_sz_t kPerSizeTotalBytes[] = {
#if (defined _EXTENT_FOR_LDB_) || (defined _EXTENT_FOR_FILEBENCH_)
      2UL * 1024 * 1024 * 1024, /* 2G for 8K pages*/
#else
      256 * 1024 * 1024, /* 256M for 8K pages*/
#endif
      128 * 1024 * 1024, /* 128 M for 48K pages*/
      8 * 1024 * 1024,   /* 8 M for 2M pages*/
  };
  constexpr static fslib_malloc_block_sz_t kBlockSizeArr[] = {
      /*8K*/ 8 * 1024,
      /*48K*/ 48 * 1024,
      /*2M*/ 2 * 1024 * 1024};

  static uint32_t getShmBufTotalNumPages() {
    uint32_t numPages = 0;
    for (fslib_malloc_block_sz_t curBytes : kPerSizeTotalBytes) {
      numPages += curBytes / kPageSize;
    }
    return numPages;
  }

  // For now, it is set by the shm key
  int appid_;
  // id of this memory buffer
  int memMngId_;
  bool initDone_;
  bool isShm_;

  // shm-alloc-related
  FsLibLinearListsAllocator *bufferAllocator_;
  std::vector<SingleSizeMemBlockArr *> bufferMemArrVec_;
  std::vector<void *> shmStartPtrVec_;
  std::vector<fslib_malloc_mem_sz_t> shmBytesVec_;
  std::vector<uint8_t> shmidVec_;
  std::vector<std::string> shmFnameVec_;

  // Actually doAllocate memory
  // @param isZero: will init memory to zero if set to true
  void *mallocInner(fslib_malloc_block_sz_t sizeBytes, bool isZero, int &err);
};

#endif  // CFS_FSLIBMALLOC_H
