#ifndef CFS_BLOCKBUFFER_H
#define CFS_BLOCKBUFFER_H

#include <stdint.h>

#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <list>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "typedefs.h"

enum class BlockBufferStatus {
  BLOCK_BUF_NONE,
  BLOCK_BUF_VALID,   // unlocked, NONE is the default
  BLOCK_BUF_LOCKED,  // locked
};

class BlockBufferItem {
 public:
  typedef uint32_t buf_idx_t;
  char *getBufPtr() { return dataPtr; }
  bool isInMem() { return inMem; }
  bool isBufDirty();
  buf_idx_t getIdx();
  block_no_t getBlockNo() { return blockNo; }
  ssize_t getBlockSize() { return blockSize; }
  // Callbacks when a block is fetched from device
  // This is the only point that, setInMem(true) should be used
  // @param notFetched: if not fetched is specified, we assume the caller hold
  //  the lock of this item, and do not need to fetch the block, note, this
  //  call will release the block's lock
  // @return false if error
  // REQUIRED, the block is locked when calling this
  bool blockFetchedCallback();
  bool blockWritenCallback(uint64_t blockReqSeqNo);
  uint64_t getLastFlushBlockReqSeqNo() { return lastFlushBlockReqSeqNo; }
  // Evaluate if a buffer has been flushed for certain time by comparing with
  // lastUpdateTime
  bool setLastFlushBlockReqSeqNo(uint64_t);
  BlockBufferStatus getStatus();

 private:
  BlockBufferItem(char *ptr, uint32_t idx, int blockSize)
      : dataPtr(ptr),
        idx(idx),
        blockSize(blockSize),
        isDirty(false),
        inMem(false),
        status(BlockBufferStatus::BLOCK_BUF_NONE) {}

  char *dataPtr;
  buf_idx_t idx;
  ssize_t blockSize;
  block_no_t blockNo;
  // index, if 0, then none index
  uint32_t itemIndex{0};
  std::atomic_bool isDirty;
  std::atomic_bool inMem;
  BlockBufferStatus status;
  uint64_t lastFlushBlockReqSeqNo = 0;
  // uint32_t dirtyCursor = 0;
  // char *savedDataForRMW{nullptr};
  // bool hasFetchedFromDisk = false;
  // bool isRMWInFlight = false;
  bool isDebug_ = false;

  void setBlockNo(block_no_t no);
  void setIndex(uint32_t index) { itemIndex = index; }
  uint32_t getIndex() { return itemIndex; }
  void resetIndex() { itemIndex = 0; }
  void setStatus(BlockBufferStatus s);
  void setInMem(bool b);
  bool setDirty(bool d);
  void setDebug(bool b) { isDebug_ = b; }
  bool getDebug() { return isDebug_; }

  // Note: BlockBuffer only access above functions, not directly manipulate the
  // data
  friend class BlockBuffer;
};

#define BLOCK_BUF_LOCK_GRAB_DEFAULT (-1)
#define BLOCK_BUF_LOCK_GRAB_YES 1
#define BLOCK_BUF_LOCK_GRAB_NO 0
class BlockBuffer {
 public:
  BlockBuffer(block_no_t blockNum, int blockSize, char *memPtr);
  BlockBuffer(block_no_t blockNum, int blockSize, char *memPtr,
              bool isReportStat, const char *bufferName);
  ~BlockBuffer();

  BlockBufferItem *getBlock(block_no_t blockNo, int &lockGrabbed,
                            bool &hasDirty, uint32_t new_index = 0);
  // get the buffer item without accessing (aka. change the order of LRU list)
  // Will return nullptr if this blockNo is not in buffer
  BlockBufferItem *getBlockWithoutAccess(block_no_t blockNo);
  std::string &getBufferName() { return bufferName; }
  void setBufferName(const std::string bn) { bufferName = bn; }
  const std::list<std::pair<block_no_t, BlockBufferItem *>>
      &getAllBufferItems();
  int releaseBlock(BlockBufferItem *item);
  int releaseBlock(block_no_t blockNo);

  // About dirty block flush
  // Set the item in this BlockBuffer as dirty, will at the same time do the
  // accounting of dirty blocks.
  // @return: original isDirty value of this block
  bool setItemDirty(BlockBufferItem *item, bool isDirty, uint32_t itemIndex);
  void releaseUnlinkedInodeDirtyBlocks(uint32_t itemIndex);
  ssize_t getDirtyItemNum() {
    // return dirtyBlocks.size();
    return dirtyItemNum_;
  }
  void adjustDirtyItemNum(int delta) {
    dirtyItemNum_ += delta;
    if (dirtyItemNum_ < 0) {
      throw std::runtime_error("dirtyItemNum < 0, delta:" + delta);
      fprintf(stdout, "[WARN] buf:%s dirtyItemNum < 0 delta:%d\n",
              bufferName.c_str(), delta);
      dirtyItemNum_ = 0;
    }
  }

  // Check if the blockBuffer needs to do a background flush to avoid using up
  // of block slots.
  bool checkIfNeedBgFlush();
  float getBgFlushNumBlockThreshold() { return dirtyFlushRatio * blockNum; }
  bool checkIfBgFlushInflight() { return bgFlushSent; }
  void setIfBgFlushInflight(bool b) { bgFlushSent = b; }
  bool checkIfFgFlushInflightReachLimit() {
    return numFgFlushInflight >= numFgFlushInflightLimit;
  }
  bool checkIfIdxFgFlushInflight(int index) {
    return waitForFlushIndexes.find(index) != waitForFlushIndexes.end();
  }
  bool checkIfFgFlushInflight() { return numFgFlushInflight > 0; }
  void addFgFlushInflightNum(int i) { numFgFlushInflight += i; }

  // FgFlush: foreground flushing -- flushing that is in critical IO path
  void addFgFlushWaitIndex(uint32_t idx);
  void removeFgFlushWaitIndex(uint32_t idx);
  // fill a list of blocks that needs to be flushed
  // will set canFlush as the FLAG for the caller to see if flush request
  // will be issued or not this time. Because at any time, there should be only
  // one inflight flush request.
  // @param canFlush: if the buffer can be flushed, will be set to true
  // @param toFlushBlockNos: save the block numbers to be flushed
  void doFlush(bool &canFlush, std::list<block_no_t> &toFlushBlockNos);
  void doFlushByIndex(uint32_t index, bool &canFlush,
                      std::list<block_no_t> &toFlushBlockNos);
  uint32_t numDirtyByIndex(uint32_t index);
  // When all the block level flush request is finished, call this to clear the
  // dirty mark of each buffer item. return -1 if error happens, otherwise,
  // return 0.
  int doFlushDone(std::vector<block_no_t> &blocks);
  float getDirtyFlushRatio() { return dirtyFlushRatio; }
  void setDirtyFlushRatio(float r);
  uint32_t getDirtyFlushOneTimeSubmitNum();
  void setDirtyFlushOneTimeSubmitNum(uint32_t n);

  // split the BufferItems that associated to the index
  // @ param items: the resuling bufferItems will be put into items
  // @ return: error will be -1, ok --> 0
  int splitBufferItemsByIndex(uint32_t index,
                              std::unordered_set<BlockBufferItem *> &itemSet);
  // install buffer slot in into this buffer
  int installBufferItemsOfIndex(
      uint32_t index, const std::unordered_set<BlockBufferItem *> &itemset);

  // remove @numSlots items from this buffer, will pick from freeList
  // @return: numSlots if success, -1 if not enough slots.
  int reclaimBufferItemSlots(uint numSlots, std::list<BlockBufferItem *> items);

  [[deprecated]]
  // previously, blockBuffer->getBlock() will resetIndex inside
  // then never get a chance to really do [old_index].erase()
  // because the old_index is always 0 when arriving here
  // now blockBuffer->getBlock() itself has the API to use index
  void
  updateBlockIndex(BlockBufferItem *item, uint32_t index) {
    // fprintf(stderr, "updateBlockIndex blockNo:%u ino:%u\n",
    // item->blockNo, index);
    auto old_index = item->getIndex();
    if (old_index != index) {
      if (old_index > 0) {
        (blockIndexMap_[old_index]).erase(item);
      }
      if (item->isBufDirty()) {
        // updateBlockIndex is not supposed to have an dirty block
        std::cerr << old_index << std::endl;
        throw std::runtime_error("update block index of dirty block");
        setItemDirty(item, false, old_index);
      }
      blockIndexMap_[index].emplace(item);
      item->setIndex(index);
    } else {
      blockIndexMap_[index].emplace(item);
    }
  }

  // Report cache hit ratio in this period and reset the cache stats.
  void reportStats();
  void setIfReportStats(bool b) { reportStat = b; }

  int getBlockSize() { return blockSize; }
  auto GetCurrentItemNum() { return _cache_items_list.size(); }

  void setFgFlushLimit(int lmt) { numFgFlushInflightLimit = lmt; }

  // By default, we only allow 6 inflight fore-ground syncing
  constexpr static int kNumFgFlushLimit = 10;

 private:
  // # of blocks in this buffer
  block_no_t blockNum;
  // block size in bytes
  int blockSize;
  // start addr of block buffers
  char *memPtr;

  // stats
  bool reportStat = false;
  uint64_t totalReqNumWithinInterval = 0;
  uint64_t totalHitNumWithinInterval = 0;
  uint64_t totalFlushSubmitWithinInterval = 0;
  int64_t lastStatReportTime = 0;

  std::string bufferName;
  // This vector is only used for desctructor
  // I.e.: no matter how the items are moved into different buffers, always
  // destructed by the original `BlockBuffer`. (see note in destructor)
  std::vector<BlockBufferItem *> bufferItems;
  std::atomic_flag bufferLock;

  // typedefs for cache replacement
  typedef std::pair<block_no_t, BlockBufferItem *> block_buf_pair_t;
  typedef std::list<block_buf_pair_t>::iterator list_iterator_t;
  std::list<block_buf_pair_t> _cache_items_list;
  std::unordered_map<block_no_t, list_iterator_t> _cache_items_map;
  std::list<BlockBufferItem *> freeItems;
  // Use this hashmap to track the dirty blocks
  // <blockNo, index>: user can give the blockNo a index to help retrieve
  // The typical use case is use inode number as index.
  // This is how we implement flush of certain index.
  // if NOT use index, simply set to 0.
  // std::unordered_map<block_no_t, uint32_t> dirtyBlocks;
  ssize_t dirtyItemNum_{0};

  // <index, blockNo>
  std::unordered_map<uint32_t, std::unordered_set<BlockBufferItem *>>
      blockIndexMap_;
  // dirty map is subset of blockIndexMap, the main purpose is for fast
  // fsync(ino)
  std::unordered_map<uint32_t, std::unordered_set<BlockBufferItem *>>
      dirtyIndexMap_;

  // when dirty blocks is over this ratio, will do the flushing.
  // by default we do not actively do flushing at all.
  float dirtyFlushRatio = 1;
  // Control how many blocks will be flushed once checkIfNeedFlush() is true
  uint32_t dirtyFlushOneTimeSubmitNum = 0;

  // at any given time, we allow only max=1 back ground flushing in-flight to
  // device
  std::atomic_bool bgFlushSent;
  int numFgFlushInflight = 0;
  int numFgFlushInflightLimit{kNumFgFlushLimit};

  // used to track if there are foreground *sync* request from Apps
  // e.g., (fsync(ino)), which ino is regarded is index
  // NOTE, only store the FsReq that does not issue *sync*'s flushing
  // It is not able to be flushed, but rejected by *checkIfReachFgFlushLimit*
  // We give the foreground fsync priority, so once this waitForFlushIndexes
  // is not empty, background flushing will be paused
  std::unordered_set<uint32_t> waitForFlushIndexes;

  // init memory layout
  void initMem();
};

#endif  // CFS_BLOCKBUFFER_H
