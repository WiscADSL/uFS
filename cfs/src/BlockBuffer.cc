#include "BlockBuffer.h"

#include <util.h>

#include <cassert>
#include <cstring>
#include <iostream>
#include <list>
#include <mutex>

#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"

// Note: must be called when the lock of this item held (LOCKED statue)
// @param d
// @return
bool BlockBufferItem::setDirty(bool d) {
  bool orig = isDirty;
  isDirty = d;
  return orig;
}

bool BlockBufferItem::blockFetchedCallback() {
  if (status != BlockBufferStatus::BLOCK_BUF_LOCKED) {
    return false;
  } else {
#if 0
    if (isRMWInFlight) {
      // copy the sanpshot back to main dataPtr
      assert(dataPtr != nullptr);
      ::memcpy(dataPtr, savedDataForRMW, dirtyCursor);
      dirtyCursor = 0;
      setDirty(false);
      isRMWInFlight = false;
    }
#endif
    // set the in-mem state
    setInMem(true);
    // FIXME: let's see if there is something wrong if I modify status here
    // without grab the buffer's lock
    setStatus(BlockBufferStatus::BLOCK_BUF_VALID);
#if 0
    if (!notFetched) {
      hasFetchedFromDisk = true;
    }
#endif
  }
  return true;
}

bool BlockBufferItem::blockWritenCallback(uint64_t blockReqSeqNo) {
  if (lastFlushBlockReqSeqNo != blockReqSeqNo) {
    return false;
  }
  bool orig = setDirty(false);
  if (!orig) {
    // something wrong
    SPDLOG_ERROR("buffer clean for blockWritenCallback blockNo:{}", blockNo);
    // need to return false here.
  }
  return true;
}

bool BlockBufferItem::setLastFlushBlockReqSeqNo(uint64_t seqno) {
  if (seqno > lastFlushBlockReqSeqNo) {
    lastFlushBlockReqSeqNo = seqno;
    return true;
  }
  return false;
}

// private functions
BlockBufferItem::buf_idx_t BlockBufferItem::getIdx() { return idx; }

void BlockBufferItem::setBlockNo(block_no_t no) {
  if (isDebug_) {
    fprintf(stderr, "setBlockNo() no:%u\n", no);
  }
  blockNo = no;
}

BlockBufferStatus BlockBufferItem::getStatus() { return status; }

void BlockBufferItem::setStatus(BlockBufferStatus s) { status = s; }

void BlockBufferItem::setInMem(bool b) { inMem = b; }

bool BlockBufferItem::isBufDirty() { return isDirty; }

// @param blockNum
// @param blockSize
// @param memPtr needs to be allocated by caller, whose size = blockNum *
// blockSize
BlockBuffer::BlockBuffer(uint32_t blockNum, int blockSize, char *memPtr)
    : blockNum(blockNum),
      blockSize(blockSize),
      memPtr(memPtr),
      bufferLock(ATOMIC_FLAG_INIT),
      bgFlushSent(false) {
  // Make sure number of index of buffer item will not overflow.
  assert(blockNum <= std::numeric_limits<BlockBufferItem::buf_idx_t>::max());
  initMem();
}

BlockBuffer::BlockBuffer(block_no_t blockNum, int blockSize, char *memPtr,
                         bool isReportStat, const char *bufferName)
    : blockNum(blockNum),
      blockSize(blockSize),
      memPtr(memPtr),
      reportStat(isReportStat),
      bufferName(bufferName),
      bufferLock(ATOMIC_FLAG_INIT),
      bgFlushSent(false) {
  initMem();
  lastStatReportTime = tap_ustime();
}

BlockBuffer::~BlockBuffer() {
  // NOTE: if we need to support `delete someBlockBuf` at runtime
  // Then we need to modify this to walk the `freeItems` and `_cache_items_list`
  for (auto ele : bufferItems) delete ele;
}

void BlockBuffer::initMem() {
  for (block_no_t i = 0; i < blockNum; i++) {
    auto *item =
        new BlockBufferItem(memPtr + ((uintptr_t)i) * blockSize, i, blockSize);
    bufferItems.push_back(item);
    freeItems.push_back(item);
  }
}

// get a buffer slot for blockNo
// if the blockNo has been in buffer, return that bufferItem
// if the blockNo has not been in buffer, allocate one
// if allocate failed (due to all buffer slot locked, like in IO, or dirty),
// return nullptr that is, only one case that nullptr will be returned: cache
// miss and cannot find a buffer for this getBlock() invocation.
// @param blockNo
// @param lockGrabbed
// @param hasDirty: only makes sense when returns nullptr, if hasDirty,
// indicates that, the caller must flush to clean the data, because all the
// unlocked buffer item is dirty
// @return nullptr when buffer is full and cannot insert new item
// Note: when successfully return, the buffer
// is in BLOCK_BUF_LOCKED status, caller check lockGrabbed to see
// if itself grab the buffer or others did before
#define BLOCK_BUF_STATS_REQ_THR (200000)
BlockBufferItem *BlockBuffer::getBlock(block_no_t blockNo, int &lockGrabbed,
                                       bool &hasDirty, uint32_t new_index) {
  //    _cache_items_map is the LRU list, whose head is the most recently used
  //    item when the <blockNo, bufferItem*> is locked, it is not allowed to be
  //    replaced
  lockGrabbed = BLOCK_BUF_LOCK_GRAB_DEFAULT;
#ifndef NONE_MT_LOCK
  while (bufferLock.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  hasDirty = false;
  BlockBufferItem *item = nullptr;
  if (reportStat) {
    if (totalReqNumWithinInterval >= BLOCK_BUF_STATS_REQ_THR) {
      reportStats();
    }
    totalReqNumWithinInterval += 1;
  }
  auto it = _cache_items_map.find(blockNo);
  if (it != _cache_items_map.end()) {
    // cache hit
    item = it->second->second;
    if (item->getStatus() == BlockBufferStatus::BLOCK_BUF_LOCKED) {
      lockGrabbed = BLOCK_BUF_LOCK_GRAB_NO;
    } else {
      _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list,
                               it->second);
      lockGrabbed = BLOCK_BUF_LOCK_GRAB_YES;
    }
    if (reportStat) {
      totalHitNumWithinInterval += 1;
    }
  } else {
    // cache miss
    if (!freeItems.empty()) {
      // cache not full yet, pick one from freelist
      item = freeItems.front();
      freeItems.pop_front();
      _cache_items_list.push_front(block_buf_pair_t(blockNo, item));
      lockGrabbed = BLOCK_BUF_LOCK_GRAB_YES;
    } else if (!_cache_items_list.empty()) {
      // replace (LRU)
      auto last = _cache_items_list.end();
      last--;
// #define BLK_BUF_DEBUG
#ifdef BLK_BUF_DEBUG
      uint32_t dirtyCnt = 0, lockedCnt = 0;
#endif  // BLK_BUF_DEBU
      while (true) {
        if (last->second->getStatus() != BlockBufferStatus::BLOCK_BUF_LOCKED) {
          // the block is not locked
          // do replace
          if (!last->second->isBufDirty()) {
            // Note: skip dirty block for now
#ifdef BLK_BUF_DEBUG
            SPDLOG_INFO(
                "{} replacement find dirtyIter:{} blockedCnt:{} "
                "officialDirtyNum:{} blockNo:{} _cache_items_map:{} "
                "bufferItems:{}",
                bufferName, dirtyCnt, lockedCnt, dirtyItemNum_, blockNo,
                _cache_items_map.size(), bufferItems.size());
#endif  // BLK_BUF_DEBUG
            item = last->second;
            break;
          }
#ifdef BLK_BUF_DEBUG
          else {
            dirtyCnt++;
            // SPDLOG_INFO("{} :dirtyCnt:{} index:{}", bufferName, dirtyCnt,
            //             last->second->getIndex());
          }
#endif  // BLK_BUF_DEBUG

        } else {
          // Here, the block is locked
#ifdef BLK_BUF_DEBUG
          lockedCnt++;
#endif  // BLK_BUF_DEBUG
        // NOTE: now I think this setting of hasDirty is incorrect,
        // but keep it here since the consequence is not serious wrong
        // in case break too much things at a time
        // basically, once a block is locked, it does matter the block
        // is dirty or not, since it means others is using it
          if (!hasDirty && last->second->isBufDirty()) hasDirty = true;
        }

        // see if end the while(true) loop to find item for replacement
        if (!(last == _cache_items_list.begin())) {
          last--;
        } else {
          break;
        }

      }  // end of while(true)

      if (item != nullptr) {
        block_no_t oldBlockNo = item->getBlockNo();
        _cache_items_map.erase(oldBlockNo);
        _cache_items_list.erase(last);
        _cache_items_list.push_front(block_buf_pair_t(blockNo, item));
        // std::cerr << "Replacement-blockidx:" << item->getIdx()
        //           << " oldBlockNo: " << oldBlockNo << " blockNo:" << blockNo
        //           << " oldIndex:" << item->getIndex() << std::endl;
      } else {
        SPDLOG_ERROR(
            "LRU buffer cannot find any block to replace. ListSize:{} ",
            _cache_items_list.size());
        throw std::runtime_error("LRU buffer all dirty");
#ifdef BLK_BUF_DEBUG
        SPDLOG_INFO(" dirtyCnt:{} lockedCnt:{}", dirtyCnt, lockedCnt);
#endif  // BLK_BUF_DEBUG
      }
    }  // end of replacement

    if (item != nullptr) {
      // cache miss, set InMem as false
      item->setInMem(false);
      lockGrabbed = BLOCK_BUF_LOCK_GRAB_YES;
    }
  }
  if (item != nullptr && lockGrabbed == BLOCK_BUF_LOCK_GRAB_YES) {
    auto orig_index = item->getIndex();
    // fprintf(stderr, "blockNo:%u oldIndex:%u newIndex:%u oldBlockNo:%u\n",
    //        blockNo, orig_index, new_index, item->getBlockNo());
    item->setBlockNo(blockNo);
    item->setStatus(BlockBufferStatus::BLOCK_BUF_LOCKED);
    _cache_items_map[blockNo] = _cache_items_list.begin();
    // NOTE: we never put 0 into the blockIndexMap
    // 0 is never a valid ino
    if (orig_index != new_index) {
      if (orig_index != 0) {
        blockIndexMap_[orig_index].erase(item);
      }
      if (new_index != 0) {
        blockIndexMap_[new_index].emplace(item);
      }
    }
    // if new_index is 0, it will be reset back to 0
    // otherwise, just set it
    item->setIndex(new_index);
  }
#ifndef NONE_MT_LOCK
  bufferLock.clear(std::memory_order_release);
#endif
  return item;
}

BlockBufferItem *BlockBuffer::getBlockWithoutAccess(block_no_t blockNo) {
  BlockBufferItem *item = nullptr;
  auto it = _cache_items_map.find(blockNo);
  if (it != _cache_items_map.end()) {
    item = it->second->second;
  }
  return item;
}

const std::list<std::pair<block_no_t, BlockBufferItem *>>
    &BlockBuffer::getAllBufferItems() {
  return _cache_items_list;
}

void BlockBuffer::reportStats() {
  uint64_t curTime = tap_ustime();
  uint64_t intervalUs = (curTime)-lastStatReportTime;
  fprintf(stdout,
          "[Buffer-%s] timeUs:%lu"
          " totalGetBlkReqNum:%lu hitGetBlkReqNum:%lu"
          " numFlushSubmit:%lu flushPerSec:%f\n",
          bufferName.c_str(), intervalUs, totalReqNumWithinInterval,
          totalHitNumWithinInterval, totalFlushSubmitWithinInterval,
          float(totalFlushSubmitWithinInterval) / (1e-6 * intervalUs));
  lastStatReportTime = curTime;
  totalHitNumWithinInterval = 0;
  totalFlushSubmitWithinInterval = 0;
  totalReqNumWithinInterval = 0;
}

// after release, the buffer can be replaced,
// but it may still be valid buffer item
// @param item
// @return
int BlockBuffer::releaseBlock(BlockBufferItem *item) {
  assert(item != nullptr);
#ifndef NONE_MT_LOCK
  while (bufferLock.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  if (item->getStatus() != BlockBufferStatus::BLOCK_BUF_LOCKED) {
    SPDLOG_ERROR("Try to release a block that is not locked");
#ifndef NONE_MT_LOCK
    bufferLock.clear(std::memory_order_release);
#endif
    return -1;
  }
  item->setStatus(BlockBufferStatus::BLOCK_BUF_VALID);
#ifndef NONE_MT_LOCK
  bufferLock.clear(std::memory_order_release);
#endif
  return 1;
}

int BlockBuffer::releaseBlock(block_no_t blockNo) {
  int rt = -1;
#ifndef NONE_MT_LOCK
  while (bufferLock.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  auto it = _cache_items_map.find(blockNo);
  if (it != _cache_items_map.end() &&
      it->second->second->getStatus() == BlockBufferStatus::BLOCK_BUF_LOCKED) {
    it->second->second->setStatus(BlockBufferStatus::BLOCK_BUF_VALID);
    rt = 1;
  }
#ifndef NONE_MT_LOCK
  bufferLock.clear(std::memory_order_release);
#endif
  return rt;
}

bool BlockBuffer::setItemDirty(BlockBufferItem *item, bool isDirty,
                               uint32_t itemIndex) {
  // std::cerr << "setItemDirty blockNo:" << item->getBlockNo()
  //          << " isDirty:" << isDirty << " index:" << itemIndex
  //          << " dirtyIndexMapSize:" << dirtyIndexMap_.size() << std::endl;
  bool orig = item->setDirty(isDirty);
  if (orig != isDirty) {
    if (orig) {
      adjustDirtyItemNum(-1);
      dirtyIndexMap_[itemIndex].erase(item);
    } else {
      adjustDirtyItemNum(1);
      dirtyIndexMap_[itemIndex].emplace(item);
    }
  }
  return orig;
}

void BlockBuffer::releaseUnlinkedInodeDirtyBlocks(uint32_t itemIndex) {
  assert(itemIndex > 1);  // cannot unlink rootino(1)
  auto all_it = blockIndexMap_.find(itemIndex);
  auto dirty_it = dirtyIndexMap_.find(itemIndex);
  int num_dirty_blocks = 0;
  int num_actual_dirty_blocks = 0;
  if (dirty_it != dirtyIndexMap_.end()) {
    num_dirty_blocks = dirty_it->second.size();
  }
  if (all_it != blockIndexMap_.end()) {
    // SPDLOG_INFO("{} releaseBuffer index:{} num:{}", bufferName, itemIndex,
    //             it->second.size());
    for (auto &item : all_it->second) {
      bool orig = item->setDirty(false);
      if (orig) {
        num_actual_dirty_blocks++;
        adjustDirtyItemNum(-1);
      }
      // put item back to the freeitems
      auto cache_it = _cache_items_map.find(item->getBlockNo());
      assert(cache_it != _cache_items_map.end());
      _cache_items_list.erase(cache_it->second);
      _cache_items_map.erase(item->getBlockNo());
      freeItems.push_back(item);
    }
    blockIndexMap_.erase(all_it);
  }
  SPDLOG_DEBUG("num_dirty_blocks:{} num_actual_dirty_blocks:{}",
               num_dirty_blocks, num_actual_dirty_blocks);
  assert(num_dirty_blocks == num_actual_dirty_blocks);
  if (num_dirty_blocks > 0) {
    // we erase this ino since for fsync(inode), we don't write
    // the data blocks to the disk
    dirtyIndexMap_.erase(dirty_it);
  }

  auto it2 = blockIndexMap_.find(itemIndex);
  if (it2 != blockIndexMap_.end()) blockIndexMap_.erase(it2);
}

bool BlockBuffer::checkIfNeedBgFlush() {
  if (checkIfBgFlushInflight()) {
    // if there is BG flushing task not done
    return false;
  }
  if (checkIfFgFlushInflight() > 0 || (!waitForFlushIndexes.empty())) {
    // if there is fore-ground flushing going on
    // or there are FsReq needs to do fore-ground flushing that is waiting
    return false;
  }
  bool nf = getDirtyItemNum() > getBgFlushNumBlockThreshold();
  if (nf) {
    SPDLOG_DEBUG("check if need flush dirtyItem:{} ratio:{} totalNum:{} ret:{}",
                 getDirtyItemNum(), dirtyFlushRatio, blockNum, nf);
  }
  return nf;
}

void BlockBuffer::addFgFlushWaitIndex(uint32_t idx) {
  SPDLOG_DEBUG("addFlushWaitIdx:{}", idx);
  waitForFlushIndexes.emplace(idx);
}

void BlockBuffer::removeFgFlushWaitIndex(uint32_t idx) {
  SPDLOG_DEBUG("remove idx:{}", idx);
  if (waitForFlushIndexes.find(idx) != waitForFlushIndexes.end()) {
    waitForFlushIndexes.erase(idx);
  } else {
    SPDLOG_DEBUG("removeFlushWaitIndex cannot find index. idx:{}", idx);
  }
}

void BlockBuffer::doFlush(bool &canFlush,
                          std::list<block_no_t> &toFlushBlockNos) {
  doFlushByIndex(0, canFlush, toFlushBlockNos);
}

// TODO (jingliu): to improve the performance of flushing.
// Need to find the dirty blocks of this inode quickly
// BlockBuffer::blockIndexMap_ can be used in this case, iterate all blocks
// belongs to that inode, and then see if it is dirty
// if index == 0, it is regarded to ignore index and flush several dirty blocks
void BlockBuffer::doFlushByIndex(uint32_t index, bool &canFlush,
                                 std::list<block_no_t> &toFlushBlockNos) {
  // find blocks to be flushed
  uint32_t bnum = 0;
  canFlush = true;
  if (checkIfFgFlushInflightReachLimit()) {
    canFlush = false;
  } else {
    if (index == 0) {
      // background flush
      for (auto &idxSetPair : dirtyIndexMap_) {
        for (auto item : idxSetPair.second) {
          assert(item->isBufDirty());
          toFlushBlockNos.push_back(item->getBlockNo());
          bnum++;
          if (bnum >= dirtyFlushOneTimeSubmitNum) {
            goto BREAK_OUT;
          }
        }
      }
    BREAK_OUT:;
    } else {
      // fsync to specific inode
      auto it = dirtyIndexMap_.find(index);
      SPDLOG_DEBUG("doFlushByIndex index:{}", index);
      // NOTE: here, for a new created file, the inode won't have entry
      // in this map, since none data block has been added to that inode
      if (it != dirtyIndexMap_.end()) {
        for (auto item : it->second) {
          if (item->isBufDirty()) {
            toFlushBlockNos.push_back(item->getBlockNo());
            bnum++;
          } else {
            std::cerr << "index:" << index << " bno:" << item->getBlockNo()
                      << " itemIndex:" << item->getIndex() << std::endl;
            throw std::runtime_error("error flushByIndex, not dirty");
          }
        }
        // SPDLOG_INFO("index:{} dirtySetSize:{} bNum:{}", index,
        // it->second.size(),
        //      bnum);
      }
    }
    // set flushSet
    if (reportStat) {
      totalFlushSubmitWithinInterval++;
    }
    if (index == 0 && (!toFlushBlockNos.empty())) bgFlushSent = true;
  }
}

uint32_t BlockBuffer::numDirtyByIndex(uint32_t index) {
  uint32_t rt = 0;
  if (index > 0) {
    auto it = dirtyIndexMap_.find(index);
    rt = it->second.size();
  }
  return rt;
}

int BlockBuffer::doFlushDone(std::vector<block_no_t> &blocks) {
  if (!(checkIfBgFlushInflight() || checkIfFgFlushInflight())) {
    SPDLOG_ERROR("doFlushDone called but flushSent is false bgSent:{} FGNum:{}",
                 bgFlushSent, numFgFlushInflight);
    return -1;
  }
  // auto pre_dirty_num = dirtyItemNum_;
  // auto pre_map_size = _cache_items_map.size();
  // cfs_ino_t index = 0;
  for (auto bno : blocks) {
    auto item = getBlockWithoutAccess(bno);
    assert(item != nullptr);
    bool orig = item->setDirty(false);
    if (!orig) {
      std::cerr << "doFlushDone bno:" << bno << " index:" << item->getIndex()
                << " orig:" << orig << std::endl;
      // throw std::runtime_error("orig not dirty");
      return -1;
    }
    adjustDirtyItemNum(-1);
    dirtyIndexMap_[item->getIndex()].erase(item);
    // adjust the LRU list, to make sure this clean blocks can be found firstly
    _cache_items_list.erase(_cache_items_map[bno]);
    _cache_items_list.emplace_back(block_buf_pair_t(bno, item));
    list_iterator_t curIt = _cache_items_list.end();
    std::advance(curIt, -1);
    _cache_items_map[bno] = curIt;
  }
  // SPDLOG_INFO("doFlushDone blockNum:{} pre_dirty_num:{} pos_dirty_num:{}
  // cache_item_size:{} pre_map_size:{}",
  //  blocks.size(), pre_dirty_num, dirtyItemNum_, _cache_items_map.size(),
  //  pre_map_size);
  // bgFlushSent = false;
  return 0;
}

void BlockBuffer::setDirtyFlushRatio(float r) {
  assert(r >= 0 and r <= 1);
  dirtyFlushRatio = r;
}

uint32_t BlockBuffer::getDirtyFlushOneTimeSubmitNum() {
  return dirtyFlushOneTimeSubmitNum;
}

void BlockBuffer::setDirtyFlushOneTimeSubmitNum(uint32_t n) {
  dirtyFlushOneTimeSubmitNum = n;
}

int BlockBuffer::splitBufferItemsByIndex(
    uint32_t index, std::unordered_set<BlockBufferItem *> &itemSet) {
  // fprintf(stderr, "buffer:%s index:%u beforeSplitNum:%lu %lu\n",
  //        bufferName.c_str(), index, _cache_items_map.size(),
  //        _cache_items_list.size());
  auto it = blockIndexMap_.find(index);
  if (it == blockIndexMap_.end()) return -1;
  auto setPtr = &(it->second);

  cfs_bno_t bno;
  auto ino_dirty_bno_it = dirtyIndexMap_.find(index);
  if (ino_dirty_bno_it != dirtyIndexMap_.end()) {
    adjustDirtyItemNum(-(ino_dirty_bno_it->second.size()));
    dirtyIndexMap_.erase(ino_dirty_bno_it);
  }
  // remove from _cache_item_lists
  for (auto item : *setPtr) {
    bno = item->getBlockNo();
    auto curItem = _cache_items_map.find(bno);
    if (curItem == _cache_items_map.end()) {
      std::cerr << "buffer:" << bufferName;
      throw std::runtime_error(
          // std::cerr <<
          " BlockBuffer: invalid bno for cache_map bno:" + std::to_string(bno) +
          " index:" + std::to_string(index) +
          " item_index:" + std::to_string(item->getIndex()));
    }
    list_iterator_t listit = curItem->second;
    BlockBufferItem *bufferItem = listit->second;
    if (bufferItem->getStatus() == BlockBufferStatus::BLOCK_BUF_LOCKED) {
      std::cerr << "buffer:" << bufferName
                << "blockNo:" << bufferItem->getBlockNo() << " locked"
                << std::endl;
      throw std::runtime_error("splitBufferItem item is BLOCK_BUF_LOCKED");
    }
    _cache_items_list.erase(listit);
    _cache_items_map.erase(curItem);
  }

  itemSet.swap(*setPtr);
  blockIndexMap_.erase(index);

  return 0;
}

int BlockBuffer::installBufferItemsOfIndex(
    uint32_t index, const std::unordered_set<BlockBufferItem *> &itemSet) {
  // NOTE: not sure if we are going to allow index==0 here or not
  // fprintf(stderr, "installTo %s index:%u beforeSize:%lu %lu\n",
  //        bufferName.c_str(), index, _cache_items_map.size(),
  //        _cache_items_list.size());
  assert(itemSet.size() > 0);
  cfs_bno_t curBlockNo;
  auto &curSet = blockIndexMap_[index];
  curSet.insert(itemSet.begin(), itemSet.end());
  std::unordered_set<BlockBufferItem *> dirty_set;
  for (auto &item : itemSet) {
    curBlockNo = item->getBlockNo();
    if (item->isBufDirty()) {
      dirty_set.emplace(item);
    }
    _cache_items_list.push_front(block_buf_pair_t(curBlockNo, item));
    _cache_items_map[curBlockNo] = _cache_items_list.begin();
  }
  auto sz = dirty_set.size();
  if (sz > 0) {
    dirtyIndexMap_.emplace(index, std::move(dirty_set));
    adjustDirtyItemNum(sz);
  }
  return 0;
}

int BlockBuffer::reclaimBufferItemSlots(uint numSlots,
                                        std::list<BlockBufferItem *> items) {
  if (freeItems.size() < numSlots) return -1;
  auto it = freeItems.begin();
  std::advance(it, numSlots);
  items.splice(items.begin(), freeItems, freeItems.begin(), it);
  return numSlots;
}
