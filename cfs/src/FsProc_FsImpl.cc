#include "FsProc_FsImpl.h"

#include <string.h>

#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

#include "FsProc_Fs.h"
#include "FsProc_Journal.h"
#include "nlohmann/json.hpp"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "stats/stats.h"
#include "util.h"

extern FsProc *gFsProcPtr;

FsImpl::FsImpl(const FsProcWorker *worker, const char *memPtr,
               float dataDirtyFlushRatio, int numPartitions)
    : numPartitions_(numPartitions),
      dataBlockBufDirtyFlushRato_(dataDirtyFlushRatio),
#ifndef NONE_MT_LOCK
      iMapBlockLock(ATOMIC_FLAG_INIT),
      inodeMapLock(ATOMIC_FLAG_INIT),
      bmapLock(ATOMIC_FLAG_INIT),
#endif
      memPtr_(memPtr) {
  static_assert(sizeof(cfs_mem_block_t) == BSIZE, "");
  //// static_assert(sizeof(cfs_dinode) == 256, "");
  // Make sure one ITable block (512B) only contains 1 inode
  static_assert(sizeof(cfs_dinode) == (SSD_SEC_SIZE), "");
  static_assert(sizeof(cfs_dirent) == 32, "");
  assert(worker != nullptr);
  fsWorker_ = worker;
  idx_ = fsWorker_->getWorkerIdx();

  initMemAddrs();
  initMemAddrBmapBlockBuf();
  initMemAddrInodeSectorBuf();
  initMemAddrDataBlockBuf();
  printSuperBlock();
  permission = FsPermission::getInstance();
}

FsImpl::FsImpl(const FsProcWorker *worker, const char *bmapMemPtr,
               char *dataBlockMemPtr, int numBmapBlocksTotal,
               float dataDirtyFlushRatio, int numPartitions)
    : numBmapOnDiskBlocksTotal_(numBmapBlocksTotal),
      numPartitions_(numPartitions),
      dataBlockBufDirtyFlushRato_(dataDirtyFlushRatio),
#ifndef NONE_MT_LOCK
      iMapBlockLock(ATOMIC_FLAG_INIT),
      inodeMapLock(ATOMIC_FLAG_INIT),
      bmapLock(ATOMIC_FLAG_INIT),
#endif
      memPtr_(nullptr),
      bmapMemPtr_(bmapMemPtr),
      inodeSectorMemPtr_(nullptr),
      dataBlockMemPtr_(dataBlockMemPtr) {
  assert(worker != nullptr);
  fsWorker_ = worker;
  idx_ = fsWorker_->getWorkerIdx();

  initMemAddrBmapBlockBuf();
  initMemAddrDataBlockBuf();
  permission = FsPermission::getInstance();
}

FsImpl::FsImpl(const FsProcWorker *worker, const char *bmapMemPtr,
               const char *inodeSectorMemPtr, char *dataBlockMemPtr,
               int numBmapBlocksTotal, float dataDirtyFlushRatio,
               int numPartitions)
    : numBmapOnDiskBlocksTotal_(numBmapBlocksTotal),
      numPartitions_(numPartitions),
      dataBlockBufDirtyFlushRato_(dataDirtyFlushRatio),
#ifndef NONE_MT_LOCK
      iMapBlockLock(ATOMIC_FLAG_INIT),
      inodeMapLock(ATOMIC_FLAG_INIT),
      bmapLock(ATOMIC_FLAG_INIT),
#endif
      memPtr_(nullptr),
      bmapMemPtr_(bmapMemPtr),
      inodeSectorMemPtr_(inodeSectorMemPtr),
      dataBlockMemPtr_(dataBlockMemPtr) {
  assert(worker != nullptr);
  fsWorker_ = worker;
  idx_ = fsWorker_->getWorkerIdx();
  initMemAddrBmapBlockBuf();
  initMemAddrInodeSectorBuf();
  initMemAddrDataBlockBuf();
  permission = FsPermission::getInstance();
}

FsImpl::~FsImpl() {
  delete inodeSectorBuf_;
  delete bmapBlockBuf_;
  delete dataBlockBuf_;
  if (idx_ == FsProcWorker::kMasterWidConst) {
    delete permission;
  }
}

InMemInode *FsImpl::rootDirInode(FsReq *fsReq) {
  InMemInode *rootInodePtr = getInode(ROOTINO, fsReq);
  return rootInodePtr;
}

//
// Look up a file in a directory
// Assume a directory can only has BSIZE/32 files (128-2 if BSIZE = 4096)
// Note because of "." and ".."
// @param dirName
// @param fileName
// @return inode number if found, 0 if not found
uint32_t FsImpl::lookupDir(FsReq *fsReq, InMemInode *dirInode,
                           const std::string &fileName, bool &error) {
  auto dentryPtr = lookupDirDentry(fsReq, dirInode, fileName, error);
  if (dentryPtr != nullptr) {
    return dentryPtr->inum;
  }
  return 0;
}

cfs_dirent *FsImpl::lookupDirDentry(FsReq *fsReq, InMemInode *dirInode,
                                    const std::string &fileName, bool &error) {
  cfs_dinode *dinodePtr = dirInode->inodeData;
  // uint32_t fileIno = 0;
  BlockBufferItem *itemPtr = nullptr;
  cfs_extent *cur_extent = nullptr;
  struct cfs_dirent *retDirent = nullptr;
  error = false;
  uint64_t totalNumDentry = dinodePtr->size / sizeof(cfs_dirent);
  uint64_t numDentries = 0;
  for (int i = 0; i < NEXTENT_ARR; i++) {
    cur_extent = &dinodePtr->ext_array[i];
    uint64_t blkno;
    for (uint ii = 0; ii < cur_extent->num_blocks; ii++) {
      blkno = cur_extent->block_no + ii;
      SPDLOG_DEBUG("lookupDir- ii:{} cur_extent->numBlocks:{}", ii,
                   cur_extent->num_blocks);
      // std::cerr << "lookupDir blockNo:" << get_data_start_block() + blkno
      //          << std::endl;
      itemPtr = getBlockForIndex(dataBlockBuf_, get_data_start_block() + blkno,
                                 fsReq, dirInode->i_no);
      if (itemPtr->isInMem()) {
        auto *direntPtr = (cfs_dirent *)itemPtr->getBufPtr();
        for (uint j = 0; j < BSIZE / (sizeof(cfs_dirent)); j++) {
          if (strlen((direntPtr + j)->name) > 0) {
            SPDLOG_DEBUG("lookupDir- ino:{}, name:{}", (direntPtr + j)->inum,
                         (direntPtr + j)->name);
          }
          retDirent = (direntPtr + j);
          if (retDirent->inum > 0 && std::string(retDirent->name) == fileName) {
            dataBlockBuf_->releaseBlock(itemPtr);
            if (fsReq->getType() == FsReqType::RENAME &&
                fsReq->getState() >= FsReqState::RENAME_LOOKUP_DST_DIR) {
              fsReq->setDstFileDirentryBlockNo(blkno, j);
            } else {
              fsReq->setFileDirentryBlockNo(blkno, j);
            }
            // return fileIno;
            return retDirent;
          }
          numDentries++;
          if (numDentries >= totalNumDentry) {
            // break this inner loop
            break;
          }
        }
        // not found in this block
        dataBlockBuf_->releaseBlock(itemPtr);
      } else {
        // no need to add submission request here
        // fileIno == 1 and error == false --> do next round lookup after
        // reading dev.
        retDirent = nullptr;
        return retDirent;
      }
    }
  }
  // SPDLOG_INFO("ERROR Cannot find inode for fileName:{}", fileName);
  // fileIno == 1 and error == true --> Inode cannot found in FS.
  error = true;
  retDirent = nullptr;
  fsReq->setError(FS_REQ_ERROR_FILE_NOT_FOUND);
  return retDirent;
}

int FsImpl::checkInodeDentryInMem(FsReq *fsReq, InMemInode *dirInode,
                                  InMemInode *targetInode,
                                  const std::string &fileName) {
  int rt = 0;
  auto posPair = targetInode->getDentryDataBlockPosition(dirInode, fileName);
  block_no_t dentryBlockNo = -1;
  int inoBlockDentryIdx = -1;
  assert(posPair != nullptr);
  SET_DENTRY_DB_POS_FROM_PAIR(posPair, dentryBlockNo, inoBlockDentryIdx);
  AE_UNUSED(inoBlockDentryIdx);

  BlockBufferItem *itemPtr = nullptr;
  if (dentryBlockNo > 0) {
    uint64_t blkno = dentryBlockNo;
    itemPtr = getBlockForIndex(dataBlockBuf_, get_data_start_block() + blkno,
                               fsReq, dirInode->i_no);
    if (!itemPtr->isInMem()) {
      rt = -1;
    } else {
      dataBlockBuf_->releaseBlock(itemPtr);
    }
  }
  return rt;
}

int FsImpl::dentryPointToNewInode(FsReq *fsReq, InMemInode *oldInode,
                                  InMemInode *newInode, InMemInode *dirInode,
                                  const std::string &fileName) {
  int inoBlockDentryIdx = -1;
  block_no_t dentryBlockNo = -1;
  auto pairPtr = oldInode->getDentryDataBlockPosition(dirInode, fileName);
  assert(pairPtr != nullptr);
  SET_DENTRY_DB_POS_FROM_PAIR(pairPtr, dentryBlockNo, inoBlockDentryIdx);

  BlockBufferItem *itemPtr = nullptr;

  if (dentryBlockNo > 0) {
    uint64_t blkno = dentryBlockNo;
    // std::cerr << "dentryPointToNewInode:" << get_data_start_block() + blkno
    //          << std::endl;
    itemPtr = getBlockForIndex(dataBlockBuf_, get_data_start_block() + blkno,
                               fsReq, dirInode->i_no);
    if (itemPtr->isInMem()) {
      // verify the <fname, fino> mapping is in this dataBlock
      auto *direntPtr = (cfs_dirent *)itemPtr->getBufPtr();
      if ((direntPtr + inoBlockDentryIdx)->inum > 0 &&
          std::string((direntPtr + inoBlockDentryIdx)->name) == fileName) {
        // do the replacement here
        (direntPtr + inoBlockDentryIdx)->inum = newInode->i_no;
        dataBlockBuf_->setItemDirty(itemPtr, true, dirInode->i_no);
        // release the block
        dataBlockBuf_->releaseBlock(itemPtr);
      } else {
        throw std::runtime_error("dentryPointToNewInode dentryInvalid");
      }
    } else {
      return -1;
    }
    writeFileInode(fsReq, dirInode->i_no);
  } else {
    SPDLOG_ERROR(
        "dentryPointToNewInode direntry's inode is not recorded in fsreq, "
        "dentryBlockNo:{} inoBlockDentryIdx:{}",
        dentryBlockNo, inoBlockDentryIdx);
    return -1;
  }
  return 0;
}

// NOTE: This function only modify data blocks of the inode.
int FsImpl::removeFromDir(FsReq *fsReq, InMemInode *dirInode,
                          const std::string &fileName) {
  assert(fsReq->getLeafName() == fileName);
  auto inodePtr = fsReq->getTargetInode();
  assert(inodePtr != nullptr);
  int ret = -1;
  int inoBlockDentryIdx = -1;
  block_no_t dentryBlockNo = -1;
  auto posPair = inodePtr->getDentryDataBlockPosition(dirInode, fileName);
  assert(posPair != nullptr);
  SET_DENTRY_DB_POS_FROM_PAIR(posPair, dentryBlockNo, inoBlockDentryIdx);
  ret = removeFromDir(fsReq, dirInode, inoBlockDentryIdx, dentryBlockNo,
                      fileName);
  if (ret == 0) {
    inodePtr->delDentryDataBlockPosition(dirInode, fileName);
  }
  return ret;
}

int FsImpl::removeFromDir(FsReq *fsReq, InMemInode *dirInode,
                          int inoBlockDentryIdx, block_no_t dentryBlockNo,
                          const std::string &fileName) {
  SPDLOG_DEBUG(
      "removeFromDir fileName:{} dentryBlockNo:{} inoBlockDentryIdx:{}",
      fileName, dentryBlockNo, inoBlockDentryIdx);
  auto inodePtr = fsReq->getTargetInode();
  assert(inodePtr != nullptr);

  BlockBufferItem *itemPtr = nullptr;

  if (dentryBlockNo > 0) {
    uint64_t blkno = dentryBlockNo;
    // std::cerr << "removeFromDir:" << get_data_start_block() + blkno
    //          << std::endl;
    itemPtr = getBlockForIndex(dataBlockBuf_, get_data_start_block() + blkno,
                               fsReq, dirInode->i_no);
    if (itemPtr->isInMem()) {
      // verify the <fname, fino> mapping is in this dataBlock
      auto *direntPtr = (cfs_dirent *)itemPtr->getBufPtr();
      if ((direntPtr + inoBlockDentryIdx)->inum > 0 &&
          std::string((direntPtr + inoBlockDentryIdx)->name) == fileName) {
        // do the removal here
        (direntPtr + inoBlockDentryIdx)->inum = 0;
        memset((direntPtr + inoBlockDentryIdx)->name, 0, DIRSIZE);
        dataBlockBuf_->setItemDirty(itemPtr, true, dirInode->i_no);
      } else {
        SPDLOG_ERROR(
            "removeFromDir, the record in inode does not match fileName");
      }
      // release the block
      dataBlockBuf_->releaseBlock(itemPtr);
    } else {
      return -1;
    }
    writeFileInode(fsReq, dirInode->i_no);
  } else {
    SPDLOG_ERROR(
        "removeFromDir direntry's inode is not recorded in fsreq, "
        "dentryBlockNo:{} inoBlockDentryIdx:{}",
        dentryBlockNo, inoBlockDentryIdx);
    return -1;
  }
  return 0;
}

InMemInode *FsImpl::getParDirInode(FsReq *fsReq, bool &is_err) {
  auto tokens = fsReq->getPathTokens();
  return getParDirInode(fsReq, tokens, is_err);
}

// Once return, the lock of directory's inode is not held.
InMemInode *FsImpl::getParDirInode(FsReq *fsReq,
                                   const std::vector<std::string> &pathTokens,
                                   bool &is_err) {
  InMemInode *dirInode = rootDirInode(fsReq);
  is_err = false;
  auto parInode = dirInode;
  if (fsReq->numTotalPendingIoReq() > 0)
    // Need to do IO to fetch the root inode
    return nullptr;
  // auto pathTokens = fsReq->getPathTokens();
  if (pathTokens.size() == 1) {
    // if target of getPar is in root directory, return it directly
    return parInode;
  }

  FsPermission::LevelMap *levelMap = nullptr;
  for (uint i = 0; i < pathTokens.size() - 1; i++) {
    while (!parInode->tryLock()) {
      // spin
    }
    uint32_t curInum = lookupDir(fsReq, dirInode, pathTokens[i], is_err);
    if (is_err || fsReq->numTotalPendingIoReq() > 0) {
      // is_err == true: Error happens, lookup fail
      // is_err == false but need to do io for further lookup
      parInode->unLock();
      return nullptr;
    }

    dirInode = getInode(curInum, fsReq);
    if (fsReq->numTotalPendingIoReq() > 0) {
      parInode->unLock();
      return nullptr;
    }

    if (fsReq->getType() == FsReqType::RENAME &&
        fsReq->getState() >= FsReqState::RENAME_LOOKUP_DST_DIR) {
      fillInodeDentryPositionAfterLookup(fsReq, dirInode, 1);
    } else {
      fillInodeDentryPositionAfterLookup(fsReq, dirInode);
    }

    // release current layer lock
    parInode->unLock();
    parInode = dirInode;

    // record this dentry to a hashmap
    auto *iData = dirInode->inodeData;
    FsPermission::LevelMap *newMap;
    // TODO: Before inserting into map, add permission check here.
    permission->setPermission(levelMap, pathTokens[i],
                              std::make_pair(iData->i_uid, iData->i_gid),
                              dirInode, &newMap);
    levelMap = newMap;
  }
  if (fsReq->getType() == FsReqType::RENAME &&
      fsReq->getState() >= FsReqState::RENAME_LOOKUP_DST_DIR) {
    fsReq->setDstDirMap(levelMap);
  } else {
    fsReq->setDirMap(levelMap);
  }
  return dirInode;
}

cfs_ino_t FsImpl::GetFreeInum(FsReq *fsReq) {
#ifndef NONE_MT_LOCK
  while (iMapBlockLock.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  int imapBlkIdx = -1;
  int inBlkino = -1;
  int k = nextImapNo;
  for (int i = k; i < (IMAPBLOCK_NUM) + k + 1; ++i) {
    imapBlkIdx = i % IMAPBLOCK_NUM;
    nextImapNo = imapBlkIdx;
    inBlkino = find_block_free_bit_no_start_from(
        (void *)&((iMapBlockPtr_ + imapBlkIdx)->blk[0]), nextImapBlockFreeBitNo,
        (BPB));
    // inBlkino = find_block_free_bit_no(
    //    (void *)&((iMapBlockPtr_ + imapBlkIdx)->blk[0]), (BPB));
    if (inBlkino >= 0) {
      nextImapBlockFreeBitNo = inBlkino;
      break;
    } else {
      nextImapBlockFreeBitNo = -1;
    }
  }
  if (inBlkino < 0) {
    SPDLOG_ERROR("Cannot allocateInum, the block is full");
    return 0;
  }
  int ino = BPB * imapBlkIdx + inBlkino;
  assert(ino != 0 && ino != 1);
  block_set_bit(inBlkino, (void *)&((iMapBlockPtr_ + imapBlkIdx)->blk[0]));
#ifndef NONE_MT_LOCK
  iMapBlockLock.clear(std::memory_order_release);
#endif
  return ino;
}

InMemInode *FsImpl::AllocateInode(FsReq *fsReq) {
  cfs_ino_t ino = fsReq->getFileInum();
  if (ino == 0) {
    ino = GetFreeInum(fsReq);
    if (ino == 0) {
      // no free inodes
      return nullptr;
    }

    // We set the fileIno so that if it requires I/O we can re-enter into this
    // function and use getFileInum rather than find another free inode.
    fsReq->setFileIno(ino);
  }

#if CFS_JOURNAL(LOCAL_JOURNAL)
  // Local journal requires syncID so we must retrieve the inode from disk.
  // Performace affects creat() but can be reduced by keeping a pool of free
  // inodes.
  InMemInode *fileInode = getInode(ino, fsReq, /*doSubmit*/ true);
#else
  InMemInode *fileInode = getInode(ino, fsReq, /*doSubmit*/ false);
#endif

  if (fileInode != nullptr) {
    // Mark bitmap_op in logEntry to indicate that inode in inode bitmap needs
    // to be set
    fileInode->logEntry->set_bitmap_op(1);
  }
  return fileInode;
}

int FsImpl::returnInode(FsReq *req, InMemInode *inode) {
  SPDLOG_DEBUG("FsImplReturnInode");
#ifndef NONE_MT_LOCK
  while (iMapBlockLock.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif
  auto imapBlkIdx = inode->i_no / (BPB);
  auto inBlkino = inode->i_no % (BPB);
  assert(block_test_bit(inBlkino,
                        (void *)&((iMapBlockPtr_ + imapBlkIdx)->blk[0])));
  block_clear_bit(inBlkino, (void *)&((iMapBlockPtr_ + imapBlkIdx)->blk[0]));
  inode->logEntry->set_bitmap_op(0);
#ifndef NONE_MT_LOCK
  iMapBlockLock.clear(std::memory_order_release);
#endif
  auto it = dirtyInodeSet_.find(inode->i_no);
  if (it != dirtyInodeSet_.end()) {
    dirtyInodeSet_.erase(it);
  }
  inodeMap_.erase(inode->i_no);
  // dataBlockBuf_->releaseUnlinkedInodeDirtyBlocks(inode->i_no);
  return 0;
}

InMemInode *FsImpl::getFileInode(FsReq *req, uint32_t inodeNo) {
  InMemInode *inodePtr = getInode(inodeNo, req);
  return inodePtr;
}

int FsImpl::writeFileInode(FsReq *req, uint32_t inodeNo,
                           InMemInode *inode_ptr) {
  if (inode_ptr != nullptr) {
    inode_ptr->setDirty(true);
    dirtyInodeSet_.emplace(inodeNo);
    return 1;
  }
  auto it = inodeMap_.find(inodeNo);
  if (it != inodeMap_.end()) {
    SPDLOG_DEBUG("writeFileInode - inodeNo:{} update inodeMap idx_:{}", inodeNo,
                 idx_);
    // inodeMap_[inodeNo]->setDirty(true);
    it->second->setDirty(true);
    dirtyInodeSet_.emplace(inodeNo);
    return 1;
  } else {
    // we cannot find an inode, and it is not loaded into the memory yet
    if (idx_ != 0) {
      throw std::runtime_error("writeFileInode: idx_ != 0");
    }
    SPDLOG_DEBUG("writeFileInode: inodeNo:{} will use getBlock()", inodeNo);
  }
  // the inode is not in-memory
  // the only reason is it is replaced out
  BlockBufferItem *itemPtr =
      getBlock(inodeSectorBuf_, ino2SectorNo(inodeNo), req);
  if (itemPtr->isInMem()) {
    // FIXME (jingliu): do we still need to set this item to dirty? InMemInode
    // has this info.
    inodeSectorBuf_->setItemDirty(itemPtr, true, 0);
    inodeSectorBuf_->releaseBlock(ino2SectorNo(inodeNo));
    return 1;
  }
  // NOTE, should never reach here...
  return 0;
}

int64_t FsImpl::readInodeToUCache(
    FsReq *req, InMemInode *inode,
    std::vector<std::pair<cfs_bno_t, size_t>> &pagesToRead,
    std::vector<std::pair<PageDescriptor *, void *>> &dstVec) {
  assert(pagesToRead.size() == dstVec.size());
  cfs_dinode *dinodePtr = inode->inodeData;
  BlockBufferItem *itemPtr = nullptr;
  cfs_extent *extentPtr = nullptr;
  uint32_t cur_inside_extent_idx, cur_extent_max_block_num;
  uint32_t blockIdx;
  int rioNum = 0;
  uint64_t realBytes = 0;

  for (uint i = 0; i < pagesToRead.size(); i++) {
    if (req->getBlockIoDone((char *)dstVec[i].second)) {
      continue;
    }
    blockIdx = pagesToRead[i].first;
    int cur_extent_arr_idx = getCurrentExtentArrIdx(
        blockIdx, cur_inside_extent_idx, cur_extent_max_block_num);
    extentPtr = &dinodePtr->ext_array[cur_extent_arr_idx];
    uint32_t dataBlockNo = extentPtr->block_no + cur_inside_extent_idx;
    itemPtr = getBlockForIndex(
        dataBlockBuf_, get_data_start_block() + dataBlockNo, req, inode->i_no);
    if (itemPtr->isInMem()) {
      memcpy(dstVec[i].second, itemPtr->getBufPtr(), pagesToRead[i].second);
      realBytes += pagesToRead[i].second;
      req->setBlockIoDone((char *)dstVec[i].second);
      dataBlockBuf_->releaseBlock(itemPtr);
    } else {
      // need to do RIO
      rioNum++;
    }
  }
  if (rioNum > 0) return 0;
  return realBytes;
}

//
// assume inode lock is held
// @param req
// @param inode
// @param dst
// @param offStart
// @param nBytes
// @return size read, or -1 when error
// Latency-ST@bumble: 4K read, if in-memory, readInode() takes ~=0.8us
// if remove *memcpy*, then, 4K read, readInode() takes ~=0.29us
int64_t FsImpl::readInode(FsReq *req, InMemInode *inode, char *dst,
                          uint64_t offStart, uint64_t nBytes, bool nocpy) {
  cfs_dinode *dinodePtr = inode->inodeData;
  uint64_t off = offStart, tot;
  uint32_t m;
  BlockBufferItem *itemPtr = nullptr;
  cfs_extent *extentPtr = nullptr;
  uint64_t realBytes = nBytes;
  char *dstPtr = dst;
  int rioNum = 0;

  // if ask for more than the size of this file
  if (off + nBytes > dinodePtr->size) {
    realBytes = dinodePtr->size - off;
  }

  uint32_t cur_inside_extent_idx, cur_extent_max_block_num;

  for (tot = 0; tot < realBytes; tot += m, off += m, dstPtr += m) {
    // block index of this inode
    uint32_t blockIdx = off / BSIZE;
    m = std::min(realBytes - tot, BSIZE - off % BSIZE);
    if (dst != nullptr && req->getBlockIoDone(dstPtr)) {
      continue;
    }

    int cur_extent_arr_idx = getCurrentExtentArrIdx(
        blockIdx, cur_inside_extent_idx, cur_extent_max_block_num);

    if (cur_extent_arr_idx < 0) {
      SPDLOG_ERROR(
          "ERROR cannot find the extent for this offset blockIdx:{} "
          "curExtArrIdx:{} offStart:{}",
          blockIdx, cur_extent_arr_idx, offStart);
      throw std::runtime_error("readInode cannot find extent idx");
    }

    extentPtr = &dinodePtr->ext_array[cur_extent_arr_idx];
    uint32_t dataBlockNo = extentPtr->block_no + cur_inside_extent_idx;
    itemPtr = getBlockForIndex(
        dataBlockBuf_, get_data_start_block() + dataBlockNo, req, inode->i_no);

    if (itemPtr->isInMem()) {
      // SPDLOG_DEBUG("off:{} m:{}", off, m);
      // fprintf(stderr, "%p buffptr()%p char:%c\n", dstPtr,
      //        itemPtr->getBufPtr(),
      //        *(static_cast<char *>(itemPtr->getBufPtr() + off % BSIZE)));
#ifndef MIMIC_FSP_ZC
      if (!nocpy) {
        memcpy(dstPtr, itemPtr->getBufPtr() + off % BSIZE, m);
      } else {
        // NOTE: dst is 0 means it is doing background readahead
        // And we use dstPtr: an address to track if the request's io is done,
        // it should be safe; since address start from 0 will never be
        // part of application
        assert(dst == nullptr);
      }
#endif
      req->setBlockIoDone(dstPtr);
      dataBlockBuf_->releaseBlock(itemPtr);
    } else {
      // need to do RIO
      rioNum++;
    }
  }
  if (rioNum > 0) return 0;
  return realBytes;
}

int64_t FsImpl::writeEntireInode(FsReq *req, InMemInode *inode,
                                 struct wsyncOp *wsync_op_ptr) {
  cfs_dinode *dinodePtr = inode->inodeData;
  uint64_t off = 0;
  uint32_t tot, m;
  BlockBufferItem *itemPtr = nullptr;
  cfs_extent *extentPtr = nullptr;
  uint64_t realBytes = wsync_op_ptr->file_size;

  struct wsyncAlloc *block_array_ptr =
      reinterpret_cast<struct wsyncAlloc *>(req->getMallocedDataPtr());
  assert(block_array_ptr != nullptr);
  char *srcPtr = nullptr;
  auto cur_app = req->getApp();
  assert(cur_app != nullptr);
  // write data
  uint32_t curInsideExtentIdx, curExtentMaxBlockNum;
  for (tot = 0; tot < realBytes; tot += m, off += m, srcPtr += m) {
    uint32_t curBlockIdx = off / BSIZE;
    assert(curBlockIdx < wsync_op_ptr->array_size);
    auto cur_shm_id = (block_array_ptr + curBlockIdx)->shmid;
    auto cur_data_ptr_id = (block_array_ptr + curBlockIdx)->dataPtrId;
    srcPtr = (char *)cur_app->getDataPtrByShmIdAndDataId(cur_shm_id,
                                                         cur_data_ptr_id);
    m = std::min(realBytes - tot, BSIZE - off % BSIZE);
    // if (req->getBlockIoDone(srcPtr)) {
    //   continue;
    // }
    int curExtentArrIdx = getCurrentExtentArrIdx(
        curBlockIdx, curInsideExtentIdx, curExtentMaxBlockNum);

    assert(curExtentArrIdx >= 0);
    extentPtr = &(dinodePtr->ext_array[curExtentArrIdx]);

    uint32_t dataBlockNo = extentPtr->block_no + curInsideExtentIdx;
    // for the master thread, the bitmap block's first 2 bits is used to
    // represent [[reserved], "/"]
    assert(dataBlockNo != 0);

    // if item not in memory, need to fetch. Thus set default to false
    bool doWrite = false;
    // NOTE: this one is a easy case, all-block write will avoid the READ-IN
    if ((off % BSIZE == 0) && ((m == BSIZE) || ((off + m) > dinodePtr->size))) {
      // only need to issue write request
      bool canOverwrite = false;
      itemPtr = getBlockForIndex(dataBlockBuf_,
                                 get_data_start_block() + dataBlockNo, req,
                                 /*doSubmit*/ false, /*doBlockSubmit*/ false,
                                 canOverwrite, inode->i_no);

      doWrite = (itemPtr->isInMem() || canOverwrite);
    } else {
      // There is no chance for this to be partial write
      throw std::runtime_error("writeEntireInode does not need to do RIO");
      // SPDLOG_DEBUG(
      //     "Partial write, not append. dataBlockNo:{} off:{} size:{}
      //     off+m:{}", dataBlockNo, off, dinodePtr->size, off + m);
      // // partial write of a block, get the block (the default getBlock will
      // // use doSubmit as true
      // itemPtr =
      //     getBlockForIndex(dataBlockBuf_, get_data_start_block() +
      //     dataBlockNo,
      //                      req, inode->i_no);
      // if (itemPtr->isInMem()) {
      //   SPDLOG_DEBUG("doWrite set to true since it is in memory\n");
      //   doWrite = true;
      // } else {
      //   // if that is not in memory, *req* now has pendingIoReqs
      //   assert(req->numTotalPendingIoReq() > 0);
      // }
    }

    assert(doWrite);
    if (doWrite) {
      memcpy(itemPtr->getBufPtr() + off % BSIZE, srcPtr, m);
      dataBlockBuf_->setItemDirty(itemPtr, true, inode->i_no);
      req->setBlockIoDone(srcPtr);
      // we don't need to do it for this, just need to update it to entire size
      // update inode
      // dinodePtr->size = std::max(dinodePtr->size, off + m);
      // inode->logEntry->set_size(dinodePtr->size);
      if (!itemPtr->isInMem()) {
        SPDLOG_DEBUG("This write does not do RIO, but set it to inMem anyway");
        // If the writing does not require fetch block data in memory, set its
        // in memory here blockFetchedCallback() will releaseBlock
        bool b = itemPtr->blockFetchedCallback();
        assert(b);
      } else {
        dataBlockBuf_->releaseBlock(itemPtr);
      }
    } else {
      // wait for other's block IO, or need to first do read.
      // fprintf(stderr, "writeInode return 0\n");
      return 0;
    }
  }

  return realBytes;
}

uint64_t FsImpl::writeInodeAllocDataBlock(FsReq *req, InMemInode *inode,
                                          uint64_t offStart, uint64_t nBytes) {
  cfs_dinode *dinodePtr = inode->inodeData;
  uint64_t off = offStart;

  if (off > dinodePtr->size && req->GetAppendOff() == 0) {
    SPDLOG_ERROR(
        "FsImpl::writeInode invalid argument. off:{}, inode size:{}, nbytes:{}",
        off, dinodePtr->size, nBytes);
    return -1;
  }

  // first allocate enough data block into this inode
  uint64_t end_off = off + nBytes;
  int32_t toAllocateBlockNum = 0;
  if (end_off > dinodePtr->size) {
    // Check if it will increase the size of the file or not.
    toAllocateBlockNum = (end_off - 1) / BSIZE + 1 - dinodePtr->i_block_count;
  }
  if (toAllocateBlockNum > 0) {
    // need to allocate data blocks
    int64_t allocatedNumBlocks =
        allocateDataBlocks(req, inode, toAllocateBlockNum);
    if (allocatedNumBlocks == 0) {
      // need to do RIO
      return 0;
    } else if (allocatedNumBlocks < toAllocateBlockNum) {
      SPDLOG_ERROR("allocate data block, {} blocks wanted, {} allocated",
                   toAllocateBlockNum, allocatedNumBlocks);
      return -1;
    }
  }
  return toAllocateBlockNum;
}

//
// Called with inode lock held.
// @return: number of bytes written if succeed.
//          0 if needs reading devices (for allocate datablock, partial
// overwriting). -1 if error happens.
//
int64_t FsImpl::writeInode(FsReq *req, InMemInode *inode, char *src,
                           uint64_t offStart, uint64_t nBytes) {
  cfs_bno_t first_byte_blkno;
  int first_byte_inblkoff;
  return writeInodeAndSaveDataBlockInfo(req, inode, src, offStart, nBytes,
                                        first_byte_blkno, first_byte_inblkoff);
}

int64_t FsImpl::writeInodeAndSaveDataBlockInfo(FsReq *req, InMemInode *inode,
                                               char *src, uint64_t offStart,
                                               uint64_t nBytes,
                                               cfs_bno_t &first_byte_blkno,
                                               int &first_byte_inblkoff) {
  // see if we need to allocate data blocks first
  int rc = writeInodeAllocDataBlock(req, inode, offStart, nBytes);
  if (rc < 0 || req->numTotalPendingIoReq() > 0) {
    SPDLOG_DEBUG(
        "writeInode after "
        "writeInodeAllocDataBlock() req->numPendingBlockReq() > 0");
    // if numPendingBlockReq() > 0, then needs to do RIO, return
    // if rc < 0, error happens, return
    return rc;
  }

  first_byte_blkno = 0;

  cfs_dinode *dinodePtr = inode->inodeData;
  uint64_t off = offStart;
  uint32_t tot, m;
  BlockBufferItem *itemPtr = nullptr;
  cfs_extent *extentPtr = nullptr;
  uint64_t realBytes = nBytes;
  char *srcPtr = src;
  // write data
  uint32_t curInsideExtentIdx, curExtentMaxBlockNum;
  for (tot = 0; tot < realBytes; tot += m, off += m, srcPtr += m) {
    uint32_t curBlockIdx = off / BSIZE;
    m = std::min(realBytes - tot, BSIZE - off % BSIZE);
    if (req->getBlockIoDone(srcPtr)) {
      continue;
    }
    int curExtentArrIdx = getCurrentExtentArrIdx(
        curBlockIdx, curInsideExtentIdx, curExtentMaxBlockNum);

    assert(curExtentArrIdx >= 0);
    extentPtr = &(dinodePtr->ext_array[curExtentArrIdx]);

    uint32_t dataBlockNo = extentPtr->block_no + curInsideExtentIdx;
    // for the master thread, the bitmap block's first 2 bits is used to
    // represent [[reserved], "/"]
    assert(dataBlockNo != 0);

    // if item not in memory, need to fetch. Thus set default to false
    bool doWrite = false;
    // fprintf(stdout,
    //        "getBlock4write off:%lu, curExtIdx:%d extPtrBlockNo:%lu "
    //        "curInsideExtentIdx:%d blockNo:%lu ino:%u\n",
    //        off, curExtentArrIdx, extentPtr->block_no, curInsideExtentIdx,
    //        dataBlockNo + get_data_start_block(), inode->i_no);
    // NOTE: this one is a easy case, all-block write will avoid the READ-IN
    if ((off % BSIZE == 0) && ((m == BSIZE) || ((off + m) > dinodePtr->size))) {
      // only need to issue write request
      bool canOverwrite = false;
      itemPtr = getBlockForIndex(dataBlockBuf_,
                                 get_data_start_block() + dataBlockNo, req,
                                 /*doSubmit*/ false, /*doBlockSubmit*/ false,
                                 canOverwrite, inode->i_no);

      doWrite = (itemPtr->isInMem() || canOverwrite);
    } else {
      SPDLOG_DEBUG(
          "Partial write, not append. dataBlockNo:{} off:{} size:{} off+m:{}",
          dataBlockNo, off, dinodePtr->size, off + m);
      // partial write of a block, get the block (the default getBlock will
      // use doSubmit as true
      itemPtr =
          getBlockForIndex(dataBlockBuf_, get_data_start_block() + dataBlockNo,
                           req, inode->i_no);
      if (itemPtr->isInMem()) {
        SPDLOG_DEBUG("doWrite set to true since it is in memory\n");
        doWrite = true;
      } else {
        // if that is not in memory, *req* now has pendingIoReqs
        assert(req->numTotalPendingIoReq() > 0);
      }
    }

    if (doWrite) {
      if (first_byte_blkno == 0) {
        first_byte_blkno = dataBlockNo;
        first_byte_inblkoff = (off % BSIZE);
      }
#ifndef MIMIC_FSP_ZC
      memcpy(itemPtr->getBufPtr() + off % BSIZE, srcPtr, m);
#endif
      dataBlockBuf_->setItemDirty(itemPtr, true, inode->i_no);
      req->setBlockIoDone(srcPtr);
      // update inode
      dinodePtr->size = std::max(dinodePtr->size, off + m);
      inode->logEntry->set_size(dinodePtr->size);
      if (!itemPtr->isInMem()) {
        SPDLOG_DEBUG("This write does not do RIO, but set it to inMem anyway");
        // If the writing does not require fetch block data in memory, set its
        // in memory here blockFetchedCallback() will releaseBlock
        bool b = itemPtr->blockFetchedCallback();
        assert(b);
      } else {
        dataBlockBuf_->releaseBlock(itemPtr);
      }
    } else {
      // wait for other's block IO, or need to first do read.
      // fprintf(stderr, "writeInode return 0\n");
      return 0;
    }
  }

  return realBytes;
}

// REQUIRED: inode lock is held
int64_t FsImpl::releaseInodeDataBlocks(FsReq *req, InMemInode *inode) {
  SPDLOG_DEBUG("releaseInodeDataBlocks ino:{}", inode->i_no);
#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "releaseInodeDataBlocks: %u\n", inode->i_no);
#endif
  cfs_extent *extentPtr = nullptr;
  uint32_t curAllocationUnit;
  for (int i = 0; i < NEXTENT_ARR; i++) {
    extentPtr = &(inode->inodeData->ext_array[i]);
    if (extentPtr->block_no > 0) {
      curAllocationUnit = extentArrIdx2BlockAllocUnit(i);
      uint64_t bmapBitOff = extentPtr->block_no % (BPB);
      auto curBmapItemPtr = getBlock(
          bmapBlockBuf_,
          extentPtr->block_no / (BPB) + get_bmap_start_block_for_worker(0),
          req);
      if (curBmapItemPtr->isInMem()) {
        if (curAllocationUnit == 1) {
          assert(block_test_bit(bmapBitOff, curBmapItemPtr->getBufPtr()));
          block_clear_bit(bmapBitOff, curBmapItemPtr->getBufPtr());
        } else if (curAllocationUnit < BPB) {
          for (uint32_t j = 0; j < curAllocationUnit; j++) {
            block_clear_bit(bmapBitOff + j,
                            (void *)curBmapItemPtr->getBufPtr());
          }
        } else {
          // block_clear_bit(0, (void *)curBmapItemPtr->getBufPtr());
          memset(curBmapItemPtr->getBufPtr(), 0, BSIZE);
        }
        bmapBlockBuf_->releaseBlock(curBmapItemPtr);
      } else {
        return 0;
      }
    } else {
      break;
    }
  }
  return 0;
}

// Allocate data blocks to inode
// @param numBlocks, number of blocks to be added
// @param needRio: reference to indicate whether need to do readIO inside.
//              This Rio will be reading the block bitmap.
// @return: number of allocated blocks.
//          -1 if error happens, 0 if need RIO
int64_t FsImpl::allocateDataBlocks(FsReq *fsReq, InMemInode *inode,
                                   uint32_t numBlocks) {
  struct cfs_dinode *dinodePtr = inode->inodeData;
  struct cfs_extent *extentPtr;

  uint32_t curIblockCount = dinodePtr->i_block_count;
  uint32_t insideExtentIdx, extentMaxBlockNum;
  uint32_t n = 0;
#ifndef NONE_MT_LOCK
  while (bmapLock.test_and_set(std::memory_order_acquire)) {
    // spin
  }
#endif

#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "allocateDataBlocks: %u\n", inode->i_no);
#endif

  while (n < numBlocks) {
    // curIblockCount is equal to the next allocated block's idx.
    int curExtentArrIdx = getCurrentExtentArrIdx(
        curIblockCount, insideExtentIdx, extentMaxBlockNum);
    if (curExtentArrIdx < 0) {
      SPDLOG_ERROR(
          "Cannot find slot in current inode's extent_array for blockCount:{}",
          curIblockCount);
      return -1;
    }
    extentPtr = &(dinodePtr->ext_array[curExtentArrIdx]);
    uint32_t curMaxBmapBlocks;
    uint32_t curBmapBlockDiskBlockNo =
        get_bmap_start_block_for_worker(idx_) +
        getDataBMapStartBlockNoForExtentArrIdx(
            curExtentArrIdx, get_dev_bmap_num_blocks_for_worker(idx_),
            curMaxBmapBlocks);
    // curExtentArrIdx, numBmapOnDiskBlocksTotal_, curMaxBmapBlocks);
    if (extentPtr->num_blocks == 0) {
      // need to update bitmap block
      uint32_t curAllocationUnit = extentArrIdx2BlockAllocUnit(curExtentArrIdx);
      if (curAllocationUnit == 1) {
        uint32_t start =
            nextDataBlockAllocWithinUnitBlockIdx[curAllocationUnit];
        for (uint32_t k = start; k < start + curMaxBmapBlocks; ++k) {
          uint32_t i = k % curMaxBmapBlocks;
          nextDataBlockAllocWithinUnitBlockIdx[curAllocationUnit] = i;
          auto curBmapBlockNo = curBmapBlockDiskBlockNo + i;
          if (isBlockBitmapImmutable(curBmapBlockNo)) {
            continue;
          }
          auto curBmapItemPtr = getBlock(bmapBlockBuf_, curBmapBlockNo, fsReq);

          if (curBmapItemPtr->isInMem()) {
            int retryCount = 0;

          FSIMPL_BMAP_ALLOC_RETRY_ONCE_FROM_START:
            int64_t freeBitNo = find_block_free_bit_no_start_from(
                (void *)curBmapItemPtr->getBufPtr(),
                nextDataBlockAllocCurBlkBitNo[curAllocationUnit], (BPB));

            if (freeBitNo >= 0) {
              block_set_bit(freeBitNo, (void *)curBmapItemPtr->getBufPtr());
              nextDataBlockAllocCurBlkBitNo[curAllocationUnit] = freeBitNo;
              extentPtr->block_no = (curBmapBlockDiskBlockNo -
                                     get_bmap_start_block_for_worker(0) + i) *
                                        (BPB) +
                                    freeBitNo;

              SPDLOG_DEBUG("extentPtr->block_no is set to:{}",
                           extentPtr->block_no);
              extentPtr->num_blocks++;
              extentPtr->i_block_offset = curIblockCount;
              inode->logEntry->update_extent(extentPtr, true,
                                             /*bmap_modified*/ true);
              bmapBlockBuf_->releaseBlock(curBmapItemPtr);
              goto FSIMPL_BMAP_ALLOCATE_SUCCESS;
            } else {
              nextDataBlockAllocCurBlkBitNo[curAllocationUnit] = -1;
              if (curMaxBmapBlocks == 1 && retryCount++ == 0)
                goto FSIMPL_BMAP_ALLOC_RETRY_ONCE_FROM_START;
            }
            bmapBlockBuf_->releaseBlock(curBmapItemPtr);
          } else {
#ifndef NONE_MT_LOCK
            // need to read the bitmap block
            bmapLock.clear(std::memory_order_release);
#endif
            dinodePtr->i_block_count = curIblockCount;
            inode->logEntry->set_block_count(dinodePtr->i_block_count);
            return 0;
          }
        }
        // cannot find blocks for this extent's allocation unit
        SPDLOG_ERROR("Cannot find block for extent_idx:{} for file ino:{}",
                     curExtentArrIdx, inode->i_no);
        return -1;
      } else if (curAllocationUnit < BPB) {
        uint32_t start =
            nextDataBlockAllocWithinUnitBlockIdx[curAllocationUnit];
        for (uint32_t k = start; k < start + curMaxBmapBlocks; ++k) {
          uint32_t i = k % curMaxBmapBlocks;
          nextDataBlockAllocWithinUnitBlockIdx[curAllocationUnit] = i;
          auto curBmapBlockNo = curBmapBlockDiskBlockNo + i;
          if (isBlockBitmapImmutable(curBmapBlockNo)) {
            continue;
          }

          auto curBmapItemPtr = getBlock(bmapBlockBuf_, curBmapBlockNo, fsReq);
          if (curBmapItemPtr->isInMem()) {
            int64_t freeBitNo = find_block_free_jump_bits_no_start_from(
                (void *)curBmapItemPtr->getBufPtr(), 0, (BPB),
                curAllocationUnit);
            if (freeBitNo >= 0) {
              block_set_bit(freeBitNo, (void *)curBmapItemPtr->getBufPtr());
              extentPtr->block_no = (curBmapBlockDiskBlockNo -
                                     get_bmap_start_block_for_worker(0) + i) *
                                        (BPB) +
                                    freeBitNo;
              // fprintf(stdout, "alloc bit number:%lu\n", extentPtr->block_no);
              // fprintf(stderr, "allocated at bmap block %u by worker %d\n",
              // curBmapBlockNo, idx_);
              extentPtr->num_blocks++;
              extentPtr->i_block_offset = curIblockCount;
              inode->logEntry->update_extent(extentPtr, true,
                                             /*bmap_modified*/ true);
              SPDLOG_DEBUG(
                  "Found block at bmap no: {} and bit: {} by worker {}",
                  curBmapBlockDiskBlockNo + i, freeBitNo,
                  fsWorker_->getWorkerIdx());
              bmapBlockBuf_->releaseBlock(curBmapItemPtr);
              goto FSIMPL_BMAP_ALLOCATE_SUCCESS;
            }
            bmapBlockBuf_->releaseBlock(curBmapItemPtr);
          } else {
#ifndef NONE_MT_LOCK
            // need to read the bitmap block
            bmapLock.clear(std::memory_order_release);
#endif
            dinodePtr->i_block_count = curIblockCount;
            inode->logEntry->set_block_count(dinodePtr->i_block_count);
            return 0;
          }
        }
        // cannot find blocks for this extent's allocation unit
        SPDLOG_ERROR("Cannot find block for extent_idx:{}", curExtentArrIdx);
        return -1;
      } else {
        int curAllocationBmapBlockNum = curAllocationUnit / (BPB);
        for (uint32_t i = 0; i < curMaxBmapBlocks;
             i += curAllocationBmapBlockNum) {
          auto curBmapBlockNo = curBmapBlockDiskBlockNo + i;
          if (isBlockBitmapImmutable(curBmapBlockNo)) continue;

          auto curBmapItemPtr = getBlock(bmapBlockBuf_, curBmapBlockNo, fsReq);
          if (curBmapItemPtr->isInMem()) {
            int *bufIntPtr = (int *)(curBmapItemPtr->getBufPtr());
            if (*bufIntPtr == 0) {
              extentPtr->block_no = (curBmapBlockDiskBlockNo -
                                     get_bmap_start_block_for_worker(0) + i) *
                                    (BPB);
              extentPtr->num_blocks++;
              extentPtr->i_block_offset = curIblockCount;
              inode->logEntry->update_extent(extentPtr, true,
                                             /*bmap_modified*/ true);
              // We only need to set the first block of each allocation unit as
              // allocated This is different from testRWFsUtil.cc (not
              // logically, only per implementation) Mark all the bmap blocks as
              // allocated makes things hard because it might require first read
              // in the bitmap blocks, which is unnecessary.
              memset(curBmapItemPtr->getBufPtr(), 1, BSIZE);
              bmapBlockBuf_->releaseBlock(curBmapItemPtr);
              goto FSIMPL_BMAP_ALLOCATE_SUCCESS;
            }
            bmapBlockBuf_->releaseBlock(curBmapItemPtr);
          } else {
#ifndef NONE_MT_LOCK
            bmapLock.clear(std::memory_order_release);
#endif
            dinodePtr->i_block_count = curIblockCount;
            inode->logEntry->set_block_count(dinodePtr->i_block_count);
            return 0;
          }
        }
        // cannot find blocks for this extent's allocation unit
        SPDLOG_ERROR("Cannot find block for extent_idx:{}", curExtentArrIdx);
        return -1;
      }
    FSIMPL_BMAP_ALLOCATE_SUCCESS:
      n++;
      curIblockCount++;
    } else {
      // Do not need to modify bitmap blocks, only need to change the extent's
      // metadata.
      uint32_t curExtentAllocationNum =
          std::min(numBlocks - n, extentMaxBlockNum - extentPtr->num_blocks);
      extentPtr->num_blocks += curExtentAllocationNum;
      n += curExtentAllocationNum;
      curIblockCount += curExtentAllocationNum;
      // increase num_blocks, no new extent added
      inode->logEntry->update_extent(extentPtr, true, /*bmap_modified*/ false);
    }
  }

#ifndef NONE_MT_LOCK
  bmapLock.clear(std::memory_order_release);
#endif
  uint32_t allocated = curIblockCount - dinodePtr->i_block_count;
  dinodePtr->i_block_count = curIblockCount;
  inode->logEntry->set_block_count(dinodePtr->i_block_count);
  return allocated;
}

int64_t FsImpl::checkAndFlushDirty(FsProcWorker *worker) {
  int64_t numFlushed = 0;
  // flush DataBuffer
  if (dataBlockBuf_->checkIfNeedBgFlush()) {
#if defined(_EXTENT_FOR_LDB_) || defined(_EXTENT_FOR_FILEBENCH_)
    throw std::runtime_error("Error, we don't want bg flush at all!");
#endif
    // SPDLOG_INFO("[BG_FLUSH] flush dirty buffer (BG)");
    if (isDebug) {
      // worker->writeToWorkerLog("will do flushToDisk");
      SPDLOG_DEBUG("checkAndFlushDirty, will do flush");
    }
#ifndef USE_SPDK
    // NOTE: we do not allow the fsp-posix version to do flushing, because it
    // does not make sense in terms of performance.
    SPDLOG_ERROR(
        "Posix Block Device does not allow flushing (i.e., disk io) currently");
    throw std::runtime_error("Posix Block Device does not allow flushing");
#endif
    auto bufFlushReq = new BufferFlushReq(dataBlockBuf_, worker);
    int rc = bufFlushReq->initFlushReqs(0);
    if (rc >= 0) {
      numFlushed = bufFlushReq->submitFlushReqs();
    }
    // Something cause there is item to be flushed but the submission to
    // device fail (e.g., due to device too busy).
    // If none of the block request is actually used, reset the
    // dataBlockBuf's flush status
    if (numFlushed == 0) {
      std::vector<block_no_t> emptyVec;
      dataBlockBuf_->doFlushDone(emptyVec);
    }
  } else {
    numFlushed = -1;
  }
  return numFlushed;
}

int64_t FsImpl::flushInodeData(FsProcWorker *worker, FsReq *req,
                               bool &needFlush) {
  needFlush = true;
  int64_t numFlushed = 0;
  if (!dataBlockBuf_->checkIfFgFlushInflightReachLimit()) {
    if (dataBlockBuf_->checkIfIdxFgFlushInflight(req->getFileInum())) {
      numFlushed = -1;
      return numFlushed;
    }
    auto bufFlushReq = new BufferFlushReq(dataBlockBuf_, worker);
    bufFlushReq->setFsReq(req);
    int numToFlush = bufFlushReq->initFlushReqs(req->getFileInum());
    if (numToFlush > 0) {
      dataBlockBuf_->addFgFlushInflightNum(1);
      numFlushed = bufFlushReq->submitFlushReqs();
      // SPDLOG_INFO("numToFlush:{} numFlushed:{}", numToFlush, numFlushed);
      // SPDLOG_DEBUG("actual numflushed:{}", numFlushed);
      if (numFlushed == 0) {
        // if numFlushed is 0 but still need flush, then call doFlushDone
        // but send it to ready list.
        std::vector<block_no_t> emptyVec;
        dataBlockBuf_->doFlushDone(emptyVec);
      } else {
        dataBlockBuf_->addFgFlushWaitIndex(req->getFileInum());
      }
    } else if (numToFlush == 0) {
      SPDLOG_DEBUG("all blocks are clean numToFlush == 0, set needFlush to F");
      // dataBlockBuf_->removeFgFlushWaitIndex(req->getFileInum());
      needFlush = false;
    }
  } else {
    dataBlockBuf_->addFgFlushWaitIndex(req->getFileInum());
    numFlushed = -1;
  }
  return numFlushed;
}

void FsImpl::appendToDir(FsReq *fsReq, InMemInode *dirInode,
                         InMemInode *fileInode) {
  appendToDir(fsReq, dirInode, fileInode, fsReq->getLeafName());
}

void FsImpl::appendToDir(FsReq *fsReq, InMemInode *dirInode,
                         InMemInode *fileInode,
                         const std::string &entryFileName) {
  struct cfs_dirent cur_dirent;
  cur_dirent.inum = fileInode->i_no;
  SPDLOG_DEBUG("appendToDir fileName:{} DirinodeSize:{}", entryFileName.c_str(),
               dirInode->inodeData->size);
  nowarn_strncpy(&(cur_dirent.name[0]), entryFileName.c_str(), DIRSIZE);
  cfs_bno_t first_byte_blkno;
  int first_byte_inblkoff;
  // NOTE: here appendToDir really just do append to the end of the dir's data.
  // This will result in fast append, but slow retrieval when a lot of files are
  // deleted and space are wasted in one directory.
  writeInodeAndSaveDataBlockInfo(
      fsReq, dirInode, (char *)(&cur_dirent), dirInode->inodeData->size,
      sizeof(struct cfs_dirent), first_byte_blkno, first_byte_inblkoff);
  SPDLOG_DEBUG("appendToDir after dirInodeSize:{}", dirInode->inodeData->size);
  SPDLOG_DEBUG("appendToDir first_byte_blkno:{} first_byte_inblkoff:{}",
               first_byte_blkno, first_byte_inblkoff);
  fileInode->addDentryDataBlockPosition(
      dirInode, entryFileName, first_byte_blkno,
      first_byte_inblkoff / (sizeof(struct cfs_dirent)));
}

//
// will NOT hold the lock of the ino's inode
// @param ino
// @param fsReq
// @return
//
InMemInode *FsImpl::getInode(uint32_t ino, FsReq *fsReq, bool doSubmit) {
  InMemInode *inodePtr = nullptr;
  BlockBufferItem *bufferItemPtr = nullptr;
#ifndef NONE_MT_LOCK
  while (inodeMapLock.test_and_set(std::memory_order_acquire)) {
    //
  }
#endif
  auto it = inodeMap_.find(ino);
  if (it != inodeMap_.end() && (it->second)->isInodeValid()) {
    inodePtr = it->second;
  } else {
    if (idx_ != 0) {
      throw std::runtime_error("Secondary attempting to load inode from disk");
    }
    auto master = static_cast<const FsProcWorkerMaster *>(fsWorker_);

    // Ideally, at this point, inode ino should truly never have been loaded in
    // mem. However, primary may be wanting to read an inode that has been
    // reassigned or for directory ops.
    // This can be safe as long as we have proper asserts in place as seen in
    // https://github.com/jingliu9/ApparateFS/pull/151/commits/f35e5c2e540923e4edc54855d6975d7bbe5f3423
    auto owner = master->getInodeOwner(ino);
    if (owner != FsProcWorker::kMasterWidConst) {
      auto search = master->unsafe_reassigned_inodes.find(ino);
      assert(search != master->unsafe_reassigned_inodes.end());
      return search->second;
    }

    if (inodeSectorBuf_ == nullptr) {
      SPDLOG_WARN(
          "getInode() called, inodeMap_ missted (ino:{}), but no inodeBuf",
          ino);
#ifndef NONE_MT_LOCK
      inodeMapLock.clear(std::memory_order_release);
#endif
      return nullptr;
    }

    // check inode Sector Buffer
    if (doSubmit) {
      bufferItemPtr = getBlock(inodeSectorBuf_, ino2SectorNo(ino), fsReq);
    } else {
      bool canOverwritte = false;
      bufferItemPtr =
          getBlock(inodeSectorBuf_, ino2SectorNo(ino), fsReq,
                   /*doSubmit*/ false, /*doBlockSubmit*/ false, canOverwritte);
      assert(canOverwritte);
      // once return, the lock is not called because we specify doSubmit=false
      bool b = bufferItemPtr->blockFetchedCallback();
      assert(b);
    }
    if (bufferItemPtr->isInMem()) {
      if (it == inodeMap_.end()) {
        // need to insert for this ino
        inodePtr = new InMemInode(ino, FsProcWorker::fromIdx2WorkerId(idx_));
        inodeMap_.insert(std::make_pair(ino, inodePtr));
      } else {
        inodePtr = it->second;
      }

      // block buffer grabbed, fill the inode
      inodePtr->setValid(true);
      cfs_dinode *inodeDataPtr = (cfs_dinode *)bufferItemPtr->getBufPtr();
      inodePtr->inodeData = inodeDataPtr;
#if CFS_JOURNAL(ON)
      // jinode data is only required for journal mode to checkpoint
      inodePtr->jinodeData = new cfs_dinode();
      memcpy(inodePtr->jinodeData, inodePtr->inodeData, sizeof(cfs_dinode));

      // NOTE: assumes inodePtr->logEntry is previously NULL.
      // TODO: possible memory leak if logEntry was previously not null.
      // Ensure that this is the only place logEntry is populated.
      // TODO: micro optimization: allocate log entry from object pool.
      // The log entry syncID is always inodeOnDisk syncID + 1
      inodePtr->logEntry =
          new InodeLogEntry(inodePtr->i_no, inodeDataPtr->syncID + 1, inodePtr);
#endif

      if (doSubmit) {
        // release block buffer
        inodeSectorBuf_->releaseBlock(bufferItemPtr);
      }
    }
  }
#ifndef NONE_MT_LOCK
  inodeMapLock.clear(std::memory_order_release);
#endif
  return inodePtr;
}

BlockBufferItem *FsImpl::getBlockForIndex(BlockBuffer *blockbuf,
                                          uint32_t blockNo, FsReq *fsReq,
                                          uint32_t index) {
  bool needSubmit = false;
  return getBlockForIndex(blockbuf, blockNo, fsReq, /*doSubmit*/ true,
                          /*doBlockSubmit*/ false, needSubmit, index);
}

BlockBufferItem *FsImpl::getBlockForIndex(BlockBuffer *blockBuf,
                                          uint32_t blockNo, FsReq *fsReq,
                                          bool doSubmit, bool doBlockSubmit,
                                          bool &canOverwritten,
                                          uint32_t index) {
  return getBlock(blockBuf, blockNo, fsReq, doSubmit, doBlockSubmit,
                  canOverwritten, index);
}

BlockBufferItem *FsImpl::getBlock(BlockBuffer *blockBuf, uint32_t blockNo,
                                  FsReq *fsReq) {
  bool needSubmit = false;
  return getBlock(blockBuf, blockNo, fsReq, /*doSubmit*/ true,
                  /*doBlockSubmit*/ false, needSubmit);
}

//
// @param blockBuf
// @param blockNo
// @param fsReq
// @param doSubmit: used for cases that only block needs to be locked for
// metadata, but no need for reading data. E.g. whole block writing, only need
// to allocate a block, write to the buffer item, submit writing request, but
// no need to submit a read request
// @param doBlockSubmit: used to distingush the difference between the size of
// buffers. currently only two size is using this blockBuffer: BLOCK (4K) and
// SECTOR (512B). I.e., doBlockSubmit set to true --> will submit a 4K RIO
// doBlockSubmit set to false --> submit a 512B RIO to device.
// -- if @doSubmit set to false, this will have no effect
// @param canOverwritten: reference value to indicate that whether or not the
// result buffer can be used for the caller to be directly overwritten. Because
// when it returns and item is not in memory, for a writing's getBlock: 1) lock
// held by caller, can do overwriting 2) lock not held by caller, then, cannot
// do overwriting Even doSubmit is set to false, it does not necessarily mean
// that caller can do overwriting (case 2 above).
// @return  return BlockBufferItem, whose status is LOCKED
// if item is not in Memory,
//   1) caller needs to submit block IO request, this can be figured out by
//      checking the fsReq's blockingPendingMap
//   2) corresponding block IO has been submitted, caller need to wait for block
// IO completion if item is in memory, the function guarantee caller will grab
// the lock of this buffer item
BlockBufferItem *FsImpl::getBlock(BlockBuffer *blockBuf, uint32_t blockNo,
                                  FsReq *fsReq, bool doSubmit,
                                  bool doBlockSubmit, bool &canOverwritten,
                                  uint32_t new_index) {
  SPDLOG_DEBUG("FsImpl::getBlock() blockSize:{} blockNo:{}",
               blockBuf->getBlockSize(), blockNo);

  BlockBufferItem *item = nullptr;
  int lockGrabbed = BLOCK_BUF_LOCK_GRAB_DEFAULT;
  bool bufferNeedFlush = false;
  while (item == nullptr) {
    item = blockBuf->getBlock(blockNo, lockGrabbed, bufferNeedFlush, new_index);
    // TODO: do flush when bufferNeedFlush is true
    if (bufferNeedFlush) {
      SPDLOG_ERROR("BufferNeedFlush bufferName:{}", blockBuf->getBufferName());
      throw std::runtime_error("buffer needs flush, all dirty");
    }
    if (item != nullptr && (lockGrabbed == BLOCK_BUF_LOCK_GRAB_NO)) {
      if (item->isInMem()) {
        // though item is in memory, lock is owned by others, retry
        item = nullptr;
      } else {
        // lock owned by others and IO is ongoing, need to
        // send req to pending list. caller will do that
        // std::cout << "FsImpl::getBlock addBlockWaitReq blockNo:" << blockNo
        // << std::endl; fsReq->setDebug(true);
        if (blockBuf->getBlockSize() == (SSD_SEC_SIZE)) {
          fsReq->addSectorWaitReq(blockNo);
        } else {
          fsReq->addBlockWaitReq(blockNo);
        }
        canOverwritten = false;
        assert(item->getBlockNo() == blockNo);
        return item;
      }
    }
  }
  // Once coming here, the lock is held by the caller.
  if (item->isInMem()) {
    // lock grabbed and block is in memory
    canOverwritten = true;
  } else {
    if (doSubmit) {
      // need to do block IO
      canOverwritten = false;
      FsBlockReqType curType;
      if (blockBuf->getBlockSize() == (SSD_SEC_SIZE)) {
        if (!doBlockSubmit) {
          curType = FsBlockReqType::READ_NOBLOCKING_SECTOR;
        } else {
          curType = FsBlockReqType::READ_BLOCKING_SECTOR;
        }
        // this block is essentially a sector
        fsReq->addSectorSubmitReq(blockNo, item, item->getBufPtr(), curType);
      } else {
        if (!doBlockSubmit) {
          curType = FsBlockReqType::READ_NOBLOCKING;
        } else {
          curType = FsBlockReqType::READ_BLOCKING;
        }
        fsReq->addBlockSubmitReq(blockNo, item, item->getBufPtr(), curType);
      }
    } else {
      canOverwritten = true;
    }
  }
  assert(item->getBlockNo() == blockNo);
  return item;
}

// Initialize the blockBuffer's for data bitmap, inode blocks, data blocks
// For the stats of bmap and inode
// bmap will only function in writing path.
// inode blocks has its own in memory hash-map (unordered_map), so the buffer
// hit ratio does not make su much sense. NOTE!!!: All the blockbuffer will use
// the real on-disk block number is the key for block access.
void FsImpl::initMemAddrs() {
  // superblock
  sbBlockPtr_ = ((cfs_mem_block_t *)memPtr_) + SB_BLOCK_NO;
  memSb_ = (struct cfs_superblock *)sbBlockPtr_;
  // calculate # of blocks that is used for data bitmap from super-block
  numBmapOnDiskBlocksTotal_ = memSb_->inode_start - memSb_->bmap_start;
  SPDLOG_INFO(
      "Original: numBmapBlocksTotal:{} memSb->inode_start:{} "
      "memSb->bmap_start:{} "
      "NMEM_BMAP_BLOCK:{}. Will do adjustment according to partitions",
      numBmapOnDiskBlocksTotal_, memSb_->inode_start, memSb_->bmap_start,
      NMEM_BMAP_BLOCK);
  assert(numPartitions_ > 0);
  // inode bitmap
  // assertion to make sure that one ibmap block is sufficient to represent
  // the number of inodes FS supports
  static_assert(IMAPBLOCK_NUM == (NMEMINODE) / BPB + 1,
                "inode buffer not large enough to cache all on-disk inodes");
  static_assert(
      IMAPBLOCK_NUM == (NINODES) / BPB + 1,
      "number of inodes goes over one imap block's representation limit");
  iMapBlockPtr_ = sbBlockPtr_ + 1;
  // data bitmap
  cfs_mem_block_t *blockPtr = iMapBlockPtr_ + IMAPBLOCK_NUM;
  bmapMemPtr_ = blockPtr->blk;
  // inode sector
  blockPtr += (NMEM_BMAP_BLOCK);
  inodeSectorMemPtr_ = blockPtr->blk;
  // data blocks
  blockPtr += (totalInodeBufferMemByte() / (BSIZE));
  dataBlockMemPtr_ = blockPtr->blk;
}

void FsImpl::initMemAddrBmapBlockBuf() {
  fprintf(stderr, "initMemAddrBmapBlockBuf called for idx:%d bmapPtr:%p\n",
          idx_, bmapBlockBuf_);
  assert(bmapBlockBuf_ == nullptr);
  block_no_t pttBmapNumBlocks = 0;
  char *bmapBlkMemPtr =
      getBmapBlockMemPtrPartition(bmapMemPtr_, numBmapOnDiskBlocksTotal_,
                                  numPartitions_, idx_, pttBmapNumBlocks);
  fprintf(stdout,
          "initBmapBuf - bmapMemPtr_:%p, numBmapOnDiskBlocksTotal_:%u, "
          "NMEM_BMAP_BLK:%d, numPartitions_:%d, idx_:%d, pttBmapNumBlocks:%u\n",
          bmapMemPtr_, numBmapOnDiskBlocksTotal_, NMEM_BMAP_BLOCK,
          numPartitions_, idx_, pttBmapNumBlocks);
  bmapBlockBuf_ = new BlockBuffer(pttBmapNumBlocks, BSIZE, bmapBlkMemPtr);
  bmapBlockBuf_->setBufferName(std::string("Bmap-") + std::to_string(idx_));
}

void FsImpl::initMemAddrInodeSectorBuf() {
  assert(inodeSectorBuf_ == nullptr);
  if (idx_ == 0) {
    block_no_t pttInodeNumSectors = 0;
    char *inodeSecMemPtr = getInodeBlockMemPtrPartition(
        inodeSectorMemPtr_, idx_, pttInodeNumSectors);
    fprintf(
        stdout,
        "initInodeBuf - inodeSectorMemPtr_:%p idx_:%d, pttInodeNumSectors:%u\n",
        inodeSectorMemPtr_, idx_, pttInodeNumSectors);
    inodeSectorBuf_ =
        new BlockBuffer(pttInodeNumSectors, ISEC_SIZE, inodeSecMemPtr);
  } else {
    assert(inodeSectorMemPtr_ != nullptr);
    throw std::runtime_error("inodeSectorMemPtr is null");
  }
}

void FsImpl::initMemAddrDataBlockBuf() {
  assert(dataBlockBuf_ == nullptr);
  block_no_t pttDataNumBlocks = 0;
  char *dataBlkMemPtr = getDataBlockMemPtrPartition(
      dataBlockMemPtr_, numPartitions_, idx_, pttDataNumBlocks);
  fprintf(stdout,
          "initDataBuf - gDataBlockMemPtr_:%p, numPartitions_:%d, idx_:%d, "
          "pttDataNumBlocks:%u curBufMemPtr:%p curBufMemEnd:%p\n",
          dataBlockMemPtr_, numPartitions_, idx_, pttDataNumBlocks,
          dataBlkMemPtr, dataBlkMemPtr + pttDataNumBlocks * (BSIZE));
  std::string curBufferName = "DBlock-" + std::to_string(idx_);
  dataBlockBuf_ = new BlockBuffer(pttDataNumBlocks, BSIZE, dataBlkMemPtr, false,
                                  curBufferName.c_str());
  // Actively flushing
  dataBlockBuf_->setDirtyFlushRatio(dataBlockBufDirtyFlushRato_);
  dataBlockBuf_->setDirtyFlushOneTimeSubmitNum(6);
  // dataBlockBuf_->setIfReportStats(true);
  fprintf(stdout,
          "initDataBuf - dataBlockBuf will flush after %f 4K blocks are dirty. "
          "(%f MB) dirtyRatio:%f oneTimeFlushBlockNum:%d initDirtyNum:%ld\n",
          dataBlockBuf_->getBgFlushNumBlockThreshold(),
          dataBlockBuf_->getBgFlushNumBlockThreshold() * 4 * 0.001,
          dataBlockBuf_->getDirtyFlushRatio(),
          dataBlockBuf_->getDirtyFlushOneTimeSubmitNum(),
          dataBlockBuf_->getDirtyItemNum());
}

void FsImpl::initDataBlockAllocNextFreeBlockIdx() {
  uint32_t curAllocUnit;
  for (int i = 0; i < (NEXTENT_ARR); i++) {
    curAllocUnit = extentArrIdx2BlockAllocUnit(i);
    nextDataBlockAllocWithinUnitBlockIdx[curAllocUnit] = 0;
    nextDataBlockAllocCurBlkBitNo[curAllocUnit] = -1;
  }
}

void FsImpl::fillInodeDentryPositionAfterLookup(FsReq *req, InMemInode *inode,
                                                int dentryNo) {
  assert(dentryNo == 0 || dentryNo == 1);
  int curInodeDentryWithinBlockIdx = 0;
  InMemInode *parInode = req->getDirInode();
  block_no_t curInodeDentryDataBlockNo;
  std::string fname;
  if (dentryNo == 0) {
    curInodeDentryDataBlockNo =
        req->getFileDirentryBlockNo(curInodeDentryWithinBlockIdx);
    fname = req->getLeafName();
  } else {
    curInodeDentryDataBlockNo =
        req->getDstFileDirentryBlockNo(curInodeDentryWithinBlockIdx);
    fname = req->getNewLeafName();
  }
  SPDLOG_DEBUG(
      "fillInodeDentryPositionAfterLookup curInum:{} dstFileInum:{} "
      "dentryBno:{} withinBlockIdx:{} dentryNo",
      req->getFileInum(), req->getDstFileInum(), curInodeDentryDataBlockNo,
      curInodeDentryWithinBlockIdx, dentryNo);
  if (curInodeDentryDataBlockNo > 0) {
    inode->addDentryDataBlockPosition(parInode, fname,
                                      curInodeDentryDataBlockNo,
                                      curInodeDentryWithinBlockIdx);
  }
}

void FsImpl::_fsExitFlushSingleBlockBuffer(FsProcWorker *procHandler,
                                           BlockBuffer *blkbuf, bool forceFlush,
                                           bool dbg) {
  auto bufferItems = blkbuf->getAllBufferItems();
  SPDLOG_INFO("Buffer-{} has {} dirty blocks. wid:{}", blkbuf->getBufferName(),
              blkbuf->getDirtyItemNum(), procHandler->getWid());
  int rc;
  int writeCnt = 0;
  for (auto &curPair : bufferItems) {
    block_no_t curBlockNo = curPair.first;
    BlockBufferItem *curItem = curPair.second;
    if (dbg) {
      fprintf(stderr,
              "===>flush Single Buffer blockno:%u wid:%d blockDirty?:%d\n",
              curBlockNo, procHandler->getWid(), curItem->isBufDirty());
    }
    assert(curItem->getBlockNo() == curBlockNo);
    if (forceFlush || curItem->isBufDirty()) {
      writeCnt++;
      BlockReq req(curBlockNo, curItem, (char *)curItem->getBufPtr(),
                   FsBlockReqType::WRITE_BLOCKING);
      rc = procHandler->submitDirectWriteDevReq(&req);
      assert(rc == 0);
    }
  }
  SPDLOG_INFO("Buffer-{} exit flushing do disk write {} blocks",
              blkbuf->getBufferName(), writeCnt);
  procHandler->writeToWorkerLog(blkbuf->getBufferName() +
                                std::string(" was flushed for number blocks:") +
                                std::to_string(writeCnt));
}

void FsImpl::_fsExitFlushSingleSectorBuffer(FsProcWorker *procHandler,
                                            BlockBuffer *blkbuf) {
  SPDLOG_DEBUG("fsExitFlushSector");
  auto bufferItems = blkbuf->getAllBufferItems();
  int rc;
  for (auto curPair : bufferItems) {
    block_no_t curSectorNo = curPair.first;
    BlockBufferItem *curItem = curPair.second;
    assert(curItem->getBlockNo() == curSectorNo);
    BlockReq req(curSectorNo, curItem, (char *)curItem->getBufPtr(),
                 FsBlockReqType::WRITE_BLOCKING_SECTOR);
    rc = procHandler->submitDirectWriteDevReq(&req);
    assert(rc == 0);
  }
}

void FsImpl::flushMetadataOnExit(FsProcWorker *procHandler) {
  SPDLOG_INFO("Flushing data block bitmaps");
  _fsExitFlushSingleBlockBuffer(procHandler, bmapBlockBuf_, /*forceFlush*/ true,
                                /*dbg*/ false);

  if (!(procHandler->isMasterWorker())) return;

  // in no journal mode, master flushes imap and inode tables (even if other
  // workers own the inodes). This is safe as by this point we are not accepting
  // any more requests that can dirty inodes.
  uint32_t imapStartNo = get_imap_start_block();
  uint32_t imapNumBlocks = get_dev_imap_num_blocks();
  // NOTE: following code assumes that the region after superblock is imap.
  // Should fail if we change the disk layout.
  assert(imapStartNo == (superBlockNumber() + 1));
  cfs_mem_block_t *blockPtr = sbBlockPtr_;
  // point to imap start block
  blockPtr++;

  SPDLOG_INFO("Flushing imap blocks");
  for (uint32_t i = 0; i < imapNumBlocks; i++) {
    BlockReq req(imapStartNo + i, nullptr, (char *)blockPtr,
                 FsBlockReqType::WRITE_BLOCKING);
    int rc = procHandler->submitDirectWriteDevReq(&req);
    if (rc != 0) throw std::runtime_error("Failed to write imap block");
    blockPtr++;
  }

  SPDLOG_INFO("Flushing inode tables");
  _fsExitFlushSingleSectorBuffer(procHandler, inodeSectorBuf_);

  SPDLOG_INFO("Flushing superblock");
  blockPtr = sbBlockPtr_;
  BlockReq sbreq(superBlockNumber(), nullptr, (char *)blockPtr,
                 FsBlockReqType::WRITE_BLOCKING);
  int rc = procHandler->submitDirectWriteDevReq(&sbreq);
  if (rc != 0) throw std::runtime_error("Failed to write superblock");
}

int FsImpl::installInode(InMemInode *inode) {
  auto it = inodeMap_.find(inode->i_no);
  if (it != inodeMap_.end()) {
    // TODO (ask jing): if we call installInode when secondary is returning
    // inode to primary during unlink, this warning will trigger. Is this
    // because primary always has the inode in it's inodeMap_ regardless of
    // whether we migrate?
    SPDLOG_WARN("install Inode, i_no:{} already existing. wid:{}", inode->i_no,
                FsProcWorker::fromIdx2WorkerId(idx_));
    return -1;
  }
  inodeMap_.emplace(inode->i_no, inode);
  if (inode->getDirty()) {
    dirtyInodeSet_.emplace(inode->i_no);
  }
  return 0;
}

void FsImpl::uninstallInode(InMemInode *inode) {
  // TODO: instead, have an export and import inode where we return all the
  // state that a worker needs to import it.
  inodeMap_.erase(inode->i_no);
  dirtyInodeSet_.erase(inode->i_no);
#if CFS_JOURNAL(ON)
  unlinkedInodeSet_.erase(inode->i_no);
#endif
}

void FsImpl::splitInodeDataBlockBufferSlot(
    InMemInode *inode, std::unordered_set<BlockBufferItem *> &items) {
  assert(inode->i_no == inode->inodeData->i_no);
  dataBlockBuf_->splitBufferItemsByIndex(inode->i_no, items);
}

void FsImpl::installInodeDataBlockBufferSlot(
    InMemInode *inode, const std::unordered_set<BlockBufferItem *> &items) {
  dataBlockBuf_->installBufferItemsOfIndex(inode->i_no, items);
}

int FsImpl::dumpAllInodesToFile(const char *dirname) {
#define DUMP_PACK_INMEMINODE_ATTR_TO_JSON(j, inode, attr) \
  (j[#attr] = inode->attr)
  int ret = 0;
  ret = mkdir(dirname, 0755);
  assert(ret == 0);
  std::string fname = std::string(dirname) + "/inodes.json";
  std::ofstream dumpf;
  dumpf.open(fname, std::ios::app);
  nlohmann::json jtotal;
  if (dumpf.is_open()) {
    for (auto &inode : inodeMap_) {
      nlohmann::json j;
      DUMP_PACK_INMEMINODE_ATTR_TO_JSON(j, inode.second, i_no);
      DUMP_PACK_INMEMINODE_ATTR_TO_JSON(j, inode.second, ref);
      DUMP_PACK_INMEMINODE_ATTR_TO_JSON(j, inode.second, flags);
      DUMP_PACK_INMEMINODE_ATTR_TO_JSON(j, inode.second, isLocked);
      cfs_dinode *dinode = inode.second->inodeData;
      nlohmann::json jsub;
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, nlink);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_dentry_count);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, type);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_no);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_uid);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_gid);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, i_block_count);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, size);
      DUMP_PACK_DINODE_ATTR_TO_JSON(jsub, dinode, syncID);
      for (int i = 0; i < NEXTENT_ARR; i++) {
        std::string attrName = "extent-" + std::to_string(i);
        jsub[attrName] = {
            {"i_block_offset", dinode->ext_array[i].i_block_offset},
            {"num_blocks", dinode->ext_array[i].num_blocks},
            {"block_no", dinode->ext_array[i].block_no},
        };
      }
      j["inodeData"] = jsub;
      jtotal[std::to_string(inode.first)] = j;
      // dumpf << j.dump();
      // dumpf << ",";
      // dumpf << std::endl;
    }
    dumpf << jtotal.dump();
    // dump imap
    if (idx_ == FsProcWorker::kMasterWidConst) {
      std::string imapName = std::string(dirname) + "/imap";
      int fd = open(imapName.c_str(), O_CREAT | O_RDWR, 0755);
      if (fd < 0) {
        fprintf(stderr, "cannot open /tmp/imap\n");
      }
      int rw = write(fd, iMapBlockPtr_, IMAPBLOCK_NUM * BSIZE);
      assert(rw == IMAPBLOCK_NUM * BSIZE);
      close(fd);
    }
    // dump data blocks
    std::string bmapName = std::string(dirname) + "/bmap";
    auto bufferItems = bmapBlockBuf_->getAllBufferItems();
    for (auto &curPair : bufferItems) {
      BlockBufferItem *curItem = curPair.second;
      std::string curF = bmapName.c_str() + std::to_string(curPair.first);
      int fd = open(curF.c_str(), O_CREAT | O_RDWR, 0755);
      assert(fd >= 0);
      int rw = write(fd, (char *)curItem->getBufPtr(), BSIZE);
      assert(rw == BSIZE);
      close(fd);
    }

  } else {
    SPDLOG_WARN("cannot open for dumpinodes:{}", fname);
    ret = -1;
  }
#undef DUMP_PACK_INMEMINODE_ATTR_TO_JSON
#undef DUMP_PACK_DINODE_ATTR_TO_JSON
  return ret;
}

void FsImpl::addPathInodeCacheItem(FsReq *req, InMemInode *inode) {
  FsPermission::LevelMap *dummy;
  SPDLOG_DEBUG("addPathInodeCacheItem: name:{} ino:{}", req->getLeafName(),
               inode->i_no);
  permission->setPermission(
      req->getDirMap(), req->getLeafName(),
      std::make_pair(inode->inodeData->i_uid, inode->inodeData->i_gid), inode,
      &dummy);
}

void FsImpl::addSecondPathInodeCacheItem(FsReq *req, InMemInode *inode) {
  FsPermission::LevelMap *dummy;
  permission->setPermission(
      req->getDstDirMap(), req->getNewLeafName(),
      std::make_pair(inode->inodeData->i_uid, inode->inodeData->i_gid), inode,
      &dummy);
}

int FsImpl::removePathInodeCacheItem(FsReq *req,
                                     FsPermission::MapEntry *entry) {
  SPDLOG_DEBUG("removeCache leafName:{} dirMapNull?:{}", req->getLeafName(),
               req->getDirMap() == nullptr);
  return permission->deleteEntry(req->getDirMap(), req->getLeafName(), entry);
}

int FsImpl::removeSecondPathInodeCacheItem(FsReq *req,
                                           FsPermission::MapEntry *entry) {
  int ret = permission->deleteEntry(req->getDstDirMap(), req->getNewLeafName(),
                                    entry);
  SPDLOG_DEBUG("remove newFileName:{} ret:{} dirMapNull?{}",
               req->getNewLeafName(), ret, req->getDstDirMap() == nullptr);
  return ret;
}

void FsImpl::printSuperBlock() {
  auto *sb = (struct cfs_superblock *)&(sbBlockPtr_->blk);
  printSuperBlockToStream(stdout, sb);
}
