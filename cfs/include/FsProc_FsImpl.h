#ifndef CFS_INCLUDE_FSPROC_FSIMPL_H_
#define CFS_INCLUDE_FSPROC_FSIMPL_H_

#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "FsProc_FsInternal.h"
#include "FsProc_FsReq.h"
#include "FsProc_Journal.h"
#include "FsProc_PageCache.h"
#include "FsProc_TLS.h"
#include "FsProc_WorkerStats.h"

//
class FsProcWorker;
class FileMng;
class InMemInode;
class InodeLogEntry;
//

typedef struct _cfs_mem_block_t {
  char blk[BSIZE];
} cfs_mem_block_t;

// FIXME: many members in InMemInode are public and can be accessed directly.
// Place them in setters which assert CORE_OWNS for safety.
#define CORE_OWNS(inode) (FsProcTLS::GetWid() == (inode)->getManageWorkerId())

class InMemInode {
 public:
  InMemInode(cfs_ino_t ino, int wid);
  cfs_ino_t i_no;  // inode number
  int ref{0};      // reference count (describe file descriptor level)
  int flags;       // mark the status of given inode (busy, valid)
  // lock guard isLocked, isValid_, inodeData (pointer assign)
  bool isLocked;
  bool isDeleted{false};  // once unlink, set to true
  // when unlink is called on inode that still has open file descriptors but no
  // direntry (nlink = 0), unlinkDeallocResourcesOnClose is set to true so that
  // the unlink resumes after all file descriptors have closed.
  bool unlinkDeallocResourcesOnClose{false};
  // logEntry records all metadata changes related to this inode that
  // should be persisted to the journal on fsync/periodic flush.
  // TODO: (anthony, jing) logEntry should be created whenever inodeData
  // is assigned as at that time we know the syncID as well as inode number.
  InodeLogEntry *logEntry;
  // NOTE: inodeData.nlink can only be modified by the master thread
  cfs_dinode *inodeData;
  // inodeData represents all dirty inode data whereas jinodeData represents
  // only the data that has been written to the journal.
  cfs_dinode *jinodeData;

#ifdef USE_UC_PAGE_CACHE
  InodePageCache inodePageCache;
#endif

  uint64_t next_append_offset = 0;

  // Used for readahead
  off_t readaheadOff{0};
  bool readaheadInflight{false};
  int readaheadBgReqNum{0};

  // lease info
  std::set<pid_t> leased_app;

  bool setDirty(bool d) {
    assert(CORE_OWNS(this));
    bool r = isDirty_;
    isDirty_ = d;
    return r;
  }

  bool getDirty() { return isDirty_; }

  bool setValid(bool v) {
    assert(CORE_OWNS(this));
    bool r = isValid_;
    isValid_ = v;
    return r;
  }

  void initNewAllocatedDinodeContent(mode_t tp);

  void adjustDentryCount(int delta) {
    assert(CORE_OWNS(this));
    inodeData->i_dentry_count += delta;
    logEntry->set_dentry_count(inodeData->i_dentry_count);
  }

  void setDentryCount(uint16_t val) {
    assert(CORE_OWNS(this));
    inodeData->i_dentry_count = val;
    logEntry->set_dentry_count(inodeData->i_dentry_count);
  }

  void adjustNlink(int delta) {
    assert(CORE_OWNS(this));
    inodeData->nlink += delta;
    assert(inodeData->nlink >= 0);
    logEntry->set_nlink(inodeData->nlink);
  }

  void setNlink(uint8_t nl) {
    assert(CORE_OWNS(this));
    inodeData->nlink = nl;
    logEntry->set_nlink(inodeData->nlink);
  }

  int8_t getNlink() { return inodeData->nlink; }

  void setDeleted() {
    assert(CORE_OWNS(this));
    assert(!isDeleted);
    isDeleted = true;
  }

  bool isInodeDeleted() { return isDeleted; }

  // check if the InMemInode is valid
  // It is possible that, though this entry exists in inode map,
  // the underlining block buffer item has been replaced
  // So, need to check if in-memory i_no == inodeData->i_no
  // To summarize the valid status of InMemInode:
  // new InMemInode --> not valid, then after getBlock() and the
  // data is fetched, getInode() will set the valid to true.
  // If underlining buffer replace the buffer, then it should
  // be regarded as invalid even though the variable isValid_ is true
  // @return true if valid
  bool isInodeValid() {
    bool blockBufferValid = true;
    if (isValid_) {
      // only if isValid_ is set: aka. the inode once been fetched
      // from disk, the blockBufferValid comparison makes sence
      // otherwise, inodeData == nullptr
      blockBufferValid = (inodeData->i_no == i_no);
    }
    return (isValid_ & blockBufferValid);
  }

  // lock the status of this inode
  // @return true if lock hold
  bool tryLock();
  bool unLock();

  bool isAppProcReferring(pid_t pid);
  bool noAppReferring() { return ref == 0; }

  static constexpr int kUnknownManageWorkerId = -1;
  int getManageWorkerId() { return mngWid_; }
  // only the current ownership can change this wid to another
  // or, if the inode is not managed by anybody, we allow someone to claim it
  bool setManageWorkerId(int curOwnerWid, int wid) {
    if (wid != kUnknownManageWorkerId && curOwnerWid != mngWid_) return false;
    mngWid_ = wid;
    return true;
  }
  bool setManageWorkerUnknown(int curOwnerWid) {
    if (curOwnerWid != mngWid_) return false;
    mngWid_ = kUnknownManageWorkerId;
    return true;
  }
  void addDentryDataBlockPosition(InMemInode *parInode,
                                  const std::string &fileName,
                                  block_no_t dentryDataBlockNo,
                                  int withinBlockDentryIndex);
  inode_dentry_dbpos_t *getDentryDataBlockPosition(InMemInode *parInode,
                                                   const std::string &fileName);
  int delDentryDataBlockPosition(InMemInode *parInode,
                                 const std::string &fileName);

  void addFd(pid_t pid, FileObj *fobj);
  void delFd(pid_t pid, FileObj *fobj);
  const auto &getAppFdMap() const { return appFdMap_; }
  int GetLastWindowNumTau() { return last_window_num_tau; }

 private:
  bool isValid_;
  bool isDirty_;
  std::atomic_flag lock_;
  std::unordered_map<pid_t, std::unordered_map<int, FileObj *>> appFdMap_;

#if defined SCALE_USE_ATOMIC_DATA
  // the version number when recording the access of this file
  uint64_t cur_stats_window_vid_;
  // pid:tid forms a unique id, we do this to make it hashable
  // so if size(this set) > 0: then this file is a shared file
  std::set<uint64_t> cur_stats_window_accessed_taus_;
  perfstat_cycle_t cur_window_cycles_ = 0;
  uint64_t last_stats_window_vid_ = 0;
  perfstat_cycle_t last_window_cycles_ = 0;
  perfstat_cycle_t last_effective_cycles_ = 0;
  uint64_t last_effective_vid_ = 0;
  int last_window_num_tau = 0;
#elif defined SCALE_USE_BUCKETED_DATA
  std::list<InMemInode *>::iterator accessRecorderIter{
      worker_stats::AccessBucketedRecorder::GetGlobalDummyIter()};
  perfstat_cycle_t accessRecorderCycles = 0;
  uint64_t accessRecordVid = 0;
  int curWindowAccessIdx = -1;
  int lastWindowFinalAccessIdx = -1;
#endif

  // workerId that currently manage this inode.
  // by default, managed by master worker
  // NOTE: ONLY THE current worker that *owns* this inode can access this
  int mngWid_;

  // <parentInode, <dentryDataBlockNo, withinBlockDentryIndex>
  std::unordered_map<InMemInode *,
                     std::unordered_map<std::string, inode_dentry_dbpos_t>>
      inodeDentryDataBlockPosMap_;
  // block_no_t dentryDataBlockNo_ = 0;
  // // in the data block of dentry pointed by fileDentryDataBlockNo, the index
  // // of which that contains <fileName, fileIno> mapping.
  // int withinBlockDentryIndex_ = 0;

  friend class worker_stats::AccessBucketedRecorder;
  friend class worker_stats::ParaAwareRecorder;
  friend class SplitPolicyDynamicBasic;
  friend class SplitPolicyDynamicDstKnown;
};

// TODO: put these along with any code for dynamic reassignment of inodes.
struct ExportedInode {
  InMemInode *inode{nullptr};
  int exporter_wid{-1};
  int importer_wid{-1};
  bool in_exporter_dirty_set{false};
#if CFS_JOURNAL(ON) || 1
  bool in_exporter_unlinked_set{false};
  bool in_exporter_journal{false};
#endif
  std::unordered_set<BlockBufferItem *> block_buffers;
  // TODO add more state for reassignment
};

//
// One implementation of FS
// The Fs decides which block # to fetch
// for each FileReq, it will generate block reqs
// which will be finished by bdev
// ---------------------
// buf memory block layout:
// ---------------------
// | superblock (sbBlockPtr_) [ 1 block ]
// | inode bitmap (iMapBlockPtr_) [1 block ]
// | data bitmap (bmapMemPtr_) [ NMEM_BMAP_BLOCK blocks ]
// | inodes (inodeSectorMemPtr_) [ NINODES sectors <=> NINODES/8 blocks ]
// | data blocks (dataBlockMemPtr_) [NMEM_DATA_BLOCK blocks]
//      -- this large trunk of memory will be used to construct several
//      -- BlockBuffer object to serve different threads usage
//      -- this only can contain partial of the on-disk data
//      -- a real cache...
// ---------------------
class FsImpl {
 public:
  static constexpr bool isDebug = false;
  static constexpr float kDataBlockStartFlushRatio = 0.2;

  // This will initialize the whole memory region (pointed by memPtr) layout,
  // build several buffers, including, iMap, iTable, bMap, dataBlockBuffer
  FsImpl(const FsProcWorker *worker, const char *memPtr,
         float dataDirtyFlushRatio, int numPartitions);

  // This will only initialize the bMap and dataBlockBuffer
  // @param numBmapBlocksTotal : real on-disk number of blocks used for bitmap
  //     *Total* means, it should describe the total number of this kind of
  //     blocks
  // @param numPartitions : this should be called by sometome that make decision
  //     on how many partitions we are going to have, mostly, should be the one
  //     that calls the constructor with *fullMemory* as arguments
  FsImpl(const FsProcWorker *worker, const char *bmapMemPtr,
         char *dataBlockMemPtr, int numBmapBlocksTotal,
         float dataDirtyFlushRatio, int numPartitions);

  // @param inodeSectorMemPtr : so the above constructor will use nullptr
  // to initialize inodeSectorMemPtr_
  // The inner difference between these two constructors are:
  //   - If we decide to give the servant the right to use inodeSectorMemPtr,
  //   - which, essentially could result in synchronization
  //      - for using the BlockBuffer (inodeSectorMemPtr)
  //      - ask for one buffer slot and release a buffer slot (getBlock())
  //      - is going to have synchronization
  //   - Currently I tend to NOT give servant thread the *inodeSectorMemPtr_*,
  //      - then, that basically means, once split out, that inode-buffer-slot
  //      - is totally in charge of the servant, the master cannot free that
  //      - buffer slot, but it apparently is not a problem since we can
  //      - cache all the inodes. Another issue is that, that also means,
  //      - servant cannot rely on *getBlock()* -> find not in memory -> doRead
  //      - to fetch inode into memory. Again, because we have inodeMap. This
  //      - is not a problem practically
  // NOTE: this one currently does not work
  FsImpl(const FsProcWorker *worker, const char *bmapMemPtr,
         const char *inodeSectorMemPtr, char *dataBlockMemPtr,
         int numBmapBlocksTotal, float dataDirtyFlushRatio, int numPartitions);

  ~FsImpl();

  static uint64_t singlePartitionInodeBufferMemByte() {
    // make sure we can cache all the inodes
    return (NINODES) * (ISEC_SIZE);
  }

  static uint64_t totalInodeBufferMemByte() {
    // return (NMAX_FSP_WORKER)*singlePartitionInodeBufferMemByte();
    return singlePartitionInodeBufferMemByte();
  }

  static uint64_t totalBlockBufferMemByte() {
    // NOTE: here for the ease of pre-fetching
    // (@FsProcWorkerMaster::initInMemDataAfterDevRead), the organization of
    // | dummy block 0 | super block | imap | (super block, imap are prefetched)
    // is exactly the same as the on-disk layout
    return (/*dummy block 0*/ (uint64_t)1 + /* super block */ (uint64_t)1 +
            /* imap */ (IMAPBLOCK_NUM) + NMEM_BMAP_BLOCK + NMEM_DATA_BLOCK) *
               BSIZE +
           totalInodeBufferMemByte();
  }

  static uint32_t superBlockNumber() { return SB_BLOCK_NO; }

  static void imapBlockNumber(uint32_t &imap_start_block_no,
                              uint32_t &imap_num_blocks) {
    imap_start_block_no = get_imap_start_block();
    imap_num_blocks = get_dev_imap_num_blocks();
  }

  static char *getBmapBlockMemPtrPartition(const char *bmapMemPtr,
                                           uint32_t numBmapBlocksTotal,
                                           int numTotalPartitions, int idx,
                                           block_no_t &pttNumBmapBlocks) {
    assert(numTotalPartitions > 0 && idx >= 0);
    assert(idx < numTotalPartitions);
    assert(numBmapBlocksTotal > 0);
    pttNumBmapBlocks = (numBmapBlocksTotal) / numTotalPartitions + 1;
    return (const_cast<char *>(bmapMemPtr) + pttNumBmapBlocks * idx * (BSIZE));
  }

  static char *getInodeBlockMemPtrPartition(const char *inodeMemPtr, int idx,
                                            block_no_t &pttNumInodeSectors) {
    if (idx != 0) {
      pttNumInodeSectors = 0;
      return nullptr;
    }
    pttNumInodeSectors = NINODES;
    // return const_cast<char *>(inodeMemPtr) +
    //       ((unsigned long)pttNumInodeSectors * idx * (SSD_SEC_SIZE));
    return const_cast<char *>(inodeMemPtr);
  }

  static char *getDataBlockMemPtrPartition(char *dataBlockMemPtr,
                                           int numTotalPartitions, int idx,
                                           block_no_t &pttNumDataBlocks) {
    assert(numTotalPartitions > 0 && idx >= 0);
    assert(idx < numTotalPartitions);
    pttNumDataBlocks = (NMEM_DATA_BLOCK) / numTotalPartitions;
    char *ret =
        (dataBlockMemPtr) + (((unsigned long)pttNumDataBlocks) * idx * (BSIZE));
    fprintf(stderr, "dataBlockMemPtr:%p idx:%d pttNumBlock:%u ret:%p\n",
            dataBlockMemPtr, idx, pttNumDataBlocks, ret);
    return ret;
  }

  float getDataDirtuFlushRatio() { return dataBlockBufDirtyFlushRato_; }
  int getNumPartitions() { return numPartitions_; }
  const char *getBmapMemPtr() { return bmapMemPtr_; }
  const char *getInodeSectorsMemPtr() { return inodeSectorMemPtr_; }
  char *getDataBlockMemPtr() { return dataBlockMemPtr_; }
  block_no_t getOnDiskNumBmapBlocksTotal() { return numBmapOnDiskBlocksTotal_; }

  // Get the root directory's inode ("/")
  InMemInode *rootDirInode(FsReq *fsReq);
  // Lookup a file (or directory) in a directory (not recursively)
  uint32_t lookupDir(FsReq *fsReq, InMemInode *dirInode,
                     const std::string &fileName, bool &error);
  // Lookup a file (or directory) in a directory
  // return the corresponding dentry's pointer
  cfs_dirent *lookupDirDentry(FsReq *fsReq, InMemInode *dirInode,
                              const std::string &fileName, bool &error);

  // see if the dirinode's data block which contains the dentry that
  // encode the <fileName:targetInode->i_no> info is in memory
  int checkInodeDentryInMem(FsReq *fsReq, InMemInode *dirInode,
                            InMemInode *targetInode,
                            const std::string &fileName);
  // in *dirInode*'s data block (dentries), let the entry of *oldInode* point
  // into *newInode*.
  // REQUIRED: fileName corresponding to srcInode, newFileName is the name in
  // target dentry
  // @return: -1 if error
  int dentryPointToNewInode(FsReq *fsReq, InMemInode *oldInode,
                            InMemInode *newInode, InMemInode *dirInode,
                            const std::string &fileName);
  // remove a file named fileName from one directory
  // NOTE: this will remove fsReq->targetInode's inode from dirInode's dentry
  int removeFromDir(FsReq *fsReq, InMemInode *dirInode,
                    const std::string &fileName);
  int removeFromDir(FsReq *fsReq, InMemInode *dirInode, int inoBlockDentryIdx,
                    block_no_t dentryBlockNo, const std::string &fileName);
  // Get the directory's inode (will lookup recursively from root)
  InMemInode *getParDirInode(FsReq *fsReq, bool &is_err);
  InMemInode *getParDirInode(FsReq *fsReq,
                             const std::vector<std::string> &pathTokens,
                             bool &is_err);
  // checks the imap and returns a free inode
  // return 0 if error, else a positive number indicating the free inode
  cfs_ino_t GetFreeInum(FsReq *req);
  // calls GetFreeInum and then reads the inode from disk if not in mem.
  InMemInode *AllocateInode(FsReq *req);
  // reset the ibmap for certain inode, aka. return this inode to un-allocated.
  // return -1 if error, else return 0
  int returnInode(FsReq *req, InMemInode *inode);
  // reset the data bmap for certain inode, aka. return the data blocks.
  // return -1 if error, else return 0
  InMemInode *getFileInode(FsReq *req, uint32_t inodeNo);
  // Set an inode (pointed by inodeNo) to flags DIRTY to avoid replacement
  // @return : > 0, write DONE; == 0, needs to fetch inode; <0, error
  int writeFileInode(FsReq *req, uint32_t inodeNo,
                     InMemInode *inode_ptr = nullptr);
  void splitRemoveDirty(uint32_t ino) { dirtyInodeSet_.erase(ino); }
  void resetFileIndoeDirty(uint32_t ino) {
    auto it = inodeMap_.find(ino);
    assert(it != inodeMap_.end());
    it->second->setDirty(false);
    dirtyInodeSet_.erase(ino);
#if CFS_JOURNAL(ON)
    unlinkedInodeSet_.erase(ino);
#endif
  }

  int64_t readInodeToUCache(
      FsReq *req, InMemInode *inode,
      std::vector<std::pair<cfs_bno_t, size_t>> &pagesToRead,
      std::vector<std::pair<PageDescriptor *, void *>> &dstVec);

  int64_t readInode(FsReq *req, InMemInode *inode, char *dst, uint64_t off,
                    uint64_t nBytes, bool nocpy = false);
  // doAllocate data blocks for processing a WRITE request
  uint64_t writeInodeAllocDataBlock(FsReq *req, InMemInode *inode, uint64_t off,
                                    uint64_t nBytes);

  // write to entire inode
  // REQUIRE: data blocks already allocated
  // NOTE: This will never generate IO because it don't need to really read any
  // data blocks into the memory
  int64_t writeEntireInode(FsReq *req, InMemInode *inode,
                           struct wsyncOp *wsync_op_ptr);
  // REQUIRED: inode lock is held
  int64_t writeInode(FsReq *req, InMemInode *inode, char *src, uint64_t off,
                     uint64_t nBytes);
  // write data into an inode (to its data blocks)
  // @param first_byte_blkno: save where the first byte to be writen on disk
  // @param first_byte_inblkoff: save in the first on-disk block, where is the
  //          first byte in terms of offset within 0-4K (block size)
  // REQUIRED: inode lock is held
  int64_t writeInodeAndSaveDataBlockInfo(FsReq *req, InMemInode *inode,
                                         char *src, uint64_t off,
                                         uint64_t nBytes,
                                         cfs_bno_t &first_byte_blkno,
                                         int &first_byte_inblkoff);

  // release the data blocks that was allocated to certain inode
  // NOTE: Currently, we do not return the data blocks to bitmap
  // So all the data blocks will simply be cleaned and zombie there.
  int64_t releaseInodeDataBlocks(FsReq *req, InMemInode *inode);
  int64_t allocateDataBlocks(FsReq *fsReq, InMemInode *inode,
                             uint32_t numBlocks);
  // if a inode is deleted, then the data blocks should be able to be
  // reused (e.g., LRU replacement)
  void releaseInodeDataBuffers(InMemInode *inode);

  // Append fileInode's DirEntry into dirInode's data.
  // SIDE_EFFECT: fileInode will store the dentry's position
  // This will use fsReq->getFileName() as the entryFileName
  void appendToDir(FsReq *fsReq, InMemInode *dirInode, InMemInode *fileInode);
  void appendToDir(FsReq *fsReq, InMemInode *dirInode, InMemInode *fileInode,
                   const std::string &entryFileName);

  // Check if the buffers need to be flushed, if yes, do flush.
  // @return: number of blocks to be flushed. set to -1 if cannot do flush now.
  int64_t checkAndFlushDirty(FsProcWorker *worker);

  // Try to flush the data blocks that belongs to one inode
  // @param: needFlush, will be set to false if all the blocks are clean
  int64_t flushInodeData(FsProcWorker *worker, FsReq *req, bool &needFlush);

  // help with FS exit (flushing)
  // Will flush all the in-memory metadata buffers to disk.
  void flushMetadataOnExit(FsProcWorker *procHandler);

  // API to access PCT
  void addPathInodeCacheItem(FsReq *req, InMemInode *inode);
  void addSecondPathInodeCacheItem(FsReq *req, InMemInode *inode);
  int removePathInodeCacheItem(FsReq *req, FsPermission::MapEntry *entry);
  int removeSecondPathInodeCacheItem(FsReq *req, FsPermission::MapEntry *entry);

  // the inode will not be put into the blockBuffer
  int installInode(InMemInode *inode);
  void uninstallInode(InMemInode *inode);
  size_t getNumInodes() { return inodeMap_.size(); }
  InMemInode *inodeBegin() {
    if (inodeMap_.size() > 0) return (inodeMap_.begin()->second);
    return nullptr;
  }
  auto GetInodeMapCopy() {
    std::unordered_map<cfs_ino_t, InMemInode *> map(inodeMap_);
    return map;
  }

  // put bufferItems for one inode into this vector
  // NOTE: this will erase these BlockBufferItem from items
  void splitInodeDataBlockBufferSlot(
      InMemInode *inode, std::unordered_set<BlockBufferItem *> &items);
  void installInodeDataBlockBufferSlot(
      InMemInode *inode, const std::unordered_set<BlockBufferItem *> &items);
  bool BufferSlotAllowMigrate() { return datablock_buf_allow_migrate; }

  int dumpAllInodesToFile(const char *fname);

 private:
  int idx_{-1};
  const FsProcWorker *fsWorker_{nullptr};
  // superblock is always kept in memory, and will be initialized
  // while FS is launching
  cfs_superblock *memSb_{nullptr};
  // Total number of blocks used for data bitmap (on-disk).
  // Will be initialized by reading the super-block.
  // NOTE: this is highly related to *NMEM_BMAP_BLOCK* (param.h)
  // Basically, NMEM_BMAP_BLOCK --> number of blocks to store the bmap blocks
  // in memory. I.e., to assure that all the bitmap blocks can be cached in
  // memory, this should always be true: NMEM_BMAP_BLOCK >= numBmapBlocksTotal
  // NOTE: after multi-workers + split/join, this is not so actively used
  block_no_t numBmapOnDiskBlocksTotal_ = 0;
  int numPartitions_ = 1;
  // only keep one single bitmap (assume large enough) for inode bitmap
  cfs_mem_block_t *iMapBlockPtr_{nullptr};
  int nextImapBlockFreeBitNo = -1;
  int nextImapNo = 0;

  // Note: all these three blockBuffer will use real absolute on-disk logic
  // block number as the key to access the blocks.
  BlockBuffer *bmapBlockBuf_{nullptr};
#if CFS_JOURNAL(LOCAL_JOURNAL) || 1
  std::unordered_set<cfs_bno_t> immutableBlockBitmaps_;
#endif
  // We still keep this function for all journal profiles which just returns
  // false and should be optimized away. That way calling code need not be
  // changed.
  bool isBlockBitmapImmutable(cfs_bno_t blockNo);
  // <alloctionUnit, nextAllocWithinUnitBlockNo>
  // This is ued for small optimization for block allocation
  // NOTE: currently only used for the first extent in the extent-array
  std::unordered_map<uint32_t, uint32_t> nextDataBlockAllocWithinUnitBlockIdx{
      NEXTENT_ARR};
  // record which bit is free inside the block indicates by above
  // nextDataBlockAllocWithinUnitBlockIdx map
  std::unordered_map<uint32_t, int> nextDataBlockAllocCurBlkBitNo{NEXTENT_ARR};

  // BlockBuffer *inodeBlockBuf_;
  // inodeBuffer will use 512Byte is block Size
  BlockBuffer *inodeSectorBuf_{nullptr};
  BlockBuffer *dataBlockBuf_{nullptr};
  float dataBlockBufDirtyFlushRato_{0.9};
  static constexpr block_no_t kBufferSlowLowWatermark = 5000;
  bool datablock_buf_allow_migrate = true;

#ifndef NONE_MT_LOCK
  std::atomic_flag iMapBlockLock;
  std::atomic_flag inodeMapLock;
  std::atomic_flag bmapLock;
#endif

  // NOTE: these memory address are global visible
  // That is, for each instance of FsImpl, no matter it is the master or the
  // servant thread, all of them will see the same value in their own copy.
  // These memory address are the starting address of each piece of memory
  // that is dedicated to some specific usage
  const char *memPtr_;  // start addr of the whole buffer's memory,
                        // REQUIRED: nullptr in the thread that is not master
  const char *bmapMemPtr_;         // start addr bmap region
  const char *inodeSectorMemPtr_;  // start addr of inode region
  char *dataBlockMemPtr_;          // start addr of data region
  cfs_mem_block_t *sbBlockPtr_;    // pointer to the superblock block

  std::unordered_map<uint32_t, InMemInode *> inodeMap_;  //  <ino, InMemInode>
  // To handle the servant threads writing Inode, let it ignore the inodeBuffer
  // directly record dirtyInodes into this set. And one return, will *tell*
  // master thread, master thread is going to test that by itself, simply
  // via BlockBufferItem->isDirty()
  // This is mainly used to handle fsync()
  // NOTE: for any modification, if attribute in cfs_dinode is modified,
  // the inode should be put into this set
  //  - includes: unlinked inode, dst inode of rename
  std::unordered_set<uint32_t> dirtyInodeSet_;

#if CFS_JOURNAL(ON) || 1
  // A collection of unlinked inodes
  // recorded at unlink, removed at syncall & syncunlinked
  std::unordered_set<uint32_t> unlinkedInodeSet_;
#endif

  // pointer to the global permission controller
  // Now it also contains the inode ptr
  FsPermission *permission;

  // For all the request that based on a path, E.q., mkdir(), rmdir()
  // create, unlink, we need to make sure that if the path is the same,
  // they are in order (some order, but no intervention)
  std::unordered_map<std::string,
                     std::pair<FsReq *, std::unordered_set<FsReq *>>>
      pendingPathBasedFsReqs_;

  // help with init
  void initMemAddrs();
  void initMemAddrBmapBlockBuf();
  void initMemAddrInodeSectorBuf();
  void initMemAddrDataBlockBuf();

  void initDataBlockAllocNextFreeBlockIdx();

  // get inode according to inode number
  // in the normal case, the inode will be fetched from the disk into inodeBuf
  // @param doSubmit: for new-allocated inode, we do not fetch it into memory
  //   then set doSubmit to true
  InMemInode *getInode(uint32_t ino, FsReq *fsReq, bool doSubmit = true);
  BlockBufferItem *getBlockForIndex(BlockBuffer *blockbuf, uint32_t blockNo,
                                    FsReq *fsReq, uint32_t index);
  BlockBufferItem *getBlockForIndex(BlockBuffer *blockBuf, uint32_t blockNo,
                                    FsReq *fsReq, bool doSubmit,
                                    bool doBlockSubmit, bool &canOverwritten,
                                    uint32_t index);
  BlockBufferItem *getBlock(BlockBuffer *blockBuf, uint32_t blockNo,
                            FsReq *fsReq);
  BlockBufferItem *getBlock(BlockBuffer *blockBuf, uint32_t blockNo,
                            FsReq *fsReq, bool doSubmit, bool doBlockSubmit,
                            bool &canOverwritten, uint32_t new_index = 0);
  BlockBufferItem *getLockedDirtyBitmap(cfs_bno_t bitmap_blockno);
  void releaseLockedDirtyBitmap(BlockBufferItem *buf);
  cfs_mem_block_t *getDirtyInodeBitmap(cfs_bno_t bitmap_blockno);

  // from inode's ino (i_no field in cfs_inode) to the on-disk block number
  // of the imap block that manage that inode
  // NOTE: this is currently not being used
  // Because we assume all the inodes can be tracked by one single 4K Block
  // which is (4096 * 8) inodes
  static uint32_t inline ino2ImapBlockNo(uint32_t ino) {
    if (ino > NINODES) {
      assert(false);
      return 0;
    }
    return get_imap_start_block() + ino / (BPB);
  }

  static uint32_t inline ino2SectorNo(uint32_t ino) {
    return calcSectorForInode(ino);
  }

  // when lookup, record the position of this inode's corresponding dentry item
  // in its parent directory's data blocks
  // record is in the format of: dataBlockNo + withinBlockIdx
  // this helper will simply retrieve info from req and set it into inode
  // related: FsReq::setFileDirentryBlockNo()
  // @dentryNo: number of dentry to set. E.g., for rename, src is 0, dst is 1
  static void fillInodeDentryPositionAfterLookup(FsReq *req, InMemInode *inode,
                                                 int dentryNo = 0);

  // helper function to flush BlockBuffer object.
  // @param forceFlush: once set to True, will not check if the block is dirty,
  //    but simply do disk write()
  static void _fsExitFlushSingleBlockBuffer(FsProcWorker *procHandler,
                                            BlockBuffer *blkbuf,
                                            bool forceFlush = false,
                                            bool dbg = false);
  static void _fsExitFlushSingleSectorBuffer(FsProcWorker *procHandler,
                                             BlockBuffer *blkbuf);
  // helper function to print super block information.
  void printSuperBlock();

  friend class JournalManager;
  friend class FileMng;
};

inline BlockBufferItem *FsImpl::getLockedDirtyBitmap(cfs_bno_t bitmap_blockno) {
  bool safeToWrite = false;
  auto buf = getBlock(bmapBlockBuf_, bitmap_blockno, /*fsReq*/ nullptr,
                      /*doSubmit*/ false, /*doBlockSubmit*/ false, safeToWrite);
  // NOTE: make sure that the caller releases the dirty bitmap!
  if (safeToWrite && buf->isInMem()) return buf;
  throw std::runtime_error(
      "Cannot retrieve dirty bitmap, either not in memory or not safe to "
      "write");
}

inline void FsImpl::releaseLockedDirtyBitmap(BlockBufferItem *buf) {
  bmapBlockBuf_->releaseBlock(buf);
}

inline cfs_mem_block_t *FsImpl::getDirtyInodeBitmap(cfs_bno_t bitmap_blockno) {
  off_t offset = bitmap_blockno - get_imap_start_block();
  return iMapBlockPtr_ + offset;
}

inline void FsImpl::releaseInodeDataBuffers(InMemInode *inode) {
  dataBlockBuf_->releaseUnlinkedInodeDirtyBlocks(inode->i_no);
}

inline bool FsImpl::isBlockBitmapImmutable(cfs_bno_t blockNo) {
#if CFS_JOURNAL(LOCAL_JOURNAL)
  // Only the local journal requires immutable block bitmaps
  if (immutableBlockBitmaps_.empty()) return false;

  auto find = immutableBlockBitmaps_.find(blockNo);
  return (find != immutableBlockBitmaps_.end());
#else
  return false;
#endif
}

inline void InMemInode::addFd(pid_t pid, FileObj *fobj) {
  auto &fdmap = appFdMap_[pid];
  bool inserted;
  std::tie(std::ignore, inserted) = fdmap.emplace(fobj->readOnlyFd, fobj);
  if (inserted) ref++;
}

inline void InMemInode::delFd(pid_t pid, FileObj *fobj) {
  auto search = appFdMap_.find(pid);
  if (search == appFdMap_.end()) return;

  auto &fdmap = search->second;
  auto erased = fdmap.erase(fobj->readOnlyFd);
  if (erased > 0) {
    assert(ref > 0);
    ref--;
  }

  if (fdmap.empty()) appFdMap_.erase(search);
}

#endif  // CFS_INCLUDE_FSPROC_FSIMPL_H_
