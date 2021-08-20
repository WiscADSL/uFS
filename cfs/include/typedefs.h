#ifndef CFS_TYPEDEFS_H
#define CFS_TYPEDEFS_H

#include <thread>
#include <utility>

typedef uint32_t cfs_bno_t;
typedef uint32_t cfs_secno_t;

typedef uint32_t cfs_ino_t;
typedef int cfs_tid_t;
// for block device request
typedef uint32_t bdev_reqid_t;

// FSP performance accounting
typedef uint64_t perfstat_ts_t;
typedef uint64_t perfstat_cycle_t;

typedef cfs_bno_t block_no_t;

typedef std::pair<block_no_t, int> inode_dentry_dbpos_t;
#define SET_DENTRY_DB_POS_FROM_PAIR(pairPtr, blkno, idx) \
  {                                                      \
    blkno = pairPtr->first;                              \
    idx = pairPtr->second;                               \
  }

#endif  // CFS_TYPEDEFS_H
