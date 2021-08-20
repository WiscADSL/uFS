#ifndef __CFS_CLIENT_API__
#define __CFS_CLIENT_API__

#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fs_defs.h"

#ifdef __cplusplus
void fs_free(void *ptr, int fsTid);
void fs_free_pad(void *ptr, int fsTid);
extern "C" {
#endif

////////////////////////////////////////////////////////////////////////////////
// Initialization of FS access.

#define FS_ACCESS_UNINIT_ERROR (-9999)

// Register user application to FS Proc.
// It will return the key to shared memory. This is not implemented in this
// version.
int fs_register(void);

// This is the current api to use for initialization.
// Basically, we have pre-defined key for the shared memory segment (see
// param.h:FS_SHM_KEY_BASE). E.g., first application will has PID=0, just simply
// pass (FS_SHM_KEY_BASE + PID) to this function.

// NOTE: only used in simple test cases.
int fs_init(key_t key);
// Init FS with multiple keys (corresponds to multiple fs processes).
int fs_init_multi(int num_key, const key_t *keys);
// Init one thread's local variables
// including tid, and data-shm
void fs_init_thread_local_mem();

// reset this FsLib from server shm
int fs_exit();
// cleanup FsLib's context
int fs_cleanup();
int fs_ping();

int fs_checkpoint();
int fs_migrate(int fd, int *dstWid);
int fs_admin_inode_reassignment(int type, uint32_t inode, int curOwner,
                                int newOwner);

// testing and experimenting
// change the handling work of the entity: pid+tid from src_wid to dst_wid
int fs_admin_thread_reassign(int src_wid, int dst_wid, int flag);

// send msg to the server such that it starts/stops dump the stats
int fs_start_dump_load_stats();
int fs_stop_dump_load_stats();

// dump all inodes to /tmp
int fs_dumpinodes(int wid);
////////////////////////////////////////////////////////////////////////////////
// POSIX API

struct CFS_DIR {
  // number of dentry that has valid value (ino != 0)
  int dentryNum;
  int dentryIdx;
  struct dirent *firstDentry;
};

// ~= stat()
int fs_stat(const char *pathname, struct stat *statbuf);
// ~= fstat()
int fs_fstat(int fd, struct stat *statbuf);
// ~= open()
int fs_open(const char *path, int flags, mode_t mode);
int fs_open2(const char *path, int flags);
// ~= close()
int fs_close(int fd);

int fs_open_lease(const char *path, int flags, mode_t mode);
int fs_close_lease(int fd);

// ~= unlink()
int fs_unlink(const char *pathname);
// ~= mkdir()
int fs_mkdir(const char *pathname, mode_t mode);
// ~= opendir()
struct CFS_DIR *fs_opendir(const char *name);
// ~= readdir()
struct dirent *fs_readdir(struct CFS_DIR *dirp);
int fs_closedir(struct CFS_DIR *dirp);
// ~= rmdir() , according to man page, must be empty directory
int fs_rmdir(const char *pathname);
// ~= rename()
int fs_rename(const char *oldpath, const char *newpath);

// ~= fsync()
int fs_fsync(int fd);
int fs_fdatasync(int fd);

// sync all the dirtry data
void fs_syncall();

// sync unlinked inodes and reclaim data blocks
void fs_syncunlinked();

// TODO (jingliu): these two are not implemented
// TODO: when allowing ftruncate to reduce size of the file (but still > 0), we
// must fix FsProc_JournalBasic.cc. Refer to discussion at
// https://github.com/jingliu9/ApparateFS/pull/163
int fs_ftruncate(int fd, off_t length);
int fs_fallocate(int fd, int mode, off_t offset, off_t len);

// lseek
off_t fs_lseek(int fd, long int offset, int whence);

// data plane operations
// ~= read()
ssize_t fs_read(int fd, void *buf, size_t count);
// ~= pread()
ssize_t fs_pread(int fd, void *buf, size_t count, off_t offset);
// ~=write()
ssize_t fs_write(int fd, const void *buf, size_t count);
// ~=pwrite()
ssize_t fs_pwrite(int fd, const void *buf, size_t count, off_t offset);

////////////////////////////////////////////////////////////////////////////////
// Copy reduction APIs

// fs_allocated_read/write will reduce one-time of copying data.
// NOTE: we assume the USER of these API make sure satisfy the following
// requirements:
//  1) writing the *buf* should be page aligned
//  2) never re-use this *buf* for other usage, always cal fs_free() after
//  use this
ssize_t fs_allocated_read(int fd, void *buf, size_t count);
ssize_t fs_allocated_pread(int fd, void *buf, size_t count, off_t offset);
ssize_t fs_allocated_write(int fd, void *buf, size_t count);
ssize_t fs_allocated_pwrite(int fd, void *buf, ssize_t count, off_t offset);

#define FS_CACHED_RW_RET_ERR_LEASE_NOT_AVAILABLE (-11)

// set of apis that is using ClientCache+Lease

// @param bufAddr:
//   This will contains the resulting buffer that stores the valid data
//   REQUIRED: (*bufAddr) set to nullptr by the caller. This is to make sure
//   it is safe to put valid address that contains the data
// NOTE: EXPERIMENTAL only; don't use it for real application
ssize_t fs_cached_read(int fd, void **bufAddr, size_t count);
ssize_t fs_cached_pread(int fd, void **bufAddr, size_t count, off_t offset);

// ~= pread()
// NOTE: this buf is allocated by app itself in heap/stack
// Basically fs_cached_pread() + one copy
ssize_t fs_cached_posix_pread(int fd, void *buf, size_t count, off_t offset);

// Support a unified page cache (for read-only w/ lease)
ssize_t fs_uc_read(int fd, void *buf, size_t count);
ssize_t fs_uc_pread(int fd, void *buf, size_t count, off_t offset);

// under this FS_LIB_SPPG macro, some attempt to directly
// let client has DMAable memory segment
#ifdef FS_LIB_SPPG
ssize_t fs_sppg_cpc_pread(int fd, void *buf, size_t count, off_t offset);
#endif

// Allocate buffer for application do IO to avoid copy.
void *fs_malloc(size_t size);
// fs_malloc + initialize the buffer with zero
void *fs_zalloc(size_t size);
void fs_free(void *ptr);

// NOTE: the buf for this cpc_pread is required to malloc by
// fs_malloc_pad, and freed by fs_free_pad
// The reason is that, the FsLib will secretly alloc one more page
// to make sure we can get aligned memory from the server
// E.g., cpc_pread(fd=1, buf, count=4096, offset=1)
// malloc_pad(4096) will allocate 2*4K pages for this; let's say
// addr is [8K -- 16K], the return of fs_malloc_pad will be 12K
// Therefore, [12K-1:16K-1] contains a valid page, and we can
// save the whole page into page cache
ssize_t fs_cpc_pread(int fd, void *buf, size_t count, off_t offset);
void *fs_malloc_pad(size_t size);
void fs_free_pad(void *ptr);

///////////////////////////////////////////////////////////////////////////////
// ldb specific APIs (use write-cache)

int fs_open_ldb(const char *path, int flags, mode_t mode);
int fs_open_ldb2(const char *path, int flags);
ssize_t fs_allocated_pread_ldb(int fd, void *buf, size_t count, off_t offset);
ssize_t fs_allocated_write_ldb(int fd, void *buf, size_t count);
int fs_close_ldb(int fd);
// full-file sync (write+fsync)
int fs_wsync(int fd);

////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif

#endif
