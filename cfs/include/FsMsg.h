#ifndef __cfs_FsMsg_h
#define __cfs_FsMsg_h

#include <stdint.h>
#include "FsLibShared.h"
#include "param.h"

#pragma pack(1)
struct rwOpCommonPacked {
  int fd;
  uint8_t flag;
  size_t count;
  // 1) in fs_cpc_pread: it is used for indicating the difference between data
  // dst vs. shm addr 2) in fs_uc_pread: it is used to store the total value of
  // this read
  size_t realCount;
  // once using lease, realOffset is used for server to save the timestamp
  // with lease, the FD's offset in server does not make sense
  off_t realOffset;
  ssize_t ret;
};

#pragma pack(1)
struct allocatedOpCommonPacked {
  fslib_malloc_block_cnt_t dataPtrId;
  uint64_t perAppSeqNo;
  uint8_t shmid;
};

#pragma pack(1)
struct readOpPacked {
  struct rwOpCommonPacked rwOp;
};

#pragma pack(1)
struct preadOpPacked {
  struct rwOpCommonPacked rwOp;
  off_t offset;
};

#pragma pack(1)
struct writeOpPacked {
  struct rwOpCommonPacked rwOp;
};

#pragma pack(1)
struct pwriteOpPacked {
  struct rwOpCommonPacked rwOp;
  off_t offset;
};

#pragma pack(1)
struct allocatedReadOpPacked {
  struct rwOpCommonPacked rwOp;
  struct allocatedOpCommonPacked alOp;
};

#pragma pack(1)
struct allocatedPreadOpPacked {
  struct rwOpCommonPacked rwOp;
  struct allocatedOpCommonPacked alOp;
  off_t offset;
};

#pragma pack(1)
struct allocatedWriteOpPacked {
  struct rwOpCommonPacked rwOp;
  struct allocatedOpCommonPacked alOp;
};

#pragma pack(1)
struct allocatedPwriteOpPacked {
  struct rwOpCommonPacked rwOp;
  struct allocatedOpCommonPacked alOp;
  off_t offset;
};

// reset pack to default
#pragma pack()

static inline void pack_rwOpCommon(struct rwOpCommon *op,
                                   struct rwOpCommonPacked *dst) {
  dst->fd = op->fd;
  dst->count = op->count;
  dst->ret = op->ret;
  dst->flag = op->flag;
  dst->realOffset = op->realOffset;
  dst->realCount = op->realCount;
}

static inline void unpack_rwOpCommon(struct rwOpCommonPacked *op,
                                     struct rwOpCommon *dst) {
  dst->fd = op->fd;
  dst->count = op->count;
  dst->ret = op->ret;
  dst->flag = op->flag;
  dst->realOffset = op->realOffset;
  dst->realCount = op->realCount;
}

static inline void pack_allocatedOpCommon(struct allocatedOpCommon *op,
                                          struct allocatedOpCommonPacked *dst) {
  dst->dataPtrId = op->dataPtrId;
  dst->perAppSeqNo = op->perAppSeqNo;
  dst->shmid = op->shmid;
}

static inline void unpack_allocatedOpCommon(struct allocatedOpCommonPacked *op,
                                            struct allocatedOpCommon *dst) {
  dst->dataPtrId = op->dataPtrId;
  dst->perAppSeqNo = op->perAppSeqNo;
  dst->shmid = op->shmid;
}

static inline void pack_readOp(struct readOp *op, struct readOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
}

static inline void unpack_readOp(struct readOpPacked *op, struct readOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
}

static inline void pack_preadOp(struct preadOp *op, struct preadOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  dst->offset = op->offset;
}

static inline void unpack_preadOp(struct preadOpPacked *op,
                                  struct preadOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  dst->offset = op->offset;
}

static inline void pack_writeOp(struct writeOp *op, struct writeOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
}

static inline void unpack_writeOp(struct writeOpPacked *op,
                                  struct writeOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
}

static inline void pack_pwriteOp(struct pwriteOp *op,
                                 struct pwriteOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  dst->offset = op->offset;
}

static inline void unpack_pwriteOp(struct pwriteOpPacked *op,
                                   struct pwriteOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  dst->offset = op->offset;
}

static inline void pack_allocatedReadOp(struct allocatedReadOp *op,
                                        struct allocatedReadOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  pack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
}

static inline void unpack_allocatedReadOp(struct allocatedReadOpPacked *op,
                                          struct allocatedReadOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  unpack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
}

static inline void pack_allocatedPreadOp(struct allocatedPreadOp *op,
                                         struct allocatedPreadOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  pack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
  dst->offset = op->offset;
}

static inline void unpack_allocatedPreadOp(struct allocatedPreadOpPacked *op,
                                           struct allocatedPreadOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  unpack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
  dst->offset = op->offset;
}

static inline void pack_allocatedWriteOp(struct allocatedWriteOp *op,
                                         struct allocatedWriteOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  pack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
}

static inline void unpack_allocatedWriteOp(struct allocatedWriteOpPacked *op,
                                           struct allocatedWriteOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  unpack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
}

static inline void pack_allocatedPwriteOp(struct allocatedPwriteOp *op,
                                          struct allocatedPwriteOpPacked *dst) {
  pack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  pack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
  dst->offset = op->offset;
}

static inline void unpack_allocatedPwriteOp(struct allocatedPwriteOpPacked *op,
                                            struct allocatedPwriteOp *dst) {
  unpack_rwOpCommon(&(op->rwOp), &(dst->rwOp));
  unpack_allocatedOpCommon(&(op->alOp), &(dst->alOp));
  dst->offset = op->offset;
}

#endif
