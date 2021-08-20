#include <stdio.h>

#include "FsMsg.h"
#include "timer_rdtsc.h"

static inline int rwOpCommon_cmp(struct rwOpCommon *a, struct rwOpCommon *b) {
  if ((a->fd == b->fd) && (a->count == b->count) && (a->ret == b->ret) &&
      (a->flag == b->flag) && (a->realOffset == b->realOffset) &&
      (a->realCount == b->realCount))
    return 0;

  return 1;
}

static inline int allocatedOpCommon_cmp(struct allocatedOpCommon *a,
                                        struct allocatedOpCommon *b) {
  if ((a->dataPtrId == b->dataPtrId) && (a->shmid == b->shmid) &&
      (a->perAppSeqNo == b->perAppSeqNo))
    return 0;

  return 1;
}

static inline int readOp_cmp(struct readOp *a, struct readOp *b) {
  return rwOpCommon_cmp(&(a->rwOp), &(b->rwOp));
}

static inline int preadOp_cmp(struct preadOp *a, struct preadOp *b) {
  if ((rwOpCommon_cmp(&(a->rwOp), &(b->rwOp)) == 0) && (a->offset == b->offset))
    return 0;
  return 1;
}

static inline int writeOp_cmp(struct writeOp *a, struct writeOp *b) {
  return rwOpCommon_cmp(&(a->rwOp), &(b->rwOp));
}

static inline int pwriteOp_cmp(struct pwriteOp *a, struct pwriteOp *b) {
  if ((rwOpCommon_cmp(&(a->rwOp), &(b->rwOp)) == 0) && (a->offset == b->offset))
    return 0;
  return 1;
}

static inline int allocatedReadOp_cmp(struct allocatedReadOp *a,
                                      struct allocatedReadOp *b) {
  if ((rwOpCommon_cmp(&(a->rwOp), &(b->rwOp)) == 0) &&
      (allocatedOpCommon_cmp(&(a->alOp), &(b->alOp)) == 0))
    return 0;

  return 1;
}

static inline int allocatedPreadOp_cmp(struct allocatedPreadOp *a,
                                       struct allocatedPreadOp *b) {
  if ((rwOpCommon_cmp(&(a->rwOp), &(b->rwOp)) == 0) &&
      (allocatedOpCommon_cmp(&(a->alOp), &(b->alOp)) == 0) &&
      (a->offset == b->offset))
    return 0;

  return 1;
}

static inline int allocatedWriteOp_cmp(struct allocatedWriteOp *a,
                                       struct allocatedWriteOp *b) {
  if ((rwOpCommon_cmp(&(a->rwOp), &(b->rwOp)) == 0) &&
      (allocatedOpCommon_cmp(&(a->alOp), &(b->alOp)) == 0))
    return 0;

  return 1;
}

static inline int allocatedPwriteOp_cmp(struct allocatedPwriteOp *a,
                                        struct allocatedPwriteOp *b) {
  if ((rwOpCommon_cmp(&(a->rwOp), &(b->rwOp)) == 0) &&
      (allocatedOpCommon_cmp(&(a->alOp), &(b->alOp)) == 0) &&
      (a->offset == b->offset))
    return 0;

  return 1;
}

#define _vv(struct_type, suffix) _var_##struct_type##_##suffix
#define test_struct(struct_type, buf)                                         \
  struct struct_type *_vv(struct_type, unpacked_src) =                        \
      (struct struct_type *)(buf);                                            \
  struct struct_type##Packed _vv(struct_type, packed_dst),                    \
      _vv(struct_type, packed_dst2);                                          \
  struct struct_type _vv(struct_type, unpacked_dst);                          \
  pack_##struct_type(_vv(struct_type, unpacked_src),                          \
                     &(_vv(struct_type, packed_dst)));                        \
  unpack_##struct_type(&(_vv(struct_type, packed_dst)),                       \
                       &(_vv(struct_type, unpacked_dst)));                    \
  pack_##struct_type(&(_vv(struct_type, unpacked_dst)),                       \
                     &(_vv(struct_type, packed_dst2)));                       \
  int _vv(struct_type, test_cmp_padded) =                                     \
      memcmp(&(_vv(struct_type, packed_dst)),                                 \
             &(_vv(struct_type, packed_dst2)), sizeof(struct_type##Packed));  \
  int _vv(struct_type, test_cmp_unpadded) = struct_type##_cmp(                \
      _vv(struct_type, unpacked_src), &(_vv(struct_type, unpacked_dst)));     \
  if (_vv(struct_type, test_cmp_padded) != 0) {                               \
    printf("%30s \033[1;31mfailed\033[0m pack(A) != pack(unpack(pack(A)))\n", \
           #struct_type);                                                     \
  } else {                                                                    \
    printf("%30s \033[1;32mpassed\033[0m pack(A) = pack(unpack(pack(A)))\n",  \
           #struct_type);                                                     \
  }                                                                           \
  if (_vv(struct_type, test_cmp_unpadded) != 0) {                             \
    printf("%30s \033[1;31mfailed\033[0m A != unpack(pack(A))\n",             \
           #struct_type);                                                     \
  } else {                                                                    \
    printf("%30s \033[1;32mpassed\033[0m A = unpack(pack(A))\n",              \
           #struct_type);                                                     \
  }

#define test_pack_performance(struct_type, buf, n)              \
  struct struct_type *_vv(struct_type, perf_unpacked_src) =     \
      (struct struct_type *)(buf);                              \
  struct struct_type##Packed _vv(struct_type, perf_packed_dst); \
  TIMER_RDTSC_INIT(struct_type##pack_timer);                    \
  TIMER_RDTSC_START(struct_type##pack_timer);                   \
  for (int i = 0; i < n; i++) {                                 \
    pack_##struct_type(_vv(struct_type, perf_unpacked_src),     \
                       &(_vv(struct_type, perf_packed_dst)));   \
  }                                                             \
  TIMER_RDTSC_STOP(struct_type##pack_timer);                    \
  printf("%30s %llu cycles to pack\n", "took",                  \
         TIMER_RDTSC_CLOCKS(struct_type##pack_timer) / n);

#define test_unpack_performance(struct_type, buf, n)              \
  struct struct_type##Packed *_vv(struct_type, perf_packed_src) = \
      (struct struct_type##Packed *)(buf);                        \
  struct struct_type _vv(struct_type, perf_unpacked_dst);         \
  TIMER_RDTSC_INIT(struct_type##unpack_timer);                    \
  TIMER_RDTSC_START(struct_type##unpack_timer);                   \
  for (int i = 0; i < n; i++) {                                   \
    unpack_##struct_type(_vv(struct_type, perf_packed_src),       \
                         &(_vv(struct_type, perf_unpacked_dst))); \
  }                                                               \
  TIMER_RDTSC_STOP(struct_type##unpack_timer);                    \
  printf("%30s %llu cycles to unpack\n", "took",                  \
         TIMER_RDTSC_CLOCKS(struct_type##unpack_timer) / n);

#define _PSIZE(s)                                                      \
  do {                                                                 \
    printf("%50s = %zu\n", "sizeof(struct " #s ")", sizeof(struct s)); \
  } while (0)

#define CHECK(s, buf)                                                          \
  do {                                                                         \
    _PSIZE(s);                                                                 \
    _PSIZE(s##Packed);                                                         \
    test_struct(s, buf) test_pack_performance(s, buf, 10000)                   \
        test_unpack_performance(s, buf, 10000) printf(                         \
            "----------------------------------------------------------------" \
            "------\n");                                                       \
  } while (0)

#define PSIZE(s)                                         \
  do {                                                   \
    printf("%50s = %zu\n", "sizeof(" #s ")", sizeof(s)); \
  } while (0)

void display_struct_stats() {
  char *buf = NULL;
  void *_buf = NULL;
  uint32_t *ints;

  if (posix_memalign(&_buf, 4096, 4096)) {
    printf("Failed to malloc\n");
    return;
  }
  buf = (char *)_buf;
  /*
  buf = (char *) malloc (4096);
  if (buf == NULL) {
      printf("failed to malloc\n");
      return;
  }
  */

  memset(buf, 0, 4096);
  // input structs will be a sequence of bytes from buf.
  ints = (uint32_t *)buf;
  for (int i = 0; i < 1024; i++) {
    ints[i] = i + 1;
  }

  CHECK(rwOpCommon, buf);
  CHECK(allocatedOpCommon, buf);
  CHECK(readOp, buf);
  CHECK(preadOp, buf);
  CHECK(writeOp, buf);
  CHECK(pwriteOp, buf);
  CHECK(allocatedReadOp, buf);
  CHECK(allocatedPreadOp, buf);
  CHECK(allocatedWriteOp, buf);
  CHECK(allocatedPwriteOp, buf);

  PSIZE(struct openOp);
  PSIZE(struct closeOp);
  PSIZE(struct mkdirOp);
  PSIZE(struct statOp);
  PSIZE(struct fstatOp);
  PSIZE(struct fsyncOp);
  PSIZE(struct unlinkOp);
  PSIZE(struct renameOp);
  PSIZE(struct opendirOp);
  PSIZE(struct rmdirOp);
  PSIZE(struct newShmAllocatedOp);
  PSIZE(struct exitOp);
  PSIZE(union CfsOp);
  PSIZE(struct clientOp);
  free(buf);
}

int main() {
  display_struct_stats();
  return 0;
}
