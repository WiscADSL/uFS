#include "shmipc/shmipc.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static_assert(sizeof(struct shmipc_msg) <= 64,
              "struct shmipc_msg must fit cacheline");

struct shmipc_qp *shmipc_qp_get(const char *name, size_t size, int create) {
  struct shmipc_qp *qp = NULL;
  mode_t tmp_mask, old_mask;

  qp = (struct shmipc_qp *)malloc(sizeof(struct shmipc_qp));
  if (qp == NULL) goto error;

  qp->fd = -1;
  qp->ptr = NULL;
  qp->size = size;
  qp->create = create;
  // FIXME: use strlcpy?
  strncpy(qp->filename, name, sizeof(qp->filename) - 1);
  qp->filename[sizeof(qp->filename) - 1] = '\0';

  // NOTE: we allow everyone to read/write this shared file.
  // This is because
  // 1. The server is usually root and the clients may not be.
  // 2. Eventually, we plan on unlinking the shm file and transferring
  // the fd. That way, only processes that have the fd can access the
  // shared memory.
  old_mask = umask((mode_t)0);
  if (create)
    qp->fd = shm_open(name, O_RDWR | O_CREAT, 0666);
  else
    qp->fd = shm_open(name, O_RDWR, 0666);

  tmp_mask = umask(old_mask);  // reset umask

  if (qp->fd == -1) goto error;

  if (create) {
    if (ftruncate(qp->fd, size) != 0) goto error;
  }

  qp->ptr =
      (char *)mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, qp->fd, 0);
  if (qp->ptr == MAP_FAILED) goto error;

  if (create) memset(qp->ptr, 0, size);

  return qp;

error:
  shmipc_qp_destroy(qp);
  return NULL;
}

void shmipc_qp_destroy(struct shmipc_qp *qp) {
  if (qp == NULL) return;

  if (qp->ptr != NULL && qp->ptr != MAP_FAILED) munmap(qp->ptr, qp->size);

  if (qp->fd != -1) {
    close(qp->fd);
    if (qp->create == 1) shm_unlink(qp->filename);
  }

  free(qp);
  return;
}

struct shmipc_mgr *shmipc_mgr_init(const char *name, size_t rsize, int create) {
  struct shmipc_mgr *mgr = NULL;
  size_t mem_required;

  // rsize must be a power of 2
  if ((rsize < 4) || ((rsize & (rsize - 1)) != 0)) return NULL;

  mgr = (struct shmipc_mgr *)malloc(sizeof(struct shmipc_mgr));
  if (mgr == NULL) goto error;

  mgr->qp = NULL;
  mgr->ring = NULL;
  mgr->xreq = NULL;
  mgr->data = NULL;
  mgr->capacity = rsize;
  mgr->mask = rsize - 1;
  mgr->next = 0;

  // TODO ensure that the shared memory vaddr is cache aligned?
  mem_required = (rsize * 64) + (rsize * shmipc_XREQ_MAX_ELEM_SIZE) +
                 (rsize * shmipc_DATA_MAX_ELEM_SIZE);
  mgr->qp = shmipc_qp_get(name, mem_required, create);
  if (mgr->qp == NULL) goto error;

  mgr->ring = (struct shmipc_msg *)mgr->qp->ptr;
  mgr->xreq = (char *)&(mgr->qp->ptr[rsize * 64]);
  mgr->data = (char *)&(
      mgr->qp->ptr[(rsize * 64) + (rsize * shmipc_XREQ_MAX_ELEM_SIZE)]);
  return mgr;

error:
  shmipc_mgr_destroy(mgr);
  return NULL;
}

struct shmipc_mgr *shmipc_mgr_init_client(const char *name, size_t rsize,
                                          int create) {
  struct shmipc_mgr *mgr = NULL;
  size_t mem_required;

  // rsize must be a power of 2
  if ((rsize < 4) || ((rsize & (rsize - 1)) != 0)) return NULL;

  mgr = (struct shmipc_mgr *)mmap(NULL, 4096, PROT_READ | PROT_WRITE,
                                  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mgr == NULL) goto error;

  mgr->qp = NULL;
  mgr->ring = NULL;
  mgr->xreq = NULL;
  mgr->data = NULL;
  mgr->capacity = rsize;
  mgr->mask = rsize - 1;
  mgr->next = 0;

  // TODO ensure that the shared memory vaddr is cache aligned?
  mem_required = (rsize * 64) + (rsize * shmipc_XREQ_MAX_ELEM_SIZE) +
                 (rsize * shmipc_DATA_MAX_ELEM_SIZE);
  mgr->qp = shmipc_qp_get(name, mem_required, create);
  if (mgr->qp == NULL) goto error;

  mgr->ring = (struct shmipc_msg *)mgr->qp->ptr;
  mgr->xreq = (char *)&(mgr->qp->ptr[rsize * 64]);
  mgr->data = (char *)&(
      mgr->qp->ptr[(rsize * 64) + (rsize * shmipc_XREQ_MAX_ELEM_SIZE)]);
  return mgr;

error:
  shmipc_mgr_destroy(mgr);
  return NULL;
}

void shmipc_mgr_destroy(struct shmipc_mgr *mgr) {
  if (mgr == NULL) return;

  shmipc_qp_destroy(mgr->qp);
  free(mgr);
}

void shmipc_mgr_destroy_client(struct shmipc_mgr *mgr) {
  if (mgr == NULL) return;

  shmipc_qp_destroy(mgr->qp);
  munmap(mgr, 4096);
}

// The server side only resets the pointer to read from.
// It never modifies data as a client might still be reading.
void shmipc_mgr_server_reset(struct shmipc_mgr *mgr) { mgr->next = 0; }

// The client side reset the pointer as well as zero out
// all the shared memory. Should be called when a client exists
// as the next client will want to use the ring.
// FIXME: client should always create shared memory and ask the
// server to use that. This will remove the need for "resets".
void shmipc_mgr_client_reset(struct shmipc_mgr *mgr) {
  mgr->next = 0;
  memset(mgr->qp->ptr, 0, mgr->qp->size);
}

struct shmipc_msg *shmipc_mgr_get_msg(struct shmipc_mgr *mgr, off_t *idx) {
  struct shmipc_msg *msg;
  off_t ring_idx;

  ring_idx = (mgr->next & mgr->mask);
  msg = IDX_TO_MSG(mgr, ring_idx);
  while (msg->status != shmipc_STATUS_READY_FOR_SERVER)
    ;

  // we have a msg
  *idx = ring_idx;
  mgr->next++;
  return msg;
}

struct shmipc_msg *shmipc_mgr_get_msg_nowait(struct shmipc_mgr *mgr,
                                             off_t *idx) {
  struct shmipc_msg *msg;
  off_t ring_idx;

  ring_idx = (mgr->next & mgr->mask);
  msg = IDX_TO_MSG(mgr, ring_idx);
  if (msg->status != shmipc_STATUS_READY_FOR_SERVER) return NULL;

  *idx = ring_idx;
  mgr->next++;
  return msg;
}

off_t shmipc_mgr_alloc_slot(struct shmipc_mgr *mgr) {
  struct shmipc_msg *rmsg;
  off_t ring_idx;

  ring_idx = __sync_fetch_and_add(&(mgr->next), 1);
  ring_idx = ring_idx & mgr->mask;
  rmsg = IDX_TO_MSG(mgr, ring_idx);

  // NOTE: If the buffer is not large enough and we happen to
  // circle back onto a slot that is still in use, we will have to wait
  // till it is free. Further, if some task ahead in the ring gets done
  // quickly, we won't be able to take it's slot as we already have one.
  // Sort of like hol blocking but only when the buffer is small.
  // We might have to change to a mechanism where we get a slot only
  // when slots are available.
  while (__builtin_expect(rmsg->status != shmipc_STATUS_EMPTY, 0))
    ;

  // NOTE: This (below) might cause cache invalidations slowing down
  // server poll. Maybe client side should have a bitmap for the
  // ring slots to ensure that it is free?
  // These are purely conflicts that should be resolved at client side.
  rmsg->status = shmipc_STATUS_RESERVED;
  return ring_idx;
}

void shmipc_mgr_dealloc_slot(struct shmipc_mgr *mgr, off_t ring_idx) {
  struct shmipc_msg *rmsg;

  rmsg = IDX_TO_MSG(mgr, ring_idx);
  memset(rmsg, 0, 64);
}

void shmipc_mgr_put_msg(struct shmipc_mgr *mgr, off_t ring_idx,
                        struct shmipc_msg *msg) {
  struct shmipc_msg *rmsg;

  // NOTE: we assume the caller is responsible.
  // ring_idx is assumed to be valid - a value provided by alloc_slot
  rmsg = IDX_TO_MSG(mgr, ring_idx);

  // We perform the copy in two stages. First, everything from the "type"
  // to the end of the struct is copied. "type" currently is at offset 9.
  // Then, a barrier is placed followed by setting the server doorbell.
  // We then wait until the server rings the client doorbell.
  memcpy((char *)rmsg + 9, (char *)msg + 9, 55);
  SHMIPC_SET_MSG_STATUS(rmsg, shmipc_STATUS_READY_FOR_SERVER);
  while (rmsg->status != shmipc_STATUS_READY_FOR_CLIENT)
    ;

  // server finished operation. zero out rmsg after copying into msg.
  memcpy(msg, rmsg, 64);  // 40 cycles
}

void shmipc_mgr_put_msg_nowait(struct shmipc_mgr *mgr, off_t ring_idx,
                               struct shmipc_msg *msg) {
  struct shmipc_msg *rmsg;

  rmsg = IDX_TO_MSG(mgr, ring_idx);
  memcpy((char *)rmsg + 9, (char *)msg + 9, 55);
  SHMIPC_SET_MSG_STATUS(rmsg, shmipc_STATUS_READY_FOR_SERVER);
}

int shmipc_mgr_poll_msg(struct shmipc_mgr *mgr, off_t idx,
                        struct shmipc_msg *msg) {
  struct shmipc_msg *rmsg;

  rmsg = IDX_TO_MSG(mgr, idx);
  if (rmsg->status != shmipc_STATUS_READY_FOR_CLIENT) return -1;

  // server finished operation, copy into rmsg.
  memcpy(msg, rmsg, 64);
  return 0;
}

void shmipc_mgr_wait_msg(struct shmipc_mgr *mgr, off_t idx,
                         struct shmipc_msg *msg) {
  struct shmipc_msg *rmsg;

  rmsg = IDX_TO_MSG(mgr, idx);
  while (rmsg->status != shmipc_STATUS_READY_FOR_CLIENT)
    ;

  memcpy(msg, rmsg, 64);
}
