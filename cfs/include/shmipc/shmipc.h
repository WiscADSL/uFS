/* shmipc creates a region that looks as follows
 *
 *  64B * RING_SIZE  1K * RING_SIZE    32K * RING_SIZE
 * +----------------+--------------+-------------------+
 * +     ring       +     xreq     +       data        +
 * +----------------+--------------+-------------------+
 *
 * When reading or writing to an index in the ring, the caller is allowed to
 * place extra request information in xreq[index] if more than 64B is needed.
 *
 */

// NOTE: assumes cacheline size is 64 bytes.
// TODO: make cmake configure script find out cacheline size and then
// using ifdef, decide on the right layout for common sizes.
// TODO: write tests to ensure that the struct actually does fit the
// cacheline size.

#ifndef __shmipc_h
#define __shmipc_h

#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define shmipc_XREQ_MAX_ELEM_SIZE 2048
#define shmipc_DATA_MAX_ELEM_SIZE 32768  // 32K

#define shmipc_FLAG_HAS_INLINE_DATA 1
#define shmipc_FLAG_HAS_EXTRA_DATA 2

#define shmipc_STATUS_EMPTY 0
#define shmipc_STATUS_RESERVED 1
#define shmipc_STATUS_READY_FOR_SERVER 2
#define shmipc_STATUS_IN_PROGRESS 3
#define shmipc_STATUS_READY_FOR_CLIENT 4

// queuepair for shared memory
struct shmipc_qp {
  int fd;       // shm file descriptor
  char *ptr;    // mmap pointer to shm
  size_t size;  // size of shm
  int create;   // should the file be created
  char filename[256];
};

struct shmipc_qp *shmipc_qp_get(const char *name, size_t size, int create);
void shmipc_qp_destroy(struct shmipc_qp *qp);

struct shmipc_msg {
  // In order to support all types of returncodes, we need 8 bytes
  // (int, size_t, ssize_t, etc) which can then be typecasted.
  // NOTE: The placement of retval as the first entry is only to allow
  // the 8 bytes after this to also be aligned. Placing retval after flags
  // would have added padding. Placing retval after inline* would
  // complicate using the inline data space.
  // Performance wise, all of these fit in the same cacheline so we should
  // be fine.
  ssize_t retval;
  volatile uint8_t status;  // shmipc_STATUS_*
  uint8_t type;             // CFS_OP_*
  uint8_t flags;            // shmipc_FLAG_*
  char inline_data[53];
};

#define SHMIPC_SET_MSG_STATUS(msg, flag) \
  do {                                   \
    __sync_synchronize();                \
    (msg)->status = flag;                \
  } while (0);

#define IDX_TO_XREQ(mgr, idx) (&(mgr->xreq[(idx)*shmipc_XREQ_MAX_ELEM_SIZE]))
#define IDX_TO_MSG(mgr, idx) (&(mgr->ring[(idx)]))
#define IDX_TO_DATA(mgr, idx) (&(mgr->data[(idx)*shmipc_DATA_MAX_ELEM_SIZE]))
struct shmipc_mgr {
  struct shmipc_qp *qp;
  struct shmipc_msg *ring;
  char *xreq;
  char *data;

  // member variables to iterate through the ring
  size_t capacity;
  size_t mask;
  off_t next;
};

struct shmipc_mgr *shmipc_mgr_init(const char *name, size_t rsize, int create);
// Init shmipc manager with shared memory so that forked processes
// can access it. Not used for now.
struct shmipc_mgr *shmipc_mgr_init_client(const char *name, size_t rsize,
                                          int create);
void shmipc_mgr_destroy(struct shmipc_mgr *mgr);
// destroy shmipc manager that is created with shared memory
void shmipc_mgr_destroy_client(struct shmipc_mgr *mgr);

void shmipc_mgr_server_reset(struct shmipc_mgr *mgr);
void shmipc_mgr_client_reset(struct shmipc_mgr *mgr);

// Single consumer model on server side. get_msg waits on ring[next] until
// it is ready and then returns the msg and sets idx so that the reader can
// read the xreq if any from that index.
struct shmipc_msg *shmipc_mgr_get_msg(struct shmipc_mgr *mgr, off_t *idx);

// Similar to get_msg but returns NULL if ring[next] is not ready.
struct shmipc_msg *shmipc_mgr_get_msg_nowait(struct shmipc_mgr *mgr,
                                             off_t *idx);

// All *put* functions first need to alloc a slot and later dealloc.
off_t shmipc_mgr_alloc_slot(struct shmipc_mgr *mgr);
// TODO in debug mode, measure how much time slots are held...
void shmipc_mgr_dealloc_slot(struct shmipc_mgr *mgr, off_t ring_idx);

// TODO write inline functions to reuse code in all these other functions.

// Multi producer model on client side. put_msg atomically updates next and
// puts the msg in ring[next]. It then waits until the server responds.
void shmipc_mgr_put_msg(struct shmipc_mgr *mgr, off_t ring_idx,
                        struct shmipc_msg *msg);

// Similar to put_msg but returns immediately without waiting for server to
// respond. Returns the index of the ring that was used.
// NOTE: The nowait here applies to waiting for the server to respond. This
// function will still block while inserting into the ring if the ring is full.
void shmipc_mgr_put_msg_nowait(struct shmipc_mgr *mgr, off_t ring_idx,
                               struct shmipc_msg *msg);

// Called after put_msg_nowait with the same idx returned by that function,
// to check if the server has finished responding and the message is ready
// for the client to read. On success, it returns 0 0 and stores the result
// in msg. If the msg cannot be read yet, -1 is returned.
int shmipc_mgr_poll_msg(struct shmipc_mgr *mgr, off_t idx,
                        struct shmipc_msg *msg);

// Similar to poll but waits until the msg is ready.
void shmipc_mgr_wait_msg(struct shmipc_mgr *mgr, off_t idx,
                         struct shmipc_msg *msg);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif
