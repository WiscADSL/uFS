#include <argp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "myargs.h"
#include "shmipc/shmipc.h"
#include "timer_rdtsc.h"

int main(int argc, char **argv) {
  struct arguments arguments;
  struct shmipc_mgr *mgr = NULL;
  off_t ring_idx;
  int ret;
  int i;

  struct shmipc_msg *msgpool = NULL;
  struct shmipc_msg *msg = NULL;
  void *_vmsgpool_ptr = NULL;
  unsigned long long *measurements = NULL;
  char xreq_data[shmipc_XREQ_MAX_ELEM_SIZE];

  TIMER_RDTSC_INIT(rtt_timer);

  get_arguments(&arguments, argc, argv);
  mgr = shmipc_mgr_init(arguments.shmfile, arguments.rsize, arguments.create);
  if (mgr == NULL) {
    LOG("Failed to init shm mgr");
    goto error;
  }

  // create a pool of 100 items
  if (posix_memalign(&_vmsgpool_ptr, 64, sizeof(struct shmipc_msg) * 100)) {
    LOG("Failed to alloc msg pool");
    goto error;
  }

  memset(_vmsgpool_ptr, 0, sizeof(struct shmipc_msg) * 100);
  msgpool = (struct shmipc_msg *)_vmsgpool_ptr;

  measurements = (unsigned long long *)malloc(sizeof(unsigned long long) *
                                              arguments.rounds);
  if (measurements == NULL) {
    LOG("Failed to allocate measurements arr");
    goto error;
  }
  memset((void *)measurements, 0,
         sizeof(unsigned long long) * arguments.rounds);

  if (arguments.xreq) {
    for (i = 0; i < arguments.rounds; i++) {
      msg = &(msgpool[i % 100]);
      msg->type = 5;
      msg->flags = 2;
      *(int *)msg->inline_data = 48;
      *(int *)xreq_data = 48;

      TIMER_RDTSC_START(rtt_timer);
      ring_idx = shmipc_mgr_alloc_slot(mgr);
      memcpy(IDX_TO_XREQ(mgr, ring_idx), xreq_data, shmipc_XREQ_MAX_ELEM_SIZE);
      shmipc_mgr_put_msg(mgr, ring_idx, msg);
      memcpy(xreq_data, IDX_TO_XREQ(mgr, ring_idx), shmipc_XREQ_MAX_ELEM_SIZE);
      shmipc_mgr_dealloc_slot(mgr, ring_idx);
      TIMER_RDTSC_STOP(rtt_timer);

      measurements[i] = TIMER_RDTSC_CLOCKS(rtt_timer);

      if (msg->retval != (ssize_t)i)
        LOG("Error: expected retval %d, got %zd", i, msg->retval);

      if (*(int *)msg->inline_data != 49)
        LOG("Error: expected 49, got %d\n", *(int *)msg->inline_data);

      if (*(int *)xreq_data != (49 + i))
        LOG("Error: expected 49 + %d, got %d\n", i, *(int *)xreq_data);
    }
  } else {
    for (i = 0; i < arguments.rounds; i++) {
      msg = &(msgpool[i % 100]);
      msg->type = 5;
      msg->flags = 2;
      *(int *)msg->inline_data = 48;

      TIMER_RDTSC_START(rtt_timer);
      ring_idx = shmipc_mgr_alloc_slot(mgr);
      shmipc_mgr_put_msg(mgr, ring_idx, msg);
      shmipc_mgr_dealloc_slot(mgr, ring_idx);
      TIMER_RDTSC_STOP(rtt_timer);

      measurements[i] = TIMER_RDTSC_CLOCKS(rtt_timer);

      if (msg->retval != (ssize_t)i)
        LOG("Error: expected retval %d, got %zd", i, msg->retval);

      if (*(int *)msg->inline_data != 49)
        LOG("Error: expected 49, got %d\n", *(int *)msg->inline_data);
    }
  }

  for (i = 0; i < arguments.rounds; i++) {
    printf("[%d] %llu\n", i, measurements[i]);
  }

  ret = 0;
  goto cleanup;
error:
  ret = 1;
cleanup:
  if (_vmsgpool_ptr != NULL) free(_vmsgpool_ptr);
  if (measurements != NULL) free(measurements);
  if (mgr != NULL) shmipc_mgr_destroy(mgr);
  return ret;
}
