#include <argp.h>
#include <stdio.h>

#include "log.h"
#include "myargs.h"
#include "shmipc/shmipc.h"

int main(int argc, char **argv) {
  struct arguments arguments;
  struct shmipc_mgr *mgr = NULL;
  struct shmipc_msg *msg = NULL;
  int i, ret;
  off_t idx;
  char *xreq;

  get_arguments(&arguments, argc, argv);
  mgr = shmipc_mgr_init(arguments.shmfile, arguments.rsize, arguments.create);
  if (mgr == NULL) {
    LOG("Failed to create shm manager");
    goto error;
  }

  if (arguments.xreq) {
    for (i = 0; i < arguments.rounds; i++) {
      msg = shmipc_mgr_get_msg(mgr, &idx);
      msg->retval = (ssize_t)i;
      *(int *)msg->inline_data = 49;
      xreq = IDX_TO_XREQ(mgr, idx);
      *(int *)xreq = 49 + i;
      SHMIPC_SET_MSG_STATUS(msg, shmipc_STATUS_READY_FOR_CLIENT);
    }
  } else {
    for (i = 0; i < arguments.rounds; i++) {
      msg = shmipc_mgr_get_msg(mgr, &idx);
      msg->retval = (ssize_t)i;
      *(int *)msg->inline_data = 49;
      SHMIPC_SET_MSG_STATUS(msg, shmipc_STATUS_READY_FOR_CLIENT);
    }
  }

  ret = 0;
  goto cleanup;
error:
  ret = 1;
cleanup:
  shmipc_mgr_destroy(mgr);
  return 1;
}
