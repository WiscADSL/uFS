#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "shmipc/shmipc.h"

#define sassert(cond, msg)       \
  do {                           \
    if (!(cond)) {               \
      fprintf(stdout, msg "\n"); \
      exit(1);                   \
    }                            \
  } while (0);

void simple_async_test() {
  struct shmipc_mgr *server_mgr = NULL;
  struct shmipc_mgr *client_mgr = NULL;
  struct shmipc_msg *server_msg = NULL;
  struct shmipc_msg *tmp_msg = NULL;
  struct shmipc_msg client_msg;
  off_t sidx, cidx, tidx;
  int cres;

  server_mgr = shmipc_mgr_init("/testshm", 64, 1);
  client_mgr = shmipc_mgr_init("/testshm", 64, 0);
  sassert(server_mgr != NULL, "Failed to server manager");
  sassert(client_mgr != NULL, "Failed to client manager");

  server_msg = shmipc_mgr_get_msg_nowait(server_mgr, &sidx);
  sassert(server_msg == NULL, "Empty ring should return NULL");

  memset(&client_msg, 0, sizeof(struct shmipc_msg));
  client_msg.type = 1;
  client_msg.flags = 1;
  *(int *)client_msg.inline_data = 7;

  cidx = shmipc_mgr_alloc_slot(client_mgr);
  shmipc_mgr_put_msg_nowait(client_mgr, cidx, &client_msg);

  cres = shmipc_mgr_poll_msg(client_mgr, cidx, &client_msg);
  sassert(cres == -1, "Poll should return -1 till server responds");

  server_msg = shmipc_mgr_get_msg_nowait(server_mgr, &sidx);
  sassert(server_msg != NULL, "Server should read msg when ring not empty");

  tmp_msg = shmipc_mgr_get_msg_nowait(server_mgr, &tidx);
  sassert(tmp_msg == NULL, "Ring should be empty again after server read");

  server_msg->type++;
  server_msg->flags++;
  *(int *)(server_msg->inline_data) = 8;
  SHMIPC_SET_MSG_STATUS(server_msg, shmipc_STATUS_READY_FOR_CLIENT);

  cres = shmipc_mgr_poll_msg(client_mgr, cidx, &client_msg);
  sassert(cres == 0, "Poll should return 0 when msg is ready");
  shmipc_mgr_dealloc_slot(client_mgr, cidx);

  sassert(client_msg.type == 2, "incorrect value");
  sassert(client_msg.flags == 2, "incorrect value");
  sassert(*(int *)client_msg.inline_data == 8, "incorrect value");

cleanup:
  if (client_mgr != NULL) shmipc_mgr_destroy(client_mgr);
  if (server_mgr != NULL) shmipc_mgr_destroy(server_mgr);
}

int main(int argc, char **argv) {
  simple_async_test();
  return 0;
}
