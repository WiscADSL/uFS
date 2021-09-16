#ifndef __coordinator_h
#define __coordinator_h

#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include <stdexcept>

#include "shmipc/shmipc.h"

/* coordinator shm layout - 8 byte for each
 * +-------------------+---------------------+-------------------+------------------+
 * + client says ready | client says stopped | server says start | server says
 * stop |
 * +-------------------+---------------------+-------------------+------------------+
 */

class CoordinatorBase {
 public:
  ~CoordinatorBase(void) {
    if (qp != nullptr) shmipc_qp_destroy(qp);
  }

 protected:
  char *shmfile{nullptr};
  struct shmipc_qp *qp{nullptr};
  uint64_t *layout{nullptr};
};

class CoordinatorServer : public CoordinatorBase {
 public:
  CoordinatorServer(const char *shmfile, size_t num_clients) {
    this->shmfile = strdup(shmfile);
    this->num_clients = num_clients;
    this->qp = shmipc_qp_get(shmfile, 4096, 0);
    if (this->qp != NULL) {
      throw std::runtime_error("coordinator shm already exists");
    }

    this->qp = shmipc_qp_get(shmfile, 4096, 1);
    if (this->qp == NULL) throw std::runtime_error("Failed to create shm qp");

    this->layout = (uint64_t *)this->qp->ptr;
  }

  inline void wait_till_all_clients_ready(void) {
    volatile uint64_t *ptr = &(layout[0]);
    while (*ptr != num_clients)
      ;
  }

  inline void wait_till_one_client_done(void) {
    volatile uint64_t *ptr = &(layout[1]);
    while (*ptr == 0)
      ;
  }

  inline void notify_all_clients_to_start(void) {
    volatile uint64_t *ptr = &(layout[2]);
    *ptr = 1;
  }

  inline void notify_all_clients_to_stop(void) {
    volatile uint64_t *ptr = &(layout[3]);
    *ptr = 1;
  }

 private:
  size_t num_clients{0};
};

class CoordinatorClient : public CoordinatorBase {
 public:
  CoordinatorClient(const char *shmfile, off_t client_id) {
    int retries = 0;
    while (true) {
      if (retries > 20)
        throw std::runtime_error("Failed to retrieve qp, max_retries=20");

      this->qp = shmipc_qp_get(shmfile, 4096, 0);
      if (this->qp != NULL) break;

      retries += 1;
      usleep(1e6);  // TODO sleep for shorter time?
    }

    this->shmfile = strdup(shmfile);
    this->client_id = client_id;
    this->layout = (uint64_t *)this->qp->ptr;
  }

  inline void notify_server_that_client_is_ready(void) {
    if (notified_server)
      throw std::runtime_error("Already notified server that client is ready");

    __sync_fetch_and_add(&(layout[0]), 1);
  }

  inline void notify_server_that_client_stopped(void) {
    volatile uint64_t *ptr = &(layout[1]);
    *ptr = 1;
  }

  inline void wait_till_server_says_start(void) {
    volatile uint64_t *ptr = &(layout[2]);
    while (*ptr == 0)
      ;
  }

  inline bool check_server_said_stop(void) { return (layout[3] == 1); }

 private:
  off_t client_id{0};
  bool notified_server{false};
};

#endif
