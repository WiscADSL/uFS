#include <string.h>
#include <unistd.h>
#include <cassert>
#include <stdexcept>

#include "FsProc_Messenger.h"

FsProcMsgRing::FsProcMsgRing(size_t capacity) : capacity(capacity) {
  assert((capacity > 4) && ((capacity & (capacity - 1)) == 0));

  producer = (struct shmipc_mgr *)malloc(sizeof(struct shmipc_mgr));
  consumer = (struct shmipc_mgr *)malloc(sizeof(struct shmipc_mgr));

  if (consumer == NULL || producer == NULL)
    throw std::runtime_error("failed to malloc shmipc_mgr instances");

  initShmipcMgr(consumer);
  initShmipcMgr(producer);

  struct shmipc_msg *ring = NULL;
  ring = (struct shmipc_msg *)malloc(sizeof(*ring) * capacity);
  if (ring == NULL) throw std::runtime_error("failed to malloc ring");

  memset(ring, 0, sizeof(*ring) * capacity);
  consumer->ring = ring;
  producer->ring = ring;
}

FsProcMsgRing::~FsProcMsgRing() {
  struct shmipc_msg *ring = NULL;
  if (consumer != NULL) {
    if (consumer->ring != NULL) ring = consumer->ring;
    free(consumer);
  }
  if (producer != NULL) {
    // both producer and consumer point to same ring, okay to overwrite
    if (producer->ring != NULL) ring = producer->ring;
    free(producer);
  }
  if (ring != NULL) free(ring);
}

void FsProcMsgRing::initShmipcMgr(struct shmipc_mgr *mgr) {
  mgr->qp = NULL;
  mgr->ring = NULL;
  mgr->xreq = NULL;
  mgr->data = NULL;
  mgr->capacity = capacity;
  mgr->mask = capacity - 1;
  mgr->next = 0;
}

FsProcMessenger::FsProcMessenger(size_t n_workers, size_t ring_size)
    : n_workers(n_workers), ring_size(ring_size) {
  for (size_t i = 0; i < n_workers; i++) {
    rings[i] = new FsProcMsgRing(ring_size);
  }
  for (size_t i = 0; i < kNumNonWorkerThreads; i++) {
    rings[kLmWid + i] = new FsProcMsgRing(ring_size);
  }
  for (size_t i = n_workers; i < NMAX_FSP_WORKER; i++) rings[i] = nullptr;
}

FsProcMessenger::~FsProcMessenger() {
  for (size_t i = 0; i < n_workers; i++) {
    if (rings[i] != nullptr) {
      delete rings[i];
      rings[i] = nullptr;
    }
  }
}
