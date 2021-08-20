#ifndef __fsproc_messenger_h
#define __fsproc_messenger_h

#include "param.h"
#include "shmipc/shmipc.h"

struct FsProcMessage {
 public:
  uint8_t type;
  void *ctx;
};

// TODO find another place for these structs and types? Don't have to be kept in
// the messenger functionality which is generic.
enum FsProcMessageType {
  NOOP,
  BITMAP_CHANGES,
  PREPARE_FOR_CHECKPOINTING,
  CHECKPOINTING_COMPLETE,
  PROPOSE_CHECKPOINT,
  ASSIGN_CHECKPOINT_TO_WORKER,
  // TODO: fix the naming convention for the other message types
  // https://google.github.io/styleguide/cppguide.html#Enumerator_Names
  kReassignment,
  kOwnerUnlinkInode,
  //
  // message with load manager
  //
  LM_REBALANCE_ALLOC_SHARE,
  LM_REBALANCE_ACK,
  LM_REBALANCE_ACK_ABORT,
  LM_JOINALL,
  LM_JOINALL_ACK,
  LM_JOINALL_ACK_ABORT,
  // v2
  // Message used in KPLoadManager's policy
  // NOTE: please make sure ACK is always = original msg+1
  // Basically add item into 'CHECK_MSG_ACK_INVARIANT'
  kLM_RedirectOneTau,
  kLM_RedirectOneTauAck,
  kLM_RedirectCreation,
  kLM_RedirectCreationAck,
  kLM_RedirectFuture,
  kLM_RedirectFutureAck,
  kLM_Rebalance,
  kLM_RebalanceAck,
  kLM_JoinAll,
  kLM_JoinAllAck,
  kLM_JoinAllCreation,
  kLM_JoinAllCreationAck,
  // end of message with load manager
};

#define CHECK_MSG_ACK_INVARIANT(msg_name)                \
  static_assert((int(FsProcMessageType::msg_name##Ack) - \
                     int(FsProcMessageType::msg_name) == \
                 1),                                     \
                #msg_name);

CHECK_MSG_ACK_INVARIANT(kLM_RedirectOneTau);
CHECK_MSG_ACK_INVARIANT(kLM_RedirectCreation);
CHECK_MSG_ACK_INVARIANT(kLM_RedirectFuture);
CHECK_MSG_ACK_INVARIANT(kLM_Rebalance);
CHECK_MSG_ACK_INVARIANT(kLM_JoinAll);
CHECK_MSG_ACK_INVARIANT(kLM_JoinAllCreation);

class FsProcMsgRing {
 public:
  FsProcMsgRing(size_t capacity);
  ~FsProcMsgRing();
  // Thread safe - puts a message into the ring. Returns success bool.
  bool put_message(FsProcMessage &fsp_msg);
  // Thread safe - gets a message from the ring. Returns success bool.
  bool get_message(FsProcMessage &fsp_msg);

 private:
  size_t capacity;
  // NOTE: implementing using shmipc_mgr since that also has a single consumer
  // multi producer model. However, the assumption there is that the client and
  // server have their own separate shmipc_mgr instances but share the
  // underlying buffer. So the head/tail pointers are independent. Due to lack
  // of time, we just use two shmipc_mgr instances here.
  // TODO consider using the dpdk ring library
  struct shmipc_mgr *consumer;
  struct shmipc_mgr *producer;

  void initShmipcMgr(struct shmipc_mgr *mgr);
};

// TODO: consider just sending messages with function and ctx.
// receiver will automatically call those functions with the ctx argument.
class FsProcMessenger {
 public:
  constexpr static int kLmWid = (NMAX_FSP_WORKER);
  constexpr static int kNumNonWorkerThreads = 1;
  constexpr static int kActualRingLen = NMAX_FSP_WORKER + kNumNonWorkerThreads;
  FsProcMessenger(size_t n_workers, size_t ring_size);
  ~FsProcMessenger();
  // sends a message to wid
  bool send_message(int wid, FsProcMessage &fsp_msg);
  // recieves a message that was sent to wid
  // NOTE: This allows anyone to recieve a message meant for anyone else.
  // Callers should only call this function using their own wid. Since this
  // is internally used by workers (that trust each other), it is fine for now.
  // TODO: allow send_to_any, but recv only on my own wid.
  bool recv_message(int wid, FsProcMessage &fsp_msg);

  bool send_message_to_loadmonitor(int fromWid, FsProcMessage &fsp_msg);

 private:
  size_t n_workers;
  size_t ring_size;
  FsProcMsgRing *rings[kActualRingLen];
};

// inline functions for FsProcMsgRing

inline bool FsProcMsgRing::put_message(FsProcMessage &fsp_msg) {
  // NOTE: a limitation with the shmipc mgr is that alloc_slot is blocking.
  // However, that is only if the entire ring is completley full.
  // Currently, messages between workers are not that frequent (during fsync and
  // checkpointing) and they are checked on every iteration, so it should not be
  // a problem.
  struct shmipc_msg msg;
  msg.status = shmipc_STATUS_RESERVED;
  msg.type = fsp_msg.type;
  void **ptr = (void **)(msg.inline_data);
  *ptr = (void *)fsp_msg.ctx;
  off_t ring_idx = shmipc_mgr_alloc_slot(producer);
  shmipc_mgr_put_msg_nowait(producer, ring_idx, &msg);
  return true;
}

inline bool FsProcMsgRing::get_message(FsProcMessage &fsp_msg) {
  off_t idx;
  struct shmipc_msg *msg = shmipc_mgr_get_msg_nowait(consumer, &idx);
  if (msg == nullptr) return false;

  fsp_msg.type = msg->type;
  void **ptr = (void **)(msg->inline_data);
  fsp_msg.ctx = *ptr;
  // NOTE: in the actual client/server model, the client allocates and
  // deallocates slots. But here, we let the other fsproc workers send
  // messages, so they alloc slots. And when we process it, we immediately
  // deallocate it can be used again.
  // FsProc workers will only send the message through the ring. They can
  // check their ctx variables to figure out if a message succeeded.
  shmipc_mgr_dealloc_slot(consumer, idx);
  return true;
}

// inline functions for FsProcMessenger
inline bool FsProcMessenger::send_message(int wid, FsProcMessage &fsp_msg) {
  // TODO eventually we might dynamically create/destroy workers. We will need
  // to register workers with the messenger and check for existence.
  return rings[wid]->put_message(fsp_msg);
}

inline bool FsProcMessenger::recv_message(int wid, FsProcMessage &fsp_msg) {
  return rings[wid]->get_message(fsp_msg);
}

inline bool FsProcMessenger::send_message_to_loadmonitor(
    int fromWid, FsProcMessage &fsp_msg) {
  // NOTE: the legacy FsProc_LoadMng.cc use 'fromWid' field as a int
  return send_message(kLmWid, fsp_msg);
}

// TODO batch_message and send_batch. Let recv_message stay the same. It would
// receive a batch and iterate over that.
#endif  // __fsproc_messenger_h
