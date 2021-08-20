
#ifndef CFS_INCLUDE_FSPROC_WORKERCOMM_H_
#define CFS_INCLUDE_FSPROC_WORKERCOMM_H_

#include <unordered_set>

#include "cfs_feature_macros.h"
#include "readerwriterqueue.h"
#include "spdlog/sinks/basic_file_sink.h"

class FsProcWorker;
class InMemInode;
struct FileObj;
class SingleSizeMemBlockArr;
class FsReq;
class BlockBufferItem;

using shmIdBufPairs = std::unordered_map<uint8_t, SingleSizeMemBlockArr *>;

// NOTE: invariant here: (xxxOutMsgReply - xxxOutMsg == 1) && (xxxOutMsg % 2 ==
// 1)
enum class WorkerCommMessageType {
  // Zero reserved as the default
  EmptyMsg = 0,
  // Split
  FileInodeSplitOutMsg = 1,
  FileInodeSplitOutMsgReply = 2,
  // unlink
  FileInodeUnlinkOutMsg = 3,
  FileInodeUnlinkOutMsgReply = 4,
  // Shm
  ShmMsg = 5,
  ShmMsgReply = 6,

  _LADMARK_MSG_SERVANT_INITIATE_ = 20,
  // Join
  FileInodeJoinBackMsg = 21,
  FileInodeJoinBackMsgReply = 22,
};

static inline bool ifCommMsgServantsInitiated(WorkerCommMessageType tp) {
  return tp > WorkerCommMessageType::_LADMARK_MSG_SERVANT_INITIATE_;
}

// define reply of msg here
#define WK_COMM_RPL_UNLINK_SAFE_TO_GC_INODE (1)
#define WK_COMM_RPL_GENERAL_OK (0)
#define WK_COMM_RPL_GENERAL_ERR (-1)

// helpers to get a string from enum type
// TODO (jingliu): make this staff as a template
// similar thing is defined for FsReq::Type
static WorkerCommMessageType gCommMsgTp0 = WorkerCommMessageType::EmptyMsg;
using wkCommMsg_t = uint32_t;
#define WK_COMM_MSG_TYPE_AD_HOC_TO_STR(A)                 \
  {                                                       \
    (static_cast<wkCommMsg_t>(WorkerCommMessageType::A) - \
     static_cast<wkCommMsg_t>(gCommMsgTp0)),              \
        #A                                                \
  }

const static std::unordered_map<uint32_t, const char *> gWkCommMsgTypeStrMap{
    WK_COMM_MSG_TYPE_AD_HOC_TO_STR(EmptyMsg),
    WK_COMM_MSG_TYPE_AD_HOC_TO_STR(FileInodeSplitOutMsg),
    WK_COMM_MSG_TYPE_AD_HOC_TO_STR(FileInodeSplitOutMsgReply),
    WK_COMM_MSG_TYPE_AD_HOC_TO_STR(FileInodeUnlinkOutMsg),
    WK_COMM_MSG_TYPE_AD_HOC_TO_STR(FileInodeUnlinkOutMsgReply),
    WK_COMM_MSG_TYPE_AD_HOC_TO_STR(FileInodeJoinBackMsg),
};

std::string inline getWkCommMsgTypeOutputString(WorkerCommMessageType tp) {
  auto it = gWkCommMsgTypeStrMap.find(static_cast<uint32_t>(tp) -
                                      static_cast<uint32_t>(gCommMsgTp0));
  if (it != gWkCommMsgTypeStrMap.end()) {
    return it->second;
  } else {
    return "NOT SUPPORT";
  }
}

bool inline checkWkCommMsgReplyTypeMatchSent(WorkerCommMessageType out,
                                             WorkerCommMessageType reply) {
  return static_cast<wkCommMsg_t>(out) % 2 == 1 &&
         (static_cast<wkCommMsg_t>(reply) - static_cast<wkCommMsg_t>(out)) == 1;
}

class WorkerCommBridge;
// Message that used to communicate via this WorkerCommBridge
struct WorkerCommMessage {
  WorkerCommMessage(WorkerCommMessageType tp) : msgType{tp} {}
  ~WorkerCommMessage() {
    perAppOpenedFiles.clear();
    perAppOpenedShms.clear();
    dataBlocks.clear();
  }

  //
  // data fields
  //
  WorkerCommMessageType msgType{WorkerCommMessageType::EmptyMsg};
  InMemInode *targetInode{nullptr};
  int dstWid{0};
  int origWid{0};
  FsReq *req{nullptr};
#if CFS_JOURNAL(ON) || 1
  // When migrating, it is important to know if an inode is already in a journal
  // so that it can be tracked and checkpointed even when migrated.
  bool journalled{false};
#endif
  // If current message represent a REPLY message, this will be set to the
  // message that needs to be relied
  WorkerCommMessage *sendMsg{nullptr};
  std::unordered_map<pid_t, std::vector<FileObj *>> perAppOpenedFiles{};
  std::unordered_map<pid_t, shmIdBufPairs *> perAppOpenedShms{};
  std::unordered_set<BlockBufferItem *> dataBlocks{};

  int reply{WK_COMM_RPL_GENERAL_ERR};
  // NOTE: this is only used when we start splitting out inode of type DIR
  // InMemInode *targetParDirInode{nullptr};
};

WorkerCommMessage *genFileInodeOutMsg(WorkerCommMessageType tp,
                                      InMemInode *inodePtr, FsReq *req);
WorkerCommMessage *genFileInodeOutReplyMsg(WorkerCommMessageType tp,
                                           WorkerCommMessage *sendMsg);

// A communication bridge between two workers
// Term: the two parties to communicate are *left (l)* and *right (r)*
// l & r should be both of *FsProcWorker*
class WorkerCommBridge {
 public:
  // max number of inflight request inside the bridge (on-direction)
  constexpr static int kMaxInflightMsgNum = 100;
  WorkerCommBridge() = delete;

  // @param lWid: workerId of l
  // @param rWid: workerId of r
  WorkerCommBridge(FsProcWorker *l, int lWid, FsProcWorker *r, int rWid) {
    workers_[0] = l;
    workers_[1] = r;
    workerWids_[0] = lWid;
    workerWids_[1] = rWid;
  }

  ~WorkerCommBridge() {}

  // Non-blocking Put(), it may fail and return false
  bool Put(FsProcWorker *putter_worker, WorkerCommMessage *msg,
           std::shared_ptr<spdlog::logger> curLogger = nullptr);
  // Non-blocking Get(), if queue is empty, return nullptr
  // @return msg : if this msg has a corresponding outBoundMsg (aka. req), will
  //   store the pointer to that outBoundMsg to *msg*, otherwise set to nullptr
  WorkerCommMessage *Get(FsProcWorker *getter_worker, WorkerCommMessage **msg,
                         std::shared_ptr<spdlog::logger> curLogger = nullptr);

  size_t getOutQueueSizeApprox(FsProcWorker *worker) {
    return getWorkerSendQueue(worker)->size_approx();
  }

  uintptr_t getOutQueueAddr(FsProcWorker *worker) {
    return reinterpret_cast<uintptr_t>(getWorkerSendQueue(worker));
  }

  uintptr_t getRecvQueueAddr(FsProcWorker *worker) {
    return reinterpret_cast<uintptr_t>(getWorkerRecvQueue(worker));
  }

  size_t getRecvQueueSizeApprox(FsProcWorker *worker) {
    return getWorkerRecvQueue(worker)->size_approx();
  }

  int getPutterGetterWid(FsProcWorker *worker) {
    int worker_idx = getWorkerIdx(worker);
    if (worker_idx < 0) return worker_idx;
    return workerWids_[worker_idx];
  }

  int getOtherSideWid(FsProcWorker *worker) {
    int worker_idx = getWorkerIdx(worker);
    if (worker_idx < 0) return worker_idx;
    return workerWids_[1 - worker_idx];
  }

 private:
  FsProcWorker *workers_[2]{nullptr, nullptr};
  int workerWids_[2]{0, 0};
  typedef moodycamel::ReaderWriterQueue<WorkerCommMessage *>
      unidirectional_msg_queue;
  unidirectional_msg_queue msgQueues_[2]{
      // workers_[0] uses this to send out - workers_[1] uses this to recv
      unidirectional_msg_queue{kMaxInflightMsgNum},
      // workers_[1] uses this to send out - workers_[0] uses this to recv
      unidirectional_msg_queue{kMaxInflightMsgNum}};
  std::unordered_set<WorkerCommMessage *> waitForReplyMsg_[2];

  int getWorkerIdx(FsProcWorker *worker) {
    int worker_idx = worker == workers_[1];
    assert(worker == workers_[worker_idx]);
    return worker_idx;
  }

  unidirectional_msg_queue *getWorkerSendQueue(FsProcWorker *worker) {
    int worker_idx = getWorkerIdx(worker);
    if (worker_idx < 0) return nullptr;
    return &msgQueues_[worker_idx];
  }

  unidirectional_msg_queue *getWorkerRecvQueue(FsProcWorker *worker) {
    int worker_idx = getWorkerIdx(worker);
    if (worker_idx < 0) return nullptr;
    return &msgQueues_[1 - worker_idx];
  }
};

#endif  // CFS_INCLUDE_FSPROC_WORKERCOMM_H_
