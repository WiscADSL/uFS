#include "FsLibShared.h"

#include <stdio.h>

#include <cassert>
#include <cstring>

#include "util.h"
#include "util/util_buf_ring.h"

// e.g. we can have a ring of size 64, but number be enquede/dequeued between
// [0,32)
// @param size of the backend data structure. The fixed number slot
// ring-buffer's size
// @param maxNo Max number that could be able to put/get from this ring
OpRingBuffer::OpRingBuffer(int size, int maxNo)
    : ringSize(size), srcDstMaxNo(maxNo), ringPtr(nullptr), ringMemSize(0) {
  this->ringMemSize = util_ring_get_memsize(size);
  // Note: I don't know why I need 2*maxNo here
  // otherwise, it will result in malloc() seg fault
  src = (void **)malloc(srcDstMaxNo * 2 * sizeof(void *));
  for (int i = 0; i < srcDstMaxNo; i++) {
    src[i] = (void *)(uint64_t)i;
  }
}

OpRingBuffer::~OpRingBuffer() { free(src); }

long OpRingBuffer::getRingMemorySize() { return ringMemSize; }

// @param idx (slotID)
// @return <0 if error, == 0 not successful, ==1 means success
int OpRingBuffer::enqueue(int idx) {
  int ret;
  assert(idx >= 0 && idx < srcDstMaxNo);
  void **cur_src = src + idx;
  ret = util_ring_mp_enqueue_bulk(ringPtr, cur_src, 1, NULL);
  return ret;
}

// @return the SlotID, a number that is >=0, <maxNo
int OpRingBuffer::dequeue(int &sid) {
  int ret;
  void *tmpDst[2] = {nullptr, nullptr};
  void **tmpDstIdx = &tmpDst[0];
  ret = util_ring_mc_dequeue_bulk(ringPtr, tmpDstIdx, 1, NULL);
  sid = (int)((uint64_t)(*tmpDstIdx));
  return ret;
}

// @param memPtr, pointer to the shared memory, need to has enough memory ==
// ringMemSize
// @return
int OpRingBuffer::initRingMemory(pid_t pid, const void *memPtr) {
  char utilRingName[64];
  memset(utilRingName, 0, 64);
  sprintf(utilRingName, "ring-%d", pid);
  ringPtr = (struct util_ring *)memPtr;
  util_ring_init(ringPtr, utilRingName, ringSize, 0);
  return 0;
}

CommuChannel::~CommuChannel() {
  // If do not clear, then will trigger delete of DataBufItem
  // result in double free
  clientOpVec.clear();
  dataBufVec.clear();
}

void *CommuChannel::slotDataPtr(int sid) { return &(dataBufVec[sid]->buf); }

struct clientOp *CommuChannel::slotOpPtr(int sid) {
  return clientOpVec[sid];
}

// ChannelMemyLayout:
// |struct util_ring |struct CommuOpDataBuf[ops|items]|
int CommuChannel::initChannelMemLayout() {
  // assume ringMemSize returned is aligned to cache-line size
  ringBufferPtr = new OpRingBuffer(realRingSize, itemNum);
  ringMemSize = ringBufferPtr->getRingMemorySize();
  //  fprintf(stdout, "CommuChannel::initChannelMemLayout - ringMemSize:%ld\n",
  //          ringMemSize);
  // allocate memory
  opDataBufMemSize = sizeof(struct CommuOpDataBuf);
  totalSharedMemSize = ringMemSize + opDataBufMemSize;
  totalSharedMemPtr = (char *)attachSharedMemory();
  if (totalSharedMemPtr == nullptr) {
    return -1;
  }
  //  fprintf(stderr,
  //          "CommuChannel::initChannelMemLayout shmKey:%d
  //          totalSharedMemPtr:%p\n", shmKey, totalSharedMemPtr);
  // init op & data buffer ptr
  opDataBufPtr = (struct CommuOpDataBuf *)(totalSharedMemPtr + ringMemSize);
  // init the vectors of op/data pointers
  struct clientOp *curOpPtr = &(opDataBufPtr->ops[0]);
  struct dataBufItem *curDataBufPtr = &(opDataBufPtr->items[0]);
  for (auto i = 0; i < itemNum; i++) {
    clientOpVec[i] = curOpPtr + i;
    dataBufVec[i] = curDataBufPtr + i;
  }
  struct util_ring *ringMemPtr = (struct util_ring *)totalSharedMemPtr;
  ringBufferPtr->initRingMemory(pid, ringMemPtr);
  return 0;
}
