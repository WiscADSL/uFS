
#include "FsLibShared.h"
#include "gtest/gtest.h"
#include <atomic>
#include <memory>
#include <sstream>
#include <stdio.h>
#include <thread>

namespace {

std::atomic_flag sidLock = ATOMIC_FLAG_INIT;
int curSlotId = 0;
int ringSize = 64;
int maxSlotId = 32;

int getAtomicSlotId() {
  int tmpSlotId;
  sidLock.test_and_set(std::memory_order_acquire);
  tmpSlotId = curSlotId;
  curSlotId++;
  if (curSlotId >= maxSlotId) {
    curSlotId = 0;
  }
  sidLock.clear(std::memory_order_release);
  return tmpSlotId;
}

class Putter {
public:
  void operator()(std::shared_ptr<OpRingBuffer> ringPtr, int tid, int nPut) {
    int nPutDone = 0;
    std::stringstream ss;
    ss << "Putter:[tid-" << tid << "] ";
    for (int i = 0; i < nPut; i++) {
      int tmpSlotId = getAtomicSlotId();
    RETRY_ENQUEUE:
      int rt = ringPtr->enqueue(tmpSlotId);
      ASSERT_TRUE(rt == 0 || rt == 1);
      if (rt == 0) {
        goto RETRY_ENQUEUE;
      } else {
        nPutDone++;
        std::cout << ss.str() << " enqueue:" << tmpSlotId << std::endl;
      }
    }
  }
};

class Getter {
public:
  void operator()(std::shared_ptr<OpRingBuffer> ringPtr, int tid, int nGet) {
    int nGetDone = 0;
    std::stringstream ss;
    ss << "Getter:[tid-" << tid << "] ";
    while (nGetDone < nGet) {
      int tmpSid;
      int rt = ringPtr->dequeue(tmpSid);
      ASSERT_TRUE(rt == 0 || rt == 1);
      if (rt == 1) {
        ASSERT_TRUE(tmpSid >= 0 && tmpSid < maxSlotId);
        nGetDone++;
        std::cout << ss.str() << " dequeue:" << tmpSid << std::endl;
      }
    }
  }
};

class OpRingBufferTest : public ::testing::Test {
protected:
  OpRingBufferTest() {
    // setup here
  }
  void SetUp() override {
    curSlotId = 0;
    ringPtr = std::make_shared<OpRingBuffer>(ringSize, maxSlotId);
    long memSize = ringPtr->getRingMemorySize();
    memPtr = malloc(memSize);
    int pid = 1;
    ringPtr->initRingMemory(pid, memPtr);
  }

  void TearDown() override { free(memPtr); }
  void *memPtr;
  std::shared_ptr<OpRingBuffer> ringPtr;
};

TEST_F(OpRingBufferTest, OnePutterOneGetter) {
  std::thread t1(Putter(), ringPtr, 1, 10);
  std::thread t2(Getter(), ringPtr, 2, 10);
  t1.join();
  t2.join();
}

TEST_F(OpRingBufferTest, MultiPutterOneGetter) {
  std::thread putters[2];
  for (int i = 0; i < 2; i++) {
    putters[i] = std::thread(Putter(), ringPtr, i, 10);
  }
  std::thread tGetter(Getter(), ringPtr, 10, 2 * 10);
  tGetter.join();
  for (int i = 0; i < 2; i++) {
    putters[i].join();
  }
}

TEST_F(OpRingBufferTest, MultiPutterMultiGetter) {
#define MT_NUM 4
  std::thread putters[MT_NUM];
  std::thread getters[MT_NUM];
  for (int i = 0; i < MT_NUM; i++) {
    putters[i] = std::thread(Putter(), ringPtr, i, 10);
    getters[i] = std::thread(Getter(), ringPtr, i + 10, 10);
  }
  for (int i = 0; i < MT_NUM; i++) {
    putters[i].join();
    getters[i].join();
  }
}

} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}