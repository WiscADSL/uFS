#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include "BlkDevSpdk.h"
#include "FsProc_Fs.h"
#include "fsapi.h"

BlkDevSpdk *devPtr;
FsProc *gFsProcPtr = nullptr;

int main(int argc, char **argv) {
  // BlkDevSpdk dev("", 1024, 4096);
  // devPtr = &dev;
  devPtr = new BlkDevSpdk("", 1024, 4096);
  devPtr->devInit();
  // FsProc p(dev);
  gFsProcPtr = new FsProc(*devPtr, 1, 0, "");
  // test dev write/read
  // devPtr->initWorker();
  // char *data_ptr1 = new char[4096];
  // char *data_ptr2 = new char[4096];
  char *data_ptr1 = (char *)devPtr->zmallocBuf(4096, 4096);
  char *data_ptr2 = (char *)devPtr->zmallocBuf(4096, 4096);
  memset(data_ptr1, 0, 4096);
  memset(data_ptr2, 0, 4096);
  for (int i = 0; i < 4096; i++) {
    data_ptr1[i] = 'a' + i % 20;
  }
  int testCheck = devPtr->checkCompletion(0);
  std::cout << "testCheck return:" << testCheck << std::endl;
  int checkNum = 0;
#if 0
    devPtr->write(0, data_ptr1);
    while(checkNum == 0) {
        checkNum = devPtr->checkCompletion(0);
    }
    assert(checkNum > 0);
    std::cout << "check Finished:" << checkNum << std::endl;
#endif
  devPtr->read(0, data_ptr2);
  checkNum = 0;
  while (checkNum == 0) {
    checkNum = devPtr->checkCompletion(0);
  }
  assert(checkNum > 0);
  std::cout << "first char:" << data_ptr2[0] << std::endl;
  // clean up
  devPtr->freeBuf(data_ptr1);
  devPtr->freeBuf(data_ptr2);
  // devPtr->devExit();
  delete (devPtr);
  delete (gFsProcPtr);
  return 0;
}
