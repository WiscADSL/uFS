
#include "FsLibLeaseShared.h"

FsLeaseCommon* FsLeaseCommon::singleInstance = nullptr;

FsLeaseCommon* FsLeaseCommon::getInstance() {
  if (singleInstance == nullptr) {
    singleInstance = new FsLeaseCommon();
  }
  return singleInstance;
}
