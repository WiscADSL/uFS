#include "FsProc_LoadMng.h"

namespace fsp_lm {

// static bool gLoadAllowanceDbgPrint = false;
uint64_t LoadAllowanceContainer::versionIncr = 1;

int LoadAllowanceContainer::sendMessageToWorkers(
    std::vector<PerWorkerLoadAllowance *> &allowVec, FsProcMessageType type) {
  int numDone = 0;
  FsProcMessage curmsg;
  for (auto wal : allowVec) {
    assert(wal->rwid >= 0);
    assert(wal->vid > 0);
    curmsg.type = type;
    curmsg.ctx = wal;
    bool success = messenger_->send_message(wal->wid, curmsg);
    if (!success) {
      numDone = -1;
      break;
    }
    numDone++;
  }
  return numDone;
}

}  // namespace fsp_lm