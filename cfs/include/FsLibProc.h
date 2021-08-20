#ifndef CFS_FSLIBPROC_H
#define CFS_FSLIBPROC_H

#include "FsLibShared.h"
#include <sys/types.h>

class CommuChannelFsSide : public CommuChannel {
public:
  CommuChannelFsSide(pid_t id, key_t shmKey);
  ~CommuChannelFsSide();
  int recvSlot(int &dstSid);

protected:
  void *attachSharedMemory(void);
  void cleanupSharedMemory(void);
};

#endif // CFS_FSLIBPROC_H
