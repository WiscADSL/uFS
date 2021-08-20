#ifndef CFS_FS_PROC_UNIX_SOCK_H
#define CFS_FS_PROC_UNIX_SOCK_H

#include <sys/socket.h>
#include <sys/un.h>

#include "FsLibShared.h"
#include "util.h"

#define kSockListenCoreId (31)

namespace fsp_sock {

class UnixSocketListener {
 public:
  UnixSocketListener(int max_nw);
  ~UnixSocketListener() { CleanupSocket(); }

  void SockListenRunner();
  void StartRun() { running_ = true; }
  void ShutDown() { running_ = false; }

 private:
  static constexpr int kRecvIntervalUs = 100000;  // wait for 100ms

  std::atomic_bool running_{false};

  int max_num_worker = 0;

  // socket usage related
  int sock_fd_ = -1;
  struct sockaddr_un recv_addr_;
  union ControlMsg control_msg_;

  void SetupSocket();
  void CleanupSocket();
  int ProcessAppRegister(struct ucred &kerr_cred, int app_idx);
};

}  // namespace fsp_sock

#endif