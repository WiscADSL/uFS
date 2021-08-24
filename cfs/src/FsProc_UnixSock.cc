
#include "FsProc_UnixSock.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>

#include <filesystem>

#include "FsLibShared.h"
#include "FsProc_Fs.h"
#include "FsProc_Messenger.h"
#include "param.h"

extern FsProc *gFsProcPtr;

namespace fsp_sock {

namespace kerr_fs = std::filesystem;

// message header def
// struct msghdr {
//     void         *msg_name;       /* optional address */
//     socklen_t     msg_namelen;    /* size of address */
//     struct iovec *msg_iov;        /* scatter/gather array */
//     size_t        msg_iovlen;     /* # elements in msg_iov */
//     void         *msg_control;    /* ancillary data, see below */
//     size_t        msg_controllen; /* ancillary data buffer len */
//     int           msg_flags;      /* flags on received message */
// };
// control message header def
// struct cmsghdr {
//   socklen_t cmsg_len; /* data byte count, including hdr */
//   int cmsg_level;     /* originating protocol */
//   int cmsg_type;      /* protocol-specific type */
//   /* followed by
//       unsigned char cmsg_data[]; */
// };

UnixSocketListener::UnixSocketListener(int nw) : max_num_worker(nw) {
  // set 'control_msg' to describe ancillary data that we want to receive
  control_msg_.cmh.cmsg_len = CMSG_LEN(sizeof(struct ucred));
  control_msg_.cmh.cmsg_level = SOL_SOCKET;
  control_msg_.cmh.cmsg_type = SCM_CREDENTIALS;
}

void UnixSocketListener::SetupSocket() {
  if (kerr_fs::exists(CRED_UNIX_SOCKET_PATH)) {
    std::cerr << "CRED_UNIX_SOCKET_PATH" << CRED_UNIX_SOCKET_PATH << std::endl;
    throw std::runtime_error("cred sock path exists");
  }

  memset(&recv_addr_, 0, sizeof(struct sockaddr_un));
  recv_addr_.sun_family = AF_UNIX;
  nowarn_strncpy(recv_addr_.sun_path, CRED_UNIX_SOCKET_PATH,
                 strlen(CRED_UNIX_SOCKET_PATH) + 1);
  sock_fd_ = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (sock_fd_ == -1) {
    fprintf(stderr, "ERROR cannot create socket [%s] \n", strerror(errno));
    throw std::runtime_error("cannot create socket");
  }

  if (bind(sock_fd_, (struct sockaddr *)&recv_addr_,
           sizeof(struct sockaddr_un)) == -1) {
    fprintf(stderr, "ERROR cannot bind addr [%s] \n", strerror(errno));
    throw std::runtime_error("cannot bind");
  }

  int optval = 1;
  if (setsockopt(sock_fd_, SOL_SOCKET, SO_PASSCRED, &optval, sizeof(optval)) ==
      -1) {
    fprintf(stderr, "ERROR cannot setsockopt failed [%s] \n", strerror(errno));
    throw std::runtime_error("setsockopt fail");
  }

  int status =
      fcntl(sock_fd_, F_SETFL, fcntl(sock_fd_, F_GETFL, 0) | O_NONBLOCK);
  if (status == -1) {
    fprintf(stderr, "cannot set sock_fd_ to noblocking\n");
    throw std::runtime_error("fcntl");
  }
}

void UnixSocketListener::CleanupSocket() {
  if (kerr_fs::exists(CRED_UNIX_SOCKET_PATH)) {
    int rt = remove(CRED_UNIX_SOCKET_PATH);
    if (rt == -1 && errno != ENOENT) {
      throw std::runtime_error("cannot cleanup socket file");
    }
  }
}

int UnixSocketListener::ProcessAppRegister(struct ucred &kerr_cred,
                                           int app_idx) {
  gFsProcPtr->g_cred_table.AddAppCredential(kerr_cred, app_idx);
  return 0;
}

void UnixSocketListener::SockListenRunner() {
  pin_to_cpu_core(kSockListenCoreId);
  SetupSocket();

  while (!gFsProcPtr->checkWorkerActive(FsProcWorker::kMasterWidConst))
    ;
  SPDLOG_INFO("{} is running ------", typeid(this).name());

#ifdef UFS_SOCK_LISTEN
  struct msghdr recv_msgh;

  struct iovec recv_iov;
  recv_msgh.msg_iov = &recv_iov;
  recv_msgh.msg_iovlen = 1;
  struct UfsRegisterOp recv_rgst_op;
  struct UfsRegisterAckOp recv_rgst_ack_op;
  recv_iov.iov_base = &recv_rgst_op;
  recv_iov.iov_len = sizeof(recv_rgst_op);

  struct sockaddr_un app_addr;
  recv_msgh.msg_name = (void *)(&app_addr);
  recv_msgh.msg_namelen = sizeof(app_addr);
  recv_msgh.msg_control = control_msg_.control;
  recv_msgh.msg_controllen = sizeof(control_msg_.control);

  fprintf(stderr, "sock_fd_:%d\n", sock_fd_);
  while (running_) {
    std::this_thread::sleep_for(std::chrono::microseconds(kRecvIntervalUs));
    pin_to_cpu_core(kSockListenCoreId);

    // Receive real plus ancillary data
    ssize_t nr = recvmsg(sock_fd_, &recv_msgh, 0);
    if (nr == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      throw std::runtime_error("recvmsg error return -1");
    }
    std::cout << "recv nr:" << nr << std::endl;

    struct cmsghdr *cmsgp = CMSG_FIRSTHDR(&recv_msgh);
    if (cmsgp == NULL || cmsgp->cmsg_len != CMSG_LEN(sizeof(struct ucred))) {
      if (cmsgp != nullptr)
        std::cerr << "cmsg_len:" << cmsgp->cmsg_len << std::endl;
      throw std::runtime_error("bad cmsg header / message length");
    }
    if (cmsgp->cmsg_level != SOL_SOCKET) {
      throw std::runtime_error("cmsg_level != SOL_SOCKET");
    }
    if (cmsgp->cmsg_type != SCM_CREDENTIALS) {
      throw std::runtime_error("cmsg_type != SCM_CREDENTIALS");
    }

    // get the valid credential information
    struct ucred rcred;
    memcpy(&rcred, CMSG_DATA(cmsgp), sizeof(struct ucred));

    // ok, now put it into the global context
    int app_idx = gFsProcPtr->GetNextAppIdxThenIncr();
    ProcessAppRegister(rcred, app_idx);

    SPDLOG_INFO(
        "credential from data pid:{} uid:{} gid:{} emu_pid:{} app_idx:{}",
        rcred.pid, rcred.gid, rcred.uid, recv_rgst_op.emu_pid, app_idx);

    // send the key back to the clients through the sockets
    struct msghdr send_msgh;
    send_msgh.msg_name = (void *)(&app_addr);
    send_msgh.msg_namelen = sizeof(app_addr);
    send_msgh.msg_control = NULL;
    send_msgh.msg_controllen = 0;
    struct iovec send_iov;
    send_iov.iov_base = &recv_rgst_ack_op;
    send_iov.iov_len = sizeof(recv_rgst_ack_op);
    send_msgh.msg_iov = &send_iov;
    send_msgh.msg_iovlen = 1;
    recv_rgst_ack_op.num_worker_max = max_num_worker;
    recv_rgst_ack_op.shm_key_base = (FS_SHM_KEY_BASE + app_idx + 1);
    recv_rgst_ack_op.worker_key_distance =
        gFsProcPtr->GetWorkerShmkeyDistance();
    ssize_t ns = sendmsg(sock_fd_, &send_msgh, 0);
    if (ns == -1) {
      throw std::runtime_error("sendmsg shmkey back to app fail");
    }

  }     // running_
#endif  // UFS_SOCK_LISTEN
}

}  // namespace fsp_sock
