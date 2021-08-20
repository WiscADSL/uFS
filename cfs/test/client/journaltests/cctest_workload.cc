#include <string.h>
#include <unistd.h>
#include <stdexcept>
#include <vector>

#include "cxxopts.hpp"
#include "spdlog/spdlog.h"

#include "fsapi.h"
#include "journaltests_common.h"

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "Checkpoint fsp");
  auto opts = options.add_options();
  opts("a,app_id", "App ID", cxxopts::value<key_t>()->default_value("0"));
  opts("n,num_workers", "Num FSP Workers",
       cxxopts::value<int>()->default_value("1"));
  opts("w,wid", "Which wid should this talk to",
       cxxopts::value<int>()->default_value("0"));
  opts("h,help", "Print Usage");

  auto args = options.parse(argc, argv);
  if (args.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  int wid = args["wid"].as<int>();
  int num_keys = args["num_workers"].as<int>();
  key_t app_id = args["app_id"].as<key_t>();
  FSPInitializer fspi(num_keys, 40960, app_id);
  memset(fspi.buf, (int)('a') + wid, 4096);

  // wid0 will write to t0
  std::stringstream fname_ss;
  fname_ss << "/t" << wid;
  std::string fname = fname_ss.str();

  if (wid != 0) {
    fs_admin_thread_reassign(0, wid, FS_REASSIGN_FUTURE);
  }

  // the first open/close is to force reassign
  int fd = fs_open(fname.c_str(), O_WRONLY, 0666);
  if (fd < 0) throw std::runtime_error("Failed to open");
  int iret = fs_close(fd);
  if (iret < 0) throw std::runtime_error("Failed to close");

  fd = fs_open(fname.c_str(), O_WRONLY, 0666);
  SPDLOG_INFO("Got fd {} for file {}", fd, fname);
  ssize_t ssret = fs_allocated_write(fd, fspi.buf, 4096);
  SPDLOG_INFO("fs_allocated_write returned {}", ssret);
  iret = fs_fdatasync(fd);
  SPDLOG_INFO("fdatasync returned {}", iret);
  sleep(2);
  ssret = fs_allocated_write(fd, fspi.buf, 4096);
  SPDLOG_INFO("fs_allocated_write returned {}", ssret);
  iret = fs_fdatasync(fd);
  SPDLOG_INFO("fdatasync returned {}", iret);
  iret = fs_close(fd);
  SPDLOG_INFO("close returned {}", iret);
  return 0;
}
