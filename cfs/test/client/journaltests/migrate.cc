#include <sys/stat.h>
#include <stdexcept>
#include <vector>

#include "cxxopts.hpp"
#include "spdlog/spdlog.h"

#include "fsapi.h"
#include "journaltests_common.h"

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "Read Write & Fsync an existing file");
  auto opts = options.add_options();
  opts("f,filename", "File name", cxxopts::value<std::string>());
  opts("w,num_workers", "Num FSP Workers",
       cxxopts::value<int>()->default_value("2"));
  opts("a,app_id", "App ID", cxxopts::value<key_t>()->default_value("0"));
  opts("h,help", "Print usage");
  auto args = options.parse(argc, argv);

  if (args.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  int num_keys = args["num_workers"].as<int>();
  std::string filename_str = args["filename"].as<std::string>();
  key_t app_id = args["app_id"].as<key_t>();
  FSPInitializer fspi(num_keys, 40960, app_id);

  const char *filename = filename_str.c_str();

  int fd = fs_open(filename, O_RDONLY, 0);
  if (fd < 0) {
    SPDLOG_ERROR("failed to open {} in read mode", filename);
    return 1;
  }

  int wid = -1;
  int iret = fs_migrate(fd, &wid);
  if (iret < 0) {
    SPDLOG_ERROR("something went wrong, fs_migrate returned {}", iret);
  }

  iret = fs_close(fd);
  SPDLOG_INFO("fs_close returned {}", iret);

  struct stat statbuf;
  iret = fs_stat(filename, &statbuf);
  if (iret != 0) {
    SPDLOG_ERROR("{} does not exist", filename);
    return 1;
  }

  SPDLOG_INFO("stat returned inode {}, size {}", statbuf.st_ino,
              statbuf.st_size);
  return 0;
}
