#include <sys/stat.h>
#include <stdexcept>

#include "cxxopts.hpp"
#include "spdlog/spdlog.h"

#include "fsapi.h"
#include "journaltests_common.h"

void write_file(const char *filename, int num_blocks, char c) {
  struct stat statbuf;
  int iret = fs_stat(filename, &statbuf);
  if (iret != 0) {
    SPDLOG_ERROR("{} does not exist", filename);
    return;
  }

  // FIXME: why is st_ino 0?
  SPDLOG_INFO("stat returned inode {}, size {}", statbuf.st_ino,
              statbuf.st_size);

  int fd = fs_open(filename, O_WRONLY, 0);
  if (fd < 0) {
    SPDLOG_ERROR("Failed to open {} in write mode", filename);
    return;
  }

  char buf[4096];
  for (int j = 0; j < num_blocks - 1; j++) {
    memset(buf, c, 4096);
    ssize_t ssret = fs_write(fd, buf, 4096);
    SPDLOG_INFO("fs_write (Block {}) returned {}", j, ssret);
  }

  memset(buf, c, 4096);
  buf[4095] = '\0';
  ssize_t ssret = fs_write(fd, buf, 4096);
  SPDLOG_INFO("fs_write (Block {}) returned {}", num_blocks - 1, ssret);

  fs_close(fd);
}

void read_file(const char *filename, int num_blocks) {
  int fd = fs_open(filename, O_RDONLY, 0);
  if (fd < 0) {
    SPDLOG_ERROR("Failed to open {} in read mode", filename);
    return;
  }

  char buf[4096];
  for (int i = 0; i < 4096; i++) buf[i] = '\0';

  for (int j = 0; j < num_blocks; j++) {
    ssize_t ssret = fs_read(fd, buf, 4096);
    SPDLOG_INFO("fs_read (Block {}) returned {}", j, ssret);
    if (ssret > 0) SPDLOG_INFO("content: [{}]", buf);
  }

  fs_close(fd);
}

void fsync_file(const char *filename) {
  int fd = fs_open(filename, O_WRONLY, 0);
  if (fd < 0) {
    SPDLOG_ERROR("Failed to open {} in write mode", filename);
    return;
  }

  int iret = fs_fsync(fd);
  SPDLOG_INFO("fs_fsync returned {}", iret);
  fs_close(fd);
}

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "Read Write & Fsync an existing file");
  auto opts = options.add_options();
  opts("f,filename", "File name", cxxopts::value<std::string>());
  opts("w,num_workers", "Num FSP Workers",
       cxxopts::value<int>()->default_value("1"));
  opts("n,num_blocks", "Num blocks to read/write",
       cxxopts::value<int>()->default_value("1"));
  opts("c,character", "Repeated character to write",
       cxxopts::value<char>()->default_value("a"));
  opts("a,app_id", "App ID", cxxopts::value<key_t>()->default_value("0"));
  opts("h,help", "Print usage");
  auto args = options.parse(argc, argv);

  if (args.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }
  int num_keys = args["num_workers"].as<int>();
  std::string filename_str = args["filename"].as<std::string>();
  int num_blocks = args["num_blocks"].as<int>();
  key_t app_id = args["app_id"].as<key_t>();
  char c = args["character"].as<char>();
  const char *filename = filename_str.c_str();

  FSPInitializer fspi(num_keys, 40960, app_id);
  write_file(filename, num_blocks, c);
  read_file(filename, num_blocks);
  fsync_file(filename);
  return 0;
}
