#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "cxxopts.hpp"
#include "spdlog/spdlog.h"

#include "fsapi.h"
#include "journaltests_common.h"

void WriteStats(std::string fsp_file_path, struct stat &buf) {
  std::filesystem::path p(fsp_file_path);
  auto basename = p.filename().string();
  auto stats_name = basename + "_stats";
  std::ofstream stats_file;
  stats_file.open(stats_name);
  stats_file << "{";
  {
    stats_file << "\"inode\":" << buf.st_ino << ",";
    stats_file << "\"size\":" << buf.st_size << ",";
    stats_file << "\"name\": \"" << fsp_file_path << "\"";
  }
  stats_file << "}";
  stats_file.close();
}

void WriteData(std::string fsp_file_path, void *buf, size_t buf_sz) {
  std::filesystem::path p(fsp_file_path);
  auto basename = p.filename().string();
  auto data_name = basename + "_data";
  int fd = open(data_name.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0) throw std::runtime_error("failed to create data file");

  ssize_t ssret = write(fd, buf, buf_sz);
  if (ssret < 0) {
    SPDLOG_ERROR("write returned {}, errno: {}", ssret, strerror(errno));
    throw std::runtime_error("Failed to write to data file");
  }

  if ((size_t)ssret != buf_sz) {
    SPDLOG_ERROR("write returned {}", ssret);
    throw std::runtime_error("Failed to write all data");
  }

  int iret = close(fd);
  if (iret != 0) throw std::runtime_error("Failed to close data file");
}

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "Checkpoint fsp");
  auto opts = options.add_options();
  opts("a,app_id", "App ID", cxxopts::value<key_t>()->default_value("0"));
  opts("n,num_files", "Num Files", cxxopts::value<key_t>()->default_value("1"));
  opts("h,help", "Print Usage");

  auto args = options.parse(argc, argv);
  if (args.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  int num_keys = 1;
  key_t app_id = args["app_id"].as<key_t>();
  FSPInitializer fspi(num_keys, 40960, app_id);
  memset(fspi.buf, 0, 40960);

  int num_files = args["num_files"].as<int>();
  for (int i = 0; i < num_files; i++) {
    // wid0 will write to t0
    std::stringstream fname_ss;
    fname_ss << "/t" << i;
    std::string fname = fname_ss.str();
    struct stat stat_buf;
    int iret = fs_stat(fname.c_str(), &stat_buf);
    if (iret < 0) throw std::runtime_error("Failed to stat");
    WriteStats(fname, stat_buf);

    int fd = fs_open(fname.c_str(), O_RDONLY, 0);
    if (fd < 0) throw std::runtime_error("Failed to open");

    if (stat_buf.st_size > 0) {
      fs_allocated_read(fd, fspi.buf, stat_buf.st_size);
      WriteData(fname, fspi.buf, stat_buf.st_size);
    }

    fs_close(fd);
  }

  return 0;
}
