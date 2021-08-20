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
  opts("h,help", "Print Usage");
  auto args = options.parse(argc, argv);
  if (args.count("help")) {
      std::cout << options.help() << std::endl;
      return 0;
  }

  key_t app_id = args["app_id"].as<key_t>();
  int iret;
  int num_keys = 1;
  FSPInitializer fspi(num_keys, 4096, app_id);

  iret = fs_checkpoint();
  if (iret < 0) SPDLOG_ERROR("Failed to fs checkpoint");
  return 0;
}
