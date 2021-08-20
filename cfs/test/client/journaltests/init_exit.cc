#include <stdexcept>

#include "spdlog/spdlog.h"

#include "fsapi.h"
#include "journaltests_common.h"

int main(int argc, char **argv) {
  int num_keys = 1;
  if (argc >= 2) {
    if (sscanf(argv[1], "%d", &num_keys) != 1) {
      SPDLOG_ERROR("Unable to get num_keys from argv[1]: {}", argv[2]);
      return 1;
    }
  }
  FSPInitializer fspi(num_keys, 40960);
  return 0;
}
