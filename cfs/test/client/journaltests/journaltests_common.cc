#include <stdexcept>
#include <vector>

#include "spdlog/spdlog.h"

#include "fsapi.h"
#include "journaltests_common.h"

FSPInitializer::FSPInitializer(size_t num_keys, size_t buf_size, key_t app_id) : buf(NULL) {
  if (num_keys == 0) throw std::invalid_argument("numkeys == 0");

  // FIXME: have a handshake with server instead of hardcoded numbers?
  const key_t base = 20190301 + app_id;
  // Pattern: base + 1, base + 11, base + 22, ....
  std::vector<key_t> keys{base + 1, base + 11};
  for (size_t i = 2; i < num_keys; i++) {
    keys.push_back(keys.back() + 11);
  }

  keys.resize(num_keys);
  _initialize(keys, buf_size);
}

FSPInitializer::FSPInitializer(std::vector<key_t> &keys, size_t buf_size)
    : buf(NULL) {
  if (keys.empty()) throw std::invalid_argument("empty vector: keys");

  _initialize(keys, buf_size);
}

void FSPInitializer::_initialize(std::vector<key_t> &keys, size_t buf_size) {
  const key_t *shmKeys = keys.data();
  int iret = fs_init_multi(keys.size(), shmKeys);
  if (iret < 0) throw std::runtime_error("Failed to fs_init_multi");

  if (buf_size == 0) return;

  buf = fs_malloc(buf_size);
  if (buf == NULL) {
    fs_exit();
    throw std::runtime_error("Failed to fs_malloc");
  }
}

FSPInitializer::~FSPInitializer() {
  if (buf != NULL) {
    fs_free(buf);
    buf = NULL;
  }

  int iret = fs_exit();
  if (iret != 0) {
    SPDLOG_ERROR("Failed to fs_exit");
  }
}
