#ifndef __journaltests_common_h
#define __journaltests_common_h

// TODO have a standard set of key patterns that are used for fsp and client.
class FSPInitializer {
 public:
  FSPInitializer(size_t num_keys, size_t buf_size, key_t app_id=0);
  FSPInitializer(std::vector<key_t> &keys, size_t buf_size);
  ~FSPInitializer();
  void *buf;

 private:
  void _initialize(std::vector<key_t> &keys, size_t buf_size);
};

#endif  // __journaltests_common_h
