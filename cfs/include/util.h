#ifndef CFS_UTIL_H
#define CFS_UTIL_H

#include <string.h>
#include <sys/time.h>

#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "typedefs.h"
// for using pthread
#include <pthread.h>

cfs_tid_t cfsGetTid(void);
void cfsSetTid(cfs_tid_t t);
#define ROUND_UP(N, S) ((((N) + (S)-1) / (S)) * (S))

// flip one bit by a mask
// mask is given by the format like 0b000100
#define UTIL_BIT_FLIP(X, MASK) (((X) & (~(MASK))) | (((X) ^ (MASK))))

template <typename T>
static inline void PrintMacroVal(std::string var_name, T t) {
  std::cout << var_name << " set to:" << t << std::endl;
}

#define macro_print(name) PrintMacroVal(#name, (name));

// #define TEST_BLOCK_ALLOC_FREE
// block bit operations
int block_set_bit(uint32_t nr, void *addr);
int block_clear_bit(uint32_t nr, void *addr);
int block_test_bit(uint32_t nr, void *addr);
int64_t find_block_free_bit_no(void *addr, int64_t numBitsPerBlock);
int64_t find_block_free_bit_no_start_from(void *addr, int start_bit_no,
                                          int64_t numBitsPerBlock);
int64_t find_block_free_multi_bits_no(void *addr, int64_t numBitsPerBlock,
                                      int numBitsNeed);
int64_t find_block_free_jump_bits_no_start_from(void *addr, int start_bit_no,
                                                int64_t numBitsPerBlock,
                                                int numBitsJump);

// check a file's existance
bool checkFileExistance(const char *filename);

// file path operation. parse path by "/", save each dir item
// @param path: a valid "path" from a file system view (e.g., a/b/c/d///e)
// @param tokenVec: result will be saved into this vector
int filepath2Tokens(char *path, std::vector<std::string> &tokenVec);

// @param standardFullPath
char *filepath2TokensStandardized(const char *path, int *pathDelimIdxArr,
                                  int &depth);

// DEPRECATED: slow version using std::stringstream
int filepath2TokensSlow(char *path, std::vector<std::string> &tokenVec);

// split a string according to one delim (char)
std::vector<std::string> splitStr(const std::string &input, char delim);

inline char *nowarn_strncpy(char *dst, const char *src, size_t n) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-truncation"
  return strncpy(dst, src, n);
#pragma GCC diagnostic pop
}

// suppose some fun thing here
// print the error exit symbol to stderr
void printOnErrorExitSymbol();
void printMasterSymbol();
void printServantSymbol();

////////////////////////////////////////////////////////////////////////////////
// inline functions
////////////////////////////////////////////////////////////////////////////////

// timing and metrics
inline int64_t tap_ustime(void) {
  struct timeval tv;
  int64_t ust;

  gettimeofday(&tv, NULL);
  ust = ((long)tv.tv_sec) * 1000000;
  ust += tv.tv_usec;
  return ust;
}

template <typename T1, typename T2>
inline uint64_t AssembleTwo32B(T1 var1, T2 var2) {
  return ((long long)var1 << 32) | var2;
}

template <typename T1, typename T2>
inline void DessembleOne64B(uint64_t key, T1 &var1, T2 &var2) {
  var1 = (key & (0xFFFFFFFF00000000)) >> 32;
  var2 = key & (0x00000000FFFFFFFF);
}

// pin the current running thread to certain cpu core.
// core_id starts from 1, return 0 on success.
inline int pin_to_cpu_core(int core_id) {
  if (core_id < 1) return -1;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id - 1, &cpuset);
  int s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  return s;
}

inline void nextHighestPow2Val(uint32_t &v) {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v++;
}

class UtilSpinLock {
 public:
  UtilSpinLock() : flag_(ATOMIC_FLAG_INIT) {}
  void lock() {
    while (flag_.test_and_set(std::memory_order_acquire)) {
      // spin
    }
  }

  void unlock() { flag_.clear(std::memory_order_release); }

 private:
  std::atomic_flag flag_;
};

#endif  // CFS_UTIL_H
