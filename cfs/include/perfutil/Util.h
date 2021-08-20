/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef PERFUTIL_UTIL_H
#define PERFUTIL_UTIL_H

#include <asm/unistd.h>
#include <assert.h>
#include <sched.h>
#include <stdint.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>

#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <mmintrin.h>
#include <xmmintrin.h>

namespace PerfUtils {

/**
 * Miscellaneous methods that seem like they might be useful in several
 * different places.
 */
namespace Util {

std::string hexDump(const void* buffer, uint64_t bytes);
std::string format(const char* format, ...)
__attribute__((format(printf, 1, 2)));
std::string vformat(const char* format, va_list ap)
__attribute__((format(printf, 1, 0)));
void split(const std::string& s, char delim, std::vector<std::string>& elems);
std::vector<std::string> split(const std::string& s, char delim);
std::vector<int> readIntegers(int fd, char delimiter);
char* fileGetContents(FILE* f);
char* fileGetContents(const char* filename);
std::vector<int> parseRanges(const char* coreDesc);
std::vector<int> readRanges(FILE* f);
std::vector<int> readRanges(const char* filename);
std::vector<int> getAllUseableCores();
void* cacheAlignAlloc(size_t size);
void pinAvailableCore();
int getPhysicalCore(int coreId);
int getHyperTwin(int coreId);

#define PERFUTIL_UNUSED(expr) do { (void)(expr); } while (0)

/* Doxygen is stupid and cannot distinguish between attributes and arguments. */
#define FORCE_INLINE __inline __attribute__((always_inline))

/**
 * Convert any templated iterable container to an unordered set.
 */
template <template <class...> class C, typename S, typename... Ts>
std::unordered_set<S>
containerToUnorderedSet(const C<S, Ts...>& container) {
  std::unordered_set<S> set;
  for (S s : container) {
    set.insert(s);
  }
  return set;
}

/**
 * A utility for function for calling rdpmc and reading Intel's performance
 * counters. Returns the value of the performance monitoring counter with
 * index specified by argumemt ecx.
 *
 * \param ecx
 *    The index of the PMC register to read the value from. The correct
 *    value of this parameter is dependent on the selected pmc.
 *
 * NB:
 *
 * 1. This function will segfault if called in userspace unless the 8th bit
 *    of the CR4 register is set.
 * 2. This function's behavior will change depending on which pmc's have been
 *    selected. The selection is done using wrmsr from inside a kernel module.
 */
static uint64_t FORCE_INLINE
rdpmc(int ecx) {
  unsigned int a, d;
  __asm __volatile("rdpmc" : "=a"(a), "=d"(d) : "c"(ecx));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

/**
 * Returns the thread id of the calling thread
 * As long as the thread continues to run, this id is unique across all threads
 * running on the system so it can be used to uniquely name per-thread
 * resources
 */
static pid_t FORCE_INLINE
gettid() {
  return static_cast<pid_t>(syscall(__NR_gettid));
}

/**
 * This function pins the currently executing thread onto the CPU Core with
 * the id given in the argument.
 *
 * \param id
 *      The id of the core to pin the caller's thread to.
 */
static FORCE_INLINE void
pinThreadToCore(int id) {
  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);
  CPU_SET(id, &cpuset);
  assert(sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0);
}

/**
 * Returns the cpu affinity mask of the currently executing thread. The type
 * cpu_set_t encodes information about which cores the current thread is
 * permitted to run on.
 */
static FORCE_INLINE cpu_set_t
getCpuAffinity() {
  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);
  assert(sched_getaffinity(0, sizeof(cpuset), &cpuset) == 0);
  return cpuset;
}

/**
 * This function sets the allowable set of cores for the currently executing
 * thread, and is usually used to restore an older set which was read using
 * getCpuAffinity().
 *
 * \param cpuset
 *      An object of type cpu_set_t which encodes the set of cores which
 *      current thread is permitted to run on.
 */
static FORCE_INLINE void
setCpuAffinity(cpu_set_t cpuset) {
  assert(sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0);
}

/**
 * This function is used to seralize machine instructions so that no
 * instructions that appear after it in the current thread can run before any
 * instructions that appear before it.
 *
 * It is useful for putting around rdpmc instructions (to pinpoint cache
 * misses) as well as before rdtsc instructions, to prevent time pollution from
 * instructions supposed to be executing before the timer starts.
 */
static FORCE_INLINE void
serialize() {
  uint32_t eax, ebx, ecx, edx;
  __asm volatile("cpuid"
  : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx)
  : "a"(1U));
}

/**
 * A compiler barrier preventing compiler reordering
 */
static FORCE_INLINE void
barrier() {
  asm volatile("" : : : "memory");
}

/**
 * This is a convenience function to make a call to rdpmc with serializing
 * wrappers to ensure all earlier instructions have executed and no later
 * instructions have executed.
 *
 * \param ecx
 *      The index of the PMC register to read the value from. The correct
 *      value of this parameter is dependent on the selected pmc.
 */
static FORCE_INLINE uint64_t
serialReadPmc(int ecx) {
  serialize();
  uint64_t retVal = rdpmc(ecx);
  serialize();
  return retVal;
}

/**
 * Prefetch the cache lines containing [object, object + numBytes) into the
 * processor's caches.
 * The best docs for this are in the Intel instruction set reference under
 * PREFETCH.
 * \param object
 *      The start of the region of memory to prefetch.
 * \param numBytes
 *      The size of the region of memory to prefetch.
 */
static inline void
prefetch(const void* object, uint64_t numBytes)
{
  uint64_t offset = reinterpret_cast<uint64_t>(object) & 0x3fUL;
  const char* p = reinterpret_cast<const char*>(object) - offset;
  for (uint64_t i = 0; i < offset + numBytes; i += 64)
    _mm_prefetch(p + i, _MM_HINT_T0);
}

/**
 * Prefetch the cache lines containing the given object into the
 * processor's caches.
 * The best docs for this are in the Intel instruction set reference under
 * PREFETCHh.
 * \param object
 *      A pointer to the object in memory to prefetch.
 */
template<typename T>
static inline void
prefetch(const T* object)
{
  prefetch(object, sizeof(*object));
}

#define PERFUTILS_DIE(format_, ...)                     \
    do {                                                \
        fprintf(stderr, format_, ##__VA_ARGS__);        \
        fprintf(stderr, "%s:%d\n", __FILE__, __LINE__); \
        abort();                                        \
    } while (0)

}  // namespace Util

}  // namespace PerfUtils

#undef FORCE_INLINE
#endif  // PERFUTIL_UTIL_H