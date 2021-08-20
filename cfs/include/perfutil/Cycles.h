/* Copyright (c) 2011-2014 Stanford University
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

#ifndef PERFUTILS_CYCLES_H
#define PERFUTILS_CYCLES_H

#include <stdint.h>

namespace PerfUtils {

/**
 * This class provides static methods that read the fine-grain CPU
 * cycle counter and translate between cycle-level times and absolute
 * times.
 */
class Cycles {
public:
  static void init();

  /**
   * Return the current value of the fine-grain CPU cycle counter
   * (accessed via the RDTSC instruction).
   */
  static __inline __attribute__((always_inline)) uint64_t rdtsc() {
#if TESTING
    if (mockTscValue)
            return mockTscValue;
#endif
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return (((uint64_t)hi << 32) | lo);
  }

  /**
   * Return the current value of the fine-grain CPU cycle counter with
   * partial serialization.  (accessed via the RDTSCP instruction).
   */
  static __inline __attribute__((always_inline)) uint64_t rdtscp() {
#if TESTING
    if (mockTscValue)
            return mockTscValue;
#endif
    uint32_t lo, hi;
    __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi) : : "%rcx");
    return (((uint64_t)hi << 32) | lo);
  }

  static __inline __attribute__((always_inline)) double perSecond() {
    return getCyclesPerSec();
  }
  static double toSeconds(uint64_t cycles, double cyclesPerSec = 0);
  static uint64_t fromSeconds(double seconds, double cyclesPerSec = 0);
  static uint64_t toMilliseconds(uint64_t cycles, double cyclesPerSec = 0);
  static uint64_t fromMilliseconds(uint64_t ms, double cyclesPerSec = 0);
  static uint64_t toMicroseconds(uint64_t cycles, double cyclesPerSec = 0);
  static uint64_t fromMicroseconds(uint64_t us, double cyclesPerSec = 0);
  static uint64_t toNanoseconds(uint64_t cycles, double cyclesPerSec = 0);
  static uint64_t fromNanoseconds(uint64_t ns, double cyclesPerSec = 0);
  static void sleep(uint64_t us);

private:
  Cycles();

  /// Conversion factor between cycles and the seconds; computed by
  /// Cycles::init.
  static double cyclesPerSec;

  /// Used for testing: if nonzero then this will be returned as the result
  /// of the next call to rdtsc().
  static uint64_t mockTscValue;

  /// Used for testing: if nonzero, then this is used to convert from
  /// cycles to seconds, instead of cyclesPerSec above.
  static double mockCyclesPerSec;

  /**
   * Returns the conversion factor between cycles in seconds, using
   * a mock value for testing when appropriate.
   */
  static __inline __attribute__((always_inline)) double getCyclesPerSec() {
#if TESTING
    if (mockCyclesPerSec != 0.0) {
            return mockCyclesPerSec;
        }
#endif
    return cyclesPerSec;
  }
};

}  // namespace PerfUtils

#endif  // PERFUTILS_CYCLES_H