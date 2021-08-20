#ifndef __timer_rdtsc_h
#define __timer_rdtsc_h

#include <x86intrin.h>

static __inline__ unsigned long long rdtscp(void) {
  unsigned hi, lo;
  asm volatile(
      "rdtscp\n\t"
      "mov %%edx, %0\n\t"
      "mov %%eax, %1\n\t"
      : "=r"(hi), "=r"(lo)::"%rax", "%rbx", "%rcx", "%rdx");
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

#define TIMER_RDTSC_INIT(X) unsigned long long X##_start, X##_end;
#define TIMER_RDTSC_START(X) X##_start = rdtscp();
#define TIMER_RDTSC_STOP(X) X##_end = rdtscp();
#define TIMER_RDTSC_PRINT(X) \
  printf(#X " took %llu cycles\n", X##_end - X##_start)
#define TIMER_RDTSC_CLOCKS(X) (X##_end - X##_start)

#endif
