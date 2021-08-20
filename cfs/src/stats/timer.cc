//
// Created by daiyi on 2020/02/02.
//

#include "stats/timer.h"
#include <x86intrin.h>
#include <cassert>
#include "perfutil/Cycles.h"
#include "stats/stats.h"

namespace adgMod {

Timer::Timer() : time_accumulated(0), started(false) {}

void Timer::Start() {
  if (started) exit(-2);
  unsigned int dummy = 0;
  time_started = __rdtscp(&dummy);
  started = true;
}

std::pair<uint64_t, uint64_t> Timer::Pause(bool record) {
  if (!started) exit(-3);
  unsigned int dummy = 0;
  uint64_t time_elapse = __rdtscp(&dummy) - time_started;
  time_accumulated += PerfUtils::Cycles::toNanoseconds(time_elapse);

  if (record) {
    Stats* instance = Stats::GetInstance();
    uint64_t start_absolute = time_started - instance->initial_time;
    uint64_t end_absolute = start_absolute + time_elapse;
    started = false;
    return {PerfUtils::Cycles::toNanoseconds(start_absolute),
            PerfUtils::Cycles::toNanoseconds(end_absolute)};
  } else {
    started = false;
    return {0, 0};
  }
}

void Timer::Reset() {
  time_accumulated = 0;
  started = false;
}

uint64_t Timer::Time() {
  // assert(!started);
  return time_accumulated;
}
}  // namespace adgMod