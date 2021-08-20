//
// Created by daiyi on 2019/09/30.
//

#include "stats/stats.h"
#include <x86intrin.h>
#include <cassert>
#include <cmath>
#include <iostream>
#include "perfutil/Cycles.h"

using std::stoull;

namespace adgMod {

Stats* Stats::singleton = nullptr;
bool report_switch = false;

Stats::Stats()
    : timers(20, Timer{}), counters(20, 0), initial_time(__rdtsc()) {}

Stats* Stats::GetInstance() {
  if (!singleton) singleton = new Stats();
  return singleton;
}

void Stats::StartTimer(uint32_t id) {
  Timer& timer = timers[id];
  timer.Start();
}

std::pair<uint64_t, uint64_t> Stats::PauseTimer(uint32_t id, bool record) {
  Timer& timer = timers[id];
  IncrementCounter(id);
  return timer.Pause(record);
}

void Stats::ResetTimer(uint32_t id) {
  Timer& timer = timers[id];
  timer.Reset();
}

uint64_t Stats::ReportTime(uint32_t id) {
  Timer& timer = timers[id];
  return timer.Time();
}

void Stats::ReportTime() {
  if (!report_switch) return;

  for (size_t i = 0; i < timers.size(); ++i) {
    printf("Timer %lu: %lu avgUs:%f\n", i, timers[i].Time(),
           0.001 * float(timers[i].Time()) / counters[i]);
  }

  for (size_t i = 0; i < counters.size(); ++i) {
    printf("Counter %lu: %lu\n", i, counters[i]);
  }
}

void Stats::IncrementCounter(uint32_t id, uint64_t value) {
  counters[id] += value;
}

void Stats::ResetCounter(uint32_t id) { counters[id] = 0; }

uint64_t Stats::GetTime() {
  unsigned int dummy = 0;
  uint64_t time_elapse = __rdtscp(&dummy) - initial_time;
  return PerfUtils::Cycles::toNanoseconds(time_elapse);
}

void Stats::ResetAll() {
  for (Timer& t : timers) t.Reset();
  for (uint64_t& c : counters) c = 0;
  initial_time = __rdtsc();
}

Stats::~Stats() { ReportTime(); }

}  // namespace adgMod
