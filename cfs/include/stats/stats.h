//
// Created by daiyi on 2019/09/30.
//

#ifndef LEVELDB_STATS_H
#define LEVELDB_STATS_H

#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <vector>
#include "timer.h"

using std::string;
using std::to_string;

namespace adgMod {

class Timer;
class Stats {
 private:
  static Stats* singleton;
  Stats();

  std::vector<Timer> timers;
  std::vector<uint64_t> counters;

 public:
  uint64_t initial_time;

  static Stats* GetInstance();

  void StartTimer(uint32_t id);
  std::pair<uint64_t, uint64_t> PauseTimer(uint32_t id, bool record = false);
  void ResetTimer(uint32_t id);
  uint64_t ReportTime(uint32_t id);
  void ReportTime();

  void IncrementCounter(uint32_t id, uint64_t value = 1);
  void ResetCounter(uint32_t id);

  uint64_t GetTime();
  void ResetAll();
  ~Stats();
};

}  // namespace adgMod

#endif  // LEVELDB_STATS_H
