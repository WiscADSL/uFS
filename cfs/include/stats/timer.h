//
// Created by daiyi on 2020/02/02.
//

#ifndef LEVELDB_TIMER_H
#define LEVELDB_TIMER_H

#include <cstdint>
#include <ctime>
#include <utility>
#include <vector>

namespace adgMod {

class Timer {
  uint64_t time_started;
  uint64_t time_accumulated;
  bool started;

 public:
  void Start();
  std::pair<uint64_t, uint64_t> Pause(bool record = false);
  void Reset();
  uint64_t Time();

  Timer();
  ~Timer() = default;
};

}  // namespace adgMod

#endif  // LEVELDB_TIMER_H
