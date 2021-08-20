#ifndef CFS_STATS_H
#define CFS_STATS_H

#include <stdint.h>

#include "perfutil/Histogram.h"

namespace PerfUtils {

// Assume the default time granularity of RDTSC and CLOCK_GETTIME is ns,
// while microsecond for GET_TIME_OF_DAY
enum TimerType {
  RDTSC,
  GET_TIME_OF_DAY,
  CLOCK_GETTIME,
};

class PerfStats {
public:
  PerfStats() {
    warningSlowOpUsThr = kSlowOpUsThr;
    numOpPerReport = kNumOpPerReport;
    timerType = kTimerType;
    logFile = stdout;
    msg.clear();
    Reset();
  }

  void OpenLogFile(const char *logName) {
    fprintf(stdout, "openLogFile %s\n", logName);
    if (logFile != stdout) {
      fprintf(stderr, "logFile has already exist\n");
      return;
    }

    logFile = fopen(logName, "a+");
    if (logFile == nullptr) {
      fprintf(stderr, "Error open file\n");
    } else {
      fflush(logFile);
    }
  }

  void Reset();
  // void Merge();
  void Stop();

  void AddMessage(const std::string &str) {
    if (msg.empty()) {
      msg.append(str);
    } else {
      msg.append("-");
      msg.append(str);
    }
  }
  // no matter what this stats is used for, simply pass in the timestamp
  // this timestamp could be rdtsc cycles or microsecond since host started.
  void FinishSingleOp(uint64_t startTs, uint64_t endTs);
  void Report();

  void setTimerType(TimerType tt) { timerType = tt; }
  void setNumOpPerReport(uint64_t num) {
    if (num > 0) {
      numOpPerReport = num;
    }
    fprintf(logFile, "setNumPerReport:%lu\n", num);
  }
  void setSlowOpUs(uint64_t us) { warningSlowOpUsThr = us; }

private:
  constexpr static uint64_t kSlowOpUsThr = 1000000;
  constexpr static uint64_t kNumOpPerReport = 100000;
  // By default, we will use rdtsc, which provides nanosecond precision
  constexpr static TimerType kTimerType = RDTSC;

  // number of op done for a report
  uint64_t numOpPerReport;
  uint64_t numDone;
  uint64_t firstStartTs;
  uint64_t lastDoneTs;
  // time in us that we will output the op's latency since it is too slow
  uint64_t warningSlowOpUsThr;
  TimerType timerType;
  Histogram hist;
  std::string msg;
  FILE *logFile;
};

} // namespace PerfUtils

#endif // CFS_STATS_H
