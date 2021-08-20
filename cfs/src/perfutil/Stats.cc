
#include "perfutil/Stats.h"
#include "perfutil/Cycles.h"

namespace PerfUtils {

void PerfStats::Reset() {
  hist.Clear();
  numDone = 0;
  firstStartTs = 0;
  lastDoneTs = 0;
}

void PerfStats::FinishSingleOp(uint64_t startTs, uint64_t endTs) {
  uint64_t diff = endTs - startTs;
  // fprintf(stdout, "diff:%lu\n", diff);
  if (timerType == RDTSC) {
    diff = PerfUtils::Cycles::toNanoseconds(diff);
  }
  if (firstStartTs == 0) {
    firstStartTs = startTs;
  }

  hist.Add(diff);

  if ((timerType != GET_TIME_OF_DAY) && diff > (warningSlowOpUsThr * 1000)) {
    fprintf(stderr, "longOp timeNs %lu\n", diff);
  }
  if ((timerType == GET_TIME_OF_DAY) && diff > warningSlowOpUsThr) {
    fprintf(stderr, "longOp timeUs %lu\n", diff);
  }

  numDone++;
  if (numDone == numOpPerReport) {
    lastDoneTs = endTs;
    Report();
    Reset();
  }
}

void PerfStats::Report() {
  if (numDone < 1)
    numDone = 1;
  auto duration = lastDoneTs - firstStartTs;
  double iops;
  iops = numDone / (double(duration) * 1e-6);
  if (timerType != GET_TIME_OF_DAY) {
    iops *= 1000;
    fprintf(logFile, "Nanoseconds per op:\n");
  } else {
    fprintf(logFile, "Microseconds per op:\n");
  }
  fprintf(logFile, "%-12s : %11.3f iops\n", msg.c_str(), iops);
  fprintf(logFile, "%s", hist.ToString().c_str());
  fprintf(logFile, "\n");
  fflush(logFile);
}

} // namespace PerfUtils