
#include <stdlib.h>
#include <time.h>

#include <algorithm>
#include <unordered_map>
#include <vector>

#include "perfutil/Cycles.h"
#include "perfutil/Stats.h"
#include "util.h"

#define BILLION 1E9

using PerfUtils::TimerType;

void emulateOpFunc() {
  srand(time(NULL));
  const int num = 10000;
  std::vector<int> data;
  for (int i = 0; i < num; i++) {
    // data.push_back(rand() % 65536 + 1);
    data.push_back(65536 - num);
  }
  std::sort(data.begin(), data.end());
}

static void computeTimespecDiff(struct timespec &start, struct timespec &finish,
                                struct timespec &tmp) {
  if ((finish.tv_sec - start.tv_sec) < 0) {
    tmp.tv_sec = finish.tv_sec - start.tv_sec - 1;
    tmp.tv_nsec = BILLION + finish.tv_nsec - start.tv_nsec;
  } else {
    tmp.tv_sec = finish.tv_sec - start.tv_sec;
    tmp.tv_nsec = finish.tv_nsec - start.tv_nsec;
  }
}

void test1(TimerType tt, const std::string &msg) {
  PerfUtils::PerfStats stats;
  stats.AddMessage(msg);
  stats.setNumOpPerReport(2000);
  stats.setTimerType(tt);
  const int opNum = 10000;
  uint64_t startTs, endTs;
  struct timespec start, finish, tmp;
  for (int i = 0; i < opNum; i++) {
    if (tt == TimerType::RDTSC) {
      startTs = PerfUtils::Cycles::rdtsc();
    } else if (tt == TimerType::GET_TIME_OF_DAY) {
      startTs = tap_ustime();
    } else {
      // clock_gettime(CLOCK_REALTIME, &start);
      clock_gettime(CLOCK_MONOTONIC, &start);
      // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);
      startTs = 0;
    }

    emulateOpFunc();

    if (tt == TimerType::RDTSC) {
      endTs = PerfUtils::Cycles::rdtsc();
    } else if (tt == TimerType::GET_TIME_OF_DAY) {
      endTs = tap_ustime();
    } else {
      // clock_gettime(CLOCK_REALTIME, &finish);
      clock_gettime(CLOCK_MONOTONIC, &finish);
      // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &finish);
      computeTimespecDiff(start, finish, tmp);
      endTs = tmp.tv_sec * BILLION + tmp.tv_nsec;
      // fprintf(stdout, "endTS:%lu sec:%lu start_sec:%lu\n", endTs,
      // finish.tv_sec, start.tv_sec);
    }
    stats.FinishSingleOp(startTs, endTs);
  }
}

void test2(int opNum) {
  fprintf(stdout, "clock_gettime\n");
  struct timespec start, finish, tmp;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (int i = 0; i < opNum; i++) {
    emulateOpFunc();
  }
  clock_gettime(CLOCK_MONOTONIC, &finish);
  computeTimespecDiff(start, finish, tmp);
  fprintf(stdout, " diff s:%f\n", tmp.tv_nsec * 1e-9 + tmp.tv_sec);
}

void test3(int opNum) {
  fprintf(stdout, "rdtsc\n");
  uint64_t start, finish;
  start = PerfUtils::Cycles::rdtsc();
  for (int i = 0; i < opNum; i++) {
    emulateOpFunc();
  }
  finish = PerfUtils::Cycles::rdtsc();
  fprintf(stdout, " diff s:%f\n", PerfUtils::Cycles::toSeconds(finish - start));
}

void test4(int opNum) {
  fprintf(stdout, "get_timeofday\n");
  uint64_t start, finish;
  start = tap_ustime();
  for (int i = 0; i < opNum; i++) {
    emulateOpFunc();
  }
  finish = tap_ustime();
  fprintf(stdout, " diff s:%f\n", (finish - start) * 1e-6);
}

void testMachineTick() {
  fprintf(stdout, "1000 cycle to ns:%lu\n",
          PerfUtils::Cycles::toNanoseconds(1000));
}

int main() {
  test1(TimerType::RDTSC, "rdtsc");
  test1(TimerType::GET_TIME_OF_DAY, "get_timeofday");
  test1(TimerType::CLOCK_GETTIME, "clock_gettime");
  testMachineTick();
  // test2(10000);
  // test3(10000);
  // test4(10000);
  return 0;
}
