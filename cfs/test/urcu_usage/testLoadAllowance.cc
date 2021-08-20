#include <unistd.h>
#include <cstdlib>
#include "FsProc_LoadMng.h"
#include "cxxopts.hpp"

// NOTE: this is testing the old version which use liburcu

#if 0
class Reader {
 public:
  Reader(LoadAllowanceContainer *l, int w, std::atomic<int> *rd)
      : lac(l), wid(w), reader_done(rd) {}
  void operator()() {
    fprintf(stderr, "reader started\n");
    // lac->readerRegistration();
    int ret;
    struct PerWorkerLoadAllowance curLa;
    usleep(100000);
    for (unsigned long i = 0; i < (1000000UL); i++) {
      ret = lac->readAllowance(wid, &curLa);
      if (ret >= 0) {
        aggLa += (curLa.cpu_allow);
        numAgg++;
      }
    }
    // lac->readerExit();
    reader_done->fetch_add(1);
    fprintf(stdout, "reader:%d allowAgg:%f numAgg:%lu\n", wid, aggLa, numAgg);
  }

 private:
  LoadAllowanceContainer *lac;
  int wid;
  std::atomic<int> *reader_done;
  double aggLa = 0;
  uint64_t numAgg = 0;
};

class Writer {
 public:
  Writer(LoadAllowanceContainer *l, int nr, std::atomic<int> *rd)
      : lac(l), num_reader(nr), reader_done(rd) {}
  void operator()() {
    std::srand(20201221);
    fprintf(stderr, "writer started reader_done num:%d\n", reader_done->load());
    while (reader_done->load() < num_reader) {
      auto newla = LoadAllowance::getLoadAllowanceInstance(num_reader);
      for (int i = 0; i < num_reader; i++) {
        newla->laArray[i].cpu_allow = (float(std::rand() % 100)) / 100;
      }
      lac->atomicUpdateAllowance(newla);
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    fprintf(stderr, "writer done\n");
  }

 private:
  LoadAllowanceContainer *lac;
  int num_reader;
  std::atomic<int> *reader_done;
};

void benchmark(int nr) {
  LoadAllowanceContainer la_container;
  std::atomic<int> readerDoneCnt(0);
  fprintf(stderr, "la_container done\n");

  Writer w(&la_container, nr, &readerDoneCnt);
  std::thread wt(w);

  std::vector<std::thread *> readers(nr, nullptr);
  for (int i = 0; i < nr; i++) {
    Reader r(&la_container, i, &readerDoneCnt);
    readers[i] = new std::thread(r);
  }

  // done
  for (auto t : readers) {
    if (t->joinable()) {
      t->join();
    }
  }
  if (wt.joinable()) wt.join();
}

#endif

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "benchmark of rcu based load allowance");
  options.add_options()
      // number of reader, we always have one writer
      ("r,nreaders", "number of reader threads", cxxopts::value<int>())
      // Help
      ("h,help", "Print Usage");

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  int nr = 0;
  if (result.count("nreaders")) {
    nr = result["nreaders"].as<int>();
  } else {
    std::cout << options.help() << std::endl;
    exit(1);
  }

  // benchmark(nr);

  return 0;
}