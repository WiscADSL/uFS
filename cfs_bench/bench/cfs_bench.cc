#include <fcntl.h>
#include <cstdint>
#include <functional>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "stats/stats.h"
#include "util.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

#include "perfutil/Cycles.h"

#include "FsPageCache_Shared.h"
#include "benchutils/coordinator.h"
#ifdef USE_PCM_COUNTER
#include "cpucounters.h"
using namespace pcm;
#endif

#ifndef CFS_USE_POSIX
#include "fsapi.h"
#else
#include <dirent.h>
#include <fcntl.h> /* Definition of AT_* constants */
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

// Comma separated strings corresponding to benchmark names.
// For example - crread,rwrite,seqwrite
// By default, all benchmarks are run.
static const char *FLAGS_benchmarks = nullptr;

// number of operations to do
static int FLAGS_numop = 1000000;

// number of concurrent threads to run
static int FLAGS_threads = 1;

// core id that the running thread will be pinned to
// valid code id start from 1 to HOST_CORE_NUM
// command line flags will be in the form of string separated by ','
#define MAX_THREAD_NUM 30
static int FLAGS_core_ids[MAX_THREAD_NUM];

// size of each op
static int FLAGS_value_size = 1024;

// If true, will issue io of random size
// For listdir, will randomize the sequence of issue stat()
static bool FLAGS_value_random_size = false;

// If true, file will open with O_APPEND
// be careful to use this with read workload
static bool FLAGS_is_append = false;

// print histogram of operation timings
static bool FLAGS_histogram = false;

// use coordinator or not
static bool FLAGS_coordinator = true;

// number of bytes written to each file (by default ~5G)
static uint64_t FLAGS_max_file_size = ((uint64_t)5 * 1024 * 1000 * 1000);

// number of bytes that is cached in memory
// used for cached workloads (by default ~=128M)
static uint64_t FLAGS_in_mem_file_size = ((uint64_t)128 * 1024 * 1024);

static int FLAGS_num_files = 0;
static int FLAGS_num_hot_files = 0;
// if true, delete the files after finish the bench
static bool FLAGS_pos_delete = false;

// Busy wait Time in nanosec (on average) between each ops
static uint64_t FLAGS_think_nano = 0;

static int FLAGS_wid = -1;
static bool FLAGS_signowait = false;

// For workloads that each app may have several segments, with each of them
// have different think time between each request
// NOTE: it's user's job to guarantee the initialization of data, such that the
// workload can be run for that long time
constexpr static int gMaxRunSegmentCnt = 100;
// How many segments this app is going to run
static int FLAGS_num_segment = 0;
static int64_t FLAGS_per_segment_think_ns_list[gMaxRunSegmentCnt + 1];
// Each segment is by default 1 second, the think time will change after such
// time
static uint64_t FLAGS_segment_us = 1000000;
// This is set to LM's sleep us
static uint64_t FLAGS_segment_bucket_us = 1000;

static int FLAGS_exit_delay_sec = 0;

// if set to false, the stat will create the directory tree. Otherwise, assume
// dir tree existing
static bool FLAGS_stat_nocreate = false;

// Arrange to generate values that shrink to this fraction of
static double FLAGS_compression_ratio = 1;

// If true, will compute the crc for the unit of FLAGS_value_size and output
// to stdout. Used for TEST.
static bool FLAGS_compute_crc = false;

// If all the threads will share one file
static bool FLAGS_share_mode = false;

// For writing, how many number of operations will be followed by a fsync()
static int FLAGS_sync_numop = -1;

// Dedicated access one single block that specified by this flag
// If specified, will overwrite all the other random access patterns
// NOTE: need to specify as random workload to let this one be effectively used
static int64_t FLAGS_block_no = -1;

// If set, will try to make sure that each aligned unit does not overlap
// Basically, it means the file needs to be large enough.
static bool FLAGS_rand_no_overlap = false;

// If set, will warm up the data, such that the operations happen (mostly) in
// memory
static bool FLAGS_warm_up = false;

// If specified, the read/write will be aligned to certain size
// By default, we do not align
static int FLAGS_rw_align_bytes = 0;

// use the root directory with the following name, default is "/"
static const char *FLAGS_dir = "/";
static const char *FLAGS_dir2 = nullptr;

// overwrite the default bench file name
static const char *FLAGS_fname = nullptr;

constexpr static int kMaxNumFile = 10;
constexpr static int kMaxFileNameLen = 64;
static const char *FLAGS_flist = nullptr;

// prefix of a file or directory's name
// E.g., for create()/mkdir(), when different app/thread
// might want to operate on the same directory, then will need
// this prefix
static const char *FLAGS_dirfile_name_prefix = nullptr;

// base to compute shared memory key
// key_t == int
static key_t FLAGS_fs_worker_key_base = 20190301;

// shm key's offset to connect to FS
static int FLAGS_fs_worker_key_offset = -1;

static const char *FLAGS_coordinator_shm_fname = nullptr;

static const uint32_t gBlockSize = 4096;

// when FSP dynamically scale, store the keys to talk to multiple FSP-threads
#define MAX_FSP_THREADS_NUM 20
static const char *FLAGS_fs_worker_key_list = nullptr;
static key_t FLAGS_fs_worker_keys[20];
static int FLAGS_fs_worker_num = 1;

// Will use ZERO-COPY api
constexpr static bool kUseCCacheLease = false;
// The api is exactly the same as posix api
constexpr static bool kUseCCacheLeasePosix = false;

namespace leveldb {

namespace {
leveldb::Env *g_env = nullptr;

static void SpinSleepNano(uint64_t ns) {
  volatile uint64_t cur_ts = 0;
  uint64_t start_ts = g_env->NowCycles();
  volatile int waitNum = 0;
  while (true) {
    cur_ts = g_env->NowCycles();
    waitNum++;
    if (PerfUtils::Cycles::toNanoseconds(cur_ts - start_ts) > ns) {
      break;
    }
  }
  // fprintf(stdout, "waitNum:%d\n", waitNum);
}

struct DynamicSegmentTracker {
  DynamicSegmentTracker(bool think_rand)
      : segment_idx(0), bucket_idx(0), think_random(think_rand) {
    cur_segment_think_ns = FLAGS_per_segment_think_ns_list[segment_idx];
    num_bucket_per_segment = FLAGS_segment_us / FLAGS_segment_bucket_us;
    seg_op_done_per_bucket =
        std::vector<uint64_t>(FLAGS_num_segment * num_bucket_per_segment, 0);
  }
  DynamicSegmentTracker() : DynamicSegmentTracker(true) {}

  int segment_idx;
  int bucket_idx;
  bool think_random = true;
  uint64_t seg_start_ts;
  uint64_t cur_segment_think_ns;
  int num_bucket_per_segment;
  std::vector<uint64_t> seg_op_done_per_bucket;
};

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    leveldb::Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      leveldb::test::CompressibleString(&rnd, FLAGS_compression_ratio, 100,
                                        &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  leveldb::Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return leveldb::Slice(data_.data() + pos_ - len, len);
  }
};

static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit - 1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

static void AppendWithSpace(std::string *str, leveldb::Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

// @return : number of valid item parsed. -1 --> ERROR
template <typename T>
static int ParseDelimittedStrList(char *input, const char *sep, T *dst,
                                  int maxValidItem_num,
                                  T (*fParse)(const char *)) {
  int idx = 0;
  int num_item = 0;
  char *token = strtok(input, sep);
  while (token != nullptr) {
    if (idx >= maxValidItem_num) {
      fprintf(stderr, "ParseDelimittedStrList ERROR: specify too many\n");
      return -1;
    }
    dst[idx++] = fParse(token);
    num_item++;
    token = strtok(nullptr, sep);
  }
  return num_item;
}

#ifdef USE_PCM_COUNTER
using std::cout;
using std::setw;
template <class State>
void print_basic_metrics(const PCM *m, const State &state1,
                         const State &state2) {
  cout << "     " << getExecUsage(state1, state2) << "   "
       << "IPC:" + std::to_string(getIPC(state1, state2)) << "   "
       << getRelativeFrequency(state1, state2);
  if (m->isActiveRelativeFrequencyAvailable())
    cout << "    " << getActiveRelativeFrequency(state1, state2);
  if (m->isL3CacheMissesAvailable())
    cout << "    "
         << "L3miss:" + std::to_string((getL3CacheMisses(state1, state2)));
  // std::string(unit_format(getL3CacheMisses(state1, state2)));
  if (m->isL2CacheMissesAvailable())
    cout << "   " << unit_format(getL2CacheMisses(state1, state2));
  if (m->isL3CacheHitRatioAvailable())
    cout << "    " << getL3CacheHitRatio(state1, state2);
  if (m->isL2CacheHitRatioAvailable())
    cout << "    " << getL2CacheHitRatio(state1, state2);
  if (m->isL3CacheMissesAvailable())
    cout << "    "
         << double(getL3CacheMisses(state1, state2)) /
                getInstructionsRetired(state1, state2);
  if (m->isL2CacheMissesAvailable())
    cout << "    "
         << double(getL2CacheMisses(state1, state2)) /
                getInstructionsRetired(state1, state2);
}

std::string temp_format(int32 t) {
  char buffer[1024];
  if (t == PCM_INVALID_THERMAL_HEADROOM) return "N/A";

  snprintf(buffer, 1024, "%2d", t);
  return buffer;
}

std::string l3cache_occ_format(uint64 o) {
  char buffer[1024];
  if (o == PCM_INVALID_QOS_MONITORING_DATA) return "N/A";

  snprintf(buffer, 1024, "%6d", (uint32)o);
  return buffer;
}

template <class State>
void print_other_metrics(const PCM *m, const State &state1,
                         const State &state2) {
  if (m->L3CacheOccupancyMetricAvailable())
    cout << "   " << setw(6) << l3cache_occ_format(getL3CacheOccupancy(state2));
  if (m->CoreLocalMemoryBWMetricAvailable())
    cout << "   " << setw(6) << getLocalMemoryBW(state1, state2);
  if (m->CoreRemoteMemoryBWMetricAvailable())
    cout << "   " << setw(6) << getRemoteMemoryBW(state1, state2);
  cout << "     " << temp_format(state2.getThermalHeadroom()) << "\n";
}

template <class State>
void print_per_core_metrics(const PCM *m, const State &states1,
                            const State &states2) {
  cout << " Core (SKT) |";
  cout << " EXEC | IPC  | FREQ  |";
  if (m->isActiveRelativeFrequencyAvailable()) cout << " AFREQ |";
  if (m->isL3CacheMissesAvailable()) cout << " L3MISS |";
  if (m->isL2CacheMissesAvailable()) cout << " L2MISS |";
  if (m->isL3CacheHitRatioAvailable()) cout << " L3HIT |";
  if (m->isL2CacheHitRatioAvailable()) cout << " L2HIT |";
  if (m->isL3CacheMissesAvailable()) cout << " L3MPI |";
  if (m->isL2CacheMissesAvailable()) cout << " L2MPI | ";
  if (m->L3CacheOccupancyMetricAvailable()) cout << "  L3OCC |";
  if (m->CoreLocalMemoryBWMetricAvailable()) cout << "   LMB  |";
  if (m->CoreRemoteMemoryBWMetricAvailable()) cout << "   RMB  |";

  cout << " TEMP" << std::endl << std::endl;
  for (uint32 i = 0; i < m->getNumCores(); ++i) {
    if (m->isCoreOnline(i) == false) continue;

    // std::cout << "core:" << i << " :";
    cout << " " << setw(3) << i << "   " << setw(2) << m->getSocketId(i);
    print_basic_metrics(m, states1[i], states2[i]);
    print_other_metrics(m, states1[i], states2[i]);
    // std::cout << "\n";
  }
}
#endif

static std::vector<std::string> splitStr(const std::string &input, char delim) {
  std::stringstream ss(input);
  std::string item;
  std::vector<std::string> elems;
  while (std::getline(ss, item, delim)) {
    elems.push_back(std::move(item));
  }
  return elems;
}

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  uint64_t first_op_nano_;
  uint64_t last_op_nano_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;
  bool stopped_{false};

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    first_op_nano_ = 0;
    last_op_nano_ = 0;
    // start_ = g_env->NowMicros();
    start_ = PerfUtils::Cycles::toNanoseconds(g_env->NowCycles());
    last_op_finish_ = start_;
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats &other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    if (!stopped_) {
      finish_ = PerfUtils::Cycles::toNanoseconds(g_env->NowCycles());
      // seconds_ = (finish_ - start_) * 1e-6;
      seconds_ = (finish_ - start_) * 1e-9;
      stopped_ = true;
      // fprintf(stderr, "finish is set to:%f seconds_:%f\n", finish_,
      // seconds_); fflush(stderr);
    }
  }

  void AddMessage(leveldb::Slice msg) { AppendWithSpace(&message_, msg); }

  void FinishedSingleOp() {
    if (first_op_nano_ == 0) {
      first_op_nano_ = PerfUtils::Cycles::toNanoseconds(g_env->NowCycles());
    }

    if (FLAGS_histogram) {
      // double now = g_env->NowMicros();
      double now = PerfUtils::Cycles::toNanoseconds(g_env->NowCycles());

      // double micros = now - last_op_finish_;
      double micros = double(now - last_op_finish_) / 1000;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const leveldb::Slice &name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;
    last_op_nano_ = (uint64_t)last_op_finish_;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      elapsed /= 1000;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    {  // json stuff
      // TODO: add the stddev?
      float throughput = done_ / seconds_;
      float latency = (seconds_ * 1e6) / done_;
      fprintf(stdout,
              "{\"json_magic\": 1, \"benchmark\": \"%s\", \"num_ops\": %d, "
              "\"microseconds\": %11.3f, \"throughput\": %11.3f, \"latency\": "
              "%11.3f }\n",
              name.ToString().c_str(), done_, seconds_ * 1e6, throughput,
              latency);
    }
    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n", name.ToString().c_str(),
            seconds_ * 1e6 / done_, (extra.empty() ? "" : " "), extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fprintf(stdout, "Verify Ts: start:%lu end:%lu\n", first_op_nano_,
            last_op_nano_);
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv GUARDED_BY(mu);
  int total GUARDED_BY(mu);

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized GUARDED_BY(mu);
  int num_done GUARDED_BY(mu);
  bool start GUARDED_BY(mu);

  SharedState(int total)
      : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;  // 0..n-1 when running in n threads
  int cid;  // logical core id, will ignore if <=0
  int aid;  // app id (currently for dynamic bench)
  int fd;
  std::string path;
  leveldb::Random rand;  // Has different seeds for different threads
  Stats stats;
  SharedState *shared;
  uint32_t *crc_arr;  // array to store the crc value of each unit
  uint64_t fileSize;

  ThreadState(int index, int cid, int aid, Slice p)
      : tid(index),
        cid(cid),
        aid(aid),
        fd(-1),
        path(p.ToString()),
        // rand(1000 + index),
        rand(1000 + index +
             aid),  // for random bench, gen different rand for different ops
        shared(nullptr),
        crc_arr(nullptr),
        fileSize(0) {}

  ~ThreadState() { delete[] crc_arr; }
};

}  // namespace

class Benchmark {
 private:
  int numop_;
  int value_size_;
  bool value_random_size_;
  bool share_one_file_;
  bool compute_crc_;
  bool fs_initialized_;
  Slice dir_;
  std::vector<std::string> fname_vec_;

  typedef void (Benchmark::*BenchmarkMethod)(ThreadState *ts);
  std::unordered_map<std::string, BenchmarkMethod> benchmark_fn_map_;
  std::string all_benchmarks_;

  void PrintHeader() {
    PrintEnvironment();
    fprintf(stdout, "Values:     %d bytes each op)\n", FLAGS_value_size);
    fprintf(stdout, "Entries:    %d\n", numop_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_value_size) * numop_) / 1048576.0));
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintDynamicBuckets(const std::vector<uint64_t> &seg_op_done_per_bucket,
                           int num_bucket_per_seg) {
    for (uint i = 0; i < seg_op_done_per_bucket.size(); i++) {
      int seg_idx = i / num_bucket_per_seg;
      fprintf(stdout,
              "segment_idx:%d segment_us:%lu segment_think_ns:%lu "
              "bucket_ops:%lu req_per_sec:%f\n",
              seg_idx, FLAGS_segment_us,
              FLAGS_per_segment_think_ns_list[seg_idx],
              seg_op_done_per_bucket[i],
              float(seg_op_done_per_bucket[i]) /
                  (0.001 * 0.001 * FLAGS_segment_bucket_us));
    }
  }

  int inline DynamicDoBenchmarkBySegment(
      const std::function<int(void *)> &op_func, void *ctx, ThreadState *thread,
      DynamicSegmentTracker *tk) {
    int ret;
    while (tk->segment_idx < FLAGS_num_segment) {
      if (tk->cur_segment_think_ns > 0) {
        constexpr uint64_t kSegStable1Ms_ns = 1000000;
        if (tk->cur_segment_think_ns < kSegStable1Ms_ns && tk->think_random) {
          SpinSleepNano(thread->rand.Next() % (tk->cur_segment_think_ns));
        } else {
          SpinSleepNano(tk->cur_segment_think_ns);
        }
      }

      // do benchmark
      ret = op_func(ctx);
      if (ret < 0) return -1;

      // if move to next segment
      uint64_t cur_ts = g_env->NowCycles();
      uint64_t cur_start_ts_diff_us =
          PerfUtils::Cycles::toMicroseconds(cur_ts - tk->seg_start_ts);
      tk->bucket_idx = cur_start_ts_diff_us / FLAGS_segment_bucket_us +
                       tk->segment_idx * tk->num_bucket_per_segment;
      if (tk->bucket_idx > tk->seg_op_done_per_bucket.size()) {
        // tk->bucket_idx = tk->seg_op_done_per_bucket.size() - 1;
        break;
      }
      tk->seg_op_done_per_bucket[tk->bucket_idx]++;
      if (cur_start_ts_diff_us > FLAGS_segment_us) {
        tk->cur_segment_think_ns =
            FLAGS_per_segment_think_ns_list[++(tk->segment_idx)];
        tk->seg_start_ts = cur_ts;
      }
    }
    return 0;
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(
        stdout,
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
  }

  void PrintEnvironment() {
    time_t now = time(nullptr);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char *sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
  }

  std::string genBenchFileName(int fid) {
    if (FLAGS_fname != nullptr) {
      return dir_.ToString() + "/" + FLAGS_fname;
    }
    if (share_one_file_) {
      return dir_.ToString() + "/bench_f";
    }
    return dir_.ToString() + "/bench_f_" + std::to_string(fid);
  }

  void registerBenchmarks() {
    // NOTE: here try to keep the name the same as filebench's workloads
    // - "crread": cached random read, first use sequential write to init the
    // file
    //    (include allocate data blocks), then read it within the whole range of
    //    the file randomly. It will use allocted_write and allocated_pread,
    //    which is the fastest option. num_op is used to specify the file length
    //    (num_op * value_size). and max_file_size can be optionally used to
    //    specify a small range to do random read.

    benchmark_fn_map_["ramrand"] = &Benchmark::BenchRamRand;
    benchmark_fn_map_["seqwrite"] = &Benchmark::SeqWrite;
    benchmark_fn_map_["rwrite"] = &Benchmark::RWrite;
    benchmark_fn_map_["crwritecross"] = &Benchmark::CachedRandomWriteCross;
    benchmark_fn_map_["seqread"] = &Benchmark::SeqRead;
    benchmark_fn_map_["rread"] = &Benchmark::RandRead;
    benchmark_fn_map_["crread"] = &Benchmark::CachedRandomRead;
    benchmark_fn_map_["csread"] = &Benchmark::CachedSeqRead;
    benchmark_fn_map_["stat1m"] = &Benchmark::BenchStatOneMissing;
    benchmark_fn_map_["statNm"] = &Benchmark::BenchStatManyMissing;
    benchmark_fn_map_["stat1"] = &Benchmark::BenchStatOne;
    benchmark_fn_map_["stat1_dynamic"] = &Benchmark::BenchStatOneDynamic;
    benchmark_fn_map_["open"] = &Benchmark::BenchOpen;
    benchmark_fn_map_["close"] = &Benchmark::BenchClose;
    benchmark_fn_map_["unlink"] = &Benchmark::BenchUnlink;
    benchmark_fn_map_["moveunlink"] = &Benchmark::BenchMovedUnlink;
    benchmark_fn_map_["create"] = &Benchmark::BenchCreate;
    benchmark_fn_map_["mkdir"] = &Benchmark::BenchMkdir;
    benchmark_fn_map_["listdir"] = &Benchmark::BenchListdir;
    benchmark_fn_map_["opendir"] = &Benchmark::BenchOpendir;
    benchmark_fn_map_["listdirinfo1"] = &Benchmark::BenchListdirInfo1;
    benchmark_fn_map_["listdirinfo2"] = &Benchmark::BenchListdirInfo2;
    benchmark_fn_map_["rmdir"] = &Benchmark::BenchRmdir;
    benchmark_fn_map_["rename"] = &Benchmark::BenchRename;
    benchmark_fn_map_["namegen"] = &Benchmark::BenchNameGeneration;
    benchmark_fn_map_["ping"] = &Benchmark::BenchPingMany;
    benchmark_fn_map_["dynamic_stat1"] = &Benchmark::BenchDynamicStatOne;
    benchmark_fn_map_["dynamic_statall"] = &Benchmark::BenchDynamicStatall;
    benchmark_fn_map_["dynamic_diskread"] = &Benchmark::BenchDynamicDiskRead;
    benchmark_fn_map_["dyn_dreaditer"] = &Benchmark::BenchDynamicDiskReadIter;
    benchmark_fn_map_["dyn_readlb"] = &Benchmark::BenchDynamicReadLb;
    benchmark_fn_map_["dyn_readlbnc"] = &Benchmark::BenchDynamicReadLbNc;
    benchmark_fn_map_["dyn_writelbnc"] = &Benchmark::BenchDynamicWriteLbNc;
    benchmark_fn_map_["dynamic_overwrite"] = &Benchmark::BenchDynamicOverwrite;
    benchmark_fn_map_["dyn_readlbio"] = &Benchmark::BenchDynamicReadLbIo;
    benchmark_fn_map_["dyn_readlbio2"] = &Benchmark::BenchDynamicReadLbIo2;
    benchmark_fn_map_["dyn_read_iomem"] = &Benchmark::BenchDynamicReadIoMem;
    benchmark_fn_map_["dynamic_writesync"] = &Benchmark::BenchDynamicWriteSync;
    benchmark_fn_map_["dynamic_fixsync"] = &Benchmark::BenchDynamicFixSync;
    benchmark_fn_map_["dynamic_create"] = &Benchmark::BenchDynamicCreate;
    benchmark_fn_map_["dynamic_mkdir"] = &Benchmark::BenchDynamicMkdir;
    benchmark_fn_map_["dynamic_unlink"] = &Benchmark::BenchDynamicUnlink;
    benchmark_fn_map_["dynamic_rename"] = &Benchmark::BenchDynamicRename;
    benchmark_fn_map_["dynamic_owsc"] = &Benchmark::BenchDynamicOwsc;
    benchmark_fn_map_["dynamic_readn"] = &Benchmark::BenchDynamicReadN;
    benchmark_fn_map_["dynamic_statn"] = &Benchmark::BenchDynamicStatN;

    // NOTE: the order of names in all_benchmarks_ may not be the same
    // as the order of insertion listed above.
    all_benchmarks_ = "";
    for (auto i : benchmark_fn_map_) {
      all_benchmarks_ += i.first + ",";
    }

    if (all_benchmarks_.size() > 1) {
      all_benchmarks_.erase(all_benchmarks_.size() - 1, 1);
    }
  }

 public:
  Benchmark()
      : numop_(FLAGS_numop),
        value_size_(FLAGS_value_size),
        value_random_size_(FLAGS_value_random_size),
        share_one_file_(FLAGS_share_mode),
        compute_crc_(FLAGS_compute_crc),
        fs_initialized_(false),
        dir_(FLAGS_dir) {
    if (FLAGS_flist != nullptr) {
      char fname_list_str[kMaxFileNameLen * kMaxNumFile];
      memset(fname_list_str, 0, kMaxFileNameLen * kMaxNumFile);
      strcpy(fname_list_str, FLAGS_flist);
      int idx = 0;
      int num_item = 0;
      const char *sep = ",";
      char *token = strtok(fname_list_str, sep);
      while (token != nullptr) {
        if (idx >= kMaxNumFile) {
          fprintf(stderr, "ParseDelimittedStrList ERROR: specify too many\n");
          exit(1);
        }
        // fname_vec_[idx++] = std::string(token);
        fname_vec_.push_back(std::string(token));
        num_item++;
        token = strtok(nullptr, sep);
      }
      fprintf(stdout, "num_file:%d dir:%s\n", num_item,
              dir_.ToString().c_str());
      // for (int i = 0; i < num_item; i++) {
      //   fprintf(stdout, "fname-%d:%s\n", i, fname_vec_[i].c_str());
      // }
    }
    registerBenchmarks();
  }

  void Run() {
    const char *benchmarks = FLAGS_benchmarks;
    if (benchmarks == nullptr) {
      benchmarks = Benchmark::all_benchmarks_.c_str();
    }

    PrintHeader();
    ConnectFS();

    while (benchmarks != nullptr) {
      const char *sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == nullptr) {
        name = benchmarks;
        benchmarks = nullptr;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      void (Benchmark::*method)(ThreadState *) = nullptr;
      int num_threads = FLAGS_threads;

      auto fn_lookup = Benchmark::benchmark_fn_map_.find(name.ToString());
      if (fn_lookup == Benchmark::benchmark_fn_map_.end()) {
        fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        continue;
      } else {
        method = fn_lookup->second;
      }

      if (method != nullptr) {
        RunBenchmark(num_threads, FLAGS_core_ids, name, method);
      }
    }

    ExitFs();
  }

 private:
  struct ThreadArg {
    Benchmark *bm;
    SharedState *shared;
    ThreadState *thread;
    void (Benchmark::*method)(ThreadState *);
  };

  static void ThreadBody(void *v) {
    ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
    SharedState *shared = arg->shared;
    ThreadState *thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, const int *cpu_ids, Slice name,
                    void (Benchmark::*method)(ThreadState *)) {
    SharedState shared(n);

    ThreadArg *arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      auto curFileName = genBenchFileName(i);
      int cur_aid = FLAGS_fs_worker_keys[0] - FLAGS_fs_worker_key_base - 1;
      if (cur_aid < 0) {
        // TODO: this is a hack to get the aid for ext4, 10 means # of workers
        // for FSP
        cur_aid = cpu_ids[i] % 10 - 1;
        fprintf(stderr, "cid:%d aid:%d\n", cpu_ids[i], cur_aid);
      }
      arg[i].thread = new ThreadState(i, cpu_ids[i], cur_aid, curFileName);
      arg[i].thread->shared = &shared;
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    if (compute_crc_) {
      for (int i = 0; i < n; i++) {
        OutputCrcArr(arg[i].thread);
      }
    }

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }

    delete[] arg;
  }

  void initFsThreadLocal() {
#ifndef CFS_USE_POSIX
    volatile void *ptr = fs_malloc(1024);
    memset((void *)ptr, 'a', 1024);
    fs_free((void *)ptr);
#endif
  }

  void ConnectFS() {
#ifndef CFS_USE_POSIX
    assert(!fs_initialized_);
    int rt;
    if (FLAGS_fs_worker_key_list != nullptr || FLAGS_fs_worker_num > 1) {
#ifdef UFS_SOCK_LISTEN
      rt = fs_register();
#else
      rt = fs_init_multi(FLAGS_fs_worker_num, FLAGS_fs_worker_keys);
#endif
    } else {
      if (FLAGS_fs_worker_key_offset < 0) {
        fprintf(stderr,
                "WARNING: use single-thread FSP, but key_offset not "
                "set, set to 1 instead\n");
        // NOTE, here this is for backward compatibility, thus the old expr.
        // script can be run
        FLAGS_fs_worker_key_offset = 1;
      }
#ifdef UFS_SOCK_LISTEN
      rt = fs_register();
#else
      rt = fs_init(FLAGS_fs_worker_key_base + FLAGS_fs_worker_key_offset);
#endif
    }
    if (rt < 0) {
      fprintf(stderr, "fs_init() error\n");
      exit(1);
    }
#endif
  }

  void ExitFs() {
#ifndef CFS_USE_POSIX
    fs_init_thread_local_mem();
    int rt = fs_exit();
    if (rt < 0) {
      fprintf(stderr, "fs_exit() error");
    }
#endif
  }

  void PinToCore(ThreadState *thread) {
    fprintf(stdout, "pinToCore cid:%d\n", thread->cid);
    if (thread->cid <= 0) return;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread->cid - 1, &cpuset);
    int s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (s != 0) {
      fprintf(stderr, "thread-%d cannot pin to core-%d\n", thread->tid,
              thread->cid);
    }
  }

  void FlushAllData(ThreadState *thread) {
#ifndef CFS_USE_POSIX
    fprintf(stderr, "syncall aid:%d\n", thread->aid);
    fs_syncall();
    fprintf(stderr, "syncall done. aid:%d\n", thread->aid);
#endif
  }

  void SignalStartDumpLoad(ThreadState *thread) {
#ifndef CFS_USE_POSIX
#ifdef DO_DUMPLOAD
    fprintf(stdout, "signal start\n");
    if (thread->aid == 0 && thread->tid == 0) {
      if (!FLAGS_signowait) {
        SpinSleepNano(5 * 1e9);
      }
      // NOTE this sleep(5) will influence a lot of the performance
      // results in some weird starting slow down, even without
      // any inode assignment
      // sleep(5);
      fs_start_dump_load_stats();
    }
    fprintf(stderr, "signal start aid:%d\n", thread->aid);
#endif
#endif
  }

  void SignalStopDumpLoad(ThreadState *thread) {
#ifndef CFS_USE_POSIX
#ifdef DO_DUMPLOAD
    fprintf(stderr, "signal stop aid:%d\n", thread->aid);
    if (thread->aid == 0 && thread->tid == 0) {
      fs_stop_dump_load_stats();
    }
#endif
#endif
  }

  void SeqWrite(ThreadState *thread) { DoWrite(thread, true); }

  void RWrite(ThreadState *thread) { DoWrite(thread, false); }

  void DoWrite(ThreadState *thread, bool seq) {
    PinToCore(thread);
#ifndef CFS_USE_POSIX
    fprintf(stderr, "init tid aid:%d\n", thread->aid);
    fs_init_thread_local_mem();
#endif
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    if (FLAGS_sync_numop > 0) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops then sync)", FLAGS_sync_numop);
      thread->stats.AddMessage(msg);
    }

    if (compute_crc_) {
      thread->crc_arr = new uint32_t[numop_];
    }

    RandomGenerator gen;
    int64_t bytes = 0;
#ifndef CFS_USE_POSIX
    char *wdata = (char *)fs_zalloc(value_size_);
#else
    char *wdata = (char *)malloc(value_size_);
#endif
    if (thread->aid != 0) {
      sleep(5);
    }
    asm volatile("" ::: "memory");
    OpenFile(thread);

    // once random write, if max_file_size is assigned by cmd argument
    // assume will be *cached random write*
    uint64_t randWriteRange = std::min(FLAGS_max_file_size, thread->fileSize);

    if (thread->aid == 0) {
      // sleep(1);
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    SignalStartDumpLoad(thread);
    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    int curSyncCounter = 0;
    for (int i = 0; i < numop_; i++) {
      int cur_value_size = value_size_;
      if (value_random_size_) {
        cur_value_size = thread->rand.Next() % value_size_;
      }

      auto slice = gen.Generate(cur_value_size);
      memcpy(wdata, slice.data(), value_size_);

      if (compute_crc_) {
        // assert(slice.size() == value_size_);
        thread->crc_arr[i] = crc32c::Value(slice.data(), cur_value_size);
      }

      int rc;
      off_t cur_off = 0;

      checkThinkTime(thread->rand);

      if (seq) {
#ifndef CFS_USE_POSIX
        // rc = fs_write(thread->fd, slice.data(), cur_value_size);
        rc = fs_allocated_write(thread->fd, wdata, cur_value_size);
#else
        rc = write(thread->fd, wdata, cur_value_size);
#endif
      } else {
        cur_off = thread->rand.Next() % randWriteRange;
        if (FLAGS_rw_align_bytes != 0) {
          cur_off = (cur_off / FLAGS_rw_align_bytes) * FLAGS_rw_align_bytes;
        }
        if (FLAGS_block_no >= 0) {
          cur_off = FLAGS_block_no * gBlockSize;
        }
#ifndef CFS_USE_POSIX
        // rc = fs_pwrite(thread->fd, slice.data(), cur_value_size, cur_off);
        rc = fs_allocated_pwrite(thread->fd, wdata, cur_value_size, cur_off);
#else
        // fprintf(stdout, "cur_size:%d, cur_off:%ld\n", cur_value_size,
        // cur_off);
        // rc = pwrite(thread->fd, slice.data(), cur_value_size, cur_off);
        rc = pwrite(thread->fd, wdata, cur_value_size, cur_off);
#endif
      }

      if (rc != cur_value_size) {
        fprintf(stderr, "fs_allocated_write() error return:%d idx:%d\n", rc, i);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      curSyncCounter++;
      if (curSyncCounter == FLAGS_sync_numop) {
        int srt;
#ifndef CFS_USE_POSIX
        srt = fs_fdatasync(thread->fd);
#else
        srt = fdatasync(thread->fd);
#endif
        if (srt < 0) {
          fprintf(stderr, "fdatasync error:%s i:%d srt:%d\n", strerror(errno),
                  i, srt);
          exit(1);
        }
        curSyncCounter = 0;
      }
      // accounting
      bytes += cur_value_size;
      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }

    cc.notify_server_that_client_stopped();
    thread->stats.AddBytes(bytes);
    thread->stats.Stop();
    fprintf(stderr, "finish all ops\n");

    SignalStopDumpLoad(thread);

#ifndef CFS_USE_POSIX
    fs_free(wdata);
#else
    free(wdata);
#endif
    CloseFile(thread);
    fprintf(stderr, "file closed\n");
    FlushAllData(thread);
  }

  // cross-file cached random write
  void CachedRandomWriteCross(ThreadState *thread) {
    assert(FLAGS_flist != nullptr);
    assert(FLAGS_block_no >= 0);
    PinToCore(thread);

#ifndef CFS_USE_POSIX
    char *wdata = (char *)fs_malloc(value_size_);
#else
    char *wdata = (char *)malloc(value_size_);
#endif
    // make sure mem is initialied to the server
    sleep(1);

    std::vector<int> fd_vec;
    char cur_path[128];
    for (int i = 0; i < fname_vec_.size(); i++) {
      // if (i != (FLAGS_block_no % 10)) continue;
      memset(cur_path, 0, 128);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      //  fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);
    }

    std::random_shuffle(fd_vec.begin(), fd_vec.end());
    off_t cur_off = ((off_t)FLAGS_block_no) * value_size_;
    uint64_t total_done_req_num = 0;
    int fd_idx = 0;
    int64_t bytes;
    int rc;

    // start coordinator
    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();
    // start stats()
    thread->stats.Start();

    while (true) {
      memset(wdata, 'a' + total_done_req_num % 20, value_size_);
#ifndef CFS_USE_POSIX
      rc = fs_allocated_pwrite(fd_vec[fd_idx], wdata, value_size_, cur_off);
#else
      rc = pwrite(fd_vec[fd_idx], wdata, value_size_, cur_off);
#endif
      if (rc != value_size_) {
        fprintf(stderr, "CRWCross write error\n");
        exit(1);
      }
      fd_idx = (fd_idx + 1) % fd_vec.size();
      total_done_req_num++;
      if (total_done_req_num >= numop_) {
        break;
      }
      thread->stats.FinishedSingleOp();
      bytes += gBlockSize;
      if (cc.check_server_said_stop()) {
        break;
      }
    }

    cc.notify_server_that_client_stopped();

    thread->stats.AddBytes(bytes);
    thread->stats.Stop();

    int rt, rt1;
    for (auto fd : fd_vec) {
#ifndef CFS_USE_POSIX
      rt1 = fs_close(fd);
#else
      rt1 = close(fd);
#endif
    }
#ifndef CFS_USE_POSIX
    fs_free(wdata);
#else
    free(wdata);
#endif
  }

  void CachedRandomRead(ThreadState *thread) { DoCachedRead(thread, false); }

  void CachedSeqRead(ThreadState *thread) { DoCachedRead(thread, true); }

  void DoCachedRead(ThreadState *thread, bool is_seq) {
    PinToCore(thread);

    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;

    // first do sequential read to warm up the FSP/KFS's cache
    // we do not count the time for this sequential read
    // NOTE. user can control this size of cache via *FLAGS_in_mem_file_size*
    uint kWarmUpAlignSize = 4096;
    uint curWriteOpNum = FLAGS_in_mem_file_size / kWarmUpAlignSize;

    if (FLAGS_in_mem_file_size > FLAGS_max_file_size) {
      throw std::runtime_error("workring set size too large\n");
    }
    int cur_value_size = kWarmUpAlignSize;

#ifndef CFS_USE_POSIX
    char *wmrdata = (char *)fs_malloc_pad(cur_value_size);
    char *rdata = (char *)fs_malloc_pad(value_size_);
    // char *wmrdata = (char*)malloc(value_size_);
    // char *rdata = (char*)malloc(value_size_);
    void *retDataPtr = nullptr;
    char *rdata_posix = (char *)malloc(value_size_);
#else
    char *wmrdata = (char *)malloc(cur_value_size);
    char *rdata = (char *)malloc(value_size_);
#endif
    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stdout, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    OpenFile(thread);
    for (int i = 0; i < curWriteOpNum; i++) {
      int rc;
#ifndef CFS_USE_POSIX
      if (kUseCCacheLease) {
        retDataPtr = nullptr;
        rc = fs_cached_pread(thread->fd, &retDataPtr, cur_value_size,
                             cur_value_size * i);
      } else {
        // rc = fs_allocated_pread(thread->fd, wmrdata, cur_value_size,
        //                        cur_value_size * i);
        rc = fs_cpc_pread(thread->fd, wmrdata, cur_value_size,
                          cur_value_size * i);
        // rc = fs_uc_pread(thread->fd, wmrdata, cur_value_size,
        //                  cur_value_size * i);
      }
#else
      rc = read(thread->fd, wmrdata, cur_value_size);
#endif
      if (rc != cur_value_size) {
        fprintf(stdout, "CachedRandomRead->warmup read error return:%d i:%d\n",
                rc, i);
        break;
      }
    }

    //
    // do random read for the cached data
    // NOTE here: *rw_align_bytes* does not has effect
    //
    int64_t bytes = 0;

    fprintf(stdout, "warmup done\n");
#ifdef USE_PCM_COUNTER
    PCM *m = nullptr;
    // const auto cpu_model = m->getCPUModel();
    SystemCounterState sstate1, sstate2;
    std::vector<CoreCounterState> cstates1, cstates2;
    std::vector<SocketCounterState> sktstate1, sktstate2;
    if (thread->aid == 0) {
      m = PCM::getInstance();
      if (m->program() != PCM::Success) {
        fprintf(stderr, "CANNOT user pcm\n");
        return;
      }
      m->getAllCounterStates(sstate1, sktstate1, cstates1);
    }
#endif

    asm volatile("" ::: "memory");
    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    off_t cur_off = 0;
    for (int i = 0; i < numop_; i++) {
      int rc;
      if (!is_seq) {
        // cached random read
        cur_off = thread->rand.Next() % FLAGS_in_mem_file_size;
        if (FLAGS_rw_align_bytes != 0) {
          cur_off = (cur_off / FLAGS_rw_align_bytes) * FLAGS_rw_align_bytes;
        }
        if (FLAGS_block_no >= 0) {
          cur_off = FLAGS_block_no * gBlockSize;
        }
      } else {
        // cached sequential read
        cur_off += value_size_;
        if (cur_off >= FLAGS_in_mem_file_size) {
          cur_off = 0;
        }
      }
#ifndef CFS_USE_POSIX
      if (kUseCCacheLease) {
        if (!kUseCCacheLeasePosix) {
          retDataPtr = nullptr;
          rc = fs_cached_pread(thread->fd, &retDataPtr, value_size_, cur_off);
        } else {
          rc = fs_cached_posix_pread(thread->fd, rdata_posix, value_size_,
                                     cur_off);
        }
      } else {
        // rc = fs_allocated_pread(thread->fd, rdata, value_size_, cur_off);
        rc = fs_cpc_pread(thread->fd, rdata, value_size_, cur_off);
        // rc = fs_uc_pread(thread->fd, rdata, value_size_, cur_off);
        // dump_pread_result(rdata, "benchf", thread->fd, cur_off, value_size_,
        // rc);
      }
#else
      rc = pread(thread->fd, rdata, value_size_, cur_off);
#endif
      if (rc != value_size_) {
#ifndef CFS_USE_POSIX
        fprintf(stderr,
                "CachedRandomRead->fs_pread error rc:%d value_size_:%d\n", rc,
                value_size_);
#else
        fprintf(stderr, "pread() errno: %s\n", strerror(errno));
#endif
      }
      thread->stats.FinishedSingleOp();
      bytes += value_size_;

      if (cc.check_server_said_stop()) break;
    }

    cc.notify_server_that_client_stopped();
#ifdef USE_PCM_COUNTER
    using std::setw;
    if (thread->aid == 0) {
      m->getAllCounterStates(sstate2, sktstate2, cstates2);
      print_per_core_metrics(m, cstates1, cstates2);
      asm volatile("" ::: "memory");
      m->cleanup();
    }
#endif

#ifndef CFS_USE_POSIX
    fs_free_pad(wmrdata);
    fs_free_pad(rdata);
    free(rdata_posix);
#else
    free(wmrdata);
    free(rdata);
#endif
    thread->stats.AddBytes(bytes);
    thread->stats.Stop();
    CloseFile(thread);
  }

  void SeqRead(ThreadState *thread) { DoRead(thread, true); }

  void RandRead(ThreadState *thread) { DoRead(thread, false); }

  void DoRead(ThreadState *thread, bool seq) {
    PinToCore(thread);
    int64_t bytes = 0;
    const int kRaNumBlock = 1;
#ifndef CFS_USE_POSIX
    char *rdata = (char *)fs_malloc(kRaNumBlock * value_size_);
#else
    char rdata[value_size_];
#endif
    memset(rdata, 0, value_size_);
    OpenFile(thread);
    if (compute_crc_) {
      thread->crc_arr = new uint32_t[numop_];
    }
    uint64_t max_req_num;
    if (FLAGS_rand_no_overlap) {
      // this benchmark would like to make sure no IO is overlap
      if (FLAGS_rand_no_overlap && FLAGS_rw_align_bytes == 0) {
        fprintf(stderr,
                "random_no_overalap=1 => rw_align_bytes should not be"
                "0\n");
        exit(1);
      }
      max_req_num = FLAGS_max_file_size / FLAGS_rw_align_bytes;
      if (numop_ > max_req_num) {
        fprintf(stderr,
                "file size not enough to complete the aligned req max:%lu\n",
                max_req_num);
        exit(1);
      }
    } else {
      // it is okay to overlap
      max_req_num = numop_;
    }
    fprintf(stdout, "FLAGS_rw_align_bytes:%d max_req_num:%lu\n",
            FLAGS_rw_align_bytes, max_req_num);
    std::vector<off_t> off_vec;
    // prepare offset here
    if (FLAGS_rw_align_bytes != 0) {
      off_vec.resize(max_req_num);
      for (uint64_t i = 0; i < max_req_num; i++) {
        off_vec[i] = (FLAGS_rw_align_bytes * i) % FLAGS_max_file_size;
      }
      std::random_shuffle(off_vec.begin(), off_vec.end());
    }

    const bool kPrintOffsetVerify = false;
    if (kPrintOffsetVerify) {
      fprintf(stdout, "=>FLAGS_rw_align_bytes:%d\n", FLAGS_rw_align_bytes);
      for (auto off : off_vec) {
        fprintf(stdout, "offset:%lu aligned_to_unit:%f\n", off,
                float(off) / FLAGS_rw_align_bytes);
      }
      // if any duplicate value
      std::vector<off_t> tmp_vec{off_vec};
      auto it = std::unique(tmp_vec.begin(), tmp_vec.end());
      bool hasDup = !(it == tmp_vec.end());
      fprintf(stdout, "=> Has duplicate offset? %d\n", hasDup);
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stdout, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    fprintf(stderr, "aid:%d\n", thread->aid);
    SignalStartDumpLoad(thread);
    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    // start the stats collection
    // NOTE: do not do anything slow down performance seriously afterwards
    thread->stats.Start();

    for (int i = 0; i < numop_; i++) {
      int rc;
      off_t cur_off = -1;
      checkThinkTime(thread->rand);
      if (seq) {
        // sequential read
#ifndef CFS_USE_POSIX
        rc = fs_allocated_read(thread->fd, rdata, value_size_);
        // fprintf(stdout, "seqread i:%d firstChar:%c\n", i, rdata[0]);
#else
        rc = read(thread->fd, rdata, value_size_);
#endif
      } else {
        // random read
        if (FLAGS_rw_align_bytes != 0) {
          cur_off = off_vec[i];
        } else {
          // no alignment requirement, random choose
          cur_off = thread->rand.Next() % FLAGS_max_file_size;
        }
        if (FLAGS_block_no >= 0) {
          cur_off = FLAGS_numop * gBlockSize;
        }

#ifndef CFS_USE_POSIX
        rc = fs_allocated_pread(thread->fd, rdata, value_size_, cur_off);
#else
        rc = pread(thread->fd, rdata, value_size_, cur_off);
#endif
      }

      if (rc != value_size_) {
        fprintf(stderr,
                "ERROR:fs_allocated_[p]read. rc:%d value_size_:%d "
                "offset:%lu opIdx(i):%d\n",
                rc, value_size_, cur_off, i);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      // verify correctness through crc
      if (compute_crc_) {
        thread->crc_arr[i] = crc32c::Value(rdata, value_size_);
      }

      // do accounting
      thread->stats.FinishedSingleOp();
      bytes += value_size_;
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();

#ifndef CFS_USE_POSIX
    fs_free(rdata);
#endif

    thread->stats.AddBytes(bytes);
    thread->stats.Stop();
    SignalStopDumpLoad(thread);

    // TODO (jingliu) close file?
  }

  void OpenFile(ThreadState *thread) {
    struct stat statbuf;
    int fd;
    if (FLAGS_share_mode && FLAGS_fs_worker_keys[0] != 1) {
      // we sleep to make sure if file is shared, only one file is created
      sleep(1);
    }
#ifndef CFS_USE_POSIX
    int rc = fs_stat(thread->path.data(), &statbuf);
#else
    int rc = ::stat(thread->path.data(), &statbuf);
#endif  // CFS_USE_POSIX
    int cur_flags;
    if (rc == 0) {
      cur_flags = O_RDWR;
      thread->fileSize = statbuf.st_size;
      fprintf(stdout, "open with O_RDWR aid:%d\n", thread->aid);
    } else {
      cur_flags = O_CREAT | O_RDWR;
      fprintf(stdout, "open with O_CREAT aid:%d\n", thread->aid);
    }
    if (FLAGS_is_append) {
      fprintf(stdout, "open with O_APPEND aid:%d\n", thread->aid);
      cur_flags |= O_APPEND;
    }
#ifndef CFS_USE_POSIX
    fd = fs_open(thread->path.data(), cur_flags, 0644);
#else
    fd = open(thread->path.data(), cur_flags, 0644);
#endif
    if (fd < 0) {
      fprintf(stderr, "fs_open(%s) error, file cannot be created/opened\n",
              thread->path.data());
      exit(1);
    }
    assert(thread->fd < 0);
    thread->fd = fd;
  }

  // Repeatedly stat a particular file that does not exist.
  void BenchStatOneMissing(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    struct stat statbuf;
    const char *path;

    path = (thread->path + std::string("shouldNotExist")).data();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
#ifndef CFS_USE_POSIX
      rc = fs_stat(path, &statbuf);
#else
      rc = ::stat(path, &statbuf);
#endif

      if (rc == 0) {
        fprintf(stderr,
                "fs_stat succeeded, expected it to fail\n");  // FIXME : stat
                                                              // does not behave
                                                              // as expected
        cc.notify_server_that_client_stopped();
        exit(1);
      }
      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
  }

  void BenchPingMany(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
#ifndef CFS_USE_POSIX
      rc = fs_ping();
#else
//    rc = fs_ping();
#endif
      if (rc != 0) {
        fprintf(stderr, "fs_ping failed\n");
        cc.notify_server_that_client_stopped();
        exit(1);
      }
      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
  }

  // In each iteration, stat a file that does not exist.
  void BenchStatManyMissing(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    struct stat statbuf;
    const char *path;
    std::string basepath = thread->path + "shouldNotExist_";

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      // TODO filename construction should not be a part of the microbenchmark
      // move it outside? Or have a separate function that only microbenchmarks
      // this and deduct that timing.
      path = (basepath + std::to_string(i)).data();

#ifndef CFS_USE_POSIX
      rc = fs_stat(path, &statbuf);
#else
      rc = ::stat(path, &statbuf);
#endif

      if (rc == 0) {
        fprintf(stderr, "fs_stat succeeded, expected it to fail\n");
        cc.notify_server_that_client_stopped();
        exit(1);
      }
      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
  }

  // Stat a single existing file.
  // NOTE: to specify the argument for using this *stat1* expr, needs to only
  // relies on `--dir=` flag, that is, it needs to have the final file name
  // E.g., `--dir=a/b/c/d/DIR/f0` is going to create all the directory hierarchy
  // and
  //   then create the file *f0* in directory */ROOT/a/b/c/d/DIR/*
  //   In this case, the target of subsequent *stat()* is *f0*
  void BenchStatOne(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    struct stat statbuf;
    std::string basepath = dir_.ToString();
    // fprintf(stdout, "basepath:%s\n", basepath.c_str());
    const char *path = basepath.c_str();
#ifndef CFS_USE_POSIX
    void *ptr = fs_malloc(1024);
    memset(ptr, 'a', 1024);
    fs_free(ptr);
#endif

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stdout, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    if (!FLAGS_stat_nocreate) {
      // first create target direcoty
      auto pathTokenVec = splitStr(basepath, '/');
      std::string curPath("/");
      for (int i = 0; i < pathTokenVec.size(); i++) {
        // fprintf(stdout, "curPath:%s\n", curPath.c_str());
#ifndef CFS_USE_POSIX
        fs_mkdir(curPath.c_str(), 0);
#else
        ::mkdir(curPath.c_str(), 0);
#endif
        curPath += '/';
        curPath += pathTokenVec[i];
      }

      // create target file
      int fd;
      // fprintf(stdout, "create path:%s\n", path);
#ifndef CFS_USE_POSIX
      fd = fs_open(path, O_CREAT | O_RDWR, 0644);
      fs_close(fd);
      fs_free(ptr);
#else
      fd = ::open(path, O_CREAT | O_RDWR, 0644);
      close(fd);
#endif
    }

    SignalStartDumpLoad(thread);

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    if (!FLAGS_coordinator) {
      cc.notify_server_that_client_stopped();
    }
    for (int i = 0; i < numop_; i++) {
      checkThinkTime(thread->rand);
#ifndef CFS_USE_POSIX
      rc = fs_stat(path, &statbuf);
#else
      rc = ::stat(path, &statbuf);
#endif
      if (rc != 0) {
        fprintf(stderr, "fs_stat: failed to stat %s\n", path);
        cc.notify_server_that_client_stopped();
        exit(1);
      }
      thread->stats.FinishedSingleOp();
      if (FLAGS_coordinator && cc.check_server_said_stop()) break;
    }
    fprintf(stderr, "finish all aid:%d \n", thread->aid);
    if (FLAGS_coordinator) cc.notify_server_that_client_stopped();
    thread->stats.Stop();
    SignalStopDumpLoad(thread);
    if (!FLAGS_stat_nocreate) {
      FlushAllData(thread);
    }
  }

  // Bench for dynamic mechanism
  void BenchStatOneDynamic(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    struct stat statbuf;
    std::string basepath = dir_.ToString();
    // fprintf(stdout, "basepath:%s\n", basepath.c_str());
    const char *path = basepath.c_str();

    if (!FLAGS_stat_nocreate) {
      // first create target direcoty
      auto pathTokenVec = splitStr(basepath, '/');
      std::string curPath("/");
      for (int i = 0; i < pathTokenVec.size(); i++) {
        // fprintf(stdout, "curPath:%s\n", curPath.c_str());
#ifndef CFS_USE_POSIX
        fs_mkdir(curPath.c_str(), 0);
#else
        ::mkdir(curPath.c_str(), 0);
#endif
        curPath += '/';
        curPath += pathTokenVec[i];
      }

      // create target file
      int fd;
      // fprintf(stdout, "create path:%s\n", path);
#ifndef CFS_USE_POSIX
      fd = fs_open(path, O_CREAT | O_RDWR, 0644);
      fs_close(fd);
#else
      fd = ::open(path, O_CREAT | O_RDWR, 0644);
      close(fd);
#endif
    }

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    const int total_runtime = 10000000;
    int64_t initial_time = tap_ustime();
    int64_t workload_start_time =
        thread->aid * total_runtime / 10 + initial_time;
    int64_t workload_end_time = workload_start_time + total_runtime;

    // wait for its time to run
    while (tap_ustime() < workload_start_time)
      ;

    thread->stats.Start();
    int64_t current_time = tap_ustime();
    std::vector<uint64_t> num_ops_stat(total_runtime * 2 / 1000, 0);
    for (int64_t current_time = tap_ustime(); current_time < workload_end_time;
         current_time = tap_ustime()) {
#ifndef CFS_USE_POSIX
      rc = fs_stat(path, &statbuf);
#else
      rc = ::stat(path, &statbuf);
#endif
      if (rc != 0) {
        fprintf(stderr, "fs_stat: failed to stat %s\n", path);
        cc.notify_server_that_client_stopped();
        exit(1);
      }
      thread->stats.FinishedSingleOp();
      num_ops_stat[(current_time - initial_time) / 1000] += 1;

      // always run to finish
      // if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();

    for (size_t i = 0; i < num_ops_stat.size(); ++i) {
      fprintf(stdout, "TimeInteval %ld %ld : %lu ops\n", i * 1000,
              (i + 1) * 1000, num_ops_stat[i]);
    }
  }

  void DoOpenOrClose(ThreadState *thread, bool isClose) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";

#ifndef CFS_USE_POSIX
    struct CFS_DIR *dentryPtr = nullptr;
    void *initShmPtr = fs_malloc(4096);
#else
    DIR *dentryPtr = nullptr;
#endif

    std::vector<std::string> dnames;

#ifndef CFS_USE_POSIX
    dentryPtr = fs_opendir(basepath.c_str());
#else
    dentryPtr = opendir(basepath.c_str());
#endif
    if (dentryPtr == nullptr) {
      fprintf(stderr, "Failed to opendir\n");
      return;
    }

    while (true) {
      struct dirent *dp;
#ifndef CFS_USE_POSIX
      dp = fs_readdir(dentryPtr);
#else
      dp = readdir(dentryPtr);
#endif
      if (dp) {
        if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0) {
          dnames.push_back(dp->d_name);
        }
      } else {
        break;
      }
    }

    fprintf(stdout, "dirwidth:%ld\n", dnames.size());

    if (FLAGS_value_random_size) {
      fprintf(stdout, "filename randomized\n");
      std::random_shuffle(dnames.begin(), dnames.end());
    }

    // warmup the open()
    if (numop_ > 1) {
      for (size_t i = 0; i < dnames.size(); ++i) {
        int rc = 0;
        std::string stat_path = basepath + dnames[i];
#ifndef CFS_USE_POSIX
        int fd = fs_open(stat_path.c_str(), O_RDWR, 0644);
#else
        int fd = open(stat_path.c_str(), O_RDWR, 0644);
#endif
        if (fd < 0) {
          fprintf(stderr, "Failed to open%s\n", stat_path.c_str());
          exit(1);
        }
      }
    }

    std::vector<int> fds;
    fds.reserve(dnames.size());

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    if (!isClose) {
      // Open() benchmark
      cc.notify_server_that_client_is_ready();
      cc.wait_till_server_says_start();
      thread->stats.Start();
    }

    // iterate the whole directory
    for (size_t i = 0; i < dnames.size(); ++i) {
      int rc = 0;
      std::string stat_path = basepath + dnames[i];
#ifndef CFS_USE_POSIX
      int fd = fs_open(stat_path.c_str(), O_RDWR, 0644);
#else
      int fd = open(stat_path.c_str(), O_RDWR, 0644);
#endif
      if (fd < 0) {
        fprintf(stderr, "Failed to open%s\n", stat_path.c_str());
        if (!isClose) cc.notify_server_that_client_stopped();
        exit(1);
      }
      if (!isClose) {
        thread->stats.FinishedSingleOp();
        // finish one op
        if (cc.check_server_said_stop()) break;
      }
      fds.push_back(fd);
    }

    if (!isClose) {
      // benchmark done (it this is open bench)
      cc.notify_server_that_client_stopped();
      thread->stats.Stop();
    }

    if (isClose) {
      cc.notify_server_that_client_is_ready();
      cc.wait_till_server_says_start();
      thread->stats.Start();
    }

    // close
    for (auto fd : fds) {
      int rt = 0;
#ifndef CFS_USE_POSIX
      rt = fs_close(fd);
#else
      rt = close(fd);
#endif
      if (rt < 0) {
        fprintf(stderr, "Failed to close:%d\n", fd);
        if (isClose) {
          cc.notify_server_that_client_stopped();
        }
        exit(1);
      }
      if (isClose) {
        thread->stats.FinishedSingleOp();
        if (cc.check_server_said_stop()) break;
      }
    }

    if (isClose) {
      cc.notify_server_that_client_stopped();
      thread->stats.Stop();
    }

#ifndef CFS_USE_POSIX
    fs_closedir(dentryPtr);
#else
    closedir(dentryPtr);
#endif
#ifndef CFS_USE_POSIX
    fs_free(initShmPtr);
#endif
  }

  void BenchOpen(ThreadState *thread) {
    DoOpenOrClose(thread, /*isClose*/ false);
  }

  void BenchClose(ThreadState *thread) {
    DoOpenOrClose(thread, /*isClose*/ true);
  }

  // Remove multiple files in the same directory.
  // NOTE: All the files must exist and be in the same directory.
  // Before running this function, Ensure that bench_prepare.py is executed
  // with the same parameters.
  void BenchUnlink(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";
    if (FLAGS_dirfile_name_prefix != nullptr) {
      basepath += FLAGS_dirfile_name_prefix;
    } else {
      basepath += "f";
    }
    std::string spath;
    const char *path;

    // warmup the dentry
    struct stat stbuf;
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
#ifndef CFS_USE_POSIX
      rc = fs_stat(path, &stbuf);
#else
      rc = stat(path, &stbuf);
#endif
      // fprintf(stderr, "fs_stat(%s) rc:%d\n", path, rc);
      if (rc != 0) {
        fprintf(stderr, "fs_stat() failed %s rc:%d\n", path, rc);
        exit(1);
      }
    }

    SignalStartDumpLoad(thread);

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      checkThinkTime(thread->rand);
      path = spath.c_str();
#ifndef CFS_USE_POSIX
      rc = fs_unlink(path);
#else
      rc = unlink(path);
#endif
      if (rc < 0) {
        fprintf(stderr, "fs_unlink: failed to unlink %s\n", path);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
    SignalStopDumpLoad(thread);
#ifndef CFS_USE_POSIX
    fs_syncunlinked();
#endif
  }

  // first do several stat() to make sure the target are moved to others
  void BenchMovedUnlink(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString() + "/f";
    std::string spath;
    const char *path;

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stdout, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    // do several stat() thus inode will be moved to another worker
    struct stat stbuf;
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
      for (int ii = 0; ii < 3; ii++) {
#ifndef CFS_USE_POSIX
        rc = fs_stat(path, &stbuf);
#else
        rc = stat(path, &stbuf);
#endif
        // fprintf(stderr, "fs_stat(%s) rc:%d\n", path, rc);
        if (rc != 0) {
          fprintf(stderr, "fs_stat() failed %s rc:%d\n", path, rc);
          exit(1);
        }
      }
    }

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
#ifndef CFS_USE_POSIX
      rc = fs_unlink(path);
#else
      rc = unlink(path);
#endif
      if (rc < 0) {
        fprintf(stderr, "fs_unlink: failed to unlink %s\n", path);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
    FlushAllData(thread);
  }

  void checkThinkTime(leveldb::Random &r) {
    if (FLAGS_think_nano > 0) {
      SpinSleepNano(r.Next() % (FLAGS_think_nano * 2));
    }
  }

  // Create many files in the same directory.
  // NOTE: does not close the files to avoid cost of "close".
  // Assumes cleanup happens automatically when process dies.
  void BenchCreate(ThreadState *thread) {
    PinToCore(thread);
    static constexpr size_t kMaxInitFileSize = 1024 * 1024;
    if (value_size_ > kMaxInitFileSize) value_size_ = kMaxInitFileSize;
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }
    char *buf = nullptr;
#ifndef CFS_USE_POSIX
    buf = reinterpret_cast<char *>(fs_malloc(kMaxInitFileSize));
#else
    buf = reinterpret_cast<char *>(malloc(kMaxInitFileSize));
#endif

    int fd;
    std::string basepath = dir_.ToString();
    basepath += "/";
    std::vector<int> openedFds;
    if (FLAGS_dirfile_name_prefix != nullptr) {
      basepath += FLAGS_dirfile_name_prefix;
    } else {
      basepath += "f";
    }
    std::string spath;
    const char *path;

    SignalStartDumpLoad(thread);

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();
    if (!FLAGS_coordinator) {
      // This is a hack that signal the client very early thus coordinator will
      // exit
      cc.notify_server_that_client_stopped();
    }

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
      checkThinkTime(thread->rand);
#ifndef CFS_USE_POSIX
      fd = fs_open(path, O_CREAT | O_WRONLY, 0644);
      // openedFds.push_back(fd);
#else
      fd = ::open(path, O_CREAT | O_WRONLY, 0644);
#endif
      if (fd < 0) {
        fprintf(stderr, "fs_open: failed to create %s\n", path);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      // write content to the file, size controlled by value_size
      if (value_size_ > 0) {
        // assert(value_size_ <= gBlockSize);
        memset(buf, 'a', value_size_);
        int rc;
#ifndef CFS_USE_POSIX
        rc = fs_allocated_write(fd, buf, value_size_);
#else
        rc = write(fd, buf, value_size_);
#endif
        if (rc != value_size_) {
          fprintf(stderr,
                  "fs_write after create, fail. error:%s rc:%d fd:%d i:%d\n",
                  strerror(errno), rc, fd, i);
        }
      }
#ifndef CFS_USE_POSIX
      // fs_close(fd);
#else
      close(fd);
#endif
      thread->stats.FinishedSingleOp();
      if ((FLAGS_coordinator) && cc.check_server_said_stop()) break;
    }
#ifndef CFS_USE_POSIX
    // NOTE: This will cause the micros/sec vs. average latency gap larger
    // for (auto curFd : openedFds) {
    //   fs_close(curFd);
    // }
#endif
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
    SignalStopDumpLoad(thread);
    FlushAllData(thread);
#ifndef CFS_USE_POSIX
    fs_free(buf);
#else
    free(buf);
#endif
  }

  // mkdir many directories inside one single directory.
  void BenchMkdir(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";
    // std::string basepath = dir_.ToString() + "/d";
    if (FLAGS_dirfile_name_prefix != nullptr) {
      basepath += FLAGS_dirfile_name_prefix;
    } else {
      basepath += "/d";
    }
    std::string spath;
    const char *path;

    SignalStartDumpLoad(thread);

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
      checkThinkTime(thread->rand);
#ifndef CFS_USE_POSIX
      rc = fs_mkdir(path, 0);
#else
      rc = mkdir(path, 0);
#endif
      if (rc != 0) {
        fprintf(stderr, "fs_mkdir: failed to make directory %s\n", path);
        fprintf(stderr, "fs_mkdir: expected returncode 0, got %d\n", rc);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
    SignalStopDumpLoad(thread);
    FlushAllData(thread);
  }

  // opendir()+readdir()
  void BenchListdir(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";

#ifndef CFS_USE_POSIX
    struct CFS_DIR *dentryPtr = nullptr;
    void *initShmPtr = fs_malloc(4096);
#else
    DIR *dentryPtr = nullptr;
#endif

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
#ifndef CFS_USE_POSIX
      dentryPtr = fs_opendir(basepath.c_str());
#else
      dentryPtr = opendir(basepath.c_str());
#endif
      if (dentryPtr == nullptr) {
        fprintf(stderr, "Failed to opendir\n");
        cc.notify_server_that_client_stopped();
        return;
      }

      // iterate the whole directory
      struct dirent *dp;
      do {
#ifndef CFS_USE_POSIX
        dp = fs_readdir(dentryPtr);
#else
        dp = readdir(dentryPtr);
#endif
      } while (dp != NULL);

#ifndef CFS_USE_POSIX
      fs_closedir(dentryPtr);
#else
      closedir(dentryPtr);
#endif
      // finish one op
      thread->stats.FinishedSingleOp();

      if (cc.check_server_said_stop()) break;
    }
#ifndef CFS_USE_POSIX
    fs_free(initShmPtr);
#endif

    // benchmark done
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
  }

  void BenchOpendir(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";

#ifndef CFS_USE_POSIX
    struct CFS_DIR *dentryPtr = nullptr;
    void *initShmPtr = fs_malloc(4096);
#else
    DIR *dentryPtr = nullptr;
#endif

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
#ifndef CFS_USE_POSIX
      dentryPtr = fs_opendir(basepath.c_str());
#else
      dentryPtr = opendir(basepath.c_str());
#endif
      if (dentryPtr == nullptr) {
        fprintf(stderr, "Failed to opendir\n");
        cc.notify_server_that_client_stopped();
        return;
      }
      // We just open this directory and close it immediately
#ifndef CFS_USE_POSIX
      fs_closedir(dentryPtr);
#else
      closedir(dentryPtr);
#endif
      // finish one op
      thread->stats.FinishedSingleOp();

      if (cc.check_server_said_stop()) break;
    }
#ifndef CFS_USE_POSIX
    fs_free(initShmPtr);
#endif

    // benchmark done
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
  }

  // opendir()+readdir()+stat()
  // timing the whole process
  void BenchListdirInfo1(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";

#ifndef CFS_USE_POSIX
    struct CFS_DIR *dentryPtr = nullptr;
    void *initShmPtr = fs_malloc(4096);
#else
    DIR *dentryPtr = nullptr;
#endif

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
#ifndef CFS_USE_POSIX
      dentryPtr = fs_opendir(basepath.c_str());
#else
      dentryPtr = opendir(basepath.c_str());
#endif
      if (dentryPtr == nullptr) {
        fprintf(stderr, "Failed to opendir\n");
        cc.notify_server_that_client_stopped();
        return;
      }

      // iterate the whole directory
      struct dirent *dp;
      struct stat statbuf;
      int rc;
      do {
#ifndef CFS_USE_POSIX
        dp = fs_readdir(dentryPtr);
#else
        dp = readdir(dentryPtr);
#endif
        if (dp == NULL) break;

        // do stat on that dentry
        std::string stat_path = basepath + dp->d_name;
#ifndef CFS_USE_POSIX
        rc = fs_stat(stat_path.c_str(), &statbuf);
#else
        rc = ::stat(stat_path.c_str(), &statbuf);
#endif
        if (rc != 0) {
          fprintf(stderr, "Failed to stat %s\n", stat_path.c_str());
          cc.notify_server_that_client_stopped();
        }
      } while (1);

#ifndef CFS_USE_POSIX
      fs_closedir(dentryPtr);
#else
      closedir(dentryPtr);
#endif
      // finish one op
      thread->stats.FinishedSingleOp();

      if (cc.check_server_said_stop()) break;
    }
#ifndef CFS_USE_POSIX
    fs_free(initShmPtr);
#endif

    // benchmark done
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
  }

  // opendir()+readdir()+stat()
  // timing only stat()
  void BenchListdirInfo2(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";

#ifndef CFS_USE_POSIX
    struct CFS_DIR *dentryPtr = nullptr;
    void *initShmPtr = fs_malloc(4096);
#else
    DIR *dentryPtr = nullptr;
#endif

    std::vector<std::string> dnames;

#ifndef CFS_USE_POSIX
    dentryPtr = fs_opendir(basepath.c_str());
#else
    dentryPtr = opendir(basepath.c_str());
#endif
    if (dentryPtr == nullptr) {
      fprintf(stderr, "Failed to opendir\n");
      return;
    }

    while (true) {
      struct dirent *dp;
#ifndef CFS_USE_POSIX
      dp = fs_readdir(dentryPtr);
#else
      dp = readdir(dentryPtr);
#endif
      if (dp) {
        dnames.push_back(dp->d_name);
      } else {
        break;
      }
    }
    fprintf(stdout, "dirwidth:%ld\n", dnames.size());

    if (FLAGS_value_random_size) {
      fprintf(stdout, "filename randomized\n");
      std::random_shuffle(dnames.begin(), dnames.end());
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
      // if (FLAGS_wid > 0) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_FUTURE);
#endif
    }

    SignalStartDumpLoad(thread);

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int j = 0; j < numop_; j++) {
      // iterate the whole directory
      for (size_t i = 0; i < dnames.size(); ++i) {
        struct stat statbuf;
        int rc;
        std::string stat_path = basepath + dnames[i];
        checkThinkTime(thread->rand);
#ifndef CFS_USE_POSIX
        rc = fs_stat(stat_path.c_str(), &statbuf);
        if (FLAGS_share_mode && j == 0) {
          SpinSleepNano(2000);
        }
#else
        rc = ::stat(stat_path.c_str(), &statbuf);
#endif
        if (rc != 0) {
          fprintf(stderr, "Failed to stat %s\n", stat_path.c_str());
          cc.notify_server_that_client_stopped();
        }
      }

      // finish one op
      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }

    // benchmark done
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();

    SignalStopDumpLoad(thread);

#ifndef CFS_USE_POSIX
    fs_closedir(dentryPtr);
#else
    closedir(dentryPtr);
#endif
#ifndef CFS_USE_POSIX
    fs_free(initShmPtr);
#endif
  }

  // mkdir many directories inside one single directory.
  void BenchRmdir(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    int rc;
    std::string basepath = dir_.ToString() + "/d";
    std::string spath;
    const char *path;

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
#ifndef CFS_USE_POSIX
      rc = fs_rmdir(path);
#else
      rc = rmdir(path);
#endif
      if (rc != 0) {
        fprintf(stderr, "fs_rmdir: failed to make directory %s\n", path);
        cc.notify_server_that_client_stopped();
        exit(1);
      }

      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
  }

  // Rename from d0 to d1
  void BenchRename(ThreadState *thread) {
    PinToCore(thread);
    assert(FLAGS_dir2 != nullptr);
    // warmup
    int rc;
    std::string basepath = dir_.ToString() + "/";
    std::string dstbasepath = std::string(FLAGS_dir2) + "/";
    if (FLAGS_dirfile_name_prefix != nullptr) {
      basepath += FLAGS_dirfile_name_prefix;
      dstbasepath += FLAGS_dirfile_name_prefix;
    } else {
      basepath += "/f";
      dstbasepath += "/f";
    }
    std::string spath;
    std::string spathdst;
    const char *path, *pathdst;
    struct stat stbuf;
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      path = spath.c_str();
#ifndef CFS_USE_POSIX
      rc = fs_stat(path, &stbuf);
#else
      rc = stat(path, &stbuf);
#endif
      if (rc != 0) {
        fprintf(stderr, "fs_stat() failed %s rc:%d\n", path, rc);
      }
    }

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      spath = basepath + std::to_string(i);
      spathdst = dstbasepath + std::to_string(i);
      path = spath.c_str();
      pathdst = spathdst.c_str();
#ifndef CFS_USE_POSIX
      rc = fs_rename(path, pathdst);
#else
      rc = rename(path, pathdst);
#endif
      thread->stats.FinishedSingleOp();
      if (rc < 0) {
        fprintf(stderr, "fs_rename fail. p1:%s p2:%s rc:%d\n", path, pathdst,
                rc);
      }
      if (cc.check_server_said_stop()) break;
    }
    cc.notify_server_that_client_stopped();
    thread->stats.Stop();
    FlushAllData(thread);
  }

  void BenchDynamicStatOne(ThreadState *thread) {
    PinToCore(thread);

    int rc;
    struct stat statbuf;
    std::string basepath = dir_.ToString();
    const char *path = basepath.c_str();

    initFsThreadLocal();

    struct StatCtx {
      const char *path;
      struct stat *buf;
      int ret;
    } cur_ctx;
    cur_ctx.path = path;
    cur_ctx.buf = &statbuf;

    auto cur_op_func = [](void *ctx) {
      struct StatCtx *stat_ctx = reinterpret_cast<StatCtx *>(ctx);
#ifndef CFS_USE_POSIX
      stat_ctx->ret = fs_stat(stat_ctx->path, stat_ctx->buf);
      return stat_ctx->ret;
#else
      return 0;
#endif
    };

    DynamicSegmentTracker seg_tracker;

    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "stat error path:%s\n", path);
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicStatall(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();

    int rc;
    std::string basepath = dir_.ToString();
    basepath += "/";

#ifndef CFS_USE_POSIX
    struct CFS_DIR *dentryPtr = nullptr;
#else
    DIR *dentryPtr = nullptr;
#endif

    std::vector<std::string> dnames;

#ifndef CFS_USE_POSIX
    dentryPtr = fs_opendir(basepath.c_str());
#else
    dentryPtr = opendir(basepath.c_str());
#endif
    if (dentryPtr == nullptr) {
      fprintf(stderr, "Failed to opendir\n");
      return;
    }

    while (true) {
      struct dirent *dp;
#ifndef CFS_USE_POSIX
      dp = fs_readdir(dentryPtr);
#else
      dp = readdir(dentryPtr);
#endif
      if (dp) {
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
          continue;
        }
        dnames.push_back(dp->d_name);
      } else {
        break;
      }
    }
    fprintf(stdout, "dirwidth:%ld\n", dnames.size());

    if (FLAGS_value_random_size) {
      fprintf(stdout, "filename randomized\n");
      std::random_shuffle(dnames.begin(), dnames.end());
    }

    struct StatallCtx {
      StatallCtx(std::string &path, std::vector<std::string> &names)
          : basepath(path), dnames(names), fidx_max(names.size() - 1) {}
      std::string &basepath;
      std::vector<std::string> &dnames;
      struct stat buf;
      size_t fidx_max{0};
      size_t fidx{0};
      int ret{0};
    };

    StatallCtx cur_ctx(basepath, dnames);

    auto cur_op_func = [](void *ctx) {
#ifndef CFS_USE_POSIX
      struct StatallCtx *statall_ctx = reinterpret_cast<StatallCtx *>(ctx);
      std::string stat_path =
          statall_ctx->basepath + statall_ctx->dnames[statall_ctx->fidx];
      statall_ctx->ret = fs_stat(stat_path.c_str(), &(statall_ctx->buf));
      statall_ctx->fidx++;
      if (statall_ctx->fidx >= statall_ctx->fidx_max) {
        statall_ctx->fidx = 0;
      }
      return statall_ctx->ret;
#else
      return 0;
#endif
    };

    DynamicSegmentTracker seg_tracker;

    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "stat error path:%s\n", dnames[cur_ctx.fidx].c_str());
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicDiskRead(ThreadState *thread) {
    BenchDynamicRead(thread, false);
  }

  // use FLAGS_num_hot_files as
  void BenchDynamicReadLbIo2(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    fprintf(stderr,
            "readlbio2 num_files:%d num_hot_files:%d in_mem_file_size:%lu "
            "op_byte:%d\n",
            FLAGS_num_files, FLAGS_num_hot_files, FLAGS_in_mem_file_size,
            value_size_);

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back("readn_a" + std::to_string(thread->aid) + "_f" +
                           std::to_string(i));
    }

    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(int aid, std::vector<int> &fds, size_t size,
                  int batch_num_files)
          : aid(aid),
            fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            read_size(size),
            in_mem_fset_size(batch_num_files) {
#ifndef CFS_USE_POSIX
        read_buf = (char *)fs_malloc(size);
#endif
      }
      int aid;
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t read_size;
      int in_mem_fset_size{0};
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      int cur_fd_idx_base{0};
      char *read_buf;
      int ret{0};
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(thread->aid, fd_vec, value_size_, FLAGS_num_hot_files);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      // fprintf(stdout, "fd:%d off:%lu\n", ctx->fd_vec[ctx->cur_fd_idx],
      // ctx->cur_off);
      ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                                    (void *)ctx->read_buf, ctx->read_size,
                                    ctx->cur_off);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      ctx->cur_fd_idx++;
      if (ctx->cur_fd_idx == ctx->cur_fd_idx_base + ctx->in_mem_fset_size ||
          ctx->cur_fd_idx == ctx->fd_vec.size()) {
        // finish this offset for all the files
        ctx->cur_off += ctx->read_size;
        ctx->cur_fd_idx = ctx->cur_fd_idx_base;
        if (ctx->cur_off >= ctx->max_file_size - ctx->read_size) {
          // reset, thus we read the file from the begining again
          // fprintf(stderr,
          //         "read from the begining cur_off:%lu fd_idx:%d,
          //         fd_base:%d\n", ctx->cur_off, ctx->cur_fd_idx,
          //         ctx->cur_fd_idx_base);
          ctx->cur_off = 0;
          ctx->cur_fd_idx_base += ctx->in_mem_fset_size;
          if (ctx->cur_fd_idx_base == ctx->fd_vec.size()) {
            fprintf(stderr, "error, fd used up for io\n");
            return -1;
          }
        }
      }
      return 0;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr,
              "aid:%d read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              thread->aid, cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx,
              cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout,
              "aid:%d read done ret:%d idx_base:%d fd_num:%lu fd_idx:%d "
              "cur_off:%lu\n",
              thread->aid, cur_ctx.ret, cur_ctx.cur_fd_idx_base, fd_vec.size(),
              cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicReadIoMem(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    fprintf(stderr,
            "read_iomem num_files:%d num_hot_files:%d in_mem_file_size:%lu "
            "op_byte:%d align_bytes:%d\n",
            FLAGS_num_files, FLAGS_num_hot_files, FLAGS_in_mem_file_size,
            value_size_, FLAGS_rw_align_bytes);

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back("readn_a" + std::to_string(thread->aid) + "_f" +
                           std::to_string(i));
    }

    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      // fprintf(stdout, "cur_path:%s fd:%d\n", cur_path, cur_fd);
      fd_vec.push_back(cur_fd);
#ifndef CFS_USE_POSIX
      // if (i == 0) {
      char *buf = (char *)fs_malloc(value_size_ * 2);
      int ret = fs_allocated_pread(cur_fd, buf, value_size_ * 2, 0);
      if (ret != value_size_ * 2) {
        fprintf(stderr, "error cannot warmup the first block\n");
        exit(1);
      }
      fs_free(buf);
      // }
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(int aid, std::vector<int> &fds, size_t size,
                  int batch_num_files)
          : aid(aid),
            fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            read_size(size),
            do_sync_mod_vec(FLAGS_num_segment, 1) {
#ifndef CFS_USE_POSIX
        read_buf = (char *)fs_malloc(size);

        if (FLAGS_rw_align_bytes == 101) {
          do_sync_mod_vec = {2,  4,  6,  8,  10, 12, 14, 16, 32, 64,
                             32, 16, 14, 12, 10, 8,  6,  4,  2};
        } else if (FLAGS_rw_align_bytes == 102) {
          do_sync_mod_vec = {2, 20, 30, 64, 30, 20, 2};
          // do_sync_mod_vec = {1, 1, 1, 1, 1, 1, 1};
        }

        if (do_sync_mod_vec.size() != FLAGS_num_segment) {
          std::cerr << FLAGS_num_segment << " size:" << do_sync_mod_vec.size()
                    << std::endl;
          throw std::runtime_error("num_seg, in_mem_vec not match");
        }
#endif
      }
      int aid;
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t read_size;
      uint64_t cur_off{0};
      int cur_fd_idx{1};
      char *read_buf;
      int ret{0};
      int *seg_idx_ptr = nullptr;
      std::vector<int> do_sync_mod_vec;
      uint64_t cur_op_idx = 0;
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(thread->aid, fd_vec, value_size_, FLAGS_num_hot_files);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      // fprintf(stdout, "fd:%d off:%lu\n", ctx->fd_vec[ctx->cur_fd_idx],
      // ctx->cur_off);
      int cur_seg_idx = *(ctx->seg_idx_ptr);
      // if (ctx->cur_op_idx <= 3) {
      //   ctx->cur_op_idx++;
      //   return 0;
      // }
      // fprintf(stderr, "aid:%d cur_seg:%d\n", ctx->aid, cur_seg_idx);
      // fprintf(stdout, "seg_idx:%d fd_idx:%d cur_off:%lu\n", cur_seg_idx,
      // ctx->cur_fd_idx, ctx->cur_off);
      // ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
      //                               (void *)ctx->read_buf, ctx->read_size,
      //                               ctx->cur_off);

      ctx->cur_op_idx++;
      if ((ctx->aid + ctx->cur_op_idx) % ctx->do_sync_mod_vec[cur_seg_idx] ==
          0) {
        // fprintf(stderr, "do_sync op_idx:%lu seg_idx:%d sync_op:%d\n",
        // ctx->cur_op_idx, cur_seg_idx,
        // ctx->do_sync_mod_vec[cur_seg_idx]);
        if (ctx->do_sync_mod_vec[cur_seg_idx] < 33) {
          const int kSyncRetryNum = 3;
          int sync_try_num = 0;
        retry_for_ebadfd:
          sync_try_num++;
          // SpinSleepNano(1000);
          ctx->ret = fs_fdatasync(ctx->fd_vec[ctx->cur_fd_idx]);
          if (ctx->ret < 0) {
            if (ctx->ret == -EBADF && sync_try_num < kSyncRetryNum) {
              SpinSleepNano(4000);
              fprintf(stderr, "get EBADF cur_retry:%d\n", sync_try_num);
              goto retry_for_ebadfd;
            }
            fprintf(stderr, "sync error, fd_idx:%d ret:%d fd:%d\n",
                    ctx->cur_fd_idx, ctx->ret, ctx->fd_vec[ctx->cur_fd_idx]);
            return ctx->ret;
          }
          return ctx->ret;
        }
      }
      ctx->ret = fs_allocated_pwrite(ctx->fd_vec[ctx->cur_fd_idx],
                                     (void *)ctx->read_buf, ctx->read_size,
                                     ctx->cur_off);
      if (ctx->ret < 0) {
        fprintf(stderr, "pwrite error, fd_idx:%d\n", ctx->cur_fd_idx);
        return ctx->ret;
      }
      ctx->cur_fd_idx++;
      if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
        ctx->cur_fd_idx = 0;
      }
      return 0;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    cur_ctx.seg_idx_ptr = &seg_tracker.segment_idx;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr,
              "aid:%d readiomem ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              thread->aid, cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx,
              cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout,
              "aid:%d read done ret:%d fd_num:%lu fd_idx:%d "
              "cur_off:%lu\n",
              thread->aid, cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx,
              cur_ctx.cur_off);
    }

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  // FLAGS_num_hot_files: if > 0, then all the files are hot
  // must specify FLAGS_in_mem_file_size for hot range, prep phase fetch
  // data into server memory
  // FLAGS_num_files: if FLAGS_num_hot_file > 0, the disk read for each file
  // will also use FLAGS_in_mem_file_size to specify size to read for each file
  void BenchDynamicReadLbIo(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    fprintf(stderr,
            "readlbio num_files:%d num_hot_files:%d in_mem_file_size:%lu "
            "op_byte:%d\n",
            FLAGS_num_files, FLAGS_num_hot_files, FLAGS_in_mem_file_size,
            value_size_);
    if (FLAGS_num_hot_files != FLAGS_num_files && FLAGS_num_hot_files > 0) {
      throw std::runtime_error(
          "if num_hot files specificed, it must be equal to num_files");
    }

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_hot_files; i++) {
      fname_vec_.push_back("readn_a" + std::to_string(thread->aid) + "_f" +
                           std::to_string(i));
    }
#ifndef CFS_USE_POSIX
    char *prep_buf = (char *)fs_malloc(value_size_);
#endif

    sleep(1);
    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);

#ifndef CFS_USE_POSIX
      // fetch data into server memory
      int ret;
      off_t cur_off = 0;
      while (cur_off <= FLAGS_in_mem_file_size - value_size_) {
        ret = fs_allocated_pread(cur_fd, prep_buf, value_size_, cur_off);
        if (ret != value_size_) {
          fprintf(stderr, "cur_off:%lu cur_fd:%d size:%d\n", cur_off, cur_fd,
                  value_size_);
          throw std::runtime_error("error in prep warmup\n");
        }
        cur_off += value_size_;
      }
      // fprintf(stderr, "finish warmup for :%s cur_off:%lu\n", cur_path,
      // cur_off);
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "aid:%d assign to:%d\n", FLAGS_wid, thread->aid);
      fs_admin_thread_reassign(0, FLAGS_wid,
                               FS_REASSIGN_FUTURE & FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(int aid, std::vector<int> &fds, size_t size, bool is_hot,
                  int batch_num_files)
          : aid(aid),
            fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            read_size(size),
            is_hot(is_hot),
            fset_size(batch_num_files) {
#ifndef CFS_USE_POSIX
        read_buf = (char *)fs_malloc(size);
#endif
      }
      int aid;
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t read_size;
      bool is_hot{false};
      int fset_size{0};
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      int open_fidx{0};
      bool is_close{false};
      char *read_buf;
      int ret{0};
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(
        thread->aid, fd_vec, value_size_, FLAGS_num_hot_files > 0,
        FLAGS_num_hot_files > 0 ? FLAGS_num_hot_files : FLAGS_num_files);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      if (ctx->is_hot) {
        ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                                      (void *)ctx->read_buf, ctx->read_size,
                                      ctx->cur_off);
        if (ctx->ret < 0) {
          return ctx->ret;
        }
        ctx->cur_fd_idx++;
        if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
          // finish this offset for all the files
          ctx->cur_off += ctx->read_size;
          ctx->cur_fd_idx = 0;
          if (ctx->cur_off >= ctx->max_file_size - ctx->read_size) {
            // reset, thus we read the file from the begining again
            // fprintf(stderr, "read from the begining cur_off:%lu\n",
            // ctx->cur_off);
            ctx->cur_off = 0;
          }
        }
        return 0;
      } else {
        if (ctx->cur_off == 0 && ctx->cur_fd_idx == 0) {
          // This is a version that will crazily open a bunch of files
          // if (!ctx->fd_vec.empty()) {
          //   for (auto fd : ctx->fd_vec) {
          //     ctx->ret = fs_close(fd);
          //     if (ctx->ret < 0) {
          //       return ctx->ret;
          //     }
          //   }
          //   ctx->fd_vec.clear();
          // }
          // // re-open the new batch
          // const int kPathLen = 256;
          // char cur_path[kPathLen];
          // int fd = -1;
          // while (ctx->fd_vec.size() < ctx->fset_size) {
          //   memset(cur_path, 0, kPathLen);
          //   // NOTE: it must be in root directory
          //   sprintf(cur_path, "readn_a%d_f%d", ctx->aid, ctx->open_fidx++);
          //   fd = fs_open(cur_path, O_RDWR, 0644);
          //   if (fd < 0) {
          //     fprintf(stderr, "open(%s) fail\n", cur_path);
          //     return -1;
          //   }
          //   fprintf(stdout, "open(%s) return :%d\n", cur_path, fd);
          //   ctx->fd_vec.push_back(fd);
          // }
          ////////
          // this is a version treat each open as op
          if (ctx->is_close && !ctx->fd_vec.empty()) {
            int fd = ctx->fd_vec[0];
            ctx->ret = fs_close(fd);
            // fprintf(stdout, "close(%d) ret:%d\n", fd, ctx->ret);
            if (ctx->ret < 0) {
              return ctx->ret;
            } else {
              ctx->fd_vec.erase(ctx->fd_vec.begin());
              if (ctx->fd_vec.empty()) {
                ctx->is_close = false;
              }
              return 0;
            }
          }
          const int kPathLen = 256;
          char cur_path[kPathLen];
          int fd = -1;
          if (ctx->fd_vec.size() < ctx->fset_size) {
            memset(cur_path, 0, kPathLen);
            // NOTE: it must be in root directory
            sprintf(cur_path, "readn_a%d_f%d", ctx->aid, ctx->open_fidx++);
            fd = fs_open(cur_path, O_RDWR, 0644);
            if (fd < 0) {
              fprintf(stderr, "open(%s) fail\n", cur_path);
              return -1;
            }
            fprintf(stdout, "open(%s) return :%d\n", cur_path, fd);
            ctx->fd_vec.push_back(fd);
            return 0;
          }
          ///////
        }
        ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                                      (void *)ctx->read_buf, ctx->read_size,
                                      ctx->cur_off);
        if (ctx->ret < 0) {
          return ctx->ret;
        }
        ctx->cur_fd_idx++;
        if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
          ctx->cur_off += ctx->read_size;
          // fprintf(stdout, "aid:%d cur_off:%lu\n", ctx->aid, ctx->cur_off);
          ctx->cur_fd_idx = 0;
          if (ctx->cur_off >= ctx->max_file_size - ctx->read_size) {
            ctx->cur_off = 0;
            ctx->is_close = true;
          }
        }
        return 0;
      }
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    // sleep(10);
    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(
          stdout,
          "read done ret:%d open_fidx:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
          cur_ctx.ret, cur_ctx.open_fidx, fd_vec.size(), cur_ctx.cur_fd_idx,
          cur_ctx.cur_off);
    }
#ifndef CFS_USE_POSIX
    fs_free(prep_buf);
#endif

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicFixSync(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    if (FLAGS_dirfile_name_prefix == nullptr) {
      std::cerr << "Error, FLAGS_dirfile_name_prefix must be specified"
                << std::endl;
      return;
    }
    fprintf(stderr, " num_files:%d dir_prefix:%s\n", FLAGS_num_files,
            FLAGS_dirfile_name_prefix);

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back(FLAGS_dirfile_name_prefix + std::to_string(i));
    }
#ifndef CFS_USE_POSIX
    char *prep_buf = (char *)fs_malloc(value_size_);
#endif

    sleep(1);
    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    size_t warmup_read_size = value_size_ > 0 ? value_size_ : 4096;
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);

#ifndef CFS_USE_POSIX
      // fetch data into server memory
      int ret;
      off_t cur_off = 0;
      while (cur_off <= FLAGS_in_mem_file_size - warmup_read_size) {
        ret = fs_allocated_pread(cur_fd, prep_buf, warmup_read_size, cur_off);
        if (ret != warmup_read_size) {
          fprintf(stderr, "cur_off:%lu cur_fd:%d size:%d warm_up_total:%lu\n",
                  cur_off, cur_fd, value_size_, FLAGS_in_mem_file_size);
          throw std::runtime_error("fix-sync error in prep warmup\n");
        }
        cur_off += warmup_read_size;
      }
      // fprintf(stderr, "finish warmup for :%s cur_off:%lu\n", cur_path,
      // cur_off);
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(std::vector<int> &fds, size_t size, leveldb::Random &rand)
          : fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            write_size(size),
            rand(rand) {
#ifndef CFS_USE_POSIX
        write_buf = (char *)fs_malloc(16384);
        for (int i = 0; i < 16384; i++) {
          write_buf[i] = (i + 'a') % 20;
        }
#endif
      }
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t write_size;
      bool is_random_size;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      char *write_buf;
      int ret{0};
      leveldb::Random &rand;
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(fd_vec, value_size_, thread->rand);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      ctx->cur_off =
          (ctx->rand.Next() % (ctx->max_file_size - ctx->write_size) / 4096) *
          4096;
      ctx->ret = fs_allocated_pwrite(ctx->fd_vec[ctx->cur_fd_idx],
                                     (void *)ctx->write_buf, ctx->write_size,
                                     ctx->cur_off);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      ctx->ret = fs_fdatasync(ctx->fd_vec[ctx->cur_fd_idx]);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      // fprintf(stdout, "fd:%d off:%ld size:%lu ret:%d\n",
      //         ctx->fd_vec[ctx->cur_fd_idx], ctx->cur_off, ctx->write_size,
      //         ctx->ret);
      // ctx->cur_fd_idx++;
      // if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
      //   ctx->cur_fd_idx = 0;
      // }
      ctx->cur_fd_idx = ctx->rand.Next() % ctx->fd_vec.size();
      return ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr,
              "overwrite error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout,
              "overwrite done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }
#ifndef CFS_USE_POSIX
    fs_free(prep_buf);
#endif

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicOverwrite(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    if (FLAGS_dirfile_name_prefix == nullptr) {
      std::cerr << "Error, FLAGS_dirfile_name_prefix must be specified"
                << std::endl;
      return;
    }
    fprintf(stderr, " num_files:%d dir_prefix:%s\n", FLAGS_num_files,
            FLAGS_dirfile_name_prefix);

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back(FLAGS_dirfile_name_prefix + std::to_string(i));
    }
#ifndef CFS_USE_POSIX
    char *prep_buf = (char *)fs_malloc(value_size_);
#endif

    sleep(1);
    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    size_t warmup_read_size = value_size_ > 0 ? value_size_ : 4096;
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);

#ifndef CFS_USE_POSIX
      // fetch data into server memory
      int ret;
      off_t cur_off = 0;
      while (cur_off <= FLAGS_in_mem_file_size - warmup_read_size) {
        ret = fs_allocated_pread(cur_fd, prep_buf, warmup_read_size, cur_off);
        if (ret != warmup_read_size) {
          fprintf(stderr, "cur_off:%lu cur_fd:%d size:%d path:%s\n", cur_off,
                  cur_fd, value_size_, cur_path);
          throw std::runtime_error("error in prep warmup\n");
        }
        cur_off += warmup_read_size;
      }
      // fprintf(stderr, "finish warmup for :%s cur_off:%lu\n", cur_path,
      // cur_off);
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(std::vector<int> &fds, size_t size, leveldb::Random &rand)
          : fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            write_size(size),
            rand(rand) {
#ifndef CFS_USE_POSIX
        write_buf = (char *)fs_malloc(16384);
        for (int i = 0; i < 16384; i++) {
          write_buf[i] = (i + 'a') % 20;
        }
#endif
      }
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t write_size;
      bool is_random_size;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      char *write_buf;
      int ret{0};
      leveldb::Random &rand;
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(fd_vec, value_size_, thread->rand);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      ctx->cur_off = ctx->rand.Next() % (ctx->max_file_size - ctx->write_size);
      ctx->ret = fs_allocated_pwrite(ctx->fd_vec[ctx->cur_fd_idx],
                                     (void *)ctx->write_buf, ctx->write_size,
                                     ctx->cur_off);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      // fprintf(stdout, "fd:%d off:%ld size:%lu ret:%d\n",
      //         ctx->fd_vec[ctx->cur_fd_idx], ctx->cur_off, ctx->write_size,
      //         ctx->ret);
      // ctx->cur_fd_idx++;
      // if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
      //   ctx->cur_fd_idx = 0;
      // }
      ctx->cur_fd_idx = ctx->rand.Next() % ctx->fd_vec.size();
      return ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr,
              "overwrite error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout,
              "overwrite done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }
#ifndef CFS_USE_POSIX
    fs_free(prep_buf);
#endif

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  // In memory read
  // FLAGS_num_files --> total number of files accessed
  // all the files are fetched into server memory in prep phase
  void BenchDynamicReadLb(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    fprintf(stderr, "readlb num_files:%d\n", FLAGS_num_files);

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back("readn_a" + std::to_string(thread->aid) + "_f" +
                           std::to_string(i));
    }
#ifndef CFS_USE_POSIX
    char *prep_buf = (char *)fs_malloc(value_size_);
#endif

    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    size_t warmup_read_size = value_size_ > 0 ? value_size_ : 4096;
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);

#ifndef CFS_USE_POSIX
      // fetch data into server memory
      int ret;
      off_t cur_off = 0;
      while (cur_off <= FLAGS_in_mem_file_size - warmup_read_size) {
        ret = fs_allocated_pread(cur_fd, prep_buf, warmup_read_size, cur_off);
        if (ret != warmup_read_size) {
          // fprintf(stderr, "cur_off:%lu cur_fd:%d size:%d\n", cur_off, cur_fd,
          // value_size_);
          throw std::runtime_error("error in prep warmup\n");
        }
        cur_off += warmup_read_size;
      }
      // fprintf(stderr, "finish warmup for :%s cur_off:%lu\n", cur_path,
      // cur_off);
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(std::vector<int> &fds, size_t size, leveldb::Random &rand)
          : fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            read_size(size),
            real_read_size(size),
            is_random_size(FLAGS_value_random_size),
            rand(rand) {
#ifndef CFS_USE_POSIX
        read_buf = (char *)fs_malloc(16384);
#endif
      }
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t read_size;
      size_t real_read_size;
      std::vector<int> rand_size_candidate = {1024, 4096, 16384};
      bool is_random_size;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      char *read_buf;
      int ret{0};
      leveldb::Random &rand;
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(fd_vec, value_size_, thread->rand);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      if (ctx->is_random_size) {
        ctx->real_read_size =
            ctx->rand_size_candidate[ctx->cur_fd_idx %
                                     ctx->rand_size_candidate.size()];
        ctx->cur_off =
            ctx->rand.Next() % (ctx->max_file_size - ctx->real_read_size);
      }
      ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                                    (void *)ctx->read_buf, ctx->real_read_size,
                                    ctx->cur_off);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      // fprintf(stdout, "fd:%d off:%ld size:%lu ret:%d\n",
      //         ctx->fd_vec[ctx->cur_fd_idx], ctx->cur_off,
      //         ctx->real_read_size, ctx->ret);
      ctx->cur_fd_idx++;
      if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
        if (!ctx->is_random_size) {
          // finish this offset for all the files
          ctx->cur_off += ctx->read_size;
          if (ctx->cur_off >= ctx->max_file_size - ctx->read_size) {
            // reset, thus we read the file from the begining again
            // fprintf(stdout, "read from the begining cur_off:%lu\n",
            // ctx->cur_off);
            ctx->cur_off = 0;
          }
        }
        ctx->cur_fd_idx = 0;
      }
      return ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout, "read done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }
#ifndef CFS_USE_POSIX
    fs_free(prep_buf);
#endif

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicReadLbNc(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    fprintf(stderr, "readlbnc num_files:%d\n", FLAGS_num_files);

    fname_vec_.clear();
    int fname_aid = thread->aid;
    if (FLAGS_share_mode) {
      fname_aid = 0;
    }
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back("readn_a" + std::to_string(fname_aid) + "_f" +
                           std::to_string(i));
    }
#ifndef CFS_USE_POSIX
    char *prep_buf = (char *)fs_malloc(value_size_);
#endif

    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    size_t warmup_read_size = value_size_ > 0 ? value_size_ : 4096;
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);

#ifndef CFS_USE_POSIX
      // fetch data into server memory
      int ret;
      off_t cur_off = 0;
      while (cur_off <= FLAGS_in_mem_file_size - warmup_read_size) {
        ret = fs_allocated_pread(cur_fd, prep_buf, warmup_read_size, cur_off);
        if (ret != warmup_read_size) {
          // fprintf(stderr, "cur_off:%lu cur_fd:%d size:%d\n", cur_off, cur_fd,
          // value_size_);
          throw std::runtime_error("error in prep warmup\n");
        }
        cur_off += warmup_read_size;
      }
      // fprintf(stderr, "finish warmup for :%s cur_off:%lu\n", cur_path,
      // cur_off);
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(std::vector<int> &fds, size_t size, leveldb::Random &rand)
          : fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            read_size(size),
            read_size_vec(size, FLAGS_num_segment),
            rand(rand) {
#ifndef CFS_USE_POSIX
        read_buf = (char *)fs_malloc(128 * 1024);
        const size_t kMaxReadSize = 32 * 1024;
        const int kBSz = 1024;
        if (FLAGS_rw_align_bytes >= kBSz) {
          // read_size_vec = {size, kMaxReadSize, size};
          std::cerr << FLAGS_rw_align_bytes << std::endl;
          read_size_vec.clear();
          size_t cur_size = FLAGS_rw_align_bytes;
          while (cur_size <= kMaxReadSize) {
            read_size_vec.push_back(cur_size);
            cur_size += FLAGS_rw_align_bytes;
          }
          while (cur_size >= FLAGS_rw_align_bytes) {
            if (cur_size <= kMaxReadSize) {
              read_size_vec.push_back(cur_size);
            }
            cur_size -= FLAGS_rw_align_bytes;
          }
        }
        std::cerr << "read_size_vec sz:" << read_size_vec.size()
                  << " num_seg:" << FLAGS_num_segment << std::endl;
        for (auto sz : read_size_vec) {
          std::cerr << " sz:" << sz;
        }
        std::cerr << std::endl;
        if (read_size_vec.size() != FLAGS_num_segment) {
          throw std::runtime_error("num_seg, read_size vec not match");
        }
#endif
      }
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t read_size;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      char *read_buf;
      int ret{0};
      leveldb::Random &rand;
      int *seg_idx_ptr = nullptr;
      std::vector<size_t> read_size_vec;
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(fd_vec, value_size_, thread->rand);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      ctx->cur_off = 0;
      int cur_seg_idx = *(ctx->seg_idx_ptr);
      if (cur_seg_idx >= ctx->read_size_vec.size()) {
        fprintf(stderr, "cur_seg_idx:%d read_size_vec.size:%lu\n", cur_seg_idx,
                ctx->read_size_vec.size());
        return -1;
      }
      ctx->ret = fs_allocated_pread(
          ctx->fd_vec[ctx->cur_fd_idx], (void *)ctx->read_buf,
          ctx->read_size_vec[cur_seg_idx], ctx->cur_off);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      // fprintf(stdout, "fd:%d off:%ld size:%lu ret:%d\n",
      //         ctx->fd_vec[ctx->cur_fd_idx], ctx->cur_off,
      //         ctx->real_read_size, ctx->ret);
      ctx->cur_fd_idx++;
      if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
        ctx->cur_fd_idx = 0;
      }
      return ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    cur_ctx.seg_idx_ptr = &seg_tracker.segment_idx;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout, "read done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }
#ifndef CFS_USE_POSIX
    fs_free(prep_buf);
#endif

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicWriteLbNc(ThreadState *thread) {
    PinToCore(thread);
    initFsThreadLocal();
    if (FLAGS_dirfile_name_prefix == nullptr) {
      std::cerr << "Error, FLAGS_dirfile_name_prefix must be specified"
                << std::endl;
      return;
    }
    fprintf(stderr, " num_files:%d dir_prefix:%s\n", FLAGS_num_files,
            FLAGS_dirfile_name_prefix);

    fname_vec_.clear();
    for (int i = 0; i < FLAGS_num_files; i++) {
      fname_vec_.push_back(FLAGS_dirfile_name_prefix + std::to_string(i));
    }
#ifndef CFS_USE_POSIX
    char *prep_buf = (char *)fs_malloc(value_size_);
#endif

    sleep(1);
    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    size_t warmup_read_size = value_size_ > 0 ? value_size_ : 4096;
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);

#ifndef CFS_USE_POSIX
      // fetch data into server memory
      int ret;
      off_t cur_off = 0;
      while (cur_off <= FLAGS_in_mem_file_size - warmup_read_size) {
        ret = fs_allocated_pread(cur_fd, prep_buf, warmup_read_size, cur_off);
        if (ret != warmup_read_size) {
          fprintf(stderr, "cur_off:%lu cur_fd:%d size:%d warm_up_total:%lu\n",
                  cur_off, cur_fd, value_size_, FLAGS_in_mem_file_size);
          throw std::runtime_error("fix-sync error in prep warmup\n");
        }
        cur_off += warmup_read_size;
      }
      // fprintf(stderr, "finish warmup for :%s cur_off:%lu\n", cur_path,
      // cur_off);
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }
    SpinSleepNano(2 * 1e9);

    struct DiskReadCtx {
      DiskReadCtx(std::vector<int> &fds, size_t size, leveldb::Random &rand)
          : fd_vec(fds),
            max_file_size(FLAGS_in_mem_file_size),
            write_size(size),
            write_size_vec(size, FLAGS_num_segment),
            rand(rand) {
#ifndef CFS_USE_POSIX
        const size_t kMaxIoSize = 64 * 1024;
        write_buf = (char *)fs_malloc(kMaxIoSize);
        for (int i = 0; i < kMaxIoSize; i++) {
          write_buf[i] = (i + 'a') % 20;
        }

        const int kBSz = 1024;
        if (FLAGS_rw_align_bytes >= kBSz) {
          // read_size_vec = {size, kMaxReadSize, size};
          std::cerr << FLAGS_rw_align_bytes << std::endl;
          write_size_vec.clear();
          // first one
          // write_size_vec.push_back(kMaxIoSize);
          size_t cur_size = kMaxIoSize;
          while (cur_size > write_size) {
            write_size_vec.push_back(cur_size);
            cur_size -= FLAGS_rw_align_bytes;
          }
          write_size_vec.push_back(write_size);
          while (cur_size < kMaxIoSize) {
            if (cur_size > write_size) {
              write_size_vec.push_back(cur_size);
            }
            cur_size += FLAGS_rw_align_bytes;
          }
          // final one
          write_size_vec.push_back(kMaxIoSize);
        }
        std::cerr << "write_size_vec sz:" << write_size_vec.size()
                  << " num_seg:" << FLAGS_num_segment << std::endl;
        for (auto sz : write_size_vec) {
          std::cerr << " sz:" << sz;
        }
        std::cerr << std::endl;
        if (write_size_vec.size() != FLAGS_num_segment) {
          throw std::runtime_error("num_seg, write_size vec not match");
        }
#endif
      }
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t write_size;
      bool is_random_size;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      char *write_buf;
      int ret{0};
      leveldb::Random &rand;
      int *seg_idx_ptr = nullptr;
      std::vector<size_t> write_size_vec;
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(fd_vec, value_size_, thread->rand);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      int cur_seg_idx = *(ctx->seg_idx_ptr);
      if (cur_seg_idx >= ctx->write_size_vec.size()) {
        fprintf(stderr, "cur_seg_idx:%d read_size_vec.size:%lu\n", cur_seg_idx,
                ctx->write_size_vec.size());
        return -1;
      }
      auto cur_write_size = ctx->write_size_vec[cur_seg_idx];
      ctx->cur_off =
          (ctx->rand.Next() % (ctx->max_file_size - cur_write_size) / 4096) *
          4096;
      ctx->ret = fs_allocated_pwrite(ctx->fd_vec[ctx->cur_fd_idx],
                                     (void *)ctx->write_buf, cur_write_size,
                                     ctx->cur_off);
      if (ctx->ret < 0) {
        fprintf(stderr, "error fs_allocated_pwrite\n");
        return ctx->ret;
      }
      const int kSyncRetryNum = 3;
      int sync_try_num = 0;
    retry_for_ebadfd:
      sync_try_num++;
      ctx->ret = fs_fdatasync(ctx->fd_vec[ctx->cur_fd_idx]);
      if (ctx->ret < 0) {
        if (ctx->ret == -EBADF && sync_try_num < kSyncRetryNum) {
          SpinSleepNano(4000);
          fprintf(stderr, "get EBADF cur_retry:%d\n", sync_try_num);
          goto retry_for_ebadfd;
        }
        fprintf(stderr, "sync error, fd_idx:%d ret:%d fd:%d\n", ctx->cur_fd_idx,
                ctx->ret, ctx->fd_vec[ctx->cur_fd_idx]);
        return ctx->ret;
      }
      // fprintf(stdout, "fd:%d off:%ld size:%lu ret:%d\n",
      //         ctx->fd_vec[ctx->cur_fd_idx], ctx->cur_off, ctx->write_size,
      //         ctx->ret);
      ctx->cur_fd_idx++;
      if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
        ctx->cur_fd_idx = 0;
      }
      // ctx->cur_fd_idx = ctx->rand.Next() % ctx->fd_vec.size();
      return ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker(true);
    cur_ctx.seg_idx_ptr = &seg_tracker.segment_idx;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout, "read done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }
#ifndef CFS_USE_POSIX
    fs_free(prep_buf);
#endif

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  // iterate a bunch of files
  // can specify some number of hot files
  void BenchDynamicDiskReadIter(ThreadState *thread) {
    if (FLAGS_flist == nullptr) {
      fprintf(stderr, "must specify flist\n");
      exit(1);
    }
    PinToCore(thread);
    initFsThreadLocal();

    int n_hot_f = 0;
#define ADD_HOT_FILES
#ifdef ADD_HOT_FILES
    if (thread->aid <= 1) {
      n_hot_f = int(fname_vec_.size() * 0.8);
      fprintf(stderr, "HAS_WRITE, n_hot_f:%d\n", n_hot_f);
    }
#endif

    sleep(1);
    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      const size_t kHotBufSize = 1024 * 32;
      DiskReadCtx(std::vector<int> &fds, int num_hot_f, size_t size,
                  bool allow_rpt)
          : fd_vec(fds),
            num_hot_file(num_hot_f),
            max_file_size(FLAGS_max_file_size),
            read_size(size),
            allow_repeat(allow_rpt) {
#ifndef CFS_USE_POSIX
        read_buf = (volatile char *)fs_malloc(size);
        hot_buf = (char *)fs_malloc(kHotBufSize);
#endif
      }
      std::vector<int> &fd_vec;
      int num_hot_file;
      uint64_t max_file_size;
      size_t read_size;
      size_t hot_buf_size{kHotBufSize};
      bool allow_repeat;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      volatile char *read_buf;
      char *hot_buf;
      int ret{0};
    };

    const int kErrFileTooSmall = -999;

    bool allowRepeat = true;
    DiskReadCtx cur_ctx(fd_vec, n_hot_f, value_size_, allowRepeat);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      if (ctx->cur_fd_idx < ctx->num_hot_file) {
        ctx->ret =
            fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                               (void *)ctx->hot_buf, ctx->hot_buf_size, 0);
      } else {
        ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                                      (void *)ctx->read_buf, ctx->read_size,
                                      ctx->cur_off);
      }
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      ctx->cur_fd_idx++;
      if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
        // finish this offset for all the files
        ctx->cur_off += ctx->read_size;
        ctx->cur_fd_idx = 0;
        if (ctx->cur_off >= ctx->max_file_size - ctx->read_size) {
          if (!ctx->allow_repeat) {
            fprintf(stderr, "cur_fd_idx set to:%d\n", ctx->cur_fd_idx);
            ctx->ret = -kErrFileTooSmall;
            return -1;
          } else {
            // reset, thus we read the file from the begining again
            ctx->cur_off = 0;
          }
        }
      }
      return 0;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout, "read done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  // It's user's job to ensure the file is long enough and interval is
  // reasonable
  void BenchDynamicRead(ThreadState *thread, bool allowRepeat) {
    if (FLAGS_flist == nullptr && FLAGS_num_files == 0) {
      fprintf(stderr,
              "must specify flist or set FLAGS_num_files some value > 0\n");
      exit(1);
    }
    if (FLAGS_num_files > 0) {
      // if specify this flag, we assume using the default prefix
      fname_vec_.clear();
      for (int i = 0; i < FLAGS_num_hot_files; i++) {
        fname_vec_.push_back("readn_a" + std::to_string(thread->aid) + "_f" +
                             std::to_string(i));
      }
    }
    PinToCore(thread);
    initFsThreadLocal();

    sleep(1);
    std::vector<int> fd_vec;
    const int kPathLen = 256;
    char cur_path[kPathLen];

    // open all the files
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      // fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct DiskReadCtx {
      DiskReadCtx(std::vector<int> &fds, size_t size, bool allow_rpt)
          : fd_vec(fds),
            max_file_size(FLAGS_max_file_size),
            read_size(size),
            allow_repeat(allow_rpt) {
#ifndef CFS_USE_POSIX
        read_buf = (volatile char *)fs_malloc(size);
#endif
      }
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t read_size;
      bool allow_repeat;
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      volatile char *read_buf;
      int ret{0};
    };

    const int kErrFileTooSmall = -999;

    DiskReadCtx cur_ctx(fd_vec, value_size_, allowRepeat);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct DiskReadCtx *ctx = reinterpret_cast<DiskReadCtx *>(vctx);
      ctx->ret = fs_allocated_pread(ctx->fd_vec[ctx->cur_fd_idx],
                                    (void *)ctx->read_buf, ctx->read_size,
                                    ctx->cur_off);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      ctx->cur_off += ctx->read_size;
      if (ctx->cur_off >= ctx->max_file_size - ctx->read_size) {
        ctx->cur_fd_idx++;
        if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
          if (!ctx->allow_repeat) {
            fprintf(stderr, "cur_fd_idx set to:%d\n", ctx->cur_fd_idx);
            ctx->ret = -kErrFileTooSmall;
            return -1;
          } else {
            // reset, thus we read the first file again
            ctx->cur_fd_idx = 0;
          }
        }
        // operate on next file
        ctx->cur_off = 0;
      }
      return 0;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "read error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout, "read done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
              cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off);
    }

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  // NOTE: this benchmark issues write+fsync to the file
  // Users shall take care of the `creation` of the file
  // If the file starts with size == 0, then fsync will include the block
  // allocation
  // In case the allocation is > max supported file size, file name shall
  // be passed by FLAGS_flist
  void BenchDynamicWriteSync(ThreadState *thread) {
    if (FLAGS_flist == nullptr && FLAGS_num_files == 0) {
      fprintf(stderr, "must specify flist\n");
      exit(1);
    }
    fprintf(stdout, "writesync with sync_op:%d file_base_idx:%d\n",
            FLAGS_sync_numop, FLAGS_num_hot_files);
    PinToCore(thread);
    initFsThreadLocal();

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_FUTURE);
#endif
    }

    sleep(1);

    if (FLAGS_num_files > 0) {
      // if specify this flag, we assume using the default prefix
      // NOTE: use FLAGS_num_hot_files as the base for open()
      // to be different from append() vs. overwrite
      fname_vec_.clear();
      for (int i = 0; i < FLAGS_num_files; i++) {
        fname_vec_.push_back(std::string(FLAGS_dirfile_name_prefix) +
                             std::to_string(i + FLAGS_num_hot_files));
        // fprintf(stderr, "cur_fname:%s\n", fname_vec_[i].c_str());
      }
    }

    std::vector<int> fd_vec;
    char cur_path[128];
    for (int i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, 128);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      int cur_fd;
      fprintf(stdout, "cur_path:%s\n", cur_path);
#ifndef CFS_USE_POSIX
      cur_fd = fs_open(cur_path, O_RDWR, 0644);
#else
      cur_fd = open(cur_path, O_RDWR, 0644);
#endif
      if (cur_fd < 0) {
        fprintf(stderr, "open(%s) return error\n", cur_path);
        exit(1);
      }
      fd_vec.push_back(cur_fd);
    }

    struct WriteSyncCtx {
      WriteSyncCtx(std::vector<int> &fds, size_t size)
          : fd_vec(fds),
            max_file_size(FLAGS_max_file_size),
            write_size(size),
            no_sync(FLAGS_sync_numop == 0) {
#ifndef CFS_USE_POSIX
        write_buf = (volatile char *)fs_malloc(size);
#endif
      }
      int aid = 1000;
      std::vector<int> &fd_vec;
      uint64_t max_file_size;
      size_t write_size;
      bool no_sync{false};
      uint64_t cur_off{0};
      int cur_fd_idx{0};
      volatile char *write_buf;
      int ret{0};
    };

    const int kErrNotEnoughFileToWrite = -999;
    WriteSyncCtx cur_ctx(fd_vec, value_size_);
    cur_ctx.aid = thread->aid;

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct WriteSyncCtx *ctx = reinterpret_cast<WriteSyncCtx *>(vctx);
      ctx->ret = fs_allocated_write(ctx->fd_vec[ctx->cur_fd_idx],
                                    (void *)ctx->write_buf, ctx->write_size);
      if (ctx->ret < 0) {
        return ctx->ret;
      }
      if (!ctx->no_sync) {
        ctx->ret = fs_fdatasync(ctx->fd_vec[ctx->cur_fd_idx]);
        if (ctx->ret < 0) {
          return ctx->ret;
        }
      }
      ctx->cur_off += ctx->write_size;
      if (ctx->cur_off >= ctx->max_file_size - ctx->write_size) {
        ctx->cur_fd_idx++;
        if (ctx->cur_fd_idx == ctx->fd_vec.size()) {
          ctx->ret = kErrNotEnoughFileToWrite;
          return -1;
        }
        // operate on next file
        ctx->cur_off = 0;
      }
      return 0;
#else
      return -1;
#endif
    };
    DynamicSegmentTracker seg_tracker;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(
          stderr,
          "writesync error ret:%d fd_num:%lu fd_idx:%d cur_off:%lu aid:%d\n",
          cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx, cur_ctx.cur_off,
          thread->aid);
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }
    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(
          stdout,
          "writesync-sync?:%d done ret:%d fd_num:%lu fd_idx:%d cur_off:%lu\n",
          (!cur_ctx.no_sync), cur_ctx.ret, fd_vec.size(), cur_ctx.cur_fd_idx,
          cur_ctx.cur_off);
    }

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicCreateOrMkdir(ThreadState *thread, bool is_mkdir) {
    if (FLAGS_dirfile_name_prefix == nullptr) {
      throw std::runtime_error("must have fname prefix");
    }
    PinToCore(thread);
    initFsThreadLocal();
    sleep(1);

    struct CreateCtx {
      CreateCtx(std::string p, bool mkdir) : fname_prefix(p), is_mkdir(mkdir) {}
      const std::string fname_prefix;
      bool is_mkdir;
      int fidx{0};
      int ret{0};
      int fd{-1};
    };

    CreateCtx ctx(FLAGS_dirfile_name_prefix, is_mkdir);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct CreateCtx *ctx = reinterpret_cast<CreateCtx *>(vctx);
      std::string cur_fname = ctx->fname_prefix + std::to_string(ctx->fidx);
      if (ctx->is_mkdir) {
        ctx->ret = fs_mkdir(cur_fname.c_str(), 0);
        if (ctx->ret < 0) {
          fprintf(stderr, "cannot mkdir %s\n", cur_fname.c_str());
          return ctx->ret;
        }
      } else {
        ctx->fd = fs_open(cur_fname.c_str(), O_CREAT | O_RDWR, 0644);
        if (ctx->fd < 0) {
          fprintf(stderr, "cannot create %s\n", cur_fname.c_str());
          return ctx->fd;
        }
        ctx->ret = fs_fdatasync(ctx->fd);
        if (ctx->ret < 0) {
          fprintf(stderr, "cannot fsync\n");
          return ctx->ret;
        }
        ctx->ret = fs_close(ctx->fd);
        if (ctx->ret < 0) {
          fprintf(stderr, "cannot close\n");
          return ctx->ret;
        }
        if (ctx->fidx > 60000) {
          fprintf(stderr, "number of files overflowed\n");
        }
      }
      ctx->fidx++;
      return ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker;

    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret =
        DynamicDoBenchmarkBySegment(cur_op_func, &ctx, thread, &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "create/mkdir error ret:%d\n", ret);
    } else {
      fprintf(stdout, "create/mkdir end with fidx:%d\n", ctx.fidx);
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  void BenchDynamicCreate(ThreadState *thread) {
    BenchDynamicCreateOrMkdir(thread, /*is_mkdir*/ false);
  }

  void BenchDynamicMkdir(ThreadState *thread) {
    BenchDynamicCreateOrMkdir(thread, /*is_mkdir*/ true);
  }

  void BenchDynamicUnlink(ThreadState *thread) {
    // TODO
  }

  void BenchDynamicRename(ThreadState *thread) {
    // TODO
  }

  // create, write, sync, close
  void BenchDynamicOwsc(ThreadState *thread) {
    if (FLAGS_dirfile_name_prefix == nullptr) {
      throw std::runtime_error("must have fname perfix");
    }
    PinToCore(thread);
    initFsThreadLocal();

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_FUTURE);
#endif
    }
    sleep(1);

    struct OWSClCtx {
      OWSClCtx(std::string p, int nf, size_t ws, size_t max_fsize, int sync_nop)
          : fname_prefix(p),
            num_files(nf),
            write_size(ws),
            max_file_size(max_fsize),
            sync_numop(sync_nop) {
#ifndef CFS_USE_POSIX
        write_buf = (char *)fs_malloc(write_size);
#endif
      }

      const std::string fname_prefix;
      int num_files;
      size_t write_size;
      size_t max_file_size;
      int sync_numop;
      int fidx{0};
      int ret{0};
      int fd{-1};
      off_t off{0};
      char *write_buf;
      bool open_done = false;
      std::vector<std::string> fname_vec;
      std::vector<int> fd_vec;
    };

    // we use the numop_ flag to specify number of files
    OWSClCtx ctx(FLAGS_dirfile_name_prefix, FLAGS_num_files, value_size_,
                 FLAGS_max_file_size, FLAGS_sync_numop);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct OWSClCtx *cur_ctx = reinterpret_cast<OWSClCtx *>(vctx);
      if (cur_ctx->fidx >= cur_ctx->num_files) {
        cur_ctx->fidx = 0;
      }
      if (!cur_ctx->open_done) {
        std::string cur_fname =
            cur_ctx->fname_prefix + std::to_string(cur_ctx->fidx);
        cur_ctx->fd = fs_open(cur_fname.c_str(), O_CREAT | O_RDWR, 0644);
        cur_ctx->open_done = true;
        cur_ctx->fd_vec.push_back(cur_ctx->fd);
        cur_ctx->fname_vec.push_back(cur_fname);
        if (cur_ctx->fd < 0) {
          fprintf(stderr, "cannot create or open file. %s\n",
                  cur_fname.c_str());
          cur_ctx->ret = cur_ctx->fd;
          return -1;
        }
      }
      cur_ctx->ret = fs_allocated_write(cur_ctx->fd, (void *)cur_ctx->write_buf,
                                        cur_ctx->write_size);
      if (cur_ctx->ret < 0) {
        fprintf(stderr, "write error\n");
        return cur_ctx->ret;
      }
      cur_ctx->off += cur_ctx->write_size;

      if (cur_ctx->sync_numop > 0) {
        // if sync_numop > 0, we will treat it as 1
        cur_ctx->ret = fs_fdatasync(cur_ctx->fd);
        if (cur_ctx->ret < 0) return cur_ctx->ret;
      }

      if (cur_ctx->off >= cur_ctx->max_file_size) {
        if (cur_ctx->sync_numop <= 0) {
          cur_ctx->ret = fs_fdatasync(cur_ctx->fd);
          if (cur_ctx->ret < 0) return cur_ctx->ret;
        }

        cur_ctx->ret = fs_close(cur_ctx->fd);
        if (cur_ctx->ret < 0) return cur_ctx->ret;
        cur_ctx->fidx++;
        cur_ctx->off = 0;
        cur_ctx->open_done = false;
      }
      return cur_ctx->ret;
#else
      return -1;
#endif
    };

    DynamicSegmentTracker seg_tracker;
    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret =
        DynamicDoBenchmarkBySegment(cur_op_func, &ctx, thread, &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "cwsynccl error ret:%d numf:%d fd_idx:%d cur_off:%lu\n",
              ctx.ret, ctx.num_files, ctx.fidx, ctx.off);
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    if (ret >= 0) {
      fprintf(stdout, "cwsynccl done ret:%d numf:%u fd_idx:%d cur_off:%lu\n",
              ctx.ret, ctx.num_files, ctx.fidx, ctx.off);
    }

    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
#ifndef CFS_USE_POSIX
    if (FLAGS_pos_delete) {
      for (int i = 0; i < ctx.fname_vec.size(); i++) {
        int cur_fd = ctx.fd_vec[i];
        ret = fs_close(cur_fd);
        if (ret < 0) {
          fprintf(stderr, "cannot close ret:%d\n", ret);
        }
        ret = fs_unlink(ctx.fname_vec[i].c_str());
        if (ret < 0) {
          fprintf(stderr, "cannot unlink path:%s\n", ctx.fname_vec[i].c_str());
        }
      }
      FlushAllData(thread);
    }
#endif
  }

  void BenchDynamicReadN(ThreadState *thread) {
    BenchDynamicRead(thread, true);
  }

  // stats N files with supplied flist
  void BenchDynamicStatN(ThreadState *thread) {
    if (FLAGS_flist == nullptr) {
      fprintf(stderr, "not specify flist\n");
      if (FLAGS_num_files > 0) {
        fname_vec_.clear();
        for (int i = 0; i < FLAGS_num_files; i++) {
          fname_vec_.push_back(std::string(FLAGS_dirfile_name_prefix) +
                               std::to_string(i));
        }
      }
    }

    PinToCore(thread);
    initFsThreadLocal();
    sleep(1);

    const int kPathLen = 256;
    std::vector<std::string> path_vec;
    path_vec.reserve(fname_vec_.size());
    char cur_path[kPathLen];
    struct stat buf;

    for (uint i = 0; i < fname_vec_.size(); i++) {
      memset(cur_path, 0, kPathLen);
      sprintf(cur_path, "%s/%s", dir_.ToString().c_str(),
              fname_vec_[i].c_str());
      path_vec.push_back(cur_path);
#ifndef CFS_USE_POSIX
      int ret = fs_stat(cur_path, &buf);
      if (ret < 0) {
        fprintf(stderr, "error cannot stat %s\n", cur_path);
      } else {
        fprintf(stdout, "ok stats warmup %s\n", cur_path);
      }
#endif
    }

    if (FLAGS_wid > 0 && (!FLAGS_share_mode)) {
#ifndef CFS_USE_POSIX
      fprintf(stderr, "assign to:%d\n", FLAGS_wid);
      fs_admin_thread_reassign(0, FLAGS_wid, FS_REASSIGN_PAST);
#endif
    }

    struct StatNCtx {
      StatNCtx(std::vector<std::string> &pathv) : path_vec(pathv) {}

      const std::vector<std::string> &path_vec;
      struct stat buf;
      int fidx{0};
      int ret{0};
    };

    StatNCtx cur_ctx(path_vec);

    auto cur_op_func = [](void *vctx) -> int {
#ifndef CFS_USE_POSIX
      struct StatNCtx *statn_ctx = reinterpret_cast<StatNCtx *>(vctx);
      statn_ctx->ret = fs_stat(statn_ctx->path_vec[statn_ctx->fidx].c_str(),
                               &statn_ctx->buf);
      statn_ctx->fidx++;
      if (statn_ctx->fidx == (statn_ctx->path_vec).size()) {
        statn_ctx->fidx = 0;
      }
      return statn_ctx->ret;
#else
      return 0;
#endif
    };

    DynamicSegmentTracker seg_tracker;

    fprintf(stdout, "segment_bucket_us:%lu num_bucket_per_segment:%d\n",
            FLAGS_segment_bucket_us, seg_tracker.num_bucket_per_segment);

    SignalStartDumpLoad(thread);
    thread->stats.Start();

    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();

    seg_tracker.seg_start_ts = g_env->NowCycles();
    fprintf(stdout, "first_seg_ts:%lu num_seg:%d\n", seg_tracker.seg_start_ts,
            FLAGS_num_segment);

    int ret = DynamicDoBenchmarkBySegment(cur_op_func, &cur_ctx, thread,
                                          &seg_tracker);
    if (ret < 0) {
      fprintf(stderr, "statN error path:%s\n", path_vec[cur_ctx.fidx].c_str());
    }

    if (FLAGS_exit_delay_sec > 0) {
      SpinSleepNano(FLAGS_exit_delay_sec * 1e9);
    }

    cc.notify_server_that_client_stopped();

    SignalStopDumpLoad(thread);
    PrintDynamicBuckets(seg_tracker.seg_op_done_per_bucket,
                        seg_tracker.num_bucket_per_segment);
  }

  // Benchmarks name generation and failure checks that are present in every
  // other benchmark. Useful to see exactly how much time an operation like
  // fs_mkdir takes without the name generation and error handling.
  void BenchNameGeneration(ThreadState *thread) {
    PinToCore(thread);
    if (numop_ != FLAGS_numop) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", numop_);
      thread->stats.AddMessage(msg);
    }

    std::string basepath = dir_.ToString() + "/x";
    std::string spath;
    const char *path;
    thread->stats.Start();
    for (int i = 0; i < numop_; i++) {
      // Most benchmarks that involve generating a name have the concatenation
      // statement as well as an if condition check for return code after the
      // function.
      // TODO look at disassembled code and see that this is not optimized
      // out.
      spath = basepath + std::to_string(i);
      path = spath.c_str();
      if (path == NULL) {
        fprintf(stderr, "whoops, shouldn't have reached here\n");
        exit(1);
      }

      thread->stats.FinishedSingleOp();
    }
  }

  void BenchRamRand(ThreadState *thread) {
    PinToCore(thread);
    void *shmPtr = nullptr;
    int shmFd;
    if (thread->aid == 0) {
      shmPtr = attachPageCacheShm(true, shmFd);
    } else {
      sleep(5);
      shmPtr = attachPageCacheShm(false, shmFd);
    }
    auto layout = reinterpret_cast<struct PageCacheLayout *>(shmPtr);
    pgc_page *pageStartPtr = layout->pages;
    fprintf(stderr, "aid:%d pagestart:%p\n", thread->aid, pageStartPtr);

    const uint kWarmUpAlignSize = 4096;
    uint curWriteOpNum = FLAGS_in_mem_file_size / kWarmUpAlignSize;
    const bool shareRegion = FLAGS_share_mode;

    // warm up it to main memory
    off_t start_off = FLAGS_in_mem_file_size * thread->aid + kWarmUpAlignSize;
    uint32_t start_pgidx = start_off / kWarmUpAlignSize + 1;
    if (shareRegion) {
      start_pgidx = 0;
    }
    fprintf(stderr, "share?:%d start_off:%lu start_pgid:%u\n", shareRegion,
            start_off, start_pgidx);
    uint64_t tmp = 0;
    for (uint i = 0; i < curWriteOpNum * 10; i++) {
      // dirty the pages?
      if (thread->aid == 0) {
        memset(pageStartPtr + i, 'a' + i / 20, kWarmUpAlignSize);
        // (*(pageStartPtr + i + start_pgidx))[0] = 'a' + i / 25;
      }
    }

    __sync_synchronize();

    char data[kWarmUpAlignSize];
    memset(data, 0, kWarmUpAlignSize);

#ifdef USE_PCM_COUNTER
    PCM *m = PCM::getInstance();
    const auto cpu_model = m->getCPUModel();
    SystemCounterState sstate1, sstate2;
    std::vector<CoreCounterState> cstates1, cstates2;
    std::vector<SocketCounterState> sktstate1, sktstate2;
    if (thread->aid == 0) {
      if (m->program() != PCM::Success) {
        fprintf(stderr, "CANNOT user pcm\n");
        return;
      }
      m->getAllCounterStates(sstate1, sktstate1, cstates1);
    }
#endif
    asm volatile("" ::: "memory");
    CoordinatorClient cc(FLAGS_coordinator_shm_fname, 0);
    cc.notify_server_that_client_is_ready();
    cc.wait_till_server_says_start();
    thread->stats.Start();
    off_t cur_off = 0;
    uint cur_idx = 0;
    volatile char c;
    for (int i = 0; i < numop_; i++) {
      cur_idx = thread->rand.Next() % curWriteOpNum + start_pgidx;
      memcpy(data, (char *)(pageStartPtr + cur_idx), kWarmUpAlignSize);
      // fprintf(stdout, "cur_idx:%u ptr:%p first:%c\n", cur_idx, pageStartPtr
      // +
      //  cur_idx, data[0]);
      c = data[0];
      thread->stats.FinishedSingleOp();
      if (cc.check_server_said_stop()) break;
    }
    thread->stats.Stop();
    cc.notify_server_that_client_stopped();
#ifdef USE_PCM_COUNTER
    using std::setw;
    if (thread->aid == 0) {
      m->getAllCounterStates(sstate2, sktstate2, cstates2);
      print_per_core_metrics(m, cstates1, cstates2);
      asm volatile("" ::: "memory");
      m->cleanup();
    }
#endif
    sleep(1);
    fflush(stdout);
    if (thread->aid == 0) {
      sleep(5);
      detachPageCacheShm(true, shmFd, shmPtr);
    } else {
      detachPageCacheShm(false, shmFd, shmPtr);
    }
  }

  void CloseFile(ThreadState *thread) {
    assert(thread->fd >= 0);
    int rt;
#ifndef CFS_USE_POSIX
    rt = fs_close(thread->fd);
#else
    rt = close(thread->fd);
#endif
    if (rt < 0) {
      fprintf(stderr, "fs_close error, file cannot be closed\n");
      exit(1);
    }
  }

  void OutputCrcArr(ThreadState *thread) {
    fprintf(stdout, "======= tid:%d CRC32 START =======\n", thread->tid);
    for (int i = 0; i < numop_; i++) {
      fprintf(stdout, "%u\n", thread->crc_arr[i]);
    }
    fprintf(stdout, "======= tid:%d CRC32 END =======\n", thread->tid);
  }
};

}  // namespace leveldb

int main(int argc, char **argv) {
  for (int i = 1; i < argc; i++) {
    int n;
    uint64_t uln;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--pos_delete=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_pos_delete = n;
      if (FLAGS_pos_delete) {
        fprintf(stdout, "will do deletion after bench\n");
      }
    } else if (sscanf(argv[i], "--signowait=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_signowait = true;
      if (FLAGS_signowait) {
        fprintf(stdout, "will not SpinWait before signal to the server\n");
      }
    } else if (sscanf(argv[i], "--coordinator=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_coordinator = n;
      if (FLAGS_coordinator == false) {
        fprintf(stdout, "no coordinator\n");
      }
    } else if (sscanf(argv[i], "--o_append=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_is_append = n;
      if (FLAGS_is_append) {
        fprintf(stdout, "is_append set to true\n");
      }
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
      if (n > MAX_THREAD_NUM) {
        fprintf(stderr, "cannot use more than {%d} threads\n", n);
        exit(1);
      }
    } else if (sscanf(argv[i], "--numop=%d%c", &n, &junk) == 1) {
      FLAGS_numop = n;
    } else if (sscanf(argv[i], "--sync_numop=%d%c", &n, &junk) == 1) {
      FLAGS_sync_numop = n;
    } else if (sscanf(argv[i], "--block_no=%d%c", &n, &junk) == 1) {
      FLAGS_block_no = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--num_files=%d%c", &n, &junk) == 1) {
      FLAGS_num_files = n;
    } else if (sscanf(argv[i], "--num_hot_files=%d%c", &n, &junk) == 1) {
      FLAGS_num_hot_files = n;
    } else if (sscanf(argv[i], "--value_random_size=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_value_random_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%lu%c", &uln, &junk) == 1) {
      FLAGS_max_file_size = uln;
    } else if (sscanf(argv[i], "--in_mem_file_size=%lu%c", &uln, &junk) == 1) {
      FLAGS_in_mem_file_size = uln;
    } else if (sscanf(argv[i], "--think_nano=%lu%c", &uln, &junk) == 1) {
      FLAGS_think_nano = uln;
    } else if (sscanf(argv[i], "--wid=%d%c", &n, &junk) == 1) {
      FLAGS_wid = n;
    } else if (sscanf(argv[i], "--num_segment=%d%c", &n, &junk) == 1) {
      FLAGS_num_segment = n;
      fprintf(stdout, "FLAGS_num_segment:%d\n", FLAGS_num_segment);
    } else if (sscanf(argv[i], "--exit_delay_sec=%d%c", &n, &junk) == 1) {
      FLAGS_exit_delay_sec = n;
      fprintf(stdout, "FLAGS_exit_delay_sec:%d\n", FLAGS_exit_delay_sec);
    } else if (sscanf(argv[i], "--segment_us=%lu%c", &uln, &junk) == 1) {
      FLAGS_segment_us = uln;
      fprintf(stdout, "FLAGS_segment_us:%lu\n", FLAGS_segment_us);
    } else if (strncmp(argv[i], "--per_segment_think_ns_list=", 28) == 0) {
      char *seg_think_ns_str = argv[i] + strlen("--per_segment_think_ns_list=");
      fprintf(stdout, "seg_think_ns_str:%s\n", seg_think_ns_str);
      int num_seg = leveldb::ParseDelimittedStrList<int64_t>(
          seg_think_ns_str, ",", FLAGS_per_segment_think_ns_list,
          (gMaxRunSegmentCnt), std::atol);
      if (num_seg < 0) {
        exit(1);
      }
      for (int ii = 0; ii < num_seg; ii++) {
        fprintf(stdout, "idx:%d think_ns:%lu\n", ii,
                FLAGS_per_segment_think_ns_list[ii]);
      }
    } else if (sscanf(argv[i], "--rw_align_bytes=%d%c", &n, &junk) == 1) {
      FLAGS_rw_align_bytes = n;
    } else if (sscanf(argv[i], "--fs_worker_key_offset=%d%c", &n, &junk) == 1) {
      FLAGS_fs_worker_key_offset = n;
    } else if (strncmp(argv[i], "--fs_worker_key_list=", 21) == 0) {
      FLAGS_fs_worker_key_list = argv[i] + strlen("--fs_worker_key_list=");
      fprintf(stdout, "FLAGS_fs_worker_key_list:%s\n",
              FLAGS_fs_worker_key_list);
      char *worker_key_list_str = argv[i] + strlen("--fs_worker_key_list=");
      int num_workers = leveldb::ParseDelimittedStrList<int>(
          worker_key_list_str, ",", FLAGS_fs_worker_keys, (MAX_FSP_THREADS_NUM),
          atoi);
      if (num_workers < 0) {
        exit(1);
      }
      for (int ii = 0; ii < num_workers; ii++) {
        FLAGS_fs_worker_keys[ii] += FLAGS_fs_worker_key_base;
        // fprintf(stdout, "idx:%d key:%d\n", ii, FLAGS_fs_worker_keys[ii]);
      }
      FLAGS_fs_worker_num = num_workers;
      fprintf(stdout, "FLAGS_fs_worker_num:%d\n", FLAGS_fs_worker_num);
    } else if (sscanf(argv[i], "--share_mode=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_share_mode = n;
    } else if (sscanf(argv[i], "--stat_nocreate=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_stat_nocreate = n;
    } else if (sscanf(argv[i], "--compute_crc=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_compute_crc = n;
    } else if (sscanf(argv[i], "--rand_no_overlap=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_rand_no_overlap = n;
    } else if (strncmp(argv[i], "--dir=", 6) == 0) {
      FLAGS_dir = argv[i] + strlen("--dir=");
      fprintf(stdout, "FLAGS_dir:%s\n", FLAGS_dir);
    } else if (strncmp(argv[i], "--dir2=", 7) == 0) {
      FLAGS_dir2 = argv[i] + strlen("--dir2=");
      fprintf(stdout, "FLAGS_dir2:%s\n", FLAGS_dir2);
    } else if (strncmp(argv[i], "--fname=", 8) == 0) {
      FLAGS_fname = argv[i] + strlen("--fname=");
      fprintf(stdout, "FLAGS_fname:%s\n", FLAGS_fname);
    } else if (strncmp(argv[i], "--flist=", 8) == 0) {
      FLAGS_flist = argv[i] + strlen("--flist=");
      fprintf(stdout, "FLAGS_flist:%s\n", FLAGS_flist);
    } else if (strncmp(argv[i], "--dirfile_name_prefix=", 22) == 0) {
      FLAGS_dirfile_name_prefix = argv[i] + strlen("--dirfile_name_prefix=");
      fprintf(stdout, "FLAGS_dirfile_name_prefix:%s\n",
              FLAGS_dirfile_name_prefix);
    } else if (strncmp(argv[i], "--core_ids=", 10) == 0) {
      char *core_id_str = argv[i] + strlen("--core_ids=");
      fprintf(stdout, "core_id_str:%s\n", core_id_str);
      int num_core_id = leveldb::ParseDelimittedStrList<int>(
          core_id_str, ",", FLAGS_core_ids, (MAX_THREAD_NUM), atoi);
      if (num_core_id < 0) {
        exit(1);
      }
      for (int ii = 0; ii < num_core_id; ii++) {
        fprintf(stdout, "idx:%d core_id:%d\n", ii, FLAGS_core_ids[ii]);
      }
    } else if (strncmp(argv[i],
                       "--coordinator=", sizeof("--coordinator=") - 1) == 0) {
      FLAGS_coordinator_shm_fname = argv[i] + sizeof("--coordinator=") - 1;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  if (FLAGS_coordinator_shm_fname == nullptr)
    FLAGS_coordinator_shm_fname = strdup("/coordinator");

  fprintf(stdout, "kUseCCacheLease set to:%d\n", kUseCCacheLease);
  fprintf(stdout, "kUseCCacheLeasePosix set to :%d\n", kUseCCacheLeasePosix);

  leveldb::g_env = leveldb::Env::Default();
  leveldb::Benchmark benchmark;
  benchmark.Run();

  adgMod::Stats *instance = adgMod::Stats::GetInstance();
  instance->ReportTime();

  return 0;
}
