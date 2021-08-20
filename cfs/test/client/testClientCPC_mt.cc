
#include <thread>
#include <vector>

#include "fcntl.h"
#include "fsapi.h"
#include "sys/stat.h"
#include "sys/types.h"
#include "testClient_common.h"
#include <algorithm>

int myrandom(int i) { return rand() % i; }

void file_reader(char* fname) {
  void* ptr = fs_malloc(1024);
  fs_free(ptr);
  int fd = fs_open2(fname, O_RDWR);
  struct stat stbuf;
  int rt = fs_stat(fname, &stbuf);
  if (rt < 0) {
    fprintf(stderr, "fs_stat error\n");
    return;
  }
  auto sz = stbuf.st_size;
  const int kReadSize = 8000;
  std::vector<off_t> offVec;
  // fs_cpc_pread(fd, count, off);
  off_t i = 0;
  while (i * kReadSize < sz) {
    offVec.push_back(i * kReadSize);
    i++;
  }
  fprintf(stdout, "fsize:%lu numOp:%lu\n", sz, offVec.size());
  char* data = (char*)fs_malloc_pad(kReadSize);
  std::random_shuffle(offVec.begin(), offVec.end(), myrandom);
  for (int ii = 0; ii < 2; ii++) {
    for (auto off : offVec) {
      rt = fs_cpc_pread(fd, data, kReadSize, off);
      if (rt < 0) {
        fprintf(stderr, "cpc_pread error off:%ld\n", off);
        fs_free_pad(data);
        return;
      }
    }
  }
  fprintf(stderr, "PASS\n");
  fs_free_pad(data);
}

int main() {
  key_t shmKeys[] = {FS_SHM_KEY_BASE + 1};
  int rt = fs_init_multi(1, shmKeys);
  if (rt < 0) {
    exit(1);
  }
  char f1_name[] = "bench_f_0";
  char f2_name[] = "bench_f_1";
  std::thread t1(file_reader, f1_name);
  std::thread t2(file_reader, f2_name);
  // std::thread t2(file_reader, f1_name);
  t1.join();
  t2.join();
  fs_exit();
  return 0;
}