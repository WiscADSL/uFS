#ifndef CFS_INCLUDE_FSLIBLEASE_H_
#define CFS_INCLUDE_FSLIBLEASE_H_

#include <fcntl.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "FsLibLeaseShared.h"
#include "FsProc_App.h"
#include "absl/container/flat_hash_map.h"
#include "fsapi.h"
#include "param.h"
#include "tbb/concurrent_unordered_map.h"

class FsLibLeaseMng {
 public:
  FsLibLeaseMng() { lc = FsLeaseCommon::getInstance(); }

  void updateFileTs(int fhid, FsLeaseCommon::rdtscmp_ts_t ts) {
    fhidTsMap_[fhid] = ts;
  }

  bool ifFileLeaseValid(int fhid) {
    FsLeaseCommon::rdtscmp_ts_t ts = fhidTsMap_[fhid];
    FsLeaseCommon::rdtscmp_ts_t curTs = FsLeaseCommon::genTimestamp();
    return !(lc->checkIfLeaseExpire(ts, curTs));
  }

 private:
  FsLeaseCommon* lc;
  // <fd, timestamp>
  tbb::concurrent_unordered_map<int, FsLeaseCommon::rdtscmp_ts_t> fhidTsMap_;
};

struct LocalFileObj {
  bool in_use;
  int flags;
  mode_t mode;
  uint64_t offset;
};

class OpenLease {
 public:
  static constexpr int local_fd_offset = 10000000;
  static constexpr uint64_t lease_term = 1000000000;

  static int FindOffset(int fd) { return (fd / local_fd_offset) % 10; }

  static int FindBaseFd(int fd) {
    return fd - FindOffset(fd) * local_fd_offset;
  }

  static bool IsLocalFd(int fd) { return FindOffset(fd) != 0; }

 private:
  int base_fd;
  std::string path;

 public:
  uint64_t size;

 private:
  uint64_t lease_start;
  // operations of the following two variables should be locked
  FreeList free_list;
  LocalFileObj* local_file_objects;

 public:
  OpenLease(int basefd, const std::string& standard_path, uint64_t file_size)
      : base_fd(basefd),
        path(standard_path),
        size(file_size),
        free_list(LOCAL_OPEN_LIMIT - 1),
        local_file_objects((LocalFileObj*)fs_malloc((LOCAL_OPEN_LIMIT) *
                                                    sizeof(LocalFileObj))) {
    for (int i = 0; i < LOCAL_OPEN_LIMIT; ++i) {
      local_file_objects[i].in_use = false;
    }
    lease_start = PerfUtils::Cycles::rdtscp();
  }

  ~OpenLease() { fs_free(local_file_objects); }

  int Open(int flags, mode_t mode) {
    int offset = free_list.Pop();
    LocalFileObj* dest = &local_file_objects[offset];
    assert(!dest->in_use);
    dest->in_use = true;
    dest->flags = flags;
    dest->mode = mode;
    dest->offset = flags & O_APPEND ? size : 0;
    return base_fd + offset * local_fd_offset;
  }

  int Close(int offset) {
    free_list.Return(offset);
    LocalFileObj* dest = &local_file_objects[offset];
    assert(dest->in_use);
    dest->in_use = false;
    return 0;
  }

  const std::string& GetPath() { return path; }

  uint64_t GetLeaseTimestamp() { return lease_start; }

  void SetLeaseTimestamp(uint64_t value) { lease_start = value; }

  bool Expired(uint64_t current) {
    return PerfUtils::Cycles::toMicroseconds(current - lease_start) >
           lease_term;
  }

  LocalFileObj* GetFileObjects() { return local_file_objects; }
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpointer-arith"
#pragma GCC diagnostic ignored "-Wsign-compare"

class LDBLease {
  enum State { read = 0, write = 1, fsync = 2 };

 private:
  static constexpr int magic = 10000;
  size_t file_size;
  std::vector<std::pair<void*, uint16_t>> data_array;
  // absl::flat_hash_map<uint32_t, uint16_t> unfull_blocks;

  std::shared_mutex file_lock;

  inline void CheckArraySize(size_t size_required) {
    if (data_array.size() < size_required) {
      data_array.resize(size_required,
                        std::make_pair<void*, uint16_t>(nullptr, 0));
    }
  }

 public:
  LDBLease(size_t si) : file_size(si){};

  bool Read(void* buf, size_t count, off_t offset, size_t* miss_count,
            off_t* miss_offset, int* num_hit) {
    size_t block_start_index = offset / gFsLibMallocPageSize;
    size_t block_start_offset = offset % gFsLibMallocPageSize;
    size_t block_end_index = (offset + count - 1) / gFsLibMallocPageSize;
    size_t block_end_offset = (offset + count - 1) % gFsLibMallocPageSize;

    std::shared_lock<std::shared_mutex> guard(file_lock);

    CheckArraySize(block_end_index + 1);

    for (int i = block_start_index; i <= block_end_index; ++i) {
      auto& pair = data_array[i];
      void*& current_data = pair.first;
      uint16_t fill_size = pair.second % magic;

      if (current_data) {
        // uint16_t page_fill_size = gFsLibMallocPageSize;
        // if (i == data_array.size() - 1 || data_array[i+1] == nullptr) {
        //   auto it = unfull_blocks.find(i);
        //   if (it != unfull_blocks.end()) {
        //     page_fill_size = it->second;
        //   }
        // }

        void* src = i == block_start_index ? current_data + block_start_offset
                                           : current_data;
        void* src_end = i == block_end_index
                            ? current_data + block_end_offset
                            : current_data + gFsLibMallocPageSize - 1;
        size_t copy_size = (char*)src_end - (char*)src + 1;
        // if (copy_size > page_fill_size) {
        //   *miss_count = count + block_start_offset - gFsLibMallocPageSize *
        //   (block_start_index - i); *miss_offset = gFsLibMallocPageSize * 1;
        //   return false;
        // }
        if ((char*)src_end - (char*)current_data >= fill_size) {
          *miss_count = count + block_start_offset -
                        gFsLibMallocPageSize * (block_start_index - i);
          *miss_offset = gFsLibMallocPageSize * i;
          *num_hit = i - block_start_index;
          return false;
        }

        void* dst = i == block_start_index
                        ? buf
                        : buf + gFsLibMallocPageSize * (i - block_start_index) -
                              block_start_offset;
        memcpy(dst, src, copy_size);
      } else {
        *miss_count = count + block_start_offset -
                      gFsLibMallocPageSize * (block_start_index - i);
        *miss_offset = gFsLibMallocPageSize * i;
        *num_hit = i - block_start_index;
        return false;
      }
    }

    return true;
  }

  void Write(void* buf, size_t count, int tid) {
    // Only valid when the write is an append
    size_t block_start_index = file_size / gFsLibMallocPageSize;
    size_t block_start_offset = file_size % gFsLibMallocPageSize;
    size_t block_end_index = (file_size + count - 1) / gFsLibMallocPageSize;

    CheckArraySize(block_end_index + 1);

    for (int i = block_start_index; i <= block_end_index; ++i) {
      auto& pair = data_array[i];
      void*& current_data = pair.first;
      uint16_t& fill_size = pair.second;

      if (!current_data) {
        current_data = fs_malloc(gFsLibMallocPageSize);
        fill_size = magic * tid;
      }

      void* dst = i == block_start_index ? current_data + block_start_offset
                                         : current_data;
      void* src = i == block_start_index
                      ? buf
                      : buf + gFsLibMallocPageSize * (i - block_start_index) -
                            block_start_offset;
      void* src_end =
          i == block_end_index
              ? buf + count - 1
              : buf + gFsLibMallocPageSize * (i - block_start_index + 1) -
                    block_start_offset - 1;
      size_t copy_size = (char*)src_end - (char*)src + 1;
      memcpy(dst, src, copy_size);
      fill_size = fill_size / magic * magic + gFsLibMallocPageSize;
    }

    file_size += count;
  }

  void Write(void* buf, size_t count, off_t offset, int tid) {
    assert(offset % gFsLibMallocPageSize == 0);

    size_t block_start_index = offset / gFsLibMallocPageSize;
    size_t block_end_index = (offset + count - 1) / gFsLibMallocPageSize;

    std::unique_lock<std::shared_mutex> guard(file_lock);

    CheckArraySize(block_end_index + 1);

    for (int i = block_start_index; i <= block_end_index; ++i) {
      auto& pair = data_array[i];
      void*& current_data = pair.first;
      uint16_t& fill_size = pair.second;

      if (!current_data) {
        current_data = fs_malloc(gFsLibMallocPageSize);
        fill_size = magic * tid;
      }

      void* dst = current_data;
      void* src = buf + gFsLibMallocPageSize * (i - block_start_index);
      void* src_end =
          i == block_end_index
              ? buf + count - 1
              : buf + gFsLibMallocPageSize * (i - block_start_index + 1) - 1;
      size_t copy_size = (char*)src_end - (char*)src + 1;

      int current_magic = fill_size / magic * magic;
      int real_size = fill_size - current_magic;
      if (copy_size > real_size) {
        fill_size = current_magic + copy_size;
        memcpy(dst + real_size, src + real_size, copy_size - real_size);
      }

      // if (i == block_end_index && copy_size != gFsLibMallocPageSize) {
      //   auto it = unfull_blocks.find(i);
      //   if (it == unfull_blocks.end()) {
      //     unfull_blocks.emplace(i, copy_size);
      //   } else if (it->second < copy_size) {
      //     it->second = copy_size;
      //   }
      // } else {
      //   unfull_blocks.erase(i);
      // }
    }
  }

  void* GetData(FsLibMemMng* mem, size_t* asize, size_t* fsize) {
    size_t data_array_size = data_array.size();
    int err = 0;
    if (data_array_size == 0) {
      *asize = 0;
      *fsize = 0;
      return nullptr;
    }

    struct wsyncAlloc* array = (struct wsyncAlloc*)fs_malloc(
        data_array_size * sizeof(struct wsyncAlloc));
    for (int i = 0; i < data_array_size; ++i) {
      mem->getBufOwnerInfo(data_array[i].first, false, array[i].shmid,
                           array[i].dataPtrId, err);
    }

    *asize = data_array_size;
    *fsize = file_size;
    return array;
  }

  ~LDBLease() {
    for (auto& pair : data_array) {
      if (pair.first != nullptr) fs_free(pair.first, pair.second / magic);
    }
  }
};

#pragma GCC diagnostic pop

#endif  // CFS_INCLUDE_FSLIBLEASE_H_
