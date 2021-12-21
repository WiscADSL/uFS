#ifndef CFS_INCLUDE_FSPERMISSION_H_
#define CFS_INCLUDE_FSPERMISSION_H_

#include <folly/concurrency/ConcurrentHashMap.h>
#include <x86intrin.h>

#include <mutex>

class InMemInode;

class FsPermission {
 public:
  static FsPermission* getInstance();

  typedef enum permissionCheckResult { OK = 0, DENIED = 1, NOTFOUND = 2 } PCR;
  typedef std::pair<int, int> Credential;

  class LevelMap;

  // A MapEntry is the content for each path cache item
  // An object of this will be returned by deleteEntry
  typedef std::pair<LevelMap*, std::pair<Credential, InMemInode*>> MapEntry;

  // LevelMap is the map for a directory
  // It is a certificate required for set/delete on non-root directory
  class LevelMap {
   public:
    folly::ConcurrentHashMap<std::string, FsPermission::MapEntry> map;
  };

  ~FsPermission();

  // path should be a vector of tokens along the full path
  // the function will return the result of permission check
  // and assign the LevelMap for the parent directory
  // and the inode for parent and target.
  PCR checkPermission(const std::vector<std::string>& path,
                      Credential credential, LevelMap** parentMap,
                      InMemInode** parentInode, InMemInode** inode) {
    LevelMap* target = rootMap;
    if (path.size() == 1) *parentMap = rootMap;
    for (size_t i = 0; i < path.size(); ++i) {
      auto it = target->map.find(path[i]);
      if (it == target->map.end()) {
        return NOTFOUND;
      } else {
        // TODO: add real permission check
        target = it->second.first;
        if (i == path.size() - 2) {
          *parentMap = target;
          *parentInode = it->second.second.second;
        } else if (i == path.size() - 1) {
          *inode = it->second.second.second;
        }
      }
    }
    return OK;
  }

  // Insert the path into the target dir map
  // In the first override, a new map will be created only if
  // the inode represents a directory
  // The second override is for rename, which already have a MapEntry
  // Returned by a previous deleteEntry
  bool setPermission(LevelMap* dir, const std::string& path,
                     Credential credential, InMemInode* inode,
                     LevelMap** dirMap);

  bool setPermission(LevelMap* dir, const std::string& path, MapEntry entry,
                     LevelMap** dirMap) {
    LevelMap* target = dir == nullptr ? rootMap : dir;
    auto it = target->map.insert(path, entry);
    *dirMap = it.first->second.first;
    return it.second;
  }

  // Delete the path entry from the target dir map
  // return whether deletion is successful and assign the deleted
  // MapEntry to entry, which may be used for further
  // setPermission or registerGC
  bool deleteEntry(LevelMap* dir, const std::string& path, MapEntry* entry) {
    LevelMap* target = dir == nullptr ? rootMap : dir;
    auto it = target->map.find(path);
    if (it != target->map.end()) {
      *entry = it->second;
      target->map.erase(it);
      return true;
    }
    return false;
  }

  // A map entry deleted must be re-inserted or registered for garbage
  // collection. The collector will run for the registered entry
  // at least gc_threhold ns later to avoid race condition.
  void RegisterGC(LevelMap* garbage) {
    garbage_new.push_back(garbage);
    if (__rdtsc() - garbage_last_time > k_gc_threhold) {
      garbage_collect();
      garbage_last_time = __rdtsc();
    }
  }

 private:
  void garbage_collect() {
    while (!garbage_old.empty()) {
      LevelMap* garbage = garbage_old.back();
      garbage_old.pop_back();
      if (garbage != nullptr) {
        for (auto& elem : garbage->map) {
          LevelMap* temp = elem.second.first;
          if (temp) garbage_old.push_back(temp);
        }
        delete garbage;
      }
    }
    garbage_old = std::move(garbage_new);
    garbage_new.clear();
  }

  FsPermission() : garbage_last_time(0), rootMap(new LevelMap){};

  static FsPermission* instance;
  static std::mutex instance_lock;

  static constexpr uint64_t k_gc_threhold = 1000000000;
  uint64_t garbage_last_time;
  std::vector<LevelMap*> garbage_new;
  std::vector<LevelMap*> garbage_old;

  LevelMap* rootMap;
};

#endif  // CFS_INCLUDE_FSPERMISSION_H_
