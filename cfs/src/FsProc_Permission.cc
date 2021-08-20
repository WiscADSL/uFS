#include "FsProc_Permission.h"
#include "FsProc_FsImpl.h"

FsPermission* FsPermission::instance = nullptr;
std::mutex FsPermission::instance_lock;

FsPermission* FsPermission::getInstance() {
  if (instance != nullptr) return instance;
  std::lock_guard<std::mutex> guard(instance_lock);
  if (instance == nullptr) instance = new FsPermission;
  return instance;
}

FsPermission::~FsPermission() {
  instance = nullptr;
  garbage_collect();
  garbage_collect();
  RegisterGC(rootMap);
  garbage_collect();
  garbage_collect();
}

bool FsPermission::setPermission(LevelMap* dir, const std::string& path,
                                 Credential credential, InMemInode* inode,
                                 LevelMap** dirMap) {
  LevelMap* target = dir == nullptr ? rootMap : dir;
  LevelMap* map = inode->inodeData->type == T_FILE ? nullptr : new LevelMap;
  auto it = target->map.insert(
      path, std::make_pair(map, std::make_pair(credential, inode)));
  if (!it.second) delete map;
  *dirMap = it.first->second.first;
  return it.second;
}
