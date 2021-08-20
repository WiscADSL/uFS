//
// This program dumps the whole contents of FSP to a ordinary location that can be normally accessed.
//

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <cstdio>
#include <fsapi.h>
#include <cstring>
#include <cassert>
#include <vector>
#include <sstream>

#include "testClient_common.h"
#include "util.h"

#define COPY_BUF_SIZE 4096

int dir_fd = 0;

void print_usage(char *basename) {
  fprintf(stderr, "Usage: %s <app pid> <destination dir>\n", basename);
}

/**
 * Copies a file from FSP to VFS
 * @param path relative path from root of FSP to copy
 * @return 0 on success
 */
int copy_file(const std::string &path) {
  // Open original file from FSP for reading
  int orig_fd = fs_open(path.c_str(), O_RDONLY, 0);
  if (orig_fd == -1) {
    fprintf(stderr, "Cannot open file on FSP: %s", path.c_str());
  }

  // Create a new file for writing
  int dest_fd = openat(dir_fd, path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644); // NOLINT(hicpp-signed-bitwise)
  if (dest_fd == -1) {
    if (errno == EEXIST) {
      fprintf(stderr, "WARNING: Overwriting existing file %s\n", path.c_str());
      dest_fd = openat(dir_fd, path.c_str(), O_WRONLY, 0644);
    } else {
      perror(("Cannot create regular file " + path).c_str());
      fs_close(orig_fd);
      return -1;
    }
  }

  std::cout << "FILE " << path << std::endl;
  // Copy
  char buf[COPY_BUF_SIZE];
  while (ssize_t size_read = fs_read(orig_fd, buf, COPY_BUF_SIZE)) {
    if (size_read == -1) {
      fprintf(stderr, "Error occurred when reading from CFS\n");
      return -1;
    }
    ssize_t size_written = write(dest_fd, buf, size_read);
    if (size_written == -1) {
      perror("Error occurred when writing output");
      return -1;
    }
  }
  close(dest_fd);
  fs_close(orig_fd);

  return 0;
}

/**
 * Copies a directory and its content from FSP to VFS
 * @param path relative path from root of FSP to copy
 * @return 0 on success
 */
int copy_directory(const std::string &path) {
  // List directory
  auto pwd = fs_opendir(path.c_str());
  struct dirent *dp;

  // Create the same directory on the VFS side
  if (path != ".") {
    if (mkdirat(dir_fd, path.c_str(), 0777) == -1) {
      if (errno != EEXIST) {
        perror(("Cannot create directory " + path).c_str());
        return -1;
      } else {
        // Check if it's already an DIR
        struct stat buf;
        if (fstatat(dir_fd, path.c_str(), &buf, 0) == -1) {
          perror(("Failed to stat " + path).c_str());
          return -1;
        }
        if (!(buf.st_mode & S_IFDIR)) {
          fprintf(stderr, "Path exists and is not a directory: %s\n", path.c_str());
        } else {
          fprintf(stderr, "WARNING: Using existing directory %s\n", path.c_str());
        }
      }
    } else {
      std::cout << "DIR  " << path << std::endl;
    }
  }

  while ((dp = fs_readdir(pwd))) {
    // Handle each sub-inode
    if (strcmp(dp->d_name, "..") == 0 || strcmp(dp->d_name, ".") == 0) {
      continue;
    }
    std::string new_path = path + "/" + dp->d_name;
    // Stat this thing and see if it is a directory
    struct stat orig_stat{};
    if (fs_stat(new_path.c_str(), &orig_stat)) {
      fprintf(stderr, "Failed to stat %s\n", new_path.c_str());
      fs_closedir(pwd);
      return -1;
    }

    if (S_ISDIR(orig_stat.st_mode)) { // NOLINT(hicpp-signed-bitwise)
      if (copy_directory(new_path) == -1) {
        fs_closedir(pwd);
        return -1;
      }
    } else if (S_ISREG(orig_stat.st_mode)) { // NOLINT(hicpp-signed-bitwise)
      if (copy_file(new_path) == -1) {
        fs_closedir(pwd);
        return -1;
      }
    } else {
      printf("WTF? %s has unknown st_mode %i\n", dp->d_name, orig_stat.st_mode);
    }
  }

  fs_closedir(pwd);
  return 0;
}

int main(int argc, char **argv) {
  if (argc != 3) {
    print_usage(argv[0]);
    return -1;
  }
  char *dest_dir = argv[2];

  // Make sure that the output directory exists
  dir_fd = open(dest_dir, O_DIRECTORY | O_CLOEXEC | O_RDONLY); // NOLINT(hicpp-signed-bitwise)
  if (dir_fd == -1) {
    perror("Cannot open output directory");
    return -1;
  }

  // Connect to FSP
  const int kMaxNumFsp = 10;
  key_t shmKeys[kMaxNumFsp];
  for (int & shmKey : shmKeys) shmKey = 0;
  auto pidStrVec = splitStr(argv[1], ',');
  int numKey = pidStrVec.size();
  assert(numKey <= kMaxNumFsp);
  for (int i = 0; i < numKey; i++) {
    shmKeys[i] = (FS_SHM_KEY_BASE) + stoiWrapper(pidStrVec[i]);
  }
  int rt = fs_init_multi(numKey, shmKeys);
  if (rt < 0) {
    fprintf(stderr, "Cannot connect to FS\n");
    return -1;
  }
  // int rt = fs_init(FS_SHM_KEY_BASE + fs_pid);

  int ret = copy_directory(".");
  fs_exit();
  return ret;
}
