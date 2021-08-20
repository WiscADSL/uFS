
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "fsapi.h"
#include "testClient_common.h"
#include "util.h"

//
// A simple CLI to test the functionality of file system
//

bool interactive = true;

#ifdef TEST_VFS_INSTEAD
#define fs_read read
#define fs_pread pread
#define fs_cached_posix_pread pread
#define fs_write write
#define fs_pwrite pwrite
#define fs_readdir readdir
#define fs_fstat fstat
#define fs_close close
#define fs_fdatasync fdatasync
#define fs_syncall sync
#define fs_syncunlinked sync
#define fs_ping sync
#define fs_dumpinodes
#define fs_migrate
#define fs_start_dump_load_stats
#define fs_stop_dump_load_stats
void clean_exit() { exit(0); };
int cur_dir_fd = 0;
#else
void clean_exit() {
  int ret = fs_exit();
  if (ret < 0) exit(ret);
  exit(fs_cleanup());
};
#endif

void handle_sigint(int sig);

void printHelp() {
  printf(ANSI_COLOR_CYAN "Help:" ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_GREEN "create" ANSI_COLOR_RESET " <PATH & FILENAME>\n");
  printf(ANSI_COLOR_GREEN "open" ANSI_COLOR_RESET " <PATH & FILENAME>\n");
  printf(ANSI_COLOR_GREEN "read" ANSI_COLOR_RESET " <fd> <sizeByte>\n");
  printf(ANSI_COLOR_GREEN "pread" ANSI_COLOR_RESET
                          " <fd> <sizeByte> <offset>\n");
#ifndef TEST_VFS_INSTEAD
  printf(ANSI_COLOR_GREEN "allocread" ANSI_COLOR_RESET " <fd> <sizeByte>\n");
  printf(ANSI_COLOR_GREEN "cacheread" ANSI_COLOR_RESET " <fd> <sizeByte>\n");
  printf(ANSI_COLOR_GREEN "allocpread" ANSI_COLOR_RESET
                          " <fd> <sizeByte> <offset>\n");
  printf(ANSI_COLOR_GREEN "cachepread" ANSI_COLOR_RESET
                          " <fd> <sizeByte> <offset>\n");
#endif
  printf(ANSI_COLOR_GREEN "write" ANSI_COLOR_RESET " <fd> <Str>\n");
  printf(ANSI_COLOR_GREEN "writen" ANSI_COLOR_RESET " <fd> <size> <c>\n");
  printf(ANSI_COLOR_GREEN "writenr" ANSI_COLOR_RESET " <fd> <size>\n");
  printf(ANSI_COLOR_GREEN "pwrite" ANSI_COLOR_RESET " <fd> <Str> <offset>\n");
#ifndef TEST_VFS_INSTEAD
  printf(ANSI_COLOR_GREEN "allocwrite" ANSI_COLOR_RESET " <fd> <Str>\n");
  printf(ANSI_COLOR_GREEN "allocpwrite" ANSI_COLOR_RESET
                          " <fd> <Str> "
                          "<offset>\n");
#endif
  printf(ANSI_COLOR_GREEN "close" ANSI_COLOR_RESET " <fd>\n");
  printf(ANSI_COLOR_GREEN "fdatasync" ANSI_COLOR_RESET " <fd>\n");
  printf(ANSI_COLOR_GREEN "stat" ANSI_COLOR_RESET " <PATH & FILENAME>\n");
  printf(ANSI_COLOR_GREEN "fstat" ANSI_COLOR_RESET " <fd>\n");
  printf(ANSI_COLOR_GREEN "mkdir" ANSI_COLOR_RESET " <PATH & FILENAME>\n");
  printf(ANSI_COLOR_GREEN "unlink" ANSI_COLOR_RESET " <PATH & FILENAME>\n");
  printf(ANSI_COLOR_GREEN "rename" ANSI_COLOR_RESET " <old path> <new path>\n");
  printf(ANSI_COLOR_GREEN "lsdir" ANSI_COLOR_RESET " <dir path> \n");
  printf(ANSI_COLOR_GREEN "statall" ANSI_COLOR_RESET " <dir path> \n");
  printf(ANSI_COLOR_GREEN "syncall" ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_GREEN "syncunlinked" ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_GREEN "ping" ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_GREEN "dumpinodes" ANSI_COLOR_RESET " <wid>\n");
  printf(ANSI_COLOR_GREEN "migrate" ANSI_COLOR_RESET " <fd>\n");
  printf(ANSI_COLOR_GREEN "<start|stop>_dump_load" ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_GREEN "inode-ownership-check" ANSI_COLOR_RESET
                          " <inode> <wid>\n");
  printf(ANSI_COLOR_GREEN "inode-ownership-set" ANSI_COLOR_RESET
                          " <inode> <curOwner> <newOwner>\n");
  printf(ANSI_COLOR_GREEN "thread_reassign" ANSI_COLOR_RESET
                          " <src_wid> <dst_wid>\n");
}

void printReturnValue(std::string const &cmd, ssize_t v) {
  if (interactive) {
    printf(ANSI_COLOR_MAGENTA "%s Return:" ANSI_COLOR_RESET "\n", cmd.c_str());
    printf(ANSI_COLOR_BLUE "%ld" ANSI_COLOR_RESET "\n", v);
    printf(ANSI_COLOR_MAGENTA "-------" ANSI_COLOR_RESET "\n");
  } else {
    printf("return %ld\n", v);
    fflush(stdout);
  }
}

static void randStr(char *dest, size_t length) {
  char charset[] =
      "0123456789"
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  while (length-- > 0) {
    size_t index = (double)rand() / RAND_MAX * (sizeof charset - 1);
    *dest++ = charset[index];
  }
  *dest = '\0';
}

static uint64_t write_char_no = 0;

void process(std::string const &line) {
  if (line == "help") {
    printHelp();
    return;
  } else if (line == "quit" || line == "exit") {
    clean_exit();
  }

  std::istringstream iss(line);
  std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                  std::istream_iterator<std::string>{}};
  if (!tokens.empty()) {
    if (tokens[0] == "create") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int ret = fs_open(tokens[1].c_str(), O_CREAT, 0644);
#else
        int ret = openat(cur_dir_fd, tokens[1].c_str(), O_CREAT | O_RDWR, 0644);
#endif
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "open") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int ret = fs_open(tokens[1].c_str(), O_RDWR, 0);
#else
        int ret = openat(cur_dir_fd, tokens[1].c_str(), O_RDWR, 0);
#endif
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "read") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        char *buf = (char *)malloc(count + 1);
        memset(buf, 0, count + 1);
        ssize_t ret = fs_read(fd, buf, count);
        // print partial of read-in result
        int print_cnt = count > 100 ? 100 : count;
        if ((size_t)ret == count) {
          for (int ii = 0; ii < print_cnt; ii++) {
            std::cout << buf[ii];
          }
          std::cout << std::endl;
        }
        printReturnValue(tokens[0], ret);
        free(buf);
      }
    } else if (tokens[0] == "pread") {
      if (tokens.size() != 4) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        off_t offset = stollWrapper(tokens[3]);
        char *buf = (char *)malloc(count + 1);
        memset(buf, 0, count + 1);
        ssize_t ret = fs_pread(fd, buf, count, offset);
        // NOTE: if testing posix_pread, use this
        // ssize_t ret = fs_cached_posix_pread(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        // print partial of read-in result
        int print_cnt = count > 100 ? 100 : count;
        if ((size_t)ret == count) {
          for (int ii = 0; ii < print_cnt; ii++) {
            std::cout << buf[ii];
          }
          std::cout << std::endl;
        }
        free(buf);
      }
#ifndef TEST_VFS_INSTEAD
    } else if (tokens[0] == "allocread") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        char *buf = (char *)fs_malloc(count + 1);
        memset(buf, 0, count + 1);
        ssize_t ret = fs_allocated_read(fd, buf, count);
        printReturnValue(tokens[0], ret);
        int print_cnt = count > 100 ? 100 : count;
        if ((size_t)ret == count) {
          for (int ii = 0; ii < print_cnt; ii++) {
            std::cout << buf[ii];
          }
          std::cout << std::endl;
        }
        fs_free(buf);
      }
    } else if (tokens[0] == "cacheread") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        char *buf = (char *)fs_malloc(count + 1);
        memset(buf, 0, count + 1);
        void *tmpBuf = static_cast<void *>(buf);
        ssize_t ret = fs_cached_read(fd, &tmpBuf, count);
        char *retDataBuf = static_cast<char *>(tmpBuf);
        printReturnValue(tokens[0], ret);
        int print_cnt = count > 100 ? 100 : count;
        if ((size_t)ret == count) {
          for (int ii = 0; ii < print_cnt; ii++) {
            std::cout << retDataBuf[ii];
          }
          std::cout << std::endl;
        }
        // fs_free(buf);
      }
    } else if (tokens[0] == "cachepread") {
      if (tokens.size() != 4) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        off_t offset = stollWrapper(tokens[3]);
        char *buf = (char *)fs_malloc(count + 1);
        memset(buf, 0, count + 1);
        void *tmpBuf = static_cast<void *>(buf);
        ssize_t ret = fs_cached_pread(fd, &tmpBuf, count, offset);
        char *retDataBuf = static_cast<char *>(tmpBuf);
        printReturnValue(tokens[0], ret);
        int print_cnt = count > 100 ? 100 : count;
        if ((size_t)ret == count) {
          for (int ii = 0; ii < print_cnt; ii++) {
            std::cout << retDataBuf[ii];
          }
          std::cout << std::endl;
        }
        // fs_free(buf);
      }
    } else if (tokens[0] == "allocpread") {
      if (tokens.size() != 4) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        off_t offset = stollWrapper(tokens[3]);
        char *buf = (char *)fs_malloc(count + 1);
        memset(buf, 0, count + 1);
        ssize_t ret = fs_allocated_pread(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        int print_cnt = count > 100 ? 100 : count;
        if ((size_t)ret == count) {
          for (int ii = 0; ii < print_cnt; ii++) {
            std::cout << buf[ii];
          }
          std::cout << std::endl;
        }
        fs_free(buf);
      }
#endif
    } else if (tokens[0] == "write") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = strlen(tokens[2].c_str());
        char *buf = (char *)malloc(count);
        strcpy(buf, tokens[2].c_str());
        ssize_t ret = fs_write(fd, buf, count);
        printReturnValue(tokens[0], ret);
        free(buf);
      }
#ifndef TEST_VFS_INSTEAD
    } else if (tokens[0] == "allocwrite") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = strlen(tokens[2].c_str());
        char *buf0 = (char *)fs_malloc(count + 1);
        char *buf = (char *)fs_malloc(count + 1);
        strcpy(buf, tokens[2].c_str());
        ssize_t ret = fs_allocated_write(fd, buf, count);
        printReturnValue(tokens[0], ret);
        fs_free(buf);
        fs_free(buf0);
      }
    } else if (tokens[0] == "allocpwrite") {
      if (tokens.size() != 4) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = strlen(tokens[2].c_str());
        off_t offset = stollWrapper(tokens[3]);
        // char *buf = (char *)fs_malloc(count + 1);
        char *buf = (char *)fs_malloc(48 * 1024);
        strcpy(buf, tokens[2].c_str());
        ssize_t ret = fs_allocated_pwrite(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        fs_free(buf);
      }
#endif
    } else if (tokens[0] == "writen") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        uint64_t filesize = stollWrapper(tokens[2]);
        // char c = tokens[3].c_str()[0];
        // char buf[filesize];
        char *buf = (char *)malloc(filesize);
        memset(buf, 0, filesize);
        std::cout << "filesize:" << filesize << std::endl;
        write_char_no++;
        for (uint ii = 0; ii < filesize; ii++) {
          buf[ii] = write_char_no % 25 + 'A';
        }
        ssize_t ret = fs_write(fd, buf, filesize);
        if ((uint64_t)ret != filesize) {
          std::cerr << "ERROR ret write size:" << ret << std::endl;
        } else {
          std::cout << "fs_write ret:" << ret << " char:" << buf[0]
                    << std::endl;
        }
        free(buf);
      }
    } else if (tokens[0] == "writenr") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        uint64_t filesize = stollWrapper(tokens[2]);
        char buf[filesize];
        std::cout << "filesize:" << filesize << std::endl;
        randStr(buf, filesize);
        ssize_t ret = fs_write(fd, buf, filesize);
        if ((uint64_t)ret != filesize) {
          std::cerr << "ERROR ret write size:" << ret << std::endl;
        } else {
          std::cout << "fs_write ret:" << ret << std::endl;
        }
      }
    } else if (tokens[0] == "pwrite") {
      if (tokens.size() != 4) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        size_t count = strlen(tokens[2].c_str());
        off_t offset = stollWrapper(tokens[3]);
        char *buf = (char *)malloc(count);
        strcpy(buf, tokens[2].c_str());
        ssize_t ret = fs_pwrite(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        free(buf);
      }
    } else if (tokens[0] == "stat") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
        struct stat statbuf;
#ifndef TEST_VFS_INSTEAD
        int ret = fs_stat(tokens[1].c_str(), &statbuf);
#else
        int ret = fstatat(cur_dir_fd, tokens[1].c_str(), &statbuf, 0);
#endif
        printReturnValue(tokens[0], ret);
        std::cout << "return: " << std::endl;
        std::cout << "st_size:" << statbuf.st_size
                  << " nblocks:" << statbuf.st_blocks << std::endl;
      }

    } else if (tokens[0] == "fstat") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
        struct stat statbuf;
        int fd = stoiWrapper(tokens[1]);
        int ret = fs_fstat(fd, &statbuf);
        printReturnValue(tokens[0], ret);
        std::cout << "return: " << std::endl;
        std::cout << "st_size:" << statbuf.st_size
                  << " nblocks:" << statbuf.st_blocks << std::endl;
      }
    } else if (tokens[0] == "mkdir") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int ret = fs_mkdir(tokens[1].c_str(), 0);
#else
        int ret = mkdirat(cur_dir_fd, tokens[1].c_str(), 0);
#endif
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "lsdir") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        auto dentryPtr = fs_opendir(tokens[1].c_str());
        std::cout << "numEntry:" << dentryPtr->dentryNum << std::endl;
#else
        // A conceptual drop in replacement for opendirat
        int dirfd = openat(cur_dir_fd, tokens[1].c_str(),
                           O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        DIR *dentryPtr = fdopendir(dirfd);
        if (dentryPtr == nullptr) {
          printf("Failed to fdopendir\n");
          return;
        }
#endif
        std::cout << "now do readdir()" << std::endl;
        struct dirent *dp;
        while ((dp = fs_readdir(dentryPtr)) != NULL) {
          std::cout << "readdir result -- ino:" << dp->d_ino
                    << " name: " << dp->d_name << std::endl;
        }
#ifdef TEST_VFS_INSTEAD
        closedir(dentryPtr);
#endif
      }
    } else if (tokens[0] == "statall") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        struct stat statbuf;
        int ret = fs_stat(tokens[1].c_str(), &statbuf);
        if (ret < 0) {
          std::cerr << "cannot stat dir:" << tokens[1] << std::endl;
          return;
        }
        auto dentryPtr = fs_opendir(tokens[1].c_str());
        std::cout << "numEntry:" << dentryPtr->dentryNum << std::endl;
        std::cout << "now do readdir()" << std::endl;
        struct dirent *dp;
        while ((dp = fs_readdir(dentryPtr)) != NULL) {
          std::cout << "readdir result -- ino:" << dp->d_ino
                    << " name: " << dp->d_name << std::endl;
          std::string path(tokens[1]);
          path += "/";
          path += (dp->d_name);
          fs_stat(path.c_str(), &statbuf);
          std::cout << "st_size:" << statbuf.st_size
                    << " nblocks:" << statbuf.st_blocks << std::endl;
        }
#endif
      }
    } else if (tokens[0] == "unlink") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int ret = fs_unlink(tokens[1].c_str());
#else
        // FIXME: On Linux unlink cannot be used to remove directories. So we
        // are doing a stat to check.
        struct stat statbuf;
        int ret;
        ret = fstatat(cur_dir_fd, tokens[1].c_str(), &statbuf, 0);
        int unlink_flags = S_ISDIR(statbuf.st_mode) ? AT_REMOVEDIR : 0;
        ret = unlinkat(cur_dir_fd, tokens[1].c_str(), unlink_flags);
#endif
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "rename") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int ret = fs_rename(tokens[1].c_str(), tokens[2].c_str());
#else
        int ret = renameat(cur_dir_fd, tokens[1].c_str(), cur_dir_fd,
                           tokens[2].c_str());
#endif
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "close") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        if (fd < 0) {
          return;
        }
        int ret = fs_close(fd);
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "fdatasync") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
        int fd = stoiWrapper(tokens[1]);
        if (fd < 0) return;
        int ret = fs_fdatasync(fd);
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "syncall") {
      if (tokens.size() != 1) {
        printHelp();
      } else {
        fs_syncall();
        printReturnValue(tokens[0], 0);
      }
    } else if (tokens[0] == "syncunlinked") {
      if (tokens.size() != 1) {
        printHelp();
      } else {
        fs_syncunlinked();
        printReturnValue(tokens[0], 0);
      }
    } else if (tokens[0] == "ping") {
      if (tokens.size() != 1) {
        printHelp();
      } else {
        fs_ping();
        printReturnValue(tokens[0], 0);
      }
    } else if (tokens[0] == "dumpinodes") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
        int wid = stoiWrapper(tokens[1]);
        int ret = fs_dumpinodes(wid);
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "start_dump_load") {
#ifndef TEST_VFS_INSTEAD
      int ret = fs_start_dump_load_stats();
      printReturnValue(tokens[0], ret);
#endif
    } else if (tokens[0] == "stop_dump_load") {
#ifndef TEST_VFS_INSTEAD
      int ret = fs_stop_dump_load_stats();
      printReturnValue(tokens[0], ret);
#endif
    } else if (tokens[0] == "migrate") {
      if (tokens.size() != 2) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int fd = stoiWrapper(tokens[1]);
        int dstWid = -10;
        int ret = fs_migrate(fd, &dstWid);
        printReturnValue(tokens[0], ret);
        fprintf(stderr, "dstWid:%d\n", dstWid);
#endif
      }
    } else if (tokens[0] == "inode-ownership-check") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int inode = stoiWrapper(tokens[1]);
        int wid = stoiWrapper(tokens[2]);
        int ret = fs_admin_inode_reassignment(0, inode, wid, -1);
        printReturnValue(tokens[0], ret);
#endif
      }
    } else if (tokens[0] == "inode-ownership-set") {
      if (tokens.size() != 4) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int inode = stoiWrapper(tokens[1]);
        int curOwner = stoiWrapper(tokens[2]);
        int newOwner = stoiWrapper(tokens[3]);
        int ret = fs_admin_inode_reassignment(1, inode, curOwner, newOwner);
        printReturnValue(tokens[0], ret);
#endif
      }
    } else if (tokens[0] == "thread_reassign") {
      if (tokens.size() != 3) {
        printHelp();
      } else {
#ifndef TEST_VFS_INSTEAD
        int src_wid = stoiWrapper(tokens[1]);
        int dst_wid = stoiWrapper(tokens[2]);
        int ret = fs_admin_thread_reassign(src_wid, dst_wid, 0);
        printReturnValue(tokens[0], ret);
#endif
      }

    } else {
      printHelp();
    }
  }
}

void cliMain() {
  for (std::string line; std::cout << (interactive ? "FsApp > " : "") &&
                         std::getline(std::cin, line);) {
    if (!line.empty()) {
      process(line);
    }
  }
}

int main(int argc, char **argv) {
#ifndef TEST_VFS_INSTEAD
  if (argc != 2) {
    fprintf(stderr, "Usage %s <pid(from 0)>|<pid1,pid2,pid3>\n", argv[0]);
    fprintf(stderr, "\t requires only one argument\n");
    fprintf(stderr, "\t Option-1 --- pid: one integer as user-supply PID\n");
    fprintf(stderr,
            "\t Option-2 --- pid1,pid2,pid3: a list of integers separated by , "
            "(comma)\n");
    exit(1);
  }
#else
  if (argc != 2) {
    fprintf(stderr, "Usage %s [base path]\n", argv[0]);
    fprintf(stderr, "[base path] should be a writable directory\n");
    exit(1);
  }
#endif
  interactive = isatty(STDIN_FILENO);
  if (!interactive && (errno != ENOTTY)) {
    fprintf(stderr, "Cannot access standard input\n");
    return -1;
  }
  if (!interactive) {
    printf("Running in batch mode\n");
  }
#ifndef TEST_VFS_INSTEAD
  // int rt = fs_init(FS_SHM_KEY_BASE + stoiWrapper(argv[1]));
  const int kMaxNumFsp = 10;
  key_t shmKeys[kMaxNumFsp];
  for (int i = 0; i < kMaxNumFsp; i++) shmKeys[i] = 0;
  auto pidStrVec = splitStr(argv[1], ',');
  int numKey = pidStrVec.size();
  assert(numKey <= kMaxNumFsp);
  for (int i = 0; i < numKey; i++) {
    shmKeys[i] = (FS_SHM_KEY_BASE) + stoiWrapper(pidStrVec[i]);
  }
#ifdef UFS_SOCK_LISTEN
  int rg_rt = fs_register();
  fprintf(stdout, "register rt:%d\n", rg_rt);
#else
  int rg_rt = -1;
#endif
  if (rg_rt < 0) {
    int rt = fs_init_multi(numKey, shmKeys);
    if (rt < 0) {
      return -1;
    }
  }
#else
  cur_dir_fd = open(argv[1], O_DIRECTORY | O_RDONLY);
  if (cur_dir_fd == -1) {
    perror("Failed to open directory");
    return -1;
  }
#endif
  signal(SIGINT, handle_sigint);
  cliMain();
  clean_exit();
  return 0;
}

void handle_sigint(int sig) {
  std::cout << std::endl << "Bye :)" << std::endl;
  clean_exit();
}
