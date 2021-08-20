#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "cxxopts.hpp"
#include "fsapi.h"
#include "testClient_common.h"

// This will read in a file, which contains a list of commands, and execute
// accordingly line by line.
// NOTE: for testing purpose, only the target file operation outcome will be
// send to stdout, thus we can check the result easily
//      For control operation (or metadata operation):
//            print return value
//      If it is a read operation, will print the returned value
//      For stats, will allow the user to select the return value by cmd arg

constexpr static int kNumLines = 1000;
static int64_t gRetValueArr[kNumLines];
// absolute line number
static int gLineno = 1;
static bool gPrintLines = false;

static void clean_exit() { exit(fs_exit()); };

void print_usage(cxxopts::Options &opts, char *bin) {
  std::cerr << opts.help() << "\n";
  // fprintf(stderr, "%s <appPid> cmdFileName\n", bin);
  fprintf(stderr, "Each line of cmdFile need to be like:\n");
  fprintf(stderr, "\tNOTE:## comments, will skip\n");
  fprintf(stderr, "\tNOTE:<fd> can be replaced by %%lineNo\n");
  fprintf(stderr, "\t=====================================================\n");
  fprintf(stderr, "\tcreate <PATH & FILENAME>\n");
  fprintf(stderr, "\topen <PATH & FILENAME>\n");

  fprintf(stderr, "\tread <fd> <sizeByte>\n");
  fprintf(stderr, "\tpread <fd> <sizeByte> <offset>\n");
  fprintf(stderr, "\twrite <fd> <STR|RPT|RAND> <Str> [len]\n");
  fprintf(stderr, "\tpwrite <fd> <STR|RPT|RAND> <Str> <offset> [len]\n");

  fprintf(stderr, "\tallocread <fd> <sizeByte>\n");
  fprintf(stderr, "\tallocwrite <fd> <STR|RPT|RAND> <Str> [len]\n");
  fprintf(stderr, "\tallocpread <fd> <sizeByte> <offset>\n");
  fprintf(stderr, "\tallocpwrite <fd> <STR|RPT|RAND> <Str> <offset> [len]\n");

  fprintf(stderr, "\tclose <fd>\n");
  fprintf(stderr, "\tstat <PATH & FILENAME> output_val_name\n");
  //  fprintf(stderr, "fstat <fd>\n");
  //  fprintf(stderr, "mkdir <PATH & FILENAME>\n");
  fprintf(stderr, "\tunlink <PATH & FILENAME>\n");
  //  fprintf(stderr, "rename <old path> <new path>\n");
  //  fprintf(stderr, "lsdir <dir path> \n");
  //  fprintf(stderr, "fdatasync <fd>\n");
}

char const *gDataOutputSignalstr = "";

void printReturnValue(std::string const &cmd, ssize_t v) {
  fprintf(stdout, "\n-------\n%s Return:\n", cmd.c_str());
  fprintf(stdout, "%ld\n", v);
  fprintf(stdout, "-------\n");
}

void print_err() { fprintf(stderr, "testClient_App: error\n"); }

bool check_comment(const std::string &line) {
  if (line.find("##") != std::string::npos) {
    return true;
  }
  return false;
}

static int getFdFromToken(std::string &token) {
  int fd;
  if (token[0] == '%') {
    std::string lineNostr = token.substr(1, token.size());
    int lineNo = stoiWrapper(lineNostr);
    fd = gRetValueArr[lineNo];
  } else {
    fd = stoiWrapper(token);
  }
  return fd;
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

static char *fillBufferAccordingToToken(std::string &token,
                                        std::string &strToken, size_t dstLen,
                                        char *dst) {
  size_t len = dstLen;
  memset(dst, 0, len);

  if (token == "STR") {
    strcpy(dst, strToken.c_str());
  } else if (token == "RPT") {
    // then strToken is supposed to be a char
    char c = strToken[0];
    for (uint i = 0; i < len; i++) {
      dst[i] = c;
    }
  } else if (token == "RAND") {
    randStr(dst, len);
  } else {
    print_err();
  }
  return dst;
}

void processOneCmd(std::string const &line) {
  std::istringstream iss(line);
  std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                  std::istream_iterator<std::string>{}};
  if (gPrintLines) {
    fprintf(stdout, ANSI_COLOR_CYAN "==>%d:" ANSI_COLOR_RESET, gLineno);
    std::cout << line << std::endl;
  }
  if (!tokens.empty()) {
    // process each token
    if (tokens[0] == "create") {
      if (tokens.size() != 2) {
        print_err();
      } else {
        int ret = fs_open(tokens[1].c_str(), O_CREAT, 0644);
        printReturnValue(tokens[0], ret);
        gRetValueArr[gLineno] = ret;
      }
    } else if (tokens[0] == "open") {
      if (tokens.size() != 2) {
        print_err();
      } else {
        int ret = fs_open(tokens[1].c_str(), O_RDWR, 0);
        printReturnValue(tokens[0], ret);
        gRetValueArr[gLineno] = ret;
      }
    } else if (tokens[0] == "read") {
      if (tokens.size() != 3) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        char *buf = (char *)malloc(count + 1);
        memset(buf, 0, count + 1);
        ssize_t ret = fs_read(fd, buf, count);
        printReturnValue(tokens[0], ret);
        if (ret > 0) {
          fprintf(stdout, "%s", gDataOutputSignalstr);
          for (int ii = 0; ii < ret; ii++) {
            fprintf(stdout, "%c", buf[ii]);
          }
        }
        free(buf);
      }
    } else if (tokens[0] == "pread") {
      if (tokens.size() != 4) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        off_t offset = stollWrapper(tokens[3]);
        char *buf = (char *)malloc(count);
        memset(buf, 0, count);
        ssize_t ret = fs_pread(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        if (ret > 0) {
          fprintf(stdout, "%s", gDataOutputSignalstr);
          for (int ii = 0; ii < ret; ii++) {
            fprintf(stdout, "%c", buf[ii]);
          }
        } else {
          print_err();
        }
        free(buf);
      }
    } else if (tokens[0] == "write") {
      if (tokens.size() != 4 && tokens.size() != 5) {
        print_err();
      } else {
        ssize_t count;
        if (tokens.size() == 4) {
          count = strlen(tokens[3].c_str());
        } else {
          count = stollWrapper(tokens[4]);
        }
        int fd = getFdFromToken(tokens[1]);
        char *buf = (char *)malloc(count);
        fillBufferAccordingToToken(tokens[2], tokens[3], count, buf);
        ssize_t ret = fs_write(fd, buf, count);
        printReturnValue(tokens[0], ret);
        free(buf);
      }
    } else if (tokens[0] == "pwrite") {
      if (tokens.size() != 5 && tokens.size() != 6) {
        print_err();
      } else {
        size_t count;
        if (tokens.size() == 5) {
          count = strlen(tokens[3].c_str());
        } else {
          count = stoiWrapper(tokens[5]);
        }
        int fd = getFdFromToken(tokens[1]);
        off_t offset = stollWrapper(tokens[4]);
        char *buf = (char *)malloc(count);
        fillBufferAccordingToToken(tokens[2], tokens[3], count, buf);
        ssize_t ret = fs_pwrite(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        free(buf);
      }
    } else if (tokens[0] == "allocwrite") {
      if (tokens.size() != 4 && tokens.size() != 5) {
        print_err();
      } else {
        ssize_t len;
        if (tokens.size() == 5) {
          len = stoiWrapper(tokens[3]);
        } else {
          len = strlen(tokens[4].c_str());
        }
        int fd = getFdFromToken(tokens[1]);
        char *buf = (char *)fs_malloc(len + 1);
        fillBufferAccordingToToken(tokens[2], tokens[3], len, buf);
        ssize_t ret = fs_allocated_write(fd, buf, len);
        printReturnValue(tokens[0], ret);
        fs_free(buf);
      }
    } else if (tokens[0] == "allocread") {
      if (tokens.size() != 3) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        char *buf = (char *)fs_malloc(count);
        memset(buf, 0, count);
        ssize_t ret = fs_allocated_read(fd, buf, count);
        printReturnValue(tokens[0], ret);
        if (ret > 0) {
          fprintf(stdout, "%s", gDataOutputSignalstr);
          for (int ii = 0; ii < ret; ii++) {
            fprintf(stdout, "%c", buf[ii]);
          }
        } else {
          print_err();
        }
        fs_free(buf);
      }
    } else if (tokens[0] == "allocpread") {
      if (tokens.size() != 4) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        off_t offset = stollWrapper(tokens[3]);
        char *buf = (char *)fs_malloc(count);
        memset(buf, 0, count);
        ssize_t ret = fs_allocated_pread(fd, buf, count, offset);
        printReturnValue(tokens[0], ret);
        if (ret > 0) {
          fprintf(stdout, "%s", gDataOutputSignalstr);
          for (int ii = 0; ii < ret; ii++) {
            fprintf(stdout, "%c", buf[ii]);
          }
        } else {
          print_err();
        }
        fs_free(buf);
      }
    } else if (tokens[0] == "cacheread") {
      if (tokens.size() != 3) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        // char *buf = (char *)fs_malloc(count + 1);
        // memset(buf, 0, count + 1);
        //  void *tmpBuf = static_cast<void *>(buf);
        // ssize_t ret = fs_cached_read(fd, &tmpBuf, count);
        void *tmpBuf = nullptr;
        ssize_t ret = fs_cached_read(fd, &tmpBuf, count);
        char *retDataBuf = static_cast<char *>(tmpBuf);
        printReturnValue(tokens[0], ret);
        if (ret > 0) {
          fprintf(stdout, "%s", gDataOutputSignalstr);
          for (int ii = 0; ii < ret; ii++) {
            fprintf(stdout, "%c", retDataBuf[ii]);
          }
        }
        // no free for cached interface
        // free(buf);
      }
    } else if (tokens[0] == "cachepread") {
      if (tokens.size() != 4) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        size_t count = stoiWrapper(tokens[2]);
        off_t offset = stollWrapper(tokens[3]);
        // char *buf = (char *)fs_malloc(count);
        // memset(buf, 0, count);
        // ssize_t ret = fs_allocated_pread(fd, buf, count, offset);
        // printReturnValue(tokens[0], ret);
        // void *tmpBuf = static_cast<void *>(buf);
        void *tmpBuf = nullptr;
        ssize_t ret = fs_cached_pread(fd, &tmpBuf, count, offset);
        char *retDataBuf = static_cast<char *>(tmpBuf);
        printReturnValue(tokens[0], ret);
        if (ret > 0) {
          fprintf(stdout, "%s", gDataOutputSignalstr);
          for (int ii = 0; ii < ret; ii++) {
            fprintf(stdout, "%c", retDataBuf[ii]);
          }
        } else {
          print_err();
        }
        // no free for cached interface
        // fs_free(buf);
      }
    } else if (tokens[0] == "stat") {
      if (tokens.size() != 3) {
        print_err();
      } else {
        struct stat statbuf;
        std::cerr << tokens[1] << std::endl;
        int ret = fs_stat(tokens[1].c_str(), &statbuf);
        printReturnValue(tokens[0], ret);
        if (ret >= 0) {
          if (tokens[2] == "st_size") {
            fprintf(stdout, "%lu\n", statbuf.st_size);
          } else if (tokens[2] == "st_blocks") {
            fprintf(stdout, "%ld\n", statbuf.st_blocks);
          } else {
            print_err();
          }
        }
      }
    } else if (tokens[0] == "close") {
      if (tokens.size() != 2) {
        print_err();
      } else {
        int fd = getFdFromToken(tokens[1]);
        if (fd < 0) {
          return;
        }
        int ret = fs_close(fd);
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "mkdir") {
      if (tokens.size() != 2) {
        print_err();
      } else {
        int ret = fs_mkdir(tokens[1].c_str(), 0);
        printReturnValue(tokens[0], ret);
      }
    } else if (tokens[0] == "unlink") {
      if (tokens.size() != 2) {
        print_err();
      } else {
        int ret = fs_unlink(tokens[1].c_str());
        printReturnValue(tokens[0], ret);
      }
    } else {
      fprintf(stderr, "Unknown command %s\n", tokens[0].c_str());
    }
    // end of one token process
    if (gLineno > kNumLines) {
      fprintf(stderr, "No more than %d lines\n", kNumLines);
      clean_exit();
    }
  }
}

void appMain(const char *fname) {
  std::string line;
  std::ifstream cmdfile(fname);
  if (cmdfile.is_open()) {
    while (getline(cmdfile, line)) {
      if (!check_comment(line)) {
        processOneCmd(line);
      }
      gLineno++;
    }
    cmdfile.close();
  } else {
    fprintf(stderr, "Cannot open command file\n");
  }
}

int main(int argc, char **argv) {
  cxxopts::Options options(
      argv[0], "A client to read command in a file and perform fs testing");
  options.positional_help("[optional args]").show_positional_help();
  options.add_options()
      // worker key to contact FSP, E.g., -k 1,11,21
      ("k,keys", "key list.", cxxopts::value<std::vector<int>>())
      // name of the file that contains the commands to be executed
      ("f,file", "command file name", cxxopts::value<std::string>())
      // if print each line of command file
      ("p,print", "print each line of commands")
      // help info
      ("h,help", "Print usage");
  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    print_usage(options, argv[0]);
    exit(0);
  }

  gPrintLines = result["print"].as<bool>();
  std::cerr << "gPrintLines set to:" << gPrintLines << std::endl;

  int kMaxNumFsp = 10;
  key_t shmKeys[kMaxNumFsp];
  int numKey = 0;
  for (int i = 0; i < kMaxNumFsp; i++) shmKeys[i] = 0;
  if (result.count("keys")) {
    const auto values = result["keys"].as<std::vector<int>>();
    numKey = values.size();
    assert(numKey <= kMaxNumFsp);
    for (int i = 0; i < numKey; i++) {
      shmKeys[i] = (FS_SHM_KEY_BASE) + values[i];
      // std::cerr << "i:" << shmKeys[i] << std::endl;
    }
  } else {
    print_usage(options, argv[0]);
    exit(1);
  }

  std::string fname;
  if (result.count("file")) {
    fname = result["file"].as<std::string>();
    // std::cerr << "command file:" << fname << std::endl;
  } else {
    print_usage(options, argv[0]);
    exit(1);
  }

  int rt = fs_init_multi(numKey, shmKeys);
  if (rt < 0) {
    fprintf(stderr, "Cannot connect to FS\n");
    return -1;
  }

  appMain(fname.c_str());

  clean_exit();
  return 0;
}
