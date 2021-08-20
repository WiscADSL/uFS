#ifndef CFSTEST_TESTCLIENT_COMMON_H
#define CFSTEST_TESTCLIENT_COMMON_H

#include <stdio.h>
#include <string>

#define FS_SHM_KEY_BASE 20190301

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_YELLOW "\x1b[33m"
#define ANSI_COLOR_BLUE "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN "\x1b[36m"
#define ANSI_COLOR_RESET "\x1b[0m"

int inline stoiWrapper(const std::string &str) {
  int r = -1;
  try {
    r = std::stoi(str);
  } catch (...) {
    fprintf(stderr, "Error invalid argument: %s\n", str.c_str());
  }
  return r;
}

long long inline stollWrapper(const std::string &str) {
  long long r = -1;
  try {
    r = std::stoll(str);
  } catch (...) {
    fprintf(stderr, "Error invalid argument: %s\n", str.c_str());
  }
  return r;
}

#endif // CFSTEST_TESTCLIENT_COMMON_H
