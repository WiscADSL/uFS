#include "util.h"

#include <assert.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <sstream>
#include <thread>

thread_local int curTid = -1;

// NOTE: we make sure the wid of each worker is set to this thread_local curTid
// see BlkDevSpdk::initWorker(int wid);
void cfsSetTid(int t) { curTid = t; }
cfs_tid_t cfsGetTid() { return curTid; }

/*
 * set the bit to 1 inside *addr, nr starts from 0
 */
int block_set_bit(uint32_t nr, void *addr) {
#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "block_set_bit: %u %p\n", nr, addr);
#endif
  int mask, retval;
  unsigned char *ADDR = (unsigned char *)addr;

  ADDR += nr >> 3;
  mask = 1 << (nr & 0x07);
  retval = mask & *ADDR;
  *ADDR |= mask;
  return retval;
}

int block_clear_bit(uint32_t nr, void *addr) {
#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "block_clear_bit: %u %p\n", nr, addr);
#endif
  int mask, retval;
  unsigned char *ADDR = (unsigned char *)addr;
  ADDR += nr >> 3;
  mask = 1 << (nr & 0x07);
  retval = mask & *ADDR;
  *ADDR &= ~mask;
  return retval;
}

int block_test_bit(uint32_t nr, void *addr) {
#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "block_test_bit: %u %p\n", nr, addr);
#endif
  int mask;
  const unsigned char *ADDR = (const unsigned char *)addr;
  ADDR += nr >> 3;
  mask = 1 << (nr & 0x07);
  return (mask & *ADDR);
}

int64_t find_block_free_bit_no(void *addr, int64_t numBitsPerBlock) {
  return find_block_free_bit_no_start_from(addr, -1, numBitsPerBlock);
}

int64_t find_block_free_bit_no_start_from(void *addr, int start_bit_no,
                                          int64_t numBitsPerBlock) {
#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "find_block_free_bit_no_start_from: %p %d %ld\n", addr,
          start_bit_no, numBitsPerBlock);
#endif
  int bit_no = start_bit_no, bit_set = -1;
  do {
    bit_no++;
    if (bit_no == numBitsPerBlock) {
      break;
    }
    bit_set = block_test_bit(bit_no, (void *)(addr));
  } while (bit_set && bit_no < (numBitsPerBlock));
  if (bit_set != 0 || bit_no == numBitsPerBlock) {
    return -1;
  }
  return bit_no;
}

int64_t find_block_free_multi_bits_no(void *addr, int64_t numBitsPerBlock,
                                      int numBitsNeed) {
  int bit_no = -1, bit_set = -1;
  int64_t start_bit_no = -1;
  int numBitsFound = 0;
  do {
    bit_no++;
    if (bit_no == numBitsPerBlock) {
      break;
    }
    bit_set = block_test_bit(bit_no, (void *)(addr));
    if (bit_set) {
      if (bit_no >= numBitsPerBlock) {
        // cannot found
        break;
      } else {
        numBitsFound = 0;
      }
    } else {
      numBitsFound++;
      if (numBitsFound == numBitsNeed) {
        // successfully found
        break;
      } else {
        if (numBitsFound == 1) {
          start_bit_no = bit_no;
        }
      }
    }
  } while ((bit_set && bit_no < (numBitsPerBlock)) ||
           ((!bit_set) && numBitsFound < numBitsNeed));
  if ((bit_set != 0) || numBitsFound != numBitsNeed) {
    return -1;
  }
  return start_bit_no;
}

int64_t find_block_free_jump_bits_no_start_from(void *addr, int start_bit_no,
                                                int64_t numBitsPerBlock,
                                                int numBitsJump) {
#ifdef TEST_BLOCK_ALLOC_FREE
  fprintf(stdout, "find_block_free_jump_bits_no_start_from: %p %d %d\n", addr,
          start_bit_no, numBitsJump);
#endif
  int bit_no = (-numBitsJump), bit_set = -1;
  if (start_bit_no > 0) bit_no = start_bit_no;
  do {
    bit_no += numBitsJump;
    if (bit_no >= numBitsPerBlock) {
      break;
    }
    bit_set = block_test_bit(bit_no, (void *)(addr));
    if (bit_set) {
      if (bit_no >= numBitsPerBlock) {
        // cannot found
        break;
      }
    } else {
      // found
      start_bit_no = bit_no;
      break;
    }
  } while ((bit_set && bit_no < (numBitsPerBlock)));
  if ((bit_set) || numBitsPerBlock - start_bit_no < numBitsJump) return -1;
  return start_bit_no;
}

int filepath2TokensSlow(char *path, std::vector<std::string> &tokenVec) {
  std::stringstream ssm(path);
  std::string intermediate;
  while (getline(ssm, intermediate, '/')) {
    if (intermediate.size() > 0) {
      tokenVec.push_back(intermediate);
    }
  }
  return tokenVec.size();
}

int filepath2Tokens(char *path, std::vector<std::string> &tokenVec) {
  // clear the vector first
  // RELIES on user to clear the vector
  // tokenVec.clear();
  int len = strlen(path);
  int depth = 0;
  int i, j;
  for (i = 0; i < len; i++) {
    // bypass '/'
    if (path[i] == '/') continue;

    // find new item
    j = i;
    while (j < len) {
      if (path[j] == '/') break;
      j++;
    }

    // finish process this item
    tokenVec.emplace_back(path, i, j - i);
    depth++;
    i = j;
  }
  return depth;
}

char *filepath2TokensStandardized(const char *path, int *pathDelimIdxArr,
                                  int &depth) {
  depth = 0;
  pathDelimIdxArr[0] = -1;

  int len = strlen(path);
  constexpr int kAdd = 1;
  char *standardPath = static_cast<char *>(malloc(len + kAdd));
  if (standardPath == nullptr) return nullptr;
  memset(standardPath, 0, len + kAdd);

  int standardPathIdx = 0;
  int i, j;
  for (i = 0; i < len; i++) {
    // bypass '/'
    if (path[i] == '/') continue;

    // find new item
    j = i;
    while (j < len) {
      if (path[j] == '/') break;
      j++;
    }

    // finish process this item
    pathDelimIdxArr[depth] = standardPathIdx;
    memcpy(standardPath + standardPathIdx, path + i, j - i);
    standardPathIdx += j - i;
    standardPath[standardPathIdx++] = '/';
    depth++;
    i = j;
  }
  assert(standardPathIdx - 1 <= len + kAdd - 1);
  if (standardPathIdx > 0 && standardPath[standardPathIdx - 1] == '/') {
    standardPath[standardPathIdx - 1] = '\0';
  }
  return standardPath;
}

std::vector<std::string> splitStr(const std::string &input, char delim) {
  std::stringstream ss(input);
  std::string item;
  std::vector<std::string> elems;
  while (std::getline(ss, item, delim)) {
    elems.push_back(std::move(item));
  }
  return elems;
}

bool checkFileExistance(const char *filename) {
  struct stat statbuf;
  return (stat(filename, &statbuf) == 0);
}

void printOnErrorExitSymbol() {
  char exitStr[] =
      "                uuuuuuu\n"
      "             uu$$$$$$$$$$$uu\n"
      "          uu$$$$$$$$$$$$$$$$$uu\n"
      "         u$$$$$$$$$$$$$$$$$$$$$u\n"
      "        u$$$$$$$$$$$$$$$$$$$$$$$u\n"
      "       u$$$$$$$$$$$$$$$$$$$$$$$$$u\n"
      "       u$$$$$$$$$$$$$$$$$$$$$$$$$u\n"
      "       u$$$$$$\"   \"$$$\"   \"$$$$$$u\n"
      "       \"$$$$\"      u$u       $$$$\"\n"
      "        $$$u       u$u       u$$$\n"
      "        $$$u      u$$$u      u$$$\n"
      "         \"$$$$uu$$$   $$$uu$$$$\"\n"
      "          \"$$$$$$$\"   \"$$$$$$$\"\n"
      "            u$$$$$$$u$$$$$$$u\n"
      "             u$\"$\"$\"$\"$\"$\"$u\n"
      "  uuu        $$u$ $ $ $ $u$$       uuu\n"
      " u$$$$        $$$$$u$u$u$$$       u$$$$\n"
      "  $$$$$uu      \"$$$$$$$$$\"     uu$$$$$$\n"
      "u$$$$$$$$$$$uu    \"\"\"\"\"    uuuu$$$$$$$$$$\n"
      "$$$$\"\"\"$$$$$$$$$$uuu   uu$$$$$$$$$\"\"\"$$$\"\n"
      " \"\"\"      \"\"$$$$$$$$$$$uu \"\"$\"\"\"\n"
      "           uuuu \"\"$$$$$$$$$$uuu\n"
      "  u$$$uuu$$$$$$$$$uu \"\"$$$$$$$$$$$uuu$$$\n"
      "  $$$$$$$$$$\"\"\"\"           \"\"$$$$$$$$$$$\"\n"
      "   \"$$$$$\"                      \"\"$$$$\"\"\n"
      "     $$$\"                         $$$$\"";
  fprintf(stderr, "\n%s\n", exitStr);
}

void printMasterSymbol() {
  char masterStr[] =
      "                  ,   __, ,\n"
      "                  _.._         )\\/(,-' (-' `.__\n"
      "                 /_   `-.      )'_      ` _  (_    _.---._\n"
      "                // \\     `-. ,'   `-.    _\\`.  `.,'   ,--.\\\n"
      "               // -.\\       `        `.  \\`.   `/   ,'   ||\n"
      "               || _ `\\_         ___    )  )     \\  /,-'  ||\n"
      "               ||  `---\\      ,'__ \\   `,' ,--.  \\/---. //\n"
      "                \\\\  .---`.   / /  | |      |,-.\\ |`-._ //\n"
      "                 `..___.'|   \\ |,-| |      |_  )||\\___//\n"
      "                   `.____/    \\\\\\O| |      \\o)// |____/\n"
      "                        /      `---/        \\-'  \\\n"
      "                        |        ,'|,--._.--')    \\\n"
      "                        \\       /   `n     n'\\    /\n"
      "                         `.   `<   .::`-,-'::.) ,'    hjw\n"
      "                           `.   \\-.____,^.   /,'\n"
      "                             `. ;`.,-V-.-.`v'\n"
      "                               \\| \\     ` \\|\\\n"
      "                                ;  `-^---^-'/\n"
      "                                 `-.______,'";
  fprintf(stderr, "\n%s\n", masterStr);
}

void printServantSymbol() {
  char servantStr[] =
      "                _      _      _\n"
      "              >(.)__ <(.)__ =(.)__\n"
      "               (___/  (___/  (___/  hjw";
  fprintf(stderr, "\n%s\n", servantStr);
}