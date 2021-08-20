#ifndef CFS_FSPROC_UTIL
#define CFS_FSPROC_UTIL

#include <unordered_map>

template <typename T, typename K>
void inline maintainTwoLargestEle(std::unordered_map<T, K> &valAccessCntMap,
                                  T thisAccessVal, T defaultVal, K &cntMax,
                                  K &secCntMax, T &maxVal, T &secMaxVal) {
  auto it = valAccessCntMap.emplace(thisAccessVal, 0);
  (it.first)->second++;
  K oldCntMax = cntMax, oldSecCntMax = secCntMax;
  if (oldCntMax < (it.first)->second) {
    // need to change the max
    cntMax = (it.first)->second;
    if (thisAccessVal != maxVal) {
      // largest val changed, it must be raised up from the second
      T oldMaxVal = maxVal;
      maxVal = thisAccessVal;
      secMaxVal = oldMaxVal;
      secCntMax = oldCntMax;
    }
  } else if (oldSecCntMax < (it.first)->second) {
    // need to change the secMax
    secMaxVal = thisAccessVal;
    secCntMax = (it.first)->second;
  }
}

#endif