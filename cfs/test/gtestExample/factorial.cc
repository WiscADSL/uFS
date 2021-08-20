/*********************************************************************************
*     File Name           :     sample.cc
*     Created By          :     jing
*     Creation Date       :     [2019-04-08 11:10]
*     Last Modified       :     [2019-04-08 11:11]
*     Description         :      
**********************************************************************************/

#include "factorial.h"

// Returns n! (the factorial of n).  For negative n, n! is defined to be 1.
int Factorial(int n) {
  int result = 1;
  for (int i = 1; i <= n; i++) {
    result *= i;
  }

  return result;
}


