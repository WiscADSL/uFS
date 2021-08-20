//-----------------------------------------------------------------------
// Copyright 2011 Ciaran McHale.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions.
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.  
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//----------------------------------------------------------------------

//--------
// #include's
//--------
#include <config4cpp/StringBuffer.h>
#include "util.h"


namespace CONFIG4CPP_NAMESPACE {

void
splitScopedNameIntoVector(const char * str, StringVector & vec)
{
	int				i;
	int				len;
	char *			startOfStr;
	StringBuffer	tmpStr; // Mutable copy of str

	vec.empty();
	tmpStr = str;
	len = tmpStr.length();
	startOfStr = &tmpStr[0];
	for (i = 0; i < len; i++) {
		if (tmpStr[i] == '.') {
			tmpStr[i] = 0;
			vec.add(startOfStr);
			startOfStr = &tmpStr[i+1];
		}
	}
	vec.add(startOfStr);
}

}; // namespace CONFIG4CPP_NAMESPACE
