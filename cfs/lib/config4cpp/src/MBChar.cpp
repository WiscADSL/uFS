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
#include "MBChar.h"
#include <ctype.h>
#include <wchar.h>
#include <wctype.h>


namespace CONFIG4CPP_NAMESPACE {

MBChar::MBChar()
{
	reset();
}



MBChar::~MBChar()
{
	// Nothing to do
}



bool
MBChar::operator==(const MBChar & other) const
{
	int				i;

	if (m_mbCharLen != other.m_mbCharLen) {
		return false;
	}
	for (i = 0; i < m_mbCharLen; i++) {
		if (m_mbChar[i] != other.m_mbChar[i]) {
			return false;
		}
	}
	return true;
}

}; // namespace CONFIG4CPP_NAMESPACE
