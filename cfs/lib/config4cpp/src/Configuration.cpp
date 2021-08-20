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
#include <config4cpp/Configuration.h>
#include "ConfigurationImpl.h"
#include "MBChar.h"
#include <string.h>
#include <assert.h>
#include <stdlib.h>


namespace CONFIG4CPP_NAMESPACE {

Configuration::Configuration()  { }
Configuration::~Configuration() { }



Configuration *
Configuration::create()
{
	return new ConfigurationImpl();
}



void
Configuration::destroy()
{
	delete this;
}



void
Configuration::mergeNames(
	const char *		scope,
	const char *		localName,
	StringBuffer &		fullyScopedName)
{
	if (scope[0] == '\0') {
		fullyScopedName = localName;
	} else if (localName[0] == '\0') {
		fullyScopedName = scope;
	} else {
		fullyScopedName.empty();
		fullyScopedName << scope << "." << localName;
	}
}



int
Configuration::mbstrlen(const char * str)
{
	int					count;
	int					status;
	char				byte;
	const char *		ptr;
	wchar_t				wChar;
	MBChar				ch;
	mbstate_t			mbtowcState;

	memset(&mbtowcState, 0, sizeof(mbtowcState));
	count = 0;
	ptr = str;
	while (*ptr != '\0') {
		status = -1;
		while (status == -1) {
			byte = *ptr;
			ptr ++;
			if (byte == '\0' && !ch.isEmpty()) {
				return -1; // invalid multi-byte string
			}
			if (byte == '\0') {
				break;
			}
			if (!ch.add(byte)) {
				return -1; // invalid multi-byte string
			}
			status = mbrtowc(&wChar, ch.c_str(), ch.length(), &mbtowcState);
			if (status == -1 && ch.isFull()) {
				return -1; // invalid multi-byte string
			}
		}
		if (byte != '\0') {
			count ++;
		}
		ch.reset();
	}
	return count;
}



//----------------------------------------------------------------------
// Function:	patternMatch()
//
// Description:	Returns true if the specified pattern matches the
//			specified string.
//
// Note:	The only wildcard supported is "*". It acts like the
//			"*" wildcard in UNIX and DOS shells, that is, it
//			matches zero or more characters.
//----------------------------------------------------------------------

bool
Configuration::patternMatch(const char * str, const char * pattern)
{
	wchar_t *				wStr;
	int						wStrLen;
	wchar_t *				wPattern;
	int						wPatternLen;
	bool					result;
	int						strLen;
	int						patternLen;

	strLen = strlen(str);
	wStr = new wchar_t[strLen + 1];
	wStrLen = mbstowcs(wStr, str, strLen + 1);
	assert(wStrLen != -1);

	patternLen = strlen(pattern);
	wPattern = new wchar_t[patternLen + 1];
	wPatternLen = mbstowcs(wPattern, pattern, patternLen + 1);
	assert(wPatternLen != -1);

	result = patternMatchInternal(wStr, 0, wStrLen,
						wPattern, 0, wPatternLen);
	delete [] wStr;
	delete [] wPattern;
	return result;
}


bool
Configuration::patternMatchInternal(
	const wchar_t *			wStr,
	int						wStrIndex,
	int						wStrLen,
	const wchar_t *			wPattern,
	int						wPatternIndex,
	int						wPatternLen)
{
	while (wPatternIndex < wPatternLen) {
		if (wPattern[wPatternIndex] != '*') {
			if (wPattern[wPatternIndex] != wStr[wStrIndex]) {
				return false;
			}
			wPatternIndex++;
			if (wStrIndex < wStrLen) {
				wStrIndex++;
			}
		} else {
			wPatternIndex++;
			while (wPattern[wPatternIndex] == '*') {
				wPatternIndex++;
			}
			if (wPatternIndex == wPatternLen) {
				return true;
			}
			for (; wStrIndex < wStrLen; wStrIndex++) {
				if (patternMatchInternal(wStr, wStrIndex,
					wStrLen, wPattern, wPatternIndex,
					wPatternLen))
				{
					return true;
				}
			}
		}
	}
	if (wPattern[wPatternIndex] != wStr[wStrIndex]) {
		return false;
	}
	return true;
}

}; // namespace CONFIG4CPP_NAMESPACE
