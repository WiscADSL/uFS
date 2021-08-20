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
#include "UidIdentifierProcessor.h"
#include "util.h"
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

UidIdentifierProcessor::UidIdentifierProcessor()
{
	assert(sizeof(long) >= 4); // at least 32-bits
	m_count = 0;
}



UidIdentifierProcessor::~UidIdentifierProcessor()
{
	// Nothing to do
}



//----------------------------------------------------------------------
// Spelling must be in one of the following forms:
//		- "foo"                 -->  "foo"
//		- "uid-<foo>"           -->  "uid-<digits>-<foo>"
//		- "uid-<digits>-<foo>"  -->  "uid-<new-digits>-<foo>"
// where "<foo>" does NOT start with a digit or "-"
//----------------------------------------------------------------------

void
UidIdentifierProcessor::expand(StringBuffer & spelling)
												throw (ConfigurationException)
{
	//--------
	// Common case optimizations
	//--------
	if (strchr(spelling.c_str(), '.') == 0) {
		expandOne(spelling);
		return;
	}
	if (strstr(spelling.c_str(), "uid-") == 0) {
		return;
	}

	//--------
	// Let's break apart the scoped name, expand each local part
	// and then recombine the parts into an expanded scoped name.
	//--------
	StringVector			vec;
	StringBuffer			buf;
	StringBuffer			msg;
	StringBuffer			result;
	int						i;
	int						len;

	splitScopedNameIntoVector(spelling.c_str(), vec);
	len = vec.length();
	for (i = 0; i < len; i++) {
		buf = vec[i];
		try {
			expandOne(buf);
		} catch (const ConfigurationException &) {
			msg << "'" << spelling << "' is not a legal identifier";
			throw ConfigurationException(msg.c_str());
		}
		result.append(buf);
		if (i < len - 1) {
			result.append(".");
		}
	}
	spelling = result;
}



void
UidIdentifierProcessor::expandOne(StringBuffer & spelling)
												throw (ConfigurationException)
{
	int					count;
	const char *		ptr;
	StringBuffer		msg;

	msg << "'" << spelling << "' is not a legal identifier";

	//--------
	// If spelling does not start with "uid-" then do nothing.
	//--------
	ptr = spelling.c_str();
	if (strncmp(ptr, "uid-", 4) != 0) {
		return;
	}

	StringBuffer		suffix;
	char				digits[10]; // big enough for 9 digits

	//--------
	// Check for "uid-" (with no suffix), because that is illegal
	//--------
	if (ptr[4] == '\0') {
		throw ConfigurationException(msg.c_str());
	}
	if (ptr[4] == '-') {
		 // illegal: "uid--foo"
		throw ConfigurationException(msg.c_str());
	}

	if (!isdigit(ptr[4])) {
		//--------
		// "uid-foo"  --> "uid-<digits>-foo"
		//--------
		assert(m_count < 1000 * 1000 * 1000);
		sprintf(digits, "%09ld", m_count);
		m_count ++;
		suffix = &(spelling.c_str()[4]); // deep copy
		spelling.empty();
		spelling << "uid-" << digits << "-" << suffix;
		return;
	}

	ptr += 4; // skip over "uid-"
	count = 0;
	while (isdigit(*ptr)) {
		ptr++;
		count ++;
	}
	assert(count > 0);
	if (*ptr == '\0' || *ptr != '-') {
		// illegal: "uid-<digits>" or "uid-<digits>foo"
		throw ConfigurationException(msg.c_str());
	}
	ptr ++; // point to just after "uid-<digits>-"
	if (*ptr == '\0') {
		// illegal: "uid-<digits>-"
		throw ConfigurationException(msg.c_str());
	}
	if (*ptr == '-') {
		// illegal: "uid-<digits>--"
		throw ConfigurationException(msg.c_str());
	}
	if (isdigit(*(ptr))) {
		// illegal: "uid-<digits>-<digits>foo"
		throw ConfigurationException(msg.c_str());
	}
	assert(m_count < 1000 * 1000 * 1000);
	sprintf(digits, "%09ld", m_count);
	m_count ++;
	suffix = ptr; // deep copy just after "uid-<digits>-"
	spelling.empty();
	spelling << "uid-" << digits << "-" << suffix;
}



const char *
UidIdentifierProcessor::unexpand(
	const char *			spelling,
	StringBuffer &			buf) const throw (ConfigurationException)
{
	//--------
	// Common case optimizations
	//--------
	if (strchr(spelling, '.') == 0) {
		return unexpandOne(spelling, buf);
	}
	if (strstr(spelling, "uid-") == 0) {
		return spelling;
	}

	//--------
	// Let's break apart the scoped name, unexpand each local part
	// and then recombine the parts into an unexpanded scoped name.
	//--------
	StringVector			vec;
	StringBuffer			result;
	StringBuffer			msg;
	const char *			str;
	int						i;
	int						len;

	splitScopedNameIntoVector(spelling, vec);
	len = vec.length();
	for (i = 0; i < len; i++) {
		try {
			str = unexpandOne(vec[i], buf);
		} catch (const ConfigurationException &) {
			msg << "'" << spelling << "' is not a legal identifier";
			throw ConfigurationException(msg.c_str());
		}
		result.append(str);
		if (i < len - 1) {
			result.append(".");
		}
	}
	buf.takeOwnershipOfStringIn(result);
	return buf.c_str();
}



const char *
UidIdentifierProcessor::unexpandOne(
	const char *			spelling,
	StringBuffer &			buf) const throw (ConfigurationException)
{
	int						count;
	const char *			ptr;

	//--------
	// If spelling does not start with "uid-<digits>-" then do nothing.
	//--------
	ptr = spelling;
	if (strncmp(ptr, "uid-", 4) != 0) { return spelling; }
	ptr += 4; // skip over "uid-"
	count = 0;
	while (isdigit(*ptr)) {
		ptr++;
		count ++;
	}
	if (count == 0 || *ptr != '-') {
		return spelling;
	}

	//--------
	// Okay, let's returned a modified spelling.
	//--------
	StringBuffer			suffix;

	suffix = (ptr + 1); // deep copy from just after "uid-<digits>-"
	buf.empty();
	buf << "uid-" << suffix;
	return buf.c_str();
}

}; // namespace CONFIG4CPP_NAMESPACE
