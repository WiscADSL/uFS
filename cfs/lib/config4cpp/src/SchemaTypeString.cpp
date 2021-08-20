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

#include "SchemaTypeString.h"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeString::checkRule(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				typeName,
	const StringVector &		typeArgs,
	const char *				rule) const throw(ConfigurationException)
{
	StringBuffer				msg;
	int							len;
	int							min;
	int							max;

	len = typeArgs.length();
	if (len == 0) {
		return;
	}
	if (len != 2) {
		msg << "the '" << typeName << "' type should take "
		    << "either no arguments or 2 arguments (denoting min_length "
		    << "and max_length values) in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	try {
		min = cfg->stringToInt("", "", typeArgs[0]);
	} catch (const ConfigurationException & ex) {
		msg << "non-integer value for the first ('min_length') argument "
			<< "in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	try {
		max = cfg->stringToInt("", "", typeArgs[1]);
	} catch (const ConfigurationException & ex) {
		msg << "non-integer value for the second ('max_length') argument "
			<< "in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	if (min < 0 || max < 0) {
		msg << "the 'min_length' and 'max_length' of a string cannot be "
			<< "negative " << "in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	if (min > max) {
		msg << "the first ('min_length') argument is larger than the second "
			<< "('max_length') argument in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
}



bool
SchemaTypeString::isA(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				value,
	const char *				typeName,
	const StringVector &		typeArgs,
	int							indentLevel,
	StringBuffer &				errSuffix) const
{
	int							strLen;
	int							minLength;
	int							maxLength;

	if (typeArgs.length() == 2) {
		strLen = Configuration::mbstrlen(value);
		minLength = cfg->stringToInt("", "", typeArgs[0]);
		maxLength = cfg->stringToInt("", "", typeArgs[1]);
		if (strLen < minLength || strLen > maxLength) {
			errSuffix << "its length is outside the permitted range ["
					  << minLength << ", " << maxLength << "]";
			return false;
		}
	}
	return true;
}

}; // namespace CONFIG4CPP_NAMESPACE
