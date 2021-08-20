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

#include "SchemaTypeInt.h"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeInt::checkRule(
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
		msg << "the '" << typeName << "' type should take either no "
			<< "arguments or 2 arguments (denoting min and max values) "
			<< "in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	try {
		min = cfg->stringToInt("", "", typeArgs[0]);
	} catch (const ConfigurationException & ex) {
		msg << "non-integer value for the first ('min') argument in rule '"
			<< rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	try {
		max = cfg->stringToInt("", "", typeArgs[1]);
	} catch (const ConfigurationException & ex) {
		msg << "non-integer value for the second ('max') argument in rule '"
			<< rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	if (min > max) {
		msg << "the first ('min') value is larger than the second ('max') "
			<< "argument " << "in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
}



bool
SchemaTypeInt::isA(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				value,
	const char *				typeName,
	const StringVector &		typeArgs,
	int							indentLevel,
	StringBuffer &				errSuffix) const
{
	int							val;
	int							min;
	int							max;

	try {
		val = cfg->stringToInt("", "", value);
	} catch (const ConfigurationException & ex) {
		return false;
	}
	if (typeArgs.length() == 0) {
		return true;
	}
	min = cfg->stringToInt("", "", typeArgs[0]);
	max = cfg->stringToInt("", "", typeArgs[1]);
	if (val < min || val > max) {
		errSuffix << "the value is outside the permitted range ["
				  << typeArgs[0] << ", " << typeArgs[1] << "]";
		return false;
	}
	return true;
}

}; // namespace CONFIG4CPP_NAMESPACE
