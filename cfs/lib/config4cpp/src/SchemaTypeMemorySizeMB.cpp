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

#include "SchemaTypeMemorySizeMB.h"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeMemorySizeMB::checkRule(
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
		msg << "The '" << typeName << "' type should take "
		    << "either no arguments or 2 arguments (denoting "
		    << "min and max memory sizes) in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	try {
		min = cfg->stringToMemorySizeMB("", "", typeArgs[0]);
	} catch (const ConfigurationException & ex) {
		msg << "Bad " << typeName << " value for the first ('min') "
			<< "argument in rule '" << rule << "'; should be in the format "
			<< "'<float> <units>' where <units> is one of: "
			<< "'MB', 'GB', 'TB', 'PB'";
		throw ConfigurationException(msg.c_str());
	}
	try {
		max = cfg->stringToMemorySizeMB("", "", typeArgs[1]);
	} catch (const ConfigurationException & ex) {
		msg << "Bad " << typeName << " value for the second ('max') "
			<< "argument in rule '" << rule << "'; should be in the format "
			<< "'<float> <units>' where <units> is one of: "
			<< "'MB', 'GB', 'TB', 'PB'";
		throw ConfigurationException(msg.c_str());
	}
	if ((min < -1) || (max < -1)) {
		msg << "The 'min' and 'max' of a " << typeName
			<< " cannot be negative in rule '" << rule << "'"
			<< "; min=" << min << "; max=" << max;
		throw ConfigurationException(msg.c_str());
	}
	if ((max != -1) && (min == -1 || min > max)) {
		msg << "The first ('min') argument is larger than the second "
			<< "('max') argument in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
}



bool
SchemaTypeMemorySizeMB::isA(
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
		val = cfg->stringToMemorySizeMB("", "", value);
	} catch (const ConfigurationException & ex) {
		errSuffix << "the value should be in the format '<units> <float>' "
				  << "where <units> is one of: 'MB', 'GB', 'TB', 'PB'";
		return false;
	}
	if (typeArgs.length() == 0) {
		return true;
	}
	min = cfg->stringToMemorySizeMB("", "", typeArgs[0]);
	max = cfg->stringToMemorySizeMB("", "", typeArgs[1]);
	if (val < min || val > max) {
		errSuffix << "the value is outside the permitted range ["
				  << typeArgs[0] << ", " << typeArgs[1] << "]";
		return false;
	}
	return true;
}

}; // namespace CONFIG4CPP_NAMESPACE
