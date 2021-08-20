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

#include "SchemaTypeBoolean.h"
#define BUG_MSG(OP) "bug in " OP " if an exception is thrown"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeBoolean::checkRule(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				typeName,
	const StringVector &		typeArgs,
	const char *				rule) const throw(ConfigurationException)
{
	StringBuffer				msg;

	if (typeArgs.length() != 0) {
		msg << "The '" << typeName << "' type should not take arguments in "
		    << "rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
}



bool
SchemaTypeBoolean::isA(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				value,
	const char *				typeName,
	const StringVector &		typeArgs,
	int							indentLevel,
	StringBuffer &				errSuffix) const
{
	bool						result;

	result = cfg->isBoolean(value);
	if (result == false) {
		errSuffix = "the value should be one of: 'false', 'true'";
	}
	return result;
}

}; // namespace CONFIG4CPP_NAMESPACE
