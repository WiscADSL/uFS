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

#include "SchemaTypeList.h"
#include <config4cpp/SchemaValidator.h>


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeList::checkRule(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				typeName,
	const StringVector &		typeArgs,
	const char *				rule) const throw(ConfigurationException)
{
	StringBuffer				msg;
	int							len;
	const char *				listElementTypeName;
	SchemaType *				typeDef;

	//--------
	// Check there is one argument.
	//--------
	len = typeArgs.length();
	if (len != 1) {
		msg << "the '" << typeName << "' type requires one type argument "
			<< "in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}

	//--------
	// The argument must be the name of a string-based type.
	//--------
	listElementTypeName = typeArgs[0];
	typeDef = findType(sv, listElementTypeName);
	if (typeDef == 0) {
		msg << "unknown type '" << listElementTypeName << "' in rule '"
			<< rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	switch (typeDef->cfgType()) {
	case Configuration::CFG_STRING:
		break;
	case Configuration::CFG_LIST:
		msg << "you cannot embed a list type ('" << listElementTypeName
			<< "') inside another list in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_SCOPE:
		msg << "you cannot embed a scope type ('" << listElementTypeName
			<< "') inside a list in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0); // Bug!
	}
}



void
SchemaTypeList::validate(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				scope,
	const char *				name,
	const char *				typeName,
	const char *				origTypeName,
	const StringVector &		typeArgs,
	int							indentLevel) const
											throw(ConfigurationException)
{
	StringBuffer				msg;
	StringBuffer				fullyScopedName;
	StringBuffer				errSuffix;
	StringVector				emptyArgs;
	const char **				array;
	int							arraySize;
	int							i;
	SchemaType *				elemTypeDef;
	const char *				elemTypeName;
	const char *				elemValue;
	bool						ok;
	const char *				sep;

	assert(typeArgs.length() == 1);
	elemTypeName = typeArgs[0];
	elemTypeDef = findType(sv, elemTypeName);
	assert(elemTypeDef->cfgType() == Configuration::CFG_STRING);

	cfg->lookupList(scope, name, array, arraySize);
	for (i = 0; i < arraySize; i++) {
		elemValue = array[i];
		ok = callIsA(elemTypeDef, sv, cfg, elemValue, elemTypeName, emptyArgs,
					 indentLevel + 1, errSuffix);
		if (!ok) {
			if (errSuffix.length() == 0) {
				sep = "";
			} else {
				sep = "; ";
			}
			cfg->mergeNames(scope, name, fullyScopedName);
			msg << cfg->fileName() << ": bad " << elemTypeName << " value ('"
				<< elemValue << "') for '" << fullyScopedName << "[" << i
				<< "]'" << sep << errSuffix;
			throw ConfigurationException(msg.c_str());
		}
	}
}

}; // namespace CONFIG4CPP_NAMESPACE
