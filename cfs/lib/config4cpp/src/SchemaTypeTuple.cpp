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

#include "SchemaTypeTuple.h"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeTuple::checkRule(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				typeName,
	const StringVector &		typeArgs,
	const char *				rule) const throw(ConfigurationException)
{
	StringBuffer				msg;
	int							i;
	int							len;
	const char *				elemType;
	SchemaType *				typeDef;

	//--------
	// Check there is at least one pair of type and name arguments.
	//--------
	len = typeArgs.length();
	if ((len == 0) || (len % 2 != 0)) {
		msg << "the '" << typeName << "' type requires pairs of type and "
			<< "name arguments in rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}

	//--------
	// Check that all the type arguments are valid types.
	//--------
	for (i = 0; i < len; i+=2) {
		elemType = typeArgs[i+0];
		typeDef = findType(sv, elemType);
		if (typeDef == 0) {
			msg << "unknown type '" << elemType << "' in rule '" << rule << "'";
			throw ConfigurationException(msg.c_str());
		}
		switch (typeDef->cfgType()) {
		case Configuration::CFG_STRING:
			break;
		case Configuration::CFG_LIST:
			msg << "you cannot embed a list type ('" << elemType
				<< "') inside a " << "tuple in rule '" << rule << "'";
			throw ConfigurationException(msg.c_str());
		case Configuration::CFG_SCOPE:
			msg << "you cannot embed a scope type ('" << elemType
				<< "') inside a " << "tuple in rule '" << rule << "'";
			throw ConfigurationException(msg.c_str());
		default:
			assert(0); // Bug!
		}
	}
}



void
SchemaTypeTuple::validate(
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
	StringBuffer				errSuffix;
	StringBuffer				fullyScopedName;
	const char **				list;
	const char *				elemValue;
	const char *				elemTypeName;
	int							i;
	int							listSize;
	int							typeArgsSize;
	int							elemNameIndex;
	int							typeIndex;
	int							rowNum;
	int							numElems;
	SchemaType *				elemTypeDef;
	StringVector				emptyArgs;
	bool						ok;
	const char *				sep;

	//--------
	// Check the length of the list matches the size of the tuple
	//--------
	typeArgsSize = typeArgs.length();
	assert(typeArgsSize != 0);
	assert(typeArgsSize % 2 == 0);
	numElems = typeArgsSize / 2;
	cfg->lookupList(scope, name, list, listSize);
	if (listSize != numElems) {
		cfg->mergeNames(scope, name, fullyScopedName);
		msg << cfg->fileName() << ": there should be " << numElems
			<< " entries in the '" << fullyScopedName << "' " << typeName
		    << "; entries denote";
		for (i = 0; i < numElems; i++) {
			msg << " '" << typeArgs[i*2+0] << "'";
			if (i < numElems-1) {
				msg << ",";
			}
		}
		throw ConfigurationException(msg.c_str());
	}
	//--------
	// Check each item is of the type specified in the tuple
	//--------
	for (i = 0; i < listSize; i++) {
		typeIndex     = (i * 2 + 0) % typeArgsSize;
		elemNameIndex = (i * 2 + 1) % typeArgsSize;
		rowNum = (i / numElems) + 1;
		elemValue = list[i];
		elemTypeName = typeArgs[typeIndex];
		elemTypeDef = findType(sv, elemTypeName);
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
				<< elemValue << "') for element " << i+1 << " ('"
			    << typeArgs[elemNameIndex] << "') of the '" << fullyScopedName
				<< "' " << typeName << sep << errSuffix;
			throw ConfigurationException(msg.c_str());
		}
	}
}

}; // namespace CONFIG4CPP_NAMESPACE
