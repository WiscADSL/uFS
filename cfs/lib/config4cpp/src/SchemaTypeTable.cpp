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

#include "SchemaTypeTable.h"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeTable::checkRule(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				typeName,
	const StringVector &		typeArgs,
	const char *				rule) const throw(ConfigurationException)
{
	StringBuffer				msg;
	int							i;
	int							len;
	const char *				columnType;
	SchemaType *				typeDef;

	//--------
	// Check there is at least one pair of column-type, column-name
	// arguments.
	//--------
	len = typeArgs.length();
	if ((len == 0) || (len % 2 != 0)) {
		msg << "the '" << typeName << "' type requires pairs of column-type "
			<< "and column-name arguments in " << "rule '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}

	//--------
	// Check that all the column-type arguments are valid types.
	//--------
	for (i = 0; i < len; i+=2) {
		columnType = typeArgs[i+0];
		typeDef = findType(sv, columnType);
		if (typeDef == 0) {
			msg << "unknown type '" << columnType << "' in rule '"
				<< rule << "'";
			throw ConfigurationException(msg.c_str());
		}
		switch (typeDef->cfgType()) {
		case Configuration::CFG_STRING:
			break;
		case Configuration::CFG_LIST:
			msg << "you cannot embed a list type ('" << columnType
				<< "') inside a table in rule '" << rule << "'";
			throw ConfigurationException(msg.c_str());
		case Configuration::CFG_SCOPE:
			msg << "you cannot embed a scope type ('" << columnType
				<< "') inside a table in rule '" << rule << "'";
			throw ConfigurationException(msg.c_str());
		default:
			assert(0); // Bug!
		}
	}
}



void
SchemaTypeTable::validate(
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
	const char *				colValue;
	const char *				colTypeName;
	int							i;
	int							listSize;
	int							typeArgsSize;
	int							colNameIndex;
	int							typeIndex;
	int							rowNum;
	int							numColumns;
	SchemaType *				colTypeDef;
	StringVector				emptyArgs;
	bool						ok;
	const char *				sep;

	//--------
	// Check that the length of the list is a multiple of the number
	// of columns in the table.
	//--------
	typeArgsSize = typeArgs.length();
	assert(typeArgsSize != 0);
	assert(typeArgsSize % 2 == 0);
	numColumns = typeArgsSize / 2;
	cfg->lookupList(scope, name, list, listSize);
	if (listSize % numColumns != 0) {
		cfg->mergeNames(scope, name, fullyScopedName);
		msg << cfg->fileName() << ": the number of entries in the '"
			<< fullyScopedName << "' " << typeName
			<< " is not a multiple of " << numColumns;
		throw ConfigurationException(msg.c_str());
	}

	//--------
	// Check each item in the list is of the type specified for its column
	//--------
	for (i = 0; i < listSize; i++) {
		typeIndex    = (i * 2 + 0) % typeArgsSize;
		colNameIndex = (i * 2 + 1) % typeArgsSize;
		rowNum = (i / numColumns) + 1;
		colValue = list[i];
		colTypeName = typeArgs[typeIndex];
		colTypeDef = findType(sv, colTypeName);
		ok = callIsA(colTypeDef, sv, cfg, colValue, colTypeName, emptyArgs,
					 indentLevel + 1, errSuffix);
		if (!ok) {
			if (errSuffix.length() == 0) {
				sep = "";
			} else {
				sep = "; ";
			}
			cfg->mergeNames(scope, name, fullyScopedName);
			msg << cfg->fileName() << ": bad " << colTypeName << " value ('"
				<< colValue << "') for the '" << typeArgs[colNameIndex]
				<< "' column in row " << rowNum << " of the '"
				<< fullyScopedName << "' " << typeName << sep << errSuffix;
			throw ConfigurationException(msg.c_str());
		}
	}
}

}; // namespace CONFIG4CPP_NAMESPACE
